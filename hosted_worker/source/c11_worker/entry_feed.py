"""Original v2 ranks carried across the VM boundary without sharing clocks."""
from exact_symbol import ID_CHAR
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import re
import secrets
import time

from bridge_protocol import Refused, need
from collector import number
import public_market
from oi20_rank_slots import bridge_slots

SOURCE = 'paper-market-openmarket-oi20-v1'
QUERY = 'ef5fea69e4bea1b579e2857dcf8dcc65f7516edecad0e0310a7b3a2ff377b815'
SYMBOL = re.compile(fr'{ID_CHAR}{{2,40}}')
RATIO = re.compile(r'-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?')
METADATA_NS = 300_000_000_000
QUARTER_NS = 900_000_000_000
ACQUISITION_NS = 60_000_000_000


@dataclass(frozen=True)
class Ranked:
    pair: str
    symbol: str
    rank: int
    ratio: Decimal
    generation: int
    received_at: str
    expires_ns: int
    classification_seen_ns: int | None = None


class EntryFeed:
    def __init__(self, pipe, generation_floor):
        self.pipe = pipe
        self.generation_floor = generation_floor
        self.classification = None
        self.retry_metadata_ns = 0
        self._reviewed_metaclass_fetch = None
        self._classification_rollover_attempt = None
        self._last_tracking = {}  # Watch only; never an entry fallback.
        self.latest_owner_observation = ()
        self._owner_read_sequence = 0

    def refresh_classification(self):
        now = time.monotonic_ns()
        if self.classification is not None and now < self.classification.received_monotonic_ns + METADATA_NS:
            return self._cover_active_acquisition(now)
        need(now >= self.retry_metadata_ns, 'C11_BINANCE_CLASSIFICATION_NOT_READY')
        self.retry_metadata_ns = now + 60_000_000_000
        fetch = self._reviewed_metaclass_fetch or public_market.fetch
        self.classification = fetch()
        return self.classification

    def _cover_active_acquisition(self, now):
        previous = self.classification
        fetch = self._reviewed_metaclass_fetch
        if not callable(fetch):
            return previous  # Existing read-only/monotonic adapter is untouched.
        cached_until = previous.received_monotonic_ns + METADATA_NS
        wall = time.time_ns()
        boundary = wall // QUARTER_NS * QUARTER_NS
        remaining = boundary + ACQUISITION_NS - wall
        if (remaining <= 0 or cached_until >= now + remaining
                or now < self.retry_metadata_ns or self._classification_rollover_attempt == boundary):
            return previous
        # This is the existing metered native classifier, not a new request
        # lane or a publisher lease extension. Try once for this overlapping
        # cache/window; a failed anticipation must not consume the regular
        # expired-cache refresh opportunity.
        self._classification_rollover_attempt = boundary
        try:
            refreshed = fetch()
            seen = refreshed.received_monotonic_ns
            need(type(seen) is int and previous.received_monotonic_ns < seen <= time.monotonic_ns()
                 and type(refreshed.verified) is dict and type(refreshed.body_sha256) is str
                 and re.fullmatch('[a-f0-9]{64}',refreshed.body_sha256),
                 'C11_THIN_CLASSIFIER_REFRESH_UNVERIFIED')
        except Exception:
            if time.monotonic_ns() < cached_until:
                return previous  # exact unexpired proof; no artificial extension
            raise
        self.classification = refreshed
        return refreshed

    def read(self):
        self.latest_owner_observation = ()
        metadata = self.refresh_classification()
        nonce = secrets.token_hex(16)
        started = time.monotonic_ns()
        self.pipe.send({'op': 'READ_C11_V2', 'nonce': nonce})
        frame = self.pipe.receive(2)
        received = time.monotonic_ns()
        need(frame.get('nonce') == nonce and frame.get('status') in {'FRESH','FRESH_PARTIAL'}
             and frame.get('source') == SOURCE and frame.get('query_sha256') == QUERY
             and frame.get('offset') == 0 and frame.get('page_size') == 60
             and type(frame.get('generation')) is int and frame['generation'] >= self.generation_floor
             and type(frame.get('left_ms')) is int and 0 < frame['left_ms'] <= 60000,
             'C11_BRIDGE_FRAME_IDENTITY')
        source_time = datetime.fromisoformat(frame['received_at'])
        source_expiry = datetime.fromisoformat(frame['lease_until'])
        need((source_expiry - source_time).total_seconds() == 60, 'C11_BRIDGE_PROVIDER_TTL')
        # Source residual was computed on the Mac. Charge the entire request RTT
        # before translating it into the Linux clock.
        deadline = min(started + frame['left_ms'] * 1_000_000,
                       metadata.received_monotonic_ns + METADATA_NS)
        need(received < deadline, 'C11_BRIDGE_SOURCE_EXPIRED')
        rows = frame['rows']
        ranks=bridge_slots(frame)
        out, seen, previous, watch = [], set(), None, {}
        for rank, row in zip(ranks,rows):
            symbol, raw = row['symbol'], row['native_oi_change_24h_raw']
            need(type(symbol) is str and SYMBOL.fullmatch(symbol) and symbol not in seen
                 and type(raw) is str and len(raw) <= 128 and RATIO.fullmatch(raw), 'C11_BRIDGE_ROW')
            value = number(raw)
            need(value is not None and (previous is None or value <= previous), 'C11_BRIDGE_RANK')
            previous = value
            seen.add(symbol)
            pair = metadata.verified.get(symbol)
            if value >= Decimal('0.20'):
                if pair:
                    out.append(Ranked(pair, symbol, rank, value, frame['generation'], frame['received_at'],
                                      deadline, metadata.received_monotonic_ns))
                    watch[symbol] = pair
                elif symbol in self._last_tracking:
                    # Missing classification is not authoritative removal.
                    watch[symbol] = self._last_tracking[symbol]
        need(time.monotonic_ns() < deadline, 'C11_BRIDGE_SOURCE_EXPIRED')
        need(frame['status']!='FRESH_PARTIAL' or bool(out), 'C11_OI20_PARTIAL_HAS_NO_VERIFIED_ELIGIBLE_ROWS')
        self.generation_floor = frame['generation']
        # Commit only after a complete, still-fresh first page, even if empty.
        if frame["status"]=="FRESH_PARTIAL":
            # Never use an omitted row for entry; preserve prior watch-only tracking.
            watch.update((s,pair) for s,pair in self._last_tracking.items() if s not in seen)
        self._last_tracking = watch
        self.latest_owner_observation = tuple(out)
        self._owner_read_sequence += 1
        return self.latest_owner_observation

    def owner_cycle_stamp(self):
        return self._owner_read_sequence

    def warmup_observation_since(self,original_stamp):
        """No refresh or retention: use only a completed read from this cycle."""
        if type(original_stamp) is not int or not 0<=original_stamp<self._owner_read_sequence:
            return ()
        now=time.monotonic_ns()
        return tuple(x for x in self.latest_owner_observation
                     if x.generation==self.generation_floor and now<x.expires_ns)

    def tracking_pairs(self):
        return tuple(self._last_tracking.values())

    def ranked(self):
        try:
            return self.read()
        except (Refused, OSError, ValueError, TypeError, KeyError, AttributeError):
            return ()

    def candidate(self, pair):
        return next((row for row in self.ranked() if row.pair == pair), None)
