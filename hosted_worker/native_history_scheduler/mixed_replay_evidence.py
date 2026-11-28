"""Fail-closed fixture-only evidence for the final packaged mixed replay.

This module does not schedule a strategy, create an order or synthesize prices.
The runner supplies its pinned replay files, clocks and disposable paths.
"""
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import io
import json
from pathlib import Path
from types import SimpleNamespace
import zipfile

from atr_er_state import REQUIRED_QUARTERS
from causal_bars import MINUTE, QUARTER, ClosedBar
from cold_prefix import verify_archive_blob
import entry_feed
from docker_observer import CaptureBridge, DockerObserver
from observer_rpc import ObserverClient


@dataclass(frozen=True)
class PinnedMinute:
    row: tuple
    available_at: int
    source_sha: str
    source_kind: str

    def closed(self):
        r = self.row
        return ClosedBar(r[0], self.available_at, *(float(r[i]) for i in (1, 2, 3, 4, 5, 7)))


def load_causal_tape(root, tail, tail_sha, end, shift):
    """Return an exact verified 401-quarter prefix plus its terminal quarter."""
    cutoff = end - QUARTER
    start = cutoff - REQUIRED_QUARTERS * QUARTER
    want = set(range(start, end, MINUTE))
    retained = {}

    def insert(raw, observed, pin, kind):
        r = list(raw)
        assert len(r) == 12
        r[0], r[6], r[8] = int(r[0]), int(r[6]), int(r[8])
        assert r[0] % MINUTE == 0 and r[6] == r[0] + MINUTE - 1
        assert type(observed) is int and observed >= r[6] + 1
        r[0] += shift
        r[6] += shift
        if r[0] not in want:
            return
        assert r[0] not in retained, ('C11_MIXED_DUPLICATE_OPEN', r[0])
        # A retrospective source may have been observed before the shifted
        # candle closes. Never claim availability before either bound.
        item = PinnedMinute(tuple(r), max(observed, r[6] + 1), pin, kind)
        assert item.closed().valid(MINUTE), 'C11_MIXED_INVALID_PINNED_ROW'
        retained[r[0]] = item

    assert len(tail_sha) == 64
    for page in tail:
        assert len(page['rows']) == page['count']
        assert [int(r[0]) for r in page['rows']] == list(range(page['start'], page['stop'], MINUTE))
        for row in page['rows']:
            insert(row, page['actual_received_ms'], tail_sha, 'PINNED_CCXT_TAIL')

    missing = want.difference(retained)
    needed_dates = {datetime.fromtimestamp((t - shift)/1000, timezone.utc).strftime('%Y-%m-%d')
                    for t in missing}
    records = json.loads((root / 'manifest.json').read_bytes())
    for record in records:
        if record.get('date') not in needed_dates:
            continue
        name = record['name']
        assert Path(name).name == name
        blob = (root / name).read_bytes()
        verify_archive_blob(name, record['bytes'], record['sha256'], blob,
                            (root / (name + '.CHECKSUM')).read_bytes())
        with zipfile.ZipFile(io.BytesIO(blob)) as archive:
            assert len(archive.namelist()) == 1
            with archive.open(archive.namelist()[0]) as member:
                stream = csv.reader(io.TextIOWrapper(member, newline=''))
                header = next(stream)
                assert header[0] == 'open_time' and len(header) == 12
                for row in stream:
                    if int(row[0]) + shift in missing:
                        insert(row, record['actual_received_ms'], record['sha256'], 'PINNED_PUBLIC_ARCHIVE')
    assert set(retained) == want, ('C11_MIXED_CLOSE_GRID_MISSING', len(want-set(retained)))
    assert all(retained[t].available_at <= end for t in range(start, cutoff, MINUTE)), (
        'C11_MIXED_RETAINED_PREFIX_NOT_AVAILABLE_BEFORE_TARGET')
    return start, cutoff, retained


class ReplayPublication:
    """Staged deterministic ranking changes; native CaptureBridge implements intake."""
    def __init__(self, symbol, wall):
        self.symbol, self.wall = symbol, wall
        self.sealed = None

    def publish(self, generation, *, qualified):
        old = self.sealed
        assert type(generation) is int and (old is None or old[0] < generation)
        self.sealed = (generation, self.wall(), qualified is True)

    def response(self, nonce):
        assert self.sealed is not None
        generation, published, qualified = self.sealed
        now = self.wall()
        assert now >= published
        received = datetime.fromtimestamp(published/1000, timezone.utc)
        lease = received + timedelta(seconds=60)
        rows = [{'symbol':self.symbol,'native_oi_change_24h_raw':'0.25' if qualified else '0.00'}]
        rows.extend({'symbol':f'BAR{i}USDT','native_oi_change_24h_raw':'0.00'} for i in range(59))
        assert len({x['symbol'] for x in rows}) == 60
        left = max(0, published+60_000-now)
        return dict(nonce=nonce,status='FRESH' if left else 'STALE',source=entry_feed.SOURCE,
                    query_sha256=entry_feed.QUERY,offset=0,page_size=60,generation=generation,
                    left_ms=left,received_at=received.isoformat(),lease_until=lease.isoformat(),rows=rows)


def observer_feed(root, symbol, pair, metadata, wall, mono):
    """The same checked EntryFeed/CaptureBridge/SQLite path the packaged Worker uses."""
    binding = sha256(b'isolated-c11-mixed-packaged-callback-v1').hexdigest()
    body_sha = sha256(metadata).hexdigest()
    # The stopped comparison bundle includes its own observer binding.
    # Give this synthetic publication its own isolated fixture store instead
    # of attempting to reuse or alter that existing stopped-bundle evidence.
    directory=root/'mixed-observer'
    directory.mkdir(mode=0o700)
    observer = DockerObserver(directory/'docker-observer.sqlite', binding, wall=wall, mono=mono)
    publication = ReplayPublication(symbol, wall)
    feed = None
    bridge = CaptureBridge(publication, observer, binding, lambda:feed.classification)
    bridge.send(dict(op='C11_PAPER_BRIDGE_V1',nonce='0'*32))
    bridge.receive(2)
    feed = entry_feed.EntryFeed(bridge, 1)
    def classify():
        return SimpleNamespace(verified={symbol:pair},body_sha256=body_sha,received_monotonic_ns=mono())
    feed.classification = classify()
    feed._reviewed_metaclass_fetch = classify
    return feed, ObserverClient(bridge, binding, docked_current=True), observer, publication


def assert_known_once(callbacks, reads, *, target, generation):
    """Authenticate exact original read, rather than equating no orders with a decision."""
    assert len(callbacks) == len(reads) == 1, ('C11_MIXED_READ_COUNT', len(callbacks), len(reads))
    cb, read = callbacks[0], reads[0]
    assert (cb['end_ms'], cb['generation']) == (target, generation)
    assert (read['end_ms'], read['generation'], read['read_at']) == (target, generation, cb['read_at'])
    assert read['status'] in ('NO_SIGNAL', 'NW15_PRIMARY_SIGNAL')
    assert read['rpd'] in ('PASS', 'VETO')
    assert read['real_read'] is True and cb['called'] is True
