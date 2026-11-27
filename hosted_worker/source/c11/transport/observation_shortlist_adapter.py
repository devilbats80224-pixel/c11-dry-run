"""Dormant pure adapter. No I/O, imports of the scanner, collection or trading.

An observation shortlist is input to LATER native-OI validation, not OI proof.
The caller must clear this lease on any IPC/transport/parser failure.
"""
import copy
import json
import math
import time
from datetime import datetime, timezone


class ObservationLease:
    MAX_BYTES = 8 * 1024 * 1024

    def __init__(self):
        # These watermarks deliberately survive clear()/IPC reconnect. Replaying
        # a generation cannot reset its first absolute lease expiry.
        self._generation = None
        self._expiry_tick = None
        self._water_tick = None
        self._water_age = None
        self._generated_epoch = None
        self.clear("NO_PUBLIC_SNAPSHOT")

    def clear(self, reason="IPC_UNAVAILABLE"):
        self._envelope = None
        self._accepted_tick = None
        self._reason = reason

    def accept_bytes(self, payload, *, monotonic_now, wall_now=None):
        """Accept one EOF-framed, same-UID native IPC response. Never read Keychain."""
        self.clear("INVALID_PUBLIC_ENVELOPE")
        if not isinstance(payload, bytes) or len(payload) > self.MAX_BYTES:
            return False
        if (isinstance(monotonic_now, bool) or not isinstance(monotonic_now, (int, float))
                or not math.isfinite(monotonic_now)):
            return False
        try:
            def unique_pairs(pairs):
                out = {}
                for key, value in pairs:
                    if key in out:
                        raise ValueError("DUPLICATE_PUBLIC_KEY")
                    out[key] = value
                return out
            value = json.loads(payload, object_pairs_hook=unique_pairs,
                               parse_constant=lambda _: (_ for _ in ()).throw(ValueError("NONFINITE")))
        except (ValueError, TypeError, UnicodeError, RecursionError):
            return False
        if not isinstance(value, dict) or value.get("schema") != "openmarket_guarded_envelope_v1":
            return False
        if value.get("entry_authorized") is not False or value.get("dryrun_consumption_authorized") is not False:
            return False
        snap = value.get("snapshot")
        if not isinstance(snap, dict):
            return False
        pid, number = snap.get("pid"), snap.get("round")
        if (isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0
                or isinstance(number, bool) or not isinstance(number, int) or number <= 0):
            self._reason = "MISSING_COLLECTOR_GENERATION"; return False
        if snap.get("schema") != "openmarket_native_page60_guarded_snapshot_v1":
            self._reason = "WRONG_SNAPSHOT_SCHEMA"; return False
        def epoch(text):
            if not isinstance(text, str) or len(text) > 40:
                raise ValueError("INVALID_LOCAL_RECEIPT_TIME")
            stamp = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if stamp.tzinfo is None or stamp.utcoffset() != timezone.utc.utcoffset(stamp):
                raise ValueError("LOCAL_RECEIPT_NOT_UTC")
            return stamp.timestamp()
        try:
            generated = epoch(value.get("generated_at"))
            started = epoch(snap.get("started_at"))
            pages = snap.get("pages")
            if not isinstance(pages, list) or not 1 <= len(pages) <= 5:
                raise ValueError("MISSING_RECEIPT")
            oldest = min(epoch(page.get("received_at")) for page in pages)
            now_wall = time.time() if wall_now is None else wall_now
            age, ttl = value.get("receive_age_seconds"), value.get("stale_after_seconds")
            if any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x)
                   for x in (now_wall, age, ttl)) or age < 0 or ttl <= 0:
                raise ValueError("INVALID_RECEIPT_AGE")
            if (generated > now_wall + 5 or oldest > now_wall + 5 or started > now_wall + 5
                    or oldest < started - 5 or generated < oldest - 5):
                raise ValueError("FUTURE_OR_REVERSED_RECEIPT")
            effective_age = max(age, max(0, now_wall - oldest))
        except (ValueError, TypeError, AttributeError, OverflowError, OSError):
            self._reason = "INVALID_OR_FUTURE_RECEIPT"; return False
        generation = (pid, number, started, oldest)
        if self._water_tick is not None and monotonic_now < self._water_tick:
            self._reason = "MONOTONIC_CLOCK_ROLLBACK"; return False
        if self._generation is not None:
            prior_pid, prior_round, prior_start, prior_receipt = self._generation
            if pid != prior_pid:
                self._reason = "COLLECTOR_IDENTITY_CHANGED_REBIND_REQUIRED"; return False
            if (number < prior_round or started < prior_start or oldest < prior_receipt
                    or generated < self._generated_epoch):
                self._reason = "GENERATION_OR_RECEIPT_ROLLBACK"; return False
            if number == prior_round and generation != self._generation:
                self._reason = "SAME_ROUND_RECEIPT_IDENTITY_CHANGED"; return False
            if generation == self._generation:
                if monotonic_now >= self._expiry_tick:
                    self._reason = "GENERATION_EXPIRED_NO_REPLAY"; return False
                effective_age = max(effective_age, self._water_age + monotonic_now - self._water_tick)
        expiry = monotonic_now + min(90.0, ttl) - effective_age
        if generation == self._generation:
            expiry = min(expiry, self._expiry_tick)
        self._generation = generation
        self._expiry_tick = expiry
        self._water_tick = float(monotonic_now)
        self._water_age = effective_age
        self._generated_epoch = generated
        if monotonic_now >= expiry:
            self._reason = "STALE_GENERATION_REJECTED"; return False
        self._envelope = value
        self._accepted_tick = float(monotonic_now)
        self._reason = "ACCEPTED_OBSERVATION_ONLY"
        return True

    def shortlist(self, verified_metadata_by_symbol, *, monotonic_now, max_receive_age=90.0):
        """Return verified-metadata subset for a separately authorized validation call.

        The existing paper_oi_scanner.scan_once(verified_metadata, end_ms, reader=...)
        can accept that subset. THIS METHOD NEVER CALLS IT OR STARTS ITS NETWORK.
        """
        out = {"status": self._reason, "observation_symbols": [],
               "downstream_validation_universe": [], "candidates": {},
               "ranked_candidates": [], "source_validation_pass": False,
               "entry_authorized": False, "dryrun_started": False,
               "scope": "OPENMARKET_OBSERVATION_SHORTLIST_NOT_FULL_UNIVERSE",
               "native_oi_points": [], "valid_24h_anchor": False,
               "acceptance_missing": ["scale_validation", "USD_basis", "source_as_of",
                                      "classification_currentness", "ordering_and_coverage",
                                      "downstream_native_OI_points_and_timing"]}
        env = self._envelope
        if env is None or self._accepted_tick is None:
            return out
        numbers = (monotonic_now, max_receive_age, env.get("receive_age_seconds"), env.get("stale_after_seconds"))
        if any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x) for x in numbers):
            self.clear("INVALID_RECEIPT_CLOCK"); out["status"] = self._reason; return out
        elapsed = monotonic_now - self._water_tick
        age = self._water_age + elapsed
        if (elapsed < 0 or max_receive_age <= 0 or age < 0
                or self._expiry_tick is None or monotonic_now >= self._expiry_tick
                or age > min(90.0, max_receive_age, env["stale_after_seconds"])):
            self.clear("STALE_PUBLIC_OBSERVATION"); out["status"] = self._reason; return out
        if (env.get("health") != "OBSERVATION_READY_PROVISIONAL"
                or env.get("receipt_fresh") is not True
                or env.get("observation_candidates_authorized") is not True):
            out["status"] = "HEALTH_BLOCKED_NO_SHORTLIST"; return out
        snap = env.get("snapshot")
        if not isinstance(snap, dict) or snap.get("entry_authorized") is not False:
            return out
        if snap.get("exchange") != "BINANCE_FUTURES" or snap.get("category") != "PERPETUAL":
            return out
        rows = snap.get("observations")
        if not isinstance(rows, list) or len(rows) > 300 or not isinstance(verified_metadata_by_symbol, dict):
            return out
        seen = set()
        for row in rows:
            if not isinstance(row, dict) or row.get("native_observation_candidate") is not True:
                continue
            market = row.get("market", {})
            if (not isinstance(market, dict) or market.get("exchange") != "BINANCE_FUTURES"
                    or market.get("category") != "PERPETUAL" or row.get("classification") != "CRYPTO"
                    or row.get("negative_markers") != [] or row.get("entry_authorized") is not False):
                continue
            symbol = market.get("rawSymbol")
            if not isinstance(symbol, str) or len(symbol) > 100 or symbol.startswith("BTC") or symbol in seen:
                continue
            meta = verified_metadata_by_symbol.get(symbol)
            if (not isinstance(meta, dict) or meta.get("symbol") != symbol
                    or meta.get("classification") != "VERIFIED_CRYPTO"
                    or meta.get("market") != "UM_FUTURES" or meta.get("eligible") is not True):
                continue
            seen.add(symbol)
            out["observation_symbols"].append(symbol)
            out["downstream_validation_universe"].append(copy.deepcopy(meta))
        out["status"] = "OBSERVATION_SHORTLIST_FOR_SEPARATE_NATIVE_VALIDATION_ONLY"
        out["receive_age_seconds"] = age
        out["absolute_lease_expiry_monotonic"] = self._expiry_tick
        out["collector_round"] = snap.get("round")
        return out
