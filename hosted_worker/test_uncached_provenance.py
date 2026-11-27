"""Separate sufficient recent prices from proof of original recurrence."""
import sys
from hashlib import sha256
from threading import Event
from types import SimpleNamespace as Ns
from unittest.mock import patch

sys.path[:0] = [
    "/opt/c11_owned_modules", "/opt/c11_worker", "/opt/c11_worker/ports",
    "/opt/c11_background_review", "/freqtrade",
]
from auto_cold_archive import ArchiveProgress, AutomaticArchive, Hold, Receipt, RECENT_PROOF_REQUIRED
from causal_bars import ClosedBar, FIVE, QUARTER
from native_primary_lane import NativePrimaryLane
from primary_only_route import NW_WARM
from rpd_checkpoint import RpdState


def boundary_cases():
    end = 1795773600000
    market_time = end + 4_000
    floor = end - NW_WARM * QUARTER
    assert end % QUARTER == 0
    for offset, can_plan_primary, origin_path_refused in (
        (QUARTER, False, False),
        (0, True, False),
        (-QUARTER, True, True),
    ):
        fact = Ns(symbol="EXAMPLEUSDT", pair="EXAMPLE/USDT:USDT", origin=floor+offset, boundary=end)
        gates = []
        asked = []
        owner = Ns(pending=None, check_store=lambda: None,
                   run=Ns(wall=lambda: market_time,
                          plan=Ns(provisional_scope=lambda *a: asked.append(a))))
        lane = NativePrimaryLane(owner, lambda _: None)
        with patch.object(lane, "phase_hold", return_value=None), patch.object(
            lane, "gate", side_effect=lambda reason, *_: gates.append(reason)
        ):
            lane.tick((fact,), lambda: True)
        assert bool(asked) is can_plan_primary
        assert ("NW_LOOKBACK_UNAVAILABLE" in gates) is not can_plan_primary

        # Explicitly exercise the unseeded origin-only transport. A future
        # trusted checkpoint or archive resolver is outside this branch.
        class PlannerReached(Exception):
            pass
        archive = AutomaticArchive(None, lambda: market_time)
        with patch("auto_cold_archive.windows", side_effect=PlannerReached):
            try:
                archive.start(fact, None)
            except PlannerReached:
                assert not origin_path_refused
            except Hold as exc:
                assert origin_path_refused and exc.status == RECENT_PROOF_REQUIRED
            else:
                raise AssertionError("missing origin admission outcome")


def identical_suffix_cannot_identify_original_state():
    start = 1795773600000
    left = RpdState("EXAMPLEUSDT", start, start, start)
    right = RpdState("EXAMPLEUSDT", start, start, start, active_long=True, long_score=80.)
    # A flat nonzero-range suffix neither confirms a pivot nor resets the
    # earlier active-long flag. This tests original recurrence, not prices
    # obtained from a real market or a trading outcome.
    for offset in range(4000):
        at = start + offset * FIVE
        candle = ClosedBar(at, at+FIVE, 100., 101., 99., 100., 10., 1000.)
        left.advance(candle, at+FIVE)
        right.advance(candle, at+FIVE)
    assert left.next_open == right.next_open and left.n == right.n == 4000
    assert left.active_long is False and right.active_long is True
    assert left.long_score is None and right.long_score == 80.


def reviewed_download_rejects_changed_checksum():
    body = b"fixture bytes"
    pin = sha256(body).hexdigest()
    name = "EXAMPLEUSDT-1m-2026-11-25.zip"
    url = "https://example.invalid/" + name
    calls = []
    def fetch(target, limit, cancel):
        calls.append(target)
        return Receipt((("f"*64)+"  "+name).encode() if target.endswith(".CHECKSUM") else body, 100)
    archive=AutomaticArchive(Ns(get=fetch),lambda:100)
    p=ArchiveProgress(archive,"EXAMPLEUSDT",1,2,0,())
    try:
        p.fetch(name,url,100,1000,Event(),pin=(pin,len(body)))
    except Hold as exc:
        assert exc.status == "AUTO_ARCHIVE_UNVERIFIED"
    else:
        raise AssertionError("C11_CHANGED_PROVIDER_CHECKSUM_ACCEPTED")
    assert calls == [url+".CHECKSUM"]


def reviewed_download_stops_after_expiry():
    name = "EXAMPLEUSDT-1m-2026-11-25.zip"
    url = "https://example.invalid/" + name
    digest = sha256(b"test").hexdigest()
    calls = []
    clock = [100]
    def fetch(target, limit, cancel):
        calls.append(target)
        clock[0] = 101
        return Receipt((digest+"  "+name).encode(), 100)
    archive = AutomaticArchive(Ns(get=fetch), lambda: clock[0])
    progress = ArchiveProgress(archive, "EXAMPLEUSDT", 1, 2, 0, (),
                               reviewed=Ns(review_expires_ms=101))
    try:
        progress.fetch(name,url,100,1000,Event(),pin=(digest,4))
    except Hold as exc:
        assert exc.status == "AUTO_ARCHIVE_REVIEWED_SCOPE"
    else:
        raise AssertionError("C11_EXPIRED_MANIFEST_FETCHED_BODY")
    assert calls == [url+".CHECKSUM"]


def discovered_archives_use_existing_bounds():
    from datetime import datetime, timezone
    def utc(day):
        return int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()*1000)
    end = utc("2026-11-27") + 2*QUARTER
    requests = []
    transport = Ns(get=lambda *args: requests.append(args))
    short = Ns(symbol="EXAMPLEUSDT",pair="EXAMPLE/USDT:USDT",
               origin=utc("2025-11-10"),boundary=end)
    too_long = Ns(symbol=short.symbol,pair=short.pair,
                  origin=utc("2020-02-03"),boundary=end)
    with patch.object(ArchiveProgress, "resume", autospec=True,
                      side_effect=lambda self, _: self):
        default = AutomaticArchive(transport, lambda: end)
        try:
            default.start(short, Event())
        except Hold as exc:
            assert exc.status == RECENT_PROOF_REQUIRED
        else:
            raise AssertionError("DISCOVERY_MUST_NOT_BE_DEFAULT")
        enabled = AutomaticArchive(transport, lambda: end, discover_unseeded=True)
        assert enabled.start(short, Event()).discovered
        try:
            enabled.start(too_long, Event())
        except Hold as exc:
            assert exc.status == "AUTO_ARCHIVE_DISCOVERY_LIMIT"
        else:
            raise AssertionError("C11_DISCOVERY_BYPASSED_ARCHIVE_BUDGET")
    assert not requests

    archive = AutomaticArchive(transport, lambda: end, max_bytes=10, discover_unseeded=True)
    progress = ArchiveProgress(archive, short.symbol, short.origin, end, 0,
                               (("one","unused",end),("two","unused",end)), discovered=True)
    progress.ledger.append({"bytes": 10})
    try:
        progress.resume(Event())
    except Hold as exc:
        assert exc.status == "AUTO_ARCHIVE_DISCOVERY_LIMIT"
    else:
        raise AssertionError("C11_DISCOVERY_BYTE_BUDGET_RESET_AFTER_YIELD")
    assert not requests


if __name__ == "__main__":
    boundary_cases()
    discovered_archives_use_existing_bounds()
    reviewed_download_rejects_changed_checksum()
    reviewed_download_stops_after_expiry()
    identical_suffix_cannot_identify_original_state()
    print("C11_UNCACHED_PROVENANCE_CONTROLS_PASS")
