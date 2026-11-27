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
from auto_cold_archive import AutoColdAdmission, ArchiveProgress, AutomaticArchive, Hold, Receipt, RECENT_PROOF_REQUIRED
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



def missing_month_finds_daily_without_a_catalogue():
    from datetime import datetime, timezone
    from io import BytesIO
    from zipfile import ZipFile
    from cold_archive_identity import archive_location
    from auto_cold_archive import DAY
    origin=int(datetime(2026,10,31,tzinfo=timezone.utc).timestamp()*1000)
    cutoff=origin+DAY
    now=cutoff+2*QUARTER
    symbol='陌生候選USDT'
    fact=Ns(symbol=symbol,pair=symbol[:-4]+'/USDT:USDT',origin=origin,boundary=now)
    _,monthly=archive_location(symbol,'2026-10')
    name,daily=archive_location(symbol,'2026-10-31')
    data=BytesIO()
    with ZipFile(data,'w') as zip_file:
        zip_file.writestr(name.removesuffix('.zip')+'.csv',''.join(
            f'{t},100,101,99,100,10,{t+59_999},1000,1,0,0,0\n'
            for t in range(origin,cutoff,60_000)))
    body=data.getvalue()
    calls=[]
    def fetch(url,maximum,cancel):
        assert not cancel.is_set()
        calls.append(url)
        if url==monthly+'.CHECKSUM':
            raise Hold('AUTO_ARCHIVE_FILE_MISSING')
        if url==daily+'.CHECKSUM':
            return Receipt(f'{sha256(body).hexdigest()}  {name}'.encode(),now)
        assert url==daily and len(body)<=maximum
        return Receipt(body,now)
    archive=AutomaticArchive(Ns(get=fetch),lambda:now)
    try:
        source,prefix=archive.prepare(fact,Event())
    except Hold as exc:
        raise AssertionError('C11_NO_CATALOGUE_MISSING_MONTH_DID_NOT_TRY_DAILY') from exc
    assert (source.origin,source.cutoff,prefix.rpd.next_open)==(origin,cutoff,cutoff)
    assert len(prefix.quarters)==96 and set(width for width,_ in prefix.atr)=={FIVE,QUARTER}
    assert calls==[monthly+'.CHECKSUM',daily+'.CHECKSUM',daily]
    with patch('auto_cold_archive.MAX_FILES',0):
        limited=ArchiveProgress(archive,symbol,origin,cutoff,0,((name,monthly,cutoff),))
        try:
            limited.substitute_closed_days(symbol+'-1m-2026-10.zip',cutoff)
        except Hold as exc:
            assert exc.status=='AUTO_ARCHIVE_BUDGET'
        else:
            raise AssertionError('C11_DAILY_SUBSTITUTE_BYPASSED_FILE_LIMIT')
    pinned=ArchiveProgress(archive,symbol,origin,cutoff,0,(),reviewed=Ns())
    assert not pinned.substitute_closed_days(symbol+'-1m-2026-10.zip',cutoff)


def expired_completed_source_cannot_block_ready_peer():
    now=1795773605000
    end=now//QUARTER*QUARTER
    origin=end-8*QUARTER
    blocked=Ns(symbol='EXPIREDUSDT',pair='EXPIRED/USDT:USDT',origin=origin,boundary=end)
    peer=Ns(symbol='陌生候選USDT',pair='陌生候選/USDT:USDT',origin=origin,boundary=end)
    owner=Ns(cold_delivery_window=lambda:True,cold_delivery_allows=lambda f:f in (blocked,peer))
    def unexpected_fetch(*_):
        raise AssertionError('C11_READY_DELIVERY_STARTED_IO')
    lane=AutoColdAdmission(lambda:now,unexpected_fetch)
    try:
        for fact,expires in ((blocked,now),(peer,now+5_000)):
            lane.offer(fact)
            lane.pending[fact.symbol].result=(Ns(cutoff=end-QUARTER,review_expires_ms=expires),fact)
        attempts=[]
        def install(_owner,fact,prefix,*,early):
            assert early and prefix is fact
            attempts.append(fact.symbol)
            return lane.sources[fact.symbol].review_expires_ms>now
        with patch.object(lane,'install',side_effect=install):
            lane.tick(owner,ready_only=True)
            assert attempts==[peer.symbol],'C11_EXPIRED_READY_SOURCE_STARVES_VALID_PEER'
            assert peer.symbol not in lane.pending
            held=lane.pending[blocked.symbol]
            assert held.result is None and held.status=='AUTO_ARCHIVE_REVIEWED_SCOPE'
            lane.tick(owner,ready_only=True)
            assert attempts==[peer.symbol]
    finally:
        lane.close()

if __name__ == "__main__":
    missing_month_finds_daily_without_a_catalogue()
    expired_completed_source_cannot_block_ready_peer()
    boundary_cases()
    discovered_archives_use_existing_bounds()
    reviewed_download_rejects_changed_checksum()
    reviewed_download_stops_after_expiry()
    identical_suffix_cannot_identify_original_state()
    print("C11_UNCACHED_PROVENANCE_CONTROLS_PASS")
