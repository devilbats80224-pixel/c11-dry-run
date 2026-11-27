"""The owner must not drop an exact, already-warmed RPD veto before strategy."""
from types import SimpleNamespace as NS
from causal_bars import CausalBars, ClosedBar, FIVE, MINUTE, QUARTER, SegmentStart
from c11_primary_decision_seam_v1.exact_rpd5_join import join, Scope
from native_primary_lane import NativePrimaryLane
from rpd_checkpoint import WARM

symbol = "EXAMPLEUSDT"
end = 1795773600000
first = end - QUARTER
now = end + 4_000
origin = first - WARM * FIVE


def run(kind):
    db = CausalBars(":memory:")
    try:
        db.set_segment(SegmentStart(symbol, origin, origin + FIVE))
        closes = []
        for t in range(origin, end, FIVE):
            if kind == "stale" and t >= first:
                break
            if t < first:
                receipt = t + FIVE
            else:
                receipt = now + 1 if kind == "future" and t == end - FIVE else end + 2_000
            closes.append(ClosedBar(t, receipt, 100, 104, 99, 101, 50, 5000))
        db.record(symbol, FIVE, closes)
        target = first if kind == "stale" else end
        progress = db.rpd_progress(symbol, target, now + (kind == "future"))
        assert progress is not None and progress.next_open == target
        if kind != "stale":
            assert join(Scope(symbol, end), now + (kind == "future"), [progress.closed()])[0] == "VETO"
        if kind != "stale":
            db.record(symbol, MINUTE, [
                ClosedBar(t, end + 2_000, 100, 104 if kind != "conflict" or t != first else 105,
                          99, 101, 10, 1000)
                for t in range(first, end, MINUTE) if kind != "missing" or t != first
            ])
        original = db.db.execute("SELECT payload FROM rpd_checkpoint").fetchone()
        plan = NS(last={}, current=lambda *_: kind != "stale_scope")
        owner = NS(store=db, run=NS(wall=lambda: now, closed=False,
                   queue=NS(inflight={}, protection_due=lambda: kind == "protection_due"), plan=plan),
                   worker=NS(freqtrade=NS(config={"dry_run": True})),
                   minute_admission=NS(cycle_headroom=lambda: True), tap=NS(current=lambda _: True))
        lane = NativePrimaryLane(owner, lambda *_: None)
        fact = NS(symbol=symbol, origin=origin)
        scope = NS(generation=17, end=end, expires=now if kind == "expired" else end+60_000,
                   pair="EXAMPLE/USDT:USDT", symbol=symbol)
        assert not lane.terminal_five
        lane.record_verified_veto(fact, scope)
        rows = db.db.execute("SELECT state FROM feature_status").fetchall()
        assert db.db.execute("SELECT payload FROM rpd_checkpoint").fetchone() == original
        return rows
    finally:
        db.db.close()


for control in ("stale", "future", "stale_scope", "expired", "protection_due", "missing", "conflict"):
    assert not run(control), control
assert run("exact") == [("VETO",)], "C11_COMPLETE_NATIVE_RPD_VETO_NOT_DELIVERED"
print("C11_EXACT_NATIVE_RPD_VETO_CONTROLS_PASS")
