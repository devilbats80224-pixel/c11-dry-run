"""Separate sufficient recent prices from proof of original recurrence."""
import sys
from types import SimpleNamespace as Ns
from unittest.mock import patch

sys.path[:0] = [
    "/opt/c11_owned_modules", "/opt/c11_worker", "/opt/c11_worker/ports",
    "/opt/c11_background_review", "/freqtrade",
]
from auto_cold_archive import AutomaticArchive, Hold, RECENT_PROOF_REQUIRED
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


if __name__ == "__main__":
    boundary_cases()
    identical_suffix_cannot_identify_original_state()
    print("C11_UNCACHED_PROVENANCE_CONTROLS_PASS")
