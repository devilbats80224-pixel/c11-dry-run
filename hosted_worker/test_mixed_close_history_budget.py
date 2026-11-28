"""Exercise the installed shared meter with a reproducible mixed-close pressure tape."""
from collections import Counter
from types import SimpleNamespace

from primary_only_route import PrimaryMinuteJob
from retained_page_schema import accepted_history_trace
from shared_native_budget import SharedNativeBudget

E = 1795822200000
GENERATION = 1
SYMBOL = "SYNTHUSDT"
clock = E + 5000


def make_meter(costs=(0, 0), used=20):
    budget = SharedNativeBudget(lambda: clock)
    budget.set_scope(GENERATION, E)
    budget.costs = costs
    budget.note_authoritative_ip_header(clock, used, 2400)
    return budget


def page(k, weight=2):
    return SimpleNamespace(ticket=f"{k:032x}", generation=GENERATION, end=E, weight=weight)


def assert_reason(reason, meter):
    assert meter.try_reserve_history(page(1)) is None
    assert meter.last_history_hold == reason, (
        "C11_MIXED_CLOSE_BUDGET_REASON", reason, meter.last_history_hold
    )
    # Previously all three ceilings collapsed into one unhelpful token.
    for stage in ("RESERVE_REJECTED", "GRANT_REJECTED"):
        assert accepted_history_trace(dict(
            schema="c11-shared-history-page-v1", symbol=SYMBOL, end_ms=E,
            generation=GENERATION, page_start_ms=E-900000, page_end_ms=E,
            requested_rows=15, stage=stage, at_ms=clock, stage_reason=reason,
        )), (stage, reason)


def main():
    assert_reason("HISTORY_BUDGET_REQUEST_COUNT", make_meter((96, 96)))
    assert_reason("HISTORY_BUDGET_WEIGHT_COUNT", make_meter((73, 143)))
    assert_reason("HISTORY_BUDGET_IP_RESERVE", make_meter((24, 24), used=1175))
    paused = make_meter()
    paused.optional_wire_fault(429, 60000)
    assert_reason("HISTORY_BUDGET_PAUSED", paused)

    # The mix is deliberately synthetic; it is not a claim about which
    # particular ceiling the production run hit.
    meter = make_meter((28, 70))
    counts = Counter()
    for i in range(1, 57):  # seven cold candidates, eight pages each
        job = page(i)
        token = meter.try_reserve_history(job)
        if token is None:
            counts[meter.last_history_hold] += 1
        else:
            assert meter.claim_history_send(token, job)
            counts["SENT"] += 1
    assert counts == {"SENT": 37, "HISTORY_BUDGET_WEIGHT_COUNT": 19}, counts
    assert meter.costs == (65, 144)
    assert meter.last_reserve_status is None

    meter = make_meter((70, 141))
    ticket = page(57)
    token = meter.try_reserve_history(ticket)
    assert token is not None
    meter.account_incumbent_before_wire(2)
    assert not meter.claim_history_send(token, ticket)
    assert meter.last_history_hold == "HISTORY_BUDGET_WEIGHT_COUNT"
    assert meter.costs == (71, 143)
    # Seven fully cold candidates (eight exact pages each) sharing the
    # actual IP meter with native requests. Data-only preservation must not
    # eat the direct request cap or extend any signal's lease.
    meter = make_meter((28, 70))
    for sym in range(7):
        right = E
        for k in range(8):
            minutes = 495 if k < 7 else 435
            left = right - minutes * 60_000
            job = PrimaryMinuteJob(f"SYNTH{sym}USDT", GENERATION, E, E+60_000, left, right,
                                   f"{100 + sym * 8 + k:032x}")
            assert job.valid(clock)
            token = meter.try_reserve_history(job)
            assert token is not None, ("C11_DATA_ONLY_REPLAY_STARVED", sym, k, meter.last_history_hold)
            assert not meter.claim_send(token, job), "HISTORY_TOKEN_CANNOT_ENTER_DIRECT_LANE"
            assert meter.claim_history_send(token, job)
            right = left
        assert right == E - 260 * 900_000
    assert meter.costs == (84, 182) and meter.history_costs == (56, 112)
    direct = SimpleNamespace(native_task=None, ticket="f"*32, deadline=E+60_000, weight=2,
                             generation=GENERATION, end=E)
    native = meter.try_reserve(direct)
    assert native is not None and meter.claim_send(native, direct), "DATA_ONLY_USED_DIRECT_CAP"
    late = PrimaryMinuteJob("SYNTHUSDT", GENERATION, E, E+60_000,
                            E-900_000, E, "d"*32)
    assert meter.try_reserve_history(late) is not None
    meter.note_authoritative_ip_header(clock, 1190, 2400)
    another = PrimaryMinuteJob("SYNTHUSDT", GENERATION, E, E+60_000,
                               E-900_000, E, "e"*32)
    token=meter.try_reserve_history(another)
    assert token is not None, "C11_TYPED_REFILL_BLOCKED_BY_DIRECT_RESERVE"
    meter.note_authoritative_ip_header(clock, 2399, 2400)
    assert not meter.claim_history_send(token, another)
    assert meter.last_history_hold == "HISTORY_BUDGET_IP_RESERVE"
    print("C11_MIXED_CLOSE_PRESSURE_REASON_AND_GRANT_ACCOUNTING_PASS")



def refill_uses_measured_ip_headroom_not_a_static_reserve():
    meter=make_meter(used=2398)
    def job(index,symbol):
        return PrimaryMinuteJob(symbol,GENERATION,E,E+60_000,E-900_000,E,
                                f"{900+index:032x}")
    # Verified header leaves two weight. Distinct candidates should both
    # reserve before either completes; a third cannot overrun the true cap.
    a,b,c=(job(i,x) for i,x in enumerate(('FIRSTUSDT','SECONDUSDT','THIRDUSDT')))
    ta=meter.try_reserve_history(a)
    tb=meter.try_reserve_history(b)
    assert ta is not None and tb is not None,(
        'C11_FIXED_RESERVE_BLOCKS_REAL_IP_HEADROOM',meter.last_history_hold)
    assert meter.try_reserve_history(c) is None
    assert meter.claim_history_send(ta,a) and meter.claim_history_send(tb,b)
    assert meter.try_reserve_history(c) is None
    meter.optional_wire_fault(429,90_000)
    assert meter.try_reserve_history(c) is None
    assert meter.last_history_hold=='HISTORY_BUDGET_PAUSED'
    print('C11_DATA_REFILL_HONORS_ACTUAL_IP_HEADROOM_AND_BACKOFF')

refill_uses_measured_ip_headroom_not_a_static_reserve()
main()
