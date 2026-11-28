"""An admitted same-close candidate must not lose its original warm offer."""
from decimal import Decimal
from types import SimpleNamespace

from compiled_candidate_factory import OriginalPlan

END=1795773600000
PAIR="EXAMPLE/USDT:USDT"
FIVE=300_000


def check(mode):
    now=[END+4_000]
    cursor=[END-6*FIVE]
    row=SimpleNamespace(pair=PAIR,symbol="EXAMPLEUSDT",generation=2,
        expires_ns=(END+27_000)*1_000_000,rank=1,ratio=Decimal(".25"),received_at=1)
    bot=SimpleNamespace(feed=SimpleNamespace(latest_owner_observation=(row,)),
                        observed_ranked=lambda r: r is row)
    plan=OriginalPlan(SimpleNamespace(freqtrade=bot),lambda:now[0],lambda:now[0]*1_000_000,
                      lambda *args:cursor[0])
    warm_selected=[]
    def select(s,start):
        warm_selected.append((s,start))
        return mode!="not-ready"
    plan.warm_prepare=select
    observed=plan.provisional_scope(PAIR,END)
    assert observed is not None
    getattr(plan,"remember_original_warm_offer",lambda _scope:None)(observed)
    cursor[0]=END if mode!="absent-state" else None
    now[0]+=250
    if mode=="source-changed":
        row.received_at+=1
    if mode=="generation-changed":
        row.generation+=1
    if mode=="expired":
        now[0]=END+28_000
    result=plan(bot)
    if mode=="ready":
        assert result is not None and len(result[2])==1, "original strategy warm offer lost after recovery"
        task,scope,_=result[2][0]
        assert task.tf=="1m"
        assert scope==observed
        assert [(s,start) for s,start in warm_selected]==[(observed,END-6*FIVE)]
    else:
        assert result is None or not result[2],mode
    if mode in ("source-changed","generation-changed","expired","absent-state"):
        assert not warm_selected


def completed_input_uses_fresh_same_close_rank_after_old_expiry():
    # The 11:00 shape: the older scope has only 11.949 s from E.
    # This fixture deliberately lets a slower refill finish after E+60.
    now=[END+4000]
    old=SimpleNamespace(pair=PAIR,symbol="EXAMPLEUSDT",generation=2,
        expires_ns=(END+11949)*1_000_000,rank=1,ratio=Decimal(".25"),received_at=1)
    feed=SimpleNamespace(latest_owner_observation=(old,))
    bot=SimpleNamespace(feed=feed,observed_ranked=lambda r:r in feed.latest_owner_observation)
    plan=OriginalPlan(SimpleNamespace(freqtrade=bot),lambda:now[0],lambda:now[0]*1_000_000,
                      lambda *args:END)
    expired=plan.provisional_scope(PAIR,END)
    assert expired is not None and expired.expires==END+11949
    ready_ms=END+68000
    # This interface must be driven by verified, complete canonical input,
    # not by minute receipt alone or a candidate's claimed readiness clock.
    plan.verified_readiness_at=lambda row,e,at: ready_ms if (
        row.symbol==old.symbol and e==END and at>=ready_ms) else None
    now[0]=END+69000
    assert plan.provisional_scope(PAIR,END) is None
    fresh=SimpleNamespace(pair=PAIR,symbol=old.symbol,generation=3,
        expires_ns=(END+125000)*1_000_000,rank=1,ratio=Decimal(".25"),received_at=2)
    feed.latest_owner_observation=(fresh,)
    scope=plan.provisional_scope(PAIR,END)
    assert scope is not None, 'C11_CLOSE_CLOCK_DISCARDS_NEWLY_READABLE_FRESH_CANDIDATE'
    assert (scope.generation,scope.end,scope.expires)==(3,END,END+125000)
    assert plan.current(scope,now[0]) and not plan.current(expired,now[0])
    now[0]=END+100000
    newer=SimpleNamespace(pair=PAIR,symbol=old.symbol,generation=4,
        expires_ns=(END+150000)*1_000_000,rank=1,ratio=Decimal(".25"),received_at=3)
    feed.latest_owner_observation=(newer,)
    scope=plan.provisional_scope(PAIR,END)
    assert scope is not None and (scope.generation,scope.expires)==(4,ready_ms+60000)
    now[0]=ready_ms+60000
    assert plan.provisional_scope(PAIR,END) is None
    assert not plan.current(scope,now[0])
    print("C11_READY_CLOCK_REQUIRES_FRESH_SAME_CLOSE_CANDIDATE")


if __name__ == "__main__":
    for case in ("ready","not-ready","source-changed","generation-changed","expired","absent-state"):
        check(case)
    print("C11_ORIGINAL_SAME_SCOPE_WARM_OFFER_SURVIVES_RECOVERY")
    completed_input_uses_fresh_same_close_rank_after_old_expiry()
