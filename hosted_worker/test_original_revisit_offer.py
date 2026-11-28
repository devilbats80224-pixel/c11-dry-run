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
    assert plan(bot)==(3,END,()), "C11_READY_ONLY_SCOPE_REPEATED_MARKET_REQUESTS"
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


def ready_decision_does_not_inherit_the_same_generation_wire_cap():
    now=[END+55_000]
    row=SimpleNamespace(pair=PAIR,symbol="EXAMPLEUSDT",generation=7,
        expires_ns=(END+110_000)*1_000_000,rank=1,ratio=Decimal(".25"),received_at=1)
    bot=SimpleNamespace(feed=SimpleNamespace(latest_owner_observation=(row,)),
                        observed_ranked=lambda r:r is row)
    plan=OriginalPlan(SimpleNamespace(freqtrade=bot),lambda:now[0],lambda:now[0]*1_000_000,
                      lambda *args:END-6*FIVE)
    first=plan.provisional_scope(PAIR,END)
    assert first is not None and first.expires==END+60_000
    offered=plan(bot)
    assert offered and offered[:2]==(7,END) and len(offered[2])==3
    from dedicated_queue import Job
    old_task,old_scope,request_id=offered[2][0]
    native_task=Job(request_id,old_scope.symbol,old_scope.generation,old_scope.end,
                    old_scope.expires,old_task.tf,2,old_task,request_id)
    assert plan.transport_still_due(native_task,now[0])
    now[0]=END+68_000
    plan.verified_readiness_at=lambda candidate,e,at: END+68_000 if (
        candidate is row and e==END and at>=END+68_000) else None
    current=plan.provisional_scope(PAIR,END)
    assert current is not None, "C11_FRESH_SAME_GENERATION_REUSES_EXPIRED_WIRE_CAP"
    assert current.generation==first.generation and current.expires==END+110_000
    assert plan.current(current,now[0]) and not plan.current(first,now[0])
    assert not plan.transport_still_due(native_task,now[0])
    assert plan(bot)==(7,END,()), "C11_FRESH_READY_REOPENED_WIRE"
    assert plan.provisional_scope(PAIR,END)==current
    print("C11_READY_SCOPE_SEPARATES_SAME_GENERATION_WIRE_CAP")


if __name__ == "__main__":
    for case in ("ready","not-ready","source-changed","generation-changed","expired","absent-state"):
        check(case)
    print("C11_ORIGINAL_SAME_SCOPE_WARM_OFFER_SURVIVES_RECOVERY")
    completed_input_uses_fresh_same_close_rank_after_old_expiry()
    ready_decision_does_not_inherit_the_same_generation_wire_cap()


def readable_decision_ignores_old_same_generation_scope_before_close_plus_60():
    now = [END + 3_271]
    row = SimpleNamespace(pair=PAIR, symbol="EXAMPLEUSDT", generation=8,
        expires_ns=(END + 4_201)*1_000_000, rank=1, ratio=Decimal(".25"),
        received_at=1, classification_seen_ns=1)
    feed = SimpleNamespace(latest_owner_observation=(row,))
    bot = SimpleNamespace(feed=feed, observed_ranked=lambda x: x is row)
    plan = OriginalPlan(SimpleNamespace(freqtrade=bot),lambda:now[0],lambda:now[0]*1_000_000,
                        lambda *_:END-6*FIVE)
    old = plan.provisional_scope(PAIR,END)
    offered = plan(bot)
    assert old is not None and offered and offered[2]
    # The provider is still live but the first, shortened wire scope expired.
    row.expires_ns = (END+61_456)*1_000_000
    row.classification_seen_ns = 2
    now[0] = END + 7_797
    ready_scope = getattr(plan,"confirmed_readable_scope",plan.provisional_scope)
    assert ready_scope(PAIR,END) is None, "A read attempt cannot claim readiness"
    ready_ms = END + 7_750
    plan.verified_readiness_at = lambda r,e,at:ready_ms if r is row and e==END and at>=ready_ms else None
    current = ready_scope(PAIR,END)
    assert current is not None, "C11_READABLE_DECISION_INHERITS_EXPIRED_PRE_WARM_SCOPE"
    assert (current.generation,current.end,current.expires)==(8,END,END+61_456)
    assert not plan.current(old,now[0])
    assert plan.current_readable_scope(current,now[0])
    assert not plan(bot)[2], "A post-ready decision cannot reopen old wire work"
    row.generation = 9
    row.expires_ns = (END+121_468)*1_000_000
    row.received_at = 2
    now[0] = END + 63_000
    current = ready_scope(PAIR,END)
    assert current is not None and current.expires==ready_ms+60_000
    now[0] = ready_ms+60_000
    assert ready_scope(PAIR,END) is None and not plan.current_readable_scope(current,now[0])
    print("C11_READABLE_DECISION_CLOCK_DOES_NOT_INHERIT_PRE_WARM_SCOPE")


if __name__ == "__main__":
    readable_decision_ignores_old_same_generation_scope_before_close_plus_60()
