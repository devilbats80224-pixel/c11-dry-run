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


if __name__ == "__main__":
    for case in ("ready","not-ready","source-changed","generation-changed","expired","absent-state"):
        check(case)
    print("C11_ORIGINAL_SAME_SCOPE_WARM_OFFER_SURVIVES_RECOVERY")
