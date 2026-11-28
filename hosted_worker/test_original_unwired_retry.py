"""A completed zero-wire attempt must not consume the current native data offer."""
from decimal import Decimal
from types import SimpleNamespace

from compiled_candidate_factory import OriginalPlan

E=1_795_873_500_000
now=[E+2250]
row=SimpleNamespace(pair="SYNTH/USDT:USDT",symbol="SYNTHUSDT",generation=6705,
                    expires_ns=(E+60_000)*1_000_000,rank=1,ratio=Decimal(".25"),received_at=1)
bot=SimpleNamespace(feed=SimpleNamespace(latest_owner_observation=(row,)),
                    observed_ranked=lambda candidate:candidate is row)
plan=OriginalPlan(SimpleNamespace(freqtrade=bot),lambda:now[0],lambda:now[0]*1_000_000,
                  lambda *_:E-6*300_000)
plan.warm_prepare=lambda *_:True
plan.warm_minute_span=lambda *_:45
first,=(plan(bot)[2])
task,scope,ticket=first
# As the observed 21:45 failure, a completed job had no wire debit and
# has already left the queue. The still-current immutable candidate remains.
now[0]=E+7250
retry=getattr(plan,"reissue_unwired_minute",lambda *_:False)
assert not retry(scope,ticket,True) and not retry(scope,ticket,None)
assert plan(bot)[2]==(first,)
retry(scope,ticket,False)
second,=(plan(bot)[2])
other,second_scope,other_id=second
assert other_id!=ticket, "C11_CONFIRMED_ZERO_WIRE_LOST_NEXT_ORIGINAL_ATTEMPT"
assert second_scope is scope and other.end==task.end and other.deadline==task.deadline
assert other.request==now[0] and other.minute_limit==task.minute_limit and other.ticket!=task.ticket
assert not retry(scope,ticket,False)
now[0]=E+60_000
assert not retry(scope,other_id,False), "C11_UNWIRED_RETRY_CANNOT_EXTEND_NATIVE_LEASE"
print("C11_CONFIRMED_UNWIRED_MINUTE_REISSUED_SAME_SCOPE_WITH_NEW_TICKET")
