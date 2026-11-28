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

# Drive the actual owner loop. Inject only its clock, transport result, and
# the owner's reservation release, not its retirement or reissue decisions.
from threading import Lock, get_ident

from capture import query_for
from shared_native_budget import SharedNativeBudget
from unified_owner import UnifiedScheduler


def owner_fixture(at):
    now[0]=E+2250
    planner=OriginalPlan(SimpleNamespace(freqtrade=bot),lambda:now[0],lambda:now[0]*1_000_000,
                         lambda *_:E-6*300_000)
    planner.warm_prepare=lambda *_:True
    planner.warm_minute_span=lambda *_:45
    initial,=(planner(bot)[2])
    request,authority,reqid=initial
    query=query_for(request)
    key=(authority.symbol,60_000,int(query['startTime']),E,int(query['limit']))
    released=[]
    cleanup=[True]
    meter=object.__new__(SharedNativeBudget)
    meter.lock=Lock()
    meter.sent=set()
    owner=object.__new__(UnifiedScheduler)
    owner.thread=get_ident()
    owner.closed=False
    owner.worker=SimpleNamespace(freqtrade=bot)
    owner.diagnostic=owner.first_gate=owner.reconfirm=owner.isolated_prior_seed_loader=None
    owner.plan=planner
    owner.wall=lambda:now[0]
    owner.marker=(authority.generation,E)
    owner.covers={key:authority}
    owner.tickets={key:reqid}
    owner.verified=set()
    owner.retired=set()
    owner.queue=SimpleNamespace(limiter=meter,inflight={},poll=lambda:())
    def forget(ticket_id):
        released.append(ticket_id)
        return cleanup[0]
    owner.adapter=SimpleNamespace(capture=SimpleNamespace(end=E),forget_unsent=forget)
    owner.pending_roster=SimpleNamespace(plan=lambda *_:())
    owner.nw_without_rpd=SimpleNamespace(after_drained=lambda *_:None)
    now[0]=at
    return owner,key,reqid,initial,released,cleanup


s,k,r,old,released,cleanup=owner_fixture(E+7250)
with s.queue.limiter.lock:
    s._offer_after_protection(bot)
assert s.tickets.get(k)==r and r not in s.retired and not released, (
    "C11_BUSY_DEBIT_PROBE_DROPPED_COMPLETED_NATIVE_TICKET_BEFORE_RECHECK"
)
assert s.plan(bot)[2]==(old,)
cleanup[0]=False
s._offer_after_protection(bot)
assert s.tickets.get(k)==r and r not in s.retired and s.plan(bot)[2]==(old,)
cleanup[0]=True
s._offer_after_protection(bot)
assert k not in s.tickets and r in s.retired and s.plan(bot)[2][0][2]!=r

sent,k,r,old,released,_=owner_fixture(E+7250)
sent.queue.limiter.sent.add(r)
sent._offer_after_protection(bot)
assert k not in sent.tickets and r in sent.retired and sent.plan(bot)[2]==(old,)

expired,k,r,old,released,_=owner_fixture(E+60_000)
# Keep the original window roster visible for this scheduler expiration test
# even when a provider publication will be refreshed on its next owner cycle.
p=expired.plan
class ExpiredPlan(SimpleNamespace):
    def __call__(self,_):
        return old[1].generation,E,(old,)
expired.plan=ExpiredPlan(entries=p.entries,reissue_unwired_minute=p.reissue_unwired_minute)
with expired.queue.limiter.lock:
    expired._offer_after_protection(bot)
assert k not in expired.tickets and r in expired.retired and released==[r]
print("C11_BUSY_DEBIT_PROBE_RECHECKED_ONLY_WITHIN_ORIGINAL_SCOPE")
