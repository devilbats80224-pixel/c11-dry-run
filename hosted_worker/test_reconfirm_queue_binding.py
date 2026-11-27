"""Exercise old two-wave dispatch through the installed shared Worker queue."""
import sys
from concurrent.futures import Future
from types import SimpleNamespace as N

sys.path[:0]=['/opt/c11_owned_modules','/opt/c11_worker','/opt/c11_worker/ports','/freqtrade']
from dedicated_queue import WorkerOwnedPublicQueue
from reconfirm_dispatch import ReconfirmDispatch
from shared_native_budget import SharedNativeBudget

end=1795773600000
now=[end+7000]
scope=N(symbol='EXAMPLEUSDT',generation=2,end=end,expires=end+60000)
# Scope must be hashable: the dispatcher deduplicates the original plan.
from collections import namedtuple
scope=namedtuple('Scope','symbol generation end expires')(*vars(scope).values())
protected=[False]
claimed=[]
def submit(job,token,claim):
    claimed.append((job,claim))
    return Future()
budget=SharedNativeBudget(lambda:now[0])
queue=WorkerOwnedPublicQueue(worker=None,wall=lambda:now[0],authority=lambda *args:True,
    protection_due=lambda:protected[0],
    executor=N(exclusive_parallelism=1,submit_original_public=submit),global_limiter=budget,
    handle_attested=lambda *args:True)
assert queue.rollover(2,end)
budget.note_authoritative_ip_header(now[0],29,2400)
leases=[]
def offer(s,limit):
    assert s==scope and limit.free_slots>=3
    leases.append(s)
    return 'SIX_NO_EXTRA_CALLS_RESERVED'
def wave(s,limit,ticket):
    assert s==scope and limit.free_slots>=3
    return tuple(N(tf=tf,stage=1,ticket=ticket+i,triad_only=True)
                 for i,tf in enumerate(('5m','15m','1m')))
dispatch=ReconfirmDispatch(N(_ready=lambda s:True,offer=offer,wave=wave),
                           queue,N(namespace='a'*32,ticket_counter=10))
protected[0]=True
dispatch.step((scope,))
assert not claimed and not leases and budget.escrow==0
protected[0]=False
dispatch.step((scope,))
assert len(claimed)==3 and leases==[scope]
assert len(queue.inflight)==3 and budget.escrow==3
assert [j.tf for j,_ in claimed]==['5m','15m','1m']
assert len({j.request_id for j,_ in claimed})==3
dispatch.step((scope,))
assert len(claimed)==3,'C11_RECONFIRM_DUPLICATE_WAVE'
print('C11_RECONFIRM_ORIGINAL_WAVE_USES_SHARED_QUEUE')
