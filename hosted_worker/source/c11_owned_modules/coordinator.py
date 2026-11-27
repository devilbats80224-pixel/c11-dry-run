"""Build-only original-owner zero-wait bounded native ready coordinator."""
from dataclasses import dataclass
from threading import Lock,get_ident

WIDTH={'1m':60_000,'5m':300_000,'15m':900_000}

@dataclass(frozen=True)
class Scope:
    pair:str
    symbol:str
    generation:int
    end:int
    expires:int

@dataclass(frozen=True)
class Ticket:
    request_id:str
    scope:Scope
    lane:str
    request_ms:int
    count:int

@dataclass(frozen=True)
class Receipt:
    ticket:Ticket
    received_ms:int
    raw:tuple
    source_proof:str

class Ready:
    def __init__(self,*,wall,authority,original_store,latency_ms=30,max_requests=60,effective_expiry=None):
        self.owner=get_ident()
        self.wall,self.authority,self.store=wall,authority,original_store
        self.latency_budget=latency_ms
        self.effective_expiry=effective_expiry or (lambda scope,_:scope.expires)
        self.limit=max_requests
        self.lock=Lock()
        self.active=None
        self.allocated=0
        self.expected={}
        self.completed={}
        self.drained=set()

    def _check_owner(self):
        if get_ident()!=self.owner:raise RuntimeError('ORIGINAL_OWNER_REQUIRED')

    def reset(self,g,end):
        self._check_owner()
        with self.lock:
            marker=(g,end)
            if marker==self.active:return
            if self.active is None or end!=self.active[1]:self.allocated=0
            self.active=marker
            self.expected.clear(); self.completed.clear(); self.drained.clear()

    def register(self,ticket):
        self._check_owner()
        t=ticket
        if (type(t) is not Ticket or type(t.scope) is not Scope or t.lane not in WIDTH or
             not t.scope.end<=t.request_ms<t.scope.expires<=t.scope.end+60_000 or
             not 0<t.count<=499 or not self.authority(t.scope,self.wall())
             or self.wall()>=self.effective_expiry(t.scope,self.wall())):return False
        with self.lock:
            if (t.scope.generation,t.scope.end)!=self.active or self.allocated>=self.limit:
                return False
            if t.request_id in self.expected or any(x.scope==t.scope and x.lane==t.lane
                                                   for x in self.expected.values()):return False
            self.expected[t.request_id]=t; self.allocated+=1
            return True

    def forget_unsent(self,ticket):
        self._check_owner()
        with self.lock:
            if self.expected.get(ticket.request_id) is not ticket or ticket.request_id in self.completed:
                return False
            del self.expected[ticket.request_id]
            self.allocated-=1
            return True

    def complete(self, receipt, *, nowait=False):
        # Transport-side validation is not equivalent to owner admission.
        # Only frozen simple primitives accepted; producer has no store access.
        r=receipt
        if (type(r) is not Receipt or type(r.ticket) is not Ticket or
                type(r.raw) is not tuple or len(r.raw)!=r.ticket.count or
                any(type(row) is not tuple or len(row)!=12 or
                    any(type(v) not in (int,str) for v in row) for row in r.raw)):
            return False
        key=r.ticket.request_id
        if not self.lock.acquire(blocking=not nowait):return False
        try:
            if self.expected.get(key)!=r.ticket or key in self.completed:return False
            self.completed[key]=r
            return True
        finally:self.lock.release()

    def available_for_owner_revisit(self,scope):
        """Only the original owner may inspect; this never claims a receipt or disk."""
        self._check_owner()
        n=self.wall()
        if n>=self.effective_expiry(scope,n) or not self.lock.acquire(blocking=False):return False
        try:
            return ((scope.generation,scope.end)==self.active and scope not in self.drained
                    and {r.ticket.lane for r in self.completed.values() if r.ticket.scope==scope}==set(WIDTH))
        finally:self.lock.release()

    def consume(self,scope,*,normalize,atomic_commit,remaining_for_protections):
        self._check_owner()
        # No waiting behind recipient/producer: a contended mailbox is a HOLD.
        if not self.lock.acquire(blocking=False):return 'HOLD_QUEUE_CONTENDED',None
        try:
            if scope in self.drained:return 'HOLD_ALREADY_DRAINED',None
            if (scope.generation,scope.end)!=self.active:return 'HOLD_GENERATION_CHANGED',None
            jobs={r.ticket.lane:r for r in self.completed.values() if r.ticket.scope==scope}
            if set(jobs)!=set(WIDTH):return 'HOLD_NOT_READY',None
            self.drained.add(scope)
            jobs=tuple(jobs[lane] for lane in ('1m','5m','15m'))
        finally:self.lock.release()
        now=self.wall()
        deadline=min(scope.expires,self.effective_expiry(scope,now))
        if (not scope.end<=now<deadline or not self.authority(scope,now) or
            any(not r.ticket.request_ms<=r.received_ms<=now for r in jobs)):
            return 'HOLD_ORIGINAL_AUTHORITY_OR_RECEIPT_CLOCK',None
        if min(remaining_for_protections(),deadline-now)<=self.latency_budget:
            return 'HOLD_TIME_RESERVED_FOR_PROTECTIONS',None
        result=normalize(scope,jobs)
        if result is None:return 'HOLD_NATIVE_COHERENCE',None
        def still_valid():
            at=self.wall()
            return (scope.generation,scope.end)==self.active and scope.end<=at<min(scope.expires,self.effective_expiry(scope,at)) and self.authority(scope,at)
        if not still_valid():return 'HOLD_ORIGINAL_AUTHORITY_OR_RECEIPT_CLOCK',None
        pre=self.wall()
        if min(remaining_for_protections(),self.effective_expiry(scope,pre)-pre)<=self.latency_budget:
            return 'HOLD_TIME_RESERVED_FOR_PROTECTIONS',None
        committed=atomic_commit(self.store,scope,result,still_valid)
        if committed is None:return 'HOLD_NATIVE_COMMIT_REFUSED',None
        if self.wall()-now>self.latency_budget or not still_valid():
            # Commit can physically finish after its last pre-commit check. A
            # genuine persisted row is not authority for an expired decision.
            return 'HOLD_POST_COMMIT_OVERRUN',None
        return 'ORIGINAL_OWNER_COMMITTED_READY',committed
