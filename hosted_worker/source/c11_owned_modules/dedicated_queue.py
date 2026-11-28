"""Build-only scheduler contract; requires a compiled shared gate and exclusive read-only executor."""
from dataclasses import dataclass
from threading import get_ident

@dataclass(frozen=True)
class Job:
    ticket:str
    symbol:str
    generation:int
    end:int
    expires:int
    tf:str
    weight:int
    native_task:object=None
    request_id:str=""

    @property
    def pair(self):
        return self.symbol[:-4]+'/USDT:USDT' if self.symbol.endswith('USDT') else ''

class WorkerOwnedPublicQueue:
    def __init__(self,*,worker,wall,authority,protection_due,executor,global_limiter,handle_attested,prewire_authority=lambda _j,_n:True):
        if getattr(executor,'exclusive_parallelism',None)!=1 or not handle_attested(worker,executor,global_limiter):
            raise ValueError('EXACT_WORKER_EXCLUSIVE_UNSIGNED_HANDLE_AND_SHARED_GATE_REQUIRED')
        self.thread=get_ident()
        self.wall,self.authority,self.protection_due=wall,authority,protection_due
        self.executor,self.limiter=executor,global_limiter
        self.prewire_authority=prewire_authority
        self.current=None
        self.inflight={}
        self._completion_boxes={}
        self._prewire_boxes={}
        self._completed_prewire_refusals={}
        self.closed=False
        self.last_submit_status=None

    def _check(self):
        if get_ident()!=self.thread:raise RuntimeError('ORIGINAL_WORKER_OWNER_QUEUE')

    def _live(self,j,n):
        return self.current==(j.generation,j.end) and j.end<=n<j.expires<=j.end+60_000 and self.authority(j,n)

    def fence(self,g,e):
        self._check()
        now=self.wall()
        if self.current is not None and (g<self.current[0] or e<self.current[1]):raise ValueError('OLDER_SCOPE')
        if self.current!=(g,e):self._completed_prewire_refusals.clear()
        self.current=g,e
        for key,(job,future,token) in tuple(self.inflight.items()):
            # Transport admitted old debits are never released or applied to a new generation.
            if not self._live(job,now) and future.cancel():
                self.limiter.release_if_never_sent(token)
                self._drop(key)

    def rollover(self,g,e):
        self.fence(g,e)
        if any(j.end!=e for j,_,_ in self.inflight.values()):return False
        self.limiter.set_scope(g,e)
        return True

    def reconfirm_slots(self,scope):
        self._check()
        if (self.closed or self.protection_due() or self.current!=(scope.generation,scope.end)
                or not scope.end<=self.wall()<scope.expires):
            return 0
        # Preserve the legacy two-wave headroom (a three-ticket wave in a
        # four-ticket envelope), not its old global candidate/queue cap.
        return max(0,4-len(self.inflight))

    def submit(self,j):
        self._check()
        self.last_submit_status='submit_shape'
        if self.closed or type(j) is not Job:return False
        if j.tf not in ('1m','5m','15m') or type(j.weight) is not int or j.weight not in (1,2):return False
        self.last_submit_status='submit_capacity'
        # Scope/phase and the installed shared native meter admit each
        # reservation. The already-attested executor remains exclusive.
        if j.ticket in self.inflight:return False
        self.last_submit_status='submit_phase'
        if self.protection_due():return False
        self.last_submit_status='submit_lease'
        if not self._live(j,self.wall()):return False
        token=self.limiter.try_reserve(j)
        if token is None:
            self.last_submit_status=getattr(self.limiter,'last_reserve_status',None) or 'submit_rate'
            return False
        refused=[None]
        def claim_before_wire():
            # All callbacks here are transport-thread-safe publications in an
            # approved factory. No native worker DB access from this thread.
            now=self.wall()
            reason=None
            if self.closed:reason='PREWIRE_QUEUE_CLOSED'
            elif self.current!=(j.generation,j.end):reason='PREWIRE_SCOPE_REPLACED'
            elif not j.end<=now<j.expires:reason='PREWIRE_SCOPE_EXPIRED'
            elif self.protection_due():reason='PREWIRE_PROTECTION_HELD'
            elif not self.prewire_authority(j,now):reason='PREWIRE_RANK_AUTHORITY_LOST'
            if reason is not None:
                if refused[0] is None:refused[0]=(now,reason)
                self.limiter.release_if_never_sent(token)
                return False
            admitted=self.limiter.claim_send(token,j)
            if admitted is False and refused[0] is None:
                refused[0]=(now,'PREWIRE_SHARED_METER_REFUSED')
            return admitted
        # Executor invokes claim in the exact signed-to-plan before-send hook.
        try:future=self.executor.submit_original_public(j,token,claim_before_wire)
        except Exception:
            self.last_submit_status='submit_executor_failure'
            self.limiter.release_if_never_sent(token)
            return False
        self.inflight[j.ticket]=(j,future,token)
        self._prewire_boxes[j.ticket]=refused
        self._watch_completion(j.ticket,future)
        self.last_submit_status=None
        return True

    def _watch_completion(self,key,future):
        # Callback publishes in its own bounded single-slot box. Early
        # owner processing never takes the Future's internal condition.
        box=[None]
        self._completion_boxes[key]=box
        def done(f):
            try:box[0]=f.result()
            except Exception:pass
        future.add_done_callback(done)

    def _drop(self,key,*,observe_refusal=False):
        del self.inflight[key]
        self._completion_boxes.pop(key,None)
        box=self._prewire_boxes.pop(key,None)
        if observe_refusal and box is not None and box[0] is not None:
            self._completed_prewire_refusals[key]=box[0]

    def take_prewire_refusal(self,ticket):
        # Observer-only transfer of constant enum and measured wall time.
        self._check()
        return self._completed_prewire_refusals.pop(ticket,None)

    def revoke_unissued(self,tickets):
        self._check()
        for key in tickets:
            item=self.inflight.get(key)
            if item is None:continue
            _,future,token=item
            # Even a running Future will fail its real before-send claim
            # if not already sent. Actual physical debit is never reversed.
            self.limiter.release_if_never_sent(token)
            if future.cancel():self._drop(key)

    def pending_or_proven(self,ticket):
        """Owner only: unresolved physical claim or completed valid receipt.
        Never block on a future and never treat a failed send as coverage."""
        self._check()
        item=self.inflight.get(ticket)
        if item is None:return False
        j,f,t=item
        if self.closed or not self._live(j,self.wall()):return False
        if not f.done():return True
        try:result=f.result()
        except Exception:return False
        return self.limiter.already_sent(t) and self.executor.owned_proof_matches(j,result)

    def poll(self):
        self._check()
        if self.closed or self.protection_due():return ()
        return self._take_already_done(lambda _:True)

    def accept_completed_original_scope(self,scope,accept):
        """Owner asks only after existing current original witness. Never waits or wires."""
        self._check()
        if self.closed or self.current!=(scope.generation,scope.end):return
        for k,(j,f,token) in tuple(self.inflight.items()):
            if ((j.symbol,j.generation,j.end,j.expires)!=(scope.symbol,scope.generation,scope.end,scope.expires)
                    or getattr(j.native_task,'triad_only',False) or (j.tf=='5m' and j.native_task.limit>32)
                    or not scope.end<=self.wall()<scope.expires
                    or not self.limiter.already_sent_nowait(token)):
                continue
            # No Future mutex or per-lane ObserverClient/SQLite read here.
            box=self._completion_boxes.get(k)
            result=box[0] if box is not None else None
            if result is not None and self.executor.owned_proof_matches(j,result) and accept(result[0]) is True:
                self._drop(k)

    def _accept_complete_reconfirm_wave_two(self,scope,waiters,accept):
        # Peek all exact remaining W2 tickets without Future locks or
        # transport. If any envelope cannot be proved now, preserve them all
        # for existing post-protection handling.
        self._check()
        marker=(scope.generation,scope.end)
        if self.closed or self.current!=marker or not 1<=len(waiters)<=3:return
        picked=[]
        for key,task in waiters:
            item=self.inflight.get(key)
            box=self._completion_boxes.get(key)
            if item is None or box is None or box[0] is None:return
            j,_,token=item
            if (j.native_task is not task or getattr(task,'stage',None)!=2
                    or not getattr(task,'triad_only',False)
                    or (j.symbol,j.generation,j.end,j.expires)!=(scope.symbol,scope.generation,scope.end,scope.expires)
                    or not scope.end<=self.wall()<scope.expires
                    or not self.limiter.already_sent_nowait(token)):return
            result=box[0]
            if not self.executor.owned_proof_matches(j,result):return
            picked.append((key,result[0]))
        handled=0
        for key,native in picked:
            if self.current!=marker or not scope.end<=self.wall()<scope.expires:break
            if accept(native) is not True:break
            self._drop(key)
            handled+=1
        return handled

    def _take_already_done(self,choose):
        ready=[]
        for k,(j,f,token) in tuple(self.inflight.items()):
            if not choose(j) or not f.done():continue
            self._drop(k,observe_refusal=not getattr(j.native_task,'triad_only',False))
            try:result=f.result()
            except Exception:
                self.limiter.release_if_never_sent(token)
                continue
            if not self.limiter.already_sent(token):
                self.limiter.release_if_never_sent(token)
                continue
            if self._live(j,self.wall()) and self.executor.owned_proof_matches(j,result):
                ready.append(result)
        return tuple(ready)

    def stop_and_join(self):
        self._check()
        self.closed=True
        for j,f,token in self.inflight.values():
            if f.cancel():self.limiter.release_if_never_sent(token)
        # Existing worker lifecycle owns shutdown; bounded timeout and joins must be attested by the factory.
        self.executor.stop_and_join()
        self.inflight.clear()
        self._completion_boxes.clear()
        self._prewire_boxes.clear()
        self._completed_prewire_refusals.clear()
