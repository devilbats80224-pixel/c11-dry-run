"""Dispatch original reconfirm through the exact incumbent optional queue."""
from dedicated_queue import Job
from triad_lane import Job as TriadJob

class ReconfirmDispatch:
    def __init__(self,actor,queue,plan):
        self.actor,self.queue,self.plan=actor,queue,plan
        self.scope=None
        self.waiting={}
        self.latency_ms=900
        self.blocked_end=None
        self.last_cross_width_event=None
        self.last_reconfirm_events=()
        self.terminal=None

    def _note(self,scope,code):
        if scope is not None:
            # At most two events per receipt and two per dispatch step.
            self.last_reconfirm_events+=((scope,code),)

    def terminal_for(self,scope):
        return self.terminal[1] if self.terminal and self.terminal[0]==scope else None

    def release(self,reason=None):
        if self.scope is not None:
            if reason is not None:
                self.terminal=(self.scope,reason)
                self._note(self.scope,reason)
            self.blocked_end=self.scope.end
            self.queue.revoke_unissued(tuple(self.waiting))
            self.queue.limiter.release_reconfirm()
        self.scope=None
        self.waiting.clear()

    def receive(self,native):
        self.last_reconfirm_events=()
        self.last_cross_width_event=None
        if type(native.task) is not TriadJob:return False
        if self.scope is None or native.request_id not in self.waiting:
            # Old routed counter survives. Unmatched ticket is never admitted or
            # attributed to a currently owned pair/epoch here.
            return True
        return self._receive_waiting(native,self.actor.accept)

    def _receive_waiting(self,native,accept):
        self.waiting.pop(native.request_id)
        lag=native.received_ms-native.requested_ms
        self.latency_ms=max(self.latency_ms,lag)
        accepted=accept(self.scope,native)
        reason=getattr(self.actor,'last_accept_outcome','RC_UNCLASSIFIED')
        if accepted:
            self._note(self.scope,reason)
        else:
            status=getattr(self.actor,'last_cross_width',None)
            if type(status) is str and status!='NORM_XTF_EXACT':
                self.last_cross_width_event=(self.scope,status)
            self.release(reason)
        return True

    def _intake_same_original_wave_two(self,scope,checked_at):
        self.last_reconfirm_events=()
        self.last_cross_width_event=None
        if (self.scope is not scope or self.terminal_for(scope) is not None
                or not self.actor._completed_wave_two_for_current_owner(scope,checked_at)):return
        waiters=tuple(self.waiting.items())
        if not waiters or any(t.stage!=2 for _,t in waiters):return
        def take(native):
            if (self.scope is not scope or self.waiting.get(native.request_id) is not native.task
                    or not self.actor._completed_wave_two_for_current_owner(scope,checked_at)):
                return False
            # No proof is published here. Original availability, commit and
            # callback still check fresh authority and unrenewed lease.
            return self._receive_waiting(native,self.actor._accept_owned_receipt)
        return self.queue._accept_complete_reconfirm_wave_two(scope,waiters,take)

    def step(self,scopes):
        self.last_reconfirm_events=()
        q=self.queue
        if self.scope is not None and not self.actor._ready(self.scope):
            self.release('RC_DISPATCH_AUTHORITY_OR_PHASE_LOST')
        elif self.scope is not None and any(k not in q.inflight for k in self.waiting):
            self.release('RC_DISPATCH_MISSING_INFLIGHT')
        scopes=tuple(dict.fromkeys(scopes))
        if self.scope is None:
            for s in scopes:
                slots=q.reconfirm_slots(s)
                if s.end==self.blocked_end or slots<3:continue
                if self.actor.offer(s,q.limiter.reconfirm_limit(slots,3*self.latency_ms))!='SIX_NO_EXTRA_CALLS_RESERVED':continue
                self.scope=s
                self.terminal=None
                self._note(s,'RC_ESCROW_HELD')
                if not q.limiter.hold_reconfirm(s):
                    self.release('RC_DISPATCH_ESCROW_REFUSED')
                break
        s=self.scope
        if s is None or self.waiting:return
        slots=q.reconfirm_slots(s)
        if slots<3:return
        jobs=self.actor.wave(s,q.limiter.reconfirm_limit(slots,3*self.latency_ms),self.plan.ticket_counter+1)
        self.plan.ticket_counter+=len(jobs)
        for t in jobs:
            rid=f'{self.plan.namespace}.{s.end}.{t.ticket}'
            job=Job(rid,s.symbol,s.generation,s.end,s.expires,t.tf,1,t,rid)
            if not q.submit(job):
                self.release(f'RC_W{t.stage}_SUBMIT_REFUSED')
                return
            self.waiting[rid]=t
        if jobs:self._note(s,f'RC_W{jobs[0].stage}_SUBMITTED')
