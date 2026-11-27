"""Pure shared accounting. Actual incumbent send hooks must bind before activation."""
from dataclasses import dataclass
from threading import Lock
from collections import deque

@dataclass(frozen=True)
class Token:
    marker:tuple
    ticket:str
    weight:int
    data_only:bool=False

class SharedNativeBudget:
    def __init__(self,wall):
        self.wall=wall
        self.lock=Lock()
        self.marker=None
        self.costs=(0,0)
        self.history_costs=(0,0)
        self.meta=None
        self.optional_pause_until=0
        self.optional_banned=False
        self.unknown_incumbent=False
        self.funding_5m=deque()
        self.pending={}
        self.sent={}
        self.escrow=0
        self.escrow_scope=None
        self.boundary_spend=deque(maxlen=8)
        self.last_reserve_status=None
        self.pre_scope_incumbent={}
        self.pre_scope_history={}

    def set_scope(self,generation,end):
        with self.lock:
            if self.marker is not None and (generation<self.marker[0] or end<self.marker[1]):
                raise ValueError('SHARED_BUDGET_ROLLBACK')
            if self.marker is None or end!=self.marker[1]:
                if self.marker is not None:
                    self.boundary_spend.append((self.marker,self.costs))
                n,w,unknown=self.pre_scope_incumbent.pop(end,(0,0,False))
                self.pre_scope_incumbent={e:v for e,v in self.pre_scope_incumbent.items() if e>end}
                self.history_costs=self.pre_scope_history.pop(end,(0,0))
                self.pre_scope_history={e:v for e,v in self.pre_scope_history.items() if e>end}
                self.costs=(n,w);self.sent.clear();self.unknown_incumbent=unknown
            if self.marker!=(generation,end):
                self.pending.clear()
                self.escrow=0
                self.escrow_scope=None
            self.marker=(generation,end)

    def note_authoritative_ip_header(self,actual_at,used,limit):
        with self.lock:
            if not(type(actual_at) is type(used) is type(limit) is int and 0<=used<=limit<=50000 and actual_at<=self.wall()):
                self.meta=None;return
            minute=actual_at//60000
            if self.meta is not None and minute<self.meta[0]:return
            previous=self.meta[1] if self.meta and self.meta[0]==minute else 0
            self.meta=(minute,max(previous,used),limit)

    def _charge_incumbent_at(self,at,calls,weight,unknown):
        """Attribute the actual incumbent before-wire send to its own wall E."""
        end=at//900_000*900_000
        if self.marker is None or end>self.marker[1]:
            # Cache only the current/upcoming calendar boundary and its
            # predecessor until the original owner offers the same E.
            self.pre_scope_incumbent={e:v for e,v in self.pre_scope_incumbent.items()
                                       if e>=end-900_000}
            if len(self.pre_scope_incumbent)>=2 and end not in self.pre_scope_incumbent:
                oldest=min(self.pre_scope_incumbent)
                if end<oldest:
                    # Clock moved backward beyond the tracked future bucket.
                    # Preserve a conservative unknown marker for the newest E.
                    newest=max(self.pre_scope_incumbent)
                    n,w,_=self.pre_scope_incumbent[newest]
                    self.pre_scope_incumbent[newest]=(n+calls,w+weight,True)
                    return
                del self.pre_scope_incumbent[oldest]
            n,w,old=self.pre_scope_incumbent.get(end,(0,0,False))
            self.pre_scope_incumbent[end]=(n+calls,w+weight,old or unknown)
            return
        n,w=self.costs
        self.costs=(n+calls,w+weight)
        self.unknown_incumbent=self.unknown_incumbent or unknown or end<self.marker[1]

    def account_incumbent_before_wire(self,weight,*,funding_requests=0):
        # Never disallow existing protection here; upcoming optional claims
        # account these original exposed costs and may subsequently hold.
        with self.lock:
            at=self.wall()
            if funding_requests:
                while self.funding_5m and self.funding_5m[0]<=at-300_000:self.funding_5m.popleft()
                self.funding_5m.append(at)
            if type(weight) is not int or weight<0 or (weight==0 and funding_requests!=1):
                self.meta=None
                self._charge_incumbent_at(at,1,0,True)
                return
            self._charge_incumbent_at(at,1,weight,False)

    def mark_unaccounted(self):
        with self.lock:
            self.meta=None
            self._charge_incumbent_at(self.wall(),0,0,True)

    def optional_wire_fault(self,status,retry_ms):
        at=self.wall()
        with self.lock:
            self.meta=None
            if status==418:self.optional_banned=True
            elif status==429:self.optional_pause_until=max(self.optional_pause_until,at+max(60_000,retry_ms))

    def _denial(self,weight,count=1,*,cold_end=None,history_detail=False,data_only=False):
        now=self.wall()
        end=now//900_000*900_000
        if cold_end is None:
            n,w=self.costs
            history_n,history_w=self.history_costs
            unknown=self.unknown_incumbent
            ready=self.marker is not None and self.marker[1]==end
        else:
            # Native incumbent protection already charged this exact off-window
            # calendar bucket even if no hot strategy plan was ever observed.
            n,w,unknown=self.pre_scope_incumbent.get(cold_end,(0,0,False))
            history_n,history_w=self.pre_scope_history.get(cold_end,(0,0))
            ready=cold_end==end and (self.marker is None or self.marker[1]<end)
        pending=tuple(self.pending.values())
        extra=sum(t.weight for t in pending)
        direct=tuple(t for t in pending if not t.data_only)
        def rate(reason):
            return reason if history_detail else 'submit_rate'
        if not ready:return rate('HISTORY_BUDGET_WINDOW_NOT_READY')
        if self.optional_banned or now<self.optional_pause_until:return rate('HISTORY_BUDGET_PAUSED')
        if unknown:return 'submit_incumbent_unknown'
        if self.meta is None or self.meta[0]!=now//60000:return 'submit_rate_header_missing'
        # Direct strategy/reconfirmation keeps its independently bounded lane.
        # A typed data-only refill is charged to the same verified IP meter
        # but does not consume direct strategy's per-round bookkeeping cap.
        if not data_only:
            if n-history_n+len(direct)+self.escrow+count>96:return rate('HISTORY_BUDGET_REQUEST_COUNT')
            if w-history_w+sum(t.weight for t in direct)+self.escrow+weight>144:
                return rate('HISTORY_BUDGET_WEIGHT_COUNT')
        if self.meta[1]+w+extra+self.escrow+weight+1200>self.meta[2]:return rate('HISTORY_BUDGET_IP_RESERVE')
        return None
    def _allow(self,weight,count=1):
        return self._denial(weight,count) is None

    def charged_for_diagnostic(self,ticket):
        if not self.lock.acquire(blocking=False):return None
        try:return ticket in self.sent
        finally:self.lock.release()

    def reconfirm_limit(self,slots,latency):
        from triad_escrow import Limit
        with self.lock:
            n,w=self.costs
            at=self.wall()
            ip=(self.meta if self.meta is not None and self.meta[0]==at//60_000
                and self.marker is not None and self.marker[1]==at//900_000*900_000
                and not self.unknown_incumbent and not self.optional_banned
                and at>=self.optional_pause_until else (0,0,0))
            hn,hw=self.history_costs
            direct=tuple(t for t in self.pending.values() if not t.data_only)
            data_reserved=sum(t.weight for t in self.pending.values() if t.data_only)
            return Limit(n-hn,w-hw,len(direct),sum(t.weight for t in direct),
                         ip[1]+hw+data_reserved,ip[2],slots,latency)

    def hold_reconfirm(self,scope):
        with self.lock:
            if self.escrow_scope is not None or self.marker!=(scope.generation,scope.end) or not self._allow(6,6):
                return False
            self.escrow_scope=(scope.symbol,scope.generation,scope.end,scope.expires)
            self.escrow=6
            return True

    def release_reconfirm(self):
        with self.lock:
            # Preserve one-claim boundary state in OriginalReconfirm; this
            # releases only unconverted reservations, never in-flight debit.
            self.escrow=0
            self.escrow_scope=None

    def try_reserve(self,j):
        with self.lock:
            self.last_reserve_status='submit_rate'
            if self.marker!=(j.generation,j.end) or j.ticket in self.sent or j.ticket in self.pending:
                return None
            is_recheck=getattr(j.native_task,'triad_only',False)
            if is_recheck:
                if self.escrow<1 or self.escrow_scope!=(j.symbol,j.generation,j.end,j.expires) or j.weight!=1:
                    return None
                self.escrow-=1
            denial=self._denial(j.weight)
            if denial is not None:
                self.last_reserve_status=denial
                if is_recheck:self.escrow+=1
                return None
            t=Token(self.marker,j.ticket,j.weight);self.pending[t.ticket]=t;return t

    def claim_send(self,t,j):
        with self.lock:
            if t.data_only or self.pending.get(t.ticket) is not t:return False
            del self.pending[t.ticket]
            if self.marker!=t.marker or t.marker!=(j.generation,j.end) or t.ticket!=j.ticket or t.weight!=j.weight or not self._allow(t.weight):
                return False
            n,w=self.costs;self.costs=(n+1,w+t.weight)
            self.sent[t.ticket]=t
            return True

    def _history_admission(self,j,cold_current):
        if self.marker is not None and self.marker[1]==j.end:
            return True,None
        if self.marker is not None and self.marker[1]>=j.end:
            return False,None
        if cold_current is None:return False,None
        from opg_settled_recovery import RecoveryPage
        from dual_history_pages import Page
        from admission_minute_job import MinuteHistoryJob
        from primary_only_route import PrimaryMinuteJob
        # Off-window data only, bound to the existing protected history owner.
        # A current ordinary Page needs no entry marker, just like OPG recovery;
        # both still use the same real, current-minute IP quota and accounting.
        if (type(j) not in (RecoveryPage,Page,MinuteHistoryJob,PrimaryMinuteJob) or not j.valid(self.wall()) or
                self.wall()//900_000*900_000!=j.end or cold_current(j) is not True):
            return False,None
        return True,j.end

    @staticmethod
    def _data_only_history(job):
        from admission_minute_job import MinuteHistoryJob
        from primary_only_route import PrimaryMinuteJob
        # Never derive this privilege from caller flags, ticket data, or
        # a fabricated object. Reviewed recovery jobs retain their old cap.
        return type(job) in (MinuteHistoryJob,PrimaryMinuteJob)

    @staticmethod
    def _history_reason(denial):
        return {'submit_rate_header_missing':'HISTORY_SHARED_METER_STALE',
                'submit_incumbent_unknown':'HISTORY_SHARED_METER_UNKNOWN'}.get(denial,denial)

    def try_reserve_history(self,j,*,cold_current=None):
        with self.lock:
            accepted,end=self._history_admission(j,cold_current)
            self.last_history_hold='HISTORY_MARKER_OR_CURRENT_REFUSED'
            if not accepted or j.ticket in self.sent or j.ticket in self.pending:
                return None
            data_only=self._data_only_history(j)
            denial=self._denial(j.weight,cold_end=end,history_detail=True,data_only=data_only)
            if denial:
                self.last_history_hold=self._history_reason(denial)
                return None
            self.last_history_hold=None
            t=Token((j.generation,j.end),j.ticket,j.weight,data_only)
            self.pending[t.ticket]=t
            return t

    def claim_history_send(self,t,j,*,cold_current=None):
        with self.lock:
            self.last_history_hold='HISTORY_MARKER_OR_CURRENT_REFUSED'
            if self.pending.get(t.ticket) is not t:return False
            del self.pending[t.ticket]
            accepted,end=self._history_admission(j,cold_current)
            if (not accepted or t.marker!=(j.generation,j.end) or t.ticket!=j.ticket or t.weight!=j.weight
                    or t.data_only!=self._data_only_history(j)):
                return False
            denial=self._denial(j.weight,cold_end=end,history_detail=True,data_only=t.data_only)
            if denial is not None:
                self.last_history_hold=self._history_reason(denial)
                return False
            self.last_history_hold=None
            if end is None:
                n,w=self.costs;self.costs=(n+1,w+t.weight)
                if t.data_only:
                    hn,hw=self.history_costs;self.history_costs=(hn+1,hw+t.weight)
            else:
                n,w,unknown=self.pre_scope_incumbent.get(end,(0,0,False))
                self.pre_scope_incumbent[end]=(n+1,w+t.weight,unknown)
                if t.data_only:
                    self.pre_scope_history={e:v for e,v in self.pre_scope_history.items() if e>=end-900_000}
                    hn,hw=self.pre_scope_history.get(end,(0,0))
                    self.pre_scope_history[end]=(hn+1,hw+t.weight)
            self.sent[t.ticket]=t
            return True

    def release_if_never_sent(self,t):
        with self.lock:
            if self.pending.get(t.ticket) is t:del self.pending[t.ticket]

    def already_sent_nowait(self,t):
        if not self.lock.acquire(blocking=False):return False
        try:return self.sent.get(t.ticket) is t
        finally:self.lock.release()

    def already_sent(self,t):
        with self.lock:return self.sent.get(t.ticket) is t
