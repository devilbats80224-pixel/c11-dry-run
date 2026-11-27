"""Build-only same-E original-owner two-wave initial-five-only confirmation."""
from dataclasses import dataclass
from decimal import Decimal
from threading import get_ident
from capture import query_for
from capture import sanitize,digest
from causal_bars import ClosedBar
from coordinator import Receipt,Ticket,WIDTH
from native_shadow_queue import Receipt as NativeReceipt
from triad_escrow import Identity,Arrival,Triad,TF
from triad_lane import Job

def parse_record(capture,native,planned,scope,now):
    if type(native) is not NativeReceipt or native.task is not planned or native.http!=200 or native.failure is not None:return None
    rec=capture.records.get(native.request_id)
    if rec is None:return None
    if rec['row_sha256']!=digest({k:v for k,v in rec.items() if k!='row_sha256'}):return None
    if (rec['symbol'],rec['generation'],rec['end_ms'],rec['deadline_exclusive_ms'])!=(scope.symbol,scope.generation,scope.end,scope.expires):return None
    if (rec['request_id'],rec['ticket'],rec['timeframe'],rec['stage'],rec['reason'],rec['capture_grade'],rec['reported_http'])!=(
        native.request_id,planned.ticket,planned.tf,planned.stage,planned.reason,'VALIDATED_ROWS',200):return None
    if native.request_id.split('.')[1:]!=[str(scope.end),str(planned.ticket)]:return None
    src=rec['source']
    if (src['method'],src['scheme'],src['host'],src['path'],src['status'],src['request_query'])!=(
        'GET','https','fapi.binance.com','/fapi/v1/klines',200,query_for(planned)):return None
    if src['initial_request_uses_tls_verification'] is not True:return None
    if (rec['queued_at_ms'],rec['request_ms'],rec['receive_ms'],rec['limit'])!=(planned.request,native.requested_ms,native.received_ms,planned.limit):return None
    if not scope.end<=planned.request<=native.requested_ms<=src['request_time_ms']<=src['response_time_ms']<=native.received_ms<=now<scope.expires:return None
    raw=tuple(map(tuple,sanitize(rec['raw_kline_rows'],planned.tf,scope.end,planned.limit,native.requested_ms)))
    if raw!=native.rows:return None
    return Receipt(Ticket(native.request_id,scope,planned.tf,planned.request,planned.limit),native.received_ms,raw,rec['row_sha256'])

def arrival(i,r,at):
    return Arrival(i,r.ticket.lane,r.ticket.request_id,at,r.received_ms,Decimal(r.raw[-1][4]),True,r.raw[-1])

def rows(r):
    return tuple(ClosedBar(x[0],r.received_ms,*(float(x[i]) for i in (1,2,3,4,5,7))) for x in r.raw)

@dataclass(frozen=True)
class Final:
    scope:object
    initial:tuple
    first:tuple
    second:tuple
    batches:tuple
    proof_sha256:str

class OriginalReconfirm:
    def __init__(self,adapter,wall,authority,protection_due):
        self.adapter=adapter
        self.wall,self.authority,self.protection_due=wall,authority,protection_due
        self.owner=get_ident()
        self._slot=None
        self._plans={}
        self._jobs={}
        self._complete=None
        self._used=False
        self._committed=False

    def _on_owner(self):
        if get_ident()!=self.owner:raise RuntimeError('ORIGINAL_RECONFIRM_OWNER_ONLY')

    def _deadline(self,s,now):
        f=getattr(self.adapter.ready,'effective_expiry',None)
        return min(s.expires,f(s,now)) if callable(f) else s.expires

    def _ready(self,s):
        at=self.wall()
        return self.adapter.marker==(s.generation,s.end) and s.end<=at<self._deadline(s,at)<=s.end+60_000 and self.authority(s,at) and not self.protection_due()

    @property
    def reserved(self):
        if self._slot is None:return 0
        s,p,_=self._slot
        return p.escrow if self.adapter.marker==(s.generation,s.end) and self.wall()<self._deadline(s,self.wall()) else 0

    def offer(self,s,lim):
        self._on_owner()
        if not self._ready(s):return 'HOLD_CURRENT_AUTHORITY'
        # Two-wave tasks encode their original deadline in capture. Do not
        # reserve or emit a clipped reconfirm under a re-tagged identity.
        if self._deadline(s,self.wall())!=s.expires:return 'HOLD_CURRENT_AUTHORITY'
        if self._slot is not None and self._slot[0].end==s.end:return 'HOLD_SINGLE_SHARED_RECONFIRM_ESCROW'
        original=self.adapter.verified_committed_receipts(s)
        if len(original)!=3:return 'HOLD_INITIAL_PROVEN_ROWS_REQUIRED'
        i=Identity(s.symbol,s.generation,s.end,s.expires)
        try:p=Triad(i,{r.ticket.lane:arrival(i,r,at) for r,at in original})
        except ValueError:return 'HOLD_NOT_INITIAL_FIVE_ONLY_DISAGREEMENT'
        if not p.reserve(self.wall(),lim):return 'HOLD_SHARED_BUDGET_OR_ORIGINAL_DEADLINE'
        self._slot=(s,p,original)
        self._plans.clear(); self._jobs.clear(); self._complete=None; self._used=False; self._committed=False
        return 'SIX_NO_EXTRA_CALLS_RESERVED'

    def wave(self,s,lim,next_ticket):
        self._on_owner()
        if self._slot is None or self._slot[0]!=s or not self._ready(s):return ()
        p=self._slot[1]
        now=self.wall()
        if self._deadline(s,now)!=s.expires:
            p.escrow=0
            return ()
        group=p.wave(p.i,now,lim,next_ticket)
        result=tuple(Job(s.symbol,a.tf,a.send_before_ms,now,s.end,a.ticket,s.generation,a.count,a.stage,a) for a in group)
        self._plans.update({j.ticket:j for j in result})
        return result

    def accept(self,s,native):
        self._on_owner()
        self.last_cross_width=None
        self.last_accept_outcome='RC_OWNER_NOT_CURRENT'
        if self._slot is None or self._slot[0]!=s or not self._ready(s):return False
        return self._accept_owned_receipt(s,native)

    def _completed_wave_two_for_current_owner(self,s,checked_at):
        # Used only after UnifiedScheduler's original fresh authority check;
        # no additional ObserverClient query or transport occurs here.
        self._on_owner()
        at=self.wall()
        return (self._slot is not None and self._slot[0] is s
                and self._slot[1].first is not None and self._slot[1].escrow==0
                and self._complete is None and not self._used
                and self.adapter.marker==(s.generation,s.end)
                and s.end<=checked_at<=at<self._deadline(s,at)==s.expires<=s.end+60_000
                and not self.protection_due())

    def _accept_owned_receipt(self,s,native):
        # Pure in-memory assembly: caller must have proven exact original
        # owner as above or via the unmodified post-protection admission.
        self.last_cross_width=None
        p=self._slot[1]
        self.last_accept_outcome='RC_UNMATCHED_RECEIPT'
        plan=self._plans.get(getattr(native.task,'ticket',None)) if type(native) is NativeReceipt else None
        if plan is None or plan.ticket in self._jobs:return False
        stage=plan.stage
        self.last_accept_outcome=f'RC_W{stage}_RAW_RECEIPT_REJECTED'
        try:r=parse_record(self.adapter.capture,native,plan,s,self.wall())
        except (ValueError,TypeError,IndexError,KeyError,AttributeError):return False
        if r is None:return False
        self._jobs[plan.ticket]=(r,native.requested_ms)
        self.last_accept_outcome=f'RC_W{stage}_RECEIPT_ACCEPTED'
        expected=tuple(j for j in self._plans.values() if j.stage==stage)
        if len(expected)!=3 or not all(j.ticket in self._jobs for j in expected):return True
        receipts=tuple(self._jobs[j.ticket] for j in expected)
        if not p.receive(p.i,tuple(j.owner_plan for j in expected),tuple(arrival(p.i,*x) for x in receipts)):
            self.last_accept_outcome=f'RC_W{stage}_TRIAD_PROOF_REJECTED'
            p.escrow=0
            return False
        if stage==1:
            self.last_accept_outcome='RC_W1_TRIAD_ACCEPTED'
            return True
        initial=self._slot[2]
        first=tuple(self._jobs[j.ticket] for j in self._plans.values() if j.stage==1)
        try:self._complete=self._compose(s,initial,first,receipts)
        except ValueError as err:
            self.last_accept_outcome={
                'OTHER_LANE_REVISION_NOT_APPROVED':'RC_W2_PEER_LINEAGE_REJECTED',
                'ORIGINAL_FIVE_ROW_LINEAGE_CONFLICT':'RC_W2_FIVE_LINEAGE_REJECTED',
                'NATIVE_PROJECTED_ROW':'RC_W2_PROJECTED_ROW_REJECTED',
            }.get(str(err),'RC_W2_XTF_REJECTED' if self.last_cross_width is not None else 'RC_W2_COMPOSE_REJECTED')
            return False
        self.last_accept_outcome='RC_W2_CHAIN_ACCEPTED'
        return True

    def _compose(self,s,ini,first,second):
        a,b,c=({r.ticket.lane:(r,m) for r,m in g} for g in (ini,first,second))
        for tf in ('1m','15m'):
            if not a[tf][0].raw[-1:]==b[tf][0].raw==c[tf][0].raw:
                raise ValueError('OTHER_LANE_REVISION_NOT_APPROVED')
        a5,b5,c5=(x['5m'][0] for x in (a,b,c))
        if len(a5.raw)<3 or len(b5.raw)!=3 or b5.raw!=c5.raw or a5.raw[-3:-1]!=c5.raw[:2]:
            raise ValueError('ORIGINAL_FIVE_ROW_LINEAGE_CONFLICT')
        from cross_width_verify import OK,verify_terminal
        self.last_cross_width=verify_terminal(s.end,a['1m'][0].raw,c5.raw,a['15m'][0].raw)
        if self.last_cross_width!=OK:
            raise ValueError(self.last_cross_width)
        b5out=rows(a5)[:-3]+rows(c5)
        batches=((WIDTH['1m'],rows(a['1m'][0])),(WIDTH['5m'],b5out),(WIDTH['15m'],rows(a['15m'][0])))
        if any(not bar.valid(width) for width,rs in batches for bar in rs):raise ValueError('NATIVE_PROJECTED_ROW')
        return Final(s,ini,first,second,batches,digest([[r.source_proof,actual] for r,actual in (*ini,*first,*second)]))

    def _chain_still_valid(self,event):
        # The current bound capture must still contain all nine immutable
        # receipts, including unchanged peers and both independent rounds.
        for r,request in (*event.initial,*event.first,*event.second):
            rec=self.adapter.capture.records.get(r.ticket.request_id)
            if rec is None or rec.get('row_sha256')!=r.source_proof:return False
            if digest({k:v for k,v in rec.items() if k!='row_sha256'})!=r.source_proof:return False
            if (rec.get('request_ms'),rec.get('receive_ms'),rec.get('capture_grade'))!=(request,r.received_ms,'VALIDATED_ROWS'):return False
            if tuple(map(tuple,rec.get('raw_kline_rows',())))!=r.raw:return False
        return True

    def owns_original_scope(self,scope):
        self._on_owner()
        return self._slot is not None and self._slot[0]==scope

    def available_for_owner_revisit(self,scope):
        self._on_owner()
        e=self._complete
        return (e is not None and e.scope==scope and not self._used and
                self._ready(scope) and self._chain_still_valid(e))

    def scoped_revision_preview(self,scope):
        # Existing compiler has already certified both waves. Expose the
        # identical canonical candidate batch, not fresh authority.
        if not self.available_for_owner_revisit(scope):return None
        return self._complete

    def scoped_revision_atomic_chain(self,scope,batches):
        self._on_owner()
        e=self._complete
        if (e is None or e.scope!=scope or e.batches is not batches or not self._used or self._committed
                 or not self._ready(scope) or not self._chain_still_valid(e)):
            return None
        return e

    def consume_original(self,scope,atomic_commit,remaining):
        self._on_owner()
        event=self._complete
        if event is None or event.scope!=scope or self._used:return 'HOLD_NO_UNUSED_RECONFIRM_CHAIN',None
        at=self.wall()
        if not self._ready(scope) or min(remaining(),self._deadline(scope,at)-at)<=30:return 'HOLD_ORIGINAL_LEASE_OR_PROTECTION',None
        if not self._chain_still_valid(event):return 'HOLD_RECONFIRM_SOURCE_CHANGED',None
        self._used=True
        at=self.wall()
        result=atomic_commit(self.adapter.ready.store,scope,event.batches,lambda:self._ready(scope))
        if result is not event.batches or not self._ready(scope) or self.wall()-at>30:return 'HOLD_RECONFIRM_NATIVE_COMMIT',None
        self._committed=True
        return 'ORIGINAL_OWNER_RECONFIRMED_READY',result

    def verified_committed_receipts(self,scope):
        event=self._complete
        if (get_ident()!=self.owner or not self._committed or event is None or event.scope!=scope or
                not self._ready(scope) or not self._chain_still_valid(event)):return ()
        return tuple(x for x in event.initial if x[0].ticket.lane in ('1m','15m'))+tuple(
            x for x in event.second if x[0].ticket.lane=='5m')

    @property
    def ready(self):
        return self.adapter.ready

    def reconfirm_chain(self,scope):
        if not self.verified_committed_receipts(scope):return None
        return self._complete
