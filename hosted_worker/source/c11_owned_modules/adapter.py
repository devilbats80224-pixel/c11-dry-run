"""Build-only bridge from owned native public 1m/5m/15m receipts to original owner Ready."""
from dataclasses import dataclass
from threading import Lock,get_ident
from coordinator import Scope,Ticket,Receipt as OwnerReceipt,WIDTH
from causal_bars import ClosedBar
from capture import Capture,query_for,sanitize,digest,TICKET
from native_shadow_queue import PrivateCCXTEndpoint,Receipt as NativeReceipt
from coverage_queue import Task as BaselineTask
from trusted_5m import TailRequest

@dataclass(frozen=True)
class Dispatch:
    task:object
    owner_ticket:Ticket

def in_phase(t):
    return ((type(t) is BaselineTask and t.tf in ('15m','1m') and getattr(t,'stage',None) is None
             and getattr(t,'reason',None) is None)
            or (type(t) is TailRequest and t.tf=='5m' and getattr(t,'stage',None)==0
                and getattr(t,'reason',None)=='PINNED_5M_CHECKPOINT_TAIL')) and not getattr(t,'triad_only',False)

def pins(record):
    return record['row_sha256']==digest({k:v for k,v in record.items() if k!='row_sha256'})

class StrictInitialReadyAdapter:
    def __init__(self,ready,wall,capture):
        if type(capture) is not Capture:raise TypeError("ORIGINAL_NATIVE_CAPTURE_REQUIRED")
        self.capture=capture
        self.ready=ready
        self.wall=wall
        self.owner=get_ident()
        self.gate=Lock()
        self.marker=None
        self.last_normalization_refusal=None
        self.last_cross_width_observation=None
        self._planned={}
        self._proven={}
        self._installed=set()

    def install_endpoint(self,endpoint):
        """Wrap only the original configured public handle; never perform the request here."""
        if get_ident()!=self.owner:raise RuntimeError('ORIGINAL_OWNER_THREAD_REQUIRED')
        if type(endpoint) is not PrivateCCXTEndpoint or endpoint in self._installed:
            raise TypeError('FRESH_NATIVE_PUBLIC_ENDPOINT_REQUIRED')
        self.capture.install(endpoint)
        safe_fetch=endpoint.fetch
        def emit(task,request_id):
            receipt=safe_fetch(task,request_id)
            self.accept_native(receipt)
            return receipt
        endpoint.fetch=emit
        self._installed.add(endpoint)

    def rebind_capture(self,capture,epoch_authority):
        if get_ident()!=self.owner:raise RuntimeError('ORIGINAL_OWNER_THREAD_REQUIRED')
        if (type(capture) is not Capture or capture.end<=self.capture.end or
                epoch_authority(capture.end) is not True):return False
        if not self.gate.acquire(blocking=False):return False
        try:
            self.capture=capture
            self._planned.clear()
            self._proven.clear()
            return True
        finally:self.gate.release()

    def reset(self,generation,end):
        if get_ident()!=self.owner:raise RuntimeError('ORIGINAL_OWNER_THREAD_REQUIRED')
        if end!=self.capture.end:return False
        marker=(generation,end)
        if self.marker is not None and (generation<self.marker[0] or end<self.marker[1]):raise ValueError('OLDER_GENERATION_OR_E_REFUSED')
        if marker!=self.marker:
            if not self.gate.acquire(blocking=False):return False
            try:
                self.ready.reset(generation,end)
                self._planned.clear()
                self._proven.clear()
                self.marker=marker
            finally:self.gate.release()
        return True

    def register_original_dispatch(self,task,scope,request_id):
        """Owner calls before creation of an original transport Future; no receipt-initiated registration."""
        if get_ident()!=self.owner:raise RuntimeError('ORIGINAL_OWNER_THREAD_REQUIRED')
        try:
            if type(scope) is not Scope or type(request_id) is not str or not TICKET.fullmatch(request_id):return False
            if request_id.split('.')[1:]!=[str(task.end),str(task.ticket)] or not in_phase(task):return False
            if scope.pair!=scope.symbol[:-4]+'/USDT:USDT' or not scope.symbol.endswith('USDT'):return False
            if (scope.symbol,scope.generation,scope.end,scope.expires)!=(task.symbol,task.generation,task.end,task.deadline):return False
            params=query_for(task); n=int(params['limit'])
            if not self.gate.acquire(blocking=False):return False
            try:
                if self.marker!=(scope.generation,scope.end) or request_id in self._planned:return False
                ticket=Ticket(request_id,scope,task.tf,task.request,n)
                if not self.ready.register(ticket):return False
                self._planned[request_id]=Dispatch(task,ticket)
                return True
            finally:self.gate.release()
        except (AttributeError,KeyError,ValueError,TypeError):return False

    def forget_unsent(self,request_id):
        if get_ident()!=self.owner:raise RuntimeError('ORIGINAL_OWNER_THREAD_REQUIRED')
        if not self.gate.acquire(blocking=False):return False
        try:
            d=self._planned.get(request_id)
            if d is None or request_id in self._proven or not self.ready.forget_unsent(d.owner_ticket):
                return False
            del self._planned[request_id]
            return True
        finally:self.gate.release()

    def accept_native(self,native,*,nowait=False):
        """Pass only exact proven raw signed-to-plan response; never a shadow verdict/sidecar result."""
        try:
            if type(native) is not NativeReceipt:return False
            if not self.gate.acquire(blocking=not nowait):return False
            try:
                d=self._planned.get(native.request_id)
                record=self.capture.records.get(native.request_id)
            finally:self.gate.release()
            # Refuse optional pre-protection processing if either mailbox or
            # capture gate is currently held. Normal after-protection owns it.
            if nowait:
                if not self.ready.lock.acquire(blocking=False):return False
                self.ready.lock.release()
            if record is None or d is None or native.task is not d.task:return False
            t=d.owner_ticket
            if not in_phase(native.task) or native.http!=200 or native.failure is not None:return False
            if not (pins(record) and record['capture_grade']=='VALIDATED_ROWS' and record['reported_http']==200):return False
            if (native.request_id,native.task.ticket,t.scope.symbol,t.scope.generation,t.scope.end,t.lane,
                 t.scope.expires,t.request_ms,t.count,native.requested_ms,native.received_ms,
                 getattr(native.task,'stage',None),getattr(native.task,'reason',None))!=tuple(
                record[k] for k in ('request_id','ticket','symbol','generation','end_ms','timeframe',
                  'deadline_exclusive_ms','queued_at_ms','limit','request_ms','receive_ms','stage','reason')):return False
            src=record['source']
            if (src['method'],src['scheme'],src['host'],src['path'],src['initial_request_uses_tls_verification'],
                src['status'],src['request_query'])!=('GET','https','fapi.binance.com','/fapi/v1/klines',True,200,query_for(d.task)):return False
            if src['initial_request_uses_tls_verification'] is not True:return False
            if not(t.scope.end<=t.request_ms<=native.requested_ms<=src['request_time_ms']<=src['response_time_ms']
                   <=native.received_ms<=self.wall()<t.scope.expires):return False
            raw=tuple(map(tuple,sanitize(record['raw_kline_rows'],t.lane,t.scope.end,t.count,native.requested_ms)))
            if native.rows!=raw:return False
            r=OwnerReceipt(t,native.received_ms,raw,record['row_sha256'])
            if not self.gate.acquire(blocking=not nowait):return False
            try:
                if self.marker!=(t.scope.generation,t.scope.end) or self._planned.get(t.request_id) is not d:
                    return False
                if self.capture.records.get(t.request_id) is not record or self.wall()>=t.scope.expires:
                    return False
                if not self.ready.complete(r,nowait=nowait):return False
                self._proven[t.request_id]=r
                return True
            finally:self.gate.release()
        except (AttributeError,KeyError,ValueError,TypeError,IndexError):return False

    def verified_committed_receipts(self,scope):
        """Immutable rows and exact wire clocks only, indexed by original accepted plan."""
        if get_ident()!=self.owner or not self.gate.acquire(blocking=False):
            return ()
        try:
            if self.marker!=(scope.generation,scope.end):return ()
            out=[]
            for k,receipt in self._proven.items():
                if receipt.ticket.scope!=scope:continue
                record=self.capture.records.get(k)
                dispatch=self._planned.get(k)
                if (record is None or dispatch is None or not pins(record)
                        or receipt.source_proof!=record['row_sha256'] or record['capture_grade']!='VALIDATED_ROWS'
                        or receipt.ticket is not dispatch.owner_ticket
                        or record['receive_ms']!=receipt.received_ms
                        or tuple(map(tuple,record['raw_kline_rows']))!=receipt.raw):
                    return ()
                out.append((receipt,record['request_ms']))
            return tuple(out) if {r.ticket.lane for r,_ in out}=={'1m','5m','15m'} and len(out)==3 else ()
        finally:self.gate.release()

    def normalize_native_minute(self,scope,minute,*,prefix_five,prefix_quarter,finality):
        """Build terminal 5m/15m only from the admitted original CCXT 1m response.

        The caller checks the exact receipt's closed-time boundary.
        Prefixes must be already-closed native data. This read-only
        boundary does not assign any wire lane or promise exchange immutability.
        """
        from native_minute_projection import project
        if get_ident()!=self.owner or not self.gate.acquire(blocking=False):
            return None
        try:
            now=self.wall()
            if (self.marker!=(scope.generation,scope.end) or minute.ticket.scope!=scope
                or minute.ticket.lane!='1m' or self._proven.get(minute.ticket.request_id) is not minute
                or not scope.end<=minute.received_ms<=now<scope.expires):
                return None
            record=self.capture.records.get(minute.ticket.request_id)
            if (record is None or not pins(record) or record['capture_grade']!='VALIDATED_ROWS'
                or record['row_sha256']!=minute.source_proof
                or tuple(map(tuple,record['raw_kline_rows']))!=minute.raw):
                return None
            if finality(scope,minute,record) is not True:
                return None
            return project(scope.end,minute.raw,minute.received_ms,now,prefix_five,prefix_quarter)
        finally:self.gate.release()

    def normalize(self,scope,jobs):
        """Owner use only; no queue waits, disk, mutable material, or external state."""
        if get_ident()!=self.owner:raise RuntimeError('ORIGINAL_OWNER_THREAD_REQUIRED')
        self.last_normalization_refusal=None
        if len(jobs)!=3:
            self.last_normalization_refusal='NORM_LANE_COUNT'
            return None
        if not self.gate.acquire(blocking=False):
            self.last_normalization_refusal='NORM_LOCK_BUSY'
            return None
        try:
            if self.marker!=(scope.generation,scope.end):
                self.last_normalization_refusal='NORM_GENERATION_OR_BOUNDARY'
                return None
            if any(self._proven.get(r.ticket.request_id) is not r or r.ticket.scope!=scope for r in jobs):
                self.last_normalization_refusal='NORM_RECEIPT_IDENTITY_OR_SCOPE'
                return None
            if tuple(r.ticket.lane for r in jobs)!=('1m','5m','15m'):
                self.last_normalization_refusal='NORM_LANE_ORDER'
                return None
        finally:self.gate.release()
        # Wider terminal candles are not authoritative for this projection.
        # Keep comparisons for diagnosis, never as an admission condition.
        from cross_width_verify import verify_terminal
        self.last_cross_width_observation=verify_terminal(scope.end,*(r.raw for r in jobs))
        anchor=scope.end-WIDTH['15m']
        def earlier(receipt):
            return tuple(ClosedBar(r[0],receipt.received_ms,*(float(r[i]) for i in (1,2,3,4,5,7)))
                         for r in receipt.raw if r[0]<anchor)
        def exact_existing_finality(active,minute,record):
            request=record.get('request_ms')
            return (active==scope and minute is jobs[0] and record['row_sha256']==minute.source_proof
                    and type(request) is int and active.end<=request<=minute.received_ms
                    and bool(minute.raw) and minute.raw[-1][6]==active.end-1
                    and minute.raw[-1][6]<request)
        projected=self.normalize_native_minute(scope,jobs[0],prefix_five=earlier(jobs[1]),
                        prefix_quarter=earlier(jobs[2]),finality=exact_existing_finality)
        if projected is None:
            self.last_normalization_refusal='NORM_NATIVE_MINUTE_PROJECTION_NOT_READY'
        return projected
