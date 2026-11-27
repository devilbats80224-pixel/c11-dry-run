"""Scratch native CCXT endpoint integration. No sidecar, C11 start, or orders."""
import concurrent.futures,email.utils,math,os,re,sys,threading,time,uuid
from dataclasses import dataclass
from collections.abc import Mapping
from pathlib import Path
from traceback import extract_tb
import json
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from coverage_queue import SPEC
from reconfirm_queue import Recheck
FREQ_VERSION='2026.10'
CCXT_VERSION='4.5.90'

@dataclass(frozen=True)
class Receipt:
    task:object
    request_id:str
    requested_ms:int
    received_ms:int
    latency_ms:int
    http:int
    ip_weight:int|None
    retry_ms:int|None
    rows:tuple
    failure:str|None=None

def headers_copy(headers):
    return {k.casefold():v for k,v in headers.items() if type(k) is str and len(k)<=100} if isinstance(headers,Mapping) else {}
def weight(headers):
    w=headers.get('x-mbx-used-weight-1m')
    return int(w) if type(w) is str and re.fullmatch('[0-9]{1,9}',w) else None
def retry_ms(headers,now):
    r=headers.get('retry-after')
    if type(r) is not str or len(r)>100:return None
    if re.fullmatch('[0-9]{1,10}',r):return int(r)*1000
    try:return max(0,math.ceil(email.utils.parsedate_to_datetime(r).timestamp()*1000)-now)
    except (ValueError,OverflowError,TypeError,AttributeError):return None
def source_status(error):
    # Prefer explicit status from the actual Binance CCXT exception. Some
    # CCXT versions only retain it in the generated HTTP prefix; never print.
    for k in ('http_status','status_code','status'):
        v=getattr(error,k,None)
        if type(v) is int and 100<=v<=599:return v
    name=type(error).__name__
    if name in ('DDoSProtection','RateLimitExceeded'):
        m=re.search(r'(?<![0-9])(418|429)(?![0-9])',str(error)[:1024])
        if m:return int(m.group())
        # Unknown DDoS may be an IP ban. Do not optimistically assume 429.
        return 418 if name=='DDoSProtection' else 429
    return 599

class PrivateCCXTEndpoint:
    """One owned handle consumed serially. Only its unsigned kline method is used."""
    def __init__(self,api,wall,mono):
        self.api=api;self.wall=wall;self.mono=mono
        self.owner_lock=threading.Lock()
        assert callable(getattr(api,'fapiPublicGetKlines',None))
        assert not getattr(api,'apiKey',None) and not getattr(api,'secret',None)
    def fetch(self,task,request_id):
        if not self.owner_lock.acquire(False):raise RuntimeError('CCXT_HANDLE_CONCURRENT')
        try:
            if getattr(task,'triad_only',False):
                w={'5m':300000,'15m':900000,'1m':60000}[task.tf];n=task.limit
            elif task.tf=='5m':w,n=300000,task.limit
            else:
                w,n,_=SPEC[task.tf]
                if isinstance(task,Recheck):n=task.limit
                elif task.tf=='1m' and getattr(task,'minute_limit',None) is not None:
                    from dynamic_minute_span import valid_minute_span
                    n=task.minute_limit
                    if not valid_minute_span(n):raise ValueError('NATIVE_MINUTE_SPAN_REFUSED')
            now=self.wall(); start=self.mono()
            if not task.end<=task.request<=now<task.deadline:
                return Receipt(task,request_id,now,now,0,0,None,None,(),'MISSED_BEFORE_SEND')
            # Clear only this private client's mutable response slot. Protected
            # by owner lock, so a prior command cannot lend telemetry to an exception.
            self.api.last_response_headers=None
            try:
                rows=self.api.fapiPublicGetKlines(dict(symbol=task.symbol,interval=task.tf,
                       startTime=task.end-n*w,endTime=task.end-1,limit=n))
                code=200
            except Exception as error:
                rows=();code=source_status(error)
            end=self.wall(); latency=(self.mono()-start)//1000000
            h=headers_copy(self.api.last_response_headers)
            if end<now:return Receipt(task,request_id,now,end,latency,0,None,None,(),'WALL_REGRESSED')
            if code==200 and (type(rows) is not list or len(rows)>n):code=0;rows=()
            # To hand off the actual event, use bounded source tuples and exact
            # request generation; no cache or copied historical response generates a new ticket.
            return Receipt(task,request_id,now,end,latency,code,weight(h),retry_ms(h,end),tuple(tuple(r) if isinstance(r,(list,tuple)) else () for r in rows))
        finally:self.owner_lock.release()

def native_receipt_rows(receipt):
    try:return [(int(r[0]),int(r[6]),r[4]) for r in receipt.rows if type(r[4]) is str]
    except (ValueError,IndexError,TypeError):return []

class Shadow:
    """Owner event-lane coordinator. Route decision/auth are out of scope."""
    def __init__(self,queue,clients,wall,mono,limit_source,on_accepted,gate=None,recovery=None,five=None):
        if not 1<=len(clients)<=4 or len(set(map(id,clients)))!=len(clients):
            raise ValueError('PRIVATE_HANDLES_REQUIRED')
        self.q=queue;self.wall=wall;self.limit_source=limit_source;self.on_accepted=on_accepted
        self.pool=concurrent.futures.ThreadPoolExecutor(max_workers=len(clients),thread_name_prefix='nw-shadow')
        self.id=uuid.uuid4().hex
        self.slots=[PrivateCCXTEndpoint(x,wall,mono) for x in clients]
        self.active={};self.dispatched={}
        self.replies=[]
        self.gate=gate
        self.recovery=recovery
        self.five=five
        self.closed=False
    def hold_future(self,plan,slot,reason,fault=None):
        q=self.q
        if q.flight.pop(plan.ticket,None)!=plan:raise ValueError('FAULTED_JOB_NOT_OWNER_DISPATCH')
        if self.five is not None:self.five.transport_holds[plan.symbol,plan.generation,plan.end]=reason
        if plan.generation==q.g:q.fail[(plan.symbol,plan.tf)]=reason
        # Header was wiped before this host slot's dispatch (no reuse). Quota
        # spent on all plans is retained; if missing, global IP becomes unknown
        # until another owned request returns fresh usable telemetry.
        own_weight=weight(headers_copy(self.slots[slot].api.last_response_headers))
        q.exchange_weight(self.wall(),own_weight,self.limit_source())
        frames=[dict(file=Path(b.filename).name,line=b.lineno,func=b.name) for b in extract_tb(fault.__traceback__)[-5:]] if fault else []
        print(json.dumps(dict(status='PAIR_TRANSPORT_FUTURE_HELD',symbol=plan.symbol,generation=plan.generation,
            kind=type(fault).__name__ if fault else 'OWNER_TICKET_MISMATCH',reason=reason,
            stale_generation=plan.generation!=q.g,header_recovered=own_weight is not None,
            held_ip_until_fresh_weight=own_weight is None,frames=frames,production_apply=False)),flush=True)
    def tick(self):
        if self.closed:raise RuntimeError('ALREADY_CLOSED')
        # Process response metadata from all completed flights before new sends.
        for f,idx in list(self.active.items()):
            if not f.done():continue
            plan=self.dispatched.pop(f);del self.active[f]
            try:r=f.result()
            except AttributeError as fault:
                self.hold_future(plan,idx,'PAIR_TRANSPORT_JOB_INTERFACE_ERROR',fault)
                continue
            if (not isinstance(r,Receipt) or r.task!=plan or
                    r.request_id!=f'{self.id}.{plan.end}.{plan.ticket}'):
                self.hold_future(plan,idx,'PAIR_TRANSPORT_RECEIPT_IDENTITY_MISMATCH')
                continue
            self.replies.append(r)
            t=r.task; k=t.symbol,t.tf
            limit=self.limit_source()
            if t.generation!=self.q.g:
                if self.q.flight.pop(t.ticket,None)!=t:raise ValueError('STALE_JOB_NOT_OWNER_DISPATCH')
                self.q.exchange_weight(r.received_ms,r.ip_weight,limit)
                if r.http==418:self.q.ban=True;self.q.metrics['http_418']+=1
                elif r.http==429:self.q.backoff=max(self.q.backoff,r.received_ms+max(60000,r.retry_ms or 0));self.q.metrics['http_429']+=1
                continue
            if t.tf=='5m' or getattr(t,'triad_only',False):
                if self.five is None or self.q.flight.pop(t.ticket,None)!=t:raise ValueError('UNKNOWN_FIVE_MINUTE_TICKET')
                self.q.exchange_weight(r.received_ms,r.ip_weight,limit)
                if r.http==418:
                    self.q.ban=True;self.q.metrics['http_418']+=1;self.q.fail[k]='GLOBAL_418_HOLD_OPERATOR'
                elif r.http==429:
                    self.q.backoff=max(self.q.backoff,r.received_ms+max(60000,r.retry_ms if type(r.retry_ms) is int and r.retry_ms>=0 else 60000))
                    self.q.metrics['http_429']+=1;self.q.fail[k]='GLOBAL_429_BACKOFF'
                if getattr(t,'triad_only',False):self.five.triad.receive(r)
                else:self.five.receive(r)
                continue
            if self.recovery is not None and isinstance(t,Recheck):
                now=self.wall()
                if self.gate is not None and not self.gate(now):
                    # Drain the one reply and telemetry; the gate prevents promotion.
                    now_for_commit=max(now,t.deadline)
                else:now_for_commit=now
                self.recovery.receive(r,limit,now_for_commit)
                continue
            if type(r.ip_weight) is not int:
                # Missing downstream telemetry never reuses an older response's slot.
                self.q.exchange_weight(r.received_ms,None,limit)
                self.q.receive(t,r.received_ms,native_receipt_rows(r),r.http,retry_ms=r.retry_ms)
            else:self.q.receive(t,r.received_ms,native_receipt_rows(r),r.http,r.ip_weight,limit,r.retry_ms)
            now=self.wall()
            if self.recovery is not None:self.recovery.initial_receipt(r)
            if k in self.q.finished and self.q.deadline_for(t.symbol,now)>now:
                self.on_accepted(r)
        occupied=set(self.active.values()); pool=[i for i in range(len(self.slots)) if i not in occupied]
        # Preserve existing strategy/rate limits, and honor authoritative loss
        # of eligibility/disabled route signaled by the owner lane.
        now=self.wall()
        if self.gate is not None and not self.gate(now):return
        if self.q.max_flight>len(self.slots):raise ValueError('QUEUE_EXCEEDS_PRIVATE_HANDLES')
        if self.five is not None:self.five.triad.sweep(now)
        new=self.q.dispatch(now)
        # Fund all eligible fresh baseline routes before discretionary reruns.
        pending_baseline=any(
            self.q.attempt.get((sym,'15m'),0)==0 or self.q.attempt.get((sym,'1m'),0)==0 or
            (self.five is not None and sym in self.five.seed and
             (sym,self.q.g,0) not in self.five.sent and
             (self.recovery._pair(self.recovery.raw,sym) is not None or
              any(v.symbol==sym and v.generation==self.q.g and v.tf in ('1m','15m')
                  for v in self.q.flight.values())))
            for sym in self.q.members if self.q.deadline_for(sym,now)>now+2000)
        if self.five is not None:new+=self.five.dispatch(now,base_only=pending_baseline)
        if not pending_baseline:
            if self.five is not None:new+=self.five.triad.dispatch(now)
            if self.recovery is not None:new+=self.recovery.dispatch(now)
        if self.q.metrics['dispatches']>self.q.call_cap or self.q.spend>self.q.quota:
            raise ValueError('SAME_E_SHARED_RESERVE_BREACH')
        if len(new)>len(pool):raise ValueError('TOTAL_SLOTS_SCOPE')
        for t,idx in zip(new,pool,strict=False):
            request_id=f'{self.id}.{t.end}.{t.ticket}'
            self.slots[idx].api.last_response_headers=None
            f=self.pool.submit(self.slots[idx].fetch,t,request_id)
            self.active[f]=idx;self.dispatched[f]=t
    def close(self):
        self.closed=True
        # Real transport must configure its own short timeout. Polling abort
        # never creates detached threads or continues scheduling after close.
        self.pool.shutdown(wait=True,cancel_futures=True)

def native_c11_public_handles(exchange,limit,kwargs):
    """Initializer signature checked separately against the exact built Docker."""
    if not 1<=limit<=4 or set(kwargs)-{'timeout'}:raise ValueError('PUBLIC_SCRATCH_SCOPE')
    # Future activation must validate installed Freqtrade/CCXT versions and
    # this factory seam. This function is not called by the offline suite.
    from importlib.metadata import version
    if version('freqtrade')!=FREQ_VERSION or version('ccxt')!=CCXT_VERSION:raise ValueError('PINNED_NATIVE_STACK')
    ex_config={'name':'binance','key':'','secret':'','password':'','uid':''}
    ccxt_kwargs={'enableRateLimit':True,'timeout':kwargs.get('timeout',5000),'options':{'defaultType':'future'}}
    return [exchange._init_ccxt(ex_config,sync=True,ccxt_kwargs=ccxt_kwargs) for _ in range(limit)]

class OwnerReceiptSink:
    """Contract-only optional hook for a later isolated C11 build, not active."""
    def __init__(self,store,closedbar,wall,current_window_member):
        self.store=store;self.closedbar=closedbar;self.wall=wall
        self.current_window_member=current_window_member;self.result={}
    def __call__(self,r):
        t=r.task; now=self.wall()
        if not t.end<=r.requested_ms<=r.received_ms<=now<t.deadline or not self.current_window_member(t,now):
            self.result[r.request_id]='RECEIPT_BOUNDARY_CHANGED';return
        width, count,_=SPEC[t.tf]
        if r.http!=200 or len(r.rows)!=count:
            self.result[r.request_id]='BAD_NATIVE_RESPONSE';return
        try:
            closed=[]
            for i,row in enumerate(r.rows):
                op=int(row[0]); ce=int(row[6])+1
                if not op==t.end-(count-i)*width or ce!=op+width or ce>r.requested_ms:
                    raise ValueError('CANDLE_WINDOW')
                o,h,l,c,vol,quote=(*(float(x) for x in row[1:6]),float(row[7]))
                if (not all(math.isfinite(x) for x in (o,h,l,c,vol,quote)) or not
                    0<l<=min(o,c)<=max(o,c)<=h or min(vol,quote)<0):
                    raise ValueError('CANDLE_VALUES')
                closed.append(self.closedbar(op,r.received_ms,o,h,l,c,vol,quote))
            # Only the owner SQLite writer may call the existing API. A
            # quarantine is not a failed transport and must never retry here.
            self.store.record(t.symbol,width,closed,quarantine_nw=True,public_request_id=r.request_id)
        except (ValueError,TypeError,IndexError,KeyError):
            self.result[r.request_id]='BAD_CLOSED_BATCH';return
        except Exception as exc:
            self.result[r.request_id]='QUARANTINE_OR_STORE_REFUSED'
            return
        self.result[r.request_id]='RECORDED_FOR_ASOF_REVALIDATION'
