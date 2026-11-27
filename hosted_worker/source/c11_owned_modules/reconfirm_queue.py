"""Bounded same-clock short read recovery; no I/O from this module."""
from dataclasses import dataclass
from decimal import Decimal,InvalidOperation
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from coverage_queue import Task,SPEC,REQ
FIRST_MS=10_000
SEPARATE_MS=5_000
MAX_EXTRA_CALLS=24
@dataclass(frozen=True)
class Recheck(Task):
    limit:int
    reason:str
    stage:int

class Recovery:
    def __init__(self,queue):
        self.q=queue;self.tries={};self.raw={};self.first={};self.second={}
        self.states={};self.extra=0;self.events=[];self.per_pair={}
        self.symbol_order=[]
    def _items(self,r,n):
        width,_,_=SPEC[r.task.tf]; result=[]
        try:
            assert r.http==200 and len(r.rows)==n
            for i,row in enumerate(r.rows):
                op=int(row[0]);ce=int(row[6])+1
                assert op==r.task.end-(n-i)*width and ce==op+width and ce<=r.requested_ms
                data=tuple(Decimal(row[j]) for j in (1,2,3,4,5,7))
                o,h,l,c,v,q=data
                assert all(d.is_finite() for d in data) and 0<l<=min(o,c)<=max(o,c)<=h and min(v,q)>=0
                result.append((op,ce,data))
        except (ValueError,TypeError,IndexError,AssertionError,InvalidOperation):return ()
        return tuple(result)
    def _history(self,r,n,kind):
        b=self._items(r,n);t=r.task
        bucket=self.per_pair.setdefault(t.symbol,[])
        bucket.append(dict(tf=t.tf,kind=kind,http=r.http,expected=n,actual=len(r.rows),
                           valid=len(b)==n,request_ms=r.requested_ms,receive_ms=r.received_ms,
                           target_end=t.end,close=str(b[-1][2][3]) if b else None))
        return b
    def initial_receipt(self,r):
        t=r.task; w,n,_=SPEC[t.tf]
        b=self._history(r,n,'initial')
        if b and (t.symbol,t.tf) in self.q.finished:
            self.raw[t.symbol,t.tf]=(r,b[-1])
        elif (t.symbol,t.tf) in self.q.finished:
            self.q.finished.pop((t.symbol,t.tf))
            self.q.fail[t.symbol,t.tf]='RAW_OHLCV_INVALID'
    def _pair(self,table,s):
        a=table.get((s,'15m'));b=table.get((s,'1m'))
        return (a,b) if a and b else None
    @staticmethod
    def matched(pair):
        return pair is not None and pair[0][1][2][3]==pair[1][1][2][3]
    def coverage(self,now):
        d={}
        for s in self.q.members:
            if self.q.deadline_for(s,now)<=now:d[s]='NOT_CURRENT_OR_EXPIRED';continue
            v=self._pair(self.raw,s)
            if v and max(x[0].received_ms for x in v)>now:
                d[s]='RECEIPT_TIME_REGRESSED';continue
            if self.matched(v):
                d[s]='CANDLE_PAIR_FULL_REFETCH_MATCH' if any(isinstance(x[0].task,Recheck) for x in v) else 'CANDLE_PAIR_INITIAL_MATCH'
                continue
            if v is None:
                d[s]='INITIAL_FRAME_MISSING_OR_LATE';continue
            a=self._pair(self.first,s);b=self._pair(self.second,s)
            if any(x and max(r[0].received_ms for r in x)>now for x in (a,b)):
                d[s]='RECEIPT_TIME_REGRESSED';continue
            if b and not self.matched(b):d[s]='POST_RECHECK_SAME_E_MISMATCH';continue
            if self.matched(a) and self.matched(b):
                if tuple(x[1] for x in a)==tuple(x[1] for x in b) and all(y[0].requested_ms-x[0].received_ms>=SEPARATE_MS for x,y in zip(a,b)):
                    d[s]='CANDLE_PAIR_TWO_RECONFIRMS_MATCH';continue
                d[s]='CORRECTION_UNCONFIRMED';continue
            if a and b:d[s]='CORRECTION_UNCONFIRMED';continue
            d[s]='SAME_END_CLOSE_MISMATCH_PENDING_RECONFIRM'
        return d
    def pending(self,now):
        for s in dict.fromkeys(self.q.cursor):
            if self.q.deadline_for(s,now)<=now:continue
            if any(x.symbol==s for x in self.q.flight.values()):continue
            available=self._pair(self.raw,s)
            if self.matched(available):continue
            if available is None:
                for tf,_,n,_ in REQ:
                    k=s,tf
                    if k in self.raw or k in self.tries:continue
                    reason=self.q.fail.get(k)
                    if reason in {'GLOBAL_429_BACKOFF','GLOBAL_418_HOLD_OPERATOR','RECEIVED_OUT_OF_WINDOW'}:continue
                    if reason and self.q.attempt.get(k)==1:
                        yield s,tf,n,'FULL_MISSING',0,self.q.end+FIRST_MS
                continue
            base_ready=max(p[0].received_ms for p in available)
            first_ready=self._pair(self.first,s)
            for tf,_,_,_ in REQ:
                key=s,tf,1
                if key not in self.tries:
                    yield s,tf,1,'SAME_E_MISMATCH',1,max(self.q.end+FIRST_MS,base_ready+SEPARATE_MS)
            if first_ready:
                first_time=max(p[0].received_ms for p in first_ready)
                for tf,_,_,_ in REQ:
                    key=s,tf,2
                    if key not in self.tries:
                        yield s,tf,1,'SAME_E_MISMATCH',2,first_time+SEPARATE_MS
    def dispatch(self,now):
        q=self.q
        if now>=q.deadline or now<q.backoff or q.ban or q.ip is None or q.minute!=now//60_000:return ()
        out=[]
        for s,tf,n,reason,stage,earliest in self.pending(now):
            if len(q.flight)>=q.max_flight or self.extra>=MAX_EXTRA_CALLS:break
            if any(x.symbol==s for x in q.flight.values()):continue
            dl=q.deadline_for(s,now); w=SPEC[tf][2] if n!=1 else 1
            if not earliest<=now<dl-2000 or q.spend+w>q.quota or q.weight+q.spend+w+q.reserve>q.ip:continue
            k=(s,tf) if stage==0 else (s,tf,stage)
            if k in self.tries:continue
            q.seq+=1;q.spend+=w;q.metrics['dispatches']+=1;self.extra+=1
            t=Recheck(s,tf,dl,now,q.end,q.seq,q.g,n,reason,stage)
            q.flight[t.ticket]=t; self.tries[k]=t;out.append(t)
        return tuple(out)
    def receive(self,r,limit,now_for_commit):
        q=self.q;t=r.task
        if q.flight.pop(t.ticket,None)!=t:raise ValueError('RECONFIRM_REQUEST_REPLAY')
        q.exchange_weight(r.received_ms,r.ip_weight,limit)
        if r.http==418:
            q.ban=True;q.metrics['http_418']+=1
        elif r.http==429:
            q.backoff=max(q.backoff,r.received_ms+max(60_000,r.retry_ms or 0));q.metrics['http_429']+=1
        b=self._history(r,t.limit,(t.reason+':'+str(t.stage)))
        if not b or not t.request<=r.requested_ms<=r.received_ms<=now_for_commit<min(t.deadline,q.deadline_for(t.symbol,now_for_commit)):
            self.states[t.symbol,t.tf,t.stage]='RECHECK_FAILED_LATE_OR_REMOVED';return
        k=t.symbol,t.tf
        self.states[t.symbol,t.tf,t.stage]='VALID_NATIVE_RECHECK'
        if t.reason=='FULL_MISSING':self.raw[k]=(r,b[-1])
        elif t.stage==1:self.first[k]=(r,b[-1])
        elif t.stage==2:self.second[k]=(r,b[-1])
