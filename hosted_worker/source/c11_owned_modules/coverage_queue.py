"""Offline independent admission/coverage core. Transport and trading are absent."""
from exact_symbol import ID_CHAR
from dataclasses import dataclass,field
from decimal import Decimal,InvalidOperation
import re
MIN=60_000
REQ=(('15m',900_000,260,2),('1m',60_000,30,1))
SPEC={k:(b,l,w) for k,b,l,w in REQ}
@dataclass(frozen=True)
class Member:
    symbol:str
    generation:int
    available:int
    expires:int
    rank:int
@dataclass(frozen=True)
class Task:
    symbol:str
    tf:str
    deadline:int
    request:int
    end:int
    ticket:int
    generation:int
    minute_limit:int|None=field(default=None,kw_only=True)
class Queue:
    def __init__(self,end,max_flight=4,quota=120,reserve=400,carry_cursor=()):
        assert end%900_000==0 and 0<max_flight<=4 and 0<quota<=120 and reserve>=0
        self.end=end;self.deadline=end+MIN
        self.members={};self.g=0;self.max_flight=max_flight;self.quota=quota;self.reserve=reserve
        assert len(carry_cursor)<=60 and len(set(carry_cursor))==len(carry_cursor) and all(re.fullmatch(f'{ID_CHAR}{{2,40}}',x) for x in carry_cursor)
        self.flight={};self.finished={};self.attempt={};self.fail={};self.cursor=list(carry_cursor);self.seq=0
        self.weight=0;self.spend=0;self.ip=None;self.minute=None;self.backoff=0;self.ban=False
        self.metrics={'dispatches':0,'http_429':0,'http_418':0,'bad_telemetry':0}
    def snapshot(self,g,members,at):
        # A confirmed complete classified provider snapshot only; expiry handles an
        # unreadable poll (do not call this with a made-up authoritative empty frame).
        assert type(g) is int and g>self.g and len(members)<=60
        s={}
        for m in members:
            assert (m.generation==g and 0<=m.available<=at<m.expires
                and type(m.rank) is int and 1<=m.rank<=60 and m.symbol not in s
                and re.fullmatch(f'{ID_CHAR}{{2,40}}',m.symbol))
            s[m.symbol]=m
        assert len({m.rank for m in members})==len(members)
        self.g=g;self.members=s
        self.cursor=[x for x in self.cursor if x in s]+[m.symbol for m in sorted(members,key=lambda x:x.rank) if m.symbol not in self.cursor]
    def deadline_for(self,s,now):
        m=self.members.get(s)
        return min(self.deadline,m.expires) if m and m.available<=now<m.expires else 0
    def exchange_weight(self,at,used,limit):
        if type(used) is not int or type(limit) is not int or not 0<=used<=limit or limit<1:
            self.ip=None;self.metrics['bad_telemetry']+=1;return
        # Single event loop aggregates immutable *per-response* headers. An older
        # completion cannot lower the conservative latest same-minute high-water.
        bucket=at//MIN
        if self.minute is not None and self.minute>bucket:return
        if self.minute!=bucket:self.weight=used;self.spend=0;self.minute=bucket
        else:self.weight=max(self.weight,used)
        self.ip=limit
    def dispatch(self,now):
        if now<self.end or now>=self.deadline or now<self.backoff or self.ban:return ()
        if self.ip is None or self.minute!=now//MIN:return ()
        out=[];size=len(self.cursor); visits=0
        while len(self.flight)<self.max_flight and visits<size:
            s=self.cursor.pop(0);self.cursor.append(s);visits+=1
            dl=self.deadline_for(s,now)
            if now+2_000>=dl:continue # need at least a bounded admission margin
            if any(x.symbol==s for x in self.flight.values()):continue
            if self.spend>=self.quota or self.weight+self.spend+1+self.reserve>self.ip:
                self.cursor.insert(0,self.cursor.pop())
                break
            for tf,_,_,weight in REQ:
                k=s,tf
                if k in self.finished or self.attempt.get(k,0)>=1:continue
                # Conservatively add already-issued local requests even if shared-IP
                # usage already counts them; safe headroom vs undercounting.
                if self.spend+weight>self.quota or self.weight+self.spend+weight+self.reserve>self.ip:continue
                self.seq+=1
                t=Task(s,tf,dl,now,self.end,self.seq,self.g)
                self.flight[t.ticket]=t;self.attempt[k]=self.attempt.get(k,0)+1
                self.spend+=weight;self.metrics['dispatches']+=1;out.append(t)
                break
        return tuple(out)
    def receive(self,t,at,rows,status=200,used=None,limit=None,retry_ms=None):
        if self.flight.pop(t.ticket,None)!=t:raise ValueError('UNKNOWN_OR_REPLAYED_TICKET')
        if used is not None or limit is not None:self.exchange_weight(at,used,limit)
        k=t.symbol,t.tf
        if status==418:
            self.ban=True;self.metrics['http_418']+=1;self.fail[k]='GLOBAL_418_HOLD_OPERATOR'
            return
        if status==429:
            self.backoff=max(self.backoff,at+max(MIN,retry_ms if type(retry_ms) is int and retry_ms>=0 else MIN))
            self.metrics['http_429']+=1;self.fail[k]='GLOBAL_429_BACKOFF';return
        if status!=200:self.fail[k]='NATIVE_TRANSPORT_UNKNOWN';return
        if not t.request<=at<min(t.deadline,self.deadline_for(t.symbol,at)):self.fail[k]='RECEIVED_OUT_OF_WINDOW';return
        width,n,_=SPEC[t.tf]
        # Tuple: native opening ms, native last ms, close string; real receipt is
        # after transport returns, never an exchange candle's synthetic timestamp.
        try:
            assert type(rows) is list and len(rows)==n
            for i,(op,last,val) in enumerate(rows):
                assert (type(op) is type(last) is int and op==self.end-(n-i)*width
                        and last==op+width-1 and last+1<=t.request)
                assert type(val) is str and 0<len(val)<=40
                d=Decimal(val);assert d.is_finite() and d>0
        except (ValueError,AssertionError,InvalidOperation,TypeError):
            self.fail[k]='CLOSED_CONTIGUITY_OR_VALUE';return
        self.finished[k]=(at,d)
    def coverage(self,now):
        out={}
        for s in self.members:
            if now>=self.deadline_for(s,now):out[s]='EXPIRED_OR_NOT_ELIGIBLE';continue
            if any((s,k) not in self.finished for k,_,_,_ in REQ):
                out[s]='UNKNOWN';continue
            a,b=(self.finished[s,k] for k,_,_,_ in REQ)
            out[s]='CANDLE_PAIR_READY_ONLY' if a[0]<=now and b[0]<=now and a[1]==b[1] else 'CLOSE_MISMATCH_OR_NOT_AVAILABLE'
        return out
