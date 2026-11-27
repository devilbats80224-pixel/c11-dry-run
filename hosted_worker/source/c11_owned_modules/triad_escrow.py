"""Offline scheduling primitive for a 5m-only initial disagreement.

No network, system clock, state mutation beyond this actor, trading, or source defaults.
The existing queue must supply one shared atomic budget view for every call.
"""
from dataclasses import dataclass
from decimal import Decimal,InvalidOperation
import re
from same_e_confirmation_clock import first_send_floor,second_send_floor,RECEIPT_GAP_MS

TF=('5m','15m','1m')
WIDTH={'5m':300_000,'15m':900_000,'1m':60_000}
ROUND_GAP=RECEIPT_GAP_MS
@dataclass(frozen=True)
class Limit:
    spent_calls:int
    spent_weight:int
    other_reserved_calls:int
    other_reserved_weight:int
    ip_hwm:int
    ip_limit:int
    free_slots:int
    request_latency_upper_ms:int
@dataclass(frozen=True)
class Identity:
    pair:str
    generation:int
    end_ms:int
    lease_exclusive_ms:int
@dataclass(frozen=True)
class Arrival:
    identity:Identity
    tf:str
    request_id:str
    requested_ms:int
    received_ms:int
    close:Decimal
    valid:bool
    final_raw:tuple
@dataclass(frozen=True)
class Task:
    identity:Identity
    tf:str
    count:int
    ticket:int
    stage:int
    send_after_ms:int
    send_before_ms:int
    @property
    def params(self):
        return dict(symbol=self.identity.pair,interval=self.tf,
                    startTime=self.identity.end_ms-self.count*WIDTH[self.tf],
                    endTime=self.identity.end_ms-1,limit=self.count)

TICKET=re.compile(r'[a-f0-9]{32}\.[0-9]+\.[1-9][0-9]*\Z')
def attributed(a,i,ticket=None):
    if (a.identity!=i or type(a.request_id) is not str or not TICKET.fullmatch(a.request_id)
        or not a.valid or a.tf not in WIDTH or type(a.final_raw) is not tuple or len(a.final_raw)!=12):
        return False
    _,request_end,seq=a.request_id.split('.')
    row=a.final_raw
    try:close=Decimal(row[4])
    except (InvalidOperation,TypeError,ValueError):return False
    return (int(request_end)==i.end_ms and (ticket is None or int(seq)==ticket) and
            type(row[0]) is type(row[6]) is int and row[0]==i.end_ms-WIDTH[a.tf] and
            row[6]==i.end_ms-1 and type(row[4]) is str and close.is_finite() and close>0 and close==a.close)

def add_bound(lim,planned):
    return (0<=lim.spent_calls+lim.other_reserved_calls+planned<=96 and
            0<=lim.spent_weight+lim.other_reserved_weight+planned<=144 and
            0<=lim.ip_hwm<=lim.ip_limit and
            lim.ip_hwm+lim.spent_weight+lim.other_reserved_weight+planned+1200<=lim.ip_limit)
class Triad:
    def __init__(self,identity,initial):
        self.i=identity
        self.deadline=min(identity.end_ms+60000,identity.lease_exclusive_ms)
        self.first=self.second=None
        self.escrow=0
        self.initial=initial
        if set(initial)!=set(TF) or not all(a.tf==tf and attributed(a,identity) and identity.end_ms<=a.requested_ms<=a.received_ms<self.deadline
                                           for tf,a in initial.items()):
            raise ValueError('INITIAL_CERTIFIED_TRIPLE_REQUIRED')
        if initial['1m'].close!=initial['15m'].close or initial['5m'].close==initial['1m'].close:
            raise ValueError('NOT_A_FIVE_ONLY_INITIAL_DISAGREEMENT')
        iids={a.request_id for a in initial.values()}
        if len(iids)!=3:raise ValueError('DUPLICATE_INITIAL_REQUEST')
        self.used=iids
        self.used_tickets={int(x.rsplit('.',1)[1]) for x in iids}
        if len(self.used_tickets)!=3:raise ValueError('DUPLICATE_INITIAL_TICKET')
        self.planned={}
    def reserve(self,at,lim):
        if not self.escrow==0 or self.first or self.second:return False
        # Two whole waves plus actual in-flight envelope must fit the existing lease.
        if lim.request_latency_upper_ms<1 or not add_bound(lim,6):return False
        floor=max(at,first_send_floor(self.i.end_ms,(a.received_ms for a in self.initial.values())))
        if floor+ROUND_GAP+2*lim.request_latency_upper_ms>=self.deadline:return False
        self.escrow=6
        return True
    def wave(self,identity,at,lim,next_ticket):
        if identity!=self.i or at>=self.deadline:
            self.escrow=0
            return ()
        if self.escrow not in (3,6) or lim.free_slots<3 or not add_bound(lim,self.escrow) or not (type(next_ticket) is int and next_ticket>0):
            return ()
        stage=2 if self.escrow==3 else 1
        if set(range(next_ticket,next_ticket+3))&self.used_tickets:return ()
        if stage==2 and self.first is None:return ()
        floor=first_send_floor(self.i.end_ms,(a.received_ms for a in self.initial.values()))
        if self.first is not None:floor=max(floor,second_send_floor(a.received_ms for a in self.first.values()))
        if at<floor or at+lim.request_latency_upper_ms>=self.deadline:return ()
        if stage==1 and at+ROUND_GAP+2*lim.request_latency_upper_ms>=self.deadline:return ()
        self.escrow-=3
        # Four-slot and actual 96/144 debit belongs to the calling shared queue
        # transaction; callers MUST DEBIT here or release and send nothing.
        jobs=tuple(Task(identity,tf,3 if tf=='5m' else 1,next_ticket+i,stage,floor,self.deadline) for i,tf in enumerate(TF))
        self.planned[stage]=jobs
        self.used_tickets.update(t.ticket for t in jobs)
        return jobs
    def receive(self,identity,wave,items):
        if identity!=self.i or len(wave)!=3 or len(items)!=3 or tuple(x.tf for x in wave)!=TF:return False
        if any(t.identity!=identity or a.tf!=t.tf or not attributed(a,identity,t.ticket) or a.request_id in self.used or
               not t.send_after_ms<=a.requested_ms<=a.received_ms<t.send_before_ms
               for t,a in zip(wave,items)) or len({a.request_id for a in items})!=3:
            return False
        if len({a.close for a in items})!=1:return False
        stage=wave[0].stage
        if tuple(wave)!=self.planned.get(stage):return False
        if (stage==1 and self.escrow!=3) or (stage==2 and self.escrow!=0):return False
        self.used.update(a.request_id for a in items)
        if stage==1:self.first={a.tf:a for a in items}; return True
        if not self.first or any(a.final_raw!=self.first[a.tf].final_raw or a.requested_ms-self.first[a.tf].received_ms<ROUND_GAP for a in items):
            return False
        self.second={a.tf:a for a in items}
        return True
