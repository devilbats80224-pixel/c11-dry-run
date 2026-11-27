"""Scratch same-clock triad scheduling/attribution; this layer never opens a client."""
from dataclasses import dataclass
from decimal import Decimal
from triad_escrow import Triad,Identity,Arrival,Limit,TF

@dataclass(frozen=True)
class Job:
 symbol:str; tf:str; deadline:int; request:int; end:int; ticket:int; generation:int
 limit:int;stage:int;owner_plan:object
 reason:str='FIVE_ONLY_TRIPLE'
 triad_only:bool=True
 @property
 def params(self):
  owner=self.owner_plan
  if (owner.identity.pair,owner.tf,owner.identity.end_ms,owner.count,owner.ticket,owner.identity.generation,owner.stage,owner.send_before_ms)!=(
      self.symbol,self.tf,self.end,self.limit,self.ticket,self.generation,self.stage,self.deadline):
   raise ValueError('TRIAD_JOB_OWNER_PLAN_DRIFT')
  if not owner.send_after_ms<=self.request<owner.send_before_ms:raise ValueError('TRIAD_JOB_OWNER_PLAN_DRIFT')
  return owner.params
 @property
 def start(self):
  return self.params['startTime']

class TriadLane:
 def __init__(self,queue,capture,wall,cohorts,recovery):
  self.q=queue;self.capture=capture;self.wall=wall;self.cohorts=cohorts;self.recovery=recovery
  self.plans={};self.attempted=set();self.observed={};self.cleared={};self.holds={}
 def limit(self,external_only):
  q=self.q
  return Limit(q.metrics['dispatches'],q.spend,external_only+96-q.call_cap,external_only+144-q.quota,q.weight,q.ip,q.max_flight-len(q.flight),
               max([900,*getattr(self.recovery,'rtts',[])[-36:]]))
 def total_reserved(self):
  return sum(p.escrow for p in self.plans.values())
 def _arrival(self,r,i):
  x=self.capture.records[r.request_id]
  assert x['request_id']==r.request_id and (x['symbol'],x['generation'],x['end_ms'])==(i.pair,i.generation,i.end_ms)
  return Arrival(i,r.task.tf,r.request_id,r.requested_ms,r.received_ms,
      Decimal(x['raw_kline_rows'][-1][4]),x['capture_grade']=='VALIDATED_ROWS',tuple(x['raw_kline_rows'][-1]))
 def register(self,key,stamp,initial):
  q=self.q
  if key in self.plans or key in self.attempted:return
  now=self.wall()
  try:
   i=Identity(stamp.symbol,stamp.generation,q.end,stamp.expires_ms)
   plan=Triad(i,{r.task.tf:self._arrival(r,i) for r in initial})
   if not plan.reserve(now,self.limit(q.reserved())):
    self.holds[key]='TRIAD_UNRESERVED_SHARED_BUDGET_OR_ORIGINAL_DEADLINE';return
   self.plans[key]=plan;self.attempted.add(key)
  except (ValueError,TypeError,KeyError,AssertionError,IndexError):
   self.holds[key]='TRIAD_INITIAL_BODY_UNVERIFIED'
 def sweep(self,now):
  for key,p in self.plans.items():
   m=self.q.members.get(key[0])
   if (key[1]!=self.q.g or m is None or now>=p.deadline) and p.escrow:
    p.escrow=0;self.holds[key]='TRIAD_UNSENT_ESCROW_RELEASED_EXPIRED_OR_ROLLOVER'
 def dispatch(self,now):
  q=self.q
  self.sweep(now)
  if now>=q.deadline or q.ban or now<q.backoff or q.ip is None or q.minute!=now//60000:return ()
  out=[]
  for k,p in self.plans.items():
   m=q.members.get(k[0])
   if not p.escrow or m is None or m.generation!=k[1] or any(x.symbol==k[0] for x in q.flight.values()):continue
   i=Identity(k[0],m.generation,q.end,m.expires)
   jobs=p.wave(i,now,self.limit(q.reserved()-p.escrow),q.seq+1)
   if not jobs:continue
   assert len(jobs)==3 and len(q.flight)+3<=q.max_flight
   for a in jobs:
     j=Job(i.pair,a.tf,a.send_before_ms,now,i.end_ms,a.ticket,i.generation,a.count,a.stage,a)
     assert j.ticket not in q.flight
     q.seq+=1;q.spend+=1;q.metrics['dispatches']+=1;q.flight[j.ticket]=j;out.append(j)
  return tuple(out)
 def receive(self,r):
  t=r.task;k=(t.symbol,t.generation)
  p=self.plans.get(k);m=self.q.members.get(t.symbol)
  if p is None or m is None or self.q.g!=t.generation or r.received_ms>=p.deadline:
   self.holds[k]='TRIAD_RECEIPT_LATE_OR_STALE';return
  d=self.observed.setdefault((k,t.stage),{})
  if t.tf in d:raise ValueError('DUPLICATE_TRIPLE_TF')
  d[t.tf]=r
  if len(d)!=3:return
  try:
   received=tuple(d[tf] for tf in TF)
   if not p.receive(p.i,tuple(x.task.owner_plan for x in received),tuple(self._arrival(x,p.i) for x in received)):
    raise ValueError('TRIAD_RECONFIRM_RAW_NOT_COHERENT')
   if t.stage==2:
    first=self.observed[(k,1)]
    self.cleared[k]=(tuple(first[tf] for tf in TF),received)
   self.holds.pop(k,None)
  except (AssertionError,IndexError,TypeError,ValueError,KeyError) as e:
   p.escrow=0;self.holds[k]=str(e) or 'TRIAD_INVALID_RESPONSE_UNSENT_RELEASE'
