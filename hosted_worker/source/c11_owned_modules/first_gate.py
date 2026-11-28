"""Optional bounded gate diagnostic only, never native authority."""
import json
from threading import get_ident
from diagnostic_schema import PATTERN,Q,TTL,SEED_HOLD,DENIAL,EXTRA,TRANSITIONS,FIRST_GATES,MAX_LINE
class FirstGate:
 def __init__(self,sink):
    self.sink=sink; self.owner=get_ident(); self.end=None;self.rows={};self.released=None
 def watch(self,w,g,s,expiry,seed=None):
    if type(g)is not int or not PATTERN.fullmatch(s):return
    end=w//Q*Q
    if self.end!=end or not end<=w<end+TTL or self.owner!=get_ident():return
    row=self.rows.setdefault((g,s),dict(first=None,wire=set(),valid=set(),decision=0,expires=expiry,transitions={},atomic_refusals={},atomic_phases={},normalization_refusals={},owner_receipt_clocks={},reconfirm_progress={},analysis_flow={},prewire_refusals={}))
    row['expires']=min(row['expires'],expiry)
    if seed in SEED_HOLD:self.note(g,s,'gate',SEED_HOLD[seed])
 def start(self,w):
    e=w//Q*Q
    if self.end is not None and w>=self.end+TTL:
       e0,rr=self.end,self.rows
       self.rows={};self.end=None;self.released=e0
       ordered=sorted(rr.items())
       for (g,s),r in ordered:
          state=r['first'] or ('ORIGINAL_CALLBACK_OBSERVED' if r['decision'] else
                               'NO_OBSERVED_OWNER_WIRE' if not r['wire'] else 'NO_TIMELY_ORIGINAL_CALLBACK')
          packet=dict(schema='c11-gate-readback-v1',symbol=s,generation=g,end_ms=e0,first_gate=state,
                      owner_wire_charged_lanes=sorted(r['wire']),valid_reply_lanes=sorted(r['valid']),
                      original_callbacks=r['decision'],observed_expiry_bound_ms=r['expires'],
                      note='FIRST_OBSERVATION_NOT_ROOT_CAUSE',transition_ms=r['transitions'])
          self._emit(packet)
       # Preserve base callback visibility ahead of every optional detail write.
       for (g,s),r in ordered:
          for k,(_,field) in EXTRA.items():
             if r[field]:self._emit(dict(schema='c11-gate-detail-v1',symbol=s,generation=g,end_ms=e0,kind=k,values=r[field]))
    if e<=w<e+TTL and self.released!=e and self.end is None:self.end=e
 def _emit(self,packet):
    try:
       line=(json.dumps(packet,separators=(',',':'),sort_keys=True)+'\n').encode()
       if len(line)<=MAX_LINE:self.sink(line)
    except Exception:pass
 def note(self,g,s,kind,value,now=None):
    row=self.rows.get((g,s))
    if row is None:return
    if kind=='gate' and row['first'] is None and type(value) is str and value in FIRST_GATES:row['first']=value
    if (kind=='transition' and type(now) is int and type(value) is str and
            value in TRANSITIONS and self.end is not None and 0<=now-self.end<TTL):
        t=now-self.end
        if value in row['transitions']:
           a=row['transitions'][value];a[0]=min(a[0],t);a[1]=max(a[1],t);a[2]=min(a[2]+1,255)
        elif len(row['transitions'])<12:row['transitions'][value]=[t,t,1]
    elif (kind in EXTRA and type(value) is str and value in EXTRA[kind][0] and
          type(now) is int and self.end is not None and 0<=now-self.end<TTL):
        t=now-self.end
        a=row[EXTRA[kind][1]].setdefault(value,[t,t,0])
        a[0]=min(a[0],t);a[1]=max(a[1],t);a[2]=min(a[2]+1,255)
    elif kind in ('wire','valid') and value in ('1m','5m','15m'):row[kind].add(value)
    elif kind=='callback':row['decision']=min(row['decision']+1,255)
