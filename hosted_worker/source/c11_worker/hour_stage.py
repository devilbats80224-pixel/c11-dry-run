"""Causal historical one-time 1h gate, selectable triggers and floor pairs, single worker."""
from dataclasses import replace
from math import isfinite
from policy_snapshot import step,Unknown
OFF='OFF'
PARAMS={'OFF':None,'ORIGINAL_3_5_08_16':(1.03,1.05,1.008,1.016),'DELAY_5_7_08_16':(1.05,1.07,1.008,1.016),'GENTLE_3_5_04_08':(1.03,1.05,1.004,1.008),'MID_4_6_08_16':(1.04,1.06,1.008,1.016),'LATE_6_8_08_16':(1.06,1.08,1.008,1.016)}
def transport(label,mode,old,bar,*,published,atr,hour,original):
 params=PARAMS[label]
 if params is None:return original(mode,old,bar,published=published,atr=atr)
 assert mode=='INTRA_WEAK'
 old_atr=atr.published if atr.guard.loss.author_armed and atr.published>old.hard_stop else 0.
 t=replace(old,gate='STAGE_BYPASS',passed_stage=max(old_atr,old.passed_stage))
 nxt,a=step(t,bar,published=published,enabled=False)
 if not nxt.held:
  if a['reason'] in ('OLD_STOP_GAP','OLD_STOP_INTRABAR'):
   gap=a['reason']=='OLD_STOP_GAP'
   if not gap or bar.open>old.hard_stop:
    if old_atr>old.hard_stop and old_atr>=old.passed_stage:a={**a,'reason':'ORIGINAL_ARMED_TRAIL_GAP' if gap else 'ORIGINAL_ARMED_TRAIL_INTRABAR'}
    elif old.passed_stage>old.hard_stop:a={**a,'reason':'HOUR_PASS_STAGE_GAP' if gap else 'HOUR_PASS_STAGE_INTRABAR'}
  return nxt,a
 p3,p5,k3,k5=params
 gate=old.gate
 assert gate in ('UNSEEN','PASS','FAIL')
 if gate=='UNSEEN' and bar.close>=old.entry*p3:
  end=bar.E//3600000*3600000
  if hour['end_E']!=end or hour['available_E']!=end or not all(isfinite(hour[k]) and hour[k]>=0 for k in ('last','previous')):
   raise Unknown('STRICT_COMPLETED_HOUR_REQUIRED')
  gate='PASS' if hour['last']>hour['previous'] else 'FAIL'
 new=old.entry*(k5 if bar.close>=old.entry*p5 else k3) if gate=='PASS' and bar.close>=old.entry*p3 else 0.
 # Never persist the temporary combined ATR in the separate early protection state.
 return replace(nxt,gate=gate,passed_stage=max(old.passed_stage,new)),a
def fixtures():
 from types import SimpleNamespace as NS
 from policy_snapshot import Bar,State,Published
 from stage_adapter_bridge import step_transport
 start=3600000
 S=State(entry=100.,hard_stop=93.,entry_E=start,last_E=start)
 atr=NS(published=0.,guard=NS(loss=NS(author_armed=False)))
 def make(e,last,prev):return dict(end_E=e//3600000*3600000,available_E=e//3600000*3600000,last=last,previous=prev)
 def run(k,s,e,c,low=94,last=2,prev=1):
  return transport(k,'INTRA_WEAK',s,Bar(e,c,max(c,110),low,c),published=Published(),atr=atr,hour=make(e,last,prev),original=step_transport)
 e=start+60000
 v,a=run('ORIGINAL_3_5_08_16',S,e,103);assert (v.gate,v.passed_stage,a['reason'])==('PASS',100.8,'HOLD')
 v,a=run('GENTLE_3_5_04_08',S,e,103);assert (v.gate,v.passed_stage)==('PASS',100.4)
 v,a=run('DELAY_5_7_08_16',S,e,103);assert (v.gate,v.passed_stage)==('UNSEEN',0.)
 v,a=run('DELAY_5_7_08_16',v,e+60000,105,low=94);assert (v.gate,v.passed_stage,a['reason'])==('PASS',100.8,'HOLD')
 _,a=run('DELAY_5_7_08_16',v,e+120000,107,low=99);assert a['reason']=='HOUR_PASS_STAGE_INTRABAR'
 v,a=run('DELAY_5_7_08_16',v,e+120000,107,low=102);assert (v.gate,v.passed_stage)==('PASS',101.6)
 v,_=run('DELAY_5_7_08_16',S,e,105,last=1,prev=1);v,_=run('DELAY_5_7_08_16',v,e+60000,107);assert (v.gate,v.passed_stage)==('FAIL',0.)
 _,a=run('ORIGINAL_3_5_08_16',S,e,103,low=91);assert a['reason']=='OLD_STOP_INTRABAR'
 # Historical helper parity with live old snapshot when hard/ATR unchanged on qualifying bar.
 for c in (102.,103.,105.,106.):
  v,a=run('ORIGINAL_3_5_08_16',S,e,c)
  bar=Bar(e,c,110,94,c); ex,b=step(S,bar,published=Published(),enabled=False,hour={**make(e,2,1),'known':True})
  assert v==ex and a==b
