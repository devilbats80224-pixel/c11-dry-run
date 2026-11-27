from decimal import Decimal as D, ROUND_FLOOR, ROUND_CEILING
import numpy as np
M=60000
F=5*M
d=lambda x:D(str(x))
def geometry(entry,entered,decided,i,clock,px):
 if decided!=((entered+F-1)//F)*F+2*F or i<9:return None
 t=clock[i-9:i+1]
 if t[0]<=entered or not np.array_equal(t,np.arange(decided-9*M,decided+M,M)):return None
 a=px[i-9:i+1].reshape(2,5,4)
 if not np.isfinite(a).all() or a.min()<=0:return None
 hi=a[:,:,1].max(axis=1);lo=a[:,:,2].min(axis=1);c=a[:,-1,3]
 if hi[0]>entry>c[0] and hi[1]<entry and c[1]<lo[0] and hi[1]-lo[1]>hi[0]-lo[0]:
  return dict(hi=hi.tolist(),lo=lo.tolist(),close=c.tolist())
def floor(q,step):return (q/step).to_integral_value(rounding=ROUND_FLOOR)*step
def plan(q,op,entry,meta,hard,buffer,stress):
 q,op,h,b=d(q),d(op),d(hard),d(buffer)
 if op<=max(h,b):return None
 lot=d(meta['lot']);tick=d(meta['tick']);mn=d(meta['minqty']);notional=d(meta['minnotional'])
 assert lot>0 and tick>0 and q%lot==0 and op%tick==0
 sold=floor(q/2,lot);remain=q-sold
 at=op
 if at<=max(h,b) or min(sold,remain)<mn or min(sold,remain)*at<notional:return None
 return dict(qty=float(sold),price=float(at),clock='OPEN',why='EARLY_TWO5_FAIL_RECOVERY_PARTIAL',remaining=float(remain),extra_fee_rate=.0012 if stress else 0.)
def attach(pol,stress):
 # Must attach after original wick; one live open decision per admission.
 start,hold,done=pol.on_entry,pol.on_hold,pol.on_exit
 pending={}; partial={}; audit=[]
 def enter(T,j,E,p,z):
  assert j not in pending and j not in partial
  start(T,j,E,p,z)
 def onhold(T,j,E,o):
  hold(T,j,E,o)
  if j in partial:return
  old=pol.states[j];atr=pol.atrs[j];info=pol.info[j]
  b=pol.data[pol.names[j]]['px']
  hit=geometry(old.entry,old.entry_E,E,T.row,pol.clock,b)
  if hit is None:return
  mat=pol.mature.get(j)
  if atr.guard.loss.author_armed or atr.published or atr.pending or old.passed_stage or old.failed_floor or old.pending or (mat and mat[0]) or pol.weak[j][1] or any(info.get(k) for k in ('half_pending','early2_pending','lowerhigh_pending')):
   audit.append(dict(E=E,s=pol.names[j],mode='SENIOR_STATE_BLOCK'));return
  pending[j]=dict(hit,E=E,entry_E=old.entry_E,s=pol.names[j])
 def openplan(T,j,E,op):
  t=pending.pop(j,None)
  if t is None:return None
  assert E==t['E']+M
  old=pol.states[j];p=pol.info[j]
  # The base hard stop is the only standing line eligible after the closed-state filter.
  qty=T.pos[j]['q']; result=plan(qty,op,old.entry,pol.data[t['s']]['meta'],old.hard_stop,p['buffer'],stress)
  audit.append(dict(t,next_E=E,op=op,status='ENQUEUE_TRIM' if result else 'OPEN_BLOCKED',plan=result))
  return result
 def onpartial(T,j,E,a,net,p):
  assert j not in partial and j not in pending and p['q']==a['remaining'] and pol.states[j].entry_E==p['entry_E']
  partial[j]=net
 def close(T,j,E,a,net,p):
  assert j not in pending
  prior=partial.pop(j,0.)
  return done(T,j,E,a,net,p)
 pol.on_entry,pol.on_hold,pol.on_exit=enter,onhold,close
 pol.partial_open,pol.on_partial=openplan,onpartial
 return audit
