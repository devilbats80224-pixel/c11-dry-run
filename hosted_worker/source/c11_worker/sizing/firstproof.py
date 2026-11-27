"""Apply the one-proof exit only while a filled sweep has never closed above entry."""
from math import isfinite
def should_exit(entry,peak_close,armed,run,was_enqueued,other_pending):
 return isfinite(peak_close) and peak_close<=entry and not armed and run==1 and not was_enqueued and not other_pending

def attach(pol,keys):
 enter,hold,leave=pol.on_entry,pol.on_hold,pol.on_exit
 mx={};events=[]
 def entry(T,j,E,p,plan):
  assert j not in mx
  enter(T,j,E,p,plan)
  if (pol.names[j],E) in keys:mx[j]=p
 def update(T,j,E,o):
  before=len(pol.events['weak'])
  hold(T,j,E,o)
  if j not in mx:return
  close=float(o[3]); mx[j]=max(mx[j],close)
  ss=pol.states[j]; ar=pol.atrs[j]; d=pol.info[j];run,already=pol.weak[j]
  if len(pol.events['weak'])==before:return
  row=pol.events['weak'][-1]
  assert (row['E'],row['entry_E'],row['symbol'])==(E,ss.entry_E,pol.names[j])
  if should_exit(ss.entry,mx[j],ar.guard.loss.author_armed,run,already,bool(d.get('early2_pending') or d.get('lowerhigh_pending') or ss.pending)):
   assert row['status']=='KNOWN_TRUE_QUOTE'
   pol.weak[j]=(run,True);row['enqueued']=True
   events.append(dict(symbol=pol.names[j],entry_E=ss.entry_E,E=E,entry=ss.entry,close=close,max_completed_close=mx[j]))
 def exit(T,j,E,act,net,pos):
  leave(T,j,E,act,net,pos)
  mx.pop(j,None)
 pol.on_entry=entry;pol.on_hold=update;pol.on_exit=exit
 return events
