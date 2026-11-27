import numpy as np
F=300000; M=60000; T=900000

def fg(t,e):
 i=int(np.searchsorted(t,e));return i if i<len(t) and t[i]==e else None

def pulses(g,t15,w15,finish=None):
 out=[]; waiting=None; fired=set(); eligible=set(g['eligible'])
 # Only known completed raw signals from the admission episode can suppress the new signal.
 rawsorted=sorted(g['r5']|g['l15']);latest=-1; rawpos=0
 for E in sorted(e for e in eligible if finish is None or e<=finish):
  start=g['origin'][E]
  while rawpos<len(rawsorted) and rawsorted[rawpos]<=E:
   latest=rawsorted[rawpos]; rawpos+=1
  if latest>=start or start in fired:continue
  # If ANY raw NW clock through now is unknown, do not infer no buy signal.
  x=fg(g['t5'],start);y=fg(g['t5'],E)
  t15start=((start+T-1)//T)*T;t15last=(E//T)*T
  u=fg(g['t15'],t15start); vv=fg(g['t15'],t15last)
  if x is None or y is None or not (g['k5'][x:y+1].all() and (np.diff(g['t5'][x:y+1])==F).all()):continue
  if t15last>=t15start and (u is None or vv is None or not (g['k15'][u:vv+1].all() and (np.diff(g['t15'][u:vv+1])==T).all())):continue
  if waiting and waiting[0]==start and waiting[1]+30*M>=E:
   _,sweep,hi,lo,shelf=waiting
   i=fg(g['t'],E)
   if i is not None and g['p'][i,3]>hi and E>sweep:
    fired.add(start);out.append((E,start,sweep,hi,lo,shelf,float(g['p'][i,3])))
    continue
  j=fg(t15,E)
  if j is not None and j>=2 and t15[j-2]==E-2*T and t15[j-1]==E-T:
   shelf=min(w15[j-2:j,2]); b=w15[j]
   if np.isfinite(w15[j-2:j+1]).all() and b[2]<shelf<b[3]:
    waiting=(start,E,float(b[1]),float(b[2]),float(shelf))
 return out
