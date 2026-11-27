import numpy as np
D=900000; MIN=60000
REASON='EARLY2_QUOTE_BREAK_NEXT_OPEN'
def evaluate(E,entry_E,entry,peak_close,c,armed,px,qs,i):
    if E != (entry_E+D-1)//D*D+2*D:return None
    if armed or peak_close>=entry*1.03 or not c<entry:return None
    if i<29:return ('UNKNOWN_ORIGIN',None,None,None)
    lows=px[i-29:i-14,2]
    if len(lows)!=15 or not np.isfinite(lows).all() or min(lows)<=0:return ('UNKNOWN_PRICE',None,None,None)
    prev_low=float(min(lows))
    if not c<prev_low:return ('NO_BREAK',prev_low,None,None)
    v,_,end=qs
    j=np.arange(i,i-26,-5);ends=E-np.arange(0,6)*300000
    if not np.array_equal(end[j],ends) or not np.isfinite(v[j]).all() or (v[j]<0).any():return ('UNKNOWN_QUOTE',prev_low,None,None)
    qnow=float(sum(v[j[:3]]));qprev=float(sum(v[j[3:]]))
    return ('SCHEDULE_NEXT_OPEN' if qnow<=qprev else 'QUOTE_EXPANDING',prev_low,qnow,qprev)
