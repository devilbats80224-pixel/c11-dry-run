from math import isfinite
def qualified(*,already_mature,atr_armed,gate,published_prv,entry,close,peak,tick,ceil_tick):
 if already_mature or atr_armed or gate!='PASS' or not published_prv>entry or close<entry*1.20:return False
 if not all(isfinite(x)and x>0 for x in (entry,close,peak,tick)) or not peak>=close:return False
 return close>ceil_tick(entry+.60*(peak-entry),tick)
if __name__=='__main__':
 from math import ceil
 d=dict(already_mature=False,atr_armed=False,gate='PASS',published_prv=101.6,entry=100.,close=120.1,peak=129.,tick=.01,ceil_tick=lambda a,b:ceil(a/b)*b)
 def f(**kw):return qualified(**dict(d,**kw))
 assert f()
 assert not any(f(**e)for e in ({'already_mature':True},{'atr_armed':True},{'gate':'FAIL'},{'published_prv':100},{'close':119.99},{'peak':141},{'peak':119},{'peak':float('nan')}))
 print('PASS completed close, previously published profitable stage, non-ARM only, traded price above tick-ceil and malformed reject')
