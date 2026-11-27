from decimal import Decimal as D,ROUND_FLOOR
CAP=.20
def rounded_cap(incumbent,nav,unit,lot,qty):
 assert nav>0 and unit>0 and lot>0 and qty>0 and incumbent>=0
 if incumbent+unit*qty<=CAP*nav:return qty
 q=min(qty,max(0.,(CAP*nav-incumbent)/unit))
 return float((D(str(q))/D(str(lot))).to_integral_value(rounding=ROUND_FLOOR)*D(str(lot)))
def tune(T,j,E,x,plan,engine_plan,pair,w,before,flag,kwargs,fee):
 if len(T.pos)!=2 or plan['quantity']<=0:return plan
 nav,inc=T.policy.risk(T,E)
 meta=kwargs['meta'];d=T.policy.cut(x*.94,meta['tick']);unit=(x-d)+(x+d)*fee
 q0=plan['quantity']; q1=rounded_cap(inc,nav,unit,meta['lot'],q0)
 if q1==q0:return plan
 if q1<max(meta['minqty'],meta['lot']) or q1*x<meta['minnotional']:
  u=dict(plan,quantity=0.,raw_confirmed_quantity=0.,notional=0.,margin=0.,reason='NATIVE_CAPACITY_REJECT')
 else:
  u=engine_plan(pair,w*1.2 if flag else w,before,x,**dict(kwargs,meta=dict(meta,maxqty=min(meta['maxqty'],q1))))
 assert 0<=u['quantity']<=q1<=q0 and (u['quantity']<=0 or inc+unit*u['quantity']<=CAP*nav+1e-7)
 T.policy.events.setdefault('book_risk_cap',[]).append(dict(s=T.policy.names[j],E=E,prior=inc,nav=nav,q0=q0,capq=q1,qty=u['quantity'],initial_stop=d))
 return u
def tests():
 assert rounded_cap(90.,1000.,1.1,1.,100.)==100.
 assert rounded_cap(100.,1000.,1.1,1.,100.)==90.
 assert rounded_cap(200.,1000.,1.,1.,50.)==0.
 return 3
