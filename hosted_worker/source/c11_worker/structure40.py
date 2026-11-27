import numpy as np
def use(E,i,clock,px,price):
    done=(E-1)//900000*900000
    j=i-(E-done)//60000
    if j<14:return 'UNKNOWN_RANGE',None
    assert (int(clock[j]),int(clock[j-14]))==(done,done-840000)
    h=px[j-14:j+1,1]
    l=px[j-14:j+1,2]
    if len(l)!=15 or not np.isfinite(l).all() or not np.isfinite(h).all() or l.min()<=0 or h.max()<=l.min():return 'UNKNOWN_RANGE',None
    base=(price-float(h.max()))/(float(h.max())-float(l.min()))
    return ('STRUCT40' if 0<base<=1 else 'UNCHANGED',base)
if __name__=='__main__':
    t=np.arange(61)*60000
    p=np.ones((61,4))*100.;p[:,1]=105.;p[:,2]=95.
    p[30,1]=800.  # current open-to-close range must not leak into structure
    assert use(t[30],30,t,p,106.)==('STRUCT40',.1)
    assert use(t[30],30,t,p,115.)==('STRUCT40',1.)
    assert use(t[30],30,t,p,105.)==('UNCHANGED',0)
    assert use(t[30],30,t,p,116.)[0]=='UNCHANGED'
    assert use(t[8],8,t,p,110.)[0]=='UNKNOWN_RANGE'
    print('PASS predecision strict15, no same-bar high, exercise exact natural bounds and unknown')
