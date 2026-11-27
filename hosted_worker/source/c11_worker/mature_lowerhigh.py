import numpy as np
MAT_REASON='MATURE20_LOWER15_HIGH_AND_CLOSE_NEXT_OPEN'
D=900000
def observe(E,i,clock,px,entry,mature_prev,close,scheduled):
    if E%D or not mature_prev or mature_prev[1] or scheduled or close<entry*1.20:return None
    if i<29 or not (int(clock[i-29])==E-29*60000 and int(clock[i-14])==E-14*60000):
        return ('UNKNOWN_MATURE_PRICE_GRID',None,None)
    h=px[i-29:i+1,1]
    if len(h)!=30 or not np.isfinite(h).all() or min(h)<=0:return ('UNKNOWN_MATURE_PRICE_HIGH',None,None)
    current=float(max(h[15:]));prior=float(max(h[:15]))
    prior_close=float(px[i-15,3])
    if not np.isfinite(prior_close) or prior_close<=0:return ('UNKNOWN_MATURE_PRIOR_CLOSE',None,None)
    if close>=prior_close:return ('MATURE20_CLOSE_NOT_LOWER',current,prior)
    return ('MATURE20_LOWER_HIGH' if current<prior else 'MATURE20_PRIOR_NOT_LOWER',current,prior)
if __name__=='__main__':
    c=np.arange(100)*60000;h=np.ones((100,4))*110;h[40:46,1]=125;h[46:61,1]=121;h[45,3]=123
    def f(known=(112,False),scheduled=False,close=120,k=60):
        return observe(int(c[k]),k,c,h,100,known,close,scheduled)
    assert f()==('MATURE20_LOWER_HIGH',121,125)
    assert f(known=None) is None and f((112,True)) is None and f(close=119.99) is None
    assert f(scheduled=True) is None and f(k=59) is None
    assert f(close=123.)[0]=='MATURE20_CLOSE_NOT_LOWER'
    h[60,1]=125;assert f()[0]=='MATURE20_PRIOR_NOT_LOWER'
    h[60,1]=np.nan;assert f()[0]=='UNKNOWN_MATURE_PRICE_HIGH'
    print('PASS old maturity, exact complete clock, original precedence, 20% threshold, lower high AND lower close, unknown closed')
