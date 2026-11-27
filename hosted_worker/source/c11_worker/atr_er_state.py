"""Exact incremental ATR/ER transition previously verified against the archived core."""

def new_atr():
    return dict(closes=[],trs=[],atr=None,multiplier=None,signed=None,n=0,clock=None)


def advance(s,r,width):
    prior=s['closes']
    if s['clock'] is not None:
        assert r.open_ms==s['clock']
    tr=r.high-r.low if not prior else max(r.high-r.low,abs(r.high-prior[-1]),abs(r.low-prior[-1]))
    if s['atr'] is None:
        s['trs'].append(tr)
        if len(s['trs'])==20:
            s['atr']=sum(s['trs'])/20
            s['trs']=[]
    else:
        s['atr']=(1/20)*tr+(1-1/20)*s['atr']
    signed=0.
    history=prior+[r.close]
    if len(history)>=21:
        den=sum(abs(history[k]-history[k-1]) for k in range(len(history)-20,len(history)))
        signed=(r.close-history[-21])/den if den>0 else 0.
    raw=1+2*abs(signed)
    s['multiplier']=raw if s['multiplier'] is None else (2/6)*raw+(1-2/6)*s['multiplier']
    s['signed']=signed if s['signed'] is None else (2/6)*signed+(1-2/6)*s['signed']
    s['closes']=history[-20:]
    s['n']+=1
    s['clock']=r.open_ms+width
    return dict(known=s['n']>=400,atr=s['atr'],multiplier=s['multiplier'],signed=s['signed'],
                source_end_ms=s['clock'],available_proxy_ms=s['clock'],
                actual_received_ms=None,consecutive_bars=s['n'])
