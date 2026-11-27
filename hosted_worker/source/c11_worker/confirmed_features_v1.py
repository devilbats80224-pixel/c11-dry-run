"""Causal Pine-formula source reconstruction, no claim of TV full-history seed parity.
ATR20 = Wilder SMA20 seed/RMA; ER20 multiplier1..3 EMA5, signed ER EMA5.
Require400 consecutive complete bars before known. Missing bars reset; never fill gaps.
"""
import math

def indicator_rows(bars,step):
    # sequence of (open_ms,open,high,low,close), sorted; conflicts must be removed upstream.
    result={};history=[];trs=[];atr=None;ema_mult=None;ema_signed=None;prev_time=None;prev_close=None
    for t,o,h,l,c in bars:
        assert type(t) is int and t%step==0
        valid=all(math.isfinite(float(x)) for x in (o,h,l,c)) and 0<l<=min(o,c)<=max(o,c)<=h
        if not valid or (prev_time is not None and t-prev_time!=step):
            history=[];trs=[];atr=None;ema_mult=None;ema_signed=None;prev_close=None
        prev_time=t
        if not valid:continue
        tr=h-l if prev_close is None else max(h-l,abs(h-prev_close),abs(l-prev_close))
        trs.append(tr)
        if len(trs)==20:atr=sum(trs)/20
        elif len(trs)>20:atr=(1/20)*tr+(1-1/20)*atr
        history.append(c)
        if len(history)>=21:
            path=sum(abs(history[k]-history[k-1]) for k in range(len(history)-20,len(history)))
            signed=(c-history[-21])/path if path>0 else 0.
        else:signed=0.
        raw=1+2*abs(signed)
        ema_mult=raw if ema_mult is None else (2/6)*raw+(1-2/6)*ema_mult
        ema_signed=signed if ema_signed is None else (2/6)*signed+(1-2/6)*ema_signed
        result[t+step]={'known':len(history)>=400,'atr':atr,'multiplier':ema_mult,'signed':ema_signed,'source_end_ms':t+step,'available_proxy_ms':t+step,'actual_received_ms':None,'consecutive_bars':len(history)}
        prev_close=c
    return result

def sample(clock,rows5,rows15):
    out=[]
    for E in clock:
        assert type(E) is int and E%60000==0
        d5=E//300000*300000;d15=E//900000*900000;a=rows5.get(d5);b=rows15.get(d15);previous=rows15.get(d15-900000)
        known=bool(a and b and previous and a['known'] and b['known'] and previous['known'])
        out.append({'E':E,'known':known,'source_at5':d5,'source_at15':d15,'source_at_er':d15,'loss_atr15':b['atr'] if known else None,'guard_atr5':a['atr'] if known else None,'guard_multiplier':b['multiplier'] if known else None,'signed_er':b['signed'] if known else None,'previous_signed_er':previous['signed'] if known else None})
    return out
