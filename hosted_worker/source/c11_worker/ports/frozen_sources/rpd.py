"""Close-only literal RPD core port. Research only, score is NOT win probability.
MTF gaps_on: on historical bars higher-timeframe values exist only on final
constituent bar. Indexing [1]/[2] is on chart bars, not higher timeframe bars.
"""
import numpy as np
import pandas as pd

def rma(x,n):
    a=np.asarray(x,float);o=np.full(len(a),np.nan);buf=[];prev=np.nan
    for i,v in enumerate(a):
        if np.isnan(v):o[i]=prev;continue
        if np.isnan(prev):
            buf.append(v)
            if len(buf)==n:prev=float(np.mean(buf))
        else:prev=(prev*(n-1)+v)/n
        o[i]=prev
    return pd.Series(o,index=x.index)

def compute(df,period=25,fractal=2,mult=4,threshold=65,distance=10,predictive=True):
    d=df.reset_index(drop=True).copy();h,l,c,v=[d[k].astype(float) for k in ['high','low','close','volume']]
    sm=((h+l+c)/3).rolling(3).apply(lambda x:np.dot(x,[1,2,3])/6,raw=True)
    hi=sm.rolling(period,min_periods=1).max();lo=sm.rolling(period,min_periods=1).min();state=np.floor((sm-lo)/(hi-lo)*5+0.5).where(hi>lo,0)
    velocity=sm-sm.shift(int(np.floor(period/2+0.5)));acc=velocity-velocity.shift()
    delta=sm.diff();cnt=delta.notna().rolling(period,min_periods=1).sum();up=(delta>0).rolling(period,min_periods=1).sum();p=up/cnt.clip(lower=1)
    entropy=pd.Series(np.where((p>0)&(p<1),-(p*np.log2(p.where(p>0,1))+(1-p)*np.log2((1-p).where(p<1,1))),.5))
    tr=pd.concat([h-l,(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1);atr=rma(tr,14);atr16=rma(tr,16)
    strength=velocity.ewm(span=10,adjust=False).mean().abs();change=c.diff();rsi=100-100/(1+rma(change.clip(lower=0),17)/rma((-change).clip(lower=0),17))
    volsp=v>1.2*v.rolling(17).mean();avg=v.rolling(5).mean()
    upper=(h+l+2*c)/4-1.1*atr16;lower=(h+l+2*c)/4+1.1*atr16;trend=np.ones(len(d),int)
    for i in range(1,len(d)):
        if c[i-1]>upper[i-1]:upper[i]=max(upper[i],upper[i-1])
        if c[i-1]<lower[i-1]:lower[i]=min(lower[i],lower[i-1])
        trend[i]=1 if trend[i-1]==-1 and c[i]>lower[i-1] else -1 if trend[i-1]==1 and c[i]<upper[i-1] else trend[i-1]
    fh=pd.Series(False,index=d.index);fl=fh.copy()
    for i in range(2*fractal,len(d)):
        center=i-fractal
        fh[i]=all(h[center]>h[center+j] and h[center]>h[center-j] for j in range(1,fractal+1))
        fl[i]=all(l[center]<l[center+j] and l[center]<l[center-j] for j in range(1,fractal+1))
    pe=(((h.shift()>h.shift(2))&(h.shift()>h))|fh.shift(fill_value=False))&(c<l.shift())
    ve=(((l.shift()<l.shift(2))&(l.shift()<l))|fl.shift(fill_value=False))&(c>h.shift())
    preq=pe&(state.shift()>=2)&(trend==-1);vreq=ve&(state.shift()<=3)&(trend==1)
    pp=predictive&(state>=2)&(acc<0)&(entropy<.85)&(trend==-1);pv=predictive&(state<=3)&(acc>0)&(entropy<.85)&(trend==1)
    # Exact source MTF query default lookahead_off gaps_on. For mult>1 sparse
    # adjacent bars cannot satisfy strict 3-point pivot; explicitly compute.
    ts=d.time_s.astype('int64');step=int(np.median(np.diff(ts)));bucket=ts//(step*mult)
    mh=h.groupby(bucket).transform('max').where((ts+step)%(step*mult)==0);ml=l.groupby(bucket).transform('min').where((ts+step)%(step*mult)==0)
    mp=mh.shift().notna()&(mh.shift()>mh.shift(2).fillna(mh.shift()))&(mh.shift()>mh.fillna(mh.shift()))
    mv=ml.shift().notna()&(ml.shift()<ml.shift(2).fillna(ml.shift()))&(ml.shift()<ml.fillna(ml.shift()))
    pe2=pe|(mp&(c<ml.shift().fillna(l.shift())));ve2=ve|(mv&(c>mh.shift().fillna(h.shift())))
    effective=threshold-strength/atr*5
    hs=h.shift();ls=l.shift();vels=velocity.shift();es=entropy.shift()
    out=[];last=-100000;lastp=lastpv=lastl=lastlv=np.nan
    for i in range(len(d)):
        bd=bool(pe[i] and hs[i]>lastp and vels[i]<lastpv);bu=bool(ve[i] and ls[i]<lastl and vels[i]>lastlv)
        def score(div):
            e=es[i]
            raw=40+strength[i]/atr[i]*30+(1-e)*10+(10+(1-e)*5 if e<.85 else -5)+(20+abs(acc[i])*2 if div else 0)
            raw+=(8+(rsi[i]-50)/5 if ((rsi[i]>65 and pe2[i]) or (rsi[i]<40 and ve2[i])) else 0)
            raw+=(5+(v[i]/avg[i]-1)*3 if volsp[i] else 0)+(10+strength[i]*.5 if strength[i]>atr[i] else 0)+(12 if (mp[i] and pe2[i]) or (mv[i] and ve2[i]) else 0)+(h[i]-l[i])/atr[i]*3-1.5
            return min(99,max(40,np.floor(raw*10+.5)/10)) if np.isfinite(raw) else np.nan
        ps=score(bd) if preq[i] or pp[i] else np.nan;vs=score(bu) if vreq[i] or pv[i] else np.nan
        vp=bool((preq[i] or pp[i]) and ps>=effective[i] and i-last>distance);vl=bool((vreq[i] or pv[i]) and vs>=effective[i] and i-last>distance)
        if vp:last=i;lastp=h[i-1];lastpv=velocity[i-1]
        if vl:last=i;lastl=l[i-1];lastlv=velocity[i-1]
        out.append((int(vl and vreq[i]),int(vp and preq[i]),vs,ps,int(vl and pv[i] and not vreq[i]),int(pv[i])))
    o=pd.DataFrame(out,columns=['long','short','long_score','short_score','predictive_long','potential_long']);o['time_s']=ts;o['available_time_s']=ts+step
    for name,x in [('entropy',entropy),('trend_strength',strength),('atr',atr),('rtrend',trend),('state',state),('velocity',velocity),('min_prob',effective)]:o[name]=x
    return o
