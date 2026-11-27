import numpy as np
import pandas as pd

def ms(s):return s.astype('datetime64[ns, UTC]').astype('int64')//1000000

def calculate(d):
 c=d.close.astype(float);h=d.high.astype(float);lo=d.low.astype(float);L=150
 z=(2*c-c.shift(74)).ewm(alpha=2/151,adjust=False).mean()
 tr=pd.concat([h-lo,(h-c.shift()).abs(),(lo-c.shift()).abs()],axis=1).max(axis=1)
 atr=np.full(len(d),np.nan)
 if len(d)>=L:
  atr[L-1]=tr.iloc[:L].mean()
  for i in range(L,len(d)):atr[i]=(atr[i-1]*149+tr.iloc[i])/150
 v=pd.Series(atr).rolling(450,min_periods=1).max()*1.2
 up=z+v;dn=z-v;trend=np.zeros(len(d),dtype=int)
 for i in range(1,len(d)):
  trend[i]=trend[i-1]
  if c.iloc[i]>up.iloc[i] and c.iloc[i-1]<=up.iloc[i-1]:trend[i]=1
  if c.iloc[i]<dn.iloc[i] and c.iloc[i-1]>=dn.iloc[i-1]:trend[i]=-1
 t=pd.Series(trend);big=(t>0)&(t.shift()<=0);small=(c>z)&(c.shift()<=z.shift())&(t==1)&(t.shift()==1)
 return pd.DataFrame({'date':d.date,'clock':ms(d.date+pd.Timedelta(minutes=15)),'close':c,'basis':z,'volatility':v,'trend':t,'big':big,'small':small,'count':np.arange(len(d))})
