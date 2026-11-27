import numpy as np
M=60000

def archived_weak(s,entry,since,E,close,F5,QUANT):
 age=E-since
 if age<60*M or age%(30*M):return 'NOT_SCHEDULED'
 if close>=entry:return 'KNOWN_FALSE_PRICE'
 e,h,q=F5[s];i=int(np.searchsorted(e,E));assert i>=11 and e[i]==E
 assert e[i]-e[i-11]==55*M
 p=slice(i-11,i-5);j=slice(i-5,i+1)
 if max(h[j])>max(h[p]):return 'KNOWN_FALSE_HIGH'
 z0,z1=QUANT.get((s,E-35*M)),QUANT.get((s,E-5*M))
 if z0 is None or z1 is None:return 'UNKNOWN_REQUIRED_QUANTITY'
 if z1>z0:return 'KNOWN_FALSE_OI'
 q0,q1=sum(q[p]),sum(q[j])
 if not(np.isfinite(q0) and np.isfinite(q1) and q0>=0 and q1>=0):raise ValueError('QUOTE_SOURCE_REQUIRED')
 return 'KNOWN_TRUE_QUOTE' if q1<=q0 else 'NO_CONFIRMED_QUOTE_ORIGINAL_CVD_UNKNOWN'
