from decimal import Decimal, localcontext
MIN=60000
WIDTH={'1m':MIN,'5m':5*MIN,'15m':15*MIN}
FIELDS=('open','high','low','close','base','quote','trades','taker_base','taker_quote')
IDX=(1,2,3,4,5,7,8,9,10)
def numeric(r): return tuple(Decimal(r[i]) for i in IDX)
def roll(rows,first,width):
    n=width//MIN
    m={r[0]:r for r in rows}
    if len(m)!=len(rows): raise ValueError('DUPLICATE_MINUTE')
    if first%width:raise ValueError('OFF_GRID')
    raw=[]
    for k in range(first,first+width,MIN):
        if k not in m: return None
        r=m[k]
        if len(r)!=12 or (r[0],r[6])!=(k,k+MIN-1):raise ValueError('MINUTE_BOUNDARY')
        vals=numeric(r)
        if not all(v.is_finite() for v in vals):raise ValueError('MINUTE_NONFINITE')
        o,h,l,c,v,q,t,tb,tq=vals
        if not (0<l<=min(o,c)<=max(o,c)<=h and 0<=tb<=v and 0<=tq<=q and t>=0 and t==int(t)):raise ValueError('MINUTE_VALUE')
        raw.append(vals)
    with localcontext() as ctx:
        ctx.prec=128
        o,h,l,c=raw[0][0],max(x[1] for x in raw),min(x[2] for x in raw),raw[-1][3]
        v,q,t,tb,tq=(sum(x[i] for x in raw) for i in range(4,9))
    # Index 11 is marked Ignore; retain native value rather than manufacture it.
    return [first,str(o),str(h),str(l),str(c),str(v),first+width-1,str(q),int(t),str(tb),str(tq)]
def diff(a,b):
    assert (a[0],a[6])==(b[0],b[6])
    return [f for i,f in zip(IDX,FIELDS) if Decimal(a[i])!=Decimal(b[i])]
