"""Verify physical same-end tuple arithmetic only. Never yields synthetic candles."""
from decimal import Decimal, InvalidOperation, localcontext

ONE=60_000
FIVE=300_000
QUARTER=900_000
FIELDS=(1,2,3,4,5,7)
OK='NORM_XTF_EXACT'
COVERAGE='NORM_XTF_COVERAGE'
OHLC='NORM_XTF_OHLC'
VOLUME='NORM_XTF_BASE_VOLUME'
QUOTE='NORM_XTF_QUOTE_UNVERIFIED'

def verify_terminal(end,minutes,fives,quarters):
    """Only latest 15m intersection; exact source timestamps, no gap fill."""
    if type(end) is not int or end%QUARTER:return COVERAGE
    with localcontext() as ctx:
        ctx.prec=128  # six finite sanitized decimal strings, <=40 chars each
        def tail(raw,n,width):
            if len(raw)<n:return None
            result=[]
            for i,r in enumerate(raw[-n:]):
                opened=end-(n-i)*width
                if len(r)!=12 or type(r[0]) is not int or type(r[6]) is not int or (r[0],r[6])!=(opened,opened+width-1):
                    return None
                if any(type(r[k]) is not str or len(r[k])>40 for k in FIELDS):return None
                vals=tuple(Decimal(r[k]) for k in FIELDS)
                if not all(v.is_finite() for v in vals):return None
                o,h,l,c,v,q=vals
                if not 0<l<=min(o,c)<=max(o,c)<=h or v<0 or q<0:return None
                result.append(vals)
            return result
        def compare(part,large):
            m=(part[0][0],max(x[1] for x in part),min(x[2] for x in part),part[-1][3])
            if m!=large[:4]:return OHLC
            if sum((x[4] for x in part),Decimal(0))!=large[4]:return VOLUME
            if sum((x[5] for x in part),Decimal(0))!=large[5]:return QUOTE
            return OK
        try:
            m=tail(minutes,15,ONE); f=tail(fives,3,FIVE); q=tail(quarters,1,QUARTER)
            if m is None or f is None or q is None:return COVERAGE
            for group, large in enumerate(f):
                r=compare(m[group*5:(group+1)*5],large)
                if r!=OK:return r
            return compare(m,q[0])
        except (InvalidOperation,IndexError,TypeError,ValueError):return COVERAGE
