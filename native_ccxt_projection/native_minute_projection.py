"""Read-only native CCXT terminal projection. No transport, proof minting or commit."""
from causal_bars import ClosedBar
from minute_rollup import roll,MIN
from dynamic_minute_span import valid_minute_span

FIVE=5*MIN
QUARTER=15*MIN

def project(end,minute_rows,received,now,prefix_five,prefix_quarter):
    if not (type(end) is type(received) is type(now) is int
            and end%QUARTER==0 and end<=received<=now<end+MIN):
        return None
    try:
        if type(minute_rows) is not tuple or not valid_minute_span(len(minute_rows)):
            return None
        if [r[0] for r in minute_rows]!=list(range(end-len(minute_rows)*MIN,end,MIN)):
            return None
        def bar(row,t):
            return ClosedBar(row[0],t,*(float(row[i]) for i in (1,2,3,4,5,7)))
        minute=tuple(bar(r,received) for r in minute_rows)
        if any(r[6]!=r[0]+MIN-1 or not b.valid(MIN) for r,b in zip(minute_rows,minute,strict=True)):
            return None
        result=[(MIN,minute[-30:])]  # reuse longer source to fill missing 5m, do not rewrite earlier 1m
        for width,prefix in ((FIVE,prefix_five),(QUARTER,prefix_quarter)):
            anchor=end-QUARTER
            if (type(prefix) is not tuple or (width==QUARTER and not prefix)
                or any(type(b) is not ClosedBar or not b.valid(width) or b.received_ms>now for b in prefix)
                or tuple(b.open_ms for b in prefix)!=tuple(range(anchor-len(prefix)*width,anchor,width))):
                return None
            suffix=tuple(bar(roll(minute_rows,t,width),received) for t in range(anchor,end,width))
            if not all(b.valid(width) for b in suffix):return None
            rows=prefix+suffix
            if len(rows)>260 or width==QUARTER and len(rows)!=260:return None
            result.append((width,rows))
        if len({rows[-1].close for _,rows in result})!=1:return None
        return tuple(result)
    except (AttributeError,ValueError,TypeError,IndexError,ArithmeticError):return None
