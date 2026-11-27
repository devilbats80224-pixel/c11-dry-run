"""Recover at most three older 5m rows already covered by the native 30×1m receipt."""
from dataclasses import dataclass
from causal_bars import ClosedBar,FIVE,QUARTER,MINUTE
from minute_rollup import roll
from dynamic_minute_span import select_minute_span
from warm_store_prefix import warm_prefix

@dataclass(frozen=True)
class PreviousFivePlan:
    end:int
    items:tuple
    quarter:tuple

def local_previous_five_plan(store,symbol,start,end,as_of):
    if (type(start) is not int or start>=end-QUARTER or not 3<=(end-start)//FIVE<=260
        or start%FIVE):
        return None
    q=warm_prefix(store,symbol,end-QUARTER,end,as_of)
    if q is None:return None
    items=[]
    expected=range(start,end-QUARTER,FIVE)
    raw=store.db.execute('SELECT open_ms,received_ms,op,high,low,close,volume,quote FROM candle '
                         'WHERE symbol=? AND width=? AND open_ms>=? AND open_ms<? AND received_ms<=?',
                         (symbol,FIVE,start,end-QUARTER,as_of))
    known={r[0]:ClosedBar(*r) for r in raw}
    if any(k not in expected or not p.valid(FIVE) for k,p in known.items()):return None
    items=tuple((t,known.get(t)) for t in expected)
    if select_minute_span(end,items) is None:return None
    return PreviousFivePlan(end,tuple(items),q[1])

def fill_previous_five(plan,end,raw,received):
    if type(plan) is not PreviousFivePlan or plan.end!=end:return None
    span=select_minute_span(end,plan.items)
    if span is None or len(raw)!=span or tuple(r[0] for r in raw)!=tuple(range(end-span*MINUTE,end,MINUTE)):return None
    five=[]
    try:
        for at,known in plan.items:
            if known is not None:
                five.append(known)
            else:
                x=roll(raw,at,FIVE)
                row=ClosedBar(at,received,*(float(x[i]) for i in (1,2,3,4,5,7)))
                if not row.valid(FIVE):return None
                five.append(row)
    except (TypeError,ValueError,IndexError,ArithmeticError):return None
    return tuple(five),plan.quarter
