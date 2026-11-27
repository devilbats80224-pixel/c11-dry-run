"""Bounded local history probe for an original, already eligible minute plan.

This does not establish checkpoint readiness, generate receipt provenance, or
write anything. The caller must already hold the same original checkout's
5m checkpoint cursor and original owner scope.
"""
from causal_bars import ClosedBar,FIVE,QUARTER

def warm_prefix(store,symbol,start,end,as_of):
    """A missing or not-yet-observed candle affects only this symbol."""
    if (type(symbol) is not str or not symbol or
            any(type(x) is not int for x in (start,end,as_of)) or
            end%QUARTER or start%FIVE or not end<=as_of<end+60_000 or
            not 3 <= (end-start)//FIVE <= 260):
        return None
    anchor=end-QUARTER
    first_q=anchor-259*QUARTER
    if first_q<0:return None
    def read(width,first):
        if first==anchor:return ()
        rows=store.range(symbol,width,first,anchor,as_of)
        if (type(rows) is not tuple or
                tuple(r.open_ms for r in rows)!=tuple(range(first,anchor,width)) or
                not all(type(r) is ClosedBar and r.valid(width) and r.received_ms<=as_of for r in rows)):
            return None
        return rows
    five=read(FIVE,start)
    if five is None:return None
    quarter=read(QUARTER,first_q)
    return (five,quarter) if quarter is not None else None
