"""Select only the missing, already closed prefix from original refresh rows.

Caller is responsible for exact endpoint, original receipt, own candidate,
and owner/lease checks. This helper does not fetch, write, or amend history.
"""
from causal_bars import ClosedBar,QUARTER

def fields(row):
    return (row.op,row.high,row.low,row.close,row.volume,row.quote)

def archive_prefix(store,symbol,end,received,body):
    if (type(symbol) is not str or not symbol or type(end) is not int or
         type(received) is not int or end%QUARTER or end<260*QUARTER or
         not end<=received<end+60_000 or type(body) not in (list,tuple) or
         not 1<=len(body)<=499):
        return None
    first,latest=end-260*QUARTER,end-QUARTER
    previous=None
    older=[]
    try:
        for x in body:
            if type(x) not in (list,tuple) or len(x)!=12:return None
            open_ms,close_ms=x[0],x[6]
            if (type(open_ms) is not int or open_ms%QUARTER or
                type(close_ms) is not int or close_ms!=open_ms+QUARTER-1 or
                (previous is not None and open_ms<=previous)):
                return None
            previous=open_ms
            # Never compare receipt's current terminal to the 1m-derived bar.
            if not first<=open_ms<latest:continue
            bar=ClosedBar(open_ms,received,*(float(x[i]) for i in (1,2,3,4,5,7)))
            if not bar.valid(QUARTER):return None
            older.append(bar)
    except (IndexError,ValueError,TypeError,OverflowError):return None
    db=store.db
    prior={r[0]:r[1:] for r in db.execute(
        'SELECT open_ms,op,high,low,close,volume,quote FROM candle WHERE symbol=? AND width=? AND open_ms>=? AND open_ms<?',
        (symbol,QUARTER,first,latest))}
    return (tuple(r for r in older if r.open_ms not in prior),
            tuple(r.open_ms for r in older if r.open_ms in prior and fields(r)!=prior[r.open_ms]))
