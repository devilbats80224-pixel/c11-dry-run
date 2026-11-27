"""Atomic same-source closed historical bars; call only from the original owner."""
from causal_bars import ClosedBar,FIVE,QUARTER
from capture import sanitize
from minute_rollup import MIN,roll

def commit(store,symbol,origin,start,stop,rows,sent,received,authority):
    if (any(type(x) is not int for x in (origin,start,stop,sent,received))
        or not 0<origin<=start<stop<=sent<=received
        or start%QUARTER or stop%QUARTER or stop-start>495*MIN
        or authority() is not True or store.db.in_transaction):
        raise ValueError('C11_CLOSED_MINUTE_PAGE_SCOPE')
    raw=sanitize(rows,'1m',stop,(stop-start)//MIN,sent)
    aggregates={}
    for width in (FIVE,QUARTER):
        width_rows=[]
        for t in range(start,stop,width):
            i=(t-start)//MIN
            r=roll(raw[i:i+width//MIN],t,width)
            if r is None:raise ValueError('C11_CLOSED_MINUTE_PAGE_GAP')
            b=ClosedBar(t,received,*map(lambda j:float(r[j]),(1,2,3,4,5,7)))
            if not b.valid(width):raise ValueError('C11_CLOSED_MINUTE_PAGE_BAR')
            width_rows.append(b)
        aggregates[width]=tuple(width_rows)
    db=store.db
    written=[]
    with db:
        db.execute('BEGIN IMMEDIATE')
        if authority() is not True:raise ValueError('C11_CLOSED_MINUTE_PAGE_EXPIRED')
        p=store.segment(symbol)
        if p is None or p.open_ms!=origin or p.proven_at_ms>received:
            raise ValueError('C11_CLOSED_MINUTE_PAGE_SEGMENT')
        for width,values in aggregates.items():
            before=db.total_changes
            store._record_checked_rows(symbol,width,values)
            written.append(db.total_changes-before)
        if authority() is not True:raise ValueError('C11_CLOSED_MINUTE_PAGE_EXPIRED')
    return tuple(written)
