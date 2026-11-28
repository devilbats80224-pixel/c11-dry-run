"""Atomic same-source closed historical bars; call only from the original owner."""
from causal_bars import ClosedBar,FIVE,MINUTE,QUARTER
from capture import sanitize
from minute_rollup import MIN,roll

def commit(store,symbol,origin,start,stop,rows,sent,received,authority):
    if (any(type(x) is not int for x in (origin,start,stop,sent,received))
        or not 0<origin<=start<stop<=sent<=received
        or start%QUARTER or stop%QUARTER or stop-start>495*MIN
        or authority() is not True or store.db.in_transaction):
        raise ValueError('C11_CLOSED_MINUTE_PAGE_SCOPE')
    raw=sanitize(rows,'1m',stop,(stop-start)//MIN,sent)
    # Keep the verified base candles so later state recovery can reproduce
    # the aggregates. Store the base and both projections in one transaction.
    minutes=tuple(ClosedBar(r[0],received,*(float(r[i]) for i in (1,2,3,4,5,7))) for r in raw)
    aggregates={MINUTE:minutes}
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
            if width!=MINUTE:written.append(db.total_changes-before)
        if authority() is not True:raise ValueError('C11_CLOSED_MINUTE_PAGE_EXPIRED')
    return tuple(written)


def restore_retained_derivatives(store,symbol,origin,start,stop,as_of,authority):
    """Derive absent wider bars without retransmitting available base bars."""
    from closed_minute_basis import project_closed_minutes
    minutes=store.range(symbol,MINUTE,start,stop,as_of)
    if not minutes:
        return False
    if authority() is not True or store.db.in_transaction:
        raise ValueError('C11_RETAINED_MINUTE_PAGE_SCOPE')
    proof=store.segment(symbol)
    if proof is None or proof.open_ms!=origin or proof.proven_at_ms>as_of:
        raise ValueError('C11_RETAINED_MINUTE_PAGE_SCOPE')
    class SavedMinutes:
        def range(self,key,width,left,right,observed):
            if (key,width,left,right,observed)==(symbol,MINUTE,start,stop,as_of):
                return minutes
            return ()
    db=store.db
    missing={}
    for width in (FIVE,QUARTER):
        have={row[0] for row in db.execute(
            'SELECT open_ms FROM candle WHERE symbol=? AND width=? AND open_ms>=? AND open_ms<? AND received_ms<=?',
            (symbol,width,start,stop,as_of))}
        if len(have)==(stop-start)//width:
            continue
        bars=project_closed_minutes(SavedMinutes(),symbol,width,start,stop,as_of)
        if not bars:
            raise ValueError('C11_RETAINED_MINUTE_PAGE_INVALID')
        missing[width]=tuple(b for b in bars if b.open_ms not in have)
    if missing:
        with db:
            db.execute('BEGIN IMMEDIATE')
            if authority() is not True:
                raise ValueError('C11_RETAINED_MINUTE_PAGE_EXPIRED')
            for width,bars in missing.items():
                store._record_checked_rows(symbol,width,bars)
            if authority() is not True:
                raise ValueError('C11_RETAINED_MINUTE_PAGE_EXPIRED')
    return True
