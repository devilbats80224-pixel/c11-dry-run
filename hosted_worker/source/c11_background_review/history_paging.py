"""Historical-only page core. No transport, decision, or order side effects."""
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Callable

FIVE=300_000
QUARTER=900_000
PAGE=499

@dataclass(frozen=True)
class Page:
    symbol: str
    origin: int
    start: int
    stop: int

    @property
    def count(self):
        return (self.stop-self.start)//FIVE

    def request(self):
        return dict(symbol=self.symbol,interval='5m',startTime=self.start,endTime=self.stop-1,limit=self.count)

def plan(symbol, launch, checkpoint_next_open, boundary, now):
    """Leave the last completed quarter to the existing fresh-candle path."""
    if (not isinstance(symbol,str) or not symbol or type(launch) is not int
            or type(boundary) is not int or type(now) is not int
            or boundary%QUARTER or boundary>now or launch<=0 or launch%FIVE):
        return None
    floor=boundary-QUARTER
    start=launch if checkpoint_next_open is None else checkpoint_next_open
    if type(start) is not int or start<launch or start>=floor or start%FIVE:
        return None
    return Page(symbol,launch,start,min(floor,start+PAGE*FIVE))

def bind_rows(page, raw, requested, received, make_bar: Callable):
    if (type(page) is not Page or type(requested) is not int or type(received) is not int
            or not 1<=page.count<=PAGE or not page.stop<=requested<=received
            or type(raw) not in (list,tuple) or len(raw)!=page.count):
        raise ValueError('C11_HISTORY_PAGE_INCOMPLETE')
    result=[]
    for i,row in enumerate(raw):
        opening=page.start+i*FIVE
        if (type(row) not in (list,tuple) or len(row)!=12
                or type(row[0]) is not int or type(row[6]) is not int
                or row[0]!=opening or row[6]!=opening+FIVE-1):
            raise ValueError('C11_HISTORY_PAGE_GRID')
        try:
            vals=tuple(Decimal(str(row[j])) for j in (1,2,3,4,5,7))
            if not all(v.is_finite() for v in vals):raise ValueError
        except (ValueError,InvalidOperation):
            raise ValueError('C11_HISTORY_PAGE_DECIMAL') from None
        bar=make_bar(opening,received,*map(float,vals))
        if not bar.valid(FIVE):raise ValueError('C11_HISTORY_PAGE_PRICE')
        result.append(bar)
    return tuple(result)

def persist_checked(store, page, rows, clock_and_budget_valid):
    """Use the original single SQLite writer/validator; no update of old rows."""
    if (not rows or len(rows)!=page.count
            or page.start+len(rows)*FIVE!=page.stop
            or any(r.open_ms!=page.start+i*FIVE for i,r in enumerate(rows))
            or not clock_and_budget_valid()):
        raise ValueError('C11_HISTORY_PAGE_STALE')
    with store.db:
        store.db.execute('BEGIN IMMEDIATE')
        if not clock_and_budget_valid():raise ValueError('C11_HISTORY_PAGE_STALE')
        prior=store.db.execute('SELECT open_ms FROM segment WHERE symbol=?',(page.symbol,)).fetchone()
        if prior is None:
            if page.start!=page.origin:raise ValueError('C11_HISTORY_WITHOUT_GENESIS')
            store.db.execute('INSERT INTO segment VALUES (?,?,?)',(page.symbol,page.origin,rows[0].received_ms))
        elif prior[0]!=page.origin:
            raise ValueError('C11_HISTORY_GENESIS_CHANGED')
        store._record_checked_rows(page.symbol,FIVE,rows)
        if not clock_and_budget_valid():raise ValueError('C11_HISTORY_PAGE_STALE')
