"""Isolated C11 historical data lanes; no entry scope or routing."""
from exact_symbol import quote_contract_shape
from dataclasses import dataclass
from decimal import Decimal,InvalidOperation
from causal_bars import FIVE,QUARTER,ClosedBar

W={FIVE:'5m',QUARTER:'15m'}
@dataclass(frozen=True)
class Page:
    symbol:str; generation:int; end:int; start:int; stop:int; ticket:str; width:int; origin:int
    @property
    def native_task(self):return None
    @property
    def n(self):return (self.stop-self.start)//self.width
    @property
    def weight(self):return 2 if self.n>=100 else 1
    def valid(self,now):
        return (self.width in W and self.end%QUARTER==0 and self.origin%self.width==self.start%self.width==self.stop%self.width==0
           and 0<self.origin<=self.start<self.stop<=self.end-QUARTER and 1<=self.n<=499 and 65000<=now-self.end<840000
           and quote_contract_shape(self.symbol))
    def child(self):
        return dict(symbol=self.symbol,start=self.start,stop=self.stop,count=self.n,width=self.width)

def lanes(facts,last):
    valid={(f.symbol,width):(f,width) for f in facts for width in W if f.origin%width==0}
    return [valid[k] for k in sorted(valid,key=lambda k:(last is not None and k<=last,k))]

def make_page(f,width,start,end_now,ticket):
    end=f.boundary-QUARTER
    page=Page(f.symbol,f.generation,f.boundary,start,min(end,start+499*width),ticket,width,f.origin)
    return page if page.valid(end_now) else None

def persist(store,page,raw,sent,received,still_current):
    if not page.valid(sent) or not page.valid(received) or received<sent or len(raw)!=page.n:
        raise ValueError('C11_DUAL_HISTORY_PAGE_CLOCK_OR_SIZE')
    rows=[]
    for i,r in enumerate(raw):
        if type(r) not in (list,tuple) or len(r)!=12 or type(r[0]) is not int or type(r[6]) is not int:
            raise ValueError('C11_DUAL_HISTORY_SHAPE')
        op=page.start+i*page.width
        if (r[0],r[6])!=(op,op+page.width-1):raise ValueError('C11_DUAL_HISTORY_GRID')
        try:vals=tuple(float(Decimal(r[k])) for k in (1,2,3,4,5,7))
        except (InvalidOperation,TypeError,ValueError,OverflowError) as ex:
            raise ValueError('C11_DUAL_HISTORY_CANDLE') from ex
        row=ClosedBar(op,received,*vals)
        if not row.valid(page.width):raise ValueError('C11_DUAL_HISTORY_CANDLE')
        rows.append(row)
    if not still_current():raise ValueError('C11_DUAL_HISTORY_STALE')
    with store.db:
        store.db.execute('BEGIN IMMEDIATE')
        if not still_current():raise ValueError('C11_DUAL_HISTORY_STALE')
        row=store.db.execute('SELECT open_ms FROM segment WHERE symbol=?',(page.symbol,)).fetchone()
        if row is None and page.start==page.origin:
            store.db.execute('INSERT INTO segment VALUES(?,?,?)',(page.symbol,page.origin,received))
        elif row!=(page.origin,):
            raise ValueError('C11_DUAL_HISTORY_GENESIS_CHANGED')
        # No fabricated counterpart: only compare when an independent
        # canonical terminal peer is already known at this actual receive.
        for item in rows:
            e=item.open_ms+page.width
            if e%QUARTER:continue
            width=FIVE if page.width==QUARTER else QUARTER
            peer=store.db.execute('SELECT close FROM candle WHERE symbol=? AND width=? AND open_ms=? AND received_ms<=?',
              (page.symbol,width,e-width,received)).fetchone()
            if peer is not None and peer[0]!=item.close:
                raise ValueError('C11_DUAL_HISTORY_NATIVE_CLOSE_CONFLICT')
        store._record_checked_rows(page.symbol,page.width,rows)
        if not still_current():raise ValueError('C11_DUAL_HISTORY_STALE')
    return len(rows)
