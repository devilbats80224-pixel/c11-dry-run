"""Original CCXT data-only background slot for a missed reviewed OPG quarter."""
from dataclasses import replace
from decimal import Decimal,InvalidOperation,localcontext
from secrets import token_hex
from threading import get_ident
from causal_bars import ClosedBar,FIVE,QUARTER
from dual_history_pages import Page
from reviewed_opg_selector import SYMBOL,PAIR,ReviewedOpgSource,enc,digest
from bridge_protocol import Refused

# Bounded 2 requests (one on each independent native lane) per plan.
# At most four contiguous settled quarters per plan; if more are missing then
# continue on fair later cycles, never certify the unhydrated suffix.
QUARTERS_PER_PLAN=4

def need(ok, why):
    if not ok:raise Refused(why)

class RecoveryPage(Page):
    def valid(self, now):
        return (self.symbol==SYMBOL and self.stop<=self.end
                and 0<self.stop-self.start<=QUARTERS_PER_PLAN*QUARTER
                and Page.valid(replace(self,end=self.end+QUARTER),now+QUARTER))

def row_data(raw,page,sent,received,now):
    need(type(page) is RecoveryPage and page.valid(sent) and page.valid(received)
         and page.valid(now) and page.stop<=sent<=received<=now and received-sent<=1200
         and type(raw) in (list,tuple) and len(raw)==page.n,'OPG_RECOVERY_PAGE_OR_TIME')
    converted=[]
    try:
        for idx,r in enumerate(raw):
            need(type(r) in (list,tuple) and len(r)==12
                and type(r[0]) is type(r[6]) is int and r[0]==page.start+(idx*page.width)
                and r[6]==r[0]+page.width-1 and all(type(r[k]) is str and 0<len(r[k])<=40
                    for k in (1,2,3,4,5,7)),'OPG_RECOVERY_CANDLE_LAYOUT')
            values=tuple(Decimal(r[k]) for k in (1,2,3,4,5,7))
            need(all(x.is_finite() and abs(x.adjusted())<=64 for x in values),'OPG_RECOVERY_CANDLE_RANGE')
            o,h,l,c,v,q=values
            need(0<l<=min(o,c)<=max(o,c)<=h and v>=0 and q>=0,'OPG_RECOVERY_CANDLE_VALUES')
            closed=ClosedBar(r[0],received,*(float(x) for x in values))
            need(closed.valid(page.width),'OPG_RECOVERY_NATIVE_CANDLE')
            converted.append((closed,values))
    except (InvalidOperation,OverflowError,TypeError,ValueError) as e:
        raise Refused('OPG_RECOVERY_CANDLE_VALUES') from e
    return tuple(converted)

def agree(five,quarter):
    if len(five)!=3*len(quarter):return False
    with localcontext() as context:
        context.prec=256
        for i,(bar,q) in enumerate(quarter):
            parts=five[3*i:3*i+3]
            if tuple(x[0].open_ms for x in parts)!=tuple(range(bar.open_ms,bar.open_ms+QUARTER,FIVE)):return False
            x=[t for _,t in parts]
            if (x[0][0],max(t[1] for t in x),min(t[2] for t in x),x[-1][3],
                sum((t[4] for t in x),Decimal(0)),sum((t[5] for t in x),Decimal(0)))!=q:return False
    return True

class SettledOpgRecovery:
    def __init__(self,selector,wall):
        need(type(selector.source) is ReviewedOpgSource,'OPG_RECOVERY_APPROVED_SELECTOR')
        self.selector,self.wall,self.owner=selector,wall,get_ident()
        self.part=None
        self.terminal=None

    def key(self,f):
        return f.pair,f.symbol,f.generation,f.boundary,f.origin,f.expires_ns

    def reset_if(self,current):
        if self.part is not None and current(self.part[0]) is not True:self.part=None

    def offer(self,f,width,current):
        at=self.wall(); src=self.selector.source
        if ((f.pair,f.symbol,f.origin)!=(PAIR,SYMBOL,src.prior.origin)
          or get_ident()!=self.owner or not src.manifest['reviewed_available_ms']<=at<src.manifest['use_until_ms']
          or current(f) is not True or not 65000<=at-f.boundary<840000
          or self.terminal==self.key(f)):
            return None
        base=self.selector._prior(f.boundary+QUARTER,at)
        if base.cursor>=f.boundary:return None
        if width==FIVE:
            if self.part is not None:return None
        elif self.part is None or self.part[0]!=f or self.part[1].pin!=base.pin:return None
        j=RecoveryPage(SYMBOL,f.generation,f.boundary,base.cursor,
                    min(f.boundary,base.cursor+QUARTERS_PER_PLAN*QUARTER),token_hex(16),width,f.origin)
        return (j,base) if j.valid(at) else None

    def consume(self,f,j,base,response,current):
        at=self.wall()
        def due():
            now=self.wall()
            return (get_ident()==self.owner and self.selector.source.manifest['reviewed_available_ms']<=now
                   <self.selector.source.manifest['use_until_ms'] and j.valid(now)
                   and current(f) is True and (j.symbol,j.generation,j.end,j.origin)==
                       (f.symbol,f.generation,f.boundary,f.origin))
        if response is None or not due():
            self.part=None
            return False
        # Parent only receives a response tuple whose exact page was charged,
        # source/TLS checked by the original confined native CCXT child.
        sent,received,raw=response
        rows=row_data(raw,j,sent,received,at)
        pin=digest(enc(('C11_ORIGINAL_CCXT_DATA_ONLY',j.child(),j.generation,j.end,j.ticket,sent,received,raw)))
        if j.width==FIVE:
            self.part=f,base,j,rows,pin,sent,received
            return False
        part=self.part
        self.part=None
        self.terminal=self.key(f)
        need(part is not None and part[0]==f and part[1].pin==base.pin
            and (part[2].start,part[2].stop)==(j.start,j.stop) and part[6]<=sent
            and part[2].ticket!=j.ticket and part[4]!=pin,'OPG_RECOVERY_FALSE_PAIR')
        need(agree(part[3],rows),'OPG_RECOVERY_CROSS_WIDTH')
        receipts=digest(enc((part[4],pin)))
        return self.selector.append_settled_data(base,j.stop,j.generation,receipts,
                    {FIVE:tuple(q for q,_ in part[3]),QUARTER:tuple(q for q,_ in rows)},due)
