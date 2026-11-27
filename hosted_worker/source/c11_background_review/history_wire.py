"""Distinct public history context. Cannot yield hot queue/Capture receipts."""
from dataclasses import dataclass
from capture import source_request,sanitize
from native_shadow_queue import headers_copy,weight,source_status
F=300000; Q=900000
@dataclass(frozen=True)
class Page:
    symbol:str; generation:int; end:int; start:int; stop:int; ticket:str
    @property
    def native_task(self):return None
    @property
    def n(self):return (self.stop-self.start)//F
    @property
    def weight(self):return 2 if self.n>=100 else 1
    def valid(self,now):
        return self.end%Q==0 and self.start%F==self.stop%F==0 and 1<=self.n<=499 and self.stop<=self.end-Q and 65000<=now-self.end<840000 and self.symbol.isascii() and self.symbol.isalnum() and self.symbol.endswith('USDT')
    def query(self):
        return dict(symbol=self.symbol,interval='5m',startTime=str(self.start),endTime=str(self.stop-1),limit=str(self.n))
@dataclass(frozen=True)
class Receipt:
    page:Page; requested:int; received:int; status:int; used:int|None; rows:tuple
class Context:
    def __init__(self,page,wall,authority):
        self.page,self.wall,self.authority=page,wall,authority;self.used=False;self.status=None
    def check_wire(self,req,kw):
        n=self.wall()
        if self.used or not self.page.valid(n) or self.authority(self.page,n) is not True:raise ValueError('BACKGROUND_PREWIRE_FENCE')
        source_request(req.method,req.url,req.headers,req.body,kw.get('verify'),self.page.query());self.used=True
    def check_reply(self,req,r):
        if r.url!=req.url or r.history:raise ValueError('BACKGROUND_REDIRECT_REFUSED')
        self.status=r.status_code;return r
def run(transport,p,claim,authority):
    ctx=Context(p,transport.wall,authority)
    a=transport.history_for_physical_task.set(ctx);b=transport.claim.set(claim)
    requested=transport.wall();transport.client.last_response_headers=None
    try:
        try: raw=transport.client.fapiPublicGetKlines({**p.query(),'startTime':p.start,'endTime':p.stop-1,'limit':p.n})
        except Exception as e:return Receipt(p,requested,transport.wall(),ctx.status or source_status(e),None,())
        now=transport.wall(); used=weight(headers_copy(transport.client.last_response_headers))
        if ctx.status!=200 or type(used) is not int or not p.valid(now) or authority(p,now) is not True:
            return Receipt(p,requested,now,ctx.status or 0,used,())
        if p.n==2 and type(raw) is list and len(raw)==2:
            rows=sanitize(raw[:1],'5m',p.stop-F,1,requested)+sanitize(raw[1:],'5m',p.stop,1,requested)
        else:
            rows=sanitize(raw,'5m',p.stop,p.n,requested)
        return Receipt(p,requested,now,200,used,tuple(tuple(r) for r in rows))
    finally:
        transport.history_for_physical_task.reset(a);transport.claim.reset(b)
