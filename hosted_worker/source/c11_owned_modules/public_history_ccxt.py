"""Actual unsigned public route implementation; no script CLI opens a network.
"""
from exact_symbol import quote_contract_shape
from dataclasses import dataclass
import time
from urllib.parse import parse_qs,urlsplit
from ccxt import binance
from installed_build_inputs import KlineOnlySession
from native_shadow_queue import headers_copy,weight,source_status
from capture import source_request,sanitize

@dataclass(frozen=True)
class ClosedPage:
    symbol:str; start:int; stop:int; count:int; width:int=300000
    def __post_init__(self):
        if (not quote_contract_shape(self.symbol)
            or len(self.symbol)>32 or any(type(x) is not int for x in (self.start,self.stop,self.count,self.width))
            or self.width not in (60000,300000,900000)
            or self.start<=0 or self.stop<=self.start or self.start%self.width or self.stop%self.width
            or not 1<=self.count<=499 or self.stop-self.start!=self.count*self.width
            or (self.width==60000 and (self.start%900000 or self.stop%900000 or self.count>495))):
            raise ValueError('HISTORY_PUBLIC_CLOSED_PAGE')
    def params(self):
        return dict(symbol=self.symbol,interval={60000:'1m',300000:'5m',900000:'15m'}[self.width],startTime=self.start,endTime=self.stop-1,limit=self.count)

@dataclass(frozen=True)
class PublicReply:
    status:int; used:int|None; rows:tuple

def installed_unsigned():
    session=KlineOnlySession()
    client=binance(dict(session=session,enableRateLimit=False,options={'maxRetriesOnFailure':0}))
    if any(getattr(client,k,None) for k in ('apiKey','secret','password','uid','accountId','walletAddress','privateKey')):
        raise ValueError('HISTORY_UNSIGNED_REQUIRED')
    return client

def checked_public_one(client,page,claim):
    """One prepared request. Caller implements parent handshake authority."""
    if type(client) is not binance or type(client.session) is not KlineOnlySession or type(page) is not ClosedPage:
        raise ValueError('HISTORY_NATIVE_CLIENT_PAGE_ONLY')
    if any(getattr(client,k,None) for k in ('apiKey','secret','password','uid','accountId','walletAddress','privateKey')):
        raise ValueError('HISTORY_UNSIGNED_REQUIRED')
    session=client.session
    if session.trust_env or session.auth or session.proxies or session.cookies:
        raise ValueError('HISTORY_AMBIENT_TRANSPORT_REFUSED')
    entered=[]; physical=session.send; status=[]
    params=page.params()
    expected={k:str(v) for k,v in params.items()}
    def restricted(prepared,**kw):
        source_request(prepared.method,prepared.url,prepared.headers,prepared.body,kw.get('verify'),expected)
        if (entered or kw.get('allow_redirects') or kw.get('cert') or kw.get('proxies')
            or kw.get('timeout')!=(.45,.45) or claim() is not True):
            raise ValueError('HISTORY_EXACT_BEFORE_WIRE_CLAIM_REQUIRED')
        entered.append(1)
        response=physical(prepared,**{**kw,'stream':True})
        try:
            if response.url!=prepared.url or response.history:raise ValueError('HISTORY_REDIRECT_REFUSED')
            status.append(response.status_code)
            declared=response.headers.get('Content-Length')
            cap=512*1024
            if declared is not None and (not declared.isdecimal() or int(declared)>cap):
                raise ValueError('HISTORY_RESPONSE_SIZE')
            body=bytearray()
            for chunk in response.iter_content(65536):
                body.extend(chunk)
                if len(body)>cap:raise ValueError('HISTORY_RESPONSE_SIZE')
            response._content=bytes(body);response._content_consumed=True
            return response
        finally:response.close()
    session.send=restricted
    client.last_response_headers=None
    try:
        try:raw=client.fapiPublicGetKlines(params)
        except Exception as e:return PublicReply(status[0] if status else source_status(e),None,())
        n=weight(headers_copy(client.last_response_headers))
        if entered!=[1] or status!=[200] or type(n) is not int or not 0<=n<=50000:
            return PublicReply(status[0] if status else 0,None,())
        if type(raw) is not list or len(raw)!=page.count or any(type(r) is not list or len(r)!=12 for r in raw):
            return PublicReply(200,n,())
        if any(type(r[0]) is not int or r[0]!=page.start+i*page.width or
               type(r[6]) is not int or r[6]!=r[0]+page.width-1 for i,r in enumerate(raw)):
            return PublicReply(200,n,())
        if page.width==60000:
            try:sanitize(raw,'1m',page.stop,page.count,time.time_ns()//1000000)
            except (TypeError,ValueError,ArithmeticError):return PublicReply(200,n,())
        return PublicReply(200,n,tuple(tuple(r) for r in raw))
    finally:session.send=physical
