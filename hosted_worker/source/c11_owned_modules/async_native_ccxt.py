"""Isolated prototype: one CCXT 4.5.90 async client, per-ticket metadata.
It grants no authorization or budget; the original owner supplies before_wire.
"""
from contextvars import ContextVar
from contextlib import asynccontextmanager
from dataclasses import dataclass
from time import time_ns
from typing import Callable
import aiohttp
from ccxt import __version__
from ccxt.async_support import binance
from capture import sanitize,source_request

BODY_CAP=512*1024
context=ContextVar('c11_kline_request',default=None)

@dataclass
class Ticket:
    expected:dict
    before_wire:Callable
    entered:bool=False
    sent:int|None=None
    received:int|None=None
    status:int|None=None
    weight:int|None=None

class CandleSession:
    headers={}  # no ambient header inheritance

    def __init__(self,session):
        self.session=session

    @asynccontextmanager
    async def get(self,url,**kw):
        t=context.get()
        headers=kw.get('headers') or {}
        if t is None or t.entered or kw.get('proxy') is not None:
            raise ValueError('C11_ASYNC_UNEXPECTED_HTTP')
        source_request('GET',str(url),headers,kw.get('data'),True,t.expected)
        if t.before_wire() is not True:
            raise ValueError('C11_ASYNC_STALE_BEFORE_WIRE')
        t.entered=True;t.sent=time_ns()//1_000_000
        async with self.session.get(url,allow_redirects=False,**kw) as response:
            if response.history or response.url!=url:raise ValueError('C11_ASYNC_REDIRECT_REFUSED')
            t.status=response.status
            w=response.headers.get('x-mbx-used-weight-1m')
            if w is not None and w.isascii() and w.isdecimal() and len(w)<=5:
                t.weight=int(w)
            length=response.content_length
            if type(length) is int and length>BODY_CAP:raise ValueError('C11_ASYNC_BODY_CAP')
            body=bytearray()
            async for part in response.content.iter_chunked(65536):
                body.extend(part)
                if len(body)>BODY_CAP:raise ValueError('C11_ASYNC_BODY_CAP')
            response._body=bytes(body)
            t.received=time_ns()//1_000_000
            yield response

class SharedNativeAsync:
    def __init__(self,session):
        if __version__!='4.5.90':raise ValueError('C11_ASYNC_CCXT_VERSION')
        self.exchange=binance(dict(session=CandleSession(session),enableRateLimit=False,timeout=900,
          enableLastHttpResponse=False,enableLastResponseHeaders=False,enableLastJsonResponse=False,
          returnResponseHeaders=False,options={'maxRetriesOnFailure':0}))
        if any(getattr(self.exchange,k,None) for k in ('apiKey','secret','password','uid','accountId','walletAddress','privateKey')):
            raise ValueError('C11_ASYNC_UNSIGNED_ONLY')

    async def page(self,params,before_wire,*,bootstrap=False):
        if (set(params)!={'symbol','interval','startTime','endTime','limit'}
            or params['interval']!='1m' or type(params['limit']) is not int
            or type(bootstrap) is not bool
            or not (1<=params['limit']<=495 if bootstrap else
                    15<=params['limit']<=495 and params['limit']%15==0)
            or (bootstrap and (any(type(params[k]) is not int for k in ('startTime','endTime'))
                   or params['startTime']%60000 or (params['endTime']+1)%60000))
            or params['endTime']+1-params['startTime']!=params['limit']*60000):
            raise ValueError('C11_ASYNC_PAGE_SHAPE')
        stamp=Ticket({k:str(v) for k,v in params.items()},before_wire)
        reset=context.set(stamp)
        try:
            try:
                rows=await self.exchange.fapiPublicGetKlines(params)
                if not (stamp.entered and stamp.status==200 and type(stamp.weight) is int and 0<=stamp.weight<=50000):
                    return stamp,()
                rows=sanitize(rows,'1m',params['endTime']+1,params['limit'],stamp.sent,bootstrap=bootstrap)
            except (ValueError,TypeError,ArithmeticError,aiohttp.ClientError):
                return stamp,()
            except Exception as err:
                from ccxt.base.errors import BaseError
                if not isinstance(err,BaseError):raise
                return stamp,()
            return stamp,tuple(rows)
        finally:
            context.reset(reset)

    async def close(self):
        await self.exchange.close()
