"""Bounded copy of a verified original async public response, without another request."""
import json
from threading import Lock
from urllib.parse import parse_qs,urlsplit

LIMIT=350_000
MAX_SYMBOLS=128

def query_for_history(url):
    u=urlsplit(str(url))
    if (u.scheme,u.netloc,u.path,u.fragment)!=('https','fapi.binance.com','/fapi/v1/klines',''):
        return None
    vals=parse_qs(u.query,keep_blank_values=True)
    if set(vals)-{'symbol','interval','limit','startTime','endTime'} or any(len(v)!=1 for v in vals.values()):
        return None
    if vals.get('interval')!=['15m'] or 'limit' not in vals or 'symbol' not in vals:return None
    symbol=vals['symbol'][0]
    count=vals['limit'][0]
    if not symbol.endswith('USDT') or not 4<len(symbol)<=64 or not count.isascii() or not count.isdigit():
        return None
    if not 259<=int(count)<=499:return None
    return symbol,int(count)

class NativeRefreshMailbox:
    def __init__(self):
        self._lock=Lock()
        self._rows={}
        self._window=None

    async def capture_verified(self,method,url,response,received):
        if method!='GET' or not callable(getattr(response,'read',None)):return
        q=query_for_history(url)
        if q is None or type(received) is not int or received%900000>=60_000:return
        symbol,limit=q
        try:
            body=await response.read()  # aiohttp caches exactly these bytes for the original CCXT consumer
            if type(body) is not bytes or len(body)>LIMIT:return
            rows=json.loads(body)
            if type(rows) is not list or not 1<=len(rows)<=limit:return
            if any(type(row) is not list or len(row)!=12 or any(type(x) not in (int,str) for x in row) for row in rows):
                return
            value=(received,tuple(map(tuple,rows)))
        except Exception:return  # leave the same error to the original CCXT consumer
        e=received//900000*900000
        if not self._lock.acquire(blocking=False):return
        try:
            if self._window is not None and e<self._window:return
            if self._window!=e:
                self._window=e
                self._rows.clear()
            if len(self._rows)<MAX_SYMBOLS and symbol not in self._rows:
                self._rows[symbol]=value
        finally:self._lock.release()

    def peek(self,symbol,end):
        if not self._lock.acquire(blocking=False):return None
        try:return self._rows.get(symbol) if self._window==end else None
        finally:self._lock.release()
