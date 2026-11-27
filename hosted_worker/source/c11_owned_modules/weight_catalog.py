"""Official Binance USDⓈ-M public route doses, not CCXT transport nominal costs."""
from dataclasses import dataclass
from urllib.parse import urlsplit,parse_qsl
import re
from exact_symbol import ID_CHAR
SYM=re.compile(ID_CHAR[:-1]+r'_]{1,40}')
INTERVALS=frozenset('1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M'.split())
LEGAL_DEPTH={5:2,10:2,20:2,50:2,100:5,500:10,1000:20}
KLINES={'/fapi/v1/klines':'symbol','/fapi/v1/markPriceKlines':'symbol','/fapi/v1/indexPriceKlines':'pair'}
TICKERS={'/fapi/v1/ticker/price':(1,2),'/fapi/v2/ticker/price':(1,2),'/fapi/v1/ticker/bookTicker':(2,5),
         '/fapi/v1/ticker/24hr':(1,40),'/fapi/v1/premiumIndex':(1,10)}
IGNORE_HEADER={'/fapi/v2/ticker/price','/fapi/v1/ticker/bookTicker','/fapi/v1/fundingInfo','/fapi/v1/fundingRate'}

@dataclass(frozen=True)
class Dose:
    weight_1m:int|None
    funding_requests_5m:int=0
    policy:str='ATTEST_1M_HEADER'

UNKNOWN=Dose(None,0,'IGNORE_1M_HEADER')

def positive(q,k,default,maxval):
    v=q.get(k,str(default))
    if not re.fullmatch('[1-9][0-9]{0,18}',v):return None
    n=int(v);return n if n<=maxval else None

def clocks(q,keys=('startTime','endTime')):
    for k in keys:
        if k in q and not re.fullmatch('(?:0|[1-9][0-9]{0,18})',q[k]):return False
    return not ('startTime' in q and 'endTime' in q and int(q['endTime'])<int(q['startTime']))

def classify_official(url):
    try:
        u=urlsplit(str(url))
        if (u.scheme,u.netloc,u.fragment)!=('https','fapi.binance.com',''):return UNKNOWN
        pairs=parse_qsl(u.query,strict_parsing=True,keep_blank_values=True)
    except (TypeError,ValueError):return UNKNOWN
    q=dict(pairs);p=u.path
    if len(pairs)!=len(q):return UNKNOWN
    policy='IGNORE_1M_HEADER' if p in IGNORE_HEADER else 'ATTEST_1M_HEADER'
    mk=lambda n,f=0:Dose(n,f,policy)
    if p in ('/fapi/v1/exchangeInfo','/fapi/v1/time','/fapi/v1/fundingInfo'):
        if q:return UNKNOWN
        return mk(0,1) if p.endswith('/fundingInfo') else mk(1)
    if p in TICKERS:
        if not q:return mk(TICKERS[p][1])
        if set(q)=={'symbol'} and SYM.fullmatch(q['symbol']):return mk(TICKERS[p][0])
        return UNKNOWN
    if p=='/fapi/v1/depth':
        if set(q)<= {'symbol','limit'} and SYM.fullmatch(q.get('symbol','')):
            return mk(LEGAL_DEPTH[positive(q,'limit',500,1000)]) if positive(q,'limit',500,1000) in LEGAL_DEPTH else UNKNOWN
        return UNKNOWN
    if p in KLINES:
        key=KLINES[p]; n=positive(q,'limit',500,1500)
        if (not set(q)<= {key,'interval','limit','startTime','endTime'} or not SYM.fullmatch(q.get(key,'')) or
              q.get('interval') not in INTERVALS or n is None or not clocks(q)):return UNKNOWN
        return mk(1 if n<100 else 2 if n<500 else 5 if n<=1000 else 10)
    if p=='/fapi/v1/aggTrades':
        if (not set(q)<= {'symbol','startTime','endTime','fromId','limit'} or not SYM.fullmatch(q.get('symbol','')) or
            not clocks(q,('startTime','endTime','fromId')) or positive(q,'limit',500,1000) is None or
            ('fromId' in q and ('startTime' in q or 'endTime' in q))):return UNKNOWN
        if 'startTime' in q and 'endTime' in q and int(q['endTime'])-int(q['startTime'])>=3600_000:return UNKNOWN
        return mk(20)
    if p=='/fapi/v1/fundingRate':
        if (not set(q)<= {'symbol','startTime','endTime','limit'} or ('symbol'in q and not SYM.fullmatch(q['symbol'])) or
            not clocks(q) or positive(q,'limit',100,1000) is None):return UNKNOWN
        # Binance only specifies the shared 500/5m/IP pool for this route,
        # unlike fundingInfo which explicitly also says 0. Do not assume 0/1.
        return mk(None,1)
    return UNKNOWN
