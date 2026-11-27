"""Scratch 5m evidence plan for exact seed → current E. No network, daemon, or I/O beyond the pinned seed read."""
from exact_symbol import ID_CHAR
from dataclasses import dataclass
from decimal import Decimal
import hashlib,json,re
from pathlib import Path
M=60_000
F=5*M
Q=15*M
SYMBOL=re.compile(fr'{ID_CHAR}{{2,40}}\Z')
SHA=re.compile(r'[a-f0-9]{64}\Z')
FIELDS=('symbol','contractType','baseAsset','quoteAsset','marginAsset','onboardDate','status')
class Hold(ValueError):pass
def need(test,why):
 if not test:raise Hold(why)
@dataclass(frozen=True)
class Stamp:
 symbol:str
 generation:int
 observed_ms:int
 expires_ms:int
 ratio:str
@dataclass(frozen=True)
class TailRequest:
 symbol:str
 tf:str
 deadline:int
 request:int
 end:int
 ticket:int
 generation:int
 limit:int
 stage:int=0
 reason:str='PINNED_5M_CHECKPOINT_TAIL'
 @property
 def start(self): return self.end-self.limit*F
 @property
 def params(self):return dict(symbol=self.symbol,interval='5m',startTime=self.start,endTime=self.end-1,limit=self.limit)
 @property
 def weight(self):
    if self.limit<100:return 1
    if self.limit<500:return 2
    if self.limit<=1000:return 5
    return 10

class Scope:
 def __init__(self,window_ms,checkpoint_pins,weight_budget=144):
    need(type(window_ms) is int and window_ms>Q and window_ms%Q==0,'INVALID_WINDOW')
    need(type(weight_budget) is int and 0<weight_budget<=144,'INVALID_AGGREGATE_SCOPE')
    need(type(checkpoint_pins) is dict and all(SYMBOL.fullmatch(s) and len(pin)==2 and SHA.fullmatch(pin[1])
          for s,pin in checkpoint_pins.items()),'MISSING_OWNER_VERIFIED_CHECKPOINT_MANIFEST')
    self.end=window_ms; self.pins=checkpoint_pins; self.weight_budget=weight_budget
 def verified_checkpoint(self,symbol,meta,verified_pair,observed_asof_ms):
    require=self.pins.get(symbol)
    need(require is not None,'NO_PINNED_ORIGIN_CHECKPOINT')
    file,sha=require; b=Path(file).read_bytes()
    need(hashlib.sha256(b).hexdigest()==sha,'CHECKPOINT_SHA256_MISMATCH')
    x=json.loads(b)
    need(x['scope']=='ISOLATED_NATIVE_CHECKPOINT' and x['live_write'] is False and
         x['requires_fresh_generation_and_same_e_tail'] is True and x['symbol']==symbol,'CHECKPOINT_SCOPE_MISMATCH')
    need(type(verified_pair) is str and symbol.endswith('USDT') and verified_pair==symbol[:-4]+'/USDT:USDT','NATIVE_MARKET_CLASSIFICATION_MISMATCH')
    known=x['contract']['payload']
    need({k:meta.get(k) for k in FIELDS}==known and known['symbol']==symbol and
         known['contractType']=='PERPETUAL' and known['quoteAsset']==known['marginAsset']=='USDT' and
         known['status']=='TRADING','CURRENT_CONTRACT_IDENTITY_DRIFT')
    origin=x['origin_ms']; cursor=x['end_ms']
    need(type(origin) is type(cursor) is type(observed_asof_ms) is int and
         origin==known['onboardDate'] and origin<cursor and cursor%Q==0 and
         max(x['artifact_available_ms'],x['earliest_research_known_ms'],x['contract']['actual_received_ms'])<=observed_asof_ms,
         'CHECKPOINT_NOT_KNOWN_AS_OF_OBSERVATION')
    need(type(x['rpd_state']) is dict and x['rpd_state']['state']['next_open']==cursor and x['rpd_state']['state']['origin']==origin and
         set(x['atr'])=={'5m','15m'} and all(x['atr'][tf]['state']['clock']==cursor for tf in ('5m','15m')),
         'CHECKPOINT_CURSOR_DRIFT')
    return x
 def _deadline(self,stamp,network_now):
    need(type(stamp.generation) is int and stamp.generation>0 and
         stamp.observed_ms<=network_now<stamp.expires_ms and Decimal(stamp.ratio)>=Decimal('.20'),
         'OI20_CANDIDATE_NOT_CURRENT')
    return min(self.end+M,stamp.expires_ms)
 def plan(self,x,stamp,network_now,ticket):
    dl=self._deadline(stamp,network_now)
    need(self.end<=network_now<dl and isinstance(stamp.symbol,str) and stamp.symbol==x['symbol'] and
         type(ticket) is int and ticket>0,'WINDOW_OR_OWNER_MISMATCH')
    # Never request zero-row gaps or accept a current/future checkpoint as
    # earlier evidence. We need at least the 3 current-closed 5m candles.
    start=x['end_ms']; need(start<=self.end-Q,'CURRENT_OR_FUTURE_CHECKPOINT_NOT_ADMISSIBLE')
    n,rem=divmod(self.end-start,F)
    need(n>=3 and rem==0,'EXACT_FIVE_MINUTE_GRID')
    need(n<=1500,'CHECKPOINT_NEEDS_OFFLINE_WARMUP')
    return TailRequest(stamp.symbol,'5m',dl,network_now,self.end,ticket,stamp.generation,n)
 def issue(self,p,network_now,active_stamp,ip_hwm,ip_lim,shared_called,shared_spend,primary_reserved,slots,*,call_cap=96,quota=None):
    # Every queue and retry uses one shared actor. Caller must reserve/update
    # its shared counters transactionally before invoking CCXT.
    need(type(slots) is int and 1<=slots<=4,'NO_FREE_OWNER_SLOT')
    need(type(shared_called) is int and shared_called>=0 and
         type(shared_spend) is int and shared_spend>=0 and type(primary_reserved) is int and primary_reserved>=0,
         'SHARED_BUDGET_NOT_KNOWN')
    dl=self._deadline(active_stamp,network_now)
    need(active_stamp.symbol==p.symbol and active_stamp.generation==p.generation and
         p.request<=network_now<min(dl,p.deadline) and p.end==self.end,'STALE_DISPATCH')
    need(type(ip_hwm) is type(ip_lim) is int and 0<=ip_hwm<=ip_lim and
         shared_called+primary_reserved+1<=call_cap and shared_spend+primary_reserved+p.weight<=min(self.weight_budget,self.weight_budget if quota is None else quota) and
         ip_hwm+shared_spend+primary_reserved+p.weight+1200<=ip_lim,'SHARED_LIMIT_DEFER')
    return p.params
