"""Build-only pure in-worker projection after authoritative market commit. No I/O."""
from dataclasses import dataclass,fields,is_dataclass
from datetime import timezone
from hashlib import sha256
import json,math,re
from threading import get_ident
from primary_coverage_gate import NativePrimaryEvidence
from c11_nw15_primary_signal_port_v1.primary15 import Candle15, Signal15, classify
from c11_primary_decision_seam_v1.primary_seam import Scope
from c11_primary_decision_seam_v1.exact_rpd5_join import ClosedRpdState
from causal_bars import ClosedBar

MINUTE=60000
QUARTER=15*MINUTE
HEX=re.compile('[a-f0-9]{64}')

def pin(value):return type(value) is str and HEX.fullmatch(value) is not None
def require(cond,code):
 if not cond:raise ValueError(code)
def encode(value):
 """Explicit typed canonical serialization. Objects must be frozen and acyclic."""
 if type(value) in (str,int,bool) or value is None:return value
 if type(value) is float:
  require(math.isfinite(value),'NONFINITE_FRAME_VALUE')
  return {'ieee754_hex':value.hex()}
 if type(value) is tuple:return [encode(v) for v in value]
 if is_dataclass(value) and not isinstance(value,type) and type(value).__dataclass_params__.frozen:
  return {'shape':type(value).__name__,'fields':[(x.name,encode(getattr(value,x.name))) for x in fields(value)]}
 raise TypeError('FROZEN_NATIVE_INPUT_REQUIRED')
def canonical(value):
 return sha256(json.dumps(encode(value),ensure_ascii=True,allow_nan=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()

FRAME_COLUMNS=('date','open','high','low','close','volume')
def frame_sha(frame,end_ms):
 """Hash original Freqtrade dataframe material (UTC timestamps; no text-rounded prices)."""
 from pandas import DataFrame,Timestamp
 require(type(frame) is DataFrame and tuple(frame.columns)==FRAME_COLUMNS and 1<=len(frame)<=499,
         'AUTHORITATIVE_FRAME_SHAPE')
 rows=[]
 for date,op,hi,lo,close,vol in frame.itertuples(index=False,name=None):
  require(type(date) is Timestamp and date.tzinfo is not None,'AUTHORITATIVE_FRAME_CLOCK')
  opened=date.tz_convert(timezone.utc).value//1_000_000
  require(date.value%1_000_000==0 and opened%QUARTER==0 and opened<end_ms,'AUTHORITATIVE_FRAME_GRID')
  vals=tuple(float(x) for x in (op,hi,lo,close,vol))
  o,h,l,c,v=vals
  require(all(type(x) in (int,float) and type(x) is not bool for x in (op,hi,lo,close,vol)) and all(math.isfinite(x) for x in vals) and 0<l<=min(o,c)<=max(o,c)<=h and v>=0,'AUTHORITATIVE_FRAME_VALUES')
  if rows:require(opened==rows[-1][0]+QUARTER,'AUTHORITATIVE_FRAME_GAP')
  rows.append((opened,*vals))
 require(rows[-1][0]+QUARTER==end_ms,'AUTHORITATIVE_FRAME_NOT_SAME_E')
 return canonical(('FREQTRADE_CANONICAL_15M_FRAME_V1',FRAME_COLUMNS,tuple(rows))),rows

@dataclass(frozen=True)
class AuthoritativePrimaryCommit:
 pair:str
 symbol:str
 generation:int
 end_ms:int
 owner_committed_ms:int
 expires_ms:int
 last_receipt_ms:int
 owner_commit_sha256:str
 canonical_frame_sha256:str
 primary_sha256:str
 rpd_state_sha256:str
 rpd_source_sha256:str

class CommittedProjection:
 """Owner callback keeps seals keyed by exact same-window transaction and its native store; no imported receipt."""
 def __init__(self,*,wall,native_owner_lookup,current_authority,trusted_rpd_pin):
  self._owner=get_ident()
  self.wall=wall
  self.lookup=native_owner_lookup
  self.current=current_authority
  self.rpd_pin=trusted_rpd_pin
  self._projected=None

 def project(self,receipt,candidate,frame,primary,rpd_state):
  if get_ident()!=self._owner:raise RuntimeError('ORIGINAL_WORKER_OWNER_THREAD_ONLY')
  self._projected=None
  if type(receipt) is not AuthoritativePrimaryCommit:raise TypeError('NATIVE_COMMITTED_RECEIPT_REQUIRED')
  r=receipt;k=(r.pair,r.symbol,r.generation,r.end_ms)
  require(k==(candidate.pair,candidate.symbol,candidate.generation,r.end_ms) and
          len(r.symbol)>4 and r.symbol.endswith('USDT') and r.pair==r.symbol[:-4]+'/USDT:USDT','PRIMARY_EXACT_SCOPE')
  require(type(r.generation) is int and r.generation>0 and all(type(x) is int for x in (r.end_ms,r.owner_committed_ms,r.expires_ms,r.last_receipt_ms)) and
     r.end_ms%QUARTER==0 and self._valid(r,k,candidate,self.wall()),'PRIMARY_FRESH_OWNER_COMMIT')
  require(all(pin(x) for x in (r.owner_commit_sha256,r.canonical_frame_sha256,r.primary_sha256,r.rpd_state_sha256,r.rpd_source_sha256)), 'PRIMARY_SOURCE_PINS')
  require(r.rpd_source_sha256==self.rpd_pin(candidate,r.end_ms,self.wall()),'PRIMARY_UNTRUSTED_RPD_SOURCE')
  require(type(primary) is tuple and len(primary)==3 and type(primary[0]) is tuple and type(primary[1]) is tuple,'PRIMARY_EXACT_CLOSED_ROWS')
  candles,minutes,signal=primary
  require(len(candles)==260 and len(minutes)==10 and type(signal) is Signal15 and signal.end_ms==r.end_ms and
          signal.status!='UNKNOWN' and all(type(x) is Candle15 for x in candles) and all(type(x) is ClosedBar and x.valid(MINUTE) for x in minutes),
          'PRIMARY_EXACT_CLOSED_ROWS')
  require(type(rpd_state) is ClosedRpdState and type(rpd_state.scope) is Scope,'ORIGINAL_NATIVE_RPD_TYPE')
  require(classify(r.end_ms,r.owner_committed_ms,candles,minutes[-1].close)==signal,'NATIVE_NW15_CLASSIFICATION_MISMATCH')
  require((rpd_state.scope.symbol,rpd_state.scope.end_ms)==(r.symbol,r.end_ms) and rpd_state.known is True and type(rpd_state.active_long) is bool and r.end_ms<=rpd_state.observed_ms<=r.last_receipt_ms,'RPD_ASOF_SCOPE')
  require(all(x.open_ms==r.end_ms-(260-i)*QUARTER and x.open_ms+QUARTER<=x.observed_ms<=r.last_receipt_ms for i,x in enumerate(candles)) and
          all(x.open_ms==r.end_ms-(10-i)*MINUTE and x.open_ms+MINUTE<=x.received_ms<=r.last_receipt_ms for i,x in enumerate(minutes)), 'PRIMARY_INPUT_AGE')
  frame_pin,rows=frame_sha(frame,r.end_ms)
  require(len(rows)>=260 and all(x.open_ms==row[0] and float(x.close).hex()==row[4].hex() for x,row in zip(candles,rows[-260:])) and
         float(candles[-1].close).hex()==float(minutes[-1].close).hex(),'PRIMARY_NATIVE_FRAME_PARITY')
  require((frame_pin,canonical(primary),canonical(rpd_state))==(r.canonical_frame_sha256,r.primary_sha256,r.rpd_state_sha256),'PRIMARY_COMMIT_CONTENT')
  require(self._valid(r,k,candidate,self.wall()) and r.rpd_source_sha256==self.rpd_pin(candidate,r.end_ms,self.wall()),'PRIMARY_SCOPE_CHANGED_DURING_PROJECTION')
  # Source-pin metadata remains in the native seal; current crossing receives only its exact existing schema.
  event=NativePrimaryEvidence(r.pair,r.symbol,r.generation,r.end_ms,r.owner_committed_ms,r.expires_ms,
                               r.canonical_frame_sha256,primary,rpd_state)
  self._projected=(event,r,k)
  return event

 def still_native(self,candidate,event,now):
  """Crossing gate checks this at use, not merely at projection time."""
  if get_ident()!=self._owner or self._projected is None:return False
  minted,r,k=self._projected
  return (event is minted and (candidate.pair,candidate.symbol,candidate.generation)==k[:3] and
          self._valid(r,k,candidate,now) and r.rpd_source_sha256==self.rpd_pin(candidate,r.end_ms,now))

 def _valid(self,r,key,candidate,now):
  return (r.end_ms<=r.last_receipt_ms<=r.owner_committed_ms<=now<r.expires_ms<=r.end_ms+MINUTE and
          self.lookup(*key) is r and self.current(candidate,r,now) is True)
