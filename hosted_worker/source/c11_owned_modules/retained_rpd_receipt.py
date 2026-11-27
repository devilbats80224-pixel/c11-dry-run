"""Build-only original writer retention; no transport, cross-worker state, or I/O imports."""
from dataclasses import dataclass
from hashlib import sha256
import json
from threading import get_ident
from native_commit_index import NativeCommitIndex, NativeRequest
from rpd_checkpoint import RpdState
from capture import sanitize

FIVE=300000
MINUTE=60000

def digest(value):
 return sha256(json.dumps(value,sort_keys=True,allow_nan=False,separators=(',',':')).encode()).hexdigest()

@dataclass(frozen=True)
class RetainedRpdReceipt:
 request:NativeRequest
 start_ms:int
 raw_sha256:str
 lane:tuple

class RetainedNativeCommitIndex(NativeCommitIndex):
 def __init__(self,store,authority):
  super().__init__(store,authority)
  self._scope=None
  self._rpd={}
  self._rpd_lineage={}
  self._revised_lineage={}

 def start(self,request):
  if get_ident()!=self.owner or type(request) is not NativeRequest:raise RuntimeError('NATIVE_WRITER_SESSION_OWNER')
  sh=(request.generation,request.end_ms)
  # Never revive a pruned frame even if a caller recreates the same stamp.
  if self._scope is not None and (sh[0]<self._scope[0] or sh[1]<self._scope[1]):
   raise ValueError('PRIOR_NATIVE_GENERATION_OR_WINDOW')
  super().start(request)
  self._rpd_lineage.clear();self._revised_lineage.clear()
  if sh!=self._scope:
   self._scope=sh
   self._rpd.clear()
   self._rpd_lineage.clear()

 def stop(self):
  super().stop()
  self._rpd_lineage.clear();self._revised_lineage.clear()

 def _key(self,session):
  return session.pair,session.symbol,session.generation,session.end_ms

 def after_original_record(self,store,symbol,width,end,requested_ms,receipt_ms,actual_ms,rows):
  # The legacy lane callback carries no exact raw bytes proof for a 5m tail.
  if width==FIVE:return False
  return super().after_original_record(store,symbol,width,end,requested_ms,receipt_ms,actual_ms,rows)

 def after_original_5m_record(self,store,symbol,start,end,count,requested,received,committed,raw,closed):
  """Original NativeCandles.query invokes only after successful existing writer commit."""
  session=self.session
  if get_ident()!=self.owner or session is None or store is not self.store:return False
  if symbol!=session.symbol or end!=session.end_ms or not end<=requested<=received<=committed<session.deadline_ms<=end+MINUTE:return False
  if type(count) is not int or not 1<=count<=499 or type(start) is not int or start!=end-count*FIVE:return False
  # Established Binance response validator accepts 1 or >=3 candles; adapt exact
  # two-candle tail by validating it as two genuine one-row windows, never padding it.
  try:
   if count==2:
    if len(raw)!=2:return False
    wire=sanitize(raw[:1],'5m',end-FIVE,1,requested)+sanitize(raw[1:],'5m',end,1,requested)
   else:wire=sanitize(raw,'5m',end,count,requested)
   if len(closed)!=count:return False
   for bar,line in zip(closed,wire):
    raw_values=tuple(float(line[i]) for i in (1,2,3,4,5,7))
    if (bar.open_ms,bar.received_ms,bar.op,bar.high,bar.low,bar.close,bar.volume,bar.quote)!=(line[0],received,*raw_values):return False
  except (TypeError,ValueError,IndexError,AttributeError):return False
  key=self._key(session)
  raw_pin=digest(('NATIVE_CCXT_ORIGINAL_FAPI_KLINES',symbol,'5m',start,end-1,count,wire))
  prior=self._rpd.get(key)
  if prior is not None and (prior.raw_sha256!=raw_pin or prior.lane[:3]!=(requested,received,committed)):return False
  if not super().after_original_record(store,symbol,FIVE,end,requested,received,committed,closed):return False
  self._rpd[key]=RetainedRpdReceipt(session,start,raw_pin,self._lanes[FIVE])
  self._rpd_lineage.pop(key,None)
  return True

 def attach_current_rpd(self,store,pair,symbol,end,state,now):
  """Prove original durable checkpoint AND matching original raw 5m receipt, or do not attach."""
  session=self.session
  if get_ident()!=self.owner or store is not self.store or session is None or store.db.in_transaction:return None
  if (pair,symbol,end)!=(session.pair,session.symbol,session.end_ms):return None
  key=self._key(session)
  self._rpd_lineage.pop(key,None)
  saved=self._rpd.get(key)
  if saved is None or not (end<=now<min(session.deadline_ms,saved.request.deadline_ms,end+MINUTE)):return None
  if self.authority(session,now) is not True or self.authority(saved.request,now) is not True:return None
  requested,received,committed,last=saved.lane
  if not end<=requested<=received<=committed<=now:return None
  if type(state) is not RpdState or (state.symbol,state.next_open)!=(symbol,end) or not end<=state.observed<=received:return None
  native=state.closed()
  if native.known is not True:return None
  stored=store.db.execute('SELECT payload FROM rpd_checkpoint WHERE symbol=?',(symbol,)).fetchone()
  packed=state.pack()
  if stored!=(packed,):return None
  segment=store.segment(symbol)
  if segment is None or state.origin!=segment.open_ms or not state.origin<=segment.proven_at_ms<=received:return None
  current=store.db.execute('SELECT received_ms,op,high,low,close,volume,quote FROM candle '
                           'WHERE symbol=? AND width=? AND open_ms=?',(symbol,FIVE,end-FIVE)).fetchone()
  if current!=(received,last.op,last.high,last.low,last.close,last.volume,last.quote):return None
  prior=self._lanes.get(FIVE)
  if prior is not None and prior!=saved.lane:return None
  self._lanes[FIVE]=saved.lane
  lineage=digest(('ORIGINAL_CURRENT_SCOPE_RPD5_RECEIPT_AND_DURABLE_CHECKPOINT',key,saved.raw_sha256,packed,
                 segment.open_ms,segment.proven_at_ms,requested,received,committed))
  self._rpd_lineage[key]=(lineage,packed,segment.open_ms,segment.proven_at_ms)
  return lineage

 def attach_scoped_revision(self,store,scope,incumbent,view,now):
  from multilane_history_view import FrozenReadView
  s=self.session
  if (type(view) is not FrozenReadView or not view.bound(scope,now) or
      s is None or (scope.pair,scope.symbol,scope.generation,scope.end,scope.expires)!=
      (s.pair,s.symbol,s.generation,s.end_ms,s.deadline_ms)):return None
  ordinary=self.attach_current_rpd(store,scope.pair,scope.symbol,scope.end,incumbent,now)
  if ordinary is None:return None
  revised=view.corrected_rpd()
  saved=self._rpd.get(self._key(s))
  if not scope.end<=revised.observed<=saved.lane[1]:return None
  derived=digest(('ORIGINAL_TERMINAL_WITH_SEALED_REVISED_LINEAGE',ordinary,
                  view.immutable_proof(),revised.pack(),saved.raw_sha256))
  self._revised_lineage[self._key(s)]=(scope,view,ordinary,derived)
  return derived

 def lineage_pin(self,pair,symbol,generation,end,now):
  original=self._ordinary_lineage_pin(pair,symbol,generation,end,now)
  selected=self._revised_lineage.get((pair,symbol,generation,end))
  if selected is None or original is None:return original
  s,v,prior,selected_id=selected
  return selected_id if v.bound(s,now) and original==prior else None

 def _ordinary_lineage_pin(self,pair,symbol,generation,end,now):
  s=self.session
  if get_ident()!=self.owner or s is None or (pair,symbol,generation,end)!=self._key(s):return None
  saved=self._rpd.get(self._key(s))
  if (saved is None or not end<=now<min(s.deadline_ms,saved.request.deadline_ms,end+MINUTE) or
      self.authority(s,now) is not True or self.authority(saved.request,now) is not True or self.store.db.in_transaction):return None
  retained=self._rpd_lineage.get(self._key(s))
  if retained is None:return None
  lineage,packed,start,proof=retained
  pin=self.store.db.execute('SELECT payload FROM rpd_checkpoint WHERE symbol=?',(symbol,)).fetchone()
  if pin!=(packed,):return None
  origin=self.store.segment(symbol)
  if origin is None or (origin.open_ms,origin.proven_at_ms)!=(start,proof):return None
  _,received,_,last=saved.lane
  row=self.store.db.execute('SELECT received_ms,op,high,low,close,volume,quote FROM candle '
                           'WHERE symbol=? AND width=? AND open_ms=?',(symbol,FIVE,end-FIVE)).fetchone()
  return lineage if row==(received,last.op,last.high,last.low,last.close,last.volume,last.quote) else None

 def index_verified_seal(self,seal,exact_projection_check):
  if get_ident()!=self.owner:return False
  binding=(seal.pair,seal.symbol,seal.generation,seal.end_ms,seal.owner_committed_ms)
  actual=self.lineage_pin(*binding)
  if actual is None or actual!=seal.rpd_source_sha256:return False
  return super().index_verified_seal(seal,exact_projection_check)
