"""Native-shaped isolated specialist, intentionally not configured by startup."""
from dataclasses import dataclass
from hashlib import sha256
from threading import get_ident
from causal_bars import MINUTE,FIVE,QUARTER
from rpd_checkpoint import RpdState
from bridge_protocol import Refused
import json

ENABLED=False
def enc(x):return json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def digest(x):return sha256(x).hexdigest()
def ensure(x,label):
    if not x:raise Refused(label)
def rowpin(r,rx):
    return digest(enc((r.open_ms,rx,*(float(getattr(r,k)).hex() for k in ('op','high','low','close','volume','quote')))))

@dataclass(frozen=True)
class AuditedKey:
    width:int
    open_ms:int
    old_seen:int
    new_seen:int
    previous_pin:str
    revised_pin:str

@dataclass(frozen=True)
class FrozenReadView:
    pair:str
    symbol:str
    generation:int
    E:int
    expiry:int
    owner:int
    prior_origin:int
    scope_seen:int
    review_seen:int
    corpus_pin:str
    current_receipt_pin:str
    derived_sha:str
    state_bytes:bytes
    keys:tuple
    batches:tuple
    def bound(self,s,now):
        return (self.owner==get_ident() and (self.pair,self.symbol,self.generation,self.E,self.expiry)
               ==(s.pair,s.symbol,s.generation,s.end,s.expires)
                and self.review_seen<self.E<=self.scope_seen<=now<self.expiry and
                digest(self.state_bytes)==self.derived_sha)
    def immutable_proof(self):
        return ('SCOPED_MULTILANE_HISTORY_PROOF_V1',self.corpus_pin,self.current_receipt_pin,self.derived_sha,
            self.symbol,self.generation,self.E,self.expiry,
            tuple((k.width,k.open_ms,k.old_seen,k.new_seen,k.previous_pin,k.revised_pin) for k in self.keys))
    @property
    def quarter_rows(self):return dict(self.batches)[QUARTER]
    def verify_keys(self,store,scope,now,batches):
        ensure(self.bound(scope,now) and self.batches is batches and store.db.in_transaction,'C11_REVISION_OWNER_OR_BATCH')
        ensure(len(self.keys)==3 and {k.width for k in self.keys}=={MINUTE,FIVE,QUARTER},'C11_REVISION_LANE_KEYS')
        expected=set(); by=dict(batches)
        segment=store.segment(self.symbol)
        ensure(segment is not None and segment.open_ms==self.prior_origin and segment.proven_at_ms<=now,'C11_REVISION_ORIGIN')
        for k in self.keys:
            ensure(k.open_ms%k.width==0 and k.open_ms+k.width<self.E and
                   k.open_ms+k.width<=k.old_seen<k.new_seen<=self.review_seen,'C11_REVISION_TERMINAL_OR_CLOCK')
            old=store.range(self.symbol,k.width,k.open_ms,k.open_ms+k.width,now)
            ensure(len(old)==1 and old[0].received_ms==k.old_seen and
                   rowpin(old[0],k.old_seen)==k.previous_pin,'C11_REVISION_PRIOR_PIN')
            candidate=tuple(v for v in by[k.width] if v.open_ms==k.open_ms)
            if candidate:
                ensure(len(candidate)==1 and rowpin(candidate[0],k.new_seen)==k.revised_pin and
                       k.revised_pin!=k.previous_pin,'C11_REVISION_NEW_PIN')
                expected.add((k.width,k.open_ms))
        return frozenset(expected)
    def corrected_rpd(self):
        x=json.loads(self.state_bytes)
        r=RpdState.restore(enc(x['rpd_state']).decode())
        ensure((r.symbol,r.origin,r.next_open)==(self.symbol,self.prior_origin,self.E)
            and self.E<=r.observed<=self.scope_seen and r.closed().known is True,'C11_REVISION_RPD_READBACK')
        return r
    def corrected_atr(self,now):
        # All closed keys must precede the longest existing 1h structural
        # and archived-half entry facts window. Never mix an overlapping
        # retained canonical 1m/5m fact with the selected revised ATR.
        ensure(all(k.open_ms+k.width<=self.E-3_600_000 for k in self.keys),'C11_REVISION_ENTRY_FACTS_OVERLAP')
        x=json.loads(self.state_bytes)['atr']
        m5,m15,previous=x['300000']['latest'],x['900000']['latest'],x['900000']['previous']
        ensure(self.E<=now<self.expiry and all(q['known'] is True and q['actual_received_ms']<=now
               for q in (m5,m15,previous)) and (m5['source_end_ms'],m15['source_end_ms'],previous['source_end_ms'])
               ==(self.E,self.E,self.E-QUARTER),'C11_REVISION_ATR_READBACK')
        from native_protection import sample
        f,=sample([self.E],{self.E:m5},{self.E:m15,self.E-QUARTER:previous})
        ensure(f['known'] is True,'C11_REVISION_ENTRY_ATR')
        return dict(f,origin_ms=self.prior_origin,observed_at_ms=max(q['actual_received_ms'] for q in (m5,m15,previous)))
