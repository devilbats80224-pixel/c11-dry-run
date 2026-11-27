"""Disabled staging source. Native states are recreated only in a separate memory DB."""
from exact_symbol import ID_CHAR
from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
from threading import get_ident
import json,re
from causal_bars import CausalBars,ClosedBar,SegmentStart,MINUTE,FIVE,QUARTER
from atr_er_checkpoint import AtrErCheckpoint,VERSION
from rpd_checkpoint import RpdState
from capture import sanitize
from c11_primary_decision_seam_v1.exact_rpd5_join import join

LIVE_OPG_REVISION_HANDOFF_ENABLED=False
W={'1m':MINUTE,'5m':FIVE,'15m':QUARTER}
S=re.compile(f'{ID_CHAR}{{2,40}}USDT')
def enc(x):return json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def sha(x):return sha256(x).hexdigest()
def need(x,reason):
    if not x:raise ValueError(reason)

@dataclass(frozen=True)
class Prior:
    symbol:str
    origin:int
    cursor:int
    observed:int
    pin:str
    source:bytes

@dataclass(frozen=True)
class CurrentWindow:
    symbol:str
    generation:int
    end_ms:int
    expires_ms:int

@dataclass(frozen=True)
class CanonicalCursors:
    origin:int
    rpd_5m:int
    atr_5m:int
    atr_15m:int
    observed_ms:int
    def vector(self):return self.rpd_5m,self.atr_5m,self.atr_15m

@dataclass(frozen=True)
class ProspectiveResult:
    symbol:str
    generation:int
    end_ms:int
    observed_ms:int
    revision_pin:str
    current_receipt_pin:str
    branch_sha:str
    result_data:bytes
    simulated_only:bool=True

def parse_prior(source,digest,*,independent_readback):
    need(type(source) is bytes and sha(source)==digest,'REVISED_PIN')
    x=json.loads(source)
    need(type(x) is dict and S.fullmatch(x['symbol']) is not None
         and x['original_canonical_changed'] is False and x['retroactive_tradable'] is False
         and x['cursor_ms']%QUARTER==0 and x['origin_ms']<x['cursor_ms']<x['source_known_at_ms']<=x['recomputation_at_ms'],
         'REVISED_CLOCK_AND_ORIGIN')
    need(independent_readback(source,digest) is True,'INDEPENDENT_REVISED_CUSTODY_REQUIRED')
    return Prior(x['symbol'],x['origin_ms'],x['cursor_ms'],x['recomputation_at_ms'],digest,source)

def restore_memory(prior):
    x=json.loads(prior.source)
    c=CausalBars(':memory:')
    ar=AtrErCheckpoint(c)
    pr=SegmentStart(prior.symbol,prior.origin,prior.observed)
    c.set_segment(pr)
    y=x['derived_state']; r=RpdState.restore(enc(y['rpd_state']).decode())
    need((r.symbol,r.origin,r.next_open)==(prior.symbol,prior.origin,prior.cursor),'REVISED_RPD_ID')
    with c.db:
      c.db.execute('BEGIN IMMEDIATE')
      c.db.execute('INSERT INTO rpd_checkpoint VALUES(?,?)',(prior.symbol,r.pack()))
      for w in (FIVE,QUARTER):
       a=y['atr'][str(w)]; state=a['state']
       need(state['clock']==prior.cursor and state['n']==(prior.cursor-prior.origin)//w,'REVISED_ATR_ID')
       c.db.execute('INSERT INTO atr_er_checkpoint VALUES (?,?,?,?,?,?,?,?)',
         (prior.symbol,w,VERSION,prior.origin,prior.observed,prior.observed,prior.cursor,enc(state).decode()))
       for label,t in (('prior',prior.cursor-w),('latest',prior.cursor)):
        q=a[label]
        need(q['known'] is True and q['source_end_ms']==t and q['actual_received_ms']<=prior.observed,'REVISED_ATR_SAMPLE_ID')
        c.db.execute('INSERT INTO atr_er_sample VALUES (?,?,?,?,?,?,?,?,?)',
                    (prior.symbol,w,t,prior.observed,1,q['consecutive_bars'],q['atr'],q['multiplier'],q['signed']))
    return c,ar,pr

def capture_scope(*,prior,scope,incumbent,now,mode,receipt_bytes,expected_receipt_sha,
                  verify_owner,verify_receipt,review_available_ms):
    # Explicit offline caller cannot be used to arm original executable.
    need(mode=='OFFLINE_PINNED_REVIEW_ONLY' and LIVE_OPG_REVISION_HANDOFF_ENABLED is False,'NO_RUNTIME_HANDOFF')
    owner=get_ident(); t=now()
    need(type(prior)is Prior and type(scope)is CurrentWindow and scope.symbol==prior.symbol and
         type(scope.generation)is int and scope.generation>0 and scope.end_ms%QUARTER==0 and
         scope.end_ms<=t<scope.expires_ms<=scope.end_ms+MINUTE and verify_owner(scope,t) is True,'PROSPECTIVE_SCOPE')
    need(type(incumbent)is CanonicalCursors and incumbent.origin==prior.origin and
         prior.observed<=review_available_ms<scope.end_ms and incumbent.observed_ms<=t,'REVIEW_BEFORE_SCOPE')
    if max(incumbent.vector())>prior.cursor:raise ValueError('ADVANCED_INCUMBENT_REBUILD_REQUIRED')
    need(sha(receipt_bytes)==expected_receipt_sha and verify_receipt(scope,expected_receipt_sha,t) is True,'SAME_E_ORIGINAL_RECEIPT_REQUIRED')
    data=json.loads(receipt_bytes)
    need(data['symbol']==scope.symbol and data['end_ms']==scope.end_ms,'CURRENT_RECEIPT_IDENTITY')
    end=scope.end_ms; out={}
    for p in data['receipts']:
        tf=p['timeframe']; width=W[tf]
        n=len(p['rows']); q=p['request_ms']; rx=p['receive_ms']
        need(tf not in out and end<=q<=rx<=t<scope.expires_ms,'SAME_E_RECEIPT_CLOCK')
        raw=sanitize(p['rows'],tf,end,n,q)
        need(raw==p['rows'],'CURRENT_NATIVE_ROWS')
        out[tf]=(raw,rx,tuple(ClosedBar(z[0],rx,*(float(z[i]) for i in (1,2,3,4,5,7))) for z in raw))
    need(set(out)==set(W) and len({Decimal(v[0][-1][4]) for v in out.values()})==1,'CURRENT_TERMINAL_PARITY')
    # This candidate validates private historical repair, not terminal finality;
    # exact current upstream finality receipt is a caller-owned prerequisite.
    prior_meta=json.loads(prior.source)
    for peer in prior_meta['per_width_exact_conflict_evidence']:
        w=peer['width']; tag=next(k for k,v in W.items() if v==w)
        candidates=[z for z in out[tag][2] if z.open_ms==peer['open_ms']]
        if candidates:
            z=candidates[0]
            stamp=(z.open_ms,peer['new_seen_ms'],*(float(getattr(z,k)).hex() for k in ('op','high','low','close','volume','quote')))
            need(len(candidates)==1 and sha(enc(stamp))==peer['revised_sha'],'CURRENT_HISTORY_VERSION_DRIFT')
    selected={}
    for label,w in (('5m',FIVE),('15m',QUARTER)):
        span=tuple(z for z in out[label][2] if z.open_ms>=prior.cursor)
        need(tuple(z.open_ms for z in span)==tuple(range(prior.cursor,end,w)),'REVISED_FUTURE_TAIL_GAP')
        selected[w]=span
    c,ar,origin=restore_memory(prior)
    try:
        c.record(prior.symbol,MINUTE,out['1m'][2])
        for w in (FIVE,QUARTER):
            c.record(prior.symbol,w,selected[w] if w==FIVE else out['15m'][2])
            need(ar.extend(prior.symbol,w,origin,end,t)==end,'REVISED_ATR_FUTURE_CURSOR')
        st=c.rpd_progress(prior.symbol,end,t); cl=st.closed()
        need(st.next_open==end and cl.known is True,'REVISED_RPD_FUTURE_CURSOR')
        quarters,minutes,signal=c.recent_primary(prior.symbol,end,t)
        need(len(quarters)==260 and len(minutes)==10 and signal.status!='UNKNOWN','REVISED_ANALYZER_CURRENT_SCOPE')
        material=dict(nw15=signal.status,rpd_state=json.loads(st.pack()),rpd_join=join(cl.scope,t,[cl])[0],
             atr={str(w):{'previous':ar.sample(prior.symbol,w,origin,end-w,t),
                           'latest':ar.sample(prior.symbol,w,origin,end,t)}
                  for w in (FIVE,QUARTER)})
        end_at=now()
        need(get_ident()==owner and t<=end_at<scope.expires_ms and verify_owner(scope,end_at) is True,'PROSPECTIVE_CLOSING_GUARD_LOST')
        packed=enc(material)
        return ProspectiveResult(prior.symbol,scope.generation,end,t,prior.pin,expected_receipt_sha,sha(packed),packed)
    finally:c.db.close()
