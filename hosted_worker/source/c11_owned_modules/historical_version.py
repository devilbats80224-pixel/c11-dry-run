"""Research only. No service, network, database or order interfaces.

Select a fully typed explicit pair of immutable observations; never auto-fallback.
The verified closed triad is a future request's native receipt, not an archive.
"""
from exact_symbol import ID_CHAR
from dataclasses import dataclass
import hashlib, json
from causal_bars import ClosedBar, MINUTE, FIVE, QUARTER

@dataclass(frozen=True)
class Receipt:
    symbol: str
    generation: int
    end: int
    expires: int
    observed: int
    batches: tuple

@dataclass(frozen=True)
class Review:
    symbol: str
    contract_identity: str
    width: int
    opened: int
    mask: int
    original_pin: str
    revised_pin: str
    revised_observed: int
    reviewed: int

def reject(ok,why):
    if not ok: raise ValueError(why)

def payload(r):
    return (r.op,r.high,r.low,r.close,r.volume,r.quote)

def signature(contract_identity,symbol,width,r):
    value=(contract_identity,symbol,width,r.open_ms,tuple(float(v).hex() for v in payload(r)))
    return hashlib.sha256(json.dumps(value,separators=(',',':')).encode()).hexdigest()

def select(*,version,review,receipt,prior,authority,contract_identity):
    """In-memory only; return complete current batch and explicit version metadata.

    prior maps exact existing keys, preserving immutable earliest observation.
    Production custody/authority or correctness is not established by this helper.
    """
    reject(version in ('ORIGINAL','REVIEWED_REVISION'),'VERSION_REQUIRED')
    r=receipt;c=review
    reject(type(r) is Receipt and type(c) is Review and r.symbol==c.symbol and
           contract_identity==c.contract_identity and type(r.generation) is int and
           r.generation>0 and r.end%QUARTER==0 and r.end<=r.observed<min(r.expires,r.end+MINUTE)
           and (r.observed < r.expires <= r.end+MINUTE) and
           authority(r.symbol,r.generation,r.end,r.observed) is True,'ORIGINAL_AUTHORITY')
    reject(c.width==QUARTER and c.opened%QUARTER==0 and c.opened+QUARTER<r.end and
           c.opened+QUARTER<=c.revised_observed<=c.reviewed<=r.end
           and type(c.mask) is int and 1<=c.mask<=63,'NO_RETROSPECTIVE_OR_TERMINAL_REVISION')
    reject(type(r.batches) is tuple and tuple(x[0] for x in r.batches)==(MINUTE,FIVE,QUARTER),'LANES')
    conflict=[]
    for w, rows in r.batches:
        n=len(rows)
        reject(type(rows) is tuple and (n==30 if w==MINUTE else n==260 if w==QUARTER else 3<=n<=499),
               'LANE_LENGTH')
        for i,row in enumerate(rows):
            reject(type(row) is ClosedBar and row.valid(w) and row.open_ms==r.end-(n-i)*w
                   and r.end<=row.received_ms<=r.observed,'LANE_NOT_EXACT_CURRENT')
            old=prior.get((r.symbol,w,row.open_ms))
            if old is None: continue
            reject(type(old) is ClosedBar and old.valid(w) and old.open_ms==row.open_ms
                   and old.received_ms<=r.observed,'PRIOR_NOT_ASOF')
            fields=tuple(a!=b for a,b in zip(payload(old),payload(row)))
            mask=sum(int(d)<<k for k,d in enumerate(fields))
            if mask: conflict.append((w,row.open_ms,mask,old,row))
    reject(len({float(rows[-1].close).hex() for _,rows in r.batches})==1,'TERMINAL_MISMATCH')
    if version=='ORIGINAL':
        reject(not conflict,'ORIGINAL_CANONICAL_CONFLICT')
        return r.batches,('ORIGINAL',r.symbol,r.generation,r.end)
    reject(len(conflict)==1,'EXPECTED_ONE_CERTIFIED_REVISION')
    w,opened,mask,old,new=conflict[0]
    reject((w,opened,mask)==(c.width,c.opened,c.mask) and old.received_ms<=c.revised_observed
           and signature(contract_identity,r.symbol,w,old)==c.original_pin and
           signature(contract_identity,r.symbol,w,new)==c.revised_pin,'REVISION_PIN_UNPROVEN')
    reject(authority(r.symbol,r.generation,r.end,r.observed) is True,'AUTHORITY_CHANGED')
    # All lanes come from the same fresh native receipt—not replacing historical
    # rows one by one during ordinary canonical reads. The returned tuple is
    # explicitly marked; it cannot be sent through old canonical write logic.
    return r.batches,('REVIEWED_REVISION',r.symbol,r.generation,r.end,c.revised_pin)

# The launcher's already exact runtime pin may opt in to one or more reviewed
# rows. A file present without explicit manifest pin is never a review grant.
from pathlib import Path
import os, re
ROOT=Path('/opt/c11_owned_modules')
CATALOG=ROOT/'historical_revision_catalog.json'
MANIFEST=ROOT/'release_stage_runtime.json'
HEX=re.compile(r'[0-9a-f]{64}\Z')
SYMBOL=re.compile(fr'{ID_CHAR}{{2,40}}USDT\Z')
REVIEW_TYPE='C11_BINANCE_USDM_OFFICIAL_HISTORICAL_REVISION_V1'

@dataclass(frozen=True)
class Certified:
    payload: Review
    origin: int
    original_seen: int
    source_sha: str
    custody_sha: str

@dataclass(frozen=True)
class VersionView:
    pair: str
    symbol: str
    generation: int
    end: int
    observed: int
    expires: int
    archived_open: int
    quarter_rows: tuple
    revision_pin: str
    source_sha: str
    custody_sha: str

    def bound(self,s,now):
        return (self.pair,self.symbol,self.generation,self.end,self.expires)==(s.pair,s.symbol,s.generation,s.end,s.expires) and self.end<=self.observed<=now<self.expires

    def commit_proof(self):
        return ('REVIEWED_OFFICIAL_HISTORICAL_15M_V1',self.pair,self.symbol,
                self.generation,self.end,self.archived_open,self.observed,self.expires,
                self.revision_pin,self.source_sha,self.custody_sha)

def compile_catalog(blob):
    def unique(pairs):
        d={}
        for k,v in pairs:
            reject(k not in d,'DUPLICATE_HISTORICAL_KEY');d[k]=v
        return d
    d=json.loads(blob,object_pairs_hook=unique)
    keys={'symbol','pair','origin','open_ms','prior_seen_ms','reference_seen_ms','reviewed_ms',
          'field_mask','old_pin','revised_pin','source_sha','custody_sha'}
    reject(type(d) is dict and set(d)=={'schema','rows'} and d['schema']==REVIEW_TYPE
           and type(d['rows']) is list and 0<len(d['rows'])<=512,'HISTORICAL_REVIEW_SHAPE')
    output={}
    for k in d['rows']:
        reject(type(k) is dict and set(k)==keys and type(k['symbol']) is str and SYMBOL.fullmatch(k['symbol'])
               and k['pair']==k['symbol'][:-4]+'/USDT:USDT','HISTORICAL_REVIEW_IDENTITY')
        s,o,g=k['symbol'],k['open_ms'],k['origin']
        reject(all(type(k[i]) is int and k[i]>0 for i in ('open_ms','origin','prior_seen_ms','reference_seen_ms','reviewed_ms','field_mask'))
               and g<=o and o%QUARTER==0 and g%FIVE==0
               and o+QUARTER<=k['prior_seen_ms']<k['reference_seen_ms']<=k['reviewed_ms']
               and 0<k['field_mask']<=63,'HISTORICAL_REVIEW_TIMES')
        reject(all(type(k[i]) is str and HEX.fullmatch(k[i]) for i in ('old_pin','revised_pin','source_sha','custody_sha'))
               and k['old_pin']!=k['revised_pin'] and s not in output,'HISTORICAL_REVIEW_PINS')
        output[s]=Certified(Review(s,k['pair']+'|BINANCE_USDM',QUARTER,o,k['field_mask'],k['old_pin'],k['revised_pin'],
                               k['reference_seen_ms'],k['reviewed_ms']),g,k['prior_seen_ms'],k['source_sha'],k['custody_sha'])
    from types import MappingProxyType
    return MappingProxyType(output)

def installed_catalog(runtime_pin):
    from stage_pin import RUNTIME_SHA,CATALOG_SHA
    reject(runtime_pin==RUNTIME_SHA,'HISTORICAL_RUNTIME_PIN')
    raw=MANIFEST.read_bytes()
    reject(hashlib.sha256(raw).hexdigest()==runtime_pin,'HISTORICAL_MANIFEST_PIN')
    digest=json.loads(raw)['files'].get(str(CATALOG))
    if digest is None:return None
    reject(type(digest) is str and HEX.fullmatch(digest) and not CATALOG.is_symlink()
           and CATALOG.is_file() and bool(os.statvfs(CATALOG).f_flag&os.ST_RDONLY),'HISTORICAL_REVIEW_NOT_IMMUTABLE')
    blob=CATALOG.read_bytes()
    reject(len(blob)<=1_048_576 and hashlib.sha256(blob).hexdigest()==digest,'HISTORICAL_REVIEW_PIN')
    records=compile_catalog(blob)
    existing=Path('/opt/c11-reviewed-seeds/review.json').read_bytes()
    reject(hashlib.sha256(existing).hexdigest()==CATALOG_SHA and set(records)<=set(json.loads(existing)['slots']),
           'HISTORICAL_REVIEW_OUTSIDE_EXISTING_COHORT')
    return records

class HistoricalVersion:
    def __init__(self,catalog,market,allowed,wall):
        self.catalog,self.market,self.allowed,self.wall=catalog,market,allowed,wall
    def protection(self,s):
        now=self.wall()
        return s.end<=now<s.expires and self.allowed(s,now) is True
    def prepare(self,store,s,batches):
        # Called inside original SQLite transaction *after* initial authority.
        cert=self.catalog.get(s.symbol)
        if cert is None or not self.protection(s):return None
        c=cert.payload
        if c.reviewed>s.end or c.opened+c.width>=s.end:return None
        segment=store.segment(s.symbol)
        market=self.market(s)
        if not (type(market) is dict and market.get('id')==s.symbol and market.get('symbol')==s.pair and
                market.get('quote')==market.get('settle')=='USDT' and market.get('linear') is True and
                market.get('swap') is True and market.get('active') is True and
                market.get('info',{}).get('onboardDate')==cert.origin and segment is not None and
                segment.open_ms==cert.origin and segment.proven_at_ms<=s.end):return None
        now=self.wall()
        prev=store.range(s.symbol,QUARTER,c.opened,c.opened+QUARTER,now)
        incoming=next((r for r in dict(batches)[QUARTER] if r.open_ms==c.opened),None)
        if len(prev)!=1 or incoming is None or prev[0].received_ms!=cert.original_seen:return None
        if payload(prev[0])==payload(incoming):return None
        from bridge_protocol import Refused
        r=Receipt(s.symbol,s.generation,s.end,s.expires,now,batches)
        try:
            full,_=select(version='REVIEWED_REVISION',review=c,receipt=r,contract_identity=s.pair+'|BINANCE_USDM',
                          prior={(s.symbol,QUARTER,c.opened):prev[0]},
                          authority=lambda _x,_g,_e,_t:self.protection(s))
        except ValueError as exc:raise Refused('C11_CLOSED_CANDLE_CHANGED') from exc
        return VersionView(s.pair,s.symbol,s.generation,s.end,now,s.expires,c.opened,
                           dict(full)[QUARTER],c.revised_pin,cert.source_sha,cert.custody_sha)
