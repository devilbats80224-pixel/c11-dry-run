"""Read-only deployment review seam. No clocks, source reads, writes or mutable importer."""
from exact_symbol import ID_CHAR
from dataclasses import dataclass
import hashlib,json,math,re

FIVE=300_000
QUARTER=900_000
HORIZON=60_000
SYMBOL=re.compile(fr'{ID_CHAR}{{2,36}}USDT')
HASH=re.compile(r'[0-9a-f]{64}')
IDENT=('symbol','baseAsset','quoteAsset','marginAsset','contractType','onboardDate','status')
SOURCE_SCOPE='ISOLATED_NATIVE_CHECKPOINT'
REVIEW_SCOPE='C11_DRY_RUN_PRIOR_CURSOR_SOURCE_REVIEW_V1'
ATR_TRANSFER='c11-atr20-er20-ema5-v1'

@dataclass(frozen=True)
class Evidence:
    # Produced only by native owner gateway: the booleans are not credentials.
    generation:int
    symbol:str
    pair:str
    end_ms:int
    first_verified_ms:int
    effective_expiry_ms:int

@dataclass(frozen=True)
class Handoff:
    generation:int
    symbol:str
    pair:str
    end_ms:int
    deadline_ms:int
    cursor_ms:int
    seed_ready_ms:int
    tail_bars:int
    source_sha256:str
    reproduction_sha256:str
    review_sha256:str
    # Canonical original RPD bytes only; no terminal, order or current lease flags.
    native_prior_state:bytes
    native_atr_rows:tuple=()

class Hold(ValueError):pass
def req(check,status):
    if not check:raise Hold(status)
def sha(b):return hashlib.sha256(b).hexdigest()
def ints(*a):return all(type(v) is int for v in a)
def mapping(value):
    req(type(value) is dict,'BAD_SOURCE')
    return value
def load(b,maximum):
    req(type(b) is bytes and 0<len(b)<=maximum,'SOURCE_BYTES')
    def pairs(items):
        out={}
        for k,v in items:
            req(k not in out,'DUPLICATE_KEY')
            out[k]=v
        return out
    def invalid_constant(_):raise Hold('NONFINITE_JSON')
    return mapping(json.loads(b,object_pairs_hook=pairs,parse_constant=invalid_constant))
def canonical(value):
    return json.dumps(value,allow_nan=False,separators=(',',':'),sort_keys=True).encode('ascii')
def atr_rows(a,r,origin,cursor,known):
    # Legacy catalogs did not review a durable ATR import. Their behavior
    # stays exact until a separately hashed pair explicitly carries the flag.
    if 'atr_transfer_version' not in a and 'atr_transfer_version' not in r:return ()
    req(a.get('atr_transfer_version')==r.get('atr_transfer_version')==ATR_TRANSFER,'ATR_TRANSFER_VERSION')
    left=mapping(a['atr']); right=mapping(r['atr'])
    req(left==right and set(left)=={'5m','15m'},'ATR_INDEPENDENT_REPRODUCTION')
    def num(v):return type(v) in (float,int) and math.isfinite(v)
    def valid_values(obj):
        return all(num(obj[k]) for k in ('atr','multiplier','signed')) and obj['atr']>=0 and 1-1e-12<=obj['multiplier']<=3+1e-12 and abs(obj['signed'])<=1+1e-12
    out=[]
    for tf,w in (('5m',FIVE),('15m',QUARTER)):
        bundle=mapping(left[tf]); req(set(bundle)=={'state','latest','prior'},'ATR_CARRIER_SCHEMA')
        state=mapping(bundle['state']); n=(cursor-origin)//w
        req(set(state)=={'atr','clock','closes','multiplier','n','signed','trs'}
            and ints(state['clock'],state['n']) and (state['clock'],state['n'])==(cursor,n)
            and (cursor-origin)%w==0 and n>=401 and type(state['closes']) is list
            and len(state['closes'])==20 and all(num(v) and v>0 for v in state['closes'])
            and state['trs']==[] and valid_values(state),'ATR_STATE_CLOCK_OR_SHAPE')
        samples=[]
        for key,clock,count in (('prior',cursor-w,n-1),('latest',cursor,n)):
            sample=mapping(bundle[key]); req(set(sample)=={'actual_received_ms','atr','available_proxy_ms','consecutive_bars',
                  'known','multiplier','signed','source_end_ms'} and sample['known'] is True
                  and ints(*(sample[k] for k in ('actual_received_ms','available_proxy_ms','consecutive_bars','source_end_ms')))
                  and (sample['source_end_ms'],sample['available_proxy_ms'],sample['consecutive_bars'])==(clock,clock,count)
                  and clock<=sample['actual_received_ms']<=known and valid_values(sample),'ATR_SAMPLE_CLOCK_OR_SHAPE')
            samples.append((clock,count,*[sample[k] for k in ('atr','multiplier','signed')]))
        req(tuple(state[k] for k in ('atr','multiplier','signed'))==samples[-1][2:]
            and bundle['prior']['actual_received_ms']<=bundle['latest']['actual_received_ms'],'ATR_LATEST_STATE_BINDING')
        out.append((w,json.dumps(state,allow_nan=False,sort_keys=True),tuple(samples)))
    return tuple(out)

def bound(proof,now_ms,expires):
    return (type(proof) is Evidence and ints(proof.generation,proof.end_ms,proof.first_verified_ms,proof.effective_expiry_ms,now_ms)
            and proof.generation>0 and type(proof.symbol) is str and SYMBOL.fullmatch(proof.symbol)
            and proof.pair==proof.symbol[:-4]+'/USDT:USDT' and proof.end_ms%QUARTER==0
            and 0<proof.first_verified_ms<=now_ms<expires and proof.end_ms<=now_ms)

def bind(proof,*,now_ms,identity,review,review_sha,read_asset,is_current,native_validate,wall_now):
    """Return (Handoff|None, hold/review label). No side effects except named read-only callbacks.

    Only trusted compiled owner assembles Evidence and supplies is_current, native_validate, wall_now.
    read_asset(role,pinned_symbol) is constrained by package manifest mount; no caller paths.
    """
    try:
        req(type(proof) is Evidence and ints(proof.end_ms,proof.effective_expiry_ms),'INVALID_OWNER_EVIDENCE')
        expires=min(proof.end_ms+HORIZON,proof.effective_expiry_ms)
        req(bound(proof,now_ms,expires) and is_current(proof,now_ms) is True,'OWNER_AUTHORITY_INVALID')
        req(type(review_sha) is str and HASH.fullmatch(review_sha) and type(review) is bytes
            and len(review)<=1_000_000 and sha(review)==review_sha,'REVIEW_PIN')
        man=load(review,1_000_000)
        req(set(man)=={'scope','slots'} and man['scope']==REVIEW_SCOPE,'REVIEW_SCOPE')
        slot=mapping(man['slots']).get(proof.symbol)
        req(slot is not None,'NOT_YET_SEEDED')
        slot=mapping(slot)
        req(set(slot)=={'source_sha256','reproduction_sha256','cursor_ms','approved_at_ms'},'SOURCE_RECORD_SCHEMA')
        source_pin=slot['source_sha256']; proof_pin=slot['reproduction_sha256']
        req(all(type(v) is str and HASH.fullmatch(v) for v in (source_pin,proof_pin))
            and source_pin!=proof_pin and ints(slot['cursor_ms'],slot['approved_at_ms'])
            and 0<slot['approved_at_ms']<=now_ms,'SOURCE_RECORD_PIN')
        source=read_asset('seed',proof.symbol)
        reproduction=read_asset('independent_reproduction',proof.symbol)
        req(type(source) is bytes and len(source)<=65_536 and sha(source)==source_pin
            and type(reproduction) is bytes and len(reproduction)<=65_536 and sha(reproduction)==proof_pin,'ASSET_PIN')
        a=load(source,65_536)
        rr=load(reproduction,65_536)
        r=mapping(rr.get(proof.symbol,rr))
        req(not any('revision_branch' in x or x.get('eligible_for_active_overlap_merge') is False
            or x.get('conflict_with_early_owner_retained') is True for x in (a,r)),
            'ARCHIVE_REVISION_QUARANTINED')
        req(a['scope']==r['scope']==SOURCE_SCOPE and all(x['live_write'] is False
            and x['tradable_at_old_boundary'] is False and x['requires_fresh_generation_and_same_e_tail'] is True
            for x in (a,r)),'SOURCE_SCOPE')
        contract=mapping(a['contract']); contract_payload=mapping(contract['payload'])
        req(type(identity) is dict and set(contract_payload)==set(IDENT) and
            contract_payload=={k:identity[k] for k in IDENT} and
            (identity['symbol'],identity['baseAsset'],identity['quoteAsset'],identity['marginAsset'],identity['status'],identity['contractType'])
            ==(proof.symbol,proof.symbol[:-4],'USDT','USDT','TRADING','PERPETUAL'),'MARKET_IDENTITY')
        state=mapping(a['rpd_state']); header=mapping(state['state'])
        origin=a['origin_ms']; cursor=a['end_ms']; obs=header['observed']
        known=a['earliest_research_known_ms']; calc=a['state_calculation_asof_ms']; ready=a['artifact_available_ms']
        req(ints(origin,cursor,obs,known,calc,ready,contract['actual_received_ms'],identity['onboardDate']) and
            a['symbol']==header['symbol']==r['symbol']==proof.symbol and
            origin==header['origin']==r['origin_ms']==identity['onboardDate'] and
            cursor==header['next_open']==r['end_ms']==slot['cursor_ms'] and
            0<origin<cursor and cursor%QUARTER==0 and
            cursor<=obs<=known<=calc<=ready<=slot['approved_at_ms'] and
            contract['actual_received_ms']<=known,'SOURCE_TIME_LINEAGE')
        req(r['rpd_state']==state and r['contract']==a['contract'] and
            ints(r['artifact_available_ms']) and obs<=r['artifact_available_ms']<=ready,'INDEPENDENT_REPRODUCTION')
        atr=atr_rows(a,r,origin,cursor,known)
        req(cursor!=proof.end_ms,'AT_END_NEEDS_OWNED_RECEIPT')
        delta=proof.end_ms-cursor
        req(delta>0 and delta%FIVE==0 and 3<=delta//FIVE<=499,'TAIL_OUTSIDE_ORIGINAL_PLAN')
        checked=native_validate(canonical(state))
        req(type(checked) is bytes and canonical(load(checked,65_536))==canonical(state),'ORIGINAL_STATE_ROUNDTRIP')
        after=wall_now()
        req(type(after) is int and now_ms<=after and bound(proof,after,expires) and is_current(proof,after) is True,'OWNER_AUTHORITY_INVALID')
        return (Handoff(proof.generation,proof.symbol,proof.pair,proof.end_ms,expires,cursor,ready,
                        delta//FIVE,source_pin,proof_pin,review_sha,checked,atr),'PRIOR_CURSOR_SOURCE_BOUND_ONLY')
    except Hold as e:return None,str(e)
    except Exception:return None,'SOURCE_OR_REVIEW_UNAVAILABLE'
