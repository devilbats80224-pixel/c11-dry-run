"""Reviewed seed catalogue read and durable scratch handoff. No network."""
from pathlib import Path
import os
import stat
import sqlite3
from threading import get_ident
from dataclasses import dataclass
from no_sqlite_busy import optional_observation
from types import MappingProxyType
from admission import OptionalAdmission
from causal_bars import CausalBars
from rpd_checkpoint import RpdState
from binding import bind,Evidence,Hold,SYMBOL,ATR_TRANSFER

GUARD_PATH=Path('/c11-paper/causal-candles.sqlite')
MAX_OPTIONAL_BLOCK_NS=25_000_000
MAX_CATALOG_BYTES=16*1024*1024

@dataclass(frozen=True)
class PaperStoreAttestation:
    # Obtained from verified package reader which owns an attested outer lock.
    # Never accepted via publication, user environment or caller-loaded JSON.
    current:object
    manifest_sha:str
    expected_manifest_sha:str
    expected_catalog_sha:str

    def valid(self,store,db_path,review_pin):
        from binding import HASH
        return (type(self.manifest_sha) is str and HASH.fullmatch(self.manifest_sha)
                and self.manifest_sha==self.expected_manifest_sha and review_pin==self.expected_catalog_sha
                and callable(self.current) and self.current(store,db_path) is True)


class SeedLoader:
 def __init__(self,*,store,plan,catalog,review_pin,reviewed_store=None,isolated_review=False):
    mounts=Path('/proc/self/mountinfo').read_text().splitlines()
    scratch_mount=any(a.split()[4]=='/c11-paper' and b.split()[0]=='tmpfs' for a,b in (r.split(' - ',1) for r in mounts))
    if (type(store) is not CausalBars
        or store.db.execute('PRAGMA database_list').fetchall()!=[(0,'main',str(GUARD_PATH))]):
       raise Hold('EXACT_DURABLE_STORE_REQUIRED')
    if reviewed_store is None:
       if not scratch_mount:raise Hold('ISOLATED_EXACT_DURABLE_STORE_REQUIRED')
    elif ((scratch_mount is not isolated_review) or type(reviewed_store) is not PaperStoreAttestation
          or not reviewed_store.valid(store,GUARD_PATH,review_pin)):
       raise Hold('UNVERIFIED_PAPER_STORE_RELEASE')
    self.reviewed_store=reviewed_store
    self.store,self.plan,self.catalog,self.pin=store,plan,catalog,review_pin
    self.owner=get_ident()
    self.status={}
    self.admission=OptionalAdmission()
    self.marker=None
    self.last_diagnostic=None
    self.fd=os.open(catalog,os.O_RDONLY|os.O_NOFOLLOW|os.O_DIRECTORY)
    if not os.fstatvfs(self.fd).f_flag&os.ST_RDONLY:
       os.close(self.fd)
       raise Hold('IMMUTABLE_CATALOG_REQUIRED')
    try:
       try:os.stat('QUARANTINE.json',dir_fd=self.fd,follow_symlinks=False)
       except FileNotFoundError:pass
       else:raise Hold('ARCHIVE_REVISION_CATALOG_QUARANTINED')
       self.review=self.read('review.json',1_000_000)
       from binding import load,sha,REVIEW_SCOPE
       if sha(self.review)!=review_pin:raise Hold('REVIEW_PIN')
       m=load(self.review,1_000_000)
       if m['scope']!=REVIEW_SCOPE:raise Hold('REVIEW_SCOPE')
       if type(m['slots']) is not dict:raise Hold('CATALOG_SCOPE')
       assets={}
       total=0
       for sym in m['slots']:
          for role in ('seed','independent_reproduction'):
             try:
                content=self._read_asset(role,sym)
             except (OSError,Hold):continue
             total+=len(content)
             if total>MAX_CATALOG_BYTES:raise Hold('CATALOG_MEMORY_BUDGET')
             assets[role,sym]=content
       self.asset_bytes=MappingProxyType(assets)
    except BaseException:
       os.close(self.fd)
       raise

 def read(self,name,limit):
    fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=self.fd)
    try:
       a=os.fstat(fd)
       if not stat.S_ISREG(a.st_mode) or a.st_nlink!=1 or not 0<a.st_size<=limit:raise Hold('ASSET_TYPE')
       with os.fdopen(os.dup(fd),'rb') as f:b=f.read(limit+1)
       z=os.fstat(fd)
       if len(b)!=a.st_size or (a.st_ino,a.st_size,a.st_mtime_ns)!=(z.st_ino,z.st_size,z.st_mtime_ns):raise Hold('ASSET_MUTATED')
       return b
    finally:os.close(fd)

 def close(self):os.close(self.fd)

 def _read_asset(self,role,symbol):
    if not SYMBOL.fullmatch(symbol) or role not in ('seed','independent_reproduction'):raise Hold('ASSET_NAME')
    return self.read(symbol+('.seed.json' if role=='seed' else '.reproduction.json'),65_536)

 def apply_held(self,bot):
    if get_ident()!=self.owner or bot is not self.plan.bot or bot.config.get('dry_run') is not True:
       return
    end,rows=self.plan.entries()
    if not rows or len({r.generation for r in rows})!=1:return
    g=rows[0].generation
    if self.marker!=(g,end):
       self.status.clear();self.marker=(g,end)
    cut=getattr(bot,'_owned_original_cycle_deadline_ns',None)
    if type(cut) is not int:return
    deadline=min(cut,self.plan.mono()+MAX_OPTIONAL_BLOCK_NS)
    transient={'busy':0,'deferred':0}
    def attempt(row):
       if ('seed',row.symbol) not in self.asset_bytes:return 'NOT_YET_SEEDED'
       try:
          label=optional_observation(lambda:self._apply_pair(bot,row,end,deadline),(self.store.db,))
       except Exception:
          label='NATIVE_DURABLE_HOLD'
       if label=='HOLD_OBSERVER_BUSY':transient['busy']+=1
       elif label=='DEFER_ORIGINAL_BUDGET':transient['deferred']+=1
       return label
    self.admission.offer(rows,end,deadline,self.plan.mono,self.status,attempt)
    self.last_diagnostic=MappingProxyType({
        'schema':'c11-reviewed-prior-seed-counts-v1','close':end,'cohort_size':len(rows),
        'registered':sum(v=='PRIOR_CURSOR_DURABLY_REGISTERED_ONLY' for v in self.status.values()),
        'missing':sum(v=='NOT_YET_SEEDED' for v in self.status.values()),
        'preserved':sum(v=='PRESERVE_NATIVE_INCUMBENT' for v in self.status.values()),
        'not_attempted':len(self.admission.pending),**transient})

 def _apply_pair(self,bot,row,end,deadline):
    scope=self.plan.provisional_scope(row.pair,end)
    if scope is None or self.plan.accepted_candidate(scope) is not row:return 'OWNER_AUTHORITY_INVALID'
    if self.store.db.in_transaction:return 'NATIVE_STORE_BUSY'
    if self.store.db.execute('SELECT 1 FROM segment WHERE symbol=? UNION ALL SELECT 1 FROM rpd_checkpoint WHERE symbol=?',(scope.symbol,scope.symbol)).fetchone():
       return 'PRESERVE_NATIVE_INCUMBENT'
    def current(_,t):
       return (self.plan.mono()<deadline and self.plan.current(scope,t) and self.plan.accepted_candidate(scope) is row
               and (self.reviewed_store is None or self.reviewed_store.valid(self.store,GUARD_PATH,self.pin)))
    try:
       market=bot.exchange.markets[row.pair]
       i=market['info']
       if not (market['active'] is market['swap'] is market['linear'] is True):return 'MARKET_IDENTITY'
       market_id=dict(symbol=market['id'],baseAsset=market['base'],quoteAsset=market['quote'],marginAsset=market['settle'],
                      contractType=i['contractType'],onboardDate=i['onboardDate'],status=i['status'])
    except (TypeError,KeyError,AttributeError):return 'MARKET_IDENTITY'
    t=self.plan.wall()
    # Claimed now is deliberate conservative upper bound on native observation:
    # scope exists only through freshly authenticated owner, never backdate.
    proof=Evidence(scope.generation,scope.symbol,scope.pair,scope.end,t,scope.expires)
    result,label=bind(proof,now_ms=t,identity=market_id,review=self.review,review_sha=self.pin,
         read_asset=lambda role,symbol:self.asset_bytes.get((role,symbol)),is_current=current,
         native_validate=lambda b:RpdState.restore(b.decode('ascii')).pack().encode('ascii'),wall_now=self.plan.wall)
    if result is None:return 'DEFER_ORIGINAL_BUDGET' if self.plan.mono()>=deadline else label
    s=RpdState.restore(result.native_prior_state.decode('ascii'))
    try:
       with self.store.db:
          self.store.db.execute('BEGIN IMMEDIATE')
          if self.plan.wall()>=result.deadline_ms or not current(proof,self.plan.wall()):
             raise Hold('OWNER_AUTHORITY_INVALID')
          if self.store.db.execute('SELECT 1 FROM segment WHERE symbol=? UNION ALL SELECT 1 FROM rpd_checkpoint WHERE symbol=?',(s.symbol,s.symbol)).fetchone():
             return 'PRESERVE_NATIVE_INCUMBENT'
          self.store.db.execute('INSERT INTO segment VALUES(?,?,?)',(s.symbol,s.origin,result.seed_ready_ms))
          self.store.db.execute('INSERT INTO rpd_checkpoint VALUES(?,?)',(s.symbol,s.pack()))
          if result.native_atr_rows:
             from atr_er_checkpoint import VERSION
             if VERSION!=ATR_TRANSFER:raise Hold('NATIVE_ATR_VERSION')
             db=self.store.db
             if db.execute('SELECT 1 FROM atr_er_checkpoint WHERE symbol=? UNION ALL SELECT 1 FROM atr_er_sample WHERE symbol=?',
                           (s.symbol,s.symbol)).fetchone():raise Hold('PRESERVE_NATIVE_ATR_INCUMBENT')
             # Schema was initialized by the existing FeatureSpan. No DDL
             # or commits in here: both lanes remain one owner transaction.
             ready=result.seed_ready_ms
             for width,payload,samples in result.native_atr_rows:
                db.execute('INSERT INTO atr_er_checkpoint (symbol,width,version,origin,proven,observed,next_open,payload) VALUES (?,?,?,?,?,?,?,?)',
                           (s.symbol,width,VERSION,s.origin,ready,ready,s.next_open,payload))
                db.executemany('INSERT INTO atr_er_sample VALUES (?,?,?,?,?,?,?,?,?)',
                               [(s.symbol,width,clock,ready,1,n,a,m,d) for clock,n,a,m,d in samples])
                if db.execute('SELECT payload FROM atr_er_checkpoint WHERE symbol=? AND width=?',
                              (s.symbol,width)).fetchone()!=(payload,):raise Hold('NATIVE_ATR_READBACK')
                if tuple(db.execute('SELECT clock,consecutive,atr,multiplier,signed FROM atr_er_sample '
                                    'WHERE symbol=? AND width=? ORDER BY clock',(s.symbol,width)))!=samples:
                   raise Hold('NATIVE_ATR_READBACK')
          out=self.store.db.execute('SELECT payload FROM rpd_checkpoint WHERE symbol=?',(s.symbol,)).fetchone()
          if out!=(s.pack(),):raise Hold('NATIVE_DURABLE_READBACK')
          if not current(proof,self.plan.wall()):raise Hold('OWNER_AUTHORITY_INVALID')
       return 'PRIOR_CURSOR_DURABLY_REGISTERED_ONLY'
    except sqlite3.OperationalError:raise
    except Exception:return 'NATIVE_DURABLE_HOLD'
