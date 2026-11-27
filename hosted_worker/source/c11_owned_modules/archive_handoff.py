"""Candidate archive-to-owner interface; disabled prototype.
Large byte validation never belongs in the PaperBot callback.
"""
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
import json,sqlite3
from decimal import Decimal,InvalidOperation
from causal_bars import ClosedBar,FIVE,QUARTER
W={FIVE:'5m',QUARTER:'15m'}

def need(ok,code):
    if not ok:raise ValueError(code)

def canonical(x):
    return json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False).encode()

@dataclass(frozen=True)
class VerifiedPage:
    span:tuple              # Raw identity only, no owner generation/order claim.
    cache_pin:str
    version:str
    source_known_ms:int
    validated_ms:int
    rows:tuple              # parsed bounded native fields; no computed state

def same_span(p):
    return p.symbol,p.width,p.start,p.stop,p.origin

def prepare(cache,cache_pin,verified_version,current_version,market,page,clock,still_current):
    """Only an isolated prep worker may call; opens immutable archive read-only."""
    need(still_current() is True,'ARCHIVE_PREP_REVOKED')
    f=Path(cache)
    need(f.is_absolute() and f.resolve()==f and f.is_file() and 0<f.stat().st_size<=64*1024*1024,'ARCHIVE_FILE')
    with f.open('rb') as s:
        sealed=s.read(64*1024*1024+1)
    need(0<len(sealed)<=64*1024*1024 and sha256(sealed).hexdigest()==cache_pin,'ARCHIVE_SOURCE_PIN')
    need(verified_version==current_version,'ARCHIVE_REVISION_CHANGED')
    with sqlite3.connect(':memory:') as s:
        s.deserialize(sealed)  # Do not reopen pathname after verifying its bytes.
        s.execute('pragma query_only=ON')
        need(s.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()==[('archive_meta',),('raw',)],'ARCHIVE_DATA_ONLY')
        meta_rows=s.execute('SELECT meta FROM archive_meta').fetchall()
        need(len(meta_rows)==1,'ARCHIVE_META')
        meta=json.loads(meta_rows[0][0]); origin=page.origin
        w=W[page.width]; q=meta['widths'][w]
        fixed={k:v for k,v in meta.items() if k not in ('cache_version','verified_after_ms','approval','raw_only')}
        need(sha256(canonical(fixed)).hexdigest()==meta['cache_version']==verified_version,'ARCHIVE_REVISION_CHANGED')
        need(meta['raw_only'] is True and meta['approval'] is False and meta['schema']==1 and meta['contract']==market and
             market['symbol']==page.symbol and market['onboardDate']==origin and origin%page.width==0,
             'ARCHIVE_MARKET_OR_POLICY')
        need(type(meta['verified_after_ms']) is int and page.stop<=meta['verified_after_ms']<=clock(),
             'ARCHIVE_BEFORE_ACQUISITION')
        need(meta['cross_native']['mismatches']==0 and q['first_unavailable_file'] is None and
             q['next_uncovered_open_ms']>=page.stop and 0<page.n<=499,'ARCHIVE_GAP_OR_CONFLICT')
        raw=s.execute('SELECT open_ms,body,source_key FROM raw WHERE width=? AND open_ms>=? AND open_ms<? ORDER BY open_ms LIMIT 500',
                      (w,page.start,page.stop)).fetchall()
        need(len(raw)==page.n,'ARCHIVE_GAP')
        rows=[]
        for i,(opening,text,key) in enumerate(raw):
            x=json.loads(text);need(key in meta['objects'] and f'/{page.symbol}/{w}/' in key,'ARCHIVE_PROVENANCE')
            need(isinstance(x,list) and len(x)==12 and type(opening) is int and
                 int(x[0])==opening==page.start+i*page.width and int(x[6])==opening+page.width-1,'ARCHIVE_GRID')
            try:v=tuple(float(Decimal(x[k])) for k in (1,2,3,4,5,7))
            except (InvalidOperation,TypeError,ValueError,OverflowError) as exc:raise ValueError('ARCHIVE_DECIMAL') from exc
            need(ClosedBar(opening,meta['verified_after_ms'],*v).valid(page.width),'ARCHIVE_ROW')
            rows.append((opening,v))
        stamped=clock()
        need(stamped>=meta['verified_after_ms'] and still_current() is True,'ARCHIVE_PREP_REVOKED')
    return VerifiedPage(same_span(page),cache_pin,verified_version,meta['verified_after_ms'],stamped,tuple(rows))

def commit(store,prepared,page,expected_pin,current_version,wall,still_current):
    """Called only by existing data-only owner after protections. No file access."""
    now=wall()
    def valid():
        return (still_current() is True and page.valid(wall()) and
                current_version()==prepared.version and prepared.span==same_span(page))
    need(type(prepared) is VerifiedPage and prepared.cache_pin==expected_pin and
         prepared.source_known_ms<=prepared.validated_ms<=now and valid(),'ARCHIVE_RECEIPT_OR_LEASE')
    # Effective owner knowledge is when data enters this process, not when a
    # separate prep process earlier obtained its archive bytes.
    rows=tuple(ClosedBar(t,now,*v) for t,v in prepared.rows)
    need(len(rows)==page.n and all(row.open_ms==page.start+i*page.width and row.valid(page.width)
        for i,row in enumerate(rows)),'ARCHIVE_OWNER_ROWS')
    with store.db:
        store.db.execute('BEGIN IMMEDIATE')
        need(valid(),'ARCHIVE_LEASE_LOST')
        proof=store.segment(page.symbol)
        if proof is None:
            need(page.start==page.origin,'ARCHIVE_MISSING_BEGIN')
            store.db.execute('INSERT INTO segment VALUES(?,?,?)',(page.symbol,page.origin,now))
        else:need(proof.open_ms==page.origin,'ARCHIVE_MARKET_ORIGIN_CHANGED')
        for row in rows:
            e=row.open_ms+page.width
            if e%QUARTER:continue
            w=FIVE if page.width==QUARTER else QUARTER
            peer=store.db.execute('SELECT close FROM candle WHERE symbol=? AND width=? AND open_ms=? AND received_ms<=?',
              (page.symbol,w,e-w,now)).fetchone()
            need(peer is None or peer[0]==row.close,'ARCHIVE_CANONICAL_PEER_MISMATCH')
        store._record_checked_rows(page.symbol,page.width,rows)
        need(valid(),'ARCHIVE_LEASE_LOST')
    return now

class OwnerIngress:
    """One preverified data-only page; no FIFO stale work or implicit activation."""
    def __init__(self,store,active_contract_version,authority,phase,wall):
        from threading import get_ident
        self.owner=get_ident()
        self.store=store;self.version=active_contract_version
        self.authority=authority;self.phase=phase;self.wall=wall
        self.ready=None
    def offer(self,prepared,original_page,explicit_pin):
        from threading import get_ident
        if (get_ident()!=self.owner or self.ready is not None
                or type(prepared) is not VerifiedPage or prepared.span!=same_span(original_page)
                or not self.phase() or not self.authority(original_page)
                or self.version(original_page.symbol)!=prepared.version
                or prepared.cache_pin!=explicit_pin):
            return False
        self.ready=(prepared,original_page,explicit_pin)
        return True
    def tick(self):
        from threading import get_ident
        from no_sqlite_busy import no_sqlite_busy
        if get_ident()!=self.owner or self.ready is None:return None
        p,j,pin=self.ready
        # Consume or discard one attempt; materialize no trading/rate receipt.
        self.ready=None
        def still():
            return self.phase() and self.authority(j)
        if not still() or self.version(j.symbol)!=p.version:return None
        with no_sqlite_busy(self.store.db):
            return commit(self.store,p,j,pin,lambda:self.version(j.symbol),self.wall,still)
