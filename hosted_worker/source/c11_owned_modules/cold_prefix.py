"""Optional verified cold prefix preparation. Only the existing owner writes."""
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import csv
from dataclasses import dataclass, replace
from hashlib import sha256
import io
import json
import os
from pathlib import Path
import re
from threading import Event, get_ident
from zipfile import ZipFile

from atr_er_checkpoint import VERSION
from atr_er_state import advance, new_atr
from bridge_protocol import Refused
from causal_bars import ClosedBar, FIVE, MINUTE, QUARTER, SegmentStart
from minute_rollup import roll
from primary_only_route import NW_WARM
from rpd_checkpoint import RpdState
from exact_symbol import quote_contract_shape
from cold_archive_identity import archive_location
from stale_state_install import install as install_stale, snapshot


def unusable_retained_prior(db,prior,fact,now):
    """Only a provable never-advanced listing marker may be replaced."""
    if (prior.symbol!=fact.symbol or prior.segment is None or len(prior.segment)!=2
            or prior.segment[0]!=fact.origin or not fact.origin<=prior.segment[1]<=now
            or prior.atr):
        return False
    if prior.checkpoint is not None:
        expected=RpdState.start(SegmentStart(fact.symbol,*prior.segment))
        if RpdState.restore(prior.checkpoint)!=expected:
            return False
    # Any prior computed evidence or provenance means this is not a blank-
    # equivalent cursor, even if the current checkpoint stopped advancing.
    for table in ('atr_er_sample','cold_prefix_lineage','retained_bootstrap_contract'):
        if (db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone()
                and db.execute(f'SELECT 1 FROM {table} WHERE symbol=?',(fact.symbol,)).fetchone()):
            return False
    return True


@dataclass(frozen=True)
class ClosedPrefixSource:
    directory: Path
    manifest_sha: str
    origin: int
    cutoff: int
    review_expires_ms: int | None = None
    download_only: bool = False
    prepared_sha256: str | None = None
    reviewed_at_ms: int | None = None
    daily_fallback: bool = False
    retained_listing_ms: int | None = None


@dataclass(frozen=True)
class PreparedPrefix:
    rpd: RpdState
    atr: tuple
    quarters: tuple
    terminal_atr_samples: tuple = ()


def derive(symbol, source, wall, cancelled, *, grid_shift=0):
    """Verify official archive and derive native state without a DB or network.

    A nonzero grid shift is only used by the isolated replay at its input
    boundary. The installed lane never passes it.
    """
    root = source.directory
    if not quote_contract_shape(symbol):
        raise ValueError('COLD_SYMBOL')
    if not os.statvfs(root).f_flag & os.ST_RDONLY:
        raise ValueError('COLD_READ_ONLY_SOURCE_REQUIRED')
    if source.prepared_sha256 is not None:
        from prepared_cold_checkpoint import read
        # A declared pinned checkpoint is the entire source for this request.
        # Reject a bad record rather than silently rerunning from listing day.
        return read(symbol,source,wall,cancelled,grid_shift=grid_shift)
    manifest_path = root/'manifest.json'
    if manifest_path.is_symlink():
        raise ValueError('COLD_MANIFEST_LINK')
    with manifest_path.open('rb') as stream:
        manifest_raw = stream.read(128*1024+1)
    def read_asset(name,maximum):
        path=root/name
        if path.is_symlink() or path.stat().st_size>maximum:
            raise ValueError('COLD_ARCHIVE_SIZE')
        with path.open('rb') as stream:
            return stream.read(maximum+1)
    return derive_records(symbol,source,manifest_raw,read_asset,wall,cancelled,grid_shift=grid_shift)


def derive_records(symbol, source, manifest_raw, read_asset, wall, cancelled, *, grid_shift=0):
    """Shared original recurrence over a verified, bounded archive reader."""
    if len(manifest_raw)>128*1024 or sha256(manifest_raw).hexdigest()!=source.manifest_sha:
        raise ValueError('COLD_MANIFEST')
    manifest = json.loads(manifest_raw)
    start, end = source.origin, source.cutoff
    if not (0 < start < end and start % QUARTER == end % QUARTER == 0
            and type(manifest) is list and 0 < len(manifest) <= 60):
        raise ValueError('COLD_SCOPE')
    known = max(x['actual_received_ms'] for x in manifest)
    if not end <= known <= wall():
        raise ValueError('COLD_NOT_YET_AVAILABLE')
    available = max(known,end+grid_shift)
    if available > wall():
        raise ValueError('COLD_GRID_NOT_AVAILABLE')
    recurrence = ArchiveRecurrence(symbol,start,grid_shift)
    names = set()
    archive_bytes = 0
    for item in manifest:
        name,size,digest = archive_item(symbol,item)
        archive_bytes += size
        if cancelled.is_set() or archive_bytes>26*1024*1024:
            raise ValueError('COLD_ARCHIVE_BOUNDED')
        if name in names:
            raise ValueError('COLD_ARCHIVE_IDENTITY')
        names.add(name)
        blob=read_asset(name,size)
        checksum=read_asset(name+'.CHECKSUM',512)
        verify_archive_blob(name,size,digest,blob,checksum)
        recurrence.consume(name,blob,end,available,cancelled)
    return recurrence.finish(end,available)


def archive_item(symbol,item):
    name,digest,size = (item[k] for k in ('name','sha256','bytes'))
    match = re.fullmatch(rf'{re.escape(symbol)}-1m-(\d{{4}}-\d{{2}}(?:-\d{{2}})?)\.zip',name)
    if (not match or (name,item['url'])!=archive_location(symbol,match[1])
            or type(size) is not int or not 0<size<=2*1024*1024
            or not re.fullmatch('[0-9a-f]{64}',digest)):
        raise ValueError('COLD_ARCHIVE_IDENTITY')
    return name,size,digest


def verify_archive_blob(name,size,digest,blob,checksum):
    if (len(blob)!=size or sha256(blob).hexdigest()!=digest
            or checksum.decode('utf-8').split() not in ([digest,name],[digest,'*'+name])):
        raise ValueError('COLD_ARCHIVE_HASH')


class ArchiveRecurrence:
    """Causal state only. No raw archive survives a call to consume."""
    def __init__(self,symbol,start,shift):
        self.symbol,self.start,self.shift=symbol,start,shift
        self.minute=start
        self.pack=[]
        self.rpd=None
        self.atr={width:new_atr() for width in (FIVE,QUARTER)}
        self.quarters=deque(maxlen=NW_WARM)

    def consume(self,name,blob,end,available,cancelled):
        if self.rpd is None:
            origin=self.start+self.shift
            self.rpd=RpdState.start(SegmentStart(self.symbol,origin,available))
        with ZipFile(io.BytesIO(blob)) as archive:
            members=archive.infolist()
            if (len(members)!=1 or members[0].filename!=name.removesuffix('.zip')+'.csv'
                    or members[0].file_size>16*1024*1024):
                raise ValueError('COLD_ARCHIVE_MEMBER')
            with archive.open(members[0]) as payload:
                self.consume_rows(csv.reader(io.TextIOWrapper(payload,encoding='utf-8',newline='')),
                                  end,available,cancelled)

    def consume_rows(self,rows,end,available,cancelled):
        for i,row in enumerate(rows):
            if not i and row and row[0] in ('open_time','open_time_ms'):
                continue
            if len(row)!=12:
                raise ValueError('COLD_ARCHIVE_GRID')
            opening=int(row[0])
            if self.minute==self.start and opening>self.start:
                raise ValueError('COLD_ARCHIVE_ORIGIN_GAP')
            if opening!=self.minute or int(row[6])!=self.minute+MINUTE-1 or self.minute>=end:
                raise ValueError('COLD_ARCHIVE_GRID')
            row[0],row[6],row[8]=opening+self.shift,opening+self.shift+MINUTE-1,int(row[8])
            self.minute+=MINUTE
            self.pack.append(row)
            if len(self.pack)!=15:
                continue
            if cancelled.is_set():
                raise ValueError('COLD_CANCELLED')
            for width,state in self.atr.items():
                count=width//MINUTE
                for offset in range(0,15,count):
                    first=self.pack[offset][0]
                    combined=roll(self.pack[offset:offset+count],first,width)
                    bar=ClosedBar(first,available,*(float(combined[k]) for k in (1,2,3,4,5,7)))
                    if not bar.valid(width):
                        raise ValueError('COLD_ARCHIVE_BAR')
                    advance(state,bar,width)
                    if width==FIVE:
                        self.rpd.advance(bar,available)
                    else:
                        self.quarters.append(bar)
            self.pack.clear()

    def finish(self,end,available):
        if self.pack or self.minute!=end or self.rpd is None or self.rpd.next_open!=end+self.shift:
            raise ValueError('COLD_ARCHIVE_INCOMPLETE')
        if self.rpd.observed>available:
            raise ValueError('COLD_ARCHIVE_CLOCK')
        return PreparedPrefix(replace(self.rpd,observed=available),tuple(self.atr.items()),
                              tuple(replace(bar,received_ms=available) for bar in self.quarters))


class ColdPrefixAdmission:
    def __init__(self, sources, wall, *, prepare=derive, prewarm_idle=False):
        if any(type(source) is not ClosedPrefixSource for source in sources.values()):
            raise ValueError('COLD_EXACT_SOURCE_REQUIRED')
        self.sources = dict(sources)
        self.prepare, self.wall, self.thread = prepare,wall,get_ident()
        self.pool = ThreadPoolExecutor(max_workers=1,thread_name_prefix='c11-cold-prefix')
        self.pending = {}
        self.active = None
        self.ready = {}
        self.failed = set()
        self.repair_prior = {}
        self.prewarm_idle=prewarm_idle
        self.prewarming=None
        self.prewarmed={}

    def _start(self,symbol,*,idle=False):
        cancel=Event()
        future=self.pool.submit(self.prepare,symbol,self.sources[symbol],self.wall,cancel)
        self.active=symbol,cancel,future
        self.prewarming=symbol if idle else None

    def cancel_idle(self):
        if (self.active is not None and self.prewarming is not None and
                self.prewarming not in self.pending):
            self.active[1].set()

    def _idle_source(self,now):
        # Only already-reviewed, locally cached raw archive material. No
        # exchange request, roster manufacture, new worker or owner write.
        boundary=now//QUARTER*QUARTER
        def ready_for_preparation(s):
            return (not s.download_only and s.prepared_sha256 is None
                    and (s.reviewed_at_ms is None or s.reviewed_at_ms<=now)
                    and (s.review_expires_ms is None or now<s.review_expires_ms)
                    and boundary-NW_WARM*QUARTER<=s.cutoff<=boundary-QUARTER)
        self.prewarmed={sym:value for sym,value in self.prewarmed.items()
                        if ready_for_preparation(self.sources[sym])}
        return next((sym for sym,s in sorted(self.sources.items())
                    if sym not in self.failed and sym not in self.prewarmed
                    and ready_for_preparation(s)),None)

    def offer(self, fact):
        source = self.sources.get(fact.symbol)
        if source is None:
            return 'COLD_PREFIX_NO_REVIEWED_SOURCE'
        if source.review_expires_ms is not None and self.wall()>=source.review_expires_ms:
            return 'COLD_PREFIX_REVIEW_EXPIRED'
        if (fact.origin != source.origin or
                not fact.boundary-NW_WARM*QUARTER<=source.cutoff<=fact.boundary-QUARTER):
            return 'COLD_PREFIX_RANGE_HELD'
        if fact.symbol in self.failed:
            return 'COLD_PREFIX_REFUSED'
        self.pending[fact.symbol] = fact
        if fact.symbol in self.prewarmed:
            self.ready[fact.symbol]=self.prewarmed[fact.symbol]
        return 'COLD_PREFIX_QUEUED'

    def offer_existing(self,owner,fact):
        source=self.sources.get(fact.symbol)
        if source is None or fact.symbol=='OPGUSDT':
            return 'NO_VERIFIED_LOCAL_REPAIR_SOURCE'
        # Enrollment is a bounded read; the worker and install retain their
        # own protection check and independently recheck authority.
        if get_ident()!=self.thread or not owner.tap.current(fact):
            return 'LOCAL_REPAIR_NOT_CURRENT'
        if fact.symbol not in self.pending:
            owner.check_store()
            old=snapshot(owner.store.db,fact.symbol)
            if not old.segment or old.segment[0]!=fact.origin:
                return 'LOCAL_REPAIR_ORIGIN_MISMATCH'
            status=self.offer(fact)
            if status=='COLD_PREFIX_QUEUED':
                self.repair_prior[fact.symbol]=old
            return status
        if fact.symbol not in self.repair_prior:
            return 'LOCAL_REPAIR_INDEPENDENT_WORK_PENDING'
        return self.offer(fact)

    def _install_existing(self,owner,f,prefix,prior,source,now,allowed):
        def authority():
            return (allowed() and owner.tap.current(f)
                    and (source.review_expires_ms is None or self.wall()<source.review_expires_ms))
        if type(prefix) is not PreparedPrefix:
            return False
        rpd=prefix.rpd
        if (rpd.next_open!=source.cutoff
                or prior.segment is None or prior.segment[1]>now
                or not f.boundary-NW_WARM*QUARTER<=rpd.next_open<=f.boundary-QUARTER):
            return False
        state=replace(rpd,observed=now).pack()
        atr=tuple((f.symbol,w,VERSION,f.origin,prior.segment[1],now,rpd.next_open,
                   json.dumps(data,allow_nan=False)) for w,data in prefix.atr)
        rows=tuple(replace(b,received_ms=now) for b in prefix.quarters)
        # The worker result is derived from this reviewed manifest and native
        # original recurrence. No supplied state is trusted by field names.
        def matched(payload,checkpoints,closed):
            return (payload==state and checkpoints==atr and closed==rows
                    and bool(rows) and rows[-1].open_ms+QUARTER==source.cutoff
                    and all(b.valid(QUARTER) for b in rows)
                    and all(b.open_ms-a.open_ms==QUARTER for a,b in zip(rows,rows[1:])))
        return install_stale(owner.store.db,prior,origin=f.origin,cutoff=rpd.next_open,
             source_ready=rpd.observed,now=now,manifest_sha=source.manifest_sha,
             rpd=state,atr=atr,closed_rows=rows,states_match=matched,authority=authority,
             write_closed=lambda verified:owner.store._record_checked_rows(f.symbol,QUARTER,verified))

    def refresh_delivery_facts(self,owner,facts):
        for f in facts:
            old=self.pending.get(f.symbol)
            if old is not None and old.origin==f.origin and old.pair==f.pair and owner.tap.current(f):
                self.pending[f.symbol]=f

    def tick(self, owner, *, ready_only=False, prepare_only=False, allow_idle=True):
        allowed=owner.cold_delivery_window if ready_only or prepare_only else owner.phase_allows
        if get_ident()!=self.thread or not allowed():
            return
        if self.active is not None:
            symbol,cancel,future = self.active
            if future.done():
                idle=self.prewarming==symbol
                self.active=None
                self.prewarming=None
                try:
                    result=future.result()
                    if not cancel.is_set():
                        if idle and type(result) is PreparedPrefix:
                            self.prewarmed[symbol]=result
                        if symbol in self.pending:
                            self.ready[symbol]=result
                except (Refused,ValueError,OSError,TypeError,KeyError,RuntimeError) as error:
                    self.pending.pop(symbol,None)
                    self.repair_prior.pop(symbol,None)
                    if not cancel.is_set() and error.args not in (('COLD_NOT_YET_AVAILABLE',),('COLD_GRID_NOT_AVAILABLE',)):
                        self.failed.add(symbol)
        for symbol,f in tuple(self.pending.items()):
            if ready_only and (symbol not in self.ready or not owner.cold_delivery_allows(f)):
                continue
            expires=self.sources[symbol].review_expires_ms
            if expires is not None and self.wall()>=expires:
                self.pending.pop(symbol)
                self.ready.pop(symbol,None)
                self.repair_prior.pop(symbol,None)
                if self.active is not None and self.active[0]==symbol:
                    self.active[1].set()
                continue
            if not owner.tap.current(f):
                self.pending.pop(symbol)
                self.ready.pop(symbol,None)
                self.repair_prior.pop(symbol,None)
                if self.active is not None and self.active[0]==symbol:
                    self.active[1].set()
                continue
            if symbol in self.ready:
                if prepare_only:continue
                try:
                    committed = (self.install(owner,f,self.ready[symbol],early=True) if ready_only
                                 else self.install(owner,f,self.ready[symbol]))
                except (Refused, ValueError, TypeError):
                    self.failed.add(symbol)
                    committed = True
                if committed:
                    self.pending.pop(symbol)
                    self.ready.pop(symbol)
                    self.repair_prior.pop(symbol,None)
                return True
            if not ready_only and self.active is None and allowed():
                self._start(symbol)
                return
        if (allow_idle and self.prewarm_idle and not ready_only and not self.pending
                and self.active is None and owner.phase_allows()):
            symbol=self._idle_source(self.wall())
            if symbol is not None:
                self._start(symbol,idle=True)

    def install(self, owner, f, prefix, *, early=False):
        allowed=(lambda:owner.cold_delivery_allows(f)) if early else owner.phase_allows
        if not allowed() or not owner.tap.current(f):
            return False
        owner.check_store()
        state = prefix.rpd
        now = self.wall()
        source=self.sources[f.symbol]
        expires=source.review_expires_ms
        if expires is not None and now>=expires:
            return False
        origin=f.origin
        bootstrap=source.retained_listing_ms is not None
        if bootstrap:
            from atr_er_state import REQUIRED_QUARTERS
            if (source.retained_listing_ms!=f.origin or not f.origin<source.origin
                    or source.origin%QUARTER or source.cutoff!=state.next_open
                    or source.cutoff-source.origin!=REQUIRED_QUARTERS*QUARTER
                    or tuple(w for w,_ in prefix.terminal_atr_samples)!=(FIVE,QUARTER)):
                self.failed.add(f.symbol)
                return True
            origin=source.origin
        if ((state.symbol,state.origin)!=(f.symbol,origin)
                or not state.next_open<=state.observed<=now
                or state.next_open>f.boundary-QUARTER):
            self.failed.add(f.symbol)
            return True
        prior=self.repair_prior.get(f.symbol)
        if prior is not None and not bootstrap:
            # Drop this old attempt if a new state won the race; next owner
            # tick reevaluates the current cursor. Do not rewrite recent data.
            if snapshot(owner.store.db,f.symbol)!=prior:
                return True
            installed=self._install_existing(owner,f,prefix,prior,source,now,allowed)
            return installed if early else True
        db = owner.store.db
        with db:
            db.execute('BEGIN IMMEDIATE')
            if not allowed() or not owner.tap.current(f):
                return False
            if prior is not None:
                if snapshot(db,f.symbol)!=prior or not unusable_retained_prior(db,prior,f,now):
                    return True
                db.execute('CREATE TABLE IF NOT EXISTS retained_bootstrap_prior ('
                           'symbol TEXT NOT NULL, start_ms INTEGER NOT NULL, '
                           'listing_ms INTEGER NOT NULL, proven_ms INTEGER NOT NULL, '
                           'previous_rpd TEXT, manifest_sha TEXT NOT NULL, PRIMARY KEY(symbol,start_ms))')
                db.execute('INSERT INTO retained_bootstrap_prior VALUES (?,?,?,?,?,?)',
                           (f.symbol,origin,*prior.segment,prior.checkpoint,source.manifest_sha))
                db.execute('DELETE FROM rpd_checkpoint WHERE symbol=?',(f.symbol,))
                db.execute('DELETE FROM segment WHERE symbol=?',(f.symbol,))
            elif any(db.execute(f'SELECT 1 FROM {table} WHERE symbol=?',(f.symbol,)).fetchone()
                     for table in ('segment','rpd_checkpoint','atr_er_checkpoint')):
                return True
            db.execute('INSERT INTO segment VALUES (?,?,?)',(f.symbol,origin,now))
            db.execute('INSERT INTO rpd_checkpoint VALUES (?,?)',(f.symbol,replace(state,observed=now).pack()))
            for width,atr in prefix.atr:
                db.execute('INSERT INTO atr_er_checkpoint VALUES (?,?,?,?,?,?,?,?)',
                           (f.symbol,width,VERSION,origin,now,now,state.next_open,json.dumps(atr,allow_nan=False)))
            owner.store._record_checked_rows(f.symbol,QUARTER,tuple(
                ClosedBar(b.open_ms,now,b.op,b.high,b.low,b.close,b.volume,b.quote) for b in prefix.quarters))
            db.execute('CREATE TABLE IF NOT EXISTS cold_prefix_lineage (symbol TEXT PRIMARY KEY, '
                       'origin INTEGER NOT NULL, cutoff INTEGER NOT NULL, manifest_sha TEXT NOT NULL, '
                       'source_ready INTEGER NOT NULL, owner_ready INTEGER NOT NULL)')
            db.execute('INSERT INTO cold_prefix_lineage VALUES (?,?,?,?,?,?)',
                       (f.symbol,origin,state.next_open,source.manifest_sha,state.observed,now))
            if bootstrap:
                db.execute('INSERT INTO retained_bootstrap_contract VALUES (?,?,?,?,?)',
                           (f.symbol,f.origin,origin,now,source.manifest_sha))
                # The original feature reader needs both the new quarter
                # and the previous one; do not silently skip its first close.
                for width, sample in prefix.terminal_atr_samples:
                    count=(source.cutoff-origin)//width
                    if sample['known'] is not True or sample['consecutive_bars']!=count:
                        raise ValueError('C11_RETAINED_BOOTSTRAP_ATR_SAMPLE')
                    db.execute('INSERT INTO atr_er_sample VALUES (?,?,?,?,?,?,?,?,?)',
                               (f.symbol,width,source.cutoff,now,1,count,sample['atr'],
                                sample['multiplier'],sample['signed']))
            if (not allowed() or not owner.tap.current(f)
                    or (expires is not None and self.wall()>=expires)):
                db.rollback()
                return False
        return True

    def close(self):
        if self.active is not None:
            self.active[1].set()
        self.pool.shutdown(wait=False,cancel_futures=True)
