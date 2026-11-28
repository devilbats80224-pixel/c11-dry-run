"""Bounded public archive preparation after reviewed owner startup."""
from collections import deque
from concurrent.futures import Future
from copy import deepcopy
from dataclasses import astuple, dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
from requests.adapters import DEFAULT_CA_BUNDLE_PATH
import re
import ssl
from threading import Event, Lock, Thread, get_ident
from time import monotonic, sleep
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, HTTPSHandler, ProxyHandler, Request, build_opener
from zipfile import BadZipFile

from cold_prefix import (ArchiveRecurrence, ColdPrefixAdmission, ClosedPrefixSource, PreparedPrefix,
                         QUARTER, archive_item, unusable_retained_prior, verify_archive_blob)
from primary_only_route import NW_WARM
from stale_state_install import snapshot
from rpd_checkpoint import RpdState
from exact_symbol import quote_contract_shape
from cold_archive_identity import archive_location, official_archive_url
from cold_archive_release import read_bounded, checked_parts

DAY=86_400_000
ANALYSIS=NW_WARM*QUARTER
MAX_BYTES=26*1024*1024
FILES_PER_SLICE=12
MAX_FILE_BYTES=2*1024*1024
RECENT_PROOF_REQUIRED='AUTO_RECENT_MINUTES_ONLY_ORIGINAL_STATE_UNVERIFIED'
DISCOVERY_LIMIT='AUTO_ARCHIVE_DISCOVERY_LIMIT'

def inside_recent_window(f):
    # Market metadata identifies the contract, not a default fetch start.
    return (type(f.origin) is int and type(f.boundary) is int
            and 0<f.origin<f.boundary and f.origin>=f.boundary-ANALYSIS)



class Hold(Exception):
    def __init__(self, status, *, wait_ms=0, continuation=None):
        super().__init__(status)
        self.status,self.wait_ms,self.continuation=status,wait_ms,continuation


class SliceElapsed(Hold):
    """A retryable elapsed slice, unlike an invalid range or oversized asset."""


class JobLimit:
    """Cooperative cancellation and an elapsed-time budget for the archive lane."""
    def __init__(self,event,seconds=180):
        self.event=event
        self.deadline=monotonic()+seconds
        self.polls=0

    def remaining(self):
        remaining=self.deadline-monotonic()
        if remaining<=0:
            raise SliceElapsed('AUTO_ARCHIVE_BUDGET')
        return remaining

    def is_set(self):
        if self.event.is_set():
            return True
        self.remaining()
        self.polls+=1
        if self.polls%400==0:
            sleep(0)
        return self.event.is_set()

    def wait(self,delay):
        result=self.event.wait(min(delay,self.remaining()))
        return self.is_set() or result


@dataclass(frozen=True)
class Receipt:
    body: bytes
    received_ms: int


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):
        return None


class OfficialArchiveHTTP:
    """Dedicated public static download only. No proxy, redirects or credentials."""
    def __init__(self,wall):
        self.wall=wall
        self.retry_lock=Lock()
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.load_verify_locations(DEFAULT_CA_BUNDLE_PATH)
        self.opener=build_opener(ProxyHandler({}),NoRedirect(),HTTPSHandler(context=context))
        self.next_allowed_ms=0

    def get(self,url,maximum,cancel):
        if not official_archive_url(url):
            raise Hold('AUTO_ARCHIVE_UNVERIFIED')
        with self.retry_lock:
            delay=max(0,self.next_allowed_ms-self.wall())
        if delay:
            raise Hold('AUTO_ARCHIVE_TRANSIENT',wait_ms=delay)
        if cancel.is_set():
            raise Hold('AUTO_ARCHIVE_CANCELLED')
        try:
            with self.opener.open(Request(url,method='GET'),timeout=7) as response:
                if response.status!=200 or response.geturl()!=url:
                    raise Hold('AUTO_ARCHIVE_UNVERIFIED')
                size=response.headers.get('Content-Length')
                if size is not None and size.isdecimal() and int(size)>maximum:
                    raise Hold('AUTO_ARCHIVE_BUDGET')
                body=bytearray()
                while not cancel.is_set():
                    chunk=response.read(min(65536,maximum+1-len(body)))
                    if not chunk:
                        return Receipt(bytes(body),self.wall())
                    body.extend(chunk)
                    if len(body)>maximum:
                        raise Hold('AUTO_ARCHIVE_BUDGET')
                raise Hold('AUTO_ARCHIVE_CANCELLED')
        except HTTPError as exc:
            if exc.code in (404,410):
                raise Hold('AUTO_ARCHIVE_FILE_MISSING') from exc
            if exc.code in (408,425,429,500,502,503,504):
                retry=exc.headers.get('Retry-After','')
                wait=int(retry)*1000 if retry.isdecimal() and len(retry)<=5 else 30_000
                with self.retry_lock:
                    self.next_allowed_ms=max(self.next_allowed_ms,self.wall()+wait)
                raise Hold('AUTO_ARCHIVE_TRANSIENT',wait_ms=wait) from exc
            raise Hold('AUTO_ARCHIVE_UNVERIFIED') from exc
        except (URLError,TimeoutError,ConnectionError,OSError) as exc:
            if isinstance(getattr(exc,'reason',None),ssl.SSLCertVerificationError):
                raise Hold('AUTO_ARCHIVE_UNVERIFIED') from exc
            raise Hold('AUTO_ARCHIVE_TRANSIENT',wait_ms=30_000) from exc


def windows(symbol,origin,boundary):
    if not quote_contract_shape(symbol) or type(origin) is not int or not 0<origin<boundary or origin%QUARTER:
        raise Hold('AUTO_ARCHIVE_NO_VERIFIED_ORIGIN')
    cutoff=boundary//DAY*DAY
    if cutoff<=origin or cutoff>boundary-QUARTER:
        cutoff-=DAY
    if cutoff<=origin:
        raise Hold('AUTO_ARCHIVE_NO_CLOSED_DAY')
    if cutoff<boundary-ANALYSIS:
        raise Hold('AUTO_ARCHIVE_BUDGET')
    day=datetime.fromtimestamp((origin//DAY*DAY)/1000,timezone.utc)
    end=datetime.fromtimestamp(cutoff/1000,timezone.utc)
    result=[]
    while day<end:
        month=day.replace(day=1)
        following=month.replace(year=month.year+(month.month==12),month=month.month%12+1)
        # The first monthly file can begin at the verified listing origin.
        if (day.day==1 or not result) and following<=end:
            due,date=following,month.strftime('%Y-%m')
        else:
            due,date=day+timedelta(days=1),day.strftime('%Y-%m-%d')
        name,url=archive_location(symbol,date)
        result.append((name,url,int(due.timestamp()*1000)))
        day=due
    return cutoff,result


class AutomaticArchive:
    def __init__(self,transport,wall,*,max_bytes=MAX_BYTES,reviewed_remote=None,discover_unseeded=False):
        if not 0<max_bytes<=MAX_BYTES or type(discover_unseeded) is not bool:
            raise ValueError('AUTO_ARCHIVE_CONFIG')
        self.transport,self.wall,self.max_bytes=transport,wall,max_bytes
        self.discover_unseeded=discover_unseeded
        self.reviewed_remote=dict(reviewed_remote or {})
        if any(type(src) is not ClosedPrefixSource or not src.download_only
               for src in self.reviewed_remote.values()):
            raise ValueError('AUTO_ARCHIVE_REVIEWED_SOURCE')

    def reviewed_progress(self,f,source,shift):
        cutoff=source.cutoff
        expires=source.review_expires_ms
        if ((source.origin+shift)!=f.origin or not f.boundary-ANALYSIS<=cutoff+shift<=f.boundary-QUARTER
                or type(expires) is not int or self.wall()>=expires):
            raise Hold('AUTO_ARCHIVE_REVIEWED_SCOPE')
        try:
            raw=read_bounded(source.directory/'manifest.json',128*1024,immutable=True)
            reviewed=checked_parts(raw,f.symbol,dict(manifest_sha256=source.manifest_sha,
                  origin_ms=source.origin,cutoff_ms=cutoff,reviewed_at_ms=self.wall()))
            if sum(item['bytes'] for item in reviewed)>self.max_bytes:
                raise Hold('AUTO_ARCHIVE_BUDGET')
            parts=[]
            for item in reviewed:
                name,size,digest=archive_item(f.symbol,item)
                date=re.search(r'-(\d{4}-\d{2}(?:-\d{2})?)\.zip$',name)[1]
                day=datetime.strptime(date,'%Y-%m-%d' if len(date)==10 else '%Y-%m').replace(tzinfo=timezone.utc)
                stop=(day+timedelta(days=1) if len(date)==10 else
                      day.replace(year=day.year+(day.month==12),month=day.month%12+1))
                parts.append((name,item['url'],min(cutoff,int(stop.timestamp()*1000)),digest,size))
        except (ValueError,KeyError,TypeError,IndexError,OSError) as exc:
            raise Hold('AUTO_ARCHIVE_UNVERIFIED') from exc
        return ArchiveProgress(self,f.symbol,source.origin,cutoff,shift,parts,reviewed=source)

    def start(self,f,cancel,*,grid_shift=0):
        reviewed=self.reviewed_remote.get(f.symbol)
        if reviewed is not None:
            return self.reviewed_progress(f,reviewed,grid_shift).resume(cancel)
        discovered=not inside_recent_window(f)
        if discovered and not self.discover_unseeded:
            raise Hold(RECENT_PROOF_REQUIRED)
        origin,boundary=f.origin-grid_shift,f.boundary-grid_shift
        try:
            cutoff,parts=windows(f.symbol,origin,boundary)
        except Hold as exc:
            if discovered and exc.status=='AUTO_ARCHIVE_BUDGET':
                raise Hold(DISCOVERY_LIMIT) from exc
            raise
        return ArchiveProgress(self,f.symbol,origin,cutoff,grid_shift,parts,discovered=discovered).resume(cancel)

    def prepare(self,f,cancel,*,grid_shift=0):
        """Run all bounded slices for the standalone archive review driver."""
        result=self.start(f,cancel,grid_shift=grid_shift)
        while isinstance(result,ArchiveProgress):
            result=result.resume(cancel)
        return result


class ArchiveProgress:
    """Job-local authenticated cursor. Never publishes partial strategy state."""
    def __init__(self,archive,symbol,origin,cutoff,grid_shift,parts,*,reviewed=None,discovered=False):
        self.archive,self.origin,self.cutoff,self.shift=archive,origin,cutoff,grid_shift
        self.parts,self.reviewed,self.discovered=parts,reviewed,discovered
        self.recurrence=ArchiveRecurrence(symbol,origin,grid_shift)
        self.ledger=[]
        self.daily_substitution=False
        self.max_received=0

    def require_current_review(self):
        if self.reviewed is not None and self.archive.wall()>=self.reviewed.review_expires_ms:
            raise Hold('AUTO_ARCHIVE_REVIEWED_SCOPE')

    def fetch(self,name,url,closed_at,limit,cancel,*,pin=None):
        self.require_current_review()
        checksum=self.archive.transport.get(url+'.CHECKSUM',512,cancel)
        try:
            expected,spelling=checksum.body.decode('utf-8').split()
        except (ValueError,UnicodeError) as exc:
            raise Hold('AUTO_ARCHIVE_UNVERIFIED') from exc
        if not re.fullmatch('[a-f0-9]{64}',expected) or spelling not in (name,'*'+name):
            raise Hold('AUTO_ARCHIVE_UNVERIFIED')
        if pin is not None and expected!=pin[0]:
            raise Hold('AUTO_ARCHIVE_UNVERIFIED')
        if pin is not None and pin[1]>limit:
            raise Hold('AUTO_ARCHIVE_BUDGET')
        self.require_current_review()
        zipped=self.archive.transport.get(url,limit if pin is None else pin[1],cancel)
        size=len(zipped.body)
        if pin is not None and size!=pin[1]:
            raise Hold('AUTO_ARCHIVE_UNVERIFIED')
        if size>limit:
            raise Hold('AUTO_ARCHIVE_BUDGET')
        if not size:
            raise Hold('AUTO_ARCHIVE_UNVERIFIED')
        verify_archive_blob(name,size,expected,zipped.body,checksum.body)
        if not closed_at<=checksum.received_ms<=zipped.received_ms<=self.archive.wall():
            raise Hold('AUTO_ARCHIVE_NOT_AVAILABLE')
        return zipped,dict(name=name,sha256=expected,url=url,bytes=size,actual_received_ms=zipped.received_ms)

    def substitute_closed_days(self,name,closed_at):
        # Pinned manifests cannot change assets. An official unpinned
        # monthly source can be replaced only after its actual 404/410
        # or a checksum-verified file exhibits a real requested-grid gap.
        match=re.fullmatch(re.escape(self.recurrence.symbol)+r'-1m-(\d{4}-\d{2})\.zip',name)
        if self.reviewed is not None or match is None:
            return False
        start=self.recurrence.minute
        day=start//DAY*DAY
        if not 0<day<closed_at<=self.cutoff or closed_at%DAY:
            return False
        replacements=[]
        while day<closed_at:
            stamp=datetime.fromtimestamp(day/1000,timezone.utc)
            if stamp.strftime('%Y-%m')!=match[1]:
                return False
            n,u=archive_location(self.recurrence.symbol,stamp.strftime('%Y-%m-%d'))
            day+=DAY
            replacements.append((n,u,day))
        i=len(self.ledger)
        self.parts=tuple(self.parts[:i])+tuple(replacements)+tuple(self.parts[i+1:])
        self.daily_substitution=True
        return True

    def resume(self,event):
        cancel=JobLimit(event)
        self.require_current_review()
        # Bound one pass, not the amount of required candidate history.
        # Reviewed manifests retain their explicitly approved byte bundle.
        budget=self.archive.max_bytes
        if self.reviewed is not None:
            budget-=sum(item['bytes'] for item in self.ledger)
        initial_count=len(self.ledger)
        for count in range(FILES_PER_SLICE):
            if len(self.ledger)==len(self.parts):
                break
            if count and budget<MAX_FILE_BYTES:
                break
            if budget<=0:
                raise Hold(DISCOVERY_LIMIT if self.discovered else 'AUTO_ARCHIVE_BUDGET')
            try:
                if cancel.is_set():
                    raise Hold('AUTO_ARCHIVE_CANCELLED')
                name,url,closed_at,*pin=self.parts[len(self.ledger)]
                try:
                    zipped,item=self.fetch(name,url,closed_at,min(budget,MAX_FILE_BYTES),cancel,pin=pin or None)
                    available=max(zipped.received_ms,closed_at+self.shift)
                    if available>self.archive.wall():
                        raise Hold('AUTO_ARCHIVE_NOT_AVAILABLE')
                except Hold as exc:
                    if (exc.status in ('AUTO_ARCHIVE_FILE_MISSING','AUTO_ARCHIVE_BUDGET') and not pin and
                            self.substitute_closed_days(name,closed_at)):
                        # Keep the verified cursor and allow a peer to run
                        # before trying freshly planned daily assets.
                        return self
                    if exc.status in ('AUTO_ARCHIVE_TRANSIENT','AUTO_ARCHIVE_FILE_MISSING','AUTO_ARCHIVE_NOT_AVAILABLE'):
                        raise Hold(exc.status,wait_ms=exc.wait_ms,continuation=self) from exc
                    if self.discovered and exc.status=='AUTO_ARCHIVE_BUDGET':
                        raise Hold(DISCOVERY_LIMIT) from exc
                    raise
                # A time slice may end after the parser has updated several
                # bars. Publish only the cursor after a whole verified file.
                candidate=deepcopy(self.recurrence)
                candidate.consume(name,zipped.body,closed_at,available,cancel)
                if candidate.minute!=closed_at or candidate.pack:
                    raise ValueError('COLD_ARCHIVE_INCOMPLETE')
                if cancel.is_set():
                    raise Hold('AUTO_ARCHIVE_CANCELLED')
            except SliceElapsed as exc:
                if len(self.ledger)>initial_count:
                    return self
                # Give peers an opportunity and retry from a verified cursor
                # rather than spin on a file that cannot finish in one slice.
                raise Hold('AUTO_ARCHIVE_SLICE_RETRY',continuation=self) from exc
            except (ValueError,RuntimeError,OSError,TypeError,KeyError,BadZipFile) as exc:
                if event.is_set():
                    raise Hold('AUTO_ARCHIVE_CANCELLED') from exc
                if exc.args in (('COLD_ARCHIVE_ORIGIN_GAP',),('COLD_ARCHIVE_GRID',),('COLD_ARCHIVE_INCOMPLETE',)):
                    # The failed parse used a disposable recurrence copy.
                    # Retry the same interval with independently verified
                    # daily evidence; never publish a partial monthly parse.
                    if self.substitute_closed_days(name,closed_at):
                        return self
                    if exc.args==('COLD_ARCHIVE_ORIGIN_GAP',):
                        raise Hold('AUTO_ARCHIVE_ORIGIN_GAP') from exc
                    raise Hold('AUTO_ARCHIVE_CONTINUITY_GAP') from exc
                raise Hold('AUTO_ARCHIVE_UNVERIFIED') from exc
            self.recurrence=candidate
            self.ledger.append(item)
            self.max_received=max(self.max_received,available)
            budget-=item['bytes']
            del zipped
        if len(self.ledger)<len(self.parts):
            return self
        raw=json.dumps(self.ledger,sort_keys=True,separators=(',',':')).encode()
        self.require_current_review()
        source=ClosedPrefixSource(Path('/unused-automatic-download'),
                                  self.reviewed.manifest_sha if self.reviewed else sha256(raw).hexdigest(),
                                  self.origin+self.shift,self.cutoff+self.shift,
                                  self.reviewed.review_expires_ms if self.reviewed else None,
                                  daily_fallback=self.daily_substitution)
        return source,self.recurrence.finish(self.cutoff,self.max_received)


def retained_minute_prefix(f,rows,captured,expires,origin,cancel):
    """Derive from a bounded immutable copy of canonical owner 1m data."""
    from atr_er_state import advance,new_atr
    from causal_bars import FIVE,MINUTE,SegmentStart
    from closed_minute_basis import project_closed_minutes

    cutoff=f.boundary-QUARTER
    class SavedMinutes:
        def range(self,symbol,width,start,end,as_of):
            if ((symbol,width,as_of)==(f.symbol,MINUTE,captured)
                    and origin<=start<end<=cutoff
                    and (start-origin)%MINUTE==(end-origin)%MINUTE==0):
                return rows[(start-origin)//MINUTE:(end-origin)//MINUTE]
            return ()
    if cancel.is_set():
        raise Hold('AUTO_ARCHIVE_CANCELLED')
    if len(rows)!=(cutoff-origin)//MINUTE:
        raise Hold('AUTO_ARCHIVE_CONTINUITY_GAP')
    basis=SavedMinutes()
    rpd=RpdState.start(SegmentStart(f.symbol,origin,captured))
    indicators={width:new_atr() for width in (FIVE,QUARTER)}
    terminal_samples={}
    last_quarters=deque(maxlen=NW_WARM)
    # The projection read limit is per slice, not a history-age cutoff.
    # Keep one recurrence across all slices; do not re-seed it at the join.
    for start in range(origin,cutoff,NW_WARM*QUARTER):
        stop=min(cutoff,start+NW_WARM*QUARTER)
        for width,indicator in indicators.items():
            bars=project_closed_minutes(basis,f.symbol,width,start,stop,captured)
            if not bars:
                raise Hold('AUTO_ARCHIVE_CONTINUITY_GAP')
            for bar in bars:
                if cancel.is_set():
                    raise Hold('AUTO_ARCHIVE_CANCELLED')
                terminal_samples[width]=advance(indicator,bar,width)
                if width==FIVE:
                    rpd.advance(bar,captured)
                else:
                    last_quarters.append(bar)
    proof=sha256(json.dumps(['owner-verified-minutes-v2',f.symbol,f.origin,origin,cutoff,
                             [astuple(bar) for bar in rows]],separators=(',',':'),
                            allow_nan=False).encode()).hexdigest()
    listing=f.origin if origin!=f.origin else None
    return (ClosedPrefixSource(Path('/unused-retained-native-minutes'),proof,origin,cutoff,expires,
                               retained_listing_ms=listing),
            PreparedPrefix(rpd,tuple(indicators.items()),tuple(last_quarters),
                           tuple(terminal_samples.items()) if listing is not None else ()))


@dataclass
class Slot:
    fact: object
    seen_ms: int
    due_ms: int
    status: str = 'AUTO_ARCHIVE_QUEUED'
    failures: int = 0
    result: tuple | None = None
    continuation: ArchiveProgress | None = None
    retained: tuple | None = None


class AutoColdAdmission(ColdPrefixAdmission):
    """Discovery slot plus cancellable off-owner job. No order or source authority."""
    RETAIN_MS=180_000
    RETRY_MAX_MS=900_000
    # This is only a dormant memory cache, not a cap on OI20 eligibility.
    # A cached item cannot schedule a request or write anything by itself.
    DORMANT_MS=3_600_000
    DORMANT_CAP=64

    def __init__(self,wall,prepare):
        super().__init__({},wall)
        self.prepare=prepare
        self.dormant={}
        self.active_jobs={}

    def _set_active_view(self):
        # Compatibility for the reviewed-source coordinator and old tests.
        self.active=next(iter(self.active_jobs.values()),None)

    def _start_candidate(self,symbol,action,args,cancel):
        result=Future()
        self.active_jobs[symbol]=(symbol,cancel,result)
        self._set_active_view()
        def run():
            if not result.set_running_or_notify_cancel():
                return
            try:
                result.set_result(action(*args,cancel))
            except BaseException as exc:
                result.set_exception(exc)
        try:
            # One task per eligible current candidate, never one process
            # per candidate. Any request backoff is shared by the transport.
            Thread(target=run,name='c11-archive-prefix',daemon=True).start()
        except (RuntimeError,OSError):
            self.active_jobs.pop(symbol)
            self._set_active_view()
            return False
        return True

    def close(self):
        for _,cancel,_ in self.active_jobs.values():
            cancel.set()
        self.active_jobs.clear()
        self._set_active_view()
        super().close()

    def _take_dormant(self,f,now):
        key=f.symbol,f.origin
        cached=self.dormant.pop(key,None)
        if cached is None:
            return None
        expires,prefix=cached
        end=prefix.cutoff+prefix.shift
        if now>=expires or not f.boundary-ANALYSIS<=end<=f.boundary-QUARTER:
            return None
        return prefix

    def _keep_dormant(self,symbol,slot,now):
        prefix=slot.continuation
        if (type(prefix) is not ArchiveProgress or not prefix.ledger
                or prefix.origin+prefix.shift!=slot.fact.origin
                or symbol in self.active_jobs):
            return
        key=symbol,slot.fact.origin
        self.dormant.pop(key,None)
        if len(self.dormant)>=self.DORMANT_CAP:
            self.dormant.pop(next(iter(self.dormant)))
        self.dormant[key]=(now+self.DORMANT_MS,prefix)

    def offer(self,f):
        now=self.wall()
        if (not quote_contract_shape(f.symbol) or f.pair!=f.symbol[:-4]+'/USDT:USDT'
                or type(f.origin) is not int or not 0<f.origin<f.boundary or f.origin%QUARTER):
            return 'AUTO_ARCHIVE_NO_VERIFIED_ORIGIN'
        if f.symbol not in self.pending:
            # A current candidate cannot be refused due to an older roster.
            # Existing retirement and exact-fact checks still govern dispatch.
            saved=self._take_dormant(f,now)
            self.pending[f.symbol]=Slot(f,now,now,
                status='AUTO_ARCHIVE_SEGMENT_RESUMED' if saved is not None else 'AUTO_ARCHIVE_QUEUED',
                continuation=saved)
        slot=self.pending[f.symbol]
        if slot.fact.origin!=f.origin:
            if f.symbol in self.active_jobs:
                self.active_jobs[f.symbol][1].set()
            del self.pending[f.symbol]
            self.repair_prior.pop(f.symbol,None)
            return 'AUTO_ARCHIVE_NO_VERIFIED_ORIGIN'
        slot.fact,slot.seen_ms=f,now
        return slot.status

    def offer_retained(self,owner,f,*,prior=None):
        from atr_er_state import REQUIRED_QUARTERS
        from causal_bars import MINUTE
        now=self.wall()
        if (get_ident()!=self.thread or f.symbol in self.active_jobs or not owner.tap.current(f)
                or not quote_contract_shape(f.symbol) or f.pair!=f.symbol[:-4]+'/USDT:USDT'
                or type(f.origin) is not int or type(f.boundary) is not int
                or not 0<f.origin<f.boundary-QUARTER or f.origin%QUARTER
                or f.boundary%QUARTER):
            return None
        existing=self.pending.get(f.symbol)
        if existing is not None and any(item is not None for item in
                (existing.result,existing.continuation,existing.retained)):
            return None
        owner.check_store()
        store=owner.store
        if prior is not None:
            if snapshot(store.db,f.symbol)!=prior or not unusable_retained_prior(store.db,prior,f,now):
                return None
        elif (store.segment(f.symbol) is not None or store.db.execute(
                'SELECT 1 FROM rpd_checkpoint WHERE symbol=?',(f.symbol,)).fetchone()):
            return None
        cutoff=f.boundary-QUARTER
        # A blank strategy has no lifetime recurrence to resume. Derive a
        # new, explicitly labeled segment from the needed contiguous local
        # minutes. A recent listing still uses its original verified start.
        origin=max(f.origin,cutoff-REQUIRED_QUARTERS*QUARTER)
        if prior is not None and origin==f.origin:
            return None
        rows=store.range(f.symbol,MINUTE,origin,cutoff,now)
        if not rows or not owner.tap.current(f):
            return None
        self.offer(f)
        slot=self.pending.get(f.symbol)
        if slot is None:
            return None
        if prior is not None:
            self.repair_prior[f.symbol]=prior
        slot.retained=(f,rows,now,f.boundary+QUARTER,origin)
        slot.due_ms=now
        slot.status='AUTO_RETAINED_MINUTES_QUEUED'
        return slot.status

    def offer_existing(self,owner,f):
        # Automatic archive derivation is a candidate data job; replacing
        # existing state still uses the original exact prior snapshot.
        if get_ident()!=self.thread or f.symbol=='OPGUSDT' or not owner.tap.current(f):
            return 'AUTO_REPAIR_NOT_CURRENT'
        old=self.repair_prior.get(f.symbol)
        if old is None:
            owner.check_store()
            old=snapshot(owner.store.db,f.symbol)
            if old.segment is None or old.segment[0]!=f.origin:
                return 'AUTO_REPAIR_ORIGIN_MISMATCH'
            retained=self.offer_retained(owner,f,prior=old)
            if retained is not None:
                return retained
            if old.checkpoint is not None:
                prior=RpdState.restore(old.checkpoint)
                if ((prior.symbol,prior.origin)!=(f.symbol,f.origin)
                        or prior.observed>self.wall()
                        or prior.next_open>=f.boundary-ANALYSIS):
                    return 'AUTO_REPAIR_PRIOR_STILL_RECENT_OR_UNBOUND'
        status=self.offer(f)
        if (f.symbol in self.pending and self.pending[f.symbol].fact.origin==f.origin
                and status not in ('AUTO_ARCHIVE_NO_VERIFIED_ORIGIN','AUTO_ARCHIVE_QUEUE_BUDGET')):
            self.repair_prior[f.symbol]=old
        return status

    def retire_stale(self):
        now=self.wall()
        for key,(expires,_) in tuple(self.dormant.items()):
            if now>=expires:
                self.dormant.pop(key)
        for symbol,slot in tuple(self.pending.items()):
            if now-slot.seen_ms>=self.RETAIN_MS:
                self._keep_dormant(symbol,slot,now)
                self.pending.pop(symbol)
                self.repair_prior.pop(symbol,None)
                if symbol in self.active_jobs:
                    self.active_jobs[symbol][1].set()

    def settled(self,symbol,cancel,future):
        slot=self.pending.get(symbol)
        if slot is None or cancel.is_set():
            return
        try:
            result=future.result()
        except Hold as exc:
            slot.status=exc.status
            slot.failures+=1
            if exc.status in (RECENT_PROOF_REQUIRED,DISCOVERY_LIMIT,'AUTO_ARCHIVE_REVIEWED_SCOPE','AUTO_ARCHIVE_BUDGET','AUTO_ARCHIVE_NO_VERIFIED_ORIGIN','AUTO_ARCHIVE_UNVERIFIED',
                              'AUTO_ARCHIVE_ORIGIN_GAP','AUTO_ARCHIVE_CONTINUITY_GAP'):
                # This unchanged identity/window needs different verified
                # evidence or budget. Do not spin on every owner cycle.
                slot.continuation=None
                slot.due_ms=2**63-1
            else:
                slot.continuation=exc.continuation
                pause=min(self.RETRY_MAX_MS, 15_000*2**min(6,slot.failures))
                slot.due_ms=self.wall()+max(pause,exc.wait_ms)
        except (OSError,ValueError,TypeError,RuntimeError,KeyError):
            slot.continuation=None
            slot.status='AUTO_ARCHIVE_UNVERIFIED'
            slot.due_ms=2**63-1
        else:
            if isinstance(result,ArchiveProgress):
                slot.continuation=result
                slot.status=('AUTO_ARCHIVE_DAILY_FALLBACK_PLANNED' if result.daily_substitution
                             else 'AUTO_ARCHIVE_SEGMENT_VERIFIED')
                self.pending.pop(symbol)
                self.pending[symbol]=slot
            else:
                slot.continuation=None
                slot.result=result
                slot.status='AUTO_ARCHIVE_VERIFIED_WAITING_OWNER'

    def tick(self,owner,*,ready_only=False,prepare_only=False):
        if get_ident()!=self.thread:
            return
        self.retire_stale()
        allowed=owner.cold_delivery_window if ready_only or prepare_only else owner.phase_allows
        if not allowed():
            return
        for symbol,job in tuple(self.active_jobs.items()):
            if job[2].done():
                self.settled(*job)
                del self.active_jobs[symbol]
        self._set_active_view()
        # Drain completed, in-scope work before launching another fetch.
        # In insertion order a due retry could otherwise bypass the ready
        # candidate at every completion, even when no new data is needed.
        for symbol,slot in tuple(self.pending.items()):
            if slot.result is not None and (owner.cold_delivery_allows(slot.fact) if ready_only else owner.tap.current(slot.fact)):
                source,prefix=slot.result
                if (source.review_expires_ms is not None
                        and self.wall()>=source.review_expires_ms):
                    # Refreshing roster membership never extends source review.
                    # Retire this result before it can take the owner's turn.
                    slot.result=None
                    slot.continuation=None
                    slot.status='AUTO_ARCHIVE_REVIEWED_SCOPE'
                    slot.due_ms=2**63-1
                    self.repair_prior.pop(symbol,None)
                    continue
                if source.cutoff<slot.fact.boundary-ANALYSIS:
                    if ready_only:continue
                    slot.result=None
                    slot.due_ms=self.wall()
                    slot.status='AUTO_ARCHIVE_QUEUED'
                    continue
                if prepare_only:continue
                self.sources[symbol]=source
                try:
                    installed=(self.install(owner,slot.fact,prefix,early=True) if ready_only
                               else self.install(owner,slot.fact,prefix))
                except (ValueError,RuntimeError,TypeError):
                    slot.result=None
                    slot.status='AUTO_ARCHIVE_UNVERIFIED'
                    slot.due_ms=2**63-1
                    installed=False
                finally:
                    self.sources.pop(symbol,None)
                if installed:
                    self.pending.pop(symbol,None)
                    self.repair_prior.pop(symbol,None)
                    if getattr(source,'daily_fallback',False) is True:
                        emit=getattr(getattr(owner,'primary_admission',None),'gate',None)
                        if callable(emit):
                            emit('AUTO_ARCHIVE_DAILY_FALLBACK_INSTALLED',slot.fact)
                return True
        if ready_only or not allowed():
            return
        for symbol,slot in tuple(self.pending.items()):
            if not allowed():
                return
            if (symbol in self.active_jobs or slot.result is not None
                    or not owner.tap.current(slot.fact) or self.wall()<slot.due_ms):
                continue
            cancel=Event()
            if slot.retained is not None:
                action,args=retained_minute_prefix,slot.retained
            elif slot.continuation is None:
                action,args=self.prepare,(slot.fact,)
            else:
                action,args=slot.continuation.resume,()
            if self._start_candidate(symbol,action,args,cancel):
                slot.retained=None
                slot.status='AUTO_ARCHIVE_FETCHING'
            else:
                # OS refused to start an actual worker. Keep the request
                # pending for the next cycle, never mark its history complete.
                return
