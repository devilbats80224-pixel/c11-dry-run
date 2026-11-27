"""Optional data ingress for the existing history owner; no trading views."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Event,get_ident
from types import MappingProxyType
from archive_handoff import prepare,same_span

@dataclass(frozen=True)
class ArchiveSource:
    path:str
    digest:str
    version:str
    market_items:tuple
    ends_ms:tuple

class ArchivePrepLane:
    """One readonly file worker; never given a DB, bot, limiter or entry authority."""
    def __init__(self,sources,wall,worker_prepare=prepare,*,cold_prefix_sources=None,public_cold_archive=False,
                 discover_unseeded=False):
        if type(sources) is not dict or not all(type(x) is ArchiveSource for x in sources.values()):
            raise ValueError('EXACT_ARCHIVE_SOURCE_REQUIRED')
        if type(public_cold_archive) is not bool:
            raise ValueError('C11_PUBLIC_COLD_ARCHIVE_EXACT_MODE')
        if type(discover_unseeded) is not bool or discover_unseeded and not public_cold_archive:
            raise ValueError('C11_UNSEEDED_DISCOVERY_EXACT_MODE')
        self.discover_unseeded=discover_unseeded
        self.cold_prefix_sources=MappingProxyType(dict(cold_prefix_sources or {}))
        self.public_cold_archive=public_cold_archive
        self.sources=dict(sources);self.wall=wall;self.worker_prepare=worker_prepare;self.owner=get_ident()
        self.pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='c11-archive-verify')
        self.active=None;self.ready=None;self.denials=set();self.closed=False
    def cold_admission(self,wall):
        if self.public_cold_archive:
            from cold_archive_binding import ColdSourceAdmission
            return ColdSourceAdmission(self.cold_prefix_sources,wall,discover_unseeded=self.discover_unseeded)
        from cold_prefix import ColdPrefixAdmission
        return ColdPrefixAdmission(self.cold_prefix_sources,wall)
    def current_version(self,symbol):
        src=self.sources.get(symbol)
        return src.version if src is not None else None
    def source_for(self,j):
        src=self.sources.get(j.symbol)
        if src is None or self.closed:return None
        ends=dict(src.ends_ms)
        return src if (j.width in ends and j.stop<=ends[j.width] and
            (src.version,same_span(j)) not in self.denials) else None
    def prime(self,j):
        """Pre-E cache-only handoff; j is used solely for(raw span), not for its g."""
        if get_ident()!=self.owner or self.active is not None or self.ready is not None:return False
        src=self.source_for(j)
        if src is None:return False
        ev=Event()
        f=self.pool.submit(self.worker_prepare,src.path,src.digest,src.version,src.version,
                       dict(src.market_items),j,self.wall,lambda:not ev.is_set())
        self.active=(j,src,ev,f)
        return True
    def offer(self,j):
        if get_ident()!=self.owner:return False
        if self.active is not None:
            old,src,ev,f=self.active
            if self.source_for(j) is not src or same_span(old)!=same_span(j) or ev.is_set():return False
            self.active=(j,src,ev,f)
            return True
        return self.prime(j)
    def poll(self):
        if get_ident()!=self.owner or self.active is None or not self.active[3].done():return None
        j,src,event,f=self.active;self.active=None
        try:p=f.result()
        except (ValueError,KeyError,TypeError,OSError,RuntimeError):
            # A vanished/gapped source does not wedge the original REST lane.
            if not event.is_set():self.denials.add((src.version,same_span(j)))
            return j,None,src
        return j,p,src
    def cancel(self):
        if self.active is not None:
            _,_,e,f=self.active;e.set();f.cancel()
            if f.cancelled():self.active=None
    def close(self):
        self.closed=True;self.cancel();self.pool.shutdown(wait=False,cancel_futures=True)
