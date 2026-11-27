"""Cold-source selection carried by the verified prepared-history factory."""
from archive_prep_lane import ArchivePrepLane
from auto_cold_archive import AutoColdAdmission, AutomaticArchive, OfficialArchiveHTTP
from cold_prefix import ColdPrefixAdmission

MODE_KEY='cold_archive_preparation'
PUBLIC_MODE='BINANCE_PUBLIC_CLOSED_1M_V1'
DISCOVERY_KEY='unseeded_archive_discovery'
DISCOVERY_MODE='BOUNDED_OFFICIAL_ARCHIVE_V1'


class ColdSourceAdmission:
    """Prefer reviewed local sources; only one prefix preparation at a time."""
    def __init__(self,reviewed,wall,*,discover_unseeded=False):
        self.reviewed=ColdPrefixAdmission({s:v for s,v in reviewed.items() if not v.download_only},wall)
        remote={s:v for s,v in reviewed.items() if v.download_only}
        archive=AutomaticArchive(OfficialArchiveHTTP(wall),wall,reviewed_remote=remote,
                                 discover_unseeded=discover_unseeded)
        self.public=AutoColdAdmission(wall,archive.start)

    def offer(self,fact):
        if fact.symbol in self.reviewed.sources:
            return self.reviewed.offer(fact)
        return self.public.offer(fact)

    def offer_existing(self,owner,fact):
        if fact.symbol in self.reviewed.sources:
            return self.reviewed.offer_existing(owner,fact)
        return self.public.offer_existing(owner,fact)

    def refresh_delivery_facts(self,owner,facts):
        # A prepared data result carries no entry authority. Revalidate it
        # against this cycle's exact candidate before any early install.
        for f in facts:
            old=self.reviewed.pending.get(f.symbol)
            slot=self.public.pending.get(f.symbol)
            local=old is not None and (old.origin,old.pair)==(f.origin,f.pair)
            remote=slot is not None and (slot.fact.origin,slot.fact.pair)==(f.origin,f.pair)
            if not (local or remote) or not owner.tap.current(f):
                continue
            if local:
                self.reviewed.pending[f.symbol]=f
            if remote:
                slot.fact=f
                slot.seen_ms=self.public.wall()

    def tick(self,owner,*,ready_only=False,prepare_only=False):
        if ready_only:
            # Both lanes can drain completed work, but neither may start work.
            if self.reviewed.tick(owner,ready_only=True):
                return
            return self.public.tick(owner,ready_only=True)
        flags={'prepare_only':True} if prepare_only else {}
        if self.public.active is not None:
            self.public.tick(owner,**flags)
            return
        self.public.retire_stale()
        self.reviewed.tick(owner,**flags)
        if self.reviewed.active is None:
            self.public.tick(owner,**flags)

    def close(self):
        self.reviewed.close()
        self.public.close()


def plan(runtime,sources,wall,history_enabled):
    mode=runtime.get(MODE_KEY)
    if MODE_KEY in runtime and (mode!=PUBLIC_MODE or history_enabled is not True):
        raise ValueError('C11_PUBLIC_COLD_ARCHIVE_MODE_OR_HISTORY_SCOPE')
    if any(source.download_only for source in sources.values()) and mode!=PUBLIC_MODE:
        raise ValueError('C11_REVIEWED_DOWNLOAD_REQUIRES_PUBLIC_HISTORY')
    if DISCOVERY_KEY in runtime and (runtime[DISCOVERY_KEY]!=DISCOVERY_MODE or mode!=PUBLIC_MODE):
        raise ValueError('C11_UNSEEDED_DISCOVERY_EXACT_MODE')
    if not sources and mode is None:
        return None
    return ArchivePrepLane({},wall,cold_prefix_sources=sources,public_cold_archive=mode==PUBLIC_MODE,
                           discover_unseeded=DISCOVERY_KEY in runtime)
