"""Proof that public archive fallback cannot reopen the listing-date scan."""
from datetime import datetime,timezone
from threading import Event
from types import SimpleNamespace

from auto_cold_archive import AutomaticArchive
from causal_bars import MINUTE,QUARTER


def main():
    from atr_er_state import REQUIRED_QUARTERS
    policy=__import__('bounded_archive_fallback')
    plan=getattr(policy,'bounded_archive_days',None)
    assert callable(plan),'C11_NO_REQUIRED_WINDOW_ONLY_OFFICIAL_ARCHIVE_BACKUP'
    now=int(datetime(2026,11,28,12,tzinfo=timezone.utc).timestamp()*1000)
    today=int(datetime(2026,11,28,tzinfo=timezone.utc).timestamp()*1000)
    end=now//QUARTER*QUARTER
    listing=int(datetime(2020,1,1,tzinfo=timezone.utc).timestamp()*1000)
    fact=SimpleNamespace(symbol='SOURCEXUSDT',origin=listing,boundary=end)
    first=today-60*MINUTE
    next_slot=first+5*MINUTE
    nearby=today+MINUTE
    outside=end-QUARTER-REQUIRED_QUARTERS*QUARTER-MINUTE
    route=tuple(plan(fact,((outside,outside+MINUTE),(first,first+MINUTE),(next_slot,next_slot+MINUTE),
                           (nearby,nearby+MINUTE)),now))
    assert len(route)==1,'C11_ARCHIVE_BACKUP_REQUESTED_UNNEEDED_FILES'
    day,spans=route[0]
    assert day=='2026-11-27' and spans==((first,first+MINUTE),(next_slot,next_slot+MINUTE))
    assert not tuple(plan(fact,((nearby,nearby+MINUTE),),now))
    print('C11_BOUNDED_ARCHIVE_BACKUP_EXACT_REQUIRED_DAYS_PASS')


if __name__=='__main__':
    main()
