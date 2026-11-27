"""Mature overlay only. Complete c>=20 after genuine ARM, then 60% known peak.
No stage/volume gate tweak, old ATR and weak continue separately."""
def advance(old,*,E,entry,close,armed,peak,tick,original_pending,ceil_tick,fraction=.60):
    if original_pending:return old,None
    if old and close<old:
        return old,'SCHEDULE_NEXT_OPEN'
    if not armed or (not old and close<entry*1.20):return old,None
    assert fraction in (.55,.60,.65)
    new=max(old,ceil_tick(entry+fraction*(peak-entry),tick))
    assert new>=old and new>entry
    return new,('ARM' if not old else 'RAISE') if new>old else None
