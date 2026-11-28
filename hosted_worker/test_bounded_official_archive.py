"""Proof that public archive fallback cannot reopen the listing-date scan."""
from datetime import datetime,timezone
from hashlib import sha256
from io import BytesIO
from zipfile import ZipFile
from threading import Event
from types import SimpleNamespace

from auto_cold_archive import Hold,Receipt
from cold_archive_identity import archive_location
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
    fact=SimpleNamespace(symbol='SOURCEXUSDT',pair='SOURCEX/USDT:USDT',generation=3,expires_ns=10**15,origin=listing,boundary=end)
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
    lookup=getattr(policy,'read_bounded_archive_day')
    name,url=archive_location(fact.symbol,day)
    raw=BytesIO()
    def row(t):
        return f'{t},100,101,99,100,10,{t+MINUTE-1},1000,2,4,400,0\n'
    with ZipFile(raw,'w') as archive:
        archive.writestr(name.removesuffix('.zip')+'.csv', ''.join(row(t) for t in
                         (first,first+MINUTE,next_slot,next_slot+MINUTE)))
    zipped=raw.getvalue()
    checksum=f'{sha256(zipped).hexdigest()}  {name}\n'.encode()
    def transcript(sidecar,received):
        calls=[]
        def get(path,maximum,cancel):
            assert not cancel.is_set()
            calls.append(path)
            body=sidecar if path==url+'.CHECKSUM' else zipped
            assert path in (url+'.CHECKSUM',url) and 0<len(body)<=maximum
            return Receipt(body,received)
        return SimpleNamespace(get=get),calls
    transport,calls=transcript(checksum,now)
    proof=lookup(fact,spans,day,lambda:now,Event(),transport=transport)
    assert [r.open_ms for r in proof.rows]==[first,next_slot]
    assert proof.received_ms==now and calls==[url+'.CHECKSUM',url]
    for bad,stamp in ((b'0'*64+checksum[64:],now),(checksum,now+1)):
        transport,_=transcript(bad,stamp)
        try:
            lookup(fact,spans,day,lambda:now,Event(),transport=transport)
        except Hold:
            pass
        else:
            raise AssertionError('C11_ARCHIVE_BACKUP_ACCEPTED_UNVERIFIED_SOURCE')
    attempted=[False]
    eligible=[True]
    committed=[]
    transport,calls=transcript(checksum,now)
    lane=policy.BoundedArchiveFallback(lambda:now,qualified=lambda _:eligible[0],
        attempted=lambda _:attempted[0],commit_raw=lambda scope,receipt:not committed.append((scope,receipt)),
        transport=transport)
    try:
        assert not lane.request(fact,spans) and not calls
        attempted[0]=True
        scoped=lane.request(fact,spans)
        assert len(scoped)==1 and not lane.request(fact,spans)
        # The primary source can fill the first gap before the archive is
        # done. The remaining gap still resolves to the same official ZIP.
        assert not lane.request(fact,(spans[-1],)), 'C11_ARCHIVE_REMAINDER_REDOWLOADS_SAME_DAY'
        next(iter(lane.pending.values()))[2].result(timeout=5)
        eligible[0]=False
        assert lane.drain()==((scoped[0],'C11_ARCHIVE_FALLBACK_SCOPE_LOST',0),)
        assert not committed
        eligible[0]=True
        scope=lane.request(fact,spans)[0]
        next(iter(lane.pending.values()))[2].result(timeout=5)
        assert lane.drain()==((scope,'C11_ARCHIVE_FALLBACK_RAW_COMMITTED',0),)
        assert len(committed)==1 and committed[0][1].rows==proof.rows
    finally:
        lane.close()
    # A new still-qualified generation can accept data-only overlap for
    # the same authenticated contract/close without reviving the old rank.
    active_generation=[fact.generation]
    receipts=[]
    transport,calls=transcript(checksum,now)
    lane=policy.BoundedArchiveFallback(lambda:now,qualified=lambda s:s.generation==active_generation[0],
        receipt_qualified=lambda s:s.symbol==fact.symbol and s.boundary==end and active_generation[0]>=s.generation,
        attempted=lambda _:True,commit_raw=lambda scope,receipt:not receipts.append((scope,receipt)),transport=transport)
    try:
        prior=lane.request(fact,spans)[0]
        active_generation[0]+=1
        next(iter(lane.pending.values()))[2].result(timeout=5)
        assert lane.drain()==((prior,'C11_ARCHIVE_FALLBACK_RAW_COMMITTED',0),)
        assert len(receipts)==1 and calls==[url+'.CHECKSUM',url]
    finally:
        lane.close()
    print('C11_BOUNDED_ARCHIVE_BACKUP_EXACT_REQUIRED_DAYS_PASS')


if __name__=='__main__':
    main()
