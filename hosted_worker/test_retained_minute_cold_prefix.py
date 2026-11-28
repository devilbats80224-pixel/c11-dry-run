"""Automatic cold start from the owner's verified 1m continuity, not a seed."""
from pathlib import Path
from types import SimpleNamespace as N

from atr_er_checkpoint import AtrErCheckpoint
from auto_cold_archive import AutoColdAdmission
from causal_bars import CausalBars,ClosedBar,FIVE,MINUTE,QUARTER
from native_minute_admission_loop import MinuteAdmissionLoop

END=1795773600000
ORIGIN=END-32*QUARTER
CUTOFF=END-QUARTER
WALL=END+100
SYMBOL='之前沒看過USDT'
FACT=N(symbol=SYMBOL,pair='之前沒看過/USDT:USDT',origin=ORIGIN,boundary=END)


def main():
    store=CausalBars(':memory:')
    AtrErCheckpoint(store)
    gap=ORIGIN+MINUTE
    def bar(t):
        return ClosedBar(t,t+MINUTE,100,101,99,100,10,1000)
    store.record(SYMBOL,MINUTE,tuple(bar(t) for t in range(ORIGIN,CUTOFF,MINUTE) if t!=gap))
    live=[True]
    owner=N(store=store,check_store=lambda:None,tap=N(current=lambda f:live[0] and f is FACT),
            cold_delivery_window=lambda:True,cold_delivery_allows=lambda f:live[0] and f is FACT)
    def unexpected(*_):
        raise AssertionError('C11_RETAINED_LOCAL_PATH_OPENED_NETWORK')
    lane=AutoColdAdmission(lambda:WALL,unexpected)
    try:
        offer=getattr(lane,'offer_retained',None)
        assert callable(offer),'C11_NO_AUTO_RETAINED_MINUTE_COLD_PREFIX'
        assert offer(owner,FACT) is None  # one missing original 1m must not guess
        assert store.segment(SYMBOL) is None
        store.record(SYMBOL,MINUTE,(bar(gap),))
        owner.cold_prefix=lane
        minute_owner=MinuteAdmissionLoop(owner,unexpected)
        minute_owner.enroll_cold((FACT,))
        assert lane.pending[SYMBOL].status=='AUTO_RETAINED_MINUTES_QUEUED'
        minute_owner.close()
        lane.tick(owner,prepare_only=True)
        assert lane.active is not None
        source,prefix=lane.active[2].result(timeout=5)
        assert not Path(source.directory).exists()
        assert (prefix.rpd.origin,prefix.rpd.next_open)==(ORIGIN,CUTOFF)
        assert {w for w,_ in prefix.atr}=={FIVE,QUARTER}
        live[0]=False
        lane.tick(owner,ready_only=True)
        assert store.segment(SYMBOL) is None
        live[0]=True
        lane.tick(owner,ready_only=True)
        assert store.segment(SYMBOL).open_ms==ORIGIN
        assert store.db.execute('SELECT cutoff FROM cold_prefix_lineage WHERE symbol=?',(SYMBOL,)).fetchone()==(CUTOFF,)
        assert store.db.execute('SELECT count(*) FROM rpd_checkpoint').fetchone()==(1,)
        assert store.db.execute('SELECT count(*) FROM atr_er_checkpoint').fetchone()==(2,)
    finally:
        lane.close()
        store.db.close()
    print('C11_RETAINED_UNFAMILIAR_COLD_PREFIX_PASS')


def older_but_fully_retained_can_queue_without_archive():
    old_origin=END-300*QUARTER  # older than the prior 260-quarter listing gate
    fact=N(symbol=SYMBOL,pair=FACT.pair,origin=old_origin,boundary=END)
    store=CausalBars(':memory:')
    AtrErCheckpoint(store)
    owner=N(store=store,check_store=lambda:None,tap=N(current=lambda x:x is fact),
            cold_delivery_window=lambda:True,cold_delivery_allows=lambda x:x is fact)
    def unexpected_archive(*_):
        raise AssertionError('C11_OLDER_RETAINED_HISTORY_UNEXPECTED_NETWORK_REQUEST')
    lane=AutoColdAdmission(lambda:WALL,unexpected_archive)
    gap=old_origin+260*QUARTER  # an actual missing minute at the page join
    def bar(t):
        return ClosedBar(t,t+MINUTE,100,101,99,100,10,1000)
    try:
        store.record(SYMBOL,MINUTE,tuple(bar(t) for t in range(old_origin,CUTOFF,MINUTE) if t!=gap))
        assert lane.offer_retained(owner,fact) is None, 'C11_OLDER_COLD_PREFIX_INVENTED_A_MISSING_MINUTE'
        store.record(SYMBOL,MINUTE,(bar(gap),))
        assert lane.offer_retained(owner,fact)=='AUTO_RETAINED_MINUTES_QUEUED',(
            'C11_OLDER_FULLY_RETAINED_COLD_PREFIX_WRONGLY_REJECTED')
        assert len(lane.pending[SYMBOL].retained[1])==(CUTOFF-old_origin)//MINUTE
        assert store.segment(SYMBOL) is None  # only queue, no premature authorization
        lane.tick(owner,prepare_only=True)
        source,prefix=lane.active[2].result(timeout=25)
        assert (source.origin,prefix.rpd.origin,prefix.rpd.next_open)==(old_origin,old_origin,CUTOFF)
        assert tuple(b.open_ms for b in prefix.quarters)==tuple(range(CUTOFF-260*QUARTER,CUTOFF,QUARTER))
        assert {width for width,_ in prefix.atr}=={FIVE,QUARTER}
        lane.tick(owner,ready_only=True)
        assert store.segment(SYMBOL).open_ms==old_origin
        assert store.db.execute('SELECT COUNT(*) FROM rpd_checkpoint WHERE symbol=?',(SYMBOL,)).fetchone()==(1,)
        assert store.db.execute('SELECT COUNT(*) FROM atr_er_checkpoint WHERE symbol=?',(SYMBOL,)).fetchone()==(2,)
        print('C11_OLDER_RETAINED_FULL_NATIVE_CHECKPOINT_WITHOUT_REDOWNLOAD')
    finally:
        lane.close()
        store.db.close()


def old_listing_with_blank_state_uses_recent_required_rows():
    # Published market identity is old, but there is no checkpoint to resume.
    # Supply only actual usable recent 1m rows, not a fake market start or 2020 replay.
    listing=1_577_836_800_000  # 2020-01-01 UTC, exact quarter boundary
    fact=N(symbol=SYMBOL,pair=FACT.pair,origin=listing,boundary=END)
    store=CausalBars(':memory:')
    AtrErCheckpoint(store)
    owner=N(store=store,check_store=lambda:None,tap=N(current=lambda x:x is fact),
            cold_delivery_window=lambda:True,cold_delivery_allows=lambda x:x is fact)
    def unexpected_archive(*_):
        raise AssertionError('C11_BLANK_OLD_CONTRACT_TRIED_ARCHIVE_FROM_LISTING')
    lane=AutoColdAdmission(lambda:WALL,unexpected_archive)
    # 401 previous quarter bars are enough for original ATR/ER data, with
    # 260 quarters for NW15 and 260 fives for RPD. The final quarter is
    # independently received by the native terminal lane.
    earliest=CUTOFF-401*QUARTER
    try:
        store.record(SYMBOL,MINUTE,tuple(ClosedBar(t,t+MINUTE,100,101,99,100,10,1000)
                     for t in range(earliest,CUTOFF,MINUTE)))
        assert store.segment(SYMBOL) is None
        actual=lane.offer_retained(owner,fact)
        assert actual=='AUTO_RETAINED_MINUTES_QUEUED',(
            'C11_BLANK_OLD_CONTRACT_IGNORED_COMPLETE_RECENT_CANONICAL_HISTORY',actual)
        assert fact.origin==listing and store.segment(SYMBOL) is None
        lane.tick(owner,prepare_only=True)
        source,prefix=lane.active[2].result(timeout=25)
        assert (source.origin,source.retained_listing_ms,prefix.rpd.next_open)==(earliest,listing,CUTOFF)
        assert prefix.rpd.origin==earliest
        assert tuple(row.open_ms for row in prefix.quarters)==tuple(range(CUTOFF-260*QUARTER,CUTOFF,QUARTER))
        lane.tick(owner,ready_only=True)
        proof=store.contract_segment(SYMBOL,listing,WALL)
        assert proof is not None and proof.open_ms==earliest
        assert store.contract_segment(SYMBOL,listing+QUARTER,WALL) is None
        assert store.contract_segment(SYMBOL,listing,WALL-1) is None
        # The sealed build test has no writable filesystem. Round-trip the
        # database image through SQLite to a separately opened connection.
        image=store.db.serialize()
        reopened=CausalBars(':memory:')
        try:
            reopened.db.deserialize(image)
            assert reopened.contract_segment(SYMBOL,listing,WALL)==proof
            assert reopened.db.execute(
                'SELECT count(*) FROM atr_er_checkpoint WHERE symbol=?',(SYMBOL,)).fetchone()==(2,)
        finally:
            reopened.db.close()
        print('C11_2020_LISTING_RECENT_LOCAL_RECONSTRUCTION_PASS')
    finally:
        lane.close()
        store.db.close()


def complete_cached_candidates_share_one_ready_owner_pass():
    """Regression: ready candidate installation was serialized after worker completion."""
    names=('CACHEDONE','CACHEDTWO','CACHEDTHREE')
    facts=tuple(N(symbol=x+'USDT',pair=x+'/USDT:USDT',origin=ORIGIN,
                  boundary=END,generation=11) for x in names)
    store=CausalBars(':memory:')
    AtrErCheckpoint(store)
    permitted={f.symbol for f in facts}
    ready=lambda f:f.symbol in permitted and any(f is x for x in facts)
    owner=N(store=store,check_store=lambda:None,tap=N(current=ready),
            cold_delivery_window=lambda:True,cold_delivery_allows=ready)
    def unexpected_archive(*_):
        raise AssertionError('C11_READY_COLD_CANDIDATE_UNEXPECTED_REFETCH')
    lane=AutoColdAdmission(lambda:WALL,unexpected_archive)
    try:
        for f in facts:
            store.record(f.symbol,MINUTE,tuple(ClosedBar(t,t+MINUTE,100,101,99,100,10,1000)
                         for t in range(ORIGIN,CUTOFF,MINUTE)))
            assert lane.offer_retained(owner,f)=='AUTO_RETAINED_MINUTES_QUEUED'
        lane.tick(owner,prepare_only=True)
        assert set(lane.active_jobs)==permitted
        for _,_,worker in tuple(lane.active_jobs.values()):
            worker.result(timeout=10)
        revoked=facts[1].symbol
        permitted.remove(revoked)
        lane.tick(owner,ready_only=True)
        actual={f.symbol for f in facts if store.segment(f.symbol) is not None}
        assert actual==permitted, ('C11_READY_COLD_OWNER_SERIALIZED_INSTALL',
                                   'expected_current',len(permitted),'installed',len(actual))
        assert store.segment(revoked) is None
    finally:
        lane.close()
        store.db.close()
    print('C11_ALL_CURRENT_READY_COLD_PREFIXES_INSTALL_IN_OWNER_PASS')


if __name__=='__main__':
    main()
    older_but_fully_retained_can_queue_without_archive()
    old_listing_with_blank_state_uses_recent_required_rows()
    complete_cached_candidates_share_one_ready_owner_pass()
