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
    owner=N(store=store,check_store=lambda:None,tap=N(current=lambda x:x is fact))
    lane=AutoColdAdmission(lambda:WALL,lambda *_:None)
    gap=old_origin+MINUTE
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
    finally:
        lane.close()
        store.db.close()


if __name__=='__main__':
    main()
    older_but_fully_retained_can_queue_without_archive()
