"""Old placeholder checkpoint cannot force listing-day acquisition."""
from types import SimpleNamespace as N

from atr_er_checkpoint import AtrErCheckpoint
from atr_er_state import REQUIRED_QUARTERS
from auto_cold_archive import AutoColdAdmission
from causal_bars import CausalBars,ClosedBar,MINUTE,QUARTER,SegmentStart
from native_minute_admission_loop import MinuteAdmissionLoop
from rpd_checkpoint import RpdState

END=1795773600000
CUTOFF=END-QUARTER
START=CUTOFF-REQUIRED_QUARTERS*QUARTER
LISTING=1577836800000
SYMBOL='舊狀態USDT'


def main():
    f=N(symbol=SYMBOL,pair='舊狀態/USDT:USDT',origin=LISTING,boundary=END,generation=11)
    store=CausalBars(':memory:')
    AtrErCheckpoint(store)
    old=SegmentStart(SYMBOL,LISTING,LISTING)
    store.set_segment(old)
    # Valid serialized state, but a placeholder has never advanced or produced
    # an RPD observation. There is no usable continuation cursor to preserve.
    state=RpdState.start(old)
    with store.db:
        store.db.execute('INSERT INTO rpd_checkpoint VALUES (?,?)',(SYMBOL,state.pack()))
    store.record(SYMBOL,MINUTE,tuple(ClosedBar(t,t+MINUTE,100,101,99,100,10,1000)
                     for t in range(START,CUTOFF,MINUTE)))
    def unexpected(*_):
        raise AssertionError('C11_UNUSABLE_STATE_FETCHED_FROM_2020_LISTING_DAY')
    eligible=[True]
    owner=N(store=store,check_store=lambda:None,tap=N(current=lambda x:x is f and eligible[0]),
            cold_delivery_window=lambda:True,cold_delivery_allows=lambda x:x is f and eligible[0])
    lane=AutoColdAdmission(lambda:END+100,unexpected)
    owner.cold_prefix=lane
    loop=MinuteAdmissionLoop(owner,unexpected)
    try:
        loop.enroll_cold((f,))
        slot=lane.pending.get(SYMBOL)
        assert slot is not None and slot.retained is not None,(
            'C11_2020_UNUSABLE_CURSOR_BYPASSES_VERIFIED_RECENT_CANDLES',
            slot.status if slot is not None else 'NONE')
        assert store.segment(SYMBOL)==old
        lane.tick(owner,prepare_only=True)
        assert lane.active is not None
        _,prefix=lane.active[2].result(timeout=25)
        assert (prefix.rpd.origin,prefix.rpd.next_open)==(START,CUTOFF)
        eligible[0]=False
        lane.tick(owner,ready_only=True)
        assert store.segment(SYMBOL)==old, 'C11_STALE_REPLACEMENT_BYPASSED_ROSTER'
        eligible[0]=True
        lane.tick(owner,ready_only=True)
        proof=store.contract_segment(SYMBOL,LISTING,END+100)
        assert proof is not None and proof.open_ms==START
        assert store.db.execute(
            'SELECT previous_rpd FROM retained_bootstrap_prior WHERE symbol=?',(SYMBOL,)).fetchone()==(state.pack(),)
        rows=store.range(SYMBOL,MINUTE,START,CUTOFF,END+100)
        assert len(rows)==(CUTOFF-START)//MINUTE and rows[0].received_ms==START+MINUTE
    finally:
        loop.close()
        lane.close()
        store.db.close()
    print('C11_2020_UNUSABLE_STATE_RETURNS_TO_LOCAL_BOOTSTRAP')


if __name__=='__main__':
    main()
