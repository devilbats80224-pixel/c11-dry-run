"""Continuous certainty, not old calendar age, distinguishes reusable state."""
from types import SimpleNamespace as N

from atr_er_checkpoint import AtrErCheckpoint
from atr_er_state import REQUIRED_QUARTERS
from auto_cold_archive import AutoColdAdmission
from causal_bars import CausalBars,ClosedBar,FIVE,MINUTE,QUARTER,SegmentStart
from rpd_checkpoint import RpdState


def main():
    end=1795773600000
    cutoff=end-QUARTER
    earliest=cutoff-REQUIRED_QUARTERS*QUARTER
    listing=1577836800000
    symbol='ADVANCEDUSDT'
    fact=N(symbol=symbol,pair='ADVANCED/USDT:USDT',origin=listing,boundary=end,generation=11)
    store=CausalBars(':memory:')
    AtrErCheckpoint(store)
    old=SegmentStart(symbol,listing,listing)
    store.set_segment(old)
    prior=RpdState.start(old)
    # Not a placeholder: the original recurrence actually processed
    # authentic-shaped earlier closed bars. Nothing authentic connects it
    # to the current retained lookback.
    for i in range(300):
        t=listing+i*FIVE
        prior.advance(ClosedBar(t,t+FIVE,100,101,99,100,10,1000),t+FIVE)
    assert prior.next_open>listing
    with store.db:
        store.db.execute('INSERT INTO rpd_checkpoint VALUES (?,?)',(symbol,prior.pack()))
    store.record(symbol,MINUTE,tuple(ClosedBar(t,t+MINUTE,100,101,99,100,10,1000)
                 for t in range(earliest,cutoff,MINUTE)))
    assert not store.range(symbol,FIVE,prior.next_open,prior.next_open+FIVE,end+100)
    def not_listing(*_):
        raise AssertionError('C11_ADVANCED_STATE_TRIED_TO_REPLAY_FROM_LISTING')
    lane=AutoColdAdmission(lambda:end+100,not_listing)
    owner=N(store=store,check_store=lambda:None,tap=N(current=lambda x:x is fact),
            cold_delivery_window=lambda:True,cold_delivery_allows=lambda x:x is fact)
    try:
        lane.offer_existing(owner,fact)
        slot=lane.pending.get(symbol)
        assert slot is not None and slot.retained is not None, 'C11_ADVANCED_OLD_GAP_IGNORED_RECENT_NATIVE_WINDOW'
        assert store.segment(symbol)==old
    finally:
        lane.close()
        store.db.close()
    print('C11_ADVANCED_LONG_GAP_PREFERS_VERIFIED_RECENT_BOOTSTRAP')


if __name__=='__main__':
    main()
