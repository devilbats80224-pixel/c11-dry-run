"""A verified recent-origin checkpoint remains usable after another gap."""
from time import monotonic_ns
from types import SimpleNamespace as N

from atr_er_checkpoint import AtrErCheckpoint
from atr_er_state import REQUIRED_QUARTERS
from auto_cold_archive import AutoColdAdmission
from causal_bars import CausalBars,ClosedBar,MINUTE,QUARTER
from native_minute_admission_loop import MinuteAdmissionLoop
from rpd_checkpoint import RpdState

E=1795773600000
LISTING=1577836800000
CUTOFF=E-QUARTER
START=CUTOFF-REQUIRED_QUARTERS*QUARTER
SYMBOL='REBUILTUSDT'


def test_continue():
    now=[E+100]
    f=N(symbol=SYMBOL,pair='REBUILT/USDT:USDT',origin=LISTING,boundary=E,generation=10)
    current=[f]
    store=CausalBars(':memory:')
    AtrErCheckpoint(store)
    store.record(SYMBOL,MINUTE,tuple(ClosedBar(t,t+MINUTE,100,101,99,100,10,1000)
                 for t in range(START,CUTOFF,MINUTE)))
    def unexpected_archive(*_):
        raise AssertionError('C11_REBUILT_CHECKPOINT_RESTARTED_FROM_LISTING')
    due=N(_owned_original_cycle_deadline_ns=monotonic_ns()+30_000_000_000)
    o=N(store=store,check_store=lambda:None,tap=N(current=lambda x:x is current[0]),
        cold_delivery_window=lambda:True,cold_delivery_allows=lambda x:x is current[0],
        run=N(queue=N(inflight=()),wall=lambda:now[0]),worker=N(freqtrade=due))
    lane=AutoColdAdmission(lambda:now[0],unexpected_archive)
    o.cold_prefix=lane
    requests=[]
    def make(authority):
        def offer(job):
            assert authority(job) is True
            requests.append(job)
            return True
        return N(offer=offer,close=lambda:None)
    loop=MinuteAdmissionLoop(o,make)
    try:
        assert lane.offer_retained(o,f)=='AUTO_RETAINED_MINUTES_QUEUED'
        lane.tick(o,prepare_only=True)
        lane.active[2].result(timeout=25)
        lane.tick(o,ready_only=True)
        assert store.contract_segment(SYMBOL,LISTING,now[0]).open_ms==START
        assert RpdState.restore(store.db.execute(
            'SELECT payload FROM rpd_checkpoint WHERE symbol=?',(SYMBOL,)).fetchone()[0]).next_open==CUTOFF
        later=E+10*QUARTER
        now[0]=later+100
        current[0]=N(symbol=SYMBOL,pair=f.pair,origin=LISTING,boundary=later,generation=11)
        due._owned_original_cycle_deadline_ns=monotonic_ns()+30_000_000_000
        loop.tick((current[0],))
        assert requests,('C11_REBUILT_PROOF_REFUSED_FOR_LATER_CONTINUATION',loop.reason.get(SYMBOL))
        assert all(j.symbol==SYMBOL and j.origin==START and j.start>=START for j in requests)
        assert any(j.start<=CUTOFF<j.stop for j in requests)
        assert store.contract_segment(SYMBOL,LISTING+QUARTER,now[0]) is None
        assert store.segment(SYMBOL).open_ms==START
    finally:
        loop.close()
        lane.close()
        store.db.close()
    print('C11_ACTUAL_REBUILT_PROOF_RESUMES_AFTER_LATER_GAP')


if __name__=='__main__':
    test_continue()
