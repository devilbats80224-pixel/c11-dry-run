"""The 15:00 KAVA behavior, on public contract identity and synthetic prices."""
from types import SimpleNamespace as N
from unittest.mock import patch

from admission_minute_job import iter_bootstrap_jobs
from atr_er_checkpoint import AtrErCheckpoint
from atr_er_state import REQUIRED_QUARTERS
from causal_bars import CausalBars, ClosedBar, FIVE, MINUTE as M, QUARTER as Q
from cold_archive_binding import plan
from native_protection import FeatureSpan

END = 1795849200000
# Exact Binance USD-M exchangeInfo for KAVAUSDT (TRADING, PERPETUAL).
# The venue's identity timestamp is not the announced 07:00 trading start.
LISTED = 1595919900000
SYMBOL = 'KAVAUSDT'
CUTOFF = END-Q
START = CUTOFF-REQUIRED_QUARTERS*Q
MISSING = 2100
GAP = START+1000*M


def main():
    assert LISTED%FIVE==0 and LISTED%Q==FIVE
    now=END+2584
    f=N(symbol=SYMBOL,pair='KAVA/USDT:USDT',origin=LISTED,boundary=END,generation=6300)
    route=plan({'cold_archive_preparation':'BINANCE_PUBLIC_CLOSED_1M_V1',
                'unseeded_recent_minute_bootstrap':'BINANCE_SHARED_RECENT_1M_V1'}, {}, lambda:now, True)
    admitted=route.cold_admission(lambda:now)
    assert admitted.allows_unseeded_minutes(f), 'C11_KAVA_PUBLIC_LISTING_AS_QUARTER_GRID'
    known=tuple(t for t in range(START,CUTOFF,M) if not GAP<=t<GAP+MISSING*M)
    assert len(known)==3915 and len(known)+MISSING==6015
    ids=iter(range(20))
    jobs=tuple(iter_bootstrap_jobs(f,known,now,(),lambda:f'{next(ids):032x}'))
    assert [j.n for j in jobs]==[495,495,495,495,120]
    assert all(j.origin==LISTED and j.start>=START and j.stop<=CUTOFF for j in jobs)
    assert tuple(t for j in jobs for t in range(j.start,j.stop,M))==tuple(range(GAP,GAP+MISSING*M,M))
    store=CausalBars(':memory:')
    AtrErCheckpoint(store)
    o=N(store=store,check_store=lambda:None,tap=N(current=lambda x:x is f),
        cold_delivery_window=lambda:True,cold_delivery_allows=lambda x:x is f)
    def row(t):
        return ClosedBar(t,t+M,100,101,99,100,10,1000)
    def unexpected_archive(*_):
        raise AssertionError('C11_KAVA_FELL_BACK_TO_LISTING_ARCHIVE')
    admitted.public.prepare=unexpected_archive
    try:
        store.record(SYMBOL,M,tuple(row(t) for t in known))
        assert admitted.offer_retained(o,f) is None
        assert store.segment(SYMBOL) is None
        store.record(SYMBOL,M,tuple(row(t) for t in range(GAP,GAP+MISSING*M,M)))
        assert admitted.offer_retained(o,f)=='AUTO_RETAINED_MINUTES_QUEUED', 'C11_KAVA_VERIFIED_LOCAL_PREFIX_NOT_QUEUED'
        admitted.tick(o,prepare_only=True)
        admitted.public.active[2].result(timeout=25)
        admitted.tick(o,ready_only=True)
        assert store.contract_segment(SYMBOL,LISTED,now).open_ms==START
        assert store.contract_segment(SYMBOL,LISTED-FIVE,now) is None
        # The independently admitted original terminal features can advance
        # both ATR widths without inventing a quarter-aligned market listing.
        store.record(SYMBOL,FIVE,tuple(ClosedBar(t,t+FIVE,100,101,99,100,50,5000)
                     for t in range(CUTOFF,END,FIVE)))
        store.record(SYMBOL,Q,(ClosedBar(CUTOFF,END,100,101,99,100,150,15000),))
        metadata={'id':SYMBOL,'info':{'onboardDate':LISTED}}
        bot=N(exchange=N(markets={f.pair:metadata}))
        with patch('native_protection.wall_ms',lambda:now):
            status,value=FeatureSpan(store,N(query=unexpected_archive)).warm(bot,f.pair,END)
            assert status=='ATR_ER_KNOWN' and value['origin_ms']==START
            metadata['info']['onboardDate']=LISTED-FIVE
            refute,_=FeatureSpan(store,N(query=unexpected_archive)).warm(bot,f.pair,END)
            assert refute!='ATR_ER_KNOWN'
    finally:
        admitted.close()
        route.close()
        store.db.close()
    print('C11_KAVA_RECENT_GAP_AND_ORIGINAL_LISTING_PROOF')


if __name__=='__main__':
    main()
