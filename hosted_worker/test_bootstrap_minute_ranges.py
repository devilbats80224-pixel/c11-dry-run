"""Blank-state data acquisition validates exact missing minutes, not pages."""
from types import SimpleNamespace as N

from atr_er_state import REQUIRED_QUARTERS
from native_cold_minute_page import M,Q
from admission_minute_job import MinuteHistoryJob

E=1795773600000
LISTED=1577836800000
START=E-Q-REQUIRED_QUARTERS*Q
NOW=E+250


def main():
    import admission_minute_job as planner
    run=getattr(planner,'iter_bootstrap_jobs',None)
    assert callable(run),'C11_BLANK_EXACT_MISSING_MINUTE_PLANNER_NOT_WIRED'
    tickets=iter(f'{value:032x}' for value in range(1,100))
    fact=N(symbol='UNSEENUSDT',origin=LISTED,boundary=E,generation=11)
    other=N(symbol='第二個USDT',origin=LISTED,boundary=E,generation=11)
    hole=START+2*M
    second_hole=START+7*M
    full=tuple(range(START,E-Q,M))
    jobs=tuple(run(fact,tuple(x for x in full if x not in (hole,second_hole)),NOW,
                   ((second_hole,second_hole+M),),lambda:next(tickets)))
    assert len(jobs)==1,'C11_BLANK_SPARSE_RAW_GAP_REQUESTED_EXTRA_MINUTES'
    j=jobs[0]
    assert type(j) is MinuteHistoryJob and j.bootstrap is True and j.valid(NOW)
    assert (j.start,j.stop,j.n)==(hole,hole+M,1)
    assert j.child()==dict(symbol=fact.symbol,start=hole,stop=hole+M,count=1,width=M,bootstrap=True)
    peer=tuple(run(other,tuple(x for x in full if x!=second_hole),NOW,(),lambda:next(tickets)))
    assert len(peer)==1 and peer[0].start==second_hole and peer[0].symbol==other.symbol
    assert not tuple(run(fact,full,NOW,(),lambda:next(tickets)))
    assert not MinuteHistoryJob(fact.symbol,fact.generation,E,LISTED,'f'*32,hole,hole+M).valid(NOW)
    from causal_bars import CausalBars
    from cold_minute_commit import commit_bootstrap
    from bridge_protocol import Refused
    db=CausalBars(':memory:')
    try:
        row=[hole,'100','101','99','100','10',hole+M-1,'1000',2,'4','400','0']
        incorrect=[*row]
        incorrect[0]+=M
        for response,sent,received,permit in (
            ([incorrect],NOW,NOW+5,lambda:True),
            ([row],NOW,NOW-1,lambda:True),
            ([row],NOW,NOW+5,iter((True,True,False)).__next__),
        ):
            try:
                commit_bootstrap(db,j,response,sent,received,permit)
            except (ValueError,Refused):
                pass
            else:
                raise AssertionError('C11_BOOTSTRAP_RAW_INVALID_RECEIPT_ACCEPTED')
            assert not db.range(fact.symbol,M,hole,hole+M,NOW+10)
        assert commit_bootstrap(db,j,[row],NOW,NOW+5,lambda:True)==1
        assert len(db.range(fact.symbol,M,hole,hole+M,NOW+10))==1
        assert db.db.execute('SELECT width,count(*) FROM candle GROUP BY width').fetchall()==[(M,1)]
        assert db.segment(fact.symbol) is None
        assert db.db.execute('SELECT count(*) FROM rpd_checkpoint').fetchone()==(0,)
    finally:
        db.db.close()
    from cold_archive_binding import plan
    archive={'cold_archive_preparation':'BINANCE_PUBLIC_CLOSED_1M_V1'}
    modern={'unseeded_recent_minute_bootstrap':'BINANCE_SHARED_RECENT_1M_V1'}
    for config,expected in ((archive,False),({**archive,**modern},True)):
        prep=plan(config,{},lambda:NOW,True)
        lane=prep.cold_admission(lambda:NOW)
        try:
            assert lane.discover_unseeded is True and lane.allows_unseeded_minutes(fact) is expected
        finally:
            lane.close()
            prep.close()
    for invalid in (modern,{**archive,**modern,'unseeded_archive_discovery':'REVIEWED_OFFICIAL_ARCHIVE_ONLY_V1'}):
        try:
            plan(invalid,{},lambda:NOW,True)
        except ValueError:
            pass
        else:
            raise AssertionError('C11_EXACT_BOOTSTRAP_SOURCE_MODE_NOT_ENFORCED')
    print('C11_BLANK_EXACT_SINGLE_MISSING_MINUTE_PLAN_PASS')


if __name__=='__main__':
    main()
