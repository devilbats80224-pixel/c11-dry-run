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
    print('C11_BLANK_EXACT_SINGLE_MISSING_MINUTE_PLAN_PASS')


if __name__=='__main__':
    main()
