"""One verified raw-source preparation may run before a candidate is ranked."""
from pathlib import Path
from threading import Event
from types import SimpleNamespace as N
from unittest.mock import patch

from causal_bars import QUARTER
from cold_prefix import ClosedPrefixSource, ColdPrefixAdmission, PreparedPrefix

T=1795773600000
SYMBOL='EXAMPLEUSDT'
SOURCE=ClosedPrefixSource(Path('/not-used'), 'a'*64,T-400*QUARTER,T-3*QUARTER,T+QUARTER)
RESULT=PreparedPrefix(N(symbol=SYMBOL,origin=SOURCE.origin,next_open=SOURCE.cutoff,observed=T-2*QUARTER),(),())
FACT=N(symbol=SYMBOL,origin=SOURCE.origin,boundary=T)
calls=[]

def prepare(symbol,source,wall,cancel):
    assert (symbol,source)==(SYMBOL,SOURCE)
    assert not cancel.is_set()
    calls.append(symbol)
    return RESULT

def main():
    wall=[T-QUARTER+65_000]
    allowed=[False]
    owner=N(phase_allows=lambda:allowed[0],cold_delivery_window=lambda: True,
            cold_delivery_allows=lambda f:f is FACT,tap=N(current=lambda f:f is FACT))
    lane=ColdPrefixAdmission({SYMBOL:SOURCE},lambda:wall[0],prepare=prepare)
    # Use public factory behavior when present. Old packages have no idle
    # preparation, so the same test reaches the exact first red assertion.
    lane.prewarm_idle=True
    try:
        lane.tick(owner,prepare_only=True)
        assert lane.active is None
        allowed[0]=True
        lane.tick(owner,prepare_only=True)
        assert lane.active is not None,'C11_IDLE_REVIEWED_ARCHIVE_NO_AUTOMATIC_PREPARATION'
        assert not lane.pending and len(calls)<=1
        lane.active[2].result(timeout=2)
        lane.tick(owner,ready_only=True)
        assert not lane.ready and not lane.pending and calls==[SYMBOL]
        wall[0]=T+1
        assert lane.offer(FACT)=='COLD_PREFIX_QUEUED'
        dispatched=[]
        with patch.object(lane,'install',side_effect=lambda owner,f,prefix,early:dispatched.append((f,prefix,early)) or True):
            lane.tick(owner,ready_only=True)
        assert dispatched==[(FACT,RESULT,True)] and calls==[SYMBOL]
        assert not lane.pending and not lane.ready
        wall[0]=T+QUARTER
        assert lane.offer(FACT)=='COLD_PREFIX_REVIEW_EXPIRED'
        assert calls==[SYMBOL]
    finally:
        lane.close()
    # Pending real work can displace speculative archive computation; an
    # actual witnessed candidate keeps its own queued preparation.
    second=ColdPrefixAdmission({SYMBOL:SOURCE},lambda:T,prepare=prepare)
    try:
        cancel=Event()
        second.active=(SYMBOL,cancel,N())
        second.prewarming=SYMBOL
        second.pending[SYMBOL]=FACT
        second.cancel_idle()
        assert not cancel.is_set()
        second.pending.clear()
        second.cancel_idle()
        assert cancel.is_set()
    finally:
        second.active=None
        second.close()
    print('C11_IDLE_REVIEWED_PREPARATION_PUBLISHED_ONLY_TO_CURRENT_CANDIDATE')
if __name__=='__main__':
    main()
