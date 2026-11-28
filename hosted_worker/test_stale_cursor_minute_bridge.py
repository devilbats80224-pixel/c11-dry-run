"""A previously unseen contract with a retained checkpoint must not go to listing."""
from types import SimpleNamespace as N
from threading import get_ident
from prepared_owner_history import BoundOwnerHistory

from admission_minute_job import make_job
from causal_bars import CausalBars, ClosedBar, FIVE, SegmentStart
from native_minute_admission_loop import MinuteAdmissionLoop
from rpd_checkpoint import RpdState
from native_cold_minute_page import M, PAGE, Q

T = 1795773600000
TOKEN = 'a' * 32
ORIGIN = T - 365 * 96 * Q



def owner_replay(*, gap_quarters=None, primary_occupied=None):
    symbol = '陌生候選USDT'
    first = T - (300 if gap_quarters is None else gap_quarters) * Q
    origin = first - Q
    wall = T + 1000
    fact = N(symbol=symbol, origin=origin, boundary=T, generation=8)
    store = CausalBars(':memory:')
    proof = SegmentStart(symbol, origin, origin + M)
    store.set_segment(proof)
    state = RpdState.start(proof)
    for t in range(origin, first, FIVE):
        state.advance(ClosedBar(t,t+FIVE,100,104,99,101,10,1000),first)
    with store.db:
        store.db.execute('INSERT INTO rpd_checkpoint VALUES (?,?)',(symbol,state.pack()))
    roster = [fact]
    calls = []

    class Lane:
        def __init__(self, valid):
            self.valid = valid
        def offer(self, job):
            assert self.valid(job)
            self.job = job
            calls.append((job.start, job.stop))
            return True
        def tick(self):
            j = self.job
            rows = [[t,'100','104','99','101','10',t+M-1,'1000',5,'4','400','0']
                    for t in range(j.start,j.stop,M)]
            return j,(wall-1,wall,rows)
        def close(self):
            pass

    def wrong_archive(_owner, _fact):
        raise AssertionError('C11_AUTHENTICATED_STALE_CURSOR_SENT_TO_ARCHIVE_FALLBACK')

    owner = N(
        store=store,check_store=lambda:None,run=N(wall=lambda:wall,queue=N(inflight={})),
        tap=N(current=lambda f:f in roster),cold_delivery_window=lambda:True,
        cold_prefix=N(offer_existing=wrong_archive))
    loop = MinuteAdmissionLoop(owner,Lane)
    loop.cycle_headroom = lambda: True
    try:
        loop.enroll_cold([fact])
        if primary_occupied is not None:
            bot=object()
            slot=(first,first+PAGE) if primary_occupied=='overlap' else (T-2*Q,T-Q)
            owner.owner=get_ident()
            owner.worker=N(freqtrade=bot)
            owner.tap.facts=lambda:tuple(roster)
            owner.pending=owner.opg_recovery=None
            owner.primary_admission=N(tick=lambda *_,**__:None,pending=N(
                busy=lambda key:key==symbol,
                inflight_ranges=lambda key:(slot,) if key==symbol else ()))
            owner.minute_headroom=lambda:True
            owner.minute_admission=loop
            owner.cold_delivery_window=owner.phase_allows=lambda:False
            owner._after_protection=lambda _:None
            BoundOwnerHistory.after_protection(owner,bot)
            if primary_occupied=='overlap':
                assert calls==[], 'C11_COLD_DUPLICATED_OVERLAPPING_NATIVE_REQUEST'
                return
            assert calls==[(first,first+PAGE)], ('C11_COLD_STALE_CURSOR_STARVED_BY_DISJOINT_PAGE',calls)
            BoundOwnerHistory.after_protection(owner,bot)
            payload,=store.db.execute('SELECT payload FROM rpd_checkpoint WHERE symbol=?',(symbol,)).fetchone()
            assert RpdState.restore(payload).next_open==first+PAGE
            assert not store.range(symbol,M,T-Q,T,wall)
            return
        if gap_quarters is not None:
            cutoff=T-Q  # the live original Worker owns the terminal close
            expected_pages=((gap_quarters-1)*Q+PAGE-1)//PAGE
            for _ in range(2*expected_pages+2):
                loop.tick([fact])
                payload,=store.db.execute('SELECT payload FROM rpd_checkpoint WHERE symbol=?',(symbol,)).fetchone()
                if RpdState.restore(payload).next_open==cutoff:
                    break
            assert len(calls)==expected_pages and calls[0][0]==first and calls[-1][1]==cutoff
            assert all(stop==following_start for (_,stop),(following_start,_) in zip(calls,calls[1:]))
            assert all(0<stop-start<=PAGE for start,stop in calls)
            assert sum(stop-start for start,stop in calls)==(gap_quarters-1)*Q
            assert RpdState.restore(payload).next_open==cutoff
            assert not store.range(symbol,M,cutoff,T,wall)
            print('C11_RESTART_EXACT_RETAINED_CURSOR_TO_TERMINAL_HANDOFF',gap_quarters,expected_pages)
        else:
            loop.tick([fact])
            loop.tick([fact])
            assert calls == [(first,first+PAGE),(first+PAGE,first+2*PAGE)]
            assert len(store.range(symbol,M,first,first+PAGE,wall)) == 495
            payload, = store.db.execute('SELECT payload FROM rpd_checkpoint WHERE symbol=?',(symbol,)).fetchone()
            assert RpdState.restore(payload).next_open == first + PAGE
            roster.clear()
            loop.tick([fact])
            assert not store.range(symbol,M,first+PAGE,first+2*PAGE,wall)
    finally:
        loop.close()
        store.db.close()


def main():
    for symbol in ('NEWCONTRACTUSDT', '陌生候選USDT'):
        fact = N(symbol=symbol, origin=ORIGIN, boundary=T, generation=8)
        cursor = T - 300 * Q + 5 * M
        first = cursor // Q * Q
        job = make_job(fact, cursor, (), T + 1, TOKEN)
        assert job is not None, 'C11_STALE_VERIFIED_CURSOR_HAS_NO_NATIVE_MINUTE_BRIDGE'
        assert (job.start, job.stop, job.n) == (first, first + PAGE, 495)
        assert job.child()['symbol'] == symbol and job.valid(T + 1)
        assert not job.valid(T + Q)
        following = make_job(fact, job.stop, (T - 230 * Q,), T + 2, TOKEN)
        assert following is not None and following.start == job.stop
        assert make_job(fact, ORIGIN, (), T + 1, TOKEN) is None
        for age in (7*96 - 1, 7*96, 7*96 + 1, 14*96):
            old=T-age*Q
            continued=make_job(fact,old,(),T+1,TOKEN)
            assert continued is not None, 'C11_COMPATIBLE_DURABLE_CURSOR_REJECTED_BY_AGE'
            assert continued.start==old and continued.stop==old+PAGE
        assert make_job(fact, ORIGIN-Q, (), T+1, TOKEN) is None
        assert make_job(fact, T, (), T+1, TOKEN) is None
        assert make_job(fact, None, (), T + 1, TOKEN) is None
    owner_replay()
    owner_replay(primary_occupied="disjoint")
    owner_replay(primary_occupied="overlap")
    for gap in (5*96, 7*96+1, 14*96):
        owner_replay(gap_quarters=gap)
    print('C11_STALE_VERIFIED_CURSOR_AUTO_MINUTE_BRIDGE_PASS')


if __name__ == '__main__':
    main()
