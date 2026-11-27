"""A previously unseen contract with a retained checkpoint must not go to listing."""
from types import SimpleNamespace as N

from admission_minute_job import make_job
from causal_bars import CausalBars, ClosedBar, FIVE, SegmentStart
from native_minute_admission_loop import MinuteAdmissionLoop
from rpd_checkpoint import RpdState
from native_cold_minute_page import M, PAGE, Q

T = 1795773600000
TOKEN = 'a' * 32
ORIGIN = T - 365 * 96 * Q



def owner_replay():
    symbol = '陌生候選USDT'
    first = T - 300 * Q
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
        assert make_job(fact, T - 2000 * Q, (), T + 1, TOKEN) is None
        assert make_job(fact, None, (), T + 1, TOKEN) is None
    owner_replay()
    print('C11_STALE_VERIFIED_CURSOR_AUTO_MINUTE_BRIDGE_PASS')


if __name__ == '__main__':
    main()
