"""Wire order regression: recover terminal 1m before wide backup."""
import sys
from types import SimpleNamespace as N
from unittest.mock import patch

sys.path.insert(0,'/opt/c11_owned_modules')
from causal_bars import CausalBars, ClosedBar, MINUTE, QUARTER
from native_candles import NativeCandles
from bridge_protocol import Refused

END=1795773600000
SYMBOL='EXAMPLEUSDT'
PAIR='EXAMPLE/USDT:USDT'
START=END-260*QUARTER

def replay(reserved=False, older_gap=False):
    store=CausalBars(':memory:')
    omit={END-MINUTE}
    if older_gap: omit.add(START)
    store.record(SYMBOL,MINUTE,tuple(ClosedBar(t,t+MINUTE,1.,1.,1.,1.,1.,1.)
                    for t in range(START,END,MINUTE) if t not in omit))
    calls=[]
    def get(params):
        tf=params['interval']; width={'1m':MINUTE,'15m':QUARTER}[tf]
        calls.append((tf,params['startTime'],params['limit']))
        volume=str(width//MINUTE)
        return [[t,'1','1','1','1',volume,t+width-1,volume,1,'1','1','0']
                for t in range(params['startTime'],params['endTime']+1,width)]
    market=dict(id=SYMBOL,symbol=PAIR,active=True,swap=True,linear=True,settle='USDT',quote='USDT')
    candidate=N(pair=PAIR,symbol=SYMBOL,expires_ns=50_000_000)
    bot=N(exchange=N(markets={PAIR:market},_api=N(fapiPublicGetKlines=get)),
          feed=N(latest_owner_observation=(candidate,)),observed_ranked=lambda _:True,
          _c11_owned_data_lane=N(covers_native_public=lambda _s,w,*_:reserved and w==MINUTE))
    warm=NativeCandles(store)
    with patch('native_candles.time.time_ns',return_value=(END+1_000)*1_000_000), \
         patch('native_candles.time.monotonic_ns',return_value=1_000):
        try:
            warm.update_nw(bot,PAIR,SYMBOL,END)
        except Refused:
            assert reserved
    if reserved:
        assert calls==[],('C11_WAITING_FOR_OWNED_MINUTE_REQUEST_STARTED_WIDE',calls)
    else:
        expected=[('1m',END-MINUTE,1)]
        if older_gap:expected.append(('15m',START,260))
        assert calls==expected,('C11_TERMINAL_MINUTE_WAS_NOT_REFILLED_BEFORE_WIDE',calls)
        assert len(store.range(SYMBOL,QUARTER,START,END,END+1000))==260
        assert len(store.range(SYMBOL,MINUTE,END-30*MINUTE,END,END+1000))==30
    store.db.close()

if __name__=='__main__':
    replay()
    replay(reserved=True)
    replay(older_gap=True)
    print('C11_TERMINAL_1M_REPAIR_FIRST_AND_SCOPED_BACKUP_PASS')
