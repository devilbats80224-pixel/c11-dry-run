"""Exercise real DataProvider.refresh while counting the underlying wire inputs."""
from types import SimpleNamespace

from freqtrade.data.dataprovider import DataProvider
from freqtrade.enums import CandleType
from causal_bars import CausalBars, ClosedBar, MINUTE, QUARTER
from local_candle_refresh import refresh_with_local_basis

end = 300 * QUARTER
as_of = end+9
pair = '龍蝦/USDT:USDT'
sym = '龍蝦USDT'
keys = [(pair,'15m',CandleType.FUTURES),(pair,'5m',CandleType.FUTURES),
        (pair,'1m',CandleType.FUTURES)]
view, wire = {}, []
s = CausalBars(':memory:')
def native(items, **options):
    wire.extend(items)
    return {}
adapter = lambda items,**kw: refresh_with_local_basis(native_refresh=native,cache_view=view,
    store=s,market_id=lambda p:sym if p==pair else None,now_ms=lambda:as_of,pair_list=items,**kw)
# DataProvider refresh behavior is original, only its exchange is supplied.
# The engine must see an actual native-format frame, not a mocked decision.
dp = DataProvider({'exchange':{}},SimpleNamespace(refresh_latest_ohlcv=adapter,_klines=view))
try:
    dp.refresh(keys)
    assert wire==[keys[2]] and not view
    start=end-260*QUARTER
    bars=tuple(ClosedBar(x, x+MINUTE+1, 4,5,3,4,1,4)
               for x in range(start,end,MINUTE))
    s.record(sym,MINUTE,bars[:-1])
    dp.refresh(keys)
    assert wire==[keys[2]]*2 and not view
    wire.clear()
    # Full warmup except the terminal minute. The original minute owner is
    # responsible for fetching it. Freqtrade must not start its own 15m first.
    def with_backup():
        return refresh_with_local_basis(
            native_refresh=native, cache_view=view, store=s, market_id=lambda _:sym,
            now_ms=lambda:as_of, pair_list=keys[:2], allow_native_15m_fallback=True)
    assert not with_backup() and not wire, ('TERMINAL_REFILL_BYPASSED',wire)
    s.record(sym,MINUTE,bars[-1:])
    assert set(with_backup()) == set(keys[:2]) and not wire
    dp.refresh(keys)
    assert wire==[keys[2]]
    assert [(len(view[k]),float(view[k].close.iloc[-1])) for k in keys[:2]]==[(260,4.0)]*2
    assert tuple(view[keys[0]].columns)==('date','open','high','low','close','volume')
    assert view[keys[0]].date.iloc[-1].value//1_000_000==end-QUARTER
    as_of+=QUARTER
    dp.refresh(keys)
    assert wire==[keys[2]]*2 and not view
    wire.clear()
    with_backup()
    assert wire==[keys[0]], ('TRUE_HISTORY_GAP_MUST_ALLOW_BACKUP',wire)
    print('ORIGINAL_DATAPROVIDER_LOCAL_WIDER_CANDLES_AND_NO_WIDER_WIRE')
finally:
    s.db.close()
