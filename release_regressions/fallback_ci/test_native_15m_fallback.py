"""The user-approved fallback is explicit, narrow, and never wins over 1m."""
from pandas import DataFrame, to_datetime
from freqtrade.enums import CandleType
from causal_bars import CausalBars, ClosedBar, MINUTE, QUARTER
from local_candle_refresh import refresh_with_local_basis

end=300*QUARTER
pair='龙虾/USDT:USDT'
symbol='龙虾USDT'
keys=[(pair,tf,CandleType.FUTURES) for tf in ('5m','15m')]
view={}; origins={}; calls=[]
reported_end=end

def native(asked,**opts):
    calls.append((tuple(asked),opts))
    times=range(reported_end-260*QUARTER,reported_end,QUARTER)
    frame=DataFrame({'date':to_datetime(list(times),unit='ms',utc=True),
                     'open':9.,'high':9.,'low':9.,'close':9.,'volume':1.})
    return {keys[1]:frame}

def refresh(store):
    return refresh_with_local_basis(native_refresh=native,cache_view=view,store=store,
      market_id=lambda _:symbol,now_ms=lambda:end+3000,pair_list=keys,
      allow_native_15m_fallback=True,provenance=origins)

s=CausalBars(':memory:')
try:
    result=refresh(s)
    assert set(result)=={keys[1]} and calls[0][0]==(keys[1],)
    assert calls[0][1]['drop_incomplete'] is True and calls[0][1]['cache'] is False
    assert origins=={keys[1]:'direct_15m_fallback'}
    assert not s.range(symbol,MINUTE,end-QUARTER,end,end+3000)

    for wrong_end in (end-QUARTER,end+QUARTER):
        reported_end=wrong_end
        assert not refresh(s) and not view and not origins

    start=end-260*QUARTER
    rows=tuple(ClosedBar(t,t+MINUTE+1,4,5,3,4,1,4) for t in range(start,end,MINUTE))
    s.record(symbol,MINUTE,rows)
    calls.clear()
    result=refresh(s)
    assert set(result)==set(keys) and not calls
    assert all(frame.close.iloc[-1]==4 for frame in result.values())
    assert origins=={k:'closed_1m_projection' for k in keys}
    print('CLOSED_1M_FIRST_EXACT_15M_FALLBACK_ONLY_ON_MISSING_BASIS')
finally:
    s.db.close()
