"""Use the sealed runtime store, adapter, and original Freqtrade DataProvider."""
from types import SimpleNamespace
from causal_bars import CausalBars, ClosedBar, MINUTE, QUARTER
from freqtrade.data.dataprovider import DataProvider
from freqtrade.enums import CandleType
from local_candle_refresh import refresh_with_local_basis

end=300*QUARTER
pair="EXAMPLE/USDT:USDT"
symbol="EXAMPLEUSDT"
keys=[(pair,tf,CandleType.FUTURES) for tf in ("1m","5m","15m")]
view, wire, provenance = {}, [], {}
store=CausalBars(":memory:")
def native(items, **_):
    wire.extend(items)
    assert list(items)==keys[:1], "C11_ROUTINE_REQUESTED_DIRECT_WIDER"
    return {}

def refresh(items, **kwargs):
    return refresh_with_local_basis(native_refresh=native,cache_view=view,store=store,
        market_id=lambda p:symbol if p==pair else None,now_ms=lambda:end+9,
        pair_list=items,allow_native_15m_fallback=True,provenance=provenance,**kwargs)

provider=DataProvider({"exchange":{}},SimpleNamespace(refresh_latest_ohlcv=refresh,_klines=view))
try:
    rows=tuple(ClosedBar(t,t+MINUTE+1,4,6,3,5 if t==end-MINUTE else 4,1,4)
               for t in range(end-260*QUARTER,end,MINUTE))
    store.record(symbol,MINUTE,rows)
    provider.refresh(keys)
    assert wire==keys[:1]
    assert provenance=={k:"closed_1m_projection" for k in keys[1:]}
    assert all(len(view[k])==260 and int(view[k].close.iloc[-1])==5 for k in keys[1:])
    assert all(int(view[k].date.iloc[-1].value)//1_000_000+width==end
               for k,width in zip(keys[1:], (5*MINUTE,QUARTER), strict=True))
    print("C11_SEALED_ORIGINAL_DATAPROVIDER_1M_ONLY_WITH_READY_BASIS")
finally:
    store.db.close()
