"""Keep native revisit available when Freqtrade has only a previous analysis."""
from types import SimpleNamespace
from pandas import DataFrame, to_datetime
from native_entry_owner import NativeEntryOwner, QUARTER

E=1_795_870_800_000
PAIR="TESTX/USDT:USDT"
def bars(end):
    stamp=to_datetime(end-QUARTER,unit="ms",utc=True)
    return DataFrame([(stamp,10.,11.,9.,10.,1.)],columns=("date","open","high","low","close","volume"))

dp=SimpleNamespace(raw=bars(E),analyzed=bars(E-QUARTER))
dp.get_analyzed_dataframe=lambda *_:(dp.analyzed,None)
dp.ohlcv=lambda *_,**_kw:dp.raw
strategy=SimpleNamespace(dp=dp,timeframe="15m",config={"dry_run":True,"candle_type_def":"futures"})
offered=[]
def provisional(pair,end):
    offered.append((pair,end))
    return SimpleNamespace(pair=pair,end=end)

ready=False
def available(_proof,_pair,_end):return ready
lane=NativeEntryOwner(trusted_owner_input=lambda *_:None,record=lambda *_:None,
                      provisional=provisional,revisit_ready=available)
lane.remember_missing_analysis(strategy,PAIR)
assert offered==[(PAIR,E)], f"C11_STALE_ANALYSIS_CURRENT_REVISIT_LOST offered={offered!r}"
assert not lane.one_public_revisit(strategy,PAIR)
ready=True
assert lane.one_public_revisit(strategy,PAIR)
assert not lane.one_public_revisit(strategy,PAIR)
lane.remember_missing_analysis(strategy,PAIR)
assert offered==[(PAIR,E)],"C11_STALE_CACHE_DUPLICATE_REVISIT"
dp.raw=bars(E+QUARTER)
dp.analyzed=bars(E+QUARTER)
lane.remember_missing_analysis(strategy,PAIR)
assert offered==[(PAIR,E)],"C11_CURRENT_ANALYSIS_SHOULD_NOT_GENERATE_REVISIT"
print("C11_STALE_ANALYZED_CACHE_CURRENT_REVISIT_ONCE")
