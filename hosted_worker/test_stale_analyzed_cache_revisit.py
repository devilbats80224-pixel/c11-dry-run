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

# At first analysis the new close is cached by Freqtrade, but the original
# native owner has not yet admitted this pair. The late owner receipt must
# open exactly one proof-bound revisit instead of treating the cache as final.
from threading import get_ident
from coordinator import Scope
from unified_owner import UnifiedScheduler

E += 2 * QUARTER
dp.raw=dp.analyzed=bars(E)
symbol="TESTXUSDT"
now=[E+3_377]
scope=Scope(PAIR,symbol,6690,E,E+60_000)
key=(symbol,60_000,E-45*60_000,E,45)
ticket="synthetic-current-owned-minute"
accepted={}
scheduler=object.__new__(UnifiedScheduler)
scheduler.thread=get_ident()
scheduler.closed=False
scheduler.marker=(scope.generation,E)
scheduler.covers={key:scope}
scheduler.tickets={key:ticket}
scheduler.verified=set()
scheduler.adapter=SimpleNamespace(_proven=accepted)
scheduler.warm=SimpleNamespace(_current=lambda s:s is scope and now[0]<scope.expires)
attempts=[]
scope_available=False
def controlled_provisional(pair,end):
    attempts.append((pair,end))
    return scope if scope_available else None
lane=NativeEntryOwner(trusted_owner_input=lambda *_:('HOLD_NO_OWNED_SCOPE',None),record=lambda *_:None,
                      provisional=controlled_provisional,revisit_ready=lambda s,p,e:s is scope)
lane.visible_warm_minute=getattr(scheduler,'has_visible_warm_minute',lambda *_:False)
lane._record_provisional(PAIR,E,'HOLD_NO_OWNED_SCOPE')
assert attempts==[(PAIR,E)] and not lane.holds
lane.remember_missing_analysis(strategy,PAIR)
assert attempts==[(PAIR,E)] and not lane.one_public_revisit(strategy,PAIR)
now[0]=E+8_300
scope_available=True
scheduler.verified.add(key)
accepted[ticket]=SimpleNamespace(ticket=SimpleNamespace(scope=Scope(
    PAIR,symbol,6689,E,E+60_000),lane='1m',request_id=ticket))
lane.remember_missing_analysis(strategy,PAIR)
assert attempts==[(PAIR,E)],"Stale-generation receipts cannot reopen analysis"
accepted[ticket]=SimpleNamespace(ticket=SimpleNamespace(scope=scope,lane='1m',request_id=ticket))
lane.remember_missing_analysis(strategy,PAIR)
assert lane.one_public_revisit(strategy,PAIR), "C11_RECEIPT_VISIBLE_BUT_CURRENT_CACHE_HIDES_NATIVE_REVISIT"
assert not lane.one_public_revisit(strategy,PAIR)
lane.remember_missing_analysis(strategy,PAIR)
assert attempts==[(PAIR,E),(PAIR,E)], "Do not repeat a spent revisit"
print("C11_CURRENT_ANALYZED_CACHE_REVISIT_REQUIRES_EXACT_VISIBLE_WARM_MINUTE")
