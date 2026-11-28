"""A reply without native commit needs observable analyzer status."""
from types import SimpleNamespace
from pandas import DataFrame, to_datetime

from native_entry_owner import NativeEntryOwner, QUARTER
from first_gate import FirstGate
from diagnostic_schema import accepted_symbol_event
import json

E=1_795_872_600_000
PAIR="SYNTH/USDT:USDT"
code=[]
def bars(end):
    return DataFrame([(to_datetime(end-QUARTER,unit="ms",utc=True),10.,11.,9.,10.,1.)],
                     columns=("date","open","high","low","close","volume"))

dp=SimpleNamespace(raw=bars(E-QUARTER),analyzed=bars(E-QUARTER))
dp.get_analyzed_dataframe=lambda *_:(dp.analyzed,None)
dp.ohlcv=lambda *_,**_kw:dp.raw
strategy=SimpleNamespace(dp=dp,timeframe="15m",config={"dry_run":True,"candle_type_def":"futures"})
lane=NativeEntryOwner(trusted_owner_input=lambda *_:("HOLD_NOT_READY",None),
                      record=lambda *_:None,provisional=lambda *_:None)
lane.analysis_diagnostic=lambda p, raw_end, why:code.append((p,raw_end,why))
lane.remember_missing_analysis(strategy,PAIR)
assert code==[(PAIR,E-QUARTER,"ANALYSIS_ALREADY_CACHED")], f"C11_REAL_ANALYSIS_STAGE_NOT_VISIBLE actual={code!r}"
packets=[]
gate=FirstGate(lambda line:packets.append(json.loads(line)))
gate.start(E+3)
gate.watch(E+10,6690,"SYNTHUSDT",E+60_000)
gate.note(6690,"SYNTHUSDT","analysis_flow","ANALYSIS_RAW_FRAME_OLDER",E+8300)
gate.note(6690,"SYNTHUSDT","analysis_flow","sensitive sample dropped",E+8310)
gate.start(E+60_000)
details=[x for x in packets if x.get("kind")=="analysis_flow"]
assert len(details)==1 and accepted_symbol_event(details[0]) and details[0]["values"]=={
   "ANALYSIS_RAW_FRAME_OLDER":[8300,8300,1]}
print("C11_VALIDATED_REPLY_TO_ORIGINAL_ANALYZER_STAGE_EVIDENCE_PASS")
