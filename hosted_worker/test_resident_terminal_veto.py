"""The live lane can use its admitted terminal minutes without making another wire request."""
import sys
from types import SimpleNamespace as NS
from unittest.mock import patch
sys.path[:0]=["/opt/c11_owned_modules","/opt/c11_worker","/opt/c11_worker/ports",
              "/opt/c11_background_review","/freqtrade"]
from causal_bars import CausalBars, ClosedBar, FIVE, MINUTE, QUARTER, SegmentStart
from native_primary_lane import NativePrimaryLane
from rpd_checkpoint import RpdState, WARM
import native_primary_lane

symbol="EXAMPLEUSDT"
end=1795773600000
start=end-QUARTER
now=end+4_000
origin=start-WARM*FIVE

def run(kind):
    db=CausalBars(":memory:")
    proof=SegmentStart(symbol,origin,origin+FIVE)
    db.set_segment(proof)
    state=RpdState.start(proof)
    for i in range(WARM):
        t=origin+i*FIVE
        state.advance(ClosedBar(t,t+FIVE,100,104,99,101,50,5000),now)
    payload=state.pack()
    db.db.execute("INSERT INTO rpd_checkpoint VALUES (?,?)",(symbol,payload))
    db.db.commit()
    rows=[ClosedBar(t,now+1 if kind=="future" and t==start else now,100,104,99,101,10,1000)
          for t in range(start,end,MINUTE) if not (kind=="missing" and t==start)]
    db.record(symbol,MINUTE,rows)
    if kind=="conflict":
        db.record(symbol,FIVE,(ClosedBar(start,now,100,105,99,101,50,5000),))
    owner=NS(store=db,run=NS(wall=lambda:now))
    lane=NativePrimaryLane(owner,lambda *_:None)
    lane.current=lambda *_:kind!="stale_scope"
    fact=NS(symbol=symbol,origin=origin)
    scoped=NS(generation=17,end=end)
    assert not lane.terminal_five
    # All storage and RPD advancement is real; the terminal policy response
    # is deterministic here so this gate tests sourcing, not market patterns.
    with patch.object(native_primary_lane,"join",return_value=("VETO",None)) as join:
        lane.record_verified_veto(fact,scoped)
        if kind=="valid":
            assert join.call_count==1, "C11_ADMITTED_TERMINAL_MINUTES_IGNORED"
            assert db.db.execute("SELECT state FROM feature_status").fetchall()==[("VETO",)]
        else:
            assert join.call_count==0 and not db.db.execute("SELECT 1 FROM feature_status").fetchall(), kind
    assert db.db.execute("SELECT payload FROM rpd_checkpoint").fetchone()==(payload,)

for case in ("valid","missing","future","conflict","stale_scope"):
    run(case)
print("C11_RESIDENT_TERMINAL_VETO_CONTROLS_PASS")
