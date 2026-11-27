"""NEW_RECONSTRUCTED_BASELINE abstract exit contract, NOT recovered portfolio engine.
E labels a completed 1m bar [E-60s,E). Signals at E submit for next open.
No cash, sizing, funding, liquidation, entry selection or ARM algorithm here.
"""
from dataclasses import dataclass, replace
from math import isfinite

@dataclass(frozen=True)
class State:
    entry: float = 100.
    hard_stop: float = 0.
    entry_E: int = 0
    last_E: int = 0
    gate: str = 'UNSEEN'  # frozen first eligible hour decision
    passed_stage: float = 0.
    failed_floor: float = 0.
    pending: bool = False
    held: bool = True
    armed: bool = False
    trail: float = 0.

@dataclass(frozen=True)
class Bar:
    E: int
    open: float
    high: float
    low: float
    close: float
    known: bool = True

@dataclass(frozen=True)
class Published:
    known: bool = True
    available_E: int = 0
    armed: bool = False
    trail: float = 0.
    original_open_exit: str | None = None

class Unknown(ValueError): pass

def step(s,b,*,enabled=False,hour=None,published=Published(),oi_eligible=None):
    if not s.held: raise ValueError('POSITION_ALREADY_CLOSED')
    if b.E!=s.last_E+60000: raise Unknown('NONCONTIGUOUS_BAR')
    if not b.known or not all(isfinite(v) and v>0 for v in [b.open,b.high,b.low,b.close]):raise Unknown('OHLC_UNKNOWN')
    if not b.low<=min(b.open,b.close)<=max(b.open,b.close)<=b.high:raise ValueError('INVALID_OHLC')
    # Only already-published orders can use this bar. New publication handled last.
    old_stop=max(s.hard_stop if s.hard_stop>0 else s.entry*.93,s.passed_stage)
    if not isfinite(published.trail) or published.trail<0:raise Unknown('TRAIL_RECORD_UNKNOWN')
    def closed(reason,price):return replace(s,held=False,last_E=b.E),{'reason':reason,'price':price,'fill_at':'OPEN' if price==b.open else 'INTRABAR_IDEAL','settle_before_next_entry':True}
    if b.open<=old_stop:return closed('OLD_STOP_GAP',b.open)
    if not published.known or published.available_E>b.E-60000:raise Unknown('PREEXISTING_EXIT_STATE_UNKNOWN')
    if published.original_open_exit:return closed(published.original_open_exit,b.open)
    if s.pending:return closed('FAILED_GATE_CLOSE_FLOOR',b.open)
    if b.low<=old_stop:return closed('OLD_STOP_INTRABAR',old_stop)
    # oi_eligible intentionally unused: expired admission is not a holding exit.
    gate=s.gate
    if gate=='UNSEEN' and b.close>=s.entry*1.03:
        if hour is None or hour['known'] is not True:raise Unknown('FIRST_HOUR_GATE_UNKNOWN')
        if not hour['end_E']<=b.E or hour['available_E']>b.E:raise Unknown('FUTURE_HOUR')
        if not all(isfinite(hour[k]) and hour[k]>=0 for k in ['last','previous']):raise Unknown('HOUR_QUOTE_UNKNOWN')
        gate='PASS' if hour['last']>hour['previous'] else 'FAIL'
    floor=s.entry*1.016 if b.close>=s.entry*1.05 else s.entry*1.008 if b.close>=s.entry*1.03 else 0.
    # Old floor only: a newly raised line must not trigger retrospectively.
    pending=bool(enabled and s.gate=='FAIL' and s.failed_floor>0 and b.close<=s.failed_floor)
    out=replace(s,last_E=b.E,gate=gate,pending=pending,
        passed_stage=max(s.passed_stage,floor) if gate=='PASS' else s.passed_stage,
        failed_floor=max(s.failed_floor,floor) if enabled and gate=='FAIL' else s.failed_floor,
        armed=published.armed or s.armed,
        trail=max(s.trail,published.trail) if published.armed or s.armed else s.trail)
    return out,{'reason':'HOLD','cash_released':False}
