"""Recovered EARLIER guard math + NEW explicit close scheduling adapter.
Not mature60, half-special, or recovered original whole kernel.
"""
from pathlib import Path
import sys,math
from dataclasses import dataclass,replace
from decimal import Decimal,ROUND_CEILING
from guard_state_v2 import advance,Guard
from loss_state_v2 import State as Loss

def ceil_tick(x,t):return float((Decimal(str(x))/Decimal(str(t))).to_integral_value(rounding=ROUND_CEILING)*Decimal(str(t)))
@dataclass(frozen=True)
class ATRState:
    entry:float
    guard:Guard
    peak:float
    published:float=0.
    pending:bool=False
    last_E:int=0

def initial(entry):return ATRState(entry,Guard(Loss(entry*.93,None,False,False,None,True)),entry)

def close_update(s,*,E,close,high,atr15,atr5,mult,er,previous_er,tick,boundary,source15,source5,source_er,known):
    if not math.isfinite(boundary) or boundary<0:raise ValueError('BOUNDARY_UNKNOWN')
    if not known or E<=s.last_E or source15>E or E-source15>=900000:raise ValueError('FEATURE_UNKNOWN_OR_CLOCK')
    if not all(math.isfinite(x) and x>0 for x in [close,high,atr15,atr5,mult,tick]) or high<close:raise ValueError('FEATURE_INVALID')
    if source5>E or source_er>E:raise ValueError('FUTURE_FEATURE')
    # Prior publication only. Pending is consumed by downstream NEXT open, not now.
    if s.pending:return s,'PENDING_ORIGINAL_CLOSE_ORDER'
    if s.published>0 and close<=s.published:return replace(s,pending=True,last_E=E),'SCHEDULE_NEXT_OPEN'
    g=s.guard
    if E%900000==0:g,_=advance(g,E=E,entry=s.entry,close=close,tick=tick,atr5=atr5,atr15=atr15,guard_multiplier=mult,signed_er=er,previous_signed_er=previous_er,source_at5=source5,source_at15=source15,source_at_er=source_er,boundary=boundary)
    peak=max(s.peak,high);line=s.published
    if g.loss.author_armed:
        fee_floor=ceil_tick(s.entry*1.0005/.9995+tick,tick)
        line=ceil_tick(max(line,g.loss.sl,peak-4*atr15,s.entry+.5*(peak-s.entry),fee_floor),tick)
        if line<=boundary:raise ValueError('BOUNDARY_INVALID')
    return replace(s,guard=g,peak=peak,published=line,last_E=E),'PUBLISH_NEXT_BAR' if line>s.published else 'HOLD'
