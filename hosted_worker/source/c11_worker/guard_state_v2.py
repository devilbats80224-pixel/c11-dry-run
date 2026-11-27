"""Compiled3b6518 default guard profile, Baseline loss permission, long only.
Fixed5m guard ATR,15m loss ATR/ER20 EMA5,15m protection updates,
legal fee-covered closes2, strengthening-raises-only coordination.
No TP module or UT entry. External native exchange boundary is an explicit port difference.
Caller must execute old orders/opposite signals BEFORE invoking this close transition.
"""
from dataclasses import dataclass,replace,field
from decimal import Decimal,ROUND_CEILING
from math import isfinite,ceil
from loss_state_v2 import State,Update,transition
@dataclass(frozen=True)
class Guard:
    loss:State
    legal_run:int=0
    legal_last:int|None=None
    common_last:int|None=None
    prior_guard:float|None=None
    prior_loss:float|None=None

def advance(g,*,E,entry,close,tick,atr5,atr15,guard_multiplier,
            signed_er,previous_signed_er,source_at5,source_at15,source_at_er,boundary):
    if E%900000!=0:return g,'NO_PROTECTION_BOUNDARY'
    if not all(isfinite(x) for x in [entry,close,tick,atr5,atr15,guard_multiplier,signed_er,boundary]) or min(entry,close,tick,atr5,atr15,guard_multiplier)<=0:raise ValueError('GUARD_INPUT_UNKNOWN')
    if not (source_at5==source_at15==source_at_er==E):raise ValueError('GUARD_SOURCE_CLOCK_UNKNOWN')
    if boundary>=close:raise ValueError('LIQUIDATION_BUFFER_BREACHED')
    s=g.loss;was_armed=s.author_armed;protected=was_armed and s.author_stop is not None
    selected=close-atr5*guard_multiplier
    floor_raw=entry*(1+.05/100)/(1-.05/100)+tick
    fee_floor=ceil(floor_raw/tick)*tick
    placeable=fee_floor<=close-tick
    valid=selected>0 and selected<close
    legal=close>entry and valid and placeable and selected>=fee_floor
    run=g.legal_run;last=g.legal_last;armed=was_armed
    if not was_armed:
        if last is None or last!=E:
            run=(run+1 if last is not None and E-last==900000 else 1) if legal else 0
            last=E
        armed=legal and run>=2
    guard=s.author_stop;profit=s.profit_seen or armed
    ready=False;loss_raise=None;common=g.common_last;pg=g.prior_guard;pl=g.prior_loss
    if armed and valid:
        bounded=max(selected,boundary)
        if protected:
            current_loss=max(close-3*atr15,boundary)
            hold=previous_signed_er is not None and isfinite(previous_signed_er) and signed_er>0 and signed_er>=previous_signed_er
            if common is None or common!=E:
                ready=(common is not None and E-common==900000 and pg is not None and pl is not None and not hold)
                guard_raise=min(pg,bounded) if ready else guard
                loss_raise=min(pl,current_loss) if ready else None
                common=E;pg=bounded;pl=current_loss
                guard=max(guard,guard_raise,boundary)
        else:guard=bounded if guard is None else max(guard,bounded)
        profit=True
    s=replace(s,profit_seen=profit,author_armed=armed,author_stop=guard)
    # Frozen initial ATR handled separately at entry. Here the existing holding order is ready.
    u=Update(E,E,entry,close,atr15,atr15,boundary,True,True,True,True,protected,ready,loss_raise,False)
    s,branch=transition(s,u)
    return Guard(s,run,last,common,pg,pl),branch

def submitted_stop(g):
    s=g.loss
    return s.sl if s.author_stop is None else s.author_stop if s.sl is None else max(s.sl,s.author_stop)
