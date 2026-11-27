"""Literal LONG Baseline-selected loss transition from pinned compiled Pine.
Research permission is explicitly Baseline, not compiled default Never widen.
Not the entire strategy: guard-state producer must be independently faithful.
No UT entry, fixedTP, funding/liquidation formula, or retroactive fills implemented.
"""
from dataclasses import dataclass,replace
from math import isfinite
from pathlib import Path
import hashlib
SOURCE=Path('/Volumes/DevilProjects/Workspace/research/ut_bot_20261115/dynamic_samebar_20261116/conditional_loss_stop_v1_20261116/UT_LOSS_STOP_NEVER_WIDEN_v1_NATIVE_COMPILED_SNAPSHOT.pine')
SOURCE_SHA='3b6518b79500adccb1a9085749cc40249d1401c74fd4df66278eacadfeded70d'
def verify_source():assert hashlib.sha256(SOURCE.read_bytes()).hexdigest()==SOURCE_SHA
@dataclass(frozen=True)
class State:
    sl:float|None
    protection_floor:float|None
    profit_seen:bool
    author_armed:bool
    author_stop:float|None
    loss_orders_ready:bool
@dataclass(frozen=True)
class Update:
    E:int
    source_available_ms:int
    initial_price:float
    close:float
    frozen_signal_atr:float
    confirmed_loss_atr:float
    boundary:float|None
    valid_boundary:bool
    signal_calculation:bool
    aligned_stops:bool
    confirmed_protection_raises:bool
    protected_before_calculation:bool
    raise_ready:bool
    confirmed_loss_raise:float|None
    widen_allowed:bool
    structure_cap_on:bool=False
    confirmed_structure_low:float|None=None
    confirmed_structure_high:float|None=None

def transition(s,u):
    for v in (s.profit_seen,s.author_armed,s.loss_orders_ready,u.valid_boundary,u.signal_calculation,u.aligned_stops,u.confirmed_protection_raises,u.protected_before_calculation,u.raise_ready,u.widen_allowed,u.structure_cap_on):
        if type(v) is not bool:raise ValueError('UNKNOWN_GUARD_OR_LOSS_STATE')
    if type(u.E) is not int or type(u.source_available_ms) is not int or u.source_available_ms>u.E:raise ValueError('FUTURE_OR_UNKNOWN_SOURCE')
    if s.loss_orders_ready and not u.signal_calculation:return s,'NO_CONFIRMED_UPDATE'
    for v in (s.sl,s.protection_floor,s.author_stop,u.confirmed_loss_raise):
        if v is not None and not isfinite(v):raise ValueError('NONFINITE_STATE_REJECTED')
    if s.loss_orders_ready and (u.confirmed_loss_atr is None or not isfinite(u.confirmed_loss_atr) or u.confirmed_loss_atr<=0):return s,'NO_VALID_CONFIRMED_ATR_KEEP_ORDER'
    price=u.close if s.loss_orders_ready else u.initial_price
    atr=u.confirmed_loss_atr if s.loss_orders_ready else u.frozen_signal_atr
    if not isfinite(price) or price<=0 or not isfinite(atr) or atr<=0:raise ValueError('UNKNOWN_PRICE_OR_ATR')
    if not u.valid_boundary and not s.loss_orders_ready:raise ValueError('INITIAL_BOUNDARY_UNKNOWN')
    boundary=u.boundary if u.valid_boundary else s.sl
    if boundary is None or not isfinite(boundary):raise ValueError('BOUNDARY_UNKNOWN')
    if u.valid_boundary and boundary>=price:raise ValueError('LIQUIDATION_BUFFER_BREACHED')
    floor=s.protection_floor if u.valid_boundary else s.sl
    candidate=price-3*atr
    if u.structure_cap_on and u.confirmed_structure_low is not None and u.confirmed_structure_high is not None:
        if not isfinite(u.confirmed_structure_low) or not isfinite(u.confirmed_structure_high):raise ValueError('NONFINITE_STRUCTURE_REJECTED')
        candidate=min(candidate,u.confirmed_structure_low)
    bounded=max(candidate,boundary)
    allow=s.author_armed and u.valid_boundary and u.widen_allowed
    if u.aligned_stops and s.author_armed and s.author_stop is not None and s.sl is not None:
        stop=max(s.sl,boundary);branch='GUARD_HANDOVER_FREEZE_LOSS'
    elif u.confirmed_protection_raises and u.protected_before_calculation and s.sl is not None:
        if u.raise_ready and u.confirmed_loss_raise is None:raise ValueError('CONFIRMED_RAISE_UNKNOWN')
        approved=u.confirmed_loss_raise if u.raise_ready else s.sl
        stop=max(s.sl,approved,boundary);branch='CONFIRMED_RAISE_ONLY'
    else:
        stop=max(s.sl,bounded) if s.profit_seen and s.sl is not None and not allow else bounded
        branch='LOCKED_NO_WIDEN' if s.profit_seen and not allow else 'LOSS_PHASE_DYNAMIC'
    if floor is not None:stop=max(stop,floor)
    return replace(s,sl=stop,protection_floor=floor,loss_orders_ready=True),branch
