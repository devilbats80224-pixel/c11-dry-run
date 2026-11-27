"""Pinned close-only C11 RPD5 producer for one contiguous causal 5m segment."""
from dataclasses import dataclass
from hashlib import sha256
from math import isfinite
from pathlib import Path
from types import ModuleType
import pandas as pd
from c11_primary_decision_seam_v1.exact_rpd5_join import ClosedRpdState, Scope

WIDTH=300_000
WARM=260
PATH=Path(__file__).resolve().parents[1] / "frozen_sources/rpd.py"
PIN='10ea1a4a74212f8549f7856c8f10bafb27848e07d479333f11275817fdba77d6'
CORE=ModuleType('pinned_rpd_core')
_source=PATH.read_bytes()
if sha256(_source).hexdigest()!=PIN:
    raise RuntimeError('Pinned RPD calculator digest mismatch')
exec(compile(_source,str(PATH),'exec'),CORE.__dict__)

@dataclass(frozen=True)
class Close5:
    symbol: str
    open_ms: int
    observed_ms: int
    high: float
    low: float
    close: float
    volume: float

@dataclass(frozen=True)
class SegmentStart:
    symbol: str
    open_ms: int  # trusted genesis, or first bar after a proven gap
    proven_at_ms: int  # as-of when genesis became known

def produce(scope: Scope, as_of_ms: int, start: SegmentStart, rows):
    """Read exact retained segment; never assume first supplied/warmup bar is genesis.

    Caller certifies gap/genesis authority and real receipt times. Missing input
    is UNKNOWN, not permission to reset state; replay only through requested E.
    """
    E=scope.end_ms
    if (not all(type(x) is int for x in (E,as_of_ms,start.open_ms,start.proven_at_ms))
        or start.symbol!=scope.symbol or E%WIDTH or start.open_ms%WIDTH
        or not E<=as_of_ms or not start.open_ms<=start.proven_at_ms<=as_of_ms):
        return None
    bars={}
    last_receipt=start.proven_at_ms
    for row in rows:
        if row.symbol!=scope.symbol:
            continue
        if type(row.open_ms) is not int or type(row.observed_ms) is not int:
            return None
        o=row.open_ms
        if o<start.open_ms or o>=E or row.observed_ms>as_of_ms:
            continue
        if (o%WIDTH or o in bars or row.observed_ms<o+WIDTH
            or not all(type(x) in (float,int) and isfinite(x) for x in (row.high,row.low,row.close,row.volume))
            or not 0<row.low<=row.close<=row.high or row.volume<0):
            return None
        bars[o]=(o//1000,row.high,row.low,row.close,row.volume)
        last_receipt=max(last_receipt,row.observed_ms)
    if sorted(bars)!=list(range(start.open_ms,E,WIDTH)) or len(bars)<WARM:
        return None
    r=CORE.compute(pd.DataFrame([bars[o] for o in sorted(bars)],columns=['time_s','high','low','close','volume']))
    assert int(r.available_time_s.iloc[-1])*1000==E
    # Archive resets on confirmed short then accepts confirmed long on same bar,
    # including during warmup; predictive flags alone are never admitted.
    score=None
    active=False
    for L,S,v in zip(r.long,r.short,r.long_score):
        if S:
            active=False
            score=None
        if L:
            active=True
            score=float(v)
    return ClosedRpdState(scope,last_receipt,True,active,score)
