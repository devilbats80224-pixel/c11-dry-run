"""Offline same-E join over trusted RPD states, not an RPD producer."""
from dataclasses import dataclass
from math import isfinite
from c11_primary_decision_seam_v1.primary_seam import Rpd5, Scope

@dataclass(frozen=True)
class ClosedRpdState:
    scope: Scope
    observed_ms: int
    known: bool
    active_long: bool
    last_long_score: float | None

def join(scope, as_of_ms, states):
    """Return (status, seam input); no previous/next row substitution."""
    E=scope.end_ms
    if type(E) is not int or E%300_000 or type(as_of_ms) is not int or as_of_ms<E:
        return "UNKNOWN",None
    chosen=[]
    for row in states:
        if row.scope != scope:
            continue
        if type(row.observed_ms) is not int or row.observed_ms<E:
            return "UNKNOWN",None
        if row.observed_ms<=as_of_ms:
            chosen.append(row)
    if len(chosen)!=1:
        return "UNKNOWN",None
    r=chosen[0]
    if r.known is not True or type(r.active_long) is not bool:
        return "UNKNOWN",None
    if not r.active_long:
        # Existing seam's nonlong representation is used solely to block;
        # inactive is NOT an assertion that a bearish pulse just printed.
        return "VETO",Rpd5(scope,r.observed_ms,"SHORT",0)
    val=r.last_long_score
    if type(val) not in (float,int) or not isfinite(val):
        return "UNKNOWN",None
    return ("PASS" if val>=0 else "VETO"), Rpd5(scope,r.observed_ms,"LONG",val)
