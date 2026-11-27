"""Bound ordinary REST tail repair to the recent analyzer window.

The bound is a transport policy, not a claim that truncating recursive RPD
or ATR preserves the original strategy. Existing, verified local inputs
can still reconstruct exact state; this rule neither initializes nor resets it.
"""
FIVE=300_000
QUARTER=900_000
ANALYZER_INPUT=260

def native_tail_allowed(page,*,segment,rpd):
    if segment is None or page.width not in (FIVE,QUARTER):return False
    first=page.end-ANALYZER_INPUT*QUARTER
    cutoff=page.end-QUARTER
    if not first<=page.start<cutoff:return False
    if page.width==FIVE:
        cursor=getattr(rpd,'next_open',None)
        return type(cursor) is int and first<=page.start<=cursor<=cutoff
    return True
