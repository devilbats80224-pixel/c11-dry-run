"""C11 offline arithmetic only. No I/O, venue, strategy, orders or position state."""
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
from math import isfinite

M = 60_000
F = 5*M

def _d(x):
    d = Decimal(str(x))
    if not d.is_finite():
        raise ValueError("nonfinite decimal")
    return d

def _tick(x, step, mode):
    x, step = _d(x), _d(step)
    if step <= 0:
        raise ValueError("invalid tick/lot")
    return (x/step).to_integral_value(rounding=mode)*step

def third_slot_limit(*, open_positions, incumbent, nav, entry, tick, lot, quantity,
                     min_qty, min_notional, fee=.0005):
    """Return ceiling for native planner only; do not interpret as filled size."""
    x,q,i,n,l,f,m,v = map(float,(entry,quantity,incumbent,nav,lot,fee,min_qty,min_notional))
    if not all(isfinite(z) for z in (x,q,i,n,l,f,m,v)) or min(x,n,l)<=0 or min(q,i,f,m,v)<0:
        raise ValueError("invalid risk inputs")
    if not isinstance(open_positions,int) or isinstance(open_positions,bool) or open_positions not in (0,1,2):
        raise ValueError("unexpected slot count")
    stop=float(_tick(x*.94, tick, ROUND_CEILING))
    risk=(x-stop)+(x+stop)*f
    if stop>=x or risk<=0:
        raise ValueError("invalid hard floor")
    if open_positions!=2 or q<=0 or i+risk*q<=.20*n:
        return {"initial_stop":stop,"cap_qty":q,"changed":False}
    cap=float(_tick(min(q,max(0.,(.20*n-i)/risk)),l,ROUND_FLOOR))
    if cap<max(m,l) or cap*x<v:
        cap=0.
    return {"initial_stop":stop,"cap_qty":cap,"changed":cap!=q}

def partial_geometry(*, entry, entered, decided, completed_rows):
    """Exactly ten (right-end ms,open,high,low,close) already finalized rows."""
    if len(completed_rows)!=10 or not isinstance(entered,int) or not isinstance(decided,int):
        return None
    if decided!=((entered+F-1)//F)*F+2*F:
        return None
    t, bars = [], []
    for r in completed_rows:
        if len(r)!=5 or not isinstance(r[0],int):
            return None
        t.append(r[0]); bars.append(tuple(map(float,r[1:])))
    if t[0]<=entered or t!=list(range(decided-9*M,decided+M,M)) or not all(isfinite(v) and v>0 for b in bars for v in b):
        return None
    if not isfinite(entry) or entry<=0:
        return None
    hi=[max(b[1] for b in bars[k:k+5]) for k in (0,5)]
    lo=[min(b[2] for b in bars[k:k+5]) for k in (0,5)]
    c=[bars[4][3],bars[9][3]]
    if hi[0]>entry>c[0] and hi[1]<entry and c[1]<lo[0] and hi[1]-lo[1]>hi[0]-lo[0]:
        return dict(hi=hi,lo=lo,close=c)
    return None

def partial_open_plan(*, quantity, observed_open, hard_floor, liquidation_buffer,
                      lot, tick, min_qty, min_notional, stress,
                      decision_end_ms, open_bar_end_ms, senior_clear):
    """One-shot portion at observed next open, subordinate to senior controls."""
    if senior_clear is not True or open_bar_end_ms!=decision_end_ms+M:
        return None
    q,op,hard,buffer,l,t,mn,mnot = map(_d,(quantity,observed_open,hard_floor,liquidation_buffer,lot,tick,min_qty,min_notional))
    if min(q,op,l,t,hard,buffer)<=0 or min(mn,mnot)<0 or q%l or op%t:
        raise ValueError("invalid observed lot/tick or protection boundary")
    if op<=max(hard,buffer):
        return None
    sold=_tick(q/2,l,ROUND_FLOOR); remains=q-sold
    if min(sold,remains)<max(mn,l) or min(sold,remains)*op<mnot:
        return None
    return dict(qty=float(sold),price=float(op),clock="OPEN",
                why="EARLY_TWO5_FAIL_RECOVERY_PARTIAL",remaining=float(remains),
                extra_fee_rate=.0012 if stress else 0.)
