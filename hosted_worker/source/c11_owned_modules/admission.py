"""Time-gated scheduling over the held roster; no max candidate count."""
from collections import deque

class OptionalAdmission:
 def __init__(self):
    self.end=None
    self.pending=()
 def offer(self,rows,end,deadline,mono,completed,attempt):
    by_symbol={r.symbol:r for r in rows if r.symbol not in completed}
    retained=self.pending if self.end==end else ()
    active=deque(sym for sym in retained if sym in by_symbol)
    seen=set(active)
    active.extend(sym for sym in by_symbol if sym not in seen)
    retry=[]
    while active and mono()<deadline:
       sym=active.popleft()
       label=attempt(by_symbol[sym])
       if label in ('HOLD_OBSERVER_BUSY','DEFER_ORIGINAL_BUDGET'):
          retry.append(sym)
       else:
          completed[sym]=label
    self.pending=tuple(active)+tuple(retry)
    self.end=end
