"""Research-only seam: local ORM before native amount capture; no transport or orders."""
from dataclasses import dataclass
from c11_pending_gate_v1 import Guard,Terminal,Position

@dataclass(frozen=True)
class LocalState:
    pos: str
    remaining: int
    trim_open: bool
    trim_filled: int

def pre_reconcile(*, guard, pos, terminal, position, now, exec_quote_known,
                  read_local, apply_terminal):
    """Call from one serialized bot loop before native execute_trade_exit.
    apply_terminal is the caller's original native Trade/Order reconciliation;
    this pure frontier does not authenticate, cancel, or submit anything.
    """
    if type(guard) is not Guard:return 'WAIT_PRIVATE_JOURNAL',None
    arm=guard.db.execute('SELECT trim,qty,previous,limit_qty,armed,ticket FROM hard WHERE pos=?',(pos,)).fetchone()
    if arm is None:return 'NO_HARD_INTENT',None
    trim,qty,previous,limit,armed,ticket=arm
    if ticket:return 'ALREADY_RESERVED_RECONCILE_BEFORE_RETRY',None
    e=terminal
    if (type(e) is not Terminal or e.order!=trim or e.status not in ('canceled','closed','expired')
        or e.evidence!=guard.terminal_evidence
        or not all(type(z) is int for z in (e.venue_ms,e.receipt_ms,now,e.total_filled))
        or not armed<=e.venue_ms<=e.receipt_ms<=now or not previous<=e.total_filled<=limit
        or (e.status=='closed' and e.total_filled!=limit)):
        return 'WAIT_TERMINAL_OR_FILLS_UNKNOWN',None
    first=read_local()
    if (type(first) is not LocalState or first.pos!=pos or type(first.trim_open) is not bool
        or type(first.trim_filled) is not int or not previous<=first.trim_filled<=e.total_filled):
        return 'WAIT_LOCAL_POSITION_RECONCILE',None
    if first.trim_open:
        apply_terminal(e)
    last=read_local()
    if (type(last) is not LocalState or last.pos!=pos or last.trim_open is not False or
        type(last.trim_filled) is not int or last.trim_filled!=e.total_filled or
        type(last.remaining) is not int or last.remaining!=qty-e.total_filled):
        return 'WAIT_LOCAL_POSITION_RECONCILE',None
    return guard.handoff_once(pos=pos,order=e,position=position,now=now,exec_quote_known=exec_quote_known)
