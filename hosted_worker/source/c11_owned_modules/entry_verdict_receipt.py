"""Passive, exact-owner entry diagnostics. No evaluation or I/O into trade state."""
from contextvars import ContextVar
import json
import time

ACTIVE = ContextVar("c11_entry_verdict", default=None)
ADMISSION = ContextVar("c11_native_admission", default=None)
Q = 900_000
ROUTES = frozenset(("c11_primary", "c11_nw5", "c11_supplement"))
PRIMARY_REASONS = frozenset((
    "NO_PRIMARY_SIGNAL", "NATIVE_PRIMARY_CALC_UNAVAILABLE", "NATIVE_INPUT_UNAVAILABLE",
    "NATIVE_FRAME_END_MISMATCH", "NATIVE_UNRECORDED_PRIMARY", "PRIMARY_SIGNAL_ONLY",
    "BAD_OR_EXPIRED_DECISION_CLOCK", "CANDLE_SYMBOL_MISMATCH", "OI20_BINDING_UNKNOWN",
    "OI20_BELOW_THRESHOLD", "RPD5_SAME_E_UNKNOWN", "RPD5_NOT_LONG_NONNEGATIVE",
    "RPD5_PRODUCER_INPUT_INVALID", "MINUTE_INPUT_INVALID", "MINUTE_SERIES_INCOMPLETE",
    "NW15_NO_PRIMARY", "CLOSED5_REJECTED_BREAK", "CLOSED5_UNKNOWN", "MALFORMED_INPUT",
    "NATIVE_RANK_AUTHORITY_UNAVAILABLE", "NATIVE_RANK_LEASE_EXPIRED",
    "NATIVE_READY_HANDOFF_UNAVAILABLE", "NATIVE_JOURNAL_NOT_READY",
    "NATIVE_READ_SCOPE_EXPIRED", "NATIVE_RPD_COMMIT_NOT_PROVEN",
    "BAD_DECISION_CLOCK", "BAD_SOURCE_CLOCK", "MISSING_CURRENT_CLOSED_15M", "GAP_OR_WARMUP",
    "NO_SAME_E_1M_CLOSE", "1M_15M_CLOSE_MISMATCH", "NW_OSCILLATOR_UNKNOWN",
    "NATIVE_EVIDENCE_ERROR",
))
ADMISSION_REASONS = frozenset((
    "NATIVE_ENTRY_PAIR_LOCKED",
    "NATIVE_ENTRY_DEPTH_REJECTED", "NATIVE_ENTRY_NOT_REQUESTED",
))
ORDER_REASONS = frozenset((
    "ENTRY_PAPER_ROUTE_GUARD", "ENTRY_FACTS_UNAVAILABLE", "ENTRY_PRICE_MISMATCH",
    "ENTRY_SCOPE_MISMATCH", "ENTRY_POSITION_STATE_CHANGED", "ENTRY_PRIMARY_RECHECK_DENIED",
    "ENTRY_NW5_ROUTE_NOT_BOUND", "ENTRY_SUPPLEMENT_NOT_AVAILABLE", "ENTRY_PLAN_ERROR",
    "ENTRY_SIZING_OR_RISK_PLAN_REFUSED", "ENTRY_RESERVATION_REFUSED",
    "NATIVE_ORDER_SENT", "NATIVE_ORDER_NOT_ACCEPTED",
))

def identity(pair, end, width):
    if type(pair) is not str or not pair.endswith("/USDT:USDT") or type(end) is not int or not 0 < end or end % width:
        return None
    base = pair.removesuffix("/USDT:USDT")
    return (base+"USDT", end) if base.isalnum() and len(base) <= 19 else None

def mark_primary(reason, force=False):
    state = ACTIVE.get()
    if state is None or type(reason) is not str or reason not in PRIMARY_REASONS:
        return
    if force or state[0] is None:
        state[0] = reason

def publish(pair, end, phase, reason, route="c11_primary"):
    key = identity(pair, end, Q if route == 'c11_primary' else 300_000)
    if key is None or type(route) is not str or route not in ROUTES or not (
        (phase == "primary" and reason in PRIMARY_REASONS)
        or (phase == "admission" and reason in ADMISSION_REASONS)
        or (phase == "order" and reason in ORDER_REASONS)
    ):
        return
    row = dict(schema="c11-entry-verdict-v1", symbol=key[0], end_ms=key[1], phase=phase,
               reason=reason, route=route, observed_ms=time.time_ns() // 1_000_000)
    # Broken stdout/log sinks cannot create a signal or interrupt the strategy.
    try:
        print(json.dumps(row, ensure_ascii=False, separators=(",", ":")), flush=True)
    except (OSError, RuntimeError, ValueError):
        pass
