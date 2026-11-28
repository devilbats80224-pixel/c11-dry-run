"""Passive, exact-owner entry diagnostics. No evaluation or I/O into trade state."""
from contextvars import ContextVar
import json
import time

ACTIVE = ContextVar("c11_entry_verdict", default=None)
ADMISSION = ContextVar("c11_native_admission", default=None)
DIAGNOSTIC_SINK = ContextVar("c11_entry_verdict_diagnostic_sink", default=None)
SCHEMA = "c11-entry-verdict-v1"
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

REASONS_BY_PHASE = {
    "primary": PRIMARY_REASONS,
    "admission": ADMISSION_REASONS,
    "order": ORDER_REASONS,
}
VERDICT_FIELDS = frozenset(("schema", "symbol", "end_ms", "phase", "reason", "route", "observed_ms"))


def valid_base(base):
    return 0 < len(base) <= 19 and base.isalnum()


def route_valid(route, end, phase, reason):
    return (type(route) is str and route in ROUTES
            and type(end) is int and 0 < end < 2**63
            and end % (Q if route == "c11_primary" else 300_000) == 0
            and type(phase) is str and type(reason) is str
            and reason in REASONS_BY_PHASE.get(phase, ()))


def accepted_verdict_event(row):
    if type(row) is not dict or set(row) != VERDICT_FIELDS or row["schema"] != SCHEMA:
        return False
    symbol = row["symbol"]
    observed = row["observed_ms"]
    return (type(symbol) is str and symbol.endswith("USDT") and valid_base(symbol[:-4])
            and type(observed) is int and 0 < observed < 2**63
            and route_valid(row["route"], row["end_ms"], row["phase"], row["reason"]))

def identity(pair, end, width):
    if type(pair) is not str or not pair.endswith("/USDT:USDT") or type(end) is not int or not 0 < end or end % width:
        return None
    base = pair.removesuffix("/USDT:USDT")
    return (base+"USDT", end) if valid_base(base) else None

def mark_primary(reason, force=False):
    state = ACTIVE.get()
    if state is None or type(reason) is not str or reason not in PRIMARY_REASONS:
        return
    if force or state[0] is None:
        state[0] = reason

def publish(pair, end, phase, reason, route="c11_primary"):
    if not route_valid(route, end, phase, reason):
        return
    key = identity(pair, end, Q if route == "c11_primary" else 300_000)
    if key is None:
        return
    row = dict(schema=SCHEMA, symbol=key[0], end_ms=key[1], phase=phase,
               reason=reason, route=route, observed_ms=time.time_ns() // 1_000_000)
    # A bound production sink alone is permitted to emit here. Never fall
    # back to raw stdout if that sink refuses or is temporarily unavailable.
    try:
        line = json.dumps(row, ensure_ascii=False, separators=(",", ":"))
        sink = DIAGNOSTIC_SINK.get()
        if sink is None:
            print(line, flush=True)
        else:
            sink((line + "\n").encode("utf-8"))
    except (OSError, RuntimeError, ValueError):
        pass
