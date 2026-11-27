"""Offline decision only. All input adapters and authority remain outside."""
from dataclasses import dataclass
from decimal import Decimal
from math import isfinite
from typing import Iterable
from c11_nw15_primary_signal_port_v1.primary15 import Candle15, WIDTH_MS, classify as nw
from c11_closed5_rejected_break_port_v1.rejected_break import ClosedMinute, MINUTE_MS, classify as rejected

@dataclass(frozen=True)
class Scope:
    symbol: str
    end_ms: int

@dataclass(frozen=True)
class BoundOI:
    """Injected validated decision binding; old producer diagnostic alone is NOT this."""
    scope: Scope
    pct: Decimal
    observed_ms: int
    valid_until_ms: int  # translate source endpoint semantics in trusted adapter

@dataclass(frozen=True)
class Rpd5:
    """Injected exact same-E carried RPD5 state, not an implementation of RPD."""
    scope: Scope
    observed_ms: int
    direction: str
    score: float

@dataclass(frozen=True)
class MinuteInput:
    symbol: str
    observed_ms: int
    bar: ClosedMinute

@dataclass(frozen=True)
class Decision:
    state: str
    reason: str

def decide(scope: Scope, *, as_of_ms: int, valid_until_ms: int,
           candle_symbol: str, candles15: Iterable[Candle15], minutes: Iterable[MinuteInput],
           oi: BoundOI | None, rpd: Rpd5 | None) -> Decision:
    """Exclusive decision expiry supplied by caller, not inferred from archived availability.
    Successful output is a *signal-only* handoff; never an order or deploy verdict.
    Receipt clocks must be actual provenance from a trusted adapter, not candle timestamps.
    """
    E = scope.end_ms
    def out(state, reason):
        return Decision(state, reason)

    if (not scope.symbol or not all(type(x) is int for x in (E, as_of_ms, valid_until_ms))
            or E % WIDTH_MS or not E <= as_of_ms < valid_until_ms):
        return out("UNKNOWN", "BAD_OR_EXPIRED_DECISION_CLOCK")
    if candle_symbol != scope.symbol:
        return out("UNKNOWN", "CANDLE_SYMBOL_MISMATCH")
    if (type(oi) is not BoundOI or oi.scope != scope
            or not all(type(x) is int for x in (oi.observed_ms, oi.valid_until_ms))
            or not oi.observed_ms <= as_of_ms < oi.valid_until_ms
            or type(oi.pct) is not Decimal or not oi.pct.is_finite()):
        return out("UNKNOWN", "OI20_BINDING_UNKNOWN")
    if oi.pct < 20:
        return out("BLOCKED", "OI20_BELOW_THRESHOLD")
    if (type(rpd) is not Rpd5 or rpd.scope != scope
            or type(rpd.observed_ms) is not int or not E <= rpd.observed_ms <= as_of_ms
            or rpd.direction not in ("LONG", "SHORT")
            or type(rpd.score) not in (float, int) or not isfinite(rpd.score)):
        return out("UNKNOWN", "RPD5_SAME_E_UNKNOWN")
    if rpd.direction != "LONG" or rpd.score < 0:
        return out("BLOCKED", "RPD5_NOT_LONG_NONNEGATIVE")
    try:
        bars = []
        for row in minutes:
            end = row.bar.end_ms
            if type(end) is not int:
                return out("UNKNOWN", "MINUTE_INPUT_INVALID")
            if not E - 9 * MINUTE_MS <= end <= E:
                continue
            if type(row.observed_ms) is not int:
                return out("UNKNOWN", "MINUTE_INPUT_INVALID")
            if row.observed_ms > as_of_ms:
                continue
            if row.symbol != scope.symbol or row.observed_ms < end:
                return out("UNKNOWN", "MINUTE_INPUT_INVALID")
            h, l, c = row.bar.high, row.bar.low, row.bar.close
            if not all(type(x) in (float, int) and isfinite(x) and x > 0 for x in (h,l,c)) or not l <= c <= h:
                return out("UNKNOWN", "MINUTE_INPUT_INVALID")
            bars.append(row.bar)
        ends = [bar.end_ms for bar in bars]
        if ends != list(range(E - 9 * MINUTE_MS, E + 1, MINUTE_MS)):
            return out("UNKNOWN", "MINUTE_SERIES_INCOMPLETE")
        signal = nw(E, as_of_ms, candles15, bars[-1].close)
        if signal.status == "UNKNOWN":
            return out("UNKNOWN", signal.reason)
        if signal.status != "NW15_PRIMARY_SIGNAL":
            return out("NO_SIGNAL", "NW15_NO_PRIMARY")
        gate = rejected(E, bars)
        if gate == "VETO":
            return out("VETO", "CLOSED5_REJECTED_BREAK")
        if gate != "KEEP":
            return out("UNKNOWN", "CLOSED5_UNKNOWN")
    except (ArithmeticError, AttributeError, AssertionError, TypeError, ValueError):
        return out("UNKNOWN", "MALFORMED_INPUT")
    return out("PRIMARY_SIGNAL_ONLY", "NW15_OI20_RPD5_CLOSED5")
