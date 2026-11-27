"""Causal C11 rejected-break gate only; no strategy callbacks or side effects."""
from dataclasses import dataclass
from math import isfinite
from typing import Iterable, Optional

MINUTE_MS = 60_000

@dataclass(frozen=True)
class ClosedMinute:
    end_ms: int
    high: float
    low: float
    close: float

def _positive(*values: float) -> bool:
    return all(isfinite(x) and x > 0 for x in values)

def from_two_5m_extrema(old_high: float, new_high: float, latest_close: float) -> Optional[str]:
    """Classify complete, positive inputs. None is UNKNOWN, not KEEP."""
    if not _positive(old_high, new_high, latest_close):
        return None
    if latest_close > new_high:
        raise AssertionError("latest close above the 5m high")
    return "VETO" if new_high > old_high and latest_close < old_high else "KEEP"

def classify(decision_end_ms: int, chronological: Iterable[ClosedMinute]) -> Optional[str]:
    """Use 10 contiguous ordered completed 1m bars (E−9m … E inclusive).

    The caller supplies one instrument with trusted exchange close-end clocks,
    bars published as of the decision. Future bars are intentionally not consumed.
    Older warm-up bars are ignored. No other instruments or admission policy here.
    """
    if type(decision_end_ms) is not int or decision_end_ms % MINUTE_MS:
        return None
    start = decision_end_ms - 9 * MINUTE_MS
    matched = []
    for bar in chronological:
        end = bar.end_ms
        if type(end) is not int:
            return None
        if end > decision_end_ms:
            break
        if end < start:
            continue
        if end != start + len(matched) * MINUTE_MS:
            return None
        if not _positive(bar.high, bar.low, bar.close):
            return None
        matched.append(bar)
    if len(matched) != 10:
        return None
    return from_two_5m_extrema(
        max(x.high for x in matched[:5]), max(x.high for x in matched[5:]), matched[-1].close
    )
