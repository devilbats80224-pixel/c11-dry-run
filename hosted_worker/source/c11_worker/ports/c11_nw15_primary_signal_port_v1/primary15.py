"""Pure signal calculation slice; this never admits, prices or submits any order."""
from dataclasses import dataclass
from hashlib import sha256
from importlib.util import module_from_spec, spec_from_file_location
from math import isfinite
from pathlib import Path
from typing import Iterable, Optional
import pandas as pd

WIDTH_MS = 900_000
WARM = 260
PIN = "a7f88ecf1cc0aba5473a556f40e8086daa4898bea6c808e74162f63ffb8c108b"
SOURCE = Path(__file__).resolve().parents[1] / "frozen_sources/nw_signals.py"
# Compile only verified source bytes: no substitute formula, execution or path discovery.
_original = SOURCE.read_bytes()
if sha256(_original).hexdigest() != PIN:
    raise RuntimeError("pinned NW source digest mismatch")
_mod = module_from_spec(spec_from_file_location("frozen_nw_signal_calculator", SOURCE))
exec(compile(_original, str(SOURCE), "exec"), _mod.__dict__)

@dataclass(frozen=True)
class Candle15:
    open_ms: int
    close: float
    observed_ms: int

@dataclass(frozen=True)
class Signal15:
    status: str
    reason: str
    end_ms: int
    nw: Optional[float] = None
    osc: Optional[float] = None
    raw_triangle: Optional[bool] = None
    green: Optional[bool] = None
    close_above_previous: Optional[bool] = None

def classify(end_ms: int, known_at_ms: int, source: Iterable[Candle15],
             completed_1m_close_same_e: Optional[float] = None) -> Signal15:
    """Only completed, observed 15m closes and independently bound 1m parity.

    Caller binds futures symbol, actual availability and same-E 1m close; 1m
    input is not resampled or inferred here. Output never asserts OI/RPD status.
    Known_at can be later than the exchange candle clock, never earlier.
    """
    def unknown(why):
        return Signal15("UNKNOWN", why, end_ms)
    if type(end_ms) is not int or type(known_at_ms) is not int or end_ms % WIDTH_MS or known_at_ms < end_ms:
        return unknown("BAD_DECISION_CLOCK")
    # The historical signal is a fixed lookback. Roll 260 lookback while
    # performing the exact pinned calculator; use the full contiguous suffix
    # when supplied to retain its pandas rolling numerical initialization.
    p = {}
    for bar in source:
        if type(bar.open_ms) is not int or type(bar.observed_ms) is not int:
            return unknown("BAD_SOURCE_CLOCK")
        e = bar.open_ms + WIDTH_MS
        if e > end_ms:
            continue
        if bar.observed_ms > known_at_ms:
            continue
        if e % WIDTH_MS or e in p or bar.observed_ms < e:
            return unknown("BAD_SOURCE_CLOCK")
        p[e] = bar.close
    closes = []
    e = end_ms
    while e in p and isfinite(p[e]) and p[e] > 0:
        closes.append(float(p[e]))
        e -= WIDTH_MS
    if not closes:
        return unknown("MISSING_CURRENT_CLOSED_15M")
    if len(closes) < WARM:
        return unknown("GAP_OR_WARMUP")
    if completed_1m_close_same_e is None or not isfinite(completed_1m_close_same_e):
        return unknown("NO_SAME_E_1M_CLOSE")
    if closes[0] != float(completed_1m_close_same_e):
        return unknown("1M_15M_CLOSE_MISMATCH")
    chart = _mod.calculate(pd.DataFrame({"close": closes[::-1]}))
    r = chart.iloc[-1]
    if not bool(r.nw_known):
        return unknown("NW_OSCILLATOR_UNKNOWN")
    up = closes[0] > closes[1]
    pulse = bool(r.nw_bullish_rebound)
    green = bool(r.nw_green)
    return Signal15("NW15_PRIMARY_SIGNAL" if pulse and green and up else "NO_SIGNAL",
                    "CLOSE_ONLY_NOT_OI_RPD_OR_TRANSPORT", end_ms, float(r.nw),
                    float(r.nw_osc), pulse, green, up)
