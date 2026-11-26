"""Adapter for Freqtrade candle refresh: local 5m/15m views from admitted 1m."""
from math import isfinite
from pandas import DataFrame, to_datetime
from freqtrade.enums import CandleType
from causal_bars import FIVE, QUARTER
from closed_minute_basis import project_closed_minutes

WIDTHS = {'5m': FIVE, '15m': QUARTER}
COLUMNS = ('date', 'open', 'high', 'low', 'close', 'volume')

def _exact_direct_15m(frame, start, end):
    if not isinstance(frame,DataFrame) or frame.empty or not set(COLUMNS)<=set(frame.columns):
        return None
    dt=to_datetime(frame['date'],utc=True,errors='coerce')
    if dt.isna().any() or not dt.is_monotonic_increasing or not dt.is_unique:
        return None
    millis=tuple(t.value//1000000 for t in dt)
    required=tuple(range(start,end,QUARTER))
    if not required or millis[-len(required):]!=required:
        return None
    result=frame.loc[:,list(COLUMNS)].tail(len(required)).copy()
    try:
        for op,hi,lo,close,vol in result.iloc[:,1:].itertuples(index=False,name=None):
            if (not all(isfinite(x) for x in (op,hi,lo,close,vol))
                    or not 0<lo<=min(op,close)<=max(op,close)<=hi or vol<0):
                return None
    except (TypeError,ValueError):
        return None
    return result


def refresh_with_local_basis(*, native_refresh, cache_view, store, market_id, now_ms,
                             pair_list, since_ms=None, cache=True, drop_incomplete=None,
                             allow_native_15m_fallback=False, provenance=None,
                             fallback_cache=None):
    """Prefer admitted 1m; optionally use a separate noncanonical 15m frame.

    Never use a wider fallback for 5m or promote it to raw 1m, history state
    or a sufficient entry decision. Existing strategy gates remain in force.
    """
    observed = now_ms()
    local = {}
    other = []
    fallback = {}
    if provenance is not None:
        provenance.clear()
    for key in dict.fromkeys(pair_list):
        pair, timeframe, candle_type = key
        if candle_type != CandleType.FUTURES or timeframe not in WIDTHS:
            other.append(key)
            continue
        width = WIDTHS[timeframe]
        end = observed // width * width
        start = end - 260 * width
        if since_ms is not None:
            start = max(start, (since_ms + width - 1) // width * width)
        symbol = market_id(pair)
        bars = project_closed_minutes(store, symbol, width, start, end, observed)
        if bars is None:
            # No fabricated gap fill and no reuse of a prior-boundary frame.
            # Release acceptance must verify the caller's missing-data behavior.
            if cache:
                cache_view.pop(key,None)
            if allow_native_15m_fallback and timeframe=='15m' and symbol and start<end:
                fallback[key]=(start,end)
            continue
        frame = DataFrame(
            ((r.open_ms,r.op,r.high,r.low,r.close,r.volume) for r in bars),
            columns=COLUMNS,
        )
        frame['date'] = to_datetime(frame['date'], unit='ms', utc=True)
        if cache:
            cache_view[key] = frame
        local[key] = frame
        if provenance is not None:
            provenance[key]='closed_1m_projection'
        if fallback_cache is not None:
            fallback_cache.pop(key,None)
    existing = native_refresh(other,since_ms=since_ms,cache=cache,drop_incomplete=drop_incomplete) if other else {}
    if fallback_cache is not None:
        active=set(fallback)
        for key in list(fallback_cache):
            if key not in active or fallback_cache[key][:2]!=fallback[key]:
                del fallback_cache[key]
    requested=[key for key in fallback if fallback_cache is None or key not in fallback_cache]
    direct=native_refresh(requested,since_ms=since_ms,cache=False,drop_incomplete=True) if requested else {}
    for key,(start,end) in fallback.items():
        cached=fallback_cache.get(key) if fallback_cache is not None else None
        frame=cached[2] if cached is not None else _exact_direct_15m(direct.get(key),start,end)
        if frame is None:
            continue
        if fallback_cache is not None:
            fallback_cache[key]=(start,end,frame)
        if cache:
            cache_view[key]=frame
        if provenance is not None:
            provenance[key]='direct_15m_fallback'
        local[key]=frame
    return {**existing, **local}
