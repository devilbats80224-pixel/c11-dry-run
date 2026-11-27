"""Read-only basis for closed 5m/15m views. No alternate dataset or fetch."""
from math import fsum
from causal_bars import ClosedBar, FIVE, MINUTE, QUARTER

def project_closed_minutes(store, symbol, width, start, end, as_of):
    """Store supplies admitted 1m candles only. None means not fully available."""
    if (type(symbol) is not str or not symbol
            or any(type(x) is not int for x in (width, start, end, as_of))
            or width not in (FIVE, QUARTER) or not 0 <= start < end <= as_of
            or start % width or end % width or end - start > 260 * QUARTER):
        return None
    rows = store.range(symbol, MINUTE, start, end, as_of)
    if (type(rows) is not tuple or len(rows) != (end - start) // MINUTE
            or not all(type(r) is ClosedBar and r.open_ms == start + i * MINUTE
                       and r.received_ms <= as_of and r.valid(MINUTE)
                       for i, r in enumerate(rows))):
        return None
    step = width // MINUTE
    output = []
    try:
        for i in range(0, len(rows), step):
            block = rows[i:i + step]
            result = ClosedBar(
                block[0].open_ms, max(r.received_ms for r in block),
                block[0].op, max(r.high for r in block), min(r.low for r in block),
                block[-1].close, fsum(r.volume for r in block), fsum(r.quote for r in block),
            )
            if not result.valid(width):
                return None
            output.append(result)
    except (ValueError, OverflowError):
        return None
    return tuple(output)


def missing_projection_quarters(store, symbol, end, as_of, count=260):
    """Compute which 15m buckets lack complete admitted 1m coverage.

    Do not count legacy, independently fetched 15m rows as proof. Keep the
    query within the native warmup window, not back to listing or a stale
    checkpoint. An in-flight page can use this output to de-duplicate work.
    """
    if (type(symbol) is not str or not symbol
            or any(type(x) is not int for x in (end, as_of, count))
            or not 1 <= count <= 260 or end % QUARTER
            or not count * QUARTER <= end <= as_of):
        raise ValueError('INVALID_MINUTE_WARMUP_WINDOW')
    start = end - count * QUARTER
    rows = store.db.execute(
        'SELECT open_ms FROM candle WHERE symbol=? AND width=? AND open_ms>=? '
        'AND open_ms<? AND received_ms<=?',
        (symbol, MINUTE, start, end, as_of),
    )
    opens = {row[0] for row in rows}
    return {q for q in range(start, end, QUARTER)
            if any(t not in opens for t in range(q, q+QUARTER, MINUTE))}
