"""Exact installed analyzer's captured original OHLCV against committed rows."""
from committed_projection import frame_sha
def same_native_frame(store,scope,frame,wall,*,quarter_view=None):
    p,rows=frame_sha(frame,scope.end)
    selected=getattr(store,'strategy_quarters',None)
    base=quarter_view
    if base is None:
        start=scope.end-260*900000
        base=(selected(scope.symbol,start,scope.end,wall,terminal_end=scope.end)
              if callable(selected) else store.range(scope.symbol,900000,start,scope.end,wall))
    expected=tuple((b.open_ms,b.op,b.high,b.low,b.close,b.volume) for b in base)
    return p if len(expected)==260 and tuple(rows[-260:])==expected else None
