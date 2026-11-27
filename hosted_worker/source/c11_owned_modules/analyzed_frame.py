"""Boundary adapter for real Freqtrade analyzed dataframes; never changes OHLCV."""
from datetime import timezone
from pandas import DataFrame,Timestamp
from committed_projection import FRAME_COLUMNS

def exact_analyzed_ohlcv(frame):
    if type(frame) is not DataFrame or not frame.columns.is_unique:
        raise ValueError('ANALYZED_FRAME_NOT_UNIQUE')
    columns=tuple(frame.columns)
    if any(type(x) is not str for x in columns) or tuple(c for c in columns if c in FRAME_COLUMNS)!=FRAME_COLUMNS:
        raise ValueError('ANALYZED_OHLCV_SCHEMA')
    # Reject aliased/offset dates rather than turning them into authority by conversion.
    data=frame.loc[:,list(FRAME_COLUMNS)]
    for dt in data['date']:
        if type(dt) is not Timestamp or not (dt.tzinfo is timezone.utc or str(dt.tzinfo)=='UTC'):
            raise ValueError('ANALYZED_OHLCV_NOT_UTC')
    return data
