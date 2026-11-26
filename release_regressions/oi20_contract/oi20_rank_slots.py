"""Validate original provider rank slots. Legacy full frames remain byte-compatible."""
from .bridge_protocol import need

PARTIAL = 'DEGRADED_PARTIAL_OBSERVATION'

def rank_slots(rows, ranks):
    need(type(rows) is list, 'C11_OI20_ROWS')
    if ranks is None:
        need(len(rows)==60,'C11_OI20_FULL_PAGE')
        return range(1,61)
    need(type(ranks) is list and 0<len(rows)==len(ranks)<60 and
         all(type(rank) is int and 1<=rank<=60 for rank in ranks) and
         all(a<b for a,b in zip(ranks,ranks[1:])), 'C11_OI20_PARTIAL_RANK_SLOTS')
    return ranks

def bridge_slots(frame):
    has_ranks='provider_ranks_v1' in frame
    need(frame.get('status')==('FRESH_PARTIAL' if has_ranks else 'FRESH'),'C11_OI20_FRAME_STATUS')
    if has_ranks:
        need(type(frame['provider_ranks_v1']) is list,'C11_OI20_PARTIAL_RANK_SLOTS')
    return rank_slots(frame['rows'],frame.get('provider_ranks_v1'))
