"""Opt-in bounded, aggregate-only owner diagnostics. No storage/transport authority."""
import json
import os
from threading import get_ident

PERIOD=900_000
DEADLINE=60_000
MAX_COUNT=65535
FIELDS=frozenset(('owner_cycles','no_plan','empty_plans','offered_item_peak','no_checkpoint_candidate',
 'plan_boundary','plan_queue','submit_shape','submit_capacity','submit_phase','submit_lease','submit_rate',
 'submit_rate_header_missing','submit_incumbent_unknown','submit_executor_failure','submitted','zero_wire_retired',
 'wire_shape_retired','wire_other_retired','retired_unclassified','validated_lane','reconfirm_valid_lane',
 'entry_no_scope','entry_partial','original_full_commit','original_full_commit_without_callback',
 'original_callback','entry_other_hold'))
VERSION='c11-owner-window-counters-v1'

def stderr_nonblocking(line):
    """Explicit fixture/debug opt-in: never block the native Worker's owner."""
    try:
        # Existing FD convention is not changed, and no extra thread/file or
        # service is created. If this FD may block, intentionally drop.
        if len(line)>2048 or os.get_blocking(2):return False
        return os.write(2,line)==len(line)
    except (AttributeError,OSError,ValueError):
        return False

class WindowCounters:
    def __init__(self,sink):
        if not callable(sink):raise ValueError('COUNTER_SINK_REQUIRED')
        self.owner=get_ident()
        self.sink=sink
        self.end=None
        self.last_closed=None
        self.values={}
        self.saturated=False
    def boundary(self,now):
        if get_ident()!=self.owner or type(now) is not int or now<=0:return False
        end=now//PERIOD*PERIOD
        if self.end is not None and not self.end<=now<self.end+DEADLINE:
            if now<self.end:return False
            old,values,large=self.end,self.values,self.saturated
            # Cut the lifetime before calling the sink; failures cannot retry
            # or back-pressure trading, nor contaminate the next window.
            self.end=None
            self.last_closed=old
            self.values={}
            self.saturated=False
            try:self.sink((json.dumps({'schema':VERSION,'end_ms':old,'counters':values,'saturated':large},
                                    sort_keys=True,separators=(',',':'))+'\n').encode())
            except Exception:pass
        if end<=now<end+DEADLINE and (self.last_closed is None or end>self.last_closed):
            if self.end is None:self.end=end
            return self.end==end
        return False
    def add(self,field,n=1,*,peak=False):
        if (get_ident()!=self.owner or self.end is None or field not in FIELDS or
            type(n) is not int or n<0):return
        before=self.values.get(field,0)
        result=max(before,n) if peak else before+n
        if result>MAX_COUNT:self.saturated=True
        self.values[field]=min(result,MAX_COUNT)
