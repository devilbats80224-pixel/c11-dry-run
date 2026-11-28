"""Only redacted protocol to Docker; no storage, threads, endpoint or authority."""
import json,os,stat
from retained_page_schema import BUDGET_REASONS, PAGE_SCHEMA, SCHEDULE_SCHEMA, accepted_history_trace
from diagnostic_schema import MAX_LINE,accepted_symbol_event
FIELDS={
 'c11-gate-readback-v1':frozenset(('schema','symbol','generation','end_ms','first_gate','owner_wire_charged_lanes','valid_reply_lanes','original_callbacks','observed_expiry_bound_ms','note','transition_ms')),
 'c11-owner-window-counters-v1':frozenset(('schema','end_ms','counters','saturated')),
 'c11-reviewed-startup-info-v1':frozenset(('schema','state')),
}
HISTORY_CODES=frozenset(('HISTORY_HEADROOM_OR_HOT_IO','HISTORY_NO_CURRENT_OPG',
   'HISTORY_WAITING_FOR_OTHER_WIDTH','HISTORY_PAGE_SENT_TO_OWNER_CHILD','HISTORY_PAGE_HELD',
   'HISTORY_SHARED_METER_STALE','HISTORY_SHARED_METER_UNKNOWN','HISTORY_SHARED_LIMIT',
   'HISTORY_MARKER_OR_CURRENT_REFUSED','HISTORY_PAGE_NOT_USABLE','HISTORY_REJECTED_DATA',
   'HISTORY_PAIRED_APPEND','HISTORY_WIDTH_BUFFERED',*BUDGET_REASONS))

def accepted_history_status(d):
    e=d.get('end_ms'); age=d.get('age_ms')
    return (set(d)=={'schema','end_ms','age_ms','status'} and type(e) is int and
            0<e<2**63 and e%900000==0 and type(age) is int and 65000<=age<840000 and
            type(d.get('status')) is str and d['status'] in HISTORY_CODES)

def redacted_fd2_only():
    # Only after explicitly verified authorization. This preserves the original
    # none-like handling for arbitrary logs, including native redirects.
    fd=os.dup(2)
    if not stat.S_ISFIFO(os.fstat(fd).st_mode):
       os.close(fd);raise ValueError('C11_REDACTED_LOG_NONPIPE')
    os.set_blocking(fd,False)
    null=os.open('/dev/null',os.O_WRONLY|os.O_CLOEXEC)
    try:os.dup2(null,1);os.dup2(null,2)
    finally:os.close(null)
    def emit(line):
       try:
          if type(line) is not bytes or not line.endswith(b'\n') or len(line)>MAX_LINE:return False
          def unique(pairs):
             d={}
             for key,value in pairs:
                if key in d:raise ValueError('C11_DIAGNOSTIC_DUPLICATE')
                d[key]=value
             return d
          d=json.loads(line,object_pairs_hook=unique)
          if type(d) is not dict:return False
          if d.get('schema') in ('c11-gate-readback-v1','c11-gate-detail-v1'):
             if not accepted_symbol_event(d):return False
          elif d.get('schema')=='c11-opg-history-status-v1':
             if not accepted_history_status(d):return False
          elif d.get('schema') in (PAGE_SCHEMA,SCHEDULE_SCHEMA):
             if not accepted_history_trace(d):return False
          elif set(d)!=FIELDS.get(d.get('schema')):return False
          return os.write(fd,line)==len(line)
       except (OSError,TypeError,ValueError,KeyError):return False
    return emit,lambda:os.close(fd)
