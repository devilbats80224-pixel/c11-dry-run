"""The parent separates scope loss from elapsed IPC; policy is unchanged."""
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import patch

root = Path(__file__).parent / "source"
sys.path[:0] = [str(root / "c11_owned_modules"), str(root / "c11_worker")]
from shared_history_forwarder import SharedHistoryForwarder
from retained_page_schema import accepted_history_trace

end = 3_600_000
job = SimpleNamespace(symbol="JCTUSDT", generation=1, end=end, start=end-900_000, stop=end, n=15)

for authoritative, now, expected in (
    (False, 1.5, "SCOPE_INVALID_AT_READ"),
    (True, 2.0, "PARENT_DELIVERY_EXPIRED"),
    (False, 2.5, "SCOPE_AND_PARENT_DELIVERY_EXPIRED"),
):
    audits = []
    finishes = []
    def audit(j, stage, reason, **evidence):
        event = dict(schema="c11-shared-history-page-v1", symbol=j.symbol,
                     generation=j.generation, end_ms=j.end, at_ms=end+5000,
                     page_start_ms=j.start, page_end_ms=j.stop,
                     requested_rows=j.n, stage=stage, stage_reason=reason, **evidence)
        assert accepted_history_trace(event)
        audits.append(event)
    parent = SimpleNamespace(waiting={"t": (SimpleNamespace(authority=lambda _: authoritative),
                                           job, None, 2.0, end+4500)},
                             audit=audit, finish=lambda *a, **k: finishes.append((a,k)))
    with patch("shared_history_forwarder.monotonic", return_value=now):
        SharedHistoryForwarder.inbound(parent, dict(tag="t", status=200, used=1,
            rows=[None]*15, received=end+4700, wire_ms=end+4600))
    assert len(audits)==1 and audits[0]["stage_reason"]==expected, audits
    assert audits[0]["parent_delivery_margin_ms"]==round((2.0-now)*1000)
    assert finishes==[(("t",None),{"cancel":True})]
print("C11_PARENT_REJECTION_SPLIT_PASS")


def granted_closed_data_has_separate_receipt_authority():
    from admission_minute_job import MinuteHistoryJob
    expired_rank=False
    start=end-1_800_000
    stop=start+900_000
    history=MinuteHistoryJob("JCTUSDT",1,end,start,"a"*32,start,stop)
    rows=[[t,'100','104','99','101','10',t+60_000-1,'1000',5,'4','400','0']
          for t in range(start,stop,60_000)]
    lane=SimpleNamespace(authority=lambda _: expired_rank,
        receipt_authority=lambda candidate,granted: candidate is history and granted==end+4500)
    outcomes=[]
    parent=SimpleNamespace(waiting={'t':(lane,history,None,2.0,end+4500)},
        wall=lambda:end+5000,audit=lambda *_a,**_k:None,
        finish=lambda *a,**k:outcomes.append((a,k)))
    with patch('shared_history_forwarder.monotonic',return_value=1.5):
        SharedHistoryForwarder.inbound(parent,dict(tag='t',status=200,used=1,
            rows=rows,received=end+4700,wire_ms=end+4600))
    assert outcomes==[(('t',(end+4500,end+4700,rows)),{})], (
        'C11_CLAMPED_RANK_LIFETIME_DISCARDS_ALREADY_GRANTED_CLOSED_DATA',outcomes)
    # No still-current rank means an ungranted request may not start on wire.
    parent.waiting['t']=(lane,history,None,2.0,None)
    with patch('shared_history_forwarder.monotonic',return_value=1.5):
        SharedHistoryForwarder.inbound(parent,dict(ready='t',abi=3))
    assert outcomes[-1]==(('t',None),{'cancel':True})


granted_closed_data_has_separate_receipt_authority()
