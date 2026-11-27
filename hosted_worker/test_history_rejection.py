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
