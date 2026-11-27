"""Public output is limited to enumerated replay status and a count."""
import json
from pathlib import Path
import sys

mode, log = sys.argv[1:]
assert mode in {"early-roster-return", "auto-stale", "reviewed-partial"}
records = []
for line in Path(log).read_text(errors="replace").splitlines():
    if line.startswith("{"):
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if isinstance(record, dict) and record.get("status") in {
            "JCT_ORIGINAL_5M_VETO", "JCT_REPLAY_HELD"
        }:
            records.append(record)
assert len(records) == 1, "C11_MISSING_UNIQUE_REPLAY_RESULT"
r = records[0]
status = r["status"]
recent = "AUTO_RECENT_MINUTES_ONLY_ORIGINAL_STATE_UNVERIFIED" in r.get("prefix_statuses", [])
print("C11_HOSTED_REPLAY", mode, status, "lacks_state", recent)
assert status == "JCT_ORIGINAL_5M_VETO" and r.get("zero_orders") is True
assert any(item.get("status") == "VETO" and item.get("method") == "record_verified_veto"
           for item in r.get("decision_sources", [])), "ORIGINAL_STRATEGY_VERDICT_NOT_PROVEN"
assert r.get("automatic_archive") is (mode != "reviewed-partial")
assert r.get("automatic_stale_repair") is (mode == "auto-stale")
assert r.get("reviewed_partial_repair") is (mode == "reviewed-partial")
if mode == "early-roster-return":
    assert r.get("roster_drop_return_verified") is True
    assert 0 <= r["initial_cold_start_age_ms"] < 60_000
assert 0 <= r["observed_replay_age_ms"] < 60_000
print("C11_HOSTED_WORKER_CASE_PASS", mode)
