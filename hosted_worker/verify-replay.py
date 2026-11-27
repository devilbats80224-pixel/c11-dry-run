"""Public output is limited to enumerated replay status and a count."""
import json
from pathlib import Path
import sys

mode, log = sys.argv[1:3]
cached = sys.argv[3] if len(sys.argv) == 4 else "no"
assert cached in {"yes", "no"}
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
stages = r.get("retained_stages", {})
missing = r.get("uncovered_tape_requests", 0)
assert type(missing) in (int, list)
missing_count = missing if type(missing) is int else len(missing)
print("C11_HOSTED_REPLAY", mode, status, "lacks_state", recent, "cache_required", cached,
      "pages_requested", len(r.get("shared_1m_requests", [])),
      "pages_persisted", int(stages.get("PERSISTED", 0)),
      "replies_rejected", int(stages.get("REPLY_REJECTED", 0)),
      "budget_held", int(stages.get("BUDGET_HELD", 0)),
      "tape_windows_missing", missing_count)
safe_refusals = {"STALE_SCOPE_OR_DELIVERY", "SCOPE_INVALID_AT_READ", "PARENT_DELIVERY_EXPIRED",
                 "SCOPE_AND_PARENT_DELIVERY_EXPIRED", "PROTOCOL", "HTTP_STATUS", "NO_HTTP_RECEIPT",
                 "NOT_DISPATCHED_BY_CHILD", "WEIGHT_HEADER", "RECEIPT_TIME", "ROW_COUNT"}
refusals = sorted({row.get("stage_reason") for row in r.get("reply_refusals", [])
                   if isinstance(row, dict) and row.get("stage_reason") in safe_refusals})
if refusals:
    print("C11_HOSTED_REPLAY_REJECTION", mode, ",".join(refusals))
safe_scope_reasons = {
    "JOB_INVALID", "FACT_NOT_CURRENT", "NO_CYCLE_HEADROOM", "MINUTE_ORIGINAL_WIRE_BUSY",
    "OWNER_CYCLE_UNAVAILABLE", "PROTECTION_PRIORITY", "OUTSIDE_SCOPE", "JOB_FACT_MISMATCH",
    "SAME_SYMBOL_WIRE_BUSY", "PLAN_NOT_CURRENT", "ALREADY_DECIDED", "HOLD_NOT_REOBSERVED",
}
for row in r.get("reply_refusals", [])[:10]:
    scope = row.get("fixture_scope_read") if isinstance(row, dict) else None
    if isinstance(scope, dict) and scope.get("lane") in ("minute", "primary"):
        reasons = scope.get("reasons", [])
        if isinstance(reasons, list) and all(isinstance(x, str) and x in safe_scope_reasons for x in reasons):
            print("C11_HOSTED_REPLAY_SCOPE_DIAGNOSTIC", mode, scope["lane"], ",".join(reasons))
assert status == "JCT_ORIGINAL_5M_VETO" and r.get("zero_orders") is True
assert bool(r.get("verified_raw_archive_cache")) == (cached == "yes")
if cached == "yes":
    assert r["archive_public_receipts"] == 0, "CACHE_REOPENED_ARCHIVE_NETWORK"
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
