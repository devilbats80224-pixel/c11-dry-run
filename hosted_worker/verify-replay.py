"""Public output is limited to enumerated replay status and a count."""
from collections import Counter
import json
import os
from pathlib import Path
import sys

mode, log = sys.argv[1:3]
cached = sys.argv[3] if len(sys.argv) >= 4 else "no"
remote = sys.argv[4] if len(sys.argv) >= 5 else "no"
assert remote in {"yes", "no"} and not (cached == remote == "yes")
assert cached in {"yes", "no"}
prepared = sys.argv[5] if len(sys.argv) >= 6 else "no"
assert prepared in {"yes", "no"} and (prepared == "no" or cached == "yes")
no_listing = sys.argv[6] if len(sys.argv) >= 7 else "no"
assert no_listing in {"yes", "no"}
assert no_listing == "no" or (cached == remote == prepared == "no" and mode == "early-roster-return")
background = sys.argv[7] if len(sys.argv) >= 8 else "no"
assert background in {"yes", "no"} and (background == "no" or (cached == "yes" and mode == "yes"))
assert mode in {"early-roster-return", "auto-stale", "reviewed-partial", "yes"}
pinned_no_discovery = sys.argv[8] if len(sys.argv) >= 9 else "no"
assert pinned_no_discovery in {"yes", "no"} and (
    pinned_no_discovery == "no" or (remote == "yes" and no_listing == "no" and mode == "early-roster-return")
)
archive_outage = sys.argv[9] if len(sys.argv) >= 10 else "no"
on_demand = sys.argv[10] if len(sys.argv) >= 11 else "no"
assert archive_outage in {"yes", "no"} and on_demand in {"yes", "no"}
assert [archive_outage,on_demand].count("yes") <= 1
assert (archive_outage == on_demand == "no" or
        (cached == remote == prepared == no_listing == background == pinned_no_discovery == "no"
         and mode == "early-roster-return"))
case_name = "archive-source-outage" if archive_outage == "yes" else "unseeded-on-demand" if on_demand == "yes" else "pinned-remote-no-listing" if pinned_no_discovery == "yes" else "unseeded-no-listing" if no_listing == "yes" else (
    "background-blank" if background == "yes" else mode
)

def failure_summary(actual, phase, reason, *, age=None, persisted=None):
    detail = (f"Expected original 5m VETO in the target cycle; actual {actual}; "
              f"phase={phase}; reason={reason}")
    if age is not None:
        detail += f"; observed_after_close_ms={age}"
    if no_listing == "yes":
        detail += "; no saved checkpoint or pinned cache; historical archives disabled by this fixture"
    elif archive_outage == "yes":
        detail += "; this control simulates rejected official archived requests, not normal operation"
    elif on_demand == "yes":
        detail += "; no preloaded checkpoint or manifest; official archive acquisition is enabled"
    if type(persisted) is int and 0 <= persisted <= 100_000:
        detail += f"; confirmed 1m pages stored: {persisted}"
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print(f"::error title=C11 replay ({case_name})::{detail}")
    if summary_path := os.environ.get("GITHUB_STEP_SUMMARY"):
        try:
            with Path(summary_path).open("a", encoding="utf-8") as summary:
                print(f"### C11 Worker replay: {case_name}", "", detail, "", sep="\n", file=summary)
        except OSError:
            print("C11_JOB_SUMMARY_UNAVAILABLE")

records = []
lines = Path(log).read_text(errors="replace").splitlines()
if on_demand == "yes" or archive_outage == "yes":
    assert "C11_ON_DEMAND_PUBLIC_ARCHIVE_NO_PRELOADED_SOURCE" in lines, "UNSEEDED_PUBLIC_ROUTE_NOT_EXERCISED"
if archive_outage == "yes":
    outage = [json.loads(line) for line in lines if line.startswith('{"status": "JCT_ARCHIVE_OUTAGE_HELD"')]
    if (len(outage) != 1 or type(outage[0].get("archive_public_receipts")) is not int
            or outage[0]["archive_public_receipts"] < 1
            or any(outage[0].get(k) is not True for k in ("no_cursor", "no_strategy_callback", "zero_orders"))):
        failure_summary("missing safe outage result", "archive source response", "OUTAGE_HOLD_NOT_PROVEN")
        raise AssertionError("OUTAGE_HOLD_NOT_PROVEN")
    assert not any(line.startswith('{"status": "JCT_ORIGINAL_') for line in lines)
    print("C11_OFFICIAL_OUTAGE_NO_FAKE_STATE_OR_VERDICT")
    sys.exit(0)
if no_listing == "yes":
    if "C11_CI_ABSENT_CHECKPOINT_NO_LISTING_SETUP" not in lines:
        failure_summary("comparator not set up", "input setup", "UNSEEDED_SETUP_NOT_PROVEN")
        raise AssertionError("UNSEEDED_SETUP_NOT_PROVEN")
    if "C11_NO_LISTING_CASE_OPENED_ARCHIVE" in lines:
        failure_summary("opened listing archive", "checkpoint preparation", "UNSEEDED_LISTING_LOOKUP")
        raise AssertionError("UNSEEDED_LISTING_LOOKUP")

for line in lines:
    if line.startswith("{"):
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if isinstance(record, dict) and record.get("status") in {
            "JCT_ORIGINAL_5M_VETO", "JCT_REPLAY_HELD"
        }:
            records.append(record)
if len(records) != 1:
    failure_summary("no unique Worker result", "receipt", "REPLAY_RESULT_MISSING_OR_DUPLICATE")
    raise AssertionError("C11_MISSING_UNIQUE_REPLAY_RESULT")
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
      "grants_written", int(stages.get("GRANT_WRITTEN", 0)),
      "replies_accepted", int(stages.get("REPLY_ACCEPTED", 0)),
      "replies_rejected", int(stages.get("REPLY_REJECTED", 0)),
      "requests_expired", int(stages.get("EXPIRED", 0)),
      "budget_held", int(stages.get("BUDGET_HELD", 0)),
      "tape_windows_missing", missing_count,
      "archive_requests", int(r.get("archive_public_receipts",0)))
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
    "FACT_NO_SUCCESSFUL_READ_THIS_CYCLE", "FACT_SYMBOL_NOT_OBSERVED", "FACT_GENERATION_CHANGED",
    "FACT_BOUNDARY_CHANGED", "FACT_ORIGINAL_LEASE_EXPIRED", "FACT_OBSERVED_LEASE_EXPIRED",
    "FACT_PAIR_CHANGED", "FACT_GENERATION_CAP_EXPIRED", "FACT_EMPTY_AFTER_CYCLE_FILTERS",
}
for row in r.get("reply_refusals", [])[:10]:
    scope = row.get("fixture_scope_read") if isinstance(row, dict) else None
    if isinstance(scope, dict) and scope.get("lane") in ("minute", "primary"):
        reasons = scope.get("reasons", [])
        if isinstance(reasons, list) and all(isinstance(x, str) and x in safe_scope_reasons for x in reasons):
            print("C11_HOSTED_REPLAY_SCOPE_DIAGNOSTIC", mode, scope["lane"], ",".join(reasons))
if status == "JCT_REPLAY_HELD":
    # Public CI sees an allowlisted first reason and counts, never raw jobs,
    # symbols, timing, exception messages, receipts, or wire data.
    safe_persist = (
        "DELIVERY_SCOPE_LOST", "INVALID_RECEIPT_CLOCK",
        "NATIVE_BATCH_REFUSED", "SQLITE_BUSY",
    )
    first_persist = None
    if isinstance(r.get("persist_refusals"), list) and r["persist_refusals"]:
        reasons = []
        for refusal in r["persist_refusals"]:
            reason = refusal.get("stage_reason") if isinstance(refusal, dict) else None
            reasons.append(reason if reason in safe_persist else "UNCLASSIFIED")
        first_persist = reasons[0]
        counts = Counter(reasons)
        summary = ",".join(f"{reason}:{counts[reason]}" for reason in sorted(counts))
        print("C11_HOSTED_PERSIST_DIAGNOSTIC", mode, f"first={reasons[0]}", f"counts={summary}")
    safe_prefix_refusals = (
        "RPD_CHECKPOINT_IDENTITY_MISMATCH", "RPD_OBSERVATION_AFTER_CYCLE_CLOCK",
        "NO_AUTHENTICATED_LOCAL_STATE",
        "NATIVE_PREPARATION_REFUSED", "NATIVE_MINUTES_REFUSED",
        "NATIVE_BUDGET_OR_AUTHORITY_HELD", "COLD_PREFIX_NO_REVIEWED_SOURCE",
        "AUTO_RECENT_MINUTES_ONLY_ORIGINAL_STATE_UNVERIFIED",
    )
    phases = ("cold", "carry", "entry", "temporarily_absent")
    phase = r.get("phase")
    phase = phase if phase in phases else "unknown"
    observations = r.get("prefix_observations")
    observation = next((x for x in observations if isinstance(x, dict)
                        and x.get("phase") in phases and x.get("reason") in safe_prefix_refusals
                        and type(x.get("age_ms")) is int and 0 <= x["age_ms"] < 900_000), None) if isinstance(observations, list) else None
    if observation:
        phase, reason, age = observation["phase"], observation["reason"], observation["age_ms"]
        print("C11_HOSTED_PREFIX_DIAGNOSTIC", mode, f"phase={phase}", f"first={reason}", f"age_ms={age}")
    else:
        reason, age = first_persist or "UNCONFIRMED", None
        print("C11_HOSTED_PREFIX_DIAGNOSTIC", mode, f"phase={phase}", f"first={reason}")
    failure_summary("JCT_REPLAY_HELD", phase, reason, age=age, persisted=stages.get("PERSISTED"))
    # Only emit machine status tokens, not any wire data, paths, or IDs.
    import re
    for field in ("entry_gate_statuses", "entry_advice_statuses", "entry_primary", "entry_rpd_checks"):
        values = r.get(field) or []
        if isinstance(values, str):
            values = [values]
        safe = [v for v in values if isinstance(v, str)
                and re.fullmatch(r"[A-Z_0-9]{2,65}", v)]
        if safe:
            print("C11_HOSTED_ENTRY_DIAGNOSTIC", mode, field, ",".join(safe[:30]))
assert status == "JCT_ORIGINAL_5M_VETO" and r.get("zero_orders") is True
assert not any("Task exception was never retrieved" in line for line in lines), (
    "C11_REPLAY_UNHANDLED_TRANSPORT_TASK"
)
assert bool(r.get("verified_raw_archive_cache")) == (cached == "yes")
assert bool(r.get("pinned_remote")) == (remote == "yes")
if remote == "yes":
    assert r["archive_public_receipts"] > 0, "PINNED_SOURCE_BYTES_NOT_FETCHED"
if cached == "yes":
    assert r["archive_public_receipts"] == 0, "CACHE_REOPENED_ARCHIVE_NETWORK"
decisions = r.get("decision_sources", [])
optional_native_veto = any(
    d.get("status") == "VETO" and d.get("method") == "record_verified_veto"
    and d.get("owner_code") == "native_primary_lane.py"
    and type(r.get("replay_boundary")) is int and d.get("boundary") == r["replay_boundary"]
    and type(d.get("age_ms")) is int and 0 <= d["age_ms"] < 60_000 for d in decisions
)
callback_native_veto = False
if mode in {"reviewed-partial", "auto-stale", "early-roster-return", "yes"} and r.get("strategy_native_veto") is True:
    calls = [item for item in r.get("original_entry_advice", [])
             if item.get("status") == "ORIGINAL_NATIVE_CALLBACK"]
    callback_native_veto = (
        len(calls) == 1 and type(calls[0].get("last_enter_long")) is int
        and calls[0]["last_enter_long"] == 0 and type(calls[0].get("age_ms")) is int
        and 0 <= calls[0]["age_ms"] < 60_000
        and any(d.get("status") == "VETO" and d.get("owner_code") == "causal_bars.py"
                and d.get("method") == "inputs" and d.get("boundary") == r["replay_boundary"]
                and d.get("inside_original_target_callback") is True
                and d.get("age_ms") == calls[0]["age_ms"] for d in decisions)
    )
if prepared == "yes":
    assert "C11_RECOVERY_REUSED_PREPARED_CHECKPOINT" in lines
    assert r["archive_public_receipts"] == 0
    assert r.get("shared_1m_requests"), "NATIVE_MINUTE_REPAIR_NOT_EXERCISED"
    assert all(r["replay_boundary"] - (260 * 15 + 45) * 60_000 <= start < stop <= r["replay_boundary"]
               for start, stop, _, _ in r["shared_1m_requests"]), "REQUEST_OUTSIDE_RECENT_STRATEGY_WINDOW"
    if not callback_native_veto:
        advice = r.get("original_entry_advice", [])
        first = advice[0].get("status") if advice and isinstance(advice[0], dict) else None
        reason = first if first in {"HOLD_NO_OWNED_SCOPE", "HOLD_NOT_READY",
                                     "HOLD_UNPROVEN_ANALYZED_FRAME"} else "UNCONFIRMED"
        failure_summary("original filter reached but full original callback absent", "strategy handoff", reason)
        raise AssertionError("PREPARED_CHECKPOINT_ORIGINAL_CALLBACK_NOT_PROVEN")
    if mode == "auto-stale":
        age = r.get("stale_cursor_age_ms")
        if type(age) is not int or age < 90*86_400_000:
            failure_summary("prior persisted cursor was not proven to be old", "recover stale checkpoint",
                            "STALE_CURSOR_AGE_NOT_PROVEN")
            raise AssertionError("STALE_CURSOR_AGE_NOT_PROVEN")
        # This 90-day floor describes this regression fixture, not the
        # policy's state-age or validity limit.
        print("C11_STALE_CHECKPOINT_NO_LISTING_AND_ORIGINAL_CALLBACK")
elif background == "yes":
    assert "C11_RAW_ONLY_STATE_READY_BEFORE_CANDIDATE" in lines, "BACKGROUND_CHECKPOINT_NOT_READY"
    assert r["archive_public_receipts"] == 0
    assert r["shared_1m_requests"], "BACKGROUND_MINUTE_CATCHUP_NOT_PROVEN"
    assert all(r["replay_boundary"] - (260 * 15 + 45) * 60_000 <= start < stop <= r["replay_boundary"]
               for start,stop,_,_ in r["shared_1m_requests"]), "BACKGROUND_REQUEST_BEFORE_RECENT_WINDOW"
    assert callback_native_veto, "BACKGROUND_ORIGINAL_CALLBACK_NOT_PROVEN"
elif on_demand == "yes":
    assert r.get("discovery_configured") is True, "ON_DEMAND_DISCOVERY_SWITCHED_OFF"
    assert r["archive_public_receipts"] > 0, "ON_DEMAND_REAL_OFFICIAL_BYTES_NOT_READ"
    if not callback_native_veto:
        failure_summary("automatic acquisition completed but original callback absent", "strategy handoff",
                        "ON_DEMAND_ORIGINAL_CALLBACK_NOT_PROVEN")
        raise AssertionError("ON_DEMAND_ORIGINAL_CALLBACK_NOT_PROVEN")
elif pinned_no_discovery == "yes":
    assert "C11_REVIEWED_REMOTE_ONLY_NO_LISTING_DISCOVERY" in lines
    assert r.get("discovery_configured") is False, "PINNED_REPLAY_OPENED_LISTING_DISCOVERY"
    assert r["archive_public_receipts"] > 0, "PINNED_REPLAY_NO_REVIEWED_BYTES"
    if not callback_native_veto:
        failure_summary("reviewed source used but original callback absent", "strategy handoff",
                        "PINNED_NO_DISCOVERY_ORIGINAL_CALLBACK_NOT_PROVEN")
        raise AssertionError("PINNED_NO_DISCOVERY_ORIGINAL_CALLBACK_NOT_PROVEN")
elif no_listing == "yes":
    assert r.get("discovery_configured") is False, "UNSEEDED_LISTING_DISCOVERY_UNEXPECTEDLY_ENABLED"
    assert r["archive_public_receipts"] == 0
    assert r["shared_1m_requests"], "UNSEEDED_MINUTE_REPAIR_NOT_PROVEN"
    assert all(r["replay_boundary"] - (260 * 15 + 45) * 60_000 <= start < stop <= r["replay_boundary"]
               for start, stop, _, _ in r["shared_1m_requests"]), "UNSEEDED_REQUEST_BEFORE_RECENT_WINDOW"
    assert callback_native_veto, "UNSEEDED_ORIGINAL_CALLBACK_NOT_PROVEN"
else:
    assert optional_native_veto or callback_native_veto, "ORIGINAL_STRATEGY_VERDICT_NOT_PROVEN"
print("C11_HOSTED_ORIGINAL_PROOF", mode, "ENTRY_CALLBACK_5M_VETO" if callback_native_veto else "ORIGINAL_5M_FILTER_VETO")
assert r.get("automatic_archive") is (mode != "reviewed-partial")
if no_listing == "no" and mode != "reviewed-partial" and cached == remote == "no":
    assert r.get("discovery_configured") is True, "UNSEEDED_DISCOVERY_NOT_ENABLED"
    assert r.get("archive_public_receipts") == 74, "UNSEEDED_PUBLIC_ARCHIVE_NOT_VERIFIED"
assert r.get("automatic_stale_repair") is (mode == "auto-stale")
assert r.get("reviewed_partial_repair") is (mode == "reviewed-partial")
if mode == "reviewed-partial":
    assert lines.count("C11_REVIEWED_FIXTURE_LEASE_CONTROLS_PASS") == 1
if mode == "early-roster-return":
    assert r.get("roster_drop_return_verified") is True
    assert 0 <= r["initial_cold_start_age_ms"] < 60_000
assert 0 <= r["observed_replay_age_ms"] < 60_000
print("C11_HOSTED_WORKER_CASE_PASS", mode)
