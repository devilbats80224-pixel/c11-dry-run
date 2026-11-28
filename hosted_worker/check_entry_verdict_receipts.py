"""Check real packaged native strategy diagnostics from the fixture."""
import json
import os
from pathlib import Path
import sys

rows = []
known_no_signal_offsets = set()
last_result = ""
for line in Path(sys.argv[1]).read_text().splitlines():
    if line.startswith("C11_LATE_ORIGINAL_RESULT "):
        last_result = line.split()[1]
    if line.startswith("C11_FRESH_READY_OFFSET_MS ") and last_result == "NO_SIGNAL":
        known_no_signal_offsets.add(int(line.split()[1]))
    if not line.startswith("{"):
        continue
    try:
        event = json.loads(line)
    except ValueError:
        continue
    if (event.get("schema") == "c11-entry-verdict-v1" and event.get("symbol") == "STBLUSDT"
            and event.get("phase") == "primary"):
        rows.append(event)
keys = {(row["symbol"], row["end_ms"], row["observed_ms"]) for row in rows}
recorded_no_signal_offsets = {row["observed_ms"] - row["end_ms"]
                              for row in rows if row["reason"] == "NO_PRIMARY_SIGNAL"}
if (not rows or len(keys) != len(rows)
        or not known_no_signal_offsets
        or not known_no_signal_offsets <= recorded_no_signal_offsets
        or not all(r["observed_ms"] >= r["end_ms"] and r["route"] == "c11_primary" for r in rows)
        or not any(r["reason"] in {
            "NO_PRIMARY_SIGNAL", "RPD5_NOT_LONG_NONNEGATIVE", "CLOSED5_REJECTED_BREAK",
            "PRIMARY_SIGNAL_ONLY", "NATIVE_INPUT_UNAVAILABLE", "NATIVE_FRAME_END_MISMATCH",
        } for r in rows)):
    msg = ("C11_PACKAGED_ENTRY_VERDICT_RECEIPT_MISSING\n"
           "Expected: unique actual original-strategy close reason in STBL native replay.\n"
           f"Actual: {len(rows)} receipts, {len(keys)} unique; "
           f"expected no-signal offsets {sorted(known_no_signal_offsets)}, "
           f"recorded {sorted(recorded_no_signal_offsets)}. Last successful stage: packaged native callback.\n")
    print(msg, file=sys.stderr)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary).open("a", encoding="utf-8") as handle:
            handle.write(msg)
    raise SystemExit(1)
print("C11_PACKAGED_ENTRY_VERDICT_RECEIPT_PASS", len(rows))
