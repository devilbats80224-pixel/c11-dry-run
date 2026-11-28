"""Check real packaged native strategy diagnostics from the fixture."""
import json
import os
from pathlib import Path
import sys

rows = []
for line in Path(sys.argv[1]).read_text().splitlines():
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
if (not rows or len(keys) != len(rows)
        or not all(r["observed_ms"] >= r["end_ms"] and r["route"] == "c11_primary" for r in rows)
        or not any(r["reason"] in {
            "NO_PRIMARY_SIGNAL", "RPD5_NOT_LONG_NONNEGATIVE", "CLOSED5_REJECTED_BREAK",
            "PRIMARY_SIGNAL_ONLY", "NATIVE_INPUT_UNAVAILABLE", "NATIVE_FRAME_END_MISMATCH",
        } for r in rows)):
    msg = ("C11_PACKAGED_ENTRY_VERDICT_RECEIPT_MISSING\n"
           "Expected: unique actual original-strategy close reason in STBL native replay.\n"
           f"Actual: {len(rows)} receipts, {len(keys)} unique. Last successful stage: packaged native callback.\n")
    print(msg, file=sys.stderr)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary).open("a", encoding="utf-8") as handle:
            handle.write(msg)
    raise SystemExit(1)
print("C11_PACKAGED_ENTRY_VERDICT_RECEIPT_PASS", len(rows))
