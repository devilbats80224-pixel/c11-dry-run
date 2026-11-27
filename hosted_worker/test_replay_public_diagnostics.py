"""Failure receipts expose actionable statuses, not private fixture contents."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class PublicDiagnosticsTest(unittest.TestCase):
    def test_failed_persistence_counts_and_reports_first_safe_reason(self):
        sensitive = "PRIVATE_FIXTURE_VALUE_DO_NOT_PRINT"
        with tempfile.TemporaryDirectory() as temp:
            log = Path(temp) / "replay.log"
            summary = Path(temp) / "summary.md"
            log.write_text(json.dumps({
                "status": "JCT_REPLAY_HELD",
                "phase": "carry",
                "prefix_statuses": ["SCHEDULED_NATIVE_MINUTES", "OLD_STATE_NOT_BOUND", sensitive],
                "prefix_observations": [
                    {"phase": "cold", "reason": "RPD_OBSERVATION_AFTER_CYCLE_CLOCK", "age_ms": 9000},
                    {"phase": "entry", "reason": sensitive, "age_ms": 8000},
                ],
                "persist_refusals": [
                    {"stage_reason": "DELIVERY_SCOPE_LOST", "symbol": sensitive},
                    {"stage_reason": "NATIVE_BATCH_REFUSED", "response": sensitive},
                    {"stage_reason": "DELIVERY_SCOPE_LOST"},
                    {"stage_reason": sensitive},
                ],
            }), encoding="utf-8")
            result = subprocess.run(
                [sys.executable, "-I", "-B", str(Path(__file__).with_name("verify-replay.py")),
                 "auto-stale", str(log)],
                capture_output=True, text=True, check=False,
                env={**os.environ, "GITHUB_ACTIONS": "true", "GITHUB_STEP_SUMMARY": str(summary)},
            )
            md = summary.read_text() if summary.exists() else ""
        self.assertNotEqual(result.returncode, 0, "a held replay must still fail")
        self.assertIn(
            "C11_HOSTED_PERSIST_DIAGNOSTIC auto-stale first=DELIVERY_SCOPE_LOST "
            "counts=DELIVERY_SCOPE_LOST:2,NATIVE_BATCH_REFUSED:1,UNCLASSIFIED:1",
            result.stdout,
        )
        self.assertIn("C11_HOSTED_PREFIX_DIAGNOSTIC auto-stale phase=cold first=RPD_OBSERVATION_AFTER_CYCLE_CLOCK age_ms=9000",
                      result.stdout)
        self.assertIn("::error title=C11 replay (auto-stale)::Expected original 5m VETO "
                      "in the target cycle; actual JCT_REPLAY_HELD; phase=cold; reason=RPD_OBSERVATION_AFTER_CYCLE_CLOCK",
                      result.stdout)
        self.assertIn("Expected original 5m VETO", md)
        self.assertIn("RPD_OBSERVATION_AFTER_CYCLE_CLOCK", md)
        self.assertNotIn(sensitive, result.stdout + result.stderr + md)

    def test_missing_result_still_has_safe_summary(self):
        with tempfile.TemporaryDirectory() as temp:
            log = Path(temp) / "replay.log"
            log.write_text("secret payload\n", encoding="utf-8")
            summary = Path(temp) / "summary.md"
            result = subprocess.run(
                [sys.executable, "-I", "-B", str(Path(__file__).with_name("verify-replay.py")),
                 "auto-stale", str(log)], capture_output=True, text=True, check=False,
                env={**os.environ, "GITHUB_ACTIONS": "true", "GITHUB_STEP_SUMMARY": str(summary)},
            )
            md = summary.read_text() if summary.exists() else ""
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("REPLAY_RESULT_MISSING_OR_DUPLICATE", md)
        self.assertNotIn("secret payload", result.stdout + result.stderr + md)


if __name__ == "__main__":
    unittest.main()
