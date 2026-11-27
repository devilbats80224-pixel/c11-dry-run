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


    def test_no_listing_case_is_identified_and_can_pass_without_archive(self):
        with tempfile.TemporaryDirectory() as temp:
            log = Path(temp) / "replay.log"
            summary = Path(temp) / "summary.md"
            e=1795773600000
            blocked=dict(status="JCT_REPLAY_HELD",phase="cold",archive_public_receipts=0,
                         retained_stages=dict(PERSISTED=8),
                         prefix_observations=[dict(phase="cold",reason="AUTO_RECENT_MINUTES_ONLY_ORIGINAL_STATE_UNVERIFIED",age_ms=5500)])
            def run(receipt, listing=True):
                log.write_text("C11_CI_ABSENT_CHECKPOINT_NO_LISTING_SETUP\n"+json.dumps(receipt),encoding="utf-8")
                return subprocess.run(
                    [sys.executable, "-I", "-B", str(Path(__file__).with_name("verify-replay.py")),
                     "early-roster-return", str(log), "no", "no", "no", "yes" if listing else "no"],
                    capture_output=True,text=True,check=False,
                    env={**os.environ,"GITHUB_ACTIONS":"true","GITHUB_STEP_SUMMARY":str(summary)})
            failed=run(blocked)
            md=summary.read_text()
            self.assertNotEqual(failed.returncode,0)
            self.assertIn("unseeded-no-listing",md)
            self.assertIn("historical archives disabled by this fixture",md)
            self.assertIn("confirmed 1m pages stored: 8",md)
            valid=dict(status="JCT_ORIGINAL_5M_VETO",zero_orders=True,archive_public_receipts=0,
                       replay_boundary=e,shared_1m_requests=[[e-900000,e,15,e+1]],
                       discovery_configured=False,automatic_archive=True,automatic_stale_repair=False,
                       reviewed_partial_repair=False,roster_drop_return_verified=True,
                       initial_cold_start_age_ms=5000,observed_replay_age_ms=6000,strategy_native_veto=True,
                       original_entry_advice=[dict(status="ORIGINAL_NATIVE_CALLBACK",last_enter_long=0,age_ms=6000)],
                       decision_sources=[dict(status="VETO",owner_code="causal_bars.py",method="inputs",
                                              boundary=e,inside_original_target_callback=True,age_ms=6000)])
            passed=run(valid)
            self.assertEqual(passed.returncode,0,passed.stderr)
            self.assertNotEqual(run(valid,False).returncode,0)
            self.assertNotEqual(run({**valid,"original_entry_advice":[]}).returncode,0)
            self.assertNotEqual(run({**valid,"archive_public_receipts":1}).returncode,0)
            def run_pinned(receipt):
                log.write_text("C11_REVIEWED_REMOTE_ONLY_NO_LISTING_DISCOVERY\n"+json.dumps(receipt),encoding="utf-8")
                return subprocess.run(
                    [sys.executable,"-I","-B",str(Path(__file__).with_name("verify-replay.py")),
                     "early-roster-return",str(log),"no","yes","no","no","no","yes"],
                    capture_output=True,text=True,check=False)
            pinned={**valid,"pinned_remote":True,"archive_public_receipts":74}
            passed=run_pinned(pinned)
            self.assertEqual(passed.returncode,0,passed.stderr)
            for invalid in ({"archive_public_receipts":0},{"discovery_configured":True},{"original_entry_advice":[]}):
                with self.subTest(invalid=invalid):
                    self.assertNotEqual(run_pinned({**pinned,**invalid}).returncode,0)

    def test_optional_veto_must_be_from_exact_close_and_original_producer(self):
        with tempfile.TemporaryDirectory() as temp:
            log = Path(temp) / "replay.log"
            tagged = dict(status="VETO", method="record_verified_veto", owner_code="native_primary_lane.py",
                          age_ms=4250, boundary=1795773600000)
            receipt = dict(status="JCT_ORIGINAL_5M_VETO", zero_orders=True,
                           verified_raw_archive_cache=True, pinned_remote=False, archive_public_receipts=0,
                           automatic_archive=True, automatic_stale_repair=False, reviewed_partial_repair=False,
                           roster_drop_return_verified=True, initial_cold_start_age_ms=5000,
                           observed_replay_age_ms=54750, replay_boundary=1795773600000)
            def run(veto):
                log.write_text(json.dumps({**receipt, "decision_sources": [veto]}), encoding="utf-8")
                return subprocess.run(
                    [sys.executable, "-I", "-B", str(Path(__file__).with_name("verify-replay.py")),
                     "early-roster-return", str(log), "yes", "no"],
                    capture_output=True, text=True, check=False,
                )
            self.assertEqual(run(tagged).returncode, 0)
            for wrong in ({"boundary": 1795772700000}, {"age_ms": 60000}, {"owner_code": "synthetic.py"}):
                with self.subTest(wrong=wrong):
                    result = run({**tagged, **wrong})
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("ORIGINAL_STRATEGY_VERDICT_NOT_PROVEN", result.stderr)


if __name__ == "__main__":
    unittest.main()
