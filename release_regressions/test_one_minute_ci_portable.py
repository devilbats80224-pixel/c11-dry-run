"""Synthetic test infrastructure only: no archive, image, network, or ledger."""
from copy import deepcopy
import hashlib
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
image = runpy.run_path(str(HERE / 'check_one_minute_image.py'))
wire = runpy.run_path(str(HERE / 'check_one_minute_warm_replay.py'))

class ImageChecks(unittest.TestCase):
    def test_absence_and_manifest_mismatch_do_not_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            example = root / 'candidate.py'
            content = b'pass\n'
            example.write_bytes(content)
            manifest = {str(example): hashlib.sha256(content).hexdigest()}
            audit = image['audit']
            self.assertEqual(audit(root,manifest,('candidate','missing')),
                             {'candidate':'MATCH','missing':'MISSING_FROM_IMAGE'})
            example.write_bytes(b'changed\n')
            self.assertEqual(audit(root,manifest,('candidate',)),
                             {'candidate':'MISSING_OR_WRONG_MANIFEST_DIGEST'})

class WireChecks(unittest.TestCase):
    def test_original_callback_and_exact_refill_required(self):
        good = wire['MARKER'] + '[["STBLUSDT","1m",45]]'
        self.assertTrue(wire['check'](good)[0])
        for report in ('',good+'\n'+good,good.replace('45','30')):
            self.assertFalse(wire['check'](report)[0])

    def test_any_direct_wider_request_fails(self):
        for timeframe in ('5m','15m'):
            report = wire['MARKER'] + f'[["STBLUSDT","1m",45],["ONUSDT","{timeframe}",499]]'
            ok, reason = wire['check'](report)
            self.assertFalse(ok)
            self.assertIn('OUTBOUND_NON_1M',reason)

    def test_bad_receipt_is_failure(self):
        for request in ('{"wrong":"shape"}','[["STBLUSDT","1m",true]]','[["STBLUSDT","1m",-45]]'):
            self.assertEqual(wire['check'](wire['MARKER']+request),(False,'INVALID_REQUEST_RECEIPT'))

class LaunchChecks(unittest.TestCase):
    def setUp(self):
        self.verify = runpy.run_path(str(HERE / 'check_effective_launch.py'))['verify']
        self.launch = dict(Image='sha256:'+'0'*64,Path='python',
            Args=['-B','/opt/c11_owned_modules/release_stage.py'], StartupEnvironment='pinned',
            Mounts=[dict(Type='volume',Name='fixture-paper',Destination='/c11-paper',RW=True)])
        self.receipt = dict(remote_merge='1'*40,local_worker_source='2'*40,tested_launch=self.launch)

    def test_same_image_different_entry_is_rejected(self):
        changed=deepcopy(self.launch)
        self.assertEqual(self.verify(self.receipt,changed),'MATCH')
        changed['Args']=['-B','/opt/c11_worker/old_entry.py']
        self.assertEqual(self.verify(self.receipt,changed),'ARGUMENTS_MISMATCH')

    def test_same_image_mount_cannot_cover_tested_code(self):
        for destination in ('/opt','/opt/c11_owned_modules/release_stage.py','/freqtrade'):
            changed=deepcopy(self.launch)
            changed['Mounts'].append(dict(Type='bind',Source='/fixture',Destination=destination,RW=False))
            self.assertEqual(self.verify(self.receipt,changed),'MOUNT_SHADOWS_TESTED_CODE')
        for destination in ('/opt/c11','/opt/c11/transport'):
            changed=deepcopy(self.launch)
            changed['Mounts'].append(dict(Type='bind',Source='/fixture',Destination=destination,RW=False))
            # A reviewed and actual launch matching each other is insufficient
            # if both hide code from the supposedly immutable artifact.
            receipt={**self.receipt,'tested_launch':changed}
            with self.subTest(destination=destination):
                self.assertEqual(self.verify(receipt,changed),'MOUNT_SHADOWS_TESTED_CODE')

    def test_effective_startup_environment_must_be_reviewed(self):
        changed=deepcopy(self.launch)
        changed.pop('StartupEnvironment')
        self.assertEqual(self.verify(self.receipt,changed),'STARTUP_ENVIRONMENT_NOT_ATTESTED')
        changed['StartupEnvironment']='rejected'
        self.assertEqual(self.verify(self.receipt,changed),'STARTUP_ENVIRONMENT_REJECTED')
        self.assertEqual(self.verify({**self.receipt,'tested_launch':changed},changed),
                         'STARTUP_ENVIRONMENT_REJECTED')
        changed['StartupEnvironment']='absent'
        self.assertEqual(self.verify(self.receipt,changed),'STARTUP_ENVIRONMENT_MISMATCH')

    def test_startup_environment_capture_uses_no_secret_values(self):
        capture = runpy.run_path(str(HERE / 'check_effective_launch.py'))['startup_environment']
        valid='/freqtrade:/opt/c11_worker:/opt/c11_worker/ports:/opt/c11_worker/sizing:/opt/c11_worker/strategy:/opt/c11:/opt/c11/transport'
        self.assertEqual(capture(['ORDINARY=example-sensitive-value','PYTHONPATH='+valid]),'pinned')
        self.assertEqual(capture(['ORDINARY=example-sensitive-value']),'absent')
        for entries in (['PYTHONPATH=/opt/unreviewed'],['C11_HISTORY_EXTRA=example-sensitive-value'],
                        ['FREQTRADE__MOCK=example-sensitive-value'],['PYTHONHOME=/opt/unreviewed'],
                        ['PYTHONPATH='+valid,'PYTHONPATH='+valid]):
            self.assertEqual(capture(entries),'rejected')

    def test_wrong_image_or_paper_volume_is_not_same_release(self):
        changed=deepcopy(self.launch)
        changed['Image']='sha256:'+'1'*64
        self.assertEqual(self.verify(self.receipt,changed),'IMAGE_MISMATCH')
        changed['Image']=self.launch['Image']
        changed['Mounts'][0]['Name']='different-fixture'
        self.assertEqual(self.verify(self.receipt,changed),'MOUNT_MISMATCH')

class MergeGateChecks(unittest.TestCase):
    def test_docs_and_portable_suite_do_not_require_private_worker(self):
        check = runpy.run_path(str(HERE / 'check_pr_scope.py'))['requires_native_replay']
        self.assertEqual(check(['README.md', 'release_regressions/test_one_minute_ci_portable.py']), [])

    def test_runtime_packaging_and_unknown_paths_fail_closed(self):
        check = runpy.run_path(str(HERE / 'check_pr_scope.py'))['requires_native_replay']
        runtime = ['native_history_scheduler/native_primary_lane.py',
                   'release_regressions/build_and_run_one_minute_ci.sh',
                   'release_regressions/check_one_minute_warm_replay.py',
                   'compose.yml', 'new-worker-entry']
        self.assertEqual(check(['README.md'] + runtime), sorted(runtime))

    def test_real_pr_merge_diff_does_not_hide_deleted_runtime_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            def git(*args):
                subprocess.run(['git', *args], cwd=tmp, check=True, capture_output=True)
            git('init', '-b', 'base')
            git('config', 'user.email', 'ci@example.invalid')
            git('config', 'user.name', 'CI')
            worker = Path(tmp) / 'worker.py'
            worker.write_text('pass')
            git('add', 'worker.py')
            git('commit', '-m', 'base')
            git('switch', '-c', 'proposed')
            git('mv', 'worker.py', 'README.md')
            git('commit', '-m', 'runtime replaced with a doc')
            git('switch', 'base')
            git('merge', '--no-ff', '-m', 'merge test', 'proposed')
            result = subprocess.run(
                ['python3', '-I', '-B', str(HERE / 'check_pr_scope.py')], cwd=tmp,
                capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
            self.assertIn('worker.py', result.stdout)


class RegistryChecks(unittest.TestCase):
    def test_missing_or_skipped_test_fails(self):
        verify = runpy.run_path(str(HERE / 'run_required_cases.py'))['unverified']
        cases = {'passed': 'Existing.test_passed', 'skipped': 'Existing.test_skipped'}
        self.assertEqual(verify(cases, {'test_one_minute_ci_portable.Existing.test_passed'}), ['skipped'])

    def test_baseline_cases_cannot_disappear_or_change(self):
        verify = runpy.run_path(str(HERE / 'run_required_cases.py'))['changed_cases']
        baseline = {'portable': {'bad_wire': 'Wire.test_five_minute'},
                    'native': {'cold': 'Actual cold replay'}}
        proposed = {'portable': {}, 'native': {'cold': 'Test different behavior'}}
        self.assertEqual(verify(baseline, proposed), ['portable:bad_wire', 'native:cold'])


class NativeStatusChecks(unittest.TestCase):
    def test_mac_results_must_be_complete_current_and_trusted(self):
        status = runpy.run_path(str(HERE / 'native_replay_status.py'))
        def entry(case, state, actor=status['ISSUER']):
            return {'context': status['PREFIX'] + case, 'state': state,
                    'creator': {'login': actor}}
        cases = ['no_direct_15m', 'blank_state']
        rows = [entry('no_direct_15m', 'failure'), entry('no_direct_15m', 'success'),
                entry('blank_state', 'success', 'other-account')]
        self.assertEqual(status['failures'](cases, rows),
                         {'blank_state': 'missing', 'no_direct_15m': 'failure'})
        self.assertEqual(status['failures'](cases, [
            entry('blank_state', 'success'), entry('no_direct_15m', 'success')]), {})

    def test_hosted_evidence_is_exact_and_does_not_clear_unproven_or_failed_cases(self):
        e = runpy.run_path(str(HERE / 'native_replay_status.py'))
        merge = 'a' * 40
        outstanding = {'blank_checkpoint': 'missing', 'stale_checkpoint': 'failure',
                       'direct_wide_data_fetch': 'missing', 'exact_package_and_entry': 'missing'}
        combine = e['merge_hosted_evidence']
        expected = dict(outstanding)
        for case in ('blank_checkpoint','direct_wide_data_fetch','exact_package_and_entry'):
            del expected[case]
        self.assertEqual(combine(outstanding, merge, merge, True), expected)
        self.assertEqual(combine(outstanding, merge, 'b' * 40, True), outstanding)
        self.assertEqual(combine(outstanding, merge, merge, False), outstanding)
        self.assertEqual(combine({'stale_checkpoint': 'missing'}, merge, merge, True), {})
        # An external green status does not substitute for the PR build.
        from unittest.mock import patch
        with patch.dict('os.environ', {'GITHUB_ACTIONS': 'true', 'HOSTED_WORKER_RESULT': 'failure',
                                      'HOSTED_VERIFIED_MERGE_SHA': merge}, clear=True):
            self.assertEqual(e['verify'](['stale_checkpoint'], merge), {'stale_checkpoint': 'missing'})

    def test_native_gate_diagnostics_keep_missing_proof_distinct_from_failure(self):
        scope = runpy.run_path(str(HERE / 'check_pr_scope.py'))
        summary = scope['native_failure_summary'](
            {'stale_checkpoint': 'missing', 'direct_wide_data_fetch': 'failure',
             'reviewed_history_preserved': 'unverified RAW_SECRET_SAMPLE'},
            {'stale_checkpoint': 'Use recent history, not listing day.',
             'direct_wide_data_fetch': 'Route a full 1m refill.',
             'reviewed_history_preserved': 'Preserve reviewed corrections.'},
        )
        self.assertIn('stale_checkpoint', summary)
        self.assertIn('Use recent history, not listing day.', summary)
        self.assertIn('No exact-commit proof was registered', summary)
        self.assertIn('Recorded replay failure', summary)
        self.assertNotIn('RAW_SECRET_SAMPLE', summary)

    def test_invalid_exact_merge_identity_never_queries_github(self):
        status = runpy.run_path(str(HERE / 'native_replay_status.py'))
        with self.assertRaisesRegex(ValueError, 'EXPECTED_EXACT_MERGE_SHA'):
            list(status['fetch']('refs/heads/main', 'unused'))


class PublicTrackedFilesChecks(unittest.TestCase):
    def test_private_types_are_ignored_and_force_add_fails(self):
        check = runpy.run_path(str(HERE / 'check_public_tracked_files.py'))['tracked_ignored']
        with tempfile.TemporaryDirectory() as folder:
            repo = Path(folder)
            def git(*args):
                return subprocess.run(['git', *args], cwd=repo, check=True, capture_output=True)
            git('init')
            (repo / '.gitignore').write_bytes((HERE.parent / '.gitignore').read_bytes())
            blocked = ['.env.local', 'scratch.sqlite-wal', 'WIRE_PRIVATE.json', 'receipt.log']
            for name in [*blocked, 'synthetic-case.json']:
                (repo / name).touch()
            git('add', '.')
            self.assertEqual(check(repo), [])
            self.assertIn(b'synthetic-case.json', git('ls-files', '-z').stdout)
            git('add', '-f', '--', *blocked)
            self.assertEqual(sorted(check(repo)), sorted(blocked))


if __name__ == '__main__':
    unittest.main()


class StoppedPaperBindingChecks(unittest.TestCase):
    def helper(self):
        probe=HERE/'stopped_paper_binding.py'
        self.assertTrue(probe.is_file(),'C11_STOPPED_BINDING_RETRY_NOT_REGISTERED')
        return runpy.run_path(str(probe))['confirm']

    def test_transient_mismatch_can_only_retry_the_same_exact_proof(self):
        events=[]
        observations=iter((False,True))
        def exact():
            events.append('exact')
            return next(observations)
        self.assertTrue(self.helper()(lambda:True,exact,lambda:events.append('settle')))
        self.assertEqual(events,['exact','settle','exact'])

    def test_persistent_mismatch_or_active_writer_never_passes(self):
        attempts=[]
        def mismatch():
            attempts.append(1)
            return False
        confirm=self.helper()
        self.assertFalse(confirm(lambda:True,mismatch,lambda:None))
        self.assertEqual(len(attempts),3)
        attempts.clear()
        self.assertFalse(confirm(lambda:False,mismatch,lambda:None))
        self.assertEqual(attempts,[])
        states=iter((True,False))
        self.assertFalse(confirm(lambda:next(states),lambda:True,lambda:None))


class ObserverLeaseChecks(unittest.TestCase):
    def test_refreshed_same_generation_proof_is_current_not_backdated(self):
        script = HERE.parent / "hosted_worker" / "test_observer_classifier_lease.py"
        result = subprocess.run(
            [sys.executable, "-I", "-B", str(script)], capture_output=True, text=True, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
        self.assertIn("C11_OBSERVER_CLASSIFIER_LEASE_PASS", result.stdout)
