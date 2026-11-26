"""Synthetic test infrastructure only: no archive, image, network, or ledger."""
from copy import deepcopy
import hashlib
from pathlib import Path
import runpy
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
            Args=['-B','/opt/c11_owned_modules/release_stage.py'],
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

    def test_wrong_image_or_paper_volume_is_not_same_release(self):
        changed=deepcopy(self.launch)
        changed['Image']='sha256:'+'1'*64
        self.assertEqual(self.verify(self.receipt,changed),'IMAGE_MISMATCH')
        changed['Image']=self.launch['Image']
        changed['Mounts'][0]['Name']='different-fixture'
        self.assertEqual(self.verify(self.receipt,changed),'MOUNT_MISMATCH')

if __name__ == '__main__':
    unittest.main()
