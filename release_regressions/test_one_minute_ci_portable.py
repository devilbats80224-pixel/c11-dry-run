"""Synthetic test infrastructure only: no archive, image, network, or ledger."""
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

if __name__ == '__main__':
    unittest.main()
