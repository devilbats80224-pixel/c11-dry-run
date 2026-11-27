"""Check the version-record contract with isolated Git histories."""
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("check_version_record.py")
FIELDS = ("改了什麼", "解決了什麼", "尚未解決或未驗證", "保留項目與驗證")


class VersionRecordTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.git("init", "-q")
        self.commit("base")
        self.base = self.git("rev-parse", "HEAD")
        self.record = self.root / "docs/version-records/example.md"
        self.record.parent.mkdir(parents=True)

    def git(self, *args):
        return subprocess.check_output(
            ["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", *args],
            cwd=self.root, text=True,
        ).strip()

    def commit(self, name):
        self.git("add", ".")
        self.git("commit", "-qm", name, "--allow-empty")

    def check(self, content=None):
        if content is not None:
            self.record.write_text(content, encoding="utf-8")
        self.commit("change")
        return subprocess.run(
            [sys.executable, "-I", "-B", str(SCRIPT), self.base, "HEAD"],
            cwd=self.root, text=True, capture_output=True, check=False,
        )

    def valid(self, base=None):
        return f"# Test\n\n## 基於哪一版\n{base or self.base}\n\n" + "\n\n".join(
            f"## {label}\n具體說明本次改動的行為、測試或限制。" for label in FIELDS
        )

    def test_missing_record(self):
        result = self.check()
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("VERSION_RECORD_MISSING", result.stdout)

    def test_only_title(self):
        result = self.check("# Test\n")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("VERSION_RECORD_SECTION_MISSING", result.stdout)

    def test_wrong_base(self):
        result = self.check(self.valid("a" * 40))
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("VERSION_RECORD_BASE_MISMATCH", result.stdout)

    def test_placeholders(self):
        result = self.check(self.valid().replace("具體說明本次改動的行為、測試或限制。", "待補"))
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("VERSION_RECORD_SECTION_EMPTY", result.stdout)

    def test_traceable_record(self):
        result = self.check(self.valid())
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_record_cannot_be_reused_from_base(self):
        self.record.write_text(self.valid(), encoding="utf-8")
        self.commit("previous record")
        self.base = self.git("rev-parse", "HEAD")
        result = self.check(self.valid())
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("VERSION_RECORD_MISSING", result.stdout)


if __name__ == "__main__":
    unittest.main()
