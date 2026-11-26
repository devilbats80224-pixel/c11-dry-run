"""Run and independently account for every registered public regression."""
import json
import subprocess
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
MANIFEST = "release_regressions/required_cases.json"
SUITE = "test_one_minute_ci_portable"
RUNNER = "release_regressions/run_required_cases.py"


def registry(raw):
    data = json.loads(raw)
    if not isinstance(data, dict) or set(data) != {"portable", "native"}:
        raise ValueError("INVALID_REGRESSION_REGISTRY")
    for cases in data.values():
        if not isinstance(cases, dict) or not cases or not all(
            isinstance(v, str) and v.strip() for v in cases.values()
        ):
            raise ValueError("EMPTY_OR_INVALID_REGRESSION_SECTION")
    if len(set(data["portable"].values())) != len(data["portable"]):
        raise ValueError("PORTABLE_CASES_MUST_MAP_TO_DISTINCT_TESTS")
    return data


def changed_cases(baseline, proposed):
    return [
        f"{lane}:{case}"
        for lane, cases in baseline.items()
        for case, value in cases.items()
        if proposed[lane].get(case) != value
    ]


def unverified(cases, passed):
    return [case for case, method in cases.items() if f"{SUITE}.{method}" not in passed]


class CountedResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.passed = set()

    def addSuccess(self, test):
        super().addSuccess(test)
        self.passed.add(test.id())


def verify_pr_baseline(proposed):
    anchor = ["git", "-C", str(HERE.parent)]
    installed = subprocess.run(
        anchor + ["cat-file", "-e", f"HEAD^1:{RUNNER}"],
        stderr=subprocess.DEVNULL,
    )
    if installed.returncode != 0:  # Initial installation only.
        subprocess.run(anchor + ["rev-parse", "--verify", "HEAD^1^{commit}"],
                       check=True, stdout=subprocess.DEVNULL)
        return
    baseline = registry(subprocess.check_output(anchor + ["show", f"HEAD^1:{MANIFEST}"]))
    changed = changed_cases(baseline, proposed)
    if changed:
        raise ValueError("REGRESSION_CASE_REMOVED_OR_CHANGED: " + ", ".join(changed))


def main():
    if sys.argv[1:] not in ([], ["--pr"]):
        raise ValueError("usage: run_required_cases.py [--pr]")
    data = registry((HERE / "required_cases.json").read_bytes())
    if sys.argv[1:]:
        verify_pr_baseline(data)
    suite = unittest.defaultTestLoader.discover(str(HERE), pattern=f"{SUITE}.py")
    result = unittest.TextTestRunner(verbosity=2, resultclass=CountedResult).run(suite)
    missing = unverified(data["portable"], result.passed)
    if missing:
        print("REGRESSION_RESULT_MISSING: " + ", ".join(missing), file=sys.stderr)
    for case in sorted(set(data["portable"]) - set(missing)):
        print(f"REGRESSION_PASS {case}")
    if not result.wasSuccessful() or missing:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
