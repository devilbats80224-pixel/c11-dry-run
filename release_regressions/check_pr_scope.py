"""Never let portable-only CI approve a C11 runtime or packaging change.

All unclassified paths are treated as execution-affecting. The public
workflow reads only verified status summaries, never private replay data.
"""
import json
from pathlib import Path
import runpy
import subprocess
import sys

PORTABLE = frozenset({
    ".github/pull_request_template.md",
    ".github/workflows/c11-ci.yml",
    "README.md",
    "release_regressions/check_pr_scope.py",
    "release_regressions/native_replay_status.py",
    "release_regressions/public_freqtrade_replay.py",
    "release_regressions/required_cases.json",
    "release_regressions/run_required_cases.py",
    "release_regressions/test_one_minute_ci_portable.py",
})


def requires_native_replay(paths):
    return sorted(set(paths) - PORTABLE)


def main():
    # GitHub checks out the synthetic PR merge commit. Diff against its base
    # parent, including deletes and both sides of renames. Do not test the
    # PR head alone or depend on labels authored by the PR.
    subprocess.run(["git", "rev-parse", "--verify", "HEAD^2"],
                   stdout=subprocess.DEVNULL, check=True)
    diff = subprocess.check_output([
        "git", "diff", "--name-only", "--no-renames", "-z", "HEAD^1", "HEAD", "--",
    ])
    blocked = requires_native_replay(
        part.decode("utf-8", "surrogateescape") for part in diff.split(b"\0") if part
    )
    if blocked:
        print("NATIVE_WORKER_REPLAY_REQUIRED: portable CI cannot approve this PR")
        for path in blocked:
            print(ascii(path))
        manifest = json.loads(Path(__file__).with_name("required_cases.json").read_bytes())
        cases = manifest["native"]
        if not cases:
            return 2
        try:
            sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
            verify = runpy.run_path(str(Path(__file__).with_name("native_replay_status.py")))["verify"]
            outstanding = verify(cases, sha)
        except (KeyError, OSError, ValueError):
            print("NATIVE_STATUS_UNAVAILABLE")
            return 2
        for case, state in outstanding.items():
            print(f"NATIVE_RESULT_REQUIRED {case}: {state}")
        if outstanding:
            return 2
        print("ALL_NATIVE_CASES_PASSED_FOR_EXACT_MERGE")
        return 0
    print("PORTABLE_ONLY_PR")
    return 0


if __name__ == "__main__":
    sys.exit(main())
