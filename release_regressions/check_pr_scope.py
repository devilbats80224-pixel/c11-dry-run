"""Never let portable-only CI approve a C11 runtime or packaging change.

All unclassified paths are treated as execution-affecting. The public
workflow intentionally has no access to private native replay data or to a
trusted replay verifier yet; until it does, those changes fail closed.
"""
import subprocess
import sys

PORTABLE = frozenset({
    ".github/pull_request_template.md",
    ".github/workflows/c11-ci.yml",
    "README.md",
    "release_regressions/check_pr_scope.py",
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
        return 2
    print("PORTABLE_ONLY_PR")
    return 0


if __name__ == "__main__":
    sys.exit(main())
