"""Reject repo-ignored files even if they were force-added to the Git index."""
import subprocess
from pathlib import Path
import sys


def tracked_ignored(repo):
    # Deliberately use this repository's policy, not a user's local ignore
    # patterns. Print paths only, never the contents of forbidden files.
    data = subprocess.check_output([
        "git", "-C", str(repo), "ls-files", "-c", "-i", "-z", "--exclude-from=.gitignore"
    ])
    return [part.decode("utf-8", "surrogateescape") for part in data.split(b"\0") if part]


if __name__ == "__main__":
    result = tracked_ignored(Path(__file__).resolve().parents[1])
    if result:
        for path in result:
            print("IGNORED_FILE_TRACKED", ascii(path))
        sys.exit(2)
    print("NO_IGNORED_FILES_TRACKED")
