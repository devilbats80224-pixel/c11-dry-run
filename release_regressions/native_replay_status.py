"""Read trusted per-case Mac replay statuses for the exact PR merge commit."""
import json
import os
import re
from urllib.request import Request, urlopen

REPO = "devilbats80224-pixel/c11-dry-run"
ISSUER = "devilbats80224-pixel"
PREFIX = "c11/native/v1/"
SHA = re.compile(r"[0-9a-f]{40}")


def failures(cases, entries):
    # GitHub lists commit statuses newest first. Ignore actors other than
    # the trusted local reporting account, not a PR comment or uploaded JSON.
    latest = {}
    for item in entries:
        if (item.get("creator") or {}).get("login") == ISSUER:
            latest.setdefault(item.get("context"), item.get("state"))
    return {case: latest.get(PREFIX + case, "missing") for case in sorted(cases)
            if latest.get(PREFIX + case) != "success"}


def fetch(sha, token):
    if not SHA.fullmatch(sha):
        raise ValueError("EXPECTED_EXACT_MERGE_SHA")
    for page in range(1, 11):
        url = f"https://api.github.com/repos/{REPO}/commits/{sha}/statuses?per_page=100&page={page}"
        request = Request(url, headers={"Authorization": f"Bearer {token}",
                                       "Accept": "application/vnd.github+json",
                                       "User-Agent": "C11-PR-native-gate"})
        with urlopen(request, timeout=15) as response:
            items = json.load(response)
        if not isinstance(items, list):
            raise ValueError("INVALID_STATUS_RESPONSE")
        yield from items
        if len(items) < 100:
            return
    raise ValueError("STATUS_HISTORY_EXCEEDS_LIMIT")


def verify(cases, sha):
    # Never query a PR branch name or the head SHA as a fallback.
    return failures(cases, fetch(sha, os.environ["GH_TOKEN"]))
