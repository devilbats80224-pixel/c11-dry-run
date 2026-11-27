"""Read trusted per-case Mac replay statuses for the exact PR merge commit."""
import json
import os
import re
from urllib.request import Request, urlopen

REPO = "devilbats80224-pixel/c11-dry-run"
ISSUER = "devilbats80224-pixel"
PREFIX = "c11/native/v1/"
SHA = re.compile(r"[0-9a-f]{40}")

# Each name is asserted by the hosted image, not inferred from job count:
# warm native exact fallback; build-time 45/9/3 storage rollback controls;
# reviewed-partial and early-roster-return original Worker cases. Stale
# release proof remains due where the criterion excludes listing-origin history.
HOSTED_CASES = frozenset({
    "exact_15m_fallback_preserves_base_history", "historic_minutes_not_saved",
    "partial_checkpoint", "blank_checkpoint",
})


def merge_hosted_evidence(outstanding, merge_sha, hosted_sha, hosted_pass):
    if hosted_pass is not True or not SHA.fullmatch(merge_sha) or merge_sha != hosted_sha:
        return dict(outstanding)
    # Do not erase an explicit negative review or infer any untested case.
    return {name: state for name, state in outstanding.items()
            if name not in HOSTED_CASES or state != "missing"}


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
    outstanding = failures(cases, fetch(sha, os.environ["GH_TOKEN"]))
    return merge_hosted_evidence(
        outstanding, sha, os.environ.get("HOSTED_VERIFIED_MERGE_SHA"),
        os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("HOSTED_WORKER_RESULT") == "success",
    )
