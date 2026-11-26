#!/bin/sh
# Local delivery entry. Caller fetches the verified remote main into a clean,
# isolated checkout of that exact SHA. Cannot activate C11 or write its ledger.
set -eu
if [ "$#" -ne 9 ]; then
    printf '%s\n' 'usage: run_merged_main_ci.sh MERGED_SHA PINNED_PARENT OWNER_SOURCE WARM_PROOF WARM_FIXTURE JCT_FIXTURE CLOSED_ARCHIVE HAN_FIXTURE OUTPUT' >&2
    exit 64
fi
d=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repo=$(dirname "$d")
sha=$1
shift
printf '%s\n' "$sha" | grep -Eq '^[0-9a-f]{40}$'
remote=$(/usr/bin/git -C "$repo" config --get remote.origin.url || true)
case "$remote" in
    https://github.com/devilbats80224-pixel/c11-dry-run|https://github.com/devilbats80224-pixel/c11-dry-run.git) ;;
    *) echo 'C11_EXPECTED_GITHUB_ORIGIN_NOT_CONFIGURED' >&2; exit 65 ;;
esac
head=$(/usr/bin/git -C "$repo" rev-parse --verify 'HEAD^{commit}')
main=$(/usr/bin/git -C "$repo" rev-parse --verify 'refs/remotes/origin/main^{commit}' || true)
if [ "$head" != "$sha" ] || [ "$main" != "$sha" ]; then
    echo 'C11_MERGED_SHA_MUST_MATCH_ORIGIN_MAIN_AND_ISOLATED_CHECKOUT' >&2
    exit 65
fi
if [ -n "$(/usr/bin/git -C "$repo" status --porcelain --untracked-files=all)" ]; then
    echo 'C11_DELIVERY_REQUIRES_CLEAN_CHECKOUT' >&2
    exit 65
fi
# Run all build inputs separately from the exact live volume. No Docker
# service, source fetch, pushes, credentials or deployment are done here.
exec sh "$d/build_and_run_one_minute_ci.sh" "$@"
