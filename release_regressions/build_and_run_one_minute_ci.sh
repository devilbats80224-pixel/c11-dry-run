#!/bin/sh
# Build a pinned disabled candidate; no pulls, activation or live directories.
set -eu
if [ "$#" != 8 ]; then
    printf '%s\n' 'usage: build_and_run_one_minute_ci.sh PINNED_PARENT OWNER_SOURCE WARM_PROOF WARM_FIXTURE JCT_FIXTURE CLOSED_ARCHIVE HAN_FIXTURE OUTPUT' >&2
    exit 64
fi
d=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repo=$(dirname "$d")
parent=$1; owner=$2; proof=$3; warm=$4; jct=$5; archive=$6; han=$7; out=$8
case "$out" in /*) ;; *) echo 'OUTPUT must be absolute' >&2; exit 64;; esac
/usr/bin/git -C "$repo" diff --quiet HEAD -- || { echo 'UNCOMMITTED_C11_INPUT' >&2; exit 65; }
mkdir -m 700 "$out"
/usr/bin/git -C "$repo" rev-parse HEAD >"$out/source.commit"
phase=stage
finish() {
    rc=$1
    trap - 0
    printf 'phase=%s\nexit_code=%s\ncutover_authorized=no\n' "$phase" "$rc" >"$out/RESULT"
    exit "$rc"
}
trap 'finish $?' 0
python3 -I -B "$repo/native_history_scheduler/stage_cold_early_delivery.py" "$parent" "$owner" "$out/build" >"$out/stage.log" 2>&1
phase=build
DOCKER_BUILDKIT=0 docker build --pull=false --network none --iidfile "$out/image.iid" "$out/build" >"$out/build.log" 2>&1
phase=isolated_ci
pin=$(shasum -a 256 "$out/build/release_stage_runtime.json" | awk '{print $1}')
sh "$d/run_one_minute_ci.sh" "$(cat "$out/image.iid")" "$pin" "$proof" "$warm" "$jct" "$archive" "$han" "$out/check"
