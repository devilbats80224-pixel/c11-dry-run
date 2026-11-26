#!/bin/sh
# Run the repeatable, isolated C11 checks. This does not issue cutover permission.
set -eu
if [ "$#" != 8 ]; then
    printf '%s\n' 'usage: run_one_minute_ci.sh IMAGE RUNTIME_SHA WARM_PROOF WARM_FIXTURE JCT_FIXTURE CLOSED_ARCHIVE HAN_FIXTURE OUTPUT' >&2
    exit 64
fi
d=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repo=$(dirname "$d")
image=$1; pin=$2; proof=$3; warm=$4; jct=$5; archive=$6; han=$7; out=$8
printf '%s\n' "$image" | grep -Eq '^sha256:[0-9a-f]{64}$'
printf '%s\n' "$pin" | grep -Eq '^[0-9a-f]{64}$'
case "$out" in /*) ;; *) echo 'OUTPUT must be absolute' >&2; exit 64;; esac
mkdir -m 700 "$out"
phase=package
finish() {
    rc=$1
    trap - 0
    printf 'image=%s\nruntime_sha=%s\nphase=%s\nexit_code=%s\ncutover_authorized=no\n' \
      "$image" "$pin" "$phase" "$rc" >"$out/RESULT"
    exit "$rc"
}
trap 'finish $?' 0
docker run --rm -i --pull never --network none --read-only --user 1000:2000 \
  --cpus 1 --memory 768m --pids-limit 64 --cap-drop ALL --security-opt=no-new-privileges \
  --entrypoint python "$image" -I -B - "$pin" <"$d/check_one_minute_image.py" >"$out/package.log" 2>&1
phase=replay_input
test -f "$proof/warm_acceptance.py" && test -d "$warm" && test -d "$jct" &&
    test -f "$archive" && test -f "$han/receipt.json" && test -f "$han/jct-after-archive.native.json" ||
    { printf '%s\n' 'MISSING_REQUIRED_REPLAY_INPUT' >"$out/replay-input.log"; exit 66; }
phase=same_image_replay
C11_HAN_FIXTURE="$han" C11_INTEGRATED_COLD_MODE=recent-only \
  sh "$repo/native_history_scheduler/run_integrated_cold_warm_review.sh" \
  "$image" "$pin" "$proof" "$warm" "$jct" "$archive" "$out/replay" >"$out/run.log" 2>&1
phase=outbound_and_original_callback
python3 -I -B "$d/check_one_minute_warm_replay.py" "$out/replay/warm.log" >"$out/outbound.log" 2>&1
# Archived references and partial continuations do not prove the unseeded branch.
# Until actual missing/stale/OPG whole-worker data exists, leave the gate closed.
phase=integration_not_yet_covered
printf '%s\n' 'MISSING_END_TO_END: blank, stale, fresh-data delivery independence, OPG' >"$out/release.log"
exit 2
