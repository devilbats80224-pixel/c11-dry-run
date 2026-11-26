#!/bin/sh
# Fixed integrated replay: one immutable image, existing warm/cold tapes,
# isolated tmpfs, no network, no sleeping for a live market boundary.
set -eu
[ "$#" = 7 ] || { echo "usage: $0 IMAGE RUNTIME_SHA WARM_PROOF WARM_FIXTURE JCT_FIXTURE CLOSED_ARCHIVE OUTPUT"; exit 64; }
i=$1; h=$2; proof=$3; warm=$4; jct=$5; archive=$6; out=$7
printf '%s\n' "$i" | grep -Eq '^sha256:[a-f0-9]{64}$'
printf '%s\n' "$h" | grep -Eq '^[a-f0-9]{64}$'
d=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
mode=${C11_INTEGRATED_COLD_MODE:-recent-only}
case "$mode" in recent-only|roster-return|early-roster-return) ;; *) exit 64;; esac
check_recent=no
if [ "$mode" = recent-only ]; then check_recent=yes; fi
mkdir -m 700 "$out"
# Old-state reproduction is a required first step, not an optional check.
docker run --rm --pull never --network none --read-only --user 1000:2000 \
  --cpus 1 --memory 768m --pids-limit 64 --cap-drop ALL --security-opt=no-new-privileges \
  --mount "type=bind,src=$d,dst=/test,readonly" \
  -e C11_EXPECT_RECENT_ONLY="$check_recent" \
  --entrypoint python "$i" -B /test/replay_band_stale_with_controls.py >"$out/ancient-state.log" 2>&1
docker run --rm --pull never --network none --read-only --user 1000:2000 \
  --cpus 1 --memory 768m --pids-limit 64 --cap-drop ALL --security-opt=no-new-privileges \
  --mount "type=bind,src=$d/test_cold_ready_priority.py,dst=/priority.py,readonly" \
  -e C11_READY_PRIORITY_SOURCE=/opt/c11_owned_modules/auto_cold_archive.py \
  --entrypoint python "$i" -I -B /priority.py >"$out/priority.log" 2>&1
docker run --rm --pull never --network none --read-only --user 1000:2000 \
  --cpus 1 --memory 768m --pids-limit 64 --cap-drop ALL --security-opt=no-new-privileges \
  --tmpfs /tmp:rw,nosuid,nodev,mode=1777 \
  --tmpfs /c11-paper:rw,nosuid,nodev,uid=1000,gid=2000,mode=0700 \
  --mount "type=bind,src=$warm,dst=/fixture,readonly" \
  --mount "type=bind,src=$proof,dst=/proof,readonly" \
  -e OPENBLAS_NUM_THREADS=1 -e OMP_NUM_THREADS=1 \
  -e C11_EXACT_FINAL_RUNTIME_SHA="$h" -e C11_OFFLINE_EXPECT_BAKED_SCOPE=no \
  -e C11_FUNCTIONAL_TRIAL=yes -e C11_INTENT_CASE=flat \
  --entrypoint python "$i" -B /proof/warm_acceptance.py >"$out/warm.log" 2>&1
grep -F PACKAGED_WARM_ROUTE_ORIGINAL_CALLBACK "$out/warm.log"
if [ "$mode" = recent-only ]; then
    docker run --rm --pull never --network none --read-only --user 1000:2000 \
      --cpus 1 --memory 768m --pids-limit 64 --cap-drop ALL --security-opt=no-new-privileges \
      --mount "type=bind,src=$d,dst=/test,readonly" \
      --entrypoint python "$i" -B /test/replay_band_recent_refill.py >"$out/recent-refill-wire.log" 2>&1
    docker run --rm --pull never --network none --read-only --user 1000:2000 \
      --cpus 1 --memory 768m --pids-limit 64 --cap-drop ALL --security-opt=no-new-privileges \
      --mount "type=bind,src=$d,dst=/test,readonly" \
      --mount "type=bind,src=$jct,dst=/archived,readonly" \
      --entrypoint python "$i" -B /test/replay_recent_cold_ab.py >"$out/recent-state-shadow.log" 2>&1
    C11_TEST_DIR="$d" C11_TEST_OUTPUT="$out" C11_TEST_IMAGE="$i" \
      C11_HAN_FIXTURE="${C11_HAN_FIXTURE:-}" sh "$d/run_prior_invariants.sh"
    # The earlier exact archived reference is not an automatic online listing fallback.
    sh "$d/run_jct_worker_review.sh" "$i" "$jct" "$archive" "$out/exact-local-reference" no no >"$out/local-reference.log" 2>&1
    python3 - "$out/local-reference.log" <<'PY2'
import json,sys
from pathlib import Path
j=json.loads(Path(sys.argv[1]).read_text().splitlines()[-1])
assert j['status']=='JCT_ORIGINAL_5M_VETO' and not j['automatic_archive']
assert j['zero_orders'] and not j['real_decision_time_claim']
print(json.dumps(j))
PY2
    exit 0
fi
sh "$d/run_jct_worker_review.sh" "$i" "$jct" "$archive" "$out/cold-start" no early-start >"$out/cold-start.log" 2>&1
grep -F C11_COLD_ASYNC_STARTED "$out/cold-start.log"
sh "$d/run_jct_worker_review.sh" "$i" "$jct" "$archive" "$out/cold-review" no "$mode" >"$out/cold.log" 2>&1
python3 - "$out/cold.log" "$mode" <<'PY'
import json,sys
from pathlib import Path
j=json.loads(Path(sys.argv[1]).read_text().splitlines()[-1])
assert j['status']=='JCT_ORIGINAL_5M_VETO'
assert j['automatic_archive'] and j['roster_drop_return_verified'] and j['zero_orders']
assert not j['real_decision_time_claim']
if sys.argv[2]=='early-roster-return': assert 0<=j['initial_cold_start_age_ms']<60_000
assert 0<=j['observed_replay_age_ms']<60_000
assert any(r['status']=='VETO' and r['method']=='record_verified_veto' for r in j['decision_sources'])
print(json.dumps(j))
PY
# Full observed roster and cutover acceptance remain a separate mandatory gate.
