#!/bin/sh
set -eu
[ "$#" = 3 ] || exit 64
case "$2" in build|package-entry|warm|warm-missing-analysis|warm-unicode|warm-ready-basis|warm-terminal-gap|warm-divergent-fallback|warm-mixed-history-pressure|early-roster-return|auto-stale|reviewed-partial|reviewed-opg|cached-blank|cached-stale|pinned-remote-blank|pinned-remote-stale|pinned-remote-no-listing|prepared-blank|prepared-stale|prepared-return|unseeded-on-demand|archive-source-outage|background-blank) ;; *) exit 64;; esac
out=$1
artifact=$3
base=sha256:1bb9a6b11a4dae39444331aa324b139dcd635f6a1c586ce9ecccac17994edc0c
fixture_sha=566dcb2865792e8f7ffdab8e0b223fb273ba303ecd8c3710fd10c7e66fac9b6a
fixture_url=https://github.com/devilbats80224-pixel/c11-dry-run/releases/download/c11-ci-replay-fixtures-v1/c11-ci-paper-replay-v1.tar.gz
d=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
mkdir -m 700 "$out"
curl -fsSL --retry 2 --max-time 180 -o "$out/input.tgz" "$fixture_url"
printf '%s  %s\n' "$fixture_sha" "$out/input.tgz" | sha256sum -c -
mkdir -m 700 "$out/fixture"
tar -xzf "$out/input.tgz" --directory "$out/fixture" --no-same-owner --no-same-permissions
# Only make the reviewed fixture mounts readable to UID 1000, not the run log folder.
find "$out/fixture" -type d -exec chmod 0755 '{}' +
find "$out/fixture" -type f -exec chmod 0444 '{}' +
warm_sha=078ac05c01d6fd2b1aed5910adf21e7f99ed9c13f86c626edf61adbec5341f9c
warm_url=https://github.com/devilbats80224-pixel/c11-dry-run/releases/download/c11-ci-replay-fixtures-v1/c11-ci-warm-replay-v1.tar.gz
curl -fsSL --retry 2 --max-time 180 -o "$out/warm-input.tgz" "$warm_url"
printf '%s  %s\n' "$warm_sha" "$out/warm-input.tgz" | sha256sum -c -
mkdir -m 755 "$out/warm"
tar -xzf "$out/warm-input.tgz" --directory "$out/warm" --no-same-owner --no-same-permissions
find "$out/warm" -type d -exec chmod 0755 '{}' +
find "$out/warm" -type f -exec chmod 0444 '{}' +
cp "$d/warm_acceptance.py" "$out/warm/warm_acceptance.py"
chmod 0444 "$out/warm/warm_acceptance.py"
if [ "$2" = build ]; then
    mkdir -p "$artifact"
    docker pull --platform linux/arm64 "docker.io/freqtradeorg/freqtrade@$base"
    python3 -I -B "$d/stage.py" "$d/source" "$out/prepare-input" "$base" "$out/warm/baked_catalog" "$out/fixture/jct"
    catalog_pin=$(sha256sum "$out/prepare-input/opt/c11-reviewed-cold-prefix/catalog.json" | awk '{print $1}')
    chmod 0755 "$out/prepare-input"
    docker run --rm --pull never --network none --read-only --user 1000:2000 --cap-drop ALL \
      --security-opt=no-new-privileges -e OPENBLAS_NUM_THREADS=1 -e OMP_NUM_THREADS=1 \
      --mount "type=bind,src=$out/prepare-input/opt,dst=/raw-build,readonly" \
      --mount "type=bind,src=$d/build_prepared_checkpoint.py,dst=/prepare.py,readonly" \
      --entrypoint python "docker.io/freqtradeorg/freqtrade@$base" -B /prepare.py "$catalog_pin" >"$out/prepared-prefix.json"
    # Package only the exact independently reviewed reference, never the
    # host's potentially CPU-dependent rederived floating-point bytes.
    prepared_pin=002fd0a569094afef11b4ae6b14722e616598aaff20b4ed8734b5934f10cb820
    reference="$d/fixtures/jct-reviewed-prefix.json"
    printf '%s  %s\n' "$prepared_pin" "$reference" | sha256sum -c -
    python3 -B "$d/test_prepared_reproduction.py"
    python3 -I -B "$d/verify_prepared_reproduction.py" "$reference" "$out/prepared-prefix.json"
    python3 -I -B "$d/stage.py" "$d/source" "$out/stage" "$base" \
      "$out/warm/baked_catalog" "$out/fixture/jct" "$reference" "$prepared_pin"
    DOCKER_BUILDKIT=0 docker build --pull=false --network=none --iidfile "$artifact/image.iid" "$out/stage"
    image=$(cat "$artifact/image.iid")
    sha256sum "$out/stage/opt/c11_owned_modules/release_stage_runtime.json" | awk '{print $1}' >"$artifact/runtime.sha"
    git -C "$d" rev-parse HEAD >"$artifact/source.sha"
    docker save -o "$out/image.tar" "$image"
    gzip -1c "$out/image.tar" >"$artifact/image.tar.gz"
    rm "$out/image.tar"
    (cd "$artifact" && sha256sum image.tar.gz image.iid runtime.sha source.sha >SHA256SUMS)
else
    (cd "$artifact" && test "$(wc -l <SHA256SUMS)" = 4 && sha256sum -c SHA256SUMS)
    test "$(git -C "$d" rev-parse HEAD)" = "$(cat "$artifact/source.sha")"
    docker load -i "$artifact/image.tar.gz"
    image=$(cat "$artifact/image.iid")
fi
printf '%s\n' "$image" | grep -Eq '^sha256:[a-f0-9]{64}$'
test "$(docker image inspect --format '{{.Id}} {{.Architecture}}' "$image")" = "$image arm64"
runtime_pin=$(cat "$artifact/runtime.sha")
echo "C11_EXACT_REPLAY_IMAGE $image $runtime_pin"
# Closed market fixtures must remain reproducible after their archival review
# dates; only this network-disabled test sets the review clock. The default
# image entry below still has no activation authorization and checks real time.
docker run --rm --pull never --network none --read-only --cap-drop ALL \
    --security-opt=no-new-privileges \
    --mount "type=bind,src=$d/../release_regressions/check_effective_launch.py,dst=/launch-checker.py,readonly" \
    --entrypoint python "$image" -B -c '
import os,runpy,sys,time
# The real absolute-path entry has this script directory automatically.
# Recreate only that entry directory for the inline check, not PYTHONPATH.
sys.path.insert(0, "/opt/c11_owned_modules")
from history_startup_receipt import Hold,_env_guard
from release_stage import check
_env_guard(os.environ)
profile=runpy.run_path("/launch-checker.py")["startup_environment"]
assert profile([key+"="+value for key,value in os.environ.items()])=="pinned"
assert profile(["PYTHONPATH=/opt/unreviewed"])=="rejected"
try:
    _env_guard(dict(os.environ, PYTHONPATH="/opt/unreviewed"))
except Hold as e:
    assert str(e) == "HISTORY_ENV_INJECTION_REFUSED"
else:
    raise AssertionError("C11_STARTUP_IMPORT_INJECTION_NOT_REJECTED")
time.time_ns = lambda: 1795773600000000000
check()
print("C11_PACKAGED_IMPORT_ROOTS_AND_INJECTION_NEGATIVE_PASS")'
if docker run --rm --pull never --network none --read-only --cap-drop ALL \
    --security-opt=no-new-privileges "$image" >"$out/activation.log" 2>&1; then
    echo "CI_IMAGE_UNEXPECTEDLY_ENABLED"
    exit 2
fi
grep -Fx 'C11_NO_NEW_BAKED_OWNER_AUTHORIZATION_NO_OPERATIONAL_ENTRY' "$out/activation.log"
if [ "$2" = build ]; then
    if ! sh "$d/check_packaged_release_facts.sh" "$image" "$out/release-facts-proof"; then
        printf '%s\n' 'C11_ACTIVATION_ASSET_REGRESSION_FAILED: expected complete release accepted and missing/tampered releases refused; actual probe result is above.'
        if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
            printf '%s\n' '### C11 packaged activation assets' '- Failed step: verify actual startup asset gate on packaged image.' '- Expected: full manifest and eight receipts pass; missing or altered assets are refused.' '- Actual: packaged startup asset regression failed; failing case and first reason are in the build log.' >>"$GITHUB_STEP_SUMMARY"
        fi
        exit 2
    fi
    docker run --rm --pull never --network none --read-only --cap-drop ALL \
        --security-opt=no-new-privileges \
        --mount "type=bind,src=$d/test_baked_predecessor.py,dst=/predecessor-test.py,readonly" \
        --mount "type=bind,src=$d/../release_regressions,dst=/c11-ci-tools,readonly" \
        --entrypoint python "$image" -B /predecessor-test.py
    for test_name in test_uncached_provenance.py test_cold_minute_retention.py test_resident_terminal_veto.py test_exact_end_rpd_veto.py test_original_1m_wire.py test_reviewed_opg_catalog.py test_recent_gap_refill.py test_mixed_close_history_budget.py test_reconfirm_queue_binding.py test_original_revisit_offer.py test_staged_revisit_not_spent.py test_warm_commit_refusal.py test_prepared_cold_checkpoint.py test_terminal_1m_first.py test_terminal_warmup_priority.py test_idle_cold_preparation.py test_stale_cursor_minute_bridge.py test_retained_minute_cold_prefix.py; do
        docker run --rm --pull never --network none --read-only --cap-drop ALL \
            --security-opt=no-new-privileges \
            --mount "type=bind,src=$d/$test_name,dst=/provenance-test.py,readonly" \
            --entrypoint python "$image" -B -c '
import runpy,sys
# Fixtures are mounted outside the sealed package; the actual entry
# already receives this script directory from Python.
sys.path.insert(0, "/opt/c11_owned_modules")
runpy.run_path("/provenance-test.py", run_name="__main__")'
    done
    exit 0
fi
package_entry() {
    mkdir -m 700 "$out/launch-paper"
    sandbox="$(cd "$out/launch-paper" && pwd -P)"
    inspect_launch() {
        cid=$(docker create --network none --read-only --cap-drop ALL --security-opt=no-new-privileges \
            --mount "type=bind,src=$sandbox,dst=/c11-paper,readonly" "$@" "$image")
        docker inspect "$cid" >"$out/launch.json"
        docker rm "$cid" >/dev/null
    }
    validate_launch() {
        python3 -I -B "$d/../release_regressions/check_ci_package_launch.py" \
            "$image" "$(cat "$artifact/source.sha")" "$sandbox" <"$out/launch.json"
    }
    inspect_launch
    validate_launch || return 1
    inspect_launch --entrypoint /bin/sh
    result="$(validate_launch || true)"
    test "$result" = C11_CI_LAUNCH_WRONG_IMAGE_OR_ENTRY || return 1
    inspect_launch --mount "type=bind,src=$sandbox,dst=/opt/c11_worker,readonly"
    result="$(validate_launch || true)"
    test "$result" = C11_CI_LAUNCH_CODE_SHADOW || return 1
    echo C11_CI_LAUNCH_NEGATIVE_WRONG_ENTRY_AND_CODE_SHADOW_REJECTED
}
warm() {
    pin=$runtime_pin
    missing=no
    unicode=no
    ready_basis=no
    terminal_gap=no
    divergent_fallback=no
    mixed_history_pressure=no
    if [ "$2" = warm-divergent-fallback ]; then divergent_fallback=yes; fi
    if [ "$2" = warm-mixed-history-pressure ]; then mixed_history_pressure=yes; fi
    if [ "$2" = warm-ready-basis ]; then ready_basis=yes; fi
    if [ "$2" = warm-terminal-gap ]; then terminal_gap=yes; fi
    if [ "$2" = warm-unicode ]; then unicode=yes; fi
    if [ "$2" = warm-missing-analysis ]; then missing=yes; fi
    docker run --rm --pull never --network none --read-only --user 1000:2000 \
      --cpus 1 --memory 768m --pids-limit 64 --cap-drop ALL --security-opt=no-new-privileges \
      --tmpfs /tmp:rw,nosuid,nodev,mode=1777 \
      --tmpfs /c11-paper:rw,nosuid,nodev,uid=1000,gid=2000,mode=0700 \
      --mount "type=bind,src=$out/warm/warm_fixture,dst=/fixture,readonly" \
      --mount "type=bind,src=$out/warm,dst=/proof,readonly" \
      -e OPENBLAS_NUM_THREADS=1 -e OMP_NUM_THREADS=1 -e C11_WARM_MISSING_ANALYSIS_CASE="$missing" -e C11_WARM_UNICODE_CASE="$unicode" -e C11_WARM_READY_BASIS_CASE="$ready_basis" -e C11_WARM_TERMINAL_GAP_CASE="$terminal_gap" -e C11_WARM_DIVERGENT_FALLBACK_CASE="$divergent_fallback" -e C11_MIXED_HISTORY_PRESSURE="$mixed_history_pressure" -e C11_EXACT_FINAL_RUNTIME_SHA="$pin" \
      -e C11_ARCHIVE_CATALOG_CHECK_MS=1795773600000 -e C11_OFFLINE_EXPECT_BAKED_SCOPE=no -e C11_FUNCTIONAL_TRIAL=yes -e C11_INTENT_CASE=flat \
      --entrypoint python "$image" -B /proof/warm_acceptance.py >"$out/warm-runtime.log" 2>&1 || {
        if [ "$terminal_gap" = yes ]; then
            echo C11_HOSTED_WORKER_CASE_FAILED warm-terminal-gap
            if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
                printf '### C11 terminal-minute refresh
- Expected: the second ranked candidate obtains exactly one closed 1m candle, persists it and composes 15m.
- Actual: the native run did not provide the matched receipt.
- Last successful stage: immutable image and fixture verified.
' >>"$GITHUB_STEP_SUMMARY"
            fi
        elif [ "$ready_basis" = yes ]; then
            if grep -q 'C11_READY_BASIS_REQUESTED_WIDE_WIRE' "$out/warm-runtime.log"; then
                reason=C11_READY_BASIS_REQUESTED_WIDE_WIRE
            else
                reason=C11_READY_BASIS_WORKER_NO_COMPLETION
            fi
            printf '%s\n' "$reason"
            if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
                printf '### Warm Worker: ready 1m basis\n- Expected: original Worker uses local projection without direct 5m/15m.\n- Actual: %s.\n- Last successful stage: verified immutable image and replay inputs.\n' "$reason" >>"$GITHUB_STEP_SUMMARY"
            fi
        else
            echo C11_HOSTED_WORKER_CASE_FAILED warm-original-callback
            # Do not print fixture rows or trace payloads. Keep the exact
            # assertion site/type and bounded allowlisted reason codes.
            detail="$(python3 - "$out/warm-runtime.log" <<'PYFAIL'
from pathlib import Path
import re,sys
rows=Path(sys.argv[1]).read_text(errors='replace').splitlines()
frame=re.compile(r'^  File "/proof/warm_acceptance\.py", line ([0-9]{1,5}), in [A-Za-z_<>]+$')
fault=re.compile(r'^(AssertionError|StopIteration|ValueError|TypeError|KeyError|UnboundLocalError|AttributeError|OperationalError|RuntimeError)(?::|$)')
code=re.compile(r'\bC11_[A-Z0-9_]{4,85}\b')
sites=[m[1] for row in rows if (m:=frame.fullmatch(row))]
errors=[(m[1],sorted(set(code.findall(row)))) for row in rows if (m:=fault.match(row))]
if errors:
    name, reasons=errors[-1]
    counts=[row.split()[1:] for row in rows if re.fullmatch(r'C11_LATE_DELIVERY_COUNTS [0-9]+ [0-9]+ [0-9]+',row)]
    if counts:
        cb,inputs,wire=counts[-1]
        print('late delivery (expected callbacks=1, valid inputs>=1, new requests=0): actual callbacks='+cb+', valid inputs='+inputs+', new requests='+wire)
    print('first failing fixture site: '+(sites[-1] if sites else 'unknown')+'; '+name+'; '+(', '.join(reasons) if reasons else 'no allowlisted code'))
else:
    print('no exception signature; last confirmed stage: immutable image and fixture loaded')
PYFAIL
)"
            printf '%s\n' "$detail"
            if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
                { echo '### Warm Worker replay failed'; echo 'Expected: original callback once in this close, with required stored provenance.'; printf 'Actual: %s\n' "$detail"; } >>"$GITHUB_STEP_SUMMARY"
            fi
        fi
        return 1
    }
    if [ "$terminal_gap" = yes ]; then
        grep -Fx C11_TERMINAL_GAP_WORKER_ONLY_1M_AND_DURABLE "$out/warm-runtime.log"
        return
    fi
    if [ "$ready_basis" = yes ]; then
        grep -E '^C11_FULL_WORKER_READY_BASIS_NO_DIRECT_WIDE [0-9]+$' "$out/warm-runtime.log"
        return
    fi
      grep -Fx C11_LEGACY_WIDE_DID_NOT_MASK_RAW_MINUTE_GAP "$out/warm-runtime.log" &&
      grep -Fx NATIVE_FALLBACK_15M_WITH_DURABLE_1M_AND_ORIGINAL_CALLBACK "$out/warm-runtime.log" &&
      grep -E '^C11_UNREADY_PEER_DOES_NOT_DELAY_ORIGINAL_CALLBACK [0-9]+$' "$out/warm-runtime.log" ||
      { echo C11_HOSTED_WORKER_CASE_FAILED warm-original-callback; return 1; }
    if [ "$mixed_history_pressure" = yes ]; then
      grep -Fx C11_MIXED_SYNTHETIC_HISTORY_ACCOUNTED_BEFORE_REAL_STRATEGY "$out/warm-runtime.log"
      grep -Fx C11_MIXED_WARM_REAL_SQLITE_WRITE_HELD "$out/warm-runtime.log"
      grep -Fx C11_MIXED_WARM_ATOMIC_SQLITE_BUSY_RECORDED "$out/warm-runtime.log"
      grep -Fx C11_WARM_OVERLAP_OLD_BAR_AND_NEW_CALLBACK_BOTH_PRESERVED "$out/warm-runtime.log"
      grep -Fx C11_COMBINED_HISTORY_PRESSURE_AND_SAME_CLOSE_REVISIT_PASS "$out/warm-runtime.log"
    fi
    if [ "$divergent_fallback" = yes ]; then
      grep -Fx C11_DIVERGENT_FALLBACK_DID_NOT_BLOCK_ORIGINAL_DECISION "$out/warm-runtime.log"
    fi
    if [ "$missing" = yes ]; then
      grep -Fx ACTUAL_ANALYZED_FRAME_MISSING "$out/warm-runtime.log"
      grep -Fx C11_RAW_REFILL_AND_ORIGINAL_CALLBACK_WITH_MISSING_ANALYZED_CACHE "$out/warm-runtime.log"
    fi
    if [ "$unicode" = yes ]; then
      grep -Fx C11_UNICODE_ORIGINAL_CONTRACT_IN_PROVIDER_AND_NATIVE_WIRE "$out/warm-runtime.log"
    fi
}
reviewed_opg() {
    docker run --rm --pull never --network none --read-only --user 1000:2000 \
      --cpus 1 --memory 768m --pids-limit 64 --cap-drop ALL --security-opt=no-new-privileges \
      --tmpfs /tmp:rw,nosuid,nodev,mode=1777 \
      --tmpfs /c11-paper:rw,nosuid,nodev,uid=1000,gid=2000,mode=0700 \
      --mount "type=bind,src=$d/fixtures/reviewed-opg-public,dst=/fixture,readonly" \
      --mount "type=bind,src=$d/reviewed_opg_acceptance.py,dst=/replay-opg.py,readonly" \
      --mount "type=bind,src=$out/warm/warm_fixture,dst=/original,readonly" \
      -e OPENBLAS_NUM_THREADS=1 -e OMP_NUM_THREADS=1 -e C11_ARCHIVE_CATALOG_CHECK_MS=1795773600000 \
      --entrypoint python "$image" -B /replay-opg.py >"$out/opg-replay.log" 2>&1 &&
      grep -Fq 'PASS_WHOLE_RECONFIRMED_OPG_WORKER_CORRECTED_NATIVE_CALLBACK_ONCE' "$out/opg-replay.log" || {
        reason=ORIGINAL_CORRECTION_CALLBACK_NOT_PROVEN
        if grep -q 'Permission denied' "$out/opg-replay.log"; then
            reason=FIXTURE_NOT_READABLE
        elif grep -q 'RUNTIME_MANIFEST_PIN' "$out/opg-replay.log"; then
            reason=IMAGE_PIN_INVALID
        fi
        echo "C11_HOSTED_WORKER_CASE_FAILED reviewed-opg $reason"
        if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
            printf '### Reviewed OPG history\n- Expected: original callback once and preserved canonical rows.\n- Actual: %s.\n- Last successful stage: exact artifact and pinned inputs selected.\n' "$reason" >>"$GITHUB_STEP_SUMMARY"
        fi
        return 1
    }
    echo 'C11_REVIEWED_OLD_CORRECTIONS_AND_ORIGINAL_STRATEGY_PASS'
}
cold() {
    remote=no
    prepared=no
    no_listing=no
    background=no
    pinned_no_discovery=no
    archive_outage=no
    on_demand=no
    case "$1" in
        background-blank) mode=yes; cached=yes; background=yes;;
        unseeded-on-demand) mode=early-roster-return; cached=no; on_demand=yes;;
        archive-source-outage) mode=early-roster-return; cached=no; archive_outage=yes;;
        prepared-blank) mode=yes; cached=yes; prepared=yes;;
        prepared-stale) mode=auto-stale; cached=yes; prepared=yes;;
        prepared-return) mode=early-roster-return; cached=yes; prepared=yes;;
        cached-blank) mode=early-roster-return; cached=yes;;
        cached-stale) mode=auto-stale; cached=yes;;
        pinned-remote-blank) mode=early-roster-return; cached=no; remote=yes;;
        pinned-remote-no-listing) mode=early-roster-return; cached=no; remote=yes; pinned_no_discovery=yes;;
        pinned-remote-stale) mode=auto-stale; cached=no; remote=yes;;
        *) mode=$1; cached=no;;
    esac
    if C11_REPLAY_ON_DEMAND="$on_demand" C11_REPLAY_ARCHIVE_OUTAGE="$archive_outage" C11_REPLAY_PINNED_REMOTE="$remote" C11_REPLAY_PINNED_WITHOUT_DISCOVERY="$pinned_no_discovery" C11_REPLAY_VERIFIED_RAW_CACHE="$cached" C11_REPLAY_PREPARED_CHECKPOINT="$prepared" C11_REPLAY_NO_LISTING_ORIGIN="$no_listing" C11_REPLAY_BACKGROUND_REVIEWED="$background" sh "$d/native_history_scheduler/run_jct_worker_review.sh" "$image" "$out/fixture/jct" \
        "$out/fixture/stopped-paper-only.tgz" "$out/$mode" no "$mode" >"$out/$mode.log" 2>&1; then
        if python3 -I -B "$d/verify-replay.py" "$mode" "$out/$mode.log" "$cached" "$remote" "$prepared" "$no_listing" "$background" "$pinned_no_discovery" "$archive_outage" "$on_demand"; then
            return 0
        fi
    fi
    # CI jobs are public; do not print raw fixture diagnostics or archive rows.
    echo "C11_HOSTED_WORKER_CASE_FAILED $mode"
    if grep -q 'C11_REPLAY_FIXTURE_NOT_READABLE' "$out/$mode.log"; then
        echo "C11_REPLAY_FIXTURE_NOT_READABLE $mode"
    elif grep -q 'Permission denied' "$out/$mode.log"; then
        echo "C11_REPLAY_PERMISSION_ERROR $mode"
    elif grep -q 'docker: ' "$out/$mode.log"; then
        echo "C11_REPLAY_DOCKER_START_ERROR $mode"
    else
        python3 -I -B "$d/verify-replay.py" "$mode" "$out/$mode.log" "$cached" "$remote" "$prepared" "$no_listing" "$background" "$pinned_no_discovery" "$archive_outage" "$on_demand" || true
    fi
    return 1
}
case "$2" in package-entry) package_entry;; warm|warm-missing-analysis|warm-unicode|warm-ready-basis|warm-terminal-gap|warm-divergent-fallback|warm-mixed-history-pressure) warm "$@";; reviewed-opg) reviewed_opg;; *) cold "$2";; esac
