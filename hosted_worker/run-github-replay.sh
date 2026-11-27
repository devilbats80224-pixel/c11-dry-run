#!/bin/sh
set -eu
[ "$#" = 3 ] || exit 64
case "$2" in build|warm|early-roster-return|auto-stale|reviewed-partial|cached-blank|cached-stale) ;; *) exit 64;; esac
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
    python3 -I -B "$d/stage.py" "$d/source" "$out/stage" "$base" "$out/warm/baked_catalog" "$out/fixture/jct"
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
    --security-opt=no-new-privileges --entrypoint python "$image" -B -c '
import time
from release_stage import check
time.time_ns = lambda: 1795773600000000000
check()'
if docker run --rm --pull never --network none --read-only --cap-drop ALL \
    --security-opt=no-new-privileges "$image" >"$out/activation.log" 2>&1; then
    echo "CI_IMAGE_UNEXPECTEDLY_ENABLED"
    exit 2
fi
grep -Fx 'C11_NO_NEW_BAKED_OWNER_AUTHORIZATION_NO_OPERATIONAL_ENTRY' "$out/activation.log"
if [ "$2" = build ]; then
    for test_name in test_uncached_provenance.py test_cold_minute_retention.py; do
        docker run --rm --pull never --network none --read-only --cap-drop ALL \
            --security-opt=no-new-privileges \
            --mount "type=bind,src=$d/$test_name,dst=/provenance-test.py,readonly" \
            --entrypoint python "$image" -B /provenance-test.py
    done
    exit 0
fi
warm() {
    pin=$runtime_pin
    docker run --rm --pull never --network none --read-only --user 1000:2000 \
      --cpus 1 --memory 768m --pids-limit 64 --cap-drop ALL --security-opt=no-new-privileges \
      --tmpfs /tmp:rw,nosuid,nodev,mode=1777 \
      --tmpfs /c11-paper:rw,nosuid,nodev,uid=1000,gid=2000,mode=0700 \
      --mount "type=bind,src=$out/warm/warm_fixture,dst=/fixture,readonly" \
      --mount "type=bind,src=$out/warm,dst=/proof,readonly" \
      -e OPENBLAS_NUM_THREADS=1 -e OMP_NUM_THREADS=1 -e C11_EXACT_FINAL_RUNTIME_SHA="$pin" \
      -e C11_ARCHIVE_CATALOG_CHECK_MS=1795773600000 -e C11_OFFLINE_EXPECT_BAKED_SCOPE=no -e C11_FUNCTIONAL_TRIAL=yes -e C11_INTENT_CASE=flat \
      --entrypoint python "$image" -B /proof/warm_acceptance.py >"$out/warm-runtime.log" 2>&1 &&
      grep -Fx NATIVE_FALLBACK_15M_WITH_DURABLE_1M_AND_ORIGINAL_CALLBACK "$out/warm-runtime.log" ||
      { echo C11_HOSTED_WORKER_CASE_FAILED warm-original-callback; return 1; }
}
cold() {
    case "$1" in
        cached-blank) mode=early-roster-return; cached=yes;;
        cached-stale) mode=auto-stale; cached=yes;;
        *) mode=$1; cached=no;;
    esac
    if C11_REPLAY_VERIFIED_RAW_CACHE="$cached" sh "$d/native_history_scheduler/run_jct_worker_review.sh" "$image" "$out/fixture/jct" \
        "$out/fixture/stopped-paper-only.tgz" "$out/$mode" no "$mode" >"$out/$mode.log" 2>&1; then
        if python3 -I -B "$d/verify-replay.py" "$mode" "$out/$mode.log" "$cached"; then
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
        python3 -I -B "$d/verify-replay.py" "$mode" "$out/$mode.log" "$cached" || true
    fi
    return 1
}
if [ "$2" = warm ]; then warm; else cold "$2"; fi
