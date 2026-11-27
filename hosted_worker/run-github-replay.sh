#!/bin/sh
set -eu
[ "$#" = 1 ] || exit 64
out=$1
base=sha256:1bb9a6b11a4dae39444331aa324b139dcd635f6a1c586ce9ecccac17994edc0c
fixture_sha=566dcb2865792e8f7ffdab8e0b223fb273ba303ecd8c3710fd10c7e66fac9b6a
fixture_url=https://github.com/devilbats80224-pixel/c11-dry-run/releases/download/c11-ci-replay-fixtures-v1/c11-ci-paper-replay-v1.tar.gz
d=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
mkdir -m 700 "$out"
curl -fsSL --retry 2 --max-time 180 -o "$out/input.tgz" "$fixture_url"
printf '%s  %s\n' "$fixture_sha" "$out/input.tgz" | sha256sum -c -
mkdir -m 700 "$out/fixture"
tar -xzf "$out/input.tgz" --directory "$out/fixture" --no-same-owner --no-same-permissions
docker pull --platform linux/arm64 "docker.io/freqtradeorg/freqtrade@$base"
python3 -I -B "$d/stage.py" "$d/source" "$out/stage" "$base"
DOCKER_BUILDKIT=0 docker build --pull=false --network=none --iidfile "$out/image.iid" "$out/stage"
image=$(cat "$out/image.iid")
docker run --rm --pull never --network none --read-only --cap-drop ALL \
    --security-opt=no-new-privileges "$image" --review-self-check
if docker run --rm --pull never --network none --read-only --cap-drop ALL \
    --security-opt=no-new-privileges "$image" >"$out/activation.log" 2>&1; then
    echo "CI_IMAGE_UNEXPECTEDLY_ENABLED"
    exit 2
fi
grep -Fx 'C11_NO_NEW_BAKED_OWNER_AUTHORIZATION_NO_OPERATIONAL_ENTRY' "$out/activation.log"
fail=0
for mode in early-roster-return auto-stale reviewed-partial; do
    if sh "$d/native_history_scheduler/run_jct_worker_review.sh" "$image" "$out/fixture/jct" \
        "$out/fixture/stopped-paper-only.tgz" "$out/$mode" no "$mode" >"$out/$mode.log" 2>&1; then
        if python3 -I -B "$d/verify-replay.py" "$mode" "$out/$mode.log"; then
            continue
        fi
    fi
    # CI jobs are public; do not print raw fixture diagnostics or archive rows.
    echo "C11_HOSTED_WORKER_CASE_FAILED $mode"
    python3 -I -B "$d/verify-replay.py" "$mode" "$out/$mode.log" || true
    fail=1
done
exit "$fail"
