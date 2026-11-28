#!/bin/sh
set -eu
umask 077
test "$#" = 2
image=$1
out=$2
d=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
tool="$d/../release_regressions"
mkdir -m755 "$out"
out=$(cd "$out" && pwd -P)
probe="$d/test_packaged_release_facts.py"
prodpins="$d/source/c11_worker/c11_v4_pin.py"
facts="$d/source/c11_worker/release_facts.py"

probe_image() {
    docker run --rm --pull never --network none --read-only --user 1000:2000 --cap-drop ALL \
        --security-opt=no-new-privileges \
        --mount "type=bind,src=$probe,dst=/ci-release-probe.py,readonly" \
        --mount "type=bind,src=$out/$1/pins.py,dst=/ci-release-pins.py,readonly" \
        --entrypoint python "$2" -B /ci-release-probe.py check /ci-release-pins.py "$3"
}

for c in complete unreadable-as-runtime-user missing-route tampered; do
    p="$out/$c"
    mkdir -p "$p/context"
    python3 -I -B "$probe" prepare "$p/source" "$p/pins.py" "$prodpins" "$facts" "$c"
    chmod 0444 "$p/pins.py"
    case "$c" in
        complete)
            # Reproduce the postmerge failure with the exact unaugmented candidate.
            probe_image "$c" "$image" C11_RELEASE_FILE_MISSING
            # The same mode-preserving context builder is used for delivery.
            rmdir "$p/context"
            sh "$tool/create_release_facts_layer_context.sh" "$p/source" "$p/pins.py" "$facts" "$p/context"
            expected=PASS
            ;;
        unreadable-as-runtime-user)
            # Reproduce the Mac CD failure with the same valid pinned bytes:
            # staged via a plain host copy under umask 077, files exist but
            # root-owned paths cannot be traversed/read by container UID 1000.
            cp -R "$p/source" "$p/context/c11-release"
            chmod 0700 "$p/context/c11-release" "$p/context/c11-release/receipts"
            find "$p/context/c11-release" -type f -exec chmod 0400 '{}' +
            expected=UNREADABLE_AS_UID_1000
            ;;
        missing-route|tampered)
            if python3 -I -B "$tool/stage_inherited_release_facts.py" "$p/source" "$p/context/c11-release" "$p/pins.py" "$facts" >"$p/reject.log" 2>&1; then
                echo "C11_PREPARE_UNEXPECTED_PASS case=$c"; exit 2
            fi
            test ! -e "$p/context/c11-release"
            # Intentionally manufacture the bad image in CI only. The production
            # staging helper above is required to refuse to construct it.
            mv "$p/source" "$p/context/c11-release"
            chmod 0755 "$p/context/c11-release" "$p/context/c11-release/receipts"
            find "$p/context/c11-release" -type f -exec chmod 0444 '{}' +
            case "$c" in
                missing-route) expected=C11_RELEASE_FILE_MISSING;;
                tampered) expected=C11_RELEASE_EVIDENCE_MISMATCH;;
            esac
            ;;
    esac
    if [ "$c" != complete ]; then
        cp "$tool/install_release_facts.Dockerfile" "$p/context/Dockerfile"
    fi
    DOCKER_BUILDKIT=0 docker build --pull=false --network none \
        --build-arg "C11_BASE_IMAGE=$image" --iidfile "$p/image.iid" \
        "$p/context"
    probe_image "$c" "$(cat "$p/image.iid")" "$expected"
done
echo C11_EXACT_ACTIVATION_ASSETS_FULL_AND_NEGATIVES_PASS
