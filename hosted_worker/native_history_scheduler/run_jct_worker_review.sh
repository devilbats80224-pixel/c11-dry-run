#!/bin/sh
set -eu
[ "$#" = 5 ] || [ "$#" = 6 ] || exit 64
image=$1
fixture=$2
stopped=$3
out=$4
baseline=$5
automatic=${6:-no}
printf '%s\n' "$image" | grep -Eq '^sha256:[a-f0-9]{64}$'
case "$baseline:$automatic" in yes:no|no:yes|no:han|no:han-restart|no:init|no:no|no:reviewed-partial|no:auto-stale|no:roster-return|no:early-roster-return|no:early-deliver|no:early-hold|no:early-start|no:early-no-start|no:mixed-blank|no:mixed-unusable|no:mixed-advanced) ;; *) exit 64;; esac
test "$(shasum -a 256 "$stopped" | awk '{print $1}')" = 470d26f561f5ef25e8467fc67b3250e36fb4f54f280e20afa8f725c5767c2eac
root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
mkdir -m 755 "$out"
cp "$root/native_history_scheduler/replay_jct_worker.py" "$root/native_ccxt_projection/current_locked_startup_check.py" "$out/"
set --
case "$automatic" in mixed-blank|mixed-unusable|mixed-advanced)
    cp "$root/native_history_scheduler/mixed_replay_evidence.py" "$out/"
    set -- "$@" -e "C11_JCT_MIXED_PACKAGED=${automatic#mixed-}"
    automatic=yes
    ;;
esac
if [ "${C11_REPLAY_ARCHIVE_OUTAGE:-no}" = yes ]; then
    set -- "$@" -e C11_JCT_ARCHIVE_OUTAGE=yes
fi
if [ "${C11_REPLAY_ON_DEMAND:-no}" = yes ]; then
    set -- "$@" -e C11_JCT_ON_DEMAND=yes
fi
if [ "${C11_REPLAY_BACKGROUND_REVIEWED:-no}" = yes ]; then
    set -- -e C11_JCT_BACKGROUND_REVIEWED=yes
fi
if [ "${C11_REPLAY_NO_LISTING_ORIGIN:-no}" = yes ]; then
    set -- "$@" -e C11_JCT_NO_LISTING_ORIGIN=yes
fi
if [ "${C11_REPLAY_PREPARED_CHECKPOINT:-no}" = yes ]; then
    set -- "$@" -e C11_JCT_PREPARED_CHECKPOINT=yes
fi
if [ "${C11_REPLAY_VERIFIED_RAW_CACHE:-no}" = yes ]; then
    set -- "$@" -e C11_JCT_REVIEWED_CACHE=yes
fi
if [ "${C11_REPLAY_PINNED_WITHOUT_DISCOVERY:-no}" = yes ]; then
    set -- "$@" -e C11_JCT_PINNED_WITHOUT_DISCOVERY=yes
fi
if [ "${C11_REPLAY_PINNED_REMOTE:-no}" = yes ]; then
    set -- "$@" -e C11_JCT_REVIEWED_REMOTE=yes
fi
if [ "$automatic" = roster-return ] || [ "$automatic" = early-roster-return ]; then
    if [ "$automatic" = early-roster-return ]; then
        set -- "$@" -e C11_JCT_EARLY_ROSTER=yes
    fi
    automatic=yes
    set -- "$@" -e C11_JCT_ROSTER_RETURN=yes
fi
case "$automatic" in early-deliver|early-hold|early-start|early-no-start)
    set -- "$@" -e "C11_JCT_EARLY_EXPECT=${automatic#early-}"
    automatic=yes
    ;;
esac
if [ "$automatic" = auto-stale ]; then
    automatic=yes
    set -- "$@" -e C11_JCT_AUTO_STALE=yes
fi
if [ "$automatic" = reviewed-partial ]; then
    automatic=no
    set -- -e C11_JCT_REVIEWED_PARTIAL=yes
fi
if [ "$automatic" != no ]; then
    mkdir -m 755 "$out/fixture"
    for file in receipt.json jct-after-archive.native.json contract.json; do
        cp "$fixture/$file" "$out/fixture/$file"
    done
    if [ "$automatic" = han ] || [ "$automatic" = han-restart ]; then
        cp "$fixture/input-pins.json" "$out/fixture/input-pins.json"
        set -- -e C11_HAN_AUTO_ARCHIVE=yes
        if [ "$automatic" = han-restart ]; then
            restart=yes
        fi
        automatic=yes
    elif [ "$automatic" = init ]; then
        cp "$fixture/input-pins.json" "$out/fixture/input-pins.json"
        set -- -e C11_INIT_AUTO_ARCHIVE=yes
        automatic=yes
    fi
    set -- "$@" --mount "type=bind,src=$fixture/jct_official_archives,dst=/archive-wire,readonly"
    fixture="$out/fixture"
fi
set -- "$@" --rm --pull never --network none --read-only --user 1000:2000 \
    --cpus 1 --memory 768m --pids-limit 64 --cap-drop ALL --security-opt=no-new-privileges \
    --tmpfs /tmp:rw,nodev,nosuid,noexec,size=64m,mode=1777 \
    --tmpfs /c11-paper:rw,nodev,nosuid,noexec,uid=1000,gid=2000,size=64m,mode=0700 \
    -e OPENBLAS_NUM_THREADS=1 -e OMP_NUM_THREADS=1 -e C11_JCT_BASELINE="$baseline" -e C11_JCT_AUTO_ARCHIVE="$automatic" \
    --mount "type=bind,src=$out,dst=/fixture-driver,readonly" \
    --mount "type=bind,src=$fixture,dst=/jct,readonly" \
    --mount "type=bind,src=$stopped,dst=/stopped.tar.gz,readonly"
# Hosted Docker runs the worker as UID 1000, not the runner's host UID.
# Require the exact isolated mounts to be readable before entering the replay.
docker run "$@" --entrypoint /bin/sh "$image" -ec '
    test -r /fixture-driver/replay_jct_worker.py &&
    test -r /jct/receipt.json && test -r /stopped.tar.gz &&
    { test ! -d /archive-wire || test -r /archive-wire/manifest.json; }' ||
   { echo C11_REPLAY_FIXTURE_NOT_READABLE >&2; exit 67; }
if [ "${restart:-no}" = yes ]; then
    docker run "$@" --entrypoint /bin/sh "$image" -ec '
        C11_HAN_RESTART_PHASE=prepare python -B /fixture-driver/replay_jct_worker.py
        C11_HAN_RESTART_PHASE=resume python -B /fixture-driver/replay_jct_worker.py
    '
else
    docker run "$@" --entrypoint python "$image" -B /fixture-driver/replay_jct_worker.py
fi
