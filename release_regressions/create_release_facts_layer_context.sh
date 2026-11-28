#!/bin/sh
# Build context only: no Docker, credentials, runtime writes or activation.
set -eu
umask 077
test "$#" = 4
d=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
src=$1
pins=$2
verifier=$3
out=$4
test ! -e "$out" && test ! -L "$out"
mkdir -m 700 "$out"
python3 -I -B "$d/stage_inherited_release_facts.py" "$src" "$out/c11-release" "$pins" "$verifier"
cp "$d/install_release_facts.Dockerfile" "$out/Dockerfile"
