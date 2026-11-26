#!/bin/sh
# Same-image regressions for earlier fixes; missing Han input fails closed.
set -eu
if [ ! -f "${C11_HAN_FIXTURE:?point to archived Chinese native replay input}/jct-after-archive.native.json" ] ||
   [ ! -f "$C11_HAN_FIXTURE/receipt.json" ]; then
    echo 'Missing archived Chinese native replay input' >&2
    exit 66
fi
for test in test_cold_delivery_isolation.py test_han_native_pages.py; do
    docker run --rm --pull never --network none --read-only --user 1000:2000 \
      --cpus 1 --memory 768m --pids-limit 64 --cap-drop ALL --security-opt=no-new-privileges \
      -e OPENBLAS_NUM_THREADS=1 -e OMP_NUM_THREADS=1 \
      -e PYTHONPATH=/opt/c11_owned_modules:/opt/c11_worker:/opt/c11_worker/ports:/opt/c11_background_review:/freqtrade \
      --mount "type=bind,src=$C11_TEST_DIR/$test,dst=/verify.py,readonly" \
      --mount "type=bind,src=$C11_HAN_FIXTURE,dst=/han,readonly" \
      --entrypoint python "$C11_TEST_IMAGE" -B /verify.py >"$C11_TEST_OUTPUT/$test.log" 2>&1
done
grep -Fx OK "$C11_TEST_OUTPUT/test_cold_delivery_isolation.py.log" >/dev/null
python3 - "$C11_TEST_OUTPUT/test_han_native_pages.py.log" <<'PY'
import json,sys
from pathlib import Path
r=json.loads(Path(sys.argv[1]).read_text().splitlines()[-1])
assert {x['case'] for x in r}=={'rate','late'}
assert all(x['symbol']=='牛来USDT' and x['sibling_persisted'] for x in r)
PY
