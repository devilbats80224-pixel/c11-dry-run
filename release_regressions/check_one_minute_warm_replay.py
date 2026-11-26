#!/usr/bin/env python3
"""Release acceptance for the archived warm regression, not just its refill."""
import json
import sys
from pathlib import Path

MARKER = 'PACKAGED_WARM_ROUTE_ORIGINAL_CALLBACK '

def check(output):
    records = [line.removeprefix(MARKER) for line in output.splitlines() if line.startswith(MARKER)]
    if len(records) != 1:
        return False, 'NO_UNIQUE_ORIGINAL_CALLBACK_RECEIPT'
    requests = json.loads(records[0])
    if not isinstance(requests, list) or not all(
        isinstance(x, list) and len(x) == 3 and isinstance(x[0], str)
        and isinstance(x[1], str) and type(x[2]) is int and x[2] > 0 for x in requests
    ):
        return False, 'INVALID_REQUEST_RECEIPT'
    if ['STBLUSDT', '1m', 45] not in requests:
        return False, 'MISSING_STBL_45_MINUTE_REFILL'
    unexpected = sorted({(symbol, interval) for symbol, interval, _ in requests if interval != '1m'})
    if unexpected:
        return False, 'OUTBOUND_NON_1M ' + json.dumps(unexpected, ensure_ascii=False)
    return True, 'ONE_MINUTE_ONLY_AND_ORIGINAL_CALLBACK'

if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('usage: check_one_minute_warm_replay.py <warm.log>')
    ok, detail = check(Path(sys.argv[1]).read_text())
    print(detail)
    raise SystemExit(0 if ok else 2)
