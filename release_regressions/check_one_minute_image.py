"""Run *inside* the immutable C11 candidate image. Only covers file presence/pins."""
from hashlib import sha256
import json
from pathlib import Path
import sys

root = Path('/opt/c11_owned_modules')
required = (
    'auto_cold_archive', 'cold_archive_identity', 'native_minute_admission_loop',
    'native_primary_lane', 'prepared_owner_history', 'shared_history_forwarder',
    'shared_native_budget', 'opg_settled_recovery', 'reconfirm_dispatch',
    'warm_release_binding', 'closed_minute_basis',
)

def audit(base, expected, names=required):
    result = {}
    for name in names:
        path = base / (name + '.py')
        if not path.is_file():
            result[name] = 'MISSING_FROM_IMAGE'
        elif sha256(path.read_bytes()).hexdigest() != expected.get(str(path)):
            result[name] = 'MISSING_OR_WRONG_MANIFEST_DIGEST'
        else:
            result[name] = 'MATCH'
    return result

if __name__ == '__main__':
    manifest = (root / 'release_stage_runtime.json').read_bytes()
    if len(sys.argv) != 2 or sha256(manifest).hexdigest() != sys.argv[1]:
        raise SystemExit('WRONG_C11_RUNTIME_MANIFEST')
    evidence = audit(root, json.loads(manifest)['files'])
    print(json.dumps(evidence, sort_keys=True))
    raise SystemExit(0 if all(state == 'MATCH' for state in evidence.values()) else 2)
