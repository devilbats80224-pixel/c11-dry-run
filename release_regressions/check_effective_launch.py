"""Verify sanitized Docker inspect data against an independently reviewed launch receipt."""
import json
from pathlib import Path, PurePosixPath
import re
import sys

CODE_ROOTS = tuple(map(PurePosixPath,(
    '/opt/c11_owned_modules','/opt/c11_worker','/opt/c11_background_review',
    '/opt/c11-reviewed-seeds','/opt/c11-reviewed-cold-prefix','/freqtrade',
)))
HEX40 = re.compile(r'[a-f0-9]{40}')

def launch(d):
    mounts = []
    for m in d['Mounts']:
        raw = m['Destination']
        if not isinstance(raw,str):
            raise ValueError('MALFORMED_MOUNT')
        p = PurePosixPath(raw)
        if not p.is_absolute() or '..' in p.parts or str(p)!=raw or raw.startswith('//'):
            raise ValueError('MALFORMED_MOUNT')
        # An ancestor mount can hide the entire runtime tree, not just a leaf.
        if any(p == root or p in root.parents or root in p.parents for root in CODE_ROOTS):
            raise ValueError('MOUNT_SHADOWS_TESTED_CODE')
        if type(m['RW']) is not bool:
            raise ValueError('MALFORMED_MOUNT')
        source = m.get('Name') if m['Type']=='volume' else m.get('Source')
        if not isinstance(source,str) or not source:
            raise ValueError('UNBOUND_MOUNT_IDENTITY')
        mounts.append((raw,m['Type'],source,m['RW']))
    if len({m[0] for m in mounts})!=len(mounts):
        raise ValueError('DUPLICATE_MOUNT')
    if not re.fullmatch(r'sha256:[a-f0-9]{64}',d['Image']):
        raise ValueError('INVALID_IMAGE')
    if (not isinstance(d['Path'],str) or not d['Path'] or type(d['Args']) is not list
            or not all(isinstance(a,str) for a in d['Args'])):
        raise ValueError('MALFORMED_ENTRYPOINT')
    return d['Image'],d['Path'],tuple(d['Args']),sorted(mounts)

def verify(receipt, actual):
    try:
        # Record GitHub delivery trigger and Mac's actual source separately.
        if not all(HEX40.fullmatch(receipt[k]) for k in ('remote_merge','local_worker_source')):
            return 'UNBOUND_SOURCE_REVISION'
        tested = launch(receipt['tested_launch'])
        current = launch(actual)
        for a,b,reason in zip(tested,current,(
                'IMAGE_MISMATCH','EXECUTABLE_MISMATCH','ARGUMENTS_MISMATCH','MOUNT_MISMATCH')):
            if a!=b:
                return reason
        return 'MATCH'
    except ValueError as e:
        return str(e)
    except (KeyError,TypeError):
        return 'MALFORMED_RECEIPT'

if __name__=='__main__':
    if len(sys.argv)!=3:
        raise SystemExit('usage: check_effective_launch.py REVIEWED_RECEIPT SANITIZED_INSPECT_JSON')
    result = verify(*(json.loads(Path(x).read_text()) for x in sys.argv[1:]))
    print(result)
    raise SystemExit(0 if result=='MATCH' else 2)
