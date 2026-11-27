"""Exact approved checkpoint must load from disk or fail closed."""
from dataclasses import replace
from hashlib import sha256
import json
from pathlib import Path
from threading import Event
from unittest.mock import patch

from causal_bars import QUARTER
from cold_archive_release import installed_sources
from cold_prefix import derive
from prepared_cold_checkpoint import NAME, decode

NOW = 1795773600000
SYMBOL = 'JCTUSDT'
PATH = Path('/opt/c11_owned_modules/release_stage_runtime.json')

def run():
    m = json.loads(PATH.read_bytes())
    source = installed_sources(m, NOW, verify_content=True)[SYMBOL]
    assert source.prepared_sha256, 'reviewed source lacks a prepared checkpoint pin'
    raw = (source.directory/NAME).read_bytes()
    with patch('cold_prefix.derive_records', side_effect=AssertionError('listing-origin replay')):
        a=derive(SYMBOL,source,lambda:NOW,Event())
        assert a.rpd.next_open==source.cutoff and len(a.quarters)==260
        for mutation in (
            replace(source,prepared_sha256='f'*64),
            replace(source,directory=source.directory/'missing'),
            replace(source,cutoff=source.cutoff+QUARTER),
            replace(source,review_expires_ms=NOW),
            replace(source,reviewed_at_ms=a.rpd.observed-1),
        ):
            try:
                derive(SYMBOL,mutation,lambda:NOW,Event())
            except (OSError,ValueError):
                continue
            raise AssertionError('bad prepared checkpoint accepted')
    body=json.loads(raw)
    body['archive_manifest_sha']='a'*64
    switched=json.dumps(body).encode()
    try:
        decode(switched,symbol=SYMBOL,source=replace(source,prepared_sha256=sha256(switched).hexdigest()),now=NOW)
    except ValueError:
        pass
    else:
        raise AssertionError('checkpoint detached from reviewed source')
    print('C11_REVIEWED_PREPARED_CHECKPOINT_OLD_ARCHIVE_NOT_OPENED')

if __name__=='__main__':
    run()
