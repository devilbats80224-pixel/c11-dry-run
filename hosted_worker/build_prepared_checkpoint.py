"""Create pinned fixture bytes offline; never registers a release approval."""
from pathlib import Path
import sys
from threading import Event

root=Path('/raw-build')
sys.path[:0]=[str(root/p) for p in ('c11_owned_modules','c11_worker','c11_worker/ports')]
from cold_archive_release import verified_catalog, read_bounded
from cold_prefix import ClosedPrefixSource, derive
from prepared_cold_checkpoint import encode

catalog=root/'c11-reviewed-cold-prefix'
# This is the archived fixture's availability clock, not a current lease.
now=1795773600000
sources,_=verified_catalog(catalog,sys.argv[1],now,lambda path,n:read_bounded(path,n,immutable=False),
                           verify_content=True)
symbol,=sources
r=sources[symbol]
source=ClosedPrefixSource(catalog/symbol,r['manifest_sha256'],r['origin_ms'],r['cutoff_ms'],
                         review_expires_ms=r['use_until_ms'])
result=derive(symbol,source,lambda:now,Event())
sys.stdout.buffer.write(encode(source,result))
