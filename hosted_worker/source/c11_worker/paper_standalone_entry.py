"""Standalone paper release entrypoint: no project grant, no runtime override."""
import json
import os
from pathlib import Path
import secrets
import sys
from importlib.metadata import version

from bridge_protocol import Pipe, Refused, need
from release_facts import ReleaseRefused, require_release_facts

ROOT = Path('/c11-paper')
RELEASE = Path('/opt/c11-release')

def startup_config(scope, seed_pairs):
    from native_worker import config_for
    cfg = config_for(scope['wallet_usdt'], seed_pairs)
    # Missing optional transports are natively off. Supplying a partial
    # disabled section still invokes that section's full credential schema.
    need(not {'telegram','api_server','webhook','discord'} & set(cfg),
         'C11_PAPER_NOTIFICATION_CONFIG_FORBIDDEN')
    need(cfg['dry_run'] is True and cfg['runmode'].value == 'dry_run'
         and cfg['exchange']['name'] == 'binance' and cfg['exchange']['enable_ws'] is False
         and set(cfg['exchange']) == {'name','enable_ws','pair_whitelist','pair_blacklist','ccxt_config'}
         and cfg['db_url'] == 'sqlite:///' + scope['ledger'],
         'C11_PAPER_NATIVE_CONFIG')
    return cfg


def entry(pipe, scope, pin):
    require_release_facts(RELEASE, pin, scope)
    need(version('freqtrade') == '2026.10' and version('ccxt') == '4.5.90', 'C11_RUNTIME_VERSION')
    from paper_runtime import PaperState, verify_state_file
    from entry_feed import EntryFeed
    from observer_rpc import ObserverClient
    from native_worker import PaperWorker
    from standalone_policy import build_policy

    nonce = secrets.token_hex(16)
    pipe.send({'op': 'C11_PAPER_BRIDGE_V1', 'nonce': nonce})
    got = pipe.receive(5)
    need(got == dict(nonce=nonce, binding=scope['publication_binding']), 'C11_PAPER_BRIDGE_IDENTITY')
    observer = ObserverClient(pipe, scope['publication_binding'])
    feed = EntryFeed(pipe, scope['generation_floor'])
    first = feed.read()
    need(bool(first), 'C11_NO_FRESH_CLASSIFIED_OBSERVATION')
    cfg = startup_config(scope, [row.pair for row in first])
    with PaperState() as state:
        for name in ('hard-stop.sqlite', 'intents.sqlite', 'paper-evidence.sqlite',
                     'causal-candles.sqlite','observer-native.sqlite'):
            for suffix in ('', '-wal', '-shm', '-journal'):
                verify_state_file(ROOT / (name + suffix))
        worker = PaperWorker(cfg, build_policy(ROOT), feed, observer)
        try:
            state.check_locked()
            worker.run()
        finally:
            worker.exit()

def main():
    out = None
    try:
        need(len(sys.argv)==1 and os.getuid()==1000 and os.getgid()==2000
             and not any(k.startswith('FREQTRADE__') for k in os.environ)
             and not Path('/run/openmarket-credential').exists()
             and not Path('/var/run/docker.sock').exists(), 'C11_PAPER_PROCESS_SCOPE')
        # Baked in by build; neither the bridge peer nor Compose env sets these.
        from paper_build_pin import RELEASE_SHA256, REVIEWED_SCOPE
        out = os.dup(1)
        os.dup2(2, 1)
        os.umask(0o077)
        entry(Pipe(0, out), REVIEWED_SCOPE, RELEASE_SHA256)
    except (Refused, ReleaseRefused) as exc:
        print(json.dumps(dict(status=str(exc))), file=sys.stderr)
        return 2
    finally:
        if out is not None:
            os.close(out)
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
