"""Dedicated Linux entrypoint. Managed approval precedes exchange or state I/O."""
import json
import os
import sys

from bridge_protocol import Pipe, Refused, need
from managed_grant import runtime_grant


def main():
    try:
        need(len(sys.argv) == 1 and not any(k.startswith('FREQTRADE__') for k in os.environ),
             'C11_CONFIG_OVERRIDE_REFUSED')
        grant = runtime_grant()
        from standalone_policy import require_release_facts
        require_release_facts()
        from paper_runtime import PaperState
        from entry_feed import EntryFeed
        from observer_rpc import ObserverClient
        from native_worker import create_worker
        # The native logs cannot go through the inherited source frame stream.
        wire_out = os.dup(1)
        os.dup2(2, 1)
        os.umask(0o077)
        try:
            with PaperState() as state:
                from native_worker import ROOT
                from paper_runtime import verify_state_file
                from pathlib import Path
                for name in ('hard-stop.sqlite', 'intents.sqlite', 'paper-evidence.sqlite', 'causal-candles.sqlite','observer-native.sqlite'):
                    for suffix in ('', '-wal', '-shm', '-journal'):
                        verify_state_file(Path(ROOT / (name + suffix)))
                pipe = Pipe(0,wire_out)
                worker = create_worker(grant,EntryFeed(pipe,grant.generation_floor),
                                       ObserverClient(pipe,grant.observation_binding))
                try:
                    state.check_locked()
                    worker.run()
                finally:
                    worker.exit()
        finally:
            os.close(wire_out)
    except Refused as error:
        print(json.dumps(dict(status=str(error), dry_run_started=False)), file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
