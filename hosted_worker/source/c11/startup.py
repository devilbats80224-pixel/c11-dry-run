"""C11-only entrypoint. A managed startup approval is required before any IO."""
from dataclasses import dataclass
import hashlib
import json
import os
import stat
from pathlib import Path
import time

from paper_runtime import PaperState, RUNTIME_IMAGE, dry_config, verify_native_exchange
from source_gate import CollectorIdentity, OriginalCollector, StartupBlocked, digest, require

STATE_VOLUME = 'ewo-c11-paperstate-v1'
RUNNER = Path('/opt/c11-released/c11_freqtrade_runner.py')


@dataclass(frozen=True)
class ApprovedStartup:
    """Return type reserved for a separately installed trusted startup verifier."""
    collector: CollectorIdentity
    runtime_image: str
    runner_sha256: str
    state_volume: str
    expires_monotonic: float


def managed_approval():
    # There is no request-controlled path, environment seal, local issuer,
    # mock signer, or alternate route to construct an approval from the CLI.
    raise StartupBlocked('C11_MANAGED_STARTUP_VERIFIER_UNAVAILABLE')


def prepare_startup(approval):
    """Check a verified handoff, then return held-state-independent diagnostics only."""
    require(type(approval) is ApprovedStartup and approval.runtime_image == RUNTIME_IMAGE
            and approval.state_volume == STATE_VOLUME and digest(approval.runner_sha256)
            and type(approval.expires_monotonic) in (int, float)
            and 0 < approval.expires_monotonic - time.monotonic() <= 30,
            'C11_APPROVED_RUNTIME_REQUIRED')
    collector = OriginalCollector(approval.collector)
    observation = collector.read()
    require(time.monotonic() < approval.expires_monotonic, 'C11_STARTUP_APPROVAL_EXPIRED')
    # Check the already reviewed runtime without importing or executing strategy code.
    try:
        fd = os.open(RUNNER, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as runner:
            info = os.fstat(runner.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and info.st_nlink == 1
                    and info.st_mode & 0o222 == 0 and 0 < info.st_size <= 1024 * 1024,
                    'C11_RUNTIME_RUNNER_IDENTITY')
            runner_bytes = runner.read(1024 * 1024 + 1)
            after = os.fstat(runner.fileno())
            require(info == after and len(runner_bytes) == info.st_size, 'C11_RUNTIME_RUNNER_CHANGED')
    except OSError as error:
        raise StartupBlocked('C11_RELEASED_NATIVE_RUNNER_UNAVAILABLE') from error
    require(hashlib.sha256(runner_bytes).hexdigest() == approval.runner_sha256, 'C11_RUNTIME_RUNNER_PIN')
    native = verify_native_exchange()
    require(time.monotonic() < min(observation.expires_monotonic, approval.expires_monotonic),
            'C11_STARTUP_FRESHNESS_EXPIRED')
    return dict(observation=observation, native=native, config=dry_config(), collector=collector)


def main():
    try:
        # Keep approval, source authentication, native config, and the private
        # mount in this order. A blocked startup must leave the trade DB alone.
        approval = managed_approval()
        prepare_startup(approval)
        with PaperState():
            # Owner-owned integration point: the approved C11 strategy and
            # stop reconciliation runner must retain this lease for its lifetime.
            raise StartupBlocked('C11_RELEASED_RUNNER_DISPATCH_UNAVAILABLE')
    except StartupBlocked as error:
        print(json.dumps(dict(status=str(error), dry_run_started=False, orders_submitted=0)))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
