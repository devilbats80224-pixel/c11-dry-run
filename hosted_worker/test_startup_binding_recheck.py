"""Model actual pre-consume refusal and later repeatable exact observer proof."""
import sys
sys.path[:0] = ["/opt/c11_worker", "/opt/c11_worker/ports"]
from bridge_protocol import Refused
import v4_entry

REASON = "C11_STARTUP_OBSERVER_BINDING"

def old_proof(verify, still_owner, pause):
    still_owner()
    verify()
    still_owner()

def run(replies):
    faults = iter(replies)
    reads, owners, sleeps = [], [], []
    def check():
        owners.append(True)
    def read():
        reason = next(faults)
        reads.append(reason)
        if reason:
            raise Refused(reason)
    proof = getattr(v4_entry, "prove_readonly_startup_lineage", old_proof)
    try:
        proof(read, check, sleeps.append)
    except Refused as err:
        return reads, owners, sleeps, str(err)
    return reads, owners, sleeps, None

reads, owners, delays, error = run([REASON, None, None])
assert error is None and reads == [REASON, None, None] and len(owners) >= 4, (
    "C11_STARTUP_BINDING_TRANSIENT_DID_NOT_RECOVER",
    "Expected unchanged exact verification to succeed twice under same writer before activation",
    "Actual", reads, error, len(owners))
assert delays and 0 < max(delays) <= 1
reads, owners, delays, error = run([REASON] * 8)
assert error == REASON and 2 <= len(reads) <= 5, ("C11_STARTUP_PERSISTENT_BINDING_NOT_REFUSED", reads, error)
reads, owners, delays, error = run(["C11_STARTUP_LEDGER_REPLACED"])
assert error == "C11_STARTUP_LEDGER_REPLACED" and len(reads) == 1 and not delays
print("C11_STARTUP_BINDING_RECOVERY_WITHOUT_IDENTITY_BYPASS")
