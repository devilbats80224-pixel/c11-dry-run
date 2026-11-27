"""Exercise the actually installed NativeEntryOwner with disposable test data."""
from types import SimpleNamespace
from pandas import DataFrame, to_datetime

from native_entry_owner import NativeEntryOwner

E = 1795823100000
Q = 900_000
PAIR = "SYNTH/USDT:USDT"
clock = E + 7200
frame = DataFrame({"date": to_datetime([E-Q], unit="ms", utc=True),
                   "open": [1.], "high": [1.], "low": [1.], "close": [1.], "volume": [1.]})
proof = SimpleNamespace(pair=PAIR, end=E, expires=E+55_000, generation=1)
phase = {"complete": False}
notes = []


def ready(p, pair, end):
    return p is proof and pair == PAIR and end == E and clock < proof.expires


def prepare(p, f):
    assert p is proof and f is frame
    return ("exact-native-frame",) if phase["complete"] else None


owner = NativeEntryOwner(trusted_owner_input=lambda *_: None, record=lambda *_: None,
                         revisit_ready=ready, revisit_frame=prepare,
                         revisit_frame_current=lambda p: p is proof,
                         revisit_diagnostic=lambda p, why: notes.append((p, why)))
# The buggy implementation ignored this scoped requirement and spent the
# genuine Freqtrade revisit even though staging had produced no frame.
owner.requires_staged_revisit_frame = lambda p: p is proof
owner.holds[PAIR] = proof
strategy = SimpleNamespace(timeframe="15m", config={"dry_run": True, "candle_type_def": "futures"},
                           dp=SimpleNamespace(ohlcv=lambda *_, **__: frame))
assert not owner.one_public_revisit(strategy, PAIR), "C11_REVISIT_SPENT_BEFORE_FRAME_WAS_STAGED"
assert owner.holds.get(PAIR) is proof and (PAIR, E) not in owner.spent
assert notes == [(proof, "REVISIT_STAGED_FRAME_PENDING")]
phase["complete"] = True
assert owner.one_public_revisit(strategy, PAIR) and owner.staged_frame==(proof, ("exact-native-frame",))
assert not owner.one_public_revisit(strategy, PAIR), "C11_REVISIT_ALREADY_RESERVED_DUPLICATED"
print("C11_STAGING_WAIT_PRESERVES_ORIGINAL_SAME_CLOSE_REVISIT")
