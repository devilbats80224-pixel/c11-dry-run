"""Installed warm path must not turn an atomic refusal into a silent pending state."""
from types import SimpleNamespace
from causal_bars import CausalBars, ClosedBar, MINUTE, FIVE, QUARTER
from warm_release_binding import WarmReleaseBinding

E = 1795824900000
SYMBOL = "SYNTHUSDT"
notes = []
store = CausalBars(":memory:")
sc = SimpleNamespace(adapter=SimpleNamespace(ready=SimpleNamespace(store=store)),
                     first_gate=SimpleNamespace(note=lambda *args: notes.append(args)),
                     wall=lambda: E + 7000)
scope = SimpleNamespace(generation=1, symbol=SYMBOL, end=E)
proof = object()
route = WarmReleaseBinding(sc)
route._current = lambda _scope: True


def bar(width, close):
    return ClosedBar(E-width, E, close, close, close, close, 1., 1.)


# A conflict on an already sealed wider candle must not erase that candle
# or leave the earlier-written 1m rows half committed.
store.record(SYMBOL, QUARTER, (bar(QUARTER, 2.),))
batches = ((MINUTE, (bar(MINUTE, 1.),)), (FIVE, (bar(FIVE, 1.),)),
           (QUARTER, (bar(QUARTER, 1.),)))
route.prepared = (proof, scope, object(), batches, False)
assert route.frame(proof) == ()
assert store.range(SYMBOL, QUARTER, E-QUARTER, E, E)[0].close == 2.
assert not store.range(SYMBOL, MINUTE, E-MINUTE, E, E)
assert not store.range(SYMBOL, FIVE, E-FIVE, E, E)
detail = {(n[2], n[3]) for n in notes}
assert ("atomic_refusal", "ATOMIC_CANONICAL_CONFLICT") in detail, "C11_WARM_ATOMIC_REFUSAL_NOT_RECORDED"
assert ("atomic_phase", "ATOMIC_ROLLBACK_CLOSED_BEFORE_COMMIT") in detail
notes.clear()
route._current = lambda _scope: False
assert route.frame(proof) == ()
assert {n[3] for n in notes} == {"ATOMIC_AUTHORITY_BEFORE_ROWS"}
assert not store.range(SYMBOL, MINUTE, E-MINUTE, E, E)

# Successful exact-store input still commits, with unchanged durable rows.
notes.clear()
route._current = lambda _scope: True
clean = (batches[0], batches[1], (QUARTER, (bar(QUARTER, 2.),)))
route.prepared = (proof, scope, object(), clean, False)
assert route.frame(proof) == clean[-1][1]
assert any(n[2:4]==("atomic_phase", "ATOMIC_COMMIT_RETURNED") for n in notes)
assert not any(n[2]=="atomic_refusal" for n in notes)
assert store.range(SYMBOL, MINUTE, E-MINUTE, E, E) == clean[0][1]
print("C11_WARM_CONFLICT_REDACTED_AND_ATOMIC_ROLLBACK")

# 09:00 Mac shape: already-closed preceding 1m rows exist, but the
# just-closed quarter has no rows yet. Re-request overlaps the prefix and
# carries a differing old sample plus a complete valid current tail.
# This may not replace the old record or discard a whole current quarter.
symbol = "OVERLAPUSDT"
scope = SimpleNamespace(generation=2, symbol=symbol, end=E)
stamp = E + 3000

def span(width, start, end, close):
    return tuple(ClosedBar(t, max(t+width, stamp), close, close, close, close, 1., 1.)
                 for t in range(start, end, width))

old_prefix = span(MINUTE, E-30*MINUTE, E-15*MINUTE, 2.)
store.record(symbol, MINUTE, old_prefix)
incoming = span(MINUTE, E-30*MINUTE, E, 1.)
terminal5 = span(FIVE, E-QUARTER, E, 1.)
terminal15 = span(QUARTER, E-QUARTER, E, 1.)
old5 = span(FIVE,E-30*MINUTE,E-QUARTER,2.)
old15 = span(QUARTER,E-30*MINUTE,E-QUARTER,2.)
store.record(symbol,FIVE,old5)
store.record(symbol,QUARTER,old15)
route.prepared = (proof, scope, object(), ((MINUTE,incoming),(FIVE,old5+terminal5),(QUARTER,old15+terminal15)), False)
assert route.frame(proof) == old15+terminal15, (
    "C11_WARM_HISTORICAL_OVERLAP_BLOCKED_FRESH_QUARTER",
    "expected original prior minutes preserved and complete new quarter committed",
    "actual no original decision frame returned")
assert store.range(symbol, MINUTE,E-30*MINUTE,E-QUARTER,stamp) == old_prefix
assert store.range(symbol, MINUTE,E-QUARTER,E,stamp) == incoming[-15:]
assert store.range(symbol, FIVE,E-QUARTER,E,stamp) == terminal5
assert store.range(symbol, QUARTER,E-QUARTER,E,stamp) == terminal15
print("C11_WARM_HISTORICAL_OVERLAP_PRESERVES_OLD_AND_COMMITS_NEW")


# If a new 5m backfill still depends on the disputed old minute, do not
# hide the conflict or claim a partly-written canonical current frame.
unsafe = "UNBOUNDUSDT"
unsafe_scope=SimpleNamespace(generation=3, symbol=unsafe, end=E)
store.record(unsafe, MINUTE, old_prefix)
new5=span(FIVE,E-30*MINUTE,E-QUARTER,1.)+terminal5
route.prepared=(proof,unsafe_scope,object(),((MINUTE,incoming),(FIVE,new5),(QUARTER,terminal15)),False)
assert route.frame(proof)==(), "C11_WARM_DISPUTED_MINUTE_USED_BY_NEW_FIVE"
assert not store.range(unsafe, MINUTE,E-QUARTER,E,stamp)
assert not store.range(unsafe, FIVE,E-QUARTER,E,stamp)
assert store.range(unsafe, MINUTE,E-30*MINUTE,E-QUARTER,stamp)==old_prefix
