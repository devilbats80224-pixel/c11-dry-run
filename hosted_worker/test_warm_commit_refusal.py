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
scope = SimpleNamespace(generation=1, symbol=SYMBOL)
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
