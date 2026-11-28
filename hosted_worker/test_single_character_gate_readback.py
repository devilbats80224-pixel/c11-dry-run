"""The production gate table must include valid one-character base symbols."""
import json
from diagnostic_schema import accepted_symbol_event
from first_gate import FirstGate

end = 1_795_869_900_000
rows = []
def sink(b):
    event = json.loads(b)
    if event["schema"] == "c11-gate-readback-v1" and accepted_symbol_event(event):
        rows.append(event)
        return True
    return False

gate = FirstGate(sink)
gate.start(end + 2)
for symbol in ("OUSDT", "ABUSDT", "牛来USDT", "USDT", "O/USDT", "x-USDT"):
    gate.watch(end + 10, 6645, symbol, end + 60_000)
    gate.note(6645, symbol, "callback", "ORIGINAL_NATIVE_CALLBACK")
gate.start(end + 60_000)
got = {row["symbol"]: row["original_callbacks"] for row in rows}
assert got == {"OUSDT":1, "ABUSDT":1, "牛来USDT":1}, f"C11_SINGLE_CHAR_DIAGNOSTIC_GATE_MISMATCH actual={got!r}"
print("C11_SINGLE_CHAR_DIAGNOSTIC_GATE_PASS")
