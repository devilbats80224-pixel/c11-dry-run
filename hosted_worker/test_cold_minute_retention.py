"""Historical minute pages retain the exact input or roll back the whole write."""
import sys
sys.path[:0] = ["/opt/c11_owned_modules", "/opt/c11_worker", "/opt/c11_worker/ports",
                "/opt/c11_background_review", "/freqtrade"]
from bridge_protocol import Refused
from causal_bars import CausalBars, ClosedBar, FIVE, MINUTE, QUARTER, SegmentStart
from cold_minute_commit import commit

symbol = "EXAMPLEUSDT"
start = 1795773600000
stop = start + 3*QUARTER
rows = [[t, "100", "104", "99", "101", "10", t+MINUTE-1, "1000", 5, "4", "400", "0"]
        for t in range(start, stop, MINUTE)]

def book():
    db = CausalBars(":memory:")
    db.set_segment(SegmentStart(symbol, start, start+MINUTE))
    return db

db = book()
commit(db, symbol, start, start, stop, rows, stop, stop+20, lambda: True)
assert tuple(map(len, (db.range(symbol, w, start, stop, stop+20)
                       for w in (MINUTE, FIVE, QUARTER)))) == (45, 9, 3), "C11_COLD_MINUTES_NOT_RETAINED"
assert not db.range(symbol, MINUTE, start, stop, stop+19)
assert all(row.close == 101 and row.quote == 1000
           for row in db.range(symbol, MINUTE, start, stop, stop+20))

db = book()
target = start+MINUTE
db.record(symbol, MINUTE, (ClosedBar(target, target+MINUTE,100,104,99,102,10,1000),))
try:
    commit(db, symbol, start, start, stop, rows, stop, stop+20, lambda: True)
except Refused:
    pass
else:
    raise AssertionError("C11_COLD_CONFLICT_NOT_REJECTED")
assert list(db.db.execute("SELECT width,open_ms,close FROM candle ORDER BY open_ms")) == [(MINUTE,target,102)]

db = book()
authority = iter([True,True,False])
try:
    commit(db, symbol, start, start, stop, rows, stop, stop+20, lambda: next(authority))
except ValueError as e:
    assert str(e) == "C11_CLOSED_MINUTE_PAGE_EXPIRED"
else:
    raise AssertionError("C11_COLD_EXPIRY_NOT_REJECTED")
assert db.db.execute("SELECT COUNT(*) FROM candle").fetchone() == (0,)
print("C11_COLD_MINUTE_RETENTION_CONTROLS_PASS")
