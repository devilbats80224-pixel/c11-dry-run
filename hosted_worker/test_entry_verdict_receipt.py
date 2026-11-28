"""Exercise diagnostic-only scoping on the same packaged Python import."""
from contextlib import redirect_stdout
import io
import json
from unittest.mock import patch

from entry_verdict_receipt import ACTIVE, mark_primary, publish

outside = [None]
mark_primary('NO_PRIMARY_SIGNAL')
assert outside == [None]
token = ACTIVE.set(outside)
try:
    mark_primary('RPD5_NOT_LONG_NONNEGATIVE')
    mark_primary('NATIVE_INPUT_UNAVAILABLE')
    assert outside == ['RPD5_NOT_LONG_NONNEGATIVE']
    mark_primary('producer text must not become a reason', force=True)
    assert outside == ['RPD5_NOT_LONG_NONNEGATIVE']
    mark_primary('CLOSED5_REJECTED_BREAK', force=True)
    assert outside == ['CLOSED5_REJECTED_BREAK']
finally:
    ACTIVE.reset(token)
assert ACTIVE.get() is None
out = io.StringIO()
with redirect_stdout(out):
    publish('牛来/USDT:USDT', 900_000, 'primary', outside[0])
    publish('ABC/USDT:USDT', 900_000, 'order', 'NATIVE_ORDER_SENT')
    publish('ABC/USDT:USDT', 900_001, 'primary', 'NO_PRIMARY_SIGNAL')
    publish('ABC/USDT:USDT', 900_000, 'primary', 'private value')
    publish('ABC/USDT:USDT', 900_000, 'order', 'NO_PRIMARY_SIGNAL')
    publish('local/file/USDT:USDT', 900_000, 'primary', 'NO_PRIMARY_SIGNAL')
a, b = map(json.loads, out.getvalue().splitlines())
assert a['symbol']=='牛来USDT' and a['reason']=='CLOSED5_REJECTED_BREAK'
assert b['symbol']=='ABCUSDT' and b['reason']=='NATIVE_ORDER_SENT' and b['phase']=='order'
assert set(a)=={'schema','symbol','end_ms','phase','reason','route','observed_ms'}
with patch('builtins.print', side_effect=OSError('ignored sink outage')):
    assert publish('ABC/USDT:USDT', 900_000, 'primary', 'NO_PRIMARY_SIGNAL') is None
print('C11_ENTRY_VERDICT_REASON_SCOPED_AND_PASSIVE')
