"""Scoped failure-fast mode on existing owner observer connections, not a clone."""
from contextlib import contextmanager
import sqlite3

@contextmanager
def no_sqlite_busy(*connections):
    restored=[]
    try:
        for connection in connections:
            if connection.in_transaction:
                raise RuntimeError('OBSERVER_NOT_IDLE')
            timeout=connection.execute('PRAGMA busy_timeout').fetchone()[0]
            connection.execute('PRAGMA busy_timeout=0')
            restored.append((connection,timeout))
        yield
    finally:
        for connection,timeout in reversed(restored):
            assert type(timeout) is int and timeout>=0
            connection.execute(f'PRAGMA busy_timeout={timeout}')

def optional_observation(capture, connections, on_busy=None):
    try:
        with no_sqlite_busy(*connections):return capture()
    except sqlite3.OperationalError as exc:
        code=getattr(exc,'sqlite_errorcode',None)
        if type(code) is int and (code&255) in (sqlite3.SQLITE_BUSY,sqlite3.SQLITE_LOCKED):
            if on_busy is not None:on_busy(code&255)
            return 'HOLD_OBSERVER_BUSY'
        raise
