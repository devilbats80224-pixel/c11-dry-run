"""Validate the combined shared-wire rejection to bounded fallback linkage."""
from time import monotonic
from types import SimpleNamespace as N

from admission_minute_job import MinuteHistoryJob
from causal_bars import MINUTE,QUARTER
from parallel_history_pages import PendingHistoryPages
from shared_history_forwarder import SharedHistoryForwarder


def verify(status,wire_ms,expected,*,allowed=True,grant=True):
    close=1795773600000
    now=close+24
    opened=close-3*QUARTER+MINUTE
    fact=N(symbol='FALLBACKUSDT',pair='FALLBACK/USDT:USDT',origin=1577836800000,
           boundary=close,generation=9)
    job=MinuteHistoryJob(fact.symbol,fact.generation,close,fact.origin,
                        'a'*32,opened,opened+MINUTE,bootstrap=True)
    calls=[]
    budget=N(release_if_never_sent=lambda _:None,optional_wire_fault=lambda *_:None)
    wire=SharedHistoryForwarder(budget,lambda:now,start=lambda:None)
    lane=wire.lane(lambda _:allowed)
    tag='b'*32
    lane.tag=tag
    wire.waiting[tag]=[lane,job,object(),monotonic()+10,now-12 if grant else None]
    book=PendingHistoryPages(lambda _:lane,lambda *_:allowed,
              parallel_ranges=True,on_unreadable=lambda j,f:calls.append((j,f)))
    book.pending[(job.symbol,job.start,job.stop)]=(job,fact,lane)
    stamp=now-7 if wire_ms else None
    wire.inbound(dict(tag=tag,status=status,used=20,rows=[],wire_ms=stamp,received=now))
    book.drain(lambda *_:None)
    assert calls==([(job,fact)] if expected else []),(
        'C11_ARCHIVE_FALLBACK_WRONG_WIRE_TRIGGER',status,wire_ms,allowed,grant,len(calls))
    wire.close()
    book.close()


if __name__=='__main__':
    verify(200,True,True)  # actual grant + valid timestamp + short body
    verify(503,True,True)
    verify(429,True,False)
    verify(418,True,False)
    verify(503,False,False)
    verify(200,True,False,allowed=False)
    verify(200,True,False,grant=False)
    print('C11_FINAL_BOUNDED_ARCHIVE_FALLBACK_REQUIRES_REAL_WIRE_EVIDENCE')
