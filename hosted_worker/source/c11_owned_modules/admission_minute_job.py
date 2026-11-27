"""Data-only 1m jobs for a current candidate, not entry authorization."""
from dataclasses import dataclass
from native_cold_minute_page import PAGE,Q,M,resolve_range
import re

MAX_ANALYZER_QUARTERS=260
# Checkpoint age alone does not invalidate a compatible native recurrence.
# The owner verifies symbol, contract origin, and observation time separately;
# physical work remains one bounded page per authorized shared-queue job.
def repairable_checkpoint(fact,cursor):
    return (type(cursor) is int and fact.origin<cursor
            and cursor//Q*Q>=fact.origin)

@dataclass(frozen=True)
class MinuteHistoryJob:
    symbol:str
    generation:int
    end:int
    origin:int
    ticket:str
    start:int
    stop:int
    @property
    def n(self):return (self.stop-self.start)//M
    @property
    def weight(self):return 2 if self.n>=100 else 1
    def valid(self,now):
        return (type(now) is int and self.end<=now<self.end+Q
            and self.end%Q==self.start%Q==self.stop%Q==0
            and 0<self.origin<=self.start<self.stop<=self.end-Q
            and self.stop-self.start<=PAGE
            and self.generation>0 and type(self.symbol) is str and self.symbol.endswith('USDT')
            and type(self.ticket) is str and bool(re.fullmatch('[0-9a-f]{32}',self.ticket)))
    def child(self):
        return dict(symbol=self.symbol,start=self.start,stop=self.stop,count=self.n,width=M)

def make_job(fact,verified_cursor,analysis_gaps,now,ticket):
    if not repairable_checkpoint(fact,verified_cursor):
        return None
    page=resolve_range(fact.boundary,verified_cursor,analysis_gaps)
    if page is None:return None
    j=MinuteHistoryJob(fact.symbol,fact.generation,fact.boundary,fact.origin,ticket,page.start,page.stop)
    return j if j.valid(now) else None
