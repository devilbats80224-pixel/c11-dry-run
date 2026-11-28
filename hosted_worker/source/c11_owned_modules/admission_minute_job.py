"""Data-only 1m jobs for a current candidate, not entry authorization."""
from dataclasses import dataclass
from native_cold_minute_page import PAGE,Q,M,resolve_range,missing_intervals,bootstrap_bounds,bootstrap_missing
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
    bootstrap:bool=False
    @property
    def n(self):return (self.stop-self.start)//M
    @property
    def weight(self):return 2 if self.n>=100 else 1
    def valid(self,now):
        if (type(self.bootstrap) is not bool or type(now) is not int
                or any(type(x) is not int for x in (self.end,self.origin,self.start,self.stop,self.generation))
                or not self.end<=now<self.end+Q
                or self.end%Q or not 0<self.origin<=self.start<self.stop<=self.end-Q
                or self.stop-self.start>PAGE
                or self.generation<=0 or type(self.symbol) is not str or not self.symbol.endswith('USDT')
                or type(self.ticket) is not str or not re.fullmatch('[0-9a-f]{32}',self.ticket)):
            return False
        if self.bootstrap:
            bounds=bootstrap_bounds(self.origin,self.end)
            return bounds is not None and bounds[0]<=self.start<self.stop<=bounds[1] and self.start%M==self.stop%M==0
        return self.start%Q==self.stop%Q==0

    def child(self):
        page=dict(symbol=self.symbol,start=self.start,stop=self.stop,count=self.n,width=M)
        return dict(page,bootstrap=True) if self.bootstrap else page

def make_job(fact,verified_cursor,analysis_gaps,now,ticket):
    if not repairable_checkpoint(fact,verified_cursor):
        return None
    page=resolve_range(fact.boundary,verified_cursor,analysis_gaps)
    if page is None:return None
    j=MinuteHistoryJob(fact.symbol,fact.generation,fact.boundary,fact.origin,ticket,page.start,page.stop)
    return j if j.valid(now) else None

def iter_jobs(fact,verified_cursor,analysis_gaps,now,blocked,next_ticket):
    if not repairable_checkpoint(fact,verified_cursor):
        return
    for page in missing_intervals(fact.boundary,verified_cursor,analysis_gaps,blocked):
        j=MinuteHistoryJob(fact.symbol,fact.generation,fact.boundary,fact.origin,
                           next_ticket(),page.start,page.stop)
        if j.valid(now):
            yield j


def iter_bootstrap_jobs(fact,available_opens,now,blocked,next_ticket):
    for page in bootstrap_missing(fact.origin,fact.boundary,available_opens,blocked):
        j=MinuteHistoryJob(fact.symbol,fact.generation,fact.boundary,fact.origin,
                           next_ticket(),page.start,page.stop,bootstrap=True)
        if j.valid(now):
            yield j
