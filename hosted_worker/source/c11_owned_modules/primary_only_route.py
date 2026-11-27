"""Use the existing entry primary without granting entry or making RPD optional."""
from dataclasses import dataclass
import re
from native_cold_minute_page import Q,M,PAGE

NW_WARM=260

def next_page(end,missing):
    if type(end) is not int or end%Q:return None
    if not missing or any(type(k) is not int or k%Q or not end-NW_WARM*Q<=k<end for k in missing):
        return None
    keys=set(missing)
    start=min(keys);stop=start+Q
    while stop in keys and stop-start<PAGE:stop+=Q
    return start,stop

def all_missing_pages(end,missing):
    """Partition only currently missing quarters, retaining the single-page bound."""
    left=set(missing)
    while left:
        page=next_page(end,left)
        if page is None:return
        yield page
        left.difference_update(range(*page,Q))

def outcome(status):
    if status=='NO_SIGNAL':return 'PRIMARY_NO_SIGNAL'
    if status=='NW15_PRIMARY_SIGNAL':return 'REQUIRES_ORIGINAL_RPD'
    return 'UNKNOWN'

@dataclass(frozen=True)
class PrimaryMinuteJob:
    symbol:str
    generation:int
    end:int
    expires:int
    start:int
    stop:int
    ticket:str
    @property
    def n(self):return (self.stop-self.start)//M
    @property
    def weight(self):return 2 if self.n>=100 else 1
    def valid(self,now):
        return (type(now) is int and self.end<=now<self.expires<=self.end+M
            and self.end%Q==self.start%Q==self.stop%Q==0
            and self.end-NW_WARM*Q<=self.start<self.stop<=self.end
            and self.stop-self.start<=PAGE and self.generation>0
            and type(self.symbol) is str and self.symbol.endswith('USDT')
            and type(self.ticket) is str and bool(re.fullmatch('[0-9a-f]{32}',self.ticket)))
    def child(self):
        return dict(symbol=self.symbol,start=self.start,stop=self.stop,count=self.n,width=M)
