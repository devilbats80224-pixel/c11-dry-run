"""Plan a native closed 1m page from a previously authenticated strategy state.

No market or seed authority is created here. The caller provides a verified,
same-symbol compatible cursor and required missing analysis intervals. A last
closed decision quarter remains the native foreground owner's responsibility.
"""
from dataclasses import dataclass

M=60_000
F=5*M
Q=15*M
PAGE=495*M

@dataclass(frozen=True)
class MinuteRange:
    start:int
    stop:int
    @property
    def count(self):
        return (self.stop-self.start)//M
    def request(self,symbol):
        return dict(symbol=symbol,interval='1m',startTime=self.start,
                    endTime=self.stop-1,limit=self.count)

def resolve_range(end,verified_cursor,analysis_gaps):
    if (type(end) is not int or end%Q or type(verified_cursor) is not int
        or verified_cursor<=0 or verified_cursor%F or verified_cursor>end-Q):
        return None
    cutoff=end-Q
    if any(type(x) is not int or x<=0 or x%Q or x>=cutoff for x in analysis_gaps):
        return None
    start=min((verified_cursor//Q*Q,*analysis_gaps))
    if not 0<start<cutoff:
        return None
    return MinuteRange(start,min(cutoff,start+PAGE))


def missing_intervals(end,verified_cursor,analysis_gaps,blocked):
    # Logical coverage is time-based. Split the actual uncovered stretches
    # only at the transport page size, without a fixed request count.
    initial=resolve_range(end,verified_cursor,analysis_gaps)
    if initial is None:
        return
    left,cutoff=initial.start,end-Q
    intervals=sorted((max(left,a),min(cutoff,b)) for a,b in blocked
                     if type(a) is type(b) is int and a%Q==b%Q==0
                     and left<b and a<cutoff and a<b)
    for a,b in (*intervals,(cutoff,cutoff)):
        while left<a:
            stop=min(a,left+PAGE)
            yield MinuteRange(left,stop)
            left=stop
        left=max(left,b)
