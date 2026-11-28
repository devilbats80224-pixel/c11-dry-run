"""Owner-thread parallel transport book. Native forwarders share one rate meter."""
from threading import get_ident

class PendingHistoryPages:
    def __init__(self, make, authority, *, parallel_ranges=False, audit=None):
        self.make, self.authority = make, authority
        self.owner = get_ident()
        self.parallel_ranges = parallel_ranges
        self.audit = audit
        self.pending = {}
        self.closed = False

    def _check_owner(self):
        if get_ident() != self.owner:
            raise RuntimeError('C11_HISTORY_WRONG_OWNER')

    def busy(self, symbol):
        if not self.parallel_ranges:
            return symbol in self.pending
        return any(key[0]==symbol for key in self.pending)

    def inflight_ranges(self, symbol):
        """Only suppress the exact ranges already submitted for this symbol."""
        self._check_owner()
        return tuple((job.start,job.stop) for job,_,_ in self.pending.values()
                     if job.symbol==symbol)

    def offer(self, job, fact):
        self._check_owner()
        symbol=fact.symbol
        key=(symbol,job.start,job.stop) if self.parallel_ranges else symbol
        if self.closed or job.symbol != symbol or key in self.pending:
            if self.audit is not None:
                self.audit(job,'SCHEDULE_REJECTED','PENDING_BOOK')
            return False
        if not self.authority(job, fact):
            if self.audit is not None:
                self.audit(job,'SCHEDULE_REJECTED','PENDING_AUTHORITY')
            return False
        lane = self.make(lambda page: page is job and self.authority(job, fact))
        kept = False
        try:
            if not lane.offer(job):
                return False
            self.pending[key] = (job, fact, lane)
            kept = True
            return True
        finally:
            if not kept:
                lane.close()

    def drain(self, complete):
        self._check_owner()
        for key, (job, fact, lane) in tuple(self.pending.items()):
            result = lane.tick()
            if result is None:
                continue
            reply_job, payload = result
            del self.pending[key]
            lane.close()
            if reply_job is job and payload is not None:
                if self.authority(job, fact):
                    complete(job, fact, payload)
                elif self.audit is not None:
                    self.audit(job,'PERSIST_REJECTED','DELIVERY_SCOPE_LOST')

    def close(self):
        self._check_owner()
        self.closed = True
        for _, _, lane in self.pending.values():
            lane.close()
        self.pending.clear()
