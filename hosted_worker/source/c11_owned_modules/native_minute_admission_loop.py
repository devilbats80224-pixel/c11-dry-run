"""Data-only work for current candidates, driven by original worker cycles."""
from secrets import token_hex
from threading import get_ident
from admission_minute_job import make_job,MAX_ANALYZER_QUARTERS,repairable_checkpoint
from cold_minute_commit import commit
from native_cold_minute_page import Q
from parallel_history_pages import PendingHistoryPages
from rpd_checkpoint import RpdState
from bridge_protocol import Refused

class MinuteAdmissionLoop:
    def __init__(self,owner,make_forwarder):
        self.owner=owner
        self.thread=get_ident()
        self.pending=PendingHistoryPages(make_forwarder,self.authority)
        self.reason={}
        self._cursor=None

    def cycle_headroom(self):
        o=self.owner
        due=getattr(o.worker.freqtrade,'_owned_original_cycle_deadline_ns',None)
        import time
        # Bound against the original protection deadline, not a fixed 65s
        # delay. The entry lease itself has not been extended.
        return (get_ident()==self.thread and type(due) is int
                and due-time.monotonic_ns()>100_000_000)

    def phase(self):
        return self.cycle_headroom() and not self.owner.run.queue.inflight

    def authority(self,j,f):
        return self.phase() and j.valid(self.owner.run.wall()) and self.owner.tap.current(f)

    def offer_reviewed_repair(self,f):
        prepare=getattr(self.owner,'cold_prefix',None)
        repair=getattr(prepare,'offer_existing',None)
        result=repair(self.owner,f) if callable(repair) else 'NO_VERIFIED_LOCAL_REPAIR_SOURCE'
        primary=getattr(self.owner,'primary_admission',None)
        if primary is not None:
            primary.gate(result,f)
        return result

    def enroll_cold(self,facts):
        """Prepare absent or stale state without inheriting a primary probe queue."""
        o=self.owner
        lane=o.cold_prefix
        if not o.cold_delivery_window():return
        o.check_store()
        for f in facts:
            if not o.cold_delivery_window():return
            if not o.tap.current(f):continue
            try:
                segment=o.store.segment(f.symbol)
                row=o.store.db.execute('SELECT payload FROM rpd_checkpoint WHERE symbol=?',(f.symbol,)).fetchone()
                if segment is None and row is None:
                    retained=getattr(lane,'offer_retained',None)
                    status=retained(o,f) if callable(retained) else None
                    status=status or lane.offer(f)
                elif segment is not None and segment.open_ms==f.origin and (row is None or
                        not repairable_checkpoint(f,RpdState.restore(row[0]).next_open)):
                    status=self.offer_reviewed_repair(f)
                else:continue
                primary=getattr(o,'primary_admission',None)
                if primary is not None:primary.gate(status,f)
            except (Refused,ValueError):
                continue

    def tick(self,facts,*,skip=lambda _:False,has_headroom=lambda:True,occupied_ranges=lambda _:()):
        if get_ident()!=self.thread:return
        self.pending.drain(self.complete)
        if not self.phase():return
        o=self.owner; o.check_store()
        ordered=sorted(facts,key=lambda f:(self._cursor is not None and f.symbol<=self._cursor,f.symbol))
        self.reason={}
        for f in ordered:
            if not self.phase():return
            if self.pending.busy(f.symbol) or skip(f.symbol) or not o.tap.current(f):continue
            if not has_headroom():return
            try:
                cursor=o.store.segment(f.symbol)
                now=o.run.wall()
                stored=o.store.db.execute('SELECT payload FROM rpd_checkpoint WHERE symbol=?',(f.symbol,)).fetchone()
                if cursor is None and not stored:
                    prepare=getattr(o,'cold_prefix',None)
                    self.reason[f.symbol]=(prepare.offer(f) if prepare is not None
                                           else 'COLD_PREFIX_NO_REVIEWED_SOURCE')
                    primary=getattr(o,'primary_admission',None)
                    if primary is not None:
                        primary.gate(self.reason[f.symbol],f)
                    continue
                if cursor is None or cursor.open_ms!=f.origin or cursor.proven_at_ms>now:
                    self.reason[f.symbol]='NO_AUTHENTICATED_LOCAL_STATE'
                    continue
                if not stored:
                    self.reason[f.symbol]=self.offer_reviewed_repair(f)
                    continue
                rpd=RpdState.restore(stored[0])
                if (rpd.symbol,rpd.origin)!=(f.symbol,f.origin):
                    self.reason[f.symbol]='RPD_CHECKPOINT_IDENTITY_MISMATCH'
                    continue
                if rpd.observed>now:
                    self.reason[f.symbol]='RPD_OBSERVATION_AFTER_CYCLE_CLOCK'
                    continue
                floor,cutoff=f.boundary-MAX_ANALYZER_QUARTERS*Q,f.boundary-Q
                if not repairable_checkpoint(f,rpd.next_open):
                    self.reason[f.symbol]=self.offer_reviewed_repair(f)
                    continue
                rows=o.store.db.execute('SELECT open_ms FROM candle WHERE symbol=? AND width=? AND open_ms>=? AND open_ms<? AND received_ms<=?',(f.symbol,Q,floor,cutoff,now))
                have={r[0] for r in rows}
                missing=[t for t in range(floor,cutoff,Q) if t>=cursor.open_ms and t not in have]
                j=make_job(f,rpd.next_open,missing,now,token_hex(16))
                if j is None:
                    self.reason[f.symbol]='ALREADY_CURRENT_OR_NO_RECENT_VERIFIED_START'
                    continue
                if any(j.start<stop and start<j.stop for start,stop in occupied_ranges(f.symbol)):
                    self.reason[f.symbol]='OVERLAPPING_SHARED_MINUTE_PAGE'
                    continue
                if self.pending.offer(j,f):
                    self.reason[f.symbol]='SCHEDULED_NATIVE_MINUTES'
                    self._cursor=f.symbol
                else:self.reason[f.symbol]='NATIVE_BUDGET_OR_AUTHORITY_HELD'
            except (Refused,ValueError):
                self.reason[f.symbol]='NATIVE_PREPARATION_REFUSED'

    def complete(self,j,f,data):
        o=self.owner; due=lambda:self.authority(j,f)
        if not due():return
        sent,received,rows=data
        try:
            o.check_store()
            commit(o.store,j.symbol,j.origin,j.start,j.stop,rows,sent,received,due)
            if due():o.store.rpd_progress(j.symbol,j.stop,o.run.wall(),authority=due)
            # The original worker consumes the updated read model through its
            # normal admission on the next local cycle, never a fabricated signal.
        except (Refused,ValueError):
            self.reason[j.symbol]='NATIVE_MINUTES_REFUSED'
    def close(self):self.pending.close()
