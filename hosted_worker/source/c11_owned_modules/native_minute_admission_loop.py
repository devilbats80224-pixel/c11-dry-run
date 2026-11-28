"""Data-only work for current candidates, driven by original worker cycles."""
from collections import deque
from secrets import token_hex
from threading import get_ident
from admission_minute_job import MinuteHistoryJob,iter_jobs,iter_bootstrap_jobs,MAX_ANALYZER_QUARTERS,repairable_checkpoint
from cold_minute_commit import commit,commit_bootstrap,restore_retained_derivatives
from native_cold_minute_page import F,Q,M,bootstrap_bounds
from parallel_history_pages import PendingHistoryPages
from rpd_checkpoint import RpdState
from bridge_protocol import Refused
from bounded_archive_fallback import BoundedArchiveFallback

class MinuteAdmissionLoop:
    def __init__(self,owner,make_forwarder):
        self.owner=owner
        self.thread=get_ident()
        self.pending=PendingHistoryPages(make_forwarder,self.authority,parallel_ranges=True,
                                         receipt_authority=self.receipt_authority,on_unreadable=self._archive_after_wire)
        self.granted={}
        self.reason={}
        self._cursor=None
        self._archive_attempts={}
        self.archive_fallback=BoundedArchiveFallback(
            lambda:self.owner.run.wall(),qualified=self._archive_qualified,
            attempted=self._archive_attempted,commit_raw=self._archive_raw_commit)

    @staticmethod
    def _archive_key(f):
        return f.symbol,f.pair,f.origin,f.generation,f.boundary

    def _archive_qualified(self,scope):
        return (get_ident()==self.thread and self.owner.tap.current(scope)
                and self.blank_bootstrap(scope) and self.bootstrap_contract(scope))

    def _archive_attempted(self,scope):
        done=self._archive_attempts.get(self._archive_key(scope),())
        return all(any(left<=a<b<=right for left,right in done) for a,b in scope.spans)

    def _archive_after_wire(self,j,f):
        if j.bootstrap is not True or not self._bootstrap_authority(j,f) or not self.owner.tap.current(f):
            return
        active=self._archive_attempts.setdefault(self._archive_key(f),set())
        active.add((j.start,j.stop))
        self.archive_fallback.request(f,((j.start,j.stop),))

    def _archive_raw_commit(self,scope,receipt):
        if not (self.phase() and self._archive_qualified(scope) and self._archive_attempted(scope)):
            return False
        o=self.owner
        o.check_store()
        store=o.store
        if store.db.in_transaction:
            return False
        try:
            with store.db:
                store.db.execute('BEGIN IMMEDIATE')
                if not (self._archive_qualified(scope) and self._archive_attempted(scope)):
                    return False
                store._record_checked_rows(scope.symbol,M,receipt.rows)
                if not (self._archive_qualified(scope) and self._archive_attempted(scope)):
                    raise ValueError('C11_ARCHIVE_RAW_SCOPE_CHANGED')
        except (Refused,ValueError):
            return False
        return True

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

    def _unusable_bootstrap_prior(self,f):
        from cold_prefix import unusable_retained_prior
        from stale_state_install import snapshot
        o=self.owner
        # Incomplete legacy test/observer stubs and entirely blank live state
        # cannot produce an old-checkpoint permit.
        if o.store.segment(f.symbol) is None:
            return None
        prior=snapshot(o.store.db,f.symbol)
        return prior if unusable_retained_prior(o.store.db,prior,f,o.run.wall()) else None

    def blank_bootstrap(self,f):
        o=self.owner
        allows=getattr(getattr(o,'cold_prefix',None),'allows_unseeded_minutes',None)
        return (callable(allows) and allows(f) is True
                and ((o.store.segment(f.symbol) is None
                      and not o.store.db.execute('SELECT 1 FROM rpd_checkpoint WHERE symbol=?',(f.symbol,)).fetchone())
                     or self._unusable_bootstrap_prior(f) is not None))

    def bootstrap_contract(self,f):
        from native_candles import NativeCandles
        o=self.owner
        try:
            bot=o.worker.freqtrade
            if bot.config.get('dry_run') is not True or not bot.native_paper_exchange():
                return False
            c=NativeCandles.market(bot,f.pair,f.symbol)['info']
            return (c['symbol']==f.symbol and c['status']=='TRADING' and c['contractType']=='PERPETUAL'
                    and c['quoteAsset']==c['marginAsset']=='USDT'
                    and type(c['onboardDate']) is int and c['onboardDate']==f.origin)
        except (AttributeError,KeyError,Refused,TypeError,ValueError):
            return False

    def _checkpoint_authority(self,j,f,as_of):
        if (j.symbol,j.generation,j.end)!=(f.symbol,f.generation,f.boundary):
            return False
        # Fact.origin remains the original exchange listing and rank witness;
        # the data page is fenced to the authenticated recurrence start.
        proof=self.owner.store.contract_segment(f.symbol,f.origin,as_of)
        return proof is not None and proof.open_ms==j.origin

    def _bootstrap_authority(self,j,f):
        return ((j.symbol,j.origin,j.generation,j.end)==(f.symbol,f.origin,f.generation,f.boundary)
                and self.blank_bootstrap(f) and self.bootstrap_contract(f))

    def authority(self,j,f):
        now=self.owner.run.wall()
        return (self.phase() and j.valid(now) and self.owner.tap.current(f)
                and (self._bootstrap_authority(j,f) if j.bootstrap else self._checkpoint_authority(j,f,now)))

    def receipt_authority(self,j,f,sent):
        o=self.owner
        if (get_ident()!=self.thread or getattr(o.run,'closed',False)
                or type(j) is not MinuteHistoryJob or not j.valid(sent) or sent>o.run.wall()):
            return False
        if j.bootstrap:
            # GO was granted for this exact fact while current. Accept the
            # completed data even after a roster change, not after the
            # exchange contract identity changed or state was installed.
            return self._bootstrap_authority(j,f)
        # No rank refresh or strategy authorization here. Only the existing
        # canonical, same-contract checkpoint may receive this public data.
        return self._checkpoint_authority(j,f,sent)

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
                    status=status or ('WAITING_FOR_EXACT_BOOTSTRAP_MINUTES' if self.blank_bootstrap(f)
                                      else lane.offer(f))
                elif self.blank_bootstrap(f):
                    prior=self._unusable_bootstrap_prior(f)
                    retained=getattr(lane,'offer_retained',None)
                    status=(retained(o,f,prior=prior) if prior is not None and callable(retained) else None)
                    status=status or 'WAITING_FOR_EXACT_BOOTSTRAP_MINUTES'
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
        self.reason={}
        self.pending.drain(self.complete)
        if not self.phase():return
        current_end=self.owner.run.wall()//Q*Q
        self._archive_attempts={key:v for key,v in self._archive_attempts.items() if key[-1]==current_end}
        for scope,status,_ in self.archive_fallback.drain():
            self.reason[scope.symbol]=status
        self.flush_granted()
        if not self.phase():return
        o=self.owner; o.check_store()
        ordered=sorted(facts,key=lambda f:(self._cursor is not None and f.symbol<=self._cursor,f.symbol))
        ready=deque()
        for f in ordered:
            if not self.phase():return
            if skip(f.symbol) or not o.tap.current(f):continue
            if not has_headroom():return
            try:
                cursor=o.store.segment(f.symbol)
                now=o.run.wall()
                stored=o.store.db.execute('SELECT payload FROM rpd_checkpoint WHERE symbol=?',(f.symbol,)).fetchone()
                use_bootstrap=self.blank_bootstrap(f)
                if (cursor is None and not stored) or use_bootstrap:
                    prepare=getattr(o,'cold_prefix',None)
                    if use_bootstrap:
                        bounds=bootstrap_bounds(f.origin,f.boundary)
                        if bounds is None:continue
                        left,cutoff=bounds
                        have=tuple(r[0] for r in o.store.db.execute(
                            'SELECT open_ms FROM candle WHERE symbol=? AND width=? AND open_ms>=? '
                            'AND open_ms<? AND received_ms<=?',(f.symbol,M,left,cutoff,now)))
                        waiting=tuple((a.start,a.stop) for a,_,_ in self.granted.values() if a.symbol==f.symbol)
                        archive=tuple(span for scope,_,_ in self.archive_fallback.pending.values()
                                      if scope.symbol==f.symbol for span in scope.spans)
                        blocked=(*self.pending.inflight_ranges(f.symbol),*occupied_ranges(f.symbol),*waiting,*archive)
                        ready.append((f,iter_bootstrap_jobs(f,have,now,blocked,lambda:token_hex(16))))
                        self.reason[f.symbol]='WAITING_FOR_EXACT_BOOTSTRAP_MINUTES'
                    else:
                        self.reason[f.symbol]=(prepare.offer(f) if prepare is not None
                                               else 'COLD_PREFIX_NO_REVIEWED_SOURCE')
                    primary=getattr(o,'primary_admission',None)
                    if primary is not None:
                        primary.gate(self.reason[f.symbol],f)
                    continue
                cursor=o.store.contract_segment(f.symbol,f.origin,now)
                if cursor is None:
                    self.reason[f.symbol]='NO_AUTHENTICATED_LOCAL_STATE'
                    continue
                if not stored:
                    self.reason[f.symbol]=self.offer_reviewed_repair(f)
                    continue
                rpd=RpdState.restore(stored[0])
                if (rpd.symbol,rpd.origin)!=(f.symbol,cursor.open_ms):
                    self.reason[f.symbol]='RPD_CHECKPOINT_IDENTITY_MISMATCH'
                    continue
                if rpd.observed>now:
                    self.reason[f.symbol]='RPD_OBSERVATION_AFTER_CYCLE_CLOCK'
                    continue
                floor,cutoff=f.boundary-MAX_ANALYZER_QUARTERS*Q,f.boundary-Q
                if not repairable_checkpoint(f,rpd.next_open,causal_origin=cursor.open_ms):
                    self.reason[f.symbol]=self.offer_reviewed_repair(f)
                    continue
                rows=o.store.db.execute('SELECT open_ms FROM candle WHERE symbol=? AND width=? AND open_ms>=? AND open_ms<? AND received_ms<=?',(f.symbol,Q,floor,cutoff,now))
                have={r[0] for r in rows}
                missing=[t for t in range(floor,cutoff,Q) if t>=cursor.open_ms and t not in have]
                waiting=tuple((a.start,a.stop) for a,_,_ in self.granted.values() if a.symbol==f.symbol)
                blocked=(*self.pending.inflight_ranges(f.symbol),*occupied_ranges(f.symbol),*waiting)
                planned=iter_jobs(f,rpd.next_open,missing,now,blocked,lambda:token_hex(16),
                                  causal_origin=cursor.open_ms)
                ready.append((f,planned))
            except (Refused,ValueError):
                self.reason[f.symbol]='NATIVE_PREPARATION_REFUSED'
        # One round per candidate, then cycle again while the same shared
        # authority, live budget and host headroom actually permit new work.
        while ready and self.phase() and has_headroom():
            f,planned=ready.popleft()
            if skip(f.symbol) or not o.tap.current(f):continue
            j=next(planned,None)
            if j is None:continue
            at=o.run.wall()
            try:
                local=not j.bootstrap and restore_retained_derivatives(
                    o.store,j.symbol,j.origin,j.start,j.stop,at,lambda:self.authority(j,f))
                if local:
                    self.reason[f.symbol]='VERIFIED_LOCAL_INTERVAL_REUSED'
                    self.advance_verified_state(j,f)
                    ready.append((f,planned))
                    continue
            except (Refused,ValueError):
                self.reason[f.symbol]='NATIVE_PREPARATION_REFUSED'
                continue
            if self.pending.offer(j,f):
                self.reason[f.symbol]=('SCHEDULED_BOOTSTRAP_MINUTES' if j.bootstrap else 'SCHEDULED_NATIVE_MINUTES')
                self._cursor=f.symbol
                ready.append((f,planned))
            else:self.reason[f.symbol]='NATIVE_BUDGET_OR_AUTHORITY_HELD'

    def advance_verified_state(self,j,f):
        o=self.owner
        due=lambda:self.authority(j,f)
        if not due():return
        target=j.end-Q
        o.store.rpd_progress(j.symbol,target,o.run.wall(),authority=due)
        atr=getattr(o,'atr',None)
        proof=o.store.segment(j.symbol) if atr is not None and due() else None
        if proof is not None and proof.open_ms==j.origin:
            for width in (F,Q):
                if not due():break
                try:
                    atr.extend(j.symbol,width,proof,target,o.run.wall(),authority=due)
                except (Refused,ValueError):
                    self.reason[j.symbol]='NATIVE_ATR_CONTINUATION_REFUSED'

    def complete(self,j,f,data):
        sent,received,_=data
        if (type(sent) is type(received) is int and sent<=received<=self.owner.run.wall()
                and self.receipt_authority(j,f,sent)):
            self.granted[(j.symbol,j.start,j.stop)]=(j,f,data)

    def flush_granted(self):
        o=self.owner
        for key,(j,f,(sent,received,rows)) in tuple(self.granted.items()):
            if not self.phase():
                return
            due=lambda:self.phase() and self.receipt_authority(j,f,sent)
            if not due():
                self.granted.pop(key)
                continue
            try:
                o.check_store()
                if j.bootstrap:
                    commit_bootstrap(o.store,j,rows,sent,received,due,
                                     prior_is_unusable=lambda:self._unusable_bootstrap_prior(f) is not None)
                else:
                    commit(o.store,j.symbol,j.origin,j.start,j.stop,rows,sent,received,due)
            except (Refused,ValueError):
                self.reason[j.symbol]='NATIVE_MINUTES_REFUSED'
                self.granted.pop(key)
                continue
            # SQLite busy propagates to the existing owner handler. Leave
            # the exact accepted payload pending so its next cycle can commit.
            self.granted.pop(key)
            try:
                if not j.bootstrap:self.advance_verified_state(j,f)
            except (Refused,ValueError):
                self.reason[j.symbol]='NATIVE_MINUTES_REFUSED'

    def close(self):
        self.archive_fallback.close()
        self._archive_attempts.clear()
        self.granted.clear()
        self.pending.close()
