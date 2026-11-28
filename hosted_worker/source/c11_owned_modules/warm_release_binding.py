"""Original Worker single-receipt route; candidate, not a release authorization."""
import sqlite3
from committed_projection import AuthoritativePrimaryCommit,CommittedProjection,canonical,frame_sha
from primary_coverage_gate import NativePrimaryHandoff
from canonical_guard import same_native_frame
from no_sqlite_busy import optional_observation
from bridge_protocol import Refused
from native_minute_clock import admitted_closed_clock
from warm_store_prefix import warm_prefix
from previous_five_from_minute import local_previous_five_plan, fill_previous_five
from causal_bars import FIVE,QUARTER,MINUTE

def _durable_terminal_matches(stored, bar, width):
    # record_owner_primary_batch deliberately preserves the first valid receipt
    # for equal candle content. A new native receipt may corroborate that row.
    return (stored is not None and type(stored[0]) is int
            and bar.open_ms+width<=stored[0]<=bar.received_ms
            and stored[1:]==(bar.op,bar.high,bar.low,bar.close,bar.volume,bar.quote))


class WarmReleaseBinding:
    def __init__(self,scheduler):
        self.sc=scheduler
        self.prefix={}
        self.selected=set()
        self.prepared=None
        self.claimed=set()
        self.claim_end=None
        self.refresh=None
        self.reused=set()

    def eligible(self,s,start):
        sc=self.sc
        if self.claim_end!=s.end:
            self.claim_end=s.end
            self.claimed.clear()
            self.reused.clear()
        self.prefix={k:v for k,v in self.prefix.items() if (k.generation,k.end)==(s.generation,s.end)}
        self.selected={k for k in self.selected if (k.generation,k.end)==(s.generation,s.end)}
        if sc.commit_and_decide.wants_multilane(s):
            return False
        bot=sc.worker.freqtrade
        if not (bot.config.get('dry_run') is True and
            getattr(bot,'_owned_revisit_no_positions',False) is True and
            getattr(bot,'_owned_revisit_reconciled',False) is True):
            return False
        store=sc.adapter.ready.store
        x=optional_observation(lambda:warm_prefix(store,s.symbol,start,s.end,sc.wall()),(store.db,))
        if (x is None and self.refresh is not None and s not in self.reused and
            sc.plan.current(s,sc.wall())):
            captured=self.refresh.peek(s.symbol,s.end)
            if captured is not None:
                from native_history_refresh import archive_prefix
                seen,rows=captured
                def refill():
                    missing=archive_prefix(store,s.symbol,s.end,seen,rows)
                    if missing is None or not sc.plan.current(s,sc.wall()):return False
                    additions,conflicts=missing
                    if additions:
                        store.record(s.symbol,QUARTER,additions)
                        sc._count('history_native_reused_rows',len(additions))
                    if conflicts:sc._count('history_native_preserved_rows',len(conflicts))
                    return True
                try:
                    if optional_observation(refill,(store.db,)) is True:self.reused.add(s)
                except (Refused,ValueError):pass
                x=optional_observation(lambda:warm_prefix(store,s.symbol,start,s.end,sc.wall()),(store.db,))
        if type(x) is not tuple or len(x)!=2:
            x=optional_observation(lambda:local_previous_five_plan(store,s.symbol,start,s.end,sc.wall()),(store.db,))
            from previous_five_from_minute import PreviousFivePlan
            if type(x) is not PreviousFivePlan:return False
        self.prefix[s]=x
        self.selected.add(s)
        return True

    def minute_span(self,s):
        from dynamic_minute_span import select_minute_span,DEFAULT
        x=self.prefix[s]
        return DEFAULT if type(x) is tuple else select_minute_span(s.end,x.items)

    def _current(self,s):
        sc=self.sc
        now=sc.wall()
        bot=sc.worker.freqtrade
        return (s in self.selected and sc.marker==(s.generation,s.end)
            and any(owned is s for owned in sc.covers.values())
            and bot.config.get('dry_run') is True
            and getattr(bot,'_owned_revisit_no_positions',False) is True
            and getattr(bot,'_owned_revisit_reconciled',False) is True
            and s.end<=now<min(s.expires,sc.adapter.ready.effective_expiry(s,now))
            and sc.adapter.ready.authority(s,now) is True
            and sc.commit_and_decide.remaining()>sc.adapter.ready.latency_budget)

    def _preview(self,s):
        sc=self.sc
        if not self._current(s):return None
        matches=[r for r in sc.adapter._proven.values() if r.ticket.scope is s and r.ticket.lane=='1m']
        if len(matches)!=1:return None
        row=matches[0]
        prefix=self.prefix[s]
        if type(prefix) is not tuple:
            prefix=fill_previous_five(prefix,s.end,row.raw,row.received_ms)
            if prefix is None:return None
        def finality(scope,minute,record):
            return (scope is s and minute is row and admitted_closed_clock(
                s.end,min(s.expires,sc.adapter.ready.effective_expiry(s,sc.wall())),
                record['request_ms'],row.received_ms,sc.wall(),row.raw))
        values=sc.adapter.normalize_native_minute(s,row,prefix_five=prefix[0],
                            prefix_quarter=prefix[1],finality=finality)
        return (row,values) if values is not None else None

    def revisit(self,first,pair,end):
        sc=self.sc
        self.prepared=None
        s=next((q for q in sc.covers.values() if (q.pair,q.end)==(pair,end) and q in self.selected),None)
        if s is None:return None
        if ((first.pair,first.symbol,first.generation,first.end)!=(s.pair,s.symbol,s.generation,s.end)
             or s.expires>first.expires or not self._current(s)):
            return False
        by_ticket={ticket:key for key,ticket in sc.tickets.items()}
        sc.queue.accept_completed_original_scope(s,
            lambda native:sc._accept_existing_native(native,by_ticket,nowait=True))
        ready=self._preview(s)
        if ready is None:return False
        self.prepared=(first,s,*ready,False)
        return True

    def _atomic_detail(self,s,kind,label):
        sc=self.sc
        first=getattr(sc,'first_gate',None)
        if first is not None:
            first.note(s.generation,s.symbol,kind,label,sc.wall())

    def frame(self,first):
        if self.prepared is None or self.prepared[0] is not first:return None
        sc=self.sc
        a,s,row,batches,committed=self.prepared
        if not self._current(s):
            self._atomic_detail(s,'atomic_refusal','ATOMIC_AUTHORITY_BEFORE_ROWS')
            return ()
        store=sc.adapter.ready.store
        if not committed:
            def write():
                store.owner_atomic_phases=()
                try:
                    store.record_owner_primary_batch(s.symbol,batches,lambda:self._current(s))
                    return True
                finally:
                    for phase in store.owner_atomic_phases:
                        self._atomic_detail(s,'atomic_phase',phase)
            def busy(code):
                self._atomic_detail(s,'atomic_refusal',
                    'ATOMIC_SQLITE_BUSY' if code==sqlite3.SQLITE_BUSY else 'ATOMIC_SQLITE_LOCKED')
            try:
                outcome=optional_observation(write,(store.db,),on_busy=busy)
                if outcome is not True:return ()
            except (Refused,ValueError) as exc:
                reason={
                    'C11_OWNER_BATCH_SHAPE':'ATOMIC_BATCH_SHAPE',
                    'C11_OWNER_BATCH_AUTHORITY':'ATOMIC_AUTHORITY_UNSPECIFIED',
                    'C11_CANDLE_CAUSAL_ROW':'ATOMIC_INVALID_ROW',
                    'C11_CLOSED_CANDLE_CHANGED':'ATOMIC_CANONICAL_CONFLICT',
                }.get(str(exc),'ATOMIC_OTHER_REFUSED')
                self._atomic_detail(s,'atomic_refusal',reason)
                return ()
        if not self._current(s):
            self._atomic_detail(s,'atomic_refusal','ATOMIC_AUTHORITY_AFTER_ROWS')
            return ()
        self.prepared=(a,s,row,batches,True)
        return batches[-1][1]

    def frame_current(self,first):
        p=self.prepared
        return p is not None and p[0] is first and self._current(p[1])

    def publish(self,s,frame,callback):
        sc=self.sc
        p=self.prepared
        if p is None or not p[-1] or p[1] is not s or not self._current(s):
            return 'HOLD_NATIVE_MINUTE_NOT_COMMITTED',None
        key=(s.pair,s.symbol,s.generation,s.end)
        dedupe=(s.pair,s.end)
        bot=sc.worker.freqtrade
        store=sc.adapter.ready.store
        if dedupe in self.claimed or store.db.in_transaction or hasattr(bot,'_c11_native_primary_handoff'):
            return 'HOLD_ALREADY_DRAINED',None
        candidate=sc.plan.accepted_candidate(s)
        if candidate is None or (candidate.pair,candidate.symbol,candidate.generation)!=key[:3]:
            return 'HOLD_NO_ORIGINAL_ACCEPTED_RANK',None
        _,_,receipt,batches,_=p
        now=sc.wall()
        before=frame()
        identity=same_native_frame(store,s,before,now)
        if identity is None:return 'HOLD_ANALYZER_NOT_DURABLE_NATIVE_FRAME',None
        terminal=tuple((w,r) for w,rows in batches for r in rows if r.open_ms>=s.end-QUARTER)
        query='SELECT received_ms,op,high,low,close,volume,quote FROM candle WHERE symbol=? AND width=? AND open_ms=?'
        st=optional_observation(lambda:store.rpd_progress(s.symbol,s.end,sc.wall()),(store.db,))
        if st is None or isinstance(st,str):return 'HOLD_NATIVE_RPD_LINEAGE',None
        origin=store.segment(s.symbol)
        if origin is None or (st.symbol,st.next_open,st.origin)!=(s.symbol,s.end,origin.open_ms):
            return 'HOLD_NATIVE_RPD_LINEAGE',None
        packed=st.pack()
        closed=st.closed()
        if closed.known is not True:return 'HOLD_NATIVE_RPD_LINEAGE',None
        material=store.recent_primary(s.symbol,s.end,sc.wall())
        durable=[]
        for width,bar in terminal:
            saved=store.db.execute(query,(s.symbol,width,bar.open_ms)).fetchone()
            if not _durable_terminal_matches(saved,bar,width):
                return 'HOLD_NATIVE_RPD_LINEAGE',None
            durable.append((width,bar.open_ms,saved))
        durable=tuple(durable)
        def still():
            if not self._current(s) or store.db.in_transaction or store.segment(s.symbol)!=origin:
                return False
            if store.db.execute('SELECT payload FROM rpd_checkpoint WHERE symbol=?',(s.symbol,)).fetchone()!=(packed,):
                return False
            for width,opened,saved in durable:
                if store.db.execute(query,(s.symbol,width,opened)).fetchone()!=saved:
                    return False
            return True
        provenance=canonical(('AUTHENTIC_ORIGINAL_CCXT_MINUTE_DERIVED',key,receipt.source_proof,batches,durable))
        lineage=canonical((provenance,packed,origin.open_ms,origin.proven_at_ms))
        last=max(receipt.received_ms,st.observed,*(r.observed_ms for r in material[0]),*(r.received_ms for r in material[1]))
        at=sc.wall()
        if not s.end<=last<=at or not still():return 'HOLD_NATIVE_RPD_LINEAGE',None
        seal=AuthoritativePrimaryCommit(*key,at,s.expires,last,provenance,identity,
                                       canonical(material),canonical(closed),lineage)
        projector=CommittedProjection(wall=sc.wall,native_owner_lookup=lambda *k:seal if k==key and still() else None,
            current_authority=lambda _c,r,_t:r is seal and still(),
            trusted_rpd_pin=lambda c,e,_t:lineage if (c.pair,c.symbol,c.generation,e)==key and still() else None)
        event=projector.project(seal,candidate,before,material,closed)
        gate=NativePrimaryHandoff(wall=sc.wall,original_scope=projector.still_native,
            frame_sha=lambda _p,e:frame_sha(frame(),e)[0])
        bot._c11_native_primary_handoff=gate
        try:
            with gate.committed(event):
                if gate.resolve(s.pair,candidate,s.end) is None or not still():
                    return 'HOLD_ORIGINAL_ANALYZER_CHANGED',None
                self.claimed.add(dedupe)
                return 'ORIGINAL_NATIVE_CALLBACK',callback()
        finally:del bot._c11_native_primary_handoff
