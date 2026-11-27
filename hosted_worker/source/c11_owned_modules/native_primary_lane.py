"""Compute the original NW15 outcome for otherwise unplanned candidates.
This data-only route never calls the strategy entry callback or bypasses RPD.
"""
from secrets import token_hex
from types import SimpleNamespace
from collections import deque
from threading import get_ident
from sqlite3 import OperationalError, SQLITE_BUSY, SQLITE_LOCKED
from bridge_protocol import Refused
from capture import sanitize
from causal_bars import ClosedBar,FIVE,QUARTER,MINUTE
from closed_minute_basis import missing_projection_quarters, project_closed_minutes
from rpd_checkpoint import RpdState,WARM
from c11_primary_decision_seam_v1.exact_rpd5_join import Scope, join
from minute_rollup import roll
from parallel_history_pages import PendingHistoryPages
from primary_only_route import all_missing_pages,outcome,PrimaryMinuteJob,NW_WARM

class NativePrimaryLane:
    def __init__(self,owner,make_forwarder):
        self.owner=owner
        self.thread=get_ident()
        self.pending=PendingHistoryPages(make_forwarder,self.authority,parallel_ranges=True,audit=self.audit)
        self.recorded={}
        self.terminal_five={}
        self.vetoed=set()
        self.marker=None
        self.last=None

    def gate(self,reason,fact=None):
        trace=getattr(self.owner,'shared_minute',None)
        emit=getattr(trace,'gate',None)
        if emit is not None:
            emit(reason,fact)

    def audit(self,job,stage,reason=None,**values):
        trace=getattr(self.owner,'shared_minute',None)
        if trace is not None:
            if reason is None:
                trace.audit(job,stage,**values)
            else:
                trace.audit(job,stage,reason,**values)

    def phase_hold(self):
        o=self.owner
        if (get_ident()!=self.thread or o.worker.freqtrade.config.get('dry_run') is not True
                or o.run.closed or not o.minute_admission.cycle_headroom()):
            return 'OWNER_CYCLE_UNAVAILABLE'
        if o.run.queue.protection_due():
            return 'PROTECTION_PRIORITY'
        return None

    def phase(self):
        return self.phase_hold() is None

    def current(self,f,s,now):
        o=self.owner
        # A data-only page shares actual native quota with original wire.
        # Another symbol's outstanding request is not a protection signal.
        # Never issue an overlapping request for the same close and symbol.
        return (self.phase() and s.end<=now<s.expires
                and not any((job.symbol,job.end)==(s.symbol,s.end)
                            for job,_,_ in o.run.queue.inflight.values())
                and o.tap.current(f) and o.run.plan.current(s,now)
                and (s.generation,s.end,s.pair) not in o.run.plan.last)

    def authority(self,j,witness):
        f,s=witness.fact,witness.scope
        return (j.valid(self.owner.run.wall()) and j.symbol==f.symbol and f.generation==j.generation
                and self.current(f,s,self.owner.run.wall()))

    def tick(self,facts,headroom):
        if get_ident()!=self.thread:return
        self.pending.drain(self.complete)
        o=self.owner
        now=o.run.wall()
        end=now//QUARTER*QUARTER
        if not end<=now<end+MINUTE:return
        hold=self.phase_hold()
        if hold is not None:
            self.gate(hold)
            return
        if end!=self.marker:
            self.marker=end
            self.recorded.clear()
            self.terminal_five.clear()
            self.vetoed.clear()
        o.check_store()
        order=sorted(facts,key=lambda f:(self.last is not None and f.symbol<=self.last,f.symbol))
        queued=deque()
        if not order:
            self.gate('NO_CURRENT_FACTS')
        for f in order:
            hold=self.phase_hold()
            if hold is not None:
                self.gate(hold)
                return
            if f.boundary!=end:
                self.gate('WRONG_BOUNDARY',f)
                continue
            if f.origin>end-NW_WARM*QUARTER:
                self.gate('NW_LOOKBACK_UNAVAILABLE',f)
                continue
            known=self.recorded.get(f.symbol)
            if o.pending is not None and o.pending[2].symbol==f.symbol:
                self.gate('OTHER_HISTORY_PENDING',f)
                continue
            s=o.run.plan.provisional_scope(f.pair,end)
            if s is None:
                self.gate('NO_PROVISIONAL_SCOPE',f)
                continue
            if not self.current(f,s,o.run.wall()):
                self.gate('SCOPE_NOT_CURRENT',f)
                continue
            try:
                if known is None and self.classify_if_ready(f,s):
                    known=self.recorded[f.symbol]
                if not headroom():
                    self.gate('NO_PID_HEADROOM',f)
                    continue
                if known=='REQUIRES_ORIGINAL_RPD' and f.symbol not in self.vetoed:
                    self.record_verified_veto(f,s)
                rpd_refill=known=='REQUIRES_ORIGINAL_RPD' and f.symbol not in self.vetoed
                projection_only=known is not None and not rpd_refill
                if rpd_refill:
                    missing=self.missing_recent_rpd_minutes(f,s,now)
                else:
                    missing=missing_projection_quarters(o.store,f.symbol,end,now,NW_WARM)
                for start,stop in self.pending.inflight_ranges(f.symbol):
                    missing.difference_update(range(start,stop,QUARTER))
                if not missing:continue
                pages=iter(all_missing_pages(end,missing))
                queued.append((SimpleNamespace(symbol=f.symbol,fact=f,scope=s,rpd_refill=rpd_refill,projection_only=projection_only),pages))
            except (Refused,ValueError):
                self.gate('PREPARATION_REFUSED',f)
                continue
        # Give each symbol one page before its siblings occupy extra budget.
        # Every page still needs a fresh per-ticket GO on the same IP budget.
        while queued:
            hold=self.phase_hold()
            if hold is not None:
                self.gate(hold)
                return
            f,pages=queued.popleft()
            req=next(pages,None)
            if req is None:
                continue
            if not headroom():
                self.gate('NO_PID_HEADROOM',f.fact)
                continue
            s=f.scope
            j=PrimaryMinuteJob(f.symbol,f.fact.generation,end,s.expires,*req,token_hex(16))
            self.audit(j,'PAGE_PLANNED')
            if self.pending.offer(j,f):
                self.last=f.symbol
            # A held page stays missing. Its siblings still get this round;
            # next owner tick replans only missing quarters not already in flight.
            queued.append((f,pages))

    def missing_recent_rpd_minutes(self,f,s,now):
        # The 15m primary has already run. Reuse it and independently collect
        # only the 1m data needed for the original 5m warmup lower bound.
        # These candles are data, never a verified strategy checkpoint.
        store=self.owner.store
        floor=((s.end-WARM*FIVE)//QUARTER)*QUARTER
        if floor<=0 or f.origin>=s.end:return set()
        state=store.db.execute('SELECT payload FROM rpd_checkpoint WHERE symbol=?',(f.symbol,)).fetchone()
        if state is not None:
            p=RpdState.restore(state[0])
            if ((p.symbol,p.origin)==(f.symbol,f.origin) and p.observed<=now and p.next_open>=floor):
                return set()  # existing exact continuation lane handles this one
        start=max(floor,((f.origin+QUARTER-1)//QUARTER)*QUARTER)
        have={r[0] for r in store.db.execute(
            'SELECT open_ms FROM candle WHERE symbol=? AND width=? AND open_ms>=? AND open_ms<? AND received_ms<=?',
            (f.symbol,MINUTE,start,s.end,now))}
        return {q for q in range(start,s.end,QUARTER) if not all(t in have for t in range(q,q+QUARTER,MINUTE))}

    def classify_if_ready(self,f,s):
        o=self.owner; now=o.run.wall()
        _,minutes,r=o.store.recent_primary(f.symbol,s.end,now)
        if r is None or not minutes or not self.current(f,s,o.run.wall()):return False
        v=outcome(r.status)
        if v=='UNKNOWN':return False
        o.store.feature_status(f.symbol,s.end,now,{'NW15_PRIMARY_BEFORE_RPD':v})
        self.recorded[f.symbol]=v
        return True

    def record_verified_veto(self,f,s):
        """A witnessed veto never grants an entry or advances the owner cursor."""
        o=self.owner
        now=o.run.wall()
        witness=self.terminal_five.get(f.symbol)
        if not self.current(f,s,now) or (witness is not None and witness[:2]!=(s.generation,s.end)):
            return
        try:
            proof=o.store.segment(f.symbol)
            record=o.store.db.execute('SELECT payload FROM rpd_checkpoint WHERE symbol=?',(f.symbol,)).fetchone()
            if proof is None or record is None or proof.open_ms!=f.origin or proof.proven_at_ms>now:
                return
            state=RpdState.restore(record[0])
            anchor=s.end-QUARTER
            if (state.symbol,state.origin)!=(f.symbol,f.origin) or state.observed>now:
                return
            if state.next_open==s.end:
                # The original feature warmup can finish RPD before this
                # optional owner lane. Prove that the sealed exact-close state
                # used the same admitted terminal basis, never an older state
                # or an inconsistent independently fetched wider candle.
                derived=project_closed_minutes(o.store,f.symbol,FIVE,anchor,s.end,now)
                stored=o.store.range(f.symbol,FIVE,anchor,s.end,now)
                fields=('op','high','low','close','volume','quote')
                if (not derived or len(stored)!=len(derived)
                        or any(a.received_ms>state.observed or
                               any(getattr(a,k)!=getattr(b,k) for k in fields)
                               for a,b in zip(stored,derived))
                        or state.bars[-len(stored):]!=[[r.high,r.low,r.close,r.volume] for r in stored]):
                    return
            else:
                if not s.end-2*QUARTER<=state.next_open<=anchor:
                    return
                # A page may already have been atomically persisted by the
                # projection/refill route, which doesn't populate terminal_five.
                # Reconstruct only the exact admitted minute window, after the
                # original checkpoint and current scope have been verified.
                bars=(witness[2] if witness is not None else
                      project_closed_minutes(o.store,f.symbol,FIVE,state.next_open,s.end,now))
                if bars is None:
                    return
                minute_five={bar.open_ms:bar for bar in bars}
                for opening in range(state.next_open,s.end,FIVE):
                    derived=minute_five.get(opening)
                    existing=o.store.range(f.symbol,FIVE,opening,opening+FIVE,now)
                    if existing and derived is not None:
                        prices=('op','high','low','close','volume','quote')
                        if any(getattr(existing[0],field)!=getattr(derived,field) for field in prices):
                            return
                    bar=existing[0] if existing else derived
                    if bar is None:
                        return
                    state.advance(bar,now)
            result,_=join(Scope(f.symbol,s.end),now,[state.closed()])
            if result=='VETO' and self.current(f,s,o.run.wall()):
                o.store.feature_status(f.symbol,s.end,now,{'RPD5_VETO_NATIVE_RECURRENCE':'VETO'})
                self.vetoed.add(f.symbol)
        except (Refused,ValueError,TypeError):
            return

    def complete(self,j,witness,data):
        o=self.owner; f,s=witness.fact,witness.scope
        due=lambda:self.authority(j,witness)
        if not due():
            self.audit(j,'PERSIST_REJECTED','DELIVERY_SCOPE_LOST')
            return
        sent,received,raw=data
        if not j.stop<=sent<=received<=o.run.wall() or not j.valid(sent) or not j.valid(received):
            self.audit(j,'PERSIST_REJECTED','INVALID_RECEIPT_CLOCK')
            return
        try:
            ar=sanitize(raw,'1m',j.stop,j.n,sent)
            def bar(r):return ClosedBar(r[0],received,*(float(r[u]) for u in (1,2,3,4,5,7)))
            if getattr(witness,'projection_only',False):
                # Data only: enrich the 1m basis, never rewrite older wider
                # bars or revise the already recorded original verdict.
                o.store.record_owner_primary_batch(j.symbol,((MINUTE,tuple(map(bar,ar))),(FIVE,()),(QUARTER,())),due)
                self.audit(j,'PERSISTED',response_ms=received)
                return
            if getattr(witness,'rpd_refill',False):
                five=tuple(bar(roll(ar[i:i+5],ar[i][0],FIVE)) for i in range(0,len(ar),5))
                o.store.record_owner_primary_batch(j.symbol,((MINUTE,tuple(map(bar,ar))),(FIVE,five),(QUARTER,())),due)
                self.audit(j,'PERSISTED',response_ms=received)
                return
            hist=[];terminal=[]
            for i in range(0,len(ar),15):
                subset=ar[i:i+15]
                first=subset[0][0]
                c=bar(roll(subset,first,QUARTER))
                if first==j.end-QUARTER:
                    terminal=[bar(r) for r in subset]
                hist.append(c)
            if not due():
                self.audit(j,'PERSIST_REJECTED','DELIVERY_SCOPE_LOST')
                return
            # Keep the admitted minutes for every historical page, not just the
            # terminal quarter. The same atomic store still refuses conflicting
            # already-observed rows and never rewrites them.
            o.store.record_owner_primary_batch(j.symbol,((MINUTE,tuple(map(bar,ar))),(5*MINUTE,()),(QUARTER,tuple(hist))),due)
        except (Refused,ValueError,TypeError):
            self.audit(j,'PERSIST_REJECTED','NATIVE_BATCH_REFUSED',response_ms=received)
            return
        except OperationalError as exc:
            code=getattr(exc,'sqlite_errorcode',None)
            if type(code) is int and (code&255) in (SQLITE_BUSY,SQLITE_LOCKED):
                self.audit(j,'PERSIST_REJECTED','SQLITE_BUSY',response_ms=received)
            raise
        self.audit(j,'PERSISTED',response_ms=received)
        try:
            if due() and terminal:
                raw=ar[-30:]
                five=tuple(bar(roll(raw[i:i+5],raw[i][0],FIVE)) for i in range(0,len(raw),5))
                self.terminal_five[f.symbol]=(s.generation,s.end,five)
            if due() and self.classify_if_ready(f,s) and self.recorded[f.symbol]=='REQUIRES_ORIGINAL_RPD':
                self.record_verified_veto(f,s)
        except (Refused,ValueError,TypeError):
            self.gate('PREPARATION_REFUSED',f)

    def close(self):self.pending.close()
