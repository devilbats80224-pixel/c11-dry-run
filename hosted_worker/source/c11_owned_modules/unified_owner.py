"""Core one-boundary original Worker-owned optional lane; compiled disabled."""
from pending_roster import PendingRoster
from threading import get_ident

from capture import query_for
from coordinator import Scope
from dedicated_queue import Job
from nw_observation import MissingCheckpointNWObservation
from terminal_window_diagnostics import WindowCounters

class UnifiedScheduler:
    def __init__(self,*,worker,queue,adapter,original_plan,original_commit_and_decide,wall,epoch_authority=lambda _e:False,reconfirm=None,diagnostic_sink=None):
        bot=worker.freqtrade
        if (queue.executor.capture is not adapter.capture or
                adapter.ready.store is not bot.policy.bars or not bot.config['dry_run']):
            raise ValueError('SAME_WORKER_STORE_CAPTURE_REQUIRED')
        self.worker,self.queue,self.adapter=worker,queue,adapter
        self.plan,self.commit_and_decide=original_plan,original_commit_and_decide
        self.wall=wall
        from warm_release_binding import WarmReleaseBinding
        self.warm=WarmReleaseBinding(self)
        self.plan.warm_prepare=self.warm.eligible
        self.plan.verified_readiness_at=self.warm.readiness_at
        self.plan.warm_minute_span=self.warm.minute_span
        # Preserve only the earlier explicitly reviewed correction. Original
        # independent position-protection cadence remains untouched.
        self.plan.warm_keep_legacy=self.commit_and_decide.wants_multilane
        self.epoch_authority=epoch_authority
        self.thread=get_ident()
        self.marker=None
        self.covers={}
        self.tickets={}
        self.verified=set()
        self.retired=set()
        self.pending_roster=PendingRoster()
        self.closed=False
        self.isolated_history=None
        self.reconfirm=reconfirm
        self.nw_without_rpd=MissingCheckpointNWObservation(wall,self.plan.mono)
        self.nw_observation_last_status=None
        self.isolated_prior_seed_loader=None
        self.first_gate=None
        if diagnostic_sink is not None:
            from first_gate import FirstGate
            self.first_gate=FirstGate(diagnostic_sink)
        self.diagnostic=WindowCounters(diagnostic_sink) if diagnostic_sink is not None else None

    def _owner(self):
        if get_ident()!=self.thread or self.closed:raise RuntimeError('ORIGINAL_SCHEDULER_OWNER')

    def _count(self,field,n=1,*,peak=False):
        if self.diagnostic is not None and self.diagnostic.end is not None:
            now=self.wall()
            if self.diagnostic.end<=now<self.diagnostic.end+60_000:
                self.diagnostic.add(field,n,peak=peak)

    def _transition(self,scope,label,at=None):
        if self.first_gate is not None:
            self.first_gate.note(scope.generation,scope.symbol,'transition',label,self.wall() if at is None else at)

    def explain_public_revisit(self,proof,label):
        if type(proof) is Scope:self._transition(proof,label)

    def observe_original_analysis(self,pair,frame_end,label):
        # Diagnostic-only: inspect the already-owned candidate, no source
        # recheck, creation of a new scope, shortened clock or I/O.
        self._owner()
        if self.first_gate is None or self.marker is None:return
        s=next((s for s in self.covers.values() if s.pair==pair and
                self.marker==(s.generation,s.end)),None)
        if s is None:return
        if type(frame_end) is int and frame_end!=s.end:
            label='ANALYSIS_RAW_FRAME_OLDER' if frame_end<s.end else 'ANALYSIS_RAW_FRAME_AHEAD'
        self.first_gate.note(s.generation,s.symbol,'analysis_flow',label,self.wall())

    def _retired_diagnostic(self,ticket,real):
        if self.diagnostic is None:return
        if real is False:
            self._count('zero_wire_retired')
            return
        if real is None:
            self._count('retired_unclassified')
            return
        record=self.adapter.capture.records.get(ticket)
        shape=frozenset(('EXACT_ROW_COUNT_REQUIRED','EXACT_TWELVE_FIELDS_REQUIRED','INVALID_TIME_GRID_OR_COUNT',
                         'INVALID_DECIMAL_LEXEME','INVALID_PRICE_VOLUME_RELATION','MISMATCHED_TLS_RESPONSE'))
        grade=record.get('capture_grade') if type(record) is dict else None
        self._count('wire_shape_retired' if type(grade) is str and grade in shape else 'wire_other_retired')

    def _accept_existing_native(self,native,by_ticket,*,nowait=False):
        if self.adapter.accept_native(native,nowait=nowait):
            self._count('validated_lane')
            key=by_ticket.get(native.request_id)
            if key is not None:
                self.verified.add(key)
                if self.first_gate is not None:
                    anchor=self.covers[key]
                    self.first_gate.note(anchor.generation,anchor.symbol,'valid',native.task.tf)
                    self.first_gate.note(anchor.generation,anchor.symbol,'wire',native.task.tf)
                    self._transition(anchor,'NATIVE_REPLY_'+native.task.tf.upper(),native.received_ms)
                    lane=native.task.tf.upper()
                    self.first_gate.note(anchor.generation,anchor.symbol,'owner_receipt_clock',
                                         'RECEIVED_'+lane,native.received_ms)
                    self.first_gate.note(anchor.generation,anchor.symbol,'owner_receipt_clock',
                                         'VISIBLE_'+lane,self.wall())
            return True
        return False

    def _drain_owner_public_queue(self):
        before=len(self.queue.inflight)
        by_ticket={ticket:key for key,ticket in self.tickets.items()}
        for native,_wire in self.queue.poll():
            if self.reconfirm is None or not self.reconfirm.receive(native):
                self._accept_existing_native(native,by_ticket)
            else:
                self._count('reconfirm_valid_lane')
                self._note_reconfirm_events()
                event=getattr(self.reconfirm,'last_cross_width_event',None)
                if event is not None and self.first_gate is not None:
                    owner_scope,status=event
                    self.first_gate.note(owner_scope.generation,owner_scope.symbol,'normalization_refusal',status,self.wall())
        return len(self.queue.inflight)<before

    def _submit_with_ready_reclaim(self, job):
        if self.queue.submit(job):
            return True
        if self.queue.last_submit_status!='submit_capacity':
            return False
        # A valid completion can arrive after the earlier nonblocking poll.
        # Process it through the same owner/OPG path, then try exactly once.
        # No waiting, physical debit refund, extra slot, or lease extension.
        return self._drain_owner_public_queue() and self.queue.submit(job)

    def _note_reconfirm_events(self):
        events=self.reconfirm.last_reconfirm_events
        if self.first_gate is None or not events:return
        w=self.wall()
        for s,code in events:
            self.first_gate.note(s.generation,s.symbol,'reconfirm_progress',code,w)

    def _reconfirm_needed(self, scope):
        """Skip redundant rechecks only on the ordinary native-minute route."""
        # The existing reviewed correction requires the original independent
        # receipt's provenance. A valid 1m projection cannot replace that
        # receipt or suppress its previously reviewed reconfirmation path.
        from reviewed_opg_selector import OpgSelector
        reviewed=getattr(self.commit_and_decide,'multilane_prepare',None)
        if type(reviewed) is OpgSelector and reviewed.claims(scope):
            return True
        if scope in self.warm.selected:return False
        receipts=self.adapter.verified_committed_receipts(scope)
        by_lane={r.ticket.lane:r for r,_ in receipts}
        if len(receipts)!=3 or set(by_lane)!={'1m','5m','15m'}:
            return True  # the incumbent fallback will check its own admission
        ordered=tuple(by_lane[k] for k in ('1m','5m','15m'))
        return self.adapter.normalize(scope,ordered) is None

    def offer_after_protection(self,bot):
        result=self._offer_after_protection(bot)
        if self.isolated_history is not None:self.isolated_history.after_protection(bot)
        return result

    def _offer_after_protection(self,bot):
        self._owner()
        if bot is not self.worker.freqtrade:raise ValueError('WRONG_NATIVE_BOT')
        if self.diagnostic is not None:self.diagnostic.boundary(self.wall())
        if self.first_gate is not None:self.first_gate.start(self.wall())
        self._count('owner_cycles')
        if self.isolated_prior_seed_loader is not None:
            self.isolated_prior_seed_loader.apply_held(bot)
        plan=self.plan(bot)
        if plan is None:
            self.pending_roster.clear()
            self._count('no_plan')
            return 'HOLD_UNPROVEN_ORIGINAL_PLAN'
        generation,end,items=plan
        _,held=self.plan.entries()
        accepted_symbols={r.symbol for r in held if r.generation==generation}
        if self.first_gate is not None:
            planned={item[1].symbol for item in items}
            for r in held:
                w=self.wall()
                horizon=min(end+60_000,w+max(0,(r.expires_ns-self.plan.mono())//1_000_000))
                l=self.isolated_prior_seed_loader
                self.first_gate.watch(w,generation,r.symbol,horizon,l.status.get(r.symbol) if l and r.symbol not in planned else None)
        self._count('offered_item_peak',len(items),peak=True)
        self._count('no_checkpoint_candidate',getattr(self.plan,'unplanned_checkpoint_count',0))
        if not items:self._count('empty_plans')
        if end!=self.adapter.capture.end:
            if not self.epoch_authority(end):
                self._count('plan_boundary')
                return 'HOLD_ORIGINAL_BOUNDARY_AUTHORITY'
            self.queue.fence(generation,end)
            self.queue.poll()  # drain only already completed stale work
            if self.queue.inflight:
                self._count('plan_queue')
                return 'HOLD_STALE_PHYSICAL_DRAIN'
            executor=self.queue.executor
            if executor.capture.end!=end and not executor.rotate_boundary(end,self.epoch_authority):
                return 'HOLD_BOUNDARY_CAPTURE_NOT_ROTATED'
            if not self.adapter.rebind_capture(executor.capture,self.epoch_authority):
                return 'HOLD_CAPTURE_REBIND_CONTENDED'
        if self.marker!=(generation,end):
            if self.marker is not None and self.marker[1]==end:
                for scope in set(self.covers.values()):self._transition(scope,'SOURCE_GENERATION_FENCED')
            if not self.adapter.reset(generation,end):
                self._count('plan_queue')
                return 'HOLD_QUEUE_CONTENDED'
            if not self.queue.rollover(generation,end):
                self._count('plan_queue')
                return 'HOLD_STALE_PHYSICAL_DRAIN'
            self.marker=(generation,end)
            self.covers.clear()
            self.tickets.clear()
            self.verified.clear()
            self.retired.clear()
        if self.first_gate is not None:
            for k,t in tuple(self.tickets.items()):
                ss=self.covers[k]
                if self.queue.limiter.charged_for_diagnostic(t) is True:
                    self.first_gate.note(ss.generation,ss.symbol,'wire',{60000:'1m',300000:'5m',900000:'15m'}[k[1]])
        self._drain_owner_public_queue()
        # No failed/cancelled receipt may hold the original path hostage.
        # Capture retains immutable outcome records even for unsent tickets.
        # Retire every attempted ticket here; never delete proof, reuse its
        # ID, or refund physical debits. Original native fallback is separate.
        for key,ticket in tuple(self.tickets.items()):
            if key not in self.verified and ticket not in self.queue.inflight:
                scope=self.covers[key]
                meter=getattr(self.queue.limiter,'charged_for_diagnostic',None)
                debit=meter(ticket) if callable(meter) else None
                forgotten=self.adapter.forget_unsent(ticket)
                if debit is False and not forgotten:
                    # No proof that the owner-side reservation is released
                    # yet. Preserve it for the next owner cycle instead.
                    continue
                self._retired_diagnostic(ticket,debit)
                self.retired.add(ticket)
                reissue=getattr(self.plan,'reissue_unwired_minute',None)
                if debit is False and forgotten and callable(reissue):
                    if reissue(scope,ticket,debit):
                        self._transition(scope,'ORIGINAL_UNWIRED_MINUTE_REISSUED')
                del self.tickets[key]
                del self.covers[key]
        if self.reconfirm is not None:
            # Keep an existing job's lifecycle; do not begin a new pair
            # of reconfirmation waves merely because larger bars disagree.
            scopes=tuple(dict.fromkeys(self.covers.values()))
            if self.reconfirm.scope is None:
                scopes=tuple(s for s in scopes if self._reconfirm_needed(s))
            self.reconfirm.step(scopes)
            self._note_reconfirm_events()
        for task,scope,reqid in self.pending_roster.plan(self.marker,items,self.wall(),lambda s:type(s) is Scope,accepted_symbols):
            if type(scope) is not Scope or (scope.generation,scope.end)!=self.marker:continue
            q=query_for(task)
            key=(scope.symbol,{'1m':60000,'5m':300000,'15m':900000}[task.tf],
                 int(q['startTime']),scope.end,int(q['limit']))
            if key in self.covers or reqid in self.retired:
                self.pending_roster.retire(reqid)
                continue
            if not self.adapter.register_original_dispatch(task,scope,reqid):continue
            job=Job(reqid,scope.symbol,scope.generation,scope.end,scope.expires,task.tf,
                    1 if int(q['limit'])<100 else 2,task,reqid)
            if not self._submit_with_ready_reclaim(job):
                self._count(self.queue.last_submit_status)
                if self.first_gate is not None:
                    from first_gate import DENIAL
                    why=DENIAL.get(self.queue.last_submit_status)
                    if why is not None:
                        self.first_gate.note(scope.generation,scope.symbol,'gate',why)
                        self._transition(scope,why)
                # No physical Future was created by queue if rejected.
                # Return its unissued ticket so no artificial cap is consumed.
                self.adapter.forget_unsent(reqid)
                continue
            self._count('submitted')
            self.pending_roster.retire(reqid)
            self.covers[key]=scope
            self.tickets[key]=reqid
        self.nw_observation_last_status=self.nw_without_rpd.after_drained(self,bot,generation,end)
        return 'OWNER_PUBLIC_PLAN_DRAINED'

    def covers_native_public(self,symbol,width,start,end,limit):
        self._owner()
        k=(symbol,width,start,end,limit)
        s=self.covers.get(k)
        if (s is None and width==300_000 and
                self.plan.has_frozen_five_offer(symbol,start,end,limit,self.marker)):
            # This same held original owner will submit its real 5m task;
            # do not let synchronous FeatureWarmup race it with a second wire.
            # No fallback receipt is promoted and all old timings still apply.
            return True
        return (s is not None and self.marker==(s.generation,s.end) and
                self.wall()<s.expires and self.adapter.ready.authority(s,self.wall()) and
                (k in self.verified or self.queue.pending_or_proven(self.tickets[k])))

    def provisional_scope(self,pair,end):
        self._owner()
        now=self.wall()
        owned=next((s for s in self.covers.values() if (s.pair,s.end)==(pair,end)),None)
        if (owned is not None and self.marker==(owned.generation,owned.end)
                and end<=now<owned.expires and self.adapter.ready.authority(owned,now)):
            return owned
        provisional=self.plan.provisional_scope(pair,end)
        if provisional is not None:
            self.plan.remember_original_warm_offer(provisional)
        return provisional

    def requires_staged_revisit_frame(self,proof):
        # The warm route installs exact reconstructed input. Ordinary and
        # verified non-warm routes may already have the canonical frame.
        self._owner()
        return (self.warm.late is not None and self.warm.late[0] is proof
                or self.warm.prepared is not None and self.warm.prepared[0] is proof)

    def original_revisit_frame(self,proof,provider_frame):
        self._owner()
        if self.warm.late is not None and self.warm.late[0] is proof:
            return self.warm.late[2].quarters if self.warm.late_current(proof) else None
        if self.warm.prepared is not None and self.warm.prepared[0] is proof:
            return self.warm.frame(proof) or None
        if not self.revisit_frame_current(proof):return None
        from analyzed_frame import exact_analyzed_ohlcv
        from committed_projection import frame_sha
        scope=self._owned_revisit_scope
        _,old=frame_sha(exact_analyzed_ohlcv(provider_frame),scope.end)
        rows=self._owned_revisit_rows
        expected=tuple((b.open_ms,b.op,b.high,b.low,b.close,b.volume) for b in rows)
        if tuple(old[-260:])==expected and not self.commit_and_decide.wants_multilane(scope):return None
        status,p=self.commit_and_decide.prepare_flat_analyzer(scope,self.adapter)
        if p is not None and self.commit_and_decide.last_atomic_commit:
            # A single in-process result of original Ready.consume, not a new proof.
            self._owned_prepared_analysis=(scope,status,p)
            return rows
        self._owned_prepared_analysis=(scope,status,None)
        return None

    def discard_revisit_frame(self):
        self.warm.clear_late()
        self.warm.prepared=None
        self._owned_prepared_analysis=None
        self._owned_revisit_scope=None
        self._owned_revisit_rows=None

    def revisit_frame_current(self,proof):
        self._owner()
        if self.warm.late is not None and self.warm.late[0] is proof:
            return self.warm.late_current(proof)
        if self.warm.prepared is not None and self.warm.prepared[0] is proof:
            return self.warm.frame_current(proof)
        scope=getattr(self,'_owned_revisit_scope',None)
        return (scope is not None and (scope.pair,scope.symbol,scope.generation,scope.end)==
                (proof.pair,proof.symbol,proof.generation,proof.end) and scope.expires<=proof.expires
                and self.marker==(scope.generation,scope.end) and self.wall()<scope.expires
                and self.adapter.ready.authority(scope,self.wall()))

    def revisit_ready(self,first,pair,end):
        self._owned_revisit_scope=None
        self._owned_revisit_rows=None
        self._owner()
        now=self.wall()
        if type(first) is not Scope or (first.pair,first.end)!=(pair,end):return False
        current=self.plan.current(first,now)
        if (now>=min(first.expires,end+60_000) or not current) and self.warm.arm_late(first,pair,end):
            return True
        if not current:
            fresh=self.plan.provisional_scope(pair,end)
            if (type(fresh) is Scope and (fresh.pair,fresh.symbol,fresh.end)==(pair,first.symbol,end)
                    and (self.plan.can_rebind_provisional(first,fresh,now)
                         if callable(getattr(self.plan,'can_rebind_provisional',None))
                         else fresh.generation>first.generation and self.plan.current(fresh,now))):
                warm=self.warm.revisit(first,pair,end,fresh_source=fresh)
                if warm is not None:return warm
        if not end<=now<first.expires:
            self._transition(first,'REVISIT_ORIGINAL_LEASE_EXPIRED')
            return False
        warm=self.warm.revisit(first,pair,end)
        if warm is not None:return warm
        # Match first original proof and the already-owned triad. A later
        # source epoch or extension may not turn an earlier HOLD into entry.
        s=next((r for r in self.covers.values() if r.pair==pair and r.end==end),None)
        if (s is None or (s.symbol,s.generation)!=(first.symbol,first.generation)
                or self.marker!=(s.generation,s.end) or not now<s.expires<=first.expires
                or not self.adapter.ready.authority(s,now)):
            self._transition(first,'REVISIT_CURRENT_SCOPE_NOT_MATCHED')
            return False
        if self.reconfirm is not None and self.reconfirm.actor.owns_original_scope(s):
            effective=getattr(self.adapter.ready,'effective_expiry',None)
            wall=self.wall()
            expiry=min(s.expires,effective(s,wall)) if callable(effective) else s.expires
            bot=self.worker.freqtrade
            if (getattr(bot,'_owned_revisit_no_positions',False) is True
                    and getattr(bot,'_owned_revisit_reconciled',False) is True
                    and min(expiry-wall,self.commit_and_decide.remaining())>self.adapter.ready.latency_budget):
                handled=self.reconfirm._intake_same_original_wave_two(s,now)
                if handled:self._count('reconfirm_valid_lane',handled)  # compatibility: routed, not validated
                self._note_reconfirm_events()
                event=self.reconfirm.last_cross_width_event
                if event is not None and self.first_gate is not None:
                    owner_scope,status=event
                    self.first_gate.note(owner_scope.generation,owner_scope.symbol,'normalization_refusal',status,self.wall())
            result=self.reconfirm.actor.available_for_owner_revisit(s)
            if not result:
                terminal=self.reconfirm.terminal_for(s)
                self._transition(s,'REVISIT_RECONFIRM_TERMINAL' if terminal else 'REVISIT_RECONFIRM_PENDING')
            if result and self.commit_and_decide.wants_multilane(s):
                bot=self.worker.freqtrade
                if (getattr(bot,'_owned_revisit_no_positions',False) is not True or
                        getattr(bot,'_owned_revisit_reconciled',False) is not True):
                    return False
                settled=self.reconfirm.actor.scoped_revision_preview(s)
                quarters=None if settled is None else dict(settled.batches).get(900_000)
                if quarters is None or len(quarters)!=260:return False
                self._owned_revisit_scope=s
                self._owned_revisit_rows=quarters
            return result
        # NativeEntryOwner already verified the Freqtrade frame boundary
        # before this call. Catch up authentic completed same-scope receipts
        # before original analysis checks readiness, never new transport.
        # Optional optimization only in a flat, proven original native
        # cycle. Existing held positions and pending opens retain untouched
        # original protection cadence. Exit checks remain unchanged.
        if getattr(self.worker.freqtrade,'_owned_revisit_no_positions',False) is True:
            by_ticket={ticket:key for key,ticket in self.tickets.items()}
            self.queue.accept_completed_original_scope(s,
                lambda native:self._accept_existing_native(native,by_ticket,nowait=True))
        if not self.adapter.ready.available_for_owner_revisit(s):
            self._transition(s,'REVISIT_READY_TRIAD_UNAVAILABLE')
            return False
        native=self.adapter.verified_committed_receipts(s)
        by_lane={r.ticket.lane:r for r,_ in native}
        if set(by_lane)!={'1m','5m','15m'}:
            self._transition(s,'REVISIT_PROVEN_LANES_MISSING')
            return False
        ordered=tuple(by_lane[k] for k in ('1m','5m','15m'))
        from reviewed_opg_selector import OpgSelector
        reviewed=getattr(self.commit_and_decide,'multilane_prepare',None)
        if type(reviewed) is OpgSelector and reviewed.claims(s):
            from cross_width_verify import OK,verify_terminal
            if verify_terminal(s.end,*(r.raw for r in ordered))!=OK:
                # Until the existing reconfirm actor owns this scope, do
                # not arm a revisit on non-original projected rows.
                self._transition(s,'REVISIT_RECONFIRM_PENDING')
                return False
        normalized=self.adapter.normalize(s,ordered)
        if normalized is None:
            self._transition(s,'REVISIT_TERMINAL_CLOSE_CONFLICT')
            if self.first_gate is not None:
                self.first_gate.note(s.generation,s.symbol,'normalization_refusal',
                    self.adapter.last_normalization_refusal,self.wall())
            return False
        ok=self.wall()<s.expires and self.adapter.ready.authority(s,self.wall())
        if ok and getattr(self.worker.freqtrade,'_owned_revisit_no_positions',False) is True:
            quarters=normalized[2][1]
            if len(quarters)==260:
                self._owned_revisit_rows=quarters
                self._owned_revisit_scope=s
        self._transition(s,'REVISIT_ARMED' if ok else 'REVISIT_SCOPE_LOST_AFTER_NORMALIZE')
        return ok

    def consume_ready_only(self,pair,end,frame,callback):
        self._owner()
        late_scope=self.warm.late[1] if self.warm.late is not None else None
        late=self.warm.publish_late(pair,end,frame,callback)
        if late is not None:
            status,_=late
            self._transition(late_scope,status)
            if status=='ORIGINAL_NATIVE_CALLBACK' and self.first_gate is not None:
                self.first_gate.note(late_scope.generation,late_scope.symbol,'callback',status)
                self._count('original_callback')
            return late
        scope=next((s for s in self.covers.values() if s.pair==pair and s.end==end),None)
        if scope is None or self.marker!=(scope.generation,end) or self.wall()>=scope.expires:
            self._count('entry_no_scope')
            return 'HOLD_NO_OWNED_SCOPE',None
        if scope in self.warm.selected:
            status,value=self.warm.publish(scope,frame,callback)
            self._transition(scope,status)
            if status=='ORIGINAL_NATIVE_CALLBACK':
                if self.first_gate is not None:
                    self.first_gate.note(scope.generation,scope.symbol,'callback',status)
                self._count('original_callback')
                self._count('original_full_commit')
            return status,value
        previous=getattr(self,'_owned_prepared_analysis',None)
        if previous is None:
            ready=self.adapter.ready
            def has_reserve():
                now=self.wall()
                return (scope.end<=now and min(scope.expires-now,self.commit_and_decide.remaining())>ready.latency_budget)
            if (getattr(self.worker.freqtrade,'_owned_revisit_no_positions',False) is True
                    and has_reserve()
                    and not ready.available_for_owner_revisit(scope)
                    and (self.reconfirm is None or not self.reconfirm.actor.owns_original_scope(scope))):
                # The already-authenticated in-memory observation only.
                # Ready.consume below still applies the original fresh owner
                # authority check; never add a blocking observer read here.
                observed_end, observed=self.plan.entries()
                by_ticket={ticket:key for key,ticket in self.tickets.items() if self.covers.get(key) is scope}
                if (by_ticket and observed_end==scope.end and
                        any((row.symbol,row.pair,row.generation)==(scope.symbol,scope.pair,scope.generation)
                            for row in observed)):
                    self.queue.accept_completed_original_scope(scope,
                        lambda native:native.request_id in by_ticket and has_reserve() and
                                      self._accept_existing_native(native,by_ticket,nowait=True))
            from reviewed_opg_selector import OpgSelector
            reviewed=getattr(self.commit_and_decide,'multilane_prepare',None)
            if type(reviewed) is OpgSelector and reviewed.claims(scope):
                # On this previously reviewed route, keep the native
                # provisional revisit when original receipts disagree.
                # A projected 1m terminal is not the independent evidence
                # needed by the reviewed OPG source.
                by_lane={r.ticket.lane:r for r,_ in self.adapter.verified_committed_receipts(scope)}
                if set(by_lane)=={'1m','5m','15m'}:
                    from cross_width_verify import OK,verify_terminal
                    comparison=verify_terminal(scope.end,*(by_lane[x].raw for x in ('1m','5m','15m')))
                    if comparison!=OK:
                        return 'HOLD_NO_UNUSED_RECONFIRM_CHAIN',None
            status,value=self.commit_and_decide(scope,self.adapter,frame,callback)
        else:
            self._owned_prepared_analysis=None
            if previous[0] is not scope:
                return 'HOLD_SCOPE_CHANGED_AFTER_COMMIT',None
            if previous[2] is None:status,value=previous[1],None
            else:status,value=self.commit_and_decide._publish_prepared(previous[2],scope,self.adapter,frame,callback)
        self._transition(scope,status)
        if self.first_gate is not None:
            w=self.wall()
            if status=='HOLD_NATIVE_COHERENCE':
                self.first_gate.note(scope.generation,scope.symbol,'normalization_refusal',
                   getattr(self.adapter,'last_normalization_refusal',None),w)
            for phase in getattr(self.commit_and_decide,'last_atomic_phases',()):
                self.first_gate.note(scope.generation,scope.symbol,'atomic_phase',phase,w)
        if self.first_gate is not None and status=='HOLD_NATIVE_COMMIT_REFUSED':
            self.first_gate.note(scope.generation,scope.symbol,'atomic_refusal',
                                 getattr(self.commit_and_decide,'last_atomic_refusal',None),self.wall())
        if getattr(self.commit_and_decide,'last_atomic_commit',False):self._transition(scope,'NATIVE_ATOMIC_COMMIT')
        if self.first_gate is not None and status=='ORIGINAL_NATIVE_CALLBACK':
            self.first_gate.note(scope.generation,scope.symbol,'callback',status)
        if self.diagnostic is not None:
            if getattr(self.commit_and_decide,'last_atomic_commit',False):
                self._count('original_full_commit')
                if status!='ORIGINAL_NATIVE_CALLBACK':self._count('original_full_commit_without_callback')
            if status=='ORIGINAL_NATIVE_CALLBACK':self._count('original_callback')
            elif status in ('HOLD_NOT_READY','HOLD_NO_UNUSED_RECONFIRM_CHAIN'):self._count('entry_partial')
            else:self._count('entry_other_hold')
        return status,value

    def stop_and_join(self):
        self._owner()
        self.closed=True
        self.pending_roster.clear()
        self.covers.clear()
        self.tickets.clear()
        self.verified.clear()
        self.retired.clear()
        if self.reconfirm is not None:self.reconfirm.release()
        try:self.queue.stop_and_join()
        finally:
            if self.isolated_history is not None:self.isolated_history.close()
