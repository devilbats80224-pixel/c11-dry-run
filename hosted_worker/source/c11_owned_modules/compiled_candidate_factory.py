"""Build-only native composition. The separate production pin must stay None."""
from dataclasses import dataclass, replace
from decimal import Decimal
from threading import get_ident
from uuid import uuid4

from adapter import StrictInitialReadyAdapter
from coordinator import Ready, Scope
from coverage_queue import Task
from dedicated_queue import WorkerOwnedPublicQueue
from exclusive_transport import ExclusiveUnsignedTransport
from owner_wrapper import NativeSchedulerBinding, OriginalStartupScopeUnavailable
from public_meter import PublicWire
from native_site_callbacks import OriginalCommitSite, ProtectionFence
from original_native_publisher import NativePublisher
from shared_native_budget import SharedNativeBudget
from trusted_5m import TailRequest
from unified_owner import UnifiedScheduler
from reconfirm_input import OriginalReconfirm
from reconfirm_dispatch import ReconfirmDispatch

QUARTER=900_000
FIVE=300_000

class OriginalPlan:
    """Uses only the native feed's most recently observed immutable candidates."""
    def __init__(self,worker,wall,mono,checkpoint_cursor):
        self.bot=worker.freqtrade
        self.wall=wall
        self.mono=mono
        self.checkpoint_cursor=checkpoint_cursor
        self.owner=get_ident()
        self.ticket_counter=0
        self.namespace=uuid4().hex
        self.last={}
        self.strict_retry={}
        self.retry_fenced=set()
        self.provisional_caps={}
        self.provisional_witness={}
        self.replaced_provisional_caps={}
        self.ready_provisional_caps={}
        self.original_warm_offers={}
        self.closed=False
        self.unplanned_checkpoint_count=0
        self.warm_prepare=None
        self.warm_minute_span=None
        self.warm_keep_legacy=None
        # Only an original owner can bind a proof of complete, readable data.
        # If unbound, the previously verified first-minute route is unchanged.
        self.verified_readiness_at=None

    def evaluation_ceiling(self,row,end,now):
        if now<end+60_000:
            return end+60_000
        if not callable(self.verified_readiness_at):
            return 0
        observed=self.verified_readiness_at(row,end,now)
        if type(observed) is not int or not end<=observed<=now:
            return 0
        return min(end+QUARTER,observed+60_000)

    def entries(self):
        now=self.wall();current_end=now//QUARTER*QUARTER
        observed=getattr(self.bot.feed,'latest_owner_observation',())
        if not observed:return current_end,()
        tick=self.mono()
        live=tuple(x for x in observed if x.expires_ns>tick and
                   now<self.evaluation_ceiling(x,current_end,now))
        if not live or len({x.generation for x in live})!=1:return current_end,()
        return current_end,live

    def startup_boundary(self):
        # Worker startup does not have to occur in a strategy evaluation
        # window. Initial Capture remains inert until entries()/epoch()
        # and every transport/consume check admit a real in-window scope.
        if get_ident()!=self.owner or self.closed:return None
        now=self.wall();tick=self.mono()
        live=tuple(x for x in getattr(self.bot.feed,'latest_owner_observation',()) if x.expires_ns>tick)
        if (not live or len({x.generation for x in live})!=1 or
                not all(x.ratio>=Decimal('0.20') for x in live) or
                not any(self.bot.observed_ranked(x) for x in live)):
            return None
        return now//QUARTER*QUARTER

    def epoch(self,end):
        actual,live=self.entries()
        return bool(live) and end==actual and all(x.ratio>=Decimal('0.20') for x in live)

    def provisional_scope(self,pair,end):
        """Existing owner-only current observation; never publish a candidate."""
        if get_ident()!=self.owner or self.closed:return None
        at=self.wall()
        actual,live=self.entries()
        if end!=actual or not end<=at<end+QUARTER:return None
        candidate=next((x for x in live if x.pair==pair),None)
        if candidate is None or not self.bot.observed_ranked(candidate):return None
        residual=(candidate.expires_ns-self.mono())//1_000_000
        expires=min(self.evaluation_ceiling(candidate,end,at),at+residual)
        if expires<=self.wall():return None
        g=candidate.generation
        key=(g,end,pair)
        if at>=end+60_000:
            self.ready_provisional_caps={k:v for k,v in self.ready_provisional_caps.items() if k[:2]==(g,end)}
            identity,expires=self.ready_provisional_caps.setdefault(key,(self._id(candidate),expires))
            if identity!=self._id(candidate):
                return None
        else:
            expires=self._provisional_cap(key,candidate,expires,at)
        if expires<=self.wall():return None
        return Scope(pair,candidate.symbol,g,end,expires)

    def _readable_authority(self,pair,end,now):
        # Evaluation only: verify durable first readiness and a separate
        # current OI witness; never consult, extend, or reissue a wire scope.
        if get_ident()!=self.owner or self.closed or not callable(self.verified_readiness_at):
            return None
        actual,live=self.entries()
        if end!=actual or not end<=now<end+QUARTER:
            return None
        row=next((r for r in live if r.pair==pair),None)
        if row is None or not self.bot.observed_ranked(row):
            return None
        at=self.verified_readiness_at(row,end,now)
        if type(at) is not int or not end<=at<=now<at+60_000:
            return None
        wall=self.wall()
        publisher_until=wall+(row.expires_ns-self.mono())//1_000_000
        expires=min(at+60_000,publisher_until,end+QUARTER)
        if self.wall()>=expires:
            return None
        return row,Scope(pair,row.symbol,row.generation,end,expires)

    def confirmed_readable_scope(self,pair,end):
        result=self._readable_authority(pair,end,self.wall())
        return result[1] if result is not None else None

    def accepted_readable_candidate(self,scope,now):
        if type(scope) is not Scope or not scope.end<=now<scope.expires:
            return None
        result=self._readable_authority(scope.pair,scope.end,now)
        if result is None:
            return None
        row,current=result
        return row if (current.symbol,current.generation)==(scope.symbol,scope.generation) else None

    def current_readable_scope(self,scope,now):
        return self.accepted_readable_candidate(scope,now) is not None

    def _provisional_cap(self,key,row,expires,now):
        epoch=key[:2]
        for book in (self.provisional_caps,self.provisional_witness,self.replaced_provisional_caps):
            for old in tuple(book):
                if old[:2]!=epoch:book.pop(old,None)
        seen=self.provisional_caps.setdefault(key,expires)
        stamp=getattr(row,'classification_seen_ns',None)
        identity=self._id(row)
        prior=self.provisional_witness.setdefault(key,(identity,stamp))
        if prior[0]!=identity:return 0
        if (seen<=now<expires and type(stamp) is int and type(prior[1]) is int
                and stamp>prior[1] and key not in self.last and key not in self.strict_retry
                and key not in self.retry_fenced):
            # A fresh market classifier can mint a new pre-wire scope for
            # the exact same still-live publication. No ticket, authorized
            # receipt, consumed callback or old clock is ever extended.
            self.replaced_provisional_caps.setdefault(key,set()).add(seen)
            self.provisional_witness[key]=(identity,stamp)
            self.provisional_caps[key]=seen=expires
        return min(expires,seen)

    def can_rebind_provisional(self,first,fresh,now):
        if ((first.pair,first.symbol,first.end)!=(fresh.pair,fresh.symbol,fresh.end)
                or not self.current(fresh,now)):
            return False
        if fresh.generation>first.generation:return True
        key=(fresh.generation,fresh.end,fresh.pair)
        return (fresh.generation==first.generation and first.expires<=now<fresh.expires
                and first.expires in self.replaced_provisional_caps.get(key,())
                and fresh.expires==self.provisional_caps.get(key))

    def remember_original_warm_offer(self,scope):
        # Called only after original entry advice requests its same-round
        # revisit. Retain that observed cursor, not an invented prior scope.
        if type(scope) is not Scope or get_ident()!=self.owner or self.closed:
            return
        key=(scope.generation,scope.end,scope.pair)
        if key in self.last or key in self.original_warm_offers:
            return
        candidate=self.accepted_candidate(scope)
        if candidate is None:
            return
        start=self.checkpoint_cursor(self.bot,candidate,scope.end)
        if (type(start) is int and start%FIVE==0 and
                3<=(scope.end-start)//FIVE<=260 and self.current(scope,self.wall())):
            self.original_warm_offers={k:v for k,v in self.original_warm_offers.items() if k[:2]==key[:2]}
            self.original_warm_offers[key]=(self._id(candidate),scope,start)

    @staticmethod
    def _id(row):
        return row.pair,row.symbol,row.generation,row.rank,row.ratio,row.received_at

    def _effective_retry_deadline(self, key, row, now, wire_expires):
        saved=self.strict_retry.get(key)
        if saved is None:return wire_expires
        identity,floor=saved
        if key in self.retry_fenced or self._id(row)!=identity:return 0
        # Never modify old Scope/task/receipt identity. This is a separate,
        # stricter permission over ALL owner/time and before-wire checks.
        instantaneous=now+max(0,(row.expires_ns-self.mono())//1_000_000)
        return min(wire_expires,floor,instantaneous)

    def _scope_deadline(self,key,row,scope,now):
        if scope.expires<=scope.end+60_000:
            return self._effective_retry_deadline(key,row,now,scope.expires)
        if now<scope.end+60_000 or self.ready_provisional_caps.get(key)!=(self._id(row),scope.expires):
            return 0
        instantaneous=now+max(0,(row.expires_ns-self.mono())//1_000_000)
        return min(scope.expires,instantaneous,self.evaluation_ceiling(row,scope.end,now))

    def effective_expiry(self,scope,now):
        # Pure accepted roster witness and monotonic clock, no observer/SQL.
        end,live=self.entries()
        if self.closed or scope.end!=end:return 0
        key=(scope.generation,scope.end,scope.pair)
        for row in live:
            if (row.pair,row.symbol,row.generation)==(scope.pair,scope.symbol,scope.generation):
                return self._scope_deadline(key,row,scope,now)
        return 0

    def transport_still_due(self,j,now):
        if not j.end<=now<j.expires<=j.end+60_000:
            return False
        # Detached transport reads only immutable accepted roster tuples and
        # the clock; the SQLite original authority stays on original owner.
        end,live=self.entries()
        return (not self.closed and j.end==end and end<=now<j.expires and
                any((x.generation,x.symbol,x.pair)==(j.generation,j.symbol,j.pair)
                    and now<self._effective_retry_deadline((j.generation,j.end,j.pair),x,now,j.expires)
                    for x in live))

    def reissue_unwired_minute(self,scope,ticket,physical_debit):
        if physical_debit is not False or get_ident()!=self.owner or type(scope) is not Scope:
            return False
        now=self.wall()
        if not scope.end<=now<scope.expires<=scope.end+60_000 or not self.current(scope,now):
            return False
        key=(scope.generation,scope.end,scope.pair)
        prior=self.last.get(key,())
        for i,(task,original,ticket_id) in enumerate(prior):
            if ticket_id!=ticket or original is not scope or type(task) is not Task or task.tf!='1m':
                continue
            # Only a verified unwired completion can retire the physical
            # attempt while preserving the admitted immutable original offer.
            self.ticket_counter+=1
            current=replace(task,request=now,ticket=self.ticket_counter)
            request=f'{self.namespace}.{scope.end}.{self.ticket_counter}'
            self.last[key]=(*prior[:i],(current,scope,request),*prior[i+1:])
            return True
        return False

    def accepted_candidate(self,s):
        if get_ident()!=self.owner or not self.current(s,self.wall()):return None
        end,live=self.entries()
        if end!=s.end:return None
        return next((x for x in live if (x.pair,x.symbol,x.generation)==(s.pair,s.symbol,s.generation)),None)

    def current(self,s,now):
        if get_ident()!=self.owner or self.closed:return False
        end,live=self.entries()
        if s.end!=end or not end<=now<s.expires:return False
        candidate=next((x for x in live if (x.symbol,x.generation)==(s.symbol,s.generation)),None)
        return (candidate is not None and candidate.pair==s.pair and self.bot.observed_ranked(candidate)
                and now<self._scope_deadline((s.generation,s.end,s.pair),candidate,s,now))

    def has_frozen_five_offer(self,symbol,start,end,count,marker):
        if get_ident()!=self.owner or self.closed or type(marker) is not tuple or marker[1]!=end:
            return False
        if type(symbol) is not str or not 3<=count<=499:return False
        g=marker[0]
        prior=self.last.get((g,end,symbol[:-4]+'/USDT:USDT'))
        if not prior:return False
        return any(type(task) is TailRequest and scope.symbol==symbol and task.limit==count and
                   start==end-count*FIVE and self.current(scope,self.wall()) for task,scope,_ in prior)

    def __call__(self,bot):
        if get_ident()!=self.owner or bot is not self.bot:return None
        end,live=self.entries()
        self.unplanned_checkpoint_count=0
        if not self.epoch(end):
            self.retry_fenced.update(k for k in self.strict_retry if k[1]==end)
            return None
        g=live[0].generation
        # Past the original acquisition window, this scope is evaluation-only
        # and never opens or repeats an original wire task. Its safe callback
        # requires a separately verified durable input owner.
        if self.wall()>=end+60_000:
            return g,end,()
        self.last={k:v for k,v in self.last.items() if k[0]==g and k[1]==end}
        self.strict_retry={k:v for k,v in self.strict_retry.items() if k[:2]==(g,end)}
        self.original_warm_offers={k:v for k,v in self.original_warm_offers.items() if k[:2]==(g,end)}
        self.retry_fenced={k for k in self.retry_fenced if k[:2]==(g,end)}
        live_keys={(p.generation,end,p.pair) for p in live}
        self.retry_fenced.update(k for k in self.strict_retry if k not in live_keys)
        self.original_warm_offers={k:v for k,v in self.original_warm_offers.items() if k in live_keys}
        jobs=[]
        for row in live:
            k=(g,end,row.pair)
            if not bot.observed_ranked(row):
                if k in self.strict_retry:self.retry_fenced.add(k)
                continue
            if k in self.retry_fenced:continue
            now=self.wall()
            lease=min(end+60000,now+max(0,(row.expires_ns-self.mono())//1_000_000))
            if k in self.provisional_caps:lease=self._provisional_cap(k,row,lease,now)
            if lease<=now:continue
            scope=Scope(row.pair,row.symbol,g,end,lease)
            prior=self.last.get(k)
            if prior is not None:
                # Preserve the original offer's honest start and scope identity.
                # A fallback may have now reached E but cannot retract already
                # witnessed provenance or create substitute receipt ownership.
                identity,floor=self.strict_retry[k]
                if identity!=self._id(row):
                    self.retry_fenced.add(k)
                    continue
                self.strict_retry[k]=(identity,min(floor,lease))
                if now<self.strict_retry[k][1] and self.current(prior[0][1],now):jobs.extend(prior)
                continue
            start=self.checkpoint_cursor(bot,row,end)
            count=(end-start)//FIVE if type(start) is int and start<end and (end-start)%FIVE==0 else 0
            # Once the original warmup finishes the cursor, it is too late
            # to infer an earlier start. Only reuse a still-current cursor
            # actually witnessed by original entry advice in this generation.
            recovered_offer=False
            if not 3<=count<=499:
                seen=self.original_warm_offers.get(k)
                if (seen is None or type(start) is not int or start%FIVE!=0
                        or not end-3*FIVE<start<=end or self._id(row)!=seen[0]
                        or not now<lease<=seen[1].expires or not self.current(seen[1],now)):
                    self.unplanned_checkpoint_count+=1
                    continue
                start=seen[2]
                recovered_offer=True
            group=[]
            if self.warm_prepare is not None and self.warm_prepare(scope,start):
                lanes=('1m',)
            elif not recovered_offer and (self.warm_keep_legacy is None or self.warm_keep_legacy(scope)):
                lanes=('15m','1m','5m')
            else:
                self.unplanned_checkpoint_count+=1
                continue
            for tf in lanes:
                self.ticket_counter+=1
                args=(row.symbol,tf,lease,now,end,self.ticket_counter,g)
                if tf=='5m':t=TailRequest(*args,count)
                elif tf=='1m' and lanes==('1m',) and self.warm_minute_span is not None:
                    t=Task(*args,minute_limit=self.warm_minute_span(scope))
                else:t=Task(*args)
                group.append((t,scope,f'{self.namespace}.{end}.{self.ticket_counter}'))
            self.last[k]=tuple(group)
            self.strict_retry[k]=(self._id(row),lease)
            jobs.extend(group)
        return g,end,tuple(jobs)

@dataclass(frozen=True)
class BuildInputs:
    wall:object
    mono:object
    make_unsigned:object
    checked_public:object
    verified_limit:object
    install_original_meters:object
    transport_attestation:object
    original_checkpoint_cursor:object
    protection_due:object
    original_protection_remaining:object
    record:object

def compiled_factory(inputs,expected_digest,*,startup_meter=None,diagnostic_sink=None,prior_seed_factory=None,history_review=None,prepared_history_review=False,reviewed_revision=None):
    """Pure construction of an explicit binder; does not connect or run here."""
    if type(inputs) is not BuildInputs:raise TypeError('COMPILED_INPUTS_REQUIRED')
    def bind(worker):
        if (type(expected_digest) is not str or len(expected_digest)!=64 or
                not all(c in '0123456789abcdef' for c in expected_digest) or
                any(not callable(getattr(inputs,k)) for k in inputs.__dataclass_fields__)):
            raise ValueError('INCOMPLETE_NATIVE_BUILD_ATTESTATIONS')
        b=worker.freqtrade
        plan=OriginalPlan(worker,inputs.wall,inputs.mono,inputs.original_checkpoint_cursor)
        if b.config['dry_run'] is not True or not b.native_paper_exchange():
            raise ValueError('NATIVE_DRY_RUN_REQUIRED')
        end=plan.startup_boundary()
        if end is None:raise OriginalStartupScopeUnavailable('NO_ORIGINAL_ACCEPTED_STARTUP_ROSTER')
        if type(end) is not int or end<=0 or end%QUARTER:raise ValueError('NO_VALID_CALENDAR_BOUNDARY')
        budget=None
        if startup_meter is not None:
            from startup_public_meter import OriginalStartupMeter
            if type(startup_meter) is not OriginalStartupMeter:
                raise ValueError('C11_UNVERIFIED_STARTUP_METER')
            budget=startup_meter.transfer(worker)
        if budget is None:budget=SharedNativeBudget(inputs.wall)
        meter=PublicWire(budget,inputs.checked_public,inputs.wall,inputs.verified_limit)
        # Hook native guarded before-wire paths, and require a reviewed detach
        # callback. Optional requests cannot have a separate effective quota.
        detach=inputs.install_original_meters(worker,meter)
        if not callable(detach):raise ValueError('NO_TRUSTED_NATIVE_METER_HOOK')
        current_meters=getattr(detach,'attest_current_sessions',None)
        fence=ProtectionFence()
        executor=None
        seed=None
        classifier=None
        try:
            if not callable(current_meters) or current_meters() is not True:
                raise ValueError('NATIVE_METER_ROUTE_NOT_CURRENT')
            if prepared_history_review is True:
                from refresh import fetch_on_metered_client
                if b.feed._reviewed_metaclass_fetch is not None:
                    raise ValueError('C11_CLASSIFIER_ALREADY_BOUND')
                api=b.exchange._api
                classifier=lambda:fetch_on_metered_client(api,meter,current_meters,mono=inputs.mono)
                b.feed._reviewed_metaclass_fetch=classifier
            executor=ExclusiveUnsignedTransport(unsigned_client=inputs.make_unsigned(worker),
                   incumbent_client=b.exchange._api,boundary_end=end,wall=inputs.wall,
                   mono=inputs.mono,exact_transport_boundary=inputs.transport_attestation,optional_wire=meter)
            queue=WorkerOwnedPublicQueue(worker=worker,wall=inputs.wall,authority=plan.current,
                   protection_due=lambda:fence.due() or not current_meters() or inputs.protection_due(),
                   executor=executor,global_limiter=budget,
                   handle_attested=lambda w,e,g:w is worker and e is executor and g is budget,
                   prewire_authority=plan.transport_still_due)
            ready=Ready(wall=inputs.wall,authority=plan.current,original_store=b.policy.bars,max_requests=96,effective_expiry=plan.effective_expiry)
            adapter=StrictInitialReadyAdapter(ready,inputs.wall,executor.capture)
            publisher=NativePublisher(b,wall=inputs.wall,authority=plan.current,
                         accepted_candidate=plan.accepted_candidate,remaining=inputs.original_protection_remaining)
            actor=OriginalReconfirm(adapter,inputs.wall,plan.current,inputs.protection_due)
            dispatcher=ReconfirmDispatch(actor,queue,plan)
            from historical_version import installed_catalog,HistoricalVersion
            from stage_pin import RUNTIME_SHA
            catalog=installed_catalog(RUNTIME_SHA)
            historical=None
            def permitted(scope,now):
                # This runs inside original synchronous PaperBot.process(), where the
                # optional-wire fence must remain held. Nothing here opens it.
                return (plan.current(scope,now) and fence.held is True and not fence.closed
                        and not inputs.protection_due() and plan.effective_expiry(scope,now)-now>30
                        and current_meters() is True and inputs.original_protection_remaining()>30)
            if catalog is not None:
                def market(scope):
                    return b.exchange._api.markets.get(scope.pair)
                historical=HistoricalVersion(catalog,market,permitted,inputs.wall)
            revision=(None if reviewed_revision is None else reviewed_revision.bind(
                b,plan,b.policy.bars,adapter,inputs.wall,permitted))
            site=OriginalCommitSite(b.policy.bars,inputs.wall,inputs.original_protection_remaining,publisher,actor,
                                    historical=historical,multilane_prepare=revision)
            run=UnifiedScheduler(worker=worker,queue=queue,adapter=adapter,original_plan=plan,
                  original_commit_and_decide=site,wall=inputs.wall,epoch_authority=plan.epoch,reconfirm=dispatcher,
                  diagnostic_sink=diagnostic_sink)
            run.warm.refresh=meter.history_refresh
            if history_review is not None:
                from owner_history_binding import OfflineHistoryReview
                if type(history_review) is not OfflineHistoryReview:
                    raise ValueError('PINNED_EXPLICIT_HISTORY_REVIEW_ONLY')
                run.isolated_history=history_review.attach(run,worker=worker)
            if prior_seed_factory is not None:
                seed=prior_seed_factory(worker,plan)
                run.isolated_prior_seed_loader=seed
            original_close=run.stop_and_join
            def close():
                plan.closed=True
                try:original_close()
                finally:
                    try:
                        if seed is not None:seed.close()
                    finally:
                        if classifier is not None and b.feed._reviewed_metaclass_fetch is classifier:
                            b.feed._reviewed_metaclass_fetch=None
                        detach()
            run.stop_and_join=close
            return NativeSchedulerBinding(worker,run,b.policy.bars,b.feed,expected_digest,fence),inputs.record
        except BaseException:
            try:
                if executor is not None:executor.stop_and_join()
            finally:
                try:
                    if 'run' in locals() and run.isolated_history is not None:run.isolated_history.close()
                    if seed is not None:seed.close()
                finally:
                    if classifier is not None and b.feed._reviewed_metaclass_fetch is classifier:
                        b.feed._reviewed_metaclass_fetch=None
                    detach()
            raise
    return bind
