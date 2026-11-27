"""Pin-candidate site callbacks; no network, FF activation or fallback."""
import inspect
import sqlite3
import ssl
from threading import get_ident

class ProtectionFence:
    """Same owner marks protected phases; transport can only query the flag."""
    def __init__(self):
        self.owner=get_ident()
        self.held=True
        self.closed=False
    def _owner(self):
        if get_ident()!=self.owner:raise RuntimeError('NATIVE_PROTECTION_OWNER')
    def begin(self):
        self._owner()
        self.held=True
    def release_after_observation(self):
        self._owner()
        if not self.closed:self.held=False
    def due(self):
        return self.held or self.closed
    def close(self):
        self._owner()
        self.closed=True

def verify_tls(value):
    return value is True or (type(value) is ssl.SSLContext and
           value.check_hostname is True and value.verify_mode==ssl.CERT_REQUIRED)

def install_native_meter(worker,meter,expected_exchange):
    """Attach only pre-existing private in-process CCXT public transport guards."""
    bot=worker.freqtrade
    exchange=bot.exchange
    if type(exchange) is not expected_exchange or not bot.config.get('dry_run') is True:
        raise ValueError('EXACT_NATIVE_PAPER_EXCHANGE_REQUIRED')
    api=getattr(exchange,'_api',None)
    api_async=getattr(exchange,'_api_async',None)
    sync_session=getattr(api,'session',None)
    async_session=getattr(api_async,'session',None)
    if sync_session is None or async_session is None or sync_session is async_session:
        raise ValueError('BOTH_ORIGINAL_NATIVE_SESSIONS_REQUIRED')
    original_send=getattr(sync_session,'send',None)
    original_request=getattr(async_session,'_request',None)
    if not callable(original_send) or not inspect.iscoroutinefunction(original_request):
        raise ValueError('ORIGINAL_NATIVE_WIRE_BOUNDARIES_REQUIRED')
    param=inspect.signature(original_request).parameters.get('ssl')
    if param is None or not verify_tls(param.default):
        raise ValueError('ORIGINAL_NATIVE_TLS_SCOPE_REQUIRED')
    send=meter.wrap_sync_send(original_send)
    request=meter.wrap_async_request(original_request,verify_tls,param.default)
    try:
        sync_session.send=send
        async_session._request=request
    except BaseException:
        if sync_session.send is send:sync_session.send=original_send
        if async_session._request is request:async_session._request=original_request
        raise
    def attest_current_sessions():
        # CCXT can replace a session; do not continue an optional budget if
        # incumbent can be using an unmetered transport after reconnection.
        return (getattr(exchange,'_api',None) is api and getattr(exchange,'_api_async',None) is api_async
                and getattr(api,'session',None) is sync_session and getattr(api_async,'session',None) is async_session
                and sync_session.send is send and async_session._request is request)
    def detach():
        if sync_session.send is send:sync_session.send=original_send
        if async_session._request is request:async_session._request=original_request
    detach.attest_current_sessions=attest_current_sessions
    return detach

class OriginalCommitSite:
    """Requires actual native same-owner published lineage, never a historical seal."""
    def __init__(self,store,wall,remaining,publish_original_input,reconfirm=None,*,historical=None,multilane_prepare=None):
        if not callable(getattr(store,'record_owner_primary_batch',None)) or not callable(publish_original_input):
            raise ValueError('NATIVE_ATOMIC_ORIGINAL_COMMIT_AND_PROJECTION_REQUIRED')
        self.store=store
        self.wall=wall
        self.remaining=remaining
        self.publish=publish_original_input
        self.reconfirm=reconfirm
        self.historical=historical
        self.multilane_prepare=multilane_prepare
        self.last_atomic_commit=False
        self.last_atomic_refusal=None
        self.last_atomic_phases=()

    def __call__(self,scope,adapter,frame,callback):
        status, prepared=self._prepare(scope,adapter)
        if prepared is None:return status,None
        return self._publish_prepared(prepared,scope,adapter,frame,callback)

    def prepare_flat_analyzer(self,scope,adapter):
        # Separate single-owner branch. Still the same native consume,
        # current exact triad and SQLite transaction as the ordinary path.
        return self._prepare(scope,adapter,allow_multilane=True)

    def wants_multilane(self,scope):
        return (self.multilane_prepare is not None and callable(getattr(self.multilane_prepare,'claims',None))
                and self.multilane_prepare.claims(scope) is True)

    def _prepare(self,scope,adapter,*,allow_multilane=False):
        self.last_atomic_commit=False
        self.last_atomic_refusal=None
        self.last_atomic_phases=()
        adapter.last_normalization_refusal=None
        from bridge_protocol import Refused
        from no_sqlite_busy import optional_observation
        if adapter.ready.store is not self.store:return 'HOLD_WRONG_ORIGINAL_STORE',None
        if not allow_multilane and self.wants_multilane(scope):
            return 'HOLD_OPG_REVISION_SCOPED_ANALYZER_REQUIRED',None
        version=None
        multi=None
        def commit(db,s,batch,current):
            nonlocal version,multi
            if db is not self.store:
                self.last_atomic_refusal='ATOMIC_STORE_IDENTITY'
                return None
            authority_phase='ATOMIC_AUTHORITY_BEFORE_ROWS'
            def checked_current():
                nonlocal authority_phase
                accepted=current()
                if accepted is not True:self.last_atomic_refusal=authority_phase
                authority_phase='ATOMIC_AUTHORITY_AFTER_ROWS'
                return accepted
            def busy(code):
                self.last_atomic_refusal='ATOMIC_SQLITE_BUSY' if code==sqlite3.SQLITE_BUSY else 'ATOMIC_SQLITE_LOCKED'
            try:
                def native():
                    nonlocal multi
                    db.owner_atomic_phases=()
                    try:
                        if allow_multilane and self.wants_multilane(s):
                            from multilane_history_view import FrozenReadView
                            if owner_input is adapter:
                                multi=self.multilane_prepare(s,batch,adapter)
                            else:
                                multi=self.multilane_prepare.from_reconfirm(s,batch,owner_input)
                            if type(multi) is not FrozenReadView:
                                raise Refused('C11_REVISION_OWNER_OR_BATCH')
                        if multi is not None:
                            append=getattr(self.multilane_prepare,'append',None)
                            db.record_owner_primary_batch(s.symbol,batch,checked_current,multi=multi,scope=s,
                                wall=self.wall,revision_append=append)
                        elif self.historical is not None and owner_input is adapter:
                            version=db.record_owner_primary_batch(s.symbol,batch,checked_current,
                                                                 historical=self.historical,scope=s)
                        else:
                            db.record_owner_primary_batch(s.symbol,batch,checked_current)
                        self.last_atomic_commit=True
                        return batch
                    finally:
                        self.last_atomic_phases=('ATOMIC_NATIVE_ENTERED',)+tuple(db.owner_atomic_phases)
                out=optional_observation(native,(db.db,),on_busy=busy)
                if out=='HOLD_OBSERVER_BUSY':
                    self.last_atomic_phases+=('ATOMIC_SQLITE_BUSY_AROUND_NATIVE',)
                else:
                    self.last_atomic_phases+=('ATOMIC_SQLITE_CONTEXT_RETURNED',)
            except Refused as err:
                if self.last_atomic_refusal is None:
                    self.last_atomic_refusal={
                        'C11_OWNER_BATCH_SHAPE':'ATOMIC_BATCH_SHAPE',
                        'C11_OWNER_BATCH_AUTHORITY':'ATOMIC_AUTHORITY_UNSPECIFIED',
                        'C11_CANDLE_CAUSAL_ROW':'ATOMIC_INVALID_ROW',
                        'C11_CLOSED_CANDLE_CHANGED':'ATOMIC_CANONICAL_CONFLICT',
                    }.get(str(err),'ATOMIC_OTHER_REFUSED')
                return None
            return None if out=='HOLD_OBSERVER_BUSY' else out
        owner_input=adapter
        if self.reconfirm is not None:
            from reconfirm_input import OriginalReconfirm
            if type(self.reconfirm) is not OriginalReconfirm or self.reconfirm.adapter is not adapter:
                return 'HOLD_WRONG_ORIGINAL_RECONFIRM_OWNER',None
            if self.reconfirm.owns_original_scope(scope):
                if allow_multilane and self.wants_multilane(scope) and not callable(
                        getattr(self.multilane_prepare,'from_reconfirm',None)):
                    return 'HOLD_OPG_REVISION_RECONFIRM_UNPROVEN',None
                owner_input=self.reconfirm
        if owner_input is adapter:
            status,batches=adapter.ready.consume(scope,normalize=adapter.normalize,
                             atomic_commit=commit,remaining_for_protections=self.remaining)
            if status!='ORIGINAL_OWNER_COMMITTED_READY':return status,None
        else:
            status,batches=owner_input.consume_original(scope,commit,self.remaining)
            if status!='ORIGINAL_OWNER_RECONFIRMED_READY':return status,None
        if not adapter.ready.authority(scope,self.wall()):return 'HOLD_SCOPE_CHANGED_AFTER_COMMIT',None
        return status,(scope,adapter,owner_input,batches,version,multi)

    def _publish_prepared(self,prepared,scope,adapter,frame,callback):
        proof,a,owner_input,batches,version,multi=prepared
        if (proof is not scope or a is not adapter or self.store.db.in_transaction
                or not adapter.ready.authority(scope,self.wall())):
            return 'HOLD_SCOPE_CHANGED_AFTER_COMMIT',None
        # Usual real strategy-frame/current authority and publisher checks.
        if multi is not None:
            if not multi.bound(scope,self.wall()):return 'HOLD_ORIGINAL_PUBLISHER_LEASE',None
            return self.publish(scope,owner_input,batches,frame,callback,multi=multi)
        if version is not None:
            if not self.historical.protection(scope):return 'HOLD_ORIGINAL_PUBLISHER_LEASE',None
            return self.publish(scope,owner_input,batches,frame,callback,version=version,version_current=self.historical.protection)
        return self.publish(scope,owner_input,batches,frame,callback)
