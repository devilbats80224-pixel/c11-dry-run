"""Build-only owned scheduler wrapper. No constructing clients or accepting shadow inputs."""
from dataclasses import dataclass
from threading import get_ident
from native_entry_owner import NativeEntryOwner

@dataclass(frozen=True)
class NativeSchedulerBinding:
    runtime_owner:object
    scheduler:object
    store:object
    intake:object
    exact_worker_seal:str
    protection_fence:object=None

class OwnedWrapper:
    def __init__(self, worker, binding, *, expected_seal):
        if type(binding) is not NativeSchedulerBinding or binding.runtime_owner is not worker:
            raise ValueError('NATIVE_SINGLE_WORKER_OWNER')
        if binding.exact_worker_seal!=expected_seal or not expected_seal:
            raise ValueError('NATIVE_SCHEDULER_UNSEALED')
        b=worker.freqtrade
        if b.policy.bars is not binding.store or b.feed is not binding.intake:
            raise ValueError('NATIVE_ORIGINAL_STORE_AND_INTAKE')
        if b.config['dry_run'] is not True or type(b.strategy).__name__!='C11ManagedPaper':
            raise ValueError('NATIVE_PAPER_ONLY')
        self.owner=get_ident()
        self.bot=b
        self.fence=binding.protection_fence
        self.scheduler=binding.scheduler
        for name in ('consume_ready_only','offer_after_protection','covers_native_public','stop_and_join'):
            if not callable(getattr(self.scheduler,name,None)):
                raise ValueError('NATIVE_EXISTING_SCHEDULER_CONTRACT')
        self.yielded=False

    def install(self, record):
        if get_ident()!=self.owner or self.yielded or hasattr(self.bot.strategy,'_c11_native_entry_owner'):
            raise RuntimeError('SCHEDULER_ALREADY_BOUND')
        # This is the existing owner-backed scheduler's ready-only ingress;
        # no transport client or worker thread is created here.
        import stage_pin
        enabled = getattr(stage_pin, "DURABLE_REVISIT_ANALYSIS_ENABLED", False) is True
        self.bot.strategy._c11_native_entry_owner=NativeEntryOwner(
            trusted_owner_input=self.scheduler.consume_ready_only,record=record,
            provisional=getattr(self.scheduler,'provisional_scope',None),
            revisit_ready=getattr(self.scheduler,'revisit_ready',None),
            revisit_diagnostic=getattr(self.scheduler,'explain_public_revisit',None),
            revisit_frame=self.scheduler.original_revisit_frame if enabled else None,
            revisit_frame_current=self.scheduler.revisit_frame_current if enabled else None,
            discard_revisit_frame=self.scheduler.discard_revisit_frame if enabled else None)
        self.bot._c11_owned_data_lane=self
        self.yielded=True

    def before_protection(self):
        if self.fence is not None:self.fence.begin()
        if get_ident()!=self.owner:raise RuntimeError('NATIVE_SINGLE_WORKER_OWNER')
        capture=getattr(self.scheduler.isolated_history,'before_protection',None)
        if capture is not None:capture()

    def after_protection(self):
        if get_ident()!=self.owner or not self.yielded:
            raise RuntimeError('NATIVE_SINGLE_WORKER_OWNER')
        # Never call a wire endpoint from this synchronous protection loop.
        if self.fence is not None:self.fence.release_after_observation()
        self.scheduler.offer_after_protection(self.bot)

    def covers_native_public(self,symbol,width,start,end,limit):
        if get_ident()!=self.owner or not self.yielded:
            return False
        return self.scheduler.covers_native_public(symbol,width,start,end,limit) is True

    def close(self):
        if get_ident()!=self.owner:raise RuntimeError('NATIVE_SINGLE_WORKER_OWNER')
        if self.yielded:
            if self.fence is not None:self.fence.close()
            del self.bot.strategy._c11_native_entry_owner
            del self.bot._c11_owned_data_lane
            self.yielded=False
            # No detached owner tasks may survive native worker exit.
            self.scheduler.stop_and_join()

class OriginalStartupScopeUnavailable(ValueError):
    """Only expected authentic source wait permits deferred binding."""

class DeferredOwnedWrapper:
    def __init__(self,worker,factory,seal):
        self.worker,self.factory,self.seal=worker,factory,seal
        self.bot=worker.freqtrade
        self.thread=get_ident()
        self.active=None
        self.gate=NativeEntryOwner(trusted_owner_input=lambda *_:('HOLD_ORIGINAL_STARTUP_SCOPE',None),
                                   record=lambda *_:None)
        if self.bot.config['dry_run'] is not True or type(self.bot.strategy).__name__!='C11ManagedPaper':
            raise ValueError('NATIVE_PAPER_ONLY')
        if hasattr(self.bot.strategy,'_c11_native_entry_owner') or hasattr(self.bot,'_c11_owned_data_lane'):
            raise RuntimeError('SCHEDULER_ALREADY_BOUND')
        self.bot.strategy._c11_native_entry_owner=self.gate
        self.bot._c11_owned_data_lane=self

    def _owner(self):
        if get_ident()!=self.thread:raise RuntimeError('NATIVE_SINGLE_WORKER_OWNER')

    def before_protection(self):
        self._owner()

    def after_protection(self):
        self._owner()
        if self.active is not None:raise RuntimeError('NATIVE_DEFERRED_OWNER_NOT_REPLACED')
        if self.bot._c11_owned_data_lane is not self or self.bot.strategy._c11_native_entry_owner is not self.gate:
            raise RuntimeError('NATIVE_DEFERRED_OWNER_CHANGED')
        try:
            b,record=self.factory(self.worker)
        except OriginalStartupScopeUnavailable:
            return
        wrapper=OwnedWrapper(self.worker,b,expected_seal=self.seal)
        del self.bot.strategy._c11_native_entry_owner
        wrapper.install(record)
        self.active=wrapper
        # Newly attached lane's protection fence remains closed; it may offer
        # only after the next full original worker protection cycle.

    def covers_native_public(self,*_):
        self._owner()
        return False

    def close(self):
        self._owner()
        if self.active is not None:
            self.active.close()
        else:
            if self.bot.strategy._c11_native_entry_owner is self.gate:
                del self.bot.strategy._c11_native_entry_owner
            if self.bot._c11_owned_data_lane is self:
                del self.bot._c11_owned_data_lane

def install_if_present(worker, compile_time_factory, compiled_expected_seal):
    if compile_time_factory is None and compiled_expected_seal is None:return None
    if (not callable(compile_time_factory) or type(compiled_expected_seal) is not str
            or len(compiled_expected_seal)!=64 or any(c not in '0123456789abcdef' for c in compiled_expected_seal)):
        raise ValueError('COMPILED_OWNED_SCHEDULER_PIN_REQUIRED')
    try:
        binding,rec=compile_time_factory(worker)
    except OriginalStartupScopeUnavailable:
        return DeferredOwnedWrapper(worker,compile_time_factory,compiled_expected_seal)
    owner=OwnedWrapper(worker,binding,expected_seal=compiled_expected_seal)
    owner.install(rec)
    return owner
