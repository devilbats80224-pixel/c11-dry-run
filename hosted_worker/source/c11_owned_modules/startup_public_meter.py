"""Scratch: passively meter exactly the original startup wire, with no new traffic."""
from contextlib import contextmanager
from contextvars import ContextVar
from inspect import iscoroutinefunction,signature
from threading import get_ident
from public_meter import PublicWire
from shared_native_budget import SharedNativeBudget
from native_site_callbacks import verify_tls

HOOK=ContextVar('c11_existing_startup_public_capture',default=None)
QUARTER=900_000
MINUTE=60_000

class BootWire(PublicWire):
    def __init__(self,budget,expected,wall,limit):
        super().__init__(budget,expected,wall,limit)
        self.read_start=ContextVar('c11_boot_wire_started',default=None)
    def before(self,*args):
        at=self.wall()
        try:self.budget.set_scope(0,at//QUARTER*QUARTER)
        except (TypeError,ValueError):
            # Diagnostics must not stop the incumbent's guarded public call.
            self.budget.mark_unaccounted()
            self.read_start.set(None)
            return super().before(*args)
        self.read_start.set(at)
        return super().before(*args)
    def after(self,response):
        start=self.read_start.get()
        at=self.wall()
        # At minute boundaries an honest response is not evidence of the
        # current venue usage bucket if it started in the preceding minute.
        if type(start) is not int or not start<=at or start//MINUTE!=at//MINUTE:
            return False
        return super().after(response)

class OriginalStartupMeter:
    def __init__(self,wall,guard,limit,*,keys):
        self.thread=get_ident()
        self.watcher=BootWire(SharedNativeBudget(wall),guard,wall,limit)
        self.keys=keys
        self.done=False
        self.clients={}
        self.sessions={}
        self.releases=[]
    @contextmanager
    def claim_original_constructor(self):
        if get_ident()!=self.thread or self.done or HOOK.get() is not None:
            raise RuntimeError('C11_BOOT_ORIGINAL_CTOR_OWNER')
        pos=HOOK.set(self)
        try:yield
        except BaseException:
            self.close()
            raise
        finally:HOOK.reset(pos)
    def _attach(self,client,sync):
        session=client.session
        if session is None or sync in self.sessions:return
        if sync:
            original=session.send
            wrap=self.watcher.wrap_sync_send(original)
            attr='send'
        else:
            original=session._request
            ssl=signature(original).parameters.get('ssl')
            if not iscoroutinefunction(original) or ssl is None or not verify_tls(ssl.default):
                raise ValueError('C11_BOOT_EXACT_TLS_DEFAULT')
            wrap=self.watcher.wrap_async_request(original,verify_tls,ssl.default)
            attr='_request'
        setattr(session,attr,wrap)
        self.sessions[sync]=(session,attr,wrap)
        self.releases.append((session,attr,wrap,original))
    def original_client(self,client,sync):
        if (get_ident()!=self.thread or self.done or HOOK.get() is not self or type(sync) is not bool
                or sync in self.clients or client.id!='binance' or any(getattr(client,k,None) for k in self.keys)):
            self.watcher.budget.mark_unaccounted()
            return client
        self.clients[sync]=client
        if sync:
            try:self._attach(client,sync)
            except (AttributeError,TypeError,ValueError):self.watcher.budget.mark_unaccounted()
        else:
            original=client.open
            def open_session(*args,**kwargs):
                result=original(*args,**kwargs)
                captured=self.sessions.get(False)
                if captured is None:
                    try:self._attach(client,False)
                    except (AttributeError,TypeError,ValueError):self.watcher.budget.mark_unaccounted()
                elif captured[0] is not client.session:
                    # Don't count header from a new/unknown session as startup
                    # proof; the original command remains under its public guard.
                    self.watcher.budget.mark_unaccounted()
                return result
            client.open=open_session
            self.releases.append((client,'open',open_session,original))
        return client
    def close(self):
        self.done=True
        for obj,key,wrapper,original in reversed(self.releases):
            if getattr(obj,key,None) is wrapper:setattr(obj,key,original)
        self.releases.clear()
    def transfer(self,worker):
        if get_ident()!=self.thread or self.done or HOOK.get() is not None:
            raise ValueError('C11_BOOT_HANDOFF_OWNER')
        b=worker.freqtrade
        ex=b.exchange
        try:
            if (b.config.get('dry_run') is not True or set(self.sessions)!={False,True}
                    or self.clients.get(True) is not ex._api or self.clients.get(False) is not ex._api_async
                    or any(getattr(obj,key,None) is not wrapper for obj,key,wrapper,_ in self.releases)):
                return None
            if any(self.clients[sync].session is not row[0] for sync,row in self.sessions.items()):return None
            return self.watcher.budget
        finally:self.close()

def original_bootstrap_client(client,sync):
    tap=HOOK.get()
    return client if tap is None else tap.original_client(client,sync)
