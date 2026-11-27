"""Nonserializable local handoff after the incumbent one-shot, no new signer."""
from contextvars import ContextVar
from dataclasses import dataclass
from hashlib import sha256
import json,os,re
from pathlib import Path
from threading import get_ident

_HEX=re.compile(r'[0-9a-f]{64}')
_RUNTIME=Path('/opt/c11_owned_modules/release_stage_runtime.json')
_AUTH=ContextVar('C11_PRIVATE_HISTORY_STARTUP',default=None)
_TOKEN=object()
_PINNED_STANDARD_IMPORT_ROOTS_SHA='2f6cee054742b03ebb87ee8fd691ad39d6b385cf21dffeb72ef3e8bfaa98cc65'
_ENTRY=Path('/opt/c11_owned_modules/release_stage.py')
# Actual OCI image identity is pinned by the approved external launcher. This
# module checks exact verified in-process stage bytes, not a self-reported OCI ID.

class Hold(ValueError):pass

@dataclass(frozen=True)
class Site:
    entrypoint:str; manifest:str; ledger:tuple; store:tuple; writer:tuple
    def __post_init__(self):
        if any(type(x) is not str or _HEX.fullmatch(x) is None for x in (self.entrypoint,self.manifest)):
            raise Hold('HISTORY_INVALID_SITE')
        if any(type(x) is not tuple or len(x)!=2 or any(type(v) is not int or v<=0 for v in x)
                for x in (self.ledger,self.store,self.writer)):
            raise Hold('HISTORY_INVALID_INODE')

class Receipt:
    __slots__=('_site','_state','_proof','_owner','_used','_revoked')
    def __init__(self,token,site,state,consumed_hash):
        if token is not _TOKEN or type(site) is not Site or type(consumed_hash) is not str or not _HEX.fullmatch(consumed_hash):
            raise Hold('HISTORY_ONE_SHOT_UNCONSUMED')
        self._site=site;self._state=state;self._proof=consumed_hash
        self._owner=(os.getpid(),get_ident());self._used=False;self._revoked=False
    def __reduce_ex__(self,*_):
        raise TypeError('HISTORY_LOCAL_CAPABILITY_NOT_SERIALIZABLE')
    def _verify(self,site,state):
        if self._revoked or self._used or self._owner!=(os.getpid(),get_ident()):
            raise Hold('HISTORY_LOCAL_RECEIPT_REPLAY_OR_REVOKED')
        if state is not self._state or site!=self._site:
            raise Hold('HISTORY_IMAGE_OR_CANONICAL_STORE_MISMATCH')
        state.check_locked()
    def revoke(self):
        self._revoked=True

def _env_guard(env):
    if any(name.startswith(('C11_HISTORY','FREQTRADE__','PYTHONHOME')) for name in env):
        raise Hold('HISTORY_ENV_INJECTION_REFUSED')
    roots=env.get('PYTHONPATH')
    if roots is not None and (type(roots) is not str or sha256(roots.encode()).hexdigest()!=_PINNED_STANDARD_IMPORT_ROOTS_SHA):
        raise Hold('HISTORY_ENV_INJECTION_REFUSED')

def _publish(receipt,site,state,env):
    _env_guard(env)
    if type(receipt) is not Receipt:raise Hold('HISTORY_ONE_SHOT_UNCONSUMED')
    if _AUTH.get() is not None:raise Hold('HISTORY_STARTUP_CONTEXT_ALREADY_BOUND')
    receipt._verify(site,state)
    return _AUTH.set(receipt)

def _factory_check(site,state):
    _env_guard(os.environ)
    r=_AUTH.get()
    if type(r) is not Receipt:raise Hold('HISTORY_ONE_SHOT_UNCONSUMED')
    r._verify(site,state)
    return r

def _factory_claim(site,state):
    r=_factory_check(site,state);r._used=True
    return r

def unpublish(ctx,receipt):
    receipt.revoke();_AUTH.reset(ctx)

def trusted_site(state,one_shot):
    from one_shot import OneShot
    from outer_seed_lock import ExistingOuterWriter,private_identity,ROOT,LEDGER,LOCK
    if type(one_shot) is not OneShot:
        raise Hold('HISTORY_ORIGINAL_ONE_SHOT_REQUIRED')
    outer=one_shot.scope.outer
    if type(outer) is not ExistingOuterWriter or outer.state is not state or outer.revoked or outer.fd!=state.fd:
        raise Hold('HISTORY_ORIGINAL_OWNED_LOCK_REQUIRED')
    state.check_locked()
    manifest=sha256(_RUNTIME.read_bytes()).hexdigest()
    if manifest!=one_shot.manifest or manifest!=outer.manifest:
        raise Hold('HISTORY_RELEASE_MANIFEST_MISMATCH')
    entrypoint=sha256(_ENTRY.read_bytes()).hexdigest()
    files=json.loads(_RUNTIME.read_bytes())['files']
    if files.get(str(_ENTRY))!=entrypoint:
        raise Hold('HISTORY_RELEASE_ENTRYPOINT_MISMATCH')
    return Site(entrypoint,manifest,private_identity(LEDGER),private_identity(ROOT/'causal-candles.sqlite'),private_identity(LOCK))

def consume_then_publish(one_shot,state,cleanup,history_startup):
    from one_shot import OneShot
    if type(one_shot) is not OneShot:raise Hold('HISTORY_ORIGINAL_ONE_SHOT_REQUIRED')
    _env_guard(os.environ)
    before=trusted_site(state,one_shot)
    if before.manifest!=history_startup.expected_manifest:
        raise Hold('HISTORY_RELEASE_MANIFEST_MISMATCH')
    mark=one_shot.consume(state)
    after=trusted_site(state,one_shot)
    if before!=after:raise Hold('HISTORY_CANONICAL_IDENTITY_CHANGED')
    receipt=Receipt(_TOKEN,after,state,mark)
    t=_publish(receipt,after,state,os.environ)
    cleanup.callback(unpublish,t,receipt)
    # Object binds to verified state; no admission hash/permit passes to child.
    history_startup.bind_private_source(after,state,lambda:trusted_site(state,one_shot))
    return receipt

def require_claimed_live_receipt(r):
    # A socket/child frame never contains this object, its digest or context.
    if (type(r) is not Receipt or _AUTH.get() is not r or not r._used or r._revoked or
            r._owner!=(os.getpid(),get_ident())):
        raise Hold('HISTORY_BOUND_STARTUP_RECEIPT_REQUIRED')
    r._state.check_locked()
    return r
