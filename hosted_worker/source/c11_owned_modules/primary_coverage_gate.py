"""Build-only native policy crossing; it does not create or authorize trade orders."""
from contextlib import contextmanager
from dataclasses import dataclass
from threading import get_ident

MINUTE=60_000

@dataclass(frozen=True)
class NativePrimaryEvidence:
    pair:str
    symbol:str
    generation:int
    end_ms:int
    owner_committed_ms:int
    expires_ms:int
    canonical_frame_sha256:str
    primary:tuple
    rpd_state:object

class NativePrimaryHandoff:
    def __init__(self, *, wall, original_scope, frame_sha, scoped_atr=None, scoped_candidate=None,
                 verified_ready_at=None):
        self.owner=get_ident()
        self.wall=wall
        self.original_scope=original_scope
        self.frame_sha=frame_sha
        self.active=None
        self.scoped_atr=scoped_atr
        self.scoped_candidate=scoped_candidate
        self.verified_ready_at=verified_ready_at

    @contextmanager
    def committed(self,event):
        if get_ident()!=self.owner or self.active is not None:
            raise RuntimeError('PRIMARY_SINGLE_OWNER_SCOPE')
        if type(event) is not NativePrimaryEvidence:
            raise TypeError('NATIVE_PRIMARY_EVIDENCE_REQUIRED')
        self.active=event
        try:yield
        finally:self.active=None

    def entry_atr(self,pair,candidate,end_ms):
        if not callable(self.scoped_atr) or self.resolve(pair,candidate,end_ms) is None:return None
        return self.scoped_atr(pair,end_ms)

    def resolve(self, pair, candidate, end_ms):
        event=self.active
        if get_ident()!=self.owner or event is None:return None
        now=self.wall()
        if not (event.pair,event.symbol,event.generation,event.end_ms)==(
                pair,candidate.symbol,candidate.generation,end_ms):return None
        if not end_ms<=event.owner_committed_ms<=now<event.expires_ms:return None
        if not self.original_scope(candidate,event,now):return None
        if event.expires_ms>end_ms+MINUTE:
            # Only an original owner can authenticate the complete durable
            # frame's readiness; a bare event or HTTP receipt is insufficient.
            if not callable(self.verified_ready_at):return None
            ready_at=self.verified_ready_at(candidate,event,now)
            if (type(ready_at) is not int or not end_ms<=ready_at<=event.owner_committed_ms
                    or event.expires_ms>min(ready_at+MINUTE,end_ms+15*MINUTE)):
                return None
        if self.frame_sha(pair,end_ms)!=event.canonical_frame_sha256:return None
        if type(event.primary) is not tuple or len(event.primary)!=3 or event.rpd_state is None:return None
        return event.primary,event.rpd_state

def native_primary_read(owner_hook, bot, policy, pair, candidate, end, errors, original_readiness):
    if owner_hook is None:
        return original_readiness(bot,policy,pair,candidate.symbol,end,errors)
    if type(owner_hook) is not NativePrimaryHandoff:
        return None,None
    bound=owner_hook.resolve(pair,candidate,end)
    # An active provisional route holds on uncertainty rather than falling
    # through to a known-incompatible incumbent primary cache.
    return bound if bound is not None else (None,None)
