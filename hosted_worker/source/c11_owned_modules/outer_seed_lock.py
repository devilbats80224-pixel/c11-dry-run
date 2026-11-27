"""Reuse existing PaperState's canonical writer lock; not an activation grant."""
import os
from pathlib import Path
import stat
from threading import get_ident
from paper_runtime import PaperState, verify_state_file
from durable_loader import GUARD_PATH, PaperStoreAttestation, SeedLoader
from binding import HASH,Hold

ROOT=Path('/c11-paper')
LEDGER=ROOT/'trades.c11.paper.sqlite'
LOCK=ROOT/'c11-paper.writer.lock'
CREDENTIAL_FIELDS=frozenset(('apikey','key','secret','password','uid','accountid','walletaddress','privatekey'))

def private_identity(p):
    verify_state_file(p)
    f=p.lstat()
    if not (stat.S_ISREG(f.st_mode) and f.st_nlink==1 and f.st_uid==os.getuid() and
            stat.S_IMODE(f.st_mode)==0o600 and f.st_dev==ROOT.lstat().st_dev):
       raise Hold('C11_CANONICAL_FILE_IDENTITY')
    return (f.st_dev,f.st_ino)

def mode():
    m=Path('/proc/self/mountinfo').read_text().splitlines()
    matches=[(a.split(),b.split()) for a,b in (r.split(' - ',1) for r in m) if a.split()[4]==str(ROOT)]
    if len(matches)!=1:raise Hold('C11_CANONICAL_MOUNT')
    return matches[0][1][0]

class ExistingOuterWriter:
 def __init__(self,state,*,review_catalog,review_pin,runtime_manifest_sha,expected_manifest_sha,isolated_test=False):
    if type(state) is not PaperState or not HASH.fullmatch(runtime_manifest_sha) or runtime_manifest_sha!=expected_manifest_sha:
       raise Hold('C11_REVIEWED_EXISTING_WRITER_REQUIRED')
    state.check_locked()
    if (mode()=='tmpfs') is not isolated_test:
       raise Hold('C11_TEST_OR_RELEASE_SCOPE')
    self.state,self.mode=state,mode()
    self.catalog,self.pin=Path(review_catalog),review_pin
    self.manifest,self.expected=runtime_manifest_sha,expected_manifest_sha
    self.owner=get_ident()
    self.isolated_test=isolated_test
    self.fd=state.fd
    self.lock_id=private_identity(LOCK)
    if not os.path.samestat(os.fstat(self.fd),LOCK.lstat()):raise Hold('C11_OUTER_WRITER_ID')
    self.revoked=False
    self.loaded=False

 def revoke(self):self.revoked=True

 def open(self,worker,plan):
    bot=worker.freqtrade
    self.state.check_locked()
    if self.revoked or self.loaded or get_ident()!=self.owner or bot is not plan.bot:
       raise Hold('C11_ONE_LOADER_PER_OUTER_WRITER')
    def exact_paper_scope():
       cfg=bot.config
       exchange=cfg.get('exchange',{})
       return (cfg.get('dry_run') is True and cfg.get('db_url')=='sqlite:///'+str(LEDGER)
           and bot.native_paper_exchange() and not any(k.lower() in CREDENTIAL_FIELDS and v for k,v in exchange.items()))
    if not exact_paper_scope():raise Hold('C11_PAPER_ONLY_SAME_LEDGER')
    store=bot.policy.bars
    ledger_id=private_identity(LEDGER)
    causal_id=private_identity(GUARD_PATH)
    if len({self.lock_id,ledger_id,causal_id})!=3:raise Hold('C11_SEPARATE_CANONICAL_FILE_IDENTITIES')

    def current(exact_store,db_path):
       try:
          if (self.revoked or get_ident()!=self.owner or self.fd!=self.state.fd or
              exact_store is not store or db_path!=GUARD_PATH or
              mode()!=self.mode or not exact_paper_scope() or
              not os.path.samestat(os.fstat(self.fd),LOCK.lstat())):return False
          self.state.check_locked()
          return (private_identity(LOCK),private_identity(LEDGER),private_identity(GUARD_PATH))==(self.lock_id,ledger_id,causal_id)
       except Exception:return False
    token=PaperStoreAttestation(current,self.manifest,self.expected,self.pin)
    result=SeedLoader(store=store,plan=plan,catalog=self.catalog,review_pin=self.pin,
                       reviewed_store=token,isolated_review=self.isolated_test)
    self.loaded=True
    return result


class ScopedSeedFactory:
 """Entry may attach only within its already-verified PaperState lifetime."""
 def __init__(self,**sealed_args):
    self.args=sealed_args
    self.outer=None
 def activate(self,state):
    if self.outer is not None:raise Hold('C11_SEED_OWNER_ALREADY_ACTIVE')
    self.outer=ExistingOuterWriter(state,**self.args)
    def revoke():
       self.outer.revoke()
       self.outer=None
    return revoke
 def open(self,worker,plan):
    if self.outer is None:raise Hold('C11_EXISTING_OUTER_WRITER_REQUIRED')
    return self.outer.open(worker,plan)
