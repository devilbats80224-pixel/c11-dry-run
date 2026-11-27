"""Consume verified baked scope exactly once under the already-owned writer lock."""
import json,os,stat
from hashlib import sha256
from pathlib import Path
from threading import get_ident
from authorization_boundary import validate_baked
from outer_seed_lock import ExistingOuterWriter, private_identity, ROOT, LEDGER, LOCK
from binding import Hold

class OneShot:
 def __init__(self,scope,auth_bytes,auth_sha,manifest,catalog,wall_ms):
    self.scope=scope
    self.doc,self.pin=auth_bytes,auth_sha
    self.manifest,self.catalog=manifest,catalog
    self.clock=wall_ms
    self.owner=get_ident()

 def consume(self,state):
    now=self.clock()
    if get_ident()!=self.owner or not validate_baked(self.doc,self.pin,self.manifest,self.catalog,now):
       raise Hold('C11_AUTHORIZATION_NOT_CURRENT_OR_UNVERIFIED')
    outer=self.scope.outer
    if (type(outer) is not ExistingOuterWriter or outer.state is not state or outer.revoked or
           outer.fd!=state.fd or self.manifest!=outer.manifest or self.catalog!=outer.pin):
       raise Hold('C11_EXISTING_OUTER_WRITER_REQUIRED')
    state.check_locked()
    ledger=private_identity(LEDGER) # must have existing reviewed ledger, never initialize a new one
    lock=private_identity(LOCK)
    if lock!=outer.lock_id or not os.path.samestat(os.fstat(outer.fd),LOCK.lstat()):
       raise Hold('C11_OUTER_WRITER_ID')
    doc=json.loads(self.doc)
    # Role metadata only; no secret, wallet, or actual order. Unique for approval, not process.
    marker='c11-compatible-six.review-'+doc['nonce']+'.consumed'
    record=(json.dumps({'schema':'c11-compatible-six-consumed-v1','approval_sha':self.pin,
          'manifest_sha':self.manifest,'catalog_sha':self.catalog,'at_ms':now},sort_keys=True,separators=(',',':'))+'\n').encode()
    directory=os.open(ROOT,os.O_RDONLY|os.O_NOFOLLOW|os.O_DIRECTORY|os.O_CLOEXEC)
    try:
       state.check_locked()
       if not validate_baked(self.doc,self.pin,self.manifest,self.catalog,self.clock()):
          raise Hold('C11_AUTHORIZATION_NOT_CURRENT_OR_UNVERIFIED')
       if private_identity(LEDGER)!=ledger or private_identity(LOCK)!=lock:
          raise Hold('C11_CANONICAL_IDENTITY_CHANGED_BEFORE_CONSUME')
       try:fd=os.open(marker,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600,dir_fd=directory)
       except FileExistsError as e:raise Hold('C11_AUTHORIZATION_ALREADY_USED_OR_UNCERTAIN') from e
       # From creation onward fail closed; no unlink/refund on partial writes,
       # fsync uncertainty, crash or a downstream constructor refusal.
       try:
          m=os.fstat(fd)
          if not(stat.S_ISREG(m.st_mode) and m.st_uid==os.getuid() and m.st_nlink==1 and stat.S_IMODE(m.st_mode)==0o600):
             raise Hold('C11_APPROVAL_MARKER_UNSAFE')
          with os.fdopen(os.dup(fd),'wb',closefd=True) as f:
             f.write(record);f.flush();os.fsync(f.fileno())
          os.fsync(directory)
          state.check_locked()
          if private_identity(LEDGER)!=ledger:raise Hold('C11_CANONICAL_IDENTITY_CHANGED_AFTER_CONSUME')
       finally:os.close(fd)
       return sha256(record).hexdigest()
    finally:os.close(directory)
