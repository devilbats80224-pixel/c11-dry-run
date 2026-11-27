"""Protected startup phase/status only. Never stores rows, wallet or credentials."""
import hashlib,json
import os
from pathlib import Path
import sqlite3,stat,time
from bridge_protocol import Refused,need

STATUS='c11-native-owned-startup-status.json'
ARCHIVE='observer-native-owned-v1.sqlite'
PRESERVED=('observer-native.sqlite','observer-native-v4a.sqlite','c11-v4a-startup-status.json')
LEDGER='trades.c11.paper.sqlite'
PHASES={'SCOPED_RELEASE_START','ARCHIVE_LINEAGE_ALLOWED','PUBLICATION_AND_CLASSIFIER_READY',
        'DRY_CONFIG_READY','NATIVE_CONSTRUCTOR_ENTERED','NATIVE_WORKER_READY','NATIVE_RUN_ENTERED',
        'NATIVE_RUN_RETURNED','REFUSED'}
ERRORS={'C11_OBSERVER_SOURCE_IDENTITY_CHANGED','C11_NATIVE_LINEAGE_MIGRATION_REQUIRES_ZERO_OPEN_PENDING',
        'C11_NO_FRESH_CLASSIFIED_OBSERVATION','C11_BINANCE_NO_VERIFIED_CRYPTO','C11_DOCKER_PUBLISH_CLOCK',
        'C11_DOCKER_READONLY_VOLUME','C11_DOCKER_REVIEWED_MOUNT','C11_DOCKER_CLOCK_IDENTITY',
        'C11_DOCKER_PRODUCER_HELD','C11_DOCKER_EXPIRED','C11_PAPER_NATIVE_CONFIG',
        'C11_NATIVE_RESOLUTION','C11_NATIVE_ARCHIVE_LINEAGE_ATTESTATION','C11_OBSERVER_NATIVE_DB_ID',
        'C11_DOCKER_OBSERVER_STORE_UNAVAILABLE','C11_PAPER_PROCESS_SCOPE','C11_RUNTIME_VERSION'}
DIAGNOSTIC_CODES=frozenset((
    'C11_STARTUP_STATUS_FILE_ID','C11_STARTUP_STATUS_ENUM','C11_STARTUP_LEDGER_FILE_ID',
    'C11_STARTUP_ARCHIVE_FILE_ID','C11_STARTUP_PRESERVED_FILE_ID','C11_STARTUP_OBSERVER_BINDING',
    'C11_STARTUP_ARCHIVE_SIDECAR','C11_STARTUP_LEDGER_REPLACED','C11_STARTUP_PREFLIGHT_ARCHIVE_BUSY'))
ERRORS.update(DIAGNOSTIC_CODES)
SAFE_CODES=ERRORS|{'OK','OTHER_STARTUP_ERROR'}

def code(exc):
    c=str(exc)
    return c if c in ERRORS else 'OTHER_STARTUP_ERROR'

def file_info(p,reason):
    try:s=p.lstat()
    except OSError as e:raise Refused(reason) from e
    need(stat.S_ISREG(s.st_mode) and s.st_nlink==1 and s.st_uid==os.getuid()
         and s.st_gid==os.getgid() and stat.S_IMODE(s.st_mode)==0o600,reason)
    return s

def quiet_readonly_uri(p):
    # Before consumption never let SQLite create rollback or WAL sidecars.
    # Immutable is accurate only for the quiescent rollback-format database.
    # Official original writer lock is already held by the caller.
    reason='C11_STARTUP_PREFLIGHT_ARCHIVE_BUSY'
    before=file_info(p,'C11_STARTUP_ARCHIVE_FILE_ID')
    def companions():
        return any(os.path.lexists(str(p)+suffix) for suffix in ('-wal','-shm','-journal'))
    need(not companions(),reason)
    fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC)
    try:
        need(os.path.samestat(before,os.fstat(fd)),reason)
        header=os.read(fd,100)
    finally:os.close(fd)
    need(header.startswith(b'SQLite format 3\0') and header[18:20]==bytes((1,1))
         and not companions() and os.path.samestat(before,p.lstat()),reason)
    return p.as_uri()+'?mode=ro&immutable=1'

def first_migration(root,*,before_consume=False):
    ledger=root/LEDGER
    s=file_info(ledger,'C11_STARTUP_LEDGER_FILE_ID')
    # These are required retired evidence files. Never infer continuity from
    # them, alter the old binding, or open any historical DB for writing.
    for name in PRESERVED:
        file_info(root/name,'C11_STARTUP_PRESERVED_FILE_ID')
    current=root/ARCHIVE
    if os.path.lexists(current):
        file_info(current,'C11_STARTUP_ARCHIVE_FILE_ID')
        lineage=hashlib.sha256(json.dumps(dict(file=LEDGER,device=s.st_dev,inode=s.st_ino),
                                  sort_keys=True,separators=(',',':')).encode()).hexdigest()
        uri=quiet_readonly_uri(current) if before_consume else current.as_uri()+'?mode=ro'
        db=sqlite3.connect(uri,uri=True,timeout=1)
        try:
            db.execute('PRAGMA query_only=ON')
            result=db.execute('SELECT binding FROM source_bucket WHERE source=?',
                              ('native-freqtrade-paper',)).fetchone()
            need(result is not None and json.loads(result[0])=={'ledger':lineage},'C11_STARTUP_OBSERVER_BINDING')
        finally:db.close()
        return
    need(not before_consume,'C11_STARTUP_ARCHIVE_FILE_ID')
    need(not any(os.path.lexists(root/(ARCHIVE+suffix)) for suffix in ('-wal','-shm','-journal')),
         'C11_STARTUP_ARCHIVE_SIDECAR')
    db=sqlite3.connect(ledger.as_uri()+'?mode=ro',uri=True,timeout=1)
    try:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        n=db.execute('SELECT COUNT(*) FROM trades WHERE is_open=1').fetchone()[0]
        m=db.execute('SELECT COUNT(*) FROM orders WHERE ft_is_open=1').fetchone()[0]
        need(n==m==0,'C11_NATIVE_LINEAGE_MIGRATION_REQUIRES_ZERO_OPEN_PENDING')
    finally:db.close()
    need(os.path.samestat(s,file_info(ledger,'C11_STARTUP_LEDGER_FILE_ID')),'C11_STARTUP_LEDGER_REPLACED')

class Status:
    def __init__(self,root):
        self.root=Path(root)
    def write(self,phase,result='OK'):
        need(phase in PHASES and result in SAFE_CODES,'C11_STARTUP_STATUS_ENUM')
        p=self.root/STATUS
        if os.path.lexists(p):file_info(p,'C11_STARTUP_STATUS_FILE_ID')
        tmp=self.root/('.c11-v4a-status-'+os.urandom(10).hex())
        flags=os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC
        try:
            fd=os.open(tmp,flags,0o600)
            with os.fdopen(fd,'w') as out:
                json.dump(dict(phase=phase,code=result,wall_ms=time.time_ns()//1_000_000),out)
                out.flush();os.fsync(out.fileno())
            os.replace(tmp,p)
            fd=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try:os.fsync(fd)
            finally:os.close(fd)
        finally:
            if tmp.exists():tmp.unlink()
