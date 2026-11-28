"""Staged C11-only SQLite observer integration. It neither fetches nor orders."""
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time

from bridge_protocol import Refused, need
from docker_publication import LoopbackBridge, QUERY_SHA, SOURCE
from observer_host import ObserverHost, store_module
from oi20_rank_slots import bridge_slots

NAME='docker-observer.sqlite'
FRESH_META=300_000_000_000

def binding_for(mount):
    need(len(mount)==4 and all(type(x) is int and x>0 for x in mount),'C11_OBSERVER_MOUNT_SCOPE')
    return hashlib.sha256(json.dumps(['docker-oi20-v1',QUERY_SHA,*mount],separators=(',',':')).encode()).hexdigest()

class DockerObserver(ObserverHost):
    def __init__(self,path,binding,wall=None,mono=None,*,production=False):
        path=Path(path)
        need(path.name==NAME and path.parent.resolve()==path.parent and
             type(binding) is str and re.fullmatch('[a-f0-9]{64}',binding),'C11_DOCKER_OBSERVER_PATH')
        if production:
            need(path==Path('/c11-paper')/NAME and os.getuid()==1000 and os.getgid()==2000,
                 'C11_DOCKER_OBSERVER_PRODUCTION_SCOPE')
        self.path,self.binding=path,binding
        self.wall=wall or (lambda:time.time_ns()//1000000)
        self.mono=mono or time.monotonic_ns
        self.root=self.path.parent.lstat()
        need(stat.S_ISDIR(self.root.st_mode) and stat.S_IMODE(self.root.st_mode)==0o700
             and self.root.st_uid==os.getuid(),'C11_DOCKER_OBSERVER_DIR')
        flags=os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC
        try:
            os.close(os.open(path,flags,0o600))
        except FileExistsError:
            pass
        self.info=self._check_files()
        self.mod=store_module()
        self.writer=self.store=None
        self.last_gap=None
        self.current_rank_receipt=None
        try:
            # Existing files must already bear this source binding; never take
            # ownership of a different lineage or an unrelated/empty archive.
            if self.info.st_size:
                ro=self.mod.ObserverStore.read_only(path)
                try:
                    r=ro.db.execute('SELECT binding FROM source_bucket WHERE source=?',(SOURCE,)).fetchone()
                    need(r is not None and json.loads(r[0])==self._binding(),'C11_DOCKER_OBSERVER_BINDING_CHANGED')
                finally:
                    ro.db.close()
            self.writer=self.mod.ObserverStore(path)
            with self.writer.transaction():
                self.writer.bind(SOURCE,self._binding())
            self.store=self.mod.ObserverStore.read_only(path)
            self._identity()
        except BaseException:
            self.close()
            raise

    def close(self):
        for key in ('store','writer'):
            value=getattr(self,key)
            if value is not None:
                setattr(self,key,None)
                value.db.close()

    def _binding(self):
        return {'publication':self.binding,'query_sha256':QUERY_SHA}

    def _check_files(self):
        need(os.path.samestat(self.root,self.path.parent.lstat()),'C11_DOCKER_OBSERVER_DIR_CHANGED')
        found=None
        for suffix in ('','-wal','-shm','-journal'):
            try:s=(self.path.parent/(NAME+suffix)).lstat()
            except FileNotFoundError:
                if suffix:continue
                raise
            need(stat.S_ISREG(s.st_mode) and stat.S_IMODE(s.st_mode)==0o600 and s.st_uid==os.getuid()
                 and s.st_nlink==1,'C11_DOCKER_OBSERVER_FILE')
            if not suffix:found=s
        return found

    def _identity(self):
        need(os.path.samestat(self.info,self._check_files()),'C11_DOCKER_OBSERVER_FILE_CHANGED')
        r=self.store.db.execute('SELECT binding FROM source_bucket WHERE source=?',(SOURCE,)).fetchone()
        need(r is not None and json.loads(r[0])==self._binding(),'C11_DOCKER_OBSERVER_BINDING_CHANGED')

    def capture(self,frame,classification,begin):
        self.current_rank_receipt=None
        now=self.wall();tick=self.mono()
        stamp=getattr(classification,'received_monotonic_ns',None)
        need(type(stamp) is int and 0<=tick-stamp<FRESH_META
            and type(getattr(classification,'body_sha256',None)) is str
            and re.fullmatch('[a-f0-9]{64}',classification.body_sha256)
            and type(getattr(classification,'verified',None)) is dict,
            'C11_DOCKER_OBSERVER_CLASSIFICATION_UNKNOWN')
        # Charge the whole local read interval, even though the feed already
        # quoted its residual near the end of that interval.
        elapsed=(tick-begin+999999)//1000000
        need(elapsed>=0,'C11_DOCKER_OBSERVER_CLOCK')
        left=min(frame['left_ms']-elapsed,(stamp+FRESH_META-tick)//1000000)
        need(left>0,'C11_DOCKER_OBSERVER_CAPTURE_EXPIRED')
        self._identity()
        self.writer.openmarket(self.binding,frame,now,left,classification)
        self._identity()
        need(self.mono()<tick+left*1000000 and self.wall()<now+left,'C11_DOCKER_OBSERVER_CAPTURE_EXPIRED')
        # A same-generation capture still proves a *new* present-tense
        # observation. openmarket() checked exact byte identity, but its
        # archival first_seen/expiry must never be rewritten or backdated.
        ranks=bridge_slots(frame)
        rows=tuple((row['symbol'],rank,row['native_oi_change_24h_raw'])
                   for rank,row in zip(ranks,frame['rows'])
                   if row['symbol'] in classification.verified)
        hard_expiry=min(now+left,self.mod.ms(frame['lease_until']))
        need(self.wall()<hard_expiry,'C11_DOCKER_OBSERVER_CAPTURE_EXPIRED')
        self.current_rank_receipt=(now,hard_expiry,tick+left*1000000,frame['generation'],rows)
        self.last_gap=None

    def current_point(self,symbol,decision):
        receipt=self.current_rank_receipt
        if receipt is None:
            return None
        seen,expiry,deadline,generation,rows=receipt
        if not seen<=decision<=self.wall()<expiry or self.mono()>=deadline:
            return None
        return next(([generation,rank,ratio] for sym,rank,ratio in rows if sym==symbol),None)

    def unavailable(self,kind='INPUT_UNAVAILABLE'):
        self.current_rank_receipt=None
        if self.last_gap==kind:return
        self._identity()
        self.writer.gap(SOURCE,'publication',self.wall(),kind)
        self.last_gap=kind

    def reply(self,req):
        # Retain the original point/span/episode/weak algorithms and transaction
        # checks. The injected time is only for deterministic isolated tests.
        need(type(req) is dict and type(req.get('end_ms')) is int and req['end_ms']<=self.wall(),
             'C11_DOCKER_OBSERVER_DECISION_CLOCK')
        return super().reply(req)

class CaptureBridge(LoopbackBridge):
    """Feed sets a single classifier snapshot before invoking the original bridge."""
    def __init__(self,publication,observer,binding,classification):
        super().__init__(publication,observer,binding)
        self.classification=classification
        self.db_held=False

    def receive(self,timeout):
        req=self.pending
        if self.ready and req is not None and req['op']=='READ_C11_V2':
            started=self.obs.mono()
            try:
                frame=super().receive(timeout)
                self.obs.capture(frame,self.classification(),started)
                self.db_held=False
                return frame
            except (Refused,OSError,ValueError,KeyError,sqlite3.Error) as e:
                if isinstance(e,sqlite3.Error):
                    self.db_held=True
                try:
                    self.obs.unavailable('TERMINAL_PUBLICATION' if str(e)=='C11_DOCKER_PRODUCER_HELD'
                                         else 'INPUT_UNAVAILABLE')
                except (Refused,OSError,sqlite3.Error):
                    self.db_held=True
                if self.db_held:
                    raise Refused('C11_DOCKER_OBSERVER_STORE_UNAVAILABLE') from e
                raise
        # Historical facts remain untouched; do not emit a current proof while
        # the capture path is in an unknown database state.
        if self.db_held and req is not None and req['op']=='READ_C11_OBSERVER_V1':
            self.pending=None
            raise Refused('C11_DOCKER_OBSERVER_STORE_UNAVAILABLE')
        try:
            return super().receive(timeout)
        except sqlite3.Error as e:
            self.db_held=True
            raise Refused('C11_DOCKER_OBSERVER_STORE_UNAVAILABLE') from e
