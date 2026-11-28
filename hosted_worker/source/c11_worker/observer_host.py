"""Read private observer storage on its owning host. No Docker mount or writes."""
from hashlib import sha256
import json
import os
import re
from pathlib import Path
import stat
import sys

HERE=Path(__file__).resolve().parent
STORE_DIR=HERE.parent/'c11_observer_state_v1'
STORE_FILE=STORE_DIR/'observer_state/observations.sqlite'
STORE_SHA='cedbbfd776d9b3dffcc78e57c934ece82c0b852f55bc5583de9e02742db69bf2'
V2_FILE=HERE.parent/'c11_v2_paper_startup_v1/v2_source.py'

from bridge_protocol import need
from observer_rpc import OP,DAY,FIVE,SYMBOL,HEX,clock,now_ms


def store_module():
    import importlib.util
    path=STORE_DIR/'observer_store.py'
    src=path.read_bytes()
    need(sha256(src).hexdigest()==STORE_SHA,'C11_OBSERVER_STORE_CODE_PIN')
    spec=importlib.util.spec_from_file_location('observer_store',path)
    mod=importlib.util.module_from_spec(spec)
    sys.modules[mod.__name__]=mod
    exec(compile(src,str(path),'exec'),mod.__dict__)
    return mod


def binding_for(publication,boot):
    mod=store_module()
    return mod.digest(dict(uid=publication.uid,device=publication.device,inode=publication.inode,
                           boot=boot,reader=sha256(V2_FILE.read_bytes()).hexdigest()))


class ObserverHost:
    def __init__(self,expected):
        need(type(expected) is str and HEX.fullmatch(expected),'C11_OBSERVER_HOST_BINDING')
        root=STORE_FILE.parent.lstat()
        need(stat.S_ISDIR(root.st_mode) and root.st_uid==os.getuid() and stat.S_IMODE(root.st_mode)==0o700,
             'C11_OBSERVER_HOST_DIRECTORY')
        self.root=root
        self.info=self._check_files()
        self.mod=store_module()
        self.store=self.mod.ObserverStore.read_only(STORE_FILE)
        self.binding=expected
        self._identity()

    def _check_files(self):
        need(os.path.samestat(self.root,STORE_FILE.parent.lstat()),'C11_OBSERVER_HOST_DIR_REPLACED')
        result=None
        for suffix in ('','-wal','-shm','-journal'):
            p=STORE_FILE.with_name(STORE_FILE.name+suffix)
            try:
                info=p.lstat()
            except FileNotFoundError:
                if suffix:
                    continue
                raise
            need(stat.S_ISREG(info.st_mode) and info.st_uid==os.getuid() and info.st_nlink==1
                 and stat.S_IMODE(info.st_mode)==0o600,'C11_OBSERVER_HOST_FILE')
            if not suffix:
                result=info
        return result

    def _identity(self):
        row=self.store.db.execute('SELECT binding FROM source_bucket WHERE source=?',(self.mod.SOURCE,)).fetchone()
        need(row is not None and json.loads(row[0])=={'publication':self.binding,'query_sha256':self.mod.QUERY},
             'C11_OBSERVER_HOST_PUBLICATION')
        need(os.path.samestat(self.info,self._check_files()),'C11_OBSERVER_HOST_FILE_REPLACED')

    def _known_transition(self,symbol,before,after):
        initial=self.store.oi20_at(self.binding,symbol,before)
        if initial is None:
            return False
        db=self.store.db
        if db.execute("SELECT 1 FROM gaps WHERE source=? AND scope='publication' "
                      "AND observed_ms>=? AND observed_ms<=? LIMIT 1",
                      (self.mod.SOURCE,before,after)).fetchone():
            return False
        rows=db.execute(
            'SELECT o.generation,o.first_seen_ms,o.expiry_ms,r.crypto_usdt '
            'FROM openmarket o LEFT JOIN openmarket_row r ON r.binding=o.binding '
            'AND r.generation=o.generation AND r.symbol=? WHERE o.binding=? '
            'AND o.generation>=? AND o.first_seen_ms<=? ORDER BY o.generation',
            (symbol,self.binding,initial[0],after)).fetchall()
        expiry=None
        previous=initial[0]-1
        for generation,seen,lease,verified in rows:
            if not verified or generation!=previous+1 or (expiry is not None and seen>expiry):
                return False
            previous,expiry=generation,lease
        return expiry is not None and after<expiry

    def _episode(self,symbol,end):
        # The archive takes a known negative at the preceding 5-minute point
        # as the observable origin. Unknown before first positive is no origin.
        if end%FIVE:
            return None
        start=end
        while end-start<=DAY:
            point=self.store.oi20_at(self.binding,symbol,start)
            if point is None:
                return None
            if point[2]<self.mod.decimal_value('0.20'):
                first=start+FIVE
                return first if (first<=end and self._known_transition(symbol,start,first)
                                 and self.store.continuous_oi20(self.binding,symbol,first,end)) else None
            start-=FIVE
        return None

    def current_point(self,symbol,decision):
        return None  # Only the docked observer can attest a new capture receipt.

    def reply(self,req):
        need(type(req) is dict and set(req)=={'op','nonce','view','symbol','end_ms','start_ms'}
             and req['op']==OP and type(req['nonce']) is str and re.fullmatch('[a-f0-9]{32}',req['nonce']),
             'C11_OBSERVER_HOST_REQUEST')
        key,symbol,end,start=(req[k] for k in ('view','symbol','end_ms','start_ms'))
        now=now_ms()
        need(type(symbol) is str and SYMBOL.fullmatch(symbol) and clock(end) and now-DAY<=end<=now
             and ((key=='span' and clock(start) and end-DAY<=start<=end)
                  or (key in ('point','current_point','weak','episode') and start is None)),
             'C11_OBSERVER_HOST_SCOPE')
        db=self.store.db
        db.execute('BEGIN')
        try:
            self._identity()
            if key=='point':
                got=self.store.oi20_at(self.binding,symbol,end)
                data=[got[0],got[1],str(got[2])] if got else None
            elif key=='current_point':
                data=self.current_point(symbol,end)
            elif key=='weak':
                row=db.execute('SELECT binding FROM source_bucket WHERE source=?',(self.mod.BINANCE_SOURCE,)).fetchone()
                expected={'host':'fapi.binance.com','path':'/futures/data/openInterestHist','period':'5m','field':'sumOpenInterest'}
                got=self.store.weak_quantity_pair(symbol,end) if row and json.loads(row[0])==expected else None
                data=[[boundary,str(quantity),seen] for boundary,quantity,seen in got] if got else None
            elif key=='span':
                data=self.store.continuous_oi20(self.binding,symbol,start,end)
            else:
                data=self._episode(symbol,end)
            self._identity()
        finally:
            db.rollback()
        return dict(nonce=req['nonce'],binding=self.binding,view=key,symbol=symbol,end_ms=end,start_ms=start,data=data)
