"""Staged read-only C11 process-local bridge; never an OpenMarket client."""
from exact_symbol import ID_CHAR
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time

from bridge_protocol import Refused, need
from oi20_rank_slots import PARTIAL, rank_slots

SOURCE='paper-market-openmarket-oi20-v1'
SCHEMA='independent_paper_market_observation_v1'
ORIGIN='https://api.openmarket.xyz'
QUERY_SHA='ef5fea69e4bea1b579e2857dcf8dcc65f7516edecad0e0310a7b3a2ff377b815'
MAX=128*1024
TTL_MS=60000
NONCE=re.compile('[a-f0-9]{32}')
BIND=re.compile('[a-f0-9]{64}')
SYM=re.compile(f'{ID_CHAR}{{2,40}}')
RATIO=re.compile(r'-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?')
TIME=re.compile(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z')
EPOCH=datetime(1970,1,1,tzinfo=timezone.utc)

def ms(s):
    need(type(s) is str and TIME.fullmatch(s),'C11_DOCKER_PROVIDER_CLOCK')
    d=datetime.fromisoformat(s[:-1]+'+00:00')-EPOCH
    return d.days*86400000+d.seconds*1000+d.microseconds//1000

def linux_clock_identity():
    return Path('/proc/sys/kernel/random/boot_id').read_text().strip(),os.readlink('/proc/self/ns/time')

CLOCK_FIELDS={'clock_monotonic_offset_ns','clock_boottime_offset_ns','published_boottime_ns'}
CLOCK_DOMAIN=re.compile(r"time:\[[1-9][0-9]*\]")
NS=1_000_000_000

def read_offsets_text(text):
    values={}
    need(type(text) is str and len(text)<=1024,'C11_DOCKER_TIMENS_FORMAT')
    for line in text.splitlines():
        parts=line.split()
        need(len(parts)==3 and parts[0] in ('monotonic','boottime') and parts[0] not in values and
             re.fullmatch(r"-?[0-9]{1,10}",parts[1]) and re.fullmatch(r"[0-9]{1,9}",parts[2]),
             'C11_DOCKER_TIMENS_FORMAT')
        v=int(parts[1])*NS+int(parts[2])
        need(abs(v)<2**63,'C11_DOCKER_TIMENS_RANGE')
        values[parts[0]]=v
    need(set(values)=={'monotonic','boottime'},'C11_DOCKER_TIMENS_FORMAT')
    return values['monotonic'],values['boottime']

def linux_offsets():
    return read_offsets_text(Path('/proc/self/timens_offsets').read_text())

class Publication:
    def __init__(self,root,uid,gid,floor,wall=None,mono=None,clock_identity=linux_clock_identity, *,
                 expected_mount=None, require_readonly=False, offsets=linux_offsets, boottime=None):
        self.root=Path(root)
        self.uid,self.gid=uid,gid
        need(type(floor) is int and 0<floor<2**53,'C11_DOCKER_RELEASE_SCOPE')
        if require_readonly:
            need((Path('/proc/self/ns/time').is_symlink()) and os.getuid()==1000 and os.getgid()==2000
                 and uid==501 and gid==2000 and expected_mount is not None,'C11_DOCKER_ACTUAL_SCOPE')
        self.floor=floor
        self.last=None
        self.wall=wall or (lambda:time.time_ns()//1_000_000)
        self.mono=mono or time.monotonic_ns
        self.clock=clock_identity
        self.offsets=offsets
        self.boottime=boottime or (lambda:time.clock_gettime_ns(time.CLOCK_BOOTTIME))
        self.fd=self.pub=None
        flags=os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC
        try:
            self.fd=os.open(self.root,flags)
            self.pub=os.open('published',flags,dir_fd=self.fd)
            self._pinned_dirs()
            a,b=os.fstat(self.fd),os.fstat(self.pub)
            identity=(a.st_dev,a.st_ino,b.st_dev,b.st_ino)
            if expected_mount is not None:
                need(identity==tuple(expected_mount),'C11_DOCKER_REVIEWED_MOUNT')
            if require_readonly:
                need(os.fstatvfs(self.fd).f_flag & os.ST_RDONLY and
                     os.fstatvfs(self.pub).f_flag & os.ST_RDONLY,'C11_DOCKER_READONLY_VOLUME')
        except BaseException:
            self.close()
            raise

    def close(self):
        for k in ('pub','fd'):
            v=getattr(self,k)
            if v is not None:
                setattr(self,k,None)
                os.close(v)

    def _pinned_dirs(self):
        for held,current in ((os.fstat(self.fd),self.root.lstat()),(os.fstat(self.pub),
                os.stat('published',dir_fd=self.fd,follow_symlinks=False))):
            need(stat.S_ISDIR(held.st_mode) and stat.S_IMODE(held.st_mode)==0o750 and
                 (held.st_uid,held.st_gid)==(self.uid,self.gid) and os.path.samestat(held,current),
                 'C11_DOCKER_PUBLICATION_DIR')

    @staticmethod
    def _file_info(s):
        return (s.st_dev,s.st_ino,s.st_size,s.st_ctime_ns,s.st_mtime_ns,s.st_mode,
                s.st_uid,s.st_gid,s.st_nlink)

    def _read(self):
        self._pinned_dirs()
        fd=os.open('latest.json',os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC,dir_fd=self.pub)
        with os.fdopen(fd,'rb') as f:
            a=os.fstat(f.fileno())
            need(stat.S_ISREG(a.st_mode) and stat.S_IMODE(a.st_mode)==0o440 and (a.st_uid,a.st_gid)==(self.uid,self.gid)
                 and a.st_nlink==1 and 1<=a.st_size<=MAX,'C11_DOCKER_PUBLICATION_FILE')
            raw=f.read(MAX+1); b=os.fstat(f.fileno())
            current=os.stat('latest.json',dir_fd=self.pub,follow_symlinks=False)
            need(len(raw)==a.st_size and self._file_info(a)==self._file_info(b)==self._file_info(current),
                 'C11_DOCKER_PUBLICATION_CHANGED')
        self._pinned_dirs()
        return raw

    def response(self,nonce):
        need(type(nonce) is str and NONCE.fullmatch(nonce),'C11_DOCKER_NONCE')
        try:
            raw=self._read()
            frame=json.loads(raw)
            return self._response(nonce,frame,hashlib.sha256(raw).digest())
        except (OSError,KeyError,ValueError,TypeError,AttributeError,RecursionError,InvalidOperation) as err:
            raise Refused('C11_DOCKER_PUBLICATION_INVALID') from err

    def _response(self,nonce,v,digest):
        seq=v['generation']; p=v['provider_ingestion']; partial=p['status']==PARTIAL; online=p['status']=='FRESH_OBSERVATION' or partial
        need(v['schema']==SCHEMA and v['source']==SOURCE and v['origin']==ORIGIN and
             v['request_sha256']==QUERY_SHA and v['source_as_of'] is None
             and v['observation_only'] is True and v['entry_authorized'] is False and v['orders_enabled'] is False
             and type(seq) is int and self.floor<=seq<2**53,'C11_DOCKER_PUBLICATION_SCOPE')

        boot,domain=self.clock()
        remote=v['clock_namespace']
        need(v['clock_boot']==boot and type(remote) is str and CLOCK_DOMAIN.fullmatch(remote)
             and type(domain) is str and CLOCK_DOMAIN.fullmatch(domain),'C11_DOCKER_CLOCK_IDENTITY')
        fields=CLOCK_FIELDS.intersection(v)
        # Old frames from a different namespace cannot prove a common
        # monotonic origin. New writer attests both numeric Linux offsets.
        need((not fields and remote==domain) or fields==CLOCK_FIELDS,'C11_DOCKER_TIME_OFFSET_UNAVAILABLE')

        if self.last is not None:
            old,d,old_src=self.last
            need(seq>old or (seq==old and d==digest),'C11_DOCKER_REPLAY_OR_EQUIVOCATION')
        else:
            old=0; old_src=0
        w=v['published_wall_ms']; t=v['published_monotonic_ns']

        need(type(w) is int and type(t) is int and w>0 and t>0,'C11_DOCKER_PUBLISH_CLOCK')
        local_t=t
        local_bt=None
        if fields:
            om,ob=self.offsets()
            wm,wb=v['clock_monotonic_offset_ns'],v['clock_boottime_offset_ns']
            b=v['published_boottime_ns']
            need(all(type(x) is int and abs(x)<2**63 for x in (om,ob,wm,wb))
                 and type(b) is int and b>0,'C11_DOCKER_TIMENS_ATTESTATION')
            if remote==domain:
                need((om,ob)==(wm,wb),'C11_DOCKER_SAME_TIMENS_OFFSET_DRIFT')
            local_t=t-wm+om
            local_bt=b-wb+ob
        need(self.wall()>=w and self.mono()>=local_t and
             (local_bt is None or self.boottime()>=local_bt),'C11_DOCKER_PUBLISH_CLOCK')

        if not online:
            need(p['terminal'] is True and p['rows']==[],'C11_DOCKER_NO_FRESH_PAGE')
            # A newer terminal marker forbids accepting an older, still valid frame.
            self.last=(seq,digest,old_src)
            raise Refused('C11_DOCKER_PRODUCER_HELD')
        src,end=ms(p['received_at']),ms(p['lease_until'])
        need(w>=src>0 and end-src==TTL_MS and (seq==old or src>old_src),'C11_DOCKER_PROVIDER_LEASE')
        need(p['terminal'] is False and type(p['http_status']) is int and p['http_status']==200
             and type(p['offset']) is int and p['offset']==0 and type(p['page_size']) is int and p['page_size']==60
             and type(p['raw_row_count']) is int and p['raw_row_count']==60 and type(p['numeric_rows']) is int
             and type(p['invalid_rows']) is int and 0<=p['invalid_rows']<60
             and p['numeric_rows']+p['invalid_rows']==60 and partial==(p['invalid_rows']>0)
             and partial==('provider_ranks_v1' in p) and p['descending'] is True and
             p['units']=='PROVIDER_PUBLISHED_RAW_RATIO','C11_DOCKER_PAGE_SCOPE')
        seen=set(); previous=None; rows=p['rows']
        ranks=p.get('provider_ranks_v1')
        need(not partial or type(ranks) is list,'C11_DOCKER_PARTIAL_RANKS')
        rank_slots(rows,ranks)
        need(len(rows)==p['numeric_rows'],'C11_DOCKER_ROW_COUNT')
        for row in rows:
            need(type(row) is dict and set(row)=={'symbol','native_oi_change_24h_raw'},'C11_DOCKER_ROW')
            symbol,raw=row['symbol'],row['native_oi_change_24h_raw']
            need(type(symbol) is str and SYM.fullmatch(symbol) and symbol not in seen and
                 type(raw) is str and len(raw)<=128 and RATIO.fullmatch(raw),'C11_DOCKER_ROW')
            value=Decimal(raw)
            need(value.is_finite() and (previous is None or value<=previous),'C11_DOCKER_RANK')
            seen.add(symbol); previous=value
        # Both clocks must agree. A stalled/reversed wall clock can never extend
        # expiry; a leap forward only decreases remaining time.
        left=min(end-self.wall(),(local_t+(end-w)*1_000_000-self.mono())//1_000_000)
        if local_bt is not None:
            left=min(left,(local_bt+(end-w)*1_000_000-self.boottime())//1_000_000)
        need(0<left<=TTL_MS,'C11_DOCKER_EXPIRED')
        self.last=(seq,digest,src)
        return ({'provider_ranks_v1':ranks} if partial else {}) | dict(status='FRESH_PARTIAL' if partial else 'FRESH',nonce=nonce,source=SOURCE,query_sha256=QUERY_SHA,
                    generation=seq,left_ms=left,received_at=p['received_at'],lease_until=p['lease_until'],
                    offset=0,page_size=60,rows=rows)

class LoopbackBridge:
    """Reuse the existing worker's EntryFeed and ObserverClient without extra process."""
    def __init__(self,publication,observer,binding):
        need(type(binding) is str and BIND.fullmatch(binding) and getattr(observer,'binding',None)==binding
             and callable(getattr(observer,'reply',None)),'C11_DOCKER_OBSERVER_BINDING')
        self.pub,self.obs,self.binding=publication,observer,binding
        self.pending=None
        self.ready=False

    def send(self,request):
        need(self.pending is None and type(request) is dict and
             type(request.get('nonce')) is str and NONCE.fullmatch(request['nonce']),
             'C11_DOCKER_RPC_REQUEST')
        self.pending=request.copy()

    def receive(self,timeout):
        req,self.pending=self.pending,None
        need(req is not None,'C11_DOCKER_RPC_MISSING')
        op=req.get('op')
        if not self.ready:
            need(op=='C11_PAPER_BRIDGE_V1' and set(req)=={'op','nonce'},'C11_DOCKER_RPC_BOOT')
            self.ready=True
            return dict(nonce=req['nonce'],binding=self.binding)
        if op=='READ_C11_V2':
            need(set(req)=={'op','nonce'},'C11_DOCKER_RPC_FEED')
            return self.pub.response(req['nonce'])
        need(op=='READ_C11_OBSERVER_V1','C11_DOCKER_RPC_OPERATION')
        return self.obs.reply(req)
