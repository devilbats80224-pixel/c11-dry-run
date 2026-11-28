"""Locked Freqtrade Worker, isolated paper state and pinned retrospective public tape."""
import asyncio
from collections import Counter
from concurrent.futures import Future
from contextlib import ExitStack, asynccontextmanager, contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
import json
import os
from pathlib import Path
import runpy
import socket
import subprocess
import sys
from threading import Thread, get_ident
import time
from types import SimpleNamespace as N
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

sys.path[:0]=['/opt/c11_owned_modules','/opt/c11_worker','/opt/c11_worker/ports',
              '/opt/c11_background_review','/freqtrade']

import aiohttp
import async_native_ccxt
from causal_bars import FIVE, MINUTE as M, QUARTER as Q
from c11_paper_exchange import C11PaperExchange
import entry_feed
from freqtrade.exchange import Binance, Exchange
from freqtrade.persistence import Trade
import installed_build_inputs as installed
from minute_rollup import roll
from multidict import CIMultiDict
from native_minute_admission_loop import MinuteAdmissionLoop
from native_primary_lane import NativePrimaryLane
import native_worker as native
import native_primary_lane
from paper_runtime import PaperState, verify_state_file
from prepared_owner_history import BoundOwnerHistory
from rpd_checkpoint import RpdState
from shared_history_forwarder import SharedHistoryForwarder
import shared_history_child
import standalone_policy
import requests
import v4_entry
from stage_pin import AUTH_SHA, RUNTIME_SHA

HAN_MODE=os.environ.get('C11_HAN_AUTO_ARCHIVE')=='yes'
INIT_MODE=os.environ.get('C11_INIT_AUTO_ARCHIVE')=='yes'
assert not (HAN_MODE and INIT_MODE)
SYMBOL='牛来USDT' if HAN_MODE else 'INITUSDT' if INIT_MODE else 'JCTUSDT'
PAIR=SYMBOL[:-4]+'/USDT:USDT'
SHIFT=24*60*M
REPLAY_MODE=os.environ.get('C11_JCT_BASELINE')=='yes'
REVIEWED_PARTIAL=os.environ.get('C11_JCT_REVIEWED_PARTIAL')=='yes'
ROSTER_RETURN=os.environ.get('C11_JCT_ROSTER_RETURN')=='yes'
EARLY_ROSTER=os.environ.get('C11_JCT_EARLY_ROSTER')=='yes'
assert not EARLY_ROSTER or ROSTER_RETURN
AUTO_MODE=os.environ.get('C11_JCT_AUTO_ARCHIVE')=='yes'
CACHED=os.environ.get('C11_JCT_REVIEWED_CACHE')=='yes'
PINNED_REMOTE=os.environ.get('C11_JCT_REVIEWED_REMOTE')=='yes'
PINNED_WITHOUT_DISCOVERY=os.environ.get('C11_JCT_PINNED_WITHOUT_DISCOVERY')=='yes'
assert not PINNED_WITHOUT_DISCOVERY or PINNED_REMOTE
AUTO_STALE=os.environ.get('C11_JCT_AUTO_STALE')=='yes'
PREPARED_CHECKPOINT=os.environ.get('C11_JCT_PREPARED_CHECKPOINT')=='yes'
NO_LISTING_ORIGIN=os.environ.get('C11_JCT_NO_LISTING_ORIGIN')=='yes'
ARCHIVE_OUTAGE=os.environ.get('C11_JCT_ARCHIVE_OUTAGE')=='yes'
ON_DEMAND=os.environ.get('C11_JCT_ON_DEMAND')=='yes'
assert not (ARCHIVE_OUTAGE and ON_DEMAND)
assert not (ON_DEMAND or ARCHIVE_OUTAGE) or (AUTO_MODE and not (CACHED or PINNED_REMOTE or PREPARED_CHECKPOINT or NO_LISTING_ORIGIN))
BACKGROUND_REVIEWED=os.environ.get('C11_JCT_BACKGROUND_REVIEWED')=='yes'
assert not BACKGROUND_REVIEWED or (CACHED and AUTO_MODE and not PREPARED_CHECKPOINT)
assert not NO_LISTING_ORIGIN or (AUTO_MODE and not (CACHED or PINNED_REMOTE or PREPARED_CHECKPOINT))
assert not PREPARED_CHECKPOINT or (CACHED and AUTO_MODE)
MIXED=os.environ.get('C11_JCT_MIXED_PACKAGED','')
assert MIXED in ('','blank','unusable')
assert not MIXED or (AUTO_MODE and not (HAN_MODE or INIT_MODE or REVIEWED_PARTIAL or AUTO_STALE))
EXACT_CALLBACK_CLOCK = bool(MIXED) or REVIEWED_PARTIAL or AUTO_STALE or EARLY_ROSTER or PREPARED_CHECKPOINT or BACKGROUND_REVIEWED
LEASE_CLOCK = EXACT_CALLBACK_CLOCK or EARLY_ROSTER
assert not AUTO_STALE or (AUTO_MODE and not (ROSTER_RETURN or REVIEWED_PARTIAL or HAN_MODE or INIT_MODE))
EARLY_EXPECT=os.environ.get('C11_JCT_EARLY_EXPECT')
assert not (CACHED and PINNED_REMOTE)
assert not (CACHED or PINNED_REMOTE) or (AUTO_MODE and not (HAN_MODE or INIT_MODE or EARLY_EXPECT))
assert EARLY_EXPECT is None or (AUTO_MODE and not (ROSTER_RETURN or HAN_MODE or INIT_MODE) and EARLY_EXPECT in ('deliver','hold','start','no-start'))
assert not (AUTO_MODE and REPLAY_MODE)
assert not REVIEWED_PARTIAL or not (AUTO_MODE or REPLAY_MODE)
assert not ROSTER_RETURN or (AUTO_MODE and not (HAN_MODE or INIT_MODE or REVIEWED_PARTIAL))
RESTART=os.environ.get('C11_HAN_RESTART_PHASE')
assert RESTART is None or (HAN_MODE and AUTO_MODE and RESTART in ('prepare','resume'))
ROOT=Path('/c11-paper')
DATA=Path('/jct')

def data(name, pin):
    raw=(DATA/name).read_bytes()
    assert sha256(raw).hexdigest()==pin,name
    return json.loads(raw)

if HAN_MODE or INIT_MODE:
    fixture_pins=json.loads((DATA/'input-pins.json').read_bytes())
    witness=data('receipt.json',fixture_pins['receipt.json'])
    tail=data('jct-after-archive.native.json',fixture_pins['jct-after-archive.native.json'])
else:
    witness=data('receipt.json','3be7ed2d6706c62084a6ac4598a05bd69f3f0ff1a2d51acdd0f37ce89732095c')
    tail=data('jct-after-archive.native.json','57265967efa958cd734e8ed4070ea91f1d703e445deef5fec32385de35df729e')
if AUTO_MODE:
    assert not (DATA/'jct-base.local.json').exists()
    contract=data('contract.json',fixture_pins['contract.json'] if HAN_MODE or INIT_MODE else 'bfdc80389259bc21d7b291b3cd98cd3325d450887842e124616e526fa8bc1f37')
    assert contract['source']=='https://fapi.binance.com/fapi/v1/exchangeInfo'
    captured=contract['payload']
    expected_origin=1788089400000 if HAN_MODE else 1744785000000 if INIT_MODE else 1762770600000
    assert captured==dict(symbol=SYMBOL,baseAsset=SYMBOL[:-4],quoteAsset='USDT',marginAsset='USDT',
                          contractType='PERPETUAL',onboardDate=expected_origin,status='TRADING')
    base=dict(symbol=captured['symbol'],origin_ms=captured['onboardDate'],
              end_ms=tail[0]['start'],known_ms=contract['actual_received_ms'])
else:
    base=data('jct-base.local.json','d4ebd00dda2fa2f8a0d59fea224869e93e7e3e6d60f57c95376e0e8caad3ce07')
END=witness['end']+SHIFT
ORIGIN=base['origin_ms']+(0 if MIXED else SHIFT)
assert witness['symbol']==base['symbol']==SYMBOL
assert base['known_ms']<END-Q and all(t['actual_received_ms']<END-Q for t in tail)
counts=[330] if HAN_MODE else [105,435] if INIT_MODE else [495,105]
assert [(p['count'],p['stop']-p['start']) for p in tail]==[(count,count*M) for count in counts]

def translated(row):
    result=list(row)
    for i in (0,6):
        result[i]+=SHIFT
    return result

bars={x[0]:x for p in tail for raw in p['rows'] if (x:=translated(raw))}
assert len(bars)==sum(counts)
terminal=[r for r in witness['rows'] if r[0]==M]
assert all(list(map(float,[bars[r[1]+SHIFT][i] for i in (1,2,3,4,5,7)]))==
           list(map(float,r[3:])) for r in terminal)
rows_by_width={M:bars, FIVE:{}, Q:{}}
for width in (FIVE,Q):
    for start in range(base['end_ms']+SHIFT,END,width):
        rows_by_width[width][start]=roll([bars[t] for t in range(start,start+width,M)],start,width)

def candle_row(r):
    _,t,_,op,hi,lo,cl,vol,quote=r
    return [t+SHIFT,*(format(Decimal(str(x)),'f') for x in (op,hi,lo,cl,vol)),
            t+SHIFT+Q-1,format(Decimal(str(quote)),'f'),0,'0','0','0']

for r in witness['rows']:
    if r[0]==Q and r[1]+SHIFT not in rows_by_width[Q]:
        q=candle_row(r)
        rows_by_width[Q][q[0]]=q

def state_receipt(db):
    segment=db.execute('SELECT * FROM segment WHERE symbol=?',(SYMBOL,)).fetchone()
    rpd=db.execute('SELECT payload FROM rpd_checkpoint WHERE symbol=?',(SYMBOL,)).fetchone()
    atr=db.execute('SELECT * FROM atr_er_checkpoint WHERE symbol=? ORDER BY width',(SYMBOL,)).fetchall()
    lineage=db.execute('SELECT * FROM cold_prefix_lineage WHERE symbol=?',(SYMBOL,)).fetchone()
    assert segment and rpd and len(atr)==2 and lineage
    assert lineage[0]==segment[0]==RpdState.restore(rpd[0]).symbol==SYMBOL
    assert all(row[0]==SYMBOL for row in atr)
    for alias in ('牛來USDT','NIULAIUSDT'):
        for table in ('segment','rpd_checkpoint','atr_er_checkpoint','cold_prefix_lineage'):
            assert not db.execute(f'SELECT 1 FROM {table} WHERE symbol=?',(alias,)).fetchone()
    return dict(segment=segment, next_open=RpdState.restore(rpd[0]).next_open,
                rpd_sha=sha256(rpd[0].encode()).hexdigest(),
                atr_sha=sha256(json.dumps(atr).encode()).hexdigest(), lineage=lineage)

def main():
    assert AUTH_SHA is None and ROOT.is_mount()
    if RESTART!='resume':
        assert not list(ROOT.iterdir())
    assert next(l.split(' - ')[1].split()[0] for l in Path('/proc/self/mountinfo').read_text().splitlines()
                if l.split()[4]==str(ROOT))=='tmpfs'
    assert os.statvfs(DATA).f_flag & os.ST_RDONLY
    assert not any(Path(p).exists() for p in ('/var/run/docker.sock','/oi20-docker-publication','/run/openmarket-credential'))
    os.umask(0o077)
    # Reuse the same owned boot/markets helpers and stopped lineage as the current fixture.
    helpers=runpy.run_path('/fixture-driver/current_locked_startup_check.py')
    seed_markets=helpers['seed_public_exchange_markets']
    seed_markets.__globals__.update(BASE=SYMBOL[:-4],SYMBOL=SYMBOL,PAIR=PAIR,NATIVE_GUA=True)
    if RESTART!='resume':
        helpers['restore_scratch']()
        # Only the temp fixture's copied causal database is discarded.
        (ROOT/'causal-candles.sqlite').unlink()
    phase='mixed' if MIXED else 'carry' if RESTART=='resume' else 'cold'
    now=END-Q+65_000 if RESTART=='resume' else END-2*Q+65_000
    if EARLY_ROSTER or EARLY_EXPECT in ('start','no-start'):
        now=END-2*Q+5_000
    if MIXED:
        now=END+4_000
    generation=8 if RESTART=='resume' else 7
    loaded=None
    qualified=True
    previous_roster_state=None
    saw_absent=False
    next_now=now
    next_generation=generation
    replay_epoch=now
    real_mono=time.monotonic_ns
    mono_epoch=real_mono()
    cycle_wall_start=mono_epoch
    last_mono=mono_epoch
    meter_headers=Counter()
    shared_requests=[]
    owner_replies=[]
    cycle_count=0
    workers=[]
    state_box=[]
    seed_seen=[]
    prefix_observations=[]
    archive_wire=[]
    exact_join=[]
    decision_origins=[]
    advice_sources=[]
    first_cold_start=[]
    uncovered_tape=[]
    loop_trace=[]
    in_target_callback=False
    mixed_reads=[]
    mixed_callbacks=[]
    mixed_shared=[]
    original_enter=PaperState.__enter__
    lock_identity=None
    def state_enter(state):
        nonlocal lock_identity
        current=original_enter(state)
        assert current is state
        state_box.append(state)
        if RESTART:
            locked=os.fstat(state.fd)
            lock_identity=(locked.st_dev,locked.st_ino)
            code='''
import fcntl, os, sys
fd=os.open(sys.argv[1],os.O_RDWR|os.O_NOFOLLOW)
try:
    fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
except BlockingIOError:
    print('SECOND_PAPER_WRITER_REFUSED')
else:
    raise AssertionError('DUPLICATE_WRITER_ENTERED')
'''
            lock_path=Path(f'/proc/self/fd/{state.fd}').readlink()
            assert lock_path.parent==ROOT and lock_path.stat().st_ino==lock_identity[1]
            guard=subprocess.run([sys.executable,'-B','-c',code,str(lock_path)],check=True,
                capture_output=True,text=True,timeout=8)
            assert guard.stdout.strip()=='SECOND_PAPER_WRITER_REFUSED'
        return current

    class Clock:
        def time_ns(self):
            return now*1_000_000
        def monotonic_ns(self):
            nonlocal last_mono
            if not LEASE_CLOCK:
                return real_mono()
            last_mono=max(last_mono,mono_epoch+(now-replay_epoch)*1_000_000+(real_mono()-cycle_wall_start))
            return last_mono
        def __getattr__(self,name):
            return getattr(time,name)

    class Wire:
        def __init__(self,*,pinned_generation=False):
            self.pinned_generation=pinned_generation
            self.publication=None
        def send(self,request):
            assert request['op']=='READ_C11_V2'
            self.nonce=request['nonce']
        def receive(self,timeout):
            # A real publisher's generation identifies one immutable
            # publication. Rereading it does not renew received_at or the
            # lease. The cross-generation negative control below injects
            # renewed old frames explicitly to exercise the stricter cap.
            if self.pinned_generation:
                if self.publication is None or self.publication[0]!=generation:
                    self.publication=(generation,now-5000)
                published=self.publication[1]
                left=published+60000-now
            else:
                published,left=now-1,55000
            source=datetime.fromtimestamp(published/1000,timezone.utc)
            lease=datetime.fromtimestamp((published+60000)/1000,timezone.utc)
            roster=[{'symbol':SYMBOL,'native_oi_change_24h_raw':'0.25' if qualified else '0.00'}]
            roster.extend({'symbol':f'BAR{i}USDT','native_oi_change_24h_raw':'0.00'} for i in range(59))
            return dict(nonce=self.nonce,status='FRESH' if left>0 else 'STALE',source=entry_feed.SOURCE,
                query_sha256=entry_feed.QUERY,offset=0,page_size=60,generation=generation,left_ms=max(left,0),
                received_at=source.isoformat(),lease_until=lease.isoformat(),rows=roster)
    feed=entry_feed.EntryFeed(Wire(pinned_generation=EXACT_CALLBACK_CLOCK),1)
    feed.classification=N(verified={SYMBOL:PAIR},received_monotonic_ns=time.monotonic_ns())
    if LEASE_CLOCK:
        # Moving the replay across closed boundaries also ages metadata.
        # Reload only the same fixture-verified market, never public network.
        feed._reviewed_metaclass_fetch=lambda:N(verified={SYMBOL:PAIR},
                                                received_monotonic_ns=time.monotonic_ns())
    observer=N(point=lambda sym,_: (generation,1,Decimal('.25')) if sym==SYMBOL and qualified else None)
    if MIXED:
        from mixed_replay_evidence import assert_known_once, load_causal_tape, observer_feed
        print('C11_MIXED_STAGE LOCKED_DRIVER_INPUTS_VERIFIED',flush=True)
        mixed_start,mixed_cutoff,mixed_rows=load_causal_tape(
            Path('/archive-wire'),tail,'57265967efa958cd734e8ed4070ea91f1d703e445deef5fec32385de35df729e',END,SHIFT)
        print('C11_MIXED_STAGE REQUIRED_CANONICAL_TAPE_VERIFIED',flush=True)
        # A real missing closed minute, rather than an artificial response or fabricated candle.
        mixed_hole=mixed_start+8*M
        c=Clock()
        feed,observer,mixed_observer,publication=observer_feed(ROOT,SYMBOL,PAIR,
            (DATA/'contract.json').read_bytes(),lambda:now,c.monotonic_ns)
        publication.publish(generation,qualified=True)
        print('C11_MIXED_STAGE INITIAL_PUBLICATION_STAGED',flush=True)
    @contextmanager
    def intake(*_):
        yield feed,observer

    def block_order(*_a,**_kw):
        raise AssertionError('NO_ORDER_ALLOWED_IN_JCT_REPLAY')

    earlier_minutes={}
    if MIXED:
        earlier_minutes={t:list(r.row) for t,r in mixed_rows.items()}
    elif EXACT_CALLBACK_CLOCK:
        # Fill only the missing replay cassette, not the worker database.
        # Restore genuine closed rows from the same hashed official archive.
        import csv,io,zipfile
        from cold_prefix import verify_archive_blob
        path=DATA/'jct_official_archives' if REVIEWED_PARTIAL else Path('/archive-wire')
        grid=base['end_ms']
        days={datetime.fromtimestamp((grid-i*24*60*M)/1000,timezone.utc).strftime('%Y-%m-%d')
              for i in range(1,5)}
        entries=json.loads((path/'manifest.json').read_bytes())
        for record in entries:
            if record.get('date') not in days:continue
            name=record['name']; raw=(path/name).read_bytes()
            verify_archive_blob(name,record['bytes'],record['sha256'],raw,(path/(name+'.CHECKSUM')).read_bytes())
            assert record['actual_received_ms']<END-2*Q
            with zipfile.ZipFile(io.BytesIO(raw)) as z, z.open(z.namelist()[0]) as f:
                rows=csv.reader(io.TextIOWrapper(f,newline='')); header=next(rows)
                assert header[0]=='open_time' and len(header)==12
                for r in rows:
                    assert len(r)==12
                    r[0],r[6],r[8]=int(r[0]),int(r[6]),int(r[8])
                    assert r[0]<grid and r[6]==r[0]+M-1
                    r=translated(r)
                    assert r[0] not in earlier_minutes
                    earlier_minutes[r[0]]=r
        assert len(earlier_minutes)==4*24*60 and not set(earlier_minutes)&bars.keys()
        earlier_minutes.update(bars)

    def candle_response(method,url,shared=False):
        u=urlsplit(str(url))
        if (method,u.scheme,u.netloc)!=('GET','https','fapi.binance.com'):
            raise AssertionError('UNSUPPORTED_FIXTURE_ENDPOINT')
        q=parse_qs(u.query,strict_parsing=True)
        if u.path=='/fapi/v1/klines':
            assert q['symbol']==[SYMBOL] and all(len(x)==1 for x in q.values())
            width={'1m':M,'5m':FIVE,'15m':Q}[q['interval'][0]]
            stop=int(q.get('endTime',[str(now-1)])[0])+1
            start=int(q.get('startTime',['0'])[0])
            limit=int(q.get('limit',['500'])[0])
            assert 0 < limit <= 1500
            rowmap=earlier_minutes if EXACT_CALLBACK_CLOCK and shared and width==M else rows_by_width[width]
            rows=[rowmap[t] for t in sorted(rowmap) if start<=t<stop and t+width<=min(now,stop)][-limit:]
            if MIXED and shared:
                assert start>=mixed_start, 'C11_MIXED_REOPENED_LISTING_START'
                # Generation 7 genuinely lacks terminal raw input. The fixture
                # supplies a short response, not a fake hold or readiness.
                if generation==7 and stop==END:
                    return []
                for row in rows:
                    assert mixed_rows[row[0]].available_at<=now, 'C11_MIXED_USED_FUTURE_SOURCE'
                mixed_shared.append((generation,start,stop,limit,len(rows)))
            if shared:
                if HAN_MODE:
                    assert 'symbol=%E7%89%9B%E6%9D%A5USDT' in u.query
                assert width==M and stop<=now
                if (EXACT_CALLBACK_CLOCK or PINNED_REMOTE) and len(rows)!=limit:
                    # Optional primary reads can precede the captured tape.
                    # Return the actual short body so the original row-count
                    # guard handles it. Never synthesize rows or raise in IPC.
                    uncovered_tape.append((start,stop,limit,len(rows)))
                    return rows
                assert len(rows)==limit
                shared_requests.append((start,stop,len(rows),now))
            else:
                owner_replies.append((width,len(rows),now))
            return rows
        if u.path in ('/fapi/v1/ticker/price','/fapi/v2/ticker/price'):
            return {'symbol':SYMBOL,'price':'0.01'} if q.get('symbol') else [{'symbol':SYMBOL,'price':'0.01'}]
        # Fail closed before network. Unexpected public requests are visible without URL/parameters.
        raise AssertionError('UNSUPPORTED_FIXTURE_FAPI_PATH:'+u.path)

    def sync_send(_session,prepared,**_):
        response=requests.Response()
        response.status_code=200
        response.headers.update({'Content-Type':'application/json','x-mbx-used-weight-1m':'40'})
        response._content=json.dumps(candle_response(prepared.method,prepared.url)).encode()
        response.request=prepared
        response.url=prepared.url
        meter_headers[now//M]+=1
        return response
    async def async_send(_session,method,url,*,ssl=True,**_):
        meter_headers[now//M]+=1
        return helpers['ArchivedResponse'](url,json.dumps(candle_response(method,url)))

    class Response:
        def __init__(self,url,rows):
            self.url=url; self.history=(); self.status=200; self.reason='OK'; self.content=self
            self.headers=CIMultiDict({'x-mbx-used-weight-1m':'43','Content-Type':'application/json'})
            self.body=json.dumps(rows).encode(); self.content_length=len(self.body)
        async def iter_chunked(self,_):
            yield self.body
        async def text(self, errors='replace'):
            return self._body.decode(errors=errors)
    class FixtureSession:
        closed=False
        @asynccontextmanager
        async def get(self,url,allow_redirects,**kw):
            assert allow_redirects is False
            yield Response(url,candle_response('GET',url,shared=True))

    def spawn():
        p,c=socket.socketpair()
        p.setblocking(False); c.setblocking(False)
        def serve():
            with c:
                try:
                    asyncio.run(shared_history_child.serve(c,8,FixtureSession()))
                except ConnectionResetError:
                    pass
        process=Thread(target=serve,daemon=True,name='isolated-jct-http-fixture')
        process.start()
        workers.append(process)
        f=Future();f.set_result(p)
        return f

    def assert_lease_controls(bot):
        from current_history import CurrentHistory
        moment=[1_000_000_000]
        roster_generation=[70]
        mono=lambda:moment[0]
        class ControlledWire(Wire):
            def receive(self,timeout):
                return {**super().receive(timeout),'generation':roster_generation[0]}
        shadow=entry_feed.EntryFeed(ControlledWire(),1)
        shadow.classification=N(verified={SYMBOL:PAIR},received_monotonic_ns=mono())
        owner=N(feed=shadow,exchange=bot.exchange)
        tap=CurrentHistory(owner,lambda:now,mono,())
        def read():
            tap.before_protection()
            shadow.read()
        with patch.object(entry_feed,'time',N(monotonic_ns=mono)):
            read()
            (prior,)=tap.facts()
            moment[0]=prior.expires_ns-1
            assert tap.current(prior)
            moment[0]=prior.expires_ns
            assert not tap.current(prior)
            read()  # A repeated numeric generation must not renew the cap.
            assert not tap.facts() and not tap.current(prior)
            roster_generation[0]+=1
            read()
            (fresh,)=tap.facts()
            assert fresh.generation==prior.generation+1 and tap.current(fresh) and not tap.current(prior)
        print('C11_REVIEWED_FIXTURE_LEASE_CONTROLS_PASS',flush=True)

    def attach(run):
        nonlocal loaded
        from atr_er_checkpoint import AtrErCheckpoint
        from current_history import CurrentHistory
        if REVIEWED_PARTIAL:
            assert_lease_controls(run.worker.freqtrade)
        h=BoundOwnerHistory.__new__(BoundOwnerHistory)
        h.worker=run.worker; h.run=run; h.owner=get_ident()
        h.tap=CurrentHistory(run.worker.freqtrade,run.wall,run.plan.mono,())
        h.store=run.worker.freqtrade.policy.bars
        if RESTART=='resume':
            loaded=state_receipt(h.store.db)
            assert loaded['next_open']==END-3*Q
        else:
            assert all(h.store.db.execute(f'SELECT count(*) FROM {tab}').fetchone()==(0,)
                       for tab in ('segment','candle','rpd_checkpoint','atr_er_checkpoint'))
        h.atr=AtrErCheckpoint(h.store)
        if MIXED:
            print('C11_MIXED_STAGE ORIGINAL_OWNER_LEDGER_BOUND',flush=True)
            if MIXED=='unusable':
                from causal_bars import SegmentStart
                old=SegmentStart(SYMBOL,ORIGIN,ORIGIN)
                h.store.set_segment(old)
                with h.store.db:
                    h.store.db.execute('INSERT INTO rpd_checkpoint VALUES (?,?)',
                        (SYMBOL,RpdState.start(old).pack()))
            # Seed only proven raw rows. No derived state, votes or readiness.
            # Both variants have the same real one-minute history hole.
            # Each must acquire it before its respective initial-state path.
            h.store.record(SYMBOL,M,tuple(mixed_rows[t].closed() for t in range(mixed_start,mixed_cutoff,M)
                if t!=mixed_hole))
        if REVIEWED_PARTIAL or AUTO_STALE:
            # Simulate the real half-initialized shape in this private
            # temporary store using the separately pinned JCT origin.
            h.store.db.execute('INSERT INTO segment VALUES (?,?,?)',(SYMBOL,ORIGIN,ORIGIN if AUTO_STALE else run.wall()))
            if AUTO_STALE:
                stale=RpdState(SYMBOL,ORIGIN,ORIGIN,ORIGIN).pack()
                h.store.db.execute('INSERT INTO rpd_checkpoint VALUES (?,?)',(SYMBOL,stale))
            h.store.db.commit()
        h.pending=h.last_background=h.archive_prep=h.opg_recovery=None
        h.history_sink=None
        # No legacy 5m/15m background transport is allowed in this fixture.
        h.io=N(spawn=None,active=None,tick=lambda:None,close=lambda:None)
        def store_check():
            state_box[0].check_locked()
            assert h.store.db.execute('PRAGMA database_list').fetchall()==[(0,'main','/c11-paper/causal-candles.sqlite')]
        h.check_store=store_check
        # Fixture-only: identify which original predicate is holding a page.
        # Every acceptance decision still comes from the unmodified authority.
        last_refusal={}
        def observed(label, owner_lane):
            book=owner_lane.pending
            decide=book.authority
            original_facts=h.tap.facts
            def authority(job, witness):
                fact_checks=[]
                def observe_facts():
                    rows=original_facts()
                    fact_checks.append((rows,h.tap.mono()))
                    return rows
                # Capture the rows actually evaluated by current(). Do not
                # perform a second lookup that could move the cap/generation.
                with patch.object(h.tap,'facts',observe_facts):
                    ok=decide(job,witness)
                last_refusal.clear()
                if ok:
                    return ok
                fact=witness if label=='minute' else witness.fact
                now=run.wall()
                reasons=[]
                if not job.valid(now): reasons.append('JOB_INVALID')
                if fact_checks:
                    rows,mono=fact_checks[-1]
                    exact=[row for row in rows if
                           (row.pair,row.symbol,row.generation,row.boundary,row.origin)==
                           (fact.pair,fact.symbol,fact.generation,fact.boundary,fact.origin)]
                    if not exact or not any(mono<min(row.expires_ns,fact.expires_ns) for row in exact):
                        reasons.append('FACT_NOT_CURRENT')
                        feed=h.tap.feed
                        if feed.owner_cycle_stamp()<=h.tap.cycle_stamp:
                            reasons.append('FACT_NO_SUCCESSFUL_READ_THIS_CYCLE')
                        raw=[row for row in feed.latest_owner_observation if row.symbol==fact.symbol]
                        if not raw: reasons.append('FACT_SYMBOL_NOT_OBSERVED')
                        if feed.generation_floor!=fact.generation:
                            reasons.append('FACT_GENERATION_CHANGED')
                        if now//Q*Q!=fact.boundary: reasons.append('FACT_BOUNDARY_CHANGED')
                        if mono>=fact.expires_ns: reasons.append('FACT_ORIGINAL_LEASE_EXPIRED')
                        if raw and all(mono>=row.expires_ns for row in raw):
                            reasons.append('FACT_OBSERVED_LEASE_EXPIRED')
                        if any(row.pair!=fact.pair for row in raw):
                            reasons.append('FACT_PAIR_CHANGED')
                        cap=h.tap.caps.get((fact.symbol,fact.pair))
                        if cap is not None and mono>=cap:
                            reasons.append('FACT_GENERATION_CAP_EXPIRED')
                        if raw and not rows and mono<fact.expires_ns and cap is not None and mono<cap:
                            reasons.append('FACT_EMPTY_AFTER_CYCLE_FILTERS')
                if label=='minute':
                    if not owner_lane.cycle_headroom(): reasons.append('NO_CYCLE_HEADROOM')
                    if run.queue.inflight: reasons.append('MINUTE_ORIGINAL_WIRE_BUSY')
                else:
                    scope=witness.scope
                    hold=owner_lane.phase_hold()
                    if hold: reasons.append(hold)
                    if not scope.end<=now<scope.expires: reasons.append('OUTSIDE_SCOPE')
                    if (job.symbol,job.generation)!=(fact.symbol,fact.generation):
                        reasons.append('JOB_FACT_MISMATCH')
                    if any((other.symbol,other.end)==(scope.symbol,scope.end)
                           for other,_,_ in run.queue.inflight.values()):
                        reasons.append('SAME_SYMBOL_WIRE_BUSY')
                    if not run.plan.current(scope,now): reasons.append('PLAN_NOT_CURRENT')
                    if (scope.generation,scope.end,scope.pair) in run.plan.last:
                        reasons.append('ALREADY_DECIDED')
                last_refusal.update(lane=label,reasons=reasons or ['HOLD_NOT_REOBSERVED'])
                return ok
            book.authority=authority
        def trace(event):
            if event.get('stage_reason') in ('SCOPE_INVALID_AT_READ','SCOPE_AND_PARENT_DELIVERY_EXPIRED'):
                event={**event,'fixture_scope_read':dict(last_refusal)}
            loop_trace.append(event)
        h.shared_minute=SharedHistoryForwarder(run.queue.limiter,run.wall,spawn,trace=trace)
        h.minute_admission=MinuteAdmissionLoop(h,h.shared_minute.lane)
        h.primary_admission=NativePrimaryLane(h,h.shared_minute.lane)
        if LEASE_CLOCK:
            observed('minute',h.minute_admission)
            observed('primary',h.primary_admission)
        if AUTO_MODE:
            from auto_cold_archive import AutoColdAdmission, AutomaticArchive, Hold, Receipt
            cassette=Path('/archive-wire')
            manifest=json.loads((cassette/'manifest.json').read_bytes())
            endpoints={row['url']:row for row in manifest}
            class TapeHTTP:
                def get(self,url,maximum,cancel):
                    if cancel.is_set():
                        raise Hold('AUTO_ARCHIVE_CANCELLED')
                    sidecar=url.endswith('.CHECKSUM')
                    base_url=url.removesuffix('.CHECKSUM')
                    if MIXED:
                        raise AssertionError('C11_MIXED_OPENED_LISTING_ARCHIVE')
                    if NO_LISTING_ORIGIN:
                        print('C11_NO_LISTING_CASE_OPENED_ARCHIVE',flush=True)
                        raise AssertionError('archive lookup prohibited by fixture')
                    archive_wire.append((url,sidecar))
                    if ARCHIVE_OUTAGE:
                        assert sidecar,'outage tried file payload without a verified checksum'
                        raise Hold('AUTO_ARCHIVE_TRANSIENT',wait_ms=30_000)
                    row=endpoints[base_url]
                    name=row['name']+('.CHECKSUM' if sidecar else '')
                    with (cassette/name).open('rb') as handle:
                        raw=handle.read(maximum+1)
                    return Receipt(raw,row.get('checksum_ms',row['actual_received_ms'])
                                   if sidecar else row['actual_received_ms'])
            runtime=json.loads(Path('/opt/c11_owned_modules/release_stage_runtime.json').read_bytes())
            raw_mode='unseeded_recent_minute_bootstrap'
            if MIXED:
                # Modern proof uses the exact policy sealed into the shipped
                # artifact. The fixture cannot promote a disabled release mode.
                assert runtime.get(raw_mode)=='BINANCE_SHARED_RECENT_1M_V1', 'C11_MIXED_POLICY_NOT_SEALED'
            elif EARLY_ROSTER or AUTO_STALE or ON_DEMAND or ARCHIVE_OUTAGE or NO_LISTING_ORIGIN or PINNED_WITHOUT_DISCOVERY:
                # Keep the established explicit archive and outage controls
                # on their original source mode while separate mixed cases
                # exercise the sealed modern source route. Do not weaken
                # their original pinned acquisition or strategy assertions.
                runtime.pop(raw_mode,None)
            if NO_LISTING_ORIGIN or PINNED_WITHOUT_DISCOVERY:
                # Remove only the hosted comparator's archive-origin option.
                # The identical market cassette and original Worker stay in place.
                runtime['unseeded_archive_discovery']='REVIEWED_OFFICIAL_ARCHIVE_ONLY_V1'
            if 'cold_archive_preparation' in runtime:
                from auto_cold_archive import OfficialArchiveHTTP
                from cold_archive_binding import plan
                from stage_pin import HISTORY_PREP_ENABLED
                # Supply only the approved replay clock and captured public wire.
                # Lane selection and construction both use the installed recipe.
                tape_http=TapeHTTP()
                start=AutomaticArchive.start
                patches.enter_context(patch.object(OfficialArchiveHTTP,'get',
                    lambda _self,*args:tape_http.get(*args)))
                patches.enter_context(patch.object(AutomaticArchive,'start',
                    lambda _self,fact,cancel:start(_self,fact,cancel,grid_shift=SHIFT)))
                sources={}
                cache_origins={}
                if CACHED or PINNED_REMOTE:
                    from dataclasses import replace
                    from cold_archive_release import installed_sources
                    cache_origins=installed_sources(runtime,run.wall(),verify_content=CACHED and not BACKGROUND_REVIEWED)
                    assert cache_origins and SYMBOL in cache_origins
                    if not PREPARED_CHECKPOINT:
                        # Keep the independent raw-cache/remote path regressions.
                        cache_origins={name:replace(src,prepared_sha256=None) for name,src in cache_origins.items()}
                    sources=({name:replace(src,download_only=True) for name,src in cache_origins.items()}
                             if PINNED_REMOTE else
                             {name:replace(src,origin=src.origin+SHIFT,cutoff=src.cutoff+SHIFT)
                              for name,src in cache_origins.items()})
                h.archive_prep=plan(runtime,sources,run.wall,HISTORY_PREP_ENABLED)
                if NO_LISTING_ORIGIN:
                    assert not sources and not h.archive_prep.discover_unseeded
                    print('C11_CI_ABSENT_CHECKPOINT_NO_LISTING_SETUP',flush=True)
                if PINNED_WITHOUT_DISCOVERY:
                    assert sources and all(src.download_only for src in sources.values())
                    assert not h.archive_prep.discover_unseeded
                    print('C11_REVIEWED_REMOTE_ONLY_NO_LISTING_DISCOVERY',flush=True)
                if ON_DEMAND or ARCHIVE_OUTAGE:
                    assert not sources and h.archive_prep.discover_unseeded
                    assert 'unseeded_archive_discovery' not in runtime
                    assert not archive_wire
                    print('C11_ON_DEMAND_PUBLIC_ARCHIVE_NO_PRELOADED_SOURCE',flush=True)
                h.cold_prefix=h.archive_prep.cold_admission(run.wall)
                if CACHED:
                    from cold_prefix import derive
                    if PREPARED_CHECKPOINT:
                        assert cache_origins[SYMBOL].prepared_sha256,'C11_REVIEWED_PREPARED_PIN_MISSING'
                        def refuse_listing_origin(*_args,**_kwargs):
                            raise AssertionError('C11_REVIEWED_PREPARED_OPENED_LISTING_ARCHIVE')
                        import cold_prefix
                        patches.enter_context(patch.object(cold_prefix,'derive_records',refuse_listing_origin))
                    def read_on_fixture_clock(sym,_spec,wall,cancel):
                        # Only the original pinned public cassette uses the shifted clock.
                        if BACKGROUND_REVIEWED:
                            assert cache_origins[sym].prepared_sha256 is None
                        prefix=derive(sym,cache_origins[sym],wall,cancel,grid_shift=SHIFT)
                        if PREPARED_CHECKPOINT:
                            assert prefix.rpd.next_open==base['end_ms']+SHIFT and prefix.rpd.origin==ORIGIN
                            print('C11_RECOVERY_REUSED_PREPARED_CHECKPOINT',flush=True)
                        return prefix
                    h.cold_prefix.reviewed.prepare=read_on_fixture_clock
                    if BACKGROUND_REVIEWED:
                        # The owner starts with no history candidate or
                        # strategy checkpoint. Release the recorded roster
                        # only after the same real background lane is ready.
                        original_history_facts=h.tap.facts
                        background_released=[False]
                        def staged_candidates():
                            if not background_released[0]:
                                if SYMBOL not in h.cold_prefix.reviewed.prewarmed:
                                    return ()
                                assert not h.cold_prefix.reviewed.pending
                                assert h.store.segment(SYMBOL) is None
                                background_released[0]=True
                                print('C11_RAW_ONLY_STATE_READY_BEFORE_CANDIDATE',flush=True)
                            return original_history_facts()
                        h.tap.facts=staged_candidates

                    # A cache-only recovery may not silently reopen the listing scan.
                    def deny_archive_download(*_args):
                        raise AssertionError('C11_CACHED_RECOVERY_USED_ARCHIVE_NETWORK')
                    patches.enter_context(patch.object(OfficialArchiveHTTP,'get',deny_archive_download))
                if PINNED_REMOTE:
                    assert not h.cold_prefix.reviewed.sources
                    # A reviewed URL/hash catalogue authorizes the fixture
                    # reader. No cached archive body or checkpoint is read.
                    def deny_cached(*_args):
                        raise AssertionError('C11_PINNED_RECOVERY_USED_RAW_CACHE')
                    h.cold_prefix.reviewed.prepare=deny_cached
                auto_lane=h.cold_prefix.public
            else:
                auto=AutomaticArchive(TapeHTTP(),run.wall)
                auto_lane=AutoColdAdmission(run.wall,lambda fact,cancel:auto.start(fact,cancel,grid_shift=SHIFT))
                h.cold_prefix=auto_lane
            assert not auto_lane.sources and not auto_lane.pending and not archive_wire
        elif not REPLAY_MODE:
            from cold_prefix import ClosedPrefixSource,ColdPrefixAdmission,derive
            source=ClosedPrefixSource(DATA/'jct_official_archives',base['archive_manifest_sha'],
                                      base['origin_ms'],base['end_ms'])
            # Only the fixture's provider input translates the archived clock.
            replay_source=ClosedPrefixSource(source.directory,source.manifest_sha,
                                              source.origin+SHIFT,source.cutoff+SHIFT)
            def prepare_replay(sym,_spec,wall,cancel):
                return derive(sym,source,wall,cancel,grid_shift=SHIFT)
            h.cold_prefix=ColdPrefixAdmission({SYMBOL:replay_source},run.wall,prepare=prepare_replay)
        run.isolated_history=h
        return h

    def check_mixed_ledger():
        import sqlite3
        p=ROOT/'trades.c11.paper.sqlite'
        with sqlite3.connect(p.as_uri()+'?mode=ro',uri=True) as db:
            assert all(db.execute(f'SELECT count(*) FROM {t}').fetchone()==(0,) for t in ('trades','orders'))

    class Done(Exception):
        pass
    original_cycle=native.PaperWorker._worker
    history=None
    def cycle(self,*a,**kw):
        nonlocal history,now,phase,generation,cycle_count,next_now,next_generation,qualified,previous_roster_state,saw_absent,cycle_wall_start
        previous_generation=generation
        now,generation=next_now,next_generation
        if MIXED and generation!=previous_generation:
            publication.publish(generation,qualified=True)
            print('C11_MIXED_STAGE NEXT_SAME_CLOSE_ROSTER_PUBLISHED',flush=True)
        cycle_wall_start=real_mono()
        scheduler=self._c11_owned_scheduler.scheduler
        if history is None:
            history=attach(scheduler)
        early_final=None
        if EARLY_EXPECT in ('deliver','hold'):
            public=history.cold_prefix.public
            active=public.active
            if active is not None and active[2].done() and active[2].exception() is None:
                proof=active[2].result()
                if type(proof) is tuple:
                    assert active[0]==SYMBOL and not active[1].is_set()
                    # Availability predates this simulated boundary. Move the
                    # clock forward, publish a fresh roster, and keep the
                    # original worker's intake and writing code unchanged.
                    now=(now//Q+1)*Q+20_000
                    generation+=1
                    early_final=proof
            assert history.store.segment(SYMBOL) is None
        # Synthetic receipt at the exact same shared limiter ingress as the
        # real CCXT meter. Historical HTTP itself remains disconnected.
        scheduler.queue.limiter.note_authoritative_ip_header(now,40,2400)
        with ExitStack() as clock_patches:
            for name,mod in tuple(sys.modules.items()):
                if name.startswith('freqtrade.') and callable(getattr(mod,'dt_now',None)):
                    clock_patches.enter_context(patch.object(mod,'dt_now',lambda:datetime.fromtimestamp(now/1000,timezone.utc)))
            result=original_cycle(self,*a,**kw)
        cycle_count+=1
        if MIXED:
            observed_proof=history.store.contract_segment(SYMBOL,ORIGIN,now)
            if (observed_proof is not None and observed_proof.open_ms==mixed_start
                    and not getattr(history,'_mixed_stage_verified',False)):
                history._mixed_stage_verified=True
                print('C11_MIXED_STAGE NATIVE_CANONICAL_STATE_PERSISTED',flush=True)
            if now>=END+120_000 or cycle_count>=300:
                slot=history.cold_prefix.public.pending.get(SYMBOL)
                proof_status=('absent' if observed_proof is None else
                              'recent' if observed_proof.open_ms==mixed_start else 'other')
                print('C11_MIXED_DIAGNOSTICS',json.dumps(dict(
                    generation=generation,age_ms=now-END,proof=proof_status,
                    slot=slot.status if slot else None,
                    minute=history.minute_admission.reason.get(SYMBOL),
                    minute_pending=history.minute_admission.pending.busy(SYMBOL),
                    covers=len(scheduler.covers),warm_scopes=len(scheduler.warm.selected),
                    statuses=sorted({x['status'] for x in advice_sources}),
                    history_requests=len(mixed_shared),callback_count=len(mixed_callbacks),
                    read_count=len(mixed_reads))),flush=True)
                raise AssertionError('C11_MIXED_NO_ORIGINAL_READ')
            assert now//Q==END//Q
            next_now=now+250
            if generation==7:
                assert not mixed_callbacks and not mixed_reads, 'C11_MIXED_PREMATURE_ORIGINAL_CALLBACK'
                # Let original authority record a real same-close pending
                # state before the next immutable publication supersedes it.
                if any(a['status']!='ORIGINAL_NATIVE_CALLBACK' for a in advice_sources):
                    next_generation=8
            elif mixed_callbacks:
                # Do not end on first result: require two subsequent actual
                # original Worker cycles without another callback.
                verify_on=mixed_callbacks[0].setdefault('verify_on',cycle_count+2)
                if cycle_count>=verify_on:
                    assert_known_once(mixed_callbacks,mixed_reads,target=END,generation=8)
                    assert not archive_wire
                    proof=history.store.contract_segment(SYMBOL,ORIGIN,now)
                    assert proof and proof.open_ms==mixed_start
                    late=[(a,b,n,k) for _g,a,b,n,k in mixed_shared if b<=mixed_cutoff]
                    expected=[(mixed_hole,mixed_hole+M,1,1)]
                    assert late==expected, ('C11_MIXED_HISTORY_REQUESTS',late,expected)
                    assert not Trade.get_open_trades()
                    check_mixed_ledger()
                    print(json.dumps(dict(status='C11_FINAL_MIXED_PACKAGED_PASS',variant=MIXED,
                        callback_count=len(mixed_callbacks),read_count=len(mixed_reads),
                        original_status=mixed_reads[0]['status'],rpd=mixed_reads[0]['rpd'],generation=8)),flush=True)
                    raise Done
            # Yield in real time to let the documented public transport and
            # background planner process their asynchronous replies.
            time.sleep(0.02)
            return result
        if REVIEWED_PARTIAL and history.shared_minute.waiting:
            # Synthetic time advances several seconds per owner poll. Give
            # the real thread on the other socket a chance to publish READY
            # or REPLY before advancing that clock. Don't consume or accept
            # the message here; the next original worker poll owns that.
            from select import select
            bridge=history.shared_minute
            sock=bridge.sock
            if sock is None and bridge.future is not None and bridge.future.done():
                sock=bridge.future.result()
            if sock is not None:
                select([sock],[],[],0.20)
        why=history.minute_admission.reason.get(SYMBOL)
        if EARLY_ROSTER and not first_cold_start:
            lane=history.cold_prefix.reviewed if CACHED else history.cold_prefix.public
            if lane.active is not None:
                assert lane.active[0]==SYMBOL
                first_cold_start.append(now%Q)
        if why is not None:
            if why not in seed_seen:
                seed_seen.append(why)
            if not any(item['phase']==phase and item['reason']==why for item in prefix_observations):
                prefix_observations.append(dict(phase=phase,reason=why,age_ms=now%Q))

        if REPLAY_MODE and cycle_count>=2:
            assert history.store.segment(SYMBOL) is None and not shared_requests
            print('JCT_BASELINE_NO_SCHEDULABLE_STATE',seed_seen,flush=True)
            raise Done
        state=history.store.db.execute('SELECT payload FROM rpd_checkpoint WHERE symbol=?',(SYMBOL,)).fetchone()
        cursor=RpdState.restore(state[0]).next_open if state else None
        if EARLY_EXPECT in ('start','no-start'):
            active=history.cold_prefix.public.active
            assert history.store.segment(SYMBOL) is None
            assert (active is not None)==(EARLY_EXPECT=='start'), (EARLY_EXPECT,why)
            if active is not None:assert active[0]==SYMBOL
            assert not Trade.get_open_trades() and now%Q==5000
            print(json.dumps(dict(status='C11_COLD_ASYNC_STARTED' if active else 'C11_COLD_ASYNC_HELD',
                 age_ms=now%Q,isolated=True,no_orders=True)),flush=True)
            raise Done
        if early_final is not None:
            source,prefix=early_final
            assert source.cutoff==prefix.rpd.next_open and prefix.rpd.observed<=now
            result=dict(status='C11_COLD_EARLY_DELIVERED' if cursor else 'C11_COLD_EARLY_HELD',
                        symbol=SYMBOL,age_ms=now%Q,archive_count=len(archive_wire),observed=prefix.rpd.observed,
                        cursor=cursor,isolated=True,no_orders=not bool(Trade.get_open_trades()),
                        authority_generation=generation,original_strategy_callback_claim=False)
            assert result['no_orders'] and now%Q==20_000
            if EARLY_EXPECT=='deliver':
                lineage=state_receipt(history.store.db)['lineage']
                assert cursor==source.cutoff and lineage[-1]==now
            else:
                assert cursor is None and history.store.segment(SYMBOL) is None
            print(json.dumps(result),flush=True)
            raise Done
        # Do not teleport the fixture to a different quarter between the
        # original async READY/GO/REPLY stages. The 950 ms GO deadline and all
        # production authority checks remain in force. Live owner polling
        # stays unmodified; this only controls the fixture's market-time jump.
        pending_replay_wire=EXACT_CALLBACK_CLOCK and bool(
            history.shared_minute.waiting or history.primary_admission.pending.pending
            or history.minute_admission.pending.pending)
        if phase=='temporarily_absent':
            assert not qualified and not any(f.symbol==SYMBOL for f in history.tap.facts())
            assert state_receipt(history.store.db)==previous_roster_state
            saw_absent=True
            qualified=True
            phase='carry'
            next_now=now+1000
            next_generation+=1
        elif phase=='cold' and not pending_replay_wire and (cursor==END-3*Q or
                (EXACT_CALLBACK_CLOCK and cursor in (END-2*Q,END-Q))):
            # Polls observe committed state, not every intermediate cursor.
            # In this reviewed case the independent accepted path can catch
            # up again before the fixture observes the preceding quarter.
            assert now<END-Q
            if RESTART=='prepare':
                assert len(archive_wire)==56 and not Trade.get_open_trades()
                assert [r[2] for r in shared_requests]==[285]
                print(json.dumps(dict(status='HAN_RESTART_PREPARED',pid=os.getpid(),
                    state=state_receipt(history.store.db),archive_requests=len(archive_wire),
                    native_requests=shared_requests,writer_lock=lock_identity,
                    second_writer_refused=True)),flush=True)
                raise Done
            next_now=END-Q+(5_000 if (EARLY_ROSTER or EXACT_CALLBACK_CLOCK) else 65_000); next_generation+=1; phase='carry'
            if ROSTER_RETURN:
                previous_roster_state=state_receipt(history.store.db)
                phase='temporarily_absent'
                qualified=False
        elif (INIT_MODE and phase=='cold' and generation==7 and cursor==base['end_ms']+SHIFT
                and any(e.get('stage_reason') in ('STALE_SCOPE','STALE_SCOPE_OR_DELIVERY',
                                                       'SCOPE_INVALID_AT_READ','SCOPE_AND_PARENT_DELIVERY_EXPIRED')
                        and e.get('symbol')==SYMBOL for e in loop_trace)):
            # A later fixture generation authorizes a real retry; it cannot
            # make the previous stale receipt admissible.
            next_now+=5000
            next_generation+=1
        elif (REVIEWED_PARTIAL and phase=='cold' and generation==7
                and cursor==base['end_ms']+SHIFT+495*M):
            # The publisher genuinely issues a new generation. An older
            # observation can never lend its authority to the next page.
            next_now+=5000
            next_generation+=1
        elif (AUTO_MODE and not HAN_MODE and not INIT_MODE and phase=='cold' and generation==7
                and cursor==base['end_ms']+SHIFT+495*M):
            # A newly recorded generation renews owner authority. The earlier
            # fixture generation stays expired; it cannot authorize a second page.
            next_now+=5000
            next_generation+=1
        elif phase=='carry' and not pending_replay_wire and (cursor==END-2*Q or (EXACT_CALLBACK_CLOCK and cursor==END-Q)):
            # Accepted pages are committed asynchronously. The original
            # worker may advance one additional closed quarter before the
            # fixture observes a cursor. Do not strand the replay on the
            # prior close or skip to an already-processed target.
            assert now<END
            next_now=END+4000; next_generation+=1; phase='entry'
        else:
            step=1000 if phase=='cold' and (cursor is None or (AUTO_STALE and cursor==ORIGIN)) and AUTO_MODE else 5000
            next_now+=min(step,250) if pending_replay_wire else step
        if EARLY_ROSTER and cycle_count%4==0:
            slot=None if CACHED else history.cold_prefix.public.pending.get(SYMBOL)
            f=slot.fact if slot else None
            data=dict(age=now%Q,cursor=cursor,ready=bool(slot and slot.result is not None),
                      status=slot.status if slot else None,current=bool(f and history.tap.current(f)),
                      primary_pending=history.primary_admission.pending.busy(SYMBOL),
                      minute_pending=history.minute_admission.pending.busy(SYMBOL),
                      original_wire=any(j.symbol==SYMBOL for j,_,_ in history.run.queue.inflight.values()),
                      facts=len(history.tap.facts()))
            print('JCT_EARLY_PATH',json.dumps(data),flush=True)
        if cycle_count%4==0:
            print('JCT_CYCLE',cycle_count,phase,cursor,why,scheduler.queue.limiter.last_history_hold
                  if hasattr(scheduler.queue.limiter,'last_history_hold') else None,
                  len(shared_requests),len(owner_replies),flush=True)
        veto=history.store.db.execute('SELECT state FROM feature_status WHERE symbol=? AND feature=? AND end_ms=?',
                                      (SYMBOL,'RPD5_VETO_NATIVE_RECURRENCE',END)).fetchone()
        primary=history.store.db.execute('SELECT state FROM feature_status WHERE symbol=? AND feature=? AND end_ms=?',
                                         (SYMBOL,'NW15_PRIMARY_BEFORE_RPD',END)).fetchone()
        original_calls=[a for a in advice_sources if a['status']=='ORIGINAL_NATIVE_CALLBACK']
        # The original Freqtrade strategy can consume the rebuilt state itself
        # before the data-only fallback writes its separate feature marker.
        # Prove the real exact-end VETO happened inside that native callback
        # and the returned last row does not request entry; no inferred VETO.
        strategy_native_veto=(EXACT_CALLBACK_CLOCK and len(original_calls)==1
            and original_calls[0].get('last_enter_long')==0
            and any(d['status']=='VETO' and d.get('boundary')==END
                    and d.get('inside_original_target_callback') is True
                    and d['owner_code']=='causal_bars.py' and d['method']=='inputs'
                    and d['age_ms']==original_calls[0]['age_ms'] for d in decision_origins))
        observed_veto=('VETO' in exact_join and veto==('VETO',)) or strategy_native_veto
        if (EARLY_ROSTER or PREPARED_CHECKPOINT or BACKGROUND_REVIEWED) and observed_veto and not original_calls and now<END+54_000:
            return result
        primary_no_signal=(HAN_MODE or INIT_MODE) and phase=='entry' and primary==('PRIMARY_NO_SIGNAL',)
        if observed_veto or primary_no_signal:
            if REVIEWED_PARTIAL:
                sent_generations={e.get('generation') for e in loop_trace
                                  if e.get('stage')=='GRANT_WRITTEN'}
                assert {7,8}<=sent_generations, 'C11_NO_FRESH_GENERATION_RETRY_REQUEST'
            assert not Trade.get_open_trades()
            exact_optional_veto=(veto==('VETO',) and any(
                d['status']=='VETO' and d.get('boundary')==END
                and d['owner_code']=='native_primary_lane.py' and d['method']=='record_verified_veto'
                and 0<=d['age_ms']<60_000 for d in decision_origins))
            # An exact admitted terminal veto is a real original 5m judgment,
            # even when full entry advice is blocked by that filter.
            assert (cursor<=END if original_calls or exact_optional_veto else cursor<END), 'UNWITNESSED_VETO_CURSOR_ADVANCE'
            stale_cursor_age_ms=None
            if REVIEWED_PARTIAL or AUTO_STALE:
                recovered=history.store.db.execute(
                    'SELECT cutoff,manifest_sha,previous_state FROM c11_state_recovery_history WHERE symbol=?',
                    (SYMBOL,)).fetchone()
                assert recovered[0]==base['end_ms']+SHIFT
                saved_rpd=json.loads(recovered[2])['rpd']
                if AUTO_STALE:
                    prior_cursor=RpdState.restore(saved_rpd).next_open
                    assert len(recovered[1])==64 and prior_cursor==ORIGIN
                    stale_cursor_age_ms=END-prior_cursor
                else:
                    assert recovered[1]==base['archive_manifest_sha'] and saved_rpd is None
            if AUTO_MODE:
                expected_requests=0 if RESTART=='resume' or CACHED or NO_LISTING_ORIGIN else 56 if HAN_MODE else 88 if INIT_MODE else 74
                assert len(archive_wire)==expected_requests and len({url for url,_ in archive_wire})==expected_requests
                if HAN_MODE:
                    assert all('%E7%89%9B%E6%9D%A5USDT' in url for url,_ in archive_wire)
                assert not (DATA/'jct-base.local.json').exists()
                if not AUTO_STALE:
                    lineage=history.store.db.execute('SELECT manifest_sha FROM cold_prefix_lineage WHERE symbol=?',
                                                      (SYMBOL,)).fetchone()
                    assert lineage is not None
            if RESTART=='resume':
                assert [(a,b,n) for a,b,n,_ in shared_requests]==[(END-3*Q,END-2*Q,15),(END-2*Q,END,30)]
                resumed=state_receipt(history.store.db)
                assert resumed['segment']==loaded['segment'] and resumed['lineage']==loaded['lineage']
                assert resumed['next_open']==loaded['next_open']+Q==END-2*Q
                assert resumed['rpd_sha']!=loaded['rpd_sha'] and resumed['atr_sha']!=loaded['atr_sha']
                print(json.dumps(dict(status='HAN_RESTART_RESUMED',pid=os.getpid(),loaded_state=loaded,
                    advanced_state=resumed,archive_requests=len(archive_wire),native_requests=shared_requests,
                    primary='PRIMARY_NO_SIGNAL',zero_orders=True,writer_lock=lock_identity,
                    second_writer_refused=True)),flush=True)
            if EARLY_ROSTER:
                assert first_cold_start and first_cold_start[0]<60_000
            if ROSTER_RETURN:
                assert saw_absent and qualified
                resumed=state_receipt(history.store.db)
                assert resumed['segment']==previous_roster_state['segment']
                assert resumed['lineage']==previous_roster_state['lineage']
                assert resumed['next_open']>previous_roster_state['next_open']
            label='HAN' if HAN_MODE else 'INIT' if INIT_MODE else 'JCT'
            suffix='PRIMARY_NO_SIGNAL' if primary_no_signal else '5M_VETO'
            print(json.dumps(dict(status=f'{label}_ORIGINAL_{suffix}',symbol=SYMBOL,
                   isolated=True,calendar_shift_ms=SHIFT,
                   original_signal_boundary=witness['end'],replay_boundary=END,cycles=cycle_count,
                   prefix_statuses=seed_seen,shared_1m_requests=shared_requests,
                   original_join=exact_join,zero_orders=True,real_decision_time_claim=False,
                   decision_sources=decision_origins,observed_replay_age_ms=now-END,
                   original_entry_advice=advice_sources,strategy_native_veto=strategy_native_veto,
                   initial_cold_start_age_ms=first_cold_start[0] if first_cold_start else None,
                   uncovered_tape_requests=len(uncovered_tape),
                   verified_archive_minute_only=(bool(archive_wire)
                       and all('/1m/' in urlsplit(url).path for url,_ in archive_wire)),
                   automatic_archive=AUTO_MODE,verified_raw_archive_cache=CACHED,pinned_remote=PINNED_REMOTE,
                   discovery_configured=bool(getattr(history.archive_prep,'discover_unseeded',False)),automatic_stale_repair=AUTO_STALE,reviewed_partial_repair=REVIEWED_PARTIAL,roster_drop_return_verified=saw_absent,archive_public_receipts=len(archive_wire),
                   stale_cursor_age_ms=stale_cursor_age_ms,
                   retained_stages=dict(Counter(x.get('stage',x.get('reason')) for x in loop_trace)))),flush=True)
            raise Done
        if ARCHIVE_OUTAGE and archive_wire and cycle_count>=12:
            assert cursor is None and not Trade.get_open_trades()
            assert not original_calls and veto!=('VETO',)
            print(json.dumps(dict(status='JCT_ARCHIVE_OUTAGE_HELD',archive_public_receipts=len(archive_wire),
                                  no_cursor=True,no_strategy_callback=True,zero_orders=True)),flush=True)
            raise Done
        if cycle_count>=36 or (REVIEWED_PARTIAL and phase=='entry' and now>=END+60_000):
            print(json.dumps(dict(status='JCT_REPLAY_HELD',phase=phase,cursor=cursor,cycles=cycle_count,
                  prefix_observations=prefix_observations,
                  prefix_statuses=seed_seen,shared_1m_requests=shared_requests,original_join=exact_join,
                  uncovered_tape_requests=uncovered_tape[:10],
                  archive_public_receipts=len(archive_wire),
                  retained_stages=dict(Counter(x.get('stage',x.get('reason')) for x in loop_trace)),
                  entry_gate_statuses=sorted({x['reason'] for x in loop_trace
                     if x.get('end_ms')==END and isinstance(x.get('reason'),str)}),
                  entry_advice_statuses=sorted({x['status'] for x in advice_sources}),
                  entry_primary=primary[0] if primary else None,
                  entry_rpd_checks=sorted(rpd_entry_checks),
                  persist_refusals=[x for x in loop_trace if x.get('stage')=='PERSIST_REJECTED'],
                  reply_refusals=[x for x in loop_trace if x.get('stage')=='REPLY_REJECTED'])),flush=True)
            raise RuntimeError('JCT_NO_ORIGINAL_VETO_RECEIPT')
        return result

    from unified_owner import UnifiedScheduler
    actual_consume=UnifiedScheduler.consume_ready_only
    def observe_consume(self,pair,end,frame,callback):
        if (end,pair)!=(END,PAIR):
            return actual_consume(self,pair,end,frame,callback)
        def witness_original():
            nonlocal in_target_callback
            in_target_callback=True
            if MIXED:
                mixed_callbacks.append(dict(end_ms=end,generation=generation,read_at=now,called=True))
            try:return callback()
            finally:in_target_callback=False
        result=actual_consume(self,pair,end,frame,witness_original if EXACT_CALLBACK_CLOCK else callback)
        event=dict(status=result[0],age_ms=now-END)
        if EXACT_CALLBACK_CLOCK and result[0]=='ORIGINAL_NATIVE_CALLBACK':
            frame=result[1]
            if frame is not None and 'enter_long' in frame and len(frame):
                event['last_enter_long']=int(frame['enter_long'].iloc[-1])
        advice_sources.append(event)
        return result

    rpd_entry_checks=set()
    original_veto=NativePrimaryLane.record_verified_veto
    def observed_veto(lane,f,s):
        if REVIEWED_PARTIAL and s.end==END and f.symbol==SYMBOL:
            store=lane.owner.store
            witness=lane.terminal_five.get(f.symbol)
            rec=store.db.execute("SELECT payload FROM rpd_checkpoint WHERE symbol=?",(f.symbol,)).fetchone()
            proof=store.segment(f.symbol)
            state=RpdState.restore(rec[0]) if rec else None
            if witness is None:
                rpd_entry_checks.add("NO_TERMINAL_MINUTE_WITNESS")
            elif witness[:2]!=(s.generation,s.end):
                rpd_entry_checks.add("TERMINAL_WITNESS_SCOPE_MISMATCH")
            if state is None or proof is None:
                rpd_entry_checks.add("NO_STATE_OR_SEGMENT")
            elif state.observed>now:
                rpd_entry_checks.add("STATE_OBSERVATION_AFTER_REPLAY_CLOCK")
            elif not s.end-2*Q<=state.next_open<=s.end-Q:
                rpd_entry_checks.add("STATE_CURSOR_NOT_ON_TERMINAL_WINDOW")
            elif witness is not None:
                five={bar.open_ms:bar for bar in witness[2]}
                for opening in range(state.next_open,s.end,FIVE):
                    rows=store.range(f.symbol,FIVE,opening,opening+FIVE,now)
                    candidate=five.get(opening)
                    if not rows and candidate is None:
                        rpd_entry_checks.add("MISSING_FIVE_MINUTE_INPUT")
                    elif rows and candidate is not None:
                        dims=('op','high','low','close','volume','quote')
                        if any(getattr(rows[0],k)!=getattr(candidate,k) for k in dims):
                            rpd_entry_checks.add("STORED_AND_SYNTHESIZED_FIVE_MINUTE_MISMATCH")
        return original_veto(lane,f,s)

    import causal_bars
    original_join=causal_bars.join
    original_inputs=causal_bars.CausalBars.inputs
    def observe_inputs(self,symbol,end,as_of,**kwargs):
        value=original_inputs(self,symbol,end,as_of,**kwargs)
        if MIXED and in_target_callback and (symbol,end)==(SYMBOL,END):
            primary,state=kwargs.get('primary'),kwargs.get('state')
            assert primary is not None and state is not None, 'C11_MIXED_MISSING_ORIGINAL_INPUT'
            status,_=original_join(causal_bars.Scope(symbol,end),as_of,[state])
            mixed_reads.append(dict(end_ms=end,generation=generation,read_at=as_of,
                status=primary[2].status,rpd=status,real_read=True))
        return value
    def observed_join(*a,**kw):
        result=original_join(*a,**kw)
        if a[0].symbol==SYMBOL:
            exact_join.append(result[0])
            caller=sys._getframe(1)
            decision_origins.append(dict(status=result[0],age_ms=now-END,boundary=a[0].end_ms,
                inside_original_target_callback=in_target_callback,
                owner_code=Path(caller.f_code.co_filename).name,method=caller.f_code.co_name))
        return result

    def seed_for_jct(exchange,*,reload):
        seed_markets(exchange,reload=reload)
        for api in (exchange._api,exchange._api_async):
            if api.markets and PAIR in api.markets:
                api.markets[PAIR]['info']['onboardDate']=ORIGIN

    clock=Clock()
    import feature_warmup
    import native_candles
    import native_refresh_cycle
    factory=installed.installed_site_factory(RUNTIME_SHA,lambda *_:None)
    try:
        with ExitStack() as patches:
            if MIXED:
                import observer_host
                # Keep the real observer freshness predicate, but reference
                # the same explicit causal tape clock as the paper Worker.
                patches.enter_context(patch.object(observer_host,'now_ms',lambda:now))
                # Historical replay never waits for real minute closes. Skip
                # only the outer throttle's real wait while advancing the
                # fixture clock and gifting real IPC threads a short turn.
                patches.enter_context(patch.object(native.PaperWorker,'_sleep',lambda *_,**__:None))
            if LEASE_CLOCK:
                import refresh
                # Align the fixture's monotonic lease with its market time,
                # without changing the original per-cycle CPU budget or the
                # IPC deadline (the latter uses real time.monotonic()).
                patches.enter_context(patch.object(time,'monotonic_ns',clock.monotonic_ns))
                patches.enter_context(patch.object(installed,'monotonic_ns',clock.monotonic_ns))
                patches.enter_context(patch.object(refresh,'monotonic_ns',clock.monotonic_ns))
            for obj,key,value in (
                (PaperState,'__enter__',state_enter),(native,'OWNED_SCHEDULER_FACTORY',factory),
                (native,'OWNED_SCHEDULER_EXPECTED_SEAL',RUNTIME_SHA),(native.PaperWorker,'_worker',cycle),
                (native,'time',clock),(standalone_policy,'time',clock),(installed,'time_ns',clock.time_ns),
                # For this replay, the original direct candle receipt and
                # warmup share the same captured clock as the Worker. Do not
                # let the runner wall clock mark archived state "from the future".
                *(([(native_candles,'time',clock),(feature_warmup,'time',clock),
                    (native_refresh_cycle,'time',clock)]) if EXACT_CALLBACK_CLOCK else []),
                (UnifiedScheduler,'consume_ready_only',observe_consume),
                (causal_bars.CausalBars,'inputs',observe_inputs),
                (NativePrimaryLane,'record_verified_veto',observed_veto),
                (shared_history_child,'millis',lambda:now),(async_native_ccxt,'time_ns',clock.time_ns),
                (causal_bars,'join',observed_join),(native_primary_lane,'join',observed_join),
                (Exchange,'_load_async_markets',seed_for_jct),(Exchange,'check_time_offset',lambda _:None),
                (Binance,'fill_leverage_tiers',lambda _:None),(Binance,'ft_additional_exchange_init',lambda _:None),
                (requests.Session,'send',sync_send),(aiohttp.ClientSession,'_request',async_send),
                (C11PaperExchange,'create_order',block_order),(C11PaperExchange,'cancel_order',block_order),
            ):
                patches.enter_context(patch.object(obj,key,value))
            v4_entry.run_locked(dict(generation_floor=1,publication_binding='isolated-jct',wallet_usdt=1000.),
                None,PaperState,verify_state_file,intake,lambda scope,pairs:native.config_for(scope['wallet_usdt'],pairs),
                standalone_policy.build_policy,native.PaperWorker)
    except Done:
        return 2 if REPLAY_MODE else 0
    finally:
        if MIXED:
            mixed_observer.close()
        for worker in workers:
            worker.join(3)
    raise AssertionError('UNEXPECTED_UNBOUNDED_WORKER_RETURN')


if __name__=='__main__':
    raise SystemExit(main())
