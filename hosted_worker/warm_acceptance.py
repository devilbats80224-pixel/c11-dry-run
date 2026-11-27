"""Installed constructors with a blank paper ledger, and synthetic exchange facts."""
import sys
sys.path[:]=["/opt/c11_owned_modules","/opt/c11_worker","/opt/c11_worker/ports"]+[p for p in sys.path if p!="/fixture"]
sys.path.extend(('/opt/c11_background_review','/scratch'))
mode='trial'  # comparison label only: ordinary imports from the immutable image
from decimal import Decimal
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path
from types import SimpleNamespace as N
from unittest.mock import patch
import os
import time
intent_case=os.environ.get('C11_INTENT_CASE','flat')
incident=os.environ.get('C11_SYNTHETIC_XTF_NEGATIVE')=='yes'
assert intent_case in ('flat','multilane','historical_revision','orphan_entry','orphan_exit','canonical','canonical_history','busy','authority_before','authority_after')
refusal={'canonical':'ATOMIC_CANONICAL_CONFLICT','canonical_history':'ATOMIC_CANONICAL_CONFLICT','busy':'ATOMIC_SQLITE_BUSY','authority_before':'ATOMIC_AUTHORITY_BEFORE_ROWS','authority_after':'ATOMIC_AUTHORITY_AFTER_ROWS'}.get(intent_case)

import aiohttp
import ccxt
import requests
from freqtrade.exchange import Exchange, Binance
from freqtrade.freqtradebot import FreqtradeBot
from freqtrade.persistence import Trade
from freqtrade.worker import Worker
from standalone_policy import build_policy
from native_worker import PaperWorker, PaperBot, config_for
import native_worker as native
import sealed_owned_scheduler as pins
import installed_build_inputs as site
from c11_paper_recovery_runner import C11PaperRecoveryRunner
from c11_paper_exchange import C11PaperExchange
from contextlib import contextmanager
import json, tarfile, shutil, re
from paper_runtime import PaperState, verify_state_file
import startup_status
import v4_entry
import entry_feed

ROOT=Path('/c11-paper')
PAIR='STBL/USDT:USDT'
UNKNOWN_PAIR='ON/USDT:USDT'
assert version('freqtrade')=='2026.10' and version('ccxt')=='4.5.90'
assert sha256(Path('/freqtrade/freqtrade/worker.py').read_bytes()).hexdigest()=='9c5a046404f69e061ead72595f8a48f84eb19d9025331f8ff91f3bfebf767f65'
assert Path(Exchange.__init__.__code__.co_filename).is_relative_to('/freqtrade/freqtrade')
assert Path(FreqtradeBot.__init__.__code__.co_filename).is_relative_to('/freqtrade/freqtrade')
assert Path(C11PaperExchange.__init__.__code__.co_filename).is_relative_to('/opt/c11_worker')
assert pins.OWNED_SCHEDULER_FACTORY is pins.OWNED_SCHEDULER_EXPECTED_SEAL is None
assert not any(Path(p).exists() for p in ('/oi20-provider-secret','/oi20-docker-publication','/run/openmarket-credential'))
assert not list(ROOT.iterdir())
os.umask(0o077)
events=[]
def reject_wire(*args,**kwargs):
    events.append('PROHIBITED_WIRE')
    raise AssertionError('FIXTURE_WIRE_FORBIDDEN')
async def reject_wire_async(*args,ssl=True,**kwargs):
    reject_wire()
def seed_public_exchange_markets(exchange, *, reload):
    assert type(exchange) is C11PaperExchange and reload is True
    assert exchange._config['dry_run'] is True
    assert exchange._api.id == exchange._api_async.id == 'binance'
    assert not any(getattr(api,k,None) for api in (exchange._api,exchange._api_async) for k in ('apiKey','secret','password'))
    async def initialize():
        exchange._api_async.open()
        assert type(exchange._api_async.session) is aiohttp.ClientSession
        # Stand-in physical response, but this is the actual original guarded
        # async CCXT public lookup which normal market bootstrap itself uses.
        info=await exchange._api_async.fapiPublicGetExchangeInfo()
        assert info['timezone']=='UTC'
    exchange.loop.run_until_complete(initialize())
    exchange._api_async.set_markets([dict(
        id=base+'USDT',symbol=pair,base=base,quote='USDT',settle='USDT',
        baseId=base,quoteId='USDT',settleId='USDT',type='swap',
        spot=False,swap=True,future=False,option=False,margin=False,
        active=True,contract=True,linear=True,inverse=False,contractSize=1,
        maker=0.0002,taker=0.0005,precision={'amount':0.001,'price':0.001},
        limits={'amount':{'min':0.001,'max':100000},'price':{'min':0.001,'max':10000},
                'cost':{'min':5,'max':None},'leverage':{'min':1,'max':20}},
        info={'contractType':'PERPETUAL','status':'TRADING','onboardDate':SEED['origin_ms']}) for base,pair in (('STBL',PAIR),('ON',UNKNOWN_PAIR))])
    events.append('synthetic-public-metadata')

from urllib.parse import urlsplit,parse_qs
from docker_observer import DockerObserver,CaptureBridge
from observer_rpc import ObserverClient
import packaged_entry
from hashlib import sha256
import sqlite3
W=json.load(open('/fixture/WIRE_PRIVATE.json')); E=W['E']
MIX=json.load(open('/fixture/MIXED_WIRE_PRIVATE.json'))
assert MIX['E']==E and MIX['g']==W['generation'] and MIX['pub']==W['actual_page'] and MIX['expiry']==W['expiry']
MISSING='ONUSDT'
B='a'*64
SEED=json.load(open('/fixture/STBL_SEED.json'))
assert SEED['symbol']=='STBLUSDT' and SEED['artifact_available_ms']<E and SEED['end_ms']+21*300000==E
MODE=os.environ.get('C11_ISOLATED_CASE','proof_then_revisit')
assert MODE in {'proof_then_revisit','observe_default'}
paths=[]
exact_requests=[]
def valid(q):
    p=urlsplit(str(q))
    assert p.scheme=='https' and p.hostname=='fapi.binance.com'
    paths.append(p.path)
    if p.path=='/fapi/v1/exchangeInfo':return {'timezone':'UTC','symbols':[]}
    assert p.path=='/fapi/v1/klines',f'UNAVAILABLE_OFFLINE_PUBLIC_PATH:{p.path}'
    fields=parse_qs(p.query); tf=fields.get('interval',[''])[0]; start=int(fields.get('startTime',['0'])[0]); end=int(fields.get('endTime',[str(E-1)])[0]);lim=int(fields.get('limit',['500'])[0])
    assert fields['symbol'] in (['STBLUSDT'],[MISSING])
    exact_requests.append((fields['symbol'][0],tf,lim))
    rr=W['actual_public_receipts'] if fields['symbol']==['STBLUSDT'] else tuple(MIX['rows'][MISSING].values())
    for x in rr:
      if x['timeframe']==tf:
        assert x['receive_ms']<=clock[0],('no time travel',fields['symbol'],tf,clock[0],x['receive_ms'])
        source=x['raw_kline_rows']
        if tf=='1m' and lim==45:
          earlier=json.load(open('/proof/earlier.json'))
          u=next(r for r in earlier['receipts'] if (r['symbol'],r['timeframe'],r['limit'])==('STBLUSDT','1m',30))
          assert earlier['end_ms']==E-2*900000 and u['receive_ms']<E
          source=u['raw_kline_rows']+source
        rows=[r for r in source if start<=r[0]<=end][-lim:]
        if tf=='1m' and lim==45: assert len(rows)==45 and rows[0][0]==E-45*60000
        print('ARCHIVED_PUBLIC_SEND',tf,start,end,lim,len(rows),clock[0]-E,x['receive_ms']-E,flush=True)
        if incident and fields['symbol']==['STBLUSDT'] and tf=='5m' and lim==21:
          # Recorded failure modality only, not historical source or OPG live tape.
          rows=[list(row) for row in rows]
          rows[-1][5]=str(Decimal(rows[-1][5])+Decimal('0.01'))
        return rows
    raise ValueError(f'NO_ARCHIVED_INTERVAL:{tf}')
def send(self,request,**kwargs):
    assert request.method=='GET'
    rows=valid(request.url)
    r=requests.Response(); r.status_code=200; r.headers['Content-Type']='application/json';r.headers['x-mbx-used-weight-1m']='12';r._content=json.dumps(rows).encode();r.url=request.url;r.request=request
    r.history=[]; return r
class AsyncResponse:
 status=200
 reason='OK'
 history=()
 headers={'Content-Type':'application/json','x-mbx-used-weight-1m':'12'}
 def __init__(self,url,rows):
    self.url=url
    self.body=json.dumps(rows)
 async def __aenter__(self):return self
 async def __aexit__(self,*a):return None
 async def text(self,*a,**k):return self.body
 async def json(self,*a,**k):return json.loads(self.body)
 async def read(self,*a,**k):return self.body.encode()
 def release(self):pass
async def async_send(self,method,url,*,ssl=True,**kwargs):
    assert method=='GET'
    return AsyncResponse(url,valid(url))
class ReplayPublication:
    def response(self,nonce):
        raw=W['actual_page']
        return {**raw,'nonce':nonce,'status':'FRESH','left_ms':W['expiry']-clock[0]}
def main(outer):
    manifest=Path('/opt/c11_owned_modules/release_stage_runtime.json').read_bytes()
    h=sha256(manifest).hexdigest()
    assert h==os.environ['C11_EXACT_FINAL_RUNTIME_SHA']
    locked_stage=json.loads(manifest)
    assert locked_stage['release_enabled'] is False
    import adapter,coordinator,dedicated_queue,shared_native_budget,unified_owner,stage_pin
    for m in (adapter,coordinator,dedicated_queue,shared_native_budget,unified_owner):
        assert Path(m.__file__) == Path('/opt/c11_owned_modules')/(m.__name__+'.py')
    assert Path(native.__file__)==Path('/opt/c11_worker/native_worker.py')
    assert stage_pin.RUNTIME_SHA==h
    expected_final = os.environ['C11_OFFLINE_EXPECT_BAKED_SCOPE'] == 'yes'
    assert (stage_pin.AUTH_SHA is not None) is expected_final
    assert Path('/opt/c11_owned_modules/fresh_owner_scope.json').is_file() is expected_final
    from inspect import getsource
    import release_stage
    from authorization_boundary import validate_baked
    assert not validate_baked(None,stage_pin.AUTH_SHA,h,stage_pin.CATALOG_SHA,E)
    activation_source=getsource(release_stage.activate)
    print('PROVED_BAKED_SCOPE_PRESENT_NO_ACTIVATION_CALLED',flush=True)
    assert 'fresh_owner_scope.json' in activation_source and 'return 2' in activation_source
    assert (stage_pin.AUTH_SHA is not None) is expected_final
    # Scope is intentionally not loaded or consumed by this test. No use
    # of activate(): ABI and native engine provenance, not an authorization test.
    from release_stage import check as check_image
    # Check file custody at the combined fixture review time, not the earlier
    # warm market event. No closed cache is supplied to this warm worker.
    check_ms=os.environ.get("C11_ARCHIVE_CATALOG_CHECK_MS")
    if check_ms is None:
        check_image()
    else:
        assert check_ms.isdecimal()
        with patch.object(time,'time_ns',lambda:int(check_ms)*1_000_000):
            check_image()
    print('ORDINARY_PACKAGE_MODULES_NO_INJECTED_OVERLAY',flush=True)
    for filename,pin in locked_stage['files'].items():
        p=Path(filename)
        assert os.statvfs(p).f_flag&os.ST_RDONLY and sha256(p.read_bytes()).hexdigest()==pin
    print('PINNED_RELEASE_PACKAGE_RUNTIME_MANIFEST_READBACK',h,flush=True)
    import startup_status
    o=DockerObserver(ROOT/'docker-observer.sqlite',B)
    f=None
    b=CaptureBridge(ReplayPublication(),o,B,lambda:f.classification)
    b.send(dict(op='C11_PAPER_BRIDGE_V1',nonce='0'*32));b.receive(2)
    f=entry_feed.EntryFeed(b,1)
    f.classification=N(verified={'STBLUSDT':PAIR,MISSING:UNKNOWN_PAIR},body_sha256=MIX['meta'],received_monotonic_ns=time.monotonic_ns())
    obs=ObserverClient(b,B)
    assert tuple(x.symbol for x in f.read())==(MISSING,'STBLUSDT')
    assert f.tracking_pairs()==(UNKNOWN_PAIR,PAIR)
    cfg=config_for(1000.0,[UNKNOWN_PAIR,PAIR])
    assert cfg['dry_run'] and not cfg['exchange'].get('key')
    seal=sha256(Path('/opt/c11_owned_modules/compiled_candidate_factory.py').read_bytes()).hexdigest()
    advice=[]
    terminal=[]
    def trace(pair,status):
        advice.append((clock[0]-E,pair,status))
        print('ACTUAL_ORIGINAL_ADVICE',status,flush=True)
    from durable_loader import SeedLoader
    from outer_seed_lock import ScopedSeedFactory
    seed_scope=ScopedSeedFactory(review_catalog=Path('/fixture/seed_catalog'),
                  review_pin='928ff248adb9a127d9788e6e362e73df82d1c125d9444911104defe05e2ff78f',
                  runtime_manifest_sha=h,expected_manifest_sha=os.environ['C11_EXACT_FINAL_RUNTIME_SHA'],isolated_test=True)
    try:seed_scope.open(None,None)
    except ValueError as e:assert str(e)=='C11_EXISTING_OUTER_WRITER_REQUIRED'
    else:raise AssertionError('ATTACH_WITHOUT_EXISTING_WRITER')
    from owner_history_binding import OfflineHistoryReview
    review=OfflineHistoryReview()
    deactivate=seed_scope.activate(outer)
    with patch.object(requests.Session,'send',send),patch.object(aiohttp.ClientSession,'_request',async_send),          patch.object(Exchange,'_load_async_markets',seed_public_exchange_markets),patch.object(Exchange,'check_time_offset',lambda _:None),          patch.object(Binance,'fill_leverage_tiers',lambda _:None),patch.object(Binance,'ft_additional_exchange_init',lambda _:None),          patch.object(C11PaperExchange,'create_order',lambda *_a,**_k:(_ for _ in ()).throw(AssertionError('NO_PAPER_ORDER_TEST_BOUNDARY'))), \
         patch.object(native,'OWNED_SCHEDULER_FACTORY',site.installed_site_factory(seal,trace,diagnostic_sink=terminal.append,prior_seed_factory=seed_scope.open,history_review=review)),patch.object(native,'OWNED_SCHEDULER_EXPECTED_SEAL',seal):
      from causal_bars import SegmentStart,ClosedBar,CausalBars
      from native_candles import NativeCandles
      from durable_loader import SeedLoader
      # Archive-to-canonical fixture at actual historical receipt, not earlier.
      def prime_unknown(policy):
        for tf,x in MIX['rows'][MISSING].items():
          assert x['receive_ms']<=clock[0]
          width={'1m':60000,'15m':900000}[tf]
          policy.bars.record(MISSING,width,[ClosedBar(int(z[0]),x['receive_ms'],*(float(v) for v in z[1:6]),float(z[7])) for z in x['raw_kline_rows']])
      from rpd_checkpoint import RpdState
      policy=build_policy(ROOT)
      assert policy.bars.db.execute('SELECT count(*) FROM rpd_checkpoint').fetchone()==(0,)
      frozen_extra={}
      if intent_case=='multilane':
          from dataclasses import replace
          for capture in W['actual_public_receipts']:
              width={'1m':60000,'5m':300000,'15m':900000}[capture['timeframe']]
              raw=capture['raw_kline_rows'][-2]
              clean=ClosedBar(raw[0],capture['receive_ms'],*(float(raw[k]) for k in (1,2,3,4,5,7)))
              older=replace(clean,received_ms=clean.open_ms+width+1,volume=clean.volume+1.)
              policy.bars.record('STBLUSDT',width,(older,))
              frozen_extra[width]=(older,clean)
      pending=None
      import historical_version as history
      assert Path(history.__file__).parent==Path('/opt/c11_owned_modules')
      assert history.installed_catalog(h) is None
      if intent_case=='historical_revision':
          from dataclasses import replace
          from causal_bars import QUARTER
          raw15=next(x for x in W['actual_public_receipts'] if x['timeframe']=='15m')
          wl=raw15['raw_kline_rows'][-94]
          rk='STBLUSDT'
          zone='STBL/USDT:USDT|BINANCE_USDM'
          new=ClosedBar(wl[0],raw15['receive_ms'],*(float(wl[j]) for j in (1,2,3,4,5,7)))
          old=replace(new,received_ms=new.open_ms+QUARTER+1,quote=new.quote+1.)
          policy.bars.record(rk,QUARTER,(old,))
          # No source/approval simulation injected except the review catalogue.
          # This isolated alternate uses a plainly synthetic prior and review.
          certify=history.compile_catalog(json.dumps(dict(schema=history.REVIEW_TYPE,rows=[
              dict(symbol=rk,pair=PAIR,origin=SEED['origin_ms'],open_ms=old.open_ms,
                   prior_seen_ms=old.received_ms,reference_seen_ms=E-2,reviewed_ms=E-1,
                   field_mask=32,old_pin=history.signature(zone,rk,QUARTER,old),
                   revised_pin=history.signature(zone,rk,QUARTER,new),
                   source_sha='e'*64,custody_sha='f'*64)])))
          pending=patch.object(history,'installed_catalog',lambda pin:certify if pin==h else None)
          pending.start()
      try:
          w=PaperWorker(cfg,policy,f,obs)
      finally:
          if pending is not None:pending.stop()

      try:
        # Test-only tracing; no delay in the native call chain.
        learner=getattr(w._c11_owned_scheduler,'active',w._c11_owned_scheduler)
        assert learner is not None
        amendment=[]
        if intent_case in ('multilane','orphan_exit'):
            from multilane_history_view import AuditedKey,FrozenReadView,rowpin,digest,enc,ENABLED
            from prospective_derivation import Prior,restore_memory
            from threading import get_ident
            assert ENABLED is False
            class ResearchSource:
              def claims(self,scope):
                return (scope.pair,scope.symbol,scope.end)==(PAIR,'STBLUSDT',E)
              def __call__(self,scope,batches,orig_adapter):
                assert self.claims(scope) and intent_case=='multilane'
                assert learner.scheduler.plan.current(scope,clock[0]) and orig_adapter is learner.scheduler.adapter
                assert w.freqtrade._owned_revisit_no_positions is True and w.freqtrade._owned_revisit_reconciled is True
                seed=SEED
                payload=dict(derived_state=dict(rpd_state=seed['rpd_state'],
                   atr={'300000':seed['atr']['5m'],'900000':seed['atr']['15m']}))
                raw=enc(payload); pin=digest(raw)
                prior=Prior(scope.symbol,seed['origin_ms'],seed['end_ms'],seed['artifact_available_ms'],pin,raw)
                db,atr,segment=restore_memory(prior)
                try:
                    receipt_time=max(q.received_ms for _,rs in batches for q in rs)
                    for width,rr in batches:
                        if width in (300000,900000):
                            db.record(scope.symbol,width,tuple(r for r in rr if r.open_ms>=prior.cursor))
                            assert atr.extend(scope.symbol,width,segment,E,receipt_time)==E
                    derived=db.rpd_progress(scope.symbol,E,receipt_time)
                    assert derived.next_open==E
                    material=enc(dict(rpd_state=json.loads(derived.pack()),
                       atr={str(k):dict(latest=atr.sample(scope.symbol,k,segment,E,receipt_time),
                            previous=atr.sample(scope.symbol,k,segment,E-k,receipt_time)) for k in (300000,900000)}))
                finally:db.db.close()
                keys=tuple(AuditedKey(width,a.open_ms,a.received_ms,E-2,rowpin(a,a.received_ms),rowpin(b,E-2))
                           for width,(a,b) in sorted(frozen_extra.items()))
                sel=FrozenReadView(scope.pair,scope.symbol,scope.generation,E,scope.expires,
                      get_ident(),seed['origin_ms'],clock[0],E-1,pin,
                      digest(enc(tuple((z,w) for z,w in sorted((r['row_sha256'],r['receive_ms']) for r in W['actual_public_receipts'])))),
                      digest(material),material,keys,batches)
                amendment.append(sel)
                return sel
            learner.scheduler.commit_and_decide.multilane_prepare=ResearchSource()

        if pending is not None:
            site_instance=learner.scheduler.commit_and_decide
            assert site_instance.historical is not None,'TEST_REVIEW_NOT_BOUND'
            srcprep=history.HistoricalVersion.prepare
            def traced(hh,st,scope,groups):
                rt=srcprep(hh,st,scope,groups)
                m=hh.market(scope); seg=st.segment(scope.symbol)
                print('ISOLATED_TEST_REVISION_PREPARE',rt is not None,hh.protection(scope),
                      type(m) is dict,seg is not None,flush=True)
                return rt
            extratest=patch.object(history.HistoricalVersion,'prepare',traced)
            extratest.start()
        assert learner.scheduler.queue.limiter is w._c11_bootstrap_rate.watcher.budget
        stake=cfg['stake_currency']
        wallet_start=w.freqtrade.wallets.get_total(stake)
        assert type(wallet_start) in (float,int) and wallet_start==1000.0, wallet_start
        import durable_loader as dl
        import tempfile
        a = SeedLoader.__new__(SeedLoader)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            (root/'regular').write_bytes(b'ok')
            (root/'symlink').symlink_to(root/'regular')
            os.link(root/'regular',root/'hardlink')
            a.fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY)
            try:
                for unsafe in ('symlink','hardlink'):
                    try:a.read(unsafe,64)
                    except (OSError,dl.Hold):pass
                    else:raise AssertionError('CATALOG_UNSAFE_FILE_TYPE_ACCEPTED')
            finally:os.close(a.fd)
            try:
                SeedLoader(store=policy.bars,plan=learner.scheduler.plan,catalog=root,review_pin='0'*64)
            except dl.Hold as err:assert str(err)=='IMMUTABLE_CATALOG_REQUIRED'
            else:raise AssertionError('WRITABLE_CATALOG_ACCEPTED')
        print('DURABLE_CATALOG_REFUSES_SYMLINK_HARDLINK_AND_MUTABLE_MOUNT',flush=True)
        loader=learner.scheduler.isolated_prior_seed_loader
        assert type(loader) is SeedLoader and loader.store is policy.bars
        from outer_seed_lock import GUARD_PATH
        assert loader.reviewed_store.valid(policy.bars,GUARD_PATH,loader.pin)
        try:seed_scope.open(w,learner.scheduler.plan)
        except ValueError as e:assert str(e)=='C11_ONE_LOADER_PER_OUTER_WRITER'
        else:raise AssertionError('DUPLICATE_SEED_WRITER')
        import fcntl
        fd=os.open('/c11-paper/c11-paper.writer.lock',os.O_RDONLY|os.O_NOFOLLOW)
        try:
            try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:pass
            else:raise AssertionError('SECOND_WRITER_ACQUIRED_EXISTING_LOCK')
        finally:os.close(fd)
        print('EXACT_EXISTING_OUTER_LOCK_SOLE_WRITER_SAME_LEDGER',flush=True)

        from binding import Evidence,bind,canonical
        prospective=SeedLoader(store=policy.bars,plan=learner.scheduler.plan,catalog=Path('/opt/c11-reviewed-seeds'),
                               review_pin=stage_pin.CATALOG_SHA)
        try:
            # Component eligibility uses future virtual clock and stipulated
            # roster contract only. Not an authenticated event or a write.
            nm=1795532400000
            test=[]
            symbols=tuple(sorted(json.loads(prospective.review)['slots']))
            assert len(symbols)==7 and {'OPGUSDT','PHAROSUSDT'}<=set(symbols)
            for symbol in symbols:
                x=json.loads(prospective.asset_bytes['seed',symbol])
                identity=x['contract']['payload']
                def only_probe(e,now):
                    return bind(e,now_ms=now,identity=identity,review=prospective.review,review_sha=prospective.pin,
                         read_asset=lambda role,sym:prospective.asset_bytes.get((role,sym)),is_current=lambda *_:True,
                         native_validate=lambda b:RpdState.restore(b.decode('ascii')).pack().encode('ascii'),wall_now=lambda:now)
                e=Evidence(396,symbol,symbol[:-4]+'/USDT:USDT',E,E-1,E+42_000)
                _,label=only_probe(e,E+2_000)
                assert label=='SOURCE_RECORD_PIN',(symbol,label)
                if symbol=='OPGUSDT':continue  # Real donor unavailable in old archived fixture.
                e=Evidence(2000,symbol,symbol[:-4]+'/USDT:USDT',nm,nm-1,nm+42_000)
                b,label=only_probe(e,nm+2_000)
                assert label=='PRIOR_CURSOR_SOURCE_BOUND_ONLY',(symbol,label)
                test.append((symbol,b.tail_bars))
            assert policy.bars.db.execute('SELECT count(*) FROM rpd_checkpoint').fetchone()==(0,)
            print('ACTUAL_APPROVED_SIX_CATALOG_NATIVE_PARSE_NO_BACKDATE_NO_WRITE',test,flush=True)
        finally:prospective.close()
        observed=learner.scheduler.queue.limiter
        assert observed.meta==((E-120)//60_000,12,2400) and observed.costs==(1,1), (observed.meta,observed.costs)
        assert paths==['/fapi/v1/exchangeInfo']
        from time import perf_counter_ns
        clock[0]=E+1800
        # Test only a distinct SQLite lock on the disposable CausalBars file;
        # then allow the unmodified Worker to invoke binding next cycle.
        lease_before=getattr(w.freqtrade,'_owned_original_cycle_deadline_ns',None)
        w.freqtrade._owned_original_cycle_deadline_ns=time.monotonic_ns()+1_000_000_000
        lock=sqlite3.connect('/c11-paper/causal-candles.sqlite')
        try:
            lock.execute('BEGIN EXCLUSIVE')
            at=perf_counter_ns()
            loader.apply_held(w.freqtrade)
            elapsed=(perf_counter_ns()-at)//1000
            assert elapsed<250_000 and 'STBLUSDT' not in loader.status,(elapsed,loader.status)
            print('LOCKED_SCRATCH_STORE_FAST_HOLD_AND_RETRY_ELIGIBLE_US',elapsed,flush=True)
        finally:
            lock.rollback()
            lock.close()
            w.freqtrade._owned_original_cycle_deadline_ns=lease_before
        rollover=[]
        original_set_scope=observed.set_scope
        def track_set_scope(g,end):
            original_set_scope(g,end)
            if end==E and not rollover:
                rollover.append((observed.costs,tuple(observed.boundary_spend),observed.meta))
                print('NATIVE_AUTHENTIC_CURRENT_SPEND_BEFORE_OPTIONAL_RESERVE',rollover[0],flush=True)
        observed.set_scope=track_set_scope
        print('EXACT_BOOT_HEADER_OWNED_BEFORE_CURRENT_WORKER_OPTIONAL_BIND',flush=True)
        binding=learner.scheduler.isolated_history
        assert binding is not None and binding.engine.budget is learner.scheduler.queue.limiter
        assert binding.offers==0 and not binding.closed
        import owner_history_binding as installed_history
        assert Path(installed_history.__file__).parent==Path('/opt/c11_background_review')
        assert os.stat(installed_history.__file__).st_mode&0o777==0o444
        in_post_dispatch=[False]
        actual_obs=f.read
        actual_offer=learner.scheduler.offer_after_protection
        totals=[0,0]
        def observation():
            totals[0]+=1
            if in_post_dispatch[0]:totals[1]+=1
            return actual_obs()
        def logged_offer(bot):
            x=actual_offer(bot)
            in_post_dispatch[0]=True
            return x
        f.read=observation
        learner.scheduler.offer_after_protection=logged_offer
        checked_projection=w.freqtrade._owned_exit_projection
        w.freqtrade._owned_exit_projection_cache=checked_projection()  # fixture prior-cycle exact flat projection, not substituted permissions
        def check_after_protection_only():
            assert in_post_dispatch[0], 'EXTRA_PROTECTED_PATH_SQL'
            return checked_projection()
        w.freqtrade._owned_exit_projection=check_after_protection_only
        assert w.freqtrade.strategy.process_only_new_candles is True
        atomic_probe=[]
        atomic_injected=[False]
        if intent_case.startswith('authority'):
            ready=learner.scheduler.adapter.ready
            original_authority=ready.authority
            checks=[]
            def fixture_authority(scope,now):
                truthful=original_authority(scope,now)
                if not atomic_injected[0]:return truthful
                assert truthful and policy.bars.db.in_transaction
                checks.append(1)
                return len(checks)!=(1 if intent_case=='authority_before' else 2)
            ready.authority=fixture_authority
        original_commit=policy.bars.record_owner_primary_batch
        if refusal:
          def fixture_atomic(symbol,batches,authority):
            from dataclasses import replace
            assert symbol=='STBLUSDT' and not atomic_probe
            atomic_probe.append(1)
            if intent_case in ('canonical','canonical_history'):
                width, rows=batches[-1 if intent_case=='canonical' else 0]
                row=rows[-1 if intent_case=='canonical' else -2]
                prior_received=row.received_ms if intent_case=='canonical' else E-1
                policy.bars.record(symbol,width,(replace(row,received_ms=prior_received,quote=row.quote+1.0),))
            elif intent_case=='busy':
                with sqlite3.connect('/c11-paper/causal-candles.sqlite') as lock:
                    lock.execute('BEGIN IMMEDIATE')
                    return original_commit(symbol,batches,authority)
            else:
                atomic_injected[0]=True
                try:return original_commit(symbol,batches,authority)
                finally:atomic_injected[0]=False
            return original_commit(symbol,batches,authority)
          policy.bars.record_owner_primary_batch=fixture_atomic
        import subprocess
        from concurrent.futures import ThreadPoolExecutor
        contention=os.environ.get('C11_CONTENTION')=='yes'
        # A deliberately stalled child before the first authenticated sample.
        # Its descriptor receives no page, no admission and never GO. This is
        # process-lifecycle contention only, not invented prior-market authority.
        cold=None
        if contention:
            with ThreadPoolExecutor(max_workers=1) as prepare:
                cold=prepare.submit(subprocess.Popen,[sys.executable,'-I','/opt/c11_background_review/prewarm_confined.py'],
                    stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,
                    close_fds=True,env={},cwd='/tmp',start_new_session=True).result(1)
            assert cold.stdout.readline().startswith(b'WARM_NO_AUTHORITY_CONFIRMED_ABI_')
            assert cold.poll() is None
        for i in range(3):
          clock[0]=E+2000+5000*i
          if i==0:
            prime_unknown(policy)
            from causal_bars import ClosedBar, FIVE, QUARTER
            for tf,width in (('5m',FIVE),):
                data=next(a for a in W['actual_public_receipts'] if a['timeframe']==tf)
                assert data['receive_ms']<=clock[0]
                prefix=tuple(ClosedBar(row[0],data['receive_ms'],*(float(row[j]) for j in (1,2,3,4,5,7)))
                    for row in data['raw_kline_rows'] if row[0]<(E-3*QUARTER if width==FIVE else E-QUARTER))
                policy.bars.record('STBLUSDT',width,prefix)
            assert policy.bars.range('STBLUSDT',900000,E-260*900000,E-900000,clock[0])==()
            assert policy.bars.range('STBLUSDT',FIVE,E-3*QUARTER,E-QUARTER,clock[0])==()
          if i==1:
            assert not Trade.get_open_trades()
            if intent_case=='orphan_entry':
                from c11_native_paper_runner import EntryJournal
                assert EntryJournal.reserve(policy.slots,'entry',PAIR,E-900_000,1.0)
                # Keep exit projection current: proves refusal is from actual
                # false native reconcile, not a changed projection counter.
                w.freqtrade._owned_exit_projection_cache=checked_projection()
                assert w.freqtrade._owned_exit_projection_unchanged()
            elif intent_case=='orphan_exit':
                w.freqtrade.hard_guard.arm('archived-fixture-orphan','archived-fixture-trim',10,0,5,E)
                assert not w.freqtrade._owned_exit_projection_unchanged()
                assert w.freqtrade.hard_guard.db.execute('SELECT count(*) FROM hard').fetchone()[0]==1
          print('ORIGINAL_PAPERBOT_PROCESS_ENTER',i,clock[0],flush=True)
          in_post_dispatch[0]=False
          w.freqtrade.process()
          if i==1:
            assert w.freqtrade._owned_revisit_no_positions is (intent_case not in ('orphan_entry','orphan_exit'))
            assert w.freqtrade._owned_revisit_reconciled is (intent_case!='orphan_entry')
            print('EXACT_RECONCILED_FLAT_PROTECTION_SCOPE',intent_case,w.freqtrade._owned_revisit_no_positions,
                  w.freqtrade._owned_revisit_reconciled,flush=True)
          if i==0:
            assert loader.status=={'ONUSDT':'NOT_YET_SEEDED','STBLUSDT':'PRIOR_CURSOR_DURABLY_REGISTERED_ONLY'},loader.status
            st=RpdState.restore(policy.bars.db.execute('SELECT payload FROM rpd_checkpoint WHERE symbol=?',('STBLUSDT',)).fetchone()[0])
            assert st.next_open==E-21*300000
            print('NATIVE_WORKER_AUTO_INVOKED_DURABLE_PRIOR_BINDING',loader.status,flush=True)
          assert w.freqtrade.strategy.process_only_new_candles is True
          x=w.freqtrade.dataprovider.get_analyzed_dataframe(PAIR,'15m')[0]
          owner=w._c11_owned_scheduler
          active=getattr(owner,'active',owner)
          sc=getattr(active,'scheduler',None)
          print('ORIGINAL_PAPERBOT_PROCESS_COMPLETED',i,clock[0],len(x),getattr(sc,'marker',None),
                len(sc.queue.inflight) if sc else None, len(sc.covers) if sc else None,flush=True)
          if sc:
            if i==0:
                # Isolate the later-first-analyzer scenario. The previous
                # unscoped fixture analysis is not this scenario's first attempt.
                lane=w.freqtrade.strategy._c11_native_entry_owner
                lane.holds.pop(PAIR,None)
                lane.spent.discard((PAIR,E))
                x=w.freqtrade.dataprovider.get_analyzed_dataframe(PAIR,'15m')[0]
                lane.advise(w.freqtrade.strategy,lambda frame,_meta:frame,x,{'pair':PAIR})
                assert (2000,PAIR,'HOLD_NATIVE_MINUTE_NOT_COMMITTED') in advice,advice
                print('DELAYED_MINUTE_FIRST_ANALYSIS_HELD',flush=True)
            for x in sc.adapter.capture.records.values():
                print('ACTUAL_OWNER_CAPTURE_GRADE',x['timeframe'],x['capture_grade'],x['reported_http'],len(x['raw_kline_rows']),flush=True)
            print('PROVEN',[(r.ticket.lane,len(r.raw))for r in sc.adapter._proven.values()],flush=True)
            if i==1:print('STRATEGY_SAME_CANDLE_OPTION',w.freqtrade.strategy.process_only_new_candles,flush=True)
          if i<2: w._sleep(w._throttle_secs)
        assert (PAIR,E) in sc.warm.claimed,advice
        assert len([a for a in advice if a[1:]==(PAIR,'ORIGINAL_NATIVE_CALLBACK')])==1,advice
        assert not any(a[1:]==(UNKNOWN_PAIR,'ORIGINAL_NATIVE_CALLBACK') for a in advice)
        assert not Trade.get_trades().all()
        assert [x for x in exact_requests if x[0]=='STBLUSDT' and x[1]=='1m']==[('STBLUSDT','1m',45)],exact_requests
        assert not any(x[0]=='STBLUSDT' and (x[1]=='5m' or x[1]=='15m' and x[2]!=499) for x in exact_requests),exact_requests
        assert sorted(x for x in exact_requests if x[1]=='15m')==[
          ('ONUSDT','15m',499),('STBLUSDT','15m',499)],exact_requests
        from freqtrade.enums import CandleType
        sources=w.freqtrade._c11_trial_native_frame_sources
        assert sources[(PAIR,'15m',CandleType.FUTURES)]=='direct_15m_fallback',sources
        assert not any(k[1]=='5m' and v=='direct_15m_fallback' for k,v in sources.items())
        from causal_bars import MINUTE
        reader=sqlite3.connect('file:/c11-paper/causal-candles.sqlite?mode=ro',uri=True)
        try:
            stored=[r[0] for r in reader.execute(
                'SELECT open_ms FROM candle WHERE symbol=? AND width=? AND open_ms>=? AND open_ms<? AND received_ms<=? ORDER BY open_ms',
                ('STBLUSDT',MINUTE,E-45*MINUTE,E,clock[0]))]
        finally:
            reader.close()
        assert stored==list(range(E-45*MINUTE,E,MINUTE)),('WARM_MINUTES_NOT_DURABLE',len(stored))
        print('NATIVE_FALLBACK_15M_WITH_DURABLE_1M_AND_ORIGINAL_CALLBACK',flush=True)
        print('PACKAGED_WARM_ROUTE_ORIGINAL_CALLBACK',json.dumps(exact_requests),flush=True)
        return
        # Flush the terminal bucket through its normal no-plan call; no
        # optional send will be permitted outside the original 60s window.
        clock[0]=E+60000
        learner.scheduler.offer_after_protection(w.freqtrade)
        learner.scheduler.offer_after_protection(w.freqtrade)
        receipts=[json.loads(x) for x in terminal]
        gate={r['symbol']:r for r in receipts if r['schema']=='c11-gate-readback-v1'}
        for extra in (r for r in receipts if r['schema']=='c11-gate-detail-v1'):
            own=gate[extra['symbol']]
            assert (own['generation'],own['end_ms'])==(extra['generation'],extra['end_ms'])
            own[extra['kind']+'_ms']=extra['values']
        terminal=[x for x in terminal if json.loads(x)['schema']=='c11-owner-window-counters-v1']
        assert len(terminal)==1 and b'STBL' not in terminal[0]
        assert set(gate)=={'ONUSDT','STBLUSDT'}, gate
        assert gate['ONUSDT']['first_gate']=='NO_APPROVED_PRIOR_STATE' and gate['ONUSDT']['original_callbacks']==0,gate
        if incident:
            memo=gate['STBLUSDT']
            assert memo['original_callbacks']==0
            assert memo['valid_reply_lanes']==['15m','1m','5m'],memo
            assert 'NATIVE_ATOMIC_COMMIT' not in memo['transition_ms'],memo
            assert 'ATOMIC_NATIVE_ENTERED' not in memo.get('atomic_phase_ms',{}),memo
            assert 'NORM_XTF_BASE_VOLUME' in memo.get('normalization_refusal_ms',{}),memo
            assert not Trade.get_open_trades()
            print('EXACT_ORIGINAL_PAPERBOT_TYPED_XTF_HOLD_BEFORE_SQL_NO_CALLBACK',flush=True)
            return
        assert gate['STBLUSDT']['original_callbacks']==(0 if refusal else 1),gate
        assert gate['STBLUSDT']['valid_reply_lanes']==['15m','1m','5m'],gate
        assert 'NATIVE_REPLY_5M' in gate['STBLUSDT']['transition_ms']
        at_cb=7000 if intent_case not in ('orphan_entry','orphan_exit') else 12000
        transitions=gate['STBLUSDT']['transition_ms']
        if refusal:
            assert atomic_probe==[1] and 'NATIVE_ATOMIC_COMMIT' not in transitions and 'ORIGINAL_NATIVE_CALLBACK' not in transitions
            assert transitions['HOLD_NATIVE_COMMIT_REFUSED']==[at_cb,at_cb,1],gate
            assert gate['STBLUSDT']['atomic_refusal_ms']=={refusal:[at_cb,at_cb,1]},gate
        else:
            assert 'atomic_refusal_ms' not in gate['STBLUSDT']
            assert transitions['ORIGINAL_NATIVE_CALLBACK']==[at_cb,at_cb,1],gate
            assert transitions['NATIVE_ATOMIC_COMMIT']==[at_cb,at_cb,1],gate
        if pending is not None:
            assert policy.bars.range(rk,QUARTER,old.open_ms,old.open_ms+QUARTER,clock[0])==(old,)
            assert learner.scheduler.commit_and_decide.historical is not None
            print('INSTALLED_NATIVE_BRANCH_HISTORICAL_ROW_PRESERVED_FULL_ORIGINAL_CALLBACK',flush=True)
        if intent_case=='multilane':
            assert len(amendment)==1 and amendment[0].corrected_rpd().next_open==E
            for k,(old,_) in frozen_extra.items():
                assert policy.bars.range('STBLUSDT',k,old.open_ms,old.open_ms+k,clock[0])==(old,)
            print('FULL_NATIVE_PAPERBOT_MULTILANE_ORIGINAL_THREE_ROWS_PRESERVED',flush=True)
        if intent_case=='orphan_exit':
            assert not amendment
            print('FULL_NATIVE_PAPERBOT_PENDING_PROTECTION_BYPASS_ALTERNATE_VIEW',flush=True)
        assert sum(s=='STBLUSDT' and tf=='5m' for s,tf,_ in exact_requests)==1,exact_requests
        print('AUTHENTIC_OWNER_5M_SINGLE_WIRE_NO_SYNTHETIC_RECEIPT',flush=True)
        print('EXACT_RELEASE_GATE_PER_NAME_NO_WALLET',json.dumps(gate,sort_keys=True),flush=True)
        unknown=policy.bars.db.execute("select state from feature_status where symbol=? and feature='NW15_OBSERVATION_ONLY'",(MISSING,)).fetchone()
        assert unknown==('NO_SIGNAL',),unknown
        assert policy.bars.db.execute('select 1 from rpd_checkpoint where symbol=?',(MISSING,)).fetchone() is None
        post=policy.bars.rpd_progress('STBLUSDT',E,clock[0])
        assert post is not None and post.next_open==(E-21*300000 if refusal else E)
        print('NATIVE_IFR_PRIMARY_ORIGINAL_RPD_STATE_AT_END',post.next_open,flush=True)
        print('EXACT_MIXED_UNKNOWN_OBSERVATION_ONLY_NO_RPD',unknown,flush=True)
        report=json.loads(terminal[0])
        assert report['counters']['validated_lane']==3
        assert report['counters'].get('original_callback',0)==(0 if refusal else 1)
        assert report['counters'].get('original_full_commit',0)==(0 if refusal else 1)
        print('REDACTED_WINDOW_GATE_COUNTERS',json.dumps(report,sort_keys=True),flush=True)
        assert w.freqtrade.wallets.get_total(stake)==wallet_start
        with sqlite3.connect('file:/c11-paper/trades.c11.paper.sqlite?mode=ro',uri=True) as ledger:
            counts=tuple(ledger.execute('SELECT COUNT(*) FROM '+name).fetchone()[0] for name in ('trades','orders'))
        assert counts==(0,0)
        receipt=dict(intent_case=intent_case,branch=mode,actual_pair=PAIR,e=E,attempt_at_e_ms=at_cb,committed_at_e_ms=None if refusal else at_cb,callback_at_e_ms=None if refusal else at_cb,
          calls_by_symbol_tf_limit=sorted(exact_requests),balance_initial=wallet_start,
          balance_final=w.freqtrade.wallets.get_total(stake),trades=counts[0],orders=counts[1],
          atomic_refusal=refusal,callback_count=report['counters'].get('original_callback',0),native_commit_count=report['counters'].get('original_full_commit',0))
        print('C11_ARCHIVED_FULL_PAPERBOT_AB',json.dumps(receipt,sort_keys=True),flush=True)
        if cold:
            assert cold.poll() is None
            cold.kill();assert cold.wait(.5)==-9
            print('WHOLE_NATIVE_CALLBACK_WHILE_UNAUTHORIZED_PREWARM_BLOCKED',flush=True)
        assert binding.engine.runner.current is None and binding.engine.runner.preparing is None
        assert binding.offers>=3
        print('INSTALLED_OWNER_FACTORY_BOUND_ACTUAL_AFTER_PROTECTION',binding.offers,flush=True)
        clone=Path('/c11-paper/readonly-ancestor-shape.sqlite')
        with sqlite3.connect('file:/c11-paper/trades.c11.paper.sqlite?mode=ro',uri=True) as source:
            with sqlite3.connect(clone) as dest:source.backup(dest)
        h0=sha256(clone.read_bytes()).hexdigest()
        with sqlite3.connect(f'file:{clone}?mode=ro&immutable=1',uri=True) as ro:
            assert ro.execute('PRAGMA quick_check').fetchone()==('ok',)
            assert all(ro.execute('SELECT count(*) FROM '+name).fetchone()==(0,) for name in ('trades','orders'))
            try:ro.execute('CREATE TABLE must_not_write(x)')
            except sqlite3.OperationalError as e:assert 'readonly' in str(e).lower()
            else:raise AssertionError('SOURCE_CLONE_MUTABLE')
        assert sha256(clone.read_bytes()).hexdigest()==h0 and not clone.with_suffix('.sqlite-wal').exists()
        print('SAME_LEDGER_SCHEMA_DISPOSABLE_CLONE_READONLY_NO_WRITES',flush=True)
        actual=w.freqtrade.config['db_url']
        try:
            w.freqtrade.config['db_url']='sqlite:////c11-paper/not-canonical.sqlite'
            assert not loader.reviewed_store.current(policy.bars,GUARD_PATH)
        finally:w.freqtrade.config['db_url']=actual
        assert loader.reviewed_store.current(policy.bars,GUARD_PATH)
        # Disrupt only this disposable fixture mount so no real lock is affected.
        for n in ('c11-paper.writer.lock','trades.c11.paper.sqlite'):
            target=ROOT/n
            backup=ROOT/(n+'.EXACT-TEST-ORIGINAL')
            assert not backup.exists()
            target.rename(backup)
            try:
                with target.open('xb') as missing:missing.write(b'DISTRUST')
                assert not loader.reviewed_store.current(policy.bars,GUARD_PATH)
            finally:
                target.unlink()
                backup.rename(target)
        assert loader.reviewed_store.current(policy.bars,GUARD_PATH)
        print('ATTESTATION_REFUSES_RUNTIME_WRONG_DB_URL_AND_REPLACED_LOCK_OR_LEDGER',flush=True)
      finally:
        if pending is not None:
            pending.stop()
            if 'extratest' in locals():extratest.stop()
        w.exit()
        assert binding.closed and binding.engine.runner.closed
        print('INSTALLED_OWNER_TEARDOWN_CLOSED_HISTORY',flush=True)
        o.close()
        deactivate()
        assert loader.reviewed_store.current(policy.bars,GUARD_PATH) is False
        print('C11_REVOKED_AT_OUTER_LIFETIME_END',flush=True)
    assert len(rollover)==1 and rollover[0][0][0]>=1 and rollover[0][0][1]>=2,rollover
    assert rollover[0][1]==(((0,E-900_000),(1,1)),),rollover
    assert rollover[0][2][0]==E//60_000
    assert paths.count('/fapi/v1/exchangeInfo')==1 and totals[0]>0 and totals[1]==0,totals
    print('AUTHENTIC_OWNER_READS_NO_POST_DISPATCH_REFRESH',totals,flush=True)
    assert not Trade.get_open_trades()
    assert [a for a in advice if a[1]==PAIR]==[(2000,PAIR,'HOLD_NO_OWNED_SCOPE'),(at_cb,PAIR,'HOLD_NATIVE_COMMIT_REFUSED' if refusal else 'ORIGINAL_NATIVE_CALLBACK')],advice
    assert not any(pair==UNKNOWN_PAIR and status=='ORIGINAL_NATIVE_CALLBACK' for _,pair,status in advice),advice
    assert cas.db.execute('SELECT symbol,end_ms,source FROM c11_owner_callback_once').fetchall()==[('STBLUSDT',E,'legacy')]
    print('ACTUAL_FULL_WORKER_LEGACY_FALLTHROUGH_PERSISTED_ONCE',flush=True)
    print('PASS_EXACT_ARCHIVED_NATIVE_ROLLOVER_CARRIED_ONE_REAL_FRAMEWORK_CALL_AND_OLD_BOOT_ONLY',flush=True)
clock=[E-120]
def caught_trace(f,event,arg):
    if event=='return' and f.f_code.co_name in ('claim_before_wire','transport_still_due','claim_send') and arg is False:
       z=f.f_locals; obj=z.get('self')
       bot=getattr(obj,'bot',None)
       j=z.get('j')
       print('CAUGHT_WIRE_PREDICATE',f.f_code.co_name,f.f_lineno,getattr(j,'tf',None),
         len(getattr(getattr(bot,'feed',None),'latest_owner_observation',())) if bot else None,
         len(z.get('live',())) if 'live' in z else None,flush=True)
    if event=='exception' and Path(f.f_code.co_filename).name in ('native_shadow_queue.py','public_meter.py','exclusive_transport.py'):
       k,v,_=arg
       if isinstance(v,(ValueError,AssertionError,AttributeError,TypeError)) and str(v):
          print('EXACT_CAUGHT',f.f_code.co_name,k.__name__,str(v)[:120],flush=True)
    return caught_trace
@contextmanager
def clock_scope():
    from contextlib import ExitStack
    from datetime import datetime,timezone
    for mod in ('freqtrade.strategy.interface','freqtrade.data.dataprovider','freqtrade.freqtradebot','freqtrade.exchange.exchange','freqtrade.util.datetime_helpers'):
        __import__(mod)
    n=lambda:datetime.fromtimestamp(clock[0]/1000,timezone.utc)
    with ExitStack() as x:
        units=[]
        for name,mod in list(sys.modules.items()):
            if name.startswith(('freqtrade.',)) and hasattr(mod,'dt_now') and callable(mod.dt_now):
                x.enter_context(patch.object(mod,'dt_now',n));units.append(name)
            origin=getattr(mod,'__file__','') or ''
            if origin.startswith(('/opt/c11_owned_modules/','/opt/c11_worker/','/trial/')) and callable(getattr(mod,'time_ns',None)):
                x.enter_context(patch.object(mod,'time_ns',lambda:clock[0]*1000000));units.append(name+':time_ns')
        print('CLOCK_ALIAS_TAPE',len(units),flush=True)
        yield

if __name__=='__main__':
    import traceback,threading
    threading.settrace(caught_trace)
    try:
      with patch.object(time,'time_ns',lambda:clock[0]*1000000), patch.object(time,'time',lambda:clock[0]/1000), clock_scope():
        with PaperState() as outer:
          main(outer)
    except BaseException as e:
      print('EXACT_HOLD',type(e).__name__,str(e)[:180],[(Path(x.filename).name,x.name,x.lineno) for x in traceback.extract_tb(e.__traceback__)][-12:],flush=True)
      raise
    finally:
      print('OFFLINE_PUBLIC_PATHS',paths,'replay_sink_network_none',flush=True)
