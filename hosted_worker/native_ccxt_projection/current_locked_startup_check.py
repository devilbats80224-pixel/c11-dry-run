"""Installed constructors with a blank paper ledger, and synthetic exchange facts."""
from decimal import Decimal
from datetime import datetime,timezone
from contextlib import ExitStack
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path
from types import SimpleNamespace as N
from unittest.mock import patch
import os,sys
sys.path[:0]=["/opt/c11_owned_modules","/opt/c11_worker","/opt/c11_worker/ports"]
import time

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
from urllib.parse import urlsplit,parse_qs
import json, tarfile, shutil, re
from paper_runtime import PaperState, verify_state_file
import startup_status
import v4_entry
import entry_feed

ROOT=Path('/c11-paper')
NATIVE_GUA=os.environ.get('C11_NATIVE_GUA_CYCLE')=='yes'
FULL_TAPE=os.environ.get('C11_NATIVE_GUA_TAPE')=='yes'
FINAL_REPLAY=os.environ.get('C11_NATIVE_GUA_FINAL')=='yes'
BASE='GUA' if NATIVE_GUA else 'FOO'
SYMBOL=BASE+'USDT'
PAIR=BASE+'/USDT:USDT'
assert version('freqtrade')=='2026.10' and version('ccxt')=='4.5.90'
assert sha256(Path('/freqtrade/freqtrade/worker.py').read_bytes()).hexdigest()=='9c5a046404f69e061ead72595f8a48f84eb19d9025331f8ff91f3bfebf767f65'
assert Path(Exchange.__init__.__code__.co_filename).is_relative_to('/freqtrade/freqtrade')
assert Path(FreqtradeBot.__init__.__code__.co_filename).is_relative_to('/freqtrade/freqtrade')
assert Path(C11PaperExchange.__init__.__code__.co_filename).is_relative_to('/opt/c11_worker')
assert pins.OWNED_SCHEDULER_FACTORY is pins.OWNED_SCHEDULER_EXPECTED_SEAL is None
assert not any(Path(p).exists() for p in ('/oi20-provider-secret','/oi20-docker-publication','/run/openmarket-credential'))
if __name__=='__main__' or os.environ.get('C11_HAN_RESTART_PHASE')!='resume':
    assert not list(ROOT.iterdir())
os.umask(0o077)
events=[]
def reject_wire(*args,**kwargs):
    events.append('PROHIBITED_WIRE')
    raise AssertionError('FIXTURE_WIRE_FORBIDDEN')
async def reject_wire_async(*args,ssl=True,**kwargs):
    reject_wire()

class ArchivedGuaWire:
    """Only returns archived, already available public responses; never sends."""
    def __init__(self, path):
        b=path.read_bytes()
        assert sha256(b).hexdigest()=='3ca3d960cf574486f51674263107fcca1ddcc0f1d588c65f887dee932b443f1a'
        data=json.loads(b)
        assert data['symbol']==SYMBOL
        self.end=data['end_ms']
        self.receipts={r['timeframe']:r for r in data['receipts']}
        assert set(self.receipts)=={'1m','5m','15m'}
        self.now=max(r['receive_ms'] for r in self.receipts.values())+1
        self.clock=lambda:self.now

    def read(self, method, url):
        p=urlsplit(str(url))
        if (method,p.scheme,p.netloc,p.path)!=('GET','https','fapi.binance.com','/fapi/v1/klines'):
            reject_wire()
        q=parse_qs(p.query,strict_parsing=True)
        assert set(q)<={'symbol','interval','startTime','endTime','limit'}
        assert all(len(v)==1 for v in q.values()) and q['symbol']==[SYMBOL]
        tf=q['interval'][0]
        receipt=self.receipts[tf]
        if receipt['receive_ms']>self.clock():
            print('GUA_HTTP_RECEIPT_NOT_YET_AVAILABLE',tf,flush=True)
            return '[]'
        start=int(q.get('startTime',['0'])[0])
        end=min(int(q.get('endTime',[str(self.end-1)])[0]),self.end-1)
        limit=int(q.get('limit',['500'])[0])
        assert 0<limit<=1500
        rows=[row for row in receipt['rows'] if start<=row[0]<=end and row[6]<self.end][-limit:]
        print('GUA_ARCHIVED_HTTP',tf,limit,len(rows),flush=True)
        return json.dumps(rows)

    def send(self, request, **_):
        response=requests.Response()
        response.status_code=200
        response.headers.update({'Content-Type':'application/json','x-mbx-used-weight-1m':'12'})
        response._content=self.read(request.method,request.url).encode()
        response.request=request
        response.url=request.url
        return response

    async def async_send(self, method, url, **_):
        return ArchivedResponse(url,self.read(method,url))

class ArchivedResponse:
    status=200
    reason='OK'
    history=()
    headers={'Content-Type':'application/json','x-mbx-used-weight-1m':'12'}
    def __init__(self,url,body): self.url=url; self.body=body
    async def __aenter__(self):return self
    async def __aexit__(self,*_):return None
    async def text(self,*_,**__):return self.body
    async def read(self): return self.body.encode()
    def release(self): pass
    def close(self): pass
    async def wait_for_close(self): pass
def seed_public_exchange_markets(exchange, *, reload):
    assert type(exchange) is C11PaperExchange and reload is True
    assert exchange._config['dry_run'] is True
    assert exchange._api.id == exchange._api_async.id == 'binance'
    assert not any(getattr(api,k,None) for api in (exchange._api,exchange._api_async) for k in ('apiKey','secret','password'))
    async def initialize():
        exchange._api_async.open()
        assert type(exchange._api_async.session) is aiohttp.ClientSession
    exchange.loop.run_until_complete(initialize())
    exchange._api_async.set_markets([dict(
        id=SYMBOL,symbol=PAIR,base=BASE,quote='USDT',settle='USDT',
        baseId=BASE,quoteId='USDT',settleId='USDT',type='swap',
        spot=False,swap=True,future=False,option=False,margin=False,
        active=True,contract=True,linear=True,inverse=False,contractSize=1,
        maker=0.0002,taker=0.0005,precision={'amount':0.001,'price':0.001},
        limits={'amount':{'min':0.001,'max':100000},'price':{'min':0.001,'max':10000},
                'cost':{'min':5,'max':None},'leverage':{'min':1,'max':20}},
        info={'contractType':'PERPETUAL','status':'TRADING',
              **({'symbol':SYMBOL,'quoteAsset':'USDT','marginAsset':'USDT','onboardDate':1766312100000}
                 if NATIVE_GUA else {})})])
    events.append('synthetic-public-metadata')

class FixtureFeed:
    def __init__(self,row):
        self.latest_owner_observation=(row,)
    def ranked(self):return self.latest_owner_observation
    def tracking_pairs(self):return (PAIR,)
    def candidate(self,pair):return self.latest_owner_observation[0] if pair==PAIR else None

def restore_scratch():
    # Only the approved stopped bundle is mounted ro; no raw output.
    assert os.statvfs('/stopped.tar.gz').f_flag & os.ST_RDONLY
    with tarfile.open('/stopped.tar.gz','r:gz') as tar:
        m=json.load(tar.extractfile('MANIFEST.json'))
        assert set(tar.getnames())==set(m['files'])|{'MANIFEST.json'}
        for name,pin in m['files'].items():
            assert re.fullmatch(r'[A-Za-z0-9_.-]+',name)
            member=tar.getmember(name)
            assert member.isfile() and member.size==pin['bytes']
            data=tar.extractfile(member).read()
            assert sha256(data).hexdigest()==pin['sha256']
            with (ROOT/name).open('xb') as out:out.write(data)
    print('SCRATCH_ACCEPTED_ARCHIVE_RESTORED',flush=True)

def main():
    restore_scratch()
    in_worker_fixture=os.environ.get('C11_COLD_IN_CONSTRUCTED_WORKER')=='yes'
    from stage_pin import RUNTIME_SHA,AUTH_SHA
    p=Path('/opt/c11_owned_modules/release_stage_runtime.json')
    assert AUTH_SHA is None and sha256(p.read_bytes()).hexdigest()==RUNTIME_SHA
    assert sha256(Path(entry_feed.__file__).read_bytes()).hexdigest()==json.loads(p.read_bytes())['files'][entry_feed.__file__]
    cycles=[]
    terminal_phase=[]
    observed_advice=[]
    def fixture_generation():return 7+len(cycles)//75+bool(terminal_phase) if FULL_TAPE else 7
    class Wire:
        def send(self,msg):
            assert msg['op']=='READ_C11_V2'
            self.nonce=msg['nonce']
        def receive(self,timeout):
            raw=[{'symbol':SYMBOL,'native_oi_change_24h_raw':'0.25'}]
            raw.extend({'symbol':f'BAR{i}USDT','native_oi_change_24h_raw':'0.02'} for i in range(59))
            return dict(nonce=self.nonce,status='FRESH',source=entry_feed.SOURCE,
                query_sha256=entry_feed.QUERY,offset=0,page_size=60,
                generation=fixture_generation(),left_ms=25000,received_at='2026-11-24T00:00:00+00:00',
                lease_until='2026-11-24T00:01:00+00:00',rows=raw)
    feed=entry_feed.EntryFeed(Wire(),1)
    feed.classification=N(verified={SYMBOL:PAIR},received_monotonic_ns=time.monotonic_ns())
    assert feed.tracking_pairs()==() and feed.latest_owner_observation==()
    def fixed_point(sym,_):
        return (fixture_generation(),1,Decimal('.25')) if sym==SYMBOL else None
    observer=N(point=fixed_point,current_point=fixed_point)
    archived=ArchivedGuaWire(Path('/archived-gua.json')) if NATIVE_GUA else None
    E=archived.end if archived else 1795488300000
    replay_wall=[(E-900000+65001 if FULL_TAPE else max(archived.now,E+65001)) if archived else E+90000]
    if archived:archived.clock=lambda:replay_wall[0]
    @contextmanager
    def synthetic_intake(*_):
        yield feed,observer
    trace=[]
    original=status_write=startup_status.Status.write
    def track(self,phase,result='OK'):
        trace.append((phase,result))
        print('STAGE',phase,result,flush=True)
        return original(self,phase,result)
    def one_start(self):
        assert type(self.freqtrade) is PaperBot
        assert type(self.freqtrade.exchange) is C11PaperExchange
        assert self.freqtrade.config['dry_run'] is True
        assert Trade.get_open_trades()==[]
        assert self.freqtrade.active_pair_whitelist==[PAIR]
        assert type(self.freqtrade.feed) is entry_feed.EntryFeed
        assert self.freqtrade.feed.tracking_pairs()==(PAIR,)
        assert len(self.freqtrade.feed.latest_owner_observation)==1
        if in_worker_fixture:
            import contextlib,io,runpy
            store=self.freqtrade.policy.bars
            assert store.db.execute('PRAGMA database_list').fetchone()[2]=='/c11-paper/causal-candles.sqlite'
            b=io.StringIO()
            with contextlib.redirect_stdout(b):
                a=runpy.run_path('/proof/cold.py',init_globals={'WORKER_STORE':store})
                cold=json.loads(b.getvalue())
                b.seek(0);b.truncate(0)
                runpy.run_path('/proof/tail.py',init_globals={'AUTOMATIC_HANDOFF':a['HANDOFF'],'WORKER_STORE':store})
                outcome=json.loads(b.getvalue())
            assert cold['empty_started'] and cold['first_page_scheduled_automatically']
            assert outcome['automatic_checkpoint_reused'] and outcome['primary_status'] in ('NO_SIGNAL','SIGNAL')
            assert store.segment('GUAUSDT') is not None
            print('LOCKED_WORKER_SAME_STORE_GUA '+json.dumps(dict(cold=cold,primary=outcome,
                    synthetic_intake=True,normal_worker_loop_exercised=False),sort_keys=True),flush=True)
    # Keep Worker.run and its cycle. Bound the offline test by a fixed number
    # of cycles, not by a live candle or simulated order.
    original_cycle=PaperWorker._worker
    installed_history=[]
    class FixtureCycleComplete(Exception):pass
    def bounded_worker_cycle(self,*args,**kwargs):
        if len(cycles)>=(370 if FULL_TAPE else 3):raise FixtureCycleComplete()
        wrapper=getattr(self.freqtrade,'_c11_owned_data_lane',None)
        scheduler=getattr(wrapper,'scheduler',None)
        if not installed_history and scheduler is not None:
            from cold_cycle_fixture import attach_offline
            installed_history.append(attach_offline(scheduler,FULL_TAPE))
            print('GUA_OFFLINE_HISTORY_ATTACHED_EMPTY',flush=True)
        cycles.append('ENTERED_ORIGINAL_WORKER_CYCLE')
        print(cycles[-1],getattr(self.freqtrade.state,'name',None),
            'scheduler_attached',getattr(self,'_c11_owned_scheduler',None) is not None,
            'market_binding_attached',getattr(self.freqtrade,'_c11_reviewed_market_binding',None) is not None,flush=True)
        calls={}
        targets={'process','after_protection','_after_protection','analyze','get_entry_signal','_process_running','_process_stopped','ranked','capture'}
        def profile(frame,event,arg):
            name=frame.f_code.co_name
            filename=frame.f_code.co_filename.rsplit('/',1)[-1]
            if FINAL_REPLAY and terminal_phase and event=='return':
                owner=frame.f_locals.get('self')
                data=None
                if (filename,name)==('compiled_candidate_factory.py','__call__') and type(owner).__name__=='OriginalPlan':
                    data={'offered':len(arg[2]) if isinstance(arg,tuple) and len(arg)==3 else None}
                elif (filename,name)==('pending_roster.py','plan'):
                    data={'retained':len(arg) if isinstance(arg,tuple) else None}
                elif (filename,name) in {('adapter.py','register_original_dispatch'),('coordinator.py','register'),('dedicated_queue.py','submit')}:
                    data={'accepted':arg is True,'last_submit_status':getattr(owner,'last_submit_status',None)}
                elif (filename,name)==('unified_owner.py','_offer_after_protection'):
                    data={'outcome':arg if type(arg) is str and len(arg)<=100 else None}
                elif (filename,name) in {('standalone_policy.py','primary_inputs'),('native_worker.py','primary_inputs')}:
                    data={'ready':isinstance(arg,dict) and bool(arg)}
                elif (filename,name)==('causal_bars.py','recent_primary'):
                    data={'primary_status':getattr(arg[2],'status',None) if isinstance(arg,tuple) and len(arg)==3 else None}
                if data is not None:
                    print('GUA_TERMINAL_REQUEST_CHAIN',filename,name,json.dumps(data,sort_keys=True),flush=True)
            if event=='call' and name in targets:
                name=Path(frame.f_code.co_filename).name+':'+frame.f_code.co_name
                calls[name]=calls.get(name,0)+1
        previous=sys.getprofile()
        sys.setprofile(profile)
        try:
            with ExitStack() as clocks:
                if FINAL_REPLAY and terminal_phase and os.environ.get('C11_FIXTURE_SYNC_WALL')=='yes':
                    # Match archived UTC in strategy readers only. The
                    # copied stopped observer lineage retains host time.
                    for name in ('standalone_policy','feature_warmup','runtime_facts','native_candles'):
                        mod=sys.modules.get(name)
                        if getattr(mod,'time',None) is time:
                            clocks.enter_context(patch.object(mod,'time',native.time))
                def replay_datetime():return datetime.fromtimestamp(replay_wall[0]/1000,timezone.utc)
                for name,mod in tuple(sys.modules.items()):
                    if name.startswith('freqtrade.') and callable(getattr(mod,'dt_now',None)):
                        clocks.enter_context(patch.object(mod,'dt_now',replay_datetime))
                result=original_cycle(self,*args,**kwargs)
            print('ORIGINAL_CYCLE_RETURNED',getattr(result,'name',None),flush=True)
            if FINAL_REPLAY and terminal_phase and scheduler is not None:
                plan=scheduler.plan
                end,live=plan.entries()
                row=next((x for x in live if x.pair==PAIR),None)
                cursor=plan.checkpoint_cursor(self.freqtrade,row,end) if row is not None else None
                print('GUA_TERMINAL_GATE_READBACK',json.dumps(dict(
                    age_ms=replay_wall[0]-end,live=len(live),epoch=plan.epoch(end),
                    observed_ranked=self.freqtrade.observed_ranked(row) if row is not None else None,
                    cursor=cursor, expected_tail=(end-cursor)//300000 if cursor is not None else None,
                    checkpoint_refused=plan.unplanned_checkpoint_count,
                    budget_boundary=scheduler.queue.limiter.wall()//900000*900000,
                    budget_matches=scheduler.queue.limiter.wall()//900000*900000==end,
                    current_capture=scheduler.adapter.capture.end, end=end,
                    marker=scheduler.marker, owned=len(scheduler.covers)),sort_keys=True),flush=True)
            replay_wall[0]+=100 if terminal_phase else (1 if FULL_TAPE else 1000)
            if installed_history:
                h=installed_history[0]
                from causal_bars import FIVE
                if h.io.requests:
                    job=h.io.requests[0]
                    segment=h.store.segment(SYMBOL)
                    if segment is not None and FULL_TAPE:
                        from causal_bars import QUARTER
                        target=h.io.stop
                        cursors=tuple(h.atr.cursor(SYMBOL,w,segment) for w in (FIVE,QUARTER))
                        if len(cycles)%25==0:
                            print('GUA_ARCHIVE_TAPE_PROGRESS',len(h.io.requests),cursors,
                                'facts',len(h.facts),'generation',h.tap.generation,flush=True)
                        if cursors==(target,target) and not terminal_phase:
                            print('GUA_ORIGINAL_LOOP_COLD_HISTORICAL_PREFIX_READY',target,len(h.io.requests),flush=True)
                            if not FINAL_REPLAY:
                                raise FixtureCycleComplete()
                            terminal_phase.append(len(cycles))
                            replay_wall[0]=archived.now
                            print('GUA_TERMINAL_REPLAY_AT_ACTUAL_RECEIPT_MS',archived.now,flush=True)
                if terminal_phase and len(cycles)>=terminal_phase[0]+8:
                    print('GUA_TERMINAL_OBSERVED_ADVICE',json.dumps(observed_advice),flush=True)
                    reads=h.store.db.execute(
                        'SELECT feature,end_ms,state FROM feature_status WHERE symbol=? ORDER BY feature',(SYMBOL,)).fetchall()
                    print('GUA_TERMINAL_ORIGINAL_FEATURE_READBACK',json.dumps(reads),flush=True)
                    print('GUA_TERMINAL_OPEN_PAPER_TRADES',len(Trade.get_open_trades()),flush=True)
                    analyzed,touched=self.freqtrade.strategy.dp.get_analyzed_dataframe(PAIR,'15m')
                    if not analyzed.empty:
                        row=analyzed.iloc[-1]
                        candle_end=int(row['date'].timestamp()*1000)+900000
                        booleans={name:bool(row.get(name,0)==1) for name in ('enter_long','enter_short','exit_long','exit_short')}
                        print('GUA_TERMINAL_ORIGINAL_ANALYZED_RESULT',json.dumps(dict(
                            candle_end=candle_end,expected_end=E,
                            original_callback_count=sum(x[1]=='ORIGINAL_NATIVE_CALLBACK' for x in observed_advice),
                            flags=booleans,source='original_strategy_dataprovider')),flush=True)
                    else:
                        print('GUA_TERMINAL_ORIGINAL_ANALYZED_RESULT_EMPTY',flush=True)
                    raise FixtureCycleComplete()
                    if segment is not None and not FULL_TAPE:
                        r=h.store.rpd_progress(SYMBOL,job.stop,replay_wall[0])
                        if r.next_open==job.stop and r.n==499 and h.atr.sample(SYMBOL,FIVE,segment,job.stop,replay_wall[0])['known']:
                            print('GUA_ORIGINAL_LOOP_COLD_FIRST_PAGE_PERSISTED rows=499',flush=True)
                            raise FixtureCycleComplete()
            return result
        finally:
            sys.setprofile(previous)
            print('GUA_ORIGINAL_CYCLE_CALLS',json.dumps(calls,sort_keys=True),'transport_attempts',events.count('PROHIBITED_WIRE'),flush=True)
    seal=RUNTIME_SHA
    def record(pair,status):
        if FINAL_REPLAY and terminal_phase and pair==PAIR:
            observed_advice.append((replay_wall[0]-E,status))
    def diagnostic(data):
        if FINAL_REPLAY and terminal_phase:
            print('GUA_TERMINAL_DIAGNOSTIC',data.decode().strip(),flush=True)
    compiled=site.installed_site_factory(seal,record,diagnostic_sink=diagnostic if FINAL_REPLAY else None)
    with ExitStack() as fixture_stack, patch.object(site,'time_ns',lambda:replay_wall[0]*1_000_000), \
         patch.object(requests.Session,'send',reject_wire),patch.object(aiohttp.ClientSession,'_request',reject_wire_async), \
         patch.object(Exchange,'_load_async_markets',seed_public_exchange_markets), \
         patch.object(Exchange,'check_time_offset',lambda _:None), \
         patch.object(Binance,'fill_leverage_tiers',lambda _:None), \
         patch.object(Binance,'ft_additional_exchange_init',lambda _:None), \
         patch.object(startup_status.Status,'write',track), \
         patch.object(native,'OWNED_SCHEDULER_FACTORY',compiled),patch.object(native,'OWNED_SCHEDULER_EXPECTED_SEAL',seal):
        if NATIVE_GUA:
            sys.path.insert(0,'/fixture-tools')
            fixture_stack.enter_context(patch.object(PaperWorker,'_worker',bounded_worker_cycle))
            if FULL_TAPE:
                fixture_stack.enter_context(patch.object(PaperWorker,'_sleep',lambda *a,**k:None))
            if FINAL_REPLAY and os.environ.get('C11_FIXTURE_SYNC_WALL')=='yes':
                class ReplayNativeClock:
                    @staticmethod
                    def time_ns():return replay_wall[0]*1_000_000
                    def __getattr__(self,name):return getattr(time,name)
                fixture_stack.enter_context(patch.object(native,'time',ReplayNativeClock()))
            fixture_stack.enter_context(patch.object(requests.Session,'send',lambda _session,*a,**k:archived.send(*a,**k)))
            async def replay(_session,*a,ssl=True,**k):return await archived.async_send(*a,**k)
            fixture_stack.enter_context(patch.object(aiohttp.ClientSession,'_request',replay))
            fixture_stack.enter_context(patch.object(C11PaperExchange,'create_order',reject_wire))
        else:
            fixture_stack.enter_context(patch.object(PaperWorker,'run',one_start))
        scope={'generation_floor':1,'publication_binding':'isolated-fixture','wallet_usdt':1000.0}
        try:
            v4_entry.run_locked(scope,None,PaperState,verify_state_file,synthetic_intake,
                lambda scope,pairs:config_for(scope['wallet_usdt'],pairs),build_policy,PaperWorker)
        except FixtureCycleComplete:
            print('GUA_REAL_CYCLE_RETURNED_NO_FULL_DECISION_ATTESTATION',flush=True)
            return 2
        except Exception as ex:
            # Print classes/location only, never provider text or SQLite payload.
            import traceback
            print('REFUSAL_CLASS',type(ex).__module__,type(ex).__name__,flush=True)
            print('REFUSAL_LOCATION',[(Path(f.filename).name,f.name,f.lineno) for f in traceback.extract_tb(ex.__traceback__)][-10:],flush=True)
            return 1
    assert events==['synthetic-public-metadata'],events
    assert trace[-2:]==[('NATIVE_RUN_ENTERED','OK'),('NATIVE_RUN_RETURNED','OK')]
    assert (ROOT/'observer-native-owned-v1.sqlite').is_file()
    print('PASS_CURRENT_LOCKED_STARTUP_CONSTRUCTOR_FROM_OFFLINE_FIXTURE_ONLY',flush=True)
    return 0

if __name__=='__main__':raise SystemExit(main())
