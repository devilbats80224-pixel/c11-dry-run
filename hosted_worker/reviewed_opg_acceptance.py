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
PAIR='OPG/USDT:USDT'
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
        info={'contractType':'PERPETUAL','status':'TRADING','onboardDate':SEED['origin_ms']}) for base,pair in (('OPG',PAIR),('ON',UNKNOWN_PAIR))])
    events.append('synthetic-public-metadata')

from urllib.parse import urlsplit,parse_qs
from docker_observer import DockerObserver,CaptureBridge
from observer_rpc import ObserverClient
import packaged_entry
from hashlib import sha256
import sqlite3
from datetime import datetime,timezone
from reviewed_opg_selector import installed_review,AuditedKey,ReviewedOpgSource,Prior,parse_prior,restore_memory,digest,enc
from bridge_protocol import Refused
from causal_bars import MINUTE,FIVE,QUARTER,ClosedBar,SegmentStart
# Only pinned public candles and derived indicator values. No private
# runtime manifest, account data, or owner authorization is in this fixture.
PINS={'base.json': '21ddb283ec82f0cc2a4c6c30844c2d240138cf0f0b05f0bccf0d43fd620e03b5', 'witness.json': 'cbdefabf6e021348e9107431083ce178f07856371ea6aa7e5470616219c60ec7', 'control_previous.json': '104b5e51d04c26da51f996196b9ff38a122b7f003f806dff94ec815427b0bd93', 'public.json': '2c83a84ffc48d3ec402c993edd52c40684baaa535f98f659378497777788963c', 'canonical.json': '286e41afc6a01641a3f6f3bd92d2784deb554e6cce9f1b8e6c6b6f75a6df643d'}
def read_fixture(name):
    value=Path('/fixture',name).read_bytes()
    assert sha256(value).hexdigest()==PINS[name],name
    return value
base_packet=read_fixture('base.json')
base_pin=PINS['base.json']
base=parse_prior(base_packet,base_pin,independent_readback=lambda b,h:b is base_packet and h==base_pin)
body=json.loads(base_packet)
SEED={'origin_ms':base.origin}
_raw_current=read_fixture('witness.json')
ref=json.loads(_raw_current); E=ref['end_ms']
from cross_width_verify import verify_terminal,OK
assert verify_terminal(E,*(ref['packet'][str(w)]['rows'] for w in (MINUTE,FIVE,QUARTER)))==OK
assert E==base.cursor+QUARTER
assert len({x['rows'][-1][4] for x in ref['packet'].values()})==1
xp=lambda n:datetime.fromtimestamp(n/1e3,timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')
_template=json.loads(Path('/original/WIRE_PRIVATE.json').read_bytes())['actual_page']
gen=250027
W={'generation':gen,'expiry':E+59000,'actual_page':{**_template,'generation':gen,
  'received_at':xp(E-1000),'lease_until':xp(E+59000),'rows':[dict(symbol='OPGUSDT',native_oi_change_24h_raw='0.31')]+
  [dict(symbol='LAB'+str(i)+'USDT',native_oi_change_24h_raw='0.01') for i in range(59)]},
  'actual_public_receipts':[dict(timeframe=x['interval'],receive_ms=x['received_ms'],raw_kline_rows=x['rows']) for x in ref['packet'].values()]}
MISSING='ONUSDT';B='a'*64
MODE=os.environ.get('C11_ISOLATED_CASE','proof_then_revisit')
assert MODE in {'proof_then_revisit','observe_default'}
paths=[]
exact_requests=[]
inject=[False]
def valid(q):
    p=urlsplit(str(q))
    assert p.scheme=='https' and p.hostname=='fapi.binance.com'
    paths.append(p.path)
    if p.path=='/fapi/v1/exchangeInfo':return {'timezone':'UTC','symbols':[]}
    assert p.path=='/fapi/v1/klines',f'UNAVAILABLE_OFFLINE_PUBLIC_PATH:{p.path}'
    fields=parse_qs(p.query); tf=fields.get('interval',[''])[0]; start=int(fields.get('startTime',['0'])[0]); end=int(fields.get('endTime',[str(E-1)])[0]);lim=int(fields.get('limit',['500'])[0])
    assert fields['symbol'] in (['OPGUSDT'],[MISSING])
    exact_requests.append((fields['symbol'][0],tf,lim))
    rr=W['actual_public_receipts'] if fields['symbol']==['OPGUSDT'] else ()
    for x in rr:
      if x['timeframe']==tf:
        assert x['receive_ms']<=clock[0],('no time travel',fields['symbol'],tf,clock[0],x['receive_ms'])
        rows=[r for r in x['raw_kline_rows'] if start<=r[0]<=end][-lim:]
        print('ARCHIVED_PUBLIC_SEND',tf,start,end,lim,len(rows),clock[0]-E,x['receive_ms']-E,flush=True)
        if fields['symbol']==['OPGUSDT'] and not inject[0] and tf=='5m' and lim==3:
          inject[0]=True
          from decimal import Decimal
          rows=[list(r) for r in rows]
          tip=rows[-1]; o,high,low,cl=map(Decimal,tip[1:5])
          candidates=[z for z in (cl+Decimal('0.0001'),cl-Decimal('0.0001'),o) if low<=z<=high and z!=cl]
          assert candidates
          tip[4]=str(candidates[0])
          print('SYNTHETIC_INITIAL_5M_ONLY_CLOSE_DRIFT',flush=True)
          return rows
        if incident and fields['symbol']==['OPGUSDT'] and tf=='5m' and lim==21:
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
    import stage_pin,release_stage
    from history_factory_admission import FactoryAdmission
    import history_startup_receipt as hr
    from outer_seed_lock import private_identity,LOCK,LEDGER
    import prepared_owner_history as prep
    assert stage_pin.AUTH_SHA is getattr(stage_pin, 'OPG_REVISION_REVIEW_SHA', None) is None
    with patch.object(time, 'time_ns', lambda: 1795773600000000000):
        release_stage.check()
    o=DockerObserver(ROOT/'docker-observer.sqlite',B)
    f=None
    bridge=CaptureBridge(ReplayPublication(),o,B,lambda:f.classification)
    bridge.send(dict(op='C11_PAPER_BRIDGE_V1',nonce='0'*32)); bridge.receive(2)
    f=entry_feed.EntryFeed(bridge,1)
    f.classification=N(verified={'OPGUSDT':PAIR},body_sha256='b'*64,received_monotonic_ns=time.monotonic_ns())
    obs=ObserverClient(bridge,B,docked_current=True)
    assert tuple(x.symbol for x in f.read())==('OPGUSDT',)
    assert f.tracking_pairs()==(PAIR,)
    cfg=config_for(1000.0,[PAIR])
    policy=build_policy(ROOT)
    from atr_er_checkpoint import AtrErCheckpoint
    AtrErCheckpoint(policy.bars)
    with sqlite3.connect(LEDGER):pass
    old=body['retained_counterfactual_state']
    cursor=old['atr'][str(QUARTER)]['state']['clock']
    packed=enc(dict(derived_state=old))
    controlled_prior=Prior('OPGUSDT',base.origin,cursor,base.observed,digest(packed),packed)
    scratch,controlled_atr,segment=restore_memory(controlled_prior)
    for tape in ('control_previous.json','public.json'):
        packet=json.loads(read_fixture(tape))
        next_e=packet['end_ms']
        for width in (FIVE,QUARTER):
            ack=packet['packet'][str(width)]
            rows=tuple(ClosedBar(r[0],ack['received_ms'],*(float(r[j]) for j in (1,2,3,4,5,7)))
                       for r in ack['rows'] if cursor<=r[0]<next_e)
            assert tuple(r.open_ms for r in rows)==tuple(range(cursor,next_e,width))
            scratch.record('OPGUSDT',width,rows)
            assert controlled_atr.extend('OPGUSDT',width,segment,next_e,base.observed)==next_e
        assert scratch.rpd_progress('OPGUSDT',next_e,base.observed).next_open==next_e
        cursor=next_e
    assert cursor==base.cursor
    controlled_rpd=scratch.db.execute('SELECT payload FROM rpd_checkpoint').fetchone()[0]
    assert json.loads(controlled_rpd)['state'] != body['derived_state']['rpd_state']['state']
    for t in ('segment','rpd_checkpoint','atr_er_checkpoint','atr_er_sample'):
        rows=scratch.db.execute(f'SELECT * FROM {t}').fetchall()
        with policy.bars.db:
            if rows:policy.bars.db.executemany(f'INSERT INTO {t} VALUES('+','.join('?' for _ in rows[0])+')',rows)
    scratch.db.close()
    keys=tuple(sorted((AuditedKey(q['width'],q['open_ms'],q['old_seen_ms'],q['new_seen_ms'],
      q['prior_sha'],q['revised_sha']) for q in body['per_width_exact_conflict_evidence']),key=lambda x:x.width))
    originals=json.loads(read_fixture('canonical.json'))['rows']
    saved=[]
    for k in keys:
        r=ClosedBar(*next(q for q in originals[str(k.width)] if q[0]==k.open_ms));saved.append((k,r))
        policy.bars.record('OPGUSDT',k.width,(r,))
    # Explicit test-only review and receipt at synthetic local owner construction.
    # No OneShot created/consumed, no startup field changed in the installed image.
    source=ReviewedOpgSource(dict(reviewed_available_ms=base.observed+10,use_until_ms=E+60000),base,keys)
    canon_before=tuple(policy.bars.db.execute('SELECT * FROM rpd_checkpoint').fetchall())
    hs=FactoryAdmission(stage_pin.RUNTIME_SHA)
    facts=hr.Site(digest(Path('/opt/c11_owned_modules/release_stage.py').read_bytes()),stage_pin.RUNTIME_SHA,
        private_identity(LEDGER),private_identity(ROOT/'causal-candles.sqlite'),private_identity(LOCK))
    receipt=hr.Receipt(hr._TOKEN,facts,outer,'1'*64)
    # Offline fixture only; exercise the existing allow-absent contract.
    ctx=hr._publish(receipt,facts,outer,os.environ)
    hs.bind_private_source(facts,outer,lambda:facts)
    phases=[];advice=[]
    def trace(pair,status):advice.append((clock[0]-E,pair,status));print('PAPER_ADVICE',*advice[-1],flush=True)
    class OmitUnrelatedBackground:
       def __init__(self,*a,**k):pass
       def before_protection(self):pass
       def after_protection(self,bot):pass
       def close(self):return True
    guard=dict(create_order=lambda *_a,**_k:(_ for _ in ()).throw(AssertionError('NO_PAPER_ORDER_TEST_BOUNDARY')))
    try:
      with patch.object(requests.Session,'send',send),patch.object(aiohttp.ClientSession,'_request',async_send),            patch.object(Exchange,'_load_async_markets',seed_public_exchange_markets),            patch.object(Exchange,'check_time_offset',lambda _:None),            patch.object(Binance,'fill_leverage_tiers',lambda _:None),            patch.object(Binance,'ft_additional_exchange_init',lambda _:None),            patch.multiple(C11PaperExchange,**guard),patch.object(prep,'BoundOwnerHistory',OmitUnrelatedBackground),            patch.object(native,'OWNED_SCHEDULER_FACTORY',site.installed_site_factory(stage_pin.RUNTIME_SHA,trace,
                diagnostic_sink=phases.append,history_startup=hs,prepared_history_quarantine=(),reviewed_revision=source)),            patch.object(native,'OWNED_SCHEDULER_EXPECTED_SEAL',stage_pin.RUNTIME_SHA):
        w=PaperWorker(cfg,policy,f,obs)
        scheduler=getattr(w._c11_owned_scheduler,'active',w._c11_owned_scheduler).scheduler
        selector=scheduler.commit_and_decide.multilane_prepare
        assert selector.source is source and receipt._used
        clock[0]=max(E+2000,max(x['receive_ms'] for x in W['actual_public_receipts'])+25)
        try:
          for i in range(8):
             w.freqtrade.process()
             print('OWNER_CYCLE',clock[0]-E,len(scheduler.covers),len(scheduler.queue.inflight),
                [(r.ticket.lane,len(r.raw)) for r in scheduler.adapter._proven.values()],flush=True)
             if i==7:break
             w._sleep(w._throttle_secs)
             clock[0]+=5000
          assert all(policy.bars.range('OPGUSDT',k.width,k.open_ms,k.open_ms+k.width,clock[0])==(z,) for k,z in saved)
          assert inject[0]
          assert scheduler.reconfirm.actor._committed is True
          assert not Trade.get_open_trades()
          assert sum(status=='ORIGINAL_NATIVE_CALLBACK' for _,_,status in advice)==1,advice
          assert policy.bars.db.execute('SELECT cursor FROM opg_corrected_lineage_v1').fetchall()==[(E,)]
          print('PASS_WHOLE_RECONFIRMED_OPG_WORKER_CORRECTED_NATIVE_CALLBACK_ONCE; true settled quote reference plus synthetic initial drift and owner; no real order or OneShot',flush=True)
        finally:w.exit()
    finally:hr.unpublish(ctx,receipt)

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
