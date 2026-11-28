"""Installed scheduler witness; synthetic prices only for IO mechanics."""
import json,sys
from pathlib import Path
from types import SimpleNamespace as N
sys.path[:0]=['/opt/c11_owned_modules','/opt/c11_worker','/opt/c11_worker/ports','/opt/c11_background_review','/freqtrade']
import native_primary_lane as native
from causal_bars import CausalBars,ClosedBar,MINUTE,FIVE,QUARTER
from rpd_checkpoint import RpdState
assert Path(native.__file__).parent==Path('/opt/c11_owned_modules')
# Freeze just the historical regression's window shape, not any market
# data, account, or prior symbol. Rows below are wholly synthetic.
E=1795696200000
orig=1596006000000
start=E-165*MINUTE
now=E+2000; sym='EXAMPLEUSDT'; pair='EXAMPLE/USDT:USDT'
f=N(symbol=sym,pair=pair,boundary=E,origin=orig,generation=2)
scope=N(symbol=sym,pair=pair,end=E,expires=E+60000,generation=2)
def run_case(case, extra_minutes, expected_pages):
    start_here=start-extra_minutes*MINUTE
    store=CausalBars(':memory:')
    def bars(a,b,w):
        return tuple(ClosedBar(t,E,1.,1.,1.,1.,float(w//MINUTE),float(w//MINUTE)) for t in range(a,b,w))
    store.record(sym,MINUTE,bars(start_here,E,MINUTE))
    store.record(sym,QUARTER,bars(E-260*QUARTER,E,QUARTER))
    db=store.db
    def series(w):return db.execute('SELECT * FROM candle WHERE symbol=? AND width=? ORDER BY open_ms',(sym,w)).fetchall()
    before=series(QUARTER)
    if case=='stale_2020':
        cursor=1596155700000
        db.execute('INSERT INTO rpd_checkpoint VALUES (?,?)',(sym,RpdState(sym,orig,cursor,cursor).pack()))
        db.commit()
    def checkpoint(): return db.execute('SELECT * FROM rpd_checkpoint').fetchall()
    seed0=checkpoint()
    owner=N(worker=N(freqtrade=N(config={'dry_run':True})),store=store,
            run=N(wall=lambda:now,closed=False,queue=N(inflight={},protection_due=lambda:False),
                  plan=N(provisional_scope=lambda p,e:scope if (p,e)==(pair,E) else None,current=lambda s,n:True,last={})),
            minute_admission=N(cycle_headroom=lambda:True),pending=None,check_store=lambda:None,
            tap=N(current=lambda fact:fact is f))
    jobs=[]
    lane=native.NativePrimaryLane(owner,lambda _:None)
    lane.pending=N(drain=lambda _:None,inflight_ranges=lambda _:(),offer=lambda j,w:(jobs.append((j,w)) or True))
    lane.marker=E
    lane.recorded[sym]='REQUIRES_ORIGINAL_RPD' # test branch prerequisite, not an observed BAND buy signal
    lane.tick((f,),lambda:True)
    assert [j.n for j,w in jobs]==expected_pages
    count=sum(expected_pages)
    assert sum(j.n for j,w in jobs)==count and jobs[0][0].start==1795617900000
    assert all(j.stop<=start_here and w.rpd_refill for j,w in jobs)
    def raw(j):
        return [[t,'1','1','1','1','1',t+MINUTE-1,'1',1,'1','1','0'] for t in range(j.start,j.stop,MINUTE)]
    for j,w in jobs:lane.complete(j,w,(E+1000,E+1500,raw(j)))
    assert series(QUARTER)==before and checkpoint()==seed0
    assert len(series(MINUTE))==1305 and len(series(FIVE))==count//5
    jobs.clear()
    lane.tick((f,),lambda:True)
    assert not jobs
    print(json.dumps(dict(case=case,input_fixture_only=True,existing_15m_unchanged=260,native_pages=expected_pages,
        new_1m=count,new_5m=count//5,requeues=0,original_state_created=False,strategy_callback=False,market_order=False)))

def run_settled_minute_repair():
    store=CausalBars(':memory:')
    missing=E-3*QUARTER
    def rows(a,b,w):
        return tuple(ClosedBar(t,E,1.,1.,1.,1.,1.,1.) for t in range(a,b,w))
    full=E-260*QUARTER
    store.record(sym,MINUTE,rows(full,missing,MINUTE)+rows(missing+QUARTER,E,MINUTE))
    store.record(sym,QUARTER,rows(full,E,QUARTER))
    store.record(sym,FIVE,rows(missing,missing+QUARTER,FIVE))
    state=lambda:store.db.execute('SELECT * FROM candle WHERE width!=? ORDER BY width,open_ms',(MINUTE,)).fetchall()
    before=state()
    owner=N(worker=N(freqtrade=N(config={'dry_run':True})),store=store,
            run=N(wall=lambda:now,closed=False,queue=N(inflight={},protection_due=lambda:False),
                  plan=N(provisional_scope=lambda p,e:scope if (p,e)==(pair,E) else None,current=lambda s,n:True,last={})),
            minute_admission=N(cycle_headroom=lambda:True),pending=None,check_store=lambda:None,
            tap=N(current=lambda fact:fact is f))
    jobs=[]
    lane=native.NativePrimaryLane(owner,lambda _:None)
    lane.pending=N(drain=lambda _:None,inflight_ranges=lambda _:(),offer=lambda j,w:(jobs.append((j,w)) or True))
    lane.marker=E
    lane.recorded[sym]='PRIMARY_NO_SIGNAL'
    owner.run.queue.protection_due=lambda:True
    lane.tick((f,),lambda:True)
    assert not jobs,'C11_CONTINUITY_INTERRUPTED_PROTECTION'
    owner.run.queue.protection_due=lambda:False
    peer=N(symbol='PEERUSDT',pair='PEER/USDT:USDT',boundary=E,origin=orig,generation=2)
    peer_scope=N(symbol=peer.symbol,pair=peer.pair,end=E,expires=E+60000,generation=2)
    owner.tap.current=lambda fact:fact is f or fact is peer
    owner.run.plan.provisional_scope=lambda p,e:{pair:scope,peer.pair:peer_scope}.get(p) if e==E else None
    lane.recorded[peer.symbol]='REQUIRES_ORIGINAL_RPD'
    lane.tick((f,peer),lambda:True)
    assert jobs and {job.symbol for job,_ in jobs}=={peer.symbol},'C11_CONTINUITY_COMPETED_WITH_DECISION_REFILL'
    jobs.clear()
    lane.tick((f,),lambda:False)
    assert not jobs,'C11_CONTINUITY_BYPASSED_HEADROOM'
    lane.tick((f,),lambda:True,occupied_ranges=lambda s:((missing,missing+QUARTER),) if s==sym else ())
    assert not jobs,'C11_PRIMARY_DUPLICATED_ACTIVE_STALE_RANGE'
    lane.tick((f,),lambda:True,occupied_ranges=lambda _:((full,full+QUARTER),))
    assert len(jobs)==1,'C11_SETTLED_RAW_1M_GAP_NOT_REFILLED'
    j,w=jobs.pop()
    assert (j.start,j.stop,j.n)==(missing,missing+QUARTER,15)
    assert w.projection_only and not w.rpd_refill
    raw=[[t,'1','1','1','1','1',t+MINUTE-1,'1',1,'1','1','0'] for t in range(j.start,j.stop,MINUTE)]
    lane.complete(j,w,(E+1000,E+1500,raw))
    assert len(store.range(sym,MINUTE,missing,missing+QUARTER,now))==15
    assert lane.recorded[sym]=='PRIMARY_NO_SIGNAL' and state()==before
    assert store.db.execute('SELECT COUNT(*) FROM rpd_checkpoint').fetchone()==(0,)
    lane.tick((f,),lambda:True)
    assert not jobs,'C11_SETTLED_RAW_1M_GAP_REFETCHED'
    print('C11_SETTLED_RAW_1M_RESTORED_WITHOUT_REJUDGMENT')

run_settled_minute_repair()
run_case('empty_strategy_state', 0, [495,495,150])
run_case('stale_2020', 0, [495,495,150])
run_case('partial_more_local_candles', 900, [240])
print('C11_NORMAL_RECENT_GAP_REGRESSION_PASS')
