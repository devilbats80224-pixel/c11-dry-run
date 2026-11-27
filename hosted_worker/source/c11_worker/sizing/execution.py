"""Source-proven older pure sizing + explicitly reconstructed fixed-5x admission.
Not final original kernel; no automatic market execution or indicator calculation.
"""
from pathlib import Path
import sys,math
from decimal import Decimal as D
sys.path.insert(0,str(Path(__file__).parent/'source'))
from native_sizing_exact_v1 import size_entry
from native_profit import realized_profit

class InputUnknown(ValueError):pass

def fixed5_plan(pair,wallet,free,price,q5,q1h,meta,tiers):
    # Reconstructed fixed-5x wrapper: unlike old resolve_entry, never fall back.
    if not tiers or any(not all(k in t for k in ('maxLeverage','minNotional','maxNotional')) for t in tiers):raise InputUnknown('TIERS_UNKNOWN')
    for t in tiers:
        if not all(isinstance(t[k],(int,float)) and math.isfinite(t[k]) for k in ('maxLeverage','minNotional','maxNotional')):raise InputUnknown('TIER_NONFINITE_OR_UNBOUNDED')
        if not (t['maxLeverage']>=1 and 0<=t['minNotional']<t['maxNotional']):raise InputUnknown('TIER_INVALID')
    ordered=sorted(tiers,key=lambda t:t['minNotional'])
    if ordered[0]['minNotional']!=0 or any(a['maxNotional']!=b['minNotional'] for a,b in zip(ordered,ordered[1:])):raise InputUnknown('TIER_COVERAGE_GAP')
    allow=[t['maxNotional'] for t in ordered if t['maxLeverage']>=5]
    if not allow:return {'quantity':0.,'reason':'COMPLETE_TIERS_MAX_LEVERAGE_BELOW5','leverage':5}
    required=('lot','tick','minnotional','maxqty','minqty','maxcost')
    if any(k not in meta for k in required):raise InputUnknown('MARKET_METADATA_MISSING')
    if math.isnan(meta['maxcost']) or meta['maxcost']<=0:raise InputUnknown('MAXCOST_INVALID')
    if not (meta['minnotional']>=0 and meta['maxqty']>0 and 0<=meta['minqty']<=meta['maxqty']):raise InputUnknown('MARKET_LIMITS_INVALID')
    vals=[wallet,free,price,q5,q1h,meta['lot'],meta['tick'],meta['minnotional'],meta['maxqty'],meta['minqty']]
    if not all(math.isfinite(x) for x in vals) or min(wallet,free,q5,q1h)<0 or min(price,meta['lot'],meta['tick'])<=0:raise InputUnknown('SIZING_INPUT_UNKNOWN')
    bracket=max(allow)
    # Exact older native validation reserve; .06 is its stake validation parameter,
    # NOT the position hard7 stop and NOT a risk-distance size scaler.
    minstake=max(meta['minnotional']*min(max(1.05/(1-.06),1.),1.5),meta['minqty']*price*1.05)/5
    maxstake=min(bracket,meta['maxqty']*price,meta.get('maxcost',float('inf')))/5
    raw,q=size_entry(wallet,free,price,5,q5,q1h,meta['maxqty'],meta['lot'],meta['tick'],meta['minnotional'],bracket,minstake,maxstake)
    if q>0:
        notional=D(str(q))*D(str(price))
        if not any(D(str(t['minNotional']))<=notional<=D(str(t['maxNotional'])) and t['maxLeverage']>=5 for t in ordered):raise InputUnknown('FINAL_NOTIONAL_TIER_NOT_SUPPORTED')
        assert D(str(q))%D(str(meta['lot']))==0
    return {'quantity':q,'raw_confirmed_quantity':raw,'leverage':5,'notional':q*price,'margin':q*price/5,'target_margin':min(wallet/3,free),'bracket5':bracket,'reason':'ACCEPTED' if q>0 else 'NATIVE_CAPACITY_REJECT','stop_distance_size_scaling':False}

class Book:
    """Older kernel cash convention: reserve margin; fees/funding settle on exit.
    Unlike integration_gate accounting.py, fees are NOT debited at entry.
    Both are bookkeeping conventions; this one explicitly matches source snapshot.
    """
    def __init__(self):self.wallet=1000.;self.cash=1000.;self.positions={};self.events=[]
    def invariant(self):assert abs(self.cash-(self.wallet-sum(p['margin'] for p in self.positions.values())))<1e-8
    def enter(self,key,q,price):
        assert key not in self.positions and len(self.positions)<3 and q>0
        m=q*price/5;assert m<=self.cash
        self.positions[key]=dict(q=q,price=price,margin=m,funding=0.);self.cash-=m;self.invariant();self.events.append(('entry',key))
    def funding(self,key,rate,mark):
        p=self.positions[key];credit=-p['q']*rate*mark;p['funding']+=credit;self.invariant();return credit
    def exit(self,key,price):
        p=self.positions.pop(key);net=realized_profit(p['q'],p['price'],price,p['funding']);self.wallet+=net;self.cash+=p['margin']+net;self.invariant();self.events.append(('exit',key));return net
    def process(self,exits,candidates):
        # Caller supplies immutable column index; exits all before entries.
        for key,price in sorted(exits):self.exit(key,price)
        for col,key,q,price in sorted(candidates):
            if key in self.positions or len(self.positions)>=3:continue
            if q*price/5<=self.cash:self.enter(key,q,price)

    def process_plans(self,exits,candidates):
        """Plan sequentially AFTER all settlements, using current wallet/free."""
        audits=[]
        for key,price in sorted(exits):self.exit(key,price)
        cols=[c['col'] for c in candidates]
        if len(cols)!=len(set(cols)):raise InputUnknown('DUPLICATE_ENTRY_COLUMN')
        for c in sorted(candidates,key=lambda x:x['col']):
            key=c['pair']
            if key in self.positions or len(self.positions)>=3:continue
            before={'wallet':self.wallet,'free':self.cash}
            plan=fixed5_plan(key,self.wallet,self.cash,c['price'],c['q5'],c['q1h'],c['meta'],c['tiers'])
            audits.append(dict(pair=key,before=before,plan=plan))
            if plan['quantity']>0:self.enter(key,plan['quantity'],c['price'])
        return audits
