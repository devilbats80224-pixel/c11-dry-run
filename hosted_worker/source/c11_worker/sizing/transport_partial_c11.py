"""Actual VectorBT shared group order callback. Its policy sees features/state, never fills.
Python callback v1 for exact Decimal economics; native VBT fill/fee records/order execution.
"""
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parent.parent/'execution_conformance'))
from execution import fixed5_plan
from native_profit import realized_profit
import vectorbt as vbt
from vectorbt.portfolio import nb
import numpy as np
FEE=.0005

def no_order(c,T):
    return T.callback(c)
def settle(c,T):
    T.settle(c)

class Engine:
    def __init__(self, bars, funding, policy, initial=1000.):
        # Funding values are chronological official (rate, mark) events attached to known boundaries.
        self.bars=bars; self.funding=funding; self.policy=policy
        self.initial=float(initial);self.wallet=self.cash=self.initial
        self.pos={}; self.trace=[];self.fills=[];self.decisions=[];self.accrual=[]
        self.partial_filled=set();self.row=-1; self.phase=0; self.i=0; self.outstanding=None;self.exit_cols=[];self.offer_cols=[]
    def snapshot(self,c):
        for j in range(c.from_col,c.to_col):
            q=self.pos.get(j,{}).get('q',0.)
            assert abs(c.last_position[j]-q)<1e-9,('POSITION',self.row,j,c.last_position[j],q)
        assert abs(self.cash-(self.wallet-sum(p['margin'] for p in self.pos.values())))<1e-8
        assert abs(float(c.last_cash[c.group])-self.cash)<1e-8, ('VBT_CASH',self.row,float(c.last_cash[c.group]),self.cash)
        assert abs(float(c.last_free_cash[c.group])-self.cash)<1e-8
    def mark(self,j,price):
        p=self.pos[j];q,entry=p['q'],p['entry']
        return round(q*(price-entry)-q*(price+entry)*FEE+p['funding'],8)
    def accrue(self,col,through):
        p=self.pos[col]
        for t,rate,mark in self.funding.get(col,()):
            if p['through']<t<=through:
                cred=-p['q']*rate*mark;p['funding']+=cred
                self.accrual.append([col,t,cred])
        p['through']=through
    def callback(self,c):
        assert self.outstanding is None
        if c.call_idx==0:
            assert c.i==self.row+1
            self.row=c.i;self.phase=0;self.i=0;self.exit_cols=sorted(self.pos)
        E=int(self.bars['clock'][self.row])
        # All exits/hold updates first; each is evaluated from currently available row + prior held state.
        if self.phase==0:
            while self.i<len(self.exit_cols):
                j=self.exit_cols[self.i];self.i+=1
                # Separate pre-bar open intent (only sees the OPEN) from
                # native full-bar residual management. Replay the same leg
                # immediately after the trim settles, without a second trim.
                key=(self.row,j)
                trim=getattr(self.policy,'partial_open',None)
                part=trim(self,j,E,float(self.bars['ohlc'][self.row,j,0])) if trim and key not in self.partial_filled else None
                if part is not None:
                    q=float(part['qty']); x=float(part['price'])
                    assert 0<q<self.pos[j]['q'] and part['clock']=='OPEN'
                    self.partial_filled.add(key); self.i-=1
                    self.accrue(j,E-60000)
                    self.outstanding=(j,False,q,x,part|{'split':True})
                    return j,nb.order_nb(-q,price=x,fees=FEE+float(part['extra_fee_rate']),allow_partial=False,raise_reject=True,lock_cash=False)
                action=self.policy.manage(self,j,E,self.bars['ohlc'][self.row,j])
                through=E-60000 if action and action['clock']=='OPEN' else E
                self.accrue(j,through)
                if action:
                    q=self.pos[j]['q'];x=float(action['price'])
                    self.outstanding=(j,False,q,x,action)
                    return j,nb.order_nb(-q,price=x,fees=FEE,allow_partial=False,raise_reject=True,lock_cash=False)
                self.policy.on_hold(self,j,E,self.bars['ohlc'][self.row,j])
            self.phase=1;self.i=0;self.offer_cols=self.policy.offer_order(self,E)
        while self.i<len(self.offer_cols):
            j=self.offer_cols[self.i];self.i+=1
            if j in self.pos or len(self.pos)>=3:continue
            candidate=self.policy.offer(self,j,E)
            if candidate is None:continue
            x=float(self.bars['ohlc'][self.row,j,3])
            before_cash=self.cash; w=self.wallet
            if len(self.pos)==2 and all(self.mark(k,float(self.bars['ohlc'][self.row,k,3]))<0 for k in self.pos):
                self.decisions.append([E,j,'THIRD_VETO',0.,w,before_cash]);self.policy.on_veto(self,j,E);continue
            if not self.policy.after_third(self,j,E):continue
            kwargs={k:candidate[k] for k in ('q5','q1h','meta','tiers')}
            pair=candidate['pair'];plan=fixed5_plan(pair,w,before_cash,x,**kwargs)
            native=plan
            flag=bool(candidate['structure']) and bool(self.pos) and plan['quantity']>0
            flag=flag and all(self.policy.floor_pnl(self,k,E)>=0 for k in self.pos)
            if flag:
                zz=fixed5_plan(pair,w*1.2,before_cash,x,**kwargs)
                assert zz['quantity']>=plan['quantity'];plan=zz
            if candidate.get('reduce_to_half') and plan['quantity']>0:
                initial=plan
                plan=fixed5_plan(pair,w*1.2 if flag else w,min(before_cash,initial['margin']/2),x,**kwargs)
                assert plan['quantity']<=initial['quantity']/2+1e-12 and plan['margin']<=initial['margin']/2+1e-12
                self.policy.events.setdefault('volume_sizing',[]).append(dict(symbol=self.policy.names[j],E=E,native_quantity=initial['quantity'],quantity=plan['quantity'],before_cash=before_cash,initial_margin=initial['margin'],new_margin=plan.get('margin',0)))
            if getattr(self.policy,'enable_book_risk_cap',False):
                from risk_cap import tune
                plan=tune(self,j,E,x,plan,fixed5_plan,pair,w,before_cash,flag,kwargs,FEE)
            self.policy.on_attempt(self,j,E,plan,native,flag)
            q=plan['quantity'];self.decisions.append([E,j,'STRUCT40' if flag else plan['reason'],q,w,before_cash])
            if q<=0:continue
            assert q*x/5<=before_cash
            self.policy.validate_entry(self,j,E,x,plan)
            # Credit principal plus temporarily advanced entry fee before actual full-notional VBT order.
            advance=q*x*(1.-1./5.)+q*x*FEE
            c.last_cash[c.group]+=advance;c.last_free_cash[c.group]+=advance
            self.outstanding=(j,True,q,x,dict(plan=plan,protected40=flag))
            return j,nb.order_nb(q,price=x,fees=FEE,allow_partial=False,raise_reject=True,lock_cash=False)
        self.snapshot(c);self.trace.append([E,self.wallet,self.cash,self.wallet+sum(self.mark(j,float(self.bars['ohlc'][self.row,j,3]))for j in self.pos)])
        return -1,nb.order_nothing_nb()
    def settle(self,c):
        assert self.outstanding is not None
        j,b,q,x,a=self.outstanding;self.outstanding=None
        z=c.order_result
        assert c.col==j and z.status==0 and (z.side==0)==b and z.size==q and z.price==x
        fee_extra=float(a.get('extra_fee_rate',0.));assert abs(z.fees-q*x*(FEE+fee_extra))<1e-10
        E=int(self.bars['clock'][self.row]); fr=0.;net=None
        if b:
            m=q*x/5
            self.pos[j]=dict(q=q,entry=x,margin=m,funding=0.,through=E,entry_E=E)
            self.cash-=m
            self.policy.on_entry(self,j,E,x,a['plan'])
        elif a.get('split'):
            p=self.pos[j]
            assert 0<q<p['q'] and p['through']==E-60000
            fr=p['funding']*q/p['q'];net=round(realized_profit(q,p['entry'],x,fr)-q*x*fee_extra,8)
            original=p['entry']*q;release=original/5
            increment=-(original*(1.-1./5.)+original*FEE)+fr
            c.last_cash[c.group]+=increment;c.last_free_cash[c.group]+=increment
            self.wallet+=net;self.cash+=release+net
            p['q']-=q;p['margin']-=release;p['funding']-=fr
            # Deliberately no on_exit and no vacancy.
            self.policy.on_partial(self,j,E,a,net,p)
        else:
            p=self.pos.pop(j); fr=p['funding'];net=realized_profit(q,p['entry'],x,fr)
            original=p['entry']*q
            settlement=-(original*(1.-1./5.)+original*FEE)+fr
            c.last_cash[c.group]+=settlement;c.last_free_cash[c.group]+=settlement
            self.wallet+=net;self.cash+=p['margin']+net
            self.policy.on_exit(self,j,E,a,net,p)
        residual=self.cash-float(c.last_cash[c.group])
        # Decimal/reference round(8) per settlement; never use this to hide accounting mismatch.
        assert abs(residual)<5.1e-9 or abs(residual)<1e-8,(E,j,residual)
        c.last_cash[c.group]+=residual;c.last_free_cash[c.group]+=residual
        self.snapshot(c)
        self.fills.append(dict(E=E,col=j,side='BUY' if b else 'SELL',qty=q,price=x,fees=float(z.fees),funding=fr,net=net,reason='ENTRY'if b else a['why'],execution_clock=E-60000 if not b and a['clock']=='OPEN'else E))

def run_vbt(bars, funding, policy, initial=1000.):
    T=Engine(bars,funding,policy,initial)
    a=np.asarray(bars['ohlc'],dtype=float)
    assert a.ndim==3 and a.shape[2]==4 and len(bars['clock'])==a.shape[0]
    assert (np.diff(bars['clock'])==60000).all()
    p=vbt.Portfolio.from_order_func(a[:,:,3],no_order,T,use_numba=False,
       flexible=True,row_wise=True,cash_sharing=True,group_by=True,init_cash=initial,
       post_order_func_nb=settle,post_order_args=(T,),update_value=False,
       max_orders=max(1,2*a.shape[0]*a.shape[1]),freq='1min')
    assert len(T.trace)==a.shape[0] and len(p.orders.records)==len(T.fills)
    for v,r in zip(p.orders.records.to_dict('records'),T.fills):
        assert (v['idx'],v['col'],v['size'],v['price'],v['fees'],v['side'])==((r['E']-int(bars['clock'][0]))//60000,r['col'],r['qty'],r['price'],r['fees'],0 if r['side']=='BUY'else 1)
    return p,T
