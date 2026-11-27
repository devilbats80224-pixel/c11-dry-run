"""Chronological C11 policy adapter; frozen pure feature/indicator helpers only.
No Python Book, archived fills or outcome-directed candidates.
"""
from dataclasses import replace
import numpy as np
from transport_c11 import run_vbt,FEE

class C11Policy:
    def __init__(self,source):
        self.x=source;self.data=source['data'];self.names=sorted(self.data);self.idx={s:j for j,s in enumerate(self.names)};self.clock=source['CLOCK']
        self.primary=source['signals'];self.extra=source['EXTRA'];self.states={};self.atrs={};self.weak={};self.mature={};self.info={};self.prep={}
        self.events={k:[] for k in ('closed','attempts','transitions','atr_audit','weak','mature','early2','new_mature','reserve','third','size','exposure')}
        self.deps=source['helpers'];self.cut=self.deps['ceil_tick']
    def offer_order(self,T,E):
        if T.row==len(self.clock)-1:return []
        return [self.idx[s] for s in sorted(self.primary.get(E,()))+sorted(self.extra.get(E,()))]
    def offer(self,T,j,E):
        s=self.names[j];d=self.data[s];i=T.row
        rule,dist=self.deps['STRUCTURE_FN'](E,i,self.clock,d['px'],float(d['px'][i,3]))
        return dict(pair=d['pair'],structure=rule=='STRUCT40',q5=float(d['qs']['5m'][0][i]),q1h=float(d['qs']['1h'][0][i]),meta=d['meta'],tiers=self.deps['TIERS'][d['pair']])
    def on_veto(self,T,j,E):
        self.events['third'].append(dict(symbol=self.names[j],E=E,prior=[[self.names[k],T.mark(k,float(self.data[self.names[k]]['px'][T.row,3])),float(self.data[self.names[k]]['px'][T.row,3]),T.pos[k]['q']] for k in sorted(T.pos)],wallet=T.wallet,free=T.cash))
    def after_third(self,T,j,E):
        s=self.names[j]
        if s in self.extra.get(E,()) and len(T.pos)>=2:
            self.events['reserve'].append(dict(symbol=s,E=E,held=[self.names[k] for k in sorted(T.pos)],basis='LAST_SLOT_RESERVED_FOR_NW15'));return False
        return bool(self.deps['SLOT_ALLOW'](E,s,len(T.pos)))
    def published_floor(self,T,j,E):
        old=self.states[j];atr=self.atrs[j];mark=float(self.data[self.names[j]]['px'][T.row,3]);peak=[old.hard_stop]
        for f in (old.passed_stage,atr.published if atr.guard.loss.author_armed else 0.):
            if 0<f<mark:peak.append(f)
        return max(peak)
    def on_attempt(self,T,j,E,plan,native,flag):
        s=self.names[j];d=self.data[s];i=T.row;px=float(d['px'][i,3]);cut=self.cut(px*.94,d['meta']['tick'])
        rule,dist=self.deps['STRUCTURE_FN'](E,i,self.clock,d['px'],px)
        if flag:
            # Exact original multiplication order for published-floor PnL.
            floors=[self.floor_pnl(T,k,E) for k in T.pos]
            self.events['size'].append(dict(symbol=s,E=E,old_qty=native['quantity'],new_qty=plan['quantity'],structure=dist,wallet=T.wallet,free=T.cash,old_margin=native['margin'],new_margin=plan['margin'],nominal_target=T.wallet*.40,actual_cap_binds=plan['margin']<T.wallet*.40-d['meta']['lot']*px/5-1e-9,unchanged=plan['quantity']==native['quantity'],published_incumbent_cost_pnl=floors))
        if plan['quantity']>0:
            # Source-proven aggregate helper requirement, implemented from exact data, not Book.
            nav,existing=self.risk(T,E)
            nr=(px-cut+(px+cut)*FEE)*plan['quantity']
            self.events['exposure'].append(dict(symbol=s,E=E,quantity=plan['quantity'],wallet=T.wallet,nav=nav,free=T.cash,total_stop_risk=existing+nr,stop_risk_pct_NAV=(existing+nr)/nav*100,actual_total_margin=T.wallet-T.cash+plan['margin'],positions=len(T.pos)+1,rule=rule))
        self.events['attempts'].append(dict(symbol=s,E=E,wallet=T.wallet,free=T.cash,plan=plan))
    def floor_pnl(self,T,k,E):
        p=T.pos[k];stop=self.published_floor(T,k,E);q=p['q'];old=p['entry']
        return round(q*(stop-old)-FEE*q*(stop+old)+p['funding'],8)
    def risk(self,T,E):
        nav=T.wallet;loss=0.
        for k,p in T.pos.items():
            mark=float(self.data[self.names[k]]['px'][T.row,3]);stop=self.published_floor(T,k,E)
            q,op=p['q'],p['entry']
            nav+=round(q*(mark-op)-FEE*q*(mark+op)+p['funding'],8)
            loss+=max(0.,(mark-stop)*(1-FEE))*q
        return nav,loss
    def validate_entry(self,T,j,E,x,z):
        s=self.names[j];d=self.data[s];p=d['pair'];tick=d['meta']['tick']
        floor=self.cut(x*.94,tick); buffer=self.deps['buffered_liquidation'](p,x,z['quantity'],z['margin'],tick)
        if floor<=buffer or floor>=x:raise ValueError(f'INITIAL_HARD_BOUNDARY:{s}:{E}')
        if not self.x['feature_map'][s]['known'][T.row]:raise ValueError(f'ATR_ENTRY_SOURCE_UNKNOWN:{s}:{E}')
        self.prep[j]=(floor,tick,buffer)
    def on_entry(self,T,j,E,px,plan):
        h,t,b=self.prep.pop(j); D=self.deps
        self.states[j]=D['State'](entry=px,hard_stop=h,entry_E=E,last_E=E,gate='UNSEEN');self.weak[j]=(0,False)
        self.info[j]=dict(tick=t,buffer=b)
        ar=D['atr_initial'](px);self.atrs[j]=replace(ar,guard=replace(ar.guard,loss=replace(ar.guard.loss,sl=h)),last_E=E)
        D['SLOT_ENTER'](E,self.names[j])
    def manage(self,T,j,E,o):
        s=self.names[j];d=self.data[s];p=self.info[j];i=T.row;op,h,l,c=map(float,o);D=self.deps
        old=self.states[j];atr=self.atrs[j];ms=self.mature.get(j);_,weak_pending=self.weak[j]
        assert E>old.entry_E and atr.last_E==E-60000
        if op<=p['buffer']:raise ValueError(f'OPEN_TOUCH_STATIC_BUFFER:{s}:{E}')
        ts,q0,q1=d['qs']['1h'][2][i],d['qs']['1h'][0][i],d['qs']['1h'][1][i]
        hour=dict(known=True,end_E=int(ts),available_E=int(ts),last=float(q0),previous=float(q1))
        pub='WEAK_PROOF_ONLY_NEXT_OPEN' if weak_pending else None
        if not pub and ms and ms[1]:pub='MATURE60_NEXT_OPEN'
        if not pub and p.get('early2_pending'):pub=D['E2_REASON']
        if not pub and p.get('lowerhigh_pending'):pub=D['MAT_EXIT_REASON']
        n,a=D['EARLY_TRANSPORT']('MID_4_6_08_16','INTRA_WEAK',old,D['Bar'](E,op,h,l,c),published=D['Published'](original_open_exit=pub),atr=atr,hour=hour,original=D['STEP_TRANSPORT'])
        if not n.held:return dict(clock=a['fill_at'],price=a['price'],why=a['reason'])
        assert a['reason']=='HOLD'
        self.prep[j]=n
    def on_hold(self,T,j,E,o):
        s=self.names[j];d=self.data[s];p=self.info[j];i=T.row;_,h,_,c=map(float,o);D=self.deps
        old=self.states[j];oldatr=self.atrs[j];ms=self.mature.get(j);v=self.x['feature_map'][s];y=v['values'][i]
        a,why=D['atr_update'](oldatr,E=E,close=c,high=h,atr15=float(y[0]),atr5=float(y[1]),mult=float(y[2]),er=float(y[3]),previous_er=float(y[4]),tick=p['tick'],boundary=p['buffer'],source15=E//900000*900000,source5=E//300000*300000,source_er=E//900000*900000,known=bool(v['known'][i]))
        if (a.guard.loss.author_armed,a.published,a.pending)!=(oldatr.guard.loss.author_armed,oldatr.published,oldatr.pending):
            self.events['atr_audit'].append(dict(symbol=s,E=E,armed=a.guard.loss.author_armed,published=a.published,pending=a.pending,why=why))
        assert not a.pending
        self.atrs[j]=a
        peak=max(p.get('early2_peak_close',old.entry),c);p['early2_peak_close']=peak
        ev=D['EVALUATE'](E,old.entry_E,old.entry,peak,c,a.guard.loss.author_armed,d['px'],d['qs']['5m'],i)
        if ev:
            why,low,q0,q1=ev;self.events['early2'].append(dict(symbol=s,entry_E=old.entry_E,E=E,reason=why,peak_close=peak,close=c,prior_low=low,q_current=q0,q_previous=q1))
            if why=='SCHEDULE_NEXT_OPEN':p['early2_pending']=True
        if not p.get('vert_mature',False) and D['VQUAL'](already_mature=bool(ms),atr_armed=a.guard.loss.author_armed,gate=old.gate,published_prv=old.passed_stage,entry=old.entry,close=c,peak=a.peak,tick=p['tick'],ceil_tick=self.cut):
            p['vert_mature']=True
            self.events['new_mature'].append(dict(symbol=s,entry_E=old.entry_E,E=E,reason='VERTICAL_MATURE_ARM_PERMISSION',entry=old.entry,close=c,previous_stage=old.passed_stage,known_peak=a.peak,prev_ATR=oldatr.guard.loss.author_armed))
        line,when=D['MATURE_ADVANCE'](ms[0] if ms else 0.,E=E,entry=old.entry,close=c,armed=(a.guard.loss.author_armed or p.get('vert_mature',False)),peak=a.peak,tick=p['tick'],original_pending=a.pending,ceil_tick=self.cut,fraction=.6)
        if line:self.mature[j]=(line,when=='SCHEDULE_NEXT_OPEN')
        mat=D['MAT_OBSERVE'](E,i,self.clock,d['px'],old.entry,ms,c,a.pending or when=='SCHEDULE_NEXT_OPEN')
        if mat:
            why,h0,h1=mat;self.events['new_mature'].append(dict(symbol=s,entry_E=old.entry_E,E=E,reason=why,old_mature=ms,close=c,entry=old.entry,prior_close=float(d['px'][i-15,3]) if i>=15 else None,high_current=h0,high_previous=h1))
            if why=='MATURE20_LOWER_HIGH':p['lowerhigh_pending']=True
        if when:self.events['mature'].append(dict(symbol=s,entry_E=old.entry_E,E=E,kind=when,close=c,line=line,peak=a.peak,original_published=oldatr.published))
        W=D['WEAK_CHECK'](s,old.entry,old.entry_E,E,c)
        if W!='NOT_SCHEDULED':
            wc=self.weak[j][0]+1 if W=='KNOWN_TRUE_QUOTE'else 0;self.weak[j]=(wc,wc>=2)
            self.events['weak'].append(dict(symbol=s,entry_E=old.entry_E,E=E,status=W,known_true_run=wc,enqueued=wc>=2))
        n=self.prep.pop(j);n=replace(n,passed_stage=self.cut(n.passed_stage,p['tick']) if n.passed_stage else 0.,failed_floor=self.cut(n.failed_floor,p['tick']) if n.failed_floor else 0.)
        if (n.gate,n.passed_stage,n.failed_floor,n.pending)!=(old.gate,old.passed_stage,old.failed_floor,old.pending):
            self.events['transitions'].append(dict(symbol=s,E=E,gate=n.gate,pass_stage=n.passed_stage,fail_floor=n.failed_floor,pending=n.pending))
        self.states[j]=n
    def on_exit(self,T,j,E,act,net,pos):
        s=self.names[j];old=self.states.pop(j); self.atrs.pop(j);self.weak.pop(j);self.info.pop(j);self.mature.pop(j,None)
        px=act['price'];why=act['why'];ee=E-60000 if act['clock']=='OPEN' else E
        self.events['closed'].append(dict(symbol=s,entry_E=old.entry_E,exit_bar_E=E,exit_time_proxy=ee,entry=old.entry,exit=px,qty=pos['q'],net=net,funding=pos['funding'],reason=why,price_return_pct=(px/old.entry-1)*100,stage_gate=old.gate))
        self.deps['SLOT_EXIT'](E,s,'WEAK_PROOF_ONLY_NEXT_OPEN' if why==self.deps['E2_REASON']else why)
    def native(self,rows=None):
        f={j:[(int(r['fundingTime']),float(r['fundingRate']),float(r['markPrice'])) for r in self.data[s]['funding']]for j,s in enumerate(self.names)}
        n=len(self.clock) if rows is None else rows
        bars=dict(clock=self.clock[:n],ohlc=np.stack([self.data[s]['px'][:n] for s in self.names],axis=1))
        return run_vbt(bars,f,self)
