"""Archived signal and route evidence evaluated only over readable native inputs."""
from decimal import Decimal
import math
import sqlite3

import numpy as np
import pandas as pd

from archived_routes import SweepBasis
from causal_exit_basis import NativeHoldingEvidence
from bridge_protocol import Refused
from causal_bars import FIVE,MINUTE,QUARTER
from nw_signals import calculate as nw
from archive_ewo import State
from archive_zero import calculate as zero
from archive_weak import archived_weak
from observer_rpc import now_ms


def array_clock(rows,width):
    return np.asarray([r.open_ms+width for r in rows],dtype=np.int64)


def prices(rows):
    return tuple((r.op,r.high,r.low,r.close) for r in rows)


def read_seeded(store,symbol,origin,width,end,as_of):
    if origin%width or (end-origin)//width<260:
        return ()
    return store.listing_range(symbol,width,origin,end,as_of)


def nw_flags(rows,width):
    closes=np.asarray([r.close for r in rows],float)
    result=nw(pd.DataFrame({'close':closes}))
    known=result.nw_known.to_numpy(bool)
    raw=result.nw_bullish_rebound.to_numpy(bool)&known
    signal=raw if width==FIVE else raw&(closes>np.r_[np.nan,closes[:-1]])
    ends=array_clock(rows,width)
    return ends,known,set(map(int,ends[signal]))


def alternate_events(rows,required):
    """Same EWO/ZL primary archive seeds and warm-up convergence checks."""
    if not required:
        return ()
    if len(rows)<701:
        return None
    hrs=pd.DataFrame(prices(rows),columns=['open','high','low','close'])
    hrs['date']=pd.to_datetime([r.open_ms for r in rows],unit='ms',utc=True)
    end=array_clock(rows,QUARTER)
    exact=zero(hrs)
    short=zero(hrs.iloc[100:].reset_index(drop=True)).set_index('clock')
    if not np.array_equal(exact.clock.to_numpy(),end):
        return None
    long_state,short_state=State(),State()
    events=[]
    pending=set(required)
    for i,r in enumerate(rows):
        allowed=long_state.flags(r.close)[0]
        long_state.commit(r.close)
        again=False
        if i>=100:
            again=short_state.flags(r.close)[0]
            short_state.commit(r.close)
        stamp=int(end[i])
        if stamp not in pending:
            continue
        if i<700 or allowed!=again or stamp not in short.index:
            return None
        x,y=exact.iloc[i],short.loc[stamp]
        if ((x.big,x.small,x.trend)!=(y.big,y.small,y.trend)
                or any(not (math.isfinite(x[k]) and math.isfinite(y[k]) and
                       abs(x[k]-y[k])<=abs(x[k])*1e-8) for k in ('basis','volatility'))):
            return None
        pending.remove(stamp)
        if allowed or x.big or x.small:
            events.append(stamp)
    return tuple(events) if not pending else None


def owned_priority(bot,trade):
    """Existing local hard/cancel evidence can veto, regardless of market history."""
    if not bot.native_paper_exchange() or not bot.entry_journal.owns(trade):
        return 'NATIVE_CAMPAIGN_UNKNOWN'
    if not trade.is_open or trade.is_short or trade.has_open_orders:
        return 'NATIVE_PENDING_ORDER_OR_POSITION_UNKNOWN'
    if not bot.hard_guard.trim_can_resume(bot.campaign(trade)):
        return 'NATIVE_HARD_INTENT_HAS_PRIORITY'
    return 'NATIVE_HARD_CHECK_ONLY'


class RuntimeRouteEvidence:
    def __init__(self,observer):
        self.observer=observer
        self.holding=NativeHoldingEvidence(self.weak_status)

    def sweep(self,bot,pair,as_of):
        end=as_of//FIVE*FIVE
        if not end<=as_of<end+MINUTE:
            return None
        market=bot.exchange.markets[pair]
        symbol=market['id']
        start=self.observer.episode(symbol,end)
        if start is None or self.observer.span(symbol,start,end) is None:
            return None
        # All prices and their receipts must already be in the native store.
        # Matching slices may not be downloaded now to backfill absence at E.
        store=bot.policy.bars
        origin=store.segment(symbol)
        last15=end//QUARTER*QUARTER
        if origin is None or origin.proven_at_ms>end or origin.open_ms!=market['info'].get('onboardDate'):
            return None
        r5=read_seeded(store,symbol,origin.open_ms,FIVE,end,as_of)
        r15=read_seeded(store,symbol,origin.open_ms,QUARTER,last15,as_of)
        r1=store.range(symbol,MINUTE,start-10*MINUTE,end,as_of)
        if not r5 or not r15 or not r1:
            return None
        e5,k5,raw5=nw_flags(r5,FIVE)
        e15,k15,long15=nw_flags(r15,QUARTER)
        eligible=tuple(range(start,end+FIVE,FIVE))
        expected15=tuple(k for k in eligible if k%QUARTER==0)
        if (not k5[(e5>=start)&(e5<=end)].all()
                or not k15[(e15>=start)&(e15<=end)].all()):
            return None
        if any(t in raw5 or t in long15 for t in eligible):
            return None
        # The archive also excludes EWO and Zero Lag events at eligible 15m clocks.
        alternate=alternate_events(r15,expected15)
        if alternate is None or alternate:
            return None
        windows=dict(eligible=eligible,origin={t:start for t in eligible},r5=raw5,l15=long15,
                     t5=e5,t15=e15,k5=k5,k15=k15,t=array_clock(r1,MINUTE),p=np.asarray(prices(r1)))
        if now_ms()>=end+MINUTE:
            return None
        # Reuse the pinned sweep prefix to identify the three exact source clocks.
        from sweep_pulses import pulses
        found=pulses(windows,e15,np.asarray(prices(r15)),finish=end)
        if len(found)!=1 or found[0][0]!=end:
            return None
        _,_,sweep,*_=found[0]
        points={}
        for t in (start,sweep,end):
            point=self.observer.point(symbol,t)
            if point is None or point[2]<Decimal('.20'):
                return None
            points[t]=point[2]*100
        ten=tuple((r.open_ms+MINUTE,r.high,r.low,r.close) for r in r1[-10:])
        needed=store.range(symbol,FIVE,last15-6*FIVE,last15,as_of)
        quote=None
        if needed:
            quote=dict(previous=float(sum(r.quote for r in needed[:3])),
                       current=float(sum(r.quote for r in needed[3:])),
                       previous_end=last15-QUARTER,current_end=last15)
        return SweepBasis(symbol,start,end,now_ms(),windows,tuple(map(int,e15)),prices(r15),
                          (),end,'ABSENT_VERIFIED',points,r5[-1].close,ten,quote)

    def weak_status(self,bot,pair,entry,since,end,*,as_of=None):
        age=end-since
        if age<60*MINUTE or age%(30*MINUTE):
            return 'NOT_SCHEDULED'
        symbol=bot.exchange.markets[pair]['id']
        as_of=end if as_of is None else as_of
        one=bot.policy.bars.range(symbol,MINUTE,end-MINUTE,end,as_of)
        if not one:
            return None
        if one[0].close>=entry:
            return 'KNOWN_FALSE_PRICE'
        five=bot.policy.bars.range(symbol,FIVE,end-12*FIVE,end,as_of)
        if not five:
            return None
        pair=self.observer.weak(symbol,end)
        clocks={t:q for t,q,_ in pair} if pair else {}
        return archived_weak(symbol,entry,since,end,one[0].close,
            {symbol:(array_clock(five,FIVE),np.asarray([r.high for r in five]),np.asarray([r.quote for r in five]))},
            {(symbol,k):q for k,q in clocks.items()})

    def exit(self,bot,trade,current_bar):
        if owned_priority(bot,trade)!='NATIVE_HARD_CHECK_ONLY':
            return None
        try:
            return self.holding.read(bot,trade,current_bar)
        except sqlite3.Error as error:
            raise Refused('C11_HOLDING_LEDGER_UNAVAILABLE') from error
