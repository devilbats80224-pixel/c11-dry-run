"""Causal, bounded checkpoint of the pinned default C11 RPD5 recurrence."""
from dataclasses import asdict, dataclass, field
import json
import math

import numpy as np
import pandas as pd

from bridge_protocol import need
from c11_primary_decision_seam_v1.exact_rpd5_join import ClosedRpdState,Scope
from c11_primary_decision_seam_v1.rpd5_producer import PIN,WIDTH,WARM,SegmentStart

SCHEMA = 'c11-rpd5-default-v1:' + PIN


@dataclass
class Rma:
    warm: list = field(default_factory=list)
    value: float | None = None

    def advance(self, value, periods):
        if value is not None:
            if self.value is None:
                self.warm.append(value)
                if len(self.warm) == periods:
                    self.value = float(np.mean(self.warm))
                    self.warm.clear()
            else:
                self.value = (self.value*(periods-1)+value)/periods
        return self.value


@dataclass
class VolumeMean:
    # Match the installed pandas fixed-window, nonnegative rolling mean,
    # including its separate compensation on removal and on addition.
    total: float = 0.
    add_error: float = 0.
    remove_error: float = 0.
    same: int = 0
    previous: float | None = None

    def advance(self, old_bars, value, width):
        if len(old_bars) >= width:
            term=-old_bars[-width][3]-self.remove_error
            update=self.total+term
            self.remove_error=(update-self.total)-term
            self.total=update
        term=value-self.add_error
        update=self.total+term
        self.add_error=(update-self.total)-term
        self.total=update
        self.same=self.same+1 if value==self.previous else 1
        self.previous=value
        if len(old_bars)+1 < width:
            return None
        return value if self.same>=width else max(0.,self.total/width)


def strength_ewma(prior, value):
    if prior is None or prior == value:
        return value
    return float(pd.Series([prior,value]).ewm(span=10,adjust=False).mean().iloc[-1])


def last(values, back=1):
    return values[-back] if len(values) >= back else None


@dataclass
class RpdState:
    symbol: str
    origin: int
    observed: int
    next_open: int
    n: int = 0
    bars: list = field(default_factory=list)
    smooth: list = field(default_factory=list)
    deltas: list = field(default_factory=list)
    velocity: float | None = None
    state: float = 0.
    entropy: float = .5
    fractal_high: bool = False
    fractal_low: bool = False
    strength_ewm: float | None = None
    atr14: Rma = field(default_factory=Rma)
    atr16: Rma = field(default_factory=Rma)
    up17: Rma = field(default_factory=Rma)
    down17: Rma = field(default_factory=Rma)
    mean5: VolumeMean = field(default_factory=VolumeMean)
    mean17: VolumeMean = field(default_factory=VolumeMean)
    upper: float | None = None
    lower: float | None = None
    trend: int = 1
    last_signal: int = -100000
    last_peak: list | None = None
    last_valley: list | None = None
    active_long: bool = False
    long_score: float | None = None

    @classmethod
    def start(cls, proof):
        need(type(proof) is SegmentStart and proof.open_ms % WIDTH == 0
             and 0 <= proof.open_ms <= proof.proven_at_ms,'C11_RPD_CHECKPOINT_ORIGIN')
        return cls(proof.symbol,proof.open_ms,proof.proven_at_ms,proof.open_ms)

    @classmethod
    def restore(cls, payload):
        obj=json.loads(payload)
        need(obj['schema']==SCHEMA,'C11_RPD_CHECKPOINT_VERSION')
        fields=obj['state']
        for key in ('atr14','atr16','up17','down17'):
            fields[key]=Rma(**fields[key])
        for key in ('mean5','mean17'):
            fields[key]=VolumeMean(**fields[key])
        return cls(**fields)

    def pack(self):
        return json.dumps(dict(schema=SCHEMA,state=asdict(self)),allow_nan=False,sort_keys=True)

    def advance(self, bar, as_of):
        need(bar.open_ms == self.next_open and bar.open_ms+WIDTH <= bar.received_ms <= as_of
             and bar.valid(WIDTH),'C11_RPD_CHECKPOINT_GAP_OR_CLOCK')
        h,l,c,v=bar.high,bar.low,bar.close,bar.volume
        old=last(self.bars)
        second=last(self.bars,2)
        smooth,vel,acc=None,None,None
        trend=self.trend
        tr=max(h-l,abs(h-old[2]),abs(l-old[2])) if old else h-l
        atr=self.atr14.advance(tr,14)
        atr16=self.atr16.advance(tr,16)
        change=c-old[2] if old else None
        up=self.up17.advance(max(change,0.) if change is not None else None,17)
        down=self.down17.advance(max(-change,0.) if change is not None else None,17)
        rsi=100-100/(1+up/down) if down else (100 if up else None)
        avg=self.mean5.advance(self.bars,v,5)
        avg17=self.mean17.advance(self.bars,v,17)
        upper,lower=None,None
        if atr16 is not None:
            mid=(h+l+2*c)/4
            upper=mid-1.1*atr16
            lower=mid+1.1*atr16
            if self.upper is not None and old[2]>self.upper:
                upper=max(upper,self.upper)
            if self.lower is not None and old[2]<self.lower:
                lower=min(lower,self.lower)
            if self.lower is not None and self.trend==-1 and c>self.lower:
                trend=1
            elif self.upper is not None and self.trend==1 and c<self.upper:
                trend=-1
        if second is not None:
            smooth=float(np.dot([(r[0]+r[1]+r[2])/3 for r in (second,old,(h,l,c))],[1,2,3])/6)
        previous_smooth=last(self.smooth)
        delta=smooth-previous_smooth if previous_smooth is not None and smooth is not None else None
        smooth_values=self.smooth+[smooth]
        window=[x for x in smooth_values[-25:] if x is not None]
        step=math.floor((smooth-min(window))/(max(window)-min(window))*5+.5) if window and max(window)>min(window) else 0.
        lag=last(self.smooth,13)
        if lag is not None and smooth is not None:
            vel=smooth-lag
            self.strength_ewm=strength_ewma(self.strength_ewm,vel)
            if self.velocity is not None:
                acc=vel-self.velocity
        strength=abs(self.strength_ewm) if self.strength_ewm is not None else None
        deltas=(self.deltas+[delta])[-25:]
        pct=sum(x>0 for x in deltas if x is not None)/max(1,sum(x is not None for x in deltas))
        entropy=float(-(pct*np.log2(pct)+(1-pct)*np.log2(1-pct))) if 0<pct<1 else .5
        peak=bool(second and ((old[0]>second[0] and old[0]>h) or self.fractal_high) and c<old[1])
        valley=bool(second and ((old[1]<second[1] and old[1]<l) or self.fractal_low) and c>old[0])
        preq=peak and self.state>=2 and trend==-1
        vreq=valley and self.state<=3 and trend==1
        pp=step>=2 and acc is not None and acc<0 and entropy<.85 and trend==-1
        pv=step<=3 and acc is not None and acc>0 and entropy<.85 and trend==1
        bd=bool(peak and self.last_peak and self.velocity is not None and self.last_peak[1] is not None
                and old[0]>self.last_peak[0] and self.velocity<self.last_peak[1])
        bu=bool(valley and self.last_valley and self.velocity is not None and self.last_valley[1] is not None
                and old[1]<self.last_valley[0] and self.velocity>self.last_valley[1])
        effective=65-strength/atr*5 if strength is not None and atr else None
        def score(divergence):
            if effective is None:
                return None
            e=self.entropy
            raw=40+strength/atr*30+(1-e)*10+(10+(1-e)*5 if e<.85 else -5)+(20+abs(acc)*2 if divergence and acc is not None else 0)
            raw+=8+(rsi-50)/5 if rsi is not None and ((rsi>65 and peak) or (rsi<40 and valley)) else 0
            raw+=(5+(v/avg-1)*3 if avg17 is not None and v>1.2*avg17 else 0)+(10+strength*.5 if strength>atr else 0)+(h-l)/atr*3-1.5
            return min(99,max(40,math.floor(raw*10+.5)/10)) if math.isfinite(raw) else None
        ps=score(bd) if preq or pp else None
        vs=score(bu) if vreq or pv else None
        vp=ps is not None and ps>=effective and self.n-self.last_signal>10
        vl=vs is not None and vs>=effective and self.n-self.last_signal>10
        if vp:
            self.last_signal=self.n
            self.last_peak=[old[0],self.velocity]
        if vl:
            self.last_signal=self.n
            self.last_valley=[old[1],self.velocity]
        short=bool(vp and preq)
        long=bool(vl and vreq)
        if short:
            self.active_long,self.long_score=False,None
        if long:
            self.active_long,self.long_score=True,float(vs)
        bars=(self.bars+[[h,l,c,v]])[-17:]
        group=bars[-5:]
        self.fractal_high=len(group)==5 and all(group[2][0]>r[0] for i,r in enumerate(group) if i!=2)
        self.fractal_low=len(group)==5 and all(group[2][1]<r[1] for i,r in enumerate(group) if i!=2)
        self.bars,self.smooth,self.deltas=bars,smooth_values[-24:],deltas
        self.velocity,self.state,self.entropy=vel,step,entropy
        self.upper,self.lower,self.trend=upper,lower,trend
        self.n+=1
        self.next_open+=WIDTH
        self.observed=max(self.observed,bar.received_ms)
        return dict(long=long,short=short,long_score=vs,short_score=ps,predictive_long=bool(vl and pv and not vreq),
                    potential_long=bool(pv),active_long=self.active_long,last_long_score=self.long_score,
                    atr=atr,trend_strength=strength,rtrend=trend,state=step,velocity=vel,entropy=entropy,min_prob=effective)

    def closed(self):
        return ClosedRpdState(Scope(self.symbol,self.next_open),self.observed,self.n>=WARM,
                              self.active_long,self.long_score)
