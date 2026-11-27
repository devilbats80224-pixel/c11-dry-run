"""Exact archived decision ports. Native dispatch requires separately verified facts."""
from dataclasses import dataclass, replace
from decimal import Decimal
import math

import numpy as np

from causal_bars import FIVE, MINUTE, QUARTER, MAX_SEGMENT_BARS
from c11_closed5_rejected_break_port_v1.rejected_break import ClosedMinute, classify as rejected_break
from contour_rank import ranks
from firstproof import should_exit
from mature_lowerhigh import MAT_REASON, observe
from mature_once import advance
from atr_component import ceil_tick
from sweep_pulses import pulses
from vertical_permission import qualified

WEAK = 'WEAK_PROOF_ONLY_NEXT_OPEN'
MATURITY = 'MATURE60_NEXT_OPEN'
CONTOURS = ('MATURE_CONTOUR_REST_GAP', 'MATURE_CONTOUR_REST_INTRABAR')
EXIT_REASONS = (WEAK, MATURITY, MAT_REASON, *CONTOURS, 'EARLY2_QUOTE_BREAK_NEXT_OPEN',
                'ORIGINAL_ARMED_TRAIL_GAP', 'ORIGINAL_ARMED_TRAIL_INTRABAR',
                'HOUR_PASS_STAGE_GAP', 'HOUR_PASS_STAGE_INTRABAR')


@dataclass(frozen=True)
class SweepBasis:
    symbol: str
    episode_start: int
    end: int
    now_ms: int
    windows: dict
    quarter_ends: tuple
    quarter_ohlc: tuple
    alternative_events: tuple
    alternatives_known_through: int
    archived_original_offer: str
    oi_points: dict
    five_close: float
    minute_rows: tuple
    quote: dict | None


@dataclass(frozen=True)
class Sweep:
    symbol: str
    episode_start: int
    confirmed_sweep: int
    end: int
    close: float
    quote: dict | None


def sweep_offer(basis):
    if type(basis) is not SweepBasis or not (basis.episode_start <= basis.end <= basis.now_ms < basis.end+MINUTE):
        return None
    # An existing offer, even an original veto, retains precedence.
    if (basis.archived_original_offer != 'ABSENT_VERIFIED'
            or basis.alternatives_known_through != basis.end
            or any(basis.episode_start <= e <= basis.end for e in basis.alternative_events)):
        return None
    if basis.end % FIVE or basis.episode_start % FIVE or basis.end-basis.episode_start >= MAX_SEGMENT_BARS*FIVE:
        return None
    eligible = basis.windows['eligible']
    # The archived mask declares the episode; the strategy-neutral latest
    # snapshot alone cannot establish this window or its first instant.
    expected = tuple(range(basis.episode_start,basis.end+FIVE,FIVE))
    if tuple(sorted(set(eligible))) != expected or any(basis.windows['origin'].get(t) != basis.episode_start for t in expected):
        return None
    rows = pulses(basis.windows,np.asarray(basis.quarter_ends),np.asarray(basis.quarter_ohlc),finish=basis.end)
    if len(rows) != 1 or rows[0][0] != basis.end:
        return None
    end, start, sweep, high, low, shelf, close = rows[0]
    if not (start <= sweep < end <= sweep+30*MINUTE and close > high > low and low < shelf
            and close == basis.five_close):
        return None
    selected = tuple(basis.oi_points.get(e) for e in (start,sweep,end))
    if any(type(p) is not Decimal or not p.is_finite() or p < 20 for p in selected):
        return None
    ten = tuple(ClosedMinute(e,h,l,c) for e,h,l,c in basis.minute_rows)
    if (tuple(x.end_ms for x in ten) != tuple(range(end-9*MINUTE,end+MINUTE,MINUTE))
            or ten[-1].close != close or rejected_break(end,ten) != 'KEEP'):
        return None
    quote = basis.quote
    if quote is not None:
        last = end//QUARTER*QUARTER
        if ((quote.get('previous_end'),quote.get('current_end'))!=(last-QUARTER,last)
                or any(type(quote.get(k)) not in (int,float) or not math.isfinite(quote[k]) or quote[k]<0
                       for k in ('previous','current'))):
            return None
    return Sweep(basis.symbol,start,sweep,end,close,quote)


@dataclass(frozen=True)
class FirstProofBasis:
    entered: int
    entry: float
    closes: tuple
    weak: tuple
    armed: bool
    early2_pending: bool
    lowerhigh_pending: bool
    original_pending: bool


def firstproof_clock(basis):
    if type(basis) is not FirstProofBasis or any(type(v) is not bool for v in
            (basis.armed,basis.early2_pending,basis.lowerhigh_pending,basis.original_pending)):
        return None
    ends = [e for e,_ in basis.closes]
    if not ends or ends != list(range(basis.entered+MINUTE,ends[-1]+MINUTE,MINUTE)):
        return None
    if any(not math.isfinite(p) or p<=0 for _,p in basis.closes):
        return None
    checks = range(basis.entered+60*MINUTE,ends[-1]+MINUTE,30*MINUTE)
    if not basis.weak or [e for e,_ in basis.weak] != list(checks) or basis.weak[-1][0] != ends[-1]:
        return None
    run = 0
    for end,status in basis.weak:
        # Explicit status receipts only. A missing scheduled evaluation above
        # is not a known false. The archive resets on non-true observations.
        if status not in ('KNOWN_TRUE_QUOTE','KNOWN_FALSE_PRICE','KNOWN_FALSE_HIGH','KNOWN_FALSE_OI',
                          'UNKNOWN_REQUIRED_QUANTITY','NO_CONFIRMED_QUOTE_ORIGINAL_CVD_UNKNOWN'):
            return None
        run = run+1 if status == 'KNOWN_TRUE_QUOTE' else 0
        # The received prefix must represent a still-open trade.
        if end != ends[-1] and run:
            return None
    peak_close = max(basis.entry,*(p for _,p in basis.closes))
    blocked = basis.early2_pending or basis.lowerhigh_pending or basis.original_pending
    return ends[-1] if should_exit(basis.entry,peak_close,basis.armed,run,run>=2,blocked) else None


@dataclass(frozen=True)
class MatureState:
    line: float = 0.
    queued: bool = False
    vertical: bool = False
    lowerhigh: bool = False


def advance_mature(prior, end, *, entry, close, peak, atr_armed, original_pending,
                   previous_gate, previous_stage, tick, clock, prices):
    if prior.queued or prior.lowerhigh:
        return None
    vertical = prior.vertical or qualified(already_mature=bool(prior.line),atr_armed=atr_armed,
        gate=previous_gate,published_prv=previous_stage,entry=entry,close=close,peak=peak,tick=tick,ceil_tick=ceil_tick)
    line, change = advance(prior.line,E=end,entry=entry,close=close,armed=atr_armed or vertical,peak=peak,tick=tick,
                          original_pending=original_pending,ceil_tick=ceil_tick,fraction=.65)
    prior_mature = (prior.line,prior.queued) if prior.line else None
    lower = observe(end,len(clock)-1,np.asarray(clock),np.asarray(prices),entry,prior_mature,close,
                    original_pending or change=='SCHEDULE_NEXT_OPEN')
    if lower and lower[0].startswith('UNKNOWN'):
        return None
    return MatureState(line,change=='SCHEDULE_NEXT_OPEN',vertical,bool(lower and lower[0]=='MATURE20_LOWER_HIGH')),change



@dataclass(frozen=True)
class MatureClose:
    end: int
    close: float
    peak: float
    atr_armed: bool
    original_pending: bool
    previous_gate: str
    previous_stage: float
    clock: tuple
    prices: tuple


def replay_mature(entry, entered, end, tick, closes):
    if (type(closes) is not tuple or not 0 < end-entered <= MAX_SEGMENT_BARS*MINUTE
            or tuple(row.end for row in closes) != tuple(range(entered+MINUTE,end+MINUTE,MINUTE))):
        return None
    state = MatureState()
    for row in closes:
        if (type(row) is not MatureClose or type(row.atr_armed) is not bool
                or type(row.original_pending) is not bool or row.original_pending
                or row.previous_gate not in ('UNSEEN','PASS','FAIL')
                or not all(math.isfinite(x) for x in (row.close,row.peak,row.previous_stage))
                or not 0 < row.close <= row.peak):
            return None
        step = advance_mature(state,row.end,entry=entry,close=row.close,peak=row.peak,atr_armed=row.atr_armed,
            original_pending=row.original_pending,previous_gate=row.previous_gate,previous_stage=row.previous_stage,
            tick=tick,clock=row.clock,prices=row.prices)
        if step is None:
            return None
        state,_ = step
    return state


def queued_exit(*, weak, mature, early2, lowerhigh):
    # Same predecessor order as C11Policy.manage. EARLY2 remains unsupported
    # here but must veto a lower-ranked route.
    if weak:
        return WEAK
    if mature:
        return MATURITY
    if early2:
        return 'EARLY2_QUOTE_BREAK_NEXT_OPEN'
    if lowerhigh:
        return MAT_REASON
    return None


def contour_exit(*, end, state, queued, previous_close, previous_highs, older_floor, opening, observed, original):
    if (not state.line or state.queued or queued or previous_close <= state.line
            or len(previous_highs)!=30 or not ranks(original,opening,state.line,observed,older_floor)):
        return None
    last = (end-MINUTE)//QUARTER*QUARTER
    if tuple(e for e,_ in previous_highs) != tuple(range(last-29*MINUTE,last+MINUTE,MINUTE)):
        return None
    highs = tuple(h for _,h in previous_highs)
    if (any(not math.isfinite(h) or h<=0 for h in highs) or max(highs[15:]) >= max(highs[:15])):
        return None
    return (CONTOURS[0],opening) if opening <= state.line else (CONTOURS[1],state.line)
