"""Durable, causal holding evidence for the native C11 paper exit dispatcher."""
from dataclasses import asdict, dataclass, replace
from hashlib import sha256
import json
import math
from types import SimpleNamespace

import numpy as np

from archived_early2 import evaluate
from archived_routes import MatureState, advance_mature, contour_exit, queued_exit
from atr_component import ATRState, ceil_tick, close_update, initial
from bridge_protocol import need
from causal_bars import FIVE, HOUR, MINUTE, QUARTER
from firstproof import should_exit
from guard_state_v2 import Guard
from hour_stage import transport
from loss_state_v2 import State as Loss
from native_protection import wall_ms
from policy_snapshot import Bar, Published, State

SCHEMA = 'c11-causal-holding-v1'
EARLY2 = 'EARLY2_QUOTE_BREAK_NEXT_OPEN'
ATR_GAP = 'ORIGINAL_ARMED_TRAIL_GAP'
ATR_TOUCH = 'ORIGINAL_ARMED_TRAIL_INTRABAR'
HOUR_GAP = 'HOUR_PASS_STAGE_GAP'
HOUR_TOUCH = 'HOUR_PASS_STAGE_INTRABAR'
WEAK_STATUSES = frozenset(('KNOWN_TRUE_QUOTE', 'KNOWN_FALSE_PRICE', 'KNOWN_FALSE_HIGH',
                           'KNOWN_FALSE_OI', 'UNKNOWN_REQUIRED_QUANTITY',
                           'NO_CONFIRMED_QUOTE_ORIGINAL_CVD_UNKNOWN'))


@dataclass(frozen=True)
class Holding:
    old: State
    atr: ATRState
    mature: MatureState
    peak_close: float
    weak_run: int = 0
    weak_pending: bool = False
    early2_pending: bool = False
    blocked: str | None = None

    @classmethod
    def start(cls, entered, price, hard):
        old = State(entry=price, hard_stop=hard, entry_E=entered, last_E=entered)
        atr = initial(price)
        atr = replace(atr, guard=replace(atr.guard, loss=replace(atr.guard.loss, sl=hard)), last_E=entered)
        return cls(old, atr, MatureState(), price)

    @classmethod
    def restore(cls, payload):
        data = json.loads(payload)
        need(data.pop('schema') == SCHEMA, 'C11_HOLDING_SCHEMA')
        data['old'] = State(**data['old'])
        spec = data['atr']
        guard = spec['guard']
        guard['loss'] = Loss(**guard['loss'])
        spec['guard'] = Guard(**guard)
        data['atr'] = ATRState(**spec)
        data['mature'] = MatureState(**data['mature'])
        return cls(**data)

    def pack(self):
        return json.dumps(dict(schema=SCHEMA, **asdict(self)), sort_keys=True, allow_nan=False)

    def queue(self):
        return queued_exit(weak=self.weak_pending, mature=self.mature.queued,
                           early2=self.early2_pending, lowerhigh=self.mature.lowerhigh)

    def floor(self):
        return max(self.old.hard_stop, self.old.passed_stage,
                   self.atr.published if self.atr.guard.loss.author_armed else 0.)


@dataclass(frozen=True)
class CausalExitBasis:
    trade_id: int
    pair: str
    entry_ms: int
    active: int
    state: Holding
    previous_close: float | None
    prior_highs: tuple


def contour_context(store, symbol, active, now):
    last = store.range(symbol, MINUTE, active-MINUTE, active, now)
    quarter = active//QUARTER*QUARTER
    highs = store.range(symbol, MINUTE, quarter-30*MINUTE, quarter, now)
    return (last[-1].close if last else None,
            tuple((r.open_ms+MINUTE,r.high) for r in highs))


def contour_choice(state, active, previous, highs, opening, observed, original):
    if previous is None or state.old.pending or state.atr.pending:
        return None
    return contour_exit(end=active+MINUTE, state=state.mature, queued=bool(state.queue()),
                        previous_close=previous, previous_highs=highs, older_floor=state.floor(),
                        opening=opening, observed=observed, original=original)


def stop_reason(state, gap):
    active_atr = state.atr.published if state.atr.guard.loss.author_armed else 0.
    return ((ATR_GAP if gap else ATR_TOUCH) if active_atr>=state.old.passed_stage
            and active_atr>state.old.hard_stop else (HOUR_GAP if gap else HOUR_TOUCH))


def live_choice(basis, opening, bid):
    """Compare real executable prices against preceding publications only."""
    state = basis.state
    floor = state.floor()
    if (state.blocked or state.old.pending or state.atr.pending
            or opening <= state.old.hard_stop or bid <= state.old.hard_stop):
        return None
    if opening<=floor:
        return (stop_reason(state,True),opening) if bid==opening else None
    queued=state.queue()
    if queued:
        return (queued,opening) if bid==opening else None
    original=None
    if bid<=floor:
        original=dict(why=stop_reason(state,False),clock='INTRABAR_IDEAL',price=floor)
    if (state.mature.line>floor and bid<=state.mature.line
            and (basis.previous_close is None or len(basis.prior_highs)!=30)
            and (original is None or original['why']==ATR_TOUCH)):
        return None
    contour=contour_choice(state,basis.active,basis.previous_close,basis.prior_highs,opening,bid,original)
    if contour is not None:
        return contour if contour[1]==bid else None
    if original is not None and bid==floor:
        return original['why'],floor
    return None


def advance_close(state, bar, *, hour, feature, tick, buffer, supplemental, weak,
                  early2, mature_grid, past_contour):
    """Compare the archived hold sequence without treating prior touches as fills."""
    E=bar.E
    if state.blocked:
        return state
    need(E==state.old.last_E+MINUTE and state.atr.last_E==E-MINUTE,
         'C11_HOLDING_NONCONTIGUOUS')
    if bar.open<=buffer:
        return replace(state,blocked='UNSETTLED_LIQUIDATION_BUFFER')
    old,act=transport('MID_4_6_08_16','INTRA_WEAK',state.old,bar,
        published=Published(original_open_exit=state.queue()),atr=state.atr,hour=hour,original=None)
    orig=None if old.held else dict(why=act['reason'],clock=act['fill_at'],price=act['price'])
    previous,highs=past_contour
    if state.mature.line and not state.queue() and (
            previous is None or (bar.low<=state.mature.line and len(highs)!=30)):
        return None
    contour=contour_choice(state,E-MINUTE,previous,highs,bar.open,bar.low,orig)
    if contour or orig:
        return replace(state,blocked='UNSETTLED_'+(contour[0] if contour else orig['why']))
    if feature is None:
        return None
    atr,_=close_update(state.atr,E=E,close=bar.close,high=bar.high,atr15=feature['loss_atr15'],
        atr5=feature['guard_atr5'],mult=feature['guard_multiplier'],er=feature['signed_er'],
        previous_er=feature['previous_signed_er'],tick=tick,boundary=buffer,
        source15=feature['source_at15'],source5=feature['source_at5'],source_er=feature['source_at_er'],known=True)
    if atr.pending:
        return replace(state,blocked='UNSETTLED_ORIGINAL_ATR')
    peak=max(state.peak_close,bar.close)
    e2=early2(peak,atr.guard.loss.author_armed)
    if e2 is not None and e2.startswith('UNKNOWN'):
        return None
    pending=e2=='SCHEDULE_NEXT_OPEN'
    clocks,prices=mature_grid
    m=advance_mature(state.mature,E,entry=old.entry,close=bar.close,peak=atr.peak,
                     atr_armed=atr.guard.loss.author_armed,original_pending=atr.pending,
                     previous_gate=state.old.gate,previous_stage=state.old.passed_stage,
                     tick=tick,clock=clocks,prices=prices)
    if m is None:
        return None
    mature,_=m
    status=weak()
    if status=='NOT_SCHEDULED':
        run,weak_pending=state.weak_run,state.weak_pending
    elif status in WEAK_STATUSES:
        run=state.weak_run+1 if status=='KNOWN_TRUE_QUOTE' else 0
        weak_pending=run>=2 or (supplemental and should_exit(old.entry,peak,atr.guard.loss.author_armed,
                    run,run>=2,pending or mature.lowerhigh or old.pending))
    else:
        return None
    old=replace(old,passed_stage=ceil_tick(old.passed_stage,tick) if old.passed_stage else 0.,
                failed_floor=ceil_tick(old.failed_floor,tick) if old.failed_floor else 0.)
    return Holding(old,atr,mature,peak,run,weak_pending,pending)


class NativeHoldingEvidence:
    def __init__(self, weak_reader):
        self.weak_reader=weak_reader

    def _weak(self,bot,trade,entry,E,deadline):
        return self.weak_reader(bot,trade.pair,trade.open_rate,entry,E,as_of=deadline)

    def _step(self,bot,trade,symbol,state,row,tick,buffer,supplemental,now):
        E=row.open_ms+MINUTE
        # Every input must have been available during the original decision
        # minute. A restarted process may verify a stored prefix but never
        # backdate a new public read or turn a closed-bar touch into a fill.
        deadline=min(now,E+MINUTE-1)
        if row.received_ms>deadline:
            return None
        store=bot.policy.bars
        feature=bot.policy.facts.features.read(bot,trade.pair,E,allow_fetch=False)
        if feature is not None and feature['observed_at_ms']>deadline:
            feature=None
        finish=E//HOUR*HOUR
        hours=store.range(symbol,HOUR,finish-2*HOUR,finish,deadline)
        hour=dict(end_E=finish,available_E=finish,last=hours[-1].quote,previous=hours[-2].quote) if hours else {
            'end_E':None,'available_E':None,'last':None,'previous':None}
        minutes=store.range(symbol,MINUTE,E-30*MINUTE,E,deadline)
        grid=(tuple(r.open_ms+MINUTE for r in minutes),tuple((r.op,r.high,r.low,r.close) for r in minutes))
        def early2(peak,armed):
            events=store.range(symbol,FIVE,E-30*MINUTE,E,deadline) if E%QUARTER==0 else ()
            closes={r.open_ms+FIVE:r.quote for r in events}
            stamps=np.arange(E-29*MINUTE,E+MINUTE,MINUTE)
            px=np.asarray(grid[1]) if minutes else np.empty((0,4))
            values=np.asarray([closes.get(x,np.nan) for x in stamps])
            ev=evaluate(E,state.old.entry_E,state.old.entry,peak,row.close,armed,px,
                        (values,values,stamps),len(px)-1)
            return ev[0] if ev is not None else None
        past=contour_context(store,symbol,E-MINUTE,deadline)
        return advance_close(state,Bar(E,row.op,row.high,row.low,row.close),hour=hour,feature=feature,
            tick=tick,buffer=buffer,supplemental=supplemental,weak=lambda:self._weak(
                bot,trade,state.old.entry_E,E,deadline),early2=early2,mature_grid=grid,past_contour=past)

    def _refresh_current(self,bot,symbol,active,now):
        store=bot.policy.bars
        for width in (MINUTE,FIVE,QUARTER,HOUR):
            if active%width==0 and not store.range(symbol,width,active-width,active,now):
                bot.policy.candles.query(bot,symbol,width,active-width,active,1,contiguous=True)
                now=wall_ms()
                if now>=active+MINUTE:
                    break
        return now

    def read(self,bot,trade,active):
        from c11_native_rules import archived_buffered_liquidation
        facts=bot.policy.facts
        campaign=facts.campaign_entry(trade)
        if campaign is None or active<campaign[0]:
            return None
        entered,qty=campaign
        if not bot.entry_journal.confirmed_holding(trade,entered,qty,wall_ms()):
            return None
        store=bot.policy.bars
        symbol=bot.exchange.markets[trade.pair]['id']
        meta,tiers=facts.market_meta(bot.exchange,trade.pair)
        hard=trade.initial_stop_loss
        buffer=archived_buffered_liquidation(SimpleNamespace(price=trade.open_rate,tiers=tiers,meta=meta),
                                             qty,qty*trade.open_rate/5)
        if not 0<buffer<hard<trade.open_rate:
            return None
        supp=bot.entry_journal.supplement_episode(trade)
        reference=[o.order_id for o in trade.orders if o.ft_order_side=='buy']
        if len(reference)!=1:
            return None
        binding=sha256(json.dumps([trade.id,trade.pair,reference[0],entered,qty,
            trade.open_rate,hard,buffer,meta['tick'],supp],allow_nan=False).encode()).hexdigest()
        db=store.db
        with db:
            db.execute('CREATE TABLE IF NOT EXISTS c11_holding '
                       '(trade_id INTEGER PRIMARY KEY,binding TEXT NOT NULL,payload TEXT NOT NULL)')
        saved=db.execute('SELECT binding,payload FROM c11_holding WHERE trade_id=?',(trade.id,)).fetchone()
        if saved is not None and saved[0]!=binding:
            return None
        state=Holding.restore(saved[1]) if saved else Holding.start(entered,trade.open_rate,hard)
        if state.blocked or state.old.last_E>active:
            return None
        now=wall_ms()
        if state.old.last_E<active:
            first=state.old.last_E
            # Only fill the new current minute using guarded native data. An
            # older unsampled gap stays blocked even if the API can backfill it.
            now=self._refresh_current(bot,symbol,active,now)
            count=min(30,(active-first)//MINUTE)
            rows=store.range(symbol,MINUTE,first,first+count*MINUTE,now)
            for row in rows:
                if row.open_ms==entered and row.op!=trade.open_rate:
                    return None
                next_state=self._step(bot,trade,symbol,state,row,meta['tick'],buffer,supp is not None,now)
                if next_state is None:
                    break
                state=next_state
                if state.blocked:
                    break
            if state.old.last_E!=first or state.blocked:
                with db:
                    db.execute('INSERT INTO c11_holding VALUES (?,?,?) ON CONFLICT(trade_id) '
                               'DO UPDATE SET payload=excluded.payload WHERE c11_holding.binding=excluded.binding',
                               (trade.id,binding,state.pack()))
        if state.blocked or state.old.last_E!=active or wall_ms()>=active+MINUTE:
            return None
        previous,highs=contour_context(store,symbol,active,wall_ms())
        return CausalExitBasis(trade.id,trade.pair,entered,active,state,previous,highs)
