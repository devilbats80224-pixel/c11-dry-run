"""Archived C11 calculations at the native paper pre-order boundary."""
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP
from math import isfinite
from types import SimpleNamespace

from c11_exact_sizing import fixed5_plan
from risk_cap import tune

FEE = .0005


@dataclass(frozen=True)
class PositionFacts:
    pair: str
    quantity: float
    entry: float
    mark: float
    initial_stop: float
    passed_stage: float
    atr_published: float
    atr_armed: bool
    funding: float

    def stop(self):
        floors = (self.passed_stage, self.atr_published if self.atr_armed else 0.)
        return max((self.initial_stop, *(p for p in floors if 0 < p < self.mark)))

    def mark_pnl(self):
        q, entry, price = self.quantity, self.entry, self.mark
        return round(q*(price-entry)-q*(price+entry)*FEE+self.funding, 8)

    def floor_pnl(self):
        q, entry, stop = self.quantity, self.entry, self.stop()
        return round(q*(stop-entry)-FEE*q*(stop+entry)+self.funding, 8)

    def valid(self):
        return (type(self.atr_armed) is bool and all(isfinite(x) for x in
                (self.quantity, self.entry, self.mark, self.initial_stop, self.passed_stage,
                 self.atr_published, self.funding))
                and min(self.quantity, self.entry, self.mark, self.initial_stop) > 0
                and min(self.passed_stage, self.atr_published) >= 0)


@dataclass(frozen=True)
class EntryFacts:
    pair: str
    symbol: str
    decision_ms: int
    price: float
    wallet: float
    free: float
    q5: float
    q1h: float
    meta: dict
    tiers: tuple
    incumbent: tuple[PositionFacts, ...]
    structure40: bool
    reduce_to_half: bool
    atr_entry_source_known: bool


@dataclass(frozen=True)
class EntryPlan:
    pair: str
    decision_ms: int
    price: float
    quantity: float
    stake: float
    stop: float
    cap_events: tuple


def ceil_tick(value, tick):
    step = Decimal(str(tick))
    return float((Decimal(str(value)) / step).to_integral_value(rounding=ROUND_CEILING) * step)


def paper_entry(f, *, slots, buffered_liquidation):
    """Apply archived rules in order before any Freqtrade order request."""
    if (type(f) is not EntryFacts or type(f.decision_ms) is not int or f.decision_ms <= 0
            or len(f.incumbent) >= 3 or len({p.pair for p in f.incumbent}) != len(f.incumbent)
            or any(type(p) is not PositionFacts or not p.valid() or p.pair == f.pair for p in f.incumbent)
            or not all(type(t) is bool for t in (f.structure40, f.reduce_to_half, f.atr_entry_source_known))
            or not f.atr_entry_source_known):
        return None
    if len(f.incumbent) == 2 and all(p.mark_pnl() < 0 for p in f.incumbent):
        return None
    if not slots.allow(f.decision_ms, f.symbol, len(f.incumbent)):
        return None
    inputs = dict(q5=f.q5, q1h=f.q1h, meta=f.meta, tiers=f.tiers)
    plan = fixed5_plan(f.pair, f.wallet, f.free, f.price, **inputs)
    protected40 = f.structure40 and bool(f.incumbent) and plan['quantity'] > 0
    protected40 = protected40 and all(p.floor_pnl() >= 0 for p in f.incumbent)
    wallet = f.wallet * 1.2 if protected40 else f.wallet
    if protected40:
        original = plan['quantity']
        plan = fixed5_plan(f.pair, wallet, f.free, f.price, **inputs)
        if plan['quantity'] < original:
            return None
    if f.reduce_to_half and plan['quantity'] > 0:
        old_plan = plan
        plan = fixed5_plan(f.pair, wallet, min(f.free, old_plan['margin'] / 2), f.price, **inputs)
        if plan['quantity'] > old_plan['quantity'] / 2 + 1e-12 or plan['margin'] > old_plan['margin'] / 2 + 1e-12:
            return None

    def risk(_book, _end):
        nav, loss = f.wallet, 0.
        for p in f.incumbent:
            q, op, mark, stop = p.quantity, p.entry, p.mark, p.stop()
            nav += round(q*(mark-op)-FEE*q*(mark+op)+p.funding, 8)
            loss += max(0., (mark-stop)*(1-FEE))*q
        return nav, loss

    policy = SimpleNamespace(risk=risk, cut=ceil_tick, names=(f.symbol,), events={})
    book = SimpleNamespace(policy=policy, pos=f.incumbent)
    plan = tune(book, 0, f.decision_ms, f.price, plan, fixed5_plan, f.pair,
                f.wallet, f.free, protected40, inputs, FEE)
    quantity = plan['quantity']
    if quantity <= 0 or quantity * f.price / 5 > f.free:
        return None
    hard_floor = ceil_tick(f.price * .94, f.meta['tick'])
    buffer = buffered_liquidation(f, quantity, plan['margin'])
    if not isfinite(buffer) or not 0 < buffer < hard_floor < f.price:
        return None
    return EntryPlan(f.pair, f.decision_ms, f.price, quantity, plan['margin'],
                     hard_floor, tuple(policy.events.get('book_risk_cap', ())))


def archived_buffered_liquidation(facts, qty, stake):
    """Pinned archived isolated-long buffer formula with explicit tier inputs."""
    tier = next((t for t in reversed(facts.tiers) if stake >= t['minNotional']), None)
    if tier is None or tier['info'].get('cum') is None:
        raise ValueError('MAINTENANCE_TIER_UNKNOWN')
    mm = tier['maintenanceMarginRate']
    amt = tier['info']['cum']
    entry = facts.price
    raw = ((stake + 0. + amt) - (qty * entry)) / ((qty * mm) - qty)
    value = max(raw + abs(entry - raw) * .05, 0.)
    grid = Decimal(str(facts.meta['tick']))
    return float((Decimal(str(value))/grid).to_integral_value(rounding=ROUND_HALF_UP)*grid)
