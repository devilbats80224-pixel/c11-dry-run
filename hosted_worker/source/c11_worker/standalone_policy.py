"""C11 native paper policy. The archived decisions remain the only order rules."""
from decimal import Decimal
import math
import sqlite3
import time
from types import SimpleNamespace

import numpy as np
from ccxt import BaseError
from freqtrade.persistence import Trade

from bridge_protocol import Refused, need
from campaign_journal import CampaignJournal
from causal_bars import CausalBars, FIVE, HOUR, MINUTE, QUARTER
from c11_native_rules import EntryFacts, PositionFacts
from native_candles import NativeCandles
from feature_warmup import readiness
from primary_coverage_gate import native_primary_read
from structure40 import use as structure40
from runtime_facts import EntryClock, ExitClock, NativeEvidence

INPUT_ERRORS = (Refused, BaseError, OSError, ArithmeticError, AssertionError, AttributeError, KeyError, TypeError, ValueError)


def market_meta(exchange, pair):
    market = exchange.markets[pair]
    need(market['symbol'] == pair and market['active'] is True and market['linear'] is True
         and market['swap'] is True and market['settle'] == market['quote'] == 'USDT'
         and market['contractSize'] == 1, 'C11_NATIVE_MARKET_CONTRACT')
    filters = {}
    for row in market['info']['filters']:
        key = row['filterType']
        need(key not in filters, 'C11_NATIVE_MARKET_FILTER_DUPLICATE')
        filters[key] = row
    lot = filters['LOT_SIZE']
    mlot = filters['MARKET_LOT_SIZE']
    need(Decimal(lot['stepSize']) == Decimal(mlot['stepSize']), 'C11_NATIVE_LOT_STEP_DRIFT')
    min_qty = max(float(lot['minQty']), float(mlot['minQty']))
    max_qty = min(float(lot['maxQty']), float(mlot['maxQty']))
    out = dict(lot=float(lot['stepSize']), tick=float(filters['PRICE_FILTER']['tickSize']),
               minqty=min_qty, maxqty=max_qty, minnotional=float(filters['MIN_NOTIONAL']['notional']),
               maxcost=float(market['limits']['cost']['max'] or 'inf'))
    need(all(math.isfinite(out[k]) and out[k] > 0 for k in ('lot', 'tick', 'minqty', 'maxqty', 'minnotional'))
         and min_qty <= max_qty and out['maxcost'] > 0, 'C11_NATIVE_MARKET_FILTER')
    tiers = tuple(exchange._leverage_tiers[pair])
    return out, tiers


def archived_half(store, symbol, end, as_of, close):
    last = end // QUARTER * QUARTER
    rows = store.range(symbol, FIVE, last-9*FIVE, last, as_of)
    if not rows or any(row.quote <= 0 for row in rows):
        return None
    quarters = store.range(symbol, QUARTER, last-3*QUARTER, last, as_of)
    closes = tuple(rows[i].close for i in (2, 5, 8))
    if not quarters or closes != tuple(row.close for row in quarters):
        return None
    a, b, c = closes
    x, y, z = np.asarray([row.quote for row in rows], float).reshape(3, 3).sum(axis=1)
    return bool(a < b < c and x > y > z and close >= c)


class StandalonePolicy:
    def __init__(self, root, *, facts=None):
        self.slots = CampaignJournal(root / 'intents.sqlite')
        self.bars = CausalBars(root / 'causal-candles.sqlite')
        self.candles = NativeCandles(self.bars)
        self.facts = NativeEvidence(self, market_meta) if facts is None else facts

    def reconcile_restart(self, bot):
        need(bot.config['dry_run'] is True and bot.native_paper_exchange(), 'C11_RESTART_PAPER_BOUNDARY')
        # Existing native protection remains available when admission freezes.
        return self.slots.reconcile(Trade.get_trades().all())

    def primary_inputs(self, bot, pair, candidate):
        now = time.time_ns() // 1_000_000
        end = now // QUARTER * QUARTER
        if now >= end + MINUTE:
            return None
        try:
            primary,state=native_primary_read(getattr(bot,'_c11_native_primary_handoff',None),
                                            bot,self,pair,candidate,end,INPUT_ERRORS,readiness)
            if primary is None or state is None:
                return None
            now=time.time_ns()//1_000_000
            if now>=end+MINUTE or not self.slots.ready:
                return None
            features=self.bars.inputs(candidate.symbol,end,now,primary=primary,state=state)
            if features:
                self.candles.update_entry_hour(bot,candidate.symbol,end)
            return features if time.time_ns()//1_000_000<end+MINUTE else None
        except INPUT_ERRORS+(sqlite3.Error,):
            return None

    def entry_facts(self, bot, pair, features):
        scope = features['scope']
        end, symbol = scope.end_ms, scope.symbol
        now = time.time_ns() // 1_000_000
        if not self.slots.ready:
            return None
        try:
            clock = self.facts.entry_clock(bot, pair, end)
            if (type(clock) is not EntryClock or (clock.pair, clock.end_ms) != (pair, end)
                    or not end <= now < min(features['valid_until_ms'], clock.valid_until_ms)
                    or not math.isfinite(clock.observed_open) or clock.observed_open <= 0
                    or clock.atr_entry_source_known is not True):
                return None
            before = (end-1)//QUARTER*QUARTER
            structure = self.bars.range(symbol, MINUTE, before-QUARTER, end, now)
            m5 = self.bars.range(symbol, FIVE, end-FIVE, end, now)
            hour = end//HOUR*HOUR
            h1 = self.bars.range(symbol, HOUR, hour-HOUR, hour, now)
            if not structure or not m5 or not h1:
                return None
            close = structure[-1].close
            # Archived entry uses the same-E close as the basis for STRUCT40.
            # A native next-open price drift is refused by the paper runner.
            rule, _ = structure40(end, len(structure)-1, np.asarray([b.open_ms+MINUTE for b in structure]),
                                  np.asarray([(b.op,b.high,b.low,b.close) for b in structure]), close)
            half = archived_half(self.bars, symbol, end, now, close)
            if rule == 'UNKNOWN_RANGE' or half is None:
                return None
            meta, tiers = market_meta(bot.exchange, pair)
            trades = tuple(Trade.get_open_trades())
            positions = []
            for trade in trades:
                state = self.facts.position_state(bot, trade, end)
                if type(state) is not PositionFacts or not state.valid() or (
                    state.pair, state.quantity, state.entry, state.initial_stop, state.funding) != (
                    trade.pair, trade.amount, trade.open_rate, trade.initial_stop_loss, trade.funding_fees):
                    return None
                positions.append(state)
            bot._entry_open_clock = clock
            wallet = bot.wallets.get_total_stake_amount()
            free = bot.wallets.get_available_stake_amount()
            return EntryFacts(pair, symbol, end, clock.observed_open, wallet, free, m5[0].quote,
                              h1[0].quote, meta, tiers, tuple(positions), rule == 'STRUCT40', half, True)
        except INPUT_ERRORS:
            return None

    def partial_facts(self, bot, trade):
        if not bot.native_paper_exchange():
            return None
        try:
            clock = self.facts.exit_clock(bot, trade)
            now = time.time_ns() // 1_000_000
            if (type(clock) is not ExitClock or clock.trade_id != trade.id
                    or not clock.entered_ms <= int(trade.open_date_utc.timestamp()*1000) < clock.entered_ms+MINUTE
                    or not clock.decided_ms <= now < min(clock.valid_until_ms, clock.decided_ms + MINUTE)
                    or clock.senior_clear is not True or clock.stress is not False
                    or not math.isfinite(clock.observed_open) or clock.observed_open <= 0):
                return None
            symbol = bot.exchange.markets[trade.pair]['id']
            bars = self.bars.range(symbol, MINUTE, clock.decided_ms-10*MINUTE, clock.decided_ms, now)
            if not bars:
                self.candles.query(bot, symbol, MINUTE, clock.decided_ms-10*MINUTE, clock.decided_ms, 10)
                now = time.time_ns() // 1_000_000
                bars = self.bars.range(symbol, MINUTE, clock.decided_ms-10*MINUTE, clock.decided_ms, now)
            if not bars or now >= min(clock.valid_until_ms, clock.decided_ms + MINUTE):
                return None
            meta, _ = market_meta(bot.exchange, trade.pair)
            return SimpleNamespace(
                campaign=bot.campaign(trade), entered=clock.entered_ms, decided=clock.decided_ms,
                open_end=clock.decided_ms + MINUTE, observed_open=clock.observed_open,
                side_quantity=clock.side_quantity, deadline_ms=clock.valid_until_ms, buffer=clock.buffer,
                lot=meta['lot'], tick=meta['tick'], min_qty=meta['minqty'], min_notional=meta['minnotional'],
                senior_clear=True, stress=False,
                rows=tuple((b.open_ms+MINUTE,b.op,b.high,b.low,b.close) for b in bars),
            )
        except INPUT_ERRORS:
            return None

    def partial_units(self, bot, trade, stake):
        from freqtrade.util import FtPrecise
        amount = abs(float(FtPrecise(stake) * FtPrecise(trade.amount) / FtPrecise(trade.stake_amount)))
        return bot.exchange.amount_to_contract_precision(trade.pair, amount)


def build_policy(root):
    return StandalonePolicy(root)


def require_release_facts():
    # Native early protection is implemented. Full C11 has independent
    # supplemental, maturity and post-early state routes still unreleased.
    raise Refused('C11_ARCHIVED_FULL_POLICY_ROUTES_UNRELEASED')
