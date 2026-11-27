"""Archived-route dispatch behind trusted evidence and the native paper boundary."""
from dataclasses import dataclass
from decimal import Decimal
import math
import time

from freqtrade.enums import ExitCheckTuple, ExitType
from freqtrade.freqtradebot import FreqtradeBot
from freqtrade.persistence import PairLocks, Trade

from archived_routes import (
    EXIT_REASONS,
    contour_exit, firstproof_clock, queued_exit, replay_mature, sweep_offer,
)
from causal_bars import MINUTE
from causal_exit_basis import CausalExitBasis, live_choice
from native_protection import BOOK_AGE_MS, next_open, wall_ms


class UnavailableRouteEvidence:
    """No supported native/managed producer currently supplies these inputs."""

    def sweep(self, bot, pair, now_ms):
        return None

    def exit(self, bot, trade, current_bar):
        return None


@dataclass(frozen=True)
class ExitBasis:
    trade_id: int
    pair: str
    entry_ms: int
    current_bar: int
    senior_state_known: bool
    senior_route_clear: bool
    firstproof: object
    mature_closes: tuple
    early2_pending: bool
    previous_close: float
    previous_highs: tuple
    older_floor: float
    opening: float
    original: dict | None


@dataclass(frozen=True)
class ExitOrder:
    reason: str
    price: float
    quantity: float
    deadline_ms: int
    bar: int


def book_bid(api, symbol, active):
    body = api.fapiPublicGetTickerBookTicker({'symbol':symbol})
    stamp, now = body['time'], wall_ms()
    if body['symbol'] != symbol or type(stamp) is not int or not active <= stamp <= now:
        return None
    deadline = min(stamp+BOOK_AGE_MS,active+MINUTE)
    p,q = Decimal(body['bidPrice']),Decimal(body['bidQty'])
    if not (now < deadline and p.is_finite() and q.is_finite() and min(p,q)>0
            and math.isfinite(float(p)) and math.isfinite(float(q))):
        return None
    return float(p),float(q),deadline


def ordered_entry_pairs(bot):
    markets = bot.exchange.markets
    eligible = ((markets[pair]['id'],pair) for pair in bot.active_pair_whitelist
                if pair in markets and isinstance(markets[pair].get('id'),str))
    return [pair for _,pair in sorted(eligible)]


class ArchivedRoutes:
    def __init__(self, evidence=None):
        self.evidence = UnavailableRouteEvidence() if evidence is None else evidence

    def supplement(self, bot, slot_count):
        if not (slot_count>0 and bot.native_paper_exchange() and bot.entry_journal.ready
                and not PairLocks.is_global_lock(side='*') and not Trade.get_open_trades()):
            return 0
        # The archive groups original offers first and sweeps in symbol order.
        # The inherited source rank is still retained by the feed.
        for pair in ordered_entry_pairs(bot):
            with bot._exit_lock:
                if Trade.get_open_trades() or bot.strategy.is_pair_locked(pair,side='long'):
                    continue
                now = wall_ms()
                basis = self.evidence.sweep(bot,pair,now)
                offer = sweep_offer(basis)
                if offer is None or (offer.symbol, pair) != (bot.exchange.markets[pair]['id'],
                                                               f'{offer.symbol[:-4]}/USDT:USDT'):
                    continue
                bot._sweep_offer = offer
                journal = bot.entry_journal
                journal.pending_sweep = offer
                try:
                    amount = bot.wallets.get_trade_stake_amount(pair,bot.config['max_open_trades'])
                    if bot.execute_entry(pair,amount,enter_tag='c11_supplement'):
                        return 1
                finally:
                    journal.pending_sweep = None
                    bot._sweep_offer = None
        return 0

    def exit_order(self, bot, trade):
        now = wall_ms()
        active = now//MINUTE*MINUTE
        campaign = bot.policy.facts.campaign_entry(trade)
        if campaign is None:
            return None
        basis = self.evidence.exit(bot,trade,active)
        if type(basis) is CausalExitBasis:
            return self.causal_order(bot,trade,campaign[0],active,basis)
        if (type(basis) is not ExitBasis or (basis.trade_id,basis.pair,basis.entry_ms,basis.current_bar)
                != (trade.id,trade.pair,campaign[0],active) or basis.senior_state_known is not True
                or basis.senior_route_clear is not True
                or type(basis.early2_pending) is not bool or not math.isfinite(basis.older_floor)
                or basis.older_floor < trade.initial_stop_loss):
            return None
        meta, _ = bot.policy.facts.market_meta(bot.exchange,trade.pair)
        mature = replay_mature(trade.open_rate,campaign[0],active,meta['tick'],basis.mature_closes)
        if mature is None:
            return None
        if basis.firstproof is not None:
            if (bot.entry_journal.supplement_episode(trade) is None
                    or (basis.firstproof.entered,basis.firstproof.entry)!=(campaign[0],trade.open_rate)):
                return None
            weak = firstproof_clock(basis.firstproof) == active
            if not weak:
                return None
        else:
            weak = False
        reason = queued_exit(weak=weak,mature=mature.queued,early2=basis.early2_pending,
                             lowerhigh=mature.lowerhigh)
        if reason and reason not in EXIT_REASONS:
            return None
        symbol = bot.exchange.markets[trade.pair]['id']
        api = bot.exchange._api
        if reason:
            quote = next_open(api,symbol,active,'sell')
            if quote is None:
                return None
            price,size,deadline = quote.price,quote.side_quantity,quote.expires_ms
        else:
            if not mature.line:
                return None
            opening = api.fapiPublicGetKlines(dict(symbol=symbol,interval='1m',startTime=active,
                                                   endTime=active+MINUTE-1,limit=1))
            if (len(opening)!=1 or int(opening[0][0])!=active
                    or int(opening[0][6])!=active+MINUTE-1 or int(opening[0][8])<=0
                    or float(opening[0][1])!=basis.opening):
                return None
            book = book_bid(api,symbol,active)
            if book is None:
                return None
            price,size,deadline = book
            route = contour_exit(end=active+MINUTE,state=mature,queued=False,
                previous_close=basis.previous_close,previous_highs=basis.previous_highs,older_floor=basis.older_floor,
                opening=basis.opening,observed=price,original=basis.original)
            if route is None or route[1]!=price:
                return None
            reason = route[0]
        if (price <= basis.older_floor or size < trade.amount or wall_ms()>=deadline
                or type(trade.amount) not in (int,float) or not math.isfinite(trade.amount) or trade.amount<=0):
            return None
        return ExitOrder(reason,price,trade.amount,deadline,active)

    def causal_order(self,bot,trade,entered,active,basis):
        if ((basis.trade_id,basis.pair,basis.entry_ms,basis.active)!=(trade.id,trade.pair,entered,active)
                or basis.state.old.last_E!=active or basis.state.blocked
                or wall_ms()>=active+MINUTE):
            return None
        state=basis.state
        if not (state.queue() or state.floor()>state.old.hard_stop or state.mature.line):
            return None
        symbol=bot.exchange.markets[trade.pair]['id']
        api=bot.exchange._api
        opening=api.fapiPublicGetKlines(dict(symbol=symbol,interval='1m',startTime=active,
                                            endTime=active+MINUTE-1,limit=1))
        if (len(opening)!=1 or int(opening[0][0])!=active
                or int(opening[0][6])!=active+MINUTE-1 or int(opening[0][8])<=0):
            return None
        price_open=float(opening[0][1])
        if not math.isfinite(price_open) or price_open<=0:
            return None
        book=book_bid(api,symbol,active)
        if book is None:
            return None
        price,size,deadline=book
        choice=live_choice(basis,price_open,price)
        if (choice is None or choice[0] not in EXIT_REASONS or choice[1]!=price
                or not math.isfinite(trade.amount) or not 0<trade.amount<=size or wall_ms()>=deadline):
            return None
        return ExitOrder(choice[0],price,trade.amount,deadline,active)

    def after_native_protection(self, bot, trade):
        journal = bot.entry_journal
        if (not bot.native_paper_exchange() or not trade.is_open or trade.is_short
                or trade.has_open_orders or not journal.owns(trade)
                or not bot.hard_guard.trim_can_resume(bot.campaign(trade))
                or journal.exit_reserved(trade.id)):
            return False
        choice = self.exit_order(bot,trade)
        if choice is None:
            return False
        rate = bot.exchange.get_rate(trade.pair,side='exit',is_short=False,refresh=True)
        if (rate!=choice.price or rate<=max(trade.initial_stop_loss or 0.,trade.stop_loss or 0.)
                or wall_ms()>=choice.deadline_ms):
            return False
        if not journal.reserve_route_exit(trade.id,trade.pair,choice.bar,choice.reason,choice.quantity,choice.price):
            return False
        # Once reserved, an uncertain call is never replayed. A subsequently
        # verified hard stop remains available through the inherited handler.
        prior = {o.order_id for o in trade.orders}
        done = FreqtradeBot.execute_trade_exit(bot,trade,rate,ExitCheckTuple(ExitType.CUSTOM_EXIT,choice.reason))
        if done:
            orders = [o for o in trade.orders if o.order_id not in prior
                      and o.ft_order_side=='sell' and o.order_id.startswith('dry_run_sell_')
                      and o.safe_amount==choice.quantity]
            if len(orders)==1:
                journal.acknowledge_route_exit(trade.id,orders[0].order_id)
        return done
