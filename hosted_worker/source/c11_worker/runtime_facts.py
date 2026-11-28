"""Bind exact supported C11 clocks to the native paper campaign."""
from dataclasses import dataclass
from types import SimpleNamespace

from causal_bars import HOUR, MINUTE
from causal_exit_basis import CausalExitBasis
from live_signal_evidence import owned_priority
from entry_verdict_receipt import unavailable_order_fact
from c11_native_rules import PositionFacts, archived_buffered_liquidation
from native_protection import FeatureSpan, archived_half_window, next_open, replay_early, wall_ms


@dataclass(frozen=True)
class EntryClock:
    pair: str
    end_ms: int
    observed_open: float
    side_quantity: float
    valid_until_ms: int
    atr_entry_source_known: bool


@dataclass(frozen=True)
class ExitClock:
    trade_id: int
    entered_ms: int
    decided_ms: int
    observed_open: float
    side_quantity: float
    valid_until_ms: int
    buffer: float
    senior_clear: bool
    stress: bool


class NativeEvidence:
    def __init__(self, policy, market_meta):
        self.policy, self.market_meta = policy, market_meta
        self.features = FeatureSpan(policy.bars, policy.candles)

    def entry_clock(self, bot, pair, end_ms):
        if not bot.native_paper_exchange():
            return unavailable_order_fact('ENTRY_FACTS_NOT_PAPER')
        from primary_coverage_gate import NativePrimaryHandoff
        view=getattr(bot,'_c11_native_primary_handoff',None)
        if type(view) is NativePrimaryHandoff and view.scoped_atr is not None:
            if (getattr(bot,'_owned_revisit_no_positions',False) is not True
                    or getattr(bot,'_owned_revisit_reconciled',False) is not True):
                return unavailable_order_fact('ENTRY_FACTS_ATR_SCOPE_UNAVAILABLE')
            feature=view.entry_atr(pair,view.scoped_candidate,end_ms)
        else:
            feature = self.features.read(bot, pair, end_ms)
        if feature is None:
            return unavailable_order_fact('ENTRY_FACTS_ATR_UNAVAILABLE')
        symbol = bot.exchange.markets[pair]['id']
        close = self.policy.bars.range(symbol, MINUTE, end_ms-MINUTE, end_ms, wall_ms())
        if not close:
            return unavailable_order_fact('ENTRY_FACTS_SIGNAL_CLOSE_UNAVAILABLE')
        quote = next_open(bot.exchange._api, symbol, end_ms, 'buy')
        # The frozen C11 entry price is the signal-bar close. A next-open that
        # differs cannot stand in for that exact archived price.
        if quote is None:
            return unavailable_order_fact('ENTRY_FACTS_CLOCK_UNAVAILABLE')
        if quote.price != close[-1].close:
            return unavailable_order_fact('ENTRY_FACTS_CLOSED_OPEN_PRICE_MISMATCH')
        if feature['observed_at_ms'] > quote.received_ms:
            return unavailable_order_fact('ENTRY_FACTS_ATR_AFTER_QUOTE')
        return EntryClock(pair, end_ms, quote.price, quote.side_quantity, quote.expires_ms, True)

    def campaign_entry(self, trade):
        rows = self.policy.slots.db.execute(
            "SELECT c.entry_ms,i.qty,i.ref FROM c11_slot_claim c JOIN intents i "
            "ON c.pair=i.pair AND c.entry_ms=i.end_ms WHERE c.pair=? "
            "AND c.close_ms IS NULL AND i.kind='entry' AND i.ref IS NOT NULL", (trade.pair,),
        ).fetchall()
        if len(rows) != 1:
            return None
        end, qty, ref = rows[0]
        if not self.policy.slots.native_entry(trade, ref, qty):
            return None
        order = next(o for o in trade.orders if o.order_id == ref)
        filled = order.order_filled_utc
        if filled is None or not end <= int(filled.timestamp()*1000) < end + MINUTE:
            return None
        return end, qty

    def _early(self, bot, trade, end, campaign):
        entered, qty = campaign
        if not entered < end < entered + 15*MINUTE:
            return None
        market = bot.exchange.markets[trade.pair]
        symbol = market['id']
        meta, tiers = self.market_meta(bot.exchange, trade.pair)
        stake = qty*trade.open_rate/5
        buffer = archived_buffered_liquidation(SimpleNamespace(price=trade.open_rate, tiers=tiers, meta=meta),
                                                qty, stake)
        if not 0 < buffer < trade.initial_stop_loss < trade.open_rate:
            return None
        store, candles = self.policy.bars, self.policy.candles
        history = store.range(symbol, MINUTE, entered, end, wall_ms())
        if not history:
            candles.query(bot, symbol, MINUTE, entered, end, (end-entered)//MINUTE, contiguous=True)
            history = store.range(symbol, MINUTE, entered, end, wall_ms())
        if not history or history[0].op != trade.open_rate:
            return None
        features = [self.features.read(bot, trade.pair, row.open_ms+MINUTE) for row in history]

        def hour_evidence(close_ms):
            hour_end = close_ms//HOUR*HOUR
            # In the archived branch this value is inspected only at the first
            # qualifying close. Missing evidence stays unknown inside transport.
            bars = store.range(symbol, HOUR, hour_end-2*HOUR, hour_end, wall_ms())
            if not bars:
                return {'end_E': None, 'available_E': None, 'last': None, 'previous': None}
            return {'end_E': hour_end, 'available_E': hour_end, 'last': bars[-1].quote,
                    'previous': bars[-2].quote}

        replay = replay_early(trade.open_rate, trade.initial_stop_loss, entered, history, features,
                              hour_evidence, meta['tick'], buffer)
        return (replay, history, buffer) if replay else None

    def position_state(self, bot, trade, end_ms):
        if not bot.native_paper_exchange() or (campaign := self.campaign_entry(trade)) is None:
            return None
        entered, original_quantity = campaign
        if end_ms < entered + 15*MINUTE and trade.amount == original_quantity:
            evidence = self._early(bot, trade, end_ms, campaign)
            if evidence is None:
                return None
            (state, atr), history, _ = evidence
            mark = history[-1].close
        else:
            # Once the early subset ends, use the active campaign's real
            # chronological holding evidence, not a restarted early replay.
            if owned_priority(bot,trade)!='NATIVE_HARD_CHECK_ONLY':
                return None
            basis = bot.archived_routes.evidence.exit(bot,trade,end_ms)
            if (type(basis) is not CausalExitBasis or
                    (basis.trade_id,basis.pair,basis.entry_ms,basis.active)!=(trade.id,trade.pair,entered,end_ms)):
                return None
            held = basis.state
            state,atr,mark = held.old,held.atr,basis.previous_close
            if (held.blocked or held.queue() or state.pending or atr.pending
                    or not state.held or mark is None
                    or (state.last_E,atr.last_E)!=(end_ms,end_ms)
                    or state.entry!=trade.open_rate or state.hard_stop!=trade.initial_stop_loss):
                return None
        result=PositionFacts(trade.pair,trade.amount,trade.open_rate,mark,trade.initial_stop_loss,
                             state.passed_stage,atr.published,atr.guard.loss.author_armed,trade.funding_fees)
        # The frozen entry sizing uses hard/hour/ARM floors. Mature line
        # ranking belongs to the separate exit policy.
        return result if result.valid() else None

    def exit_clock(self, bot, trade):
        if (not bot.native_paper_exchange() or (campaign := self.campaign_entry(trade)) is None
                or trade.has_open_orders or trade.amount != campaign[1]):
            return None
        entered, _ = campaign
        decided = archived_half_window(entered)
        if not decided <= wall_ms() < decided + MINUTE:
            return None
        if (evidence := self._early(bot, trade, decided, campaign)) is None:
            return None
        (state, atr), _, buffer = evidence
        symbol = bot.exchange.markets[trade.pair]['id']
        quote = next_open(bot.exchange._api, symbol, decided, 'sell')
        if (quote is None or quote.price <= max(state.hard_stop, buffer)
                or atr.pending or atr.guard.loss.author_armed or state.passed_stage):
            return None
        # The only reviewed route here is ideal/no-stress EARLY_TWO5 before
        # any senior state. The staged worker itself has no stress mode knob.
        return ExitClock(trade.id, entered, decided, quote.price, quote.side_quantity, quote.expires_ms, buffer, True, False)
