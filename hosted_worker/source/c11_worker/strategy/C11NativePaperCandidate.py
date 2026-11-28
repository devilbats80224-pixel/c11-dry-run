"""Freqtrade strategy callbacks for the bounded, verified C11 primary path."""
from math import isfinite

from C11OfflineAssemblyV2 import C11OfflineAssemblyV2
from entry_verdict_receipt import ADMISSION, mark_primary
from c11_arithmetic import partial_geometry, partial_open_plan
from freqtrade.exchange import timeframe_to_msecs
from freqtrade.exceptions import OperationalException


class C11NativePaperCandidate(C11OfflineAssemblyV2):
    position_adjustment_enable = True
    order_types = {'entry': 'market', 'exit': 'market', 'stoploss': 'market', 'stoploss_on_exchange': False}
    order_time_in_force = {'entry': 'gtc', 'exit': 'gtc'}

    def bot_start(self, **kwargs):
        raise OperationalException('C11_MANAGED_RUNTIME_AND_SOURCE_NOT_RELEASED')

    def populate_entry_trend(self, dataframe, metadata):
        out = super().populate_entry_trend(dataframe, metadata)
        inputs = getattr(self, 'primary_inputs', None)
        if inputs is None or out.empty:
            mark_primary('NATIVE_INPUT_UNAVAILABLE')
            return out
        evidence = inputs(metadata['pair'])
        if not evidence:
            mark_primary('NATIVE_INPUT_UNAVAILABLE')
            return out
        scope = evidence['scope']
        if scope.end_ms != int(out.iloc[-1]['date'].timestamp()*1000) + timeframe_to_msecs(self.timeframe):
            mark_primary('NATIVE_FRAME_END_MISMATCH', force=True)
            return out
        decision = self.preview_primary(**evidence)
        reason = ('PRIMARY_SIGNAL_ONLY' if decision.state == 'PRIMARY_SIGNAL_ONLY'
                  else 'NO_PRIMARY_SIGNAL' if decision.state == 'NO_SIGNAL' else decision.reason)
        mark_primary(reason, force=True)
        if decision.state == 'PRIMARY_SIGNAL_ONLY':
            out.iat[-1, out.columns.get_loc('enter_long')] = 1
            out.iat[-1, out.columns.get_loc('enter_tag')] = 'c11_primary'
        return out

    def get_entry_signal(self, pair, timeframe, dataframe):
        result = super().get_entry_signal(pair, timeframe, dataframe)
        trace = ADMISSION.get()
        if trace is not None and trace['pair'] == pair and result[0] and result[1] == 'c11_primary':
            try:
                closed = dataframe.iloc[-1]['date']
                trace['end'] = int(closed.timestamp()*1000) + timeframe_to_msecs(timeframe)
            except (ValueError, AttributeError, KeyError, TypeError, IndexError, OverflowError):
                pass
        return result

    def is_pair_locked(self, pair, *args, **kwargs):
        result = super().is_pair_locked(pair, *args, **kwargs)
        trace = ADMISSION.get()
        if trace is not None and trace['pair'] == pair and result:
            trace['reason'] = 'NATIVE_ENTRY_PAIR_LOCKED'
        return result

    def leverage(self, **kwargs):
        return 5.

    def custom_stake_amount(self, pair, current_rate, max_stake, min_stake, leverage, entry_tag, side, **kwargs):
        plan = getattr(self, 'entry_plan', None)
        if (plan is None or pair != plan.pair or side != 'long' or leverage != 5
                or entry_tag not in ('c11_primary','c11_nw5','c11_supplement') or current_rate != plan.price
                or not isfinite(max_stake) or not (min_stake or 0) <= plan.stake <= max_stake):
            return 0.
        return plan.stake

    def confirm_trade_entry(self, pair, amount, rate, entry_tag, side, **kwargs):
        plan = getattr(self, 'entry_plan', None)
        return bool(plan is not None and side == 'long' and pair == plan.pair
                    and rate == plan.price and entry_tag in ('c11_primary','c11_nw5','c11_supplement'))

    def adjust_trade_position(self, trade, current_exit_rate, **kwargs):
        facts = self.partial_facts(trade)
        if (facts is None or getattr(facts, 'observed_open', None) != current_exit_rate
                or facts.stress is not False or trade.has_open_orders or trade.is_short
                or trade.leverage != 5 or trade.stop_loss != trade.initial_stop_loss
                or not facts.entered <= int(trade.open_date_utc.timestamp()*1000) < facts.entered+60000
                or not self.hard_guard.trim_can_resume(facts.campaign)):
            return None
        if any(order.ft_order_tag == 'EARLY_TWO5_FAIL_RECOVERY_PARTIAL' for order in trade.orders):
            return None
        if partial_geometry(entry=trade.open_rate, entered=facts.entered, decided=facts.decided,
                            completed_rows=facts.rows) is None:
            return None
        plan = partial_open_plan(quantity=trade.amount, observed_open=current_exit_rate,
                                 hard_floor=trade.stop_loss, liquidation_buffer=facts.buffer,
                                 lot=facts.lot, tick=facts.tick, min_qty=facts.min_qty,
                                 min_notional=facts.min_notional, stress=facts.stress,
                                 decision_end_ms=facts.decided, open_bar_end_ms=facts.open_end,
                                 senior_clear=facts.senior_clear)
        if plan is None:
            return None
        stake = plan['qty'] * trade.stake_amount / trade.amount
        # Freqtrade floors the negative stake conversion. Refuse rounding drift.
        if self.partial_units(trade, stake) != plan['qty']:
            return None
        if not self.entry_journal.reserve('trim', facts.campaign, facts.decided, plan['qty']):
            return None
        self.pending_trim = (trade.id, facts.campaign, facts.decided, plan['qty'])
        return -stake, 'EARLY_TWO5_FAIL_RECOVERY_PARTIAL'
