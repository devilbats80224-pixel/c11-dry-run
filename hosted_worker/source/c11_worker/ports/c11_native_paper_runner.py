"""Native pre-order and pre-exit integration. No production approval is installed."""
import sqlite3
import time

from entry_verdict_receipt import ADMISSION, ORDER_FACT, publish

from c11_native_runner_v1 import C11PreAmountOfflineRunnerV1, SyntheticExchange
from c11_native_rules import paper_entry
from freqtrade.enums import ExitType
from freqtrade.exceptions import OperationalException
from freqtrade.persistence import Trade


class EntryJournal:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS intents (kind TEXT, pair TEXT, end_ms INTEGER, '
                        'qty REAL NOT NULL, ref TEXT, PRIMARY KEY(kind,pair,end_ms))')

    def reserve(self, kind, pair, end_ms, quantity):
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            if kind == 'entry' and self.db.execute(
                'SELECT 1 FROM intents WHERE kind=? AND ref IS NULL', (kind,)
            ).fetchone():
                return False
            return self.db.execute('INSERT OR IGNORE INTO intents VALUES (?,?,?,?,NULL)',
                                   (kind, pair, end_ms, quantity)).rowcount == 1

    def acknowledge(self, kind, pair, end_ms, order_id):
        with self.db:
            count = self.db.execute('UPDATE intents SET ref=? WHERE kind=? AND pair=? AND end_ms=? AND ref IS NULL',
                                    (order_id, kind, pair, end_ms)).rowcount
            if count != 1:
                raise OperationalException('C11_INTENT_RECEIPT_CONFLICT')


class C11NativePaperRunner(C11PreAmountOfflineRunnerV1):
    trim_order_tag = 'EARLY_TWO5_FAIL_RECOVERY_PARTIAL'
    def __init__(self, *args, **kwargs):
        raise OperationalException('C11_MANAGED_RUNTIME_AND_SOURCE_NOT_RELEASED')

    def native_paper_exchange(self):
        return type(self.exchange) is SyntheticExchange

    def execute_entry(self, pair, stake_amount, price=None, *, is_short=False, ordertype=None,
                      enter_tag=None, trade=None, mode='initial', leverage_=None):
        trace = ADMISSION.get()
        if trace is not None and trace['pair'] == pair:
            trace['attempted'] = True
        width = 300_000 if enter_tag in ('c11_nw5', 'c11_supplement') else 900_000
        order_end = (time.time_ns() // 1_000_000 // width) * width
        if trace is not None and trace['pair'] == pair and trace['end'] is not None:
            order_end = trace['end']
        def denied(reason):
            if self.config.get('dry_run') is True:
                publish(pair, order_end, 'order', reason, enter_tag)
            return False

        # The parent native method's strategy callbacks can fall back to the
        # proposed stake or a permissive confirm on exception. Check at the
        # runner boundary as well, before it can call Exchange.create_order.
        if (not self.native_paper_exchange() or self.config.get('dry_run') is not True
                or self.config.get('stake_currency') != 'USDT'
                or mode != 'initial' or trade is not None or is_short or ordertype is not None
                or leverage_ is not None or enter_tag not in ('c11_primary','c11_nw5','c11_supplement')):
            return denied('ENTRY_PAPER_ROUTE_GUARD')
        first = [None]
        token = ORDER_FACT.set(first if enter_tag=='c11_primary' else None)
        try:
            if enter_tag=='c11_nw5':
                facts,features = self.nw5_inputs(pair)
            elif enter_tag=='c11_supplement':
                facts,features = self.supplement_inputs(pair)
            else:
                facts,features = self.entry_inputs(pair)
        finally:
            ORDER_FACT.reset(token)
        if facts is None or features is None or facts.pair != pair:
            return denied(first[0] or ('ENTRY_FACTS_PAIR_MISMATCH' if (
                enter_tag=='c11_primary' and facts is not None and features is not None and facts.pair != pair)
                else 'ENTRY_FACTS_UNAVAILABLE'))
        order_end = facts.decision_ms
        if price is None:
            price = self.exchange.get_rate(pair, side='entry', is_short=False, refresh=True)
        if price != facts.price:
            return denied('ENTRY_PRICE_MISMATCH')
        scope = features.get('scope')
        if scope is None or scope.symbol != facts.symbol or scope.end_ms != facts.decision_ms:
            return denied('ENTRY_SCOPE_MISMATCH')
        open_trades = Trade.get_open_trades()
        if (any(t.has_open_orders or t.is_short or t.leverage != 5 for t in open_trades)
                or {t.pair: (t.amount, t.open_rate, t.initial_stop_loss) for t in open_trades} !=
                   {p.pair: (p.quantity, p.entry, p.initial_stop) for p in facts.incumbent}):
            return denied('ENTRY_POSITION_STATE_CHANGED')
        if enter_tag=='c11_primary' and self.strategy.preview_primary(**features).state!='PRIMARY_SIGNAL_ONLY':
            return denied('ENTRY_PRIMARY_RECHECK_DENIED')
        if enter_tag=='c11_nw5' and (len(open_trades)>=2 or not self.entry_journal.nw5_bound(
                pair,facts.decision_ms,self.entry_journal.pending_nw5)):
            return denied('ENTRY_NW5_ROUTE_NOT_BOUND')
        if enter_tag=='c11_supplement' and (self.entry_journal.pending_sweep is None or open_trades):
            return denied('ENTRY_SUPPLEMENT_NOT_AVAILABLE')
        try:
            plan = paper_entry(facts, slots=self.slots, buffered_liquidation=self.buffered_liquidation)
        except (ValueError, ArithmeticError, TypeError, KeyError, AssertionError):
            return denied('ENTRY_PLAN_ERROR')
        if plan is None:
            return denied('ENTRY_SIZING_OR_RISK_PLAN_REFUSED')
        if not self.entry_journal.reserve('entry', pair, plan.decision_ms, plan.quantity):
            return denied('ENTRY_RESERVATION_REFUSED')
        self.strategy.entry_plan = plan
        try:
            ok = super().execute_entry(pair, plan.stake, price=price, enter_tag=enter_tag)
            if ok:
                opened = next(t for t in Trade.get_open_trades() if t.pair == pair)
                if opened.amount != plan.quantity or opened.stop_loss != plan.stop or len(opened.orders) != 1:
                    raise OperationalException('C11_ENTRY_NATIVE_RECEIPT_DRIFT')
                self.entry_journal.acknowledge('entry', pair, plan.decision_ms, opened.orders[0].order_id)
                self.slots.enter(facts.decision_ms, facts.symbol)
            publish(pair, order_end, 'order', 'NATIVE_ORDER_SENT' if ok else 'NATIVE_ORDER_NOT_ACCEPTED', enter_tag)
            return ok
        finally:
            self.strategy.entry_plan = None

    def get_valid_enter_price_and_stake(self, pair, price, stake_amount, trade_side, entry_tag, trade, mode,
                                        leverage_):
        rate, stake, leverage = super().get_valid_enter_price_and_stake(
            pair, price, stake_amount, trade_side, entry_tag, trade, mode, leverage_)
        plan = self.strategy.entry_plan
        if (plan is None or rate != plan.price or stake != plan.stake or leverage != 5
                or self.exchange.amount_to_contract_precision(pair, (stake / rate) * leverage) != plan.quantity):
            return rate, 0., leverage
        return rate, stake, leverage


    def execute_trade_exit(self, trade, limit, exit_check, *, exit_tag=None, **kwargs):
        if exit_check.exit_type is not ExitType.PARTIAL_EXIT:
            return super().execute_trade_exit(trade, limit, exit_check, exit_tag=exit_tag, **kwargs)

        pending = getattr(self.strategy, 'pending_trim', None)
        if (pending is None or exit_tag != self.trim_order_tag or pending[0] != trade.id
                or kwargs.get('sub_trade_amt') != pending[3]):
            return False
        self.strategy.pending_trim = None
        accepted = self.submit_partial(trade, limit, exit_check, exit_tag=exit_tag, **kwargs)
        if accepted:
            matching = [o for o in trade.orders if o.ft_order_tag == self.trim_order_tag]
            if len(matching) != 1 or matching[0].safe_amount != pending[3]:
                raise OperationalException('C11_TRIM_NATIVE_RECEIPT_DRIFT')
            self.entry_journal.acknowledge('trim', pending[1], pending[2], matching[0].order_id)
        return accepted


    def submit_partial(self, trade, limit, exit_check, *, exit_tag, **kwargs):
        return super().execute_trade_exit(trade, limit, exit_check, exit_tag=exit_tag, **kwargs)
