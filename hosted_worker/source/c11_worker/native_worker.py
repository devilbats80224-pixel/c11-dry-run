"""C11 worker using Freqtrade's normal bot constructor and loop."""
from dataclasses import dataclass
from datetime import datetime
from decimal import localcontext
from importlib.metadata import version
import math
import sqlite3
from pathlib import Path
import time

import freqtrade.exchange as exchanges
from freqtrade.enums import ExitType, MarginMode, RunMode, TradingMode
from freqtrade.freqtradebot import FreqtradeBot
from freqtrade.worker import Worker

from bridge_protocol import need
from owner_wrapper import install_if_present
from sealed_owned_scheduler import OWNED_SCHEDULER_EXPECTED_SEAL, OWNED_SCHEDULER_FACTORY
from feature_warmup import FeatureWarmup
from c11_native_rules import archived_buffered_liquidation
from c11_paper_evidence import PaperEvidence
from c11_paper_exchange import C11PaperExchange
from c11_paper_recovery_runner import C11PaperRecoveryRunner
from c11_pending_gate_v1 import Guard
from c11_primary_decision_seam_v1.primary_seam import BoundOI, Scope
from route_worker import ArchivedRoutes, ordered_entry_pairs
from live_signal_evidence import RuntimeRouteEvidence
from nw5_entry import Nw5Evidence, Nw5Offer
from native_position_observer import NativePositionObserver
from freqtrade.persistence import PairLocks, Trade
from standalone_policy import INPUT_ERRORS
from causal_bars import MINUTE
from paper_context import current
from standalone_policy import build_policy

ROOT = Path('/c11-paper')
LEDGER = ROOT / 'trades.c11.paper.sqlite'
STRATEGY_PATH = '/opt/c11_worker/strategy'
NATIVE_THROTTLE_SECS = 5
OWNED_OPTIONAL_CYCLE_NS = NATIVE_THROTTLE_SECS * 1_000_000_000


@dataclass(frozen=True)
class RuntimeGrant:
    wallet_usdt: float
    generation_floor: int
    consume_before_ns: int
    observation_binding: str


def config_for(wallet, seed_pairs):
    return {
        'dry_run': True, 'dry_run_wallet': wallet, 'runmode': RunMode.DRY_RUN,
        'trading_mode': TradingMode.FUTURES, 'margin_mode': MarginMode.ISOLATED,
        'strategy': 'C11ManagedPaper', 'strategy_path': STRATEGY_PATH,
        'user_data_dir': ROOT, 'db_url': 'sqlite:///' + str(LEDGER),
        'timeframe': '15m', 'max_open_trades': 3, 'stake_currency': 'USDT',
        'stake_amount': 'unlimited', 'tradable_balance_ratio': 1.0,
        'initial_state': 'running', 'cancel_open_orders_on_exit': False,
        'exchange': {'name': 'binance', 'enable_ws': False, 'pair_whitelist': list(seed_pairs),
                     'pair_blacklist': [], 'ccxt_config': {'options': {'fetchMarkets': {'types': ['linear']}}}},
        'entry_pricing': {'price_side': 'other', 'use_order_book': True, 'order_book_top': 1},
        'exit_pricing': {'price_side': 'other', 'use_order_book': True, 'order_book_top': 1},
        'pairlists': [{'method': 'StaticPairList'}],
        'internals': {'sd_notify': False},
    }


class PaperBot(C11PaperRecoveryRunner):
    def __init__(self, config, policy, feed, observer):
        self.policy, self.feed = policy, feed
        self.observer = observer
        self.archived_routes = ArchivedRoutes(RuntimeRouteEvidence(observer))
        self._sweep_offer = None
        self.nw5_evidence = Nw5Evidence()
        self.feature_warmup = FeatureWarmup()
        self.hard_guard = Guard(ROOT / 'hard-stop.sqlite', paper=True)
        self.entry_journal = policy.slots
        self.slots = policy.slots
        self.buffered_liquidation = archived_buffered_liquidation
        self.started_strategy = None
        token = current.set(self)
        original = exchanges.Binance
        try:
            # The pinned ExchangeResolver resolves Binance by its module export.
            # Scoped to this worker's standard constructor in a dedicated process.
            exchanges.Binance = C11PaperExchange
            FreqtradeBot.__init__(self, config)
        finally:
            exchanges.Binance = original
            current.reset(token)
        need(self.started_strategy is self.strategy and type(self.exchange) is C11PaperExchange,
             'C11_NATIVE_RESOLUTION')
        self.paper_evidence = PaperEvidence(self.exchange, ROOT / 'paper-evidence.sqlite')
        self.position_observer = NativePositionObserver(ROOT,LEDGER)
        self.policy.reconcile_restart(self)
        self.observe_native_positions()

    def _owned_exit_projection(self):
        # Called only *after* original protection and native warmup. Ambiguous
        # or nonempty historical exit records conservatively disable this
        # convenience optimization, without delaying original protection.
        from sqlite3 import Error
        from no_sqlite_busy import optional_observation
        hard=self.hard_guard.db
        journal=self.entry_journal.db
        def check():
            if (hard.execute('SELECT 1 FROM hard LIMIT 1').fetchone() is not None
                    or journal.execute('SELECT 1 FROM c11_route_exit LIMIT 1').fetchone() is not None
                    or journal.execute("SELECT 1 FROM intents WHERE kind IS NULL OR kind!='entry' LIMIT 1").fetchone() is not None):
                return None
            return hard,hard.total_changes,journal,journal.total_changes
        try:
            token=optional_observation(check,(hard,journal))
            return token if type(token) is tuple else None
        except (Error,RuntimeError):
            return None

    def _owned_exit_projection_unchanged(self):
        # These attributes are read without executing SQLite statements.
        # Under the existing sole-writer guard all intents are created
        # through these original Worker connections.
        token=getattr(self,'_owned_exit_projection_cache',None)
        if type(token) is not tuple:return False
        hard=self.hard_guard.db
        journal=self.entry_journal.db
        return (hard is token[0] and hard.total_changes==token[1]
                and journal is token[2] and journal.total_changes==token[3])

    def _refresh_active_whitelist(self, trades=None):
        # Exact installed Freqtrade passes its in-cycle open-trade list.
        # Unknown and any open/pending simulated position disable early work.
        self._owned_revisit_no_positions=(type(trades) is list and not trades
            and getattr(self,'_owned_revisit_reconciled',False) is True
            and self._owned_exit_projection_unchanged())
        self.feed.ranked()  # Unavailable is not an authoritative empty list.
        watching = self.feed.tracking_pairs()
        exchange = self.config['exchange']
        if tuple(exchange['pair_whitelist']) != watching:
            exchange['pair_whitelist'] = list(watching)
        if not watching:
            return list(dict.fromkeys(trade.pair for trade in trades or ()))
        return super()._refresh_active_whitelist(trades)

    def observe_native_positions(self):
        return self.position_observer.observe(Trade.get_open_trades(),self.entry_journal)

    def process(self):
        self._owned_revisit_no_positions=False
        self._owned_revisit_reconciled=False
        owned=getattr(self,'_c11_owned_data_lane',None)
        if owned is None:
            self.policy.reconcile_restart(self)
            super().process()
            self.policy.reconcile_restart(self)
            self.observe_native_positions()
            self.feature_warmup.after_protection(self)
            return
        # The Worker keeps its existing throttle. This optional time
        # budget never grants authority beyond the independent original lease.
        # Close the protected phase before changing its optional cutoff.
        observed_before=self.feed.owner_cycle_stamp()
        owned.before_protection()
        self._owned_original_cycle_deadline_ns = time.monotonic_ns() + OWNED_OPTIONAL_CYCLE_NS
        try:
            self._owned_revisit_reconciled=self.policy.reconcile_restart(self) is True
            from native_refresh_cycle import flat_native_refresh
            with flat_native_refresh(self) as due_terminal:
                super().process()
            self.policy.reconcile_restart(self)
            self.observe_native_positions()
            owned.after_protection()
            # Read the just-authenticated native roster, not the provider
            # again while dispatched detached sends recheck owner authority.
            observed_now=self.feed.warmup_observation_since(observed_before)
            self.feature_warmup.after_protection(
                self,verified_cycle_rows=observed_now,due_terminal=due_terminal)
            self._owned_exit_projection_cache=self._owned_exit_projection()
        except BaseException:
            self._owned_exit_projection_cache=None
            self._owned_original_cycle_deadline_ns = None
            owned.before_protection()
            raise
    def enter_positions(self, free_trade_slots):
        # Freqtrade's entry hooks are unchanged. Give its primary pass a
        # private sorted view and restore the collector-ranked watch list.
        ranked = self.active_pair_whitelist
        pairs = ordered_entry_pairs(self)
        self.active_pair_whitelist = pairs
        try:
            primary = super().enter_positions(free_trade_slots)
        finally:
            self.active_pair_whitelist = ranked
        left = max(0,free_trade_slots-primary)
        nw5 = self.enter_nw5(pairs,left)
        left -= nw5
        if left<=0:
            return primary+nw5
        try:
            return primary+nw5+self.archived_routes.supplement(self,left)
        except INPUT_ERRORS:
            return primary+nw5

    def enter_nw5(self,pairs,free_trade_slots):
        if not self.native_paper_exchange() or not self.entry_journal.ready:
            return 0
        entered = 0
        for pair in pairs:
            with self._exit_lock:
                try:
                    offer = self.nw5_evidence.read(self,pair,time.time_ns()//1_000_000)
                    if type(offer) is not Nw5Offer or not self.entry_journal.record_nw5_proposal(offer):
                        continue
                    trades = Trade.get_open_trades()
                    if (not offer.keep or entered>=free_trade_slots or len(trades)>=2
                            or PairLocks.is_global_lock(side='*') or self.strategy.is_pair_locked(pair,side='long')
                            or any(t.pair==pair for t in trades)):
                        continue
                    self.entry_journal.pending_nw5 = offer
                    try:
                        amount=self.wallets.get_trade_stake_amount(pair,self.config['max_open_trades'])
                        if self.execute_entry(pair,amount,enter_tag='c11_nw5'):
                            entered += 1
                    finally:
                        self.entry_journal.pending_nw5 = None
                except INPUT_ERRORS+(sqlite3.Error,):
                    continue
        return entered

    def nw5_inputs(self,pair):
        self._entry_open_clock = None
        offer = self.entry_journal.pending_nw5
        candidate = self.feed.candidate(pair)
        now = time.time_ns()//1_000_000
        if (type(offer) is not Nw5Offer or not offer.keep or candidate is None or candidate.symbol!=offer.symbol
                or candidate.expires_ns<=time.monotonic_ns() or not offer.end<=now<offer.end+MINUTE
                or not self.observed_ranked(candidate)):
            return None,None
        self._entry_generation=(pair,candidate.generation)
        self.policy.candles.update_entry_hour(self,offer.symbol,offer.end)
        features=dict(scope=Scope(offer.symbol,offer.end),valid_until_ms=offer.end+MINUTE)
        facts=self.policy.entry_facts(self,pair,features)
        return (facts,features) if facts is not None and facts.price==offer.close else (None,None)

    def supplement_inputs(self,pair):
        self._entry_open_clock = None
        offer = self._sweep_offer
        candidate = self.feed.candidate(pair)
        now = time.time_ns()//1_000_000
        if (offer is None or candidate is None or candidate.symbol!=offer.symbol
                or candidate.expires_ns <= time.monotonic_ns() or not offer.end<=now<offer.end+MINUTE
                or not self.observed_ranked(candidate)):
            return None,None
        self._entry_generation = (pair,candidate.generation)
        features = {'scope': Scope(offer.symbol,offer.end),'valid_until_ms':offer.end+MINUTE}
        facts = self.policy.entry_facts(self,pair,features)
        if facts is None or facts.price!=offer.close or facts.incumbent:
            return None,None
        return facts,features

    def handle_trade(self,trade):
        if super().handle_trade(trade):
            return True
        try:
            return self.archived_routes.after_native_protection(self,trade)
        except INPUT_ERRORS:
            return False

    def execute_trade_exit(self, trade, limit, exit_check, *, exit_tag=None, **kwargs):
        if not self.entry_journal.owns(trade):
            return False
        if exit_check.exit_type == ExitType.PARTIAL_EXIT:
            facts = self.policy.partial_facts(self, trade)
            pending = getattr(self.strategy, 'pending_trim', None)
            if (facts is None or facts.observed_open != limit or pending is None
                    or facts.side_quantity < pending[3] or time.time_ns()//1_000_000 >= facts.deadline_ms):
                return False
        if (exit_check.exit_type == ExitType.STOP_LOSS and self.native_paper_exchange()
                and exit_tag is None and not trade.has_open_orders and self.hard_guard.trim_can_resume(self.campaign(trade))
                and kwargs.get('ordertype') is None and kwargs.get('sub_trade_amt') is None
                and math.isfinite(limit) and limit > 0):
            return FreqtradeBot.execute_trade_exit(self, trade, limit, exit_check, **kwargs)
        return super().execute_trade_exit(trade, limit, exit_check, exit_tag=exit_tag, **kwargs)

    def observed_ranked(self,candidate):
        try:
            proof = self.observer.current_point(candidate.symbol,time.time_ns()//1_000_000)
            return proof==(candidate.generation,candidate.rank,candidate.ratio)
        except INPUT_ERRORS:
            return False

    def primary_inputs(self, pair):
        candidate = self.feed.candidate(pair)
        if candidate is None or not self.observed_ranked(candidate):
            return None
        features = self.policy.primary_inputs(self, pair, candidate)
        if not features:
            return None
        scope = features['scope']
        need(scope.symbol == candidate.symbol, 'C11_PRIMARY_SYMBOL_BINDING')
        wall_now = time.time_ns() // 1_000_000
        received_ms = int(datetime.fromisoformat(candidate.received_at).timestamp() * 1000)
        remaining = (candidate.expires_ns - time.monotonic_ns()) // 1_000_000
        if not received_ms <= wall_now or remaining <= 0:
            return None
        with localcontext() as ctx:
            ctx.prec = 40
            percent = candidate.ratio.scaleb(2)
        bound = BoundOI(scope, percent, received_ms, wall_now + remaining)
        self._entry_generation = (pair, candidate.generation)
        return {**features, 'bound_oi': bound, 'as_of_ms': wall_now,
                'valid_until_ms': min(wall_now + remaining, features['valid_until_ms'])}

    def entry_inputs(self, pair):
        self._entry_open_clock = None
        inputs = self.primary_inputs(pair)
        if inputs is None:
            return None, None
        return self.policy.entry_facts(self, pair, inputs), inputs

    def get_valid_enter_price_and_stake(self, pair, *args, **kwargs):
        rate, stake, leverage = super().get_valid_enter_price_and_stake(pair, *args, **kwargs)
        candidate = self.feed.candidate(pair) if stake else None
        clock = getattr(self, '_entry_open_clock', None)
        plan = getattr(self.strategy, 'entry_plan', None)
        if (candidate is None or not self.observed_ranked(candidate)
                or self._entry_generation != (pair, candidate.generation)
                or clock is None or plan is None or (clock.pair,clock.end_ms,clock.observed_open) !=
                   (pair,plan.decision_ms,rate) or clock.side_quantity < plan.quantity
                or time.time_ns()//1_000_000 >= clock.valid_until_ms):
            return rate, 0., leverage
        return rate, stake, leverage


class PaperWorker(Worker):
    def __init__(self, config, policy, feed, observer):
        self.policy, self.feed = policy, feed
        self.observer = observer
        super().__init__({}, config=config)

    def _init(self, reconfig):
        need(not reconfig, 'C11_RUNTIME_CONFIG_RELOAD_REFUSED')
        self._c11_bootstrap_rate=None
        if callable(OWNED_SCHEDULER_FACTORY):
            from public_only import check_request,CCXT_KEYS
            from installed_build_inputs import BINANCE_WEIGHT_LIMIT
            from startup_public_meter import OriginalStartupMeter
            self._c11_bootstrap_rate=OriginalStartupMeter(lambda:time.time_ns()//1_000_000,
                      check_request,lambda:BINANCE_WEIGHT_LIMIT,keys=CCXT_KEYS)
            with self._c11_bootstrap_rate.claim_original_constructor():
                self.freqtrade = PaperBot(self._config,self.policy,self.feed,self.observer)
        else:
            self.freqtrade = PaperBot(self._config,self.policy,self.feed,self.observer)
        self._throttle_secs = NATIVE_THROTTLE_SECS
        self._heartbeat_interval = 60
        self._sd_notify = None
        self._c11_owned_scheduler = install_if_present(self,OWNED_SCHEDULER_FACTORY,OWNED_SCHEDULER_EXPECTED_SEAL)

    def exit(self):
        try:
            if getattr(self,'_c11_owned_scheduler',None) is not None:
                self._c11_owned_scheduler.close()
        finally:
            try:
                tap=getattr(self,'_c11_bootstrap_rate',None)
                if tap is not None:tap.close()
            finally:super().exit()


def create_worker(grant, feed, observer):
    need(type(grant) is RuntimeGrant and grant.observation_binding==observer.binding and type(grant.wallet_usdt) in (int, float)
         and math.isfinite(grant.wallet_usdt) and grant.wallet_usdt > 0
         and 0 < grant.consume_before_ns - time.monotonic_ns() <= 30_000_000_000,
         'C11_RUNTIME_GRANT_SCOPE')
    need(version('freqtrade') == '2026.10' and version('ccxt') == '4.5.90', 'C11_RUNTIME_VERSION')
    first = feed.read()
    need(bool(first), 'C11_NO_FRESH_CLASSIFIED_OBSERVATION')
    policy = build_policy(ROOT)
    need(time.monotonic_ns() < grant.consume_before_ns, 'C11_RUNTIME_GRANT_EXPIRED')
    return PaperWorker(config_for(grant.wallet_usdt, [row.pair for row in first]), policy, feed, observer)
