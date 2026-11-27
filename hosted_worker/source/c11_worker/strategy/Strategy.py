"""Native Freqtrade resolver strategy for the managed C11 paper worker."""
from freqtrade.enums import RunMode
from freqtrade.exceptions import OperationalException

from C11NativePaperCandidate import C11NativePaperCandidate
from paper_context import current
from native_entry_owner import NativeEntryAdviceMixin


class C11ManagedPaper(NativeEntryAdviceMixin,C11NativePaperCandidate):
    def bot_start(self, **kwargs):
        context = current.get()
        if (context is None or self.config.get('dry_run') is not True
                or self.config.get('runmode') != RunMode.DRY_RUN
                or self.config.get('stake_currency') != 'USDT'
                or self.config.get('max_open_trades') != 3
                or self.order_types.get('stoploss_on_exchange') is not False
                or not self.dp or not self.wallets):
            raise OperationalException('C11_MANAGED_DRY_BOT_START_REQUIRED')
        self.primary_inputs = context.primary_inputs
        self.partial_facts = lambda trade: context.policy.partial_facts(context, trade)
        self.partial_units = lambda trade, stake: context.policy.partial_units(context, trade, stake)
        self.hard_guard = context.hard_guard
        self.entry_journal = context.entry_journal
        self.entry_plan = None
        self.pending_trim = None
        context.started_strategy = self
