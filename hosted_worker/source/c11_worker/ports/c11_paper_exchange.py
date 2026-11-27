"""A dry-run-only Binance adapter with an atomic local cancel observation."""
from copy import deepcopy

from freqtrade.exceptions import OperationalException
from freqtrade.exchange.binance import Binance
from freqtrade.persistence import Order


class C11PaperExchange(Binance):
    def __init__(self, config, *, validate=True, exchange_config=None, load_leverage_tiers=False):
        from bridge_protocol import need
        need(config.get('dry_run') is True and config['exchange']['name'] == 'binance'
             and config['exchange'].get('enable_ws') is False, 'C11_NATIVE_PAPER_EXCHANGE_CONFIG')
        self._confirmed_cancel_input = None
        super().__init__(config, validate=validate, exchange_config=exchange_config,
                         load_leverage_tiers=load_leverage_tiers)

    def _init_ccxt(self, exchange_config, sync, ccxt_kwargs):
        from public_only import guard_client
        from startup_public_meter import original_bootstrap_client
        client=guard_client(super()._init_ccxt(exchange_config,sync,ccxt_kwargs))
        return original_bootstrap_client(client,sync)

    def fetch_dry_run_order(self, order_id):
        pinned = self._confirmed_cancel_input
        if pinned is not None:
            if order_id != pinned['id']:
                raise OperationalException('C11_PAPER_CANCEL_ORDER_CHANGED')
            return deepcopy(pinned)
        # Native limit matching tests status != "closed", so an already
        # canceled limit must never be passed back to that matcher.
        cached = self._dry_run_open_orders.get(order_id)
        if cached is not None and cached['status'] in ('canceled', 'closed', 'expired'):
            return deepcopy(cached)
        if cached is None:
            persisted = Order.order_by_id(order_id)
            if (persisted is not None and not persisted.ft_is_open
                    and persisted.status in ('canceled', 'closed', 'expired')):
                return persisted.to_ccxt_object(self._ft_has['stop_price_prop'])
        return super().fetch_dry_run_order(order_id)

    def cancel_confirmed_open(self, observed):
        """Apply native dry-run cancel to the same observed zero-fill order.

        Native cancel refetches and returns zero filled. Pinning the prior native
        response inside this serialized operation prevents a second fill test
        from reaching a different market observation and losing such a fill.
        """
        if (self._config.get('dry_run') is not True or self._confirmed_cancel_input is not None
                or observed['status'] != 'open' or observed['filled'] != 0
                or observed['remaining'] != observed['amount']):
            raise OperationalException('C11_PAPER_CANCEL_UNRESOLVED')
        self._confirmed_cancel_input = deepcopy(observed)
        try:
            final = super().cancel_order(observed['id'], observed['symbol'])
        finally:
            self._confirmed_cancel_input = None
        if (final.get('status') != 'canceled' or final.get('filled') != 0
                or final.get('id') != observed['id'] or final.get('symbol') != observed['symbol']
                or final.get('amount') != observed['amount']):
            raise OperationalException('C11_PAPER_CANCEL_RECEIPT_INVALID')
        self._dry_run_open_orders[observed['id']] = deepcopy(final)
        return final
