"""Per-bot paper P3 capacity intent and exact-E final quantity guard.

This pure component does not touch a Freqtrade wallet or place an order. A
strategy callback must supply its own current realized wallet and free margin.
"""
from math import ulp
from decimal import Decimal, InvalidOperation

from paper_capacity_c3oi_capped import plan_p3, CapacityError

# Freqtrade 2026.6 dry-run market buys cap their simulated fill at +5% of
# the requested rate (exchange/exchange.py:get_dry_market_fill_price).
# Reserve that entire bound before sizing. This is a dry-run bound only;
# it makes no claim about unbounded real-market slippage.
DRY_MARKET_BUY_FILL_MULTIPLIER = Decimal('1.05')


class IntentError(ValueError):
    pass


def _d(value):
    try:
        x = Decimal(str(value))
    except (TypeError, ValueError, InvalidOperation) as exc:
        raise IntentError('DECIMAL_INVALID') from exc
    if not x.is_finite():
        raise IntentError('DECIMAL_INVALID')
    return x


class PaperP3Intents:
    def __init__(self):
        self._pending = {}

    def prepare(self, *, pair, E_ms, side, realized_wallet, available_margin,
                pending_margin, requested_margin, leverage, entry_price,
                quote_volume_5m, quote_volume_1h, market_max_quantity,
                quantity_step, price_tick, min_notional, eligible_notional_bracket,
                tier_quality, preview=False):
        if not pair or side != 'long' or isinstance(E_ms, bool) or not isinstance(E_ms, int) or E_ms % 60_000:
            raise IntentError('ENTRY_IDENTITY_INVALID')
        key = pair, E_ms, side
        if key in self._pending:
            raise IntentError('DUPLICATE_EXACT_E_INTENT')
        wallet = _d(realized_wallet)
        available = _d(available_margin)
        pending = _d(pending_margin)
        if pending >= available:
            raise IntentError('NO_AVAILABLE_MARGIN_AFTER_PENDING')
        price = _d(entry_price)
        tick = _d(price_tick)
        if price <= 0 or tick <= 0:
            raise IntentError('PRICE_OR_TICK_INVALID')
        # Freqtrade rounds the bounded simulated market fill to exchange
        # price precision, which may add one tick above the 5% raw ceiling.
        worst_price = price * DRY_MARKET_BUY_FILL_MULTIPLIER + tick
        worst_factor = worst_price / price
        safe_request = min(_d(requested_margin),
                           wallet / 3 / worst_factor,
                           (available-pending) / worst_factor)
        inputs = dict(realized_wallet=realized_wallet, available_margin=available_margin,
                      pending_margin=pending_margin, requested_margin=safe_request,
                      leverage=leverage, entry_price=entry_price,
                      quote_volume_5m=quote_volume_5m, quote_volume_1h=quote_volume_1h,
                      market_max_quantity=market_max_quantity, quantity_step=quantity_step,
                      price_tick=price_tick,
                      min_notional=min_notional, eligible_notional_bracket=eligible_notional_bracket,
                      tier_quality=tier_quality)
        plan = plan_p3(**{k:v for k,v in inputs.items() if k != 'price_tick'})
        if not preview:
            self._pending[key] = {'inputs': inputs, 'plan': plan}
        return {'key': key, **plan,
                'dry_market_worst_price_bound': str(worst_price),
                'paper_intent_only': True}

    def confirm(self, *, pair, E_ms, side, quantity, entry_price,
                realized_wallet, available_margin, pending_margin):
        key = pair, E_ms, side
        intent = self._pending.pop(key, None)
        if intent is None:
            return {'permit': False, 'reason': 'NO_EXACT_E_INTENT'}
        old = intent['plan']
        inputs = dict(intent['inputs'], realized_wallet=realized_wallet,
                      available_margin=available_margin, pending_margin=pending_margin,
                      entry_price=entry_price)
        if _d(entry_price) != _d(intent['inputs']['entry_price']):
            return {'permit': False, 'reason': 'ENTRY_RATE_CHANGED_FROM_PRECAP'}
        try:
            current = plan_p3(**{k:v for k,v in inputs.items() if k != 'price_tick'})
        except CapacityError as exc:
            return {'permit': False, 'reason': str(exc)}
        q, step = _d(quantity), _d(inputs['quantity_step'])
        if isinstance(quantity, float):
            grid = (q / step).to_integral_value() * step
            if abs(quantity-float(grid)) <= ulp(quantity):
                q = grid
        notional = q * _d(entry_price)
        worst_price = _d(entry_price) * DRY_MARKET_BUY_FILL_MULTIPLIER + _d(inputs['price_tick'])
        worst_notional = q * worst_price
        permit = (q > 0 and q % step == 0 and q <= _d(old['quantity']) and
                  q <= _d(current['quantity']) and notional >= _d(inputs['min_notional']) and
                  notional <= _d(old['notional']) and notional <= _d(current['notional']) and
                  worst_notional / _d(inputs['leverage']) <= _d(realized_wallet) / 3 and
                  worst_notional / _d(inputs['leverage']) <= _d(available_margin) - _d(pending_margin))
        return {'permit': bool(permit), 'reason': 'PASS_EXACT_E_P3_CAPACITY_QTY' if permit else
                'QTY_OR_WALLET_EXCEEDS_PRECAP', 'quantity': str(q),
                'notional': str(notional),
                'dry_market_worst_price_bound': str(worst_price),
                'dry_market_worst_margin': str(worst_notional / _d(inputs['leverage'])),
                'tier_quality': old['tier_quality'],
                'leverage': str(inputs['leverage']), 'price': str(entry_price),
                'available_margin': str(available_margin), 'pending_margin': str(pending_margin),
                'realized_wallet': str(realized_wallet),
                'precap_margin': old['resolved_margin'], 'precap_quantity': old['quantity'],
                'precap_notional': old['notional'], 'current_cap_quantity': current['quantity'],
                'current_cap_notional': current['notional'], 'quantity_step': str(step),
                'min_notional': str(inputs['min_notional'])}

    def expire_before(self, E_ms):
        if isinstance(E_ms, bool) or not isinstance(E_ms, int):
            raise IntentError('EXPIRY_CLOCK_INVALID')
        stale = [key for key in self._pending if key[1] < E_ms]
        for key in stale:
            del self._pending[key]
        return len(stale)
