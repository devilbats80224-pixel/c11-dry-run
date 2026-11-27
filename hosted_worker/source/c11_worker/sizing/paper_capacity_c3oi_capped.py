"""Isolated P3 and Snowball-capacity calculation; no order or wallet mutation.

A current venue leverage bracket is not available from unsigned Binance UM
endpoints. Callers must label any bundled-tier input as a static proxy and
must not claim current exchange or historical effective capacity parity.
"""
from decimal import Decimal, InvalidOperation, ROUND_DOWN


class CapacityError(ValueError):
    pass


def D(value, name):
    if isinstance(value, bool):
        raise CapacityError(name + '_INVALID')
    try:
        out = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise CapacityError(name + '_INVALID') from exc
    if not out.is_finite() or out < 0:
        raise CapacityError(name + '_INVALID')
    return out


def plan_p3(*, realized_wallet, available_margin, pending_margin,
            requested_margin, leverage, entry_price, quote_volume_5m,
            quote_volume_1h, market_max_quantity, quantity_step,
            min_notional, eligible_notional_bracket, tier_quality):
    if tier_quality not in ('BUNDLED_STATIC_PROXY', 'VERIFIED_CURRENT_PUBLIC'):
        raise CapacityError('TIER_SOURCE_UNVERIFIED')
    w = D(realized_wallet, 'REALIZED_WALLET')
    available = D(available_margin, 'AVAILABLE_MARGIN')
    pending = D(pending_margin, 'PENDING_MARGIN')
    request = D(requested_margin, 'REQUESTED_MARGIN')
    L = D(leverage, 'LEVERAGE')
    price = D(entry_price, 'ENTRY_PRICE')
    q5 = D(quote_volume_5m, 'QUOTE_VOLUME_5M')
    q1h = D(quote_volume_1h, 'QUOTE_VOLUME_1H')
    max_qty = D(market_max_quantity, 'MARKET_MAX_QUANTITY')
    step = D(quantity_step, 'QUANTITY_STEP')
    minimum = D(min_notional, 'MIN_NOTIONAL')
    bracket = D(eligible_notional_bracket, 'BRACKET')
    if not 1 <= L <= 5 or any(x <= 0 for x in (w, request, price, q5, q1h, max_qty, step, minimum, bracket)) or pending >= available:
        raise CapacityError('NO_POSITIVE_CAUSAL_CAPACITY')
    margin = min(request, w / 3, available - pending)
    if margin <= 0:
        raise CapacityError('NO_AVAILABLE_P3_MARGIN')
    target = margin * L
    cap5 = q5 * Decimal('0.01')
    cap1h = q1h * Decimal('0.0025')
    labelled = [('FREE_CAPITAL', target), ('BINANCE_CAP', bracket),
                ('LIQUIDITY_5M', cap5), ('LIQUIDITY_1H', cap1h),
                ('QTY_LIMIT', max_qty * price)]
    binding, raw = min(labelled, key=lambda item: item[1])
    qty = min((raw / price / step).to_integral_value(rounding=ROUND_DOWN) * step, max_qty)
    notional = qty * price
    if notional < minimum or notional <= 0:
        raise CapacityError('LIQUIDITY_CAP_TOO_SMALL')
    return {'requested_margin': str(request), 'p3_realized_cap': str(w / 3),
            'available_after_pending': str(available - pending),
            'resolved_margin': str(notional / L), 'quantity': str(qty),
            'notional': str(notional), 'binding_constraint': binding,
            'liquidity_cap_5m': str(cap5), 'liquidity_cap_1h': str(cap1h),
            'tier_quality': tier_quality, 'order_authorized': False}
