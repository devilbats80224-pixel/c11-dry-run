"""Read native Freqtrade paper orders and persist their exact terminal evidence."""
import hashlib
import json
import sqlite3
import time

from freqtrade.persistence import Trade

from c11_pending_gate_v1 import Terminal, Position

from c11_paper_exchange import C11PaperExchange

PAPER_TERMINAL = 'VERIFIED_NATIVE_PAPER_TERMINAL_AND_FILLS'
PAPER_POSITION = 'VERIFIED_NATIVE_PAPER_POSITION'


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def lots(value):
    if type(value) not in (int, float) or not 0 <= value <= 10**12 or int(value) != value:
        raise ValueError('C11_PAPER_NONINTEGER_LOTS')
    return int(value)


class PaperEvidence:
    """Single writer under the C11 paper state lease. No private exchange IO."""
    def __init__(self, exchange, path):
        if type(exchange) is not C11PaperExchange or exchange._config.get('dry_run') is not True:
            raise ValueError('C11_PAPER_EXCHANGE_REQUIRED')
        self.exchange = exchange
        self.db = sqlite3.connect(path)
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS terminal (order_id TEXT PRIMARY KEY, '
                        'trade_id INTEGER NOT NULL, pair TEXT NOT NULL, amount INTEGER NOT NULL, '
                        'prior_filled INTEGER NOT NULL, payload TEXT, sha256 TEXT, observed_ms INTEGER)')

    def cancel_or_observe(self, trade, order):
        key = order.order_id
        if (not key.startswith('dry_run_sell_') or order.ft_pair != trade.pair
                or order.ft_trade_id != trade.id or order.ft_order_side != 'sell'):
            raise ValueError('C11_PAPER_ORDER_SCOPE')
        amount, before = lots(order.safe_amount), lots(order.safe_filled)
        row = self.db.execute('SELECT trade_id,pair,amount,prior_filled,payload,sha256,observed_ms '
                              'FROM terminal WHERE order_id=?', (key,)).fetchone()
        if row is not None:
            trade_id, pair, prior_amount, prior, body, checksum, observed_ms = row
            if (trade_id, pair, prior_amount) != (trade.id, trade.pair, amount):
                raise ValueError('C11_PAPER_CANCEL_IDENTITY_CHANGED')
            if body is None:
                return None
            if hashlib.sha256(body.encode()).hexdigest() != checksum:
                raise ValueError('C11_PAPER_CANCEL_JOURNAL_HASH')
            final = json.loads(body)
            if not prior <= before <= lots(final['filled']):
                raise ValueError('C11_PAPER_CANCEL_PRIOR_FILL_DRIFT')
            return self.receipt(final, observed_ms)
        # Unknown/pending observations never become a zero by inference.
        # The native simulator supports an open zero-fill limit or a full fill.
        # A locally recorded partial is held unless the native terminal agrees.
        native = self.exchange.fetch_order(key, trade.pair)
        if (native.get('id') != key or native.get('symbol') != trade.pair or native.get('side') != 'sell'
                or lots(native.get('amount')) != amount):
            raise ValueError('C11_PAPER_NATIVE_ORDER_MISMATCH')
        filled = lots(native.get('filled'))
        if filled < before or filled > amount:
            raise ValueError('C11_PAPER_NATIVE_FILL_DRIFT')
        status = native['status']
        if status == 'open' and filled != 0:
            return None
        if status not in ('open', 'closed', 'canceled') or (status == 'closed' and filled != amount):
            return None
        if status == 'canceled' and filled != before:
            return None
        with self.db:
            self.db.execute('INSERT INTO terminal(order_id,trade_id,pair,amount,prior_filled) VALUES (?,?,?,?,?)',
                            (key, trade.id, trade.pair, amount, before))
        final = self.exchange.cancel_confirmed_open(native) if status == 'open' else native
        observed_ms = self.now_ms()
        body = canonical(final)
        with self.db:
            updated = self.db.execute(
                'UPDATE terminal SET payload=?,sha256=?,observed_ms=? WHERE order_id=? AND payload IS NULL',
                (body, hashlib.sha256(body.encode()).hexdigest(), observed_ms, key))
            if updated.rowcount != 1:
                raise ValueError('C11_PAPER_CANCEL_COMMIT_RACE')
        return self.receipt(final, observed_ms)

    def receipt(self, order, observed_ms):
        return Terminal(order['id'], order['status'], lots(order['filled']), PAPER_TERMINAL,
                        observed_ms, observed_ms), order

    @staticmethod
    def now_ms():
        return time.time_ns() // 1_000_000

    @staticmethod
    def reload(trade):
        Trade.session.expire_all()
        return Trade.session.get(Trade, trade.id)

    def remaining_position(self, trade, campaign, proof):
        current = self.reload(trade)
        if current is None or current.is_short or not current.is_open or current.has_open_orders:
            return None
        orders = current.orders
        buy = sell = 0
        final = None
        for item in orders:
            if not item.order_id.startswith('dry_run_') or item.ft_pair != current.pair:
                return None
            if item.ft_order_side == 'buy':
                buy += lots(item.safe_filled)
            elif item.ft_order_side == 'sell':
                sell += lots(item.safe_filled)
            else:
                return None
            if item.order_id == proof.order:
                final = item
        remaining = buy - sell
        if (final is None or final.ft_is_open or final.status != proof.status
                or lots(final.safe_filled) != proof.total_filled or remaining <= 0
                or lots(current.amount) != remaining):
            return None
        now = self.now_ms()
        return Position(campaign, remaining, now, now, PAPER_POSITION)
