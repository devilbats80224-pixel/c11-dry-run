"""C11 local-paper exit path. Start remains blocked at the managed boundary."""
from math import isfinite

from freqtrade.enums import ExitType
from freqtrade.exceptions import OperationalException
from freqtrade.freqtradebot import FreqtradeBot
from freqtrade.persistence import Trade

from c11_native_paper_runner import C11NativePaperRunner
from c11_paper_evidence import PAPER_POSITION, PAPER_TERMINAL, PaperEvidence, lots
from c11_paper_exchange import C11PaperExchange
from c11_pending_bridge_v2 import LocalState, pre_reconcile


class C11PaperRecoveryRunner(C11NativePaperRunner):
    def native_paper_exchange(self):
        return bool(self.config.get('dry_run') is True
                    and type(self.exchange) is C11PaperExchange
                    and self.exchange._config.get('dry_run') is True)

    def submit_partial(self, trade, limit, exit_check, *, exit_tag, **kwargs):
        if not self.native_paper_exchange():
            return False
        return FreqtradeBot.execute_trade_exit(self, trade, limit, exit_check, exit_tag=exit_tag, **kwargs)

    @staticmethod
    def campaign(trade):
        return f'{trade.id}:{trade.pair}'

    def arm_paper_stop(self, trade):
        proof = self.paper_evidence
        guard = self.hard_guard
        if (not self.native_paper_exchange() or type(proof) is not PaperEvidence
                or proof.exchange is not self.exchange or guard.terminal_evidence != PAPER_TERMINAL
                or guard.position_evidence != PAPER_POSITION):
            raise OperationalException('C11_PAPER_RECOVERY_BOUNDARY')
        pos = self.campaign(trade)
        if guard.trim_can_resume(pos):
            open_orders = trade.open_orders
            if len(open_orders) != 1 or open_orders[0].ft_order_tag != self.trim_order_tag:
                return None
            order = open_orders[0]
            guard.arm(pos, order.order_id, lots(trade.amount), lots(order.safe_filled),
                      lots(order.safe_amount), proof.now_ms())
            return order
        key = guard.db.execute('SELECT trim FROM hard WHERE pos=?', (pos,)).fetchone()[0]
        return next((o for o in trade.orders if o.order_id == key and o.ft_order_tag == self.trim_order_tag), None)

    def execute_trade_exit(self, trade, limit, exit_check, *, exit_tag=None, **kwargs):
        if exit_check.exit_type is ExitType.PARTIAL_EXIT:
            return super().execute_trade_exit(trade, limit, exit_check, exit_tag=exit_tag, **kwargs)
        if (exit_check.exit_type is not ExitType.STOP_LOSS or exit_tag is not None
                or kwargs.get('ordertype') is not None or kwargs.get('sub_trade_amt') is not None
                or not self.native_paper_exchange() or not isfinite(limit) or limit <= 0):
            return False

        guard = self.hard_guard
        proof = self.paper_evidence
        order = self.arm_paper_stop(trade)
        if order is None:
            return False
        pos = self.campaign(trade)
        if guard.db.execute('SELECT ticket FROM hard WHERE pos=?', (pos,)).fetchone()[0] is not None:
            return False
        terminal_and_body = proof.cancel_or_observe(trade, order)
        if terminal_and_body is None:
            return False
        terminal, body = terminal_and_body
        order_key = order.order_id
        if order.ft_is_open:
            self.update_trade_state(trade, order_key, body, send_msg=False)
            Trade.commit()
        position = proof.remaining_position(trade, pos, terminal)
        if position is None:
            return False

        def read_local():
            fresh = proof.reload(trade)
            if fresh is None:
                return None
            updated = next((o for o in fresh.orders if o.order_id == order_key), None)
            if updated is None:
                return None
            return LocalState(self.campaign(fresh), lots(fresh.amount), fresh.has_open_orders, lots(updated.safe_filled))

        _, ticket = pre_reconcile(guard=guard, pos=pos, terminal=terminal, position=position, now=proof.now_ms(),
                                  exec_quote_known=True, read_local=read_local,
                                  apply_terminal=lambda _: self.update_trade_state(trade, order_key, body, send_msg=False))
        if ticket is None:
            return False
        fresh = proof.reload(trade)
        if fresh is None or not fresh.is_open or fresh.has_open_orders or lots(fresh.amount) != ticket['amount']:
            return False
        return FreqtradeBot.execute_trade_exit(self, fresh, limit, exit_check, **kwargs)
