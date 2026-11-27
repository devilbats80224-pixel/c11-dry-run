"""Isolated pre-amount bridge for the pinned native Freqtrade fixture.
Deliberately not an installable or runnable bot program.
"""
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Callable
from freqtrade.enums import ExitType
from freqtrade.exceptions import OperationalException
from freqtrade.freqtradebot import FreqtradeBot
from c11_pending_bridge_v2 import LocalState
from c11_pending_gate_v1 import Guard,Terminal,Position

class SyntheticExchange(SimpleNamespace):
    """Explicit synthetic-delegate envelope, not a Freqtrade/CCXT exchange."""

@dataclass(frozen=True)
class OfflinePorts:
    strategy: object
    journal: Guard
    campaign_id: Callable[[object],str]
    now_ms: Callable[[],int]
    verified_terminal: Callable[[object],Terminal|None]
    verified_position: Callable[[object],Position|None]
    quote_known: Callable[[object,float],bool]
    read_local: Callable[[object,str],LocalState]
    apply_terminal: Callable[[object,Terminal],None]
    reload_trade: Callable[[object],object]
    emit: Callable[[str],None]
    trim_fixture: Callable[...,bool]

class C11PreAmountOfflineRunnerV1(FreqtradeBot):
    trim_order_tag = 'fixture_c11'
    def __init__(self,*args,**kwargs):
        # Do not allow even a FreqtradeBot constructor (it can initialize an
        # exchange before strategy startup). Fixture tests use __new__ only.
        raise OperationalException('C11_NATIVE_RUNNER_OFFLINE_ONLY')
    def execute_trade_exit(self,trade,limit,exit_check,*,exit_tag=None,ordertype=None,
                          sub_trade_amt=None,skip_custom_exit_price=False):
        p=getattr(self,'offline_ports',None)
        if (type(p) is not OfflinePorts or type(getattr(self,'exchange',None)) is not SyntheticExchange
            or self.config.get('dry_run') is not True):
            raise OperationalException('C11_REFUSE_NONFIXTURE_EXIT')
        if exit_check.exit_type is ExitType.PARTIAL_EXIT:
            if not p.journal.trim_can_resume(p.campaign_id(trade)):
                p.emit('PARTIAL_DISABLED_BY_HARD_INTENT');return False
            return p.trim_fixture(trade,limit,exit_check,sub_trade_amt=sub_trade_amt,exit_tag=exit_tag)
        if (exit_check.exit_type is not ExitType.STOP_LOSS or sub_trade_amt is not None
            or ordertype is not None or exit_tag is not None):
            p.emit('WAIT_UNSUPPORTED_PROTECTIVE_ROUTE');return False
        pos=p.campaign_id(trade)
        now=p.now_ms()
        if p.journal.trim_can_resume(pos):
            if not trade.has_open_orders:
                # Route outside this overlap fixture. This class is not a full
                # policy; previous isolated hard-stop test covers this route.
                p.emit('WAIT_NO_PENDING_TRIM_FIXTURE_OUT_OF_SCOPE');return False
            oo=trade.open_orders
            if (len(oo)!=1 or oo[0].ft_order_tag != self.trim_order_tag
                or any(int(n)!=n for n in (trade.amount,oo[0].safe_amount,oo[0].safe_filled))):
                p.emit('WAIT_NOT_SINGLE_BOUND_PENDING_TRIM');return False
            order=oo[0]
            p.journal.arm(pos,order.order_id,int(trade.amount),int(order.safe_filled),int(order.safe_amount),now)
        status,ticket=p.strategy.preview_pending_stop(guard=p.journal,pos=pos,now=now,
                 terminal=p.verified_terminal(trade),position=p.verified_position(trade),
                 exec_quote_known=p.quote_known(trade,limit),
                 read_local=lambda:p.read_local(trade,pos),
                 apply_terminal=lambda proof:p.apply_terminal(trade,proof))
        p.emit(status)
        if ticket is None:return False
        fresh=p.reload_trade(trade)
        if (fresh is None or fresh.id!=trade.id or fresh.pair!=trade.pair or p.campaign_id(fresh)!=pos
            or not fresh.is_open or fresh.has_open_orders or fresh.amount!=ticket['amount']):
            # Reservation stays unknown; do not re-send even if submission status is absent.
            p.emit('WAIT_NATIVE_AMOUNT_CHANGED_AFTER_RESERVATION');return False
        return super().execute_trade_exit(fresh,limit,exit_check,skip_custom_exit_price=skip_custom_exit_price)
