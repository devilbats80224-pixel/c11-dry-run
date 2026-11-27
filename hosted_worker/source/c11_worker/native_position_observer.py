"""Record restartable native paper positions. No exchange reference or order methods."""
from decimal import Decimal,InvalidOperation
import os
from pathlib import Path
import stat

from bridge_protocol import need
from observer_store import ObserverStore,digest,wall_ms


def as_quantity(value):
    try:
        out=Decimal(str(value))
    except InvalidOperation:
        return None
    return out if out.is_finite() and 0<=out<=Decimal('1000000000000000000') else None


def snapshot_trade(trade,journal,seen):
    base=dict(id=trade.id,pair=trade.pair)
    if (not journal.owns(trade) or not trade.is_open or trade.is_short or trade.leverage!=5):
        return base | {'status':'NATIVE_POSITION_IDENTITY_UNKNOWN'}
    holdings=Decimal(0)
    entries=[]
    pending=[]
    settled=[]
    seen_ids=set()
    for order in trade.orders:
        side=order.ft_order_side
        if (side not in ('buy','sell') or order.ft_pair!=trade.pair
                or not order.order_id.startswith(f'dry_run_{side}_')
                or order.order_id in seen_ids):
            return base | {'status':'NATIVE_ORDER_IDENTITY_UNKNOWN'}
        seen_ids.add(order.order_id)
        if order.ft_is_open:
            pending.append(order.order_id)
            continue
        filled=as_quantity(order.safe_filled)
        if filled is None or order.status not in ('closed','canceled','expired'):
            return base | {'status':'NATIVE_ORDER_TERMINAL_UNKNOWN'}
        if not filled and side=='buy':
            return base | {'status':'NATIVE_ORDER_TERMINAL_UNKNOWN'}
        if filled:
            clock=order.order_filled_utc
            if clock is None or not 0<=int(clock.timestamp()*1000)<=seen:
                return base | {'status':'NATIVE_FILL_CLOCK_UNKNOWN'}
            holdings+=filled if side=='buy' else -filled
            settled.append(dict(order_id=order.order_id,side=side,filled=str(filled),
                                filled_ms=int(clock.timestamp()*1000),terminal=order.status))
            if side=='buy':
                entries.append(order.order_id)
    quantity=as_quantity(trade.amount)
    entry=as_quantity(trade.open_rate)
    floor=as_quantity(trade.initial_stop_loss)
    stop=as_quantity(trade.stop_loss)
    if (len(entries)!=1 or quantity is None or quantity<=0 or holdings!=quantity
            or not entry or not floor or not stop or floor>=entry):
        return base | {'status':'NATIVE_POSITION_RECONCILIATION_UNKNOWN'}
    return base | dict(status='PENDING_NATIVE_ORDER' if pending else 'NATIVE_PAPER_POSITION_READBACK',
                       quantity=str(quantity),entry=str(entry),initial_stop=str(floor),
                       native_stop=str(stop),entry_order_id=entries[0],
                       pending=pending,settled=settled)


class NativePositionObserver:
    def __init__(self,root,trade_db):
        # This object is constructed only from the worker's dedicated state
        # mount after native init_db, or from an isolated test fixture.
        root,trade_db=Path(root),Path(trade_db)
        need(trade_db.parent==root and trade_db.name=='trades.c11.paper.sqlite','C11_OBSERVER_NATIVE_SCOPE')
        st=trade_db.lstat()
        need(stat.S_ISREG(st.st_mode) and st.st_nlink==1 and st.st_uid==os.getuid()
             and stat.S_IMODE(st.st_mode)==0o600,'C11_OBSERVER_NATIVE_DB_ID')
        self.ledger=digest(dict(file=trade_db.name,device=st.st_dev,inode=st.st_ino))
        self.store=ObserverStore(root/'observer-native-owned-v1.sqlite')
        with self.store.transaction():
            self.store.bind('native-freqtrade-paper',{'ledger':self.ledger})

    def observe(self,trades,journal):
        seen=wall_ms()
        ordered=sorted(trades,key=lambda t:t.id)
        need(len({t.id for t in ordered})==len(ordered),'C11_OBSERVER_DUPLICATE_NATIVE_TRADE')
        positions=[snapshot_trade(t,journal,seen) for t in ordered]
        result=self.store.native_sample(self.ledger,seen,positions)
        return positions,result
