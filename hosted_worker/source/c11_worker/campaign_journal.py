"""Couple archived slot state to Freqtrade's durable native order identities."""
from decimal import Decimal
import math
import sqlite3

from archived_routes import EXIT_REASONS, WEAK
from reserve_last import choose

from bridge_protocol import need
from causal_bars import MINUTE
from native_protection import archived_half_window
from c11_native_paper_runner import EntryJournal
from slot_mark import SingleWalletSlots

ORDINARY_HARD_EXIT = 'stop_loss'


class CampaignJournal(EntryJournal):
    def __init__(self, path, *, primary_quote=None):
        super().__init__(path)
        self.primary_quote = primary_quote
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS c11_slot_claim (
                pair TEXT NOT NULL, entry_ms INTEGER NOT NULL, symbol TEXT NOT NULL,
                slot INTEGER NOT NULL CHECK(slot>=0 AND slot<3),
                predecessor_ms INTEGER,
                close_ms INTEGER, close_reason TEXT,
                PRIMARY KEY(pair, entry_ms)
            );

            CREATE TABLE IF NOT EXISTS c11_supplement (
                symbol TEXT NOT NULL, episode_ms INTEGER NOT NULL, pair TEXT NOT NULL,
                entry_ms INTEGER NOT NULL, PRIMARY KEY(symbol,episode_ms), UNIQUE(pair,entry_ms)
            );
            CREATE TABLE IF NOT EXISTS c11_nw5_proposal (
                symbol TEXT NOT NULL, spell_start INTEGER NOT NULL, decision INTEGER NOT NULL,
                PRIMARY KEY(symbol,spell_start), UNIQUE(symbol,decision)
            );
            CREATE TABLE IF NOT EXISTS c11_nw5_entry (
                pair TEXT NOT NULL, entry_ms INTEGER NOT NULL, PRIMARY KEY(pair,entry_ms)
            );
            CREATE TABLE IF NOT EXISTS c11_route_exit (
                trade_id INTEGER PRIMARY KEY, pair TEXT NOT NULL, clock_ms INTEGER NOT NULL,
                reason TEXT NOT NULL, qty REAL NOT NULL, price REAL NOT NULL, ref TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS c11_active_slot
            ON c11_slot_claim(slot) WHERE close_ms IS NULL;
        """)
        self.slots = SingleWalletSlots({})
        self.ready = False
        self.pending_sweep = None
        self.pending_nw5 = None

    @property
    def active(self):
        return self.slots.active

    def needs_primary_quote(self):
        occupied = {slot for slot, _ in self.active.values()} | set(self.slots.tags)
        return (self.ready and self.pending_sweep is None and len(self.active)<3
                and set(range(3)) <= occupied)

    def nw5_seen(self,symbol,start):
        return self.db.execute('SELECT 1 FROM c11_nw5_proposal WHERE symbol=? AND spell_start=?',
                               (symbol,start)).fetchone() is not None

    def record_nw5_proposal(self,offer):
        if not self.ready or not 0<offer.spell_start<=offer.end or offer.end%300000:
            return False
        with self.db:
            return self.db.execute('INSERT OR IGNORE INTO c11_nw5_proposal VALUES (?,?,?)',
                                   (offer.symbol,offer.spell_start,offer.end)).rowcount==1

    def nw5_bound(self,pair,end,offer):
        if offer is None or (f'{offer.symbol[:-4]}/USDT:USDT',offer.end)!=(pair,end):
            return False
        return self.db.execute('SELECT 1 FROM c11_nw5_proposal WHERE symbol=? AND spell_start=? AND decision=?',
            (offer.symbol,offer.spell_start,end)).fetchone() is not None

    def allow(self, end, symbol, native_count):
        if not self.ready or symbol in self.active or native_count != len(self.active) or native_count >= 3:
            return False
        sweep = self.pending_sweep
        nw5 = self.pending_nw5
        if nw5 is not None and (sweep is not None or native_count>=2 or (nw5.symbol,nw5.end)!=(symbol,end)
                or not self.nw5_bound(f'{symbol[:-4]}/USDT:USDT',end,nw5)):
            return False
        if sweep is None:
            quote = None
            if self.needs_primary_quote() and self.primary_quote is not None:
                quote = self.primary_quote(symbol,end)
            # Ordinary slots are exempt. A tagged slot keeps its original
            # strict comparison and refuses missing evidence independently.
            self.slots.quote[symbol,end] = quote
            return self.slots.allow(end, symbol, native_count)
        if native_count != 0 or (sweep.symbol,sweep.end)!=(symbol,end):
            return False
        if self.db.execute('SELECT 1 FROM c11_supplement WHERE symbol=? AND episode_ms=?',
                           (symbol,sweep.episode_start)).fetchone():
            return False
        self.slots.quote[symbol,end] = sweep.quote
        return choose(self.slots,end,symbol,native_count)

    def reserve(self, kind, pair, end_ms, quantity):
        if kind != 'entry':
            return super().reserve(kind, pair, end_ms, quantity)
        if not self.ready or self.slots.pending is None:
            return False
        symbol, end, slot, _ = self.slots.pending
        need(end == end_ms and pair == f'{symbol[:-4]}/USDT:USDT' and symbol.endswith('USDT'), 'C11_SLOT_BINDING')
        try:
            with self.db:
                self.db.execute('BEGIN IMMEDIATE')
                if self.db.execute("SELECT 1 FROM intents WHERE kind='entry' AND ref IS NULL").fetchone():
                    return False
                if not self.db.execute("INSERT OR IGNORE INTO intents VALUES ('entry',?,?,?,NULL)",
                                       (pair, end_ms, quantity)).rowcount:
                    return False
                predecessor = self.db.execute(
                    'SELECT MAX(close_ms) FROM c11_slot_claim WHERE slot=?', (slot,)).fetchone()[0]
                if predecessor is not None and predecessor > end:
                    raise sqlite3.IntegrityError('C11_SLOT_PREDECESSOR_CLOCK')
                self.db.execute('INSERT INTO c11_slot_claim VALUES (?,?,?,?,?,NULL,NULL)',
                                (pair, end_ms, symbol, slot, predecessor))
                if self.pending_sweep is not None:
                    self.db.execute('INSERT INTO c11_supplement VALUES (?,?,?,?)',
                                    (symbol,self.pending_sweep.episode_start,pair,end_ms))
                if self.pending_nw5 is not None:
                    if not self.nw5_bound(pair,end_ms,self.pending_nw5) or len(self.active)>=2:
                        raise sqlite3.IntegrityError('C11_NW5_ENTRY_NOT_BOUND')
                    self.db.execute('INSERT INTO c11_nw5_entry VALUES (?,?)',(pair,end_ms))
        except sqlite3.IntegrityError:
            return False
        return True

    def enter(self, end, symbol):
        pending = self.slots.pending
        need(pending is not None and pending[:2] == (symbol, end), 'C11_SLOT_COMMIT_UNBOUND')
        self.slots.enter(end, symbol)

    def native_entry(self, trade, reference, quantity):
        orders = [order for order in trade.orders if order.order_id == reference]
        if len(orders) != 1:
            return False
        order = orders[0]
        return (trade.is_short is False and trade.leverage == 5 and order.ft_order_side == 'buy'
                and order.ft_pair == trade.pair and order.ft_is_open is False and order.status == 'closed'
                and order.safe_filled == quantity and reference.startswith('dry_run_buy_')
                and getattr(trade,'enter_tag',None)==self.entry_route(trade.pair,reference))

    def entry_route(self,pair,reference):
        if self.db.execute("SELECT 1 FROM c11_nw5_entry n JOIN intents i ON i.kind='entry' "
                           "AND i.pair=n.pair AND i.end_ms=n.entry_ms WHERE i.pair=? AND i.ref=?",
                           (pair,reference)).fetchone():
            return 'c11_nw5'
        if self.db.execute("SELECT 1 FROM c11_supplement n JOIN intents i ON i.kind='entry' "
                           "AND i.pair=n.pair AND i.end_ms=n.entry_ms WHERE i.pair=? AND i.ref=?",
                           (pair,reference)).fetchone():
            return 'c11_supplement'
        return 'c11_primary'

    def confirmed_holding(self, trade, entered, original_quantity, as_of):
        """Accept only the single archived trim when native and intent agree."""
        if not self.owns(trade) or not trade.is_open or trade.is_short or trade.has_open_orders:
            return False
        original=Decimal(str(original_quantity))
        remaining=Decimal(str(trade.amount))
        if not original.is_finite() or not remaining.is_finite() or not 0<remaining<=original:
            return False
        key=f'{trade.id}:{trade.pair}'
        intents=self.db.execute(
            "SELECT end_ms,qty,ref FROM intents WHERE kind='trim' AND pair=?",(key,)).fetchall()
        sells=[o for o in trade.orders if o.ft_order_side=='sell']
        if not sells:
            return remaining==original and not intents
        if len(sells)!=1 or len(intents)!=1:
            return False
        order,=sells
        clock,quantity,reference=intents[0]
        if (clock!=archived_half_window(entered) or not reference or reference!=order.order_id
                or not reference.startswith('dry_run_sell_') or order.ft_pair!=trade.pair
                or order.ft_is_open or order.status!='closed'
                or order.ft_order_tag!='EARLY_TWO5_FAIL_RECOVERY_PARTIAL'
                or not order.average or not math.isfinite(order.average) or order.average<=0):
            return False
        filled_at=order.order_filled_utc
        if filled_at is None:
            return False
        seen=int(filled_at.timestamp()*1000)
        size=Decimal(str(quantity))
        return (clock<=seen<clock+MINUTE and seen<=as_of and size.is_finite()
                and 0<size<original and size==Decimal(str(order.safe_amount))==Decimal(str(order.safe_filled))
                and remaining==original-size)

    def supplement_episode(self,trade,reference=None):
        if reference is None:
            refs = [o.order_id for o in trade.orders if o.ft_order_side=='buy']
            if len(refs)!=1:
                return None
            reference = refs[0]
        row = self.db.execute(
            "SELECT s.episode_ms FROM c11_supplement s JOIN intents i "
            "ON i.pair=s.pair AND i.end_ms=s.entry_ms WHERE s.pair=? AND i.kind='entry' AND i.ref=?",
            (trade.pair,reference)).fetchone()
        return row[0] if row else None

    def exit_reserved(self,trade_id):
        return self.db.execute('SELECT 1 FROM c11_route_exit WHERE trade_id=?',(trade_id,)).fetchone() is not None

    def reserve_route_exit(self,trade_id,pair,bar,reason,qty,price):
        if reason not in EXIT_REASONS:
            return False
        with self.db:
            return self.db.execute('INSERT OR IGNORE INTO c11_route_exit VALUES (?,?,?,?,?,?,NULL)',
                                   (trade_id,pair,bar,reason,qty,price)).rowcount==1

    def acknowledge_route_exit(self,trade_id,reference):
        with self.db:
            need(self.db.execute('UPDATE c11_route_exit SET ref=? WHERE trade_id=? AND ref IS NULL',
                                 (reference,trade_id)).rowcount==1,'C11_ARCHIVED_EXIT_INTENT_CHANGED')

    def settled_route(self,trade,sells):
        row = self.db.execute('SELECT pair,clock_ms,reason,qty,price,ref FROM c11_route_exit WHERE trade_id=?',
                              (trade.id,)).fetchone()
        if row is None or row[0]!=trade.pair or row[2]!=trade.exit_reason or row[2] not in EXIT_REASONS:
            return None
        pair,clock,reason,quantity,price,reference = row
        matched = [o for o in sells if o.order_id==reference and o.safe_filled==quantity and o.average==price]
        if len(matched)!=1:
            return None
        stamp = matched[0].order_filled_utc
        if stamp is None or not clock <= int(stamp.timestamp()*1000) < clock+60000:
            return None
        # Archive slot clock is the end-label of the bar whose open/intrabar
        # action just settled. Native real time and this logical label differ.
        return clock+60000,reason

    def owns(self, trade):
        rows = self.db.execute(
            "SELECT i.ref,i.qty FROM intents i JOIN c11_slot_claim c ON c.pair=i.pair AND c.entry_ms=i.end_ms "
            "WHERE i.kind='entry' AND i.pair=? AND c.close_ms IS NULL AND i.ref IS NOT NULL",
            (trade.pair,),
        )
        return any(self.native_entry(trade, reference, quantity) for reference, quantity in rows)

    def reconcile(self, trades):
        """Recover only acknowledged order identities; ambiguous orders freeze entry."""
        self.ready = False
        native_by_order = {}
        for trade in trades:
            for order in trade.orders:
                if order.order_id in native_by_order:
                    return False
                native_by_order[order.order_id] = trade
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            claims = tuple(self.db.execute(
                "SELECT c.pair,c.entry_ms,c.symbol,c.slot,c.predecessor_ms,c.close_ms,c.close_reason,i.qty,i.ref "
                "FROM c11_slot_claim c LEFT JOIN intents i ON i.kind='entry' "
                'AND c.pair=i.pair AND c.entry_ms=i.end_ms ORDER BY c.entry_ms,c.slot'))
            intents = self.db.execute("SELECT COUNT(*) FROM intents WHERE kind='entry'").fetchone()[0]
            if len(claims) != intents:
                return False
            active, matched, updates, tags = {}, set(), [], {}
            seen, released = set(), {}
            for pair, end, symbol, slot, predecessor, close_ms, reason, qty, ref in claims:
                if (ref is None or ref in seen or predecessor != released.get(slot)
                        or (predecessor is not None and predecessor > end)):
                    return False
                trade = native_by_order.get(ref)
                if trade is None or trade.pair != pair or not self.native_entry(trade, ref, qty):
                    return False
                seen.add(ref)
                tags.pop(slot,None)
                if trade.is_open:
                    if close_ms is not None or symbol in active or slot in [v[0] for v in active.values()]:
                        return False
                    active[symbol] = slot, end
                    matched.add(trade.id)
                    continue
                # Only the known protective transport may release a slot in
                # this candidate. Other reasons need their specific producer.
                if trade.close_date_utc is None:
                    return False
                closed = int(trade.close_date_utc.timestamp() * 1000)
                sells = [o for o in trade.orders if o.ft_order_side == 'sell' and o.ft_is_open is False
                         and o.status == 'closed' and o.safe_filled > 0 and o.order_id.startswith('dry_run_sell_')]
                if (closed <= end or not sells or not all(not o.ft_is_open for o in trade.orders)
                        or not self.closed_quantity(sells, qty)):
                    return False
                settled = ((closed,ORDINARY_HARD_EXIT) if trade.exit_reason==ORDINARY_HARD_EXIT
                           else self.settled_route(trade,sells))
                if settled is None:
                    return False
                release, exit_reason = settled
                released[slot] = release
                if exit_reason in (WEAK,'EARLY2_QUOTE_BREAK_NEXT_OPEN'):
                    tags[slot] = (symbol,end,release)
                if close_ms is None:
                    updates.append((release,exit_reason,pair,end))
                elif (close_ms, reason) != settled:
                    return False
            if matched != {trade.id for trade in trades if trade.is_open}:
                return False
            self.db.executemany('UPDATE c11_slot_claim SET close_ms=?,close_reason=? WHERE pair=? AND entry_ms=?',
                                updates)
        self.slots.active, self.slots.tags = active, tags
        self.slots.pending = None
        self.ready = True
        return True

    @staticmethod
    def closed_quantity(sells, quantity):
        from decimal import Decimal
        return sum(Decimal(str(order.safe_filled)) for order in sells) == Decimal(str(quantity))
