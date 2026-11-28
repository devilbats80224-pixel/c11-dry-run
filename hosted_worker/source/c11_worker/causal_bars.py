"""Durable, causal bars for pinned C11 admission. No inferred segment start."""
from dataclasses import dataclass
import math
import sqlite3

from bridge_protocol import Refused, need
from c11_primary_decision_seam_v1.primary_seam import Scope, Candle15, MinuteInput, ClosedMinute
from c11_primary_decision_seam_v1.rpd5_producer import SegmentStart
from c11_primary_decision_seam_v1.exact_rpd5_join import join
from rpd_checkpoint import RpdState

MINUTE = 60_000
FIVE = 5 * MINUTE
QUARTER = 15 * MINUTE
HOUR = 60 * MINUTE
UTC_DAY = 24 * HOUR
MAX_SEGMENT_BARS = 25_000


@dataclass(frozen=True)
class ClosedBar:
    open_ms: int
    received_ms: int
    op: float
    high: float
    low: float
    close: float
    volume: float
    quote: float

    def payload(self):
        return (self.op,self.high,self.low,self.close,self.volume,self.quote)

    def valid(self, width):
        return (type(self.open_ms) is type(self.received_ms) is int and self.open_ms >= 0
                and self.open_ms % width == 0 and self.open_ms + width <= self.received_ms
                and all(math.isfinite(x) for x in (self.op, self.high, self.low, self.close, self.volume, self.quote))
                and 0 < self.low <= min(self.op, self.close) <= max(self.op, self.close) <= self.high
                and min(self.volume, self.quote) >= 0)


class CausalBars:
    def __init__(self, path, *, read_only=False):
        self.db = sqlite3.connect(path, uri=read_only)
        if read_only:
            self.db.execute("PRAGMA query_only=ON")
            return
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS segment (
                symbol TEXT PRIMARY KEY, open_ms INTEGER NOT NULL, proven_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS rpd_checkpoint (
                symbol TEXT PRIMARY KEY, payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS feature_status (
                symbol TEXT NOT NULL, feature TEXT NOT NULL, end_ms INTEGER NOT NULL,
                observed_ms INTEGER NOT NULL, state TEXT NOT NULL,
                PRIMARY KEY(symbol,feature)
            );
            CREATE TABLE IF NOT EXISTS candle (
                symbol TEXT NOT NULL, width INTEGER NOT NULL, open_ms INTEGER NOT NULL,
                received_ms INTEGER NOT NULL, op REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL,
                close REAL NOT NULL, volume REAL NOT NULL, quote REAL NOT NULL,
                PRIMARY KEY(symbol,width,open_ms)
            );
        """)

    def set_segment(self, proof):
        need(type(proof) is SegmentStart and type(proof.open_ms) is type(proof.proven_at_ms) is int
             and proof.open_ms % FIVE == 0 and 0 <= proof.open_ms <= proof.proven_at_ms, 'C11_RPD_SEGMENT_PROOF')
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            record = self.db.execute('SELECT open_ms,proven_ms FROM segment WHERE symbol=?', (proof.symbol,)).fetchone()
            if record is None:
                self.db.execute('INSERT INTO segment VALUES (?,?,?)',
                                (proof.symbol, proof.open_ms, proof.proven_at_ms))
            else:
                need(record[0] == proof.open_ms, 'C11_RPD_GENESIS_CHANGED')

    def segment(self, symbol):
        row = self.db.execute('SELECT open_ms,proven_ms FROM segment WHERE symbol=?', (symbol,)).fetchone()
        return SegmentStart(symbol, *row) if row else None

    def record(self, symbol, width, rows):
        need(width in (MINUTE, FIVE, QUARTER, HOUR), 'C11_CANDLE_WIDTH')
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            self._record_checked_rows(symbol, width, rows)

    def _record_checked_rows(self, symbol, width, rows, *, archived_open=None, reviewed_keys=()):
        for bar in rows:
            need(type(bar) is ClosedBar and bar.valid(width), 'C11_CANDLE_CAUSAL_ROW')
            if (width==QUARTER and archived_open==bar.open_ms) or (width,bar.open_ms) in reviewed_keys:
                # This exact archived key is already checked by the owner-
                # selected version under the same original transaction.
                continue
            key = (symbol, width, bar.open_ms)
            values = (bar.op, bar.high, bar.low, bar.close, bar.volume, bar.quote)
            prior = self.db.execute(
                'SELECT op,high,low,close,volume,quote FROM candle WHERE symbol=? AND width=? AND open_ms=?',
                key,
            ).fetchone()
            if prior is not None:
                need(prior == values, 'C11_CLOSED_CANDLE_CHANGED')
            else:
                self.db.execute('INSERT INTO candle VALUES (?,?,?,?,?,?,?,?,?,?)', (*key, bar.received_ms, *values))

    def _warm_immutable_minutes(self, symbol, batches, scope):
        """Select stored prefix rows only when no planned 5m aggregate depends on that new value."""
        end=scope.end
        need(scope.symbol==symbol and type(end) is int and end%QUARTER==0,
             'C11_OWNER_BATCH_SHAPE')
        need(all(type(bar) is ClosedBar for _,rows in batches for bar in rows),
             'C11_CANDLE_CAUSAL_ROW')
        fives={bar.open_ms:bar for bar in batches[1][1]}
        chosen=[]
        retained=[]
        for bar in batches[0][1]:
            replacement=bar
            if bar.valid(MINUTE) and bar.open_ms+MINUTE<=end-QUARTER:
                previous=self.range(symbol,MINUTE,bar.open_ms,bar.open_ms+MINUTE,bar.received_ms)
                if previous and previous[0].payload()!=bar.payload():
                    at=bar.open_ms//FIVE*FIVE
                    use=fives.get(at)
                    if use is None or self.range(symbol,FIVE,at,at+FIVE,bar.received_ms)==(use,):
                        replacement=previous[0]
                        retained.append(replacement)
            chosen.append(replacement)
        return ((MINUTE,tuple(chosen)),*batches[1:]),tuple(retained)

    def record_owner_primary_batch(self, symbol, batches, authority, *, historical=None, scope=None, multi=None, wall=None, revision_append=None, preserve_warm_prefix=False):
        # Only the original owner may call through its guarded session. Scope
        # callback belongs to the actual current original worker, not receipt data.
        phases=['ATOMIC_PRECHECK']
        self.owner_atomic_phases=phases
        began=False
        committing=False
        version=None
        preserved=()
        try:
            need(not self.db.in_transaction and type(batches) is tuple and
                 tuple(width for width,_ in batches)==(MINUTE,FIVE,QUARTER), 'C11_OWNER_BATCH_SHAPE')
            with self.db:
                phases.append('ATOMIC_BEGIN_ATTEMPT')
                self.db.execute('BEGIN IMMEDIATE')
                began=True
                phases.append('ATOMIC_BEGIN_OK')
                phases.append('ATOMIC_PRE_ROWS_AUTH_CHECK')
                need(authority() is True,'C11_OWNER_BATCH_AUTHORITY')
                phases.append('ATOMIC_ROWS_ENTERED')
                reviewed_keys=()
                if preserve_warm_prefix:
                    need(historical is None and multi is None and revision_append is None and scope is not None,
                         'C11_OWNER_BATCH_SHAPE')
                    batches,preserved=self._warm_immutable_minutes(symbol,batches,scope)
                    if preserved:
                        phases.append('ATOMIC_HISTORICAL_MINUTE_RETAINED')
                if multi is not None:
                    from multilane_history_view import FrozenReadView
                    need(type(multi) is FrozenReadView and historical is None and callable(wall),'C11_OWNER_BATCH_SHAPE')
                    reviewed_keys=multi.verify_keys(self,scope,wall(),batches)
                if historical is not None:
                    from historical_version import HistoricalVersion
                    need(type(historical) is HistoricalVersion and scope.symbol==symbol,'C11_OWNER_BATCH_SHAPE')
                    version=historical.prepare(self,scope,batches)
                if version is not None:
                    for width,rows in batches:
                        self._record_checked_rows(symbol,width,rows,archived_open=version.archived_open)
                else:
                    for width,rows in batches:
                        self._record_checked_rows(symbol,width,rows,reviewed_keys=reviewed_keys)
                if revision_append is not None:
                    need(multi is not None and callable(revision_append)
                         and revision_append(self,scope,multi) is True,'C11_OPG_CORRECTED_LINEAGE_APPEND')
                phases.append('ATOMIC_ROWS_RETURNED')
                phases.append('ATOMIC_POST_ROWS_AUTH_CHECK')
                need(authority() is True,'C11_OWNER_BATCH_AUTHORITY')
                if version is not None:
                    need(historical.protection(scope),'C11_OWNER_BATCH_AUTHORITY')
                if multi is not None:
                    need(multi.bound(scope,wall()),'C11_OWNER_BATCH_AUTHORITY')
                phases.append('ATOMIC_COMMIT_ENTERED')
                committing=True
            phases.append('ATOMIC_COMMIT_RETURNED')
            return preserved if preserve_warm_prefix else version
        except BaseException:
            # No extra SQL nor rollback/commit control. Transaction-state only.
            try:active=self.db.in_transaction
            except Exception:active=None
            if active is True:phases.append('ATOMIC_TX_STILL_OPEN')
            elif active is None:phases.append('ATOMIC_TX_STATE_UNKNOWN')
            elif not began and 'ATOMIC_BEGIN_ATTEMPT' in phases:phases.append('ATOMIC_BEGIN_REFUSED_INACTIVE')
            elif began and not committing:phases.append('ATOMIC_ROLLBACK_CLOSED_BEFORE_COMMIT')
            elif committing:phases.append('ATOMIC_COMMIT_OUTCOME_UNCERTAIN_CLOSED')
            else:phases.append('ATOMIC_PRECHECK_REFUSED')
            raise

    def range(self, symbol, width, start, end, as_of):
        count = (end - start) // width
        need(0 < count <= MAX_SEGMENT_BARS and end - start == count * width, 'C11_CANDLE_RANGE_CAP')
        rows = tuple(ClosedBar(*row) for row in self.db.execute(
            'SELECT open_ms,received_ms,op,high,low,close,volume,quote FROM candle '
            'WHERE symbol=? AND width=? AND open_ms>=? AND open_ms<? AND received_ms<=? ORDER BY open_ms',
            (symbol, width, start, end, as_of),
        ))
        return rows if len(rows) == count and all(bar.open_ms == start + i * width for i, bar in enumerate(rows)) else ()

    def listing_range(self, symbol, width, start, end, as_of):
        """Read an exact proven listing sequence in UTC-day windows."""
        proof = self.segment(symbol)
        if (width not in (FIVE, QUARTER) or proof is None
                or proof.open_ms != start or proof.proven_at_ms > as_of
                or not 0 < start < end <= as_of or start % width or end % width):
            return ()
        out = []
        while start < end:
            boundary = min(end, (start // UTC_DAY + 1) * UTC_DAY)
            rows = self.range(symbol, width, start, boundary, as_of)
            if not rows:
                return ()
            out.extend(rows)
            start = boundary
        return tuple(out)

    def rpd_progress(self, symbol, end, as_of, *, budget=499, authority=None):
        proof=self.segment(symbol)
        if proof is None or proof.proven_at_ms>as_of or end%FIVE or end>as_of:
            return None
        need(authority is None or authority() is True,'C11_RPD_PREP_AUTHORITY')
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            need(authority is None or authority() is True,'C11_RPD_PREP_AUTHORITY')
            prior=self.db.execute('SELECT payload FROM rpd_checkpoint WHERE symbol=?',(symbol,)).fetchone()
            state=RpdState.restore(prior[0]) if prior else RpdState.start(proof)
            need(state.symbol==symbol and state.origin==proof.open_ms,'C11_RPD_CHECKPOINT_BINDING')
            if state.observed>as_of or state.next_open>end:
                return None
            first=state.next_open
            for raw in self.db.execute(
                'SELECT open_ms,received_ms,op,high,low,close,volume,quote FROM candle '
                'WHERE symbol=? AND width=? AND open_ms>=? AND open_ms<? AND received_ms<=? '
                'ORDER BY open_ms LIMIT ?', (symbol,FIVE,first,end,as_of,budget),
            ):
                bar=ClosedBar(*raw)
                if bar.open_ms!=state.next_open:
                    break
                state.advance(bar,as_of)
            if state.next_open!=first:
                self.db.execute(
                    'INSERT INTO rpd_checkpoint VALUES (?,?) ON CONFLICT(symbol) '
                    'DO UPDATE SET payload=excluded.payload',(symbol,state.pack()))
            need(authority is None or authority() is True,'C11_RPD_PREP_AUTHORITY')
            return state

    def feature_status(self, symbol, end, as_of, states):
        with self.db:
            self.db.executemany(
                'INSERT INTO feature_status VALUES (?,?,?,?,?) ON CONFLICT(symbol,feature) '
                'DO UPDATE SET end_ms=excluded.end_ms,observed_ms=excluded.observed_ms,state=excluded.state '
                'WHERE excluded.end_ms>=feature_status.end_ms',
                [(symbol,feature,end,as_of,status) for feature,status in states.items()],
            )

    def recent_primary(self,symbol,end,as_of,*,scoped_quarters=None):
        from c11_nw15_primary_signal_port_v1.primary15 import WARM as NW_WARM, classify
        quarters=self.range(symbol,QUARTER,end-NW_WARM*QUARTER,end,as_of) if scoped_quarters is None else scoped_quarters
        if scoped_quarters is not None:
            need(type(quarters) is tuple and len(quarters)==NW_WARM and all(
                type(r) is ClosedBar and r.valid(QUARTER) and r.open_ms==end-(NW_WARM-i)*QUARTER
                and r.received_ms<=as_of for i,r in enumerate(quarters)),'C11_HISTORICAL_VERSION_GRID')
        minutes=self.range(symbol,MINUTE,end-10*MINUTE,end,as_of)
        last=self.range(symbol,MINUTE,end-MINUTE,end,as_of)
        candles=tuple(Candle15(r.open_ms,r.close,r.received_ms) for r in quarters)
        signal=classify(end,as_of,candles,last[0].close if last else None) if quarters else None
        return candles,minutes,signal

    def inputs(self,symbol,end,as_of,*,primary=None,state=None):
        candles,minutes,signal=primary if primary is not None else self.recent_primary(symbol,end,as_of)
        if signal is None or signal.status!='NW15_PRIMARY_SIGNAL' or not minutes or state is None:
            return None
        scope=Scope(symbol,end)
        status,_=join(scope,as_of,[state])
        if status!='PASS':
            return None
        return dict(scope=scope,candle_symbol=symbol,valid_until_ms=end+MINUTE,candles15=candles,
                    minutes=tuple(MinuteInput(symbol,row.received_ms,ClosedMinute(
                        row.open_ms+MINUTE,row.high,row.low,row.close)) for row in minutes),
                    rpd_segment=None,completed_bars5=(),rpd_checkpoint=state)
