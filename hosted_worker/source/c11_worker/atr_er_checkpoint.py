"""Durable read-as-of ATR/ER state and closed-clock samples."""
import json

from atr_er_state import advance, new_atr
from bridge_protocol import need
from causal_bars import ClosedBar, FIVE, QUARTER

VERSION = 'c11-atr20-er20-ema5-v1'
PAGE = 499


class AtrErCheckpoint:
    def __init__(self, store):
        self.store=store
        self.db=store.db
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS atr_er_checkpoint (
                symbol TEXT NOT NULL, width INTEGER NOT NULL, version TEXT NOT NULL,
                origin INTEGER NOT NULL, proven INTEGER NOT NULL, observed INTEGER NOT NULL,
                next_open INTEGER NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(symbol,width)
            );
            CREATE TABLE IF NOT EXISTS atr_er_sample (
                symbol TEXT NOT NULL, width INTEGER NOT NULL, clock INTEGER NOT NULL,
                observed INTEGER NOT NULL, known INTEGER NOT NULL, consecutive INTEGER NOT NULL,
                atr REAL, multiplier REAL NOT NULL, signed REAL NOT NULL, PRIMARY KEY(symbol,width,clock)
            );
        """)

    def _state(self,symbol,width,proof):
        saved=self.db.execute('SELECT version,origin,proven,observed,next_open,payload FROM atr_er_checkpoint '
                              'WHERE symbol=? AND width=?',(symbol,width)).fetchone()
        if saved is None:
            return proof.open_ms,proof.proven_at_ms,new_atr()
        version,origin,proven,observed,next_open,payload=saved
        need((version,origin,proven)==(VERSION,proof.open_ms,proof.proven_at_ms),'C11_ATR_ER_SEED_BINDING')
        state=json.loads(payload)
        need(state['clock']==next_open and next_open==origin+state['n']*width and observed>=proven,
             'C11_ATR_ER_CHECKPOINT_CLOCK')
        return next_open,observed,state

    def cursor(self,symbol,width,proof):
        return self._state(symbol,width,proof)[0]

    def extend(self,symbol,width,proof,target,as_of,*,authority=None):
        need(width in (FIVE,QUARTER) and proof.symbol==symbol and proof.open_ms%width==target%width==0
             and proof.open_ms<=target<=as_of and proof.proven_at_ms<=as_of, 'C11_ATR_ER_SCOPE')
        need(authority is None or authority() is True, 'C11_ATR_PREP_AUTHORITY')
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            need(authority is None or authority() is True, 'C11_ATR_PREP_AUTHORITY')
            start,observed,state=self._state(symbol,width,proof)
            if start>=target or observed>as_of:
                return start
            next_open=start
            pending=[]
            raw=self.db.execute(
                'SELECT open_ms,received_ms,op,high,low,close,volume,quote FROM candle '
                'WHERE symbol=? AND width=? AND open_ms>=? AND open_ms<? AND received_ms<=? '
                'ORDER BY open_ms LIMIT ?', (symbol,width,start,target,as_of,PAGE),
            )
            for row in raw:
                bar=ClosedBar(*row)
                if bar.open_ms!=next_open:
                    break
                need(bar.valid(width),'C11_ATR_ER_CANDLE')
                out=advance(state,bar,width)
                next_open=bar.open_ms+width
                observed=max(observed,bar.received_ms)
                pending.append((symbol,width,next_open,observed,int(out['known']),
                                out['consecutive_bars'],out['atr'],out['multiplier'],out['signed']))
            if pending:
                self.db.executemany('INSERT INTO atr_er_sample VALUES (?,?,?,?,?,?,?,?,?)',pending)
                self.db.execute('INSERT INTO atr_er_checkpoint VALUES (?,?,?,?,?,?,?,?) '
                                'ON CONFLICT(symbol,width) DO UPDATE SET observed=excluded.observed,'
                                'next_open=excluded.next_open,payload=excluded.payload',
                                (symbol,width,VERSION,proof.open_ms,proof.proven_at_ms,observed,next_open,
                                 json.dumps(state,allow_nan=False,sort_keys=True)))
                need(authority is None or authority() is True, 'C11_ATR_PREP_AUTHORITY')
            return next_open

    def sample(self,symbol,width,proof,end,as_of):
        if self.cursor(symbol,width,proof)<end or proof.proven_at_ms>as_of:
            return None
        row=self.db.execute('SELECT observed,known,consecutive,atr,multiplier,signed FROM atr_er_sample '
                            'WHERE symbol=? AND width=? AND clock=? AND observed<=?',
                            (symbol,width,end,as_of)).fetchone()
        if row is None:
            return None
        received,known,count,atr,mult,signed=row
        need(end==proof.open_ms+count*width and received>=max(end,proof.proven_at_ms),'C11_ATR_ER_SAMPLE_CLOCK')
        return dict(known=bool(known),consecutive_bars=count,atr=atr,multiplier=mult,signed=signed,
                    source_end_ms=end,available_proxy_ms=end,actual_received_ms=received)
