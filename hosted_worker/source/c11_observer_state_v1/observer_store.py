"""Append-only C11 observations with provider time and first-seen time."""
from exact_symbol import ID_CHAR
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
import json
from pathlib import Path
import re
import sqlite3
import time

from bridge_protocol import need
from oi20_rank_slots import bridge_slots

SOURCE = 'paper-market-openmarket-oi20-v1'
QUERY = 'ef5fea69e4bea1b579e2857dcf8dcc65f7516edecad0e0310a7b3a2ff377b815'
BINANCE_SOURCE = 'binance-usdm-openInterestHist-5m-sumOpenInterest'
SYMBOL = re.compile(fr'{ID_CHAR}{{2,40}}')
HEX = re.compile(r'[a-f0-9]{64}')
FIVE = 300_000


def wall_ms():
    return time.time_ns() // 1_000_000


def wire(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(obj):
    return sha256(wire(obj).encode()).hexdigest()


def ms(value):
    parsed = datetime.fromisoformat(value)
    need(parsed.utcoffset() == timezone.utc.utcoffset(parsed), 'C11_OBSERVER_UTC_REQUIRED')
    return int(parsed.timestamp()*1000)


def decimal_value(text):
    need(type(text) is str and 0 < len(text) <= 128 and re.fullmatch(
        r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?', text),
        'C11_OBSERVER_DECIMAL')
    val = Decimal(text)
    need(val.is_finite() and (val == 0 or abs(val.adjusted()) <= 128), 'C11_OBSERVER_DECIMAL')
    return val


class ObserverStore:
    def __init__(self, path):
        self.db = sqlite3.connect(path, timeout=5)
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS source_bucket (
                source TEXT PRIMARY KEY, binding TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS openmarket (
                binding TEXT NOT NULL, generation INTEGER NOT NULL, source_ms INTEGER NOT NULL,
                first_seen_ms INTEGER NOT NULL, expiry_ms INTEGER NOT NULL, digest TEXT NOT NULL,
                metadata_sha TEXT, raw TEXT NOT NULL, PRIMARY KEY(binding,generation)
            );
            CREATE TABLE IF NOT EXISTS openmarket_row (
                binding TEXT NOT NULL, generation INTEGER NOT NULL, rank INTEGER NOT NULL,
                symbol TEXT NOT NULL, ratio TEXT NOT NULL, crypto_usdt INTEGER NOT NULL,
                PRIMARY KEY(binding,generation,symbol), UNIQUE(binding,generation,rank)
            );
            CREATE TABLE IF NOT EXISTS binance_quantity (
                source TEXT NOT NULL, symbol TEXT NOT NULL, period_end_ms INTEGER NOT NULL,
                first_seen_ms INTEGER NOT NULL, requested_ms INTEGER NOT NULL,
                quantity TEXT NOT NULL, digest TEXT NOT NULL, raw TEXT NOT NULL,
                PRIMARY KEY(source,symbol,period_end_ms)
            );
            CREATE TABLE IF NOT EXISTS gaps (
                source TEXT NOT NULL, scope TEXT NOT NULL, observed_ms INTEGER NOT NULL,
                reason TEXT NOT NULL, UNIQUE(source,scope,observed_ms,reason)
            );
            CREATE TABLE IF NOT EXISTS native_position_sample (
                ledger TEXT NOT NULL, seen_ms INTEGER NOT NULL, digest TEXT NOT NULL, raw TEXT NOT NULL,
                PRIMARY KEY(ledger,seen_ms)
            );
        """)

    @classmethod
    def read_only(cls,path):
        """Open an approved observer path without creating or updating evidence."""
        self=cls.__new__(cls)
        self.db=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=5)
        self.db.execute('PRAGMA query_only=ON')
        return self

    @contextmanager
    def transaction(self):
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            yield

    def bind(self, source, identity):
        binding = wire(identity)
        self.db.execute('INSERT OR IGNORE INTO source_bucket VALUES (?,?)',(source,binding))
        stored, = self.db.execute('SELECT binding FROM source_bucket WHERE source=?',(source,)).fetchone()
        need(stored == binding,'C11_OBSERVER_SOURCE_IDENTITY_CHANGED')

    def gap(self, source, scope, seen, reason):
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO gaps VALUES (?,?,?,?)',(source,scope,seen,reason))

    def openmarket(self,binding,frame,first_seen,left_ms,classification=None):
        need(frame['source']==SOURCE and frame['query_sha256']==QUERY and frame['status'] in {'FRESH','FRESH_PARTIAL'}
             and frame['offset']==0 and frame['page_size']==60 and type(frame['generation']) is int
             and frame['generation']>0 and type(first_seen) is int and type(left_ms) is int and 0<left_ms<=60000,
             'C11_OBSERVER_OPENMARKET_CONTRACT')
        source_time,provider_end=ms(frame['received_at']),ms(frame['lease_until'])
        expiry=min(provider_end,first_seen+left_ms)
        need(provider_end-source_time==60000 and source_time<=first_seen<expiry,'C11_OBSERVER_OPENMARKET_LEASE')
        rows=frame['rows']
        ranks=bridge_slots(frame)
        metadata_sha=classification.body_sha256 if classification else None
        verified=classification.verified if classification else {}
        if metadata_sha is not None:
            need(HEX.fullmatch(metadata_sha) and isinstance(verified,dict),'C11_OBSERVER_METADATA')
        cursor=frame['generation']
        values=[]
        seen=set()
        previous=None
        for rank,row in zip(ranks,rows):
            sym,ratio=row['symbol'],row['native_oi_change_24h_raw']
            value=decimal_value(ratio)
            need(type(sym) is str and SYMBOL.fullmatch(sym) and sym not in seen
                 and (previous is None or previous>=value),'C11_OBSERVER_UPSTREAM_RANK')
            seen.add(sym)
            previous=value
            values.append((binding,cursor,rank,sym,ratio,int(sym in verified)))
        raw={k:frame[k] for k in ('source','query_sha256','generation','received_at','lease_until','offset','page_size','rows')}
        if frame["status"]=="FRESH_PARTIAL":
            raw["provider_ranks_v1"]=frame["provider_ranks_v1"]
        proof=digest(raw)
        with self.transaction():
            self.bind(SOURCE,{'publication':binding,'query_sha256':QUERY})
            last=self.db.execute('SELECT generation,source_ms,first_seen_ms,digest FROM openmarket '
                                 'WHERE binding=? ORDER BY generation DESC LIMIT 1',(binding,)).fetchone()
            if last is not None:
                need(cursor>=last[0],'C11_OBSERVER_GENERATION_ROLLBACK')
                if cursor==last[0]:
                    need(proof==last[3],'C11_OBSERVER_GENERATION_REWRITE')
                    return False
                need(source_time>last[1] and first_seen>=last[2],'C11_OBSERVER_CLOCK_ROLLBACK')
                if cursor!=last[0]+1:
                    self.db.execute('INSERT INTO gaps VALUES (?,?,?,?)',(SOURCE,str(cursor),first_seen,'GENERATION_GAP'))
            self.db.execute('INSERT INTO openmarket VALUES (?,?,?,?,?,?,?,?)',
                            (binding,cursor,source_time,first_seen,expiry,proof,metadata_sha,wire(raw)))
            self.db.executemany('INSERT INTO openmarket_row VALUES (?,?,?,?,?,?)',values)
        return True

    def quantity(self,symbol,expected,requested,seen,row):
        need(SYMBOL.fullmatch(symbol) and symbol.endswith('USDT') and type(expected) is int and expected%FIVE==0
             and expected <= requested <= seen < expected+FIVE,'C11_OBSERVER_QUANTITY_SCOPE')
        stamp=row['timestamp']
        need(row['symbol']==symbol and type(stamp) is int and stamp==expected,'C11_OBSERVER_QUANTITY_POINT_MISSING')
        value=row['sumOpenInterest']
        need(decimal_value(value)>0,'C11_OBSERVER_QUANTITY_VALUE')
        raw={k:row[k] for k in ('symbol','timestamp','sumOpenInterest')}
        proof=digest(raw)
        with self.transaction():
            self.bind(BINANCE_SOURCE,{'host':'fapi.binance.com','path':'/futures/data/openInterestHist',
                                      'period':'5m','field':'sumOpenInterest'})
            old=self.db.execute('SELECT digest FROM binance_quantity WHERE source=? AND symbol=? AND period_end_ms=?',
                                (BINANCE_SOURCE,symbol,stamp)).fetchone()
            if old:
                need(old[0]==proof,'C11_OBSERVER_QUANTITY_REWRITE')
                return False
            latest=self.db.execute('SELECT MAX(period_end_ms) FROM binance_quantity WHERE source=? AND symbol=?',
                                   (BINANCE_SOURCE,symbol)).fetchone()[0]
            need(latest is None or stamp>latest,'C11_OBSERVER_QUANTITY_BACKFILL')
            self.db.execute('INSERT INTO binance_quantity VALUES (?,?,?,?,?,?,?,?)',
                            (BINANCE_SOURCE,symbol,stamp,seen,requested,value,proof,wire(raw)))
        return True

    def oi20_at(self,binding,symbol,decision):
        # Demand a frame available at the decision itself. No after-the-fact
        # observations and no fallback to an older generation.
        row=self.db.execute('SELECT generation,expiry_ms,first_seen_ms FROM openmarket WHERE binding=? AND first_seen_ms<=? '
                            'ORDER BY generation DESC LIMIT 1',(binding,decision)).fetchone()
        if not row or decision>=row[1]:
            return None
        if self.db.execute("SELECT 1 FROM gaps WHERE source=? AND scope='publication' "
                           'AND observed_ms>=? AND observed_ms<=? LIMIT 1',(SOURCE,row[2],decision)).fetchone():
            return None
        value=self.db.execute('SELECT rank,ratio,crypto_usdt FROM openmarket_row '
                              'WHERE binding=? AND generation=? AND symbol=?',(binding,row[0],symbol)).fetchone()
        if not value or not value[2]:
            return None
        return (row[0],value[0],decimal_value(value[1]))

    def continuous_oi20(self,binding,symbol,start,end):
        """An exact covered span, separate from archive origin/raw-signal proof."""
        need(type(start) is type(end) is int and start<=end,'C11_OBSERVER_SPAN')
        initial=self.oi20_at(binding,symbol,start)
        if initial is None or self.db.execute(
            "SELECT 1 FROM gaps WHERE source=? AND scope='publication' "
            'AND observed_ms>=? AND observed_ms<=? LIMIT 1',(SOURCE,start,end)).fetchone():
            return None
        rows=self.db.execute(
            'SELECT g.generation,g.first_seen_ms,g.expiry_ms,r.ratio,r.crypto_usdt FROM openmarket g '
            'LEFT JOIN openmarket_row r ON r.binding=g.binding AND r.generation=g.generation AND r.symbol=? '
            'WHERE g.binding=? AND g.generation>=? AND g.first_seen_ms<=? ORDER BY g.generation',
            (symbol,binding,initial[0],end)).fetchall()
        expires,prior=None,initial[0]-1
        for generation,seen,expiry,ratio,verified in rows:
            if (generation!=prior+1 or (expires is not None and seen>expires)
                    or not verified or decimal_value(ratio)<Decimal("0.20")):
                return None
            prior,expires=generation,expiry
        return (initial[0],prior) if expires is not None and end<expires else None

    def weak_quantity_pair(self,symbol,decision):
        if type(decision) is not int or decision%FIVE or decision<7*FIVE:
            return None
        boundaries=(decision-7*FIVE,decision-FIVE)
        values=[]
        for boundary in boundaries:
            row=self.db.execute('SELECT quantity,first_seen_ms FROM binance_quantity WHERE source=? '
                                'AND symbol=? AND period_end_ms=? AND first_seen_ms<=?',
                                (BINANCE_SOURCE,symbol,boundary,decision)).fetchone()
            if not row:
                return None
            values.append((boundary,decimal_value(row[0]),row[1]))
        return tuple(values)

    def native_sample(self,ledger,seen,rows):
        payload=wire(rows)
        proof=digest(rows)
        with self.transaction():
            old=self.db.execute('SELECT seen_ms,digest FROM native_position_sample WHERE ledger=? '
                                'ORDER BY seen_ms DESC LIMIT 1',(ledger,)).fetchone()
            if old is not None:
                need(seen>=old[0] and (seen!=old[0] or proof==old[1]),'C11_OBSERVER_NATIVE_CLOCK')
                if proof==old[1]:
                    return proof
            self.db.execute('INSERT INTO native_position_sample VALUES (?,?,?,?)',(ledger,seen,proof,payload))
        return proof
