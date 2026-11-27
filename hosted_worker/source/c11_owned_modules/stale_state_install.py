"""Staged same-origin local state recovery. No acquisition or order authority.

This helper is for the isolated recovery lane only. The caller must verify the
source and recurrence before handing in a prepared state, and retain the
snapshot taken before building it. This module is not wired into live startup.
"""
from dataclasses import dataclass
from hashlib import sha256
import json
import re
import sqlite3
from typing import Callable

FIVE=300_000
QUARTER=900_000
MAX_RECENT_QUARTERS=260

@dataclass(frozen=True)
class Evidence:
    symbol: str
    segment: tuple | None
    checkpoint: str | None
    atr: tuple

def snapshot(db,symbol):
    origin=db.execute("SELECT open_ms,proven_ms FROM segment WHERE symbol=?",(symbol,)).fetchone()
    rpd=db.execute("SELECT payload FROM rpd_checkpoint WHERE symbol=?",(symbol,)).fetchone()
    atr=tuple(db.execute("SELECT * FROM atr_er_checkpoint WHERE symbol=? ORDER BY width",(symbol,)))
    return Evidence(symbol,origin,rpd[0] if rpd else None,atr)

def install(db,previous,*,origin,cutoff,source_ready,now,manifest_sha,rpd,atr,closed_rows,
            write_closed:Callable,authority:Callable,states_match:Callable):
    """Install only a verified same-origin, newer prefix against a frozen prior.

    `states_match` verifies prepared RPD and ATR payloads against the already
    vetted exact continuation artifact. Caller also owns phase, roster and
    single writer. On every refusal the transaction is rolled back.
    """
    if (type(previous) is not Evidence or not previous.segment or db.in_transaction
            or not previous.symbol.endswith("USDT") or previous.symbol=="OPGUSDT"
            or type(origin) is not int or type(cutoff) is not int or type(now) is not int
            or type(source_ready) is not int or not 0<origin<cutoff<=source_ready<=now
            or cutoff%QUARTER or origin!=previous.segment[0]
            or type(manifest_sha) is not str or not re.fullmatch("[a-f0-9]{64}",manifest_sha)
            or authority() is not True):
        return False
    def view(payload):
        state=json.loads(payload)["state"]
        if (state["symbol"],state["origin"])!=(previous.symbol,origin):
            raise ValueError("state identity mismatch")
        return state
    try:
        new=view(rpd)
        if (new["next_open"]!=cutoff or not source_ready<=new["observed"]<=now
                or previous.checkpoint and view(previous.checkpoint)["next_open"]>=cutoff-MAX_RECENT_QUARTERS*QUARTER):
            return False
        if (set(row[1] for row in atr)!={FIVE,QUARTER} or len(atr)!=2
                or any(len(row)!=8 or (row[0],row[3],row[6])!=(previous.symbol,origin,cutoff)
                       or row[4]!=previous.segment[1] or not source_ready<=row[5]<=now for row in atr)
                or any(row[3]!=origin or row[6]>=cutoff for row in previous.atr)
                or states_match(rpd,atr,closed_rows) is not True):
            return False
        with db:
            db.execute("BEGIN IMMEDIATE")
            if authority() is not True or snapshot(db,previous.symbol)!=previous:
                db.rollback()
                return False
            row=(previous.symbol,origin,cutoff,manifest_sha,source_ready,now)
            old=json.dumps({"segment":previous.segment,"rpd":previous.checkpoint,"atr":previous.atr},
                           ensure_ascii=False,separators=(",",":"))
            db.execute("""CREATE TABLE IF NOT EXISTS c11_state_recovery_history (
              symbol TEXT, origin INTEGER, cutoff INTEGER, manifest_sha TEXT, source_ready INTEGER,
              installed_at INTEGER, previous_state TEXT, previous_sha TEXT, PRIMARY KEY(symbol,cutoff))""")
            db.execute("INSERT INTO c11_state_recovery_history VALUES (?,?,?,?,?,?,?,?)",
                       (*row,old,sha256(old.encode()).hexdigest()))
            write_closed(closed_rows)
            db.execute("""INSERT INTO rpd_checkpoint VALUES (?,?)
                          ON CONFLICT(symbol) DO UPDATE SET payload=excluded.payload""",
                       (previous.symbol,rpd))
            for item in atr:
                # Only analytical state, never paper ledger or previous candles.
                db.execute("INSERT INTO atr_er_checkpoint VALUES (?,?,?,?,?,?,?,?) "
                           "ON CONFLICT(symbol,width) DO UPDATE SET "
                           "version=excluded.version, origin=excluded.origin, "
                           "proven=excluded.proven, observed=excluded.observed, "
                           "next_open=excluded.next_open, payload=excluded.payload",item)
            if authority() is not True:
                db.rollback()
                return False
        return True
    except (ValueError,KeyError,TypeError,sqlite3.DatabaseError):
        return False
