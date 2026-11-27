"""Isolated stop-after-cancel intent guard. No I/O beyond its private SQLite."""
from dataclasses import dataclass
from hashlib import sha256
from sqlite3 import connect

@dataclass(frozen=True)
class Terminal:
    order: str
    status: str
    total_filled: int
    # Fixture proxy for independently verified terminal order/trade evidence.
    evidence: str
    venue_ms: int
    receipt_ms: int

@dataclass(frozen=True)
class Position:
    pos_id: str
    lots: int
    venue_ms: int
    receipt_ms: int
    evidence: str

def _clock(obj,now):
    return all(type(n) is int for n in (obj.venue_ms,obj.receipt_ms,now)) and 0<obj.venue_ms<=obj.receipt_ms<=now
class Guard:
    def __init__(self,path, *, paper=False):
        self.terminal_evidence = ('VERIFIED_NATIVE_PAPER_TERMINAL_AND_FILLS' if paper
                                  else 'VERIFIED_EXCHANGE_TERMINAL_AND_FILLS')
        self.position_evidence = 'VERIFIED_NATIVE_PAPER_POSITION' if paper else 'VERIFIED_EXCHANGE_POSITION'
        self.db=connect(path,timeout=5,isolation_level=None)
        self.db.execute('CREATE TABLE IF NOT EXISTS hard(pos TEXT PRIMARY KEY, trim TEXT NOT NULL, qty INTEGER, previous INTEGER, limit_qty INTEGER, armed INTEGER, ticket TEXT, reserved_qty INTEGER)')
    def arm(self, pos, trim, qty, previous, limit_qty, armed):
        assert pos and trim and all(type(x) is int for x in (qty,previous,limit_qty,armed))
        assert qty>limit_qty>0 and 0<=previous<=limit_qty and armed>0
        try:
            self.db.execute('BEGIN IMMEDIATE')
            self.db.execute('INSERT OR IGNORE INTO hard(pos,trim,qty,previous,limit_qty,armed) VALUES(?,?,?,?,?,?)',
                            (pos,trim,qty,previous,limit_qty,armed))
            r=self.db.execute('SELECT trim,qty,previous,limit_qty,armed FROM hard WHERE pos=?',(pos,)).fetchone()
            if r!=(trim,qty,previous,limit_qty,armed):
                raise ValueError('HARD_INTENT_CONFLICT')
            self.db.commit()
        except BaseException:
            self.db.rollback();raise
    def trim_can_resume(self,pos):
        return self.db.execute('SELECT 1 FROM hard WHERE pos=?',(pos,)).fetchone() is None
    def close(self):self.db.close()
    def handoff_once(self,*,pos,order,position,now,exec_quote_known):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            ans=self._locked(pos,order,position,now,exec_quote_known)
            self.db.commit()
            return ans
        except BaseException:
            self.db.rollback();raise
    def _locked(self,pos,o,p,now,quote):
        row=self.db.execute('SELECT trim,qty,previous,limit_qty,armed,ticket,reserved_qty FROM hard WHERE pos=?',(pos,)).fetchone()
        if row is None:return 'NO_HARD_INTENT',None
        trim,qty,prev,limit,armed,ticket,rq=row
        if ticket:return 'ALREADY_RESERVED_RECONCILE_BEFORE_RETRY',None
        if (type(o) is not Terminal or o.order!=trim or o.status not in ('canceled','closed','expired')
            or o.evidence!=self.terminal_evidence or not _clock(o,now) or o.venue_ms<armed
            or type(o.total_filled) is not int or not prev<=o.total_filled<=limit):
            return 'WAIT_TERMINAL_OR_FILLS_UNKNOWN',None
        if (type(p) is not Position or p.pos_id!=pos or p.evidence!=self.position_evidence
            or not _clock(p,now) or p.venue_ms<o.venue_ms or p.receipt_ms<o.receipt_ms
            or type(p.lots) is not int or p.lots!=qty-o.total_filled):
            return 'WAIT_POSITION_RECONCILE',None
        if quote is not True:return 'WAIT_EXECUTABLE_QUOTE',None
        key=sha256(f'C11_STOP_V1\0{pos}\0{trim}\0{armed}'.encode()).hexdigest()
        self.db.execute('UPDATE hard SET ticket=?,reserved_qty=? WHERE pos=?',(key,p.lots,pos))
        return 'FIXTURE_HANDOFF_ONLY_NO_ORDERS',dict(amount=p.lots,ticket=key)
