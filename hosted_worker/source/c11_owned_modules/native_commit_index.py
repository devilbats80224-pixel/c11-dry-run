"""The original writer's optional in-memory post-commit evidence index."""
from dataclasses import dataclass
from threading import get_ident

@dataclass(frozen=True)
class NativeRequest:
    pair:str
    symbol:str
    generation:int
    end_ms:int
    deadline_ms:int

class NativeCommitIndex:
    def __init__(self,store,authority):
        self.owner=get_ident()
        self.store=store
        self.authority=authority
        self.session=None
        self._lanes={}
        self._seals={}

    def start(self, request):
        if get_ident()!=self.owner or self.session is not None or type(request) is not NativeRequest:
            raise RuntimeError('NATIVE_WRITER_SESSION_OWNER')
        self.session=request
        self._lanes.clear()
        self._seals.clear()

    def stop(self):
        if get_ident()!=self.owner:raise RuntimeError('NATIVE_WRITER_SESSION_OWNER')
        self.session=None
        self._lanes.clear()
        self._seals.clear()

    def after_original_record(self, store, symbol, width, end, requested_ms, receipt_ms, actual_ms, rows):
        s=self.session
        if (get_ident()!=self.owner or store is not self.store or store.db.in_transaction or s is None
                or (symbol,end)!=(s.symbol,s.end_ms) or width not in (60000,300000,900000)
                or not s.end_ms<=requested_ms<=receipt_ms<=actual_ms<s.deadline_ms
                or self.authority(s,actual_ms) is not True):
            return False
        terminal=tuple(row for row in rows if row.open_ms==end-width)
        if len(terminal)!=1 or terminal[0].received_ms!=receipt_ms:return False
        got=store.db.execute('SELECT received_ms,op,high,low,close,volume,quote FROM candle '
                             'WHERE symbol=? AND width=? AND open_ms=?',(symbol,width,end-width)).fetchone()
        r=terminal[0]
        if got!=(receipt_ms,r.op,r.high,r.low,r.close,r.volume,r.quote):
            return False
        previous=self._lanes.get(width)
        if previous is not None:
            return previous==(requested_ms,receipt_ms,actual_ms,r)
        self._lanes[width]=(requested_ms,receipt_ms,actual_ms,r)
        return True

    def index_verified_seal(self, seal, exact_projection_check):
        """Called only after original analysis with genuine frame, lineage and tuple."""
        s=self.session
        if get_ident()!=self.owner or s is None or self.store.db.in_transaction:return False
        key=(s.pair,s.symbol,s.generation,s.end_ms)
        if key!=(seal.pair,seal.symbol,seal.generation,seal.end_ms):return False
        if set(self._lanes)!={60000,300000,900000}:return False
        if seal.last_receipt_ms!=max(v[1] for v in self._lanes.values()):return False
        if seal.owner_committed_ms<max(v[2] for v in self._lanes.values()):return False
        if seal.expires_ms>s.deadline_ms or not exact_projection_check(seal,tuple(sorted(self._lanes.items()))):
            return False
        self._seals[key]=seal
        return True

    def lookup(self,*key):
        return self._seals.get(key) if get_ident()==self.owner else None
