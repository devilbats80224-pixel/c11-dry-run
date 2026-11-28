"""Data-only recovery for exact missing minutes within already closed UTC days."""
from concurrent.futures import Future
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO, TextIOWrapper
import re
from threading import Event, Thread, get_ident
from zipfile import BadZipFile, ZipFile

from atr_er_state import REQUIRED_QUARTERS
from auto_cold_archive import Hold, OfficialArchiveHTTP
from causal_bars import ClosedBar, MINUTE, QUARTER
from cold_archive_identity import archive_location
from cold_prefix import verify_archive_blob
from exact_symbol import quote_contract_shape

DAY = 86_400_000
ZIP_CAP = 2 * 1024 * 1024
CSV_CAP = 16 * 1024 * 1024
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
HEADER_TAIL = ('open','high','low','close','volume','close_time','quote_volume',
               'count','taker_buy_volume','taker_buy_quote_volume','ignore')
NUMBER = re.compile(r'[0-9]{1,28}(?:\.[0-9]{1,24})?')
STAMP = re.compile(r'[0-9]{1,14}')


def bounded_archive_days(fact, missing, now):
    """Return only affected closed UTC days with the exact minute ranges."""
    end, listing = fact.boundary, fact.origin
    if (not quote_contract_shape(fact.symbol)
            or any(type(x) is not int for x in (end, listing, now))
            or end % QUARTER or listing % MINUTE
            or not 0 < listing < end <= now < end + QUARTER):
        return ()
    stop = min(end - QUARTER, now // DAY * DAY)
    start = max(listing, end - QUARTER - REQUIRED_QUARTERS * QUARTER)
    clipped = []
    for lo, hi in missing:
        if (type(lo) is not int or type(hi) is not int or lo % MINUTE
                or hi % MINUTE or lo >= hi):
            return ()
        lo, hi = max(start, lo), min(stop, hi)
        if lo < hi:
            clipped.append((lo, hi))
    merged = []
    for lo, hi in sorted(clipped):
        if merged and lo <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(hi, merged[-1][1]))
        else:
            merged.append((lo, hi))
    days = {}
    for lo, hi in merged:
        while lo < hi:
            day = lo // DAY
            upper = min(hi, (day + 1) * DAY)
            days.setdefault(day, []).append((lo, upper))
            lo = upper
    return tuple(((EPOCH + timedelta(days=day)).date().isoformat(), tuple(spans))
                 for day, spans in days.items())


@dataclass(frozen=True)
class ArchiveMinuteReceipt:
    symbol: str
    listing_ms: int
    boundary_ms: int
    day: str
    sha256: str
    received_ms: int
    rows: tuple


def read_bounded_archive_day(fact, missing, day, wall, cancel, *, transport=None):
    """Read one planned day, returning exact raw rows with time of receipt.

    Does not write, project, or grant authority; the owner must validate
    current market identity, plan and candidate eligibility before storing.
    """
    spans = dict(bounded_archive_days(fact, missing, wall())).get(day)
    if spans is None:
        raise Hold('AUTO_ARCHIVE_NOT_AVAILABLE')
    if cancel.is_set():
        raise Hold('AUTO_ARCHIVE_CANCELLED')
    transport = OfficialArchiveHTTP(wall) if transport is None else transport
    name, url = archive_location(fact.symbol, day)
    sidecar = transport.get(url+'.CHECKSUM',512,cancel)
    try:
        if type(sidecar.body) is not bytes or not 0 < len(sidecar.body) <= 512:
            raise ValueError('COLD_ARCHIVE_HASH')
        digest, filename = sidecar.body.decode('utf-8').split()
        if not re.fullmatch('[a-f0-9]{64}',digest) or filename not in (name,'*'+name):
            raise ValueError('COLD_ARCHIVE_HASH')
    except (ValueError, UnicodeError) as exc:
        raise Hold('AUTO_ARCHIVE_UNVERIFIED') from exc
    day_open = spans[0][0] // DAY * DAY
    day_close = day_open + DAY
    if type(sidecar.received_ms) is not int or not day_close <= sidecar.received_ms <= wall():
        raise Hold('AUTO_ARCHIVE_NOT_AVAILABLE')
    if cancel.is_set():
        raise Hold('AUTO_ARCHIVE_CANCELLED')
    zipped = transport.get(url,ZIP_CAP,cancel)
    receipt = zipped.received_ms
    if type(receipt) is not int or not sidecar.received_ms <= receipt <= wall():
        raise Hold('AUTO_ARCHIVE_NOT_AVAILABLE')
    try:
        if type(zipped.body) is not bytes or not 0 < len(zipped.body) <= ZIP_CAP:
            raise ValueError('COLD_ARCHIVE_SIZE')
        verify_archive_blob(name,len(zipped.body),digest,zipped.body,sidecar.body)
        expected = tuple(t for lo, hi in spans for t in range(lo,hi,MINUTE))
        wanted = set(expected)
        rows = []
        previous = day_open - MINUTE
        with ZipFile(BytesIO(zipped.body)) as archive:
            members = archive.infolist()
            if (len(members)!=1 or members[0].is_dir()
                    or members[0].filename!=name.removesuffix('.zip')+'.csv'
                    or not 0 < members[0].file_size <= CSV_CAP):
                raise ValueError('COLD_ARCHIVE_MEMBER')
            with archive.open(members[0]) as payload:
                for i, row in enumerate(csv.reader(TextIOWrapper(payload,encoding='utf-8',newline=''))):
                    if cancel.is_set():
                        raise Hold('AUTO_ARCHIVE_CANCELLED')
                    if not i and row and row[0] in ('open_time','open_time_ms'):
                        if tuple(row[1:]) != HEADER_TAIL:
                            raise ValueError('COLD_ARCHIVE_HEADER')
                        continue
                    if len(row)!=12 or not all(STAMP.fullmatch(row[k]) for k in (0,6,8)):
                        raise ValueError('COLD_ARCHIVE_GRID')
                    opened, closed = int(row[0]), int(row[6])
                    if (not previous < opened or not day_open <= opened < day_close
                            or opened%MINUTE or closed!=opened+MINUTE-1
                            or not all(NUMBER.fullmatch(row[k]) for k in (1,2,3,4,5,7,9,10,11))):
                        raise ValueError('COLD_ARCHIVE_GRID')
                    previous = opened
                    candle = ClosedBar(opened,receipt,*(float(row[k]) for k in (1,2,3,4,5,7)))
                    if not candle.valid(MINUTE):
                        raise ValueError('COLD_ARCHIVE_BAR')
                    if opened in wanted:
                        rows.append(candle)
        if tuple(row.open_ms for row in rows)!=expected:
            raise ValueError('COLD_ARCHIVE_INCOMPLETE')
    except (BadZipFile, ValueError, TypeError, OSError, RuntimeError, EOFError, csv.Error) as exc:
        raise Hold('AUTO_ARCHIVE_UNVERIFIED') from exc
    return ArchiveMinuteReceipt(fact.symbol,fact.origin,fact.boundary,day,digest,receipt,tuple(rows))


@dataclass(frozen=True)
class ArchiveFallbackScope:
    symbol: str
    pair: str
    generation: int
    boundary: int
    origin: int
    expires_ns: int
    day: str
    spans: tuple

    def matches(self, receipt, now):
        if (type(receipt) is not ArchiveMinuteReceipt
                or (receipt.symbol,receipt.listing_ms,receipt.boundary_ms,receipt.day)
                != (self.symbol,self.origin,self.boundary,self.day)
                or not re.fullmatch('[a-f0-9]{64}',receipt.sha256)
                or type(receipt.received_ms) is not int
                or not self.boundary<=receipt.received_ms<=now<self.boundary+QUARTER):
            return False
        expected=tuple(t for start,stop in self.spans for t in range(start,stop,MINUTE))
        return (type(receipt.rows) is tuple and len(receipt.rows)==len(expected)
                and all(type(r) is ClosedBar and r.open_ms==t and r.received_ms==receipt.received_ms
                        and r.valid(MINUTE) for r,t in zip(receipt.rows,expected)))


class BoundedArchiveFallback:
    """Original-owner transport adapter. Workers receive only frozen data scope.

    Callbacks qualified(scope), attempted(scope), and commit_raw(scope,receipt)
    must return literal True. The last callback owns the raw-only transaction
    and its final authority check; this adapter cannot write any SQLite table.
    Request/drain/close must run on the originating owner thread.
    """
    def __init__(self, wall, *, qualified, attempted, commit_raw, receipt_qualified=None, transport=None):
        self.wall=wall
        self.qualified=qualified
        self.receipt_qualified=receipt_qualified or qualified
        self.attempted=attempted
        self.commit_raw=commit_raw
        self.transport=OfficialArchiveHTTP(wall) if transport is None else transport
        self.owner=get_ident()
        self.pending={}
        self.closed=False

    def _check_owner(self):
        if get_ident()!=self.owner:
            raise RuntimeError('C11_ARCHIVE_FALLBACK_WRONG_OWNER')

    def _active(self, scope, *, received=False):
        now=self.wall()
        qualified=self.receipt_qualified if received else self.qualified
        return (not self.closed and qualified(scope) is True
                and self.attempted(scope) is True
                and bounded_archive_days(scope,scope.spans,now)==((scope.day,scope.spans),))

    def request(self, fact, missing):
        self._check_owner()
        if (self.closed or type(fact.generation) is not int or fact.generation<=0
                or type(fact.expires_ns) is not int or fact.expires_ns<=0
                or not quote_contract_shape(fact.symbol)
                or fact.pair!=fact.symbol[:-4]+'/USDT:USDT'):
            return ()
        result=[]
        for day,spans in bounded_archive_days(fact,missing,self.wall()):
            # All differently sized spans on the day download the same
            # signed official ZIP. Do not duplicate its in-flight read.
            key=(fact.symbol,day)
            scope=ArchiveFallbackScope(fact.symbol,fact.pair,fact.generation,fact.boundary,
                                       fact.origin,fact.expires_ns,day,spans)
            if key in self.pending or not self._active(scope):
                continue
            event=Event()
            done=Future()
            self.pending[key]=(scope,event,done)
            try:
                Thread(target=self._download,args=(scope,event,done),
                       name='c11-bounded-archive',daemon=True).start()
            except (RuntimeError,OSError):
                del self.pending[key]
                continue
            result.append(scope)
        return tuple(result)

    def _download(self, scope, cancel, done):
        if not done.set_running_or_notify_cancel():
            return
        try:
            done.set_result(read_bounded_archive_day(
                scope,scope.spans,scope.day,self.wall,cancel,transport=self.transport))
        except BaseException as exc:
            done.set_exception(exc)

    def drain(self):
        self._check_owner()
        if self.closed:
            return ()
        results=[]
        for key,(scope,cancel,done) in tuple(self.pending.items()):
            if not self._active(scope,received=True):
                cancel.set()
            if not done.done():
                continue
            try:
                if cancel.is_set():
                    results.append((scope,'C11_ARCHIVE_FALLBACK_SCOPE_LOST',0))
                    continue
                try:
                    receipt=done.result()
                except Hold as exc:
                    results.append((scope,exc.status,exc.wait_ms))
                    continue
                except Exception:
                    results.append((scope,'AUTO_ARCHIVE_UNVERIFIED',0))
                    continue
                if not self._active(scope,received=True) or not scope.matches(receipt,self.wall()):
                    results.append((scope,'C11_ARCHIVE_FALLBACK_RECEIPT_DENIED',0))
                    continue
                committed=self.commit_raw(scope,receipt) is True
                results.append((scope,'C11_ARCHIVE_FALLBACK_RAW_COMMITTED' if committed
                                else 'C11_ARCHIVE_FALLBACK_COMMIT_DENIED',0))
            finally:
                self.pending.pop(key,None)
        return tuple(results)

    def close(self):
        self._check_owner()
        self.closed=True
        for _,cancel,done in self.pending.values():
            cancel.set()
            done.cancel()
        self.pending.clear()
