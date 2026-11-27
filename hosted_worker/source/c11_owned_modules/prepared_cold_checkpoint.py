"""Optional sealed result of independently verified original recurrence."""

from dataclasses import asdict, replace
from hashlib import sha256
import json
import math

from atr_er_checkpoint import VERSION
from atr_er_state import new_atr
from causal_bars import ClosedBar, FIVE, QUARTER
from cold_archive_release import read_bounded, unique
from primary_only_route import NW_WARM
from rpd_checkpoint import RpdState

NAME = 'prepared-prefix.json'
LIMIT = 256*1024
SCHEMA = 'c11-reviewed-prepared-prefix-v1'

def number(value):
    return type(value) in (int, float) and math.isfinite(value)

def encode(source, prefix):
    """Offline construction; only the reviewed catalogue can admit these bytes."""
    return (json.dumps(dict(schema=SCHEMA, atr_version=VERSION,
                archive_manifest_sha=source.manifest_sha, rpd=json.loads(prefix.rpd.pack()),
                atr={str(w):x for w,x in prefix.atr}, quarters=[asdict(b) for b in prefix.quarters]),
                sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()

def decode(raw, *, symbol, source, now, grid_shift=0):
    from cold_prefix import PreparedPrefix
    if not 0<len(raw)<=LIMIT or sha256(raw).hexdigest()!=source.prepared_sha256:
        raise ValueError('C11_PREPARED_PREFIX_PIN')
    try:
        packet=json.loads(raw,object_pairs_hook=unique)
        if (type(packet) is not dict or set(packet)!={'schema','atr_version','archive_manifest_sha','rpd','atr','quarters'}
                or (packet['schema'],packet['atr_version'],packet['archive_manifest_sha'])!=(SCHEMA,VERSION,source.manifest_sha)):
            raise ValueError('C11_PREPARED_PREFIX_SCHEMA')
        rpd=RpdState.restore(json.dumps(packet['rpd'],allow_nan=False))
        cutoff=source.cutoff
        observed=rpd.observed
        if ((rpd.symbol,rpd.origin,rpd.next_open)!=(symbol,source.origin,cutoff)
                or not all(type(x) is int for x in (observed,rpd.n,now,grid_shift))
                or rpd.n!=(cutoff-source.origin)//FIVE
                or not cutoff<=observed<=now
                or (source.reviewed_at_ms is not None and not observed<=source.reviewed_at_ms<=now)
                or (source.review_expires_ms is not None and now>=source.review_expires_ms)):
            raise ValueError('C11_PREPARED_PREFIX_IDENTITY_OR_CLOCK')
        if set(packet['atr'])!={str(FIVE),str(QUARTER)}:
            raise ValueError('C11_PREPARED_PREFIX_ATR_WIDTHS')
        atr=[]
        for w in (FIVE,QUARTER):
            a=packet['atr'][str(w)]
            if (type(a) is not dict or set(a)!=set(new_atr())
                    or type(a['clock']) is not int or a['clock']!=cutoff
                    or type(a['n']) is not int or a['n']!=(cutoff-source.origin)//w
                    or any(type(a[k]) is not list or len(a[k])>20 or not all(number(v) for v in a[k])
                           for k in ('closes','trs'))
                    or any(a[k] is not None and not number(a[k]) for k in ('atr','multiplier','signed'))):
                raise ValueError('C11_PREPARED_PREFIX_ATR_STATE')
            atr.append((w,a|{'clock':cutoff+grid_shift}))
        available=max(observed,cutoff+grid_shift)
        q=packet['quarters']
        if (type(q) is not list or len(q)!=min(NW_WARM,(cutoff-source.origin)//QUARTER)
                or not q or grid_shift%QUARTER or available>now):
            raise ValueError('C11_PREPARED_PREFIX_WINDOW')
        bars=tuple(ClosedBar(**x) for x in q)
        if any(not b.valid(QUARTER) or b.received_ms!=observed or
               b.open_ms!=cutoff-(len(bars)-i)*QUARTER for i,b in enumerate(bars)):
            raise ValueError('C11_PREPARED_PREFIX_CLOSED_ROWS')
        if grid_shift:
            # Only the frozen replay translates the approved input clock.
            rpd=replace(rpd,origin=rpd.origin+grid_shift,next_open=cutoff+grid_shift,observed=available)
            bars=tuple(replace(b,open_ms=b.open_ms+grid_shift,received_ms=available) for b in bars)
        return PreparedPrefix(rpd,tuple(atr),bars)
    except (AttributeError, KeyError, TypeError, OverflowError) as error:
        raise ValueError('C11_PREPARED_PREFIX_SCHEMA') from error

def read(symbol,source,wall,cancelled,*,grid_shift=0):
    if cancelled.is_set():
        raise ValueError('COLD_CANCELLED')
    raw=read_bounded(source.directory/NAME,LIMIT,immutable=True)
    result=decode(raw,symbol=symbol,source=source,now=wall(),grid_shift=grid_shift)
    if cancelled.is_set():
        raise ValueError('COLD_CANCELLED')
    return result
