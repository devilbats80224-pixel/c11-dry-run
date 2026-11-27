"""Read the archive catalogue sealed by the existing paper release manifest."""
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import stat
from cold_archive_identity import archive_location
from exact_symbol import quote_contract_shape

ROOT = Path('/opt/c11-reviewed-cold-prefix')
INDEX = 'catalog.json'
SCHEMA = 'c11-reviewed-cold-prefix-v1'
EXCHANGE_INFO = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
Q = 900_000
HASH = re.compile('[a-f0-9]{64}')
DATE = re.compile(r'\d{4}-\d{2}(?:-\d{2})?')
CONTRACT_KEYS = {
    'symbol', 'baseAsset', 'quoteAsset', 'marginAsset', 'contractType', 'onboardDate', 'status',
}


def require(condition, code):
    if not condition:
        raise ValueError(code)


def unique(pairs):
    fields = {}
    for key,value in pairs:
        require(key not in fields,'C11_COLD_DUPLICATE_FIELD')
        fields[key] = value
    return fields


def parse(raw):
    return json.loads(raw,object_pairs_hook=unique)


def digest(raw):
    return sha256(raw).hexdigest()


def pinned(raw, expected):
    return type(expected) is str and HASH.fullmatch(expected) is not None and digest(raw)==expected


def record(raw, pin, now):
    require(pinned(raw,pin),'C11_COLD_CATALOG_PIN')
    data = parse(raw)
    require(type(data) is dict and set(data)=={'schema','sources'} and data['schema']==SCHEMA
            and type(data['sources']) is dict,'C11_COLD_CATALOG_SHAPE')
    for symbol,src in data['sources'].items():
        keys={'origin_ms','cutoff_ms','contract_sha256','manifest_sha256','reviewed_at_ms','use_until_ms'}
        require(quote_contract_shape(symbol) and type(src) is dict
                and (set(src)==keys or set(src)==keys|{'transport'} and
                     src['transport']=='pinned_official_download'), 'C11_COLD_SOURCE_SHAPE')
        origin,cutoff,reviewed,expires=(src[k] for k in ('origin_ms','cutoff_ms','reviewed_at_ms','use_until_ms'))
        require(all(type(x) is int for x in (origin,cutoff,reviewed,expires))
                and origin%Q==cutoff%Q==0 and 0<origin<cutoff<=reviewed<=now<expires,
                'C11_COLD_SOURCE_CLOCK')
        require(all(type(src[k]) is str and HASH.fullmatch(src[k]) is not None
                    for k in ('contract_sha256','manifest_sha256')),'C11_COLD_SOURCE_PIN')
    return data['sources']


def checked_contract(raw, symbol, src):
    require(pinned(raw,src['contract_sha256']),'C11_COLD_CONTRACT_PIN')
    evidence = parse(raw)
    require(type(evidence) is dict and set(evidence)=={'source','actual_received_ms','payload'}
            and evidence['source']==EXCHANGE_INFO,'C11_COLD_CONTRACT_SOURCE')
    available=evidence['actual_received_ms']
    require(type(available) is int and src['origin_ms']<=available<=src['reviewed_at_ms'],
            'C11_COLD_CONTRACT_OBSERVATION')
    require(type(evidence['payload']) is dict and set(evidence['payload'])==CONTRACT_KEYS
            and evidence['payload']==dict(symbol=symbol,baseAsset=symbol[:-4],quoteAsset='USDT',
                marginAsset='USDT',contractType='PERPETUAL',onboardDate=src['origin_ms'],status='TRADING'),
            'C11_COLD_CONTRACT_IDENTITY')


def checked_parts(raw, symbol, src):
    require(pinned(raw,src['manifest_sha256']),'C11_COLD_ARCHIVE_PIN')
    parts = parse(raw)
    require(type(parts) is list and 0<len(parts)<=60,'C11_COLD_ARCHIVE_SHAPE')
    names = set()
    weight = 0
    for part in parts:
        require(type(part) is dict and set(part)=={'date','frequency','name','sha256','url','bytes',
                                                   'actual_received_ms'},'C11_COLD_ARCHIVE_PART_SHAPE')
        date,frequency,name=(part[key] for key in ('date','frequency','name'))
        require(type(date) is str and DATE.fullmatch(date) is not None
                and frequency==('daily' if len(date)==10 else 'monthly')
                and name not in names and (name,part['url'])==archive_location(symbol,date),
                'C11_COLD_ARCHIVE_IDENTITY')
        start=datetime.strptime(date,'%Y-%m-%d' if frequency=='daily' else '%Y-%m').replace(tzinfo=timezone.utc)
        if frequency=='daily':
            stop=start+timedelta(days=1)
        else:
            stop=start.replace(year=start.year+(start.month==12),month=start.month%12+1)
        last_close=min(src['cutoff_ms'],int(stop.timestamp())*1000)
        available,size=part['actual_received_ms'],part['bytes']
        require(type(available) is int and max(src['origin_ms'],last_close)<=available<=src['reviewed_at_ms']
                and type(size) is int and 0<size<=2*1024*1024
                and type(part['sha256']) is str and HASH.fullmatch(part['sha256']) is not None,
                'C11_COLD_ARCHIVE_ACQUISITION')
        names.add(name)
        weight+=size
    require(weight<=26*1024*1024 and max(p['actual_received_ms'] for p in parts)>=src['cutoff_ms'],
            'C11_COLD_ARCHIVE_OBSERVATION')
    return parts


def verified_catalog(root, pin, now, read_file, *, verify_content):
    """Parse all metadata with the same policy in packaging and readback."""
    sources=record(read_file(root/INDEX,128*1024),pin,now)
    parts_by_symbol={}
    for symbol,src in sources.items():
        base=root/symbol
        checked_contract(read_file(base/'contract.json',4096),symbol,src)
        parts=checked_parts(read_file(base/'manifest.json',128*1024),symbol,src)
        if verify_content and src.get('transport')!='pinned_official_download':
            for part in parts:
                name=part['name']
                content=read_file(base/name,part['bytes'])
                checksum=read_file(base/(name+'.CHECKSUM'),512)
                h=part['sha256']
                require(len(content)==part['bytes'] and pinned(content,h)
                        and checksum.decode('utf-8').split() in ([h,name],[h,'*'+name]),
                        'C11_COLD_ARCHIVE_BYTES')
        parts_by_symbol[symbol]=parts
    return sources,parts_by_symbol


def read_bounded(path,maximum,*,immutable):
    parent=path.parent
    st=parent.lstat()
    require(stat.S_ISDIR(st.st_mode),'C11_COLD_DIRECTORY')
    if immutable:
        require(st.st_uid==st.st_gid==0 and not st.st_mode&0o022
                and os.statvfs(parent).f_flag&os.ST_RDONLY,'C11_COLD_NOT_IMMUTABLE')
    with os.fdopen(os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK),'rb') as f:
        st=os.fstat(f.fileno())
        require(stat.S_ISREG(st.st_mode) and st.st_nlink==1 and 0<st.st_size<=maximum,'C11_COLD_FILE_METADATA')
        if immutable:
            require(st.st_uid==st.st_gid==0 and stat.S_IMODE(st.st_mode)==0o444
                    and os.fstatvfs(f.fileno()).f_flag&os.ST_RDONLY,'C11_COLD_NOT_IMMUTABLE')
        raw=f.read(maximum+1)
        require(len(raw)==st.st_size,'C11_COLD_FILE_SIZE')
        return raw


def installed_sources(verified_manifest, now, *, verify_content=False):
    from cold_prefix import ClosedPrefixSource
    for line in Path('/proc/self/mountinfo').read_text().splitlines():
        target=Path(line.split(' - ',1)[0].split()[4])
        if target!=Path('/') and (target.is_relative_to(ROOT) or ROOT.is_relative_to(target)):
            raise ValueError('C11_COLD_BAKED_ROOT_REQUIRED')
    pin=verified_manifest['files'].get(str(ROOT/INDEX))
    require(pin is not None,'C11_COLD_CATALOG_UNPINNED')
    sources,_=verified_catalog(ROOT,pin,now,lambda path,n:read_bounded(path,n,immutable=True),
                               verify_content=verify_content)
    return {symbol:ClosedPrefixSource(ROOT/symbol,src['manifest_sha256'],src['origin_ms'],
                                      src['cutoff_ms'],review_expires_ms=src['use_until_ms'],
                                      download_only=src.get('transport')=='pinned_official_download')
            for symbol,src in sources.items()}
