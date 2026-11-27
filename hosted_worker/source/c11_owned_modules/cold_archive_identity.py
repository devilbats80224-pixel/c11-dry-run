"""Canonical static-archive identity for the raw native contract ID."""
import re
from urllib.parse import quote, unquote, urlsplit

from exact_symbol import quote_contract_shape

ARCHIVE_ROOT='https://data.binance.vision/data/futures/um'
PERIOD=re.compile(r'[0-9]{4}-[0-9]{2}(?:-[0-9]{2})?')


def archive_location(symbol,period):
    if not quote_contract_shape(symbol) or type(period) is not str or not PERIOD.fullmatch(period):
        raise ValueError('COLD_ARCHIVE_COMPONENT')
    distribution='daily' if len(period)==10 else 'monthly'
    filename=f'{symbol}-1m-{period}.zip'
    url=f'{ARCHIVE_ROOT}/{distribution}/klines/{quote(symbol,safe="")}/1m/{quote(filename,safe="")}'
    return filename,url


def official_archive_url(url):
    if type(url) is not str or any(ord(c)<32 for c in url):
        return False
    try:
        u=urlsplit(url)
        path=u.path.strip('/').split('/')
        if (u.scheme,u.netloc,u.query,u.fragment)!=('https','data.binance.vision','','') or len(path)!=8:
            return False
        symbol=unquote(path[5],errors='strict')
        filename=unquote(path[7],errors='strict')
        sidecar=filename.endswith('.CHECKSUM')
        filename=filename.removesuffix('.CHECKSUM')
        prefix=f'{symbol}-1m-'
        if not filename.startswith(prefix) or not filename.endswith('.zip'):
            return False
        _,expected=archive_location(symbol,filename[len(prefix):-4])
        return url==expected+('.CHECKSUM' if sidecar else '')
    except (ValueError,UnicodeError):
        return False
