"""Fetch and classify only public Binance USD-M futures exchangeInfo."""
from exact_symbol import ID_CHAR
import hashlib
import http.client
import json
import re
import ssl
import time
from dataclasses import dataclass

from bridge_protocol import need
from collector import marker, NEGATIVE

HOST = 'fapi.binance.com'
PATH = '/fapi/v1/exchangeInfo'
MAX_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class Classification:
    verified: dict[str, str]
    body_sha256: str
    received_monotonic_ns: int


def classify(body, received):
    symbols = json.loads(body)['symbols']
    need(type(symbols) is list and 0 < len(symbols) <= 10000, 'C11_BINANCE_SYMBOLS')
    seen, verified = set(), {}
    for row in symbols:
        symbol = row['symbol']
        need(type(symbol) is str and 0 < len(symbol) <= 80 and symbol.isprintable()
             and symbol not in seen, 'C11_BINANCE_SYMBOL_IDENTITY')
        seen.add(symbol)
        subtypes = row.get('underlyingSubType')
        if type(subtypes) is not list or not all(type(x) is str for x in subtypes):
            continue
        labels = {marker(x) for x in subtypes}
        tags = row.get('tags', [])
        if type(tags) is not list or not all(type(x) is str for x in tags):
            continue
        labels.update(marker(x) for x in tags)
        labels.update(marker(row.get(k, 'UNKNOWN')) for k in ('underlyingType', 'contractType', 'asset_class'))
        base = row.get('baseAsset')
        if (row.get('underlyingType') != 'COIN' or 'CRYPTO' not in {marker(x) for x in subtypes}
                or labels & NEGATIVE or row.get('contractType') != 'PERPETUAL'
                or ('asset_class' in row and marker(row['asset_class']) != 'CRYPTO')
                or set(map(marker, tags)) - {'CRYPTO', 'RWA'}
                or row.get('status') != 'TRADING' or row.get('quoteAsset') != 'USDT'
                or row.get('marginAsset') != 'USDT' or type(base) is not str
                or not re.fullmatch(fr'{ID_CHAR}{{1,32}}', base) or symbol != f'{base}USDT'):
            continue
        verified[symbol] = f'{base}/USDT:USDT'
    need(bool(verified), 'C11_BINANCE_NO_VERIFIED_CRYPTO')
    return Classification(verified, hashlib.sha256(body).hexdigest(), received)


def fetch():
    connection = http.client.HTTPSConnection(HOST, timeout=7, context=ssl.create_default_context())
    try:
        connection.request('GET', PATH, headers={'Accept': 'application/json'})
        response = connection.getresponse()
        need(response.status == 200, f'C11_BINANCE_HTTP_{response.status}')
        need(response.getheader('Content-Type', '').split(';')[0].strip() == 'application/json',
             'C11_BINANCE_CONTENT_TYPE')
        body = response.read(MAX_BYTES + 1)
        need(0 < len(body) <= MAX_BYTES, 'C11_BINANCE_BODY_LIMIT')
        return classify(body, time.monotonic_ns())
    finally:
        connection.close()
