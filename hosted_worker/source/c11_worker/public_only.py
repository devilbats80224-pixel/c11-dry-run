"""Enforce public GET transport before either CCXT client opens a connection."""
from inspect import iscoroutinefunction
from urllib.parse import parse_qsl, urlsplit

from freqtrade.exceptions import OperationalException

PATHS = frozenset('/fapi/' + version + '/' + name for version, name in (
    ('v1', 'exchangeInfo'), ('v1', 'time'), ('v1', 'ticker/24hr'), ('v1', 'ticker/price'),
    ('v1', 'ticker/bookTicker'), ('v1', 'depth'), ('v1', 'klines'), ('v1', 'markPriceKlines'),
    ('v1', 'indexPriceKlines'), ('v1', 'premiumIndex'), ('v1', 'fundingRate'),
    ('v1', 'aggTrades'), ('v2', 'ticker/price'), ('v1', 'fundingInfo'),
))
CCXT_KEYS = ('apiKey', 'secret', 'password', 'uid', 'accountId', 'walletAddress', 'privateKey')


def check_request(url, method='GET', headers=None, body=None):
    u = urlsplit(url)
    if (method != 'GET' or body is not None or u.scheme != 'https'
            or u.netloc != 'fapi.binance.com' or u.path not in PATHS or u.fragment
            or any(k.lower() in ('authorization', 'x-mbx-apikey') for k in (headers or {}))
            or any(k.lower() in ('signature', 'apikey') for k, _ in parse_qsl(u.query))):
        raise OperationalException('C11_BINANCE_PUBLIC_GET_ONLY')


def guard_client(client):
    if client.id != 'binance' or any(getattr(client, name, None) for name in CCXT_KEYS):
        raise OperationalException('C11_BINANCE_NO_ORDER_CREDENTIALS')
    original_fetch = client.fetch
    if iscoroutinefunction(original_fetch):
        async def fetch(url, method='GET', headers=None, body=None):
            check_request(url, method, headers, body)
            return await original_fetch(url, method, headers, body)
    else:
        def fetch(url, method='GET', headers=None, body=None):
            check_request(url, method, headers, body)
            return original_fetch(url, method, headers, body)
    client.fetch = fetch
    return client
