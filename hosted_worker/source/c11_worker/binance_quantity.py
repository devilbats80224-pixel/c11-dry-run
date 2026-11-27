"""Official Binance public current 5m quantity. No historical backfill API."""
import http.client
import json
import ssl
from urllib.parse import urlencode

from bridge_protocol import need
from observer_store import FIVE, SYMBOL, wall_ms

HOST = 'fapi.binance.com'
PATH = '/futures/data/openInterestHist'


def latest_quantity(symbol):
    need(type(symbol) is str and SYMBOL.fullmatch(symbol) and symbol.endswith('USDT'),
         'C11_QUANTITY_VERIFIED_SYMBOL_REQUIRED')
    query = urlencode({'symbol':symbol,'period':'5m','limit':'1'})
    connection = http.client.HTTPSConnection(HOST, timeout=5, context=ssl.create_default_context())
    try:
        requested = wall_ms()
        connection.request('GET', PATH+'?'+query,headers={'Accept':'application/json'})
        response = connection.getresponse()
        need(response.status==200,f'C11_QUANTITY_HTTP_{response.status}')
        need(response.getheader('Content-Type','').split(';')[0].strip()=='application/json',
             'C11_QUANTITY_CONTENT_TYPE')
        raw = response.read(32769)
        seen = wall_ms()
        need(0<len(raw)<=32768,'C11_QUANTITY_SIZE')
        rows = json.loads(raw)
        need(type(rows) is list and len(rows)==1,'C11_QUANTITY_RESPONSE')
        return requested//FIVE*FIVE,requested,seen,rows[0]
    finally:
        connection.close()
