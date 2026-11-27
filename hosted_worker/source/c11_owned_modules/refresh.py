"""Inactive comparator: substitute only the original due refresh, with one send."""
from contextvars import ContextVar
from time import monotonic_ns
from public_market import classify, MAX_BYTES
ENDPOINT='https://fapi.binance.com/fapi/v1/exchangeInfo'
current=ContextVar('exact_due_classifier',default=None)
def fetch_on_metered_client(api,wire,installed_now,*,mono=monotonic_ns):
    if not installed_now() or api.options.get('maxRetriesOnFailure',0)!=0:
        raise ValueError('EXACT_NATIVE_METER_AND_NO_RETRIES_REQUIRED')
    track=dict(wire=wire,attempts=0,raw=None,mono=mono)
    if current.get() is not None:raise ValueError('NESTED_CLASSIFIER')
    token=current.set(track)
    try:
        api.fapiPublicGetExchangeInfo()
        if track['attempts']!=1 or track['raw'] is None:raise ValueError('CLASSIFIER_WITHOUT_VERIFIED_RAW_REPLY')
        original_body,at=track['raw']
        return classify(original_body,at)
    finally:
        current.reset(token)

def begin(wire,prepared,kwargs):
    track=current.get()
    if track is None or track['wire'] is not wire:return None
    if (track['attempts'] or prepared.url!=ENDPOINT or prepared.method!='GET' or
        prepared.body not in (None,b'', '') or kwargs.get('verify') is not True or
        any(x.lower() in ('x-mbx-apikey','authorization','cookie') for x in prepared.headers)):
        raise ValueError('NO_SECOND_OR_UNTRUSTED_CLASSIFIER_SEND')
    track['attempts']=1
    return track,wire.wall()

def finish(wire,seed,response,kw):
    track,start=seed
    if response.status_code in (418,429):
        from native_shadow_queue import retry_ms
        wire.budget.optional_wire_fault(response.status_code,retry_ms(response.headers,wire.wall()) or 0)
    # Clock disagreement conservatively never seeds the venue minute cache.
    if (response.status_code!=200 or response.history or response.url!=ENDPOINT or
        wire.wall()//60_000!=start//60_000 or
        response.headers.get('Content-Type','').split(';')[0].strip()!='application/json'):
        wire.budget.mark_unaccounted()
        return
    raw=bytearray()
    try:
        declared=response.headers.get('Content-Length')
        if declared is not None and (not declared.isdecimal() or int(declared)>MAX_BYTES):
            raise ValueError('CLASSIFIER_BODY_LIMIT')
        for block in response.iter_content(65536):
            raw.extend(block)
            if len(raw)>MAX_BYTES:raise ValueError('CLASSIFIER_BODY_LIMIT')
        if not raw:raise ValueError('CLASSIFIER_BODY_LIMIT')
        body=bytes(raw)
        response._content=body
        response._content_consumed=True
    except Exception:
        wire.budget.mark_unaccounted()
        raise
    finally:response.close()
    if wire.wall()//60_000 == start//60_000 and wire.after(response):
        track['raw']=body,track['mono']()
    else:wire.budget.mark_unaccounted()
