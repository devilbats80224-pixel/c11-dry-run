"""Candidate pre-wire metering; no change or access to any account/private activity."""
from urllib.parse import urlsplit, parse_qsl
from collections.abc import Mapping

from weight_catalog import classify_official
from native_shadow_queue import retry_ms
import refresh

class PublicWire:
    def __init__(self,budget,checked_public,wall,verified_weight_limit):
        self.budget,self.checked_public,self.wall,self.limit=budget,checked_public,wall,verified_weight_limit
        from native_refresh_mailbox import NativeRefreshMailbox
        self.history_refresh=NativeRefreshMailbox()
    def before(self,url,method,headers,body):
        # Preserve the installed public-only guard before any accounting.
        self.checked_public(str(url),method,headers,body)
        dose=classify_official(url)
        self.budget.account_incumbent_before_wire(dose.weight_1m,funding_requests=dose.funding_requests_5m)
        return dose
    def after(self,response):
        headers=response.headers
        if not isinstance(headers,Mapping):return
        if getattr(response,'status_code',getattr(response,'status',0))!=200:return
        if urlsplit(str(response.url)).netloc!='fapi.binance.com':return
        value=next((v for k,v in headers.items() if k.casefold()=='x-mbx-used-weight-1m'),None)
        if type(value) is not str or not value.isascii() or not value.isdigit():return
        self.budget.note_authoritative_ip_header(self.wall(),int(value),self.limit())
        return True

    def optional_after(self,prepared,response,kwargs):
        # The shared budget was debited at exact prepared send, not again here.
        # Only exact TLS receipt may replenish minute high-water telemetry.
        if (response is not None and kwargs.get('verify') is True and not response.history and
                response.url==prepared.url):
            status=response.status_code
            if status==200 and self.after(response):return
            self.budget.optional_wire_fault(status,retry_ms(response.headers,self.wall()) or 0)
        else:self.budget.optional_wire_fault(0,0)

    def wrap_sync_send(self,actual):
        def send(prepared,**kwargs):
            classifier=refresh.begin(self,prepared,kwargs)
            dose=self.before(prepared.url,prepared.method,prepared.headers,prepared.body)
            if classifier is not None:
                kwargs={**kwargs,'allow_redirects':False,'timeout':(7,7),'stream':True}
            try:response=actual(prepared,**kwargs)
            except Exception:
                self.budget.mark_unaccounted()
                raise
            if classifier is not None:
                refresh.finish(self,classifier,response,kwargs)
                return response
            # On physical transport, never use CCXT's mutable last-response slot.
            if not (kwargs.get('verify') is True and not response.history and
                    response.url==prepared.url and ((dose.policy=='IGNORE_1M_HEADER' and getattr(response,'status_code',getattr(response,'status',0))==200) or (dose.policy!='IGNORE_1M_HEADER' and self.after(response)))):
                self.budget.mark_unaccounted()
            return response
        return send

    def wrap_async_request(self,actual,verified_ssl,default_ssl):
        # Install on the exact coroutine _request, not ClientSession.get:
        # CCXT's async-with semantics remain owned by the installed aiohttp.
        async def request(method,url,**kwargs):
            dose=self.before(url,method,kwargs.get('headers'),kwargs.get('data'))
            try:response=await actual(method,url,**kwargs)
            except BaseException:
                self.budget.mark_unaccounted()
                raise
            tls=kwargs.get('ssl',default_ssl)
            verified=(verified_ssl(tls) and not response.history and
                    str(response.url)==str(url) and ((dose.policy=='IGNORE_1M_HEADER' and getattr(response,'status_code',getattr(response,'status',0))==200) or (dose.policy!='IGNORE_1M_HEADER' and self.after(response))))
            if not verified:
                self.budget.mark_unaccounted()
            else:
                await self.history_refresh.capture_verified(method,str(url),response,self.wall())
            return response
        return request
