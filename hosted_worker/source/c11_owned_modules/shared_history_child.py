"""One guarded public-only async CCXT child. No account or database access."""
import asyncio,os,re,socket,ssl,struct,sys,time
sys.path[:0]=['/opt/c11_owned_modules','/opt/c11_worker','/opt/c11_worker/ports',
              '/opt/c11_background_review','/freqtrade','/home/ftuser/.local/lib/python3.14/site-packages']
import aiohttp
from requests.adapters import DEFAULT_CA_BUNDLE_PATH
from exact_symbol import quote_contract_shape
from async_native_ccxt import SharedNativeAsync
from child_fs_profile import lock_system_reads
from owner_child_protocol import frame,strict_line,PeerHold
import stage_pin

def millis():
    return time.time_ns()//1000000

def decode_page(p):
    if type(p) is not dict or set(p)!={'symbol','start','stop','count','width'}:
        raise PeerHold('C11_SHARED_PAGE_SHAPE')
    s,a,b,n,w=(p[k] for k in ('symbol','start','stop','count','width'))
    if (not quote_contract_shape(s) or len(s)>32 or any(type(v) is not int for v in (a,b,n,w))
            or w!=60000 or not (0<a<b<=millis()) or a%900000 or b%900000
            or not 15<=n<=495 or b-a!=n*60000):
        raise PeerHold('C11_SHARED_CLOSED_1M_ONLY')
    return dict(symbol=s,interval='1m',startTime=a,endTime=b-1,limit=n)

async def serve(sock,abi,session):
    loop=asyncio.get_running_loop()
    e=SharedNativeAsync(session)
    buf=b''; parked={}; running={}; revoked=set(); write=asyncio.Lock()
    async def send(value,cap=None):
        data=frame(value) if cap is None else frame(value,cap)
        async with write:
            await loop.sock_sendall(sock,data)
    async def run(tag,params,until):
        try:
            status=0;used=None;rows=();at=millis(); wire_ms=None
            def authorize_wire():
                nonlocal wire_ms
                now=millis()
                if tag in revoked or now>=until:return False
                wire_ms=now;return True
            try:
                async with asyncio.timeout(max(0,(until-at)/1000)):
                    t,rows=await e.page(params,authorize_wire)
                    status,used,at=t.status or 0,t.weight,millis()
            except TimeoutError:
                at=millis()
            try:
                await send(dict(tag=tag,status=status,used=used,rows=rows,received=at,wire_ms=wire_ms))
            except PeerHold:
                await send(dict(tag=tag,status=status,used=used,rows=[],received=at,wire_ms=wire_ms),256)
        finally:
            running.pop(tag,None)
            revoked.discard(tag)
    try:
        while True:
            more=await loop.sock_recv(sock,4096)
            if not more:return
            buf+=more
            lines=buf.split(b'\n');buf=lines.pop()
            if len(buf)>=4096:raise PeerHold('C11_SHARED_COMMAND_TOO_LONG')
            for l in lines:
                x=strict_line(l+b'\n',4096)
                key=next((k for k in ('tag','go','cancel') if k in x),None)
                tag=x.get(key)
                if type(tag) is not str or re.fullmatch('[0-9a-f]{32}',tag) is None:
                    raise PeerHold('C11_SHARED_COMMAND_TAG')
                if set(x)=={'tag','page'}:
                    if tag in parked or tag in running:raise PeerHold('C11_SHARED_DUPLICATE_TAG')
                    parked[tag]=decode_page(x['page'])
                    await send({'ready':tag,'abi':abi},256)
                elif set(x)=={'go','until'}:
                    until=x['until'];p=parked.pop(tag,None);now=millis()
                    if p is not None and type(until) is int and now<until<=now+950:
                        running[tag]=asyncio.create_task(run(tag,p,until))
                    elif p is not None:
                        await send(dict(tag=tag,status=0,used=None,rows=[],received=now,wire_ms=None),256)
                elif set(x)=={'cancel'}:
                    parked.pop(tag,None)
                    # Do not cancel a partial socket frame and corrupt the peers.
                    if tag in running:revoked.add(tag)
                else:raise PeerHold('C11_SHARED_COMMAND_SHAPE')
    finally:
        tasks=tuple(running.values())
        for t in tasks:t.cancel()
        if tasks:await asyncio.gather(*tasks,return_exceptions=True)
        await e.close()

async def main(sock,abi):
    tls=ssl.create_default_context(cafile=DEFAULT_CA_BUNDLE_PATH)
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=tls),
                cookie_jar=aiohttp.DummyCookieJar(),trust_env=False) as session:
        await serve(sock,abi,session)

def guarded_main():
    with socket.socket(fileno=1) as sock:
        peer=struct.unpack('3i',sock.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,struct.calcsize('3i')))
        if peer!=(os.getppid(),1000,2000):raise PeerHold('C11_SHARED_CHILD_PEER_MISMATCH')
        abi,_=lock_system_reads(DEFAULT_CA_BUNDLE_PATH)
        sock.setblocking(False)
        asyncio.run(main(sock,abi))

if __name__=='__main__':
    if stage_pin.AUTH_SHA is None:raise SystemExit(23)
    if sys.argv[1:]!=['--fd','1']:raise SystemExit(24)
    guarded_main()
