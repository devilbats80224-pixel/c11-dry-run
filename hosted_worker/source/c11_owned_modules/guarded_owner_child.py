"""Only the approved installed parent can start this role in a reviewed release."""
import os,signal,socket,struct,sys,time
sys.path[:0]=['/opt/c11_owned_modules','/opt/c11_worker','/opt/c11_worker/ports','/opt/c11_background_review','/freqtrade','/home/ftuser/.local/lib/python3.14/site-packages']
from child_fs_profile import lock_system_reads
from owner_child_protocol import frame,strict_line,PeerHold,CAP
from public_history_ccxt import ClosedPage,installed_unsigned,checked_public_one
from requests.adapters import DEFAULT_CA_BUNDLE_PATH
import stage_pin

def serve(sock):
    if stage_pin.AUTH_SHA is None:
        raise PeerHold('C11_HISTORY_CHILD_NO_AUTHORIZED_RELEASE')
    that=struct.unpack('3i',sock.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,struct.calcsize('3i')))
    if that!=(os.getppid(),1000,2000):raise PeerHold('C11_HISTORY_CHILD_PEER_MISMATCH')
    # After checking peer/socket, leave only standard FDs for policy and the
    # existing private pipe. The fd is treated as stdout for descriptor audit.
    chan=sock.detach()
    if chan!=1:
        os.dup2(chan,1,inheritable=False);os.close(chan)
    duplex=socket.socket(fileno=1)
    client=installed_unsigned()
    abi,_=lock_system_reads(DEFAULT_CA_BUNDLE_PATH)
    duplex.settimeout(12.0)
    with duplex.makefile('rb') as f:
        req=strict_line(f.readline(4097),4096)
        if set(req)!={'tag','page'} or not isinstance(req['tag'],str) or len(req['tag'])!=32:
            raise PeerHold('C11_HISTORY_CHILD_PAGE_TAG')
        page=ClosedPage(**req['page'])
        duplex.sendall(frame({'ready':req['tag'],'abi':abi}))
        go=strict_line(f.readline(256),255)
        if go!={'go':req['tag']}:raise PeerHold('C11_HISTORY_CHILD_GO_DENIED')
        # SIGALRM default terminates this disposable process even if DNS or
        # a blocked socket does not return within ordinary read timeout.
        signal.setitimer(signal.ITIMER_REAL,.95)
        out=checked_public_one(client,page,lambda:True)
        sent_at=time.time_ns()//1_000_000
        duplex.sendall(frame({'tag':req['tag'],'status':out.status,'used':out.used,'rows':out.rows,'received':sent_at}))

if __name__=='__main__':
    if stage_pin.AUTH_SHA is None:raise SystemExit(23)
    if sys.argv[1:]!=['--fd','1']:raise SystemExit(24)
    serve(socket.socket(fileno=1))
