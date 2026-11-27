"""Disabled build-only exclusive unsigned native market transport; no top-level API."""
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from threading import get_ident
from native_shadow_queue import PrivateCCXTEndpoint
from capture import Capture,TICKET,query_for
from adapter import in_phase
from dedicated_queue import Job
from triad_lane import Job as TriadJob

KEYS=('apiKey','secret','password','uid','accountId','walletAddress','privateKey')

class ExclusiveUnsignedTransport:
    exclusive_parallelism=1
    def __init__(self,*,unsigned_client,incumbent_client,boundary_end,wall,mono,exact_transport_boundary,optional_wire=None):
        if (unsigned_client is incumbent_client or unsigned_client.session is incumbent_client.session or
            unsigned_client.id!='binance' or any(getattr(unsigned_client,k,None) for k in KEYS)):
            raise ValueError('EXCLUSIVE_UNSIGNED_CLIENT_REQUIRED')
        if exact_transport_boundary(unsigned_client) is not True:raise ValueError('BOUNDED_TRANSPORT_LIFECYCLE_NOT_ATTESTED')
        self.client=unsigned_client
        self.wall,self.mono=wall,mono
        self.worker=get_ident()
        self.claim=ContextVar('strict_unsigned_owned_claim',default=None)
        self.capture_for_physical_task=ContextVar('strict_unsigned_capture',default=None)
        actual_send=self.client.session.send
        def once(req,**kwargs):
            # Capture.install validated the exact PreparedRequest before this boundary.
            claim=self.claim.get()
            self.claim.set(None)
            if claim is None or claim() is not True:raise ValueError('CURRENT_SHARED_BUDGET_CLAIM_REQUIRED')
            try:
                reply=actual_send(req,**kwargs)
            except Exception:
                if optional_wire is not None:optional_wire.optional_after(req,None,kwargs)
                raise
            if optional_wire is not None:optional_wire.optional_after(req,reply,kwargs)
            return reply
        self.client.session.send=once
        if type(boundary_end) is not int or boundary_end%900_000 or boundary_end<=0:
            raise ValueError('STRICT_ONE_BOUNDARY_HANDLE_REQUIRED')
        self.capture=Capture(boundary_end,wall)
        self.endpoint=PrivateCCXTEndpoint(self.client,wall,mono)
        self.capture.install(self.endpoint,self.capture_for_physical_task.get)
        self.pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='c11-owned-unsigned')
        self.closed=False
        self.physical=set()

    def submit_original_public(self,j,token,claim_before_wire):
        if self.closed or get_ident()!=self.worker:raise RuntimeError('ORIGINAL_PUBLIC_QUEUE_OWNER')
        t=j.native_task
        if type(j) is not Job or j.end!=self.capture.end or not (in_phase(t) or type(t) is TriadJob) or not TICKET.fullmatch(j.request_id) or j.ticket!=j.request_id:
            raise ValueError('ORIGINAL_NATIVE_JOB_SCOPE')
        if (j.symbol,j.tf,j.generation,j.end,j.expires)!=(t.symbol,t.tf,t.generation,t.end,t.deadline):
            raise ValueError('ORIGINAL_NATIVE_JOB_MISMATCH')
        n=int(query_for(t)['limit'])
        if j.weight!=(1 if n<100 else 2):raise ValueError('ORIGINAL_NATIVE_WEIGHT_MISMATCH')
        if j.request_id.split('.')[1:]!=[str(t.end),str(t.ticket)]:raise ValueError('ORIGINAL_NATIVE_TICKET_MISMATCH')
        capture=self.capture
        def sent():
            cfg=self.claim.set(claim_before_wire)
            stamp=self.capture_for_physical_task.set(capture)
            try:
                r=self.endpoint.fetch(t,j.request_id)
                return r,capture.records.get(j.request_id)
            finally:
                self.capture_for_physical_task.reset(stamp)
                self.claim.reset(cfg)
        self.physical={f for f in self.physical if not f.done()}
        future=self.pool.submit(sent)
        self.physical.add(future)
        return future

    def rotate_boundary(self,end,authorized):
        if self.closed or get_ident()!=self.worker:raise RuntimeError('ORIGINAL_PUBLIC_QUEUE_OWNER')
        if (type(end) is not int or end%900_000 or end<=self.capture.end
                or any(not f.done() for f in self.physical) or authorized(end) is not True):
            return False
        self.physical.clear()
        self.capture=Capture(end,self.wall)
        return True

    def owned_proof_matches(self,j,result):
        r,p=result
        return (r.task is j.native_task and r.request_id==j.request_id and p is not None and
                p['capture_grade']=='VALIDATED_ROWS' and p['generation']==j.generation and p['end_ms']==j.end and
                p['request_id']==j.request_id and j.end<=r.requested_ms<=r.received_ms<=self.wall()<j.expires)

    def stop_and_join(self):
        if get_ident()!=self.worker:raise RuntimeError('ORIGINAL_PUBLIC_QUEUE_OWNER')
        self.closed=True
        self.pool.shutdown(wait=True,cancel_futures=True)
        self.client.session.close()
