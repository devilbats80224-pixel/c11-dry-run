"""Owner-side multiplexing. Each page retains its usual budget and freshness checks."""
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from secrets import token_hex
from time import monotonic
import json,os,signal,socket,subprocess,sys

from owner_child_protocol import CAP,frame,strict_line

POLL=12.
MAX_RESPONSE_MS=1200

class Peer:
    def __init__(self):
        self.pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='c11-shared-history')
        self.process=None
    def _start(self):
        p,c=socket.socketpair()
        try:
            self.process=subprocess.Popen(
                [sys.executable,'-I','/opt/c11_owned_modules/shared_history_child.py','--fd','1'],
                stdin=subprocess.DEVNULL,stdout=c,stderr=subprocess.DEVNULL,cwd='/tmp',
                env={'OPENBLAS_NUM_THREADS':'1','OMP_NUM_THREADS':'1'},
                close_fds=True,start_new_session=True)
        except BaseException:
            p.close();raise
        finally:c.close()
        p.setblocking(False)
        return p
    def start(self):
        return self.pool.submit(self._start)
    def retire(self):
        p=self.process
        if p is not None and p.poll() is None:
            try:os.killpg(p.pid,signal.SIGKILL)
            except ProcessLookupError:pass
        if p is not None:
            self.pool.submit(p.wait)
    def close(self):
        self.pool.shutdown(wait=False)

def owner_may_receive(lane,job,granted):
    # An ungranted request always needs current rank/phase. An exact data
    # job already granted before expiry may carry separately verified,
    # data-only receipt authority. No effect on original strategy scopes.
    if granted is not None:
        from admission_minute_job import MinuteHistoryJob
        allowed=getattr(lane,'receipt_authority',None)
        if type(job) is MinuteHistoryJob and callable(allowed) and allowed(job,granted) is True:
            return True
    return lane.authority(job)


class Lane:
    def __init__(self,wire,authority):
        self.wire,self.authority=wire,authority
        self.tag=None
        self.result=None
        self.last_offer_reason=None
        self.archive_retry_eligible=False
        self.receipt_authority=None
    def offer(self,job):
        return self.wire.offer(self,job)
    def tick(self):
        self.wire.pump()
        x,self.result=self.result,None
        return x
    def close(self):
        if self.tag is not None:
            self.wire.finish(self.tag,None,cancel=True)

class SharedHistoryForwarder:
    def __init__(self,budget,wall,start=None,*,trace=None):
        self.budget,self.wall=budget,wall
        self.peer=Peer() if start is None else None
        self.start=start or self.peer.start
        self.future=None;self.sock=None;self.closed=False
        self.waiting={};self.tx=deque();self.rx=b''
        self.trace=trace
        self.trace_window=None
        self.trace_seen=set()
        self.gate_seen=set()
    def _emit(self, event, key, seen, limit):
        # One current wall-clock bucket even when an old job is being retired.
        window = event['at_ms']//900_000
        if self.trace_window != window:
            self.trace_window = window
            self.trace_seen.clear()
            self.gate_seen.clear()
        if key in seen or len(seen) >= limit:
            return
        seen.add(key)
        try:
            self.trace(event)
        except Exception:
            # Diagnostics never take ownership of scheduling or protection.
            pass

    def gate(self, reason, fact=None):
        if self.trace is None:
            return
        now = self.wall()
        end = now//900_000*900_000
        from retained_page_schema import ARCHIVE_EVIDENCE_REASONS
        if not end <= now < end+(900_000 if reason in ARCHIVE_EVIDENCE_REASONS else 60_000):
            return
        event = dict(schema='c11-primary-schedule-v1', end_ms=end, at_ms=now, reason=reason)
        if fact is not None:
            event.update(symbol=fact.symbol, generation=fact.generation)
        key = (event.get('generation'), event.get('symbol'), reason)
        self._emit(event, key, self.gate_seen, 128)

    def audit(self,job,stage,reason=None,**values):
        if self.trace is None:return
        end=getattr(job,'end',None)
        key=(end,getattr(job,'generation',None),job.symbol,getattr(job,'start',None),
             getattr(job,'stop',None),stage,reason)
        data=dict(schema='c11-shared-history-page-v1',symbol=job.symbol,
            end_ms=end,generation=getattr(job,'generation',None),
            page_start_ms=getattr(job,'start',None),page_end_ms=getattr(job,'stop',None),
            requested_rows=job.n,stage=stage,at_ms=self.wall())
        if reason is not None:data['stage_reason']=reason
        self._emit(data|values,key,self.trace_seen,768)
    def lane(self,authority):
        return Lane(self,authority)
    def offer(self,lane,job):
        lane.last_offer_reason='HISTORY_PAGE_HELD'
        lane.archive_retry_eligible=False
        if self.closed or lane.tag is not None or not lane.authority(job):return False
        token=self.budget.try_reserve_history(job,cold_current=lane.authority)
        if token is None:
            lane.last_offer_reason=self.budget.last_history_hold
            self.audit(job,'RESERVE_REJECTED',lane.last_offer_reason);return False
        tag=token_hex(16)
        lane.tag=tag;lane.last_offer_reason=None
        self.waiting[tag]=[lane,job,token,monotonic()+POLL,None]
        self.audit(job,'RESERVED')
        self.tx.append((frame({'tag':tag,'page':job.child()},4096),None))
        if self.sock is None and self.future is None:
            self.future=self.start()
        return True
    def finish(self,tag,payload,*,cancel=False):
        lane,job,token,_,_=self.waiting.pop(tag)
        self.budget.release_if_never_sent(token)
        lane.tag=None;lane.result=(job,payload)
        if cancel and (self.sock is not None or self.future is not None):
            self.tx.append((frame({'cancel':tag},256),None))
    def inbound(self,x):
        tag=x.get('ready',x.get('tag'))
        if type(tag) is not str or tag not in self.waiting:return
        lane,job,token,delivery_until,sent=self.waiting[tag]
        base={'tag','status','used','rows','received'}
        valid_keys=set(x) in (base,base|{'wire_ms'}) and sent is not None
        status=x.get('status')
        status=status if type(status) is int else None
        used=x.get('used')
        received=x.get('received')
        count=len(x['rows']) if type(x.get('rows')) is list else None
        wire_ms=x.get('wire_ms')
        evidence=dict(http_status=status,reply_rows=count,grant_ms=sent,
                      wire_ms=wire_ms if type(wire_ms) is int else None,
                      response_ms=received if type(received) is int else None)
        if valid_keys and x['status'] in (418,429):
            self.budget.optional_wire_fault(x['status'],60000)
        authority = owner_may_receive(lane,job,sent)
        parent_now = monotonic()
        if not authority or parent_now >= delivery_until:
            reason = ('SCOPE_AND_PARENT_DELIVERY_EXPIRED' if not authority and parent_now >= delivery_until
                      else 'SCOPE_INVALID_AT_READ' if not authority else 'PARENT_DELIVERY_EXPIRED')
            margin_ms = round((delivery_until-parent_now)*1000)
            self.audit(job,'REPLY_REJECTED',reason,
                       parent_delivery_margin_ms=max(-600000,min(600000,margin_ms)),**evidence)
            self.finish(tag,None,cancel=True);return
        if set(x)=={'ready','abi'} and sent is None and type(x['abi']) is int and x['abi']>=3:
            self.audit(job,'READY_READ')
            now=self.wall()
            expires=getattr(job,'expires',now+950)
            if now>=expires or not self.budget.claim_history_send(token,job,cold_current=lane.authority):
                reason=('HISTORY_MARKER_OR_CURRENT_REFUSED' if now>=expires else
                        getattr(self.budget,'last_history_hold',None) or 'SCOPE_OR_SHARED_BUDGET')
                self.audit(job,'GRANT_REJECTED',reason)
                self.finish(tag,None,cancel=True);return
            self.waiting[tag][4]=now
            # Transport timestamp and owner polling are different clocks.
            # Keep the existing bounded IPC housekeeping window. The actual
            # child wire deadline and admissible receipt timestamp are unchanged.
            self.waiting[tag][3]=monotonic()+POLL
            self.tx.append((frame({'go':tag,'until':min(expires,now+950)},256),(job,now)))
            self.audit(job,'GRANT_QUEUED',grant_ms=now);return
        if not valid_keys: reason='PROTOCOL'
        elif status!=200: reason='HTTP_STATUS' if status else 'NO_HTTP_RECEIPT' if type(wire_ms) is int else 'NOT_DISPATCHED_BY_CHILD'
        elif type(used) is not int or not 0<=used<=50000: reason='WEIGHT_HEADER'
        elif type(received) is not int or not sent<=received<=min(self.wall(),sent+MAX_RESPONSE_MS): reason='RECEIPT_TIME'
        elif getattr(job,'bootstrap',False) is True and (type(wire_ms) is not int or not sent<=wire_ms<=received):
            reason='BOOTSTRAP_WIRE_TIME'
        elif count!=job.n: reason='ROW_COUNT'
        else:
            self.audit(job,'REPLY_ACCEPTED',**evidence)
            self.finish(tag,(sent,received,x['rows']));return
        # A rejected result with a witnessed request may use independently
        # verified archived data. Budget refusal, stale scope, malformed IPC
        # and rate enforcement are not evidence that the endpoint is short.
        lane.archive_retry_eligible=(
            getattr(job,'bootstrap',False) is True and valid_keys
            and type(wire_ms) is int and sent<=wire_ms<=self.wall()
            and (reason in ('ROW_COUNT','NO_HTTP_RECEIPT')
                 or (reason=='HTTP_STATUS' and status in (404,500,502,503,504))))
        self.audit(job,'REPLY_REJECTED',reason,**evidence)
        self.finish(tag,None,cancel=True)
    def abort(self):
        if self.sock is not None:self.sock.close();self.sock=None
        if self.peer is not None:self.peer.retire()
        self.tx.clear();self.rx=b''
        for tag,(_,job,_,_,sent) in tuple(self.waiting.items()):
            self.audit(job,'PEER_ENDED','WITHOUT_REPLY' if sent is not None else 'BEFORE_GRANT')
            self.finish(tag,None)
    def _expire(self):
        for tag,(lane,job,_,until,sent) in tuple(self.waiting.items()):
            active=owner_may_receive(lane,job,sent)
            if not active or monotonic()>=until:
                self.audit(job,'EXPIRED','STALE_SCOPE' if not active else 'NO_REPLY' if sent is not None else 'NO_READY',grant_ms=sent)
                self.finish(tag,None,cancel=True)
    def pump(self):
        if self.future is not None:
            if not self.future.done():self._expire();return
            f,self.future=self.future,None
            try:self.sock=f.result()
            except OSError:self.abort();return
        if self.sock is None:self._expire();return
        try:
            # Bound work per poll on the owner thread; later polls drain more.
            for _ in range(16):
                try:chunk=self.sock.recv(65536)
                except BlockingIOError:break
                if not chunk:raise EOFError()
                self.rx+=chunk
                lines=self.rx.split(b'\n');self.rx=lines.pop()
                if len(self.rx)>=CAP:raise ValueError('C11_SHARED_RESPONSE_CAP')
                for line in lines:self.inbound(strict_line(line+b'\n'))
            self._expire()
            for _ in range(min(64,len(self.tx))):
                payload,grant=self.tx[0]
                try:n=self.sock.send(payload)
                except BlockingIOError:break
                if not n:raise EOFError()
                if n==len(payload):
                    self.tx.popleft()
                    if grant is not None:
                        job,at=grant
                        self.audit(job,'GRANT_WRITTEN',grant_ms=at)
                else:
                    self.tx[0]=(payload[n:],grant)
        except (OSError,EOFError,ValueError,TypeError):self.abort()
    def close(self):
        self.closed=True
        f,self.future=self.future,None
        self.abort()
        if f is not None:
            def reap(f):
                try:f.result().close()
                except OSError:pass
                if self.peer is not None:self.peer.retire();self.peer.close()
            f.add_done_callback(reap)
        if f is None and self.peer is not None:self.peer.close()

def retained_page_trace(sink):
    if sink is None:
        return None

    def emit(event):
        sink((json.dumps(event, separators=(',', ':'))+'\n').encode())

    return emit
