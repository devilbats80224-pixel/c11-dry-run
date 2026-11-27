"""Production-shaped owner-to-child page forwarding, off-wire in review builds."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from secrets import token_hex
from threading import get_ident
import os,signal,socket,subprocess,sys,time
from sqlite3 import OperationalError
# Checked read-only code is already in the installed runtime manifest.
if '/opt/c11_background_review' not in sys.path:sys.path.append('/opt/c11_background_review')
from binder import Tap
from causal_bars import ClosedBar,CausalBars
from history_paging import bind_rows,persist_checked,plan
from history_wire import Page
from history_startup_receipt import require_claimed_live_receipt,Hold
from owner_child_protocol import frame,strict_line,PeerHold,CAP
from shared_native_budget import SharedNativeBudget
import stage_pin

OWNER_POLL_LIMIT=12.0
MAX_WIRE_RESPONSE_MS=1200

def launch():
    parent,child=socket.socketpair()
    try:
        p=subprocess.Popen([sys.executable,'-I','/opt/c11_owned_modules/guarded_owner_child.py','--fd','1'],
             stdin=subprocess.DEVNULL,stdout=child,stderr=subprocess.DEVNULL,cwd='/tmp',
             env={'OPENBLAS_NUM_THREADS':'1','OMP_NUM_THREADS':'1'},close_fds=True,start_new_session=True)
    except BaseException:
        parent.close();raise
    finally:child.close()
    parent.setblocking(False)
    return p,parent

def terminate(p,s):
    s.close()
    if p.poll() is None:
        try:os.killpg(p.pid,signal.SIGKILL)
        except ProcessLookupError:pass

class BoundForwarder:
    def __init__(self,budget,wall,authority):
        if type(budget) is not SharedNativeBudget:raise Hold('HISTORY_ORIGINAL_LIMITER_REQUIRED')
        self.budget=budget;self.wall=wall;self.authority=authority
        self.pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='c11-history-spawn')
        self.spawn=None;self.active=None;self.dead=None;self.closed=False
    def offer(self,j):
        if self.closed or self.spawn or self.active or not self.authority(j):return False
        if self.dead is not None:
            if self.dead.poll() is None:return False
            self.dead=None
        token=self.budget.try_reserve(j)
        if token is None:return False
        self.spawn=(j,token,self.pool.submit(launch),time.monotonic()+OWNER_POLL_LIMIT)
        return True
    def _finish(self,res):
        j,t,p,s,tag,_,buf,_,_=self.active
        self.active=None;self.dead=p
        terminate(p,s);self.budget.release_if_never_sent(t)
        return j,res
    def _read(self,buf):
        if b'\n' in buf:return buf
        incoming=self.active[3].recv(65536)
        if not incoming:raise PeerHold('C11_HISTORY_PEER_EOF')
        x=buf+incoming
        if len(x)>CAP:raise PeerHold('C11_HISTORY_FRAME_LIMIT')
        return x
    def tick(self):
        if self.spawn is not None and self.spawn[2].done():
            j,t,f,until=self.spawn;self.spawn=None
            try:p,s=f.result()
            except OSError:
                self.budget.release_if_never_sent(t);return j,None
            tag=token_hex(16)
            self.active=[j,t,p,s,tag,until,b'',False,None]
            try:
                if not self.authority(j) or time.monotonic()>=until:return self._finish(None)
                s.sendall(frame({'tag':tag,'page':dict(symbol=j.symbol,start=j.start,stop=j.stop,count=j.n)},4096))
            except (OSError,ValueError):return self._finish(None)
        if self.active is None:return None
        j,t,p,s,tag,until,buf,claimed,sent=self.active
        if not self.authority(j) or time.monotonic()>=until:return self._finish(None)
        try:read=self._read(buf)
        except BlockingIOError:return None
        except (OSError,ValueError):return self._finish(None)
        if b'\n' not in read:
            self.active[6]=read;return None
        try:
            line,rest=read.split(b'\n',1)
            x=strict_line(line+b'\n')
            if rest:raise PeerHold('C11_HISTORY_EXTRA_FRAME')
            if not claimed:
                if set(x)!={'ready','abi'} or x['ready']!=tag or type(x['abi']) is not int or x['abi']<3:
                    return self._finish(None)
                if not self.authority(j) or not self.budget.claim_send(t,j):return self._finish(None)
                self.active[6]=b'';self.active[7]=True;self.active[8]=self.wall()
                self.active[5]=time.monotonic()+OWNER_POLL_LIMIT
                s.sendall(frame({'go':tag},256));return None
            if set(x)!={'tag','status','used','rows','received'} or x['tag']!=tag:
                return self._finish(None)
            if x['status'] in (418,429):self.budget.optional_wire_fault(x['status'],60_000)
            if (x['status']!=200 or type(x['used']) is not int or not 0<=x['used']<=50000
                or type(x['received']) is not int or not sent<=x['received']<=min(self.wall(),sent+MAX_WIRE_RESPONSE_MS) or
                type(x['rows']) is not list or len(x['rows'])!=j.n):
                return self._finish(None)
            return self._finish((sent,x['received'],x['rows']))
        except (OSError,ValueError,TypeError):return self._finish(None)
    def close(self):
        self.closed=True
        if self.active:self._finish(None)
        if self.spawn:
            _,t,f,_=self.spawn;self.spawn=None;self.budget.release_if_never_sent(t)
            def reap(x):
                if x.cancelled():return
                try:
                    p,s=x.result();terminate(p,s);p.wait(.5)
                except (OSError,subprocess.TimeoutExpired):pass
            if not f.cancel():f.add_done_callback(reap)
        self.pool.shutdown(wait=False,cancel_futures=True)

class BoundOwnerHistory:
    def __init__(self,run,receipt):
        if stage_pin.AUTH_SHA is None:raise Hold('C11_HISTORY_OWNER_NO_AUTHORIZED_RELEASE')
        require_claimed_live_receipt(receipt)
        self.run=run;self.receipt=receipt;self.tap=Tap(run.plan);self.worker=run.worker
        self.owner=get_ident();self.store=self.worker.freqtrade.policy.bars
        if type(self.store) is not CausalBars:raise Hold('HISTORY_CANONICAL_OWNER_STORE')
        self.check_store()
        self.facts=();self.next=0;self.pending=None
        self.io=BoundForwarder(run.queue.limiter,run.wall,self.due_to_send)
    def check_store(self):
        require_claimed_live_receipt(self.receipt)
        files=self.store.db.execute('pragma database_list').fetchall()
        if files!=[(0,'main','/c11-paper/causal-candles.sqlite')]:raise Hold('HISTORY_CANONICAL_OWNER_STORE')
        z=Path(files[0][2]).lstat()
        if (z.st_dev,z.st_ino)!=self.receipt._site.store:raise Hold('HISTORY_CANONICAL_OWNER_INODE')
    def phase_allows(self):
        bot=self.worker.freqtrade
        due=getattr(bot,'_owned_original_cycle_deadline_ns',None)
        return (type(due) is int and time.monotonic_ns()+25_000_000<due
                and 65000<=self.run.wall()%900000<840000 and not self.run.queue.inflight)
    def due_to_send(self,j):
        return (self.pending is not None and self.pending[0] is j and j.valid(self.run.wall())
                and self.pending[2] is self.facts and self.phase_allows())
    def after_protection(self,bot):
        try:self._after_protection(bot)
        except (OperationalError,OSError,ValueError,TypeError,KeyError):
            # Optional failure must never alter original offer result. Retire
            # this lane fail-closed (restart/diagnostic review required).
            self.io.close()
            self.pending=None
            self._after_protection=lambda _bot:None
    def _after_protection(self,bot):
        if get_ident()!=self.owner or bot is not self.worker.freqtrade:return
        self.tap.sample(bot);self.facts=self.tap.facts
        # Never read or write SQLite for optional history in protected timing.
        allowed=self.phase_allows()
        out=self.io.tick()
        if out is not None:
            j,data=out
            if data is not None and allowed and self.due_to_send(j):
                self.check_store()
                sent,arrived,raw=data;q=self.pending[1]
                persist_checked(self.store,q,bind_rows(q,raw,sent,arrived,ClosedBar),lambda:self.due_to_send(j))
            self.pending=None
        if not allowed or self.pending or not self.facts:return
        self.check_store()
        facts=self.facts
        for skip in range(len(facts)):
            f=facts[(self.next+skip)%len(facts)]
            if self.run.wall()//900000*900000!=f.boundary:continue
            end=f.boundary-900000
            if f.origin>=end:continue
            state=self.store.rpd_progress(f.symbol,end,self.run.wall()) if self.store.segment(f.symbol) else None
            q=plan(f.symbol,f.origin,state.next_open if state else None,f.boundary,self.run.wall())
            if q is None:continue
            j=Page(f.symbol,f.generation,f.boundary,q.start,q.stop,token_hex(16))
            self.pending=(j,q,self.facts)
            if self.io.offer(j):
                self.next=(self.next+skip+1)%len(facts)
                return
            self.pending=None
    def close(self):self.io.close()
