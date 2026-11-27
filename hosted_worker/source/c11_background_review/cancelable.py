"""Candidate parent controller. Caller is existing sole writer; no persistence in child."""
import ccxt,freqtrade
import json, os, signal, subprocess, sys, time
from pathlib import Path
from threading import get_ident
from concurrent.futures import ThreadPoolExecutor
from secrets import token_hex
from history_wire import Page
from shared_native_budget import SharedNativeBudget
from child_fs_profile import CA_SHA256

class HistoryHelper:
    def __init__(self, budget, wall, hot_due, authority, helper, *, deadline_ms=300):
        if type(budget) is not SharedNativeBudget or not (1 <= deadline_ms <= 1000):
            raise ValueError('SHARED_NATIVE_OWNER_REQUIRED')
        self.budget,self.wall,self.hot_due,self.authority=budget,wall,hot_due,authority
        self.helper=str(Path(helper).resolve(strict=True));self.deadline=deadline_ms/1000
        self.spawner=ThreadPoolExecutor(max_workers=1,thread_name_prefix="archive-only-spawn"); self.preparing=None
        self.closed=False
        self.owner=get_ident();self.current=None;self.retiring=None;self.trace=[]
    def _own(self):
        if self.owner!=get_ident():raise ValueError('ORIGINAL_WRITER_ONLY')
    def _valid(self,p):
        n=self.wall()
        return not self.hot_due() and p.valid(n) and self.authority(p,n) is True
    def start(self,p,fixture):
        self._own()
        if self.retiring is not None:
            if self.retiring.poll() is None:return False
            self.retiring=None
        if self.closed or self.current or type(p) is not Page or not self._valid(p):return False
        token=self.budget.try_reserve(p)
        if token is None:return False
        if self.preparing:
            self.budget.release_if_never_sent(token);return False
        tag=token_hex(16)
        def spawn():
            c=subprocess.Popen([sys.executable,'-I',self.helper,str(Path(ccxt.__file__).resolve().parent.parent),str(Path(freqtrade.__file__).resolve().parent.parent)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                stderr=None,cwd='/tmp',env={'OPENBLAS_NUM_THREADS':'1','OMP_NUM_THREADS':'1'},close_fds=True,start_new_session=True,bufsize=0)
            os.set_blocking(c.stdout.fileno(),False)
            try:c.stdin.write((json.dumps({'query':p.query(),'fixture':fixture,'tag':tag})+'\n').encode())
            except OSError:
                c.kill();c.wait();raise
            return c,get_ident()
        self.preparing=(p,token,self.spawner.submit(spawn),tag)
        return True
    def _finish(self,reason,result=None):
        p,t,c,_,_,charged,sent,tag=self.current;self.current=None;self.retiring=c
        if c.poll() is None:
            try:os.killpg(c.pid,signal.SIGKILL)
            except ProcessLookupError:pass
        # Do not put wait()/join() on the hot event loop. Retain process handle for later reaping.
        c.stdin.close();c.stdout.close();self.budget.release_if_never_sent(t)
        self.trace.append((reason,p.ticket,self.wall(),charged,sent))
        return reason,result,c
    def close(self):
        self._own()
        if self.closed:return
        self.closed=True
        if self.current is not None:self._finish('OWNER_TEARDOWN')
        if self.preparing is not None:
            _,token,spawn,_=self.preparing
            self.preparing=None
            self.budget.release_if_never_sent(token)
            def dispose(f):
                if f.cancelled():return
                try:
                    child,_=f.result()
                    if child.poll() is None:
                        try:os.killpg(child.pid,signal.SIGKILL)
                        except ProcessLookupError:pass
                    child.stdin.close();child.stdout.close()
                    child.wait(timeout=.5)
                except (OSError,subprocess.TimeoutExpired):pass
            if not spawn.cancel():spawn.add_done_callback(dispose)
        self.spawner.shutdown(wait=False,cancel_futures=True)
    def step(self):
        self._own()
        if self.current is None and self.preparing:
            p,t,f,tag=self.preparing
            if not f.done():return None
            self.preparing=None
            try:c,thread=f.result()
            except OSError:
                self.budget.release_if_never_sent(t);return ('SPAWN_FAILED',None,None)
            assert thread!=self.owner
            self.current=[p,t,c,time.monotonic()+3.0,b'',False,False,tag]
            self.trace.append(('spawn_off_owner',p.ticket,self.wall()))
        if self.current is None:return None
        p,t,c,until,b,charged,sent,tag=self.current
        if not self._valid(p):return self._finish('FENCE_CANCEL')
        if time.monotonic()>=until:return self._finish('HARD_DEADLINE')
        if b'\n' not in b:
            try: incoming=os.read(c.stdout.fileno(),65536)
            except BlockingIOError:return None
            if not incoming:return self._finish('PIPE_EOF')
            b+=incoming;self.current[4]=b
        if len(b)>1_000_000:return self._finish('OVERSIZE')
        if b'\n' not in b:return None
        line,rest=b.split(b'\n',1);self.current[4]=rest
        try: frame=json.loads(line)
        except ValueError:return self._finish('BAD_FRAME')
        if not charged and set(frame)=={'ready','tag','profile'} and frame['ready']==p.query() and frame['tag']==tag:
            proof=frame['profile']
            if (type(proof) is not dict or set(proof)!={'ca','abi','uid','resolver'} or proof['ca']!=CA_SHA256
                or proof['uid']!=1000 or proof['resolver']!='LOCAL_FIXTURE_ONLY'
                or type(proof['abi']) is not int or proof['abi']<3):return self._finish('CHILD_STARTUP_PRELOAD_REFUSED')
            self.trace.append(('child_preload_verified',p.ticket,self.wall()))
            if not self._valid(p) or not self.budget.claim_send(t,p):return self._finish('ADMISSION_REFUSED')
            self.current[3]=time.monotonic()+self.deadline
            self.current[5]=True
            self.trace.append(('charged_before_release',p.ticket,self.wall()))
            try:c.stdin.write(f'GO {tag}\n'.encode())
            except OSError:return self._finish('GO_LOST_STILL_CHARGED')
            return None
        if charged and not sent and frame=={'attempt':p.query(),'tag':tag}:
            self.current[6]=True
            self.trace.append(('observed_helper_attempt',p.ticket,self.wall()))
            return None
        if not charged or not sent or set(frame)!={'status','weight','raw','tag'} or frame['tag']!=tag:return self._finish('BAD_FRAME')
        if frame['status'] in (418,429): self.budget.optional_wire_fault(frame['status'],60000)
        if frame['status']!=200 or type(frame['weight']) is not int or frame['weight']<0:
            return self._finish('RECEIPT_HOLD')
        return self._finish('COMPLETED_FOR_OWNER',(frame['raw'], frame['weight']))
