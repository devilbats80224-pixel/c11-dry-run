"""Production-shaped owner-to-child page forwarding, off-wire in review builds."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from secrets import token_hex
from threading import get_ident
import os,signal,socket,subprocess,sys,time
from sqlite3 import OperationalError,SQLITE_BUSY,SQLITE_LOCKED
from no_sqlite_busy import no_sqlite_busy
# Checked read-only code is already in the installed runtime manifest.
if '/opt/c11_background_review' not in sys.path:sys.path.append('/opt/c11_background_review')
from current_history import CurrentHistory
from bridge_protocol import Refused
from causal_bars import ClosedBar,CausalBars
from dual_history_pages import lanes,make_page,persist
from archive_handoff import commit as commit_archive
from archive_prep_lane import ArchivePrepLane
from atr_er_checkpoint import AtrErCheckpoint
from rpd_checkpoint import RpdState
from causal_bars import FIVE, QUARTER
from history_startup_receipt import require_claimed_live_receipt,Hold
from owner_child_protocol import frame,strict_line,PeerHold,CAP
from shared_native_budget import SharedNativeBudget
from native_minute_admission_loop import MinuteAdmissionLoop
from native_primary_lane import NativePrimaryLane
from cold_prefix import ColdPrefixAdmission
from shared_history_forwarder import SharedHistoryForwarder,retained_page_trace
import stage_pin

OWNER_POLL_LIMIT=12.0
MAX_WIRE_RESPONSE_MS=1200
# Off-window only; fixture peak ~28 ms, leave additional storage/CPU headroom.
BACKGROUND_OWNER_MIN_HEADROOM_NS=100_000_000

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
        self.last_offer_reason=None
    def offer(self,j):
        self.last_offer_reason='HISTORY_PAGE_HELD'
        if self.closed or self.spawn or self.active or not self.authority(j):return False
        if self.dead is not None:
            if self.dead.poll() is None:return False
            self.dead=None
        token=self.budget.try_reserve_history(j,cold_current=self.authority)
        if token is None:
            self.last_offer_reason=self.budget.last_history_hold
            return False
        self.last_offer_reason=None
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
                s.sendall(frame({'tag':tag,'page':j.child()},4096))
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
                if not self.authority(j) or not self.budget.claim_history_send(t,j,cold_current=self.authority):return self._finish(None)
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
    def __init__(self,run,receipt,*,verified_quarantine,archive_prep=None,reviewed_opg_selector=None,diagnostic_sink=None):
        if stage_pin.AUTH_SHA is None:raise Hold('C11_HISTORY_OWNER_NO_AUTHORIZED_RELEASE')
        require_claimed_live_receipt(receipt)
        self.run=run;self.receipt=receipt;self.tap=CurrentHistory(run.worker.freqtrade,run.wall,run.plan.mono,verified_quarantine);self.worker=run.worker
        self.owner=get_ident();self.store=self.worker.freqtrade.policy.bars
        if type(self.store) is not CausalBars:raise Hold('HISTORY_CANONICAL_OWNER_STORE')
        self.check_store()
        self.facts=();self.last_background=None;self.pending=None
        self.atr=AtrErCheckpoint(self.store)
        if archive_prep is not None and type(archive_prep) is not ArchivePrepLane:
            raise Hold('C11_ARCHIVE_PREP_OWNER_TYPE')
        self.archive_prep=archive_prep
        self.cold_prefix=(archive_prep.cold_admission(run.wall) if archive_prep is not None
                          else ColdPrefixAdmission({},run.wall))
        self.io=BoundForwarder(run.queue.limiter,run.wall,self.due_to_send)
        self.shared_minute=SharedHistoryForwarder(run.queue.limiter,run.wall,trace=retained_page_trace(diagnostic_sink))
        self.minute_admission=MinuteAdmissionLoop(
            self,self.shared_minute.lane)
        self.primary_admission=NativePrimaryLane(
            self,self.shared_minute.lane)
        self.opg_recovery=None
        self.history_sink=diagnostic_sink
        self.history_seen=None,set()
        if reviewed_opg_selector is not None:
            from opg_settled_recovery import SettledOpgRecovery
            self.opg_recovery=SettledOpgRecovery(reviewed_opg_selector,run.wall)
    def check_store(self):
        require_claimed_live_receipt(self.receipt)
        files=self.store.db.execute('pragma database_list').fetchall()
        if files!=[(0,'main','/c11-paper/causal-candles.sqlite')]:raise Hold('HISTORY_CANONICAL_OWNER_STORE')
        z=Path(files[0][2]).lstat()
        if (z.st_dev,z.st_ino)!=self.receipt._site.store:raise Hold('HISTORY_CANONICAL_OWNER_INODE')
    def phase_allows(self):
        bot=self.worker.freqtrade
        due=getattr(bot,'_owned_original_cycle_deadline_ns',None)
        return (type(due) is int and time.monotonic_ns()+BACKGROUND_OWNER_MIN_HEADROOM_NS<due
                and 65000<=self.run.wall()%900000<840000 and not self.run.queue.inflight)
    def cold_delivery_window(self):
        bot=self.worker.freqtrade
        due=getattr(bot,'_owned_original_cycle_deadline_ns',None)
        return (get_ident()==self.owner and bot.config.get('dry_run') is True
                and not self.run.closed and type(due) is int
                and time.monotonic_ns()+BACKGROUND_OWNER_MIN_HEADROOM_NS<due)

    def cold_delivery_allows(self,f):
        return (self.cold_delivery_window()
                and f.boundary<=self.run.wall()<f.boundary+900000
                and not (self.opg_recovery is not None and f.symbol=='OPGUSDT')
                and not (self.pending is not None and self.pending[2].symbol==f.symbol)
                and not any(j.symbol==f.symbol for j,_,_ in self.run.queue.inflight.values())
                and not self.primary_admission.pending.busy(f.symbol)
                and not self.minute_admission.pending.busy(f.symbol)
                and self.tap.current(f))

    def due_to_send(self,j):
        return (self.pending is not None and self.pending[0] is j and j.valid(self.run.wall())
                and self.tap.current(self.pending[2]) and self.phase_allows())
    def note_opg_history(self,code):
        sink=getattr(self,'history_sink',None)
        if sink is None or get_ident()!=self.owner:return
        now=self.run.wall(); e=now//900000*900000; age=now-e
        if not 65000<=age<840000:return
        if self.history_seen[0]!=e:self.history_seen=e,set()
        if code in self.history_seen[1]:return
        self.history_seen[1].add(code)
        from json import dumps
        d=dict(schema='c11-opg-history-status-v1',end_ms=e,age_ms=age,status=code)
        sink((dumps(d,separators=(',',':'))+chr(10)).encode())

    def before_protection(self):
        self.tap.before_protection()

    def minute_headroom(self):
        if self.shared_minute.sock is not None or self.shared_minute.future is not None:
            return True  # Existing child; candidates do not consume a new process.
        try:
            root=Path('/sys/fs/cgroup')
            maximum=(root/'pids.max').read_text().strip()
            return maximum=='max' or int((root/'pids.current').read_text())+4<int(maximum)
        except (OSError,ValueError):
            return False

    def after_protection(self,bot):
        if get_ident()==self.owner and bot is self.worker.freqtrade:
            try:
                # Each optional mutation is short and never waits for SQLite.
                with no_sqlite_busy(self.store.db):
                    facts=self.tap.facts()
                    if self.cold_delivery_window():
                        # Nonblocking SQLite, after original protection, before
                        # scheduling overlapping history for these candidates.
                        self.cold_prefix.refresh_delivery_facts(self,facts)
                        self.cold_prefix.tick(self,ready_only=True)
                        self.minute_admission.enroll_cold(facts)
                        self.cold_prefix.tick(self,prepare_only=True)
                    self.primary_admission.tick(facts,self.minute_headroom,
                        occupied_ranges=self.minute_admission.pending.inflight_ranges)
                    self.minute_admission.tick(
                        facts,
                        skip=lambda s:(self.pending is not None and self.pending[2].symbol==s)
                               or (self.opg_recovery is not None and s=='OPGUSDT'),
                        has_headroom=self.minute_headroom,
                        occupied_ranges=self.primary_admission.pending.inflight_ranges)
            except OperationalError as exc:
                code=getattr(exc,'sqlite_errorcode',None)
                if not(type(code) is int and (code&255) in (SQLITE_BUSY,SQLITE_LOCKED)):
                    raise
                self.primary_admission.gate('SQLITE_BUSY')
        try:
            if self.phase_allows():
                with no_sqlite_busy(self.store.db):
                    self._after_protection(bot)
            else:
                self._after_protection(bot)
        except OperationalError as exc:
            code=getattr(exc,'sqlite_errorcode',None)
            if type(code) is int and (code&255) in (SQLITE_BUSY,SQLITE_LOCKED):
                if self.io.spawn is None and self.io.active is None:self.pending=None
                return
            self._retire()
        except (OSError,ValueError,TypeError,KeyError):
            self._retire()

    def _retire(self):
        # Optional structural failure must never alter original offer.
        self.io.close()
        if self.archive_prep is not None:self.archive_prep.close()
        self.pending=None
        self._after_protection=lambda _bot:None
    def _after_protection(self,bot):
        if get_ident()!=self.owner or bot is not self.worker.freqtrade:return
        self.facts=self.tap.facts()
        # Never read or write SQLite for optional history in protected timing.
        allowed=self.phase_allows()
        archive=self.archive_prep
        sr=self.opg_recovery
        if sr is not None:
            sr.reset_if(lambda f:allowed and self.tap.current(f))
            if not allowed:self.note_opg_history('HISTORY_HEADROOM_OR_HOT_IO')
            elif not any(f.symbol=='OPGUSDT' for f in self.facts):self.note_opg_history('HISTORY_NO_CURRENT_OPG')
        if self.pending is not None and self.pending[1]=='archive':
            j,_,_=self.pending
            if not allowed or not self.due_to_send(j):
                archive.cancel()
                self.pending=None
            done=archive.poll()
            if done is not None and self.pending is not None and done[0] is j:
                _,prepared,src=done
                try:
                    if prepared is not None and self.due_to_send(j):
                        self.check_store()
                        due=lambda:self.due_to_send(j)
                        landed=commit_archive(self.store,prepared,j,src.digest,
                                  lambda:archive.current_version(j.symbol),self.run.wall,due)
                        proof=self.store.segment(j.symbol)
                        self.atr.extend(j.symbol,j.width,proof,j.stop,landed,authority=due)
                        if j.width==FIVE and due():self.store.rpd_progress(j.symbol,j.stop,landed,authority=due)
                except (Refused,ValueError):
                    pass
                finally:self.pending=None
                return
            if self.pending is not None:return
        elif archive is not None and archive.active is not None and archive.active[2].is_set():
            archive.poll() # drain discarded scope, never accept a canceled result.
        out=self.io.tick()
        if out is not None:
            j,data=out
            if sr is not None and self.pending is not None and self.pending[1]=='opg_recovery':
                _j,_,fact,base=self.pending
                try:
                    if data is not None and _j is j and allowed and self.due_to_send(j):
                        self.check_store()
                        ok=sr.consume(fact,j,base,data,lambda f:self.due_to_send(j) and self.tap.current(f))
                        self.note_opg_history('HISTORY_PAIRED_APPEND' if ok else 'HISTORY_WIDTH_BUFFERED')
                    else:
                        sr.part=None
                        self.note_opg_history('HISTORY_PAGE_NOT_USABLE')
                except (Refused,ValueError):
                    sr.part=None
                    self.note_opg_history('HISTORY_REJECTED_DATA')
                finally:self.pending=None
                return
            if data is not None and allowed and self.due_to_send(j):
                self.check_store()
                sent,arrived,raw=data
                try:
                    persist(self.store,j,raw,sent,arrived,lambda:self.due_to_send(j))
                    if self.due_to_send(j):
                        proof=self.store.segment(j.symbol)
                        self.atr.extend(j.symbol,j.width,proof,j.stop,self.run.wall(),
                                        authority=lambda:self.due_to_send(j))
                        if j.width==FIVE and self.due_to_send(j):
                            self.store.rpd_progress(j.symbol,j.stop,self.run.wall(),authority=lambda:self.due_to_send(j))
                except (Refused,ValueError):
                    # Refused data/recurrence for this page mutates no partial
                    # ATR state, but must not retire everyone else's lane.
                    pass
            self.pending=None
        if not allowed or self.pending or not self.facts:return
        self.check_store()
        for f,width in lanes(self.facts,self.last_background):
            # The phase budget is global, but loss of authority for this
            # fact is per-candidate. Do not starve the rest of the roster.
            if not self.phase_allows():return
            if not self.tap.current(f) or self.minute_admission.pending.busy(f.symbol):continue
            if self.minute_admission.blank_bootstrap(f):
                # An authenticated blank old listing uses only its exact
                # missing recent raw minutes, even if shared budget is held.
                continue
            if sr is not None and f.symbol=='OPGUSDT':
                try:
                    choice=sr.offer(f,width,self.tap.current)
                    if choice is not None:
                        j,base=choice
                        self.pending=(j,'opg_recovery',f,base)
                        if self.io.offer(j):
                            self.note_opg_history('HISTORY_PAGE_SENT_TO_OWNER_CHILD')
                            self.last_background=f.symbol,width
                            return
                        self.note_opg_history(self.io.last_offer_reason or 'HISTORY_PAGE_HELD')
                    elif sr.part is not None:
                        self.note_opg_history('HISTORY_WAITING_FOR_OTHER_WIDTH')
                except (Refused,ValueError):
                    sr.part=None
                    self.note_opg_history('HISTORY_REJECTED_DATA')
                finally:
                    if self.io.spawn is None and self.io.active is None:self.pending=None
            rpd=None
            proof=self.store.segment(f.symbol)
            if proof is not None and proof.open_ms!=f.origin:continue
            try:
                start=self.atr.cursor(f.symbol,width,proof) if proof is not None else f.origin
            except (Refused,ValueError):
                continue
            if width==FIVE and proof is not None:
                # One possible bounded recurrence unit per cycle; no physical
                # fetch if prior authorized raw rows already satisfy the page.
                row=self.store.db.execute('SELECT payload FROM rpd_checkpoint WHERE symbol=?',(f.symbol,)).fetchone()
                rpd=RpdState.restore(row[0]) if row else None
                if rpd is not None and (rpd.symbol,rpd.origin)!=(f.symbol,f.origin):continue
                start=min(start,rpd.next_open if rpd else f.origin)
            j=make_page(f,width,start,self.run.wall(),token_hex(16))
            if j is None:continue
            self.pending=(j,None,f)
            try:
                if proof is not None and self.store.range(f.symbol,width,j.start,j.stop,self.run.wall()):
                    if not self.due_to_send(j):return
                    self.atr.extend(f.symbol,width,proof,j.stop,self.run.wall(),
                                    authority=lambda:self.due_to_send(j))
                    if width==FIVE and self.due_to_send(j):
                        # Existing native RPD consumer owns recurrence; do not
                        # make or refresh an entry scope here.
                        self.store.rpd_progress(f.symbol,j.stop,self.run.wall(),authority=lambda:self.due_to_send(j))
                    self.last_background=f.symbol,width
                    return
                if archive is not None and archive.offer(j):
                    self.pending=(j,'archive',f)
                    self.last_background=f.symbol,width
                    return
            except (Refused,ValueError):
                continue
            finally:
                if self.io.spawn is None and self.io.active is None and (self.pending is None or self.pending[1]!='archive'):self.pending=None
    def close(self):
        self.cold_prefix.close()
        self.minute_admission.close()
        self.primary_admission.close()
        self.shared_minute.close()
        if self.archive_prep is not None:self.archive_prep.close()
        self.io.close()
