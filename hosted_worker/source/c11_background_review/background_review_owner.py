"""Dormant owner composition. No running Worker, seal, or endpoint activation."""
from pathlib import Path
from threading import get_ident
from secrets import token_hex
from binder import Tap
from cancelable import HistoryHelper
from causal_bars import CausalBars,ClosedBar
from history_paging import FIVE,bind_rows,persist_checked,plan as page_plan
from history_wire import Page
from shared_native_budget import SharedNativeBudget

class ReviewOwner:
    def __init__(self, scheduler, store, *, due, helper):
        if type(store) is not CausalBars or type(scheduler.queue.limiter) is not SharedNativeBudget:
            raise ValueError('EXACT_OWNER_AND_BUDGET_REQUIRED')
        db=store.db.execute('pragma database_list').fetchall()
        if not (len(db)==1 and db[0][:2]==(0,'main') and (not db[0][2] or Path(db[0][2]).is_relative_to('/testwork'))):
            raise ValueError('DORMANT_SCRATCH_STORE_ONLY')
        if not str(Path(helper).resolve(strict=True)).startswith('/opt/c11_background_review/'):
            raise ValueError('INSTALLED_REVIEW_HELPER_REQUIRED')
        self.owner=get_ident();self.tap=Tap(scheduler.plan);self.store=store
        self.budget=scheduler.queue.limiter;self.due=due;self.wall=scheduler.plan.wall
        self.auth=None;self.pending=None;self.counter=0
        self.runner=HistoryHelper(self.budget,self.wall,due,self._authority,helper,deadline_ms=1000)
    def _authority(self,p,n):
        return self.auth is not None and self.auth(p,n)
    def observed_after_original(self,bot):
        if get_ident()!=self.owner:raise ValueError('ORIGINAL_WRITER_ONLY')
        return self.tap.sample(bot)
    def submit(self,symbol,*,boundary,generation,fixture):
        if get_ident()!=self.owner:raise ValueError('ORIGINAL_WRITER_ONLY')
        if self.due() or self.pending:return False
        try:self.auth=self.tap.history_callback(boundary=boundary,generation=generation)
        except ValueError as ex:
            if str(ex)!='HOT_WINDOW':raise
            self.auth=None;return False
        fact=next((x for x in self.tap.facts if (x.symbol,x.generation,x.boundary)==(symbol,generation,boundary)),None)
        if fact is None:return False
        end=boundary-900000
        seg=self.store.segment(symbol)
        state=self.store.rpd_progress(symbol,end,self.wall()) if seg else None
        if state:
            cursor=state.next_open
        else:cursor=None
        q=page_plan(symbol,fact.origin,cursor,boundary,self.wall())
        if q is None:return False
        self.counter+=1
        p=Page(symbol,generation,boundary,q.start,q.stop,f'bg.{generation}.{boundary}.{self.counter}.{token_hex(8)}')
        if not self.runner.start(p,fixture):return False
        self.pending=(p,q,self.wall());return True
    def step(self):
        if get_ident()!=self.owner:raise ValueError('ORIGINAL_WRITER_ONLY')
        result=self.runner.step()
        if not result:return None
        p,q,sent_or_after_enqueue=self.pending;self.pending=None
        status,x,_=result
        if (status!='COMPLETED_FOR_OWNER' or self.due()
            or not self._authority(p,self.wall())):return status
        raw,used=x
        try:
            rows=bind_rows(q,raw,sent_or_after_enqueue,self.wall(),ClosedBar)
            persist_checked(self.store,q,rows,lambda:self._authority(p,self.wall()) and not self.due())
        except ValueError:return 'BAD_HISTORY_GRID_NO_WRITE'
        return 'COMMITTED_BY_ORIGINAL_OWNER'
