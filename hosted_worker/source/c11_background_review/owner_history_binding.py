"""Typed installed owner handoff (offline review only; cannot authorize startup)."""
from pathlib import Path
from threading import get_ident
from background_review_owner import ReviewOwner
from causal_bars import CausalBars

class OfflineHistoryReview:
    """Explicit injection for exact fixture replay. No operational branch."""
    def __init__(self,*,fixture_symbols=()):
        self.fixture_symbols=frozenset(fixture_symbols)
    def attach(self, run, *, worker):
        if (run.worker is not worker or not worker.freqtrade.config['dry_run'] or
            not worker.freqtrade.native_paper_exchange()):
            raise ValueError('ORIGINAL_DRY_OWNER_REQUIRED')
        return AttachedOfflineHistory(run,worker,self.fixture_symbols)

class AttachedOfflineHistory:
    def __init__(self,run,worker,fixture_symbols):
        self.owner=get_ident();self.worker=worker
        self.store=CausalBars(':memory:')
        self.closed=False;self.offers=0;self.last_facts=();self.diagnostic=None
        self.fixture_symbols=fixture_symbols;self.run=run
        def due():
            phase=run.wall()%900000
            return phase<65000 or phase>=840000 or bool(run.queue.inflight)
        self.engine=ReviewOwner(run,self.store,due=due,
                    helper='/opt/c11_background_review/native_helper.py')
    def after_protection(self,bot):
        if self.closed or self.owner!=get_ident() or bot is not self.worker.freqtrade:
            return
        self.offers+=1
        try:
            self.last_facts=self.engine.observed_after_original(bot)
            if self.engine.pending:self.diagnostic=self.engine.step()
            if self.engine.due() or self.engine.pending:return
            for fact in self.last_facts:
                if fact.symbol in self.fixture_symbols and self.engine.submit(fact.symbol,boundary=fact.boundary,
                                                                           generation=fact.generation,fixture='fast'):
                    break
        except (ValueError,TypeError,KeyError):
            self.diagnostic='HISTORY_OBSERVATION_HELD'
    def close(self):
        if self.closed:return
        if self.owner!=get_ident():raise ValueError('ORIGINAL_WRITER_ONLY')
        self.closed=True
        self.engine.runner.close()
        self.store.db.close()
