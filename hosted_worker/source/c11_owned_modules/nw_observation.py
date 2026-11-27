"""After-protection observation-only scheduling; never reads/changes order/journal state."""
from threading import get_ident
from sqlite3 import OperationalError
from bridge_protocol import Refused
from native_candles import NativeCandles,QUARTER,MINUTE,NW_WARM
from no_sqlite_busy import optional_observation

class MissingCheckpointNWObservation:
 def __init__(self,wall,mono):
    self.wall,self.mono=wall,mono
    self.owner=get_ident()
    self.marker=None;self.completed=set()
 def after_drained(self,scheduler,bot,g,end):
    if get_ident()!=self.owner or bot is not scheduler.worker.freqtrade:
        raise RuntimeError('C11_OBSERVATION_OWNER')
    queue=scheduler.queue
    if bot.config.get('dry_run') is not True or scheduler.closed or scheduler.marker!=(g,end):
        return 'HOLD_OBSERVATION_OWNER'
    if queue.inflight or any(not f.done() for f in queue.executor.physical) or queue.protection_due():
        return 'HOLD_ORIGINAL_WORK_OWNS_PHASE'
    now=self.wall()
    if not end<=now<end+MINUTE:return 'HOLD_OBSERVATION_OUTSIDE_WINDOW'
    if self.marker!=(g,end):self.marker=g,end;self.completed.clear()
    bar=bot.policy.bars;c=bot.policy.candles
    if type(c) is not NativeCandles or c.store is not bar or bar.db.in_transaction:return 'HOLD_OBSERVATION_STORE'
    actual=bot.feed.latest_owner_observation
    for candidate in actual:
      if candidate.generation!=g or candidate.expires_ns<=self.mono() or not bot.observed_ranked(candidate):
        continue
      symbol=candidate.symbol;pair=candidate.pair
      if symbol in self.completed:continue
      try:
        existing=optional_observation(
            lambda:bar.db.execute('SELECT 1 FROM rpd_checkpoint WHERE symbol=?',(symbol,)).fetchone(),
            (bar.db,))
        if existing is not None:continue
        missing=optional_observation(lambda:tuple((w,n)for w,n in ((QUARTER,NW_WARM),(MINUTE,30))
                  if not bar.range(symbol,w,end-w*n,end,self.wall())),(bar.db,))
        if type(missing) is not tuple:return 'HOLD_OBSERVATION_STORE_BUSY'
        if missing:
          # Never issue request from UnifiedScheduler; original warmup,
          # periodic protections and detached jobs own their existing slots.
          continue
        now=self.wall()
        if not (end<=now<end+MINUTE and candidate.expires_ns>self.mono() and bot.observed_ranked(candidate)):
           return 'HOLD_OBSERVATION_AUTHORITY_LOST'
        res=optional_observation(lambda:bar.recent_primary(symbol,end,now),(bar.db,))
        if type(res) is not tuple or len(res)!=3 or res[2] is None:
           return 'HOLD_OBSERVATION_UNCONFIRMED'
        outcome=res[2].status
        if outcome not in ('NO_SIGNAL','NW15_PRIMARY_SIGNAL'):
           return 'HOLD_OBSERVATION_UNCONFIRMED'
        def save():bar.feature_status(symbol,end,now,{'NW15_OBSERVATION_ONLY':outcome});return True
        if optional_observation(save,(bar.db,)) is not True:return 'HOLD_OBSERVATION_STORE_BUSY'
        self.completed.add(symbol)
        return 'NW15_OBSERVED_ONLY'
      except (Refused,ValueError,TypeError,KeyError,OperationalError):
        return 'HOLD_OBSERVATION_INPUT'
    return 'NO_MISSING_CHECKPOINT_OBSERVATION_DUE'
