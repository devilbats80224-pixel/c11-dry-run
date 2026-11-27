"""Archive-only candidate seam. No permit manufacture, network, or worker startup."""
from dataclasses import dataclass
from decimal import Decimal
from threading import get_ident
from compiled_candidate_factory import OriginalPlan
from native_candles import NativeCandles
Q=900000
@dataclass(frozen=True)
class Fact:
    symbol:str; pair:str; generation:int; boundary:int; observed:int; expires:int; origin:int
class Tap:
    def __init__(self,plan,*,quarantined=()):
        if type(plan) is not OriginalPlan:raise ValueError('ORIGINAL_PLAN_REQUIRED')
        self.plan=plan;self.owner=get_ident();self.facts=();self.quarantine=frozenset(quarantined)
    def sample(self,bot):
        if get_ident()!=self.owner or bot is not self.plan.bot:raise ValueError('NO_OWNER')
        now=self.plan.wall(); E,rows=self.plan.entries()
        facts=[]
        for row in rows:
            if row.symbol in self.quarantine or row.ratio<Decimal('.20'):continue
            scope=self.plan.provisional_scope(row.pair,E)
            if scope is None or self.plan.accepted_candidate(scope) is not row:continue
            try:
                market=NativeCandles.market(bot,row.pair,row.symbol)
                c=market['info']; origin=c['onboardDate']
                if c['status']!='TRADING' or c['contractType']!='PERPETUAL' or type(origin) is not int or origin%300000 or origin<=0 or origin>=E:continue
            except Exception:continue
            facts.append(Fact(row.symbol,row.pair,row.generation,E,now,scope.expires,origin))
        if facts:
            latest=max(f.generation for f in facts)
            if self.facts and latest<max(f.generation for f in self.facts):raise ValueError('OLDER_OBSERVATION')
            self.facts=tuple(facts)
        elif rows:
            # A newer authenticated observation without available metadata
            # revokes old background work; don't lend it to a changed roster.
            self.facts=()
        return self.facts
    def history_callback(self,*,boundary,generation,max_elapsed_ms=5000):
        if get_ident()!=self.owner or not 0<max_elapsed_ms<=5000:raise ValueError('BACKGROUND_TIMEBOX')
        start=self.plan.wall(); restrict=self.facts
        if not (65000<=start-boundary<840000):raise ValueError('HOT_WINDOW')
        allowed={f.symbol:f for f in restrict if (f.boundary,f.generation)==(boundary,generation)}
        def authorize(page,t):
            f=allowed.get(page.symbol)
            return (self.facts is restrict and f is not None and page.start>=f.origin and page.stop<=boundary-Q
               and (page.end,page.generation)==(boundary,generation) and start<=t<min(start+max_elapsed_ms,boundary+840000))
        return authorize
