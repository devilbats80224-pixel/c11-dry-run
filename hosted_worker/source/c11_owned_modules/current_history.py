"""Current native owner observation, for historical data preparation only."""
from dataclasses import dataclass
from decimal import Decimal
from threading import get_ident
from entry_feed import EntryFeed,Ranked
from native_candles import NativeCandles
Q=900_000; FIVE=300_000

@dataclass(frozen=True)
class Fact:
    symbol:str; pair:str; generation:int; boundary:int; origin:int; expires_ns:int

class CurrentHistory:
    def __init__(self,bot,wall,mono,quarantined):
        if type(bot.feed) is not EntryFeed:raise ValueError('EXACT_HISTORY_FEED_REQUIRED')
        self.bot=bot;self.feed=bot.feed;self.wall=wall;self.mono=mono
        self.quarantined=frozenset(quarantined)
        self.thread=get_ident();self.cycle_stamp=-1;self.caps={};self.classification_witness={}
        self.generation=None
    def before_protection(self):
        self.cycle_stamp=self.feed.owner_cycle_stamp() if get_ident()==self.thread else -1
    def facts(self):
        if get_ident()!=self.thread or self.bot.feed is not self.feed or self.cycle_stamp<0:return ()
        rows=self.feed.warmup_observation_since(self.cycle_stamp)
        g=self.feed.generation_floor; now=self.mono(); E=self.wall()//Q*Q
        if any(type(r) is not Ranked or r.generation!=g for r in rows):
            return ()
        if self.generation is not None and g<self.generation:return ()
        if g!=self.generation:self.caps={};self.classification_witness={};self.generation=g
        out=[]
        for r in rows:
            if r.symbol in self.quarantined or r.ratio<Decimal('.20'):continue
            try:
                m=NativeCandles.market(self.bot,r.pair,r.symbol)
                c=m['info']; origin=c['onboardDate']
                if (c['symbol']!=r.symbol or c['status']!='TRADING' or c['contractType']!='PERPETUAL'
                    or c['quoteAsset']!='USDT' or c['marginAsset']!='USDT'
                    or type(origin) is not int or not 0<origin<E or origin%FIVE):
                    continue
            except Exception:continue
            key=r.symbol,r.pair
            identity=(r.pair,r.symbol,r.generation,r.rank,r.ratio,r.received_at,origin)
            stamp=r.classification_seen_ns
            prior=self.classification_witness.get(key)
            earlier=self.caps.get(key,r.expires_ns)
            if prior is None or (prior[0]==identity and type(stamp) is int
                                 and type(prior[1]) is int and stamp>prior[1] and now>=earlier):
                # This is a new verified Fact, not a longer lifetime for any
                # already granted page: current(previous) checks its old cap.
                self.classification_witness[key]=(identity,stamp)
                earlier=r.expires_ns
            elif prior[0]!=identity:
                continue
            cap=min(r.expires_ns,earlier)
            self.caps[key]=cap
            if now<cap:out.append(Fact(r.symbol,r.pair,g,E,origin,cap))
        return tuple(out)
    def current(self,previous):
        # New g never inherits data-page authority even for the same symbol.
        # An empty/failed source read cannot authenticate by last-tracking.
        return any((f.pair,f.symbol,f.generation,f.boundary,f.origin)==
                   (previous.pair,previous.symbol,previous.generation,previous.boundary,previous.origin)
                   and self.mono()<min(f.expires_ns,previous.expires_ns) for f in self.facts())
