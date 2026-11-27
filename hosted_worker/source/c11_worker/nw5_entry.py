"""Native interpretation of the frozen first-NW5 entry keys."""
from dataclasses import dataclass
from decimal import Decimal

from causal_bars import FIVE, MINUTE, QUARTER
from c11_closed5_rejected_break_port_v1.rejected_break import ClosedMinute, classify as rejected_break
from c11_primary_decision_seam_v1.exact_rpd5_join import join
from c11_primary_decision_seam_v1.primary_seam import Scope
from live_signal_evidence import nw_flags, read_seeded
from native_protection import wall_ms


@dataclass(frozen=True)
class Nw5Offer:
    symbol: str
    spell_start: int
    end: int
    close: float
    keep: bool


def _rpd_status(store,symbol,end,as_of):
    state=store.rpd_sample(symbol,end,as_of)
    return join(Scope(symbol,end),as_of,[state] if state else [])[0]


def original_nw15(store,symbol,end,as_of):
    rpd=_rpd_status(store,symbol,end,as_of)
    if rpd=='VETO':
        return False
    _,_,nw=store.recent_primary(symbol,end,as_of)
    if nw is None or nw.status=='UNKNOWN':
        return None
    if nw.status=='NO_SIGNAL':
        return False
    return True if rpd=='PASS' and nw.status=='NW15_PRIMARY_SIGNAL' else None


def first_offer(store,observer,symbol,start,end,now,rows):
    """All earlier checks use their own available-at time and original order."""
    if not (start%FIVE==end%FIVE==0 and start<=end<=now<end+MINUTE):
        return None
    spans=range(start,end+FIVE,FIVE)
    positions={r.open_ms+FIVE:i for i,r in enumerate(rows)}
    if any(E not in positions or positions[E]==0 for E in spans):
        return None
    _,known,triggers=nw_flags(rows,FIVE)
    if end not in triggers:
        return None
    seen=0
    seen_at={}
    for row in rows:
        seen=max(seen,row.received_ms)
        seen_at[row.open_ms+FIVE]=seen
    for E in spans:
        as_of=min(now,E+MINUTE-1)
        if E%QUARTER==0:
            old=original_nw15(store,symbol,E,as_of)
            if old is not False:
                return None
        if not known[positions[E]] or seen_at[E]>as_of:
            return None
        if E not in triggers:
            continue
        i=positions[E]
        if rows[i].close<=rows[i-1].close:
            return None
        rpd=_rpd_status(store,symbol,E,as_of)
        if rpd=='UNKNOWN':
            return None
        if rpd!='PASS':
            continue
        minute=store.range(symbol,MINUTE,E-MINUTE,E,as_of)
        if not minute:
            return None
        if minute[-1].close!=rows[i].close:
            continue
        point=observer.point(symbol,E)
        if point is None or point[2]<Decimal('.20'):
            return None
        if E!=end:
            # A prior qualified offer consumes this spell even when no order
            # existed, e.g. all slots were occupied at that earlier close.
            return None
        recent=store.range(symbol,MINUTE,end-10*MINUTE,end,as_of)
        veto=rejected_break(end,tuple(ClosedMinute(r.open_ms+MINUTE,r.high,r.low,r.close) for r in recent)) if recent else 'UNKNOWN'
        return Nw5Offer(symbol,start,end,rows[i].close,veto=='KEEP')
    return None


class Nw5Evidence:
    def read(self,bot,pair,now):
        end=now//FIVE*FIVE
        if not end<=now<end+MINUTE:
            return None
        candidate=bot.feed.candidate(pair)
        symbol=bot.exchange.markets[pair]['id']
        if candidate is None or candidate.symbol!=symbol or not bot.observed_ranked(candidate):
            return None
        start=bot.observer.episode(symbol,end)
        if (start is None or bot.entry_journal.nw5_seen(symbol,start)
                or bot.observer.span(symbol,start,end) is None):
            return None
        store=bot.policy.bars
        proof=store.segment(symbol)
        if proof is None or proof.proven_at_ms>now or proof.open_ms!=bot.exchange.markets[pair]['info'].get('onboardDate'):
            return None
        bot.policy.candles.update_rpd(bot,pair,symbol,end)
        now=wall_ms()
        rows=read_seeded(store,symbol,proof.open_ms,FIVE,end,now)
        if not rows or now>=end+MINUTE:
            return None
        if not store.range(symbol,MINUTE,end-MINUTE,end,now):
            bot.policy.candles.query(bot,symbol,MINUTE,end-30*MINUTE,end,30)
            now=wall_ms()
        return first_offer(store,bot.observer,symbol,start,end,now,rows)
