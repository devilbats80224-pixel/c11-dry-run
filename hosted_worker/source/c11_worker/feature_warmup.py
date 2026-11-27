"""Independent recent Nadaraya and explicit per-feature readiness."""
import sqlite3
import time

from causal_bars import FIVE, MINUTE, QUARTER
from c11_nw15_primary_signal_port_v1.primary15 import WARM as NW_WARM
from c11_primary_decision_seam_v1.rpd5_producer import WARM as RPD_WARM

INTERVAL_NS = 60_000_000_000


def readiness(bot, policy, pair, symbol, end, errors):
    launch = bot.exchange.markets[pair]['info'].get('onboardDate')
    launch = launch if type(launch) is int and 0 < launch <= end else None
    statuses={}
    try:
        policy.candles.update_nw(bot,pair,symbol,end)
    except errors:
        statuses['NW15']='RECENT_PUBLIC_INPUT_UNAVAILABLE'
    now=time.time_ns()//1_000_000
    candles,minutes,nw=policy.bars.recent_primary(symbol,end,now)
    age15=(end-launch)//QUARTER if launch else None
    if nw is not None:
        statuses['NW15']=nw.status if nw.status!='UNKNOWN' else nw.reason
    else:
        statuses.setdefault('NW15','WARMUP_LT_260_15M' if age15 is not None and age15<NW_WARM
                            else 'RECENT_CLOSED_15M_INCOMPLETE')
    state=None
    try:
        state=policy.candles.update_rpd(bot,pair,symbol,end)
    except errors:
        statuses['RPD5']='CONTINUATION_UNAVAILABLE'
    if state is not None:
        if state.next_open==end:
            statuses['RPD5']='KNOWN' if state.n>=RPD_WARM else 'WARMUP_LT_260_5M'
        else:
            statuses['RPD5']='CONTIGUOUS_INPUT_PENDING'
    else:
        statuses.setdefault('RPD5','WARMUP_LT_260_5M' if launch is not None and (end-launch)//FIVE<RPD_WARM
                            else 'TRUSTED_ORIGIN_OR_CHECKPOINT_REQUIRED')
    # The 5m route can advance the checkpoint beyond today's last 15m
    # decision. Read the actual older sample without scrolling it backwards.
    closed_rpd=state.closed() if state is not None and state.next_open==end else None
    if closed_rpd is None:
        closed_rpd=policy.bars.rpd_sample(symbol,end,time.time_ns()//1_000_000)
        if closed_rpd is not None:
            statuses['RPD5']='KNOWN' if closed_rpd.known else 'WARMUP_LT_260_5M'
    try:
        statuses['ATR_ER'],_=policy.facts.features.warm(bot,pair,end)
    except errors+(sqlite3.Error,):
        statuses['ATR_ER']='ATR_ER_CONTINUATION_UNAVAILABLE'
    statuses['EWO_ZL']=('WARMUP_LT_701_15M' if age15 is not None and age15<701
                        else 'INDEPENDENT_STATE_NOT_CHECKED')
    now=time.time_ns()//1_000_000
    policy.bars.feature_status(symbol,end,now,statuses)
    return (candles,minutes,nw),closed_rpd


class FeatureWarmup:
    def __init__(self):
        self.next_attempt_ns=0
        self.last_symbol=None

    def after_protection(self,bot,*,verified_cycle_rows=None):
        from standalone_policy import INPUT_ERRORS
        tick=time.monotonic_ns()
        if tick<self.next_attempt_ns:
            return
        self.next_attempt_ns=tick+INTERVAL_NS
        # None is the exact original disabled owner path. An active lane
        # passes an explicit tuple, even empty; never retry or reuse the
        # watchlist when a just-authenticated current observation is missing.
        ranked=bot.feed.ranked() if verified_cycle_rows is None else verified_cycle_rows
        for i,item in enumerate(ranked):
            if item.symbol==self.last_symbol:
                ranked=ranked[i+1:]+ranked[:i+1]
                break
        for candidate in ranked:
            if candidate.expires_ns<=time.monotonic_ns() or not bot.observed_ranked(candidate):
                continue
            self.last_symbol=candidate.symbol
            now=time.time_ns()//1_000_000
            try:
                readiness(bot,bot.policy,candidate.pair,candidate.symbol,now//QUARTER*QUARTER,INPUT_ERRORS)
            except INPUT_ERRORS+(sqlite3.Error,):
                pass
            return
