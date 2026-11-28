"""Credential-free next-open and protection evidence for archived C11."""
from atr_er_state import REQUIRED_QUARTERS
from dataclasses import dataclass, replace
from decimal import Decimal
import math
import time

from atr_component import initial, close_update, ceil_tick
from atr_er_checkpoint import AtrErCheckpoint, PAGE
from bridge_protocol import need
from causal_bars import FIVE, HOUR, MINUTE, QUARTER
from confirmed_features_v1 import sample
from hour_stage import transport
from policy_snapshot import Bar, Published, State

# This is a maximum observation age, not a trading grace period.
BOOK_AGE_MS = 2_000


def wall_ms():
    return time.time_ns() // 1_000_000


@dataclass(frozen=True)
class NextOpen:
    symbol: str
    side: str
    open_ms: int
    price: float
    side_quantity: float
    quote_ms: int
    received_ms: int
    expires_ms: int


def next_open(api, symbol, end, side):
    """Prove the active candle's open and an equal, executable-side public quote."""
    start = wall_ms()
    if type(end) is not int or not end % MINUTE == 0 or not end <= start < end + MINUTE:
        return None
    need(side in ('buy', 'sell'), 'C11_OPEN_ORDER_SIDE')
    rows = api.fapiPublicGetKlines(dict(symbol=symbol, interval='1m', startTime=end,
                                       endTime=end + MINUTE - 1, limit=1))
    sampled = wall_ms()
    if (type(rows) is not list or len(rows) != 1 or sampled >= end + MINUTE
            or not start <= sampled or type(rows[0]) is not list or len(rows[0]) < 9):
        return None
    row = rows[0]
    if int(row[0]) != end or int(row[6]) != end + MINUTE - 1 or int(row[8]) <= 0:
        return None
    op = Decimal(row[1])
    book = api.fapiPublicGetTickerBookTicker(dict(symbol=symbol))
    received = wall_ms()
    quote = book['time']
    if book['symbol'] != symbol or type(quote) is not int or not end <= quote <= received:
        return None
    deadline = min(end + MINUTE, quote + BOOK_AGE_MS)
    if not sampled <= received < deadline:
        return None
    side_price = Decimal(book['askPrice'] if side == 'buy' else book['bidPrice'])
    size = Decimal(book['askQty'] if side == 'buy' else book['bidQty'])
    if not (op.is_finite() and side_price.is_finite() and size.is_finite()
            and op == side_price and op > 0 and size > 0 and math.isfinite(float(op)) and math.isfinite(float(size))):
        return None
    return NextOpen(symbol, side, end, float(op), float(size), quote, received, deadline)


class FeatureSpan:
    """Join native closed-clock ATR/ER checkpoints without resetting their origin."""
    def __init__(self, store, candles):
        self.store, self.candles = store, candles
        self.checkpoint = AtrErCheckpoint(store)
        self.attempts = set()

    def _extend(self, bot, symbol, width, proof, target, now, allow_fetch):
        before = self.checkpoint.cursor(symbol, width, proof)
        next_open = self.checkpoint.extend(symbol, width, proof, target, now)
        key = symbol, width, target
        left = (target - next_open) // width
        # Use bounded public I/O only to fill a near tail from a known seed.
        if (not allow_fetch or key in self.attempts or not 0 < left <= PAGE
                or next_open - before >= PAGE * width):
            return
        self.attempts = {item for item in self.attempts if item[2] >= target - QUARTER}
        self.attempts.add(key)
        self.candles.query(bot, symbol, width, next_open, target, left, contiguous=True)
        self.checkpoint.extend(symbol, width, proof, target, wall_ms())

    def warm(self, bot, pair, end, *, allow_fetch=False):
        now = wall_ms()
        if type(end) is not int or end % MINUTE or end > now:
            return 'ATR_ER_INVALID_CLOCK', None
        market = bot.exchange.markets[pair]
        symbol, launch = market['id'], market['info'].get('onboardDate')
        if type(launch) is not int or launch <= 0 or launch % QUARTER or launch > end:
            return 'ATR_ER_ORIGIN_UNVERIFIED', None
        warming = (end // QUARTER * QUARTER - launch) // QUARTER < REQUIRED_QUARTERS
        proof = self.store.contract_segment(symbol,launch,now)
        if proof is None:
            return ('WARMUP_LT_401_15M' if warming else 'ATR_ER_TRUSTED_ORIGIN_REQUIRED'), None
        warming = (end // QUARTER * QUARTER - proof.open_ms) // QUARTER < REQUIRED_QUARTERS
        targets = {width: end // width * width for width in (FIVE, QUARTER)}
        for width, target in targets.items():
            self._extend(bot, symbol, width, proof, target, wall_ms(), allow_fetch)
        if warming:
            return 'WARMUP_LT_401_15M', None
        now = wall_ms()
        m5 = self.checkpoint.sample(symbol, FIVE, proof, targets[FIVE], now)
        m15 = self.checkpoint.sample(symbol, QUARTER, proof, targets[QUARTER], now)
        previous = self.checkpoint.sample(symbol, QUARTER, proof, targets[QUARTER] - QUARTER, now)
        if not (m5 and m15 and previous):
            return 'ATR_ER_CONTIGUOUS_INPUT_PENDING', None
        result, = sample([end], {targets[FIVE]: m5},
                         {targets[QUARTER]: m15, targets[QUARTER] - QUARTER: previous})
        if not result['known']:
            return 'ATR_ER_WARMUP_INCOMPLETE', None
        return 'ATR_ER_KNOWN', dict(result, origin_ms=proof.open_ms,
            observed_at_ms=max(x['actual_received_ms'] for x in (m5, m15, previous)))

    def read(self, bot, pair, end, *, allow_fetch=True):
        return self.warm(bot, pair, end, allow_fetch=allow_fetch)[1]


def replay_early(entry, hard, started, bars, features, hour_evidence, tick, buffer):
    """Replay only the early phase preceding the archive's 30m and 60m checks."""
    if (not bars or not started % MINUTE == 0 or not 0 < hard < entry
            or bars[-1].open_ms + MINUTE >= started + 15*MINUTE
            or len(bars) != len(features)):
        return None
    state = State(entry=entry, hard_stop=hard, entry_E=started, last_E=started)
    atr = initial(entry)
    atr = replace(atr, guard=replace(atr.guard, loss=replace(atr.guard.loss, sl=hard)), last_E=started)
    for index, (row, feature) in enumerate(zip(bars, features)):
        end = row.open_ms + MINUTE
        if (row.open_ms != started+index*MINUTE or feature is None or feature['E'] != end
                or feature['known'] is not True or row.op <= buffer):
            return None
        bar = Bar(end, row.op, row.high, row.low, row.close)
        state, action = transport('MID_4_6_08_16', 'INTRA_WEAK', state, bar,
                                  published=Published(), atr=atr, hour=hour_evidence(end), original=None)
        if action['reason'] != 'HOLD' or not state.held:
            return None
        atr, _ = close_update(
            atr, E=end, close=row.close, high=row.high, atr15=feature['loss_atr15'],
            atr5=feature['guard_atr5'], mult=feature['guard_multiplier'], er=feature['signed_er'],
            previous_er=feature['previous_signed_er'], tick=tick, boundary=buffer,
            source15=feature['source_at15'], source5=feature['source_at5'],
            source_er=feature['source_at_er'], known=True)
        state = replace(state, passed_stage=ceil_tick(state.passed_stage,tick) if state.passed_stage else 0.)
        # Subset where maturity and both original queued exits have no predecessor.
        if atr.pending or atr.guard.loss.author_armed or atr.published or state.passed_stage or state.pending:
            return None
    return state, atr


def archived_half_window(entered):
    return ((entered + FIVE - 1)//FIVE)*FIVE + 2*FIVE
