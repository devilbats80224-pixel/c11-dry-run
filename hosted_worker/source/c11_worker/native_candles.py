"""Record only Binance's actual closed data and local receipt times."""
import time

from bridge_protocol import Refused, need
from causal_bars import ClosedBar, MINUTE, FIVE, QUARTER, HOUR
from c11_primary_decision_seam_v1.rpd5_producer import SegmentStart
from c11_nw15_primary_signal_port_v1.primary15 import WARM as NW_WARM

INTERVALS = {MINUTE: '1m', FIVE: '5m', QUARTER: '15m', HOUR: '1h'}


class NativeCandles:
    def __init__(self, store):
        self.store = store
        self.filled = set()
        self.filled_nw_lanes = set()
        self.rpd_filled = set()

    def query(self, bot, symbol, width, start, end, limit, *, contiguous=False, primary_pull=False,
              fallback_lease_ns=None):
        owned=getattr(bot,'_c11_owned_data_lane',None)
        if (primary_pull and owned is not None and
                owned.covers_native_public(symbol,width,start,end,limit)):
            # Let existing feature callers report unavailable until owner commit;
            # never repeat an already reserved/issued exact public request.
            raise Refused('C11_PRIMARY_OWNER_PUBLIC_PENDING')
        requested = time.time_ns() // 1_000_000
        need(0 < limit <= 1500 and end <= requested, 'C11_PUBLIC_KLINE_SCOPE')
        if fallback_lease_ns is not None:
            need(end<=requested<end+MINUTE and time.monotonic_ns()<fallback_lease_ns,
                 'C11_PRIMARY_NATIVE_FALLBACK_EXPIRED')
        raw = bot.exchange._api.fapiPublicGetKlines(
            dict(symbol=symbol, interval=INTERVALS[width], startTime=start, endTime=end - 1, limit=limit))
        receipt = time.time_ns() // 1_000_000
        need(type(raw) is list and len(raw) <= limit and receipt >= requested, 'C11_PUBLIC_KLINE_RESPONSE')
        rows = []
        for row in raw:
            opening = int(row[0])
            need(start <= opening < end and int(row[6]) == opening + width - 1,
                 'C11_PUBLIC_KLINE_CLOCK')
            if opening + width <= requested:
                rows.append(ClosedBar(opening, receipt, *(float(x) for x in row[1:6]), float(row[7])))
        need(all(a.open_ms < b.open_ms for a,b in zip(rows, rows[1:])), 'C11_PUBLIC_KLINE_ORDER')
        if contiguous:
            need(bool(rows) and all(row.open_ms == start+i*width for i,row in enumerate(rows)),
                 'C11_RPD_HISTORY_GAP')
        self.store.record(symbol, width, rows)
        return rows, receipt

    @staticmethod
    def market(bot,pair,symbol):
        market=bot.exchange.markets.get(pair)
        need(market is not None and market['id']==symbol and market['symbol']==pair
             and market['active'] is True and market['swap'] is True and market['linear'] is True
             and market['settle']==market['quote']=='USDT','C11_NATIVE_MARKET_ID')
        return market

    def update_nw(self,bot,pair,symbol,end):
        self.market(bot,pair,symbol)
        key=pair,end
        if key in self.filled:
            return
        self.filled={x for x in self.filled if x[1]>=end-QUARTER}
        owned=getattr(bot,'_c11_owned_data_lane',None)
        if owned is None:
            # Preserve original disabled-path behavior unchanged.
            self.filled.add(key)
            self.query(bot,symbol,QUARTER,end-NW_WARM*QUARTER,end,NW_WARM,primary_pull=True)
            self.query(bot,symbol,MINUTE,end-30*MINUTE,end,30,primary_pull=True)
            return
        self.filled_nw_lanes={x for x in self.filled_nw_lanes if x[1]>=end-QUARTER}
        waiting=False
        for width,count in ((QUARTER,NW_WARM),(MINUTE,30)):
            start=end-count*width
            lane=pair,end,width
            if lane in self.filled_nw_lanes:continue
            now=time.time_ns()//1_000_000
            # CausalBars.range requires a complete contiguous canonical span,
            # with actual persisted receipts no later than the observed as-of.
            # Previously complete rows need no duplicate wire.
            if self.store.range(symbol,width,start,end,now):
                self.filled_nw_lanes.add(lane)
                continue
            if owned.covers_native_public(symbol,width,start,end,count):
                waiting=True
                continue
            if not end<=now<end+MINUTE:
                waiting=True
                continue
            candidate=next((x for x in bot.feed.latest_owner_observation
                            if x.pair==pair and x.symbol==symbol and time.monotonic_ns()<x.expires_ns),None)
            if candidate is None or not bot.observed_ranked(candidate):
                waiting=True
                continue
            if width==QUARTER:
                from closed_minute_basis import project_closed_minutes
                quarters=project_closed_minutes(self.store,symbol,width,start,end,now)
                if quarters is not None:
                    # The already admitted minute basis is sufficient. Canonical
                    # record refuses a disagreement; never overwrite it or ask
                    # the network to adjudicate an already available projection.
                    def still_current():
                        return (end<=time.time_ns()//1_000_000<end+MINUTE
                                and time.monotonic_ns()<candidate.expires_ns
                                and any(row is candidate for row in bot.feed.latest_owner_observation))
                    self.store.record_owner_primary_batch(
                        symbol,((MINUTE,()),(FIVE,()),(QUARTER,quarters)),still_current)
                    self.filled_nw_lanes.add(lane)
                    continue
            self.query(bot,symbol,width,start,end,count,primary_pull=True,
                       fallback_lease_ns=candidate.expires_ns)
            if self.store.range(symbol,width,start,end,time.time_ns()//1_000_000):
                self.filled_nw_lanes.add(lane)
            else:
                # A 200 with empty/short/gapped raw rows may advance storage,
                # but cannot poison success tracking. No extra request here:
                # the next natural native owner cycle may attempt only the
                # still missing lane if the same authority is still valid.
                waiting=True
        if waiting:
            raise Refused('C11_PRIMARY_OWNER_PUBLIC_PENDING')
        self.filled.add(key)

    def update_rpd(self,bot,pair,symbol,end):
        market=self.market(bot,pair,symbol)
        launch=market['info'].get('onboardDate')
        if type(launch) is not int or launch<=0 or launch%FIVE or launch>=end:
            return None
        now=time.time_ns()//1_000_000
        proof=self.store.segment(symbol)
        need(proof is None or proof.open_ms==launch,'C11_RPD_GENESIS_CHANGED')
        state=self.store.rpd_progress(symbol,end,now) if proof is not None else None
        if state is not None and state.next_open==end:
            return state
        first=state.next_open if state is not None else launch
        # The local checkpoint may catch up one page per loop. Network only
        # fills a genuinely recent tail or a genuinely recent listing origin.
        count=(end-first)//FIVE
        key=pair,end
        if not 0<count<=499 or key in self.rpd_filled:
            return state
        self.rpd_filled={x for x in self.rpd_filled if x[1]>=end-QUARTER}
        owned=getattr(bot,'_c11_owned_data_lane',None)
        if owned is None:self.rpd_filled.add(key)  # exact disabled/original path
        rows,receipt=self.query(bot,symbol,FIVE,first,end,count,contiguous=True,primary_pull=True)
        if owned is not None:self.rpd_filled.add(key)
        if proof is None and rows[0].open_ms==launch:
            self.store.set_segment(SegmentStart(symbol,launch,receipt))
        return self.store.rpd_progress(symbol,end,receipt)

    def update_entry_hour(self,bot,symbol,end):
        hour_end=end//HOUR*HOUR
        self.query(bot,symbol,HOUR,hour_end-2*HOUR,hour_end,2)
