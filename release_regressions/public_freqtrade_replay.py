"""Public contract replay: real Freqtrade classes, synthetic wire/store/strategy.

This fixture does not import or certify the private C11 Worker.
"""
import argparse
import sqlite3
from datetime import datetime, timezone

import pandas as pd
from freqtrade import __version__
from freqtrade.data.dataprovider import DataProvider
from freqtrade.enums import CandleType, RunMode
from freqtrade.strategy import IStrategy
from oi20_contract.fixture import make_publication, test_scope

PAIR = "龙虾/USDT:USDT"
TYPE = CandleType.FUTURES
MINUTE = 60
START = 1767225600  # UTC on a 15m boundary
BASE = (PAIR, "1m", TYPE)
WIDE = [(PAIR, period, TYPE) for period in ("5m", "15m")]
COLUMNS = ["date", "open", "high", "low", "close", "volume"]


class FixtureExchange:
    def __init__(self, db, end, inject):
        self.db, self.end, self.inject = db, end, inject
        self.cache, self.wire = {}, []
        self.fixture = [(ts, 100 + i) for i, ts in enumerate(range(START, end, MINUTE))]
        with db:
            db.execute("CREATE TABLE IF NOT EXISTS minutes (ts INTEGER PRIMARY KEY, close INTEGER)")

    def seed(self, rows):
        with self.db:
            self.db.executemany("INSERT OR IGNORE INTO minutes VALUES (?, ?)", rows)

    def native_wire(self, key):
        self.wire.append(key)
        if key == BASE:
            # Pretend this page came from an exchange; include an open candle
            # so the test can detect using it before it is closed.
            return self.fixture + [(self.end, 999999)]
        return []

    def refresh_latest_ohlcv(self, pairs, *args, **kwargs):
        self.cache.clear()
        if BASE not in pairs:
            raise AssertionError("MISSING_NATIVE_MINUTE_REQUEST")
        if self.inject == "no-refill":
            return  # Reproduce the broken dependency on an already-analyzed frame.
        if self.inject == "direct-wide":
            self.native_wire(WIDE[-1])
        closed = [row for row in self.native_wire(BASE) if row[0] + MINUTE <= self.end]
        if self.inject == "drop-minute":
            closed.pop(0)
        self.seed(closed)
        self.project(pairs)

    def project(self, pairs):
        for key in WIDE:
            if key not in pairs:
                continue
            width = int(key[1][:-1])
            rows = self.db.execute(
                "SELECT ts, close FROM minutes WHERE ts >= ? AND ts < ? ORDER BY ts",
                (START, self.end),
            ).fetchall()
            if [x[0] for x in rows] != list(range(START, self.end, MINUTE)):
                self.cache.pop(key, None)
                continue
            bars = []
            for idx in range(0, len(rows) - width + 1, width):
                part = rows[idx:idx + width]
                values = [x[1] for x in part]
                bars.append((datetime.fromtimestamp(part[0][0], timezone.utc),
                             values[0], max(values), min(values), values[-1], width))
            self.cache[key] = pd.DataFrame(bars, columns=COLUMNS)

    def klines(self, key, copy=True):
        frame = self.cache.get(key, pd.DataFrame())
        return frame.copy() if copy else frame


class FixtureSignal(IStrategy):
    timeframe = "15m"
    minimal_roi = {"0": 0.1}
    stoploss = -0.1

    def populate_indicators(self, dataframe, metadata):
        return dataframe

    def populate_entry_trend(self, dataframe, metadata):
        dataframe["enter_long"] = (dataframe["close"] >= 129).astype(int)
        return dataframe

    def populate_exit_trend(self, dataframe, metadata):
        dataframe["exit_long"] = 0
        return dataframe


def require(ok, message):
    if not ok:
        raise AssertionError(message)


def verify(case, inject):
    cfg = {"exchange": {}, "runmode": RunMode.DRY_RUN, "timeframe": "15m",
           "candle_type_def": TYPE}
    with sqlite3.connect(":memory:") as db:
        ex = FixtureExchange(db, START + 30 * MINUTE, inject)
        if case == "partial":
            ex.seed(ex.fixture[:22])
        elif case == "stale":
            ex.seed([(START - 100 * 86400, 777)])
        dp = DataProvider(cfg, ex)
        s = FixtureSignal(cfg)
        s.dp = dp

        # A blank/partial/stale store alone must not fabricate a usable bar.
        ex.project(WIDE)
        require(dp.ohlcv(PAIR, "15m", candle_type=TYPE).empty, "PREMATURE_NATIVE_FRAME")
        snapshot = make_publication(ex.end * 1000)
        candidates = test_scope(snapshot, ex.end * 1000)
        require(candidates == [(3, PAIR)], "OI20_RANK_OR_SYMBOL_NOT_PRESERVED")
        require(not test_scope(snapshot, snapshot["expires_ms"]), "EXPIRED_OI20_REUSED")
        malformed = {**snapshot, "provider_ranks_v1": [3] * len(snapshot["rows"])}
        require(not test_scope(malformed, ex.end * 1000), "MALFORMED_OI20_ACCEPTED")
        # Only candidates actually delivered by the isolated OI20 boundary
        # can produce a provider refresh or be evaluated by Freqtrade.
        for _, admitted in candidates:
            dp.refresh([(admitted, tf, TYPE) for tf in ("1m", "5m", "15m")])
        require(not any(key != BASE for key in ex.wire), "UNEXPECTED_WIDER_WIRE")
        require(ex.wire == [BASE], "MISSING_BASE_REFILL_REQUEST")
        n = db.execute(
            "SELECT count(*) FROM minutes WHERE ts >= ? AND ts < ?", (START, ex.end)
        ).fetchone()[0]
        require(n == 30, "CLOSED_MINUTES_NOT_PERSISTED")
        require(db.execute("SELECT count(*) FROM minutes WHERE ts >= ?", (ex.end,)).fetchone()[0]
                == 0, "OPEN_CANDLE_WAS_ADMITTED")
        df5 = dp.ohlcv(PAIR, "5m", candle_type=TYPE)
        df15 = dp.ohlcv(PAIR, "15m", candle_type=TYPE)
        require(len(df5) == 6 and len(df15) == 2, "MISSING_NATIVE_WIDER_FRAME")
        require(df5.iloc[-1].close == df15.iloc[-1].close == 129,
                "INCONSISTENT_LAST_CLOSE")
        # Use the actual Freqtrade strategy analyzer and analyzed-data cache.
        for _, admitted in candidates:
            s.analyze_pair(admitted)
        result, _ = dp.get_analyzed_dataframe(PAIR, "15m")
        require(not result.empty and result.enter_long.tolist() == [0, 1],
                "NO_EXPECTED_NATIVE_DECISION")
        ex.end += 15 * MINUTE  # no new data in the fixture
        ex.project(WIDE)
        require(dp.ohlcv(PAIR, "15m", candle_type=TYPE).empty, "STALE_NATIVE_FRAME_REUSED")
        print(f"FREQTRADE_NATIVE_CONTRACT_PASS {case}")
        print(f"OI20_CANDIDATE_DELIVERED_TO_FREQTRADE {case}")


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("--case", choices=("blank", "partial", "stale"))
    cli.add_argument("--inject", choices=("direct-wide", "drop-minute", "no-refill"))
    args = cli.parse_args()
    require(__version__ == "2026.10" or __version__.startswith("2026.10-"),
            "WRONG_FREQTRADE_VERSION")
    for case in (args.case,) if args.case else ("blank", "partial", "stale"):
        verify(case, args.inject)
