"""Exercise proposed C11 projection -> persisted 1m -> real Freqtrade.

The transport, disk-backed test store and strategy are synthetic. This
regression deliberately does not claim full proprietary Worker acceptance.
"""
import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pandas as pd
from freqtrade.data.dataprovider import DataProvider
from freqtrade.enums import CandleType, RunMode
from freqtrade.strategy import IStrategy

from causal_bars import ClosedBar
from local_candle_refresh import refresh_with_local_basis
from native_minute_projection import MIN, QUARTER, project

PAIR = "龍蝦/USDT:USDT"
TYPE = CandleType.FUTURES
END = 1767225600000
START = END - 260 * QUARTER
RECEIVED = END + 900
KEYS = [(PAIR, tf, TYPE) for tf in ("1m", "5m", "15m")]


class Store:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("CREATE TABLE IF NOT EXISTS minutes (t INTEGER PRIMARY KEY, close REAL)")

    def save(self, rows):
        with self.db:
            self.db.executemany("INSERT INTO minutes VALUES (?, ?)", rows)

    def range(self, symbol, width, start, end, now):
        assert symbol == PAIR and width == MIN and now == RECEIVED
        rows = self.db.execute("SELECT t, close FROM minutes WHERE t>=? AND t<? ORDER BY t",
                               (start, end)).fetchall()
        return tuple(ClosedBar(t, RECEIVED, c, c, c, c, 1, c) for t, c in rows)


class Signal(IStrategy):
    timeframe = "15m"
    minimal_roi = {"0": 0.1}
    stoploss = -0.1

    def populate_indicators(self, dataframe, metadata):
        return dataframe

    def populate_entry_trend(self, dataframe, metadata):
        self.calls += 1
        dataframe["enter_long"] = (dataframe["close"] == 105).astype(int)
        return dataframe

    def populate_exit_trend(self, dataframe, metadata):
        dataframe["exit_long"] = 0
        return dataframe


def check(path):
    opens = list(range(END - 45 * MIN, END, MIN))
    def price(t):
        return 105 if t == END - MIN else 100
    raw = tuple((t, price(t), price(t), price(t), price(t), 1,
                 t + MIN - 1, price(t), 1, 0, 0, 0) for t in opens)
    prefix = tuple(ClosedBar(t, t + QUARTER, 100, 100, 100, 100, 15, 1500)
                   for t in range(START, END - QUARTER, QUARTER))
    batches = project(END, raw, RECEIVED, RECEIVED, (), prefix)
    assert batches is not None and batches[0][0] == MIN, "NO_NATIVE_PROJECTION"
    store = Store(path)
    store.save((t, price(t)) for t in range(START, opens[0], MIN))
    store.save((bar.open_ms, bar.close) for bar in batches[0][1])
    store.db.close()

    store = Store(path)
    try:
        actual = store.db.execute("SELECT t, close FROM minutes WHERE t>=? ORDER BY t",
                                  (opens[0],)).fetchall()
        assert actual == [(t, price(t)) for t in opens], (
            f"RAW_1M_DURABLE_ROUNDTRIP expected=45 actual={len(actual)}"
        )
        cache, wire, provenance = {}, [], {}
        def native(keys, **kw):
            wire.extend(keys)
            return {}
        def refresh(keys, **kw):
            return refresh_with_local_basis(
                native_refresh=native, cache_view=cache, store=store,
                market_id=lambda pair: pair, now_ms=lambda: RECEIVED,
                pair_list=keys, allow_native_15m_fallback=True,
                provenance=provenance, **kw)
        exchange = SimpleNamespace(
            refresh_latest_ohlcv=refresh,
            klines=lambda key, copy=True: cache.get(key, pd.DataFrame()).copy(),
        )
        cfg = {"exchange": {}, "runmode": RunMode.DRY_RUN, "timeframe": "15m",
               "candle_type_def": TYPE}
        provider = DataProvider(cfg, exchange)
        provider.refresh(KEYS)
        assert wire == [KEYS[0]], "WIDER_WIRE_DESPITE_COMPLETE_MINUTES"
        assert provenance == {k: "closed_1m_projection" for k in KEYS[1:]}
        assert cache[KEYS[1]].close.iloc[-1] == cache[KEYS[2]].close.iloc[-1] == 105
        strategy = Signal(cfg)
        strategy.dp, strategy.calls = provider, 0
        strategy.analyze_pair(PAIR)
        result, _ = provider.get_analyzed_dataframe(PAIR, "15m")
        assert strategy.calls == 1 and len(result) == 260, "MISSING_FREQTRADE_CALLBACK"
        assert result.enter_long.tolist() == [0] * 259 + [1], "WRONG_FREQTRADE_DECISION"
    finally:
        store.db.close()


with TemporaryDirectory() as scratch:
    check(Path(scratch) / "projection.sqlite")
print("C11_45_DURABLE_MINUTES_AND_REAL_FREQTRADE_CALLBACK_PASS")
