"""Synthetic stand-in for durable storage, only used by public adapter CI."""
from dataclasses import dataclass
from types import SimpleNamespace

MINUTE = 60_000
FIVE = 5 * MINUTE
QUARTER = 15 * MINUTE


@dataclass(frozen=True)
class ClosedBar:
    open_ms: int
    received_ms: int
    op: float
    high: float
    low: float
    close: float
    volume: float
    quote: float

    def valid(self, width):
        return self.open_ms % width == 0 and self.received_ms >= self.open_ms + width


class CausalBars:
    def __init__(self, path):
        if path != ":memory:":
            raise ValueError("public fixture may not access a real store")
        self.items = {}
        self.db = SimpleNamespace(close=lambda: None)

    def record(self, symbol, width, bars):
        for b in bars:
            self.items[symbol, width, b.open_ms] = b

    def range(self, symbol, width, start, end, now):
        return tuple(self.items[symbol, width, t] for t in range(start, end, width)
                     if (symbol, width, t) in self.items
                     and self.items[symbol, width, t].received_ms <= now)
