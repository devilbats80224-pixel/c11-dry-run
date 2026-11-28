"""Native feed and rank observer must agree across a classifier lease rollover."""
from datetime import datetime, timezone
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

root = Path(__file__).parent / "source"
sys.path[:0] = [str(root / "c11_owned_modules"), str(root / "c11_worker")]
import entry_feed
from docker_observer import CaptureBridge, DockerObserver
from observer_rpc import ObserverClient

end = 90_000_000
now = end + 1_004
symbol = "TESTUSDT"
pair = "TEST/USDT:USDT"
generation = 42
provider_seen = end - 874
provider_expires = provider_seen + 60_000
binding = "a" * 64


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()


def classifier(stamp):
    return SimpleNamespace(
        verified={symbol: pair}, body_sha256="b" * 64,
        received_monotonic_ns=stamp * 1_000_000,
    )


class Publication:
    def response(self, nonce):
        rows = [{"symbol": symbol, "native_oi_change_24h_raw": "0.25"}]
        rows.extend({"symbol": f"BAR{i}USDT", "native_oi_change_24h_raw": "0.00"} for i in range(59))
        return dict(nonce=nonce, status="FRESH", source=entry_feed.SOURCE,
                    query_sha256=entry_feed.QUERY, offset=0, page_size=60,
                    generation=generation, left_ms=provider_expires-now,
                    received_at=iso(provider_seen), lease_until=iso(provider_expires), rows=rows)


def wall():
    return now


def mono():
    return wall() * 1_000_000


with TemporaryDirectory() as dirname, patch("time.time_ns", mono), patch("time.monotonic_ns", mono):
    observer = DockerObserver(Path(dirname).resolve() / "docker-observer.sqlite", binding, wall=wall, mono=mono)
    try:
        bridge = CaptureBridge(Publication(), observer, binding, lambda: feed.classification)
        bridge.send(dict(op="C11_PAPER_BRIDGE_V1", nonce="0" * 32))
        bridge.receive(2)
        feed = entry_feed.EntryFeed(bridge, generation)
        # Exactly 297 ms of the initial Binance exchangeInfo lease remains.
        feed.classification = classifier(end + 1_301 - 300_000)
        feed._reviewed_metaclass_fetch = lambda: classifier(now)
        rank = ObserverClient(bridge, binding)
        (first,) = feed.read()
        assert rank.point(first.symbol, now) == (first.generation, first.rank, first.ratio)
        assert rank.point(first.symbol, end) is None, "Must not backdate observer evidence"

        now = end + 2_000
        assert rank.point(first.symbol, now) is None, "Thin proof really expired before refresh"
        now = end + 3_491
        (current,) = feed.read()
        assert current.expires_ns > mono() and current.generation == generation
        assert rank.point(current.symbol, now) == (current.generation, current.rank, current.ratio), (
            "C11_FRESH_FEED_BUT_OBSERVER_SCOPE_EXPIRED", now-end, current.expires_ns//1_000_000-end)
        assert rank.point(current.symbol, end) is None, "Must not backdate observer evidence"
        assert rank.point(current.symbol, end+2_000) is None, "Do not erase an elapsed classifier gap"
        now = provider_expires
        assert rank.point(current.symbol, now) is None, "Must not exceed the provider's lease"
    finally:
        observer.close()

print("C11_OBSERVER_CLASSIFIER_LEASE_PASS")
