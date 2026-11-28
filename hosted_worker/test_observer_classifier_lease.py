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
from bridge_protocol import Refused
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
    ratio = "0.25"

    def response(self, nonce):
        rows = [{"symbol": symbol, "native_oi_change_24h_raw": self.ratio}]
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
        publication = Publication()
        bridge = CaptureBridge(publication, observer, binding, lambda: feed.classification)
        bridge.send(dict(op="C11_PAPER_BRIDGE_V1", nonce="0" * 32))
        bridge.receive(2)
        feed = entry_feed.EntryFeed(bridge, generation)
        # Exactly 297 ms of the initial Binance exchangeInfo lease remains.
        feed.classification = classifier(end + 1_301 - 300_000)
        feed._reviewed_metaclass_fetch = lambda: classifier(now)
        rank = ObserverClient(bridge, binding, docked_current=True)
        # Inject the existing refresh cooldown to force the real expiry seam.
        feed.retry_metadata_ns = (end + 1_301) * 1_000_000
        (first,) = feed.read()
        assert rank.current_point(first.symbol, now) == (first.generation, first.rank, first.ratio)
        assert rank.point(first.symbol, end) is None, "Must not backdate observer evidence"

        now = end + 2_000
        assert rank.current_point(first.symbol, now) is None, "Thin proof really expired before refresh"
        now = end + 3_491
        (current,) = feed.read()
        assert current.expires_ns > mono() and current.generation == generation
        assert rank.current_point(current.symbol, now) == (current.generation, current.rank, current.ratio), (
            "C11_FRESH_FEED_BUT_OBSERVER_SCOPE_EXPIRED", now-end, current.expires_ns//1_000_000-end)
        assert rank.point(current.symbol, end) is None, "Must not backdate observer evidence"
        assert rank.point(current.symbol, end+2_000) is None, "Do not erase an elapsed classifier gap"
        assert rank.point(current.symbol, now) is None, "Current-only proof must not become a historical point"
        assert rank.current_point(current.symbol, end+2_000) is None, "Cannot backdate a renewed receipt"
        now += 1
        publication.ratio = "0.26"
        try:
            feed.read()
        except Refused as e:
            assert str(e) == "C11_OBSERVER_GENERATION_REWRITE"
        else:
            raise AssertionError("Changed source payload must not renew this generation")
        assert rank.current_point(current.symbol, now) is None, "A failed refresh must revoke current proof"
        publication.ratio = "0.25"
        feed.read()
        assert rank.current_point(current.symbol, now) == (current.generation, current.rank, current.ratio)
        now = provider_expires
        assert rank.current_point(current.symbol, now) is None, "Must not exceed the provider's lease"
        assert rank.current_point(current.symbol, now) is None, "Current proof must obey publisher expiry"
    finally:
        observer.close()

print("C11_OBSERVER_CLASSIFIER_LEASE_PASS")

# Production-shaped boundary: fresh source generation, but the cached
# exchange classifier would otherwise clip all offers to E+4.201s.
provider_seen = end + 1_456
provider_expires = provider_seen + 60_000
now = end + 3_271
with TemporaryDirectory() as dirname, patch("time.time_ns", mono), patch("time.monotonic_ns", mono):
    observer = DockerObserver(Path(dirname).resolve() / "docker-observer.sqlite", binding, wall=wall, mono=mono)
    try:
        bridge = CaptureBridge(Publication(), observer, binding, lambda: feed.classification)
        bridge.send(dict(op="C11_PAPER_BRIDGE_V1", nonce="0"*32))
        bridge.receive(2)
        feed = entry_feed.EntryFeed(bridge, generation)
        feed.classification = classifier(end + 4_201 - 300_000)
        refreshes = []
        def refresh():
            refreshes.append(now)
            return classifier(now)
        feed._reviewed_metaclass_fetch = refresh
        (first,) = feed.read()
        assert refreshes == [end + 3_271], (
            "C11_THIN_CLASSIFIER_MINTED_SHORT_NATIVE_SCOPE", first.expires_ns//1_000_000-end
        )
        assert first.expires_ns == provider_expires*1_000_000
        now = end + 7_797
        (current,) = feed.read()
        proof = ObserverClient(bridge, binding, docked_current=True)
        assert first.generation == current.generation and current.expires_ns == first.expires_ns
        assert proof.current_point(current.symbol, now) == (current.generation,current.rank,current.ratio)
        assert refreshes == [end + 3_271], "Do not refetch metadata for each candidate"

        # A transient early refresh failure leaves only the exact still-live
        # prior proof; the regular refresh can still run at cache expiry.
        feed = entry_feed.EntryFeed(bridge, generation)
        feed.classification = classifier(end + 8_500 - 300_000)
        attempts = []
        def fail_then_refresh():
            attempts.append(now)
            if len(attempts) == 1:
                raise OSError("synthetic classifier refresh failure")
            return classifier(now)
        feed._reviewed_metaclass_fetch = fail_then_refresh
        (short,) = feed.read()
        assert short.expires_ns == (end + 8_500)*1_000_000
        now = end + 8_100
        feed.read()
        assert attempts == [end + 7_797]
        now = end + 8_501
        (restored,) = feed.read()
        assert restored.expires_ns == provider_expires*1_000_000
        assert attempts == [end + 7_797,end + 8_501]
        assert proof.current_point(restored.symbol, now) == (restored.generation,restored.rank,restored.ratio)
    finally:
        observer.close()

# The reviewed-partial replay and legacy read-only adapters have only the
# monotonic clock when no metered classifier is installed.
passive = entry_feed.EntryFeed(None, generation)
passive.classification = classifier(now)
with patch.object(entry_feed,"time",SimpleNamespace(monotonic_ns=mono)):
    assert passive.refresh_classification() is passive.classification

print("C11_FRESH_CLASSIFIER_BEFORE_SHORT_SCOPE_PASS")
