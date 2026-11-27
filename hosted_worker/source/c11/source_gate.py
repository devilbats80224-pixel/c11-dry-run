"""C11 first-layer read gate over the original OpenMarket collector IPC."""
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from math import isfinite
from pathlib import Path
import re
import time
from uuid import UUID

from c11_original_kernel import KernelTransport
from strict_transport import load_json

IPC_PATH = '/ipc/instance/snapshot.sock'
BINDING_PATH = '/run/c11-managed/collector-binding.json'
SOURCE = 'https://api.openmarket.xyz/v1/markets'
ORDER = 'MARKET_SYMBOL_OI_CHANGE_24H'
METADATA_SOURCE = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
COLLECTOR_IMAGE = 'sha256:ef9dd5eca115bde2303e1f80b85dcccb424d4887434d4a9b4f57435293319cf1'
HEX = re.compile('[0-9a-f]{64}')
SYMBOL = re.compile('[A-Z0-9]{2,40}')


class StartupBlocked(RuntimeError):
    pass


def require(condition, reason):
    if not condition:
        raise StartupBlocked(reason)


def digest(value):
    return isinstance(value, str) and HEX.fullmatch(value) is not None and value != '0' * 64


@dataclass(frozen=True)
class CollectorIdentity:
    """Only construct from independently verified, scoped managed startup approval."""
    cid: str
    image: str
    binding_sha256: str
    boot_id: str
    instance_uuid: str
    peer_pid: int


@dataclass(frozen=True)
class Candidate:
    symbol: str
    pair: str
    admitted_percent: str


@dataclass(frozen=True)
class Observation:
    collector_round: int
    received_at: str
    received_epoch: float
    expires_monotonic: float
    candidates: tuple[Candidate, ...]


def value_time(value):
    require(type(value) is str and len(value) <= 40, 'C11_SOURCE_TIME_INVALID')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(parsed.utcoffset() == timezone.utc.utcoffset(parsed), 'C11_SOURCE_TIME_INVALID')
    return parsed.timestamp()


def validate_one_page(envelope, *, identity, wall_now, monotonic_now, deadline):
    """A data check. The caller must authenticate the kernel peer before calling."""
    try:
        require(envelope['schema'] == 'openmarket_guarded_envelope_v1'
                and envelope['health'] == 'OBSERVATION_READY_PROVISIONAL'
                and envelope['receipt_fresh'] is True
                and envelope['observation_candidates_authorized'] is True
                and envelope['entry_authorized'] is False
                and envelope['dryrun_consumption_authorized'] is False
                and envelope['failure_latched'] is False and envelope.get('failure_detail') is None
                and envelope['source_as_of'] is None and envelope['formal_candidates'] == [],
                'C11_COLLECTOR_NOT_READY')
        require(envelope['collector_identity'] == dict(pid=identity.peer_pid, instance_uuid=identity.instance_uuid),
                'C11_COLLECTOR_INSTANCE_MISMATCH')
        snapshot = envelope['snapshot']
        coverage = snapshot['coverage']
        age = envelope['receive_age_seconds']
        ttl = envelope['stale_after_seconds']
        require(all(type(x) in (float, int) and isfinite(x) for x in (age, ttl, wall_now, monotonic_now, deadline))
                and 0 <= age < ttl <= 90 and monotonic_now < deadline, 'C11_SOURCE_STALE')
        require(snapshot['schema'] == 'openmarket_native_page60_guarded_snapshot_v1'
                and snapshot['phase'] == 'DIRECT_OPENMARKET_OI20'
                and snapshot['exchange'] == 'BINANCE_FUTURES' and snapshot['category'] == 'PERPETUAL'
                and snapshot['sort_field'] == ORDER and snapshot['direction'] == 'SORT_DIRECTION_DESC'
                and snapshot['usd_base_status'] == 'USER_CONFIRMED_USD_VALUE'
                and snapshot['scale_evidence'] == 'USER_CONFIRMED_OPENMARKET_USD_OI24H_RATIO_20261108'
                and snapshot['scale'] == 'ratio'
                and snapshot['direct_candidates_authorized'] is True and snapshot['entry_authorized'] is False
                and snapshot['observed_order_nonincreasing'] is True
                and snapshot['adjacent_raw_order_violations'] == 0 and snapshot['duplicate_observations'] == 0,
                'C11_SOURCE_CONTRACT')
        require(snapshot['status'] == 'SINGLE_PAGE_OBSERVED'
                and snapshot['coverage_scope'] == 'OFFSET_ZERO_PAGE_60_ONLY'
                and snapshot['request_attempts'] == snapshot['page_budget'] == 1
                and snapshot['requested_page_size'] == 60 and len(snapshot['pages']) == 1
                and coverage['initial_offset'] == coverage['next_offset'] == 0
                and coverage['continuation_pending'] is False,
                'C11_SOURCE_SINGLE_PAGE_REQUIRED')
        page = snapshot['pages'][0]
        require(page['offset'] == 0 and page['row_count'] == 60 and page['http_status'] == 200,
                'C11_SOURCE_PAGE_RECEIPT')
        received = value_time(page['received_at'])
        started = value_time(page['request_started_at'])
        generated = value_time(envelope['generated_at'])
        require(started <= received <= generated <= wall_now and 0 <= wall_now - received < ttl,
                'C11_SOURCE_STALE_OR_FUTURE')
        round_ = snapshot['round']
        require(type(round_) is int and round_ > 0 and snapshot['pid'] == identity.peer_pid,
                'C11_COLLECTOR_INSTANCE_MISMATCH')
        observations = snapshot['observations']
        require(type(observations) is list and len(observations) <= 60, 'C11_SOURCE_ROW_COUNT')
        rows, seen, previous = [], set(), None
        for row in observations:
            market = row['market']
            symbol = market['rawSymbol']
            require(isinstance(symbol, str) and SYMBOL.fullmatch(symbol) and symbol not in seen,
                    'C11_SOURCE_SYMBOL')
            seen.add(symbol)
            require(row['received_at'] == page['received_at']
                    and row['observation_generation'] == {'pid': identity.peer_pid, 'round': round_},
                    'C11_SOURCE_OLD_OR_MIXED_ROUND')
            if row['native_observation_candidate'] is not True:
                continue
            field, = market['oi_24h_change_fields']
            evidence = row['classification_evidence']
            require(row['classification'] == 'CRYPTO' and row['negative_markers'] == []
                    and row['entry_authorized'] is False and row['source_time_verified'] is False
                    and market['exchange'] == 'BINANCE_FUTURES' and market['category'] == 'PERPETUAL'
                    and field['field'] == 'oiChange24h' and field['presence'] == 'present'
                    and type(field['raw']) is str and 0 < len(field['raw']) <= 128
                    and evidence['source_url'] == METADATA_SOURCE and digest(evidence['sha256'])
                    and value_time(evidence['received_at']) <= wall_now,
                    'C11_SOURCE_CANDIDATE_UNVERIFIED')
            admitted = row['native_oi24h_percent']
            require(type(admitted) is str and 0 < len(admitted) <= 128, 'C11_SOURCE_PERCENT')
            percent = Decimal(admitted)
            require(percent.is_finite() and percent >= 20 and (previous is None or percent <= previous),
                    'C11_SOURCE_PERCENT_OR_ORDER')
            previous = percent
            quote = next((q for q in ('USDT', 'USDC') if symbol.endswith(q) and len(symbol) > len(q)), '')
            require(bool(quote), 'C11_SOURCE_PAIR')
            rows.append(Candidate(symbol, symbol[:-len(quote)] + '/' + quote + ':' + quote, admitted))
        require(bool(rows), 'C11_SOURCE_NO_FRESH_CANDIDATES')
        return Observation(round_, page['received_at'], received, deadline, tuple(rows))
    except (KeyError, TypeError, ValueError, InvalidOperation, OverflowError, RecursionError) as error:
        raise StartupBlocked('C11_SOURCE_DOCUMENT_INVALID') from error


class OriginalCollector:
    """No file, HTTP, synthetic or historical candidate fallback."""
    def __init__(self, identity):
        require(type(identity) is CollectorIdentity and digest(identity.cid)
                and identity.image == COLLECTOR_IMAGE and digest(identity.binding_sha256)
                and type(identity.peer_pid) is int and identity.peer_pid > 0, 'C11_MANAGED_COLLECTOR_IDENTITY')
        try:
            require(str(UUID(identity.instance_uuid)) == identity.instance_uuid
                    and str(UUID(identity.boot_id)) == identity.boot_id, 'C11_MANAGED_COLLECTOR_IDENTITY')
        except (ValueError, TypeError, AttributeError) as error:
            raise StartupBlocked('C11_MANAGED_COLLECTOR_IDENTITY') from error
        self.identity = identity
        self.transport = KernelTransport(IPC_PATH, BINDING_PATH, identity.binding_sha256)

    def read(self):
        transport = self.transport
        identity = self.identity
        try:
            current_boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
            require(current_boot == identity.boot_id, 'C11_COLLECTOR_BOOT_MISMATCH')
            require(transport.refresh() is True and transport.status == 'ACCEPTED_KERNEL_BOUND',
                    'C11_COLLECTOR_KERNEL_BINDING_REFUSED')
            binding = transport.approved_binding
            token = binding['kernel_token']
            require(binding['instance_uuid'] == identity.instance_uuid
                    and binding['peer_pid'] == identity.peer_pid
                    and token['source_container'] == identity.cid and token['kernel_boot'] == identity.boot_id
                    and transport.peer == {'uid': 1001, 'gid': 2000, 'pid': identity.peer_pid},
                    'C11_COLLECTOR_KERNEL_IDENTITY')
            body = load_json(transport.last_body)
            return validate_one_page(body, identity=identity, wall_now=time.time(),
                                     monotonic_now=time.monotonic(), deadline=transport.lease._expiry_tick)
        except (OSError, ValueError, TypeError, KeyError, AttributeError) as error:
            raise StartupBlocked('C11_COLLECTOR_EVIDENCE_UNAVAILABLE') from error
