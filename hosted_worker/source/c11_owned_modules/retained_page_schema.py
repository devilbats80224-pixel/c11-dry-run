"""Closed allowlist for market-history diagnostics at the retained pipe."""
from diagnostic_schema import PATTERN, Q, TTL
from exact_symbol import quote_contract_shape

PAGE_SCHEMA = 'c11-shared-history-page-v1'
SCHEDULE_SCHEMA = 'c11-primary-schedule-v1'

SCHEDULE_REASONS = frozenset((
    'NO_CURRENT_FACTS', 'OWNER_CYCLE_UNAVAILABLE', 'PROTECTION_PRIORITY',
    'WRONG_BOUNDARY', 'NW_LOOKBACK_UNAVAILABLE', 'OTHER_HISTORY_PENDING',
    'NO_PROVISIONAL_SCOPE', 'SCOPE_NOT_CURRENT', 'NO_PID_HEADROOM',
    'PREPARATION_REFUSED', 'SQLITE_BUSY',
    'AUTO_ARCHIVE_QUEUED', 'AUTO_ARCHIVE_FETCHING', 'AUTO_ARCHIVE_SEGMENT_VERIFIED',
    'AUTO_ARCHIVE_VERIFIED_WAITING_OWNER', 'AUTO_RETAINED_MINUTES_QUEUED',
    'AUTO_ARCHIVE_NO_VERIFIED_ORIGIN', 'AUTO_ARCHIVE_QUEUE_BUDGET', 'AUTO_ARCHIVE_NO_CLOSED_DAY',
    'AUTO_ARCHIVE_FILE_MISSING', 'AUTO_ARCHIVE_BUDGET', 'AUTO_ARCHIVE_TRANSIENT',
    'AUTO_ARCHIVE_NOT_AVAILABLE', 'AUTO_ARCHIVE_UNVERIFIED', 'AUTO_ARCHIVE_CANCELLED',
    'AUTO_ARCHIVE_ORIGIN_GAP', 'AUTO_ARCHIVE_CONTINUITY_GAP',
    'COLD_PREFIX_NO_REVIEWED_SOURCE', 'COLD_PREFIX_RANGE_HELD', 'COLD_PREFIX_QUEUED', 'COLD_PREFIX_REFUSED', 'COLD_PREFIX_REVIEW_EXPIRED',
))
STAGES = {
    'PAGE_PLANNED': (),
    'SCHEDULE_REJECTED': ('PENDING_BOOK', 'PENDING_AUTHORITY'),
    'RESERVE_REJECTED': ('HISTORY_MARKER_OR_CURRENT_REFUSED', 'HISTORY_SHARED_METER_STALE',
                         'HISTORY_SHARED_METER_UNKNOWN', 'HISTORY_SHARED_LIMIT', 'HISTORY_PAGE_HELD'),
    'RESERVED': (),
    'READY_READ': (),
    'GRANT_REJECTED': ('SCOPE_OR_SHARED_BUDGET',),
    'GRANT_QUEUED': (),
    'GRANT_WRITTEN': (),
    'REPLY_ACCEPTED': (),
    'REPLY_REJECTED': ('STALE_SCOPE_OR_DELIVERY', 'SCOPE_INVALID_AT_READ', 'PARENT_DELIVERY_EXPIRED',
                       'SCOPE_AND_PARENT_DELIVERY_EXPIRED', 'PROTOCOL', 'HTTP_STATUS', 'NO_HTTP_RECEIPT',
                       'NOT_DISPATCHED_BY_CHILD', 'WEIGHT_HEADER', 'RECEIPT_TIME', 'ROW_COUNT'),
    'EXPIRED': ('STALE_SCOPE', 'NO_REPLY', 'NO_READY'),
    'PEER_ENDED': ('WITHOUT_REPLY', 'BEFORE_GRANT'),
    'PERSISTED': (),
    'PERSIST_REJECTED': ('DELIVERY_SCOPE_LOST', 'INVALID_RECEIPT_CLOCK', 'NATIVE_BATCH_REFUSED', 'SQLITE_BUSY'),
}
PAGE_KEYS = frozenset(('schema', 'symbol', 'generation', 'end_ms', 'page_start_ms',
                       'page_end_ms', 'requested_rows', 'stage', 'at_ms'))
METADATA = {
    "parent_delivery_margin_ms": (-600000, 600000),
    'grant_ms': (0, 2**63-1), 'wire_ms': (0, 2**63-1), 'response_ms': (0, 2**63-1),
    'http_status': (0, 599), 'reply_rows': (0, 4096),
}


def is_int(value, lo, hi):
    return type(value) is int and lo <= value <= hi


def valid_identity(event):
    symbol, generation = event.get('symbol'), event.get('generation')
    return (type(symbol) is str and len(symbol) <= 54 and (PATTERN.fullmatch(symbol) is not None or quote_contract_shape(symbol))
            and is_int(generation, 1, 2**63-1))


def accepted_history_trace(event):
    end, now = event.get('end_ms'), event.get('at_ms')
    if not is_int(end, Q, 2**63-1) or end % Q or not is_int(now, end, end+Q):
        return False
    if event['schema'] == SCHEDULE_SCHEMA:
        keys = {'schema', 'end_ms', 'at_ms', 'reason'}
        reason = event.get('reason')
        return (now < end+TTL and type(reason) is str and reason in SCHEDULE_REASONS
                and (set(event) == keys or
                     (set(event) == keys | {'symbol', 'generation'} and valid_identity(event))))
    if event['schema'] != PAGE_SCHEMA or not PAGE_KEYS <= event.keys() or not valid_identity(event):
        return False
    if set(event) - PAGE_KEYS - METADATA.keys() - {'stage_reason'}:
        return False
    stage, reason = event['stage'], event.get('stage_reason')
    if type(stage) is not str or stage not in STAGES:
        return False
    reasons = STAGES[stage]
    if reasons:
        if type(reason) is not str or reason not in reasons:
            return False
    elif 'stage_reason' in event:
        return False
    a, b, n = (event[k] for k in ('page_start_ms', 'page_end_ms', 'requested_rows'))
    if not (is_int(a, Q, end) and is_int(b, a+Q, end) and a % Q == b % Q == 0
            and is_int(n, 15, 495) and b-a == n*60_000):
        return False
    return all(k not in event or event[k] is None or is_int(event[k], lo, hi)
               for k, (lo, hi) in METADATA.items())
