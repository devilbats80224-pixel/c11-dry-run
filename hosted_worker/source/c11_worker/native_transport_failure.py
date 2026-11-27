"""Narrow source-scoped transport classification; no IO, retries or authority.

Never serializes exception text, URLs, headers, arguments, locals or key values.
Only the fixed existing transport call sites may create the scope wrapper.
"""
import errno
from http.client import RemoteDisconnected
import socket
import ssl
from pathlib import Path
from urllib.error import HTTPError, URLError

SOURCES = frozenset({'BINANCE_PUBLIC_OI', 'OPENMARKET_MARKET_SNAPSHOT'})
PUBLIC_FILES = frozenset({'native_transport_failure.py', 'paper_oi_public_source.py',
    'production_producer_auto.py', 'producer_diagnostics.py', 'collector.py'})
MAX_CAUSES = 8
SAFE_CODES = frozenset({'SOURCE_ORIGIN_OR_STATUS_CHANGED', 'NATIVE_PUBLIC_TRANSPORT_FAILURE'})


class PublicTransportFailure(Exception):
    def __init__(self, original_error, source, *, response_observed=None):
        super().__init__('NATIVE_PUBLIC_TRANSPORT_FAILURE')
        self.original_error = original_error
        self.public_source = source
        self.response_observed = response_observed


def _walk(error):
    queue = [(error, None, 'error', frozenset())]
    nodes = []
    seen = set()
    complete = True
    while queue:
        current, parent, relation, ancestors = queue.pop(0)
        if not isinstance(current, BaseException):
            complete = False
            continue
        identity = id(current)
        if identity in ancestors:
            complete = False
            continue
        if identity in seen:
            continue
        if len(nodes) >= MAX_CAUSES:
            complete = False
            break
        seen.add(identity)
        index = len(nodes)
        nodes.append((current, parent, relation))
        links = []
        if type(current) is PublicTransportFailure:
            links.append((current.original_error, 'original'))
        if type(current) is URLError:
            if isinstance(current.reason, BaseException):
                links.append((current.reason, 'reason'))
            else:
                complete = False  # A string like "timed out" is not typed proof.
        if current.__cause__ is not None:
            links.append((current.__cause__, 'cause'))
        if current.__context__ is not None:
            links.append((current.__context__, 'context'))
        queue.extend((child, index, link, ancestors | {identity}) for child, link in links)
    return nodes, complete


def _transient(error):
    number = getattr(error, 'errno', None)
    # Exact class only; neither subclasses nor generic HTTPException inherit
    # permission. The caller below also requires explicit no-response evidence.
    if type(error) is RemoteDisconnected:
        return 'REMOTE_DISCONNECTED' if number is None else None
    if type(error) is socket.gaierror:
        return 'TEMPORARY_DNS' if number == socket.EAI_AGAIN else None
    typed = {TimeoutError: (errno.ETIMEDOUT, 'TIMEOUT'),
        ConnectionResetError: (errno.ECONNRESET, 'CONNECTION_RESET'),
        ConnectionAbortedError: (errno.ECONNABORTED, 'CONNECTION_ABORTED'),
        BrokenPipeError: (errno.EPIPE, 'BROKEN_PIPE')}
    if type(error) in typed:
        expected, code = typed[type(error)]
        return code if number is None or type(number) is int and number == expected else None
    if type(error) is OSError and type(number) is int:
        return {errno.ETIMEDOUT: 'TIMEOUT', errno.ECONNRESET: 'CONNECTION_RESET',
            errno.ECONNABORTED: 'CONNECTION_ABORTED', errno.EPIPE: 'BROKEN_PIPE'}.get(number)
    return None


def _safe_node(error, parent, relation):
    safe_types = {PublicTransportFailure, URLError, HTTPError, TimeoutError, RemoteDisconnected,
        OSError, ConnectionResetError, ConnectionAbortedError, BrokenPipeError,
        PermissionError, ConnectionRefusedError, FileNotFoundError, socket.gaierror,
        ssl.SSLError, ssl.SSLCertVerificationError, ssl.SSLEOFError,
        ssl.SSLZeroReturnError, ssl.SSLWantReadError, ssl.SSLWantWriteError,
        ValueError, RuntimeError, TypeError}
    result = {'parent': parent, 'relation': relation,
        'exception_type': type(error).__name__ if type(error) in safe_types else 'OtherException'}
    for attr, name in [('errno', 'errno'), ('code', 'http_status'), ('verify_code', 'tls_verify_code')]:
        value = getattr(error, attr, None)
        # Only standardized numeric status fields; no arbitrary strings/str().
        if type(value) is int and -1000000 <= value <= 1000000:
            if name != 'http_status' or isinstance(error, HTTPError) and 100 <= value <= 599:
                result[name] = value
    args = error.args
    code = args[0] if len(args) == 1 and type(args[0]) is str else None
    result['code'] = code if code in SAFE_CODES else 'UNLISTED_MESSAGE_WITHHELD'
    frames = []
    tb = error.__traceback__
    for _ in range(64):
        if tb is None:
            break
        filename = Path(tb.tb_frame.f_code.co_filename).name
        frames.append({'file': filename if filename in PUBLIC_FILES else '<external>', 'line': tb.tb_lineno})
        tb = tb.tb_next
    result['origin_frames'] = frames[-8:]
    return result


def describe_transport_failure(error):
    nodes, complete = _walk(error)
    scoped = (type(error) is PublicTransportFailure and type(error.public_source) is str
              and error.public_source in SOURCES)
    source = error.public_source if scoped else 'UNPROVEN'
    response_observed = error.response_observed if type(error) is PublicTransportFailure else None
    if type(response_observed) is not bool:
        response_observed = None
    reason = 'NATIVE_TRANSPORT_UNKNOWN_STOP'
    retryable = False
    if any(isinstance(item, (ssl.SSLError, ssl.CertificateError)) for item, _, _ in nodes):
        reason = 'NATIVE_TRANSPORT_TLS_STOP'
    elif any(isinstance(item, HTTPError) for item, _, _ in nodes):
        reason = 'NATIVE_TRANSPORT_HTTP_STOP'
    elif not scoped:
        reason = 'NATIVE_TRANSPORT_CONTEXT_UNPROVEN_STOP'
    elif source == 'OPENMARKET_MARKET_SNAPSHOT' and (response_observed is not False or
            any(getattr(item, field, None) is not None
                for item, _, _ in nodes for field in ('status', 'code', 'http_status'))):
        reason = 'NATIVE_TRANSPORT_NO_RESPONSE_UNPROVEN_STOP'
    elif not complete:
        reason = 'NATIVE_TRANSPORT_CAUSAL_CHAIN_UNPROVEN_STOP'
    elif any(type(item) is RemoteDisconnected for item, _, _ in nodes) and (
            response_observed is not False or
            any(getattr(item, field, None) is not None
                for item, _, _ in nodes for field in ('status', 'code', 'http_status'))):
        reason = 'NATIVE_TRANSPORT_NO_RESPONSE_UNPROVEN_STOP'
    else:
        typed = []
        known = True
        for item, _, _ in nodes:
            if item is error or type(item) is URLError:
                continue
            code = _transient(item)
            if (source == 'OPENMARKET_MARKET_SNAPSHOT' and response_observed is False
                    and type(item) is socket.gaierror and item.errno == socket.EAI_NONAME):
                code = 'OFFICIAL_OPENMARKET_DNS_NAME_PENDING'
            if code is None:
                known = False
            else:
                typed.append(code)
        if known and typed:
            retryable = True
            reason = 'NATIVE_TRANSPORT_TRANSIENT_' + (typed[0] if len(set(typed)) == 1 else 'MULTIPLE')
    return {'schema': 'public_transport_failure_v1', 'source': source,
        'retryable': retryable, 'reason_code': reason, 'causal_chain_complete': complete,
        'response_observed': response_observed,
        'cause_chain': [_safe_node(item, parent, relation) for item, parent, relation in nodes]}
