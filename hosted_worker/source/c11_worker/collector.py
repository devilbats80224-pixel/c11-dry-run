"""Linux collector port. Public snapshot/receipt protocol; no trading operations."""
import argparse
import copy
import ctypes
import datetime as dt
import decimal
import email.utils
import errno
import hashlib
import http.client
import json
import os
import re
import signal
import socket
import stat
import struct
import sys
import threading
import time
from native_transport_failure import PublicTransportFailure, describe_transport_failure

BODY_LIMIT = 2 * 1024 * 1024
FRAME_LIMIT = (8 * 1024 * 1024 - 16384) // 3
SCHEMA = 'openmarket_native_page60_guarded_snapshot_v1'

def utc(value=None):
    return dt.datetime.fromtimestamp(time.time() if value is None else value, dt.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

class Failure(Exception):
    def __init__(self, state, stop=True, retry=None, detail=None):
        self.state, self.stop, self.retry = state, stop, retry
        self.detail = detail

class Number(str):
    pass

def pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError('duplicate')
        result[key] = value
    return result

def decode(body):
    return json.loads(body, parse_int=Number, parse_float=Number, object_pairs_hook=pairs,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError('constant')))

def parse(body):
    try:
        root = decode(body)
        assert isinstance(root, dict)
        assert not any(k in root for k in ('error', 'errors', 'code', 'message', 'msg'))
        assert all(root.get(k) is not False for k in ('success', 'ok'))
        for k in ('status', 'statusCode'):
            if k in root:
                assert isinstance(root[k], str) and root[k].lower() in ('ok', 'success', '200')
        rows = root['symbols']
        assert isinstance(rows, list) and len(rows) <= 60
        nxt = root.get('nextOffset')
        if nxt is not None:
            assert isinstance(nxt, Number) and re.fullmatch(r'[0-9]{1,7}', nxt) and int(nxt) <= 1000000
            nxt = int(nxt)
        result = []
        for row in rows:
            try:
                assert isinstance(row, dict) and row.get('exchange') == 'BINANCE_FUTURES' and row.get('category') == 'PERPETUAL'
                raw = row['rawSymbol']
                assert type(raw) is str and 1 <= len(raw) <= 100 and raw.isalnum()
                normalized = row.get('normalizedSymbol')
                assert normalized is None or (type(normalized) is str and 1 <= len(normalized) <= 120 and all(c.isalnum() or c in '._:/-' for c in normalized))
                value = row.get('oiChange24h')
                presence = 'missing' if 'oiChange24h' not in row else 'null' if value is None else 'present' if isinstance(value, Number) else 'invalid_suppressed'
                result.append(dict(exchange='BINANCE_FUTURES', category='PERPETUAL', rawSymbol=raw, normalizedSymbol=normalized,
                    oi_24h_change_fields=[dict(field='oiChange24h', presence=presence, json_type='number' if presence == 'present' else presence, raw=str(value) if presence == 'present' else None)],
                    source_as_of=None, source_as_of_status='unverified_candidates_or_missing', oi_amount_semantics='unverified', oi_change_semantics='unverified', classification_status='NOT_SUPPLIED_NOT_FILTERED'))
            except (ValueError, AssertionError, KeyError, TypeError):
                result.append({'row_invalid': True, 'reason': 'INVALID_MARKET_ROW', 'rawSymbol':row.get('rawSymbol') if isinstance(row,dict)and isinstance(row.get('rawSymbol'),str)and re.fullmatch('[A-Z0-9]{2,40}',row['rawSymbol'])else None})
        return result, nxt, 'PRESENT_INTEGER' if nxt is not None else 'NULL' if 'nextOffset' in root else 'MISSING'
    except (ValueError, AssertionError, KeyError, TypeError, RecursionError):
        raise Failure('SCHEMA_OR_SERVICE_ENVELOPE_STOP') from None

def number(token):
    if token is None or len(token) > 128 or len(re.sub(r'\D', '', re.split('[eE]', token)[0])) > 32:
        return None
    try:
        value = decimal.Decimal(token)
        # Match Foundation Decimal's bounded representable range conservatively.
        return value if value.is_finite() and (value == 0 or -128 <= value.adjusted() <= 127) else None
    except decimal.InvalidOperation:
        return None

NEGATIVE = {'STOCK','STOCK_TOKEN','EQUITY','CN_EQUITY','HK_EQUITY','KR_EQUITY','COMMODITY','FX','INDEX','TRADFI','TRADIFI_PERPETUAL'}
ALIASES = dict(STOCKS='STOCK',STOCK_TOKENS='STOCK_TOKEN',TOKENIZED_STOCK='STOCK_TOKEN',TOKENIZED_STOCKS='STOCK_TOKEN',EQUITIES='EQUITY',EQUITY_TOKEN='EQUITY',EQUITY_TOKENS='EQUITY',COMMODITIES='COMMODITY',FOREX='FX',FOREIGN_EXCHANGE='FX',INDICES='INDEX',INDEXES='INDEX',STOCK_INDEX='INDEX',TRAD_FI='TRADFI',TRADITIONAL_FINANCE='TRADFI')
def marker(v):
    text = re.sub('[ _-]+', '_', str(v).strip().upper())
    return ALIASES.get(text, text)

class Metadata:
    def __init__(self, path):
        with open(path, 'rb') as f:
            body = f.read(16 * 1024 * 1024 + 1)
        if len(body) > 16 * 1024 * 1024:
            raise Failure('METADATA_LIMIT')
        root = json.loads(body)['universe']
        assert root['source_url'] == 'https://fapi.binance.com/fapi/v1/exchangeInfo'
        self.path, self.sha, self.asof = path, hashlib.sha256(body).hexdigest(), root['received_ms'] / 1000
        self.products = {}; self.classification_evidence = {}
        for bucket in ('verified','excluded','unknown','filtered_out'):
            assert isinstance(root[bucket], list)
            for entry in root[bucket]:
                c = entry.get('source_contract_metadata', entry)
                labels = [marker(c.get('underlyingType','UNKNOWN')), marker(c.get('contractType','UNKNOWN')), marker(entry.get('asset_class','UNKNOWN'))] + list(map(marker, c.get('underlyingSubType',[]))) + list(map(marker, entry.get('tags',[])))
                negative = sorted(set(labels) & NEGATIVE)
                valid = c.get('underlyingType') == 'COIN' and 'CRYPTO' in list(map(marker,c.get('underlyingSubType',[]))) and c.get('contractType') == 'PERPETUAL' and ('asset_class' not in entry or marker(entry['asset_class']) == 'CRYPTO') and all(marker(t) in ('CRYPTO','RWA') for t in entry.get('tags',[]))
                old = self.products.get(entry['symbol'])
                if old:
                    negative = sorted(set(negative + old[1])); valid = False
                self.products[entry['symbol']] = ('EXCLUDED_TRADFI_OR_STOCK' if negative else 'CRYPTO' if valid else 'UNKNOWN', negative)
                self.classification_evidence[entry['symbol']] = dict(source_url=root['source_url'],received_at=utc(self.asof),sha256=self.sha)

    def classify(self, symbol):
        return self.products.get(symbol, ('UNKNOWN', []))

def refresh_classification(metadata):
    """Preserve per-product classification facts, never restamp them on reload failure.
    Execution availability and precision retain separate original freshness gates.
    """
    prior=copy.deepcopy(metadata.products)
    evidence=copy.deepcopy(getattr(metadata,'classification_evidence',{}))
    metadata.reload()
    current=metadata.products;new_evidence=getattr(metadata,'classification_evidence',{})
    for symbol,classification in current.items():
        if classification[0]!='UNKNOWN' or symbol not in prior:
            prior[symbol]=classification
            if symbol in new_evidence:evidence[symbol]=copy.deepcopy(new_evidence[symbol])
    metadata.products=prior;metadata.classification_evidence=evidence

class Store:
    def __init__(self, interval=60):
        self.lock = threading.RLock(); self.ttl = min(90,max(30,interval*2))
        self.payload = {}; self.receipt = None; self.tick = time.monotonic()
        self.state = 'STARTING'; self.failure = None; self.refreshing = False; self.round = 0
        self.terminal = False; self.failure_detail = None

    def begin(self, round_):
        with self.lock:
            if self.terminal: return
            self.round = round_; self.refreshing = True

    def fail(self, state, detail=None):
        with self.lock:
            if self.terminal: return
            self.payload = {}; self.receipt = None; self.state = state
            self.failure = state; self.failure_detail = copy.deepcopy(detail); self.refreshing = False

    def terminate(self, state):
        with self.lock:
            if self.terminal: return
            self.fail(state)
            self.terminal = True

    def publish(self, snapshot):
        encoded = json.dumps(snapshot).encode()
        if len(encoded) > FRAME_LIMIT:
            self.fail('RESOURCE_LIMIT'); raise Failure('RESOURCE_LIMIT')
        with self.lock:
            if self.terminal: return
            if snapshot.get('direct_candidates_authorized') is not True:
                self.fail(snapshot['status']); return
            self.payload = snapshot
            self.receipt = min(dt.datetime.fromisoformat(p['received_at'].replace('Z','+00:00')).timestamp() for p in snapshot['pages'])
            self.age = max(0,time.time()-self.receipt); self.tick = time.monotonic()
            self.state = 'OBSERVATION_READY_PROVISIONAL'; self.failure = None; self.failure_detail = None; self.refreshing = False

    def render(self):
        with self.lock:
            age = self.age + time.monotonic()-self.tick if self.receipt is not None else None
            fresh = age is not None and 0 <= age <= self.ttl
            if age is not None and not fresh:
                self.fail('EXPIRED'); age = None
            allowed = fresh and self.failure is None and self.state == 'OBSERVATION_READY_PROVISIONAL'
            return dict(schema='openmarket_guarded_envelope_v1',health=self.state,health_before_expiry=self.state,generated_at=utc(),receive_age_seconds=age,
                receive_age_clock='MONOTONIC_ELAPSED_PLUS_LOCAL_AGE_AT_PUBLISH',stale_after_seconds=self.ttl,receipt_fresh=fresh,
                refresh_in_progress=self.refreshing,refresh_attempt_round=self.round,failure_latched=self.failure is not None,last_failure=self.failure,failure_detail=copy.deepcopy(self.failure_detail),
                observation_candidates_authorized=allowed,entry_authorized=False,dryrun_consumption_authorized=False,formal_candidates=[],source_as_of=None,
                source_time_status='UNKNOWN_NOT_HTTP_DATE',snapshot=copy.deepcopy(self.payload))

def record_transport_failure(store, failure, detail=None):
    """Retain only an already valid original snapshot across a typed network fault.
    No original page, receipt, snapshot round, publish tick or TTL is modified.
    Semantic/provider refusal, expiry, terminal state and unknown failures clear.
    """
    detail = failure.detail if detail is None else detail
    transport = detail.get('transport_failure', {}) if isinstance(detail, dict) else {}
    transient = (type(failure) is Failure and failure.stop is False
        and (failure.state in ('HTTP_429_RETRY_WAIT', 'HTTP_5XX_RETRY_WAIT', 'HTTP_408_RETRY_WAIT') or
             failure.state == 'NETWORK_FAILED_CURRENT_ROUND_NOT_FRESH'
             and transport.get('source') == 'OPENMARKET_MARKET_SNAPSHOT'
             and transport.get('retryable') is True))
    with store.lock:
        current = store.render()  # Enforces original monotonic expiry, never restamps.
        if transient and not store.terminal and current['observation_candidates_authorized'] is True:
            store.refreshing = False
            store.failure_detail = copy.deepcopy(detail)
            return True
        store.fail(failure.state, detail)
        return False

class Collector:
    def __init__(self, metadata, store, stop, scale='UNKNOWN', evidence=None, interval=60, page_interval=3, max_pages=5, timeout=15, synthetic_port=None, *, single_page=False):
        self.metadata, self.store, self.stop = metadata, store, stop
        self.scale,self.evidence,self.interval,self.page_interval,self.max_pages,self.timeout = scale,evidence,interval,page_interval,max_pages,timeout
        self.single_page = single_page
        self.synthetic_port = synthetic_port; self.next_request = 0
        self.rate_headers = {}; self.rate_received = None
        self.resume_offset = 0; self.row_cache = {}
        assert scale == 'UNKNOWN' or scale in ('ratio','percent') and evidence
        assert 1 <= max_pages <= 5

    def fetch(self, offset, key):
        if self.stop.wait(max(0,self.next_request-time.monotonic())):
            raise Failure('CANCELLED')
        if self.stop.is_set(): raise Failure('CANCELLED')
        connection = http.client.HTTPConnection('127.0.0.1',self.synthetic_port,timeout=self.timeout) if self.synthetic_port else http.client.HTTPSConnection('api.openmarket.xyz', timeout=self.timeout)
        started = utc()
        r = None
        finished = threading.Event(); watcher = None
        deadline = time.monotonic() + self.timeout
        try:
            connection.connect()
            active_socket = connection.sock
            def interrupt_io():
                while not finished.wait(.02):
                    if self.stop.is_set() or time.monotonic() >= deadline:
                        try: active_socket.shutdown(socket.SHUT_RDWR)
                        except OSError: pass
                        return
            watcher = threading.Thread(target=interrupt_io,daemon=True); watcher.start()
            path = '/v1/markets?exchange=BINANCE_FUTURES&category=PERPETUAL&pageSize=60&pageOffset='+str(offset)+'&sortCriteria.field=MARKET_SYMBOL_OI_CHANGE_24H&sortCriteria.direction=SORT_DIRECTION_DESC'
            connection.request('GET',path,headers={'Accept':'application/json','X-OpenMarket-Key':key})
            r = connection.getresponse()
            self.rate_headers = {}
            for name in ('X-RateLimit-Limit','X-RateLimit-Remaining','X-RateLimit-Used','X-RateLimit-Reset'):
                value = r.getheader(name)
                if value and re.fullmatch('[0-9]{1,20}',value) and int(value)<=18446744073709551615:
                    self.rate_headers[name] = value
            self.rate_received = utc()
            if r.status == 429 or r.status == 408 or 500 <= r.status <= 599:
                retry = r.getheader('Retry-After'); delay = self.interval
                if retry and re.fullmatch('[0-9]{1,10}', retry):
                    delay = max(self.interval, int(retry))
                elif retry and len(retry) <= 100:
                    try: delay = max(self.interval, email.utils.parsedate_to_datetime(retry).timestamp()-time.time())
                    except (ValueError, TypeError, OverflowError): pass
                # Long provider waits are honored; the wait is cancellable. No inline burst retries.
                state = 'HTTP_429_RETRY_WAIT' if r.status == 429 else 'HTTP_408_RETRY_WAIT' if r.status == 408 else 'HTTP_5XX_RETRY_WAIT'
                raise Failure(state, stop=False, retry=delay,
                              detail={'kind':'PUBLIC_HTTP_TRANSIENT','status':r.status,'retry_delay_seconds':delay})
            if r.status != 200: raise Failure('HTTP_'+str(r.status)+'_STOP')
            body = r.read(BODY_LIMIT+1)
            if self.stop.is_set(): raise Failure('CANCELLED')
            if len(body)>BODY_LIMIT: raise Failure('BODY_LIMIT_STOP')
            rows,nxt,nstate = parse(body)
            return rows,nxt,dict(offset=offset,next_offset=nxt,next_offset_state=nstate,row_count=len(rows),request_started_at=started,received_at=utc(),http_status=200,http_date=r.getheader('Date'),http_date_role='TRANSPORT_DATE_NOT_SOURCE_TIMESTAMP'),len(body)
        except Failure: raise
        except (OSError,http.client.HTTPException) as error:
            if self.stop.is_set(): raise Failure('CANCELLED') from None
            # Exact existing official HTTPS transport only. Synthetic endpoints
            # and unknown/TLS failures never gain retry permission from errno.
            scoped = PublicTransportFailure(error, 'OPENMARKET_MARKET_SNAPSHOT', response_observed=r is not None) if not self.synthetic_port else error
            detail = {'kind':'PUBLIC_TRANSPORT_FAILURE', 'transport_failure':describe_transport_failure(scoped)}
            transient = detail['transport_failure']['retryable']
            raise Failure('NETWORK_FAILED_CURRENT_ROUND_NOT_FRESH' if transient else 'PUBLIC_TRANSPORT_REFUSED_STOP',stop=not transient,detail=detail) from None
        finally:
            finished.set()
            if watcher: watcher.join(1)
            if r is not None: r.close()
            connection.close(); self.next_request=time.monotonic()+self.page_interval

    def cycle(self, round_, key):
        self.rate_headers = {}; self.rate_received = None
        self.store.begin(round_); started=utc(); tick=time.monotonic()
        pages=[]; seen=set(); offset=0 if self.single_page else self.resume_offset; initial_offset=offset
        page_budget = 1 if self.single_page else self.max_pages
        previous=None; attempts=0; total=0; invalid=duplicates=inversions=0; observed={}
        status='FAST_PAGE_BUDGET_PARTIAL'; failure=None; boundary=False
        for _ in range(page_budget):
            if self.stop.is_set(): failure=Failure('CANCELLED'); break
            if time.monotonic()-tick>=30: status='SOFT_TIME_BUDGET_PARTIAL'; break
            attempts+=1
            try:
                batch,nxt,page,size=self.fetch(offset,key); total+=size
                if total>8*1024*1024: raise Failure('RESOURCE_LIMIT')
                pages.append(page)
                for market in batch:
                    if market.get('row_invalid'):
                        invalid+=1
                        if market.get('rawSymbol'):self.row_cache.pop(market['rawSymbol'],None)
                        continue
                    symbol=market['rawSymbol'];token=market['oi_24h_change_fields'][0]['raw'];value=number(token)
                    if symbol in seen:
                        duplicates+=1;observed.pop(symbol,None);self.row_cache.pop(symbol,None);continue
                    seen.add(symbol)
                    if value is None: invalid+=1;self.row_cache.pop(symbol,None);continue
                    if previous is not None and value>previous: inversions+=1
                    previous=value
                    with decimal.localcontext() as ctx:
                        ctx.prec=80;pct=value*100 if self.scale=='ratio' else value if self.scale=='percent' else None
                    classification,negative=self.metadata.classify(symbol)
                    above=pct>=20 if pct is not None else None
                    boundary=boundary or above is False
                    observed[symbol]=dict(market=market,received_at=page['received_at'],observation_generation=dict(pid=os.getpid(),round=round_),classification=classification,
                        classification_reason='RETAINED_CLASSIFICATION_FACT',negative_markers=negative,
                        classification_evidence=copy.deepcopy(getattr(self.metadata,'classification_evidence',{}).get(symbol,dict(source_url='https://fapi.binance.com/fapi/v1/exchangeInfo',received_at=utc(self.metadata.asof),sha256=self.metadata.sha))),
                        native_oi24h_percent=str(pct) if pct is not None else None,native_threshold_ge20=above,
                        native_observation_candidate=above is True and classification=='CRYPTO',
                        oi_semantics='USER_CONFIRMED_USD_VALUE_24H_CHANGE',source_time_verified=False,entry_authorized=False)
                if self.scale=='UNKNOWN': failure=Failure('SCALE_NOT_CONFIGURED');break
                if self.single_page: status='SINGLE_PAGE_OBSERVED';break
                if boundary and not inversions: status='FAST_BOUNDARY_OBSERVED_PARTIAL_NO_SNAPSHOT_TOKEN';offset=0;break
                if not batch or nxt is None: status='PAGINATION_END_OBSERVED_NON_ATOMIC' if not batch else 'NEXT_OFFSET_MISSING_PARTIAL';offset=0;break
                if nxt<=offset: status='NEXT_OFFSET_NOT_ADVANCING_PARTIAL';offset=0;break
                offset=nxt
            except Failure as exc: failure=exc;break
        if failure and (failure.stop or not pages):
            # A fatal source error invalidates all admission. Transient transport never renews old receipts.
            record_transport_failure(self.store,failure)
            return failure
        if failure:status='TRANSIENT_PAGE_FAILURE_PARTIAL'
        self.resume_offset=offset
        now=time.time()
        if self.single_page or (initial_offset==0 and boundary and not inversions):
            self.row_cache={}
        self.row_cache.update(observed)
        self.row_cache={s:r for s,r in self.row_cache.items()
            if 0<=now-dt.datetime.fromisoformat(r['received_at'].replace('Z','+00:00')).timestamp()<self.store.ttl}
        rows=sorted(self.row_cache.values(),key=lambda r:number(r['native_oi24h_percent']),reverse=True)
        snapshot=dict(schema=SCHEMA,pid=os.getpid(),round=round_,started_at=started,completed_at=utc(),snapshot_generated_at=utc(),
            elapsed_seconds=time.monotonic()-tick,status=status,phase='DIRECT_OPENMARKET_OI20',
            direct_candidates_authorized=bool(pages),publication_state='ROW_ISOLATED_PARTIAL_COVERAGE',request_attempts=attempts,
            pages=pages,observations=rows,threshold_boundary_observed_with_declared_scale=boundary,
            adjacent_raw_order_violations=inversions,duplicate_observations=duplicates,missing_oi_during_scan=invalid,
            observed_order_nonincreasing=not bool(inversions),counts=dict(unique_rows=len(rows),native_ge20_crypto=sum(r['native_observation_candidate'] for r in rows)),
            metadata_path=self.metadata.path,metadata_sha256=self.metadata.sha,metadata_as_of=utc(self.metadata.asof),
            metadata_source_url='https://fapi.binance.com/fapi/v1/exchangeInfo',scale=self.scale,scale_evidence=self.evidence,
            interval_seconds=self.interval,requested_page_size=60,page_budget=page_budget,soft_cycle_budget_seconds=30,
            exchange='BINANCE_FUTURES',category='PERPETUAL',sort_field='MARKET_SYMBOL_OI_CHANGE_24H',direction='SORT_DIRECTION_DESC',
            source_time_status='UNKNOWN_NO_VERIFIED_SOURCE_AS_OF_FIELD',usd_base_status='USER_CONFIRMED_USD_VALUE',
            entry_authorized=False,persistence='MEMORY_ONLY_ORIGINAL_ROW_RECEIPTS',
            coverage_scope=('OFFSET_ZERO_PAGE_60_ONLY' if self.single_page
                            else 'PARTIAL_NON_ATOMIC_PAGINATION_NOT_FULL_MARKET_PROOF'),
            coverage=dict(initial_offset=initial_offset,next_offset=offset,continuation_pending=offset>0,
                complete_market_claim=False,resource_bound_not_top_n=True,invalid_rows=invalid,order_inversions=inversions))
        if key and key in json.dumps(snapshot): self.store.fail('PUBLICATION_REJECTED'); return Failure('PUBLICATION_REJECTED')
        snapshot.update(latest_rate_limit_headers=self.rate_headers,rate_headers_received_at=self.rate_received,
            rate_header_scope='LAST_HTTP_RESPONSE_IN_THIS_CYCLE_ONLY_NOT_A_CURRENT_QUOTA_GUARANTEE')
        self.store.publish(snapshot)
        if failure:record_transport_failure(self.store,failure)
        return failure

    def run(self, key, max_rounds=0):
        round_=0; rates=networks=0
        while not self.stop.is_set():
            round_+=1; tick=time.monotonic(); failure=self.cycle(round_,key)
            rendered=self.store.render()
            try: print(json.dumps(dict(round=round_,health=rendered['health'],failure_detail=rendered.get('failure_detail'))),flush=True)
            except (OSError,ValueError): pass
            if failure and failure.stop: return failure.state
            rates=rates+1 if failure and failure.state.startswith('HTTP_429') else 0
            networks=networks+1 if failure and failure.state.startswith('NETWORK_') else 0
            transient = (failure is not None and (failure.state in ('HTTP_429_RETRY_WAIT','HTTP_5XX_RETRY_WAIT','HTTP_408_RETRY_WAIT') or
                 failure.state=='NETWORK_FAILED_CURRENT_ROUND_NOT_FRESH'
                 and isinstance(failure.detail,dict) and failure.detail.get('transport_failure',{}).get('retryable') is True))
            if max_rounds and round_>=max_rounds: return 'STOP_EXPLICIT_ROUND_LIMIT'
            deadline=max(tick+self.interval,time.monotonic()+(failure.retry or 0) if failure else 0)
            if transient:
                # One full configured interval AFTER every transient failure.
                # This is no faster than the original start-to-start schedule,
                # with no increasing blackout or extra request/inline retry.
                delay=max(self.interval,failure.retry or 0)
                detail=dict(failure.detail,recovery={'state':'WAITING_FOR_FRESH_ROUND',
                    'consecutive_transient_failures':min(networks,10**12),'retry_delay_seconds':delay})
                record_transport_failure(self.store,failure,detail)  # original snapshot only while still valid
                try: print(json.dumps(dict(round=round_,health=failure.state,failure_detail=detail)),flush=True)
                except (OSError,ValueError): pass
                deadline=max(deadline,time.monotonic()+delay)
            while deadline-time.monotonic()>3600:
                if self.stop.wait(3600):return 'CANCELLED'
            if self.stop.wait(max(0,deadline-time.monotonic())): break
        self.store.fail('CANCELLED'); return 'CANCELLED'

class IPC:
    def __init__(self, path, store, stop, *, consumer_uid=None, shared_gid=None):
        self.path,self.store,self.stop=path,store,stop
        self.consumer_uid=os.getuid() if consumer_uid is None else consumer_uid
        cross_uid=self.consumer_uid!=os.getuid()
        if type(self.consumer_uid) is not int or self.consumer_uid<0 or cross_uid and (type(shared_gid) is not int or shared_gid!=os.getgid()):
            raise ValueError('EXPLICIT_IPC_UID_GID_REQUIRED')
        folder_mode,socket_mode=(0o750,0o660) if cross_uid else (0o700,0o600)
        # Dedicated empty directory; never replace an existing socket or directory.
        os.mkdir(os.path.dirname(path),folder_mode)
        self.sock=socket.socket(socket.AF_UNIX); self.sock.bind(path); os.chmod(path,socket_mode); self.sock.listen(8); self.sock.settimeout(.2)
        self.directory_identity=os.lstat(os.path.dirname(path))
        self.socket_identity=os.lstat(path)
        self.thread=threading.Thread(target=self.serve,daemon=True); self.thread.start()

    def serve(self):
        try:
            self._serve()
        except Exception:
            self.store.terminate('IPC_LISTENER_FAILED')
            self.stop.set()

    def _serve(self):
        while not self.stop.is_set():
            try: client,_=self.sock.accept()
            except socket.timeout: continue
            except OSError as exc:
                if self.stop.is_set(): return
                if exc.errno in (errno.EINTR, errno.ECONNABORTED): continue
                self.store.terminate('IPC_LISTENER_FAILED')
                self.stop.set()
                return
            with client:
                try:
                    client.settimeout(1)
                    if hasattr(socket,'SO_PEERCRED'):
                        _,uid,_=struct.unpack('3i',client.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,12))
                        if uid!=self.consumer_uid: continue
                    elif hasattr(client,'getpeereid'):
                        if client.getpeereid()[0]!=self.consumer_uid: continue
                    else: continue
                    if client.recv(16)!=b'GET\n': continue
                    body=json.dumps(self.store.render(),separators=(',',':')).encode()
                    if len(body)>FRAME_LIMIT+16384: self.store.fail('RESOURCE_LIMIT'); continue
                    client.settimeout(2); client.sendall(body)
                except OSError: continue

    def close(self):
        self.store.terminate('STOPPED')
        self.stop.set(); self.sock.close(); self.thread.join(2)
        try:
            folder=os.lstat(os.path.dirname(self.path)); item=os.lstat(self.path)
            if (folder.st_dev,folder.st_ino,folder.st_uid)!=(self.directory_identity.st_dev,self.directory_identity.st_ino,os.getuid()): return
            if (item.st_dev,item.st_ino,item.st_uid)==(self.socket_identity.st_dev,self.socket_identity.st_ino,os.getuid()) and stat.S_ISSOCK(item.st_mode): os.unlink(self.path)
            os.rmdir(os.path.dirname(self.path))
        except OSError: pass  # Preserve unknown/replaced/nonempty paths.

def reject_acl(fd, *, directory=False):
    # ENODATA proves this Linux inode has no stored POSIX ACL. Unsupported,
    # permission denied, malformed or any present ACL is not absence evidence.
    if not sys.platform.startswith('linux') or not hasattr(os, 'getxattr'):
        raise Failure('CREDENTIAL_ACL_UNVERIFIABLE')
    for name in ('system.posix_acl_access', 'system.posix_acl_default') if directory else ('system.posix_acl_access',):
        try:
            os.getxattr(fd, name)
        except OSError as exc:
            if exc.errno == errno.ENODATA: continue
            raise Failure('CREDENTIAL_ACL_UNVERIFIABLE') from None
        raise Failure('CREDENTIAL_ACL_PRESENT')

def require_nondumpable():
    """Mandatory process-local guard; no credential path is touched on failure."""
    if not sys.platform.startswith('linux'):
        raise Failure('CREDENTIAL_PROCESS_PROTECTION_UNAVAILABLE')
    try:
        libc=ctypes.CDLL(None,use_errno=True)
        prctl=libc.prctl
        prctl.argtypes=[ctypes.c_int,ctypes.c_ulong,ctypes.c_ulong,ctypes.c_ulong,ctypes.c_ulong]
        prctl.restype=ctypes.c_int
        if prctl(4,0,0,0,0)!=0 or prctl(3,0,0,0,0)!=0:
            raise Failure('CREDENTIAL_PROCESS_PROTECTION_REJECTED')
    except (AttributeError,OSError):
        raise Failure('CREDENTIAL_PROCESS_PROTECTION_UNAVAILABLE') from None

def read_credential(path):
    require_nondumpable()  # Every entry must pass before opening any directory/key.
    # Walk directory FDs so neither the file nor an ancestor symlink is followed.
    if not os.path.isabs(path) or any(part in ('.', '..') for part in path.split('/')):
        raise Failure('CREDENTIAL_PATH_REJECTED')
    parts = [part for part in path.split('/') if part]
    if not parts: raise Failure('CREDENTIAL_PATH_REJECTED')
    directory = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory); directory = child
        info = os.fstat(directory)
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise Failure('CREDENTIAL_DIRECTORY_REJECTED')
        reject_acl(directory, directory=True)
        fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        with os.fdopen(fd, 'rb') as stream:
            before = os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid() or stat.S_IMODE(before.st_mode) != 0o600 or before.st_nlink != 1:
                raise Failure('CREDENTIAL_METADATA_REJECTED')
            reject_acl(stream.fileno())
            key = stream.read(8193)
            after = os.fstat(stream.fileno())
            fields = ('st_dev','st_ino','st_uid','st_mode','st_nlink','st_size','st_mtime_ns','st_ctime_ns')
            if any(getattr(before, field) != getattr(after, field) for field in fields):
                raise Failure('CREDENTIAL_CHANGED_DURING_READ')
            reject_acl(stream.fileno())
            reject_acl(directory, directory=True)
        if not 1 <= len(key) <= 8192 or not all(33 <= c <= 126 for c in key):
            raise Failure('CREDENTIAL_FORMAT_REJECTED')
        return key
    finally:
        os.close(directory)

def main():
    parser=argparse.ArgumentParser(); parser.add_argument('--metadata',required=True); parser.add_argument('--credential-file',required=True); parser.add_argument('--socket',default='/run/openmarket/instance/snapshot.sock'); parser.add_argument('--scale',choices=['UNKNOWN','ratio','percent'],default='UNKNOWN'); parser.add_argument('--scale-evidence'); parser.add_argument('--interval',type=int,default=60)
    parser.add_argument('--ipc-consumer-uid',type=int);parser.add_argument('--ipc-shared-gid',type=int)
    parser.add_argument('--single-page', action='store_true', help='Read only offset zero each round')
    args=parser.parse_args()
    if not 15<=args.interval<=3600: parser.error('interval out of bounds')
    if args.ipc_consumer_uid is not None and (args.ipc_consumer_uid<0 or args.ipc_consumer_uid!=os.getuid() and args.ipc_shared_gid!=os.getgid()):
        parser.error('explicit cross-UID IPC group required')
    stop=threading.Event(); store=Store(args.interval)
    for sig in (signal.SIGINT,signal.SIGTERM): signal.signal(sig,lambda *_:stop.set())
    ipc=None
    try:
        metadata=Metadata(args.metadata)
        key=read_credential(args.credential_file)
        collector=Collector(metadata,store,stop,args.scale,args.scale_evidence,args.interval,single_page=args.single_page)
        ipc=IPC(args.socket,store,stop,consumer_uid=args.ipc_consumer_uid,shared_gid=args.ipc_shared_gid)
        result=collector.run(key.decode('ascii'))
        return 0 if result=='CANCELLED' else 1
    except Exception:
        print('STOP_LOCAL_FAILURE_DETAILS_SUPPRESSED',flush=True); return 1
    finally:
        if ipc: ipc.close()

if __name__=='__main__':
    raise SystemExit(main())
