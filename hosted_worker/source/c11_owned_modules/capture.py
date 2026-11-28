"""Bounded local evidence, not an authority signer, trading adapter or replay clock."""
from exact_symbol import ID_CHAR
from dynamic_minute_span import valid_minute_span
import hashlib,json,re,threading
from decimal import Decimal
from urllib.parse import parse_qsl,urlsplit

Q=900_000; M=60_000
WIDTH={'15m':Q,'1m':M,'5m':5*M}
INITIAL={'15m':260,'1m':30,'5m':3}
SYMBOL=re.compile(fr'{ID_CHAR}{{2,40}}\Z')
TICKET=re.compile(r'[a-f0-9]{32}\.[0-9]+\.[1-9][0-9]*\Z')
DEC=re.compile(r'[0-9]{1,32}(?:\.[0-9]{1,32})?\Z')
OI_RAW=re.compile(r'-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?')
def source_request(method,url,headers,body,verify,expected):
    # Check at the actual PreparedRequest boundary, before the SSL send.
    u=urlsplit(url)
    q=parse_qsl(u.query,strict_parsing=True)
    if not (method=='GET' and body is None and u.scheme=='https' and
            u.netloc=='fapi.binance.com' and not u.fragment and
            verify is True and u.path=='/fapi/v1/klines' and len(q)==len(expected)
            and dict(q)==expected and not any(k.lower() in
            ('authorization','proxy-authorization','x-mbx-apikey','cookie') for k in headers)):
        raise ValueError('EXACT_PUBLIC_TLS_ROUTE_REFUSED')
    return u.path
def count(t):
    x=getattr(t,'minute_limit',None)
    return x if t.tf=='1m' and x is not None else getattr(t,'limit',INITIAL.get(t.tf))
def acceptable(tf,n):
    return (tf=='5m' and type(n) is int and 3<=n<=499) or (tf=='1m' and valid_minute_span(n)) or n in (INITIAL[tf],1)
def query_for(t):
    n=count(t)
    if not SYMBOL.fullmatch(t.symbol) or t.tf not in WIDTH or not acceptable(t.tf,n):
        raise ValueError('INVALID_REQUEST_SCOPE')
    return dict(symbol=t.symbol,interval=t.tf,startTime=str(t.end-n*WIDTH[t.tf]),
                endTime=str(t.end-1),limit=str(n))

def sanitize(rows,tf,end,n,send,*,bootstrap=False):
    width=WIDTH[tf]
    legal=(tf=='1m' and type(n) is int and 1<=n<=495) if bootstrap is True else acceptable(tf,n)
    if len(rows)!=n or not legal:raise ValueError('EXACT_ROW_COUNT_REQUIRED')
    safe=[]
    for i,row in enumerate(rows):
        if len(row)!=12:raise ValueError('EXACT_TWELVE_FIELDS_REQUIRED')
        op,cl,nt=(row[j] for j in (0,6,8))
        if not (type(op) is type(cl) is type(nt) is int and nt>=0 and
                op==end-(n-i)*width and cl==op+width-1 and cl<send):
            raise ValueError('INVALID_TIME_GRID_OR_COUNT')
        lex=[row[j] for j in (1,2,3,4,5,7,9,10,11)]
        if not all(type(x) is str and DEC.fullmatch(x) for x in lex):
            raise ValueError('INVALID_DECIMAL_LEXEME')
        o,h,l,c,b,q,tb,tq,_=map(Decimal,lex)
        if not (0<l<=min(o,c)<=max(o,c)<=h and 0<=tb<=b and 0<=tq<=q):
            raise ValueError('INVALID_PRICE_VOLUME_RELATION')
        safe.append(list(row))
    return safe

def digest(p):
    return hashlib.sha256(json.dumps(p,sort_keys=True,allow_nan=False,separators=(',',':')).encode()).hexdigest()

class Capture:
    def __init__(self,end,wall):
        self.end=end; self.wall=wall; self._lock=threading.Lock(); self.records={}; self.authority=[]
    def snapshot(self,gen,members,at,ratios):
        if len(self.authority)>=96:raise ValueError('SNAPSHOT_PROOF_LIMIT')
        entry=dict(generation=gen,observed_ms=at,members=[
            dict(symbol=m.symbol,rank=m.rank,first_seen_ms=m.available,lease_exclusive_ms=m.expires,
              source_oi_change_decimal=ratios[m.symbol]) for m in members])
        if any(m.generation!=gen or not m.available<=at<m.expires
               or len(ratios[m.symbol])>128 or not OI_RAW.fullmatch(ratios[m.symbol])
               or Decimal(ratios[m.symbol])<Decimal('0.20') for m in members):
            raise ValueError('INVALID_OBSERVATION')
        self.authority.append(entry)
    def install(self,endpoint,select_capture=None):
        client=endpoint.api
        orig_send=client.session.send
        orig_fetch=endpoint.fetch
        state=threading.local()
        def send(req,**kw):
            if not hasattr(state,'expected') or state.trace is not None:
                raise ValueError('UNOWNED_OR_REDIRECTED_TLS_REQUEST')
            path=source_request(req.method,req.url,req.headers,req.body,kw.get('verify'),state.expected)
            state.trace=dict(method='GET',scheme='https',host='fapi.binance.com',path=path,
                             initial_request_uses_tls_verification=True,request_query=state.expected,
                             request_time_ms=self.wall())
            reply=orig_send(req,**kw)
            # Never treat redirects as an official same-origin transport proof.
            if reply.url!=req.url or reply.history or not 100<=reply.status_code<=599:
                raise ValueError('MISMATCHED_TLS_RESPONSE')
            state.trace.update(status=reply.status_code,response_time_ms=self.wall())
            return reply
        def fetch(task,request_id):
            target=self if select_capture is None else select_capture()
            if type(target) is not Capture or target.end!=task.end:
                raise ValueError('UNOWNED_CAPTURE_BOUNDARY')
            state.expected=query_for(task)
            state.trace=None
            try:
                r=orig_fetch(task,request_id)
                record=target.record(r,state.trace)
                with target._lock:
                    if len(target.records)>=96 or request_id in target.records:
                        raise ValueError('NON_UNIQUE_OR_OVER_BUDGET')
                    target.records[request_id]=record
                return r
            finally:
                del state.expected,state.trace
        client.session.send=send
        endpoint.fetch=fetch
    def record(self,r,trace):
        t=r.task
        if not TICKET.fullmatch(r.request_id) or r.request_id.split('.')[1:]!=[str(t.end),str(t.ticket)]:
            raise ValueError('OWNER_TICKET_MISMATCH')
        if t.end!=self.end:raise ValueError('WRONG_CAPTURE_WINDOW')
        n=count(t); status='UNVERIFIED_TRANSPORT'; raw=[]
        if trace and r.http==200 and trace.get('status')==200:
            try:
                if not t.end<=t.request<=r.requested_ms<=trace['request_time_ms']<=trace['response_time_ms']<=r.received_ms:
                    raise ValueError('RECEIPT_ORDER_UNVERIFIED')
                raw=sanitize(r.rows,t.tf,t.end,n,r.requested_ms)
                status='VALIDATED_ROWS' if r.received_ms<min(t.deadline,t.end+M) else 'VALIDATED_LATE_NO_ADMISSION'
            except ValueError as e:status=str(e)
        row=dict(request_id=r.request_id,ticket=t.ticket,symbol=t.symbol,timeframe=t.tf,
          generation=t.generation,end_ms=t.end,queued_at_ms=t.request,request_ms=r.requested_ms,
          receive_ms=r.received_ms,deadline_exclusive_ms=t.deadline,stage=getattr(t,'stage',None),
          reason=getattr(t,'reason',None),limit=n,reported_http=r.http,source=trace,raw_kline_rows=raw,
          capture_grade=status)
        return dict(row,row_sha256=digest(row))
    def payload(self,gaps,first,metrics):
        result=dict(schema='c11-public-kline-capture-v1',mode='ACTUAL_ISOLATED_PUBLIC_TRANSPORT_NOT_TRADE',
          end_ms=self.end,deadline_exclusive_ms=self.end+M,declared_call_cap=96,declared_weight_cap=144,
          observed_roster_snapshots=self.authority,unknown_source_intervals=gaps,unobserved_before_first_read=first,
          dispatch_count=metrics,receipts=sorted(self.records.values(),key=lambda x:(x['request_ms'],x['ticket'])),
          synthetic_provenance=False,production_apply=False,slot_wallet_orders_checked=False)
        assert len(result['receipts'])<=metrics<=96
        return dict(result,body_sha256=digest(result))
