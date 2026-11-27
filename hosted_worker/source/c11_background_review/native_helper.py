"""Dormant proof helper. Exact installed CCXT + guarded Session, injected physical tape."""
import sys
assert len(sys.argv)==3 and sys.argv[2].startswith('/') and sys.argv[1].startswith('/') and sys.argv[1].endswith('/site-packages')
sys.path[:0]=[sys.argv[1],sys.argv[2],'/opt/c11_owned_modules','/opt/c11_worker','/opt/c11_worker/ports','/opt/c11_background_review']
import csv,hashlib,io,json,os,socket,ssl,time,zipfile
from pathlib import Path
from urllib.parse import urlsplit,parse_qs
from ccxt import binance
from requests import Response
from requests.adapters import DEFAULT_CA_BUNDLE_PATH
from child_fs_profile import CA_SHA256,lock_system_reads
from installed_build_inputs import KlineOnlySession
from capture import source_request
from native_shadow_queue import headers_copy,weight
assert not __import__('os').environ.keys()-{'LC_CTYPE','OPENBLAS_NUM_THREADS','OMP_NUM_THREADS'}
p=Path('/scratch/SAPIENUSDT-5m-2025-08.zip')
raw_zip=p.read_bytes()
assert hashlib.sha256(raw_zip).hexdigest()==p.with_suffix('.zip.CHECKSUM').read_text().split()[0]
with zipfile.ZipFile(io.BytesIO(raw_zip)) as z:
 with z.open(z.namelist()[0]) as f:
  tape_rows=[x for x in csv.reader(io.TextIOWrapper(f)) if x[0].isdigit()]
for x in tape_rows:
 for i in (0,6,8):x[i]=int(x[i])
s=KlineOnlySession()
client=binance(dict(session=s,enableRateLimit=False,options={'maxRetriesOnFailure':0}))
assert all(not getattr(client,k,None) for k in ('apiKey','secret','password','uid','accountId','walletAddress','privateKey'))
abi,files=lock_system_reads(DEFAULT_CA_BUNDLE_PATH)
ctx=ssl.create_default_context(cafile=DEFAULT_CA_BUNDLE_PATH)
assert ctx.verify_mode==ssl.CERT_REQUIRED and ctx.check_hostname
assert socket.getaddrinfo('fixture.c11.invalid',443,socket.AF_INET,socket.SOCK_STREAM)[0][-1][0]=='127.0.0.43'
for sensitive in ('/testwork/rejoin.sqlite','/c11-paper/trades.c11.paper.sqlite'):
 try:fd=os.open(sensitive,os.O_RDONLY)
 except (PermissionError,FileNotFoundError):pass
 else:os.close(fd);raise ValueError('SQLITE_READ_DENIED_REQUIRED')
profile={'ca':CA_SHA256,'abi':abi,'uid':os.getuid(),'resolver':'LOCAL_FIXTURE_ONLY'}
r=json.loads(sys.stdin.readline(2048)); q=r['query']; tag=r['tag']
assert len(tag)==32 and set(q)=={'symbol','interval','startTime','endTime','limit'}
count=int(q['limit']);start=int(q['startTime']);stop=int(q['endTime'])+1
assert q['interval']=='5m' and q['symbol']=='SAPIENUSDT' and 1<=count<=499 and start%300000==stop%300000==0 and stop-start==count*300000
calls=[]
def tape(req,**kw):
 source_request(req.method,req.url,req.headers,req.body,kw.get('verify'),q)
 assert kw['timeout']==(.45,.45) and not kw['allow_redirects'] and s.trust_env is False
 assert not calls; calls.append(req.url)
 print(json.dumps({'attempt':q,'tag':tag}),flush=True)
 if r['fixture']=='stall':time.sleep(60)
 rows=[row for row in tape_rows if start<=row[0]<stop]
 if r['fixture']=='short': rows=rows[:-1]
 if r['fixture']=='gap': rows[0]=rows[1]
 z=Response();z.status_code=200;z.url=req.url;z.headers['Content-Type']='application/json'
 if r['fixture']!='noheader':z.headers['X-MBX-USED-WEIGHT-1M']='17'
 z._content=json.dumps(rows).encode()
 return z
s.send=tape
c=client
print(json.dumps({'ready':q,'tag':tag,'profile':profile}),flush=True)
assert sys.stdin.readline(100)==f'GO {tag}\n'
raw=c.fapiPublicGetKlines(dict(symbol=q['symbol'],interval=q['interval'],limit=count,startTime=start,endTime=stop-1))
assert len(calls)==1 and len(raw)<=count
print(json.dumps({'tag':tag,'status':200,'weight':weight(headers_copy(c.last_response_headers)),'raw':raw}),flush=True)
