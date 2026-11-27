import os,sys
sys.path.insert(0,'/opt/c11_background_review')
from restrict_fs import confine
paths=('/c11-paper/trades.c11.paper.sqlite','/c11-paper/causal-candles.sqlite')
assert all(os.path.isfile(path) for path in paths)
abi=confine([])
for path in paths:
 for flags in (os.O_RDONLY,os.O_WRONLY,os.O_RDWR):
  try:fd=os.open(path,flags)
  except PermissionError:continue
  else:os.close(fd);raise AssertionError('LEDGER_ACCESS')
print(f'WARM_NO_AUTHORITY_CONFIRMED_ABI_{abi}',flush=True)
assert sys.stdin.buffer.read(1)==b''
