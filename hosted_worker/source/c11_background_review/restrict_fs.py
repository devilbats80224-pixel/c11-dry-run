import ctypes as C,os,sys
from pathlib import Path
class Rules(C.Structure):_fields_=[('access',C.c_uint64)]
class PathRule(C.Structure):_pack_=1;_fields_=[('allow',C.c_uint64),('fd',C.c_int)]
lib=C.CDLL(None,use_errno=True)
def check(x):
 if x<0:raise OSError(C.get_errno(),os.strerror(C.get_errno()))
 return x
def confine(read_roots):
 if sys.platform!='linux' or os.uname().machine not in ('aarch64','x86_64'):raise RuntimeError('NO_KNOWN_LANDLOCK_ABI')
 abi=check(lib.syscall(444,0,0,1));assert abi>=3
 rules=Rules((1<<15)-1);fd=check(lib.syscall(444,C.byref(rules),C.sizeof(rules),0))
 try:
  for path in read_roots:
   p=os.open(path,os.O_PATH|os.O_CLOEXEC)
   try:
    r=PathRule((1<<2)|((1<<3) if Path(path).is_dir() else 0),p);check(lib.syscall(445,fd,1,C.byref(r),0))
   finally:os.close(p)
  check(lib.prctl(38,1,0,0,0));check(lib.syscall(446,fd,0))
 finally:os.close(fd)
 return abi
