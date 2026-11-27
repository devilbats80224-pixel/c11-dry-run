"""Fail-closed Linux child policy for literal file-only NSS and HTTPS prerequisites."""
import ctypes as c
import hashlib
import os
import stat
from pathlib import Path

class Rule(c.Structure):
    _pack_=1
    _fields_=[("access",c.c_uint64),("parent_fd",c.c_int)]

# Linux arm64/x86_64 on the installed kernel and Docker default seccomp only.
lib=c.CDLL(None,use_errno=True)
GET_VERSION=1
READ_FILE=1<<2
# ABI3 adds TRUNCATE; later IOCTL_DEV has no positive grant. No path rule
# permits directory traversal listing, creation, write, executable or rename.
HANDLE_FS=(1<<15)-1

DNS_FILES=('/etc/hosts','/etc/resolv.conf','/etc/nsswitch.conf','/etc/gai.conf','/etc/host.conf')
CA_PATH='/home/ftuser/.local/lib/python3.14/site-packages/certifi/cacert.pem'
CA_SHA256='50f79f98dac192b20dbf73f35c56f0b0c557ab267ff5199d1117cc1180b70785'

def ck(ret):
    if ret<0:
        err=c.get_errno()
        raise OSError(err,os.strerror(err))
    return ret

def lock_system_reads(ca):
    if os.uname().sysname!='Linux' or os.uname().machine not in ('x86_64','aarch64'):
        raise RuntimeError('UNSUPPORTED_LANDLOCK_PLATFORM')
    if (os.getuid(),os.getgid())!=(1000,2000):
        raise RuntimeError('UNEXPECTED_REVIEW_CHILD_IDENTITY')
    real_ca=Path(ca).resolve(strict=True)
    if str(real_ca)!=CA_PATH:
        raise ValueError('CA_PATH_NOT_INSTALLED')
    valid=DNS_FILES+(CA_PATH,)
    fd_names=os.listdir('/proc/self/fd')
    # listdir uses one temporary directory FD; verify only live unexpected FDs.
    if any(name.isdecimal() and int(name)>2 and os.path.lexists('/proc/self/fd/'+name) for name in fd_names):
        raise RuntimeError('INHERITED_CHILD_FD')
    abi=ck(lib.syscall(444,0,0,GET_VERSION))
    if abi<3:raise RuntimeError('LANDLOCK_ABI3_REQUIRED')
    flags=HANDLE_FS|((1<<15) if abi>=5 else 0)
    fd=ck(lib.syscall(444,c.byref(c.c_uint64(flags)),8,0))
    added=[]
    try:
        for filename in dict.fromkeys(valid):
            try:k=os.open(filename,os.O_PATH|os.O_CLOEXEC|os.O_NOFOLLOW)
            except FileNotFoundError:
                if filename in ('/etc/gai.conf','/etc/host.conf'):continue
                raise
            try:
                st=os.fstat(k)
                readonly_ca=(filename==str(real_ca) and st.st_uid==1000
                            and os.fstatvfs(k).f_flag&os.ST_RDONLY)
                if not(stat.S_ISREG(st.st_mode) and (st.st_uid==0 or readonly_ca)
                       and not st.st_mode&0o022):
                    raise ValueError('NON_ROOT_OR_MUTABLE_TRUST_FILE:'+filename)
                if filename==CA_PATH:
                    checked_fd=os.open('/proc/self/fd/'+str(k),os.O_RDONLY|os.O_CLOEXEC)
                    with os.fdopen(checked_fd,'rb') as pem:
                        if hashlib.file_digest(pem,'sha256').hexdigest()!=CA_SHA256:
                            raise ValueError('CA_BYTES_DIFFER_FROM_REVIEW_PIN')
                rule=Rule(READ_FILE,k)
                ck(lib.syscall(445,fd,1,c.byref(rule),0))
                added.append(filename)
            finally:os.close(k)
        ck(lib.prctl(38,1,0,0,0))
        ck(lib.syscall(446,fd,0))
    finally:os.close(fd)
    return abi,tuple(added)
