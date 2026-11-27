"""Kernel-authenticated Linux IPC. Binding digest is supplied by trusted orchestration."""
import hashlib,json,os,socket,stat,struct,time
from datetime import datetime
from observation_shortlist_adapter import ObservationLease

MAX=3*1024*1024
def unique(items):
    result={}
    for key,value in items:
        if key in result: raise ValueError('DUPLICATE_FIELD')
        result[key]=value
    return result

def load_json(body):
    return json.loads(body,object_pairs_hook=unique,parse_constant=lambda _:(_ for _ in ()).throw(ValueError('NONFINITE')))

def atomic_publish(path,body):
    """Only the separate trusted publisher gets a writable binding volume."""
    import uuid
    folder=os.path.dirname(path); temporary=folder+'/.binding-'+uuid.uuid4().hex
    fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o400)
    try:
        with os.fdopen(fd,'wb') as stream:
            stream.write(body); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary,path)
        directory=os.open(folder,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try: os.fsync(directory)
        finally: os.close(directory)
    finally:
        try: os.unlink(temporary)
        except FileNotFoundError: pass

class StrictTransport:
    def __init__(self,path,binding_path,expected_sha):
        self.path,self.binding_path,self.expected_sha=path,binding_path,expected_sha
        self.lease=ObservationLease(); self.peer=None; self.last_body=None; self.status='NOT_READ'; self.last_error=None; self.approved_binding=None

    def clear(self,reason):
        self.status=reason; self.lease.clear(reason)

    def binding(self):
        fd=os.open(self.binding_path,os.O_RDONLY|os.O_NOFOLLOW)
        with os.fdopen(fd,'rb') as stream:
            info=os.fstat(stream.fileno()); body=stream.read(8193)
        # replace() can unlink an already-open approved inode. Its immutable
        # bytes remain valid only if the externally pinned digest still matches.
        if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid() or stat.S_IMODE(info.st_mode)!=0o400 or info.st_nlink not in (0,1) or len(body)>8192:
            raise ValueError('BINDING_FILE_REJECTED')
        if hashlib.sha256(body).hexdigest()!=self.expected_sha: raise ValueError('BINDING_NOT_APPROVED')
        value=load_json(body)
        if value.get('schema')!='openmarket_kernel_peer_binding_v1' or type(value.get('peer_pid')) is not int or value['peer_pid']<=0 or type(value.get('uid')) is not int or value['uid']<0 or value.get('consumer_uid',value['uid'])!=os.getuid():
            raise ValueError('BINDING_FIELDS_REJECTED')
        if value['uid']!=os.getuid() and value.get('ipc_gid')!=os.getgid():
            raise ValueError('CROSS_UID_IPC_GROUP_NOT_BOUND')
        if value.get('pid_namespace')!=os.readlink('/proc/self/ns/pid'):
            raise ValueError('PID_NAMESPACE_NOT_BOUND')
        return value

    def refresh(self):
        self.clear('IPC_REFRESH_PENDING'); self.last_error=None; stage='BINDING'; size=0  # Preserve all expiry/replay watermarks.
        try:
            binding=self.binding()
            folder=os.path.dirname(self.path); directory=os.open(folder,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try:
                d=os.fstat(directory); node=os.stat(os.path.basename(self.path),dir_fd=directory,follow_symlinks=False)
                cross_uid=binding['uid']!=os.getuid()
                if d.st_uid!=binding['uid'] or stat.S_IMODE(d.st_mode)!=(0o750 if cross_uid else 0o700) or not stat.S_ISSOCK(node.st_mode) or node.st_uid!=binding['uid'] or stat.S_IMODE(node.st_mode)!=(0o660 if cross_uid else 0o600) or node.st_nlink!=1:
                    raise ValueError('IPC_PATH_REJECTED')
                if cross_uid and (d.st_gid!=binding['ipc_gid'] or node.st_gid!=binding['ipc_gid']):
                    raise ValueError('IPC_PATH_GROUP_REJECTED')
                with socket.socket(socket.AF_UNIX) as sock:
                    stage='STRICT_CONNECT';sock.settimeout(2); deadline=time.monotonic()+2; sock.connect(self.path)
                    pid,uid,gid=struct.unpack('3i',sock.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,12))
                    self.peer=dict(pid=pid,uid=uid,gid=gid)
                    if pid<=0 or pid!=binding['peer_pid'] or uid!=binding['uid'] or cross_uid and gid!=binding['ipc_gid']:
                        raise ValueError('KERNEL_PEER_NOT_BOUND')
                    current=os.stat(os.path.basename(self.path),dir_fd=directory,follow_symlinks=False); current_dir=os.lstat(folder)
                    if (current.st_dev,current.st_ino)!=(node.st_dev,node.st_ino) or (current_dir.st_dev,current_dir.st_ino)!=(d.st_dev,d.st_ino):
                        raise ValueError('IPC_PATH_CHANGED')
                    stage='STRICT_SEND';sock.sendall(b'GET\n'); chunks=[]; size=0
                    while True:
                        stage='STRICT_RECV';remaining=deadline-time.monotonic()
                        if remaining<=0: raise TimeoutError('IPC_DEADLINE')
                        sock.settimeout(remaining); block=sock.recv(min(65536,MAX+1-size))
                        if not block: break
                        size+=len(block)
                        if size>MAX: raise ValueError('IPC_FRAME_LIMIT')
                        chunks.append(block)
            finally: os.close(directory)
            stage='STRICT_PARSE';body=b''.join(chunks); envelope=load_json(body); identity=envelope.get('collector_identity',{})
            if identity.get('instance_uuid')!=binding['instance_uuid'] or identity.get('pid')!=binding['peer_pid']:
                raise ValueError('BOOT_IDENTITY_NOT_BOUND')
            snapshot=envelope.get('snapshot',{})
            if snapshot.get('pid')!=self.peer['pid']: raise ValueError('SNAPSHOT_KERNEL_PID_MISMATCH')
            started=datetime.fromisoformat(binding['started_at'].replace('Z','+00:00')).timestamp()
            oldest=min(datetime.fromisoformat(p['received_at'].replace('Z','+00:00')).timestamp() for p in snapshot['pages'])
            if oldest<started: raise ValueError('RECEIPT_PREDATES_BOUND_CONTAINER')
            if not self.lease.accept_bytes(body,monotonic_now=time.monotonic()):
                self.status='LEASE_REJECTED'; return False
            self.last_body=body; self.approved_binding=binding; self.status='ACCEPTED_KERNEL_BOUND'; return True
        except (OSError,ValueError,TypeError,KeyError,AttributeError,OverflowError,RecursionError) as exc:
            self.last_error=dict(type=type(exc).__name__,errno=getattr(exc,'errno',None),stage=stage,bytes_received=size)
            self.clear('IPC_OR_BINDING_REJECTED'); return False

    def shortlist(self,verified_metadata_by_symbol):
        return self.lease.shortlist(verified_metadata_by_symbol,monotonic_now=time.monotonic())

    def rebind(self,expected_sha):
        """Explicit trusted-controller operation; caller must serialize access.

        Same-boot reapproval cannot reset receipt/generation/expiry watermarks.
        A new lease requires a new kernel PID AND a new approved boot UUID.
        """
        self.clear('EXPLICIT_REBIND_PENDING')
        candidate=StrictTransport(self.path,self.binding_path,expected_sha)
        if not candidate.refresh(): return False
        old=self.approved_binding; new=candidate.approved_binding
        if old is not None:
            same_pid=old['peer_pid']==new['peer_pid']; same_boot=old['instance_uuid']==new['instance_uuid']
            if same_pid and same_boot:
                self.expected_sha=expected_sha
                return self.refresh()  # Keep this object's original lease.
            if same_pid or same_boot: return False
        self.__dict__.update(candidate.__dict__)
        return True
