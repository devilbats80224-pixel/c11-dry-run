"""Original collector KernelTransport and kernel probes, extracted without edits."""
from strict_transport import StrictTransport
import os

def cgroup(pid):
 with open('/proc/'+str(pid)+'/cgroup') as f:value=f.read(4097)
 # This exact kernel layout was measured, not inferred from a server field.
 import re
 match=re.fullmatch(r'0::/\.\./([0-9a-f]{64})\n',value)
 if not match:raise ValueError('CGROUP_LAYOUT_NOT_VERIFIABLE')
 return match[1]

def starttime(pid):
 with open('/proc/'+str(pid)+'/stat') as f:value=f.read(8193)
 return int(value.rsplit(')',1)[1].split()[19])

def kernel_token(pid):
 return {'kernel_boot':bootid(),'pid_namespace':os.readlink('/proc/self/ns/pid'),'anchor_container':cgroup(1),'anchor_start':starttime(1),'source_container':cgroup(pid),'peer_pid':pid,'peer_start':starttime(pid),'uid':1001,'gid':2000}

class KernelTransport(StrictTransport):
 """PID reuse is legal only after a newly verified complete kernel token."""
 def refresh(self):
  self.clear('KERNEL_TOKEN_PENDING')
  try:
   binding=self.binding();expected=binding['kernel_token']
   if kernel_token(binding['peer_pid'])!=expected:raise ValueError('KERNEL_TOKEN_CHANGED')
   accepted=super().refresh()
   if kernel_token(binding['peer_pid'])!=expected:raise ValueError('KERNEL_TOKEN_CHANGED_DURING_READ')
   return accepted
  except (OSError,ValueError,KeyError,TypeError):self.clear('KERNEL_TOKEN_REFUSED');return False
 def rebind(self,expected_sha):
  self.clear('KERNEL_REBIND_PENDING');candidate=KernelTransport(self.path,self.binding_path,expected_sha)
  if not candidate.refresh():return False
  old=self.approved_binding;new=candidate.approved_binding
  if old:
   prior=old['kernel_token'];current=new['kernel_token']
   if prior==current:
    if old['instance_uuid']!=new['instance_uuid']:return False
    self.expected_sha=expected_sha;return self.refresh() # Preserve original lease.
   if old['instance_uuid']==new['instance_uuid']:return False
   # Full CID/initial image approval is checked by outer Recovery. Here keep
   # both principals, kernel boot and increasing process/anchor watermarks.
   for key in ('kernel_boot','source_container','anchor_container','uid','gid'):
    if prior[key]!=current[key]:return False
   if current['peer_start']<=prior['peer_start'] or current['anchor_start']<prior['anchor_start']:return False
   # Kernel namespace inode numbers themselves may be recycled after the old
   # namespace dies. Anchor starttime is therefore part of the identity, not
   # inferred from namespace inode inequality alone.
   if current['anchor_start']==prior['anchor_start'] and current['pid_namespace']!=prior['pid_namespace']:return False
   if current['peer_start']<current['anchor_start']:return False
  self.__dict__.update(candidate.__dict__);return True

def bootid():return open('/proc/sys/kernel/random/boot_id').read().strip()
