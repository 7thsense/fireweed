"""Bounded private-file sequential-write calibration; not workflow qualification."""
import hashlib,json,mmap,os,shutil,subprocess,tempfile,time,resource
from pathlib import Path
import sys,socket,platform

base=Path(sys.argv[1]).expanduser().resolve()
assert base.is_dir()
assert shutil.disk_usage(base).free > 80*1024**3, 'Need 80 GiB free for bounded 64 GiB calibration'
mode=sys.argv[2]
assert mode in ('direct','buffered')
mount_info=json.loads(subprocess.check_output(['findmnt','--json','--target',str(base)]))
fstype=mount_info['filesystems'][0]['fstype']
assert fstype not in ('tmpfs','ramfs'), 'Must use persistent storage'
metadata={'host':socket.gethostname(),'kernel':platform.release(),'python':sys.version,'mode':mode,'mount':mount_info,'lsblk':subprocess.check_output(['lsblk','-e','7','-o','NAME,TYPE,MODEL,SIZE,FSTYPE,MOUNTPOINTS']).decode(),'loadavg_before':Path('/proc/loadavg').read_text(),'diskstats_before':Path('/proc/diskstats').read_text()}
root=Path(tempfile.mkdtemp(prefix='sequential-headroom-',dir=base))
path=root/'probe.bin'
chunk=16*1024*1024
size=64*1024**3
requested_size=size
max_write_seconds=180
path.touch()
if mode=='direct' and fstype=='btrfs':
 subprocess.run(['chattr','+C',str(path)],check=True)
attributes=subprocess.check_output(['lsattr','-d',str(path)]).decode().strip()
if fstype=='btrfs':
 assert ('C' in attributes.split()[0]) == (mode=='direct')
# Repeated incompressible blocks avoid entropy-generation cost in the timed path.
buffer=mmap.mmap(-1,chunk)
buffer[:]=os.urandom(chunk)
expected=hashlib.sha256(buffer).hexdigest()
fd=os.open(path,os.O_WRONLY|(os.O_DIRECT if mode=='direct' else 0))
times=[]
before=resource.getrusage(resource.RUSAGE_SELF)
start=time.monotonic()
actual_size=0
try:
 for offset in range(0,size,chunk):
  began=time.monotonic()
  assert os.pwrite(fd,buffer,offset)==chunk
  times.append(time.monotonic()-began)
  actual_size=offset+chunk
  if time.monotonic()-start>=max_write_seconds:
   break
 size=actual_size
 flush=time.monotonic()
 os.fdatasync(fd)
 flush_s=time.monotonic()-flush
 wall=time.monotonic()-start
finally:
 os.close(fd)
after=resource.getrusage(resource.RUSAGE_SELF)
with path.open('rb') as f:
 assert hashlib.sha256(f.read(chunk)).hexdigest()==expected
 f.seek(size-chunk)
 assert hashlib.sha256(f.read(chunk)).hexdigest()==expected
report={'diagnostic':True,'purpose':'ideal contiguous private-file write calibration, not a workflow or COW-page-overwrite benchmark',
 'environment':metadata,'diskstats_after':Path('/proc/diskstats').read_text(),
 'root':str(root),'attributes':attributes,'bytes':size,'requested_bytes':requested_size,'max_write_seconds':max_write_seconds,'stopped_at_deadline':size<requested_size,'chunk_bytes':chunk,
 'direct_io_requested':mode=='direct','fdatasync_included':True,'wall_s':wall,'fdatasync_s':flush_s,
 'mib_s':size/2**20/wall,'user_cpu_s':after.ru_utime-before.ru_utime,'system_cpu_s':after.ru_stime-before.ru_stime,
 'block_sha256':expected,'first_last_blocks_verified':True,'chunk_write_s':times,
 'mount':json.loads(subprocess.check_output(['findmnt','--json','--target',str(path)]))}
assert root.parent==base and root.name.startswith('sequential-headroom-')
shutil.rmtree(root)
report['private_file_removed']=True
report['gib_chunks_mib_s']=[len(times[i:i+64])*chunk/2**20/sum(times[i:i+64]) for i in range(0,len(times),64)]
print(json.dumps(report,indent=2))
