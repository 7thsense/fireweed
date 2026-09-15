"""Bounded private-file sequential-write calibration; not workflow qualification."""
import hashlib,json,mmap,os,shutil,subprocess,tempfile,time,resource
from pathlib import Path
base=Path('target/workflow-capacity').resolve()
root=Path(tempfile.mkdtemp(prefix='sequential-headroom-',dir=base))
path=root/'probe.bin'
chunk=16*1024*1024
size=8*1024**3
path.touch()
subprocess.run(['chattr','+C',str(path)],check=True)
attributes=subprocess.check_output(['lsattr','-d',str(path)]).decode().strip()
assert 'C' in attributes.split()[0]
# Repeated incompressible blocks avoid entropy-generation cost in the timed path.
buffer=mmap.mmap(-1,chunk)
buffer[:]=os.urandom(chunk)
expected=hashlib.sha256(buffer).hexdigest()
fd=os.open(path,os.O_WRONLY|os.O_DIRECT)
times=[]
before=resource.getrusage(resource.RUSAGE_SELF)
start=time.monotonic()
try:
 for offset in range(0,size,chunk):
  began=time.monotonic()
  assert os.pwrite(fd,buffer,offset)==chunk
  times.append(time.monotonic()-began)
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
 'head':subprocess.check_output(['git','rev-parse','HEAD']).decode().strip(),
 'dirty':subprocess.check_output(['git','status','--porcelain']).decode(),
 'root':str(root),'attributes':attributes,'bytes':size,'chunk_bytes':chunk,
 'direct_io_requested':True,'fdatasync_included':True,'wall_s':wall,'fdatasync_s':flush_s,
 'mib_s':size/2**20/wall,'user_cpu_s':after.ru_utime-before.ru_utime,'system_cpu_s':after.ru_stime-before.ru_stime,
 'block_sha256':expected,'first_last_blocks_verified':True,'chunk_write_s':times,
 'mount':json.loads(subprocess.check_output(['findmnt','--json','--target',str(path)]))}
assert root.parent==base and root.name.startswith('sequential-headroom-')
shutil.rmtree(root)
report['private_file_removed']=True
print(json.dumps(report,indent=2))
