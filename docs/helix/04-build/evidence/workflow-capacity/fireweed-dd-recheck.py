import hashlib,json,os,shutil,subprocess,tempfile,time,threading
from pathlib import Path
root=Path(tempfile.mkdtemp(prefix='dd-recheck-',dir='target/workflow-capacity')).resolve()
source=Path(tempfile.mkstemp(prefix='fireweed-dd-source-',dir='/tmp')[1])
size=8*1024**3;block=16*1024**2
out=Path('/tmp/fireweed-dd-recheck-results.json');results=[]
def disk():return list(map(int,Path('/sys/block/nvme0n1/stat').read_text().split()))
def sample():
 r={'monotonic':time.monotonic(),'disk':disk()}
 r['temperature_millic']={str(p):int(p.read_text()) for h in Path('/sys/class/hwmon').glob('hwmon*') if (h/'name').read_text().strip()=='nvme' for p in h.glob('temp*_input')}
 r['io_pressure']=Path('/proc/pressure/io').read_text().strip()
 return r
try:
 assert shutil.disk_usage('/tmp').free>size+1024**3
 print('Preparing 8 GiB incompressible tmpfs source; excluded from measurement',flush=True)
 subprocess.run(['dd','if=/dev/urandom','of='+str(source),'bs=16M','count=512','iflag=fullblock','status=none'],check=True)
 with source.open('rb') as f:
  first=hashlib.sha256(f.read(block)).hexdigest();f.seek(size-block);last=hashlib.sha256(f.read(block)).hexdigest()
 for mode in ['direct-nocow','buffered-normal']:
  target=root/mode;target.touch()
  if mode=='direct-nocow':subprocess.run(['chattr','+C',str(target)],check=True)
  attrs=subprocess.check_output(['lsattr','-d',str(target)],text=True).strip()
  cmd=['dd','if='+str(source),'of='+str(target),'bs=16M','count=512','iflag=fullblock','conv=fdatasync','status=progress']
  if mode=='direct-nocow':cmd.append('oflag=direct')
  samples=[];stop=threading.Event()
  def monitor():
   while not stop.is_set():samples.append(sample());stop.wait(1)
  thread=threading.Thread(target=monitor);thread.start()
  print('Starting '+mode,flush=True)
  start=time.monotonic()
  with Path('/tmp/fireweed-dd-'+mode+'.log').open('w') as log:
   proc=subprocess.Popen(cmd,stderr=log);rc=proc.wait()
  end=time.monotonic();stop.set();thread.join();samples.append(sample())
  assert rc==0
  with target.open('rb') as f:
   assert hashlib.sha256(f.read(block)).hexdigest()==first;f.seek(size-block);assert hashlib.sha256(f.read(block)).hexdigest()==last
  assert target.stat().st_size==size
  a,b=samples[0],samples[-1];d=[y-x for x,y in zip(a['disk'],b['disk'])];elapsed=b['monotonic']-a['monotonic']
  r={'mode':mode,'command':cmd,'file_attributes':attrs,'wall_s':end-start,'bytes':size,'MiB_s':size/(end-start)/1048576,'dd_output':Path('/tmp/fireweed-dd-'+mode+'.log').read_text(),'device_MiB_s':d[6]*512/1048576/elapsed,'device_write_await_ms':d[7]/d[4] if d[4] else None,'device_busy_percent':d[9]/elapsed/10,'verified_first_last':True,'samples':samples}
  results.append(r);out.write_text(json.dumps({'kernel':os.uname().release,'source_tmpfs':True,'source_preparation_excluded':True,'results':results},indent=2)+'\n')
  print(json.dumps({k:r[k] for k in ['mode','wall_s','MiB_s','device_MiB_s','device_write_await_ms','device_busy_percent']}),flush=True)
  target.unlink()
finally:
 source.unlink(missing_ok=True)
 if root.exists() and not any(root.iterdir()):root.rmdir()
