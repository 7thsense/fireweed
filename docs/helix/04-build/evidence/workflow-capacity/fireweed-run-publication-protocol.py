from pathlib import Path
import os,subprocess,tempfile,json,time,hashlib
repo=Path('/home/erik/Projects/fireweed');binary=Path('/tmp/fireweed-publication-protocol');base=Path('/tmp/fireweed-publication-protocol-comparison')
assert not subprocess.run(['ps','-C','fireweed-workload','-o','pid='],capture_output=True).stdout.strip()
root=Path(tempfile.mkdtemp(prefix='publication-protocol-',dir=repo/'target/workflow-capacity'))
fd=os.open(root.parent,os.O_RDONLY|os.O_DIRECTORY);os.fsync(fd);os.close(fd)
provenance={'purpose':'same-filesystem durable publication protocol comparison, not workflow qualification or an SSD ceiling','root':str(root),'threads':48,'iterations_per_thread':32,'payload_bytes':262144,'manifest_bytes':4096,'order':['immutable','append','append','immutable'],'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'source_sha256':hashlib.sha256(Path('/tmp/fireweed-publication-protocol.c').read_bytes()).hexdigest(),'mount':json.loads(subprocess.check_output(['findmnt','--json','--target',str(root)])),'directory_compression':subprocess.check_output(['btrfs','property','get',str(root),'compression'],text=True),'append_setup_includes_durable_segment_creation_before_timed_loop':True,'all_mode_roots_retained_until_all_timed_runs_finish':True}
Path(str(base)+'-provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
results=[]
for index,mode in enumerate(provenance['order']):
 run=root/f'{index}-{mode}';run.mkdir();fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY);os.fsync(fd);os.close(fd)
 prefix=Path(str(base)+f'-{index}-{mode}');stop=Path(str(prefix)+'.stop');stop.unlink(missing_ok=True);pidfile=Path(str(prefix)+'.pid');pidfile.unlink(missing_ok=True)
 monitor=subprocess.Popen(['python3','/tmp/fireweed-device-monitor-v3.py',str(prefix)+'-device.jsonl',str(stop),str(pidfile)])
 try:
  with Path(str(prefix)+'.json').open('w') as out,Path(str(prefix)+'.stderr').open('w') as err:
   p=subprocess.Popen([str(binary),str(run),mode,'48','32','262144'],stdout=out,stderr=err);pidfile.write_text(str(p.pid));print('running',index,mode,'pid',p.pid,flush=True)
   try:rc=p.wait(timeout=240)
   except subprocess.TimeoutExpired:p.kill();p.wait();raise
  assert rc==0,(mode,rc)
 finally:stop.touch();monitor.wait(timeout=10)
 r=json.loads(Path(str(prefix)+'.json').read_text());r['index']=index;results.append(r);print(json.dumps(r),flush=True)
 with Path(str(prefix)+'-device-summary.json').open('w') as out:subprocess.run(['python3','/tmp/fireweed-device-summary-v3.py',str(prefix)+'-device.jsonl'],stdout=out,check=True)
# Validate identical streams only after all timing, preserving comparability.
checks=[]
for index,mode in enumerate(provenance['order']):
 hashes=[]
 for worker in range(48):
  d=root/f'{index}-{mode}'/f'thread-{worker:03}';h=hashlib.sha256()
  if mode=='append':
   with (d/'log.segment').open('rb') as f:
    while chunk:=f.read(1024*1024):h.update(chunk)
  else:
   for n in range(32):
    h.update((d/f'{n:08}.data').read_bytes());h.update((d/f'{n:08}.manifest').read_bytes())
  hashes.append(h.hexdigest())
 checks.append(hashes)
assert all(x==checks[0] for x in checks)
Path(str(base)+'-summary.json').write_text(json.dumps({'runs':results,'identical_streams_verified':True,'stream_hashes':checks[0],'root_requires_owned_cleanup':str(root)},indent=2)+'\n');print('all protocols wrote identical streams; root',root,flush=True)
