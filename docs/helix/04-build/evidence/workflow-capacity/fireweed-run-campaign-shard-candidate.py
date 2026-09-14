from pathlib import Path
import subprocess,sys,tempfile,json,time,hashlib,os
repo=Path(os.environ.get('FIREWEED_CANDIDATE_REPO','/home/erik/Projects/fireweed'))
tag=sys.argv[1]
cycles=sys.argv[2] if len(sys.argv)>2 else "3"
batch=sys.argv[3] if len(sys.argv)>3 else "1000"
extra_args=sys.argv[4:]
assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo).strip()
assert not subprocess.run(['pgrep','-f','^'+str(repo/'target/release/fireweed-workload')],capture_output=True).stdout.strip()
root=Path(tempfile.mkdtemp(prefix='compressed-'+tag+'-',dir=repo/'target/workflow-capacity'))
subprocess.run(['btrfs','property','set',str(root),'compression','zstd'],check=True)
base=Path('/tmp/fireweed-campaign-'+tag)
Path(str(base)+'-provenance.json').write_text(json.dumps({'projection_root':str(root),'property':subprocess.check_output(['btrfs','property','get',str(root),'compression'],text=True),'only_new_private_projection_directory_changed':True,'runtime_environment':{k:os.environ.get(k) for k in ['OBJECT_LOG_FLUSH_RUNTIME_THREADS']}},indent=2)+'\n')
stop=Path(str(base)+'-device.stop');stop.unlink(missing_ok=True)
pidfile=Path(str(base)+'.pid');pidfile.unlink(missing_ok=True)
monitor=subprocess.Popen(['python3','/tmp/fireweed-device-monitor-v3.py',str(base)+'-device.jsonl',str(stop),str(pidfile)])
try:
 with Path(str(base)+'.json').open('w') as output:
  runner=subprocess.Popen(['python3','scripts/perf/workflow-capacity.py','--profile','campaign','--campaign-metadata','--recycle','--cycles',cycles,'--items','1000000','--batch',batch,'--shards',os.environ.get('FIREWEED_CANDIDATE_SHARDS','32'),'--workers',os.environ.get('FIREWEED_CANDIDATE_WORKERS','2'),'--load-workers','2','--deadline-seconds','1800','--projection-root',str(root)]+extra_args+(['--qualify'] if int(cycles)>=3 else []),cwd=repo,stdout=output)
  while runner.poll() is None:
   if not pidfile.exists():
    ids=subprocess.run(['pgrep','-f','^'+str(repo/'target/release/fireweed-workload')],capture_output=True,text=True).stdout.split()
    if ids: pidfile.write_text(ids[0])
   time.sleep(1)
  print('runner_exit',runner.returncode,flush=True)
finally:
 stop.touch();monitor.wait(timeout=5)
for script,suffix,arg in [('/tmp/fireweed-campaign-summary.py','-summary.json',str(base)+'.json'),('/tmp/fireweed-device-summary-v3.py','-device-summary.json',str(base)+'-device.jsonl')]:
 with Path(str(base)+suffix).open('w') as output: subprocess.run(['python3',script,arg],stdout=output,check=True)
print('artifacts',base,'projection_root',root,flush=True)
