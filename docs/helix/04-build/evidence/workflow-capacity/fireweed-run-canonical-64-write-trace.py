from pathlib import Path
import json,os,subprocess,sys,time
repo=Path('/home/erik/Projects/fireweed')
output=repo/'target/workflow-capacity/canonical-64-write-trace-20260916.json'
assert not output.exists(), 'Refuse to overwrite qualification evidence'
assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo).strip()
assert not subprocess.run(['pgrep','-f','^'+str(repo/'target/release/fireweed-workload')],capture_output=True).stdout.strip()
os.environ['FIREWEED_PROJECTION_IO_TRACE']='1'
log=output.open('w')
runner=subprocess.Popen(['python3','scripts/perf/workflow-capacity.py','--profile','campaign','--campaign-metadata','--campaign-timestamp-priority','--items','1000000','--batch','1000','--purge-batch','8000','--shards','64','--workers','2','--load-workers','2','--recycle','--cycles','8','--deadline-seconds','1800','--stretch'],cwd=repo,stdout=log,stderr=subprocess.STDOUT)
active=None
phases=[]
def finish_active():
 global active
 if active is None:return
 active['stop'].touch()
 active['monitor'].wait(timeout=5)
 base=active['base']
 with Path(str(base)+'-device-summary.json').open('w') as f:
  subprocess.run(['python3','/tmp/fireweed-device-summary-v3.py',str(base)+'-device.jsonl'],stdout=f,check=True)
 active=None
try:
 while runner.poll() is None:
  ids=subprocess.run(['pgrep','-f','^'+str(repo/'target/release/fireweed-workload')],capture_output=True,text=True).stdout.split()
  assert len(ids)<=1, 'Qualification workloads overlapped'
  pid=ids[0] if ids else None
  if active is not None and active['pid']!=pid:finish_active()
  if pid is not None and active is None:
   base=Path(f'/tmp/fireweed-canonical-64-write-trace-phase-{len(phases)+1}')
   stop=Path(str(base)+'-device.stop');stop.unlink(missing_ok=True)
   pidfile=Path(str(base)+'.pid');pidfile.write_text(pid)
   try:command=Path('/proc/'+pid+'/cmdline').read_bytes().decode().split('\0')
   except FileNotFoundError:command=[]
   phase={'pid':pid,'command':command,'base':str(base),'observed_start_monotonic_s':time.monotonic()}
   phases.append(phase)
   monitor=subprocess.Popen(['python3','/tmp/fireweed-device-monitor-v3.py',str(base)+'-device.jsonl',str(stop),str(pidfile)])
   active={'pid':pid,'base':base,'stop':stop,'monitor':monitor}
   print('observed workload phase',len(phases),'pid',pid,flush=True)
  time.sleep(1)
finally:
 finish_active()
 log.close()
 Path('/tmp/fireweed-canonical-64-write-trace-phases.json').write_text(json.dumps({'runner_exit':runner.poll(),'output':str(output),'phases':phases,'host_counters_are_not_process_attribution':True,'initial_and_final_observation_gaps_possible':True},indent=2)+'\n')
print('qualification_exit',runner.returncode,'output',output,flush=True)
sys.exit(runner.returncode)
