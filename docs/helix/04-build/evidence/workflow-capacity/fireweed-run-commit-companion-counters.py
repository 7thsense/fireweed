from pathlib import Path
import hashlib,json,os,re,subprocess,sys,time
repo=Path('/home/erik/Projects/fireweed')
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert subprocess.check_output(['git','log','-1','--format=%s'],cwd=repo,text=True).strip() == 'perf: briefly join in-flight successors into durable manifest commits'
assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo).strip()
base=repo/'target/workflow-capacity/commit-companion-counters-20260916'
base.mkdir(exist_ok=False)
# Reuse the exact checked-in recorder with only explicit repo/executable selection.
source=(repo/'scripts/perf/workflow-capacity.py').read_text()
source=source.replace('from workflow_storage_monitor import WalMonitor', "sys.path.insert(0, '/home/erik/Projects/fireweed/scripts/perf')\nfrom workflow_storage_monitor import WalMonitor")
source=source.replace('repo = Path(__file__).resolve().parents[2]', "repo = Path('/home/erik/Projects/fireweed')")
source=source.replace('binary = repo / "target/release/fireweed-workload"', 'binary = Path(os.environ["FIREWEED_DIAGNOSTIC_BINARY"])')
# Count only the workload process and its threads, not the Python recorder.
source=source.replace('command = [str(binary), *args]', 'workload_command = [str(binary), *args]\ncommand = ["perf", "stat", "-x,", "-o", os.environ["FIREWEED_PERF_COUNTER_OUTPUT"], "-e", "cycles:u,instructions:u", "--", *workload_command]')
source=source.replace('monitor = WalMonitor(projection_root, pid=child.pid)', '# perf is a wrapper: find its actual workload child for memory sampling.\n    monitored_pid = None\n    for _ in range(1000):\n        children_path = Path(f"/proc/{child.pid}/task/{child.pid}/children")\n        try:\n            children = children_path.read_text().split()\n        except FileNotFoundError:\n            children = []\n        if children:\n            assert len(children) == 1, children\n            monitored_pid = int(children[0])\n            break\n        time.sleep(0.001)\n    assert monitored_pid is not None, "perf did not start workload"\n    monitor = WalMonitor(projection_root, pid=monitored_pid)')
recorder=Path('/tmp/fireweed-commit-companion-counter-recorder.py');recorder.write_text(source)
binaries=[repo/'target/release/fireweed-workload',Path('/tmp/fireweed-workload-before-commit-companion')]
def digest(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
hashes=[digest(p) for p in binaries]
assert hashes[1]=='83e48da6ebda99cc9351f43808a779a6aae5ec23995068ad54977db0af080147'
assert hashes[0]!=hashes[1]
args=['--profile','campaign','--campaign-metadata','--campaign-timestamp-priority','--items','1000000','--batch','1000','--purge-batch','8000','--shards','64','--workers','2','--load-workers','2','--recycle','--cycles','1','--deadline-seconds','900']
records=[]
for name,index,origin in [('candidate-1',0,head),('control',1,'78d36c7cf51838c16a94161c7dcb1276ff5ba2fa'),('candidate-2',0,head)]:
 binary=binaries[index]
 for b in binaries:
  assert not subprocess.run(['pgrep','-f','^'+re.escape(str(b))+r'( |$)'],capture_output=True).stdout.strip(), 'Other workload active'
 provenance={'diagnostic_only':True,'build_source_head':origin,'executable_sha256':hashes[index],'recorder_source_head':head,'recorder_sha256':digest(recorder),'purpose':'one-cycle commit-companion hardware-counter and projection-write comparison; perf counts workload only; not sustained qualification'}
 prov=base/(name+'-provenance.json');prov.write_text(json.dumps(provenance,indent=2)+'\n')
 output=base/(name+'.json')
 env=dict(os.environ,FIREWEED_DIAGNOSTIC_BINARY=str(binary),FIREWEED_PERF_COUNTER_OUTPUT=str(base/(name+'-perf.csv')))
 for key in ['FIREWEED_PROJECTION_IO_TRACE','FIREWEED_LOG_TRACE','FIREWEED_APPLY_TRACE','OBJECT_LOG_LOCAL_PUBLISH_TRACE','LD_PRELOAD']:
  assert key not in env
 env['FIREWEED_PROJECTION_IO_TRACE']='1'
 with output.open('w') as f:
  runner=subprocess.Popen(['python3',str(recorder),*args,'--diagnostic-provenance',str(prov)],cwd=repo,env=env,stdout=f)
  prefix=base/(name+'-device');stop=Path(str(prefix)+'.stop');pidfile=Path(str(prefix)+'.pid');monitor=None
  while runner.poll() is None:
   ids=subprocess.run(['pgrep','-f','^'+re.escape(str(binary))+r'( |$)'],capture_output=True,text=True).stdout.split()
   assert len(ids)<=1
   if ids and monitor is None:
    pidfile.write_text(ids[0]); monitor=subprocess.Popen(['python3','/tmp/fireweed-device-monitor-v3.py',str(prefix)+'.jsonl',str(stop),str(pidfile)])
    print(name,'pid',ids[0],flush=True)
   time.sleep(1)
  if monitor is not None:
   stop.touch();monitor.wait(timeout=5)
   with Path(str(prefix)+'-summary.json').open('w') as summary:
    subprocess.run(['python3','/tmp/fireweed-device-summary-v3.py',str(prefix)+'.jsonl'],stdout=summary,check=True)
 d=json.loads(output.read_text());assert runner.returncode==0 and d['exit_code']==0,d.get('stderr')
 records.append({'run':name,'rate':d['result']['completed_lifecycles_per_s'],'cpu_ms_per_recipient':(d['user_cpu_s']+d['system_cpu_s'])*1000/1_000_000,'rss_gib':d['max_rss_kib']/2**20,'binary_sha256':d['binary_sha256'],'build_source_head':origin})
 print(json.dumps(records[-1]),flush=True)
(base/'summary.json').write_text(json.dumps(records,indent=2)+'\n')
