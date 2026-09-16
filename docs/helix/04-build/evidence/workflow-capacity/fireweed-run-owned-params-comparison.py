from pathlib import Path
import hashlib,json,os,re,subprocess,sys,time
repo=Path('/home/erik/Projects/fireweed')
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head.startswith('fa49f380')
assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo).strip()
base=repo/'target/workflow-capacity/owned-params-comparison-20260916'
base.mkdir(exist_ok=False)
# Reuse the exact checked-in recorder with only explicit repo/executable selection.
source=(repo/'scripts/perf/workflow-capacity.py').read_text()
source=source.replace('from workflow_storage_monitor import WalMonitor', "sys.path.insert(0, '/home/erik/Projects/fireweed/scripts/perf')\nfrom workflow_storage_monitor import WalMonitor")
source=source.replace('repo = Path(__file__).resolve().parents[2]', "repo = Path('/home/erik/Projects/fireweed')")
source=source.replace('binary = repo / "target/release/fireweed-workload"', 'binary = Path(os.environ["FIREWEED_DIAGNOSTIC_BINARY"])')
recorder=Path('/tmp/fireweed-owned-params-capacity-recorder.py');recorder.write_text(source)
binaries=[repo/'target/release/fireweed-workload',Path('/tmp/fireweed-workload-before-owned-params')]
def digest(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
hashes=[digest(p) for p in binaries]
assert hashes[1]=='dcaf90ac65ca719923bc000fd55b61eda531662729fde27a9f8b25b77e5eb525'
assert hashes[0]!=hashes[1]
args=['--profile','campaign','--campaign-metadata','--campaign-timestamp-priority','--items','1000000','--batch','1000','--purge-batch','8000','--shards','64','--workers','2','--load-workers','2','--recycle','--cycles','1','--deadline-seconds','900']
records=[]
for name,index,origin in [('candidate-1',0,head),('control',1,'64eb686f03ece8df75c3452be5c1a0b4d3cca066'),('candidate-2',0,head)]:
 binary=binaries[index]
 for b in binaries:
  assert not subprocess.run(['pgrep','-f','^'+re.escape(str(b))+r'( |$)'],capture_output=True).stdout.strip(), 'Other workload active'
 provenance={'diagnostic_only':True,'build_source_head':origin,'executable_sha256':hashes[index],'recorder_source_head':head,'recorder_sha256':digest(recorder),'purpose':'untraced one-cycle owned-parameter CPU-cost comparison; not sustained qualification'}
 prov=base/(name+'-provenance.json');prov.write_text(json.dumps(provenance,indent=2)+'\n')
 output=base/(name+'.json')
 env=dict(os.environ,FIREWEED_DIAGNOSTIC_BINARY=str(binary))
 for key in ['FIREWEED_PROJECTION_IO_TRACE','FIREWEED_LOG_TRACE','FIREWEED_APPLY_TRACE','LD_PRELOAD']:
  assert key not in env
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
