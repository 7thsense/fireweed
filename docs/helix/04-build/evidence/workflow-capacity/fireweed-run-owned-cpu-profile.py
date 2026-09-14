import hashlib,json,os,subprocess,sys,tempfile,time
from pathlib import Path
repo=Path('/home/erik/Projects/fireweed')
assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo).strip()
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
base=Path('/tmp/fireweed-current-cpu-'+head[:8])
root=Path(tempfile.mkdtemp(prefix='cpu-profile-'+head[:8]+'-',dir=repo/'target/workflow-capacity'))
projection=root/'projections';projection.mkdir()
subprocess.run(['btrfs','property','set',str(projection),'compression','zstd'],check=True)
mount=json.loads(subprocess.check_output(['findmnt','--json','--target',str(root)]))
assert mount['filesystems'][0]['fstype']=='btrfs'
binary=repo/'target/release/fireweed-workload'
command=[str(binary),'--profile','campaign','--campaign-metadata','--recycle','--cycles','1','--items','1000000','--batch','1000','--shards','64','--workers','2','--load-workers','2','--deadline-seconds','1800','--projection-root',str(projection),'--campaign-timestamp-priority','--root',str(root/'data')]
provenance={'diagnostic':True,'qualification':False,'purpose':'current 64-store/two-worker CPU attribution after lifecycle metrics and owned claim decoding','head':head,'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'command':command,'root':str(root),'mount':mount,'projection_compression':subprocess.check_output(['btrfs','property','get',str(projection),'compression'],text=True),'sample_frequency_hz':199,'environment':{'OBJECT_LOG_FLUSH_RUNTIME_THREADS':os.environ.get('OBJECT_LOG_FLUSH_RUNTIME_THREADS')}}
Path(str(base)+'-provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
with Path(str(base)+'.symbols').open('w') as out:subprocess.run(['nm','-n','-C',str(binary)],stdout=out,check=True)
sys.path.insert(0,str(repo/'scripts/perf'))
from workflow_storage_monitor import WalMonitor
monitor=WalMonitor(projection)
with Path(str(base)+'.json').open('w') as out,Path(str(base)+'.stderr').open('w') as err:
    start=time.monotonic()
    child=subprocess.Popen(['/tmp/fireweed-sample-all',str(base),*command],stdout=out,stderr=err,cwd=repo)
    print('sampler_pid',child.pid,'prefix',base,flush=True)
    monitor.start()
    try:rc=child.wait()
    finally:observation=monitor.finish()
Path(str(base)+'-observation.json').write_text(json.dumps({'exit_code':rc,'wall_s':time.monotonic()-start,'wal_observation':observation},indent=2)+'\n')
print('exit',rc,'root_requires_owned_cleanup',root,flush=True)
raise SystemExit(rc)
