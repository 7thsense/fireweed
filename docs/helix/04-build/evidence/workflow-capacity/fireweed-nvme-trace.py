import os,json,subprocess,tempfile,time,threading,re,statistics
from pathlib import Path
assert os.geteuid()==0, 'Run through sudo in the authenticated terminal'
repo=Path('/home/erik/Projects/fireweed')
assert subprocess.run(['pgrep','-x','dd'],capture_output=True).returncode==1, 'Another dd is running'
assert subprocess.run(['pgrep','-f','^'+str(repo/'target/release/fireweed-workload')],capture_output=True).returncode==1, 'Campaign is running'
out=Path(tempfile.mkdtemp(prefix='fireweed-nvme-trace-',dir='/tmp'))
instance=Path('/sys/kernel/tracing/instances')/('fireweed-'+str(os.getpid()))
stop=threading.Event();reader=None;fd=None
try:
 instance.mkdir();(instance/'tracing_on').write_text('0');(instance/'trace_clock').write_text('mono');(instance/'buffer_size_kb').write_text('1024')
 for name in ['nvme_setup_cmd','nvme_complete_rq']:
  event=instance/'events/nvme'/name
  (out/(name+'-format.txt')).write_text((event/'format').read_text())
  (event/'filter').write_text('ctrl_id == 0 && qid > 0')
  (event/'enable').write_text('1')
 (out/'smart-before.json').write_bytes(subprocess.check_output(['nvme','smart-log','/dev/nvme0','-o','json']))
 fd=os.open(instance/'trace_pipe',os.O_RDONLY|os.O_NONBLOCK)
 def consume():
  with (out/'trace.txt').open('wb') as f:
   while True:
    try:
     data=os.read(fd,1024*1024)
     if data:f.write(data);continue
    except BlockingIOError:pass
    if stop.is_set():break
    stop.wait(.02)
 reader=threading.Thread(target=consume);reader.start();(instance/'tracing_on').write_text('1')
 print('NVMe trace started: '+str(out),flush=True)
 start=time.monotonic()
 with (out/'benchmark.json').open('w') as f:
  rc=subprocess.run(['runuser','-u',os.environ['SUDO_USER'],'--','python3',str(repo/'docs/helix/04-build/evidence/workflow-capacity/fireweed-repeat-original-sequential.py')],cwd=repo,stdout=f).returncode
 end=time.monotonic();(instance/'tracing_on').write_text('0');stop.set();reader.join();reader=None
 (out/'buffer-stats.json').write_text(json.dumps({str(p.relative_to(instance)):p.read_text() for p in instance.glob('per_cpu/cpu*/stats')},indent=2))
 (out/'smart-after.json').write_bytes(subprocess.check_output(['nvme','smart-log','/dev/nvme0','-o','json']))
 (out/'run.json').write_text(json.dumps({'kernel':os.uname().release,'benchmark_exit':rc,'start_monotonic':start,'end_monotonic':end,'note':'Diagnostic only. Setup-to-completion includes driver/controller/device latency and completion handling; not exclusive physical media time. Captures all controller 0 I/O queues, including background I/O.'},indent=2))
 assert rc==0,'Benchmark failed; raw trace retained'
 print('NVMe trace complete: '+str(out),flush=True)
finally:
 if instance.exists():
  (instance/'tracing_on').write_text('0');stop.set()
  if reader:reader.join(timeout=5)
  if fd is not None:os.close(fd)
  for name in ['nvme_setup_cmd','nvme_complete_rq']:
   p=instance/'events/nvme'/name/'enable'
   if p.exists():p.write_text('0')
  instance.rmdir()
 for p in out.iterdir():p.chmod(0o644)
 out.chmod(0o755)
 Path('/tmp/fireweed-nvme-trace-latest.txt').write_text(str(out)+'\n')
 Path('/tmp/fireweed-nvme-trace-latest.txt').chmod(0o644)
