import json,sys,time,os
from pathlib import Path
# Host-wide device counters, not attribution to an individual process.
output=Path(sys.argv[1]);stop=Path(sys.argv[2]);pidfile=Path(sys.argv[3])
freqs=list(Path('/sys/devices/system/cpu').glob('cpu*/cpufreq/scaling_cur_freq'))
temps=list(Path('/sys/class/hwmon').glob('hwmon*/temp*_input'))
with output.open('w') as f:
 while not stop.exists():
  sample={'monotonic_s':time.monotonic(),'wall_time_s':time.time()}
  sample['host_cpu_ticks'] = [int(v) for v in Path('/proc/stat').read_text().splitlines()[0].split()[1:]]
  sample['pressure'] = {kind: Path('/proc/pressure/'+kind).read_text().strip() for kind in ['cpu','io','memory']}

  for line in Path('/proc/diskstats').read_text().splitlines():
   parts=line.split()
   if parts[2]=='nvme0n1':sample['nvme0n1']=list(map(int,parts[3:]))
  try:
   sample['cpu_mean_khz']=sum(int(p.read_text()) for p in freqs)/len(freqs) if freqs else None
   sample['temperatures_millic']={str(p):int(p.read_text()) for p in temps}
  except (OSError,ValueError):pass
  if pidfile.exists():
   pid=pidfile.read_text().strip()
   try:
    stat=Path('/proc/'+pid+'/stat').read_text(); fields=stat[stat.rfind(')')+2:].split()
    sample['process_cpu_s']=(int(fields[11])+int(fields[12]))/os.sysconf('SC_CLK_TCK')
    sample['process_io']={k:int(v.strip()) for k,v in (line.split(':',1) for line in Path('/proc/'+pid+'/io').read_text().splitlines())}
   except FileNotFoundError:pass
  f.write(json.dumps(sample)+'\n');f.flush();time.sleep(1)
