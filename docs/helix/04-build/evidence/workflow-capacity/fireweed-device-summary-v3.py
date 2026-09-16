import json,sys,os
from pathlib import Path
rows=[json.loads(line) for line in Path(sys.argv[1]).read_text().splitlines()]
active=[r for r in rows if 'process_cpu_s' in r]
assert len(active)>1
first,last=active[0],active[-1]
elapsed=last['monotonic_s']-first['monotonic_s']
disk=[b-a for a,b in zip(first['nvme0n1'],last['nvme0n1'])]
result={'sampled_active_s':elapsed,'host_wide_not_process_attribution':True,
 'host_written_gib':disk[6]*512/2**30,'host_write_mib_s':disk[6]*512/2**20/elapsed,
 'host_read_gib':disk[2]*512/2**30,'device_busy_percent':disk[9]/(elapsed*10),
 'mean_write_request_ms':disk[7]/disk[4] if disk[4] else None,
 'process_cpu_s':last['process_cpu_s']-first['process_cpu_s'],
 'startup_tail_omitted':True}
if 'host_cpu_ticks' in first:
 names=['user','nice','system','idle','iowait','irq','softirq','steal']
 d=[b-a for a,b in zip(first['host_cpu_ticks'],last['host_cpu_ticks'])]
 result['host_cpu_s_per_wall_s']={name:d[i]/os.sysconf('SC_CLK_TCK')/elapsed for i,name in enumerate(names)}
 result['host_busy_cpu_s_per_wall_s']=sum(d[i] for i in [0,1,2,5,6,7])/os.sysconf('SC_CLK_TCK')/elapsed
 def pressure(r):
  return {(kind,line.split()[0]):int(line.split('total=')[1]) for kind,value in r['pressure'].items() for line in value.splitlines()}
 a,b=pressure(first),pressure(last)
 result['host_pressure_percent']={kind+'_'+scope:100*(b[(kind,scope)]-v)/1e6/elapsed for (kind,scope),v in a.items()}
print(json.dumps(result,indent=2))
