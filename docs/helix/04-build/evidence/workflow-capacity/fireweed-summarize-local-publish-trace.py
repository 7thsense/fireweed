from pathlib import Path
import collections,json,math,re,sys
report=json.loads(Path(sys.argv[1]).read_text())
samples=collections.defaultdict(list);sums=collections.Counter();counts=collections.Counter();ends=collections.Counter();cycle=0;cycles={};io={}
def add(group,key,value):
 for scope in ('overall',f'cycle_{cycle}' if cycle<8 else 'after_cycles'):
  samples[(scope,group,key)].append(value)
def total(group,key,value):
 for scope in ('overall',f'cycle_{cycle}' if cycle<8 else 'after_cycles'):
  sums[(scope,group,key)]+=value
for line in report.get('stderr','').splitlines():
 if line.startswith('campaign_cycle_complete '):
  d=json.loads(line[line.index('{'):]);c=d['cycle'];ends[c]+=1;cycles[c]=max(cycles.get(c,0),d['wall_s'])
  if ends[c]==128:cycle=c+1
  continue
 if line.startswith('projection_io '):
  values=dict(re.findall(r'(\w+)=(\S+)',line));group=values.pop('class');values.pop('store',None);b=io.setdefault(group,{'handles':0});b['handles']+=1
  for k,v in values.items():b[k]=max(b.get(k,0),int(v)) if k=='max_us' else b.get(k,0)+int(v)
  continue
 if line.startswith('local_publish '):group='local_publish'
 elif line.startswith('log_blob '):group='log_blob_'+dict(re.findall(r'(\w+)=(\S+)',line))['class']
 elif line.startswith('log_pre_us='):group='log_produce'
 elif line.startswith('apply cmds='):group='projection_apply'
 elif line.startswith('apply_join '):group='projection_join'
 else:continue
 values=dict(re.findall(r'(\w+)=(\S+)',line));counts[('overall',group)]+=1;counts[(f'cycle_{cycle}' if cycle<8 else 'after_cycles',group)]+=1
 for k,v in values.items():
  if v.isdigit():
   if k=='started_unix_us':continue
   if k.endswith('_us') or k=='us':add(group,k,int(v))
   else:total(group,k,int(v))
  elif k=='ok' and v!='true':total(group,'errors',1)
output={'head':report['head'],'binary_sha256':report['binary_sha256'],'exit_code':report['exit_code'],'wall_s':report['process_wall_s'],'rate':report.get('result',{}).get('completed_lifecycles_per_s'),'cpu_s':report['user_cpu_s']+report['system_cpu_s'],'rss_gib':report['max_rss_kib']/2**20,'cycle_max_wall_s':cycles,'cycle_reports':dict(ends),'projection_vfs':io,'groups':{},'caveats':['Timings overlap across shards and cannot be summed into wall or CPU time.','Cycle buckets follow the 128 completion reports before the next global cycle barrier. Cycle zero includes startup traces.','Projection VFS totals are emitted at handle drop and are only attributed overall.','Instrumented diagnostic, not qualification.']}
for (scope,group),count in counts.items():
 entry={'calls':count,'totals':{},'latency_us':{}}
 for (s,g,k),v in sums.items():
  if (s,g)==(scope,group):entry['totals'][k]=v
 for (s,g,k),values in samples.items():
  if (s,g)!=(scope,group):continue
  values.sort();n=len(values)
  entry['latency_us'][k]={'sum':sum(values),'mean':sum(values)/n,'p50':values[math.ceil(n*.50)-1],'p95':values[math.ceil(n*.95)-1],'p99':values[math.ceil(n*.99)-1],'max':values[-1],'over_100ms':sum(v>=100000 for v in values)}
 output['groups'].setdefault(scope,{})[group]=entry
print(json.dumps(output,indent=2))
