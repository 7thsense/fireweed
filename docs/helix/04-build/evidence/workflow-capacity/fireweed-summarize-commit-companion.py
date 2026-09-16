import csv,json,re,sys
from pathlib import Path
base=Path(sys.argv[1]);out=[]
for name in ['candidate-1','control','candidate-2']:
 d=json.loads((base/(name+'.json')).read_text());r=d['result'];io={}
 for line in d['stderr'].splitlines():
  if not line.startswith('projection_io '):continue
  x=dict(re.findall(r'(\w+)=(\S+)',line));g=io.setdefault(x.pop('class'),{'handles':0});g['handles']+=1
  for k,v in x.items():g[k]=max(g.get(k,0),int(v)) if k=='max_us' else g.get(k,0)+int(v)
 counters={}
 for row in csv.reader((base/(name+'-perf.csv')).read_text().splitlines()):
  if len(row)>4 and row[2] in ['cycles:u','instructions:u']:
   counters[row[2]]={'count':int(row[0]),'running_percent':float(row[4])}
 assert len(counters)==2
 objects=sum(v.get('log_data_objects',0) for v in d['storage'].values());manifests=sum(v.get('log_manifests',0) for v in d['storage'].values())
 out.append({'run':name,'head':d['head'],'binary_sha256':d['binary_sha256'],'build_provenance':d['diagnostic_provenance'],'rate':r['completed_lifecycles_per_s'],'cpu_ms_per_recipient':(d['user_cpu_s']+d['system_cpu_s'])/1000,'rss_gib':d['max_rss_kib']/2**20,'objects':objects,'manifests':manifests,'manifests_per_object':manifests/objects,'data_manifest_sync_calls':2*(objects+manifests),'counters':counters,'projection_vfs':io,'progress_reads':sum(c['progress_reads'] for c in r['campaigns']) if isinstance(r.get('campaigns'),list) and r['campaigns'] and 'progress_reads' in r['campaigns'][0] else None})
print(json.dumps(out,indent=2))
