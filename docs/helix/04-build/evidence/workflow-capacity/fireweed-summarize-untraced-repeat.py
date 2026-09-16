import json,sys
from pathlib import Path
sys.path.insert(0,'scripts/perf')
from workflow_capacity_gate import qualify
base=Path(sys.argv[1]);out={}
for name in ['campaign-1','primitives-1','campaign-2','primitives-2']:
 p=base/(name+'.json')
 if not p.exists() or not p.stat().st_size:continue
 d=json.loads(p.read_text());r=d.get('result') or {}
 entry={'head':d['head'],'binary_sha256':d['binary_sha256'],'dirty':d['dirty'],'diagnostics':d.get('diagnostics'),'exit_code':d['exit_code'],'process_wall_s':d['process_wall_s'],'cpu_s':d['user_cpu_s']+d['system_cpu_s'],'rss_gib':d['max_rss_kib']/2**20,'overall_rate':r.get('completed_lifecycles_per_s'),'phases':r.get('aggregate_phases'),'command':d['command'],'targets':{}}
 for target in [10000,12500]:
  q=qualify(d,campaign_target=target)
  entry['targets'][target]={'passed':q['passed'],'failed_checks':[c for c in q['checks'] if not c['passed']],'cycle_rates':[c['actual'] for c in q['checks'] if c['name'].endswith('_slowest_campaign_equivalent_rate')]}
 out[name]=entry
print(json.dumps({'reports':out,'complete':len(out)==4,'same_binary':len({x['binary_sha256'] for x in out.values()})==1,'same_head':len({x['head'] for x in out.values()})==1,'all_stretch_pass':len(out)==4 and all(x['targets'][12500]['passed'] for x in out.values())},indent=2))
