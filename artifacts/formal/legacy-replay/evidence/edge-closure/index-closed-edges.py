from pathlib import Path
import gzip,hashlib,json,sys
root=Path('/tmp/fireweed-all-edge-worktree');source=Path('/tmp/fireweed-edge-closure-results')
sys.path.insert(0,str(root/'scripts/formal'))
from generate_replay import graph
raw=gzip.decompress((root/'docs/helix/04-build/evidence/all-edge-replay/report.json.gz').read_bytes());old=json.loads(raw)
new_raw=(source/'exhaustive.json').read_bytes();new=json.loads(new_raw)
assert old['execution']['model_sha256']==new['execution']['model_sha256']
universe={f'{a}|{b}|{c}' for a,b,c,_ in graph()[3]};missing=universe-set(old['execution']['matched_edges']);assert len(missing)==3918
assert set(new['execution']['matched_edges'])==universe
witnesses={}
for case in new['execution']['cases']:
 for step in case['steps']:
  for edge in step['matched_edges']:
   if edge in missing and edge not in witnesses:
    witnesses[edge]={'case_index':case['index'],'case_id':case['trace'],'base_trace':case['base_trace'],'schedule':case['schedule'],'step':step['step'],'action':step['action']}
assert set(witnesses)==missing
out={'schema':'fireweed-previously-unwitnessed-edge-index-v1','status':'PASS','previous_report_sha256':hashlib.sha256(raw).hexdigest(),'current_report_sha256':hashlib.sha256(new_raw).hexdigest(),'observations_sha256':new['observations_sha256'],'model_sha256':new['execution']['model_sha256'],'previous_unwitnessed':3918,'closed':len(witnesses),'remaining':0,'witnesses':witnesses}
(source/'closed-edge-witness-index.json').write_text(json.dumps(out,sort_keys=True,indent=2)+'\n');print('Indexed all3918 previously unwitnessed edges')
