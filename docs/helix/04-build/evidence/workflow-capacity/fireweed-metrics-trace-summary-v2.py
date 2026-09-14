import json,re,sys,collections
from pathlib import Path
report=json.loads(Path(sys.argv[1]).read_text())
# Search all string fields: runner archives stderr alongside parsed workload JSON.
strings=[]
def visit(v):
 if isinstance(v,str):strings.append(v)
 elif isinstance(v,dict):
  for x in v.values():visit(x)
 elif isinstance(v,list):
  for x in v:visit(x)
visit(report)
rows=[]
for s in strings:
 for m in re.finditer(r'metrics_read path=(\w+) phases_us=\[([\d, ]+)\]',s):
  values=list(map(int,m[2].split(',')))
  rows.append((m[1],values+[0]*(9-len(values))))
names=['high_water','first_admission','snapshot_sql','claim_tail','coverage','second_admission','final_sql','membership_tail','membership_sql']
def summary(rows):
 if not rows:return {'count':0}
 totals=sorted(sum(r[1]) for r in rows)
 def pct(a,p):return sorted(a)[min(len(a)-1,int((len(a)-1)*p))]/1000
 return {'count':len(rows),'total_ms_percentiles':{str(p):pct(totals,p) for p in [.5,.9,.95,.99,1]},
 'phase_sum_s':{n:sum(r[1][i] for r in rows)/1e6 for i,n in enumerate(names)},
 'phase_p95_ms':{n:pct([r[1][i] for r in rows],.95) for i,n in enumerate(names)},
 'dominant_phase_counts':dict(collections.Counter(names[max(range(9),key=lambda i:r[1][i])] for r in rows))}
print(json.dumps({'all':summary(rows),'by_path':{p:summary([r for r in rows if r[0]==p]) for p in sorted(set(r[0] for r in rows))},'over_1_second':summary([r for r in rows if sum(r[1])>1e6])},indent=2))
