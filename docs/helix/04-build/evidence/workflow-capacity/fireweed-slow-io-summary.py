import collections,json,re,sys
from pathlib import Path
path=Path(sys.argv[1]);pid=int(sys.argv[2])
events=[];invalid=0
for line in path.read_text().splitlines():
 try: r=json.loads(line.removeprefix('fw_slow_io '))
 except json.JSONDecodeError: invalid+=1;continue
 if r['pid']!=pid:continue
 name=r['path'];r['class']=('wal' if name.endswith('projection.db-wal') else
     'main' if name.endswith('projection.db') else 'log' if '/log/' in name else 'other')
 m=re.search(r'/shard-(\d+)/',name);r['shard']=int(m.group(1)) if m else None
 r['end_s']=r['unix_end_us']/1e6;r['start_s']=r['end_s']-r['elapsed_us']/1e6
 events.append(r)
summary={}
for cls in sorted(set(e['class'] for e in events)):
 rows=[e for e in events if e['class']==cls]
 summary[cls]={'calls':len(rows),'summed_overlapping_wall_s':sum(e['elapsed_us'] for e in rows)/1e6,
  'max_call_s':max(e['elapsed_us'] for e in rows)/1e6,'requested_mib':sum(e['bytes'] for e in rows)/2**20,
  'errors':sum(e['result']<0 for e in rows),'ops':dict(collections.Counter(e['op'] for e in rows))}
projection=[e for e in events if e['class'] in ['main','wal']]
points=[]
for e in projection:
 if e['shard'] is not None:points.extend([(e['start_s'],e['shard'],1),(e['end_s'],e['shard'],-1)])
points.sort();active=collections.Counter();seconds=collections.Counter();max_stores=0;last=None
for t,shard,change in points:
 if last is not None:
  for threshold in [1,8,16,24,32,40,48]:
   if len(active)>=threshold:seconds[threshold]+=t-last
 active[shard]+=change
 if not active[shard]:del active[shard]
 max_stores=max(max_stores,len(active));last=t
log_points=[]
for e in events:
 if e['class']=='log' and e['shard'] is not None:
  log_points.extend([(e['start_s'],e['shard'],1),(e['end_s'],e['shard'],-1)])
log_points.sort();log_active=collections.Counter();log_seconds=collections.Counter();log_max=0;previous=None
for t,shard,change in log_points:
 if previous is not None:
  for threshold in [1,8,16,24,32,40,48]:
   if len(log_active)>=threshold:log_seconds[threshold]+=t-previous
 log_active[shard]+=change
 if not log_active[shard]:del log_active[shard]
 log_max=max(log_max,len(log_active));previous=t
out={'pid':pid,'events':len(events),'invalid_lines':invalid,
 'meaning':'completed calls >= threshold only; caller wall time, not device service time; sums overlap across threads; a store with one slow log sync may still have other puts in flight',
 'by_class':summary,'max_simultaneously_blocked_projection_stores':max_stores,
 'seconds_with_at_least_n_stores_in_long_projection_writes':dict(seconds),
 'max_stores_with_slow_log_sync_in_flight':log_max,
 'seconds_with_at_least_n_stores_having_a_slow_log_sync_in_flight':dict(log_seconds),
 'longest_calls':[{k:e[k] for k in ['op','class','shard','bytes','elapsed_us','unix_end_us','result']} for e in sorted(events,key=lambda e:e['elapsed_us'],reverse=True)[:12]]}
print(json.dumps(out,indent=2))
