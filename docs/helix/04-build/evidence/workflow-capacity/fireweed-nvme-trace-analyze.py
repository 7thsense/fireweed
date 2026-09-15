"""Analyze the extracted nvme-trace directory; no disk workload is run.

Usage: python3 fireweed-nvme-trace-analyze.py /path/to/nvme-trace
Latency includes driver/controller/completion handling, not just flash time.
"""
import json
import re
import statistics
import sys
from pathlib import Path

root = Path(sys.argv[1])
events = []
for line in (root / 'trace.txt').read_text().splitlines():
    match = re.search(r' (\d+\.\d+): (nvme_setup_cmd|nvme_complete_rq): nvme(\d+): .*?qid=(\d+), cmdid=(\d+), (.*)', line)
    assert match, line
    timestamp, event, controller, queue, command, body = match.groups()
    events.append((float(timestamp), event, (controller, queue, command), body))

pending = {}
pairs = []
for timestamp, event, key, body in sorted(events, key=lambda item: item[0]):
    if event == 'nvme_setup_cmd':
        assert key not in pending, ('duplicate setup', key)
        pending[key] = (timestamp, body)
    else:
        assert key in pending, ('unmatched completion', key)
        start, setup = pending.pop(key)
        assert 'status=0x0' in body and 'retries=0,' in body, body
        pairs.append((start, timestamp, setup))
assert not pending, pending
stats = json.loads((root / 'buffer-stats.json').read_text())
for cpu, value in stats.items():
    for field in ('entries', 'overrun', 'commit overrun', 'dropped events'):
        assert re.search(r'^' + field + r': 0$', value, re.M), (cpu, field)
assert sum(int(re.search(r'read events: (\d+)', value)[1]) for value in stats.values()) == len(events)

writes = [item for item in pairs if 'cmd=(nvme_cmd_write ' in item[2]]
latency = sorted((end - start) * 1000 for start, end, _ in writes)
union = end_of_union = 0
for start, end, _ in sorted(writes):
    union += max(0, end - max(start, end_of_union))
    end_of_union = max(end_of_union, end)
benchmark = json.loads((root / 'benchmark.json').read_text())
print(json.dumps({
    'events': len(events), 'pairs': len(pairs), 'write_commands': len(writes),
    'write_bytes': sum((int(re.search(r'len=(\d+)', item[2])[1]) + 1) * 512 for item in writes),
    'latency_ms': {'mean': statistics.mean(latency), 'p50': latency[len(latency)//2],
                   'p95': latency[int(len(latency)*.95)], 'p99': latency[int(len(latency)*.99)], 'max': max(latency)},
    'write_outstanding_union_s': union,
    'gib_chunks_mib_s': [1024 / sum(benchmark['chunk_write_s'][i:i+64]) for i in range(0, 512, 64)],
    'coverage': 'All events parsed and paired; no retries, status errors, unread entries, overruns or dropped events.'
}, indent=2))
