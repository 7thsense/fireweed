# Within five percent of 10,000 complete workflows/sec

The 2026-09-10 follow-up target is at least **9,500 complete workflows/sec**.
The acceptance gate now requires that rate overall and in every physical shard's
fair share in every cycle. Primitive insertion and both individually addressed
update gates remain at 10,000/sec. Durability, correctness, fairness, recovery,
retention, and sampled storage bounds are unchanged. The previous 7.9–8.0k
qualification is a baseline, not a pass against this new target.

## Baseline CPU profile

An isolated 500,000-recipient, three-cycle, 16-shard run on source `8a580e83`
produced 337,538 user-space instruction-pointer samples across all 16 logical
CPUs, with zero reported lost samples. The software CPU-clock sampling rate was
199 Hz per CPU. The instrumented run completed at 7,746 lifecycles/sec; it is
diagnostic evidence only. Full qualification still needs six-cycle repeats
without instrumentation.

Approximately one quarter of samples fall in glibc allocation/freeing paths:
`_int_malloc` alone accounts for 8.15%, `__libc_free` 4.59%, `unlink_chunk`
4.14%, and `_int_free_create_chunk` 2.65%, plus smaller malloc/free paths.
The two leading memcpy implementations account for 8.89%. SQL interpreter,
column decoding, comparisons, and B-tree seeks are other distributed costs.
The remaining commit cache recount is not a leading standalone sampled symbol;
inlining and the absence of stacks prevent assigning its exact inclusive cost.

This prioritizes an executable allocator experiment before changing native
cache semantics. The service and workload binaries can select mimalloc while
the library continues to leave allocator policy to its embedding application.
The first trial preserves 16 shards and all workload parameters to isolate this
change. No improvement or target pass is claimed until measured.

Evidence:

- [Provenance](../helix/04-build/evidence/workflow-capacity/fireweed-9500-baseline-profile-provenance.json)
- [Workload result](../helix/04-build/evidence/workflow-capacity/fireweed-9500-baseline-profile.json.gz)
- [Sample summary](../helix/04-build/evidence/workflow-capacity/fireweed-9500-baseline-summary.txt.gz)
- [Sampler source](../helix/04-build/evidence/workflow-capacity/fireweed-sample-all.c.gz)
- [Symbolizer source](../helix/04-build/evidence/workflow-capacity/fireweed-profile-summary.py.gz)

Raw samples, memory mappings, and cycle stderr are retained beside those files.
Instruction-pointer sampling is statistical self-time evidence, not an exact
allocation count or attribution of allocations to callers. The libc labels were
resolved from local disassembly rather than misleading nearest exported symbols.

## First allocator trial: useful improvement, target still unmet

The uninstrumented 16-shard, six-cycle trial on `969dda9c` completed all three
million lifecycles at **9,085/sec**. CPU cost fell to **1.042 CPU-ms/lifecycle**
from the earlier uninstrumented 1.425–1.437 ms, approximately a 27% reduction.
Process-accounted output remained approximately 47.03 GiB. Correctness, RSS,
projection size, and sampled WAL checks passed. The new rate gate failed:
overall throughput was below 9,500/sec, and cycles 1–5 had slowest-shard
equivalent rates of 8,076, 8,070, 7,706, 7,271, and 7,670/sec.

This demonstrates an allocator improvement without establishing steady target
capacity. The next trial changes physical shards from 16 to 32, retaining eight
workers and four loaders per shard and the same workload, batching, and gates.
It tests smaller per-shard working sets and write coalescing on the shared SSD.

[Complete first trial report](../helix/04-build/evidence/workflow-capacity/fireweed-9500-mimalloc-16-c6-a.json.gz).
