# Within five percent of 10,000 complete workflows/sec

> 2026-09-11: These are historical results for the lighter saturation workload.
> The mixed-priority campaign stress case currently measures 10,850 recipients/sec but has not qualified against its 10k/12.5k targets; see the
> [campaign plan, updated math and measurements](campaign-qualification-plan.md).

The 2026-09-10 follow-up target is at least **9,500 complete workflows/sec**.
The acceptance gate now requires that rate overall and in every physical shard's
fair share in every cycle. Primitive insertion and both individually addressed
update gates remain at 10,000/sec. Durability, correctness, fairness, recovery,
retention, and sampled storage bounds are unchanged. The previous 7.9–8.0k
qualification is a baseline, not a pass against this new target.

## Qualified result

**Achieved on clean source `a73b067f`: all four fresh qualification reports
pass.** Two serial workflow runs each recycled 500,000 original rows through
six cycles (three million complete lifecycles per run). Two serial primitive
runs each operated on one million resident rows. All four used binary SHA-256
`0dd7e7c584b4ea3a744b01ea65dddf26717ea208b5c39899499b1afc4169a25a`.

| Public API measurement | Repetition 1 | Repetition 2 | Gate |
|---|---:|---:|---:|
| Complete workflows/sec | 12,125 | 11,810 | 9,500 |
| Slowest shard's equivalent rate, worst cycle | 9,839 | 10,036 | 9,500 |
| Inserts/sec | 67,738 | 67,311 | 10,000 |
| Enrichment updates by key/sec | 97,186 | 77,121 | 10,000 |
| Scheduling updates by ID/sec | 70,739 | 61,756 | 10,000 |
| Maximum sampled shard WAL | 259.0 MiB | 259.6 MiB | 512 MiB |
| Peak process RSS | 8.67 GiB | 8.52 GiB | Last-three-cycle range ≤10% |

Exact outcomes, retries, original-row payloads, purge, per-cycle fairness,
last-three-cycle projection/RSS stability, and WAL sampling all passed. This
is finite-run qualification on the local filesystem log and Turso, not a
multi-day soak or a claim about remote-log/network service latency. Payloads
are deterministic, compressible 1 KiB bodies. Row operations are individually
addressed within bounded API batches; this does not promise the same rate for
one separately durable request outstanding at a time.

The retained configuration is 32 physical log/projection shards on the same
SSD, eight workers per shard, four loaders, batch 1,000, purge batch 8,000,
4 KiB projection pages, and the unchanged 64,000-frame checkpoint budget.
Executable-owned mimalloc, portable thin LTO with one codegen unit, and the
native partial-checkpoint retry correction provide the additional improvement.
The original row remains the workflow entity and the log remains the sole
durability authority. No side workflow records or Snorri interface migration
were introduced. Embedders choose their own allocator and root Cargo profile.

Validation: **106 public/adapter release tests passed, one existing ignored**;
**77 native WAL tests passed**; **four Python gate/monitor tests passed**.
The new partial-backfill regression failed before the fix and passed afterward.
The failed candidates below remain part of the evidence rather than being
replaced by successful runs.

Reproduce from the qualified source with a new output directory:

```sh
bash scripts/perf/qualify-workflow-capacity.sh /tmp/fireweed-capacity-repeat
```

- [Workflow 1](../helix/04-build/evidence/workflow-capacity/fireweed-9500-a73b067f-workflow-1.json.gz)
- [Workflow 2](../helix/04-build/evidence/workflow-capacity/fireweed-9500-a73b067f-workflow-2.json.gz)
- [Primitives 1](../helix/04-build/evidence/workflow-capacity/fireweed-9500-a73b067f-primitives-1.json.gz)
- [Primitives 2](../helix/04-build/evidence/workflow-capacity/fireweed-9500-a73b067f-primitives-2.json.gz)
- [Qualification log](../helix/04-build/evidence/workflow-capacity/fireweed-9500-a73b067f-qualification.log.gz)

The sections below record the chronological investigation, including hypotheses
that later measurements rejected. The current hardware comparison is in
[hardware headroom](workflow-hardware-headroom.md).

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

## Thirty-two shards: first complete pass

The otherwise unchanged 32-shard trial completed three million lifecycles at
**11,311/sec**, passing every throughput, correctness, and storage check. Its
slowest-shard equivalent cycle rates were 11,047, 9,916, 10,300, 9,881, 9,867,
and **9,561/sec**. That narrow final-cycle margin requires fresh repetitions.

CPU cost was 1.243 CPU-ms/lifecycle, higher than the 16-shard allocator trial,
but process-accounted output fell from 47.03 to **41.93 GiB**. Peak RSS was
8.53 GiB. This supports improved write coalescing and more CPU concurrency as
the benefit of additional physical stores on this shared device; it does not
imply extra physical disk bandwidth.

The repeat qualification preset now selects 32 shards for both workflows and
primitives. Two fresh complete preset repetitions remain necessary before
declaring the new target achieved.

[Complete 32-shard trial report](../helix/04-build/evidence/workflow-capacity/fireweed-9500-mimalloc-32-c6-a.json.gz).

Before the fresh repetitions, `cargo test --locked --release -p fireweed-workload
-- --test-threads=1` passed all 24 public tests, including four log-only recovery
cases. `cargo check --locked --release -p fireweed-server --bin fireweed-service`
also passed. Allocator/runtime environment overrides were absent. The existing
Python gate/monitor suite passed all four tests with the tightened rate gate.

[Public tests](../helix/04-build/evidence/workflow-capacity/fireweed-9500-mimalloc-tests.log.gz),
[service check](../helix/04-build/evidence/workflow-capacity/fireweed-9500-service-check.log.gz).

## Fresh repetitions exposed retained-WAL growth

Both fresh workflow runs at `949f3fc6` failed full qualification. Run 1 averaged
11,932/sec and its slowest cycle reached 9,501/sec, but one shard's sampled WAL
peaked at 513.6 MiB. Run 2 averaged 11,079/sec, with a 9,434/sec slowest cycle
and a **770.7 MiB** WAL peak. Both million-row primitive runs passed. These
reports are retained; the rate and 512 MiB WAL gates were not relaxed.

- [Workflow 1](../helix/04-build/evidence/workflow-capacity/fireweed-9500-949f3fc6-workflow-1.json.gz)
- [Workflow 2](../helix/04-build/evidence/workflow-capacity/fireweed-9500-949f3fc6-workflow-2.json.gz)
- [Primitives 1](../helix/04-build/evidence/workflow-capacity/fireweed-9500-949f3fc6-primitives-1.json.gz)
- [Primitives 2](../helix/04-build/evidence/workflow-capacity/fireweed-9500-949f3fc6-primitives-2.json.gz)

The native trigger compared `max_frame - nbackfills` with the threshold. A reader
can force a partial passive checkpoint, advancing `nbackfills` while preventing
WAL restart. That subtraction postpones the next attempt until another full
budget of frames accumulates, even if the reader has since released its snapshot.
Repeated partial checkpoints therefore allow growth by multiple budget windows.

The candidate correction compares **total retained frames** with the unchanged
64,000-frame threshold. Subsequent commits keep attempting passive checkpoints
until a writer can restart the WAL. This matches the documented
[SQLite auto-checkpoint trigger](https://www.sqlite.org/c3ref/wal_autocheckpoint.html).
Backfill safety, reader snapshot protection, restart locking, and sync behavior
are unchanged. It does not make the sampled footprint budget an engine-enforced
cap against arbitrarily long external read transactions.

A native regression holds a real reader snapshot, performs partial backfill,
checks the reader still sees its original rows, then releases it and verifies
that ordinary commits finish backfill and restart the WAL. It failed at the
retry assertion before the correction and passed afterward. The complete native
WAL suite then passed **77 tests**. Public API tests and fresh capacity runs are
required before qualifying this candidate.

[Regression before](../helix/04-build/evidence/workflow-capacity/fireweed-9500-checkpoint-regression-red.log.gz),
[regression after](../helix/04-build/evidence/workflow-capacity/fireweed-9500-checkpoint-regression-green.log.gz),
[native WAL tests](../helix/04-build/evidence/workflow-capacity/fireweed-9500-wal-tests.log.gz).

The corrected trigger also passed the combined Fireweed Turso adapter and public
workload release suite: **106 passed, one existing ignored**. Command:
`cargo test --locked --release -p fireweed-workload -p fireweed-turso --features
fireweed-turso/local -- --test-threads=1`.
[Public/adapter validation](../helix/04-build/evidence/workflow-capacity/fireweed-9500-checkpoint-public-tests.log.gz).

## Corrected checkpoint repetitions and remaining rate margin

At `d76cae32`, the first fresh workflow repetition passed every gate at
**11,789/sec**, with a 9,705/sec slowest cycle and 266.7 MiB sampled WAL peak.
The second averaged **11,190/sec** and kept WAL to 266.0 MiB, but one cycle
reached only **9,406/sec**. Both primitive repetitions passed. Thus the observed
WAL growth is corrected, while repeated full throughput qualification remains
unmet.

- [Workflow 1](../helix/04-build/evidence/workflow-capacity/fireweed-9500-d76cae32-workflow-1.json.gz)
- [Workflow 2](../helix/04-build/evidence/workflow-capacity/fireweed-9500-d76cae32-workflow-2.json.gz)
- [Primitives 1](../helix/04-build/evidence/workflow-capacity/fireweed-9500-d76cae32-primitives-1.json.gz)
- [Primitives 2](../helix/04-build/evidence/workflow-capacity/fireweed-9500-d76cae32-primitives-2.json.gz)

A three-cycle timing diagnostic found 3.3 commands per durable append and 4,345
logical row operations per projection batch on average. Most apply wall time
was in the update stage (570 ms mean per batch), compared with 18 ms in commit.
These are aggregate concurrent wall-time observations, not a CPU profile. The
trace is diagnostic only and does not count toward qualification.
[Batching trace](../helix/04-build/evidence/workflow-capacity/fireweed-9500-checkpoint-batching-trace.json.gz).

Doubling workers per shard from eight to sixteen did not help: the six-cycle run
averaged 10,395/sec, and cycles 2–5 missed the rate gate, with a minimum of
8,939/sec. The preset retains eight workers.
[Sixteen-worker trial](../helix/04-build/evidence/workflow-capacity/fireweed-9500-d76cae32-32-w16-c6-a.json.gz).

The next candidate enables portable thin LTO and one codegen unit in the release
profile to optimize across facade, adapter, and native engine boundaries. No
host-specific target CPU, durability, or acceptance setting changes. Cargo's
[profile documentation](https://doc.rust-lang.org/cargo/reference/profiles.html#lto)
describes this whole-program optimization and the build-time tradeoff. Its
performance benefit must be measured. Embedding applications own their root
workspace's profile; a dependency cannot impose these flags on Snorri.

## Portable LTO trial

The first six-cycle trial at `40aebf1d` passed every gate at **12,262/sec**.
Its slowest-shard equivalent cycle rate was **10,275/sec**, giving more margin
than the earlier candidates. The sampled WAL peak was 263.4 MiB, peak RSS was
8.50 GiB, and CPU cost was 1.171 CPU-ms/lifecycle. The initial optimized build
took 5m 16s. The combined public/adapter release suite passed under these
compiler settings: **106 passed, one existing ignored**. Fresh repeated
capacity qualification follows this validation.

[LTO trial](../helix/04-build/evidence/workflow-capacity/fireweed-9500-lto-32-c6-a.json.gz),
[build log](../helix/04-build/evidence/workflow-capacity/fireweed-9500-lto-build.log.gz).

[LTO public/adapter test log](../helix/04-build/evidence/workflow-capacity/fireweed-9500-lto-public-tests.log.gz).
