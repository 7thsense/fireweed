# Workflow capacity versus hardware cost

## Current campaign result: both targets remain unmet

2026-09-11. The representative workload loads one million original rows,
persists top-time and other enrichment metadata, schedules four future windows,
delivers bounded chunks with retries/failures, polls progress, verifies every
final disposition, and discovers retained rows for purge through the public API.
There are two campaigns per physical store. Metadata enrichment retains varied
input bodies; a separate payload-rewrite stress variant remains tested.

The best complete three-cycle measurement is **9,564.67 recipients/sec** on
clean `18e9aa33`, with explicit zstd on its private projection directory and
clearing obsolete bytes in already-dirty free pages. Correctness, due times,
RSS and sampled WAL passed. Overall/late-cycle rates, 63 progress checks and
32 projection-size stability checks failed. The objectives remain **10,000
complete recipients/sec**, then **12,500/sec** (+25%), with repeated qualification
and 10k primitive floors. No campaign candidate is qualified.
See the [plan and full evidence](campaign-qualification-plan.md).

| Complete campaign measurement | Value |
|---|---:|
| Resident recipients / completed cycles | 1,000,000 / 3 |
| Physical stores / campaigns per store | 32 / 2 |
| Workers / loaders per campaign | 2 / 2 |
| Storage batch / purge batch | 1,000 / 8,000 |
| Enrichment / scheduling / delivery handler limits | 500 / 200 / 500 |
| Process wall | 314.012 s |
| Average complete recipients/sec | 9,564.67 |
| Worst campaign wall, cycles 0 / 1 / 2 | 83.67 / 109.66 / 116.84 s |
| CPU time/recipient | 1.14346 ms |
| Average charged logical CPU occupancy | 10.924 |
| Peak RSS | 10.486 GiB |
| Process-accounted output/recipient | 12,574.92 bytes |
| Logical retained log bytes/recipient | 1,823.51 bytes |

Sampled host writes were 10.723 GiB, approximately **3,838 bytes/recipient**.
At that observed cost, 10k needs **36.6 MiB/sec** and 12.5k needs **45.8 MiB/sec**.
The run delivered 35.25 MiB/sec; the private sequential reference measured
38.23 MiB/sec. That reference would support roughly 10.4k recipients/sec at this
byte cost. Reaching 12.5k at the reference bandwidth needs about **16.5% fewer
physical bytes/recipient**, or about **20% more bandwidth** at unchanged cost.
These are host/filesystem measurements, not NAND write amplification or a proven
hardware ceiling. Samples omit startup/tail and include other host traffic.

Compression configuration is part of the measurement. Btrfs can mark a file
incompressible after an unfavorable attempt; an explicit property prevents that
sticky fallback. All 64 database/WAL properties in this run read back zstd.
Fireweed does not set the property automatically. See the
[Btrfs documentation](https://btrfs.readthedocs.io/en/latest/Compression.html) and
[Linux v7.2 implementation](https://github.com/torvalds/linux/blob/v7.2/fs/btrfs/inode.c#L920).
A 64-store control with the same total workers was slower and stopped after two
cycles; adding shards alone has not resolved the remaining waits.

The subsequent cross-queue flush candidate `e1ee74b2` completed at 9,313.74/sec,
with 1.11226 CPU-ms/recipient and approximately 3,682 sampled host bytes/recipient.
Its observed cost implies 11.12 / 13.90 CPU-seconds/sec and 35.1 / 43.9 MiB/sec
at 10k / 12.5k. These lower costs did not translate into faster completion:
delivered bandwidth was 32.92 MiB/sec and progress latency still failed. The
9,564.67 result remains the best throughput; neither run is qualified.

### Napkin math aligned with this workload

A complete recipient entails approximately `8 + 2/19 = 8.1053` logical row
operations: insertion, three claim/mutation pairs, purge, and retry claim/mutation
for every nineteenth recipient. Thus 10k recipients/sec means about **81k logical
row operations/sec**; 12.5k means **101k/sec**. These are not separately synced SQL
statements. Storage batching amortizes log syncs, and projection fusion can
combine transitions. Handler/provider chunk limits remain independently enforced.

Three full retained-row exports per cycle cover scheduled-state verification,
final disposition and retention discovery. Each campaign observer waits one second after each public progress read;
actual frequency therefore falls as read latency rises (gate: at least 0.5 Hz).
Reads, processing, settlement and purge are timed. Virtual-clock jumps remove
intentional calendar waiting only. Provider/model latency is stubbed. The fixture
does not establish delayed retry backoff, overlapping campaign intake, continuous
enrichment-stage aggregate reporting or legacy atomic callback groups.

Actual initial bodies average **934.89 bytes**, despite the nominal 1 KiB fixture
setting. The metadata variant writes the body once. Its retained log averages
**1,824 bytes/recipient**, including encoding and commands: about **17.4 / 21.7
MiB/s** at the 10k / 12.5k targets. Process-accounted output implies approximately
**119.9 / 149.9 MiB/s** at current amplification. Neither measure is physical NAND
traffic. The payload-rewrite variant has a different, larger byte budget.

At **1.14346 CPU-ms/recipient**, 10k needs **11.43 CPU-s/s**, and 12.5k needs
**14.29 CPU-s/s**. The stretch target now fits an optimistic sixteen-logical-thread
accounting model, but with very little margin. This host has eight physical cores;
SMT sharing, frequency, cache behavior and waiting prevent treating that arithmetic
as guaranteed capacity. Compared with the observed 10.924 average occupancy,
removing waiting remains essential. Further CPU reduction also creates margin.
These figures do not establish a fundamental hardware ceiling or impossibility.

### Reporting and checkpoint costs

Public lifecycle metrics now read four counters on the existing queue metadata
row. The projection transaction maintains them from actual before/after item
states, with migration and replay coverage. Ordinary addressed commands use
bounded primary-key reads; cohort/supersession commands conservatively scan.
The measured CPU cost includes this maintenance. Progress still waits for
committed coverage, so constant-time SQL alone does not guarantee low latency.

Checkpointing now orders latest-safe frames by destination page before forming
512-page write batches. In a native test updating 2,048 existing 4 KiB pages in
interleaved order, destination writes fell from 1,973 calls to eight. Both paths
write the same 8 MiB; this is a locality/call-count result, not a throughput
multiplier. Cached-page and WAL-read cases verify newest values after truncating
the WAL and reopening. Safe-frame selection and sync semantics are preserved.

The preceding `440a60fd` campaign's sampled active interval observed host-wide **13.70 GiB**
written in **392.58 s**, mean write-request latency **55.95 ms**, and **88.0%** busy
time. Sampling omits startup/tail margins and includes other host activity;
these are not exact per-recipient physical amplification measurements. Load grew
from 7.07 s in cycle zero to 37.48 / 44.34 s. Purge maxima were 11.93 / 15.96 /
33.04 s. Phase maxima are not additive. Later-cycle write waiting remains visible.
Counter units and overlapping request-time accounting follow the
[Linux block statistics documentation](https://docs.kernel.org/block/stat.html).

A same-binary filesystem control disabled copy-on-write only for a new disk
projection directory; the durable log retained its normal path/protocol. It had
no complete campaign reports after **175.28 s** and was stopped with SIGTERM.
Charged CPU averaged only 5.21 logical CPUs. Its preserved nonzero result is not
a throughput measurement, and no NOCOW default was introduced. This control also
loses filesystem compression/checksums and is not equivalent storage behavior.

The current candidate lets another queue apply ready work while a claim waits for
its handler follow-up, keeping each queue's ordered prefix and bounded join
window. Its complete-run gain was 3.3%, with 3.6% lower CPU cost. Current load maxima
were 6.57 / 48.16 / 41.56 s and purge maxima 11.31 / 14.59 / 25.12 s; these phase
maxima are not additive. Both performance objectives remain unmet.

A separate one-cycle diagnostic on the same binary collected 205,265 user-IP
samples at 199 Hz with zero samples lost. Allocation, copying and metadata cloning
were prominent: the top allocator routine accounted for 8.46%, two memcpy routines
7.88%, and the metadata-map clone routine 2.01%, excluding their callees. This is
sampling evidence, not exact allocation attribution. The next candidate removes
redundant command/request copies and reuses a disposable projection image for the
same pre-append mutation validation. No log format, authority or API semantics
change. Current primitive qualification and repeated campaign passes are still
required on a final build.

### Local write calibration and async runtime starvation

The allocation candidate `b508d888` did not produce a qualifying result. Its
32-store run failed after 480.33 s with an object-log post-position timeout in
cycle three; the two completed cycles took 95.45 / 166.84 s. A subsequent
16-store control was stopped after two failing cycles (164.34 / 286.08 s).
Neither has a complete-run rate. The control began with a warm drive and halved
the aggregate checkpoint budget as well as store concurrency, so it does not
isolate a causal store-count effect. Raw nonzero exits and partial results remain
in the evidence archive.

An 8 GiB private-file calibration then measured **38.23 MiB/sec** in 214.30 s,
including fdatasync. It used 16 MiB incompressible writes, requested O_DIRECT,
and disabled COW only on that new temporary file. First/last blocks verified and
the file was removed. The sampled device was 99.5% busy, with approximately
0.23 host CPU-seconds per second. This is a local sequential-write reference,
not a fundamental device ceiling or a simulation of campaign page overwrites.
It ran immediately after the campaign controls, with a warm device. NVMe
composite temperature rose from roughly 34°C to 68°C during the preceding
32-store run; temperature alone does not establish thermal throttling.

The best complete `386d79f7` run sampled **12.42 GiB** of host writes in 380.62 s.
Dividing by its three million recipients gives roughly **4,445 device bytes per
recipient**, with startup/tail omission and host-wide attribution caveats. At the
sequential reference rate that byte cost implies roughly **9,018 recipients/sec**.
The 10k / 12.5k targets would require approximately **4,008 / 3,207 device bytes
per recipient** at that bandwidth. The stretch target therefore calls for about
**28% lower physical byte cost**, higher sustained bandwidth, or both. These
estimates are additional resource constraints, not revised performance goals.

Code review also found native Unix VFS writes occurring on application async
workers during projection commit/checkpoint. A real WAL-write gate reproduced
worker starvation on both a current-thread runtime and a one-worker multi-thread
runtime. The next candidate moves the entire admitted apply to a blocking worker,
retaining writer ownership through commit and token publication. Its existing
RelTx hop stays separate to avoid nested runtimes. Both starvation regressions
pass; this establishes the scheduling defect, not that it caused every observed
log timeout. The rebuildable checkpoint window also increases from 250 to
**448 MiB**, retaining the 512 MiB sampled WAL gate, fixed cache cap, page-size
normalization and standalone 1,000-frame policy. Capacity effects remain to be
measured. No log durability protocol or workflow representation changes.

## Historical all-due saturation qualification (2026-09-10)

The following measurements apply to the earlier, lighter fixture and retain its
original 9,500/sec qualification floor. They do not certify the campaign above.

2026-09-10 follow-up, clean source `a73b067f`. The new changes are local and
unreleased; the earlier v0.31.26 release candidate below is a historical baseline.
The [full investigation and qualification evidence](workflow-9500-iteration.md)
records two passing three-million-lifecycle runs and two passing million-row
primitive runs on the same binary.

| Measured quantity | Repetition 1 | Repetition 2 |
|---|---:|---:|
| Complete original-row workflows/sec | 12,125 | 11,810 |
| Worst cycle, slowest-shard equivalent workflows/sec | 9,839 | 10,036 |
| Inserts/sec | 67,738 | 67,311 |
| Key updates/sec | 97,186 | 77,121 |
| ID updates/sec | 70,739 | 61,756 |
| CPU time/lifecycle | 1.154 ms | 1.235 ms |
| Average logical CPUs occupied | 13.98 | 14.58 |
| Process-accounted output/lifecycle | 15,008 bytes | 15,018 bytes |
| Process-accounted output rate | 173.4 MiB/s | 169.1 MiB/s |
| Peak RSS | 8.67 GiB | 8.52 GiB |
| Maximum sampled shard WAL | 259.0 MiB | 259.6 MiB |

The workload passes the new **9,500/sec overall and every-cycle floor**, with
exact outcomes, retries, purge, and storage/memory stability. Compared with the
7,873–8,029/sec baseline, average workflow throughput improved about **47–54%**.
CPU cost fell roughly **14–20%**, while CPU utilization increased. These are
combined measured improvements, not an isolated attribution of every gain.

Hardware is unchanged: Ryzen 7 4800H, eight physical cores/16 logical CPUs,
62 GiB RAM, one Kingston NVMe under encrypted Btrfs with zstd:3. The preset now
uses 32 independent physical log/projection shards sharing that device, eight
workers per shard, four loaders, batches of 1,000 and purge batches of 8,000.
Qualified data is on disk. All measurements use the public API and deterministic
compressible 1 KiB enrichment bodies, with log durability enabled.

## Updated napkin arithmetic

A lifecycle entails approximately `8 + 2/19 = 8.1053` logical row operations:
insert, three claim/mutation pairs, purge, and occasional retry. Thus the new
11.8–12.1k workflows/sec represents about **95.7–98.3k logical operations/sec**.
Fusion means these are not counts of physical SQL statements or disk writes.
The original target of 10k inserts or individually addressed updates/sec was
reasonable; bounded batching exceeds it by a large margin. It is not equivalent
to 10k serial, separately synced requests/sec.

At **1.154–1.235 CPU-ms/lifecycle**, 10k workflows/sec needs **11.54–12.35
CPU-seconds/sec**. A simplistic 16-logical-CPU constant-cost division yields
**13.0–13.9k workflows/sec**, only about **10–14%** above the corresponding
measured average. This is an optimistic accounting comparison, not a measured
hardware ceiling: SMT shares physical execution resources, and CPU cost changes
with concurrency, cache behavior, frequency, and waiting. 20k workflows/sec
would require **23.1–24.7 CPU-seconds/sec** at this cost, exceeding that budget.
Substantially higher rates on this host require reducing per-lifecycle work,
not just adding shards or workers.

Three approximately 1 KiB body versions per lifecycle require **29.3 MiB/sec**
at 10k workflows/sec before log encoding, indexes, WAL, checkpoint and metadata
costs. Measured process-accounted output is about **15 KB/lifecycle**, or
143 MiB/sec at 10k/sec. This is not device or NAND traffic: compression,
coalescing, and filesystem accounting prevent that inference. Four approximate
sync calls per durable append group (segment and manifest file/directory syncs)
remain enabled. Batching amortizes those syncs across many independent rows.

## What was worth fixing, and what remains

The baseline CPU profile placed roughly one quarter of user-space samples in
allocation/freeing paths. Executable-owned mimalloc materially reduced CPU cost.
Thirty-two shards improved working-set behavior; doubling workers per shard
from eight to sixteen made the corrected candidate slower. Portable thin LTO
and one codegen unit then provided enough repeated rate margin, at a cost of
several minutes of additional build/link time.

A separate correctness/performance issue appeared during repetition: after a
partial checkpoint, subtracting backfilled frames postponed retry for another
full WAL budget. Retrying against total retained frames corrected observed WAL
growth without raising the budget or changing reader safety. The new real-reader
regression failed before the change and passed afterward. The sampled footprint
is a finite-run bound, not a hard cap against arbitrarily long reader snapshots.

There may be modest additional headroom, but the data does not justify promising
another order of magnitude. The next useful performance investigation would
profile allocation callers, copies/encoding, and SQL interpreter/B-tree work
on this final build, together with device latency and durable batch occupancy.
The old profile did not identify cache reconciliation as a leading standalone
symbol, so speculative cache rewrites are lower priority. Current average CPU
occupancy already reaches 14–14.6 logical CPUs; another worker increase has no
measured support. More physical cores or independent devices deserve separate
measurements rather than a linear-scaling assumption.

For Snorri adoption, keep allocator and root Cargo release-profile choices in
the embedding executable. The representative public API already achieves the
basic performance goal without auxiliary workflow entities. Separately qualify
Snorri's actual payload distributions, resident population, indexes, network/
remote-log path, and latency requirements before extending this result to them.
No Snorri migration or interface change was made here.

## Historical v0.31.26 baseline analysis

The remaining analysis records the earlier state and proposed experiments.
Its 8k rate and next-step recommendations are superseded by the results above.

2026-09-10. Release candidate: v0.31.26, source `9230efbc`. The version-only
release change preserves production implementation `b0f89563`, measured twice
with each qualification workload. Full evidence and experimental history are in
the [capacity review](../helix/04-build/workflow-capacity-review.md).

### Assessment

The original 10,000 inserts/sec and 10,000 individually addressed updates/sec
targets were reasonable and are exceeded. The system sustains approximately
8,000 complete recipient lifecycles/sec, including two enrichments, delivery,
retry/failure handling, and purge. Further improvement is plausible, but the
evidence does not establish a hardware ceiling or promise another order of
magnitude on this host. Reaching 10,000 complete workflows/sec is a useful next
experiment: it requires approximately 25% more throughput, or 20% less resource
cost per lifecycle at the same available resource budget.

These are bounded batches of independent row mutations. They do not imply the
same throughput for one outstanding, separately durable request per row.

### Measured resource budget

The host reports an AMD Ryzen 7 4800H, eight physical cores / 16 logical CPUs,
about 62 GiB RAM, and a Kingston OM8PCP3512F-AB NVMe. Qualified data lived on
encrypted Btrfs with zstd:3 compression. All 16 independent shard log/projection
pairs shared this device. Host inventory was checked with `lscpu`, `lsblk`, and
`findmnt -T .`; no manufacturer throughput rating is assumed.

The following figures are calculated from the raw `process_wall_s`,
`user_cpu_s`, `system_cpu_s`, `filesystem_output_blocks`, and `max_rss_kib` in
[workflow A](../helix/04-build/evidence/workflow-capacity/fireweed-qualified-workflow-cache-accounting-page4k-b0f89563-500k-16-c6-a.json.gz)
and [workflow B](../helix/04-build/evidence/workflow-capacity/fireweed-qualified-workflow-cache-accounting-page4k-b0f89563-500k-16-c6-b.json.gz).
Each performed three million recipient lifecycles.

| Resource | Run A | Run B |
|---|---:|---:|
| Process wall time | 381.48 s | 374.08 s |
| User CPU time | 3,643.01 s | 3,653.33 s |
| System CPU time | 633.48 s | 657.26 s |
| Average logical CPUs occupied | 11.21 | 11.52 |
| CPU time per lifecycle | 1.425 ms | 1.437 ms |
| CPU time per logical row operation | 175.9 µs | 177.3 µs |
| Process-accounted output per lifecycle | 16,701 bytes | 16,622 bytes |
| Process-accounted output rate | 125.3 MiB/s | 127.1 MiB/s |
| Peak RSS | 6.11 GiB | 6.03 GiB |

The workload's own timed windows give 7,873 and 8,029 lifecycles/sec; dividing
by whole-process time, including startup/shutdown, gives 7,864 and 8,020/sec.
Neither calculation excludes projection settlement from the workflow.

#### CPU arithmetic

One lifecycle entails insert + three claims + three mutations + purge, with
one extra claim/mutation pair for approximately 1/19 of recipients:

`operations/lifecycle ≈ 8 + 2/19 = 8.1053`

Thus 8,000 lifecycles/sec represents approximately 64,842 logical row operations
per second. Fused projection application can combine logical operations, so this
is not a count of physical SQL updates or disk writes.

At the measured 1.43 CPU-ms/lifecycle, 10,000 lifecycles/sec would consume about
14.3 CPU-seconds per wall second. That fits below a simplistic 16-logical-CPU
accounting limit, but SMT siblings share execution resources: 16 threads are
not 16 independent cores. Dividing 16 by 0.00143 gives about 11,200/sec only as
an optimistic constant-cost accounting estimate, not an achievable CPU ceiling.
Frequency, memory stalls, contention, I/O waits, and batch fill can all change
the cost. Likewise, 20,000/sec would need about 28.6 CPU-seconds/sec at today's
cost, making it implausible on this host without substantial cost reduction.

Approximately 15% of charged CPU time is in the kernel. The much larger user
CPU cost merits profiling the native engine and application before concentrating
only on filesystem tuning. Peak RSS is far below host RAM capacity; that does
not establish that cache misses or memory bandwidth are insignificant.

#### Durability and bytes

At 1 KiB per insert, 10,000 inserts/sec is only 9.77 MiB/sec of body data.
At 1,000 rows per batch, it is ten batch requests/sec. A local-log append group
publishes segment and manifest objects, each through file sync and parent
directory sync: approximately four sync calls per append group. An illustrative
one-batch-per-group insert stream therefore needs about 40 sync calls/sec,
versus about 40,000/sec if every row is its own append group. Actual grouping
and concurrency determine the observed rate. Log durability remains enabled.

The workflow replaces a roughly 1 KiB body at load and both enrichments, so its
three body versions alone represent about 23.4 MiB/sec at 8,000 workflows/sec.
Log command encoding, indexes, page WAL writes, checkpoints, and metadata add
work. The roughly 16.7 KB of process-accounted output per lifecycle demonstrates
substantial additional output beyond body bytes. It is not an SSD write-
amplification ratio: Linux process block accounting, filesystem compression,
write coalescing, and device/controller behavior differ. The accounting figures
cannot be substituted for device or NAND bytes.

An earlier short device sample observed approximately 55 MiB/sec writes and
99% busy time. That is evidence of pressure under that workload, not the NVMe's
sequential bandwidth limit, and it was not a synchronized device sample of these
two final runs. Sync latency and small dependent writes can dominate without
approaching advertised sequential throughput. A precise final CPU-versus-device
split still needs simultaneous device latency, queue depth, sync, and CPU samples.

### Where to spend the next optimization effort

1. **Measure and reduce the remaining cache reconciliation cost.** The repaired
   engine counts evictable pages once per WAL commit. That removes the prior
   repeated allocation-time scans but still costs O(cache pages) per commit.
   Profile its fraction of current CPU time, then consider incremental accounting
   for pages changed by commit. Rollback, spill, eviction, header exclusion, and
   idempotence need to retain their current invariants. This is the most concrete
   remaining algorithmic opportunity; its present CPU share is unmeasured.
2. **Attribute encoding, copies, and batch fill.** Measure user CPU stacks and
   actual items/commands per durable group and projection commit. Optimize the
   dominant paths with unchanged log durability and per-row fencing. More workers
   and longer waits are not presumed improvements: doubling workers within eight
   shards was slower in the retained experiments.
3. **Keep shard working sets appropriate to the cache.** Sixteen shards passed
   repeatedly where eight did not reliably sustain every-cycle throughput at
   500,000 resident workflows. Larger shard caches or a 32-shard diagnostic may
   expose headroom, but more shards also create more log streams, buffers, and
   checkpoint work on the same SSD. Measure through recycling, not just ingestion.
4. **Test independent devices or server hardware after attribution.** Separate
   physical devices could remove shared write/sync contention. More physical
   cores can help if user CPU dominates. Neither scales linearly by assumption.
   A second directory on the same SSD provides no additional physical bandwidth.

Use the existing six-cycle, 500,000-recipient workload for diagnostics, recording
CPU stacks, device bytes/latency, and append/commit batch occupancy together.
Then rerun both workflow and primitive qualifications twice without profilers
or competing builds. Keep the fairness, retry, recovery, payload, retention,
and storage gates unchanged. Do not weaken log sync or inflate WAL thresholds
to manufacture a higher result.

The practical next objective is to test whether 10,000 complete workflows/sec
is attainable by removing approximately 20% of current cost. This is an
engineering hypothesis supported by the budget, not a qualified rate. Current
evidence already supports adopting the released basic API for a separate Snorri
integration qualification; production service latency, optional indexes, and
remote log storage still require their own representative runs.
