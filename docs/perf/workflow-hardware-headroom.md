# Workflow capacity versus hardware cost

2026-09-14 write attribution on the full disk workload recorded **55.13 GB of
requested WAL writes**, **5.91 GB of main-file writes**, and **0.49 GB of temporary
file writes** over six million recipients. Time inside those VFS calls totaled
669/93/3.8 seconds respectively, overlapping across stores. These are neither
NAND bytes nor device-service times; creation/removal of temporary files is not
timed. The traced run is diagnostic only. The [qualification plan](campaign-qualification-plan.md)
contains raw provenance and apply/log timings. The one-second background
claim-join experiment subsequently completed at 8,151.94/sec and failed its
throughput and progress gates; the window is restored to 500 ms. This
does not change the fixed 10k/12.5k targets or establish an intrinsic SSD ceiling.

2026-09-13: no SSD maintenance was performed. It is no longer a dependency for
optimization. New first-cycle diagnostics and the statement-execution reuse
candidate are recorded in the [qualification plan](campaign-qualification-plan.md).
The projection already omits stable-storage sync through `RebuildableIo`; the
authoritative log retains durability. First-cycle tracing attributes about 71%
of aggregate apply time to update SQL and 2.6% to commit/checkpoint. Those times
overlap across stores, came from a default temporary root on tmpfs, and do not
establish the sustained disk bottleneck. No physical device ceiling has been
demonstrated.

The subsequent same-binary, six-cycle comparison keeps the log on disk in both
runs. With the projection also on disk it takes **704.47 s at 8,520.83/sec**;
with only the projection on tmpfs it takes **526.72 s at 11,401.01/sec**. CPU cost
is nearly unchanged: **1.03661 versus 1.04597 CPU-ms/recipient**. Host writes fall
from **24.8708 to 12.0149 GiB**. This single pair supports investigating the
projection write/checkpoint path, not declaring an intrinsic SSD limit or moving
qualification to RAM. Both runs fail qualification; exact gates and provenance
are in the qualification plan. The first-cycle 17k CPU diagnostics used tmpfs
for both log and projection and must not be presented as durable-disk results.

At the new disk run's measured CPU/byte costs, 10k/12.5k needs approximately
**10.37/12.96 CPU-seconds per second** and **42.45/53.11 MiB/sec** of host writes.
These are constant-cost demand estimates, not measured hardware limits. The
projection-only comparison changes the backing filesystem and its CPU/cache
behavior as well as physical I/O; it does not attribute the entire difference
to SSD service time. It is not yet evidence that a faster computer is necessary.

## Current timestamp-workflow measurements

2026-09-12. The source-aligned timestamp workload's fastest six-cycle baseline is
**8,329.69/sec**, failing throughput and reporting. A later one-worker control
reached **7,911.78/sec** and passed every non-throughput gate, including all 384
campaign-cycle reporting checks. The goals remain 10k and 12.5k complete
recipients/sec; neither is qualified. Full configuration and raw evidence are in
the [qualification plan](campaign-qualification-plan.md).

The 2 KiB page experiment did not improve sustained runtime: 8,306.86/sec versus
8,329.69/sec at 4 KiB, with 6.4% more CPU/recipient and only 0.7% fewer host writes.
New projections return to 4 KiB; existing-file compatibility and smaller-page
regressions remain. The table below retains the original 4 KiB baselines.

| Storage batch | Recipients/sec | Sampled host bytes/recipient | Host MiB/s needed at 10k / 12.5k | CPU-s/s needed at 10k / 12.5k |
|---|---:|---:|---:|---:|
| 500 | 7,271.46 | 4,809.75 | 45.87 / 57.34 | 10.54 / 13.18 |
| 1,000 | 8,329.69 | 4,092.98 | 39.03 / 48.79 | 10.68 / 13.35 |

For batch 1,000, the earlier 38.23 MiB/s private sequential reference implies
about 9.79k recipients/sec at the observed byte cost. Reaching 12.5k at that
reference would require about 21.7% fewer host bytes/recipient, or 27.6% more
bandwidth. This is a diagnostic arithmetic reference, not a proven device ceiling.
The actual run averaged 32.61 MiB/s with 88.1% device busy time and 72.2 ms mean
write-request latency. CPU demand is also substantial on eight physical cores:
16 logical threads do not guarantee 16 independent cores of useful work.

These runs retain about 1,834–1,836 logical log bytes/recipient. Batch 1,000
accounts for 12,336 output bytes/recipient at process level; host writes average
about 4,093 bytes/recipient after filesystem effects. These are three distinct
measurements; none is NAND traffic. Host counters include other activity and
omit startup/tail. All DB/WAL files read back the explicit zstd property.

The operation budget remains `8 + 2/19 ≈ 8.1053` mutations/recipient plus three
full retained-row reads and public progress polling. Thus 10k/12.5k complete
recipients/sec means approximately 81k/101k logical row operations/sec, not
10k/12.5k individual updates. Batching and fusion amortize this work; reporting
and retention remain timed. Initial bodies average 934.89 bytes and are retained
through metadata enrichment. No acceptance threshold has changed.

### Reporting-safe worker control and host maintenance hypothesis

The one-worker 2 KiB control costs 1.08231 CPU-ms/recipient and approximately
4,693 host bytes/recipient. At 10k/12.5k this implies **10.82/13.53 CPU-s/s** and
**44.76/55.95 MiB/s** of host writes. It averaged 35.50 MiB/s. Reducing workers
fixed reporting latency but increased host writes 15.5%; this is a measured
tradeoff, not a qualified deployment.

All 32 DB/log pairs share one NVMe. Read-only checks found that the encrypted
root mapping blocks discard/TRIM and periodic `fstrim` is disabled. That limits
what can be concluded from the earlier 38.23 MiB/s sequential reference: it is a
measurement of this media/configuration state, not an intrinsic device ceiling.
The kernel documents discard blocking and its encryption-policy tradeoff;
Kingston explains how TRIM assists reclamation. A causal performance comparison
still requires the [reviewed one-time control](storage-trim-control.md), which
the user has now approved. The first noninteractive invocation could not obtain
administrator authentication; a desktop terminal is waiting at the sudo prompt.
No completed maintenance or post-maintenance measurement is established yet.
[Kernel dm-crypt documentation](https://docs.kernel.org/admin-guide/device-mapper/dm-crypt.html),
[Kingston garbage-collection discussion](https://www.kingston.com/en/blog/servers-and-data-centers/garbage-collection).

### Low throughput is not proof of a device limit

The user's objection is correct: observed throughput and device busy time do not
establish that Fireweed approaches the device's IOPS or bandwidth capability.
Re-accounting the archived raw samples gives the following host-wide values:

| Canonical campaign control | 4 KiB, two workers | 2 KiB, one worker |
|---|---:|---:|
| Write IOPS | 1,247.64 | 1,442.90 |
| KiB/write request | 26.77 | 25.19 |
| Mean write-request latency, ms | 72.19 | 50.22 |
| Weighted mean outstanding I/Os | 90.24 | 72.67 |
| Maximum sampled outstanding I/Os | 767 | 762 |
| Device flushes/sec | 18.92 | 21.34 |
| Mean device flush latency, ms | 9.13 | 9.05 |

For the one-worker run, `1,442.90 × 25.19 / 1024 = 35.50 MiB/s` and
`1,442.90 × 0.05022 ≈ 72.47` outstanding writes. This is consistent with
the measured queue-depth integral. It demonstrates substantial backlog at low
throughput, rather than a globally single-outstanding-request workload. It does
not prove a firmware bottleneck: request times include block-layer residence,
and application/filesystem burst patterns can create long queues. A one-second
sample maximum is not a bound on instantaneous peak depth. Device flush counts
are not application fsync counts; the block layer combines flush requests.

At unchanged host bytes/recipient, the one-worker target needs only 44.76 MiB/s
for 10k or 55.95 MiB/s for 12.5k. Treating the observed 35.50 MiB/s as the maximum
would be circular. The remaining investigation must distinguish CPU work and
application serialization, projection checkpoint/writeback bursts, and storage
service latency. TRIM is a controlled diagnostic for the last category, not an
assumed fix or a prerequisite for all future code optimization.

Reproduce these calculations with `scripts/perf/workflow_device_accounting.py`
and the archived `*-device.jsonl.gz` sources. It rejects counter resets and
non-increasing sample times; two unit tests verify units and failure handling.
Derived artifacts are `fireweed-w{1,2}-device-accounting.json`. Counter semantics
follow the [Linux I/O statistics documentation](https://docs.kernel.org/admin-guide/iostats.html).

The separate million-row primitive control passed at 103.95k inserts/sec,
107.20k key updates/sec and 91.64k ID updates/sec. Those bodies use repetitive
padding; their compression costs must not be used for the campaign. The new
`--primitive-varied-payload` control shares the campaign body distribution and
reports actual byte totals. It retains primitive semantics and a body replacement,
so only a complete campaign run supplies the primary workflow's CPU/write budget.

### Repeated component floors with varied bodies

Clean `fda0dcab` repeated the million-row component test with the campaign's
varied body generator, 1,000-row batches and 32 independent sequential store loops.
Both runs qualified: inserts **113.78k / 29.57k**, key-addressed updates
**29.34k / 27.41k**, and ID-addressed updates **44.19k / 49.23k rows/sec**.
Thus the original 10k component targets are demonstrated with batching and varied
records; this does not establish either complete-campaign target.

Actual initial bodies average **934.889 bytes** and replacement bodies
**959.889 bytes**. Process walls were 96.08 / 131.22 seconds while CPU work was
555.79 / 566.52 CPU-seconds. Sampled host writes were 3.984 / 4.274 GiB at
43.79 / 33.82 MiB/s, with 90.3% / 95.6% device busy time. Phase timing overlaps
across stores and must not be added. These are component-body-replacement costs;
use the complete campaign for its metadata-keep workflow coefficients.

The 43.79 MiB/s sample exceeds the earlier 38.23 MiB/s sequential reference,
confirming that the latter is not an intrinsic ceiling. The strong serial-run
variation with little CPU-work change is another reason to test the observed
blocked-discard configuration before claiming an unavoidable hardware limit.

## Historical mixed-priority stress measurements

The older fixture mixed FIFO ordinals with small scheduled-second values;
Snorri uses availability timestamps and keeps unscheduled work ahead of scheduled
work. The figures below describe the retained `mixed_sequence_stress` fixture.
They must not be relabeled as timestamp-workflow performance or used as its
current CPU/byte coefficients.

2026-09-11. The representative workload loads one million original rows,
persists top-time and other enrichment metadata, schedules four future windows,
delivers bounded chunks with retries/failures, polls progress, verifies every
final disposition, and discovers retained rows for purge through the public API.
There are two campaigns per physical store. Metadata enrichment retains varied
input bodies; a separate payload-rewrite stress variant remains tested.

The best complete three-cycle measurement is **10,850.17 recipients/sec** on
clean `f99aa404`, with explicit zstd on its private projection directory,
cleared free pages, a 448 MiB checkpoint window and
`OBJECT_LOG_FLUSH_RUNTIME_THREADS=1`. Correctness, due times,
RSS and sampled WAL passed. The last-cycle rate, 90 progress checks and
32 projection-size stability checks failed. The objectives remain **10,000
complete recipients/sec**, then **12,500/sec** (+25%), with repeated qualification
and 10k primitive floors. Overall throughput now clears 10k, but no campaign candidate is qualified.
See the [plan and full evidence](campaign-qualification-plan.md).

| Complete campaign measurement | Value |
|---|---:|
| Resident recipients / completed cycles | 1,000,000 / 3 |
| Physical stores / campaigns per store | 32 / 2 |
| Workers / loaders per campaign | 2 / 2 |
| Storage batch / purge batch | 1,000 / 8,000 |
| Enrichment / scheduling / delivery handler limits | 500 / 200 / 500 |
| Process wall | 276.868 s |
| Average complete recipients/sec | 10,850.17 |
| Worst campaign wall, cycles 0 / 1 / 2 | 74.70 / 96.59 / 102.40 s |
| CPU time/recipient | 1.03862 ms |
| Average charged logical CPU occupancy | 11.254 |
| Peak RSS | 10.301 GiB |
| Process-accounted output/recipient | 10,914.00 bytes |
| Logical retained log bytes/recipient | 1,823.51 bytes |

Sampled host writes were 9.382 GiB, approximately **3,358 bytes/recipient**.
At that observed cost, 10k needs **32.0 MiB/sec** and 12.5k needs **40.0 MiB/sec**.
The run delivered 35.08 MiB/sec; the private sequential reference measured
38.23 MiB/sec. That reference supports roughly 11.94k recipients/sec at this
byte cost. Reaching 12.5k at the reference bandwidth needs about **4.5% fewer
physical bytes/recipient**, or about **4.7% more bandwidth** at unchanged cost.
These are host/filesystem measurements, not NAND write amplification or a proven
hardware ceiling. Samples omit startup/tail and include other host traffic.
These average costs also do not replace the stricter worst-cycle rate checks.

Compression configuration is part of the measurement. Btrfs can mark a file
incompressible after an unfavorable attempt; an explicit property prevents that
sticky fallback. All 64 database/WAL properties in this run read back zstd.
Fireweed does not set the property automatically. See the
[Btrfs documentation](https://btrfs.readthedocs.io/en/latest/Compression.html) and
[Linux v7.2 implementation](https://github.com/torvalds/linux/blob/v7.2/fs/btrfs/inode.c#L920).
A 64-store control with the same total workers was slower and stopped after two
cycles; adding shards alone has not resolved the remaining waits.

The preceding cross-queue flush candidate `e1ee74b2` completed at 9,313.74/sec,
with 1.11226 CPU-ms/recipient and approximately 3,682 sampled host bytes/recipient.
Its observed cost implies 11.12 / 13.90 CPU-seconds/sec and 35.1 / 43.9 MiB/sec
at 10k / 12.5k. These lower costs did not translate into faster completion:
delivered bandwidth was 32.92 MiB/sec and progress latency still failed. The
copying reduction subsequently raised the best result to 10,109.38/sec; none of
these runs is qualified.

A wider checkpoint window then reduced sampled host writes about 11%, and a
same-binary one-worker log-runtime control raised the best rate to 10,850.17/sec.
It still failed progress latency and the last-cycle rate. Runtime worker count
is an explicit measured setting, not a silent code default. See the full paired
results and the planned phase-level progress attribution in the campaign plan.

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
**104.1 / 130.1 MiB/s** at current amplification. Neither measure is physical NAND
traffic. The payload-rewrite variant has a different, larger byte budget.

At **1.03862 CPU-ms/recipient**, 10k needs **10.39 CPU-s/s**, and 12.5k needs
**12.98 CPU-s/s**. The stretch target now fits an optimistic sixteen-logical-thread
accounting model, but with very little margin. This host has eight physical cores;
SMT sharing, frequency, cache behavior and waiting prevent treating that arithmetic
as guaranteed capacity. Compared with the observed 11.254 average occupancy,
removing waiting remains essential. Further CPU reduction also creates margin.
These figures do not establish a fundamental hardware ceiling or impossibility.

### Progress latency and admission budget

One-cycle phase diagnostics found loading progress reads averaging 1.68 seconds
and reaching 8.02 seconds, while final verification reads were below a millisecond.
Almost no retries occurred. The generic async policy permits up to 512 MiB of
encoded unapplied work per queue; its 60-second age threshold is not a one-second
visibility contract. The next experiment tightens this existing admission budget
to 2 MiB for the fixed 1 KiB fixture, retaining 1,000-row storage batches. A
500-row control passed progress checks but lost 17.7% one-cycle throughput.
Neither short diagnostic is qualification, and no read-consistency gate changed.

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
