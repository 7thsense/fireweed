# Campaign qualification and performance plan

## Exact-binary projection I/O isolation (2026-09-16)

Four serial disk/tmpfs/tmpfs/disk one-cycle campaigns use the same preserved
`4f4acde0` executable (`9f54d120…e6a3763`), with the authoritative log on Btrfs
throughout. Each processes one million original recipients over 64 physical stores,
with unchanged metadata/timestamp enrichment, stage limits, reporting, retries,
verification and retention. All four exit zero and their per-campaign logical
counts match. Both VFS/publication traces and user-mode CPU counters are enabled;
these diagnostic runs are **not sustained qualification**.

| Projection / order | Recipients/sec | CPU ms/recipient | Peak process RSS GiB | Tmpfs allocated GiB | Sampled host writes GiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Disk 1 | 16,030.05 | 0.88844 | 12.784 | — | 2.917 |
| Tmpfs 1 | 17,000.75 | 0.86142 | 14.188 | 6.141 | 1.862 |
| Tmpfs 2 | 16,919.21 | 0.86287 | 14.149 | 6.120 | 1.756 |
| Disk 2 | 14,902.93 | 0.89157 | 14.205 | — | 3.341 |

The two-run means improve throughput **9.66%** and CPU cost **3.13%** with
projection tmpfs; user instructions fall only 0.59%, and user cycles are flat
(+0.02%). Process RSS rises 4.99% and excludes the additional tmpfs allocation.
Logical WAL bytes are nearly unchanged (+0.29%); each disk run writes about
6.56 GB of WAL and only 256 KiB of main-database pages. Publication counts and
bytes differ by less than 0.03%. This comparison changes filesystem placement,
not the amount of logical workflow work.

Disk control 2 has 12.49 ms mean host write-request latency versus 0.85 ms in
control 1. Summed WAL VFS-call time rises from 14.33 to 93.31 seconds; summed
publication time rises from 404.34 to 1,217.39 seconds. These calls overlap across
stores and **cannot be added to wall time**. Host statistics cover the whole
device, omit startup/tail samples and are not process-attributed. Order effects
remain possible; the short ABBA supports an I/O contribution, not a precise
causal decomposition or a device throughput ceiling.

At 12.5k recipients/sec, this sample implies about **78.24 MiB/sec logical WAL**
plus **21.48 MiB/sec immutable-log publications**, and **11.13 CPU-seconds/sec**
at the disk runs' mean process CPU cost. Logical writes are not physical device
writes; Btrfs compression/coalescing and publication barriers require separate
accounting. This short-run resource estimate does not supersede the eight-cycle
mixed-I/O measurements or prove the sustained stretch target.

Next extend this exact-binary diagnostic through eight cycles, retaining disk
logs and all workflow semantics. Check tmpfs capacity and sample its allocation
separately from RSS; prior disk qualification's approximately 28.3 GiB aggregate
per-store WAL peaks plus 1.84 GiB main files suggest it fits the existing 32 GiB
mount, with limited margin. Abort cleanly if space becomes insufficient rather
than changing host mounts or weakening workload gates. A tmpfs result remains
ineligible for qualification and is not a proposed production deployment.

Raw reports, counters, device samples, storage allocation, runner and analysis
are archived in `fireweed-projection-io-isolation-screen-manifest.json`.

## Publication replay completes: substantial isolated headroom (2026-09-16)

The native publication replay on clean `60bf12a1`, executable
`a4c97a0b157b1a70b6d8b8ace4c98dfc0c849fd59099207c9ca47846f197ac00`, exits zero.
All **351,040 destination objects** are byte-for-byte identical to their captured
sources and all destination key sets match. Every cycle publishes 10,970 objects,
1,802,012,562 bytes and 21,940 native file/directory sync calls. All 32 cycles finish
in **280.974 timed seconds**, or **113,889.74 publication-only equivalent
recipients/sec**; the minimum cycle is **51,351.59/sec**. These are not completed
workflow rates. Preload takes 0.940 seconds and full verification 194.580 seconds,
both excluded from the publication timers.

| Replay cycles | Equivalent recipients/sec | Logical publication MiB/sec | Sampled host write MiB/sec |
| --- | ---: | ---: | ---: |
| 0–7 | 217,068.95 | 373.04 | 263.84 |
| 8–15 | 222,449.47 | 382.29 | 256.93 |
| 16–23 | 121,709.16 | 209.16 | 133.57 |
| 24–31 | 56,169.84 | 96.53 | 61.18 |

The full timed marker span records about **35.80 GiB of host writes** at
130.34 MiB/sec, versus 53.704 GiB of logical publication bytes. The last eight
cycles show 88.22% busy, 2,498.99 write IOPS, 24.18 ms mean write-request latency,
60.72 outstanding I/Os and 0.827 workload CPU-seconds/sec. These sampled host
windows omit boundary gaps and include inter-cycle gaps/other processes. Whole-
process CPU counters include verification and must not be assigned to publication.
The burst-to-lower-rate transition is observed; its device-internal cause is not
proved by these counters.

At 12.5k recipients/sec, the captured immutable log requires **21.482 MiB/sec of
logical publication bytes and 274.25 native durability calls/sec**. Even the
slowest isolated cycle is 4.108 times that target. This rules out treating an
8–12k workflow result as the standalone capacity of this publication stream; it
does not rule out log waits under mixed projection traffic, serialization/engine
costs, or stage-barrier effects omitted by the isolation model. The retained
workflow evidence still proves repeated 10k, not repeated 12.5k.

Next use the existing public `--projection-root` diagnostic with the exact same
preserved workflow executable: serial disk/tmpfs/tmpfs/disk one-cycle runs,
keeping the authoritative log on disk, all logical work and tracing identical.
A one-cycle projection is almost entirely WAL, so this isolates the cost of
projection filesystem traffic while retaining application/engine/serialization
work. Record tmpfs storage separately from process RSS. Such runs remain explicitly
ineligible for qualification. The result will guide a code change; moving the
whole projection to tmpfs is not being adopted as the solution.

Reports, input linkage, raw samples, code and analysis are archived in
`fireweed-publication-replay-results-manifest.json`. The replay destinations and
original capture remain local under `target/workflow-capacity/`.

## Exact publication replay prepared; measurement pending (2026-09-16)

A complete one-million-recipient campaign capture on the preserved clean
`4f4acde0` binary exits zero at 17,167.46 recipients/sec. This one-cycle capture
is diagnostic, not sustained qualification. It retains 64 physical stores under
`target/workflow-capacity/publication-capture-20260916/source`. The immutable
inventory contains **5,525 data objects and 5,445 manifests**, totaling
**1,802,012,562 bytes**, with a SHA-256 for every input file. Input bytes remain
local rather than adding a 1.8 GB dataset to Git; the capture command and inventory
are archived in `fireweed-publication-replay-input-manifest.json`.

The new `vendor/object-log/examples/replay_publications.rs` diagnostic preloads
these actual contents and reconstructs original data chunk boundaries from
manifest locations. It rejects missing objects, unreferenced data, incomplete
partition prefixes, invalid ranges, and gaps/overlaps in object chunk coverage.
It publishes through public native `LocalBlobStore::put_chunks`/`put`, one ordered
stream per captured store with all 64 streams concurrent on a 16-worker Tokio
runtime. Each manifest follows durable completion of all its referenced data.
Each replay cycle uses a new destination, so it repeats the captured single-cycle
directory structure rather than the application's growing long-lived namespaces.

All six diagnostic tests pass, including dependency ordering, failure before
manifest publication, exact byte/media accounting, and detection of destination
corruption. Real-capture preflight also passes. After timed publication cycles,
the diagnostic reopens every store, compares every object byte-for-byte, and
checks the complete key sets. Source reading and final verification are outside
the cycle timers. No production implementation or qualification gate is changed.

Planned measurement: 32 copies (57.664 GB of logical publications), with serial
execution relative to every other workload/build and host/process monitoring.
The result must disclose exclusions: application/Turso work, command serialization,
manifest planning, online stage/reporting barriers, original engine runtime and
queue topology, and other catalog/mutable metadata publications. The 256 omitted
snapshot files total 98,944 bytes, but their overwritten publication history is
unknown; that snapshot size is not their original write volume. This diagnostic
can isolate publication cost under its stated scheduling. It cannot qualify the
workflow or be treated as a hardware ceiling under mixed projection/log traffic.

```sh
CARGO_TARGET_DIR="$PWD/target/object-log-tests" CARGO_BUILD_JOBS=8 \
  cargo build --locked --release --manifest-path vendor/object-log/Cargo.toml \
  --example replay_publications
target/object-log-tests/release/examples/replay_publications --inspect \
  target/workflow-capacity/publication-capture-20260916
target/object-log-tests/release/examples/replay_publications \
  target/workflow-capacity/publication-capture-20260916 \
  target/workflow-capacity/NEW-publication-replay-destination 32
```

## Directory-sync sharing rejected: too little concurrent coverage (2026-09-16)

All four serial one-million-row diagnostic runs finish with exit zero and identical
logical campaign counts. Both candidate runs use clean `5b65a281`, executable
`11bce25e1799f5ba91c93444ce817a4eddfd60223e978915d04a4c167a4ad26e`.
Controls use the preserved clean `4f4acde0` executable. Publication/VFS tracing and
user-mode perf counters match across the four runs; this is not qualification.

| Run | Recipients/sec | CPU-ms/recipient | Publications | Directory syncs avoided |
| --- | ---: | ---: | ---: | ---: |
| Control 1 | 16,668.46 | 0.855165 | 11,060 | 0 |
| Candidate 1 | 15,505.34 | 0.897354 | 11,090 | 28 (0.252%) |
| Candidate 2 | 14,666.95 | 0.874748 | 11,048 | 55 (0.498%) |
| Control 2 | 13,881.71 | 0.940339 | 11,008 | 0 |

The small number of shared barriers is direct evidence from successful native
LocalBlobStore publications. Mean total file-plus-directory sync calls actually
rise 0.13% (22,068 to 22,096.5) because grouping produces slightly more objects.
Mean throughput falls 1.24%, CPU cost falls 1.30%, instructions fall 0.73%, RSS
rises 1.93%, and logical WAL bytes fall 0.13%. CPU worsens in the first pair and
improves in the second; these results do not establish a useful causal gain.
There is no basis for spending a sustained qualification run on this candidate.

Restore `blob.rs` byte-for-byte from `af87903b`, removing the coordination state,
trace field and its implementation-specific unit tests. Retain the independent
64-object concurrent publication/reopen/accounting test. All 45 log contract tests
pass after rollback. Production source under `crates/` and `vendor/object-log/src/`
matches `af87903b`; its already-passing public workflow/recovery tests were not
repeated. Candidate validation separately includes 49 log and 12 public tests,
plus the two intentionally rejected unsafe coverage mutations. The rejected binary
is preserved at `/tmp/fireweed-workload-directory-sync-sharing-rejected`.
Evidence: `fireweed-directory-sync-screen-manifest.json`,
`fireweed-directory-sync-validation-manifest.json`, and
`fireweed-directory-sync-rollback-validation.json`.

The packer already groups commands, and each queue's metadata permit spans epoch
validation, durable produce and high-water publication. The measured publication
concurrency supplies very little remaining opportunity to share directory barriers.
Do not infer that increasing in-flight limits or dropping barriers is safe or useful.
The next diagnostic should capture a real campaign's immutable data/manifest
objects and replay their exact bytes through the native LocalBlobStore publication
API, preserving per-store data-before-manifest dependencies across 64 stores.
Preload source bytes outside timing and verify destination contents afterward;
report exclusions (application/Turso work, serialization, online stage barriers,
and any mutable metadata writes) explicitly. This isolates publication cost and
compression/file-size effects without substituting a synthetic-zero bandwidth
number for the real workflow. It is an isolation diagnostic, never a replacement
for the unchanged original-row qualification or evidence that its target is met.

## Directory-sync sharing candidate: barrier tests pass, performance pending (2026-09-16)

LocalBlobStore now registers a generation only after a file has been fdatasynced
and renamed. Under a per-parent mutex, a directory fsync snapshots the already-
registered generations, performs the syscall, and advances durable coverage only
on success. Followers whose generation is covered can then return without another
directory fsync. A rename registered during the syscall needs a subsequent sync.
There is no added linger, no file-sync removal, and no publication/manifest format
change. Different parent directories have separate coverage; cloned stores share
it. A weak registry retains only in-flight owners and prunes retired directories
on cache misses, avoiding growth with historical object directories.

This preserves the explicit parent-directory barrier described by the
[Linux fsync documentation](https://man7.org/linux/man-pages/man2/fsync.2.html).
The source ordering, rather than any assumption about a filesystem-specific
implicit directory flush, determines which renames may acknowledge. The diagnostic
`dir_sync_ops` field reports zero/one actual directory sync for each successful
publication. Media accounting counts every file sync and each performed directory
sync once. `dir_sync_us` includes time waiting for shared coverage; overlapping
publisher timings are not additive wall time.

All 49 log contract tests pass. New tests cover pending renames, arrivals during
sync, failure without coverage advancement, directory isolation/retirement, and
64 concurrent real publications followed by independent reopen/read verification.
Two deliberate incorrect implementations (coverage before syscall success and
including later renames) fail their respective tests. Evidence:
`fireweed-directory-sync-validation-manifest.json`. Public campaign/primitive/
log-only-recovery validation is next, followed by an unchanged one-million-row
serial ABBA screen with matching publication/VFS traces and perf counters.

The preserved clean control is `4f4acde0`, executable
`9f54d1207211f5c12104bac95e70289479d2fdecd53baa7562926ae60e6a3763`, at
`/tmp/fireweed-workload-before-directory-sync-sharing`. Its normal successful
flush path matches the restored per-engine runtime path. The candidate additionally
retains the independent startup-error and failed-committer draining fixes; the
screen's successful operations do not exercise those failure branches. Neither
unit sharing nor a traced screen is sustained qualification. Both fixed targets
and all existing workload/resource gates remain unchanged.

## Shared runtime rejected after sustained qualification (2026-09-16)

The complete serial qualification on clean `2e3674d3`, executable
`c627a3f1a1cbe51bcccd883411ce1292c378482dbcddbcf12879e23e10b19097`, finishes all
four workload children with exit zero. Both million-row primitive suites pass
all five floors. The first 8M campaign passes every 12.5k gate; the repeat fails
one 10k gate and five 12.5k gates. The wrapper correctly exits one. No diagnostics,
concurrent benchmark/build, host tuning or workload/gate changes were used.

| Campaign | Overall recipients/sec | Slowest cycle/sec | CPU-ms/recipient | Peak RSS GiB |
| --- | ---: | ---: | ---: | ---: |
| First | 15,419.08 | 13,449.95 | 0.865807 | 19.522 |
| Repeat | 12,043.94 | 8,907.78 | 0.919543 | 17.234 |

First-run cycle minima: 16,656.68 / 17,426.69 / 16,367.24 / 16,296.94 /
14,272.88 / 15,235.40 / 16,279.17 / 13,449.95. Repeat: **11,546.32** /
16,012.35 / 13,483.57 / 13,702.46 / **8,907.78 / 10,334.28 / 11,938.45** /
14,945.76. At 12.5k the repeat fails overall rate and cycles zero, four, five and
six; at 10k only cycle four fails. All other checks pass, including independent
row outcomes, reporting, due-claim latency, retention, sampled WAL budget,
materialized/stable projection files and RSS stability (2,278 checks per target).

| Primitive | First rows/sec | Repeat rows/sec |
| --- | ---: | ---: |
| Insert | 47,137 | 45,442 |
| Enrich by key | 42,573 | 42,907 |
| Schedule by ID | 51,604 | 62,993 |
| Claim and complete | 43,621 | 45,609 |
| Purge | 62,181 | 63,952 |

Against the preceding retained lease-index pair, mean rate is 3.89% lower,
CPU cost only 0.42% lower and RSS 5.65% lower. This sequential comparison does not
isolate causality from run variation, but supplies no sustained throughput gain
and fails base qualification. Remove runtime pooling and its implementation-only
identity/capacity assertions. Restore the former per-engine runtime construction;
retain independent engine close/commit coverage, explicit accepted-upload draining
on failure, and fail-closed counter initialization with their regression tests.
The rejected binary is preserved at `/tmp/fireweed-workload-shared-log-runtime-rejected`.
After rollback, all 44 log contract tests and 12 public campaign/primitive/recovery
tests pass, with zero failures or ignored tests. Logs and hashes are in
`fireweed-shared-runtime-rollback-validation.json`.

The repeat's maximum-busy sampled window starts 340.946 seconds into observation:
11.242 seconds at 99.98% busy, 38.12 MiB/sec writes, 1,194.66 write IOPS,
235.28 ms mean write-request latency, 281.09 weighted outstanding I/Os and just
1.43 workload CPU-seconds/sec. These are host-wide counters; latency includes
queueing. They demonstrate storage-path waiting, not a device bandwidth ceiling
or a proof that pooling caused the stall. No hardware tuning is justified by this
comparison. Reports, full gate results, raw device samples and reproduction scripts
are archived in `fireweed-shared-log-runtime-sustained-manifest.json`.

Next investigate redundant directory durability barriers in LocalBlobStore.
Every write currently fdatasyncs its temporary file, renames it, and separately
fsyncs the parent. A bounded experiment may let already-completed renames share a
successful parent sync, with no added linger. It must preserve file-sync-before-
rename ordering and acknowledge each write only after a sync that started after
its rename. Tests must distinguish renames before versus during a sync, prohibit
advancing coverage after failure, and isolate different directories. Then measure
actual sync reduction and an unchanged serial screen before sustained qualification.
This is a proposed experiment, not an implemented optimization or accepted gain.
The retained performance evidence remains repeated 10k on the lease-index baseline;
repeated 12.5k remains unproved.

## Shared-runtime screen and log startup failure (2026-09-16)

The serial control/candidate/candidate/control screen is complete on candidate
`74608459`, executable `693faa1693643119aacd6ecd8d0fda416c2d4e54dbe9b77228f3dfefae481f18`.
All four one-million-recipient processes exit zero. Relative to the retained
`4f4acde0` control, mean CPU time falls **3.37%**, user-mode instructions **1.22%**,
and peak RSS **8.15%**. Mean throughput falls **1.95%** (15,272.84 to 14,975.55/sec),
so a throughput benefit is unproved. Mean sampled maximum thread count falls
from 1,096.5 to 522; these are sampled process totals, not runtime-worker counts.
Projection WAL bytes rise 0.26%. At 12.5k/sec the measured CPU cost implies
10.764 CPU-seconds/sec versus 11.139 for control; this extrapolation is not a
throughput ceiling or sustained qualification. The modest CPU/resource savings
justify running the unchanged untraced two-repeat qualification. Evidence and
reproduction scripts: `fireweed-shared-log-runtime-screen-manifest.json`.

A separate startup failure-injection test then reproduced an existing durability
bug: an object-listing error defaulted the recovered data counter to zero, and
the next successful produce overwrote sealed object 1. The failing assertion
shows its original bytes replaced with the new payload. Initialization now fails
admission and flush waiters before any PUT on listing failure. Successful counter
recovery and log format are unchanged. This is an independent correctness fix,
not the cause of the screen's resource improvement. Qualification must use the
new fixed revision; the screen above predates this fix. All 45 log contract tests
and 12 public campaign/primitive/recovery tests pass after the fix (zero failures
or ignored tests). Red/green logs: `fireweed-counter-listing-validation-manifest.json`.

## Shared log flush-runtime candidate: correctness passes, performance pending (2026-09-16)

The candidate shares Tokio flush runtimes across engines with the same selected
worker count. Each runtime has at most 64 owning engines, leaving room for I/O in
Tokio's default blocking pool alongside synchronous commit jobs. Additional engines
use another pool. Registry entries are weak: the last owning flush thread drops
the runtime outside async context, and closing one engine cannot shut down its
siblings. Per-engine queues, byte budgets, in-flight limits, grouping, ordered
committer and worker-count selection are unchanged. Manifest sequencers retain
their own existing runtimes; only flush execution resources are shared.

A shared runtime can outlive a failed engine, so runtime destruction can no longer
serve as its implicit I/O drain. After a committer panic, admission and waiters still
fail closed, but the engine now explicitly awaits already-started PUTs before its
flush thread exits. Those uploads are never sequenced past the failure. This keeps
close/reopen/orphan inspection from racing unfinished writes; it can wait for a
started upload to return even after the commit failure. Normal shutdown continues
to drain buffered work and await manifest durability.

The pre-change test fails because two actual data-PUT paths use different runtime
IDs. The candidate passes 44 log tests and 12 public campaign/primitive/recovery
tests. New coverage verifies runtime identity through real PUTs, independent
commit/close while a sibling manifest is blocked, survival after sibling close,
bounded pool ownership/retirement, and failure draining followed by reopen without
publishing the failed prefix or reusing object IDs. No source or test relies on
the retired SQLite backend. Evidence is in
`fireweed-shared-log-runtime-validation-manifest.json`.

Next run the unchanged serial control/candidate/candidate/control one-million-row
screen against `/tmp/fireweed-workload-before-shared-log-runtime` (clean source
`4f4acde0`, SHA-256
`9f54d1207211f5c12104bac95e70289479d2fdecd53baa7562926ae60e6a3763`). Both variants
use identical VFS and user-mode perf counters, plus sampled thread counts. Verify
that sharing occurs on the actual workload path, then assess CPU/instructions,
throughput, RSS and writes before deciding on full sustained qualification. The
fixed repeated 12.5k target and all five primitive floors remain unchanged.


## Checkpoint timing does not justify background execution as the next fix (2026-09-16)

The complete traced campaign on clean `4f4acde0`, executable
`9f54d1207211f5c12104bac95e70289479d2fdecd53baa7562926ae60e6a3763`, exits zero
with all eight million original recipients processed and verified. Overall rate
is 15,620.86/sec, CPU cost 0.871341 ms/recipient and peak RSS 19.479 GiB.
All 2,278 stretch checks except the diagnostic-override check pass. That explicit
failure is correct: this run is **not qualification**, and the goal remains open.
Cycle minima are 17,145.74 / 15,986.84 / 16,113.76 / 16,047.51 / 14,532.84 /
15,356.52 / 16,236.76 / 15,817.55 recipients/sec.

There are 106 auto-checkpoint attempts, all within the interval between complete
cycle-three and cycle-four report groups. All 64 stores report successful
checkpointing: 101 attempts succeed, while five fail in 8–12 microseconds and
are followed by success. The trace does not record their error causes; do not
classify them as I/O failures or assume a specific busy condition. These are
post-commit maintenance results, not failed campaign commands.

Median attempt time is **0.071972 seconds**, maximum attempt **1.570503 seconds**,
and maximum cumulative time for any store **1.590187 seconds**. The sum across
all stores is 13.711741 seconds, but those operations overlap and that sum is
not lost wall time. The slowest campaign in cycle four belongs to store four:
its whole cycle takes **68.805 seconds**, while that store spends only
**0.115326 seconds** in measured checkpoint execution. No further auto-checkpoint
attempts occur during cycles five through seven.

This confirms the foreground blocking path but weakens it as the main explanation
for multi-second deficits in the slower untraced runs. It does not establish an
upper bound for other hardware states or exclude indirect contention between
projection writes and durable-log publication. Do not change checkpoint thresholds,
add background checkpoint ownership or revisit reader-construction checkpointing
on this evidence. Checkpoint writes already sort pages and group contiguous pages
into vectored writes (`WriteBatch` / `write_pages_vectored`); missing batching is
not an established issue either.

The next bounded candidate is sharing **log flush runtimes across engines**, not
reducing workers separately within every engine (the earlier one-worker experiment
was rejected). `vendor/object-log/src/engine.rs::flush_loop` constructs its own
Tokio runtime per engine, normally eight workers; the campaign has 64 engines.
That implies 512 runtime workers before flush threads, blocking I/O and other
runtimes. This is structural evidence and a hypothesis about overhead, not proof
of a bottleneck. A candidate must preserve per-engine in-flight limits, commit
order, completion ownership, independent shutdown and durability, and must be
screened with identical instrumentation before sustained qualification.

The exact current executable is preserved at
`/tmp/fireweed-workload-before-shared-log-runtime` for that comparison. Full report,
checkpoint events, analysis, device samples, scripts and binary provenance are
archived with verified hashes in `fireweed-auto-checkpoint-diagnostic-manifest.json`.
The latest retained untraced qualification remains the lease-index pair below:
repeated 10k passes, repeated 12.5k unproved. No source edits, other workloads,
compilation or storage-setting changes overlapped this diagnostic.


## Measure foreground auto-checkpoint elapsed time (2026-09-16)

Source inspection establishes a blocking path: native `Pager::commit_tx` runs its
`AutoCheckpoint` state before returning completion, and Fireweed's `apply_owned`
retains the writer mutex through `transaction.commit().await`. This does not
establish how much campaign time that path costs. Existing apply traces combine
WAL commit and checkpoint time, so the next diagnostic separates that phase.

An opt-in timer, enabled by the existing `FIREWEED_PROJECTION_IO_TRACE` flag,
records each auto-checkpoint's database path, start wall-clock timestamp, elapsed
microseconds, terminal status and frame/backfill positions. The timer starts after
WAL publication and includes transaction-lock release, checkpoint I/O yields and
scheduling delays through the terminal checkpoint result. It excludes subsequent
savepoint cleanup and trace emission. It measures elapsed foreground occupancy,
not pure CPU, device service time or bytes; frame positions are not physical
write counts. Timers reset on commit cleanup, and the disabled path reads no clock.
No checkpoint threshold, WAL limit, reader ownership or durability rule changes.
The recorder already identifies this flag as a diagnostic override, so these runs
cannot pass qualification.

The native 24-pooled-reader/WAL-freeze regression passes with trace emission and
successful checkpoint events. Its temporary filesystem is RAM-backed: this checks
trace execution and preserved reader behavior, not local disk performance. Raw
output and verified digest are recorded in
`fireweed-auto-checkpoint-trace-validation.json`.

Next measure the complete original-row 1M-resident/8M-recipient campaign on the
repository filesystem, with identical batches, handlers, workers, reporting,
retention and fixed checkpoint budget. Compare actual per-store checkpoint delays
with the remaining cycle budget before considering background execution. No
explicit checkpoint calls are added to reader construction or production paths.


## Compact priority format rejected after complete measurement (2026-09-16)

Restore production priority codecs and all six writers exactly to `9339e1b9`.
The candidate reduced logical WAL bytes but did not establish a sustained CPU,
physical-write or throughput improvement sufficient to justify another projection
format. Keep native Turso regression coverage for all four priority types across
reopen, payload enrichment, priority preservation/replacement/clearing and expected
eligibility order. Eight focused native/public recovery tests pass on the restored
code. The earlier facade SQLite test port remains in `61b4b80f`; this experiment
adds no SQLite backend coverage.

The serial control/candidate/candidate/control one-cycle screen used clean
`e6c9ce25` and candidate executable
`e315db6b5b6b1e7cdb4a35c16cb353a44eaf0728d86132847465a7b29214c76d`, versus
retained lease-index executable
`5fae9e11e803ab456411bbf3a1cb29f40fb52e47cbb689563d1fce6e2c6085c0` from
`1b8cb97c`. All children exited zero, all 128 campaigns' logical counts match,
and user-mode counters have 100% running coverage. Identical VFS/perf tracing
makes these diagnostic runs, not qualification.

| Run | Recipients/sec | CPU-ms/recipient | Instructions, trillions | Requested WAL bytes |
| --- | ---: | ---: | ---: | ---: |
| Control 1 | 16,437.81 | 0.851850 | 2.591683 | 6,624,084,488 |
| Candidate 1 | 16,281.55 | 0.850283 | 2.585943 | 6,271,074,648 |
| Candidate 2 | 15,100.79 | 0.929397 | 2.656588 | 6,382,388,808 |
| Control 2 | 13,414.40 | 0.940059 | 2.639519 | 6,802,381,608 |

Mean logical WAL bytes decrease **5.76%**, with a decrease in both pairs. Mean
CPU time changes -0.68%, instructions +0.22%, cycles +0.87%, RSS +3.37%, and
rate +5.13%. Large control timing variation prevents treating the rate difference
as a stable gain. Host writes do not repeat the VFS reduction: candidate/control
pairs are 2.877/3.269 and 3.083/2.787 GiB, with opposite signs and monitor gaps.

The complete untraced qualification then used the same clean candidate and binary
for both eight-million-recipient campaigns and both million-row primitive suites.
Every child exited zero; the wrapper exited one for the single stretch failure.
Both campaigns pass all **10k** gates. The first passes all **12.5k** gates;
the repeat fails only cycle four's rate. All non-rate checks pass, including full
row/outcome verification, reporting, due-claim latency, WAL limits, materialized
main-file stability and RSS stability. There are 2,278 checks per campaign target.

| Candidate campaign | Overall recipients/sec | Slowest cycle/sec | CPU-ms/recipient | Peak RSS GiB |
| --- | ---: | ---: | ---: | ---: |
| First | 14,789.93 | 13,064.85 | 0.885654 | 18.923 |
| Repeat | 13,412.06 | 11,863.58 | 0.910395 | 18.972 |

First cycle minima: 17,195.97 / 16,009.76 / 15,822.55 / 16,019.94 /
13,539.11 / 16,032.59 / 13,723.51 / 13,064.85. Repeat minima: 12,573.65 /
15,596.98 / 16,024.76 / 15,559.21 / **11,863.58** / 12,586.87 /
13,050.69 / 13,733.85. Every store first checkpoints in cycle four. The repeat's
slowest campaign spends 10.83/24.73/33.87/0.23/9.33 seconds in
load/prepare/delivery/verify/purge within an 84.286-second total that also includes
other work and barriers. A timestamp association is not proof that checkpoint
execution itself accounts for the missing 4.286 seconds.

| Primitive | First rows/sec | Repeat rows/sec |
| --- | ---: | ---: |
| Insert | 43,496 | 38,335 |
| Enrich by key | 43,383 | 32,703 |
| Schedule by ID | 53,854 | 54,666 |
| Claim and complete | 45,541 | 82,264 |
| Purge | 62,258 | 60,896 |

All five primitive counts/windows/rates pass the strengthened gate in both runs.
Against the preceding retained qualification, arithmetic mean campaign rate is
1.30% lower, CPU cost 0.17% higher and sampled host writes only 0.34% lower.
These sequential, variable runs do not establish a regression either, but provide
no convincing sustained improvement to justify retaining the format change.

The current retained baseline and its napkin budget remain the lease-index results
below: repeated 10k passes, repeated 12.5k still open. Do not promote the rejected
candidate's smaller VFS budget or its higher worst-cycle rate into retained claims.
Next investigate whether native checkpoint execution blocks foreground progress
and how that interacts with durable-log publication. Preserve checkpoint/WAL
budgets and workload semantics; establish the execution path before changing it.
This is a code investigation, not another SSD setting or a proven diagnosis.

The complete screen, qualification reports, device samples, scripts, rejected
binary provenance and restored-code test log are archived with verified hashes in
`fireweed-compact-priority-results-manifest.json`. Candidate correctness logs remain
in `fireweed-compact-priority-validation-manifest.json`. No benchmark, test or
build overlapped another workload; no host/storage settings changed.


## Compact stored priority candidate validation (subsequently rejected; 2026-09-16)

The next bounded candidate replaces verbose priority enum JSON in the projection
column with tagged arrays. Timestamp values sampled from this campaign save
34 bytes per stored priority. This is a column-size calculation, not a measured
WAL, physical-write or throughput improvement. Sort keys, metadata, public serde,
log serialization, durability and workload/gates are unchanged.

The dedicated projection decoder accepts existing object-form rows and compact
arrays. Direct typed sequence decoding preserves signed 128-bit decimals and
scale; malformed tags, missing/extra fields and numeric coercions fail closed.
All six relational priority writers use the dedicated encoder, including fused
replacements and regular updates. Existing rows do not need a rewrite. Older
binaries cannot read new compact rows; downgrade requires rebuilding the disposable
projection from the unchanged log, not reusing the new projection file.

Validation: the compact decode test failed before implementation; afterward all
11 relational tests, 76 native tests and 12 public workload tests pass, with two
existing native tests ignored. Native mixed-format coverage exercises timestamp,
integer, decimal and text rows across reopen, Keep/Set/clear, payload enrichment
and independent expected eligibility order. Public tests cover campaign chunks,
windows, reporting, discovered retention, primitives and log-only recovery.
Raw logs and verified hashes are in
`fireweed-compact-priority-validation-manifest.json`.

Next run the serial control/candidate/candidate/control one-million-recipient
counter/VFS screen against the retained lease-index executable
`5fae9e11e803ab456411bbf3a1cb29f40fb52e47cbb689563d1fce6e2c6085c0`
(source `1b8cb97c`). Performance is pending, and neither the screen nor these
correctness checks establish the repeated 12.5k qualification target.


## Lease-index sustained results: base target passes, stretch remains open (2026-09-16)

All four serial workload children exited zero on clean `1b8cb97c`, executable
`5fae9e11e803ab456411bbf3a1cb29f40fb52e47cbb689563d1fce6e2c6085c0`, with empty
diagnostics. Both eight-million-recipient campaigns pass every **10k** gate.
First passes every **12.5k** gate; repeat fails only three cycle-rate checks.
The shell exits one for those failures, after completing both primitive suites.

| Campaign | Overall recipients/sec | Slowest cycle/sec | CPU-ms/recipient | Peak RSS GiB |
| --- | ---: | ---: | ---: | ---: |
| First | 15,350.12 | 12,866.97 | 0.880054 | 19.072 |
| Repeat | 13,223.67 | 11,260.08 | 0.912916 | 19.887 |

First-run cycle minima are 17,689.14 / 17,716.54 / 16,286.12 / 15,333.98 /
14,170.47 / 16,142.14 / 15,790.38 / 12,866.97. Repeat minima are 13,112.83 /
15,868.76 / 15,965.28 / 15,359.66 / **12,406.34 / 11,260.08** / 12,949.05 /
**12,271.17**. All 2,278 checks pass at 10k in each campaign; at 12.5k only
repeat cycles four, five and seven fail. Correctness, full-row oracles, progress,
due-claim latency, sampled WAL, main-file materialization/stability and RSS
stability all pass. The worst cycle needs about 11.0% more throughput: its
88.804-second wall must fall below 80 seconds. The goal is not complete.

| Primitive | First rows/sec | Repeat rows/sec |
| --- | ---: | ---: |
| Insert | 45,703 | 45,433 |
| Enrich by key | 45,006 | 42,497 |
| Schedule by ID | 55,293 | 62,428 |
| Claim and complete | 45,326 | 43,424 |
| Purge | 61,420 | 58,029 |

Both primitive reports pass the strengthened five-operation gate, including full
million-row counts and reconciliation of each rate with its measured phase window.
All reports share one source and binary; no benchmark/build overlap or host
setting change occurred.

The repeat's first main-file checkpoint occurs in cycle four on every store.
Its next cycle is slowest: load/preparation/delivery/purge are 16.22 / 32.20 /
26.21 / 7.72 seconds for the slowest campaign, within an 88.804-second total that
also includes other work/barriers. These phase values are not independent device
costs. A 12.020-second maximum-busy observed rolling window records 98.48% device
busy, 48.98 MiB/sec host writes, 863.81 write IOPS, 144.18 ms mean write-request
latency, 124.72 mean outstanding I/Os, and only 4.46 workload CPU-seconds/sec.
This confirms remaining storage-path waits; it does not identify an SSD ceiling
or isolate filesystem, durable-log synchronization and controller costs.

Retain the lease-index change as the modest CPU improvement supported by the
balanced screen and successful base qualification; it has not solved the late
stalls. Preserve this executable as `/tmp/fireweed-workload-after-lease-index`
for the next comparison. Further work should examine actual hot-row serialization
and write volume, with legacy projection decoding and public/log formats preserved,
rather than rerun the rejected checkpoint-policy tweak or change hardware settings.

Complete reports, strengthened gate summaries, phase/device analysis, raw samples,
exact scripts and preserved-binary provenance are archived with verified hashes
in `fireweed-lease-index-sustained-manifest.json`. The fixed repeated 12.5k target,
all-five primitive floors and every workload/resource gate remain unchanged.


## Lease-index screen improves CPU; sustained qualification follows (2026-09-16)

The complete serial control/candidate/candidate/control screen uses the original
million-row metadata/timestamp-priority campaign, 64 stores, two workers and two
loaders per campaign. Candidate source is clean `5e41dfc4`, executable
`76c2bbe999fbb5d96a327e0e5afd897c300a639aa13216ac1e7f60a7880b4b42`;
control is the preserved `2e8f8ff5` executable `9124cfd7...`. Both enable identical
projection VFS accounting and user-mode instruction/cycle counters. Every child
exits zero, all write-error counters are zero, hardware-counter running coverage
is 100%, and all 128 campaigns perform identical logical work across all four runs.

| Run | Recipients/sec | CPU-ms/recipient | User instructions (trillions) | Requested WAL bytes |
| --- | ---: | ---: | ---: | ---: |
| Control 1 | 15,575.62 | 0.883208 | 2.685732 | 6,583,481,888 |
| Candidate 1 | 15,923.56 | 0.878957 | 2.643084 | 6,596,583,488 |
| Candidate 2 | 15,007.26 | 0.905949 | 2.642492 | 6,554,209,288 |
| Control 2 | 14,541.42 | 0.953333 | 2.710068 | 6,630,165,608 |

Candidate arithmetic means improve rate **2.70%**, CPU time **2.81%**, instructions
**2.04%** and cycles **1.38%**. Paired CPU reductions are 0.48% and 4.97%; paired
instruction reductions are 1.59% and 2.49%. This supports a modest CPU improvement,
with substantial control timing variation. WAL bytes improve only 0.48% on average
(one pair increases 0.20%, the other decreases 1.15%), so this is not demonstrated
relief from the sustained write stalls. Peak RSS means differ by only -0.21%.

These one-cycle instrumented observations do not qualify either sustained target.
Retain the candidate for the unchanged serial campaign/primitive/campaign/primitive
qualification: each campaign processes eight million recipients with one million
resident, and every primitive must process one million rows at 10k/sec under the
strengthened count/window/rate gate. No tracing or runtime override is enabled in
qualification. Archive complete reports, device samples, counter CSVs, exact
recorder/runner, analysis and build log in `fireweed-lease-index-screen-manifest.json`.


## Consolidated lease-index candidate validated (2026-09-16)

Native ordinary-expiry and pending-lease reads are queue-scoped and return item-ID
order. The candidate replaces the two overlapping leased indexes with one partial
index over `(tenant_id, queue_id, item_id, lease_expires_at, retry_count,
cohort_size, fenced, superseded)` for all `Leased` rows. Ordinary expiry filters
remain covered; the broader pending view still includes fenced/cohort leases.
The PostgreSQL adapter maintains its separate schema unchanged. Each ordinary
lease that actually reaches SQL now maintains one leased tree instead of two;
existing claim/mutation fusion already skips some intermediate leases, so the
remaining benefit must be measured rather than inferred from logical claim count.

An expiry-first candidate failed the query-plan regression: Turso selected the
main primary-key index. The retained candidate puts the required item-ID order
first and covers the actual pending-view fields. Both real queries now select
the new index without forced-index SQL. Expiry is filtered while scanning the
ordered leased subset; this is not a claim of an expiry-range seek or constant
work with arbitrarily large outstanding-lease backlogs.

The file-backed regression creates the previous indexes and mixed ordinary,
fenced, cohort, superseded, non-leased and other-queue/tenant rows. Two migrations
and a reopen preserve the complete rows. Independent expected results check
strict expiry and broader pending visibility, and EXPLAIN checks index selection.
The test is explicitly gated on the optional `local` feature; Cargo metadata
verifies that boundary. The older native reclaim tests are renamed to describe
their actual assertions rather than claiming a SQLite comparison.

All **3 focused checks, 65 adapter/cancellation/concurrency/recovery checks and
12 public campaign/primitive/log-only recovery checks pass**. Two existing adapter
diagnostics remain ignored. The public workload keeps original IDs and row
metadata, due-window delivery, retries, progress/disposition and discovered
retention; no workflow, durability, checkpoint or resource gate changes.
Evidence: `fireweed-lease-index-validation-manifest.json`, including the red
migration regression and rejected expiry-first plan.

Performance is unmeasured. Next compare control/candidate/candidate/control,
one million recipients each, with identical user-mode counters and projection
VFS tracing. Use the preserved `2e8f8ff5` control executable (`9124cfd7...`), whose
production behavior matches the restored pre-candidate baseline. Compare logical
work, CPU, instructions, WAL bytes and wall rate; do not qualify from a one-cycle
diagnostic or overlapping VFS elapsed times. Only a supported improvement advances
to the unchanged repeated eight-cycle qualification.


## Reject checkpoint staggering; enforce every primitive floor (2026-09-16)

The complete serial qualification used clean `7aaf819343ab7f9acb2ff2f3cd34b95c137f939f`
and binary `b77ddaf077934e3c62daff69d2188d3e8658cb01a3cd09dc94a2365ae2157216`.
All four workload children exited zero, with empty diagnostics and no overlap;
the qualification shell exited one because the campaign rate gates failed.

| Campaign | Overall recipients/sec | Slowest cycle/sec | CPU-ms/recipient | Peak RSS GiB |
| --- | ---: | ---: | ---: | ---: |
| First | 14,151.16 | 9,425.56 | 0.904802 | 19.152 |
| Repeat | 11,345.44 | 8,774.47 | 0.966630 | 20.086 |

First-run cycle minima: 16,471.55 / 16,202.28 / 15,661.31 / 15,453.14 /
15,534.90 / 15,016.83 / 14,554.31 / 9,425.56. Repeat: 11,635.21 / 13,748.35 /
14,765.60 / 14,318.93 / 11,106.29 / 10,255.12 / 10,101.19 / 8,774.47.
Both fail the 10k floor only in cycle seven. At 12.5k, the repeat additionally
fails overall rate and cycles zero, four, five and six. Every correctness,
progress, due-claim, WAL, database-size and RSS-stability check passes.

The first main-file checkpoint did spread: materialized physical stores at the
ends of cycles two/three/four were 10/55/64, then 20/48/64 in the repeat. That
did not prevent later stalls. The repeat also missed 12.5k before any main file
materialized, contradicting checkpoint synchronization as a complete explanation.
The busiest observed rolling windows used 97.05% / 99.99% device busy time,
52.11 / 18.61 MiB/sec host writes, 166.59 / 310.07 ms mean write-request latency
and only 5.06 / 4.27 workload CPU-seconds per wall second. These are host-wide
queueing observations, not drive bandwidth ceilings or isolated device-service
measurements. No host settings changed.

Reject the path-derived 256–448 MiB policy and restore the previous 448 MiB
budget. Retain the added reopen readback check; both checkpoint configuration
tests pass after restoration. This is rejection for failing
qualification, not a controlled estimate of causal regression against a
contemporaneous old-policy run. Both complete measurements and their failures
remain evidence; the repeat was not stopped when its early rate failed.

| Primitive | First rows/sec | Repeat rows/sec |
| --- | ---: | ---: |
| Insert | 46,667 | 46,392 |
| Enrich by key | 43,312 | 42,313 |
| Schedule by ID | 56,161 | 113,561 |
| Claim and complete | 84,214 | 45,534 |
| Purge | 64,261 | 58,863 |

The gate previously enforced only the first three floors. New regression tests
reproduce accepting an omitted purge phase and an incomplete row count. The
fixed gate requires all five phases to process the full reported resident count,
checks finite positive phase windows and rate = records/window, and enforces
10k for every phase. Exact-floor cases pass; missing phases, incomplete work,
nonfinite values and inconsistent or sub-floor rates fail. All 18 harness tests
pass. Re-evaluating the saved reports under the stronger gate preserves both
primitive passes and both campaign failures; original reports are not rewritten.

Evidence, original/strengthened gate summaries, policy derivation, raw device
samples, analysis scripts and validation logs are preserved in
`fireweed-staggered-checkpoints-repaired-results-manifest.json`. The goal remains
unmet. The next bounded code investigation is consolidating the two lease indexes:
current native expiry and pending-lease reads are queue-scoped, but each ordinary
lease maintains both a queue-scoped partial tree and a global leased tree.
A candidate must preserve fenced/cohort filtering and pending-lease reads,
prove its query plans, and demonstrate lower work in a serial campaign comparison.


## Repaired-host checkpoint staggering candidate (2026-09-16)

The candidate derives each rebuildable projection's checkpoint budget from its
configured database path using FNV-1a, in the **256–448 MiB** range. The same path
keeps its budget across reopen; no new durable record, global counter or host
setting is needed. `checkpoint_frames` uses the actual page size, and the
existing 448 MiB upper budget remains. Standalone projections keep their existing
1,000-frame policy; committed read-only connections keep automatic checkpointing
disabled. The native log-backed writer applies and verifies this setting on the
active path. The custom metadata JSON decoder has already been removed.

The lower bound preserves a larger coalescing window than the earlier pre-repair
192–448 MiB trial. This is a fresh scheduling experiment motivated by the measured
all-store checkpoint burst, not evidence that staggering already improves
throughput. Real adapter tests verify the applied setting for new and existing
page sizes, reopen stability and the standalone policy; a 64-path check guards
against all stores receiving one window. The adapter library plus cancellation,
concurrency and recovery suites pass **66 tests, zero failures, two existing
ignored diagnostics**. All **12 public campaign/primitive/recovery tests pass**;
the standalone workload build passes. Logs and hashes are archived in
`fireweed-staggered-checkpoints-repaired-validation-manifest.json`. Performance
remains unmeasured.

The unchanged repeated eight-cycle qualification will record the path-derived
budgets alongside each workload's command and host-device measurements. Those
budgets are calculated policy metadata, not independent live SQL observations.
All workload counts, handler limits, fairness, log durability, reporting latency,
WAL budget and storage/RSS stability gates remain required. No SSD settings change.

## Reject custom JSON decoder; measured checkpoint burst (2026-09-16)

The first untraced eight-cycle campaign completed successfully at **13,673.93
recipients/sec overall**, 0.902964 CPU-ms/recipient and 18.917 GiB peak RSS.
Clean source `6bea8067` produced executable `be199017…`; diagnostics were empty.
Of 2,278 checks at either target, only cycle 4's rate failed: **9,678.17/sec**.
Cycle minima were 17,159.97 / 16,079.85 / 15,868.92 / 16,081.51 / 9,678.17 /
12,720.59 / 14,055.85 / 12,744.19. All correctness, progress, due-claim, WAL and
storage/RSS stability checks passed. This is not repeated qualification.

Source review found a compatibility gap outside JSON text: the new visitor
omitted accepted human-readable Serde 128-bit integer and `Some` inputs, and
changed nonfinite float inputs from the previous null result to rejection.
Three new tests reproduced all three failures. Restore the previous production
implementation byte-for-byte to `405c671f`; retain those regression tests.
The complete restored core suite passes **123 tests, zero failures/ignores**.
The candidate's small diagnostic averages did not justify a custom decoder
with incomplete compatibility. Its earlier JSON-only equivalence checks were
insufficient to prove the full generic Serde contract.

The qualification shell was paused while its first campaign and recorder kept
running. After the completed report was saved, that paused shell was deliberately
terminated. No primitive suite or second campaign started. The wrapper's -15 exit
is this controlled stop, not a workload crash or timeout; the campaign child
exited zero. Logs, exact regression inputs, reports and analysis are archived in
`fireweed-direct-json-sustained-manifest.json`.

The remaining performance evidence points to a larger issue. Every physical
projection was still 4 KiB at the end of cycle 3; all 64 had materialized roughly
31 MiB main files by cycle 4. For the slowest campaigns, delivery grew from
16.75 to 57.39 seconds while preparation stayed about 25 seconds. A nearby
11.643-second device interval, starting 292.106 seconds after monitor start,
recorded **99.52% device busy, 57.99 MiB/sec host writes, 1,955.94 write IOPS,
120.18 ms mean write-request latency and 235.23 mean outstanding I/Os**.
The workload used only **1.246 CPU-seconds per wall second** in that interval.
These host counters establish a storage stall during the burst. Request latency
includes queueing; this is not an SSD bandwidth ceiling or a diagnosis of
controller versus filesystem behavior.

The next code experiment staggers rebuildable projection checkpoint thresholds
while retaining the existing 448 MiB upper budget. The earlier 192–448 MiB
attempt failed before the local storage repair; it is not evidence that the
repaired-host checkpoint burst is harmless. The fresh phase and device evidence
justify revisiting scheduling, with no SSD settings, log durability changes,
extra workflow records or weakened qualification gates.

## Direct metadata JSON screen; advance to sustained measurement (2026-09-16)

The serial control/candidate/candidate/control screen completed correctly.

| Run order | Recipients/sec | CPU-ms/recipient | User instructions (trillions) |
| --- | ---: | ---: | ---: |
| Control 1 | 15,641.69 | 0.86472 | 2.67273 |
| Candidate 1 | 14,918.02 | 0.91246 | 2.70436 |
| Candidate 2 | 14,936.44 | 0.90123 | 2.66915 |
| Control 2 | 13,614.89 | 0.95537 | 2.72386 |

Arithmetic mean throughput improves 2.04%, CPU time falls 0.35%, instructions
fall 0.43%, user cycles fall 1.60%, and peak RSS falls 0.53%. These small average
differences do not establish a repeatable benefit against the control variation.
Both candidate runs used the same `466c764…` executable from `13488cd8`.
All four runs have identical item, claim, enrichment, disposition, retry, verify,
purge and payload-accounting counts for all 128 campaigns. Complete outputs,
provenance, counters, device telemetry and calculations are archived in
`fireweed-direct-json-screen-manifest.json`.

Proceed to the unchanged two eight-cycle campaign qualifications, each followed
by the million-row primitive suite. This is an experiment to establish sustained
behavior, not a claim that a 2% short-screen average closes the prior 7.4% gap.
The code and all gates remain fixed during measurement. No hardware settings
change, and the runtime has no diagnostic overrides during qualification.

The prior baseline's slowest cycle (cycle 5, shard 37/campaign 1) spent 18.15 s
loading, 33.04 s preparing, 25.72 s delivering, 0.24 s in final verification and
5.79 s purging, within 85.93 s total. Some waits lie outside individual phase
timers; delivery includes window barriers. The phase analysis is archived with
the screen. Dropping the final verification cannot explain or close this gap;
its independent row oracle remains mandatory.

## Direct scalar/array metadata JSON decoding candidate (2026-09-16)

Relational metadata decoding deserializes a map of `MetadataValue` entries. Each
entry previously became a `serde_json::Value` first; natural arrays then allocated
a second vector while converting their children. The campaign persists an array
of top times, so this path is exercised during real projection reads. The existing
allocator profile attributes work to metadata decoding during addressed planning,
claim rendering and retained-item reporting; it does not quantify the saving of
this particular change.

The candidate deserializes scalar and array entries directly into their final
metadata types. Objects still use the existing compatibility decoder, preserving
legacy tags, decimal objects and wrappers. Binary/postcard encoding and decoding,
stored JSON, SQL, public response contents, workload and qualification gates are
unchanged. The old tree conversion remains the test oracle: values and rejection
behavior agree for integer limits, floats, malformed JSON, escaped strings,
legacy forms, decimal objects and nested arrays. All **31 core library tests
pass**. The complete core suite passes **121 tests**, with 0 existing ignores;
all **12 public campaign/primitive/recovery tests pass**, and the standalone
workload builds successfully. Validation output and hashes are archived in
`fireweed-direct-json-validation-manifest.json`. Performance remains unmeasured.
The next diagnostic uses control/candidate/candidate/control ordering to balance
first-versus-last placement within four serial runs.

A separate build observation explains repeated native compilation: Turso's
`build.rs` watches repository HEAD and embeds it in `sqlite_source_id()`, so a
documentation-only commit can invalidate the native crate. This build behavior
has not been changed as part of the decoder experiment. Build/test activity must
still finish before workload measurement begins.

## Reject response ownership after reversed-order controls (2026-09-16)

The reverse-order screen completed correctly:

| Run order | Recipients/sec | CPU-ms/recipient | User instructions (trillions) |
| --- | ---: | ---: | ---: |
| Candidate 1 | 16,321.01 | 0.85869 | 2.64687 |
| Control 1 | 15,491.87 | 0.90661 | 2.66888 |
| Candidate 2 | 13,544.88 | 0.95285 | 2.66637 |
| Control 2 | 14,805.65 | 0.95924 | 2.68511 |

Across all eight screens, arithmetic mean candidate instruction count fell
1.08%, user cycles fell 1.01%, and CPU time fell only 0.62%. Mean throughput
fell 4.21% (14,650 versus 15,293 recipients/sec); peak RSS averaged 0.38% higher.
Three of four adjacent comparisons have lower candidate throughput. Reversing
order demonstrates variability but does not establish a useful performance
benefit. Remove the candidate and restore `turso_compose.rs` byte-for-byte to
`b195e379`. Its 166 passing tests establish correctness, not performance.
Do not spend sustained qualification runs on this change.

All eight reports match on items, claims, dispositions, handler row counts and
payload accounting separately for all 128 campaigns. No skipped work explains
the instruction reduction. The reverse-screen manifest archives raw outputs,
provenance, counters, device observations, scripts, combined arithmetic and the
exact compared fields. The existing 10k evidence remains; repeated 12.5k is still
unmet. No hardware changes or gate relaxations were made.
Evidence: `fireweed-member-ownership-reverse-manifest.json`.

## Response ownership screen: fewer instructions, throughput unresolved (2026-09-16)

Four serial original-row million-recipient diagnostics completed correctly.

| Run | Recipients/sec | CPU-ms/recipient | User instructions (trillions) |
| --- | ---: | ---: | ---: |
| Control 1 | 16,389.15 | 0.86660 | 2.65339 |
| Candidate 1 | 14,927.37 | 0.86689 | 2.61078 |
| Control 2 | 14,485.86 | 0.92582 | 2.70578 |
| Candidate 2 | 13,806.84 | 0.95728 | 2.67294 |

Candidate instructions fell 1.61% and 1.21%; user cycles fell 3.62% and 1.60%.
CPU time changed +0.03% and +3.40%; throughput fell 8.92% and 4.69%.
RSS improved in the first pair but worsened in the second. This does not prove
throughput or memory improvement. Unlike the rejected admission cap, which
increased instructions by 7.40% and 8.98%, this candidate reduces measured CPU
work in both pairs. A reverse candidate/control/candidate/control screen follows
to counter the fixed run-order confound before accepting or rejecting it.
All settings and the preserved control executable remain the same.

The first candidate also had more observed I/O waiting: IO-full pressure 4.92%
versus 0.15%, and mean host write-request latency 12.11 ms versus 2.76 ms.
These are host observations, not proof that disk hardware or this code change
caused the throughput difference. No SSD settings or workload gates change.
`fireweed-member-ownership-screen-manifest.json` archives all raw reports,
provenance, counters, device samples, runner source and calculations. Candidate
source is `2a88cf2e`, binary `19b72a8…`; controls use `9124cfd…`. Sustained
12,500 recipients/sec remains unqualified.

## Retain generation responses while extracting append requests (2026-09-16)

A more detailed offline allocator caller report of the existing eight-cycle
profile identifies allocation through generation outcome clones. In particular,
`drive_started_generation` clones all members to consume just their log requests,
copying claimed payload/metadata and response state unnecessarily. The candidate
moves members into the existing prepared generation, clones only append requests,
and returns those original members after append and frontier publication. The
sequencer owner remains held over the same operations. No public outcome fields,
commit contents, log durability, workload or qualification gates change.

The report uses a 0.1% global-period threshold; the prior report used 0.5%.
It retains the same 171K samples, zero reported lost samples and short stacks.
The 0.26% allocator branch under the generation driver and 0.13% branch under
`GenerationJoin::member` are sampled caller attribution, not complete clone CPU
cost or a predicted throughput benefit. This candidate changes only the former;
member delivery retains its existing clone. Evidence and exact offline command:
`fireweed-active-profile-allocation-detail-manifest.json`.

Read-statement reuse was considered but already rejected in `cf4e26f8`'s screen;
it is not repeated. Keyless admission specialization would miss this campaign,
which supplies client keys, and was not implemented. Validation and a serial
control/candidate screen must precede any performance claim for response ownership.
The complete release façade library suite passes **154 tests, zero failures, one
existing ignore**. All **12 public campaign/primitive/recovery tests pass**.
The standalone workload build passes. Logs and hashes are recorded in
`fireweed-member-ownership-validation-manifest.json`; performance remains pending.

## Reject global projection admission (2026-09-16)

The four serial million-recipient diagnostic screens completed successfully.
The cap at 16 active applies regressed both comparisons:

| Run | Recipients/sec | CPU-ms/recipient |
| --- | ---: | ---: |
| Control 1 | 16,248.06 | 0.85992 |
| Candidate 1 | 15,156.11 | 0.93044 |
| Control 2 | 14,507.96 | 0.91583 |
| Candidate 2 | 13,782.85 | 1.02271 |

Throughput fell 6.72% and 5.00%; CPU cost rose 8.20% and 11.67%.
Restore `projection.rs` byte-for-byte to parent `153003bb`, removing the cap
and its implementation-specific tests. These results reject this admission
policy; they do not establish why it regressed or the optimal concurrency.
The candidate passed all 12 public campaign/primitive/recovery tests as well as
the adapter checks recorded below. Correctness alone did not justify retaining it.

`fireweed-apply-admission-screen-manifest.json` archives raw reports, provenance,
counters, device samples, runner/recorder source, public test and build logs.
Controls use executable `9124cfd…`; candidates use `dfedfc3…` from `b0056dd5`.
These are one-cycle screens, not repeated eight-cycle qualification. The 12,500
recipient/sec goal remains unmet; no workload or hardware assumptions change.

## Bound active projection transactions across stores (2026-09-16)

The candidate adds process-wide admission to the actual `apply_owned` path,
independent of which Tokio runtime supplies its blocking worker. It allows up to
`std::thread::available_parallelism()` active owned applies (16 logical CPUs on
this host). It first acquires the per-store writer, then admission; waiters for
one busy store therefore cannot occupy all global slots. Once submitted, the
owned task retains both writer and admission through commit/rollback even if its
response waiter is cancelled. Cancellation while awaiting admission releases the
writer without starting a transaction. Existing pre-transaction wait telemetry
includes admission waiting. No host, log-runtime, batch, row-model or gate setting
changes.

Three focused tests pass: a busy store leaves capacity for another store;
cancelling a globally queued apply releases its writer without leaking capacity;
and independent stores obey the cap and release their writers on admission
closure. The adapter library plus real cancellation, concurrency and recovery
suites also pass; complete output and hashes are in
`fireweed-apply-admission-tests-manifest.json`. Public campaign/retention/recovery
checks and an alternating one-cycle counter screen follow. The cap is an
experiment motivated by observed scheduling pressure, not a proven optimum or
throughput improvement. Repeated eight-cycle qualification remains required.

## Reject record-buffer reuse; inspect projection apply concurrency (2026-09-16)

Four serial original-row million-recipient screens completed correctly with no
trace overrides. Controls use preserved `2e8f8ff5` executable `9124cfd…`;
candidates use clean `c06b48bc` executable `89354e6…`. They are one-cycle diagnostic
comparisons, not qualification. Full hashes and commands are archived.

| Run | Recipients/sec | CPU-ms/recipient | User instructions (trillions) |
| --- | ---: | ---: | ---: |
| Control 1 | 16,465.52 | 0.85423 | 2.62570 |
| Candidate 1 | 16,062.15 | 0.86501 | 2.66180 |
| Control 2 | 15,749.51 | 0.90388 | 2.65702 |
| Candidate 2 | 15,020.47 | 0.93602 | 2.66489 |

Both pairs increase CPU cost (+1.26%, +3.56%) and instructions (+1.37%, +0.30%),
and decrease wall throughput (-2.45%, -4.63%). Remove the candidate. Production
prefixes in `types.rs` and `vdbe/execute.rs` are restored byte-for-byte to
`4d469f1a`. Keep the independent serialization regression across record sizes;
it passes after restoration. The candidate had passed 2,126 native tests and all
12 public campaign/primitive/recovery tests. Passing correctness did not establish
a performance benefit. The 28-artifact `fireweed-record-buffer-screen-manifest.json`
records raw reports, CPU counters, device samples, runner/recorder source, build
and public test output, analysis and restoration validation.

The next larger software candidate is **bounded projection apply concurrency**.
The existing controls report substantial CPU scheduling pressure (for example,
the prior second control had 65.36% CPU-some pressure and 14.50 busy CPU-seconds
per wall second). This is a lead, not proof that concurrency is excessive.
`apply_owned` takes the store writer then offloads an owned transaction through
`Handle::try_current`; its shared runtime is only a fallback. Relational work
uses another blocking hop with a thread-local current-thread runtime. Changing
only the fallback runtime would miss normal Tokio callers. Any global admission
experiment must act on the actual `apply_owned` path, retain ownership through
commit/rollback, and preserve cancellation while queued, per-store serialization,
reporting responsiveness and all qualification gates. It must not repeat the
already-rejected one-worker **log** runtime experiment. No concurrency setting has
changed in this commit and no throughput target is relaxed.

## Record output-buffer reuse candidate (2026-09-16)

The new candidate reuses the owned destination record buffer in native Turso's
`MakeRecord` opcode when the destination is outside the input register range.
Overlapping inputs keep the original allocating path. Serialization sizes the
output to the new record length, grows fallibly when required, and overwrites
all returned bytes; a shorter record cannot expose the old tail. Header-vector
allocation, SQL shape, row model, workload and qualification gates are unchanged.
No allocation or throughput reduction is claimed before measurement.

Two focused tests pass: byte equality with the independent `Record` serializer
across empty, mixed-type, wide-header and growing/shrinking records; and
preservation of overlapping input/output registers. The complete native suite
passes **2,126 tests, zero failures, 16 existing ignores**. Public campaign and
recovery validation, then an alternating control/candidate/control/candidate
hardware-counter screen, follow on clean source. Only repeated full qualification
can establish the throughput milestone.

The prior one-worker runtime control already failed to improve repaired-host
performance, so it is not repeated. An offline allocation caller report from the
existing full profile confirms 6.12% allocator self samples; its short stacks and
0.5% display threshold do not establish how much this particular candidate can
save. `fireweed-record-buffer-native-manifest.json` records the native test logs,
allocation caller report and exact offline command. The shim is offline only.

## Reject the record-comparison continuation candidate (2026-09-16)

The serial control/candidate/control/candidate screen completed four one-million
recipient campaigns with the original workload, no trace overrides, and successful
correctness exits. Counters cover the workload process and threads; these one-cycle
runs are diagnostic, not sustained qualification. Controls use preserved
`2e8f8ff5` binary `9124cfd…`; candidates use clean `2b70c01a` binary `98a232f…`.
Full hashes, commands and provenance are in the archived reports.

| Run | Recipients/sec | CPU-ms/recipient | User instructions (trillions) | User cycles / user CPU-ns |
| --- | ---: | ---: | ---: | ---: |
| Control 1 | 16,511.27 | 0.85909 | 2.64889 | 3.103 |
| Candidate 1 | 16,153.67 | 0.88146 | 2.66145 | 3.044 |
| Control 2 | 13,956.02 | 0.94625 | 2.66423 | 2.769 |
| Candidate 2 | 14,714.56 | 0.96251 | 2.70107 | 2.729 |

Both candidate pairs increase CPU time (+2.60%, +1.72%) and instructions
(+0.47%, +1.38%). Wall-rate results disagree in direction. This does not justify
retaining the fast path: production `types.rs` is restored byte-for-byte to
`61b4b80f` before its test module. Retain the 48,600-case independent ordering
oracle, which passes on the restored implementation. Candidate validation before
rejection was 2,124 native tests and 239 public/library/workflow tests, all passing
(16 and 3 existing ignores respectively). No performance gain is claimed.

A useful next lead is **variation within the identical control**: user cycles
fall from 2.266T to 2.211T while user CPU time rises from 730.29s to 798.33s.
Their ratio drops from 3.103 to 2.769 cycles per user CPU-ns. This is consistent
with a lower effective clock, but is not proof of thermal throttling; wrapper
accounting and counter semantics must be considered. `ref-cycles:u` is not
supported on this Ryzen 7 4800H. The existing device-monitor-v3 observations already include frequency and
CPU temperature, so no new run is necessary to retrieve them. Control median
host-mean sampled frequency declines from **3.263 to 2.920 GHz**, consistent
with the counter/time ratio change; median CPU temperature declines from
**77.19 to 72.06 °C** (maxima 82.13 and 80.00 °C). This supports clock-rate
variation but does not establish its cause, thermal throttling, or an SSD limit.
Sampled host-wide frequency is not workload-time-weighted frequency. Fixed
single-run CPU-ms budgets must not silently assume boost speed persists.
The next optimization still needs to reduce measured work per recipient and
pass repeated full-length gates; host settings remain unchanged.

The 28-artifact `fireweed-record-continuation-manifest.json` includes all raw
reports, counter CSVs, device observations, exact runner/recorder sources, build
and public validation logs. Restoration output is archived separately as
`fireweed-record-continuation-restoration.log.gz`. The repeated 12.5k goal remains
unmet; no workload gate or target was relaxed.

## Active-backend CPU profile and record-comparison candidate (2026-09-16)

The full eight-cycle profile uses the preserved `2e8f8ff5` control binary
(`9124cfd715be116ce28c5ed83ded45fb0773c9ff45772dfab8e54614999acf61`),
recorded from clean `acd4a9ab` at 19 Hz with 2048-byte DWARF stacks. It completed
8 million original-row recipients at 15,360.74/sec overall, with 0.8910 CPU-ms
per recipient and 19.475 GiB peak RSS. CPU accounting includes the perf wrapper.
Every 12.5k workload/resource check passes except the deliberately disqualifying
diagnostic-provenance check. This is a profile, not a repeated qualification.

The recording contains 171,068 samples; the self report shows zero lost samples.
Self CPU: allocation 6.12%, column extraction 5.61%, VM stepping 4.58%, generic
record comparison 2.65%, and in-process WAL latest-frame iteration **0.34%**.
Inclusive CPU: index seeking 14.81%, column extraction 13.50%, record comparison
6.68%. Inclusive categories overlap and short stacks truncate ancestry; these
percentages cannot be added or directly compared with earlier 4096-byte-stack
profiles. WAL iteration is active but too small to be the principal next target.

A bounded candidate in `types.rs` resumes record comparison after an equal first
text key using the already decoded header/data positions. The prior string path
re-entered the generic comparator, reparsed the record header and skipped the
same first field again. Remaining fields share one comparison loop, preserving
collation, sort order and prefix-key tie handling. The new decoded-value oracle
covers 48,600 combinations of text prefixes, mixed field values, sort direction,
collation, record/key lengths and tie outcomes. It passes. The full native Turso suite also passes: **2,124 passed,
16 ignored**. Public workflow and performance validation of this candidate
remain pending; no improvement is
claimed from source inspection or the focused test.

Profile evidence, raw perf parts, report commands, recorder and device monitor
sources, and the complete gate results are listed in
`fireweed-active-wal-profile-manifest.json`. Reconstruct the raw recording by
concatenating the decompressed parts in manifest order. The unwind shim is used
only by offline `perf report`, never by the measured workload.

## Retire disabled SQLite facade fixtures (2026-09-16)

The facade no longer retains `cfg(any())` SQLite tests or its unreachable
SQLite composition/lifecycle implementation. Native Turso tests run against a
filesystem object log through the public API on a current-thread Tokio runtime.

| Retired coverage | Disposition |
| --- | --- |
| Filesystem/SQLite claim then commit | Ported to filesystem/Turso; checks original ID, completion and zero remaining leases. |
| SQLite-log/memory synchronous and asynchronous constructor lifecycle tests | Removed as duplicates of the active filesystem/memory constructor tests. |
| SQLite/SQLite constructor lifecycle | Consolidated into Turso lifecycle coverage; both public `open` and `open_async` now run the same oracle. |
| Snorri frozen-clock claim, repeated queue creation, then commit | Ported using Turso priority claiming; verifies that re-ensuring the queue preserves the lease for commit. |
| Disabled SQLite matrix cells, empty wrapper test and constructor source assertion | Removed; active memory/Turso/filesystem matrix and retired-configuration rejection tests remain. |

**Interface limitation discovered during the port:** a literal port of the old
`claim_by_query` test returned `EngineError::Unavailable`. The native Turso
composition implements `HotProjectionQueryPort` with default capability flags
and does not expose declared-index query claims. The port explicitly checks that
rejection and that the row remains pending before exercising the supported
priority-claim lifecycle. This is not evidence that query claims work on Turso;
that interface work remains separate from the representative campaign benchmark.

The release coverage loop now names `fireweed-turso` with `--features local`
instead of the deleted `fireweed-sqlite` crate. Shell syntax validation passes;
the complete release/coverage pipeline was not run for this change.

Validation, run serially:

```sh
CARGO_BUILD_JOBS=8 cargo test --locked --release -p fireweed --lib -- --test-threads=1
bash -n scripts/ci/release-gate.sh
```

**154 passed, zero failed, one pre-existing ignored test** (the direct object-log
product's unavailable commit-transition port). All three new native Turso tests
pass. The complete final build/test log is archived as
`fireweed-turso-facade-test-port-20260916.log.gz`; SHA-256 of decompressed bytes:
`6795b8af2b311ceef771092a56301241b71b9f9e13aec7ccd3b698f6b1f8b99a`.

This change retires facade fixtures and restores executable coverage; it does
not claim removal of every historical SQLite reference elsewhere in the repo.
No capacity measurement or throughput target changes with this test cleanup.

## Correction: optimized the inactive WAL backend; remove that fast path (2026-09-16)

The narrow-frame experiment targeted the wrong backend. Fireweed's
`TursoRelational::open_with_io` calls `Builder::new_local` and changes only the I/O
implementation. Turso defaults `enable_multiprocess_wal` to false; without that
flag, `open_shared_wal_coordination_inner` returns None and the WAL uses
`InProcessWalCoordination`. Therefore the multiprocess fast path in `604c22a9`
is **not exercised by this workload**. My earlier reasoning incorrectly assumed
that mapped shared coordination was active. These recordings cannot establish
any performance benefit from that patch. Source excerpts, hashes and the backend
factory chain are archived in `fireweed-narrow-frame-untraced-backend-source-proof.json.gz`.

Remove the 15-line inactive fast path. Production code in
`shared_wal_coordination.rs` is restored byte-for-byte to `ab0e0d0a`; retain the
useful latest-visible-frame oracle test. All **44 shared-coordination tests pass**
after restoration. The original selective page-cache change remains active for
both backends: its measured 46.7% VFS-read reduction is separate from this failed
backend assumption and is not retracted.

All four completed runs use clean `604c22a9`, executable
`89abe51884017c2112999a0b1a0c932702b0be9fded86b6a618de4fc6205dcae`, empty
diagnostics and successful workload exits. They are additional **in-process
control observations**, not a demonstrated optimization comparison.

| Campaign | Overall recipients/sec | Slowest cycle/sec | CPU-ms/recipient | Peak RSS GiB |
| --- | ---: | ---: | ---: | ---: |
| First | 15,273.21 | 13,226.85 | 0.884260 | 19.359 |
| Repeat | 13,285.76 | 11,637.16 | 0.915180 | 18.511 |

First passes every 12.5k gate. Repeat fails cycle-rate checks 1, 5 and 7
(zero-based): 12,492.49 / 11,637.16 / 12,427.83. All correctness, reporting,
due-to-claim, WAL/DB/RSS and 10k checks pass. All five primitive rates exceed
10k in both repeats: insert 31,551/36,848; enrich 39,396/44,397; schedule
117,381/115,372; claim/complete 45,366/45,836; purge 161,008/147,184 rows/sec.
These remain million-row, varied-payload, batched public operations.

The slowest additional observation needs **7.41%** more throughput (85.932
seconds down to 80). CPU budgets at 12.5k are **11.053/11.440 CPU-seconds/sec**.
Host write rates are 61.48/53.24 MiB/sec, mean write-request latency 4.82/17.31 ms,
and busy time 35.07/56.81%. These measurements document variability, not a device
bandwidth ceiling or causal benefit from an unused code path. No host settings,
workload limits or gates changed, and runs were sequential.

Next profile the actual in-process backend across all eight cycles using the
preserved `9124cfd7...` executable built from `2e8f8ff5`. Its frame-range iterator
scans per-page historical vectors, including old versions when no new page frame
exists in the requested range; that is an active code path worth measuring.
Use 19 Hz user-CPU samples with 2 KiB DWARF stacks to include late-cycle work with
bounded trace volume. This is diagnostic only; resource usage includes the perf
wrapper. Confirm hot paths before selecting another change. The full goal
remains open. Twenty-two artifacts, including restoration tests, are indexed by
`fireweed-narrow-frame-untraced-repeat-manifest.json` with decompressed hashes.


## Bound small frame-range scans before the next qualification (2026-09-16)

`MappedSharedWalCoordination::iter_latest_frames` now directly enumerates the
visible slots when a range contains at most 4,096 entries. It sorts one vector
by page ascending/frame descending and keeps the first entry per page. This
avoids scanning an 8,192-slot hash table for even a single changed frame, plus
its temporary maps and seen-page tree. Larger ranges retain the existing
block-deduplicating checkpoint path. Snapshot boundaries, rollback handling,
cache caps, on-disk formats and public workflow behavior are unchanged.

The new native oracle test compares the exact latest frame for each page over
frame-ID gaps, duplicate page versions, old snapshots with newer index entries,
block crossings, empty/reversed ranges and ranges on both sides of the cutoff.
All **44 shared-coordination tests pass**. The complete native debug suite passes
**2,123 tests, zero failures, 16 ignored** in 233.22 seconds. Fireweed release
validation passes **328 tests, zero failures, four ignored and two live-S3 tests
filtered**, including public strict/async durability, original-row campaign and
log-only recovery coverage. Validation runs were sequential; logs and source
hash are archived in `fireweed-narrow-frame-scan-validation-manifest.json`.

This is a validated implementation candidate, not a measured speedup. The last
untraced repeat still misses the worst-cycle target by 4.80%. Next run the same
canonical two campaign/primitive qualifications without diagnostic overrides.
The previous executable is preserved as
`9124cfd715be116ce28c5ed83ded45fb0773c9ff45772dfab8e54614999acf61` for comparison.


## Selective cache untraced repeats: stretch still fails (2026-09-16)

All four canonical runs use clean `2e8f8ff5` and executable
`9124cfd715be116ce28c5ed83ded45fb0773c9ff45772dfab8e54614999acf61`, with empty
diagnostics and successful workload exits. The shell qualification exits 1
because the second campaign fails five per-cycle 12.5k rate checks. Both
campaigns and both primitive runs pass every 10k check. No source, workload,
cache-cap or host setting changed between runs, and none overlapped.

| Campaign | Overall recipients/sec | Slowest cycle/sec | CPU-ms/recipient | Peak RSS GiB | 12.5k gates |
| --- | ---: | ---: | ---: | ---: | --- |
| First | 15,624.42 | 13,746.67 | 0.878902 | 19.812 | All pass |
| Repeat | 12,752.16 | 11,927.47 | 0.927261 | 19.991 | Five cycle-rate failures |

Repeat cycle rates are 12,285.35 / 13,908.45 / 15,152.13 / 15,352.31 /
12,067.35 / 12,195.79 / 12,393.79 / 11,927.47. Correctness, due-to-claim,
reporting, outcome reconciliation, WAL, checkpoint materialization,
database stability and RSS checks all pass. The worst cycle needs **4.80%**
more throughput (83.840 seconds down to 80), so the stretch goal remains open.

Primitive rates (first/repeat rows/sec, million varied-payload rows, batch 1000):
insert 45,958/43,068; enrich 43,115/44,303; schedule 57,011/108,421;
claim/complete 47,044/44,942; purge 64,138/115,135. All exceed 10k, but purge
is lower than the prior baseline; do not claim improvement in every primitive.
These are batched public operations, not one durable transaction per row.

At 12.5k, measured CPU cost requires **10.986/11.591 CPU-seconds/sec**.
Host writes are 31.740/31.100 GiB at 63.65/50.79 MiB/sec. Mean write-request
latency is 4.59/19.23 ms, busy time 31.80/57.15%, and full I/O pressure
1.67/7.30%. CPU cost also increases in the repeat. These measurements show
synchronization/CPU variability alongside write latency, not a proven physical
bandwidth ceiling or a reason to change SSD settings. The prior traced read
reduction remains real, but does not establish sustained stretch capacity.

Code review identifies a next bounded hypothesis: `iter_latest_frames` bounds
which blocks it visits, yet even a tiny changed-frame range still invokes
`latest_entries_in_block`, scanning the full hash block and allocating maps,
followed by a seen-page tree. Cache invalidation can instead enumerate only the
visible changed slots for narrow ranges, preserving latest-per-page semantics
and the conservative snapshot checks. This is not implemented or measured yet;
verify range boundaries, duplicate pages and snapshot visibility before testing
its CPU/throughput impact. No weakening of cache coherence or workload gates.

Twenty artifacts in `fireweed-selective-cache-untraced-repeat-manifest.json`
archive the four raw reports, summaries/budgets, device observations, exact
runner/parser/qualification command and logs, with decompressed hashes.


## Selective cache measurement: fewer reads, modest CPU change (2026-09-16)

Clean `141d4be7`, binary
`520abf5e083fd751c223ec475521a5686d3eb999638fa86e861b19729cd32cab`, completes
all eight million recipients at **15,236.67/sec**, with **13,107.79/sec** slowest
cycle, **0.885019 CPU-ms/recipient** and **19.516 GiB** peak RSS. All rate,
correctness, fairness/reporting and WAL/DB/RSS gates pass; tracing/provenance is
the sole failed qualification gate. This remains a diagnostic, not qualification.

Projection reads total **22,310,784 calls and 91,378,663,424 requested bytes**:
1,721,755 main/other calls (7,046,000,640 bytes), and 20,589,029 WAL calls
(84,332,662,784 bytes). No immediate VFS errors occur. Compared with the earlier
unchanged eight-cycle control, calls and bytes fall **46.7%**, throughput rises
**2.9%**, and CPU cost falls **2.4%**. These are single sequential recordings at
different times, not proof of a repeatable throughput gain. The minimum cycle
rate is lower than the control's 13,361.81/sec despite the higher aggregate rate.
The cache mechanism removes considerable read traffic but does not remove the
remaining SQL/queue/serialization work; do not convert the read reduction into
an equivalent throughput claim.

Host observation records only **0.004959 GiB device reads**, 33.081 GiB writes,
64.76 MiB/sec writes, 35.20% device busy and 4.91 ms mean write-request time.
Requested VFS reads are overwhelmingly OS-cached, not physical SSD bandwidth.
At 12.5k recipients/sec the measured workload implies **34,861 read calls/sec**,
**136.16 MiB/sec requested VFS reads** and **11.063 CPU-seconds/sec**. These are
resource budgets, not independent throughput ceilings. No host settings, cache
caps, batch limits or acceptance gates changed; the build and workload ran
sequentially. The prior control binary is preserved for further comparison.

The twelve artifacts in `fireweed-selective-cache-read-trace-manifest.json`
include raw results, exact runner/parser, device monitor/summary, build/run logs
and decompressed hashes. Next run the canonical untraced campaign/primitive
qualification twice; the repeated 12.5k goal remains unproven.


## Selective Turso reader-cache candidate (2026-09-16)

The read-trace investigation now has an implemented candidate. On a strictly
forward read snapshot within the same WAL generation, the pager uses the
in-memory committed frame index to discard changed pages and retain unchanged
ones. Cursor and schema invalidation remain in place. Restart, unavailable
history, backfill beyond the previous snapshot, dirty/pending state, failed
cache deletion and alternate WAL implementations retain full invalidation.
Enumeration is attempted only when the intervening frame count is smaller than
the cache entry count. Shared-index enumeration skips blocks wholly before the
requested frame range; it does not read raw WAL frames.

Two new native Turso tests exercise both legacy in-process coordination and
separate database handles with shared coordination. They verify unchanged-page
identity, fresh committed values, stable active snapshots, writer and reader
rollback, reused uncommitted frames, schema/index changes, checkpoint restart,
backfill and dropped/recreated table pages. The shared-index block-boundary test
also covers narrow and empty ranges. No SQLite fixture was introduced.

Focused tests pass, and the complete native debug suite passes **2,122 tests,
zero failures, 16 ignored** in 231.76 seconds. Fireweed release validation passes
**328 tests, zero failures, four ignored and two live-S3 tests filtered**. This
includes strict/async public durability with log-only rebuild and the CLI campaign,
primitive and recovery suites. Runs were sequential. Logs and decompressed hashes
are archived in `fireweed-selective-cache-validation-manifest.json`. Performance
comparison remains pending. This is a correctness-tested candidate,
**not a demonstrated speedup or a completed stretch qualification**. Preserve
control binary `a287648d51d4b45ac0c19ce8632ccb4e5025b8a10d9e7916d39beff103295262`
for serial comparison; the workload, durability and acceptance gates are unchanged.

## Eight-cycle read trace: OS-cached WAL traffic and full invalidation (2026-09-16)

Clean `0ea4fc24`, binary
`a287648d51d4b45ac0c19ce8632ccb4e5025b8a10d9e7916d39beff103295262`, completes
all eight million recipients at **14,805.44/sec**, with **13,361.81/sec** slowest
cycle, **0.906722 CPU-ms/recipient** and **18.982 GiB** peak RSS. Every rate,
correctness, fairness/reporting and storage/memory gate passes. The diagnostic
flag/provenance is the sole failing qualification check: **this is not a repeated
untraced qualification result**. No cache settings, workload limits or host options
changed; no concurrent build/benchmark ran.

| VFS class | Read calls | Requested read bytes | Accumulated VFS read elapsed seconds |
| --- | ---: | ---: | ---: |
| Main/other | 2,587,569 | 10,592,396,288 | 24.739 |
| WAL | 39,274,701 | 160,869,175,296 | 386.867 |
| Total | 41,862,270 | 171,461,571,584 | 411.606 |

No immediate VFS read/write errors were recorded. WAL writes request
54,610,002,576 bytes and main writes 2,070,343,680 bytes. The host records only
**0.003918 GiB (about 4 MiB) of device reads** during its sampled interval, versus
31.734 GiB writes. Thus the VFS read volume is overwhelmingly served above the
physical device; do not call 171.46 GB an SSD-read workload. Accumulated VFS elapsed
time overlaps across threads, includes callback/scheduling time, and is neither
CPU time nor an additive decomposition of campaign wall time. There is no claim
that eliminating all these reads is possible or would save 411 wall seconds.

Source review supplies a stronger hypothesis than merely enlarging a cache:
`Pager::begin_read_tx` calls `clear_page_cache(false)` whenever WAL snapshot state
changes, and invalidates the schema cookie. A larger cap cannot preserve cached
pages through this full clear. Writer/serving caps remain 128 MiB; 16 driver plus
eight outcome readers each have 4 MiB. Raising every pool cache to 16 MiB adds an
18 GiB theoretical cap across 64 stores, without evidence it avoids those clears.
No cache-cap trial is justified yet.

Next investigate preserving demonstrably unchanged pages across append-only WAL
snapshot advances, while retaining cursor/schema invalidation and conservative
full clears on restart, checkpoint-history loss, rollback, MVCC and any ambiguous
state. This is an engine-coherence change requiring native cross-connection,
checkpoint/restart, rollback and snapshot tests before campaign measurement.
The existing `Wal::changed_pages_after` helper reads each intervening raw frame;
blindly using it could add more reads than it saves. Compare in-memory index lookup
cost against the pages that would otherwise be discarded. This remains an
unimplemented hypothesis, not a speedup claim or authorization to relax isolation.

Manifest `fireweed-projection-read-trace-manifest.json` covers nine raw reports,
device samples/summary, exact runner/parser and build/run logs; hashes identify
decompressed contents. The repeated stretch goal remains active. The last
untraced qualified baseline still has a 4.2% worst-cycle gap.

## Measure projection reads before changing pooled-cache budgets (2026-09-16)

The previous turn produced progress by rejecting an unsupported decoder speedup
and correlating later-cycle slowdowns with main-file materialization. Current
runtime retains linear purge validation, with the decoder restored. No workload
or build remained active when this investigation began.

Extend the existing `FIREWEED_PROJECTION_IO_TRACE` diagnostic with per-file-class
`projection_read` totals. Read/write requested bytes and counters remain separate;
existing `projection_io` write output retains its format. Disabled tracing forwards
`pread` directly without clocks or counter locks. Enabled tracing forwards the
original completion and result, including short reads and callbacks. On Unix,
PlatformIO performs synchronous calls including their completion callback. Elapsed
time is VFS invocation time, **not** device service time; requested bytes may be
served by the OS cache. Calls also include headers/other explicit reads, so they
are not exact page-cache-miss counts. `errors` counts immediate Result errors,
not later asynchronous callback failures on other I/O platforms.

All three focused I/O tests pass, including a real short file read with one
callback and unchanged data, separate requested-byte accounting, and propagated
read errors. The native projection library suite passes **60 tests, zero failures,
two intentional ignores**. Logs are archived as
`fireweed-projection-read-trace-{tests,native-tests}.log.gz`. No cache setting,
SQL, log durability barrier or public API changes. The existing diagnostic flag
already disqualifies traced recordings from qualification.

Next run an unchanged eight-cycle million-resident campaign to measure main/WAL
read volumes through the post-checkpoint cycles. This is instrumentation-only
progress; the repeated 12.5k target remains unproven and the last qualified baseline
still has a 4.2% worst-cycle gap. A larger cache requires measured benefit and the
same RSS/isolation/correctness gates, not merely the 30 MiB main-file size.

## Offline late-cycle review after the rejected decoder (2026-09-16)

Read-only analysis of the existing guarded-counter qualification records shows
both campaigns first materialize all 64 main projection files by the end of
cycle 4 (zero-based). Final file sizes are roughly 29.4–29.8 MiB/store. The first
campaign's maximum delivery duration rises from 19.00 seconds in cycle 3 to 25.14
in cycle 4, then returns to about 19 seconds. The repeat rises from 19.46 to 33.25,
then remains 27.61–28.27 seconds in cycles 5–7. Repeat preparation also rises to
about 33 seconds in cycles 5/6. This correlates the sustained slowdown with the
post-checkpoint portion of the run; it does **not** prove checkpointing alone is
the cause. Phase maxima come from different campaigns and are not additive.

Do not repeat the earlier staggered-checkpoint experiment without a new mechanism
check: `3b8f1036` already failed and was reverted. Existing main-file sizes also
supply a bounded reader-cache question. Writer and serving-reader caps are
128 MiB, but each committed pooled reader has a 4 MiB cap. Turso creates a distinct
pager/cache per connection; setting the small pool cap does not shrink the writer's
cache. A 30 MiB database does not prove a 30 MiB hot working set, but outcome and
retention queries contribute about 8% inclusive user-cycle samples together.
Next measure physical main/WAL read requests or cache misses before deciding
whether a larger bounded pool cache is warranted. Keep total RSS, its last-three-
cycle growth gate, all reader isolation guarantees and the same workflow. This
is a projection cache investigation, not SSD tuning or a new capacity claim.

Raw derived stage/file-size table: `fireweed-guarded-counter-cycle-stage-analysis.json.gz`.
Runtime source after decoder restoration is byte-identical to `1719a76f`; the
working tree is clean once this evidence is committed. The goal remains active.

## Owned-decoder candidate rejected after serial comparison (2026-09-16)

A clean `77fdfa73` candidate/control/candidate diagnostic compares combined
linear purge validation and owned addressed-row decoding against preserved
`f931a497` qualification binary `94adbef091a89a54df649805542fbd7128dfa4769a7b06b96420c269efc29fe8`.
Candidate binary is `6322f4b5afb9e9fa92e6d53caff197b09addb3cba048066e8452ba60130421c6`.
Each run completes the same one-million-recipient original-row campaign. User
cycles/instructions have 100% running coverage. Projection I/O tracing and explicit
provenance make these diagnostics, not sustained qualification. No competing
workload/build or host setting change ran.

| Run | Recipients/sec | CPU-ms/recipient | Instructions/recipient | WAL requested bytes |
| --- | ---: | ---: | ---: | ---: |
| Candidate 1 | 16,215.57 | 0.877085 | 2,690,035 | 6,640,568,608 |
| Control | 15,239.12 | 0.892236 | 2,699,142 | 6,664,341,008 |
| Candidate 2 | 14,417.95 | 0.929320 | 2,743,133 | 6,644,560,888 |

Candidate instructions are 0.34% lower / 1.63% higher; CPU is 1.70% lower /
4.16% higher. There is no useful repeatable improvement demonstrated here.
The combined design does not isolate the decoder from the linear purge fix,
and time-varying host state remains a limitation. It neither proves a decoder
regression nor justifies a new full repeated qualification for this candidate.
Restore `projection.rs` byte-for-byte to `1719a76f`, removing the helper and its
two candidate-specific tests. Preserve the simple set-based purge validation and
its previously passing 276 engine / 327 selected release tests. No tests are
rerun solely for the exact source restoration; no new performance claim follows.

The 4.2% gap remains the last **measured** sustained gap for the qualified
pre-purge baseline. The retained purge change is still not independently
performance-qualified. The canonical target/release executable remains the
rejected candidate until rebuilt; do not label it as current-source output.
The preserved `94adbef0...` executable remains the authoritative comparison
control. Raw records, counters, device observations and exact scripts/build log
are listed in `fireweed-owned-addressed-counters-manifest.json` (decompressed
SHA-256 hashes). Continue toward the original repeated stretch goal with stronger
remaining-cost evidence, without weakening workflow or gates.

## Owned addressed-row decoding candidate (2026-09-16)

After the linear purge-validation change, native addressed-mutation planning now
consumes its already-owned SQL Text/Blob values when constructing the projection
image. The old decoder cloned these buffers before parsing or wrapping them.
The query shape, full planner fields, group/cohort exclusion, payload Keep versus
BeforeSnapshot/NoChange selection, pending-claim overlay and transaction boundaries
are unchanged. The decoder requires exactly the query's 25 columns and returns a
storage error on malformed row width instead of indexing a short vector.

Two focused tests pass: payload pointer identity verifies transfer without copying,
and field assertions preserve metadata, fields, optional values, lease/version and
terminal tracking. Malformed values, negative/overflowing counters, grouped rows,
and missing/extra columns are rejected. A NULL terminal epoch still makes its
unused sequence irrelevant, preserving previous behavior. The broader selected
release suite passes **329 tests, zero failures, four intentional ignores**, with
two unconfigured S3 probes filtered. Public campaign tests cover Keep, snapshots,
NoChange, due-window delivery, reporting, retention and log-only recovery.
Logs: `fireweed-owned-addressed-{focused,full}-tests.log.gz`.

No speedup has yet been measured for this candidate. Next compare the combined
linear-purge plus owned-decoder changes against preserved qualification binary
`94adbef0...`, with serial candidate/control/candidate hardware counters. Report
combined effects explicitly; this comparison cannot isolate either change alone.
The fixed repeated 12.5k goal remains open; the last measured worst-cycle gap is
4.2%. No host or workload/gate changes follow from this implementation.

## Fresh CPU attribution and linear purge validation (2026-09-16)

The previous goal turn made progress: guarded lifecycle-counter inference passed
validation, reduced adjacent-run instructions about 6%, and the canonical repeated
qualification narrowed the measured worst-cycle gap to 4.2%. Revalidate that state
at clean `34661518`; no benchmark or build remained active.

A separate one-cycle million-recipient campaign profiles the same qualification
executable `94adbef091a89a54df649805542fbd7128dfa4769a7b06b96420c269efc29fe8`
(build source `f931a497`). Only the workload process is sampled with pacman perf,
`cycles:u`, 49 Hz and 4096-byte DWARF stacks. All child checks pass at 15,460.83/sec,
0.883607 CPU-ms/recipient and 14.063 GiB peak RSS. This is diagnostic attribution,
not qualification or evidence of an additional speedup. No workload/build overlap.

Approximately 89k user-cycle samples have 1,023 lost samples and 41 lost chunks.
The earlier validated unwind-bias shim is loaded into **offline perf report only**,
never the workload. Loss and truncated stacks limit attribution; inclusive
percentages overlap and exclude blocked time/kernel CPU. Summing thread labels,
Turso `normal_step` accounts for 60.12% inclusive, `op_column` 13.99%, index B-tree
seek 14.37%, accepted-claim realization 8.81%, addressed planning 5.69%, membership
reporting 4.26%, and retained-item query 3.98%. Allocation/free self samples remain
significant. Another column-header cache is not justified by this alone: previous
comparisons already rejected that approach.

One direct source-level defect is `validate_purge_plan`, at 0.77% self samples:
it builds a set of planned IDs for duplicate detection, then performs a linear
request-vector search for each planned ID. A full ordered batch of 8,000 distinct
IDs requires 8,000×8,001/2 = **32,004,000 ID comparisons** in those searches.
The candidate builds one requested-ID set and removes each planned ID. A failed
remove rejects either a foreign ID or a repeated planned ID; duplicate requested
IDs and reordered/empty valid subsets retain their existing semantics. Expected
work becomes linear in request plus plan size. All other envelope, epoch, force,
checksum and commit-fault guards remain unchanged. No log/API change or additional
persistent state is introduced.

Validation: seven focused purge tests and all **276 engine library tests** pass;
the broader selected release suite passes **327 tests, zero failures, four
intentional ignores**, with two unconfigured S3 probes filtered. New cases cover
reordered subsets, duplicate requested IDs, duplicate/foreign planned IDs, empty
inputs and 8,192-item batches with late invalid entries. Existing tests retain
before-commit rejection, same-queue serialization and cancellation guarantees.

**No end-to-end speedup is yet claimed for the purge candidate.** The profile's
roughly 0.8% attribution cannot by itself close the 4.2% remaining stretch gap.
Preserve `/tmp/fireweed-workload-before-linear-purge` (the exact `94adbef0...`
control) for the next serial comparison. Source inspection identifies another
bounded opportunity to examine: addressed-mutation planning clones owned text/blob
SQL values during decoding, while accepted-claim decoding already moves its
buffers. Any change there must retain the full planner semantics, including
payload Keep/BeforeSnapshot/NoChange, pending claim overlays and validation errors.

Archive `fireweed-guarded-counter-profile-manifest.json` lists 18 artifacts:
raw perf data, recorder/provenance, reports/loss logs, device counters, exact runner
and all three validation logs. Hashes identify decompressed contents.

## Guarded-counter repeated qualification: 10k passes, stretch still open (2026-09-16)

Canonical campaign/primitive/campaign/primitive execution from clean `f931a497`
uses binary `94adbef091a89a54df649805542fbd7128dfa4769a7b06b96420c269efc29fe8`
in all four records, with empty diagnostics and child exit zero. The canonical
script rebuilt the executable before running; its hash differs from the preceding
explicit `SOURCE_DATE_EPOCH=1789179522` diagnostic build. Do not identify those
executables as byte-identical. Runtime source is the guarded-counter candidate.
No concurrent benchmark/build, manual TRIM or host changes ran during workloads.

| Campaign | Overall/sec | Slowest cycle/sec | CPU-ms/recipient | Peak RSS GiB | 10k | 12.5k |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| First | 15,107.95 | 13,921.58 | 0.910039 | 18.8975 | Pass | Pass |
| Repeat | 12,887.70 | 11,996.39 | 0.960937 | 18.6873 | Pass | Fail |

The repeat fails only cycle rate checks 4–7 (zero-based): 11,996.39,
12,411.71, 12,426.41 and 12,190.80/sec. Every correctness, fairness/reporting,
WAL, checkpoint/main-file and RSS check passes. Both primitive runs pass every
gate. Wrapper exit one represents the stretch-rate failures, not a child crash.
Worst-cycle time is 83.3584 seconds per million: meeting 80 seconds requires
**4.198% more throughput / 4.029% less wall time**. The earlier baseline worst
was 11,712/sec (6.73% throughput gap); this comparison is observational across
qualifications, not an isolated causal estimate. The adjacent diagnostic
candidate/control/candidate counter comparison remains the direct CPU evidence.

| Primitive, batch 1000 | First records/sec | Repeat records/sec |
| --- | ---: | ---: |
| Insert | 45,327 | 45,172 |
| Enrich by key | 42,831 | 43,110 |
| Schedule by ID | 54,314 | 54,249 |
| Claim and complete | 44,529 | 43,359 |
| Purge | 157,420 | 148,496 |

Sampled host writes are 31.036 / 30.719 GiB, averaging 60.17 / 50.73 MiB/sec.
Mean device write-request latency rises 5.16 to 16.59 ms and busy time 31.74%
to 53.48%; CPU cost also rises. Host counters include all host activity and omit
small observation edges. These correlations do not prove SSD bandwidth is the
exclusive remaining bottleneck or justify host tuning. Retain the six-percent
instruction reduction, keep the goal active, and examine remaining projection
CPU work alongside synchronization variance. A passing first run alone does not
meet the repeated stretch requirement.

Archive manifest: `fireweed-authority-metrics-untraced-repeat-manifest.json`
(18 raw reports, samples/summaries, exact runner/parser and logs; decompressed
SHA-256 hashes). Read-only report summarization occurred during the qualification;
no additional measured workload ran. All four final reports were reevaluated
against both fixed targets after workloads completed.

## Guarded-counter comparison supports sustained qualification (2026-09-16)

Sequential candidate/control/candidate used clean `ebd55df0`, candidate binary
`7cbab73fd4769f3c11e97d1b0c5ff75ca2effb2c67b394255f67c2447dd05979`,
and preserved baseline `83e48da6...`. Each run completed one million original-row
campaign recipients with the existing limits, two enrichments, delivery, reporting
and retention. User cycles/instructions had 100% counter running coverage;
projection I/O tracing makes these diagnostic comparisons, not qualification.

| Run | Recipients/sec | CPU-ms/recipient | Instructions/recipient |
| --- | ---: | ---: | ---: |
| Candidate 1 | 16,306.75 | 0.882760 | 2,677,272 |
| Control | 14,575.45 | 0.932700 | 2,851,733 |
| Candidate 2 | 15,044.10 | 0.904906 | 2,680,160 |

Instructions decrease 6.12% / 6.02%; CPU decreases 5.35% / 2.98%. Throughput
increases 11.88% / 3.21%, with substantial timing variance. Candidate 2 has 35
projection WAL writes taking over 100 ms, versus zero in the preceding runs;
this is observed latency, not proof of an exclusive device bottleneck. WAL
requested bytes are 6.535 / 6.750 / 6.641 GB, so the saved reads do not establish
a major write-volume improvement. Retain the candidate for canonical repeated
untraced eight-cycle campaign and primitive qualification. The 12.5k milestone
remains open until every unchanged gate passes twice.

Raw records, counters, device samples, exact recorder/runner, build log and parser
are archived in `fireweed-authority-metrics-counters-manifest.json`; hashes refer
to decompressed artifact contents. No host tuning or concurrent workloads ran.

## Guarded-claim lifecycle counter inference (2026-09-16)

`MetricsDelta` now infers Pending before and Leased after a fresh authority-first
claim. A complete contiguous sequence may infer its initial counts only when
**every addressed row first appears in such a claim**. Later resolved mutations
or purges determine its final counts. Conditional claims, unknown initial states,
unsupported command families, replayed positions and gaps retain conservative
SQL reads. Mixed known/unknown rows do not introduce a second per-row data structure.

This relies on existing SQL Pending/non-superseded guards and exact moved-row
validation, including fused claims. Inferred deltas apply only after successful
relational application, inside the same transaction; a failed claim rolls back
rows, counters and cursor. Neither the authoritative log nor public API changes.

The focused native tests cover zero-read inference, fallback selection, exact
replay and failed missing/already-leased/superseded claims. Each rejected claim
leaves the same cursor position usable and persisted public counters equal to
an independent row scan. The existing live/recovery paired test establishes two
saved reads for claim-plus-mutation (previously one), unchanged write counts and
matching row-derived totals. Its old one-read expectation initially failed and
was corrected; a separate initial fixture error used nondecimal item IDs and was
corrected before the focused suite passed.

Validation: four focused tests pass; the complete selected release suite passes
**327 tests, zero failures, four intentional ignores**, with two unconfigured S3
probes filtered. Logs are archived as
`fireweed-authority-metrics-{focused,full}-tests.log.gz`.

Performance remains unmeasured at this commit. Compare candidate/control/candidate
on the unchanged one-million-recipient campaign, with user CPU instruction
counters and diagnostic projection-write tracing. Only a subsequent repeated
untraced eight-cycle campaign/primitive qualification can establish the 12.5k
milestone. Preserve the existing 10k qualification as the baseline.

## Stop a duplicate column-cache experiment; inspect metric reads (2026-09-16)

A cursor-owned incremental column-offset cache was implemented with no borrowed
buffer pointers or shared mutable record cache. Two decoder tests passed and
the debug native library suite completed **2,122 passes, 16 ignored, zero
failures**. A native SQL integration regression was drafted but not executed.
No Fireweed campaign or performance comparison used this candidate.

History review then identified the earlier atomic record-position cache removed
in `a3b24317`. Its baseline/candidate/candidate/baseline comparison found only
0.84% mean CPU reduction without a useful repeatable workflow gain. The new
cursor-owned variant avoids atomic packing and resumes after the previous field,
but repeats the same underlying optimization. That difference alone is not
sufficient evidence to justify another expensive validation/benchmark cycle.
Remove all three edited runtime/test files back to `d3031ab8`; preserve the patch
and native logs as `fireweed-cursor-column-cache-abandoned.patch.gz` and
`fireweed-column-cache-native-{tests,full}.log.gz`. No new capacity claim follows.

A more direct opportunity appears in `MetricsDelta::capture`: it reads addressed
rows to discover their pre-apply states even when successful authority-first
claim SQL must validate every named row as active Pending. Claim-only generations
also classify their final state as unresolved, forcing an after-read although
successful authority-first application establishes Leased. The normal and fused
claim paths enforce exact moved-row counts and Pending/superseded guards.

Next test counter inference for those validated claims, retaining contiguous
fresh-position checks and the existing transactional rollback boundary. Infer a
row's initial Pending state only when its first command is an authority-first
claim; prior mutations, purges, mixed unsupported commands, conditional claims,
replays and missing/superseded rows require conservative handling. Successful
command validation must precede applying the counter delta. Native/public tests
must compare persisted metrics with an independent row scan and verify failures
roll back counters and rows together. No log format, durability barrier or public
API needs to change for this candidate.


## Repeated 10k qualification achieved; 12.5k remains open (2026-09-16)

All four sequential runs use clean `5cff73d4`, binary
`83e48da6ebda99cc9351f43808a779a6aae5ec23995068ad54977db0af080147`,
with no tracing, diagnostic override, runtime-thread override, host tuning or
concurrent benchmark/build. The canonical script runs campaign 1, primitives 1,
campaign 2, primitives 2. Each campaign has eight million total lifecycles with
one million resident rows per cycle, 64 stores/two workers, original-row metadata,
timestamp priorities, distinct handler limits, retries, reporting and retention.

| Campaign | Overall recipients/sec | Slowest cycle/sec | 10k qualification | 12.5k qualification |
| --- | ---: | ---: | --- | --- |
| First | 14,423.72 | 13,037.86 | Pass | Pass |
| Second | 13,145.01 | 11,712.03 | Pass | Fail |

The second campaign fails only the 12.5k rate checks for cycles 4/5/6:
11,712.03 / 12,242.56 / 12,303.90/sec. Every correctness, reporting, fairness
at the 10k floor, WAL, checkpoint and RSS gate passes in both runs. Rates are
assessed per campaign in every cycle, not just as whole-run averages. The
wrapper exits 1 because the stretch goal fails; all four workload processes
exit successfully. Re-evaluation at the unchanged 10k target passes both.

| Primitive, records/sec | First | Second |
| --- | ---: | ---: |
| Insert | 38,235.91 | 42,697.08 |
| Enrich by key | 42,798.04 | 44,713.62 |
| Schedule by ID | 104,621.60 | 107,900.40 |
| Claim and complete | 42,725.43 | 42,292.93 |
| Purge | 59,740.74 | 56,957.77 |

Both million-row varied-payload primitive runs pass all gates. These rates use
1,000-row public API batches across 32 stores; they are not individual unbatched
request rates. No gate was relaxed to obtain the repeated 10k milestone.

Campaign CPU costs are 0.94448 / 0.98161 ms per recipient; peak RSS is
18.664 / 19.351 GiB. Host-wide writes are 31.140 / 29.853 GiB, with mean write
request latency 5.80 / 14.86 ms and device busy time 33.40 / 50.02%. These
samples omit short startup/tail intervals and do not isolate process I/O.
The repeat writes fewer physical bytes yet takes longer: byte volume alone
does not explain the timing. CPU demand and synchronization variance remain
separate constraints. The slowest cycle needs 6.73% more throughput for 12.5k.

Evidence: `fireweed-interleaved-untraced-repeat-{campaign,primitives}-{1,2}.json.gz`,
independent both-target evaluation, exact runner, canonical build/run log and
four device sample/summary pairs. The manifest hashes all uncompressed artifacts.
The full stretch goal remains active. The next code review targets repeated
record-header parsing in Turso Column execution; cache invalidation and record
reuse must be proved before retaining an optimization.


## Reject companion wait; resume repeated untraced qualification (2026-09-16)

Clean candidate `78d03aec`, binary `bad4707b...`, was compared serially with
preserved `83e48da6...` (source `78d36c7c`). Each ran the same million-row,
one-cycle, 64-store/two-worker campaign, with identical VFS and user-mode hardware
counters. Counters had 100% running coverage; no builds or workloads overlapped.

| Run | Recipients/sec | CPU-ms/recipient | Data objects | Manifests | Estimated data/manifest sync calls |
| --- | ---: | ---: | ---: | ---: | ---: |
| Candidate | 15,673.91 | 0.91235 | 5,536 | 5,449 | 21,970 |
| Control | 14,489.28 | 0.94175 | 5,537 | 5,449 | 21,972 |
| Candidate repeat | 14,182.35 | 1.01242 | 5,532 | 5,401 | 21,866 |

The first candidate produces exactly as many manifests as control. The repeat
reduces estimated data/manifest sync calls by only 0.48%, while CPU cost and
throughput are worse than control. User instructions per recipient are
2,797,156 / 2,842,811 / 2,862,395, also straddling control. WAL requested bytes
are 6,551,947,408 / 6,572,209,568 / 6,596,624,688. This does not establish a
repeatable benefit or justify the extra latency. Remove the wait and its
candidate-specific tests; restore the engine byte-for-byte to `78d36c7c`.
Validation evidence is retained, as are the unchanged workflow gates.

Raw reports, counters, monitors, provenance, exact runner/recorder, summary and
build log are archived under `fireweed-commit-companion-counters-*` and companion
artifacts; the evidence manifest hashes uncompressed contents. These one-cycle
diagnostics qualify neither milestone. No host changes were made.

The retained implementation most recently cleared every rate/resource gate in
an eight-cycle instrumented run. Next use the canonical qualification script
for two untraced eight-cycle campaigns and two varied-payload primitive runs,
without intervening tuning or simultaneous work. A failure remains a failure;
do not substitute the passing diagnostic for these repeatability requirements.


## Bounded manifest companion candidate validated (2026-09-16)

The candidate allows up to five milliseconds of grouping opportunity when the
ready successful head has an already-uploading immediate successor. It never
waits for a new producer, does not delay an already-ready multi-object group,
and bypasses the wait on shutdown, failed/unfinished heads, failed successors,
or single-object sequencers. The deadline stays attached to the head; later
arrivals cannot renew it. Scheduling delays can exceed this decision budget,
so five milliseconds is not an end-to-end latency guarantee.

Ready-prefix grouping, manifest format, offsets, acknowledgments, error
propagation, byte admission and recovery use their existing paths. New tests
exercise exact deadline expiry, non-extension by arrivals, immediate grouping
when ready, and the bypass conditions. The 36 focused engine/blob checks and
seven manifest/sequencer checks pass. The broader release suite passes **325
checks**, with four existing diagnostics ignored and two unconfigured live-S3
checks filtered. Public campaign, chunking, reporting, durability and log-only
recovery tests remain included. All tests/builds ran sequentially.

No performance benefit is claimed yet. The comparison is candidate/control/
candidate, one million original-row recipients each, using identical VFS and
user-mode hardware counters. Preserve control `83e48da6...` (source `78d36c7c`)
and compare durable manifest counts as well as CPU and rate. A short screen
cannot establish repeated eight-cycle qualification. Evidence: the three
`fireweed-commit-companion-*-tests.log.gz` validation logs.


## Publication phases: synchronization dominates; diagnostic clears rates (2026-09-16)

Clean `78d36c7c`, binary `83e48da6...`, completes the unchanged eight-cycle
million-resident campaign at **14,863.73 recipients/sec**, with a **13,298.53/sec**
slowest cycle. All workload/resource checks at 10k and 12.5k pass. Explicit
instrumented provenance still rejects qualification; this is not a repeated
untraced pass. Cycle rates are 16,515 / 15,730 / 16,302 / 15,603 / 13,299 /
14,813 / 14,381 / 14,672. Peak RSS is 18.836 GiB; CPU cost is 0.92693
ms/recipient. The runtime changes only add optional instrumentation.

Across 87,983 successful local publications, fdatasync accounts for **54.06%**
and directory fsync **35.03%** of accumulated elapsed publication time. Writes
account for **0.82%**, rename/close 6.59%, create 3.22%, other phases under 1%.
Mean fdatasync / directory fsync / total are 45.24 / 29.31 / 83.69 ms; p99 is
597.30 / 319.49 / 1,195.47 ms. These timings overlap across shards and include
scheduler delays; they are neither additive campaign wall time nor device-only
service time. Failed publications do not produce a successful phase record.

Host counters: 31.712 GiB written, 60.49 MiB/sec, 33.59% device busy, 5.316 ms
mean write-request latency, and 14.877 host busy CPU-seconds/sec. The process
uses 7,415.42 CPU-seconds over 538.65 seconds. Projection VFS requested
54,671,802,576 WAL and 2,081,656,832 main-file bytes, with no write errors.
These observations preserve the distinction between logical writes, host-wide
physical writes, elapsed synchronization time and CPU demand.

Next test a bounded five-millisecond wait only when a successful head already
has an uploading immediate successor. Ready groups, idle work, custom
single-object sequencers, failed prefixes and shutdown must bypass the wait.
Additional arrivals must not extend its deadline. Existing manifests, ordering,
byte admission, durable acknowledgment and failure handling stay intact.
Retention depends on a serial publication-count/throughput comparison, not on
this hypothesis alone. No host changes or overlapping workloads ran.

Evidence: `fireweed-local-publish-trace-20260916.json.gz`,
`fireweed-local-publish-{accounting,gates}.json.gz`, phase/device samples,
provenance, runner, parser and build log, with uncompressed artifact hashes in
`fireweed-local-publish-evidence-manifest.json`.


## Local log publication phase instrumentation (2026-09-16)

`OBJECT_LOG_LOCAL_PUBLISH_TRACE=1` enables successful-publication timings in
LocalBlobStore: directory creation, temporary-file creation, writes, fdatasync,
close/rename, directory open, directory fsync and total elapsed time. It emits
byte counts but no paths or payloads. Failed operations retain their original
error propagation; they do not emit a successful phase record. The switch is
cached on first use, so set it before process startup. Disabled tracing makes
no clock calls. The harness records the switch in diagnostic provenance.

The same publication operations and durability barriers remain in order; this
is attribution instrumentation, not a throughput optimization. All 78 object-log
library tests pass with tracing enabled, including local reopen/snapshot and
failure handling checks; two unconfigured live-S3 tests are filtered. The trace
contains 164 successful publication records with valid phase/total timing bounds.
All 17 harness tests pass separately. Logs are archived as
`fireweed-local-publish-{trace-test,trace-contracts,harness-tests}.log.gz`.

Next run the unchanged eight-cycle million-resident campaign with these timings
and explicit diagnostic provenance. Distinguish elapsed synchronization latency
from CPU time and avoid adding overlapping timings across stores.


## Interleaved fusion comparison and joint trace (2026-09-16)

Clean source `7e999e9d`, binary `14a0ddad...`, was compared serially with
preserved control `5d0ac295...` (runtime source `fa49f380`). All three runs use
the same one-million-row original-row campaign, 64 stores and two workers.
Projection VFS counters and user-mode hardware counters were enabled identically;
hardware counters had 100% running coverage. No benchmarks or builds overlapped.

| Run | Recipients/sec | CPU-ms/recipient | User instructions/recipient | Requested WAL bytes |
| --- | ---: | ---: | ---: | ---: |
| Candidate | 15,748.64 | 0.91253 | 2,820,831 | 6,583,016,328 |
| Control | 14,393.41 | 0.92814 | 2,845,657 | 6,588,570,088 |
| Candidate repeat | 14,816.43 | 0.93004 | 2,809,280 | 6,533,741,128 |

Instructions decrease 0.87–1.28% and WAL bytes 0.08–0.83%; CPU cost straddles
control. Reporting reads differ (6,102 / 6,888 / 6,203), so not all instruction
savings can be attributed to fusion. This is a small improvement candidate,
not evidence that the sustained goal is achieved. The existing guarded fusion
is retained with its interleaving correctness coverage.

The subsequent eight-cycle joint log/apply/VFS trace completes eight million
recipients at **14,028.16/sec** overall, **0.93407 CPU-ms/recipient**, and
18.252 GiB peak RSS. Cycle rates are 15,985 / 16,549 / 16,303 / 15,967 /
13,149 / 13,334 / 12,349 / **11,992/sec**. All workload/resource checks at 10k
pass, but explicit diagnostic provenance rejects qualification. At 12.5k the
last two cycle rates also fail. This is one instrumented run, not repeated
untraced qualification, and neither milestone is declared complete.

Mean segment publication grows from 53.2 ms in cycle zero to 257.8 ms in cycle
seven; mean manifest publication grows from 44.4 to 170.9 ms. Producer wait
means grow from 224.9 to 589.3 ms. Mean projection apply duration is roughly
steady, 502.1 versus 492.1 ms, dominated by its update phase. Timing includes
scheduling/waiting and overlaps across stores; these values are not CPU costs
or additive elapsed time. Cycle zero includes startup. Publication timing does
not yet separate write, fdatasync, rename and directory fsync.

VFS totals: 55,926,029,456 WAL bytes in 32,434 calls, 2,077,458,432 main-file
bytes in 27,844 calls, zero write errors. Accumulated WAL call time is 256.91 s
(overlapping), with 320 calls at least 100 ms and a maximum 1.814 s. Host counters
show 30.737 GiB written, 55.36 MiB/sec, 43.71% device busy, 10.32 ms mean write
request, and 14.13 busy logical CPU-seconds/sec. Host counters include other
processes and omit short startup/tail intervals.

A one-time control thread snapshot records 985 threads, including 25 blocked
in `wait_log_commit` and many sleeping on futexes. This is a snapshot, not proof
that thread count causes the slowdown; the previous one-worker flush-runtime
experiment already failed to improve performance and should not be repeated.
Next isolate local log publication phases before changing commit grouping;
retain all authoritative-log durability barriers and acknowledgment semantics.

Evidence: `fireweed-interleaved-fusion-counters-*`,
`fireweed-joint-log-projection-trace-*`, `fireweed-joint-trace-*`, exact runners,
recorder, parser, thread snapshot and canonical build log. The evidence manifest
records SHA-256 hashes of uncompressed artifact contents.


## Interleaved claim/mutation fusion validation record (2026-09-16)

The relational apply window now recognizes interleaved independent handlers,
for example `Claim(A), Claim(B), Mutate(A), Claim(C), Mutate(B), Mutate(C)`.
Previously the third claim ended the initial claim-then-mutation window, forcing
B's intermediate lease update even when its follow-up shared the transaction.
The candidate skips only claims paired with a later lease-invalidating replacement,
using the existing Pending/version guards; it does not reorder commands, remove
log events, enlarge batches or change the coordinator's join delay. Unpaired
claims keep the normal path. Repeated identities, unsupported commands and
queue/epoch/sequence boundaries end the window before the offending command.

**325 local release tests pass**, four existing diagnostics ignored and two
unconfigured live-S3 checks filtered. Four new window tests cover pair identity,
interleaving, whole-command barriers, non-clearing/Leased/invalid-version
replacements and queue/epoch/sequence boundaries. A native differential test
compares combined apply against individual Turso replay: full projection image,
reads, metadata/payload, versions, attempts, leases, cursors, metrics and request
receipts. It also verifies exact replay and atomic rollback of all touched tables
for an invalid re-claim and a conflict in the final replacement. Existing public
campaign, primitive, durability and log-only recovery tests pass. The 17 harness
tests passed separately. No build or test overlapped the remote benchmark.
Evidence: `fireweed-interleaved-fusion-validation.log.gz`.

The canonical rebuild and serial comparison have since completed; see the
measurement above. The fixed 10k and 12.5k sustained targets, fairness, reporting
and resource limits remain intact.

## Baldr unchanged-binary eight-cycle diagnostic (2026-09-16)

After the local three-run comparison ended, Baldr ran the identical control
executable `5d0ac295...` (build source `fa49f380`), hash-verified remotely. The
million-row/eight-cycle workload, 64 stores, two workers, handler bounds, metadata,
timestamp priority, reporting, failures/retries and retention were unchanged.
No trace flags, package installs, host tuning, other benchmarks or builds overlapped
this run. The adapted recorder explicitly reports **no source checkout** (`head:
null`, dirty/source qualification rejected), alongside binary provenance. It is
not a qualification run and its measurements must not be silently substituted
for Forseti's target measurements.

Baldr has an i9-8950HK, six physical/twelve logical CPUs, about 31 GiB RAM and
its existing encrypted Btrfs/NVMe setup. Its kernel is 7.2.3; CPU, memory, disk
and kernel differ from Forseti, so this is not an isolated disk experiment.
All eight million lifecycles complete correctly: **9,843.15/sec** overall,
813.16 seconds, 8,459.09 CPU-seconds (**1.05739 CPU-ms/recipient**) and
15.051 GiB peak RSS. Cycle rates are 10,753 / 9,965 / 10,253 / 9,487 / 9,646 /
10,468 / 10,207 / 8,946/sec. The 10k overall gate and cycles 1/3/4/7 fail;
all 12.5k cycle gates fail. RSS snapshots over the last three cycles vary by
30.59%, also failing the unchanged 10% stability gate (the final snapshot drops,
not grows). Other workload/resource gates pass, excluding the explicitly absent
source identity and diagnostic provenance.

Host observations: 32.86 GiB written, 15.55 GiB read, 41.46 MiB/sec writes,
33.38% device busy, **0.607 ms mean write request**, 0.19% full I/O pressure,
3.32% some memory pressure, and **11.66 host busy CPU-seconds/sec** out of 12
logical CPUs. The workload occupies **10.40 CPU-seconds/sec**. These host-wide
counters are not process-level device attribution. Baldr's low write latency
and high CPU occupancy support further CPU reduction; they do not prove a
specific cause for Forseti's write stalls or make Baldr a faster qualification host.
See the napkin update for explicit target resource arithmetic.

Raw report, hardware, remote hashes, recorder/runner, provenance, both target
evaluations and device samples are archived as `fireweed-baldr-owned-params-*`.
The gate now treats a null/non-string source or binary identity as a failed
identity check rather than raising TypeError. It still rejects this diagnostic;
no gate is relaxed. All 17 harness tests pass, including malformed/null identity
regressions (`fireweed-null-identity-gate-tests.log.gz`). The interleaved claim/mutation optimization is being validated
separately and was not present in this remote executable.

## Reject identity-aware join candidate after comparison (2026-09-16)

Clean candidate `49fed4e2`, executable `bde46a680e0ca4002a6ca05a90c775caf93855d415f8e9e9d61fa21ac80629e4`,
was compared serially with preserved control `5d0ac295...` (runtime `fa49f380`).
Each is the same one-million-row, one-cycle, 64-store/two-worker campaign with
metadata, timestamp priority, handler limits, retries and public reporting.
Only projection VFS write counters were enabled, identically for all three runs.
The tree stayed clean and no workloads overlapped. All three completed correctly.

| Run | Recipients/sec | CPU-ms/recipient | Requested WAL bytes | WAL calls | Accumulated WAL call seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| Candidate | 15,923.20 | 0.90288 | 6,569,980,648 | 6,346 | 23.29 |
| Control | 14,238.96 | 0.92883 | 6,588,718,408 | 6,357 | 313.59 |
| Candidate repeat | 13,784.48 | 0.96574 | 6,676,404,368 | 6,413 | 155.36 |

Candidate WAL volume ranges from 0.28% below to 1.33% above control; CPU cost
and throughput also straddle control. No isolated benefit is established.
All 64 WAL handles report zero write errors; main-file writes are 262,144 bytes
in every run. Accumulated VFS times overlap across stores and are neither CPU
nor elapsed campaign time. Host mean write-request latency is 3.45 / 14.55 /
11.05 ms. These screens do not qualify either sustained milestone.

Remove the candidate predicate and its candidate-specific tests, retaining this
comparison and the validation evidence. The production code returns to the
previously validated owned-parameter implementation. Avoid retaining extra
per-selection bookkeeping or altered waiting behavior without measured benefit.
Artifacts: `fireweed-claim-identity-comparison-*` and exact runner/recorder.
Next use the already-authorized idle Baldr host for an unchanged-binary diagnostic;
explicitly account for its different CPU/memory/disk rather than interpreting
raw cross-host rates as an isolated disk speedup. No host configuration changes.

## Match claim-followup identities before releasing the join (2026-09-16)

The coordinator now tracks outstanding claimed item IDs in command order. A
complete finalization or lease-invalidating replacement only closes the matching
claims; completing an older batch no longer immediately releases a newer claim
in the same selected prefix. Re-claiming an item after completion starts its
wait again. The original 500 ms deadline, dependent-read coverage bypass,
per-queue FIFO selection, generation bounds and empty-command behavior remain.
No SQL, schema, log format, runtime API or workload gate changes are involved.

**323 local release tests pass**, with four existing diagnostics ignored and two
unconfigured live-S3 checks filtered. New regressions cover unrelated/partial
completions, re-claims, matching versus non-invalidating metadata replacements,
the unchanged original deadline, immediate release after all completions and
coverage bypass. Existing queue-fairness, fault/retry, public durability, campaign,
primitive, differential and log-only recovery tests also pass. The earlier focused
coordinator run passed 35 tests before the metadata case was added. No other
build, test or benchmark overlapped these runs. Evidence:
`fireweed-claim-identity-{tests,validation}.log.gz`.

**Performance has not been measured for this candidate.** Preserve the old
`5d0ac295...` executable (`/tmp/fireweed-workload-before-claim-identity`) as the
control, rebuild the canonical CLI and compare serially with exact provenance.
Measure both CPU and projection write volume; only a promising screen warrants
another full repeated qualification. Both sustained milestones remain open.

## Current-binary CPU attribution (2026-09-16)

After the failed repeat and offline WAL read, a separate one-cycle million-row
campaign sampled only the workload process with installed pacman `perf`: `cycles:u`,
49 Hz, 4,096-byte DWARF stacks. Source `56789181`, runtime `fa49f380`, executable
`5d0ac295...`; default filesystem policy and unchanged handler/reporting workload.
This instrumented screen completed correctly at 15,692/sec, 0.90705 CPU-ms/recipient,
and 14.114 GiB peak RSS. It is not qualification or a comparison speedup.

There are 89,279 SAMPLE events, 96 reported LOST_SAMPLES and nine lost chunks.
Sampling loss and truncated/unwound stacks limit attribution. The previously
smoke-tested offline-only `fireweed-perf-unwind-bias.c.txt` shim corrects perf's
module bias and synthetic-thread lookup; it was not loaded into the workload.
Inclusive user-cycle percentages overlap: Turso `normal_step` 60.85%, index B-tree
seek 14.72%, `op_column` 14.66%, accepted-claim realization 8.66%, addressed mutation
planning 5.55%, public membership reporting 4.24%, retained-item query 3.87%.
Allocator `_mi_page_malloc_zero` has 5.50% self attribution, `mi_free` 2.06%.
User-cycle samples do not measure blocked time or kernel CPU.

Source inspection also finds the same-queue join predicate stops waiting after
*any* complete finalization or lease-invalidating mutation, even when a different
claim in the selected prefix remains outstanding. Next test a bounded candidate
that tracks those claimed item identities in command order, retaining the existing
500 ms deadline, coverage bypass, queue fairness and generation budgets. This
might avoid materializing intermediate leases; frequency and benefit are unproven.
Do not extend the delay or change the workload to demonstrate a gain.

Exact recorder, provenance, raw perf data, loss statistics, self/inclusive reports
and device observations are archived as `fireweed-owned-params-profile-*` and
`fireweed-run-owned-params-profile.py.gz`.

## Owned-parameter repeated qualification fails (2026-09-16)

All four serial runs used clean source `8be4986ab85071eb3864bb44b6edff29d7c125e3`
and executable `5d0ac2955435d238bb631b9e432e8834c6fe5d9f53964825f70d98097df9be8f`.
Runtime implementation is `fa49f380`; intervening commits record evidence.
Canonical filesystem policy, 64 stores, two workers, eight million original-row
lifecycles and all existing campaign checks are unchanged. No build, test or
benchmark overlapped the sequence.

| Campaign | Overall recipients/sec | Slowest cycle/sec | CPU-ms/recipient | Peak RSS GiB |
| --- | ---: | ---: | ---: | ---: |
| 1 | 14,071.58 | 12,077.85 | 0.94603 | 19.206 |
| 2 | 11,312.26 | 8,734.21 | 0.98477 | 18.569 |

Campaign 1 passes every 10k check, but cycles 6/7 miss 12.5k. Campaign 2 misses
10k in cycles 4/7 (8,734.21 / 9,842.18/sec), and misses 12.5k overall and in
cycles 2/4/5/6/7. Both pass every non-rate gate, including public correctness,
reporting, recovery, fairness checks other than the cycle-rate floor, claim
latency and WAL/database/RSS stability. **Neither repeated milestone is met.**

| Primitive run | Insert/sec | Enrich by key/sec | Schedule by ID/sec |
| --- | ---: | ---: | ---: |
| 1, between campaigns | 115,334 | 104,108 | 104,548 |
| 2, after campaign 2 | 38,147 | 39,231 | 55,970 |

Both primitive runs pass all qualification gates. These are batched public API
operations, not independent individually durable single-row RPCs. Host mean
write-request latency increases from 7.43 to 29.89 ms between campaigns; primitive
latencies are 1.74 and 39.69 ms. This is evidence of changing write service, not a
proof of a specific device mechanism. No isolated benefit of parameter ownership
is claimed. Reports, both target evaluations, external monitors and exact runners
are archived under `fireweed-owned-params-repeat-*`, `fireweed-owned-params-eight-*`
and `fireweed-owned-params-followup-*`.

### Bound transaction-combining benefit before implementation

After the series ended, an offline read of retained WALs from the earlier
`fireweed-campaign-trim-register-copy-eight` run sampled shards 0/1/2. The parser
validates the WAL header and every frame checksum/salt, reads at most 32,768
frames per file, and excludes an incomplete trailing transaction. The 200
complete transactions contain 96,720 frames, with no duplicate page within a
transaction. Optimally choosing nonoverlapping adjacent transaction pairs could
eliminate at most 6,648 frames (**6.87%**) in this sample. Fixed pairing saves
1.75–7.17% per shard, depending on offset.

This optimistic bound ignores queue identity, readiness, isolation and batch
limits. It is a historical prefix of the final WAL generation, not representative
coverage of every stage or a batching speedup measurement. It does not justify
claiming cross-queue batching will close the worst-cycle gap (43.1% improvement
needed from 8,734 to 12,500). Keep that proposal unimplemented for now. Next
refresh CPU attribution on the current binary and measure log/apply waiting;
do not repeat full qualification without a new candidate or diagnostic question.
The bounded read-only parser and results are archived as
`fireweed-wal-overlap.py.gz` and `fireweed-owned-params-wal-overlap.json.gz`.

## Owned-parameter comparison is inconclusive (2026-09-16)

Clean candidate source `fa49f380`, executable
`5d0ac2955435d238bb631b9e432e8834c6fe5d9f53964825f70d98097df9be8f`,
was compared serially with preserved `dcaf90ac...` (build source `64eb686f`).
Each run uses one million rows, 64 stores, two workers, all handler limits and
public correctness checks, default co-located filesystem roots and no forced
compression property. The adapted recorder selects an explicit executable and
records its build-source provenance separately from the current recorder source.
No runs overlap. These are one-cycle screens, not sustained qualification.

| Untraced run | Recipients/sec | CPU-ms/recipient | Sampled mean CPU MHz |
| --- | ---: | ---: | ---: |
| Candidate | 15,985 | 0.88892 | 3,309 |
| Control | 14,802 | 0.91175 | 3,061 |
| Candidate repeat | 14,362 | 0.96974 | 2,871 |

The candidate results straddle the control. Frequency samples average all logical
CPUs and are not execution-weighted; do not use them as an exact normalization.
A second serial comparison uses `perf stat` around only the workload process and
its threads, excluding the recorder/monitor. Memory sampling follows the actual
workload child rather than the perf wrapper. Both counters report 100% running
coverage:

| Counter run | User instructions/recipient | User cycles/recipient | Recipients/sec |
| --- | ---: | ---: | ---: |
| Candidate | 2,801,930 | 2,388,664 | 15,543 |
| Control | 2,831,849 | 2,401,067 | 15,112 |
| Candidate repeat | 2,857,474 | 2,364,057 | 13,783 |

Again, executed-instruction counts straddle the control. Claims, nonempty batches
and empty claims are identical; 1 Hz reporting performs 5,968 / 6,175 / 6,643
reads as wall time varies. No isolated throughput or instruction-count benefit
is established. The ownership transfer removes an unnecessary buffer clone and
is correctness-validated, but is not presented as the solution to WAL stalls.
Full raw reports, counters, monitors, provenance and recorder scripts are archived
as `fireweed-owned-params-{comparison,counters}-*` and associated Python scripts.

Next run the unchanged full eight-cycle candidate with tracing disabled and all
10k/12.5k gates. Do not claim either milestone from these short screens.

## Owned Turso parameters validated; performance comparison pending (2026-09-16)

The candidate adds `RelTx::execute_owned` with a default borrowing implementation,
and Turso implementations that move text/blob buffers into bound values. Resolved
lease-clearing replacements, general row inserts and payload upserts consume
their existing temporary vectors. Borrowed callers remain supported. The observed
adapter retains statement counts, bind counts and phase timing for this path;
SQL, apply transaction boundaries, authoritative-log encoding and semantics are
unchanged.

**320 release tests pass**, including the expanded cached-statement test:
1,024 owned text/blob inserts, verification after rebinding, borrowed NULL
updates, uniqueness failure, rollback and a fresh transaction. The full local
set also covers public strict/async durability, projection deletion, original-row
campaigns, differential histories, leases and log-only recovery. Four existing
diagnostics are ignored and two unconfigured live-S3 checks are filtered.
The five additional tests versus the previous 315-test set come from explicitly
including the relational crate. Evidence: `fireweed-owned-params-validation.log.gz`.

No benchmark overlapped the build or tests. Next rebuild the canonical workload
binary and compare untraced one-cycle candidate/control/candidate runs on the
default filesystem policy, recording exact executable and source provenance.
These screens estimate CPU/allocation benefit, not sustained qualification.
Only subsequent full repeated runs can establish the 10k/12.5k milestones.

## More workers increase WAL traffic; rejected (2026-09-16)

The same `dcaf90ac...` executable on clean source `f0e54a2f` completed the full
64-store, four-worker-per-campaign trace. It reaches **10,542.61 recipients/sec**,
with a slowest cycle of **6,583.64/sec**, **1.18063 CPU-ms/recipient** and
**19.43 GiB peak RSS**. Rate and RSS-stability gates fail; correctness, reporting,
latency, WAL and main-file stability checks pass. This traced run does not qualify.

WAL writes rise from 56.68 to **63.82 GB** (+12.6%); main writes rise from 2.07 to
**4.12 GB**. CPU work rises from 7,844.81 to **9,445.03 seconds** (+20.4%).
Accumulated synchronous WAL write-call time is 1,117.95 seconds across 64 stores;
main-file time is 196.87 seconds. These are overlapping VFS timings, not additive
wall time. Four workers did not improve the existing coordinator's write
coalescing in this comparison. Retain the canonical two-worker configuration.
Evidence: `fireweed-canonical-64-w4-write-trace*`.

A bounded code candidate removes an avoidable parameter copy: resolved replacement
and insert/payload helpers already own their parameter vectors, but the Turso
adapter clones their text/blob buffers through a borrowed executor interface.
Add an owned execution route with a borrowing default for other adapters, preserve
statement reuse and observation, and exercise owned text/blob inserts followed by
borrowed rebinding and rollback. This targets CPU/allocation overhead only; it
does not change SQL, transaction boundaries, log format, or claim/durability
semantics, and is not claimed to remove WAL stalls. Validate before measurement.

## Write attribution: WAL dominates projection write calls (2026-09-16)

The complete 64-store/eight-cycle diagnostic used unchanged `dcaf90ac...`,
clean source `414f73b9`, default filesystem policy and only
`FIREWEED_PROJECTION_IO_TRACE=1`. All 64 WAL and 64 main-file handles reported
close-time totals with zero write errors:

| Projection file class | Requested bytes | Write calls | Accumulated VFS time | Longest call |
| --- | ---: | ---: | ---: | ---: |
| WAL | 56,684,381,376 | 33,136 | 575.762 s | 7.044 s |
| Main/other | 2,068,824,064 | 30,791 | 67.530 s | 4.698 s |

These times overlap across stores and include time blocked inside synchronous
VFS calls; they are neither CPU cost nor additive wall-time attribution. There
were 675 WAL calls and 92 main-file calls above 100 ms. WAL appends account for
96.5% of requested projection bytes and 89.5% of accumulated projection write
call time. This makes reducing WAL page versions a better next hypothesis than
assuming main-file checkpoint writes alone explain the slowdown. Log publication
was not instrumented in this run and remains an attribution gap.

The diagnostic completes at 12,339.50 recipients/sec, 648.955 seconds process
wall, 0.98060 CPU-ms/recipient and 19.23 GiB peak RSS. Cycles 4 and 7 fail even
the 10k floor (9,598 and 9,829); all non-rate checks pass. Do not count this traced
run as performance qualification. Host writes total 31.30 GiB, illustrating why
requested VFS bytes cannot substitute for physical write counters. Full evidence
is `fireweed-canonical-64-write-trace*`.

Next compare four workers per campaign with the same tracing, 64 stores, all
million rows, eight cycles, handler/storage batch limits and reporting. More
ready commands may allow the existing bounded apply coordinator to combine more
page changes into a transaction; this is a hypothesis, not a claimed benefit.
The 8,192-item, envelope, byte, contiguous-prefix and coverage bounds remain
unchanged. Keep the canonical two-worker runner until a candidate passes
repeated untraced qualification.

## Fewer stores reduce CPU cost but not late stalls (2026-09-16)

An unchanged `dcaf90ac...` binary, clean source `88ae2078`, ran the same
million-row/eight-cycle campaign with 32 rather than 64 physical stores.
No host settings or compression properties changed. Two campaigns per store
and two workers per campaign mean total campaign/reporting streams and workers
also halve; each campaign contains twice as many rows. This is a configuration
comparison, not an isolated sharding effect or a gate change.

Overall throughput is **13,411.59 recipients/sec**, CPU cost **0.86818 ms per
recipient**, peak RSS **11.68 GiB**. The first six cycle rates are
16,924 / 16,971 / 14,462 / 17,289 / 14,228 / 15,311. The last two fall to
**8,690 / 10,690**, failing both qualification milestones. All non-rate gates
pass. Keep the canonical runner unchanged; this is not a qualified replacement.

Host writes total **36.05 GiB**, versus 30.74–31.48 GiB in the preceding 64-store
pair. Lower CPU work does not establish a sustained throughput improvement.
A late 33.95-second interval observes CPU occupancy 5.42 CPU-seconds/sec,
device busy 88.04%, mean write-request latency 77.50 ms and 75.28 MiB/sec writes.
These host observations locate an I/O-path stall but do not distinguish
checkpoint admission, filesystem work and device service. Raw evidence and the
external monitor are preserved as `fireweed-canonical-32-stores*`.

Next use the existing opt-in projection VFS write timer on a full 64-store
campaign. It separates synchronous WAL-write time from main-file write time.
Treat tracing as diagnostic overhead and retain all workload/correctness gates;
do not claim qualification from that run. No SSD tuning is proposed.

## Final repeated qualification failed (2026-09-16)

The canonical serial four-run qualification completed on clean source
`64eb686f03ece8df75c3452be5c1a0b4d3cca066`, executable
`dcaf90ac65ca719923bc000fd55b61eda531662729fde27a9f8b25b77e5eb525`.
Both campaigns passed correctness, reporting, latency, WAL, database-size and
RSS-stability gates, but failed the fixed 12,500/sec throughput requirement:

| Campaign | Overall recipients/sec | Slowest cycle equivalent/sec | CPU-ms/recipient |
| --- | ---: | ---: | ---: |
| 1 | 13,579 | 11,286 | 0.97178 |
| 2 | 12,016 | 9,065 | 1.01089 |

Campaign 2 also fails the 10,000/sec cycle floor. Neither milestone is repeatedly
qualified. Both million-row primitive runs pass all primitive gates: insert
34,390/46,048, key enrichment 33,820/44,673, and scheduling 48,205/54,395 rows/sec.
All four reports have identical clean source and binary identities. Full reports
and failed checks are preserved in `fireweed-final-register-reuse-*`; external
one-second device observations are in `fireweed-final-qualification-phase-*`.
The monitor is external and its disk counters are host-wide, not attribution to
Fireweed alone. No tests, builds or benchmarks overlapped these workloads.

The canonical runner uses default co-located log/projection roots inheriting
mount `compress=zstd:3`. Earlier manual candidate runs additionally forced a
`compression=zstd` property on a separate private projection directory. This is
a configuration difference, so the prior passing run is not a controlled
comparison against this series. Absence of the explicit inode compression flag
does not establish absence of compression.

After all four workloads exited, the exact archived 8 GiB Python calibrations
ran serially, direct then buffered, without TRIM or settings changes. Direct
private-file NOCOW/O_DIRECT writes reached **912.77 MiB/sec** (8.975 s);
normal buffered Btrfs writes reached **317.77 MiB/sec** (25.780 s). Both include
fdatasync and verify first/last blocks. These tests establish sequential
headroom at calibration time, not small-object durable-write or checkpoint
latency. They do not justify calling the campaign's 47–55 MiB/sec average a
raw device bandwidth ceiling. Campaign mean write-request latency rose from
6.23 to 27.37 ms; primitive runs observed 32.96/30.72 ms. Those are observed I/O
waiting symptoms, not an identified root cause.

Next isolate application I/O shape: correlate checkpoint/write bursts with
slow stages, and compare fewer physical stores using the unchanged million-row,
eight-cycle workload and stage limits. Keep host configuration and filesystem
policy fixed. Preserve failed candidates; require repeated full passes before
claiming qualification. The Turso migration and log-derived receipt repair
remain validated by 315 local release tests and 16 harness tests.

## Upgrade repair validated; final qualification next (2026-09-15)

Normal startup now repairs old 32-byte push fingerprints from the original
fingerprints in the authoritative log's existing recovery scan. Updates match
queue, request ID and exact `(epoch, sequence)` receipt position, and only old
32-byte rows. The operation is transactional and idempotent; it does not alter
receipt expiry, response IDs, log entries, or add serving-path work. A newer
reuse of a request ID cannot be overwritten by an older envelope.

The expanded local release set passes **315 tests**, with four existing
diagnostics ignored and two unconfigured live-S3 tests filtered. The new adapter
regression checks wrong positions/queues and repeated repair. Public strict and
async tests cover normal reopen, complete projection loss, and an existing
old-format receipt, including same-body replay and changed-indexed-entity
conflict rejection. Evidence: `fireweed-push-receipt-upgrade-validation.log.gz`.
This closes the earlier upgrade concern for the durable-log path tested here.

All **16 capacity-harness tests** also pass, including the qualification runner
and failure-preservation checks (`fireweed-final-capacity-harness-tests.log.gz`).

Next run `scripts/perf/qualify-workflow-capacity.sh` on the final clean source:
two unchanged million-row/eight-cycle stretch campaigns and two million-row
varied-body primitive runs. No host tuning or gate changes accompany it. The
previous full candidate passed once; final repeated qualification remains open.

## Register-reuse sustained candidate passes once (2026-09-15)

The `a9605e58...` candidate passes every 10k and 12.5k gate over eight million
complete recipient lifecycles: **14,166/sec overall**, **13,242/sec slowest
cycle**, **15.70 GiB peak RSS**, and **0.97191 CPU-ms/recipient**. This is one
full pass, not repeated qualification. Preserved evidence:
`fireweed-campaign-trim-register-copy-eight*`.

The 535 memory samples have no collection errors. Process swap reaches
3.79 GiB; major faults increase by
763,808, while host available memory remains above
41.01 GiB. Swap is zram, swappiness is 150,
and cgroup memory limits are unlimited with no high/max/OOM events. These are
observations, not proof that swap bounds throughput; the run passes unchanged
gates. No host settings were changed. Device writes total 28.03 GiB, mean write
request latency 3.31 ms, device busy 31.7%. Evidence includes the late-run
`fireweed-register-copy-eight-memory-context.json`.

At this run's CPU cost, 12.5k recipients/sec requires **12.15 CPU-seconds/sec**;
the measured process averages **13.75 CPU-seconds/sec**. This supports a modest
CPU margin, not a sequential-SSD-derived throughput prediction. The pending
request-fingerprint upgrade repair must be validated before repeating full
qualification on the final binary.

## Unprofiled register-reuse diagnostic comparison (2026-09-15)

Three sequential, unprofiled one-cycle million-recipient campaigns compare the
new `a9605e58...` executable with the preserved `be319723...` baseline:

| Run | Recipients/sec | CPU-ms/recipient | Peak RSS GiB |
| --- | ---: | ---: | ---: |
| Candidate | 15,605 | 0.90707 | 13.49 |
| Preserved baseline | 15,157 | 0.91424 | 12.87 |
| Candidate repeat | 15,363 | 0.91888 | 13.06 |

All three finish correctly. Candidate throughput is 1.36–2.96% above the single
control, but CPU cost straddles it; this does not establish a sustained benefit.
The baseline also predates the fingerprint correctness fix, so this is not an
isolated causal estimate for register reuse. Baseline executable provenance is
explicitly marked diagnostic. One-cycle runs cannot satisfy qualification's
repeated-cycle storage, RSS and fairness checks. Proceed to the unchanged
64-store/eight-cycle campaign before deciding whether to retain this candidate.
Evidence: `fireweed-register-copy-diagnostic-comparison.json` and
`fireweed-campaign-trim-register-copy-*`.

## Retired SQLite tests and durable request identity (2026-09-15)

The broader release invocation exposed stale `public_durability_matrix` calls
to retired SQLite constructors. The two SQLite-only cases were duplicate runs
of the same fixture and are removed. Filesystem-log/Turso strict and async tests
now cover request replay, conflict rejection, batch update replay, original-row
fields/payload, queue definitions and pending counts after ordinary reopen and
after deleting the projection and rebuilding from the log. The original memory
projection fixture retains selector, typed-index query, gate and lease checks:
Turso currently rejects the legacy selector and index-query operations, so those
checks are not represented as passing Turso coverage.

The port found a product defect: index admission can remove a fully indexed
entity document from `PushItem`, but the projection recomputed the push request
fingerprint from those transformed items. An identical original request then
failed with `RequestIdConflict`. Projection apply now retains the original
fingerprint in the durable envelope, matching log-only replay. This is the
existing 64-bit durable fingerprint format accepted by the read path; it does
not claim a newly persisted SHA-256 request identity.

The revised matrix passes all three cases, including both Turso barriers with
and without projection files. The negative assertion changing only the fully indexed entity also passes.
The expanded local release validation passes **314 tests**, with four existing
diagnostics ignored and two live-S3 tests filtered because no endpoint is
configured. This includes facade/log unit tests, native projection adapter
tests, the migrated matrix, and CLI campaign/primitive/log-only recovery tests.
Archived red/green matrix logs and `fireweed-register-copy-local-validation.log.gz`
document the request-replay regression and recovery checks.
Existing projections with old recomputed 32-byte push fingerprints remain an
upgrade concern: a log-only rebuild repairs them, but normal reopen currently
preserves already-applied rows. Address automatic repair or an explicit upgrade
procedure before releasing this change. The register-reuse optimization is
still unqualified; no throughput claim follows from these correctness tests.

## CPU profile and register-copy candidate (2026-09-15)

The installed pacman `perf` 7.2.3 samples a one-cycle, 64-store default-runtime
campaign at 99 Hz (`cpu-clock:u`): **74,182 samples, zero losses**. This is a
flat user-CPU diagnostic, not a qualification result or an off-CPU analysis.
Self attribution includes `_mi_page_malloc_zero` 6.96%, `mi_free` 2.24%,
Turso `op_column` 5.17%, `Program::normal_step` 4.47%, B-tree move/seek and
record comparison, and `op_copy` 0.78%. Helpers inherited profiling too;
the report retains DSO attribution to distinguish Fireweed from the runner.
Evidence: `fireweed-trim-cpu-profile.json`, `fireweed-trim-cpu-self.txt`,
`fireweed-trim-cpu.perf.data.gz`, and `fireweed-campaign-trim-cpu-s64-w2-one*`.

The next bounded candidate reuses existing owned text/blob buffers for VDBE
register copies and parameter loads. It must retain independent ownership,
text subtypes, unbound-parameter NULL behavior, static-text allocation-free
copies, and sequential overlap semantics. All three focused regressions pass,
including the static-text refinement. Full native release testing passes 2,118
checks, with 16 ignored and two failures. Repeating the same suite on unchanged
code passes 2,115 checks and reproduces both failures at the same assertions:
`test_wal_readlock0_optimization_behavior` (slot-zero expectation) and
`test_make_sure_correct_insn_table` (`StructField` function-address equality).
This establishes no new failures in this suite, not a fully green native suite.
Evidence: `fireweed-register-copy-native-comparison.json` and both native logs.
The candidate also passes the expanded 314-test local release/recovery set.
No performance improvement is claimed yet; a short comparison precedes full
campaign qualification.

## One-worker runtime rejected after repaired-storage control (2026-09-15)

The `OBJECT_LOG_FLUSH_RUNTIME_THREADS=1` eight-cycle control completes correctly
at **13,126 recipients/sec**, with **11,161/sec** in the slowest cycle. All 10k
gates pass; stretch throughput fails in cycles 4, 5 and 6. CPU cost increases to
0.9950 ms/recipient, versus 0.9545 ms in the preceding default-runtime run;
peak RSS is 19.13 GiB. This does not establish a benefit from reducing runtime
threads on the repaired host. Retain the default eight workers per store.
Evidence: `fireweed-campaign-trim-flush1-s64-w2-eight*` and its summary.

The next diagnostic samples CPU execution in a shorter representative campaign
with the default runtime. Profiling results are not qualification runs; full
eight-cycle repetition remains required for any retained optimization.

## Memory-attributed repeat and runtime control (2026-09-15)

The unchanged 64-store/eight-cycle campaign with external memory sampling
passes every 10k and 12.5k gate at **14,393 recipients/sec**, slowest cycle
**12,776/sec**, peak RSS **18.79 GiB**. There are 527 memory samples without
errors or PID identity changes. Process swap stays zero, major faults total
one, and host available memory remains above 37.8 GiB. RSS drops occur in
anonymous memory; this run does not support blaming swap or host memory
shortage. It does not retroactively prove the cause of the earlier failed run.
Evidence: `fireweed-campaign-trim-memory-s64-w2-eight*` and its summary.

Two of the three repaired full runs now pass, but one late-cycle failure and
the narrow final-cycle margin warrant improving consistency. The next control
sets only `OBJECT_LOG_FLUSH_RUNTIME_THREADS=1`, retaining 64 stores, two
workers/campaign, eight in-flight log flushes/store, all durability barriers,
and the entire eight-cycle workload. The current default is eight runtime
workers/store (512 workers across the test), versus 64 with this setting.
Earlier pre-repair evidence showed a modest benefit; it must be remeasured.

The checked-in `qualify-workflow-capacity.sh` now runs two representative
million-row eight-cycle campaigns at the stretch target plus two varied-body
primitive runs, replacing its stale generic 500k-row recipe. It records the
inherited runtime setting and preserves a failed attempt even if later runs
succeed. CLI/source changes are not needed for the runtime control.

## Current status: post-TRIM repeat still misses stability (2026-09-15)

The unchanged CLI `be319723...` passes every 10k and 12.5k gate in its first
post-repair eight-cycle million-resident-row run: 15,064 complete recipients/sec
overall, 14,342/sec in the slowest cycle, 18.49 GiB peak RSS. The repeat reached
14,096/sec overall but failed final-cycle throughput (11,548/sec) and RSS
stability; every other gate passed. Two refreshed primitive runs pass all gates
with insert/key-update/ID-update rates around 99k–118k/sec. The full goal remains
unmet. Investigate late-cycle CPU and memory/cache reclamation next; do not
weaken the stability gate or infer a new SSD ceiling.
See [disk baseline and napkin math](disk-baseline-and-napkin-math.md) for the
local triage, verified discard repair, and updated CPU/write cost model.
The entries below are retained history and their superseded next steps.

The capacity harness now samples child RSS, anonymous/file memory, swap,
minor/major faults, and host available/cache/dirty memory once per second in
the existing external WAL-monitor thread. This does not alter the workload,
database connections, CLI binary, or acceptance gates. All 14 Python capacity
tests pass, including proc field indexing and child-exit races. The next
unchanged eight-cycle run uses those counters to explain the final-cycle RSS
drop before choosing a cache or allocator change.

Current retained code: native-memory SQL scratch (`ea4805cc`), with **311
release checks and nine capacity-harness checks passing**. Both million-row
varied-body primitive runs on CLI `be319723...` pass the three 10k floors:
inserts 109,125 / 32,667 per second; key-addressed updates 30,541 / 20,354;
ID-addressed updates 42,043 / 56,869. Claim/completion and purge also exceed
10k. These use 1,000-row API batches, 32 independent stores, and one
sequential batch loop/store; they are not unbatched individual-request rates.
All 64 DB/WAL properties per run read back zstd, with 4 KiB DB pages.
Evidence: `fireweed-memory-scratch-primitive-pair.json` and
`fireweed-memory-scratch-varied-primitives-*`.

The eight-million-recipient campaign passes every non-throughput gate, but
**the sustained 10k/12.5k goals remain unmet**: 11,358/sec overall and
8,887/sec in the slowest cycle. Its untraced cost is 1.00848 CPU-ms and
about 3,299 host-write bytes per recipient. At 12.5k, constant-cost demand
is **12.61 CPU-s/s and 39.33 MiB/sec host writes**. That does not establish
a hardware ceiling. The earlier write trace measured projection WAL calls
as long as 4.45 seconds, even though projection sync is omitted. Local rechecks
now show 741–886 MiB/sec short sequential bursts but
38.5–45.0 MiB/sec sustained 8 GiB writes. The normal buffered test was observed
waiting in `balance_dirty_pages`. These distinguish burst bandwidth from
sustained writeback without establishing the cause. Another host is not a
prerequisite: continue local code and I/O attribution. See the dated recheck in
`workflow-hardware-headroom.md` and its archived scripts/results. The other
local disk is an unmounted BitLocker system volume and has not been used.
No SSD/host options were changed, and no release or push was performed.

The eight-cycle run (`b33e8806`, runtime `ea4805cc`, CLI `be319723...`)
completed eight million recipient lifecycles at **11,358/sec overall** in
705.07 seconds, using 1.00848 CPU-ms/recipient and peaking at 16.36 GiB RSS.
**Every non-throughput gate passed**, including independent outcomes,
reporting, due latency, sampled WAL bounds, RSS stability, materialized
checkpoints, and main-file stability across all 64 stores. Throughput remains
unqualified: the slowest cycle was 8,887/sec, and the overall rate was below
12.5k. The first three cycle-equivalent rates passed 12.5k; a later load
phase took 40.20 seconds. This validates bounded behavior for the measured
workflow, not arbitrary unbounded SQL. The CPU improvement is retained,
while both sustained performance goals remain unmet.

Raw evidence uses `fireweed-campaign-memory-scratch-s64-w2-eight*`,
including a reproducibly selected high-latency device window. The next
verification is the pair of million-row varied-body primitive benchmarks on
the same candidate. The earlier requirement for another storage path/execution host is withdrawn;
the local recheck above supersedes that proposed blocker. No host SSD settings were changed.

The native-memory scratch six-cycle run completed correctly at **10,900
recipients/sec overall** (551.01 seconds, 1.02868 CPU-ms/recipient, 16.20 GiB
peak RSS). It does not qualify either goal: cycle five fell to 7,766/sec,
with a 54.97-second load phase. RSS, sampled WAL and reporting gates passed.
Host writes averaged 35.22 MiB/sec with 36.13 ms mean write-request latency.
The apparent main-file instability was initial checkpoint materialization:
materialized store counts were 0/0/0/7/64/64, and **all 64 main-file sizes
were identical in the final two cycles**. Thus 57 last-three-snapshot checks
straddled a header-only file and its first populated checkpoint.

Qualification now separately requires all three final main-file snapshots
to exceed the 4 KiB header of these fresh campaign databases. A regression
demonstrated the old gate accepting constant header-only files; all nine
Python capacity tests pass with the stronger check. The next 64-store run
uses **eight cycles**, keeping every cycle in throughput/latency gates, to
observe three snapshots after initial materialization. This lengthens the
test; it does not omit startup or relax the 5% size/10% RSS stability rules.
Evidence: `fireweed-campaign-memory-scratch-s64-w2-six*` and
`fireweed-materialized-gate-{before,after}.log`.

The SQL scratch candidate (`ea4805cc`, CLI `be319723...`) advances to sustained
validation after the ordered baseline/candidate/candidate/baseline comparison.
Rates were 13,718 / 13,716 / 13,722 / 12,061 recipients/sec; CPU-ms per
recipient were 0.95736 / 0.92794 / 0.94777 / 1.03116. Mean CPU cost fell
5.67%, with both candidate values below both controls; control variability
limits causal precision. Candidate peak RSS was 12.99/13.09 GiB versus
13.91/13.25 GiB for controls. All children passed workflow correctness,
but these single-cycle trials do not qualify throughput or long-run stability.
The exact preserved baseline binary was an explicit unqualified source
override; all private projection roots were kept until timing finished, then
archived and removed. Evidence: `fireweed-memory-scratch-comparison.json`
and `fireweed-campaign-{memory-scratch,scratch-baseline}-*`.

The native-memory SQL scratch candidate passes **311 release checks**, with
four ignored diagnostics and two unconfigured live-S3 exclusions. Log-backed
connections select and verify `temp_store=MEMORY`; standalone defaults stay
unchanged. A regression checks writer, serving, pooled-driver and transient
recovery connections, independently verifies sorted/distinct results, and
checks that the main database and WAL remain files. This removes filesystem
scratch creation, not persistent projection storage. Performance remains
unestablished; native memory temp storage disables sorter spilling, so
representative memory stability is a retention requirement, and arbitrary
unbounded SQL is not justified by this workload.

The completed trace updates the constant-cost napkin estimate: 1.03774 CPU-ms
and about 3,291 host-device write bytes per recipient imply **12.97 CPU-s/s
and 39.23 MiB/sec host writes at 12,500 recipients/sec** (10.38 CPU-s/s and
31.38 MiB/sec at 10k). Projection WAL logical writes alone are 7,751 bytes
per recipient, implying 92.4 MiB/sec at 12.5k before filesystem compression.
Logical VFS bytes, log command bytes and physical host bytes must not be
added together or treated as interchangeable. These are workload-cost
extrapolations, not device ceilings; raw inputs and caveats are in
`fireweed-restored-write-trace-napkin.json`.

A six-cycle trace of the restored runtime (`6e257bfe`, CLI `66b85c4a...`)
completed successfully in 598.12 seconds at **10,040 recipients/sec overall**,
but does **not** qualify: diagnostic tracing was enabled, three cycles were
below 10k, all six were below 12.5k, and shard 57 missed projection stability.
WAL and reporting gates passed. Summed VFS write-call wall time across threads
was **1,535.75 seconds for projection WAL**, 60.16 seconds for main files,
and 21.52 seconds for temporary files. The largest WAL call took 4.453 seconds.
These overlapping lifecycle totals include shutdown and are neither CPU time
nor physical-device service time. They weaken the case for focusing exclusively
on checkpoint write admission. Host device writes averaged 31.63 MiB/sec
(18.39 GiB total), with 48.90 ms mean write-request latency.

The trace also found **463,230 temporary files, each written exactly once for
4 KiB** (1.767 GiB requested). An initial live interpretation that these were
empty trace records was wrong and is explicitly retracted. They are native
SQL intermediates created eagerly by `TempFile::with_temp_store` and
`OpenEphemeral`; `/tmp` is tmpfs here, so they are not SSD write traffic.
The next bounded experiment uses native memory scratch for log-backed SQL
connections, measuring CPU and resident-memory stability. The projection
database and WAL remain files. Native MEMORY temp storage disables sorter
spill limits, so memory behavior and the typed workload bounds must be
reviewed before retention. Evidence uses
`fireweed-campaign-retained-write-trace-s64-w2-six*`.

The apply-read reuse candidate (`cf4e26f8`) is **rejected and removed**.
The ordered baseline/candidate/candidate/baseline million-recipient runs
measured 13,599 / 13,752 / 11,871 / 12,706 recipients/sec and
0.97082 / 0.96557 / 1.00876 / 0.97348 CPU-ms per recipient. Mean CPU cost
increased 1.54%; the small first-pair gain did not repeat. All children
completed successfully, but none is a sustained qualification pass. The
baseline wrapper selected the exact preserved `8a12e2de` runtime and marked
its source override as ineligible for qualification. All projection roots
were retained through the entire comparison, then archived and removed.
Evidence is `fireweed-read-reuse-comparison.json` and the four
`fireweed-campaign-read-reuse-*` artifact groups. The runtime is restored
to the previously tested implementation; the new ignored query-plan
diagnostic remains. The 10k/12.5k sustained goals remain unmet.

The apply-read statement reuse candidate passes **311 release checks**, with
four ignored diagnostics and the same two unconfigured live-S3 exclusions.
Read queries now share the existing bounded statement cache with writes only
inside one owned apply transaction; rows are fully consumed into owned values.
The new regression covers read-after-write visibility, NULL/empty rebinding,
returned-value ownership, rollback, and VM reuse after an evaluation error.
Its initial combined error/rollback check incorrectly assumed Turso kept an
explicit transaction open after integer overflow; normal rollback and error
reset are now checked separately. Performance is not yet established.

A retained-statement diagnostic with 20,000 resident rows rejected a simpler
replacement UPDATE without the extra target alias: native EXPLAIN chose a
queue-prefix search followed by scanning the incoming batch, rather than
full item-key lookups. ANALYZE did not repair this, nor did an experimental
VALUES cardinality estimate; that native-core experiment was removed. The
retained joined statement measured 50,644–51,623 updates/sec across these
isolated runs. These are SQL diagnostics, not campaign qualification, and
production SQL is unchanged. Raw output is archived under
`fireweed-direct-*-diagnostic.log`. The next code experiment reuses read
statement execution state within the already-owned apply transaction.

The staggered-checkpoint candidate (`3b8f1036`) is **rejected and reverted**.
It failed naturally during cycle three with another 30-second post-position
produce timeout, after 380.17 process seconds. The planned early-stop command
found the process already gone and sent no signal. Completed cycle rates were
12,528 and 9,540 recipients/sec; reporting p95 was 0.333/0.225 seconds. There
is no complete six-cycle throughput or reliability pass. Twenty main databases
had materialized at failure, versus nine after the last completed cycle.

A recorded 29.053-second interval had 5.675 MiB/sec writes, 121.1 completed
write requests/sec, 1.926-second mean write latency, 233.3 mean outstanding I/O
requests, 99.46% device busy and 2.27 application CPU cores active. Spreading
checkpoints did not resolve the storage-path stall. The exact interval was
reconstructed from archived device samples; it is a diagnostic interval, not
a hardware maximum or a whole-run average. Both failed roots are retained:
`failed-run-xk7sac3j` (authoritative log) and the corresponding
`compressed-staggered-checkpoints-s64-w2-six-*` projection root under
`target/workflow-capacity`. Evidence uses
`fireweed-campaign-staggered-checkpoints-s64-w2-six*`.

The 448 MiB checkpoint policy is restored. **310 release checks pass**, with
three ignored diagnostics and the same two unconfigured live-S3 exclusions.
The failed-run preservation and surfaced shard identity fixes remain. An alternate execution location was previously requested, but is not a
prerequisite; local code review and failure analysis can continue. The 10k/12.5k goals remain unmet.

The retained-code 64-store six-cycle attempt (`a3b24317`, runtime `8a12e2de`)
**failed during cycle five**, after 388.40 process seconds, with an object-log
post-position produce timeout. Four completed cycles had gate-derived rates
14,142 / 12,288 / 11,677 / 11,301 recipients/sec and reporting p95 maxima
0.733 / 0.239 / 0.251 / 0.298 seconds. These partial results do not qualify
10k or 12.5k. The projection root is intentionally retained for investigation;
the old harness deleted its log root, so log-based postmortem recovery from this
particular run is unavailable. Raw reports, failure analysis and device windows
are archived as `fireweed-campaign-retained-partial-keys-s64-w2-six*`.

The final measured 21.7 seconds had 99.46% device busy, 1.04-second mean write
request latency and roughly one application CPU core active. Main-file
materialization increased from five stores at the last completed cycle to 37
at failure. The next bounded code experiment spreads rebuildable-projection
checkpoint budgets across 192–448 MiB using a reproducible hash of the configured
path. This keeps large coalescing windows and the existing upper bound while
reducing identical checkpoint triggers. It can increase main-file write volume;
only sustained measurement can decide whether the tradeoff is beneficial.
Standalone projection behavior and the 512 MiB observed-WAL gate stay intact.
A single cycle would not adequately exercise this policy, so validation proceeds
to a full sustained run after the release checks.

The capacity harness now preserves an automatically-created log root when the
child fails and reports its retained path. Explicit roots remain at their
requested path; successful automatic roots are still cleaned. A subprocess
regression demonstrates the old harness deleting the failed-run log; all nine
capacity artifact/gate Python tests pass with the fix. Post-position errors also
retain their shard identity in the surfaced error, aiding the next investigation.

The column-position cache candidates are **rejected and removed**. A fresh
baseline/candidate/candidate/baseline comparison used the exact preserved
pre-cache CLI (`7297c9fc...`, runtime `8a12e2de`) and wider-record candidate
(`91145cb7...`, runtime `12d21797`). Mean CPU cost was 1.02211 versus
1.01354 ms/recipient: only 0.84% lower, with individual rates ranging from
11,014 to 13,640/sec. This does not establish a useful, repeatable gain.
All owned roots remained present until the four timings finished, then were
archived and removed. Baseline wrapper provenance explicitly identifies its
runtime source and prevents qualification of the diagnostic override.

Runtime sources under `crates/`, `vendor/`, and Cargo manifests/lock are now
byte-for-byte back at the validated `8a12e2de` baseline. Its exact previously
validated CLI is restored; this rollback does not claim a new build or new
310-test run. The experimental patches and their 312 passing checks remain
in history, with raw comparison evidence in `fireweed-column-cache-abba-*`
and `fireweed-campaign-{column-baseline,wide-column-cache}-s64-w2-*`.
Next is a clean sustained 64-store run of the retained fixes; the fixed
10k/12.5k targets and every correctness/reporting/storage gate remain intact.

The all-record column-position cache (`c67da262`) passed **312 release checks**
(four ignored diagnostics, two unconfigured live-S3 exclusions), but its clean
64-store first cycle did **not** demonstrate a workflow gain: 13,749.57/sec,
0.96637 CPU-ms/recipient, 14.13 GiB peak RSS and 0.541-second reporting p95.
The preceding partial-index candidate observed 14,287.40/sec and 0.94561 ms.
All single-cycle application checks completed; the qualification report correctly
rejected the insufficient cycle count. Evidence uses
`fireweed-campaign-column-cache-s64-w2-one*`; the owned root was removed.

A focused parsing diagnostic showed a roughly one-third reduction for 20-column
records, but no reliable improvement for eight-column records. The next bounded
candidate leaves short index records and column zero on the original decoding
path, using the cache only for wider records. It still needs full validation and
campaign measurement. No sustained performance improvement is claimed.

The durable publication comparison did not establish a preferred protocol:
immutable runs differed by 12x (373.03 versus 31.03 MiB/sec), with append
runs at 48.35 and 91.32 MiB/sec. Identical streams were verified. This is
neither workflow qualification nor evidence of an SSD ceiling. Next work
returns to the measured projection CPU hot paths; the fixed sustained
10k/12.5k targets remain unmet.

The two-cycle 48-store DWARF profile (`2eabe833`, runtime `8a12e2de`)
completed at 8,400.17 recipients/sec. It is diagnostic, not qualification.
There were 79,941 CPU samples and zero recorded losses. Offline fixes for
perf 7.2 module load bias (executable virtual address minus file offset) and
its reused synthetic thread ID recovered multi-frame stacks for 79,884
samples. A two-thread Rust smoke test validated all 44/46 sampled leaf-to-parent
paths. The raw profile, fixes, source/provenance and resolved stacks are archived
as `fireweed-stack-cpu-2eabe833*`; uncorrected caller reports are explicitly
invalid. The owned projection root was removed after attribute capture.

Inclusive CPU attribution: Turso normal statement execution 67.66%, retained
apply execution 38.86%, row iteration 30.74%, and resolved clearing replacements
16.15%. These overlap and must not be added. Direct workload body construction
was 1.71% inclusive. This identifies substantial projection CPU work; it does
not measure off-CPU publication waits. Next is an isolated, durable publication
protocol comparison on the same filesystem before considering a log-layout
change. It compares immutable data/manifest files with two synced append writes,
using identical streams, and does not count as workflow qualification.

Partial-index key guard (`8a12e2de`), clean 64-store/two-worker single cycle:
**14,287.40 recipients/sec**, 70.22 seconds, CPU 0.94561 ms/recipient, peak
RSS 13.73 GiB, reporting p95 0.532 seconds. Compared with the preceding
readable-index observation (13,418.17/sec, 0.97445 ms), throughput rose 6.48%
and CPU cost fell 2.96%. This is a useful serial observation, not replicated
sustained qualification. Host writes were 2.69849 GiB at 40.62 MiB/sec.
Evidence uses `fireweed-campaign-partial-index-keys-s64-w2-one*`; its owned
root was removed. Next is a diagnostic DWARF caller profile through two
48-store cycles to attribute remaining costs before further code changes.

The retained-VM projection diagnostic rejected direct point replacements:
48,624/49,147 updates/sec versus 53,322/53,644 for the existing 56-row joined
query. Parameters were built outside timing; both paths used the actual
transaction-local statement reuse adapter. This is a SQL diagnostic, not a
workflow rate. Production keeps the joined query.

A subsequent native regression found UPDATE evaluates partial-index new keys
even when the new row fails the index predicate. SQLite skips those keys;
Turso currently raises integer overflow for an unused `abs(i64::MIN)` key.
A candidate moves the predicate guard before expression/key construction.
The regression now passes, including unique/nonunique indexes, mixed multi-row
predicate outcomes, rollback and integrity. All 310 Fireweed release checks
pass (three ignored diagnostics, two unconfigured live-S3 checks excluded).
Post-change retained-VM rates were mixed (51,424/57,371 batched updates/sec),
so capacity gain remains unproven. A clean campaign comparison follows.

The six-cycle readable-index candidate was **rejected early after cycle two**:
all 96 second-cycle campaign reports were present, with a maximum active wall
time of 102.956 seconds. That bounds global cycle throughput below 9,713/sec,
already violating both the 10k floor and 12.5k stretch target. The process was
then deliberately terminated (exit -15); no six-cycle rate or qualification
is claimed. Cycle-one maximum wall was 73.468 seconds; worst reporting p95
was 0.309/0.245 seconds in cycles one/two. The publication-path fixes have not
resolved sustained capacity. Partial reports, reason, device observations and
runner output are archived as `fireweed-campaign-readable-manifests-s48-w2-six*`.
The owned root was removed. Next is a retained-statement diagnostic comparing
projection point UPDATEs with the current bounded VALUES/join replacement path.

Readable committed index (`6ae766a6`), clean 64-store/two-worker single cycle:
**13,418.17 recipients/sec**, 74.81 seconds, CPU 0.97445 ms/recipient, peak
RSS 14.40 GiB, reporting p95 0.523 seconds. This is slightly slower than the
preceding dispatch-only observation (13,652.15/sec), so no first-cycle gain
is established. The read-progress regression is fixed; a clean six-cycle
48-store/two-worker run now tests whether the publication-path changes help
the sustained slowdown. All workload gates remain in force. Evidence uses
`fireweed-campaign-readable-manifests-s64-w2-one*`; its owned root was removed.

The manifest reader regression reproduced blocking of already committed index
reads during a new durable publication. A separate mutation-order mutex now
serializes commits and retention while short index locks expose only the last
durable state. All 45 focused log tests pass, including publication failure,
reopen and direct concurrent commits with retention. All 309 Fireweed release
checks pass (two ignored diagnostics, two unconfigured live-S3 checks excluded).
The same single-cycle comparison follows; no capacity gain is assumed.

Ordered upload dispatch (`bd2a1f25`), clean 64-store/two-worker single cycle:
**13,652.15 recipients/sec**, 73.53 seconds, CPU 0.97332 ms/recipient, peak
RSS 14.26 GiB, reporting p95 0.478 seconds. The rate difference from grouping
alone (13,546.96/sec) is under 1%, with slightly higher CPU cost; this does
not establish a throughput improvement or sustained qualification. The
regression proves upload progress during manifest I/O, but it is insufficient
to explain the remaining multi-cycle shortfall. Evidence uses
`fireweed-campaign-dispatch-manifests-s64-w2-one*`.

Next candidate: the manifest sequencer holds its index mutex across durable
publication, blocking reads of already committed entries. Prove that behavior
with a gated reader test, then separate ordered publication from index access
without exposing uncommitted entries or changing mutation ordering.

The upload-dispatch stall is now reproduced and fixed in code. A gated first
manifest prevented three subsequent uploads in the original dispatcher; the
regression failed before the change and passes with one ordered blocking commit
job running alongside upload dispatch. All 43 focused object-log tests pass,
including byte-admission release with no further upload, shutdown drain, and
commit panic. Durable publication barriers and the workload gates are unchanged.
All 309 Fireweed release checks also pass (two ignored diagnostics and two
unconfigured live-S3 checks excluded). The same 64-store/two-worker single-cycle
comparison follows; this is not yet evidence of a workflow capacity gain.

Ready-manifest grouping (`2f992d3d`), clean 64-store/two-worker single cycle:
**13,546.96 recipients/sec**, 74.06 seconds, CPU 0.96188 ms/recipient, peak
RSS 13.79 GiB, reporting p95 0.565 seconds. Compared with the prior direct-join
observation (13,339.15/sec, 0.97226 ms), this is a modest serial improvement,
not replicated qualification. There were **5,498 data objects and 5,219
manifests**: 5.07% fewer manifests than one per object, or an estimated 2.54%
fewer data/manifest sync barriers (21,434 versus 21,992), excluding other
metadata publications. Host writes were 2.76691 GiB at 39.31 MiB/sec.

The small grouping opportunity has a concrete scheduling explanation to test:
`finish_flush_work` performs the durable manifest commit synchronously on the
same loop that dispatches uploads. Producers queued during a slow commit
cannot start their PUTs until it returns. Next is a gated regression proving
that stall, followed by one ordered commit worker while the upload dispatcher
continues. Ordering, memory bounds, failure handling and drain must remain
covered. No full qualification is claimed for the grouping-only candidate.
Evidence uses `fireweed-campaign-ready-manifests-s64-w2-one*`; its owned root
was removed after property capture.

The ready-manifest grouping candidate passed **309 Fireweed release checks**
and **39 focused object-log checks**. Only built-in sequencers that explicitly
opt in can receive several already-durable objects in one atomic commit.
Custom sequencers retain one-object calls. The configured in-flight upload
bound caps each group, and an unfinished or failed PUT stops grouping.
No linger or storage-format change is introduced. Producer durability levels,
per-partition offsets, object byte ranges and aggregate byte accounting remain
covered by tests. Failed-prefix flush barriers were fixed in the preceding
commit rather than allowing failed grouped work to appear durable.

The new tests verify exact grouping bounds, failed/unfinished boundaries,
multiple partitions, gated manifest acknowledgement timing, whole-group
manifest failure, exact replayed locations, concurrent public produces and
reopen, and resuming the data-object counter when it differs from the manifest
count. The runner now counts immutable data objects and manifests after the
timed process, allowing clean runs to demonstrate publication reduction.
Next is the same 64-store/two-worker million-row single-cycle comparison;
capacity benefit is not assumed from the protocol-level reduction.

Object-log v0.3.1 is now vendored from pinned commit `dcd37c0e…` for a
reviewable publication-path change; upstream licenses and provenance are
retained, and neither the Cargo cache nor sibling checkout was modified.
Its unchanged focused baseline passed 29 tests. Before grouping commits, a
new regression reproduced a false-success `flush()` after manifest failure.
The fix retains the earliest failed enqueue position: covering barriers fail
even after later successful appends, while earlier barriers can still succeed.
Producer acknowledgements, offset assignment and storage format are unchanged.
All **31 focused tests passed**, including Buffered/Durable/Sequenced failure
cases, a settled PUT failure and the earlier/later barrier boundary. Full
Fireweed integration validation will accompany the ready-group candidate;
these focused tests are not a capacity measurement.

The retained-code six-cycle I/O diagnostic (source `3cdfb41a`, binary
`338b79da…`, 48 stores/two workers) completed at **10,584.67 recipients/sec**
in 567.21 seconds. It is not qualification: tracing was enabled, throughput
failed, and cycles 3/5 fell below 10k. All other workload/resource gates passed.
No device settings or checkpoint thresholds changed. The standalone release
build command and binary hash are recorded separately from prior test builds.

The process-local trace recorded **43,713 log sync calls >=100 ms**, with
15,209 summed overlapping seconds and a 5.605-second maximum. By comparison,
400 long main-file writes summed to 232.01 seconds and 1,083 long WAL writes
to 825.70 seconds. Existing VFS counters independently found 400/1,084 such
main/WAL calls; total VFS write time was 268.30/919.33 seconds respectively.
Temporary-file writes added 23.91 seconds, none >=100 ms. These are caller
wall times including scheduling, not device service times; parallel durations
must not be added to process elapsed time. A store with one slow log sync can
still have other uploads in flight. No sync/write errors were observed.

Source inspection explains four durability barriers per sealed object:
LocalBlobStore writes a temp file, fdatasyncs, renames, and fsyncs its directory;
ManifestSequencer repeats that publication for the commit manifest. Concurrent
object uploads are available, but the flush loop sequences each completed
object separately, even if multiple ordered uploads are already ready. The
next candidate combines only that ready contiguous success prefix into one
manifest, preserving existing ordering, format and durable acknowledgement
rules. It must test unfinished/failed puts, manifest failures and recovery.
Evidence uses `fireweed-campaign-direct-join-io-s48-w2-six*`; the interposer
source, smoke tests and parser are also archived. The owned projection root
was removed only after file-property capture.

The 224-row replacement candidate (`86ddc211`) is **not retained**. Its clean
64-store/two-worker million-row cycle reached 13,356.81 recipients/sec and
0.97154 CPU-ms/recipient, effectively unchanged from the 56-row direct-join
result (13,339.15 and 0.97226). Peak RSS was 15.33 versus 14.00 GiB.
The native SQL timing gain did not carry through to the production retained-VM
execution path. This serial comparison does not establish a small causal
effect; it supplies no compelling workflow gain for the larger adapter bound.
All workload assertions completed and reporting p95 was 0.517 seconds, but
a single cycle does not qualify. The four code files are restored to their
previous validated state. Evidence uses `fireweed-campaign-replacement-batch224-s64-w2-one*`.
Next is a six-cycle I/O attribution run on the retained direct-join code, using
existing VFS aggregate tracing plus a temporary process-local slow-write
interposer. No device settings or checkpoint thresholds change.

The current candidate uses **224-row native Turso replacement statements**
(3,588 binds) through an adapter-specific `RelTx` bound. The portable default
remains 56 rows/900 binds; unrelated operations keep their existing budgets.
A 1,000-row replacement now needs five replacement statements instead of 18.
The measured complete apply transaction in the native regression uses 28
statements (five reads, 23 writes including auxiliary work), no broad current-row
read, and an observed maximum of 3,588 binds. Full tenant/queue/item and rowid
seeks remain. Complete row/payload/gate images match sequential lowering;
late version, missing-row and receipt conflicts roll back earlier chunks.
All **309 release checks passed**, with two ignored checks/diagnostics and
two unconfigured live-S3 exclusions. Evidence uses `fireweed-replacement-batch224-*`.
The same 64-store/two-worker million-row single-cycle comparison is next;
no workflow performance gain is claimed from the SQL diagnostic alone.

The priority-exclusion candidate (`bf539d9e`) is **not retained**. Its clean
64-store/two-worker single cycle reached 13,103.57 recipients/sec at 1.00318
CPU-ms/recipient, versus 13,339.15 and 0.97226 for direct joined replacements.
This serial comparison does not prove regression, but it does not demonstrate
a benefit from the additional JSON exclusion set either. Reporting p95 was
0.600 seconds and all workload assertions completed; a single cycle remains
unqualified. Evidence uses `fireweed-campaign-priority-exclusions-s64-w2-one*`.
The production query and its added regression are restored to the preceding
validated source; the candidate commit and test/measurement evidence remain
in history. Next is an adapter-specific larger replacement batch, preserving
the portable 900-bind policy for other operations and adapters.

The next runtime candidate filters pending authoritative claim exclusions in
the priority candidate subquery, before loading payloads, fields and metadata.
A non-correlated JSON list supplies one bound exclusion set; the no-exclusion
and FIFO paths keep their existing queries. Priority scans request only the
remaining selected count instead of overfetching full excluded bodies.
The regression covers 2,400 rows, priority ties, future not-before values,
1,700-prefix and 1,200-interleaved exclusions, empty/all-excluded lists, exact
returned body/metadata/identity/lease values, and native query bytecode.
The candidate coroutine reads its item columns from the eligibility index;
its exclusion list is non-correlated. All **310 release checks passed**,
with two ignored diagnostics/checks and two unconfigured live-S3 exclusions.
An initial assertion incorrectly expected the EQP label “COVERING INDEX”;
Turso reports “USING INDEX” even for covered reads. The corrected check
verifies actual bytecode rather than weakening the coverage requirement.
The failed initial run and corrected results are archived. Next is the
same 64-store/two-worker million-row single-cycle comparison; no throughput
benefit is claimed yet.

The completed six-cycle direct-join run (runtime `3cdfb41a`, 48 stores, two
workers/campaign) reached **9,845.15 recipients/sec** over 609.89 seconds.
All non-throughput gates passed, including reporting p95 (worst 0.521 seconds),
physical projection-size stability, correctness checks exercised by
the workload, retention and sampled WAL bounds. Overall and five cycle rate
gates failed at 12.5k; three cycles also fell below 10k. This is not qualification.
CPU cost was 1.03450 ms/recipient and peak RSS 13.40 GiB. Cycle 2 purge took
24.59 seconds and cycle 5 delivery 72.31 seconds, versus roughly 6–8 and 28–33
seconds otherwise. The forced TRUNCATE workaround is bypassed for this
NORMAL-accounting configuration. Native auto-checkpoint executes inside commit;
its timing is a hypothesis for these stalls, not an established cause.
End-of-cycle file observations strengthen that timing hypothesis: all 48 main
files are 4 KiB through cycle 1 and approximately 39.3 MiB after cycle 2.
Median WAL length rises from 152.6 to 310.7 MiB, drops to 16.4 MiB in cycle 2,
then grows to 331.6 MiB before dropping to 54.4 MiB in cycle 5. These are
one observation per physical store (campaign 0), not timestamped checkpoint
durations. The two slow cycles coincide with checkpoint/restart activity;
causal duration attribution still needs traces. The derived evidence is
`fireweed-joined-replacements-checkpoint-cycle-observation.json`.
No device ceiling or hardware replacement conclusion follows from this run.
Evidence uses `fireweed-campaign-joined-replacements-s48-w2-six*`; properties
were captured before removing its private projection root. The next code
candidate filters authoritative claim exclusions before body materialization.

Direct joined replacements (`3cdfb41a`), clean 64-store/two-worker single cycle:
**13,339.15 recipients/sec**, 75.19 seconds, worst reporting p95 0.553 seconds.
CPU cost was **0.97226 ms/recipient**, 9.7% below the preceding owned-decoder
candidate's 1.07716; preparation fell from 34.30 to 27.52 seconds. Overall rate
rose 6.9%. These are serial observations, not replicated causal estimates.
Process output was 9,342.32 bytes/recipient, peak RSS 14.00 GiB, and host writes
3.07121 GiB at 43.14 MiB/sec. The single cycle does not qualify the target.

Next is a clean six-cycle **48-store/two-worker** layout comparison using the
same binary: 96 campaigns and 192 campaign workers, still one million global
rows, the same payloads, handler/storage batches, global phase barriers and all
gates. It reduces the nominal aggregate 448 MiB/store WAL window from 28 to
21 GiB relative to 64 stores. The prior 64-store six-cycle run experienced
late reads and initial main-file checkpoint materialization in cycle 4; the
48-store result must establish its own throughput and stability rather than
assuming a benefit. This changes both store and total worker counts and is
not an isolated sharding-effect claim. Evidence for the completed single cycle
uses `fireweed-campaign-joined-replacements-s64-w2-one*`; its private root was
removed after property capture. No host settings changed.

The current native CPU profile led to a concrete bulk-update query change.
The previous `UPDATE ... FROM` built a rowid-list subquery and a temporary
index over incoming values. A direct `incoming CROSS JOIN target` makes one
full tenant/queue/item-key seek followed by the target rowid seek, without
those intermediate structures. All version, fused-claim and namespace guards
remain, as do the 56-row chunk and 900-bind ceiling.

An explicit native SQL diagnostic, excluded from workflow qualification,
measured 8,000 updates after warmup for each case. At 56 rows/statement the
old query measured 18,008 and 17,854 updates/sec in bracketing controls;
the scalar-subquery form reached 23,537 and the direct join 24,967. Larger
112/224-row variants also executed successfully but are **not adopted**.
This diagnostic uses an in-memory fixture and the public adapter's statement
execution path, not the production apply transaction's retained-VM cache or
the workload CLI's allocator. Its rates are screening evidence, not campaign
capacity predictions. The old query is frozen inside the diagnostic so later
production edits do not silently replace its control.

The production candidate adopts only the direct join. Its native regression
now includes an identical item ID in another queue and compares complete row,
payload and gate images with sequential lowering; late version/missing-row
conflicts must roll back all preceding chunks. Query-plan assertions reject
the former temporary incoming index and rowid-list subquery. All **309 release checks passed**; the pre-existing ignored check and explicit
SQL timing diagnostic are ignored in that suite, and two unconfigured live-S3
checks are excluded. The public workflow measurement is next. Evidence uses
`fireweed-replacement-shape-diagnostic.log` and `fireweed-joined-replacements-*`.

Owned claim decoding (`d024a298`), clean 64-store/two-worker single cycle:
**12,478.68 recipients/sec**, 80.41 seconds, reporting p95 0.327 seconds,
CPU 1.07716 ms/recipient, mean occupancy 13.40, peak RSS 14.59 GiB.
Process output was 9,300.37 bytes/recipient; host writes were 2.86237 GiB
at 37.48 MiB/sec. This does **not demonstrate a throughput or CPU improvement**
over the preceding lifecycle trace (12,812.28/sec, 1.06857 ms/recipient).
Tracing differs and neither comparison is replicated. The buffer ownership
property is verified by tests; a workflow performance benefit is not established.
The next diagnostic profiles current user CPU on the same million-row layout.
Evidence uses `fireweed-campaign-owned-claim-s64-w2-one*`; the owned root was
removed after property capture. This is not a qualification pass.

The next candidate consumes owned claim-row buffers in the actual FIFO/priority
claim path, rather than cloning payload/fields/metadata through `get_value`.
It checks exclusion immediately after decoding the ID; cursor and eligibility
handling stay before that check. Selected rows preserve versions, attempts,
defaults, payloads and all metadata. Tests assert original buffer addresses
survive decoding and an excluded row does not consume its body iterator.
All **309 release checks passed**, with one existing ignored test and the same
two unconfigured live-S3 exclusions. A same-layout, clean 64-store/two-worker
single-cycle comparison follows; throughput benefit is not yet established.
Evidence uses `fireweed-owned-claim-*`.

Clean six-cycle lifecycle run (`ea40bd7c`, runtime `264d9a3c`, 64 stores/two
workers): **10,067.49 recipients/sec**, 596.52 seconds, **not qualified**.
Every reporting check passed, worst p95 0.253 seconds. Five cycles missed
12.5k; cycles 3/4 also missed 10k (9,897.10 and 7,954.74). Slowest cycle times
were 78.99/91.66/96.05/101.03/125.70/96.45 seconds. All correctness, fairness,
due-time, retention, WAL and RSS gates passed; 62 physical projection-size
stability gates failed. CPU cost was 1.16622 ms/recipient, peak RSS 15.41 GiB,
process output 10,087.07 bytes/recipient, and host writes 19.96585 GiB.

Read-only post-run SQLite inspection explains the physical-size failures:
representative shard 0's main file remained 4,096 bytes through cycle 3 and
became 31,039,488 bytes in cycle 4 when WAL contents were checkpointed. Its
final 7,578 pages include 6,198 free pages, zero items/payload rows and 62
retained idempotency receipts occupying about 5.4 MB. This is evidence of
late main-file materialization, not evidence of unbounded live-row growth.
The existing physical-size gate remains failed and unchanged; this diagnosis
does not turn the run into a pass. Selected shards 0/33/37/63 are captured
in `fireweed-campaign-lifecycle-s64-w2-six-file-diagnosis.json`.

The run also read 4.984 GiB from the device, versus zero in the first-cycle
trace, and averaged 11.73 logical CPUs. Additional sharding has not produced
a stable throughput improvement. The next code candidate removes avoidable
claim-row buffer copies and avoids decoding excluded claimed rows. No SSD
settings or qualification gates change. Raw evidence uses
`fireweed-campaign-lifecycle-s64-w2-six*`; the owned root is removed after
property capture and the read-only file diagnosis.

Lifecycle-tail metrics (`264d9a3c`), 64 stores/two workers, one traced million-row
cycle: **12,812.28 recipients/sec**, 78.39 seconds; worst campaign reporting
p95 **0.338 seconds** versus 2.867 seconds in the preceding same-layout run.
The new path served 3,145 reads, p95 147 ms and maximum 323 ms. Of 8,995 total
metrics reads, 39 fell back to coverage; 34 of these exceeded one second.
All 128 campaign p95 values met the one-second gate. This remains a single
traced diagnostic, not stable qualification or an isolated throughput gain.

CPU cost was 1.06857 ms/recipient, mean occupancy 13.63 logical CPUs, peak RSS
14.31 GiB, process output 9,371.31 bytes/recipient, host writes 2.88332 GiB at
38.93 MiB/sec. More progress reads now complete during active processing, and
tracing adds work; the higher CPU cost needs a clean comparison. The next run
uses the exact validated binary, six cycles, no tracing and the 12.5k target.
All workload and correctness gates remain unchanged. Evidence uses
`fireweed-campaign-lifecycle-s64-w2-trace-one*`; the private root was removed
after property capture.

The same-binary 64-store/two-worker diagnostic (`0be1e74f`) reached
**13,100.43 recipients/sec** for one million recipients in 76.61 seconds,
but **all 128 campaign reporting latency gates failed**, worst p95 2.867 seconds.
Enrichment reporting was the main slow phase: one campaign's prepare reads
had p95 3.325 seconds versus 0.053 seconds during load. One cycle is not
qualification, and this result does not demonstrate stable 12.5k capacity.
CPU cost was 0.97452 ms/recipient, mean occupancy 12.72 logical CPUs, peak
RSS 14.04 GiB, process output 9,292.64 bytes/recipient, and sampled host writes
2.83965 GiB at 38.83 MiB/sec. Evidence uses
`fireweed-campaign-fixed-target-s64-w2-one*`; its private root was removed
after property capture. No host settings changed.

The next code candidate extends exact progress reporting to a bounded,
contiguous tail of authoritative claims and resolved lease-clearing row
replacements. It checks actual row state, version and supersession against
one SQL counter/cursor/row snapshot and rebases if application advanced.
Missing rows, version conflicts, gaps, unsupported commands and bounds retain
the normal coverage wait. Mutation acknowledgments and physical row reads
still wait for projection coverage. The target is reporting latency during
both enrichment and delivery, without changing the workload or its gates.
All **307 release checks passed** after the test correction described below,
with one existing ignored test and two unconfigured live-S3 exclusions.
The next run traces reporting on the unchanged 64-store/two-worker million-row
workload; measurements are pending. The broad all-target check finds
legacy integration targets still referencing retired SQLite constructors;
these are not current-backend failures. The first release test run failed the
new second-mutation test because it reused an idempotency request ID with a
different body, then waited for an append that could not occur. The test now
uses a distinct request ID and detects premature completion; the failed log
is retained. No timeout or product guarantee was relaxed. The optional Turso-only feature
check also exposes an existing constructor/module-gating mismatch in
`fireweed/src/lib.rs`: its constructor references `blocking_backend`, whose
module requires objectlog/postgres/test. This candidate does not change that
file; the default filesystem-log/Turso configuration passes the release suite.
The failed feature check is archived, not counted as a pass.

Same-binary 64-store/one-worker diagnostic (`f174475d`): **11,509.56/sec**
for one million recipients in 87.17 seconds, worst reporting p95 1.121 seconds.
This does not qualify and does not improve the 32-store first-cycle result.
CPU cost was 1.02504 ms/recipient, mean 11.76 logical CPUs, peak RSS 12.37 GiB;
process writes were 10,593.83 bytes/recipient and host writes 3.21092 GiB at
38.27MiB/sec. The next single-cycle control keeps 64 stores and raises workers
per campaign to 2 (256 total). It tests additional concurrency explicitly; it
cannot be interpreted as an isolated sharding gain. Same binary and all rows,
payloads, handler limits, global barriers and oracles are retained.
Evidence uses `fireweed-campaign-fixed-target-s64-w1-one*`; the private root was
removed after property capture. No runtime code or host settings changed.

2026-09-14 fixed-frontier full run (`d124c24b`, 32 stores, two workers per
campaign): **10,167.33 recipients/sec**, 590.48 seconds. **All reporting and
all other non-rate gates passed**, with worst p95 0.982 seconds. This is still
**not qualified**: cycles 2/4/5 reached 9,913.32 / 9,298.54 / 8,970.78 equivalent
recipients/sec. Slowest cycle times were 77.17/91.18/100.87/96.29/107.54/111.47
seconds. CPU cost was 1.01615 ms/recipient, peak RSS 10.87 GiB, process output
11,449.50 bytes/recipient, and sampled host writes 20.1801 GiB at 35.13 MiB/sec.
The serial comparison with `cd5db494` observed CPU cost down 2.1%, host writes
down 4.4%, and reporting failures down from 12 to zero; it is not a replicated
isolated causal rate claim. Raw evidence uses `fireweed-campaign-fixed-target-w2-six*`.
The owned projection root was removed after capturing properties.

Next, compare the exact same binary with 64 physical stores and one worker per
campaign: 128 campaign workers in both layouts, still one million recipients,
two campaigns per store, the same handler/storage limits and every oracle.
The strided ID distribution preserves all recipients with odd store populations.
This starts as a one-cycle diagnostic, not qualification. Smaller store indexes
may reduce dirty pages per transaction, but extra stores also add memory,
coordination and tail risk; scaling is not assumed.

A source-review correction: the campaign phase barrier is global
(`Barrier(shards * CAMPAIGNS)`), enforcing full-population residence and scheduling
before proceeding. It is not merely per-store. The shard comparison preserves
that barrier. Independently overlapping campaigns would be an additional workload,
not a replacement for the current bulk-residence qualification.

Current candidate: metrics fallback waits for the log frontier captured at
read entry, instead of capturing a newer frontier after the snapshot/fast-path
attempts. A deterministic race test advances only the captured prefix while a
later durable write remains paused: metrics completes, but a new physical read
still waits for the later write. The 448 MiB WAL window and original JSON-pair
membership query are restored. All **305 release checks passed**, with the same
one ignored test and two unconfigured live-S3 exclusions. The exact workload
CLI from release validation is used for the next six-cycle, two-worker run;
only Rust formatting followed validation. Build/test provenance is archived as
`fireweed-fixed-target-*`.

The experimental JSON-object membership rewrite is not retained. Its first
native run caught loss of full-key index seeks and incorrect escaped-key
matching in the native scalar JSON path. A cast plus array-wrapped client keys
restored correctness and full-key plans, but its single 8192-identity diagnostic
was 40.120 ms versus the earlier 35.626 ms reference, demonstrating no benefit.
The stronger escaped-client-key and maximum-u64-ID checks remain and pass with
the original query. Failed compile/test logs and the corrected native diagnostic
are retained as `fireweed-membership-object-*` and
`fireweed-fixed-target-hook-initial-build.log`; none are qualifying measurements.

2026-09-14 short-checkpoint trial (`0b85c778`) **rejected**. Its single
million-recipient cycle completed at only **5,847.83/sec** (171.28 seconds),
with reporting p95 6.573 seconds. Process writes were 25,257.13 bytes/recipient;
sampled host writes were 7.614 GiB at 46.07 MiB/sec. CPU cost was 1.03131
ms/recipient, but average CPU occupancy fell to 6.02. Peak RSS fell to 7.78 GiB,
which does not compensate for the throughput and latency regression.

For context, the earlier two-worker one-cycle trace wrote 9,906.91 process
bytes/recipient and 2.868 GiB of host data, completing at 12,928.60/sec. It had
tracing enabled and preceded the genesis fix, so this is not a fully isolated
comparison. Nevertheless the short-window candidate clearly fails the target
and its write-volume reduction hypothesis. Restore 448 MiB; do not promote the
shorter window or infer that 40.58 MiB/sec calibration is a hard device ceiling.
The measured trial itself sustained 46.07 MiB/sec of host writes.

The subsequent JSON-object query experiment was also rejected, as recorded above.
Short-window evidence uses `fireweed-campaign-checkpoint4m-w2-one*`; the owned
projection root was removed after capturing its properties.

2026-09-14 genesis reporting fix, three-worker comparison (`dec6e387`):
**8,637.08 recipients/sec**, 695.11 seconds, **not qualified**. Five cycle-rate
checks and 54 reporting checks failed; all other gates passed. Slowest cycle
times were 86.51/115.74/107.83/132.32/111.09/135.23 seconds. CPU cost rose to
1.20484 ms/recipient, peak RSS was 11.31 GiB, and host writes were 23.39 GiB at
34.56 MiB/sec. This combines the genesis fix and a worker-count change, so it
is not an isolated measurement of either. It provides no evidence for retaining
three workers; the next trial returns to two.

The next isolated code experiment reduces the log-backed projection's automatic
checkpoint window from 448 MiB to 4 MiB, preserving its byte size across database
page sizes. At 32 stores this reduces the nominal aggregate transient WAL window
from 14 GiB to 128 MiB. It may reduce old WAL versions written before file reuse,
but may increase main-file writes and checkpoint CPU. Only measurements decide.
The log's durable append and sync path, rebuildable I/O adapter, read barriers,
row workload, and qualification gates remain unchanged. Readers use OFF; writers
use NORMAL for correct checkpoint accounting while the VFS omits physical sync.
This is a disk-backed database policy experiment, not RAM-WAL or host tuning.

The full three-worker evidence uses `fireweed-campaign-dec6e387-genesis-w3-six*`
and `fireweed-genesis-release-build.log`. The owned projection root was removed
after capturing compression properties. For subsequent builds/tests,
`SOURCE_DATE_EPOCH=1789179522` is fixed to the last vendored-core source commit.
The existing Turso build script supports this reproducibility seed; it prevents
unrelated documentation commits from invalidating the entire core build and
changes source-id metadata. This value must stay consistent and be recorded;
it does not change the SQL or I/O policy.

2026-09-14 membership candidate with explicit projection barriers (`cd5db494`):
**10,097.35 recipients/sec overall**, 594.45 seconds. This is **not qualification**:
cycles 2, 4 and 5 reached only 9,220.75 / 9,050.08 / 8,681.55 equivalent
recipients/sec, and 12 reporting checks failed (worst p95 1.411 s). All other
gates passed. Reporting failures by cycle were 8/1/0/2/0/1, versus 27/13/2/9/10/6
in the earlier two-worker control. CPU cost was 1.03782 ms/recipient (+2.1%),
peak RSS 10.90 GiB, process output 11,391.17 bytes/recipient and logical log
output 1,834.32 bytes/recipient. Host writes were 21.11 GiB at 36.52 MiB/sec.
Observed rate rose 8.7% in this serial comparison, while measured device bandwidth
also rose; this is not a replicated causal throughput claim.

Slowest cycle times were 73.75/84.39/108.45/96.27/110.50/115.19 seconds. Late
loads reached 39.26 seconds and the final purge reached 29.82 seconds, which is
included by the explicit retained-row completion barrier. Raw, summary, device,
provenance and compression properties use `fireweed-campaign-cd5db494-membership-w2-six*`.
The private projection root was removed after capture. The next short same-binary
trace distinguishes remaining initial-cursor/unsupported-tail fallbacks from
the new membership query cost. No SSD/host settings changed.

2026-09-14 reporting candidate (on `e27e7442`): public metrics can fold a
complete retained tail of either Push or PurgeItems commands over a fresh SQL
snapshot. The coordinator copies only identities, caps the tail at 16 commands
and 8,192 identities, and rejects gaps, duplicate positions/IDs/keys, mixed
command families, foreign epochs and unsupported commands. No authoritative
counter cache, side records, storage settings or workload changes are introduced.

The second SQL statement reads counters, their cursor, addressed row states and
proposed active-key presence together. If apply advanced since the first read,
commands already represented by the new cursor are excluded before folding.
Existing push IDs or active keys, missing snapshot rows, cursor regression and
count overflow/underflow fall back to normal projection coverage. Purge subtracts
only present, unsuperseded rows. Both the initial and folded reads check poison.
An initial queue without an applied cursor retains the coverage fallback.

Focused release validation passed: bounded tail selection, cursor rebase/conflict
checks, native snapshot consistency, and actual Turso query-plan/identity-bound
checks. The 8,192-identity lookup took 36.09 ms on an empty native projection;
this is a query diagnostic, not a loaded-workflow latency claim. Its plan uses
full `(tenant_id, queue_id, item_id)` and `(tenant_id, queue_id, client_item_key)`
index seeks. All 11 activation tests passed, including paused-projection push
and purge reads that return exact durable counts without moving SQL counts or
cursor, followed by recovery/reopen verification. The first complete object-log
unit invocation had 77 passes and two failures due to absent
`FIREWEED_S3_TEST_ENDPOINT`; the live S3 tests are explicitly excluded from the
subsequent local-disk suite, not counted as passes. Full logs are retained.

The expanded Fireweed library suite initially passed 149 tests, ignored one and
failed the legacy outbox fixture. That fixture directly changed a modern row
from Pending to Leased while leaving `resident_counts_version=1` and its counters
unchanged. It now marks counters uninitialized to represent the pre-counter
schema it claims to model, then asserts reopen's existing migration backfills
exactly one lease. That stronger assertion then exposed a real startup defect:
legacy outbox drain appended a Claim but never applied it, leaving recovery's
coordinator frontier unseeded and subsequent public metrics waiting for missing
coverage. Drain now uses the existing packed-apply publisher before deleting the
outbox entry, so startup can seed a frontier covering the newly logged claim.
Both failure logs are retained. Counter backfill and nonnegative-counter checks
remain unchanged; the test requires public metrics to work on the first reopen.

The final expanded local release suite passes **304 tests**: 150 Fireweed library,
77 object-log, 52 native Turso, seven adapter/history, one WAL/free-page, three
native recovery, two workload unit, six public campaign, two primitive CLI and
four workload recovery. One test remains ignored and the two unavailable live-S3
tests are explicitly filtered. Logs use `fireweed-membership-*` and include both
recovery failures and their final passing run. The query timing summarizer v2
supports the appended membership-tail and membership-SQL timing phases while
preserving historical seven-phase totals.

Before measuring the membership candidate, the workload's implicit metrics
barriers were made explicit: public retained-row reads now settle projection
work after load and purge (also primitive phase timing and other recycle
profiles). Metrics can legitimately report exact durable counts before SQL
applies, so a zero counter alone would otherwise let the last purge escape the
timed workload. The first workload rerun passed all six disk campaign tests but
failed the memory primitive path because that synchronous backend has no
retained-row API. The new settle reads are required only on the asynchronous
disk backend; the memory backend's existing metrics follows synchronous apply.
Both runs are retained. The corrected workload passes all **14 release checks**
(two workload unit, six campaign, two primitive CLI covering disk and memory,
and four recovery), following the 304-check engine/native suite. The retained
read must see an empty queue after purge and runs
inside the measured cycle/primitive phase. No full performance run of the new
membership path was taken without this fix. This preserves the original
projection-completion requirement; it does not count queued work as completed.

The next measurement is the same six-cycle disk campaign with two workers,
all correctness/reporting/storage gates intact and diagnostic tracing disabled.
The performance goal remains unmet until repeated full qualification passes.

2026-09-14 metrics-phase diagnostic (`47875b5f`, two workers, two cycles):
9,693 successful metrics calls, including 511 over one second. **Every slow
call was dominated by projection coverage**: 1,047.49 aggregate seconds in that
wait versus 0.410 seconds in both SQL reads combined. Across all calls, coverage
was 1,990.59 seconds, snapshot SQL 1.822 seconds, final SQL 0.802 seconds and
high-water lookup 5.550 seconds. Call times overlap across queues. Covered reads
had p95 0.215 ms and claim-tail reads 0.809 ms; fallback reads had p95 2,642.24 ms.
The two-cycle rate was 11,688.25/sec, but this traced short run is **not qualification**.
Complete evidence and summarizer use `fireweed-campaign-47875b5f-metrics-w2-two*`
and `fireweed-metrics-trace-summary.py`; owned projection files were removed.

The next write-path changes build general INSERT parameters directly per SQL
chunk (preserving the global FIFO offset) and omit PurgeItems metadata prefetch
only when retained keys and grouped rows cannot use it. New native tests cover
chunk boundaries with nonzero FIFO base, group discovery after reopen, same-apply
group creation, retained-key behavior and exact SQL read savings. These do not
change log durability or add workflow records. A bounded exact push/purge-tail
metrics read is under review; it is not yet implemented.

All **75 release checks pass** for the insert/purge changes (50 native plus
25 adapter, WAL, recovery, workload and CLI checks). The first run had 49 native
passes and one new-test assertion failure: decimal ItemId input `00000` is
canonically stored as `0`. Correcting that expected representation made the
full suite pass; both failed and successful logs are retained.

2026-09-14 same-binary two-worker control (`1e15109d`): **9,287.72 recipients/sec**
over 646.48 seconds, versus 7,651.62/sec with one worker. CPU cost was
1.01649 ms/recipient, peak RSS 11.25 GiB, process output 11,542.90 bytes/recipient,
and logical log output 1,834.32 bytes/recipient. All correctness, due-time, retention,
WAL and stability checks passed, but 67 campaign/cycle reporting checks failed
(worst p95 1.7205 s). Overall and cycles 2–5 throughput failed. This is not a
qualified result. All raw, device, summary, provenance and compression-property
artifacts use `fireweed-campaign-1e15109d-known-after-w2-six*`; the owned directory
was removed after capture. Host writes were 21.24 GiB at 33.77 MiB/sec.

The next diagnostic adds `FIREWEED_METRICS_TRACE`: exclusive microseconds for
high-water lookup, first admission, snapshot SQL, retained claim-tail lookup,
fallback coverage, second admission, and final SQL, in that order. It preserves
read semantics and admission order; traced runs are diagnostic, not qualification.
This will distinguish waiting for projection coverage from the SQL query itself
before expanding reporting logic. No host/storage settings are changed.

2026-09-14 guarded counter-read candidate: clean `70e64556` completed six disk
cycles at **7,651.62 recipients/sec**, 784.43 seconds. All non-throughput gates
passed; worst reporting p95 was 0.6014 s. Overall and cycles 1–5 throughput
failed. CPU cost was **1.03330 ms/recipient**, 1.5% below the preceding owned-row
run, but measured throughput fell 7.0%. Peak RSS was 10.93 GiB, process output
12,686.02 bytes/recipient, and logical log output 1,834.83 bytes/recipient.
Host writes were 25.01 GiB at 32.75 MiB/sec. This serial comparison does not
establish a causal speedup; fewer read queries have not met the workflow target.
Raw/summary/device/provenance artifacts use
`fireweed-campaign-70e64556-known-after-w1-six*`; compression properties were
captured before removing the owned projection directory.

Read-only thread-state observations during this run found filesystem waits.
The archived snapshot contains five threads in `wait_log_commit`, 26 in
`folio_wait_bit_common`, and one in `btrfs_btree_wait_writeback_range`; an earlier
interactive snapshot saw 31 log-commit waits and one `write_all_supers` wait.
Kernel stacks were inaccessible, so these names do not establish exact call
stacks or a single bottleneck. Host RAM is about 64 GiB; existing dirty-page
limits were 256 MiB foreground / 64 MiB background. No host settings changed.
These observations do not establish an intrinsic SSD ceiling or TRIM cause.

The next controlled run uses two workers per campaign on the same code and
binary, retaining all workload operations and qualification gates. Earlier
concurrency runs predated the exact claim-tail reporting and counter-read fixes;
their reporting failures do not establish the current concurrency tradeoff.
The guarded purge-metadata skip and its tests remain prepared outside the
checkout for subsequent evaluation.

2026-09-14 owned-row candidate: clean `f9598882` completed the canonical six-cycle
disk workload at **8,230.52 recipients/sec**, 729.58 seconds. All non-throughput
gates passed, including all 384 reporting checks (worst p95 0.9427 s), stronger
concurrent count assertions, final disposition checks,
retention, due latency, WAL bounds and memory/storage stability. Overall and
cycles 1–5 throughput failed. CPU cost was **1.04907 ms/recipient**, peak RSS
11.59 GiB, process output 12,426.76 bytes/recipient, logical log output
1,834.83 bytes/recipient. Host writes were 24.91 GiB at 35.09 MiB/sec.

Compared with `4256e0c0` (8,355.53/sec, 1.04291 CPU-ms/recipient, two reporting
failures), this does not demonstrate a throughput or CPU improvement. These
serial observations are not replicated causal estimates. The ownership change
removes a proven redundant buffer copy but remains unqualified for performance.
Raw/summary/device/provenance artifacts use
`fireweed-campaign-f9598882-owned-rows-w1-six*`; compression properties were
captured before removing the owned projection directory.

The next candidate avoids the post-mutation lifecycle aggregate only when fresh,
contiguous writer cursor positions and explicit final operations prove the result.
Replacements additionally require the pre-mutation aggregate to prove every
addressed row is present and unsuperseded. Unresolved claims, replay/covered
prefixes, duplicate positions, missing/superseded replacement targets, mixed
push/mutation batches and unsupported command families retain measured counts.
Fresh pure inserts use their successful ordinary INSERT guarantee; pure purge
knows its post-state is absent. Any SQL/version/receipt failure still rolls back
before counter application. The optimization runs inside the projection
transaction, not in public log-tail reporting.

A measurement correction accompanies this: the old statement-shape observer
classified every non-SELECT prefix as a write, including `WITH ... SELECT` reads.
Historical 1,022/40/85 “write statement” numbers therefore include CTE reads;
the recorded total statement counts remain valid. Explicit execution/query
classification now records the CTE aggregate reads correctly. Do not reinterpret
those historical labels as exact write counts. A new native comparison uses
live apply versus the unchanged measured recovery path and an independent
resident-row aggregate, asserting actual read reductions and replay/supersession
fallbacks. All **72 release checks pass**: 47 native, seven adapter/history,
one WAL/free-page, three native recovery, two workload unit, six campaign,
two primitive CLI and four workload recovery tests. A focused native rerun
records **41 total statements, five reads and 36 writes** for 1,000 guarded
replacements, with a maximum of 900 binds and no broad current-row scan.
Validation logs are `fireweed-known-after-release-validation.log` and
`fireweed-known-after-statement-shape.log`. Sustained performance remains to
be measured; query-count savings alone are not qualification.

2026-09-14 current CPU attribution: clean `391829ee`, with normal release binary
and explicit Btrfs log/projection roots, completed three million-recipient cycles
under a 199 Hz user-IP sampler. It collected **540,506 samples with zero lost**.
Largest symbols included allocation (6.91%), SQL column decoding (5.90%), memcmp
(4.95%), VM stepping (4.70%), two memcpy routines (4.53% and 1.70%), and B-tree
traversal. This identifies broad CPU costs, not call stacks attributing all
allocation/copy cost to a specific adapter. Instrumented rate was 8,120.34/sec;
this three-cycle diagnostic is not qualification or a code-speedup comparison.
Artifacts use `fireweed-current-cpu-391829ee*`; sampler/driver sources and normal
release build log are included. Projection compression properties were captured
before removing the owned data directory.

The next candidate consumes owned Turso row values in the relational and
public-read collection helpers. Previously the SDK materialized owned values,
then `Row::get_value` cloned each text/blob again before the caller discarded the
row. `Row::into_values` transfers those buffers through the existing value
conversion. Queries, result columns, errors, transaction boundaries, persisted
representation and qualification gates remain unchanged. A focused SDK test
asserts value types and original text/blob buffer addresses; a native regression
checks values survive statement rebinding and teardown. The focused SDK test
passes, and all **71 workspace release checks pass** (46 native, seven
adapter/history, one WAL/free-page, three native recovery, two workload unit,
six campaign, two primitive CLI and four workload recovery tests). Logs use
`fireweed-owned-row-sdk-*` and `fireweed-owned-rows-*`. Initial test invocations
hit vendored-workspace/lock restrictions; the standalone test passed after
resolving its dev dependencies. A subsequent workspace cache conflict was
resolved by clearing the affected Turso build artifacts. Both lockfiles remain
unchanged. The successful workspace run used `--locked`. Sustained performance
measurement is pending; no speedup is claimed. The reported libc copy/compare
symbol offsets were verified against current build ID
`503200d7fda94a5dc6058d7e0694e5d1dcb2e372`; that provenance is archived too.

2026-09-14 16-row replacement experiment rejected: clean `1af38acf` completed
all six canonical disk cycles at **7,428.07 recipients/sec** in 808.11 seconds,
compared with 8,355.53/sec for 56-row batches. CPU cost rose from 1.04291 to
1.13144 ms/recipient (+8.5%); throughput fell 11.1%. Peak RSS was 10.87 GiB.
Overall throughput, cycles 1–5 throughput, and six progress checks failed;
all other gates passed. The strengthened concurrent resident/acknowledged-terminal
count assertions passed throughout. These are serial observations, not a
replicated causal estimate, but provide no reason to retain the smaller cap.
The code restores 56-row chunks and keeps the stronger progress oracle. All
70 release checks pass after restoration: native SQL/rollback, adapter/history,
WAL/free-page, campaign, primitive CLI and recovery suites. The log is
`fireweed-restored-batches-release-validation.log`.

Process output was 12,588.58 bytes/recipient; logical log output remained
1,834.83 bytes/recipient. Host writes were 25.35 GiB, 32.23 MiB/sec, over the
sampled process window. Raw results, device samples, summaries, build log and
compression-property readbacks are archived under
`fireweed-campaign-1af38acf-batched16-w1-six*`; the owned projection root was
removed after property capture. Child execution succeeded; qualification failed.

A separate clean-HEAD storage-path diagnostic then wrote 8 GiB through 32
preallocated private files, using verified O_DIRECT and 1 MiB incompressible
writes, with final fdatasync included: **201.87 seconds, 40.58 MiB/sec**.
Preallocation (1.15 s) and first/last-block verification were outside the timer.
Process CPU was 1.90 CPU-seconds total. Timed-window device samples recorded
40.71 MiB/sec, 336.84 write IOPS at 123.77 KiB/request, and 0.24 host busy CPUs.
This reproduces the earlier slow storage path without Fireweed SQL or a
single-writer bottleneck. It does not prove the intrinsic SSD maximum or its
cause. The 32-stream count, call size and preallocation all differ from the
prior reference; this is not a single-variable causal comparison. No device,
mount, encryption or discard settings changed. New owned files used the same
file-local NOCOW attribute as the prior diagnostic and were removed afterward.
Scripts, full results and device accounting are archived under
`fireweed-parallel-headroom*` and `fireweed-parallel-write-headroom.py.txt`.
This is diagnostic evidence only and does not qualify the workflow or change
its fixed 10k/12.5k targets. Next work returns to the code's measured SQL and
write amplification costs; SSD maintenance remains unnecessary for continuing.

2026-09-14 bounded replacement batching: clean `4256e0c0` completed six disk
cycles at **8,355.53 recipients/sec** in 718.50 seconds. This is 5.8% above the
preceding claim-metrics run, but CPU cost increased 6.3% to 1.04291 ms/recipient
and peak RSS rose to 11.38 GiB. Two progress checks failed (shard 1/campaign 1/cycle
0: 1.1387 s; shard 12/campaign 1/cycle 4: 1.1310 s). Overall throughput and cycles
1–5 throughput failed; all other qualification checks passed. One serial pair
cannot establish a precise causal speedup. The candidate remains unqualified.

The slow progress samples occurred during load and purge; preparation and delivery
p95s in those two reports were below 0.21 s. Host writes were 25.09 GiB at
35.91 MiB/sec; process output was 12,269.61 bytes/recipient and logical log output
1,834.83 bytes/recipient. This does not demonstrate a drive ceiling. Raw/summary/
device/provenance files use `fireweed-campaign-4256e0c0-batched-w1-six*`; compression
properties were captured before removing the private projection directory.

The next candidate caps guarded replacement VALUES chunks at 16 rows instead of
using all 56 rows permitted by 900 binds. The purpose is to reduce generated-program
and temporary-index cost; client/storage batch and handler limits remain unchanged.
The actual native execution test and full-key/rowid plan assertions remain gates.
The 16-row candidate passes those checks at 85 writes per thousand replacements;
its 8.02-second debug regression time is essentially unchanged from the previous
8.16 seconds and is not evidence of a sustained speedup. All 70 release checks
pass with the stronger progress oracle: 45 native tests, seven adapter/history
checks, one WAL/free-page test, three native recovery tests, two workload unit
tests, six campaign tests (24.13 s), two primitive CLI tests and four workload
recovery tests (1.12 s). Results are archived in
`fireweed-batched-replacements-16-release-validation.log`. No campaign performance
benefit has been measured yet. The native result is `fireweed-batched-replacements-16.log`.

The concurrent progress oracle is strengthened independently: between completed
load and the start of purge, every read must count the full resident list and
include at least the terminal outcomes acknowledged before the read began.
Intervals crossing into purge retain the legitimate declining-count behavior.
The existing latency, frequency and final/window disposition gates remain intact.
Older hardware-note text that still described a live authentication prompt and
TRIM as a pending dependency is corrected; no maintenance was performed.

2026-09-14 exact claim-tail metrics candidate: clean `387f0c82` completed the
canonical six-cycle disk workload at **7,899.88 recipients/sec**, 760.03 seconds.
All non-throughput qualification gates passed, including all 384 campaign/cycle
progress checks; worst p95 was 0.7363 seconds. Overall throughput and every cycle's
slowest-campaign equivalent throughput failed. This is not a qualification pass.
The previous 4 KiB/one-worker disk run was 8,520.83/sec with three progress failures.
This single serial comparison shows a reporting improvement with lower measured
throughput, not an overall speedup or a precise causal regression estimate.

CPU cost was 0.98098 ms/recipient, mean occupancy 7.74 logical CPUs, peak RSS
10.45 GiB, process output 12,398.53 bytes/recipient, and authoritative log bytes
1,834.83/recipient. Host writes were 24.09 GiB at 32.58 MiB/sec; request latency
and busy percentage do not establish an intrinsic SSD ceiling. Raw/summary/device
artifacts use `fireweed-campaign-387f0c82-claim-metrics-w1-six*`. All projection file
compression properties were captured before removing the private run directory.

The unchanged 90 ms native reader deadline passed in the optimized release suite:
all 44 tests passed in 2.76 seconds. The prior debug failures remain preserved;
release validation does not erase them. Logs are `fireweed-claim-metrics-native-release.log`
and `fireweed-claim-metrics-release-build.log`.

The next code change targets a concrete unbatched operation: resolved `MutateItems`
currently runs the main guarded UPDATE once per recipient. Payload and gate work
is already batched. The older round-trip arithmetic test does not execute this
path and cannot prove bounded SQL execution. A new native 1,000-row test executes
the real adapter, limits observed statement counts, compares all main/payload/gate
columns with the existing sequential path, and checks rollback for a late version
conflict, missing row, and request-receipt conflict. The proposed fast path is
limited to distinct, lease-clearing replacements without grouping or typed indexes.
Other vectors preserve their ordered path. The new test failed on the original
code with 1,022 writes for 1,000 replacements, demonstrating the missing batching.

The first VALUES UPDATE draft used 40 writes and passed row/rollback checks, but
its real Turso plan searched only `(tenant_id,queue_id)` and then scanned incoming
rows. Debug test time was 25.87 s. An item-ID IN restriction still failed the
full-key plan assertion and was not retained. The corrected statement discovers
rowids with incoming-first, full-key indexed seeks, then updates by integer
primary key; Turso also indexes the incoming item IDs. It passes both plan
assertions, issues 40 writes (maximum 900 binds), preserves every persisted
main/payload/gate column versus the sequential lowering, and passes late missing
row, version conflict, receipt conflict and covered-replay checks. Debug test time
was 8.16 s. These times are diagnostic, not campaign capacity or a speedup over
the old code (whose test stopped at the statement-count failure). Logs are
`fireweed-batched-replacements-{before,after,plan,keyed,rowids}.log`.

The arithmetic-only helper test is renamed to describe its scope. Two legacy
test names claiming SQLite/Turso comparison are corrected: their fixtures both
instantiate Turso, so they establish repeatability, not independent-engine parity.
The new batching test deliberately forces the existing sequential lowering in its
reference vector with a lease-preserving sentinel. The black-box campaign oracle
and public API recovery tests remain the workflow correctness evidence. All 70
release checks pass: 45 native tests, seven adapter/history tests, one WAL/free-page
test, three native recovery tests, two workload unit tests, six campaign tests,
two primitive CLI tests and four workload recovery tests. The release reader
latency deadline remains unchanged. Full validation is archived in
`fireweed-batched-replacements-release-validation.log`. A normal production build
and the unchanged six-cycle disk measurement follow; no campaign speedup is yet
claimed for batching.

2026-09-14 one-second claim-join experiment rejected: clean `1879ecc6`
completed the canonical six-cycle disk run at **8,151.94 recipients/sec** in
736.54 seconds, versus 8,520.83/sec in the preceding uninstrumented 500 ms run.
It failed overall throughput, cycles 1–5 throughput, and eight campaign progress
checks (worst p95 1.409 s). Correctness, due-time, WAL, database and RSS gates
passed. CPU cost fell to 0.97611 ms/recipient, but wall time increased 4.55%.
One serial pair does not establish a precise regression magnitude; it provides
no reason to retain the longer delay. Restore 500 ms, keeping the regression for
follow-ups arriving just before the original deadline. All acceptance gates stay
unchanged.

Host writes were 25.09 GiB at 35.03 MiB/sec; these are host-wide measurements,
not a physical device ceiling. Raw, summary, device and provenance artifacts use
`fireweed-campaign-1879ecc6-join1s-w1-six*`. Projection file compression properties
were captured before removing the private run directory.

The next code candidate avoids forcing intermediate claim projection writes for
progress counts: one SQL statement reads exact lifecycle counters and their
applied cursor, then only a complete bounded tail of distinct authoritative
claims may adjust Pending/Leased counts. Counts come from the same SQL snapshot;
no coordinator watermark is substituted. Missing/pruned entries, gaps, epoch
changes, historical claims, repeated IDs, mixed mutations and arithmetic overflow
fall back to the coverage barrier. No extra workflow records or durability changes
are introduced. The paused-apply integration regression verifies exact claimed
counts without advancing SQL, and verifies that a mixed claim/completion tail
still waits. All nine activation tests, 76 local object-log tests, six campaign
tests (53.22 s), and four recovery tests (2.45 s) pass. Existing retained-tail
regressions cover gaps, missing entries, repeated IDs, historical authority,
epoch changes, bounds and poison. Native metrics tests also verify legacy missing
cursors and decode `(next_seq=8, epoch=2)` as applied position `(7,2)`.

The broader native debug suite passed 43/44 tests. Its existing live-writer
reader-pool latency test failed at 110.246 ms against 90 ms, then failed in
isolation at 114.544 ms. It does not call the new metrics method. These failures
are preserved in `fireweed-claim-metrics-native.log` and `-reader-rerun.log`;
no test deadline is changed and no clean native-suite pass is claimed. Release
validation of the same deadline is pending. The next six-cycle capacity run is
conditional on passing the release native suite, followed by a normal release
workload build. No performance gain is yet claimed.

2026-09-14 completed write-attribution run: clean `b9dd17ef` used the full
six-cycle disk workload with apply/log/VFS tracing enabled. It completed at
8,627.19 recipients/sec in 695.96 process seconds, costing 1.00353 CPU-ms per
recipient. This is instrumented evidence, not qualification or a speedup over
the uninstrumented run. All 32 WAL and 32 main-file handles emitted close totals;
no traced write errors occurred.

| VFS class | Write calls | Requested bytes | Aggregate call seconds | Longest call, seconds |
|---|---:|---:|---:|---:|
| WAL | 40,794 | 55,125,488,736 | 668.96 | 2.392 |
| Main/other | 6,849 | 5,909,356,544 | 93.04 | 1.767 |
| Temporary | 119,728 | 490,405,888 | 3.77 | 0.017 |

These overlapping VFS times do not measure physical-device service time.
Temporary-file creation/removal is not included. Apply time aggregated across
stores was 6,608.72 s: update SQL 4,129.20 s, read SQL 993.12 s, transformation
508.88 s, commit 924.28 s and writer wait 5.77 s. Writes can also occur during
SQL execution, so VFS totals must not be added to those phase totals.

The authoritative log produced 45,366 append observations. Produce latency
p50/p90/p95/p99 was 433/1,061/2,266/3,556 ms; 19,250 (42.4%) exceeded 500 ms.
Of 19,053 background claim-join windows, 5,207 expired after at least 500 ms
unchanged and without a coverage waiter. Another 7,646 unchanged windows ended
with a waiter. A growing generation is not itself proof of per-row fusion.

This evidence prioritizes avoiding unnecessary intermediate leased-state writes
over temporary-file write tuning. The next candidate increases only the bounded
background claim-follow-up window from 500 to 1,000 ms. It leaves immediate
strong-read bypass, original deadlines, ready-neighbor scheduling, log durability,
generation caps and every acceptance gate unchanged. The intended benefit is
fewer intermediate writes when a durable follow-up arrives late; it is not yet
measured. A new regression covers a follow-up at 750 ms without extending the
deadline. Existing tests verify waiter preemption and FIFO/neighbor behavior.

All 76 local object-log tests pass, excluding the same two live-S3 tests. The first
suite was interrupted after an empty-store open test stalled; that test passed
in isolation and the full rerun finished in 1.22 s. No fix for that unreproduced
stall is claimed. The new regression's initial push fixture was corrected to a
completion before the passing rerun. All six campaign tests (55.45 s) and four
workload recovery tests (2.41 s) also pass. A clean six-cycle performance
measurement is pending; no speedup is claimed for the one-second window.

Raw and derived artifacts use `fireweed-campaign-b9dd17ef-vfs-attribution-six*`,
`fireweed-b9dd17ef-vfs-accounting.json` and `fireweed-b9dd17ef-join-accounting.json`.
The archived `fireweed-account-vfs-trace.py.txt` reproduces VFS/phase aggregation
and checks that successful close totals cover all stores. The private disk
projection directory was removed after property readback and evidence capture.

2026-09-14 attribution follow-up: added opt-in
`FIREWEED_PROJECTION_IO_TRACE=1` to the disposable projection VFS. It reports
per-file write calls, requested bytes (including failed calls), elapsed time
inside pwrite/pwritev, maximum call time, counts at 1/10/100 ms and errors when
the handle closes. Classes distinguish WAL, Turso's `tursodb_temp_file`, and
main/other files. No paths or row data are logged. With tracing disabled it
does not collect clocks/counters; writes, completions, errors and omitted sync
retain their existing behavior. The runner records this flag as instrumentation,
so traced results cannot qualify performance.

These are VFS call times, not physical device service times or CPU times. Unix
PlatformIO completes these writes synchronously. Calls across stores overlap;
their elapsed times must not be added to campaign wall time. Temporary file
creation/removal and reads are not timed by these write counters. Abnormal
termination can omit handle-close totals, so only successful completed runs
support aggregate accounting.

Two VFS tests and six Python gate tests pass. An explicit repository-filesystem
debug campaign with 4,480 recipients, two stores, metadata/timestamp stages and
retention verified normal output and all three trace classes: 146 temporary
file writes (598,016 requested bytes), 147 WAL writes (32,873,544 bytes), and
two main-file writes (8,192 bytes). This is instrumentation validation, not
capacity evidence. Artifacts use `fireweed-vfs-trace-smoke-20260914*`.

Source review found no `temp_store` setting in the connection configuration.
Turso's `TempFile::with_temp_store` uses files for its Default/File settings;
the smoke trace confirms this path is exercised. That makes temporary execution
storage a candidate for measurement, but it does not establish its share of the
large-run slowdown or justify unbounded in-memory query scratch space. The next
run combines projection-write, apply-phase and authoritative-log tracing on the
unchanged full disk workload. The 10k/12.5k qualification goals remain unmet.

2026-09-11. This supersedes treating the original-row saturation test as full
campaign qualification. Historical measurements remain valid for their declared
workload and source. The source-preview v0.31.27 is committed locally; publication
was blocked by GitHub credential scope.

Current status (2026-09-12): the source-aligned timestamp workload's fastest
six-cycle result remains **8,329.69/sec**, failing throughput and reporting.
A one-worker control on `1fe89a44` achieved **7,911.78/sec** and passed every
non-throughput gate, including all 384 campaign-cycle progress checks. Neither
10k nor 12.5k is qualified. The 2 KiB default experiment did not improve sustained
runtime and is being reverted to 4 KiB. A read-only hardware review found that
the encrypted root device blocks TRIM and periodic fstrim is disabled. A controlled
one-time maintenance comparison is approved and awaits desktop administrator authentication;
its contribution to the storage bottleneck remains unproven.
See the [current resource math](workflow-hardware-headroom.md) and evidence below.

2026-09-13 update: storage maintenance is no longer a dependency for this work.
The authorized helper never ran: sudo timed out before authentication. The user
directed the investigation back to code. Two new instrumented, one-cycle,
million-recipient diagnostics on the existing binary reached 17,157.49 and
16,873.64 recipients/sec. Correction: those direct CLI commands omitted `--root`,
so their default temporary data directories were on `/tmp` (tmpfs). They provide
CPU profiles only, not durable-filesystem baselines. They are neither sustained
qualification nor evidence of a code speedup. The six-cycle targets remain open.

Apply tracing recorded 3,839 transactions and 1,049.62 aggregate apply-seconds,
overlapping across 32 stores: update SQL 742.00 s, read SQL 164.20 s, transformation
105.55 s, commit/checkpoint 27.59 s and writer wait 3.94 s. This first-cycle
attribution points to SQL execution, but cannot explain later checkpoint-heavy
cycles by itself. A separate 199 Hz user-IP profile collected 155,597 samples
without loss. Allocation, copying, VM execution and B-tree traversal dominate.

The next code candidate reuses at most 32 write-statement execution objects
within an owned apply transaction. Turso's existing compiled-SQL cache creates
a fresh VM and tracked statement per call; the new bounded cache targets that
allocation cost for repeated point updates. The SDK resets VM state and bindings
before execution. Cache lifetime ends before commit/rollback, read queries retain
their existing path, and no statement crosses a transaction or connection. No
log durability, SQL semantics, workload or acceptance gate changes.

The new regression passes 1,024 inserts and NULL updates, constraint-error
rollback and a subsequent fresh transaction. The existing fused-claim differential
test also passes. Release validation passed: 43 native, seven differential, six
campaign, two primitive CLI, two workload unit, seven recovery tests across the
two packages, and the free-page regression. Diagnostic artifacts use
`fireweed-code-*-20260913*`; sustained measurements follow below.

### Statement-reuse candidate and isolated projection I/O comparison

Clean `daa0dd77`, binary SHA256
`c2691498f6d97fcde90e3164f7695a837cd7d9c6b2f36169cf882f25d0d98d35`,
ran two serial six-cycle campaigns, each with one million resident recipients,
32 stores, two campaigns/store, 1,000-row storage batches, one worker/campaign,
two loaders/campaign, timestamp priority, metadata enrichment and RT1. Both used
the qualification runner's explicit repository-filesystem authoritative log.
Only the second run's projection root was on a private `/dev/shm` directory.
No TRIM or host configuration change ran. Private projection roots were removed
after recording evidence. This single pair isolates a configuration difference;
run order/media state remain possible confounders and repetition is still needed.

| Measurement | Log + projection on disk | Disk log, tmpfs projection (diagnostic) |
|---|---:|---:|
| Complete recipients/sec | 8,520.83 | 11,401.01 |
| Process wall, seconds | 704.47 | 526.72 |
| CPU ms/recipient | 1.03661 | 1.04597 |
| Average charged CPUs | 8.83 | 11.91 |
| Process output bytes/recipient | 12,869.29 | 2,555.91 |
| Logical log bytes/recipient | 1,834.826 | 1,834.828 |
| Host writes, GiB | 24.8708 | 12.0149 |
| Host write MiB/sec | 36.29 | 23.43 |
| Peak process RSS, GiB | 10.25 | 10.84 |

Disk cycle maxima were 87.82/122.39/110.42/140.88/114.00/122.83 seconds.
It fails overall throughput, cycles 1–5 throughput, and three reporting checks
(p95 1.030/1.139/1.149 seconds). Correctness, due-time, WAL, DB and RSS gates pass.
The tmpfs projection run fails the required on-disk projection gate, three
reporting checks and RSS stability. It is explicitly not qualification even
though its overall and individual-cycle rates exceed 10k. Neither configuration
qualifies 10k or 12.5k. Process RSS excludes tmpfs page storage.

Removing projection disk writes reduced wall time 25.2% and increased throughput
33.8%, while CPU work increased only 0.9%. This implicates the projection I/O
path without establishing an intrinsic NVMe limit. Projection stable-storage
sync is already omitted; remaining work includes WAL/main-file writes,
checkpoint execution and buffered-write blocking. The next code attribution
must distinguish those costs and authoritative-log waits under sustained load.
Do not call all elapsed commit time fsync, infer NAND traffic from host counters,
or treat observed bandwidth as device capacity. Statement reuse itself still
needs a same-configuration before/after comparison; no isolated speedup is claimed.

Artifacts use `fireweed-campaign-daa0dd77-{reuse-w1,logdisk-projectionram}-six*`.
The diagnostic's negative gate audit is `fireweed-projectionram-diagnostic-gates.json.gz`.

## Fixed objectives and units

First qualify **10,000 completed campaign recipients/sec**, then **12,500/sec**
(25% above that target), with two clean serial repetitions of each candidate.
Primitive insertion and addressed-update floors remain 10,000 rows/sec.
A throughput target is an engineering objective, not a theoretical hardware cap.

The baseline row-operation budget is insert + three claims + three mutations +
purge + occasional retry claim/mutation = approximately 8.105 operations/recipient.
10k recipients/sec therefore means about 81k logical row operations/sec; 12.5k
means about 101k. Projection fusion can combine operations. Public reporting
reads, retained-state verification, and retention discovery are additional work
and must be measured, not assumed free. In the payload-rewrite stress variant, three nominal 1 KiB body versions represent
29.3 MiB/sec at 10k and 36.6 MiB/sec at 12.5k before encoding/index/WAL overhead.
The primary metadata-enrichment variant retains the body; its measured initial
body size and logical log budget are recorded in the hardware-cost document.
Batch size and residency are independent of throughput and fixed in each report.

## Implementation sequence

1. Harden current report qualification against contradictory outcome totals,
   unsupported historical schema, dirty provenance, missing storage evidence,
   and invalid/non-finite rates. Preserve historical artifacts unchanged.
2. Add a bounded, generic public read of retained original rows, including
   terminal lifecycle, payload, metadata, attempts and version. Existing live-only
   reads retain their semantics. No auxiliary workflow entities or direct SQL
   in application tests. Unsupported backends must fail explicitly.
3. Implement a campaign CLI profile with independent deterministic handler/oracle
   fixtures: ingest list records, persist top-time candidates and metadata, use
   persisted candidates to schedule, release future windows using an injected
   clock, deliver bounded chunks with partial/transient/permanent outcomes.
   Separate stage limits (legacy scheduling 200, Cayce handlers 500), stable
   recipient identities, and original-row lease/version fencing are mandatory.
4. Poll public progress while work runs; independently verify persisted final
   disposition for every recipient before purge. Verify payload preservation,
   due gating, FIFO/priority, no omissions, duplicate handling, log-only recovery,
   and retained reporting. Cover multiple campaigns and a million-row resident
   backlog, not only cumulative recycling. Retention must not depend solely on
   producer memory; use a supported public discovery/read path.
5. Report real elapsed processing time including reads, settlement and purge;
   virtual-clock jumps incur no artificial calendar wait. Record phase time,
   stage batch occupancy, progress latency, physical shards, resident population,
   payload distribution, faults and storage bounds. Keep separate all-due and
   scheduled-window scenarios; never silently substitute one for another.
6. Establish a clean baseline, profile the dominant costs, implement evidence-led
   optimizations, and repeat qualification. Record failures as well as passes.
   Preserve authoritative-log sync and all correctness gates. Do not declare the
   target achieved from a primitive rate, aggregate average hiding starvation,
   or instrumentation-only result.

## Application boundary

Fireweed supplies generic state transitions, priority/eligibility, bounded reads,
and retention. The harness supplies deterministic top-time and provider stubs,
application progress aggregation and a separate expected-results oracle. Actual
ML/provider network performance and durable campaign archives remain application
concerns; the test must account for the queue reads and mutations they require.
No Snorri/Cayce implementation migration is part of this task.

## Initial implementation and validation

The first candidate implements a generic bounded `retained_items` read on the
composed Turso cell, campaign CLI profile, retained-state oracle, discovered
purge, future-window barriers, handler limits and live lifecycle-count polling.
Five campaign/read tests pass, including terminal reporting rebuilt from log
alone and full-sized handler chunk limits. Six Python qualification/monitor
tests pass, including negative contradictory-outcome, provenance, residency,
reporting-latency and stretch-target cases. Reports now require current schema.

The new row-operation math still has approximately 8.105 state-changing logical
operations per recipient, but also three full public retained-row reads per
cycle: scheduled-state verification, final-disposition verification, and retention
discovery. About 3N row reads plus 1 Hz lifecycle metrics per campaign are included
in elapsed time. This is deliberately more work than the old saturation profile.
The measurements below supersede this initial implementation status.

The initial campaign fixture covers load-before-scheduled delivery, with two
campaigns per store and four future windows. Immediate deterministic retries
remain; delayed retry backoff, overlap-mode campaign acceptance and continuously
aggregated enrichment-stage progress are not yet established by this profile.
These limits must remain visible while completing the broader plan.

Fixed before first capacity measurement: campaign progress query p95 ≤1 second,
due-to-claim maximum ≤60 seconds, at least 0.5 observed progress reads/second
(target polling interval 1 second), 512 MiB sampled WAL/store, last-three-cycle
RSS range ≤10% and projection range ≤5%. First-rate target 10k, stretch 12.5k;
32 stores, two campaigns/store, four workers/campaign, two loaders/campaign,
1,000-row loading, 500/200/500 handler maxima, at least one million resident rows.


## First measured baseline: target unmet

Clean source `e27754ce`, one million resident recipients, three complete cycles,
32 physical stores, two campaigns/store, four workers/campaign: **2,999 complete
recipients/sec** overall (1,000.77 seconds process wall). The application exited
successfully after independent retained-state checks; qualification rejected it.

| Slowest campaign phase/window | Cycle 1 | Cycle 2 | Cycle 3 |
|---|---:|---:|---:|
| Whole cycle seconds | 198.64 | 330.15 | 468.99 |
| Load seconds | 6.61 | 28.65 | 114.17 |
| Preparation plus scheduled export seconds | 96.92 | 227.34 | 182.98 |
| Delivery seconds | 74.37 | 46.52 | 138.28 |
| Final disposition export seconds | 0.50 | 0.53 | 0.53 |
| Purge seconds | 15.86 | 27.28 | 33.10 |
| Progress p95 seconds | 0.970 | 0.257 | 0.136 |

Phase maxima belong to potentially different campaigns and must not be summed.
Equivalent per-campaign rates fell from 5,034 to 3,029 to 2,132/sec. Some cycle-3
due-to-claim maxima exceeded the 60-second budget, reaching 65.27 seconds; the
first gate labels this under its combined independent-outcomes check. Actual
persisted payloads, terminal states, identities and retries passed verification.
The gate diagnostic will separate latency from outcome reconciliation.

Sampled WAL peak: 262.33 MiB. Peak RSS: 8.96 GiB. Last-three-cycle RSS passed;
projection-size stability failed for every shard. Example shard 0 grew from
65,638,400 to 68,444,160 to 89,333,760 bytes. Process-accounted output was
57.41 GiB; this is not a device/NAND byte count. Charged CPU was 5,130.85 user +
891.06 system seconds, **2.007 CPU-ms/recipient**, averaging about six logical
CPUs across the whole run. At unchanged cost, 10k/sec would require 20.1 CPU-s/s,
and 12.5k/sec 25.1 CPU-s/s. Both need cost reduction on this host; this measured
cost is not a fundamental minimum. Low average utilization also leaves a
scheduling/I/O opportunity that a pure CPU-bound estimate does not capture.

The broader release suite passed **111 tests, one existing ignored** before
measurement. Six Python qualification/monitor tests passed. Evidence:
[baseline](../helix/04-build/evidence/workflow-capacity/campaign-e27754ce-baseline.json.gz),
[release tests](../helix/04-build/evidence/workflow-capacity/campaign-e27754ce-release-tests.log.gz).

Next changes: collect bounded handler results into one bounded public mutation
per claim, independent of the 200/500 handler limits; reclaim expired unique
request receipts using their existing expiry semantics. The relational path
currently replaces expired receipts only when the same request ID returns.
The campaign's unique cycle IDs exposed retention growth hidden by key reuse.
No performance improvement is claimed until measured.


## First correction candidate

Handler limits remain 500/200/500. A worker now claims up to the public 1,000-row
limit, processes bounded handler chunks, and publishes the resulting guarded
patches in one mutation batch. This reduces durable command/receipt overhead and
allows more claim/update pairs to share projection application. It does not
combine recipients or skip their lifecycle transitions. All five campaign/read
correctness tests pass after the batching change.

The relational apply path now opportunistically collects up to 64 expired unique
request receipts before persisting a new receipt. It uses the same inclusive
expiry boundary as request replay, a queue-scoped expiry index, and deletes
associated claim-replay edges through bounded primary-key lookups. Unexpired
receipts and other queues are preserved. This is projection housekeeping; log
retention and durability are unchanged. The new native regression failed before
cleanup (73 receipts retained versus nine expected after the bounded sweep) and
passed afterward. The complete local relational suite passed 22 tests; six
Python gate/monitor tests passed. Release validation subsequently passed all 27 focused tests (22 native relational,
five public campaign/read); the capacity result follows.

[Regression before](../helix/04-build/evidence/workflow-capacity/campaign-receipt-gc-red.log.gz),
[regression after](../helix/04-build/evidence/workflow-capacity/campaign-receipt-gc-green.log.gz),
[relational suite](../helix/04-build/evidence/workflow-capacity/campaign-receipt-gc-suite.log.gz).


## Batching and expiry-GC measurement: still below target

Clean source `e4830172`, identical million-resident/three-cycle dimensions:
**3,873.70 recipients/sec**, up 29.18% from the first campaign baseline.
All persisted outcome and projection-size stability checks passed. Throughput,
18 first-cycle progress latency checks, and last-three-cycle RSS stability failed.
Neither 10k nor 12.5k is qualified.

| Slowest campaign phase | Cycle 1 | Cycle 2 | Cycle 3 |
|---|---:|---:|---:|
| Whole cycle seconds | 175.61 | 251.61 | 344.51 |
| Load seconds | 6.66 | 29.52 | 32.78 |
| Preparation plus scheduled export seconds | 76.75 | 154.09 | 168.28 |
| Delivery seconds | 64.08 | 44.82 | 44.45 |
| Purge seconds | 23.76 | 22.56 | 98.52 |
| Progress p95 seconds | 1.649 | 0.649 | 0.494 |

Again these are independent maxima, not additive phase accounting. Shard 0's
maximum queue-observed projection endpoints were 66,895,872; 68,030,464; and
68,407,296 bytes. Bounded expiry cleanup removed the prior projection growth
failure, but did not eliminate progressive throughput degradation.

Process wall was 774.79 seconds, charged CPU 5,539.29 seconds, peak RSS 10.61 GiB,
and process-accounted output 56.31 GiB. CPU cost is **1.846 ms/recipient**, down
8.0%; at unchanged cost 10k/sec needs 18.46 CPU-s/s and 12.5k needs 23.08.
This remains implementation cost, not a hardware lower bound. Average occupancy
was only 7.15 logical CPUs, leaving waiting/scheduling and filesystem work outside
that simple accounting. A mid-run observation recorded about 196 GB of cached
file reads and almost no physical reads; it does not establish a device-read
bandwidth bottleneck.

Next: collect user-space CPU samples and slow SQL timings on the same residency
and application shape. Progress counts currently scan item rows; preparation
and purge also require diagnosis. Instrumented results cannot qualify capacity.

[Capacity](../helix/04-build/evidence/workflow-capacity/campaign-e4830172-batched-gc.json.gz),
[27 release tests](../helix/04-build/evidence/workflow-capacity/campaign-e4830172-release-tests.log.gz).


## Profile and progress-index candidate

The `e4830172` diagnostic retained million-row residency, 32 stores, two
campaigns/store, and the same handler limits. With SQL/apply tracing and 199 Hz
user-space IP sampling across 16 logical CPUs, it completed one full cycle but
failed during the second with `object-log post-position produce timed out`.
This is failed diagnostic evidence, not a throughput qualification. It collected
478,616 samples, zero reported lost. Allocation, copies, SQL execution and B-tree
searches were distributed costs; no single serialization routine dominated.

Lifecycle counts were the largest accumulated slow-SQL category: 19,787 observed
queries, 1,025.99 seconds summed elapsed. These times overlap across tasks and
include only statements taking at least 2 ms; they are not total CPU or process
wall time. The query used item-row lookup plus grouping. The next candidate adds
a narrow partial `(tenant_id,queue_id,lifecycle_state)` index for non-superseded
rows and explicitly uses it for progress counts. Its write overhead must pass the
same uninstrumented capacity gate. The main priority materialization query
already filters eligibility before joining payloads; it was not changed.

The harness additionally reports nonempty claim batches, mutation batches,
maximum claim size and empty worker claims, separately from handler batches.
Its final oracle now checks persisted completion timestamps. A focused query-plan
regression prevents reintroducing a temporary grouping tree for lifecycle counts.

[Profile analysis](../helix/04-build/evidence/workflow-capacity/campaign-e4830172-profile-analysis.json.gz),
[CPU sample summary](../helix/04-build/evidence/workflow-capacity/campaign-e4830172-profile-summary.txt.gz),
[diagnostic trace and failure](../helix/04-build/evidence/workflow-capacity/campaign-e4830172-profile-stderr.gz).

All 28 focused development tests passed for the index candidate (23 native
relational, five public campaign/read), as did six Python gate/monitor tests.
Release validation and uninstrumented measurement remain pending.


## Reject the progress index; test cross-queue apply coupling

Clean `ad6fb2d9` passed all 28 focused release tests. Its first complete million-row
cycle took **201.06 seconds** (slowest campaign), versus 175.61 seconds for
`e4830172`. Worst progress p95 was **1.715 seconds**, still failing. The run was
intentionally terminated during cycle two, after the first-cycle rate already
failed; exit -15 is the operator's SIGTERM, not an application crash. Partial-run
resource totals cannot be divided by one million to claim per-recipient cost.
The index experiment is rejected and its index is removed on reopen as well.
No full three-cycle result or storage-stability claim is made for this candidate.

A new regression demonstrates cross-queue coupling in the apply coordinator:
an uncovered read for queue B bypasses queue A's bounded Claim/follow-up join,
even though B's read does not depend on A. The original counter was global to the
physical store. The candidate uses queue-scoped waiter registrations, removes
them on cancellation/completion, and preserves same-queue read bypass, the
500 ms maximum join window, FIFO runnable selection and all coverage checks.
The regression failed before this change and all 24 coordinator tests passed
afterward. Opt-in apply tracing now reports join time and envelope counts so the
batching benefit can be measured. Public campaign validation and uninstrumented
qualification are still required before claiming an improvement.

[Stopped index run](../helix/04-build/evidence/workflow-capacity/campaign-ad6fb2d9-index-aborted.json.gz),
[index release tests](../helix/04-build/evidence/workflow-capacity/campaign-ad6fb2d9-release-tests.log.gz),
[queue-coupling regression before](../helix/04-build/evidence/workflow-capacity/campaign-queue-join-red.log.gz),
[24 coordinator tests after](../helix/04-build/evidence/workflow-capacity/campaign-queue-join-green.log.gz).

All five public campaign/read tests also passed with queue-scoped waiters and
the rejected lifecycle index removed. Release validation and capacity are next.


## Queue-scoped waiter result and hardware observations

Clean `861619a9` completed its first cycle in **188.55 seconds**, with worst
progress p95 **1.731 seconds**. It was stopped by SIGTERM in cycle two after the
first-cycle rate failed. Queue-scoped waiting alone has not demonstrated an
improvement over the 175.61-second best initial cycle. It remains a candidate
whose benefit must be checked with the next application-level optimization.

All five public campaign release tests and 24 coordinator release tests passed.
A broader object-log release run passed 66 tests and failed two live S3 tests
because `FIREWEED_S3_TEST_ENDPOINT` was unset. Docker socket access was denied;
those live S3 checks remain unverified. This qualification uses filesystem logs.

A 266.92-second host-wide NVMe observation around the partial run recorded
10.87 GiB written, **41.71 MiB/sec**, approximately 1,025 completed writes/sec,
82.72 ms mean write request time and 15.60 ms mean flush request time. Recorded
busy time was about 90% of the interval; weighted in-flight time averaged 85.
These include host activity and start/stop margins. They are not process-exclusive
or NAND measurements, nor proof of a fixed sequential bandwidth limit. The
observed queueing motivates reducing write amplification. CPU frequency and
available temperature readings are retained in the per-second raw trace.

Sector counts use 512-byte units; accumulated request time can exceed elapsed
time when requests overlap. See the kernel's [block statistics definitions](https://docs.kernel.org/block/stat.html)
and [I/O accounting caveats](https://docs.kernel.org/admin-guide/iostats.html).

[Stopped workload](../helix/04-build/evidence/workflow-capacity/campaign-861619a9-queue-join-aborted.json.gz),
[device summary](../helix/04-build/evidence/workflow-capacity/campaign-861619a9-device-analysis.json.gz),
[device samples](../helix/04-build/evidence/workflow-capacity/campaign-861619a9-device.jsonl.gz),
[monitor source](../helix/04-build/evidence/workflow-capacity/campaign-device-monitor.py.gz),
[broader release run](../helix/04-build/evidence/workflow-capacity/campaign-861619a9-release-with-s3-environment-failures.log.gz).

## Explicit metadata enrichment variant and revised byte budget

The initial fixture unnecessarily rewrote the whole payload twice to persist a
few enrichment attributes. The original-row workflow does not require that
representation. The reviewed legacy scheduled-action persistence updates status,
message and completion columns; Cayce scheduling returns structured decision
attributes. A prospective Fireweed integration can keep input/profile payloads
and use the existing structured row metadata for enrichment. This is a mapping
choice, not a claim that today's Snorri adapter already implements it.

`--campaign-metadata` explicitly selects that variant. Top-time candidates are a
stored typed array; scheduling reads it and persists the selected time. Color,
score, stage and delivery disposition remain on the same original row. The body
and its varied padding are retained unchanged. The payload-rewrite variant stays
available without that flag and remains independently tested. Neither uses side
workflow entities, changes handler limits, skips a lifecycle transition, omits
reporting, weakens log durability, or reduces resident population.

Both variants still require approximately **8.105 logical row operations per
recipient**, plus three full retained exports and progress queries. The byte
budget differs: the payload-rewrite variant writes three body versions; metadata
enrichment writes one original body plus changing metadata. Nominal 1 KiB bodies
alone therefore imply about 9.8 MiB/sec at 10k or 12.2 MiB/sec at 12.5k before
metadata, log encoding, indexes, WAL and checkpoint amplification. The exact
initial/replacement body byte totals are now measured rather than inferred from
`--payload-bytes` (which reserves padding space for enrichment).

Schema `campaign-capacity/v2` records `enrichment_storage`, exact initial body
bytes, replacement counts/bytes, and storage-batch occupancy. Qualification
requires a declared representation, nominal payload size >=1 KiB, the appropriate
replacement count (zero or 2N), and the same outcomes, 10k/12.5k rates, fairness,
reporting, residency and storage gates. Results must always identify the variant;
a metadata pass must not be reported as a payload-rewrite pass. All five campaign
suite tests passed, now exercising both representations in retained/disposition
verification and log-only recovery; six gate/monitor tests passed. Performance of
the metadata variant remains unmeasured.


## Complete metadata baseline: improvement, still not qualified

Clean `930f5c15`, one million resident rows, three complete cycles, 32 stores,
two campaigns/store, unchanged concurrency and handler limits: **4,900.59
recipients/sec**. All independent persisted outcomes and projection-size stability
checks passed. Throughput, 81 progress checks, 36 third-cycle due-latency checks,
and RSS stability failed. This is a metadata-enrichment result, not a
payload-rewrite result.

| Slowest campaign phase | Cycle 1 | Cycle 2 | Cycle 3 |
|---|---:|---:|---:|
| Whole cycle seconds | 137.90 | 224.66 | 246.93 |
| Load seconds | 7.01 | 33.27 | 40.89 |
| Preparation plus scheduled export seconds | 48.58 | 52.38 | 57.43 |
| Delivery seconds | 37.94 | 72.35 | 120.06 |
| Purge seconds | 40.29 | 66.26 | 28.14 |
| Progress p95 seconds | 2.630 | 1.321 | 1.120 |

Phase maxima are not additive. Total process wall: 612.60 seconds. Charged CPU:
**1.691 CPU-ms/recipient**, implying 16.91 CPU-s/s at 10k and 21.13 at 12.5k if
cost stayed unchanged. The simplistic 16-thread division yields only about 9.46k;
that is a current-cost comparison, not a theoretical limit. Lower cost and less
waiting are still required. Average process CPU occupancy was about 8.28 logical
CPUs. Peak RSS: 10.97 GiB. Process-accounted output: **41.31 GiB**, or
**14,786.90 bytes/recipient**, down from 20,153.44 in the batched payload baseline.

Exact initial body bytes totaled 2,804,666,670 (934.89/recipient); enrichment body
replacements were zero. Retained log files totaled 5,470,852,468 logical bytes,
or **1,823.62/recipient**, versus 3,662.96 for the batched payload baseline.
The log contains more than body bytes: metadata, identities, guards and outcomes
also matter. Log file size is not device write traffic. The device trace also
includes start/stop and runner cleanup; do not divide its entire interval by the
recipient count to claim exact workload-only write amplification.

[Full run](../helix/04-build/evidence/workflow-capacity/campaign-930f5c15-metadata-baseline.json.gz),
[device observations](../helix/04-build/evidence/workflow-capacity/campaign-930f5c15-device.jsonl.gz),
[release tests](../helix/04-build/evidence/workflow-capacity/campaign-930f5c15-release-tests.log.gz).

## Remove redundant planning reads and batch discovered retention

The next native change omits the payload-table join only when every addressed
patch retains its payload and the caller requests identity results. Payload
replacement still reads the old body for equality/NoChange, and BeforeSnapshot
still loads it. A public 128 KiB-body regression checks metadata-only updates,
Keep/no-change, equal replacement/no-change, before-snapshot payloads and explicit
body removal. Lease/version/predicate planning otherwise remains the same.

The campaign had ignored the existing `--purge-batch` setting and issued one
purge per 1,000-row discovery page. It now accumulates discovered IDs into a
bounded batch (default 8,000; maximum 8,192). Read pages remain <=1,000 and no
producer ID list drives retention. A non-page-aligned 1,025-row purge test checks
the boundary and final partial batch. This reduces public/log calls, not the
number of purged rows or retention checks. Six public campaign/read tests and
six gate/monitor tests passed; release validation and measurement are next.

Schema `campaign-capacity/v3` adds purge-batch occupancy and explicitly records
the existing one-hour lease/request-retention durations and 7,200-second cycle
clock step. These durations are unchanged. The gate checks declared temporal
assumptions and exact purge batch counts in addition to the earlier requirements.


## Payload-read/purge candidate screening and next query change

Clean `385d9335` passed all six release campaign/read tests. Single-cycle,
one-million-resident metadata screens kept total workers (256) and loaders (128)
constant; these are **not sustained qualification**:

| Physical stores | Recipients/sec | Process wall | CPU-ms/recipient | Peak RSS GiB | Worst progress p95 |
|---|---:|---:|---:|---:|---:|
| 32 | 8,671.81 | 115.46 s | 1.363 | 9.50 | 3.380 s |
| 64 | 8,523.98 | 117.63 s | 1.341 | 12.00 | 3.666 s |

The 32-store slowest-campaign phase maxima were load 11.55 s, preparation
43.51 s, delivery 38.02 s and purge 19.71 s. At 64 stores they were 27.03,
41.35, 37.88 and 7.46 s respectively; maxima are not additive. More stores did
not improve throughput and increased memory. Neither screen met rate or progress
latency targets, and neither establishes last-three-cycle storage stability.
The two production changes were measured together; this is not isolated causal
attribution of their individual gains.

[32-store screen](../helix/04-build/evidence/workflow-capacity/campaign-385d9335-screen32.json.gz),
[64-store screen](../helix/04-build/evidence/workflow-capacity/campaign-385d9335-screen64.json.gz),
[release validation](../helix/04-build/evidence/workflow-capacity/campaign-385d9335-release-tests.log.gz).

The next candidate replaces the pending priority index with one that also stores
not-before, eligibility and cohort scalars. The priority query first selects a
bounded list of eligible IDs using index columns, then seeks full rows and
payloads by their complete keys. It preserves priority/FIFO tiebreaks, future
eligibility, exclusions and payload materialization. It does not change the test
clock to avoid future-priority backlog, remove a workflow transition, or add a
second pending-order index. Existing projections drop the superseded index and
create the replacement on migration.

Turso's EXPLAIN QUERY PLAN labels covered range seeks merely USING INDEX. The
regression therefore inspects actual VM column reads in the bounded candidate
coroutine, plus full-key materialization seeks. A lazy unused table cursor can
still be opened because the partial predicate's columns are not stored; its
DeferredSeek only records intent and no candidate body column is read. Extra
constant lifecycle/superseded index columns were tried during development and
removed as unnecessary. Complete-workflow measurements must still justify the
wider index's write cost. Target achievement is still pending.

Development validation: six public campaign tests and 22 local relational tests
passed, including schema/reopen checks. The native suite passed 35 tests; one
existing debug timing gate failed (first read 111,375 us versus 90,000 us). The
new bytecode regression passed. Release validation must resolve the timing check
before claiming this candidate qualified. Logs are retained as
`campaign-covering-{native,local,campaign}-tests.log.gz` in the evidence directory.


## Covering-index full run and concurrency diagnostic

Clean `85b5554e` passed 36 native release tests and all six campaign release
tests; the earlier debug read-timing failure did not recur in release. Its full
one-million-resident, three-cycle metadata run completed at **5,663.00/sec**
(process wall 529.89 s). All independent outcomes, due latency, RSS stability and
sampled WAL budgets passed. Rate, 172 progress checks and six projection-size
stability checks failed. The goal remains unmet.

| Slowest campaign phase | Cycle 1 | Cycle 2 | Cycle 3 |
|---|---:|---:|---:|
| Whole cycle seconds | 109.96 | 199.78 | 216.87 |
| Load seconds | 6.93 | 46.23 | 39.97 |
| Preparation/export seconds | 44.46 | 48.60 | 50.56 |
| Delivery seconds | 37.71 | 38.03 | 60.98 |
| Purge seconds | 16.19 | 66.44 | 64.13 |
| Progress p95 seconds | 4.766 | 2.557 | 1.761 |

Phase maxima are not additive. CPU cost was 1.515 CPU-ms/recipient, peak RSS
10.81 GiB, process-accounted output 15,148.52 bytes/recipient, and logical retained
log files 1,823.45 bytes/recipient. The device monitor added per-process CPU time.
Its sampled active-process interval (521.47 s, missing startup/tail margins)
observed host-wide 17.67 GiB written, 82.76 ms mean write-request latency and 87.9%
busy time. These are not process-exclusive writes or a raw/NAND throughput cap.
During a processing interval CPU occupancy reached about 15 logical CPUs; during
a retention interval it fell to 2.3 while device busy time approached 96%. There
are both CPU work and writeback/queueing costs to reduce.

A subsequent one-cycle **instrumented diagnostic**, same source/binary and 32
stores but one worker/campaign, measured 6,728.05/sec (148.78 s process wall),
1.321 CPU-ms/recipient, 7.28 GiB peak RSS and worst progress p95 0.801 s. It is not
qualification or an uninstrumented A/B result. The trace recorded 2,951 join
windows, 293 that grew during the wait, 1,856 ending with a coverage waiter, and
798 apply groups containing both claim and other commands out of 6,783 groups.
Reduced concurrency improved progress latency but did not achieve throughput.
The native claim API already returns empty results without appending an empty
Claim command; changing empty-command join handling would not help this fixture.

[Full result](../helix/04-build/evidence/workflow-capacity/campaign-85b5554e-covering.json.gz),
[device trace](../helix/04-build/evidence/workflow-capacity/campaign-85b5554e-device.jsonl.gz),
[one-worker diagnostic](../helix/04-build/evidence/workflow-capacity/campaign-85b5554e-worker1-diagnostic.json.gz).
Release logs and monitor source are archived beside these artifacts.

Next: replace lifecycle GROUP BY sorting with a single-pass four-count aggregate,
verify exact counts/isolation/empty queues and the absence of a sorter, then
measure without adding a metrics index. Shorter reads may also reduce checkpoint
pinning, but that benefit must be measured. Continue investigating projection
write pressure and bounded coordination waits rather than increasing concurrency
or weakening durability, reporting, residency or rate gates.

The no-sort regression failed against the original GROUP BY query and passed
with four scalar aggregates. Exact empty/mixed lifecycle counts, superseded-row
exclusion, tenant/queue isolation and terminal totals passed. All six public
campaign tests passed with the aggregate query. Red/green and campaign logs are
archived as `campaign-metrics-aggregate-*.log.gz`; release measurement follows.


## Aggregate-count screens; correct apply queue fairness

Clean `91f15186` passed 38 native and six campaign release tests. Uninstrumented
one-cycle million-resident screens, 32 stores and two loaders/campaign:

| Workers/campaign | Recipients/sec | CPU-ms/recipient | Process wall | Worst progress p95 |
|---|---:|---:|---:|---:|
| 4 | 8,651.08 | 1.409 | 115.75 s | 4.179 s |
| 2 | 9,628.81 | 1.287 | 104.08 s | 2.463 s |

Neither screen qualifies; three-cycle stability is unmeasured for this candidate.
The four-worker result does not demonstrate an improvement from the aggregate
query. Two workers improved this screen but still missed both rate and reporting
latency. Keep reviewing the aggregate's cost instead of assuming removal of a
sorter necessarily improves the whole workload. Full three-cycle best remains
5,663/sec. Raw screens and release tests are archived under `campaign-91f15186-*`.

Further source review found a real fairness defect: `next_runnable` replaced an
already selected runnable queue whenever it encountered a different later queue.
It therefore preferred later admissions across queues, contrary to its FIFO
comment. The new regression fails against that selector and checks both initial
order and a queue replenishing while another waits. Selection now preserves the
first runnable queue; within that queue it still chooses the earliest eligible
log position. Existing bounded coalescing and gap/poison/reservation rules remain.

A separate bounded optimization reuses claim commands already retained by the
apply coordinator after their authoritative append. Mutation planning previously
reread this same tail from the log. Only a complete, contiguous, same-epoch tail
of at most sixteen disjoint authoritative claims is reusable. Missing entries,
non-claim commands, repeated IDs, old claim semantics or epoch changes retain the
existing log-read/coverage fallback. No new durability authority, workflow entity
or persistent side record is introduced. The existing validator is shared by
retained and fetched tails. Optional apply tracing records hit/miss and elapsed
time without request IDs or row data.

Twenty-six coordinator tests pass, including retained-tail bounds, completeness,
isolation, classification, duplicate-ID rejection and poison handling. The real
composed-backend lease/version test additionally compares the retained tail with
the authoritative log. Public campaign and release validation/measurement follow;
no performance gain is claimed for these changes yet.

The selector defect also affected join refresh: after A's claim started waiting,
newer ready work in B hid A's already-arrived follow-up from the refresh. A second
regression failed with the old selector (selected entry 2 instead of claim and
follow-up entries 1 and 3), then passed with FIFO selection. This establishes the
mechanism, not a measured rate improvement. Twenty-seven coordinator tests, the
real composed lease/version/claim-tail test, and all six public campaign tests
passed on the corrected implementation in development mode. The candidate is
ready for release validation and an instrumented join comparison followed by
uninstrumented sustained qualification.

## FIFO/tail measurements and maintained lifecycle counts

Clean `82d3adcb` passed 27 release coordinator tests, the composed tail/lease
regression, and six public campaign tests (also rerun under the canonical
workload feature graph). Its one-cycle instrumented million-row screen reached
10,556.86 recipients/sec at 1.251 CPU-ms/recipient, two workers/campaign,
batch 1,000. All 1,980 observed claim-tail lookups reused retained authoritative
commands. This is **not qualification**: one cycle, diagnostics enabled, progress
p95 up to 2.127 s. Process output was 10,202 bytes/recipient, versus 10,677 in the
prior comparable two-worker screen; comparing it with a full three-cycle run
would confound duration and warm-state behavior.

The subsequent uninstrumented three-cycle run used two workers and batch 500.
It completed at **4,968.46 recipients/sec**, 603.98 s, 1.516 CPU-ms/recipient,
7.53 mean charged CPUs, 7.95 GiB peak RSS and 15,530 process-output bytes/recipient.
Worst campaign walls were 105.57 / 235.73 / 260.42 s. Load maxima rose from 8.62 s
to 49.07 / 48.53 s; purge maxima were 9.23 / 65.99 / 21.76 s. These phase maxima
are not additive. Rate, progress, third-cycle due latency and all 32 projection
size stability gates failed. Smaller batches did not solve sustained performance.
The best complete run remains 5,663/sec; neither fixed objective is achieved.
Raw reports, summaries, diagnostic traces and host-wide device traces are archived
under `campaign-82d3adcb-*`; device traces include startup/tail margins.

The next candidate removes full resident-row scans from public lifecycle metrics.
Four counters live on the existing `queues` metadata row. They are rebuildable
projection data, not workflow entities or a new authority. Each projection apply
transaction captures actual lifecycle counts before and after its addressed item
changes and applies the delta in that same transaction. This handles fusion,
replay no-ops, partial historical claims, rejected operations and rollback without
reimplementing transition rules. Cohort/supersession commands conservatively use
whole-queue before/after counts; lifecycle-neutral commands skip this work through
an exhaustive command classification. Column constraints reject negative/noninteger
counts. An atomic versioned migration backfills existing projections once.

The first implementation exposed a planner trap: despite a target-first CROSS
JOIN, Turso chose the active-key index using only tenant/queue, scanning the queue
for each requested ID. The bounded-key regression failed with that exact plan.
An explicit primary-key index selection makes all three key parts participate;
the regression now passes. The obsolete slow campaign test process was stopped,
and correctness tests are rerun with the corrected query. No throughput benefit
is claimed until clean sustained measurements complete.

Before the index correction, 39 native unit tests and 22 integration tests passed;
a strengthened independent row-count oracle then passed all 22 lifecycle tests
and three recovery tests, checking counters after success/rejection and after
reopen, genesis rebuild and overlapping replay. Four focused counter/migration/
query-plan tests passed after the correction. Public campaign validation follows.

All six public campaign tests passed with explicit primary-key counting, including
large storage batches, both enrichment modes, retained reporting, payload Keep
semantics and log-only recovery (37.66 s in development mode). Release validation
and clean sustained measurement are next; no rate or stability gate is waived.

## Counter candidate: failed sustained run; checkpoint destination locality

Clean `88c14a08` passed all six public campaign release tests (14.72 s).
Its uninstrumented million-resident, three-cycle run used batch 1,000 and two
workers/campaign. Cycle zero completed in a worst-campaign 96.044 s (about
10,412 equivalent recipients/sec), but progress p95 reached 1.552 s. During cycle
one, only 43 of 64 campaign completion reports arrived before an ambiguous
object-log produce timeout ended the run at 322.061 s. The reported cycle-one
maxima included 222.607 s wall, 59.059 s load and 67.371 s purge. Those are partial
cycle maxima, not a completed-cycle rate. There is **no overall throughput result**
and no qualifying pass. The complete failed attempt charged 2,500.975 CPU seconds
and 28.206 GB of process output. Raw reports and partial summaries are archived
under `campaign-88c14a08-*`. Counters have not solved the sustained-performance
problem; the best complete run remains 5,663/sec.

Source review ruled out duplicate bodies in claim replay receipts: they contain
item IDs and lease metadata. Checkpoint ordering exposes a separate concrete
problem. Turso selected latest safe frames in WAL-frame order, then built bounded
512-page destination-write batches. Reused database pages adjacent on disk can
therefore fall into different batches and become separate writes. The candidate
orders the same selected frames by destination page before batching. Safe-frame
selection, locks, sync publication, limits and log authority are unchanged.

A native integration regression updates 2,048 existing rows/pages in interleaved
order and observes actual database-storage write calls. The old ordering issued
1,973 calls for 2,048 pages; destination ordering issued eight. It then truncates
the WAL, reopens and checks identities and newest values independently. The test
covers both a large pager cache and a reduced cache requiring WAL reads. This is
an I/O-call reduction, not a claimed throughput multiplier or reduction in bytes.
The checked-in vendor patch was regenerated against the checksum-verified
published crate and its complete patch round-trip verified.

Thirty-nine of forty native unit tests passed in the first broader run; a 90 ms
reader latency assertion took 111 ms while public-test compilation ran alongside
it. That timing check is being repeated serially without changing its deadline.
All six public campaign tests passed (36.57 s development mode). Broader native
checkpoint/concurrency/recovery checks and clean release measurement follow.

The serial debug reader check also took 109 ms. Restoring the original checkpoint
ordering as an isolated control reproduced the failure at 107 ms. The ordering
change therefore does not explain this debug timing failure; the 90 ms deadline
is preserved and will be checked in release mode. With destination ordering
restored, all 38 selected native integration tests passed: cached/WAL-read
checkpoint locality and reopen, checkpoint policy and pinned readers, concurrent
writers, cancellation, differential projection histories, lifecycle operations
and recovery. The full vendor patch still round-trips from the published crate.

## Destination-ordered checkpoint result and nonblocking join scheduling

Clean `440a60fd` passed all 40 native unit tests and six public campaign tests in
release mode, including the unchanged 90 ms reader check. The canonical CLI
feature graph rebuilt without changes. The complete three-cycle campaign reached
**7,470.33 recipients/sec**, 401.804 s wall, **1.26286 CPU-ms/recipient**, 9.43 mean
charged CPUs, 9.78 GiB peak RSS, 13,367.79 process-output bytes/recipient and
1,823.51 retained-log bytes/recipient. Worst campaign walls were 96.15 / 131.97 /
169.66 s; load maxima 7.07 / 37.48 / 44.34 s and purge 11.93 / 15.96 / 33.04 s.
These maxima are not additive. Outcomes, due times, RSS and sampled WAL passed;
overall and late-cycle rates, 64 progress and 17 projection-stability checks failed.
The best complete result improved about 32%, with about 17% lower CPU cost than
`85b5554e`; that comparison includes multiple code/concurrency changes.

The same-binary NOCOW projection control was stopped at 175.278 s with no complete
campaign reports and 5.21 mean charged CPUs. Both DB and WAL inherited the recorded
`C` attribute; the durable log kept its normal attributes. SIGTERM/nonzero exit,
resource usage and filesystem evidence are preserved. No throughput claim or new
filesystem default follows from this unfavorable control. Its temporary data was
removed after measurement. Raw reports/device traces are `campaign-440a60fd-*`.

A red coordinator regression then showed a ready neighbor missing a 200 ms
coverage deadline because the shared apply worker spent up to 500 ms waiting
for another queue's claim follow-up. The next implementation defers only that
queue while selecting other runnable queues. Join windows are tracked by their
first retained entry, never restarted by incoming notifications; independent
windows overlap. Expired windows regain FIFO selection. Own-queue coverage still
bypasses its window, while serving a neighbor does not prematurely apply the
waiting claim. Each selected generation retains the existing contiguous-prefix,
reservation, epoch, poison and bounded-coalescing checks.

The worker arms notification before inspecting state and sleeps only when every
runnable queue is deferred. Selection now borrows its first retained batch rather
than cloning it and immediately cloning its commands again; idle-work detection
also avoids constructing a throwaway generation. Thirty coordinator tests passed,
including the red/green ready-neighbor case, fixed-deadline fairness, overlapping
windows and queue-specific coverage preemption. All six public campaign tests
passed (37.30 s development mode). The composed lease/version regression and
clean release measurement follow. Neither performance objective is achieved yet.

The composed acknowledged-claim-tail lease/version regression passed. The final
review also scoped the armed notification to selection/waiting so it does not
cause unrelated notification wakeups while a selected SQL apply is in flight.
All thirty coordinator tests passed again after that scope adjustment.

## Ready-queue scheduling result and allocation review

Clean `386d79f7` passed all six public campaign tests in release mode. The complete
million-resident, three-cycle run reached **7,718.50 recipients/sec**, 388.995 s
wall, **1.21699 CPU-ms/recipient**, 9.39 mean charged CPUs, 9.71 GiB peak RSS,
12,863.20 process-output bytes/recipient and 1,823.51 retained-log bytes/recipient.
Worst campaign walls were 97.80 / 139.08 / 148.89 s. Correctness, due times, RSS
and sampled WAL passed; overall/late-cycle rates, 61 progress checks and all 32
projection-size stability checks failed. The gain over `440a60fd` was 3.3%, with
3.6% lower CPU cost. Neither target is achieved. Raw report, summary, device samples
and release tests are archived as `campaign-386d79f7-*`.

A separate one-cycle CPU diagnostic on that same binary used 199 Hz inherited
user-IP sampling and apply tracing. It completed with 205,265 samples and zero
lost; it is not qualification. Provenance, workload output, traces, samples, maps,
executable symbols, sampler source and symbolization script are preserved.
Allocator/copy functions and metadata-map cloning were prominent. Review found
redundant deep copies in addressed-request grouping, projection-worker handoff,
mutation scratch validation and old-row bookkeeping. The next candidate replaces
these with Arc ownership, moved commands, a consumed temporary projection image,
and only the old lifecycle/gate/lease fields required for bookkeeping. Both
planner entry points retain the same pre-append apply validation; the borrowed
planner still preserves its input. No command format or durability change.

Validation passed: six public campaign tests (37.90 s), 32 projection tests
including owned/borrowed planner equivalence across dry runs, return modes,
metadata changes, completion, purge and missing rows; 30 coordinator tests,
including apply failure/retry; and the composed unapplied-claim lease/version
guard regression (0.54 s). Release validation and clean capacity measurements
follow. Repository-wide formatting check still reports pre-existing formatting
in unrelated files; changed Rust files were formatted without unrelated edits.

## Allocation candidate failure, device calibration and blocking commit fix

Clean `b508d888` passed six release campaign tests (14.61 s), but its 32-store
capacity run failed at 480.332 s with `object-log post-position produce timed out`.
Exactly 128 campaign reports cover two complete cycles: maximum walls 95.45 /
166.84 s, progress p95 1.678 / 0.901 s. The third cycle has no completed campaign
reports. Total attempt CPU was 3,365.24 s and output 36.18 GB; neither is normalized
by an assumed completed-recipient count. Peak RSS was 9.96 GiB. The best complete
rate remains 7,718.50/sec on `386d79f7`.

A same-binary 16-store control completed two cycles in 164.34 / 286.08 s, with
progress p95 0.538 / 0.699 s and due maxima 26.95 / 40.94 s. It was stopped with
SIGTERM at 555.400 s because the throughput failure was already established and
code review identified the blocked-commit issue below. Its 64 campaign reports,
nonzero exit and stop reason are preserved. It began with a warm device and
halved aggregate WAL capacity; no isolated causal store-count claim is made.

The subsequent private-file 8 GiB sequential-write calibration reached 38.23
MiB/sec in 214.296 s including fdatasync. It used aligned 16 MiB incompressible
writes with O_DIRECT requested and COW disabled only for that temporary file;
first/last blocks verified and the file was removed. This is a diagnostic
reference, not qualification or a proven device maximum. Reports, host CPU/I/O
pressure traces and the scripts are `campaign-b508d888-*`. The updated hardware
math retains the fixed goals and quantifies the approximate 28% physical-byte
reduction needed for 12.5k at this reference bandwidth.

A native MemoryIO wrapper then gated an actual WAL pwrite. An unrelated async
worker could not resume until its 500 ms native timeout fired, reproducing the
problem on both current-thread and one-worker multi-thread runtimes. The async
SDK does not make Unix VFS calls nonblocking. The fix runs the entire owned apply,
including commit/checkpoint, on a blocking worker with a local runtime; the RelTx
hop remains separate to avoid nested block_on. Writer ownership, cancellation
cuts, transaction validation and log authority are preserved. Both red/green
regressions are archived. All 42 native unit tests (20.03 s) and six public
campaign tests (38.41 s) pass.

The candidate also increases only the rebuildable checkpoint window from 250 to
448 MiB, with the 512 MiB measured peak gate unchanged. Explicit readback tests
check 114,688 frames at 4 KiB and 229,376 at 2 KiB, and retain standalone 1,000
frames. This is a write-coalescing experiment within the existing disk budget;
its throughput, stability and transaction overshoot still require measurement.

All eight selected native integration tests also passed: cancellation, concurrent
writers, checkpoint policy and pinned readers, plus three recovery histories.
The cgroup ancestry had no CPU quota or explicit I/O rate cap; that read-only
observation is archived. No system settings were changed. Release validation
and capacity measurements on the fixed candidate follow.

## Blocking-commit measurement and free-page write experiment

Clean `b86382a1` passed 42 native release unit tests and six public campaign
tests, but the full capacity attempt failed after 324.756 seconds with the same
post-position timeout. Only the first cycle completed: maximum campaign wall
101.098 s and progress p95 2.396 s. There is no valid whole-run throughput.
Device sampling recorded 8.689 GiB written and 0.341 GiB read during 322.058 s,
with 92.02% busy time and 122.18 ms mean write latency. The larger checkpoint
window did not produce a successful result and is reverted to 250 MiB; this
combined experiment does not isolate its causal effect. The independently
reproduced blocking-worker fix is retained. The timeout diagnostic now distinguishes
log production from high-water metadata publication under the same deadline.

Review found that retention writes freed dirty page images containing obsolete
recipient bodies. The next candidate clears only unused bytes on pages already
requiring writes, preserving undo, readers, reserved bytes and clean overflow
leaves. Its native red/green test reduced nonzero WAL bytes from 940,006 to 8,869
out of the same 1,062,992 bytes in the first case. All four body/cache cases pass,
including savepoint rollback, old-reader isolation, checkpoint/reopen and reuse.
This is a filesystem-compression hypothesis, not a measured throughput gain.
The fixed campaign workload and all qualification gates remain unchanged.

Validation: 41 native functional unit tests, ten native integration tests, six
public campaign tests (37.95 s), and four workload recovery tests passed. One
90 ms reader latency test failed twice in debug mode (106 / 112 ms); it remains
required in the release validation. The first broad object-log run also exposed
two unavailable live-S3 probes and a wall-clock-sensitive retry-saturation test.
The latter assumed 1,024 failures could enqueue before a 10 ms retry; paused
Tokio time now deterministically exercises the same full-queue assertion. All
72 local object-log tests then passed; the two live-S3 probes remain unverified.
The published vendor checksum and complete five-file patch roundtrip passed.

## Free-page result, concurrency control and bounded image planner

Clean `3f43c661` passed 42 native release unit tests, the free-page regression,
and six public campaign tests (14.55 s), but its complete capacity result was
**7,390.37 recipients/sec** over 406.164 s. CPU fell to 1.1620 ms/recipient;
process output was 13,196.37 bytes/recipient and peak RSS 9.875 GiB. Maximum
cycle walls were 85.750 / 132.368 / 184.244 s, load 6.603 / 36.805 / 45.673 s,
and purge 6.338 / 10.084 / 35.080 s. Progress p95 was 1.772 / 0.959 / 0.908 s.
Overall/later-cycle rates, 47 progress checks and 32 projection-size checks
failed. Correctness, due-time, RSS and WAL gates passed. No qualification.

Sampled host writes were 15.1647 GiB over 403.574 s (38.48 MiB/s), device busy
87.38%, mean write latency 58.43 ms. These exceed the prior best's 12.4189 GiB,
so the free-page experiment has not demonstrated physical-write savings. It is
removed; its rollback, reader, checkpoint/reopen and reuse regression remains,
including the bound against dirtying clean overflow pages. The earlier zeroing
assertion and red/green evidence remain in history, not as a current gate.

A same-binary one-worker/one-loader control completed only its first cycle,
maximum 117.656 s and progress p95 0.579 s. It was stopped with SIGTERM at
139.851 s because the cycle target already failed. No full-run rate is assigned.
It started on a warm device, so this does not isolate a concurrency effect.
Raw output, stop reason and device traces are archived as `campaign-3f43c661-*`.

The next planner candidate avoids building temporary eligibility, lease, client-key
and reporting indexes for independent unindexed addressed images. It uses the
existing per-record planner and checks replacement existence plus old/new index
key validity before returning commands. Gate changes, grouped/cohort rows, entity
documents, index fields, secondary indexes and selection operations retain the
full import-and-apply path. The temporary records never serve public queries.
A deterministic 1,024-case differential test compares responses and commands to
full import/plan/apply across state, lease/version/predicate failures, duplicate
and missing IDs, payload replacement, metadata, dry runs, snapshots and fallback
shapes. All 33 projection tests, 41 native functional unit tests, four native
free-page/recovery tests, six public campaign tests (37.76 s) and four workload
recovery tests pass. The debug reader latency test remains required in release.

File-attribute review also found a hardware-specific source of variability:
`3f43c661` ended with NOCOMPRESS (`m`) on nine databases and eight WALs, versus
zero databases and one WAL in the best `386d79f7` run. This is correlation, not
isolated causation. Btrfs can mark a whole file incompressible after a failed
compression attempt; the kernel's explicit compression property prevents setting
that sticky flag. The next controlled storage experiment sets `compression=zstd`
on a new project-private projection directory and checks inheritance/readback.
System mount settings and authoritative-log durability are unchanged. Sources:
[Btrfs compression documentation](https://btrfs.readthedocs.io/en/latest/Compression.html)
and [Linux v7.2 compression fallback](https://github.com/torvalds/linux/blob/v7.2/fs/btrfs/inode.c#L920).
The host runs 7.2.3-arch1-3; this source explains the hypothesis, which still
requires a measured control.

## Same-binary compression control

Clean `c8b0be7d` passed 42 native release unit tests (2.70 s), the free-page
history test and six public campaign tests (13.61 s). Its fixed executable
`33d0e21cd2eddd9b27848e70095cd52c26e540d5d3e76ad0cec5d35122f55749`
then ran two serial full campaigns with separate projection directories on the
same disk. The first used default attributes; the second inherited an explicit
`compression=zstd` property. No mount or log durability settings changed.

| Measurement | Default | Explicit zstd |
|---|---:|---:|
| Complete recipients/sec | 6,571.84 | 7,051.21 |
| Process wall, seconds | 456.715 | 425.638 |
| CPU-ms/recipient | 1.1675 | 1.1740 |
| Process output bytes/recipient | 13,470.61 | 13,288.66 |
| Sampled host writes, GiB | 15.4573 | 13.3137 |
| Mean sampled write MiB/sec | 34.82 | 32.21 |
| Device busy | 90.27% | 91.26% |
| Maximum cycle walls, seconds | 99.67 / 164.42 / 189.24 | 106.78 / 151.29 / 165.49 |
| Maximum progress p95, seconds | 1.646 / 0.833 / 0.674 | 1.111 / 1.111 / 0.981 |
| Final NOCOMPRESS DB/WAL files | 12 / 6 | 0 / 0 |

Explicit compression reduced sampled host writes 13.9% and increased complete
throughput 7.3% in this pair. It started with a warmer device: first load took
22.13 s versus 6.49 s, and measured device bandwidth differed. This is not an
isolated coefficient or repeatable qualification. All 64 explicit file-property
readbacks reported zstd. Both runs passed correctness and due-time checks but
failed throughput, progress and database stability; default also failed RSS
stability. Default had 52 failed checks, explicit 39. The planner change has
not yet demonstrated an end-to-end CPU or throughput gain. The best complete
result remains 7,718.50/sec on `386d79f7`; neither target is met.

The next candidate restores clearing unused bytes in already-dirty freed pages,
now with an explicitly compressed projection directory. The previous experiment
had mixed compression attributes, so that storage configuration did not establish
the combination's effect. The same native four-case history/reuse test and its
nonzero-byte assertion pass (7.41 s). No frames are omitted and clean overflow
pages stay clean. Release validation and unchanged full campaign gates follow.


## Explicit compression with cleared free pages; cross-queue log batching

Clean `18e9aa33` completed three million recipient lifecycles at **9,564.67/sec**
(314.012 seconds), using 32 stores, two campaigns/store, two workers and two
loaders/campaign. Binary SHA-256:
`d7a743d4f3a70d89b9f269d040ab90b95f20b6e1671fbac8efedd4c9ec6b9b31`.
The new private projection directory explicitly inherited `compression=zstd`;
all 64 database/WAL properties were read back. This configuration matters to the
result; Fireweed does not silently configure it. The log remains durable on the
normal filesystem. Release validation passed 42 native unit tests, the free-page
history/reuse test and six public campaign tests.

| Measurement | Value |
|---|---:|
| Maximum cycle wall, seconds | 83.671 / 109.659 / 116.843 |
| Maximum load, seconds | 6.591 / 30.076 / 35.206 |
| Maximum preparation, seconds | 32.400 / 38.056 / 42.765 |
| Maximum delivery, seconds | 33.542 / 32.837 / 30.949 |
| Maximum purge, seconds | 5.981 / 7.527 / 7.189 |
| Maximum progress p95, seconds | 1.687 / 1.201 / 1.279 |
| CPU-ms/recipient | 1.14346 |
| Process output bytes/recipient | 12,574.92 |
| Peak RSS, GiB | 10.486 |
| Sampled host writes, GiB | 10.7230 |
| Sampled device MiB/sec / busy | 35.254 / 81.22% |

Correctness, due-time, RSS and WAL gates passed. Overall throughput, cycles one
and two, 63 progress checks and 32 database stability checks failed. This is the
best completed measurement, **not qualification**. Against the previous explicit
compression run it combines free-page clearing with the same storage property;
one run does not establish a repeatable effect size.

A same-binary 64-store control used one worker/loader per campaign, preserving
128 total workers while halving per-store residency and doubling aggregate
checkpoint allowance. It was stopped after two complete cycles: maximum walls
99.301 and 124.585 seconds, progress p95 1.601 and 1.185 seconds. It was slower
than the 32-store candidate and already failed gates. Exit -15 after 271.879
seconds is an interrupted run, with no full-run rate or per-recipient cost.
All 128 DB/WAL compression properties were read back. Raw runner reports,
provenance, device samples, summaries and validation logs are archived under
`evidence/workflow-capacity/fireweed-campaign-18e9aa33-*` in the build evidence tree.

The next code change removes the store-wide produce mutex. Queue-specific
metadata permits still span epoch validation, durable append and high-water
publication. Independent queues can now share a LogEngine group commit. Packed
seals also submit independent groups concurrently. Stress testing exposed a
pre-existing phase-map collision between overlapping seals with the same
queue/epoch/lane: the older seal could remove the newer seal's phase. Each waiter
now owns its append phase, preventing false before-position rejection and
incorrect retry classification after a dropped result channel. No log format,
durability boundary or retry gate changes.

A regression demonstrates that independent queues enter one unsealed buffer and
publish in one durable manifest while a same-queue epoch change waits. The old
mutex fails the regression. A filesystem stress test mixes all three append paths
across four queues and four workers each, checks 768 contiguous unique positions,
reopens from the log and verifies continuation without offset reuse. A separate
regression isolates dropped-waiter disposition across repeated logical keys.
End-to-end throughput must be remeasured on a clean build before claiming benefit.

Local validation passed 75 object-log unit tests (two live-S3 probes excluded),
six public campaign tests and four log-only recovery tests. One preceding full
unit run hit the existing single-queue large-batch reopen test's 30-second produce
timeout; its isolated rerun and the subsequent full suite passed. The failure is
archived as a transient observation, not claimed fixed by the cross-queue change.


## Cross-queue flush measurement and allocation reduction

Clean `e1ee74b2`, binary
`de47bc595329bf1ca3bd81357dce305b95a76d9adb1a3c0b5d2b7406c134c96a`,
passed 42 native release tests (2.50 s), free-page history/reuse (0.38 s) and six
public campaign tests (14.53 s). The unchanged 32-store explicit-zstd campaign
completed at **9,313.74/sec**, below the 9,564.67 best. Wall time was 322.594 s;
CPU cost 1.11226 ms/recipient; process output 11,876.64 bytes/recipient; peak RSS
10.327 GiB. Sampled host writes were 10.2872 GiB at 32.92 MiB/sec, 82.61% busy.
All 64 DB/WAL properties read back zstd. No diagnostic tracing was enabled.

| Cycle | Maximum wall s | Load s | Preparation s | Delivery s | Purge s | Progress p95 s |
|---|---:|---:|---:|---:|---:|---:|
| 0 | 86.106 | 6.216 | 38.109 | 30.657 | 6.070 | 1.816 |
| 1 | 111.931 | 37.995 | 38.587 | 27.950 | 6.765 | 1.594 |
| 2 | 120.669 | 41.679 | 38.467 | 33.313 | 6.816 | 1.454 |

Correctness, due-time, RSS and WAL passed; overall/late-cycle throughput,
105 progress checks and 32 DB stability checks failed. CPU cost fell 2.7% and
sampled host writes fell 4.1% versus the preceding run, but lower delivered
bandwidth offset those savings. This is not evidence of a throughput improvement.
The per-waiter phase correction remains required for safe append disposition.

The next candidate removes three avoidable command-tree clones in packed append:
move each waiter's commands into the sealed batch, borrow that batch for durable
encoding, then move it into the leader's projection publication. Byte accounting
uses the codec's exact size serializer instead of allocating encoded buffers.
Native metadata serialization borrows strings/maps/arrays rather than constructing
an owned recursive wire tree. Framed encoding writes into one vector. Native tag
numbers, framing bytes, human-readable JSON, durability and byte limits stay the
same. Compatibility tests compare nested metadata against the old owned wire
form, and complete envelopes/batches against the old framing algorithm.

Allocation-candidate validation passed 30 core and 275 engine unit tests, 75 local
object-log tests, six public campaign tests (37.96 s) and four recovery tests
(2.30 s). The exact-size helper uses Postcard 1.1.3's size serializer; it still
traverses the value and propagates serialization errors. This removes temporary
output and metadata trees, not validation or debt accounting. Release validation
and a fresh full-capacity measurement are required before claiming a speedup.


## Copy reduction: overall 10k crossed, qualification still fails

Clean `3d2cb57e`, executable
`9e07a58c4e90a851d2132f5785ae4188cd6217645075a3058cde0df70bb802e4`,
passed 42 native release unit tests (2.77 s), free-page history/reuse (0.37 s)
and six public campaign tests (14.58 s). The unchanged three-cycle million-resident
explicit-zstd campaign completed at **10,109.38 recipients/sec** in 297.045 s.
This is the first overall 10k pass for the representative campaign, **not a
qualified target achievement**. Late-cycle rate, 65 progress and 32 database
stability checks still fail. Correctness, due-time, RSS and WAL checks pass.

| Measurement | Value |
|---|---:|
| Maximum cycle wall, seconds | 79.018 / 103.996 / 110.746 |
| Maximum load, seconds | 9.978 / 30.266 / 30.404 |
| Maximum preparation, seconds | 30.287 / 38.384 / 42.802 |
| Maximum delivery, seconds | 30.628 / 27.932 / 28.985 |
| Maximum purge, seconds | 7.208 / 6.843 / 8.105 |
| Maximum progress p95, seconds | 1.457 / 1.136 / 1.440 |
| CPU-ms/recipient | 1.04485 |
| Process output bytes/recipient | 11,607.58 |
| Peak RSS, GiB | 8.946 |
| Sampled host writes, GiB | 10.5129 |
| Sampled device MiB/sec / busy | 36.527 / 82.74% |

CPU cost fell 6.1% from the preceding candidate and full-run throughput rose 8.5%
in this pair. Device bandwidth also rose, so the rate gain is not an isolated CPU
coefficient. All 64 DB/WAL files retained explicit zstd. The log remains the sole
durability source and its bytes remain approximately 1,823.51 per recipient.
Artifacts use the `fireweed-campaign-3d2cb57e-zstd-*` prefix.

Next, retry the 448 MiB automatic checkpoint window on this corrected, explicitly
compressed configuration, retaining the 512 MiB/store WAL gate. The previous
448 MiB attempt was confounded by different free-page/compression behavior and
failed before full measurement. This candidate changes only the byte threshold:
114,688 frames at 4 KiB, 229,376 at 2 KiB. It retains NORMAL accounting, the
log-backed sync adapter, and the standalone 1,000-frame policy. Coalescing may
reduce intermediate main-database writes, but may also lengthen checkpoint pauses
or violate the WAL bound; the full unchanged gates determine acceptance.

The actual-page-size configuration regression passes for new 4 KiB files,
existing 2 KiB files and the unchanged standalone policy (0.29 s). Release
correctness/recovery validation and capacity measurement follow on clean HEAD.


## Wider checkpoint window and same-binary runtime control

Clean `f99aa404`, executable
`e5e061cb4a52d47c0537e827e2dfc67a1777f2827482679caadab1965aa925a5`,
passed 42 native release tests (2.53 s), free-page history/reuse (0.37 s) and six
public campaign tests (13.56 s). The 448 MiB candidate completed at 10,337.24/sec.
A serial control used the same binary and all workload/storage settings, with
`OBJECT_LOG_FLUSH_RUNTIME_THREADS=1` instead of the default eight per store. All
75 local object-log tests passed under that setting (1.24 s). The control reached
**10,850.17/sec**, the new best overall result. Neither run is qualified.

| Measurement | Default runtime | One runtime worker/store |
|---|---:|---:|
| Complete recipients/sec | 10,337.24 | 10,850.17 |
| Process wall s | 290.688 | 276.868 |
| CPU-ms/recipient | 1.06627 | 1.03862 |
| Process output bytes/recipient | 11,037.17 | 10,914.00 |
| Peak RSS GiB | 10.620 | 10.301 |
| Sampled host writes GiB | 9.3412 | 9.3824 |
| Sampled device MiB/sec | 33.21 | 35.08 |
| Maximum cycle walls s | 83.538 / 99.911 / 103.375 | 74.699 / 96.586 / 102.399 |
| Maximum load s | 6.382 / 24.072 / 26.457 | 6.133 / 22.564 / 28.427 |
| Maximum preparation s | 36.873 / 34.570 / 36.738 | 31.704 / 33.958 / 38.066 |
| Maximum delivery s | 29.669 / 26.974 / 32.934 | 26.617 / 28.742 / 28.097 |
| Maximum purge s | 6.317 / 12.852 / 6.680 | 6.178 / 10.366 / 7.260 |
| Maximum progress p95 s | 1.784 / 1.426 / 1.300 | 1.950 / 1.257 / 1.435 |

Both passed correctness, due-time, RSS and WAL gates. Both failed the final
cycle's rate, 32 database stability checks and progress checks (84 default,
90 control). The runtime control removes 224 configured async workers across
32 stores while retaining eight in-flight flush slots and the same log durability.
Its 5.0% rate gain is a single-pair observation, not a repeatable isolated effect.
All 64 DB/WAL compression properties in each run read back zstd. Runtime-setting
provenance is archived with the control; the runner now records it directly too.
Artifacts use `fireweed-campaign-f99aa404-zstd-*` and `fireweed-campaign-f99aa404-rt1-*`.

The preceding 250 MiB run helps interpret DB stability: all main DB files were
4 KiB after cycle zero, while current data remained in WAL. After checkpointing,
main files ranged 59.81–60.40 MB in cycle one and 59.96–60.57 MB in cycle two.
The first size jump is initial file materialization, not evidence of unbounded
growth. Keep the existing last-three-cycle stability gate and extend a promising
candidate to six cycles to establish the plateau; all cycle rates still count.

The next measurement adds phase attribution to the existing public progress
observer. It records start/end phase (including transitions), latency and API
attempt counts, while preserving the observer cadence, retry behavior and global
p95 gate. This distinguishes load, preparation, delivery, final verification and
retention stalls without implementation hooks or weaker read consistency. All
six public campaign tests pass (36.71 s), including accounting for every observed
read exactly once in the phase report. This reporting-only candidate should first
run a one-cycle million-resident diagnostic to localize the failures; that short
run cannot qualify either target.


## Phase diagnostics and bounded async-debt experiment

Clean `3f297574`, executable
`7a87884c4d7616b7e7d0ba4ccc769bbe3eb8684386bb52b00d42bf4634ca1679`,
passed 42 native release tests (2.73 s), free-page history/reuse (0.38 s) and six
public campaign tests (13.64 s). Two serial **one-cycle diagnostics** used one
million resident recipients, explicit zstd, the 448 MiB checkpoint window and
one log runtime worker/store. These short runs cannot qualify either target.

| Diagnostic | Batch 1,000 | Batch 500 |
|---|---:|---:|
| Complete recipients/sec, one cycle only | 12,528.78 | 10,309.54 |
| Process wall s | 80.104 | 97.241 |
| Maximum campaign wall s | 78.456 | 95.710 |
| CPU-ms/recipient | 1.00862 | 0.97407 |
| Maximum preparation s | 34.289 | 48.300 |
| Maximum global progress p95 s | 2.371 | 0.628 |
| Mean load progress latency s | 1.676 | 0.128 |
| Mean preparation progress latency s | 0.291 | 0.073 |
| Mean delivery progress latency s | 0.158 | 0.070 |
| Mean purge progress latency s | 0.570 | 0.531 |

Batch 1,000 loading reads reached 8.025 seconds. Only four extra API attempts
occurred among 234 loading reads; preparation and delivery reads never retried.
Final verification progress reads were below a millisecond in that run. This
points to projection coverage lag, rather than expensive metrics SQL or a retry
storm. Batch 500 passed every campaign's existing progress check in its one cycle,
but lost 17.7% throughput and substantially slowed preparation. It is not yet a
sustained candidate. Phase summaries and all raw evidence use the
`fireweed-campaign-3f297574-phase-b1000-*` and `...-b500-*` prefixes.

The workload had used the generic `AsyncProjectionSpec::default()`: up to
512 MiB unapplied encoded bytes per queue, 100,000 unapplied commands, queue depth
1,024 and a 60-second oldest-unapplied admission threshold. Those are resource
bounds, not a one-second projection visibility guarantee. Large accepted ingestion
bursts can consequently leave linearizable progress reads waiting for a long tail.

The CLI now exposes `--apply-debt-bytes` for campaign runs and records the chosen
value. It forwards to the existing public async policy; library defaults and read
consistency remain unchanged. The next diagnostic tests **2 MiB** at batch 1,000,
with all work and other limits unchanged. The bound must fit individual encoded
commands; this experiment is for the declared approximately 1 KiB record fixture,
not an arbitrary large-payload recommendation. Before-position backpressure uses
the existing public retry path and remains inside measured wall time.

Six public campaign tests passed with a 2 MiB override (36.41 s). The existing
two-mode, two-cycle test then passed with a tighter 96 KiB budget (12.78 s),
verifying retained metadata, retries/dispositions, reporting and discovered purge.
This checks the existing admission policy through the same public workflow API.

All workload targets also pass `cargo check --locked -p fireweed-workload --all-targets`.


## Debt diagnostic and priority-model correction

Clean `7af493c8`, executable
`c5a9a7c087778f9fc4a85b55301a77bb14acb49a92869e0a463cf0cc0ff492dd`,
passed 42 native release tests (2.58 s), free-page history/reuse (0.38 s) and six
public campaign tests (14.54 s). Its one-cycle, 2 MiB debt diagnostic completed
at 11,615.76/sec, CPU 1.12725 ms/recipient, worst campaign wall 85.082 s and
progress p95 2.219 s. Mean loading-read latency fell to 1.158 s but still reached
4.483 s; preparation, delivery and purge means were 0.353 / 0.174 / 0.660 s.
The smaller budget did not solve reporting and increased CPU cost. Do not adopt
it as a qualified latency policy. This remains a one-cycle diagnostic, archived
under `fireweed-campaign-7af493c8-debt2m-*`.

A more fundamental fixture issue emerged when rechecking actual source behavior.
Snorri revision `c11dc2b07ba7c18bce97fb1c15190c9460a9f17a`,
`crates/snorri-fireweed/src/lib.rs:14109`, uses a timestamp priority equal to
`not_before`, or timestamp 1 for immediately available work. Its comment explicitly
requires unscheduled work to sort ahead of scheduled work. The workflow-item path
at line 15236 uses `available_at` for both priority and `not_before`. The legacy
7thsense scheduled-actions query filters `scheduledTimestamp <= asOf` and orders
by that same timestamp (`QuillScheduledActionsPersistence.scala:70–73`). Source
excerpts, revisions and file hashes are preserved in
`campaign-priority-source-review.json` in the evidence directory.

Our existing fixture instead placed FIFO integer ordinals 0…999,999 and virtual
scheduled seconds 1,000…1,180 in the same integer priority domain. At large list
sizes, future scheduled rows therefore formed a prefix ahead of unenriched rows.
The 448-row correctness fixture did not have that rank inversion; the larger
2,240-row chunk test and capacity runs did. This is a useful generic priority-queue
stress case, but it is not Snorri's availability ordering and must not silently
stand in for that workflow's capacity.

`--campaign-timestamp-priority` now selects a timestamp queue and an explicit
`availability_timestamp` report label. Unscheduled priorities start at timestamp
1 and increment by one nanosecond per ordinal, preserving FIFO ahead of future
windows. Scheduling replaces priority with the persisted chosen timestamp and
sets the matching eligibility time. The virtual calendar offsets, records, body
variation, handler limits, retries, reporting, exports and purge are unchanged.
Without the flag, `mixed_sequence_stress` preserves the old priority values and
all previous reproduction commands. The gate accepts those named variants and
rejects unknown priority labels. Neither variant has yet qualified.

The canonical workflow target is now evaluated with the source-aligned timestamp
mode. This is a corrected workload baseline, not a claimed backend speedup over
the old priority mixture. Both still perform approximately 8.105 logical row
operations per recipient, but encoded bytes, index costs and scanning differ;
CPU/byte coefficients must be measured again. The existing stress fixture remains
available for generic future-prefix performance investigation and correctness.

Validation covers both priority models crossed with metadata/payload enrichment,
including two-cycle outcomes under a 96 KiB async budget. The public campaign
suite passed (48.94 s), and four workload recovery tests passed (2.48 s). The
expanded child-exit/log-only rebuild test passed all four mode combinations
(11.10 s), checking retained priority type and values as well as outcomes. A new
order regression checks FIFO ordinals through one billion remain before the first
scheduled timestamp; the old stress values remain exact. Claims now reject a
missing or wrong priority type instead of silently omitting their order check.
Five qualification-gate tests also pass. Fresh release validation and separate
million-resident timestamp diagnostics follow before any qualification claim.

## Source-aligned sustained baselines: `60c699e5`

Both clean serial runs use the same release binary
`0a46d2c734c8cca5597e136e84204f13763068fe618bd077a12f26076f471c7f`,
`--campaign-timestamp-priority`, one million resident recipients, six cycles,
32 stores, two campaigns/store, two workers and two loaders/campaign, original
bodies with metadata enrichment, 500/200/500 handler limits and 8,000-row purge.
Each new private projection directory explicitly uses zstd; all 64 DB/WAL
properties per run read back zstd. Log runtime workers/store are explicitly one.
Checkpoint budget remains 448 MiB and async debt remains the default 512 MiB.
Release validation passed: 42 native tests (2.68 s), free-page regression (0.37 s),
workload ordering test and six public campaign tests (23.99 s).

| Measurement | Batch 500 | Batch 1,000 |
|---|---:|---:|
| Complete recipients/sec | 7,271.46 | 8,329.69 |
| Process wall, seconds | 825.587 | 720.774 |
| CPU milliseconds/recipient | 1.05449 | 1.06787 |
| Mean charged CPU occupancy | 7.664 | 8.889 |
| Process output bytes/recipient | 13,141.82 | 12,336.45 |
| Retained logical log bytes/recipient | 1,835.80 | 1,834.33 |
| Peak RSS, GiB | 9.079 | 9.678 |
| Sampled host writes, GiB | 26.877 | 22.871 |
| Host write MiB/sec | 33.434 | 32.613 |
| Device busy | 86.85% | 88.12% |
| Mean write request milliseconds | 59.19 | 72.19 |
| Worst campaign walls, cycles 0–5, seconds | 90.74 / 140.48 / 127.85 / 156.12 / 170.17 / 133.89 | 96.89 / 122.07 / 102.60 / 145.67 / 113.68 / 132.79 |
| Maximum campaign progress p95, cycles 0–5, seconds | 1.169 / .357 / .495 / .456 / .474 / .410 | 1.407 / 1.313 / 1.432 / 1.422 / 1.372 / 1.615 |

Both failed overall throughput and every cycle-rate gate after cycle zero.
Batch 500 additionally failed five first-cycle progress checks and RSS stability;
batch 1,000 failed 92 progress checks and passed RSS stability. Both passed
independent outcomes, due-time and sampled WAL bounds. All 32 database-size
stability gates passed: initial main-file materialization was followed by a
plateau, unlike the misleading three-cycle startup comparison. Sampled device
counters include other host traffic and omit startup/tail; they are not NAND
write amplification. These are separate timestamp baselines, not speedups over
historical mixed-priority runs. The 14.6% batch-size rate difference is a single
serial comparison, not a repeatable qualification claim.

Artifacts use `fireweed-campaign-60c699e5-timestamp-b{500,1000}-six-*` in the
[evidence directory](../helix/04-build/evidence/workflow-capacity/), including raw
runner reports, device samples, summaries, provenance and property readbacks.
Next: eliminate command copies during deferred apply selection and metadata
serialization; independently test 2 KiB new-file projection pages to reduce
page-level write amplification. Keep existing-file compatibility and every gate.

## Avoid copies when deferring apply

The apply selector previously materialized owned command vectors before deciding
whether to defer a claim. Notifications could repeat that copying during the
join window. It now builds a borrowed plan under the same state lock and copies
commands only for the selected apply. The retained batches still own retry data;
FIFO, contiguous-position, byte/item caps, coverage preemption and join deadlines
are unchanged. Relational metadata serialization now borrows its map directly,
using the existing identical map serializer rather than cloning `into_inner()`.

All 75 local object-log tests and five relational tests passed (1.25 s / <.01 s);
two live-S3 tests remain excluded without their service. Six public campaign
tests passed (54.83 s), as did four recovery tests (2.50 s). Logs are archived as
`fireweed-borrowed-apply-*`. No throughput improvement is claimed before a fresh
release measurement.

## Candidate: smaller new-file projection pages

New log-backed projections now request 2 KiB pages, testing whether smaller dirty
page images reduce addressed-update write amplification. Existing 2 KiB and
4 KiB files keep their page sizes; standalone projections still default to 4 KiB.
The checkpoint budget stays 448 MiB by reading the actual page size, and NORMAL
backfill accounting, log durability, cache byte caps and 512 MiB WAL gate remain
unchanged. This is an unqualified candidate, not an established improvement.
The next full run includes the preceding allocation refactor; its total difference
from `60c699e5` cannot isolate the CPU contribution of either change.

All 42 native tests passed (19.94 s). The free-page regression now crosses
2/4 KiB pages with 32 MiB/64 KiB caches and 900/5,000-byte bodies; all eight cases
passed (15.61 s), preserving rollback, concurrent-reader history and reopen/reuse.
Six public campaign tests passed (59.42 s), plus four recovery tests (2.50 s).
The configuration regression checks both existing page sizes, new-file settings
and the unchanged standalone policy. Logs are archived as `fireweed-2k-pages-*`.
Release validation and six-cycle capacity measurement follow on a clean revision.

## Completed 2 KiB experiment and worker-count control: `1fe89a44`

Both runs used binary `bb7ee7df61f0b3306bbe22fd8bfe696ab648522e295a6041773f80029ff9d080`,
six million complete lifecycles, one million resident recipients, 32 stores and
two campaigns/store, timestamp priorities, metadata enrichment, 1,000-row storage
batches, two loaders/campaign and one log runtime worker/store. Actual DB headers
confirmed 2,048-byte pages; all 64 DB/WAL properties per run read back zstd.

| Measurement | Two workers/campaign | One worker/campaign |
|---|---:|---:|
| Complete recipients/sec | 8,306.86 | 7,911.78 |
| Process wall, seconds | 722.606 | 758.574 |
| CPU milliseconds/recipient | 1.13585 | 1.08231 |
| Mean charged CPU occupancy | 9.431 | 8.561 |
| Process output bytes/recipient | 12,031.18 | 12,490.13 |
| Retained logical log bytes/recipient | 1,834.32 | 1,834.83 |
| Peak RSS, GiB | 10.225 | 9.717 |
| Sampled host writes, GiB | 22.705 | 26.226 |
| Host write MiB/sec | 32.295 | 35.496 |
| Device busy | 86.58% | 85.61% |
| Mean write request milliseconds | 64.22 | 50.22 |
| Worst campaign walls, cycles 0–5, seconds | 81.22 / 102.91 / 120.55 / 160.55 / 105.18 / 145.24 | 94.65 / 122.89 / 116.69 / 154.14 / 116.26 / 146.36 |
| Max campaign progress p95, cycles 0–5, seconds | 2.077 / 1.388 / 1.301 / 1.338 / 1.398 / 1.426 | .985 / .667 / .931 / .614 / .827 / .676 |
| Failed progress checks | 154 | 0 |

Both failed overall and cycles 1–5 throughput. Both passed correctness, due-time,
RSS, all database stability and WAL bounds. The one-worker control passed every
non-throughput check. It reduced CPU cost about 4.7% and reporting latency, but
increased host writes about 15.5%, and completed about 4.8% slower. This is one
serial control, not repeated qualification. Raw artifacts use
`fireweed-campaign-1fe89a44-timestamp-b1000-{,w1-}six*` in the evidence directory.

Compared with the preceding 4 KiB two-worker baseline, the 2 KiB plus allocation
candidate was 0.3% slower, used 6.4% more CPU/recipient and reduced host writes
only 0.7%. The data do not justify that page-size default. Restore new files to
4 KiB, keep both existing-file sizes supported and retain all eight free-page
regression cases. The allocation refactor still avoids unnecessary copies, but
its isolated throughput contribution has not been established.

Release validation for `1fe89a44` passed 42 native tests (2.68 s), the expanded
free-page regression (0.80 s), the ordering test and six campaign tests (24.15 s).

## Primitive control and body-distribution alignment

A clean million-row `1fe89a44` primitive run passed its component floors:
103,948.61 inserts/sec, 107,199.60 key-addressed updates/sec and 91,641.77
ID-addressed updates/sec. Claim/complete measured 25,784.24/sec and purge
37,874.96/sec. Process wall was 76.53 s; phase windows overlap across independent
stores and must not be added. It uses the older highly compressible repeated
padding, one claim/complete pass and producer-returned addresses. These are valid
component measurements, not canonical campaign throughput or byte coefficients.
Artifacts are `fireweed-1fe89a44-primitives*`.

`--primitive-varied-payload` now uses the campaign's deterministic varied JSON
bodies and adds an enrichment revision on the first addressed update while
preserving identity and padding; the second update keeps the body. The old padded
control remains available. Reports explicitly label both models and count actual
initial/replacement payload bytes. Component semantics remain deliberately distinct
from the campaign's three claimed stages and primary metadata-keep mode. Final
component floor validation should use the varied-body option as well.

## Host discard hypothesis: approval pending

Read-only checks found a Kingston OM8PCP3512F-AB NVMe with discard support, but its
`root` encrypted mapping reports zero discard granularity/maximum. The root Btrfs
mount has no discard option, and `fstrim.timer` is disabled/inactive. This means
filesystem-free space is not automatically communicated through that mapping to
the SSD. The kernel documents default discard blocking and allocation-information
leakage when enabled; Kingston documents the role of TRIM in garbage collection.
This is a plausible contributor to sustained write performance, not a measured
causal explanation or a firmware diagnosis. See the
[reviewed control procedure](storage-trim-control.md).

A clean detached `1fe89a44` checkout and identical binary are frozen for a
same-binary after-maintenance control. The helper preserves existing known flags,
refuses unexpected state, temporarily allows discard, trims filesystem-free
extents, and restores flags even on failure. Six mocked safety/control-flow tests
pass. No root-device setting, trim operation, boot file or timer has been changed.
Explicit approval was requested because this is a host encryption-policy choice,
not an ordinary repository edit. Goal status remains active and unmet.

The restored 4 KiB configuration regression passes (0.39 s), retaining checks for
both existing page sizes. The new primitive body/order unit tests pass, and two
CLI tests pass (2.85 s), crossing disk/memory with both body models and rejecting
the flag outside the primitive profile. Six gate tests pass (7.98 s), including
unknown body labels, undersized varied input and inconsistent byte totals. The
primitive report also records its actual single sequential loop/store; generic
workflow worker flags do not alter component-phase concurrency. Release validation
and a million-row varied-body component measurement remain to be run.

## Varied-body primitive qualification pair: `fda0dcab`

Two clean serial million-row component runs on binary
`2d2a99012f08141d40ad2c8fde3f68948f71db93e3e62ce88e4385ab47c8038f`
passed the three 10k component floors with `--primitive-varied-payload`,
1,000-row batches, 32 stores and one sequential batch loop/store. Actual headers
confirmed 4 KiB pages, and all 64 DB/WAL properties/run read back zstd. Log runtime
workers/store remain explicitly one. Neither run included a host TRIM operation.

| Measurement | Run 1 | Run 2 |
|---|---:|---:|
| Inserts/sec | 113,776.51 | 29,570.63 |
| Key-addressed updates/sec | 29,339.61 | 27,405.31 |
| ID-addressed updates/sec | 44,187.24 | 49,229.72 |
| Claim/complete rows/sec | 32,049.20 | 31,014.91 |
| Purge rows/sec | 62,711.65 | 69,884.20 |
| Process wall, seconds | 96.084 | 131.222 |
| Total CPU seconds | 555.791 | 566.520 |
| Process output bytes | 12,419,059,712 | 12,517,896,192 |
| Peak RSS, GiB | 5.987 | 5.169 |
| Sampled host writes, GiB | 3.984 | 4.274 |
| Host write MiB/sec | 43.792 | 33.823 |
| Device busy | 90.26% | 95.61% |

Each run inserted exactly 934,888,890 body bytes and replaced them with
959,888,890 bytes: 934.889 / 959.889 bytes per row. These inputs now share the
campaign's initial body distribution. This remains a component ladder with one
claim/complete pass and an explicit body replacement, rather than the campaign's
three claimed stages and primary metadata-keep path. Phase windows overlap
across stores; do not add them or turn component rates into campaign throughput.
The second run took 36.6% longer with only 1.9% more CPU time, while sampled host
bandwidth fell 22.8%. This reinforces the need to separate media-state effects
from code effects. The first run also exceeded the old sequential reference's
bandwidth, directly showing why 38.23 MiB/s must not be called a hardware ceiling.

Artifacts use `fireweed-fda0dcab-varied-primitives-{1,2}*`. Release validation
passed: 42 native tests (2.67 s), free-page coverage (0.79 s), three Turso recovery
tests (0.10 s), two workload unit tests, six campaign tests (23.98 s), two primitive
CLI tests (1.32 s), and four workload recovery tests (1.05 s).

The maintenance helper additionally requires Python isolated mode before loading
non-builtin modules; all eight mocked/helper-invocation tests pass. Its updated
reviewed source and hash are in the maintenance document. The requested host
approval is still pending. The component milestone is demonstrated with varied
bodies, but the 10k and 12.5k complete-campaign objectives remain unmet.

## Follow-up: unchanged-index shortcut review

Source review at `67cc79a4` rejected a proposed shortcut before implementation:
skipping the pending-index rewrite for a fused claim plus first enrichment.
Although this stage preserves FIFO priority and `not_before=1`, returning the
claimed row to Pending changes `eligible_since`. In cycle zero, ingestion stores
1 and the mutation stores `max(not_before, evaluated_at)=940`. The covering
pending index includes `eligible_since`, so its old and new keys differ.

The relevant paths are `insert_item_specs` in relational `apply.rs`, mutation
planning in projection `lib.rs`, and
`fireweed_items_pending_eligible_order_idx` in relational `schema.rs`. Turso's
`translate/update.rs::collect_indexes_to_update` selects indexes from assigned
columns and partial-predicate dependencies; `translate/emitter/update.rs`
evaluates old/new predicate membership and deletes/inserts the applicable keys.
There is no general equal-key elimination there, but that observation alone
does not establish redundant index I/O for this campaign stage. The next
scheduling stage also changes priority and eligibility. Omitting these updates
would change persisted semantics; no such optimization was made and no speedup
is attributed to this review.

No benchmark or host mutation ran during this follow-up. The prepared identical-
binary storage control remains pending the previously requested root-device
maintenance approval; elapsed time or automatic goal continuation is not consent.

### First-cycle membership trace and genesis follow-up

The same `cd5db494` release binary (SHA-256
`f1b0db63ddd7af1bb64e5ec41d45e5161f7aab76316193ca255ecd3c9b7e4c81`),
run from clean `a17af0fc` with two workers and metrics tracing, completed one
million-recipient cycle at 12,928.60/sec. This is diagnostic only: one cycle,
tracing enabled, and reporting p95 reached 1.184 seconds. It does not meet the
repeatability or reporting gates.

The 4,213 reporting reads included 1,251 coverage fallbacks. Of 133 reads over
one second, 129 were dominated by coverage waits. The 290 successful membership
reads had total p95 743.955 ms, of which membership SQL p95 was 710.361 ms;
returning thousands of addressed identities is not negligible under load.
Coverage fallback phase timings show no membership SQL executed. They do not
by themselves distinguish initial cursors, unsupported tails, identity limits,
or pruning. Source inspection establishes that an initial `None` applied cursor
could not attempt the membership path at all.

The next change permits that initial tail only for epoch zero, contiguous
retained positions starting at zero, and an actual SQL cursor row whose epoch
matches. Missing cursors, foreign epochs, gaps and conflicts still use the
coverage barrier. Physical row reads remain coverage barriers; the public test
pauses apply before the first push and verifies these separate contracts.
Evidence is archived under `fireweed-campaign-a17af0fc-membership-trace-w2-one*`
and `fireweed-membership-trace-v2-summary.json` in the workflow-capacity evidence
directory. No device or host settings were changed.

The genesis change passed 279 release library checks (150 Fireweed, 77
object-log, 52 native Turso), including the paused initial-push test. One
existing direct object-log commit test remains ignored; two live S3 endpoint
tests are explicitly filtered because this environment has no configured
endpoint. The four focused membership checks also passed. Logs are archived as
`fireweed-genesis-membership-tests.log` and `fireweed-genesis-library-tests.log`.
No sustained performance improvement is claimed before the next full run.

### Short checkpoint candidate validation

The 4 MiB candidate passed all 304 release checks, including native WAL reuse,
recovery from the authoritative log, public campaigns and component workflows.
The same one existing ignored test and two unconfigured live-S3 tests remain
excluded as documented above. Release validation also built the workload CLI;
that exact executable is used for the initial one-cycle diagnostic, with its
SHA-256, build command, reproducibility seed and modified source blob recorded
in `fireweed-checkpoint4m-build-provenance.json`. The diagnostic is not a full
qualification. Sustained improvement still requires clean six-cycle repeats.
