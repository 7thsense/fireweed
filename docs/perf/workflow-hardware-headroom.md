# Workflow capacity versus hardware cost

## Current maintenance capacity status (2026-09-18 UTC)

**The current maintenance source is not campaign-qualified.** On clean S2
`654e4a175aaaf420d88fbdec5794687b72442ea0`, normal release binary
`8071feb98f8ce9c951be891b77d87deaff25f32a06a2f6eac7fdc99287b6c573`, the serial
C1/P1/C2/P2 attempt passed both million-row primitive repeats (29/29 checks each).
The slower repeat delivered 22,616 insert, 21,549 enrichment, 40,536 scheduling,
13,036 claim-and-complete and 29,509 purge rows/sec. These are individual
operation rates; they are not a complete campaign recipient rate.

Both campaigns failed during cycle seven after six complete million-recipient
cycles, with an ambiguous log position caused by a produce timeout. They emitted
no final correctness result. Do not divide their elapsed time, CPU or device
bytes by the configured eight million recipients: **current full-campaign
throughput and per-recipient resource cost are unavailable**. The historical
`49f1b6b` repeats did qualify at 12.5k, but that result does not qualify S2.
[The disk baseline](disk-baseline-and-napkin-math.md) records all four outcomes
and the fresh calibration.

For any completed workload, the conditional resource arithmetic is:

- CPU demand in CPU-seconds/sec = target work units/sec × CPU-ms/work unit / 1000.
- Host write demand in MiB/sec = target work units/sec × sampled host bytes/work unit / 2^20.

| Measured cost basis | CPU-ms/unit | Host bytes/unit | CPU demand at 10k / 12.5k units/sec | Host MiB/sec at 10k / 12.5k units/sec |
| --- | ---: | ---: | ---: | ---: |
| Historical qualified campaign, one complete recipient | 0.81836–0.85866 | about 3,964–3,978 | 8.18–8.59 / 10.23–10.73 | 37.80–37.94 / 47.25–47.42 |
| S2 primitives, one input row through all five phases | 0.56167–0.57698 | 6,827–7,789 | 5.62–5.77 / 7.02–7.21 | 65.11–74.28 / 81.38–92.85 |
| S2 full campaign | Unavailable: both attempts failed | Unavailable | Unavailable | Unavailable |

The primitive budget covers all five phases per input row, including varied
payload bodies; it neither represents campaign metadata-only updates nor
attributes cost to any one primitive. Passing each 10k operation floor does not
establish that all five phases can continuously process 10k complete rows/sec.
These extrapolations hold measured cost constant and predict neither tail
latency nor completion under load.

Fresh 8 GiB `dd` observations including final sync measured **75.84 MiB/sec direct
NoCOW** and **50.55 MiB/sec buffered**; host-device counters measured 76.51 and
51.30 MiB/sec respectively. The historical stretch write budget is 92.11–92.44%
of that buffered host-device observation. Finite sequential transfers and mixed
queue/log I/O differ, so this ratio establishes neither remaining capacity nor
an SSD ceiling. C1/C2 themselves averaged 8.46/8.17 process CPU-seconds/sec and
43.12/40.52 host-device MiB/sec over the failed attempts, with write-request
means 77.90/83.89 ms. Those are observations, not timeout attribution.

Keep the unchanged 10k complete-recipient target, 12.5k stretch target and all
correctness/reporting/retention gates. The next code investigation must explain
the repeatable log timeout and complete both eight-cycle campaigns. The
[maintenance baseline](maintenance-release-baseline.md) records the completed
same-binary trace and disk/RAM/disk diagnostic. At two-cycle costs, 12.5k complete
recipients/sec would demand 10.68 / 10.60 / 11.74 CPU-seconds/sec and
37.48 / 16.83 / 35.19 host MiB/sec respectively. None of those arms reached
materialized-main checkpointing, so these remain short-run conditional budgets.
The full campaign costs remain unavailable. Source-preview artifacts use the
scanner-only descendant S3; runtime measurements remain attributed to S2.

The [final verification manifest](evidence/maintenance-verification-final-v0.31.28/manifest.json)
preserves the four campaign/primitive reports, fresh disk calibration, trace,
placement attempts and their original gates. Host-device counters are host-wide,
exclude sampling gaps at the start/end, and are not NAND-internal writes.

**Historical baseline, 2026-09-15:** blocked discard through LUKS was repaired;
8 GiB direct/buffered writes then measured 900/738 MiB/sec. The unchanged
Fireweed CLI completed its first fully qualifying eight-cycle run at 15,064
recipients/sec (slowest cycle 14,342/sec). See
[disk baseline and napkin math](disk-baseline-and-napkin-math.md) for the
triage procedure, measured resource budget and remaining repetition gates.
The sections below retain historical observations, not current diagnoses or
hardware ceilings. The separate Btrfs kernel defect was fixed by upgrading
to 7.2.6 before the discard repair.

## 2026-09-15: local recheck; another host is not a prerequisite

The 39.33 MiB/sec figure is projected **workload demand** at 12.5k
recipients/sec, not measured device capacity. Requiring an SSH host to continue
was unjustified. Local investigation can and must continue. The underlying
cause of sustained storage-path slowdowns remains unresolved; neither TRIM nor
an intrinsic SSD bandwidth limit has been established.

Fresh 512 MiB private-file sequential tests on the unchanged project Btrfs
mount measured 874.64 and 741.36 MiB/sec with buffered 1 MiB writes, and
886.20 MiB/sec with O_DIRECT requested. All include final fdatasync and
first/last-block verification. Host device counters recorded approximately
514–526 MiB of writes per test, so the reported rates are not merely dirty
page-cache acceptance rates. These short tests establish burst headroom, not
sustained throughput. Compression can affect direct-I/O handling on Btrfs;
O_DIRECT requested on the normal mount is not proof of a raw-device bypass.

A separate 128-operation diagnostic writing and synchronizing each 4 KiB
block measured median 2.55 ms/operation and 1.52 MiB/sec. This illustrates
synchronization cost for that serial access pattern; it is not a Fireweed
transaction benchmark or its throughput ceiling.

Repeating the archived original 8 GiB diagnostic exactly (private NOCOW file,
16 MiB incompressible blocks, O_DIRECT, final fdatasync) measured 44.99 MiB/sec
in 182.10 seconds. Individual GiB segments ranged from 30.92 to 106.42 MiB/sec.
This reproduces a slow sustained path independently of Fireweed, while the
short tests show that approximately 39 MiB/sec is not a universal device limit.
No system setting was changed; NOCOW applied only to that private test file.

The equally sized normal-filesystem buffered test measured **38.51 MiB/sec**
in 212.73 seconds, including 5.50 seconds of final fdatasync. It ran immediately
after the direct NOCOW test; the order is not a controlled isolation of filesystem
policy. During an 18.69-second interval, host device writes averaged 35.84
MiB/sec, 288.95 requests/sec, 342.66 ms/request, and 99.99% busy. All 15 process
wait-channel samples were `balance_dirty_pages`. NVMe temperature samples rose
from 67.85 to 68.85 C; this does not establish thermal throttling. Buffered
projection writes can therefore block on writeback even when their explicit
sync method is a no-op. The current projection adapter already omits sync;
the authoritative local log retains file and directory synchronization.

These results distinguish short-burst throughput, sustained writeback, and
per-operation synchronization. They do not isolate the cause of the sustained
slowdown, establish a hardware maximum, or qualify Fireweed. Continue locally
by attributing campaign time and writes to log publication, projection WAL,
checkpointing, and CPU/SQL work, then measure code changes against the same
representative workflow. Do not require another host or change SSD settings.

Scripts and raw results are archived under
`docs/helix/04-build/evidence/workflow-capacity/` as
`fireweed-local-storage-check*`, `fireweed-repeat-original-sequential*`,
`fireweed-repeat-normal-sequential*`, and `fireweed-normal-sequential-samples.json`.
All owned test data files were verified and removed. The tests used no raw-device
writes, no additional disk, no SSH host, and no Fireweed code changes.


Spreading checkpoint windows across 192–448 MiB did not fix the stall. The
candidate failed during cycle three; its observed 29.053-second interval had
**5.675 MiB/sec writes, 121.1 write requests/sec, 1.926-second write latency,
233.3 outstanding I/O requests and 99.46% device busy**. Application CPU was
2.27 cores. The experiment is reverted. These counters demonstrate a storage-path
bottleneck during the observed interval; they do not establish the SSD's intrinsic
maximum or make a claim about TRIM. A different host could provide an optional comparison; it is not required
for continued local investigation. Failed roots and raw device evidence
are preserved under `fireweed-campaign-staggered-checkpoints-s64-w2-six*`.

A clean 64-store sustained run (`a3b24317`, runtime `8a12e2de`) failed during
cycle five with a **30-second object-log post-position produce timeout**.
This is a failed reliability run, not a throughput qualification. Its last
21.708 measured process-active seconds show a concrete storage-path stall:

- 16.06 MiB/sec writes, 267.55 write requests/sec, 61.46 KiB/request.
- 1,035.87 ms mean completed write-request latency; approximately 277.6 I/O
  requests in progress from the device's weighted I/O time.
- 99.46% device busy, application CPU occupancy 0.967 cores, host full I/O
  pressure 74.38%. NVMe flush requests averaged 26.83 ms at 0.83/sec.

These are host NVMe block counters, not exclusive process attribution or an
intrinsic SSD ceiling. They establish a low-service, deep-queue interval while
the application mostly waited. Five projection main files were materialized at
the last completed cycle; 37 were materialized at failure. The overlap makes
checkpoint scheduling a concrete hypothesis to test, not proof that changing
checkpoint policy will fix it. The next code experiment spreads large projection
checkpoint windows across configured paths, preserving the 448 MiB upper budget,
log durability barriers and timeout. CPU-only napkin extrapolation does not
predict this tail. Do not divide the failed run's total bytes/CPU by its four
completed million-row cycles: those totals also contain partial fifth-cycle work.
Raw and windowed evidence uses `fireweed-campaign-retained-partial-keys-s64-w2-six*`.

A same-filesystem publication protocol diagnostic wrote identical 390 MiB
streams with 48 threads, in immutable/append/append/immutable order. Rates were
373.03 / 48.35 / 91.32 / 31.03 MiB/sec. Immutable publication used 6,144 timed
file/directory syncs; append used 3,072, plus durable segment creation before
timing. All four streams matched by SHA-256. The enormous within-protocol
variation prevents attributing a gain to append publication or treating any
observation as a hardware ceiling. No log-layout rewrite is justified by this
comparison. No filesystem settings were changed. Source, setup costs, device
samples, provenance and results are archived as `fireweed-publication-protocol*`
and `fireweed-run-publication-protocol.py`; the owned root was removed.

Skipping unused partial-index keys (`8a12e2de`) lowered observed first-cycle
CPU cost to 0.94561 ms/recipient at 14,287.40 recipients/sec. Constant-cost
demand at 10k/12.5k is 9.46/11.82 CPU-seconds/sec and 27.63/34.54 MiB/sec
host writes (2.69849 GiB per million measured). This is a single-cycle
observation; the sustained target remains unmet. The change removes SQL work
for index predicates that are false or NULL; no drive setting or durability
barrier changed.

The publication-path changes remove two demonstrated software stalls: upload
dispatch waiting for a manifest commit, and committed index reads waiting for
a new manifest publication. Single-cycle rates remain in the same range
(13,547 grouping-only, 13,652 dispatch, 13,418 readable-index recipients/sec);
these observations do not establish a sustained improvement. The readable-index
run used 0.97445 CPU-ms/recipient and wrote 2.71894 host GiB per million.
Constant-cost demand at 10k/12.5k is 9.74/12.18 CPU-seconds/sec and
27.84/34.80 MiB/sec host writes. The measured 38.41 MiB/sec is not an SSD
maximum, and this first-cycle cost must not replace six-cycle qualification.

The retained-code I/O diagnostic identifies **durability-publication latency**
as a material cost that bandwidth-only math missed. Its 43,713 log syncs
lasting at least 100 ms had 15,209 summed overlapping caller-seconds, maximum
5.605 seconds. Projection main/WAL long writes were far fewer (400/1,083),
with 232/826 overlapping seconds. This does not prove an intrinsic SSD ceiling:
filesystem publication, scheduling and competing writes contribute to those
wall times. Local publication plus its manifest requires four durable barriers
per sealed object. The final 62,550 log files suggest roughly 125,100 file/dir
syncs if each was published once: about **209/261 syncs/sec at 10k/12.5k**,
before repeated metadata publications. This is a file-count model, not a
complete syscall count.

Measured CPU cost was 1.01778 ms/recipient; host writes were 20.60377 GiB
for six million recipients. Constant-cost demands at 10k/12.5k are therefore
**10.18/12.72 CPU-seconds/sec** and **35.16/43.95 MiB/sec writes**. The run
observed 37.34 MiB/sec and 80.25% device busy time; neither establishes the
drive hardware maximum. The concrete code opportunity is to combine already
durable ready uploads into fewer manifest publications, preserving barriers.

Six-cycle direct-join costs (48 stores, runtime `3cdfb41a`) were **1.03450
CPU-ms/recipient**, 21.31616 GiB host writes and 0.01965 GiB host reads for
six million recipients. Constant-cost demand at 10k/12.5k is **10.345/12.931
CPU-seconds/sec** and **36.38/45.48 MiB/sec writes**. Observed throughput was
9,845.15/sec, mean CPU occupancy 10.18, host writes 35.91 MiB/sec, device busy
82.07%, mean write-request latency 47.57 ms and full I/O pressure 15.59%.
These show application/storage-path stalls alongside CPU work; they do not
establish the SSD hardware bandwidth limit. The earlier 40.58 MiB/sec path
calibration is not a hard ceiling: separate application runs exceeded it.
No host settings changed. All non-rate campaign gates passed, but throughput
did not. Further work targets code and records checkpoint timing before any
checkpoint-specific causal claim.

Direct joined replacements (`3cdfb41a`) reduced observed first-cycle CPU cost
from 1.07716 to **0.97226 ms/recipient**, while the clean 64-store/two-worker
rate rose from 12,478.68 to **13,339.15/sec**. First-cycle constant-cost demands
at 10k/12.5k are **9.72/12.15 CPU-seconds/sec** and **31.45/39.31 MiB/sec** of
host writes (3.07121 GiB/million measured). The nominal WAL windows for 64 and
48 stores are 28 and 21 GiB respectively; these are configured bounds, not
measurements of resident memory. The next six-cycle 48-store control tests
whether the improved SQL and smaller layout sustain the target. No intrinsic
SSD ceiling is inferred from these rates or byte demands.

Current user-IP CPU profile (`cb9d1fe9`, runtime `d024a298`, one million rows,
64 stores/two workers) collected **179,311 samples at 199 Hz, zero lost**.
Largest leaf symbols: allocation 6.77%, SQL `op_column` 5.96%, VM `normal_step`
4.46%, `memcmp` 4.36%, two memcpy variants totaling 6.15%, B-tree `move_to`
2.71%, record comparison 2.54%, index move/seek 2.41%/2.34%, and free 2.22%.
This resembles the earlier profile despite removing claim-carrier copies.
It measures sampled executing user instructions, not call-stack ownership,
off-CPU waits or device service time. The next diagnostic compares native
replacement query shapes and statement sizes; no SQL fixture rate counts as
workflow qualification. Raw samples, symbols, mappings, summary and provenance
use `fireweed-current-cpu-cb9d1fe9*`. The owned root was removed after capture.

The clean six-cycle lifecycle run (`264d9a3c`, 64 stores/two workers) reached
10,067.49/sec, with all reporting gates passing but throughput and physical
file-size stability still failing. Cost was **1.16622 CPU-ms/recipient**,
19.96585 GiB host writes and 4.98410 GiB host reads for six million recipients.
Constant-cost demands at 10k/12.5k are **11.66/14.58 CPU-seconds/sec**,
**34.07/42.59 MiB/sec writes**, and **8.51/10.63 MiB/sec reads**.
Mean CPU occupancy was 11.73, host writes 34.41 MiB/sec, device busy 79.90%,
and full I/O pressure 9.51%. These observations do not establish an intrinsic
SSD limit. Late physical-file growth coincides with initial WAL checkpoint
materialization; the inspected final databases have mostly free pages and
no retained workflow rows. See the qualification plan and archived file diagnosis.

The lifecycle reporting trace (`264d9a3c`, 64 stores/two workers) reduced worst
campaign reporting p95 from 2.867 to 0.338 seconds, at 12,812.28 recipients/sec.
Its 1.06857 CPU-ms/recipient and 2.88332 GiB host writes per million imply
**10.69/13.36 CPU-seconds/sec** and **29.53/36.91 MiB/sec** at 10k/12.5k,
assuming constant first-cycle costs. Mean occupancy was 13.63 logical CPUs;
host CPU pressure was 57.21% some, versus 0.81% full I/O pressure. This is
consistent with substantial CPU contention in this traced run, not proof of
an SSD throughput ceiling. Clean multi-cycle costs are the next measurement.

The 64-store/two-worker single-cycle control reached 13,100.43 recipients/sec
but failed reporting latency across all campaigns. Its measured cost was
0.97452 CPU-ms/recipient and 2.83965 GiB of host writes per million recipients.
At constant cost, 10k/12.5k would require **9.75/12.18 CPU-seconds/sec** and
**29.08/36.35 MiB/sec** of host writes. These are first-cycle demands, not
steady-state capacities. Reporting during enrichment was slow despite fast
load reporting; the next code change targets the projection-coverage dependency
in lifecycle counts. Neither the observed 38.83 MiB/sec nor 82.05% device busy
time establishes an intrinsic SSD ceiling or justifies moving hardware.

The fixed-frontier run (`d124c24b`) reached **10,167.33 recipients/sec**, with
all non-rate gates passing but three late cycles below10k. Cost was
1.01615 CPU-ms/recipient and 20.18009 GiB of sampled host writes per six million
recipients. Constant-cost 10k/12.5k demands are **10.16/12.70 CPU-seconds/sec**
and **34.44/43.05 MiB/sec** of host writes. Mean occupancy was10.33 logical CPUs;
measured host writes averaged35.13 MiB/sec, device busy82.85%, mean write-request
latency40.00ms. These numbers still do not establish an intrinsic SSD ceiling.
The next same-binary control changes software sharding from32 stores ×2 workers
per campaign to64 ×1, preserving128 total campaign workers and the full million
rows. It tests locality and parallelism together without changing device settings.

The 4 MiB checkpoint experiment (`0b85c778`) **regressed and is reverted**:
5,847.83 recipients/sec, 1.03131 CPU-ms/recipient, 6.02 mean logical CPUs,
7.61397 GiB of host writes for one million recipients. At that byte cost,
12.5k/sec would require **97.46 MiB/sec**, versus 46.07 MiB/sec measured in this
trial, while CPU demand would be 12.89 CPU-seconds/sec. The smaller window
increased write work substantially; lower RSS was not a performance win.
The observed 46.07 MiB/sec also exceeds the earlier 40.58 MiB/sec calibration:
neither result establishes the drive's intrinsic ceiling. The appropriate
code action is to restore the 448 MiB coalescing window, not adopt a hardware
limit based on that calibration or weaken log durability.

The latest three-worker comparison (`dec6e387`) is **not qualified**:
8,637.08 recipients/sec, 1.20484 CPU-ms/recipient, 10.40 mean logical CPUs,
23.38694 GiB of sampled host writes over six million recipients. Constant-cost
10k/12.5k demands are **12.05/15.06 CPU-seconds/sec** and **39.91/49.89 MiB/sec**.
These exceed the better two-worker costs below. Increasing workers has not
resolved the stalls. Device busy time was 84.12%, measured host writes averaged
34.56 MiB/sec and mean write-request time was 47.11 ms. These observations do not
identify an intrinsic SSD limit. The next code trial shortens the rebuildable
WAL checkpoint window, testing the tradeoff between transient WAL writes and
main-file/checkpoint work. No SSD or host configuration changes are involved.

`cd5db494` with exact membership metrics and explicit disk-projection phase
barriers completed six million recipients at **10,097.35/sec**, but failed
three per-cycle throughput gates and 12 reporting checks. CPU cost was
1.03782 ms/recipient; host writes were 21.1052 GiB. Constant-cost demands at
10k/12.5k are **10.38/12.97 CPU-seconds/sec** and **36.02/45.02 MiB/sec** of
host writes. These are demands, not hardware ceilings. The overall rate was
8.7% above the preceding same-worker control, but CPU cost rose 2.1% and device
bandwidth also changed; no isolated causal throughput gain is established.
All other correctness, retention, due-time, WAL and stability gates passed.

Reporting-phase attribution on `47875b5f` (two diagnostic cycles) found all 511
reads over one second dominated by projection coverage. Across these calls,
coverage accumulated 1,047.49 seconds; their two SQL reads accumulated just
0.410 seconds. These are overlapping wall times, not CPU or device service time.
This establishes a coordination dependency in slow metrics reads and does not
establish an SSD limit. The new bounded push/purge-tail read targets that wait;
its throughput impact must be measured in the unchanged full workload.

Same-binary two-worker control `1e15109d` measured 9,287.72 recipients/sec,
1.01649 CPU-ms/recipient and 21.2378 GiB of host writes over six million recipients.
Constant-cost demands at 10k/12.5k are **10.16/12.71 CPU-seconds/sec** and
**36.25/45.31 MiB/sec** of host writes. It failed 67 reporting latency checks
and throughput qualification. This serial concurrency comparison shows useful
headroom but does not establish either an intrinsic hardware ceiling or a
qualified improvement. Timing of public metrics phases is the next code diagnostic.

Current uninstrumented disk comparisons (32 stores, two campaigns/store, one
worker/campaign, 1,000-row storage batches, six million complete recipients):

| Candidate | Recipients/sec | CPU-ms/recipient | Progress failures | Throughput qualified |
|---|---:|---:|---:|---|
| `daa0dd77`, cached single-row writes | 8,520.83 | 1.03661 | 3 | No |
| `387f0c82`, exact claim-tail metrics | 7,899.88 | 0.98098 | 0 | No |
| `4256e0c0`, guarded replacement batches | 8,355.53 | 1.04291 | 2 | No |
| `1af38acf`, rejected 16-row chunks | 7,428.07 | 1.13144 | 6 | No |
| `f9598882`, owned row transfer | 8,230.52 | 1.04907 | 0 | No |
| `70e64556`, guarded counter-read shortcut | 7,651.62 | 1.03330 | 0 | No |

These are serial candidate observations, not replicated causal comparisons.
The counter-read run's cost implies **10.33/12.92 CPU-seconds per second** and
approximately **42.7/53.3 MiB/sec of host writes** at 10k/12.5k recipients/sec.
It passed all non-throughput gates but showed lower throughput; this is not a
qualified performance gain. The next same-code control retests two workers per
campaign now that reporting behavior has changed.

The owned-row run passes all non-throughput gates but demonstrates no throughput
or CPU gain over the earlier 56-row candidate. Its measured cost implies
10.49/13.11 CPU-seconds per second and approximately 42.5/53.1 MiB/sec of host
writes at 10k/12.5k recipients/sec, assuming constant costs. These remain demands,
not proven ceilings. The next candidate targets duplicate lifecycle aggregate
reads. Historical SQL “write” counts included CTE reads due to an observer
classification bug; total statement counts were unaffected. See the
[qualification plan](campaign-qualification-plan.md) for the correction.

The retained 56-row candidate cost implies **10.43 / 13.04 CPU-seconds per second** and approximately
**42.8 / 53.5 MiB/sec of host writes** at the unchanged 10k / 12.5k targets,
assuming constant per-recipient costs. Those are demands, not proven ceilings.
Reducing SQL call count from 1,022 to 40 per thousand replacements has not by
itself met the end-to-end target. Reducing generated SQL chunks to 16 rows
increased CPU cost and reduced measured throughput; the cap is restored to 56.
The stronger concurrent progress-count oracle remains.

A separate clean `1af38acf` parallel direct-write diagnostic measured **40.58
MiB/sec** for 8 GiB over 201.87 seconds on the same Btrfs storage path. It used
32 preallocated private NOCOW files, verified O_DIRECT, 1 MiB incompressible
writes, and included final fdatasync. Preallocation and verification were
excluded. Process CPU was 1.90 seconds; sampled whole-host busy CPU occupancy
was 0.24 CPUs. The device recorded 40.71 MiB/sec and 336.84 write IOPS with mean
123.77 KiB requests. This is numerical evidence of a slow observed storage path
independent of Fireweed SQL; it does **not** identify an intrinsic SSD limit,
TRIM cause, or need for new hardware. No system settings changed.

At the retained candidate's measured write cost, 10k/12.5k recipients/sec demand
42.8/53.5 MiB/sec, about 1.05/1.32 times this diagnostic rate. That comparison
shows why storage-path delays deserve measurement, but the diagnostic's large
preallocated NOCOW writes differ from compressed projection/checkpoint and log
traffic. It is not a workflow upper bound. Code can still reduce CPU and write
amplification; maintenance is not a prerequisite. Full scripts and measurements
are archived alongside the campaign evidence, as detailed in the
[qualification plan](campaign-qualification-plan.md).

2026-09-14 claim-tail metrics run (`387f0c82`): 7,899.88 recipients/sec;
all non-throughput gates passed, but every throughput gate failed. Its measured
0.98098 CPU-ms/recipient implies **9.81 / 12.26 CPU-seconds per second** at
10k / 12.5k recipients/sec if per-recipient cost stays constant. Host writes of
24.09 GiB over six million recipients imply approximately **41.1 / 51.4 MiB/sec**
at those targets. These are resource demands, not throughput ceilings; contention,
SMT scaling and the serial parts of the pipeline remain unmodeled. The run is a
reporting improvement, not a throughput improvement over the prior 8,520.83/sec
control. The next optimization targets the per-recipient guarded UPDATE loop.

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

All 32 DB/log pairs share one NVMe. Earlier read-only checks found that the
encrypted root mapping blocks discard/TRIM and periodic `fstrim` is disabled.
Those observations motivated a maintenance hypothesis; they did not demonstrate
its effect on this workload. The [proposed control](storage-trim-control.md) was
never executed, the authentication terminal was closed, and no before/after
maintenance measurement exists. Work proceeds on application code. The earlier
38.23 MiB/s private-file result remains a measurement of that run and configuration;
an intrinsic device ceiling and a causal TRIM explanation remain unproven.
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
