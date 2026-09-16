# Disk baseline and Fireweed capacity estimates

## Rejected compact priority experiment does not change the retained budget (2026-09-16)

A compact projection-only timestamp representation saved 34 column bytes and
reduced mean requested WAL writes by 5.76% in a balanced, serial one-cycle screen:
6,713.23 to 6,326.73 bytes per recipient. At 12.5k recipients/sec that would be
80.03 versus 75.42 MiB/sec of logical WAL writes. It did not establish an equivalent
physical saving, CPU improvement or repeatable throughput improvement. The format
change is removed; these candidate numbers are not the retained capacity budget.

The candidate's two untraced 8M campaigns cost 0.885654/0.910395 CPU-ms per
recipient: a conditional 11.071/11.380 CPU-seconds/sec at 12.5k. Sampled host writes
were 30.875/30.331 GiB, or 4,144/4,071 bytes per recipient and 49.400/48.529 MiB/sec
at the target. Across the two runs, host writes differ by only -0.34% from the
preceding retained pair, versus a much larger logical VFS reduction. Those layers
must not be equated or added; host counters also include other processes and omit
short observation startup/tail gaps.

The first candidate campaign passes every 12.5k gate. The repeat averages
13,412/sec but fails cycle four at 11,864/sec; all non-rate and five-primitive
checks pass. Both pass every 10k gate. Mean sustained rate is 1.30% below the
preceding retained pair and CPU cost 0.17% above it, within substantial run
variation. This does not establish either a causal regression or a useful gain.

The repeat's maximum-busy observed rolling window is near its beginning, not the
failed checkpoint cycle: 11.975 seconds at 98.03% busy, 48.80 MiB/sec writes,
902.27 write IOPS, 134.62 ms mean write-request latency and 121.75 mean outstanding
I/Os, while the workload consumes 5.86 CPU-seconds/sec. This demonstrates storage-
path queueing during the run, not a device bandwidth ceiling or the cause of the
failed cycle. The sequential disk baselines and retained lease-index budget below
remain unchanged. Complete calculations and evidence are in
`fireweed-compact-priority-results-manifest.json`.


## Current sustained lease-index budget and remaining gap (2026-09-16)

The two untraced 8M-recipient campaigns cost **0.880054 / 0.912916 CPU-ms per
complete recipient**. At 12.5k recipients/sec, constant-cost CPU demand would be
**11.001 / 11.411 CPU-seconds/sec**. Both campaigns meet every 10k gate; only the
first meets every 12.5k gate. The repeat's slowest cycle is **11,260/sec**, requiring
about 11.0% more throughput (88.804 seconds down to 80 for one million recipients).
An overall 13,224/sec repeat does not establish the per-cycle fairness target.

Sampled host writes are **30.342 / 31.073 GiB**: **4,072 / 4,171 bytes per
recipient**, or **48.547 / 49.716 MiB/sec** at the stretch rate if costs held
constant. These host-wide estimates omit short monitor startup/tail gaps and are
not projection-only writes or independent hardware ceilings. The earlier
sequential disk baselines remain unchanged; do not add these bytes to VFS totals.

The repeat still has a 12.020-second observed window with **98.48% device busy**,
48.98 MiB/sec host writes and 144.18 ms average write-request latency. At 863.81
write IOPS, Little's-law arithmetic yields about 124.54 outstanding writes,
consistent with 124.72 measured weighted all-I/O depth. Workload CPU occupancy
falls to 4.46 cores in the same window. Average CPU and bandwidth budgets alone
therefore do not predict the minimum cycle rate; storage-path queueing remains.
These measurements do not separate filesystem, log-sync and device service costs.

All five million-row primitive operations pass 10k/sec twice, with validated
count/window/rate arithmetic. The 12.5k campaign target remains fixed and unmet
across repeats. Complete evidence and calculations are in
`fireweed-lease-index-sustained-manifest.json`; see the campaign plan for every
cycle rate, validation scope and the preserved control executable.


## Lease-index CPU screen updates the conditional estimate (2026-09-16)

Four serial one-million-recipient diagnostic runs yield mean CPU costs of
**0.918270 ms/recipient for controls** and **0.892453 for the consolidated-index
candidate**. At 12.5k recipients/sec, holding those costs constant would require
**11.478 versus 11.156 CPU-seconds/sec**, a saving of about 0.323. Both pairs
reduce instructions, by 1.59% and 2.49%; average wall rate improves 2.70%.

Requested projection WAL writes average 6,606.82 versus 6,575.40 bytes/recipient,
only a 0.48% reduction. At 12.5k that corresponds to 78.76 versus 78.38 MiB/sec
of logical WAL writes, before other projection/log activity and filesystem
compression/coalescing. These are VFS bytes, not host-device writes. The change
therefore supports a modest CPU-budget improvement, not a claim that storage
stalls or the sustained rate gap are solved. Existing disk baselines remain intact.

The workload counts match across all four runs; both variants use identical
instrumentation, which prevents qualification. The unchanged repeated eight-cycle
campaign and all-five-primitive qualification follows without tracing. Evidence
and reproducible calculations: `fireweed-lease-index-screen-manifest.json`.


## Checkpoint scheduling did not qualify; updated resource demand (2026-09-16)

Two complete untraced eight-cycle campaigns with staggered checkpoint windows
cost **0.904802 / 0.966630 CPU-ms per complete recipient**. At 10k recipients/sec,
constant-cost demand is **9.048 / 9.666 CPU-seconds/sec**; at 12.5k it is
**11.310 / 12.083 CPU-seconds/sec**. Average throughput is 14,151 / 11,345, but
worst-cycle throughput is only **9,426 / 8,774**, so neither repeated milestone
qualifies. All non-rate campaign checks pass. Restore the previous checkpoint
policy; spreading the first main-file writes did not remove later stalls.

Sampled host writes are 36.689 / 36.440 GiB for eight million recipients:
**4,924 / 4,891 bytes per recipient**. Holding that cost constant would require
**46.961 / 46.643 MiB/sec at 10k**, or **58.702 / 58.304 MiB/sec at 12.5k**.
These are host-wide write-demand estimates with monitor startup/tail omitted,
not projection-only bytes, application write sizes, or a new hardware ceiling.
They must not be added to VFS logical-write measurements. Existing sequential
disk baselines remain unchanged.

The busiest rolling windows show 97.05% / 99.99% device busy, 52.11 / 18.61
MiB/sec host writes, 967.47 / 536.56 write IOPS and 166.59 / 310.07 ms mean
write-request latency. IOPS times latency gives about 161.16 / 166.38 outstanding
writes, close to the measured all-I/O weighted queue depths 161.53 / 167.55.
Workload CPU occupancy in those intervals is only 5.06 / 4.27 cores. Queueing and
workload shape therefore matter; average CPU demand and a sequential bandwidth
number alone do not predict the minimum cycle rate. These counters do not
separate controller, filesystem and durable-log synchronization costs.

All five million-row public primitive operations exceed 10k in both repeats.
The strengthened acceptance gate now verifies every operation and reconciles
its rate with full row count and measured phase duration. Its regression tests
and re-evaluation of the saved evidence pass. See the campaign plan and
`fireweed-staggered-checkpoints-repaired-results-manifest.json` for exact inputs,
commands, hashes and failures. No workload, durability or fairness requirement
was relaxed, and no SSD setting changed.


## Sustained checkpoint burst, not a sequential bandwidth ceiling (2026-09-16)

The rejected JSON-decoder candidate completed one untraced 8M-recipient campaign
at 13,673.93/sec overall. CPU cost was 0.902964 ms/recipient, implying **11.287
CPU-seconds/sec at 12.5k** if costs held constant. That average did not prevent a
9,678/sec cycle: all 64 main projection files first materialized in that cycle,
and delivery time rose from about 17 to 57 seconds.

One observed 11.643-second burst had 99.52% block-device busy time, 57.99 MiB/sec
host writes, 1,955.94 write IOPS and 120.18 ms mean request latency. Little's-law
check: 1,955.94 requests/sec × 0.12018 sec ≈ **235 outstanding requests**, matching
the measured 235.23 weighted queue depth. Average completed write size was
30.36 KiB; this differs from the large sequential baseline. Workload CPU usage
fell to 1.246 cores in the same interval. These are measured burst demands and
queueing observations, not a new claim about the drive's maximum speed.

The code-level lead is checkpoint scheduling across stores. A constant CPU-cost
napkin estimate cannot predict a worst-cycle rate while synchronized storage
bursts stall otherwise available CPUs. Preserve the existing disk baselines and
all gates; test spreading projection checkpoint work before changing hardware.
Raw samples, complete campaign report, calculations and interpretation limits:
`fireweed-direct-json-sustained-manifest.json`.

## Direct metadata JSON diagnostic budget (2026-09-16)

The balanced four-run screen averages 0.910048 CPU-ms/recipient for controls
and 0.906844 for the candidate. At 12.5k recipients/sec, holding those costs
constant would require **11.376 versus 11.336 CPU-seconds/sec**. This 0.040
CPU-second/sec difference is too small, relative to observed run variation,
to claim that the sustained throughput gap is closed. The candidate's mean wall
rate is 2.04% higher, but repeated eight-cycle qualification is still required.
Logical-work accounting matches for all 128 campaigns in all four screens.
The existing disk baseline and qualification gates remain unchanged. Evidence:
`fireweed-direct-json-screen-manifest.json`.

## Response-copy experiment and CPU-cost uncertainty (2026-09-16)

Eight serial one-cycle runs across both orderings reject the response-copy
candidate: its 1.08% instruction reduction did not translate into throughput.
Four unchanged controls average 0.914565 CPU-ms/recipient, ranging from 0.866597
to 0.959236. At 12.5k recipients/sec, the constant-cost calculation is
**11.432 CPU-seconds/sec**, with observed-control endpoints **10.832–11.990**.
The rejected candidate averages 0.908927 CPU-ms/recipient, or 11.362 CPU-seconds/sec
at the same target. This small CPU difference is not a throughput prediction;
mean measured throughput was lower. The tests performed identical logical work
for all 128 campaigns in every run. Existing sustained gates and disk baselines
remain authoritative. See `fireweed-member-ownership-reverse-manifest.json`.

## Global apply admission screen (2026-09-16)

The 16-active-apply cap regressed both serial comparisons and was removed.
Unchanged controls cost 0.859922 and 0.915832 CPU-ms per recipient, implying
**10.749 and 11.448 CPU-seconds/sec at 12.5k recipients/sec** if those costs
held constant. Candidates cost 0.930441 and 1.022708 CPU-ms per recipient;
limiting transaction concurrency did not reduce CPU cost here. These one-cycle
observations neither qualify the sustained target nor establish a hardware
ceiling. Preserve the previous disk baseline and all eight-cycle gates.
Full reports and provenance: `fireweed-apply-admission-screen-manifest.json`.

## Record-buffer screen (2026-09-16)

Output-buffer reuse was rejected after both alternating pairs increased CPU
cost and instruction count. Identical controls cost 0.85423 and 0.90388 CPU-ms
per recipient: at 12.5k/sec, about **10.678 and 11.299 CPU-seconds/sec**, before
host work outside the measured process. These remain one-cycle observations,
not sustained throughput predictions. The prior control's 65.36% CPU-some
pressure and 14.50 busy CPU-seconds/sec motivate inspecting actual projection
apply concurrency. That evidence establishes neither an optimal thread count
nor an SSD ceiling. See `fireweed-record-buffer-screen-manifest.json` and the
campaign plan for the complete screen and unchanged qualification requirements.


## Repeated CPU-cost observations (2026-09-16)

The record-comparison candidate was rejected: both alternating pairs increased
instructions and CPU time. Identical control binaries measured **0.8591 and
0.9463 CPU-ms/recipient**, corresponding to **10.739 and 11.828 CPU-seconds/sec**
at 12.5k recipients/sec. User cycles per user CPU-ns fell from 3.103 to 2.769;
this is consistent with an effective-clock change, not proof of its cause.
The existing telemetry confirms median sampled host frequency fell from
3.263 to 2.920 GHz while median CPU temperature fell from 77.19 to 72.06 °C.
This supports clock-rate variation, not a thermal-throttling explanation.
Do not turn one observed CPU cost or short burst clock into a hardware ceiling.
The previous 0.8910 profile-derived budget remains an explicitly conditional
observation, not a fixed machine constant. See the campaign plan and
`fireweed-record-continuation-manifest.json` for all four runs and provenance.


## Active-backend CPU budget (2026-09-16)

The full 8M-recipient, eight-cycle control profile measured 0.891019 CPU-ms per
complete recipient (perf-wrapper CPU included). Holding that cost constant,
10k recipients/sec consumes about **8.910 CPU-seconds/sec**, and 12.5k consumes
**11.138 CPU-seconds/sec**. These are resource requirements, not achievable-rate
predictions: barriers, serialization, queue fairness, I/O waits and CPU scaling
still determine each cycle's rate. The diagnostic run passed the resource and
workload checks but cannot qualify because profiling was enabled.

Actual in-process WAL latest-frame iteration accounts for only **0.34% self
CPU**. Even eliminating all of it saves at most roughly 0.0030 CPU-ms/recipient
under this profile; it cannot explain or close the latest 7.41% worst-cycle rate
gap by itself. Composite-index comparison is a stronger candidate: 6.68%
inclusive CPU, including 2.65% self CPU in the generic comparator. These are
sampled categories with overlapping ancestry, not independently additive costs.
The candidate reuses decoded record positions after the first equal text key;
its benefit remains unmeasured. No device ceiling is inferred from this profile.

See `fireweed-active-wal-profile-manifest.json` in the workflow-capacity evidence
directory for the raw recording, commands, gates and reports. Existing sustained
write baselines and fixed campaign qualification targets remain unchanged.


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


## Read amplification above the device (2026-09-16)

The eight-cycle diagnostic requests **5.233 VFS reads / 21,433 read bytes per
recipient**: 41.86 million calls and 171.46 GB total. At 12.5k/sec, that is about
**65,410 calls/sec and 255.50 MiB/sec of requested reads**. Host device reads are
only about 4 MiB over the whole sampled run, so these are software/OS-cache
traffic budgets, not SSD bandwidth requirements. Do not add them to physical
write demand or use them to declare device saturation.

CPU costs 0.90672 ms/recipient, implying **11.33 CPU-seconds/sec** at 12.5k.
All traced cycle rates exceed 12.5k, but the recording is diagnostic and cannot
establish repeated untraced capacity. Accumulated read-call elapsed time includes
callbacks, scheduling and overlap; it does not establish recoverable CPU or wall
time. Full reader-cache invalidation on WAL snapshot changes is a concrete code
path to investigate before enlarging bounded caches. Isolation and fallback
behavior remain mandatory. See the [read-trace record](campaign-qualification-plan.md).

## Decoder screen supplies no new capacity estimate (2026-09-16)

The serial combined linear-purge/owned-decoder screen is inconclusive: candidate
CPU costs 0.87709 / 0.92932 ms per recipient, versus 0.89224 control; instructions
are 0.34% lower / 1.63% higher. Do not treat the faster first run as a stable
capacity increase. Revert the decoder candidate and retain only the previously
validated linear purge change. Its independent sustained effect is still unknown.
The last qualified baseline retains the **4.2%** worst-cycle stretch gap. No new
disk limit, target, durability assumption or workflow/gate change follows.
See the [comparison record](campaign-qualification-plan.md) for exact provenance.

## Purge validation algorithmic budget (2026-09-16)

A fresh user-cycle profile of the guarded-counter qualification binary attributes
0.77% self samples to purge-plan validation. The old full-batch validation performs
32,004,000 linear ID comparisons for 8,000 distinct requested/planned IDs. One
requested-ID hash set with removals changes this to expected-linear construction
and validation while enforcing both subset membership and unique planned IDs.
That operation-count reduction is **not** an end-to-end speedup estimate: profile
loss, hashing costs and blocked time prevent deriving capacity from it alone.

The candidate passes engine and public/native release tests, but has not yet been
performance-compared. Keep the last measured worst-cycle gap at **4.2%** and the
repeated 12.5k goal open. Neither disk baselines nor workload/gate assumptions
change. See the [profile and validation record](campaign-qualification-plan.md).

## Guarded-counter sustained budget and remaining gap (2026-09-16)

The untraced repeated qualification retains every 10k gate but still misses
12.5k on four cycles of the second campaign. First/repeat overall rates are
15,108 / 12,888/sec; worst cycles are 13,922 / 11,996/sec. The remaining worst
cycle needs **4.20% more throughput**, or **4.03% less elapsed processing time**.
The goal is not complete. See [qualification evidence](campaign-qualification-plan.md).

Measured CPU cost is **0.91004 / 0.96094 ms/recipient**. At the fixed 12.5k
target that requires **11.38 / 12.01 CPU-seconds/sec**. Sampled host writes are
**4,166 / 4,123 bytes/recipient**, equivalent to **49.66 / 49.15 MiB/sec** at
12.5k. These are resource budgets derived from this workload, not independent
hardware ceilings. The longer sequential calibration does not capture file and
directory synchronization or establish a sole cause of campaign variance.

The repeat has comparable write volume but 16.59 ms mean device write-request
latency versus 5.16 ms initially; device busy rises 31.74% to 53.48% and CPU
cost increases 5.59%. Keep both effects in the model. The adjacent diagnostic
comparison demonstrated about 6% fewer instructions from guarded-counter
inference, but that improvement alone has not removed the sustained stretch gap.
No SSD settings, log durability barriers, workload scope or qualification gates
changed to obtain these measurements.

## Guarded-counter candidate CPU budget (2026-09-16)

One-cycle diagnostic candidate/control/candidate measurements reduce instructions
per recipient about 6%, with candidate CPU cost 0.88276 / 0.90491 ms versus
0.93270 ms control. At 12.5k recipients/sec these candidate costs require
**11.03 / 11.31 CPU-seconds/sec**, versus **11.66** for the adjacent control.
These are measured resource budgets, not independent throughput ceilings or
sustained qualification. The repeated eight-cycle result remains to be measured;
retain the fixed 10k/12.5k targets and all resource/correctness gates.
See the [comparison record](campaign-qualification-plan.md) for provenance and
variance. No disk baseline or durability assumption changes.

## Repeated 10k established; remaining stretch budget (2026-09-16)

The canonical untraced pair now passes every **10k** campaign and primitive gate.
Campaigns average 14,424 / 13,145 recipients/sec, with worst cycles 13,038 /
11,712. Only the first campaign qualifies at 12.5k; the repeat misses three
cycle-rate checks. See the [full qualification record](campaign-qualification-plan.md).
This is progress on the fixed objective, not a replacement for the stretch goal.

Measured CPU cost is **0.94448 / 0.98161 ms per recipient**, implying **11.81 /
12.27 CPU-seconds/sec** at 12.5k. Host writes are about **4,180 / 4,007 bytes per
recipient**, implying **49.82 / 47.77 MiB/sec** at that rate. The repeated run
has lower total write volume but higher mean device write-request latency
(14.86 versus 5.80 ms), and higher CPU cost. These are measured resource budgets,
not independent capacity predictions or proof of a single bottleneck.

The slowest cycle requires **6.73% more throughput**, equivalent to reducing
its 85.377-second wall time by **6.30%** to reach 80 seconds per million rows.
Keep both CPU reduction and synchronization variance in the model. The longer
81.36 MiB/sec calibration interval bounds optimism about sequential headroom;
it does not turn elapsed fsync time into a bandwidth measurement.

History review rejected another column-header cache trial because the earlier
serial comparison showed no useful repeatable workflow gain. Next measure
removing redundant lifecycle-counter reads for guarded authority-first claims.
Preserve per-cycle fairness, reporting, durability and all resource gates in
subsequent comparison and repeated qualification.


## Publication-phase budget after a passing diagnostic (2026-09-16)

The unchanged workflow with publication tracing costs **0.92693 CPU-ms per
recipient**, uses **13.77 process CPU-seconds/sec**, and completes at 14,864/sec
with a 13,299/sec slowest cycle. Instrumented provenance prevents qualification;
repeated untraced 10k and 12.5k milestones remain open.

At this measured cost, 10k / 12.5k require **9.27 / 11.59 CPU-seconds/sec**.
Host writes are about **4,256 bytes/recipient**, requiring **40.59 / 50.74
MiB/sec**. Relative to the previously measured later contiguous-write interval
of 81.36 MiB/sec, the stretch write budget has about **1.60x** headroom. This
cross-workload budget is neither an independent throughput prediction nor proof
that synchronization latency cannot limit the workflow below that bandwidth.

The eight million recipients cause **87,983 successful local publications**:
44,074 segments, 43,141 manifests and 768 metadata objects. At two synchronization
barriers per publication, the stretch rate implies roughly **275 sync calls/sec**.
Despite manifests totaling only 7.26 MB, each requires durable publication;
there are only 2.12% fewer manifests than segments. Byte bandwidth alone omits
this cost. The phase trace attributes 89.09% of accumulated publication elapsed
time to file/directory synchronization, versus 0.82% to writes. Those durations
include scheduling and overlap; do not sum them into campaign wall time.

This motivates testing bounded grouping of already-uploading successors, with
publication counts as the direct mechanism check. A faster instrumented run
alone does not establish a speedup. Preserve the same public workflow, durability,
correctness, fairness, reporting and resource gates in the serial comparison.
Evidence and qualifications: [publication-phase record](campaign-qualification-plan.md).


## Extended write calibration and joint campaign budget (2026-09-16)

An immediate serial follow-up to the eight-cycle joint trace extended the same
16 MiB-block private-file direct/NOCOW calibration beyond its previous 8 GiB
burst. It requested at most 64 GiB, stopped after 180 seconds of writes, included
final fdatasync, verified first/last block hashes, and removed its private file.
No manual TRIM, host tuning, build or competing benchmark intervened. The buffer
was generated before timing. Exact script and raw per-block timings are archived.

**32.125 GiB completed in 180.125 seconds: 182.63 MiB/sec overall.** The first
19 GiB averaged **884.81 MiB/sec**. The twentieth GiB averaged 179.93 MiB/sec;
the next twelve complete GiB averaged **81.36 MiB/sec**, with individual full-GiB
intervals spanning 58.41–117.82 MiB/sec. The remaining partial interval also
completed. Python consumed 0.059 user + 2.977 system CPU-seconds, so Python
execution cost does not explain this drop. Final fdatasync took 0.114 seconds.

This is direct evidence that this device/filesystem stack does not sustain its
roughly 900 MiB/sec burst rate across this longer write stream in its measured
state. It does **not** identify NAND type, cache capacity, garbage collection,
TRIM failure, or an indefinite steady-state ceiling. The test uses a contiguous
NOCOW file; it does not reproduce campaign COW writes, compression, durable file
publication or synchronization latency. A campaign slowdown cannot be assigned
entirely to this measured bandwidth drop.

The preceding traced campaign costs **0.93407 CPU-ms/recipient**, requiring
**9.34 / 11.68 CPU-seconds/sec** at 10k / 12.5k. Actual process occupancy is
13.09 CPU-seconds/sec. Its host-wide write volume is about **4,125 bytes per
recipient**, implying **39.34 / 49.18 MiB/sec** at the two targets. Compared with
the later 81.36 MiB/sec contiguous calibration interval, the stretch write budget
has approximately **1.65x** bandwidth headroom, not the 18x suggested by the
short 900 MiB/sec burst. This is a cross-workload resource comparison, not a
capacity prediction: file-sync latency and competing reads can matter below
sequential bandwidth saturation.

Projection VFS requested **7,250 bytes/recipient** (WAL plus main-file writes),
or **86.43 MiB/sec** at 12.5k. Do not compare that uncompressed logical byte
count directly with the compressed physical-device counter or add the two.
The full trace still misses the last two 12.5k cycle gates. Increasing log
publication latency warrants phase-level investigation while preserving every
log durability barrier; projection CPU work remains a separate optimization
opportunity. No target or resource gate changes follow from this calibration.

Evidence: `fireweed-post-joint-sustained-direct-20260916.json.gz`,
`fireweed-sustained-sequential.py`, and the joint-trace artifacts listed in the
[qualification record](campaign-qualification-plan.md).


## Baldr control separates a CPU budget from write latency (2026-09-16)

The unchanged eight-cycle control on Baldr reaches 9,843/sec at **1.05739
CPU-ms/recipient** and **10.40 process CPU-seconds/sec**. A 10k rate requires
10.57 CPU-seconds/sec; 12.5k requires **13.22**, exceeding its twelve logical CPUs
at the measured cost even under ideal occupancy. At observed occupancy, the
stretch target instead requires at most **0.83222 CPU-ms/recipient**, a **21.3%**
reduction. That is an aggregate resource estimate, not proof that every cycle
will meet the target, and SMT threads are not independent physical cores.

Baldr writes 32.86 GiB for eight million lifecycles (about **4,410 bytes/recipient**),
which scales to roughly **52.6 MiB/sec** at 12.5k. Mean device write-request latency
is only **0.607 ms**, versus 29.89 ms in Forseti's failed repeat; Baldr nevertheless
misses the target with almost all host logical CPU time busy. Therefore faster
write service alone is insufficient on this smaller CPU. CPU/memory/kernel/disk
all differ; this is not a controlled attribution of Forseti's stalls to hardware.
The unchanged rate and RSS gates still fail. Neither performance milestone is met.

The raw hardware/workload/counter evidence and the diagnostic provenance limits
are in the [qualification record](campaign-qualification-plan.md). Continue code
optimization and retain disk latency, CPU cost and worst-cycle rate separately;
do not treat sequential bandwidth or the best short run as sustained capacity.

## Owned-parameter repeat: target still requires sustained occupancy (2026-09-16)

The current binary completes the two eight-million-recipient campaigns at
14,071.58 and 11,312.26/sec, with slowest cycles 12,077.85 and 8,734.21/sec.
Neither milestone is repeatedly met. CPU costs of 0.94603 / 0.98477 ms per
recipient require **11.83 / 12.31 CPU-seconds per second** at the 12,500 target;
actual process occupancy is **13.30 / 11.13**. These measured costs support a
feasible CPU budget on this host, but the second run does not sustain it.
Logical threads are not independent physical cores, and occupancy alone is not
an independent prediction of scalable throughput.

Host writes total 29.75 / 33.36 GiB: approximately 3,994 / 4,477 bytes per
recipient, implying **47.61 / 53.37 MiB/sec** at the stretch target. Mean device
write-request latency is 7.43 / 29.89 ms, with device busy 40.43% / 65.74%.
These host-wide totals include other activity and exclude monitor startup/tail;
they neither establish a sequential bandwidth ceiling nor identify the cause
of the stalls. No host tuning was performed.

A checksum-validated historical WAL sample puts an optimistic adjacent-pair
page-elimination bound at 6.87%, before queue/readiness restrictions. That is
insufficient evidence to attribute the campaign gap to transaction boundaries.
See the current [qualification record](campaign-qualification-plan.md) for exact
provenance, unchanged gates, serial primitive results and the next attribution work.

## Distinguish logical projection writes from device writes (2026-09-16)

The complete traced canonical campaign requests **7,085.55 WAL bytes plus
258.60 main-file bytes per recipient**, versus approximately **4,201.3 host-device
write bytes/recipient**. At 12,500 recipients/sec these correspond to **87.55 MiB/sec
of projection VFS writes**, before log writes, and approximately **50.08 MiB/sec
host-device writes** for the observed complete workload. Compression, caching,
filesystem metadata and unrelated host writes make these different accounting
layers; do not add them or treat either as an independent device ceiling.

WAL VFS calls total 575.76 seconds across 64 stores, main-file calls 67.53 seconds;
individual calls reach 7.04/4.70 seconds. These overlapping times identify where
projection calls block but cannot account for all wall time or exclude durable
log waits. Further optimization should measure fewer WAL page versions per
recipient, CPU cost and the unchanged end-to-end gates together. Evidence:
`fireweed-canonical-64-write-trace-accounting.json.gz` and full qualification-plan
entry. Neither performance milestone is repeatedly met.

## Repeated canonical run updates the estimate (2026-09-16)

The final clean candidate failed sustained qualification: 13,579 and 12,016
complete recipients/sec overall, with slowest cycles 11,286 and 9,065/sec.
Measured CPU costs are 0.97178 and 1.01089 CPU-ms/recipient. At 12,500/sec,
these imply **12.15–12.64 CPU-seconds/sec**, compared with actual process
occupancy **13.18 and 12.14 CPU-seconds/sec**. The second run cannot attain the
target at its observed CPU cost and occupancy. Increasing occupancy or reducing
cost is necessary; aggregate averages do not guarantee the cycle fairness gate.

Host writes were 31.48 and 30.74 GiB for eight million lifecycles, approximately
4.23 and 4.13 kB/recipient, implying about **50.4 and 49.2 MiB/sec** at 12,500/sec.
Host counters include other activity. These are retrospective measured resource
budgets, not independent hardware ceilings. An unchanged serial 8 GiB calibration
afterward measured **912.77 MiB/sec direct** and **317.77 MiB/sec buffered**.
Sequential bandwidth is therefore not demonstrated to be the campaign limit;
small-write latency, checkpoint bursts, CPU and coordination need attribution.
Earlier manually forced projection compression also differs from this canonical
configuration and must not be silently treated as identical.

See [the qualification record](campaign-qualification-plan.md) and archived
`fireweed-final-register-reuse-*`, `fireweed-final-qualification-phase-*`, and
`fireweed-post-qualification-*` for source identities, full failures and samples.
Both the repeated 10k and 12.5k milestones remain open.

## Latest sustained candidate: one full pass, repetition pending

The register-buffer reuse candidate (`a9605e58...`, source `6b55ef72`) completes
all eight million original-row lifecycles at **14,166 recipients/sec**, with
**13,242/sec** in its slowest cycle. Every 10k/12.5k gate passes in this run.
This supersedes the historical next-investigation notes below, but does not
establish repeated qualification or an isolated performance gain from reuse.
The request-fingerprint upgrade repair now passes the 315-test local suite;
final repeated qualification remains pending.

Using its full-process CPU cost and sampled host-wide physical writes:

| Quantity | Measured value |
| --- | ---: |
| CPU per complete recipient | 0.97191 ms |
| Host writes per recipient | 3762 bytes |
| Process CPU occupancy | 13.76 CPU-seconds/sec |
| CPU demand at 12,500 recipients/sec | 12.15 CPU-seconds/sec |
| Write demand at 12,500 recipients/sec | 44.85 MiB/sec |

Holding the measured cost and occupancy fixed gives approximately 14.2k/sec.
This retrospective resource model supports the target's plausibility; it is
not an independently measured hardware ceiling. The sustained buffered disk
calibration remains 738 MiB/sec. That bandwidth figure alone cannot predict
workflow throughput or durable publication latency. Actual campaign device
write latency averaged 3.31 ms at 31.7% busy time.

Memory conditions also belong in the estimate: this run observed up to 3.79 GiB
of process swap on zram and 763,808 major faults, despite at least 41.01 GiB host
available memory. Swappiness was 150, with no cgroup memory cap or OOM events.
No host settings were changed. These observations do not establish swap as a
throughput limit; the run passes all unchanged stability gates. The earlier
memory-attributed run had no process swap, so it should not be substituted for
this run's resource evidence.

Evidence: `fireweed-campaign-trim-register-copy-eight*` and
`fireweed-register-copy-eight-memory-context.json` under
`docs/helix/04-build/evidence/workflow-capacity/`.

## What the Forseti investigation established

The full chronology, commands and archived evidence are in
[local machine diagnosis](local-machine-diagnosis.md). The useful sequence was:

1. Reproduce the slowdown outside Fireweed using the same Python write loop.
   Compare with independent dd measurements, including final fdatasync. Short
   512 MiB bursts reached hundreds of MiB/sec, while 8 GiB tests collapsed.
   Python CPU time was under one second during a roughly two-minute direct run.
2. Inspect kernel errors, storage topology, health and thermal counters. Fix the
   independently identified Btrfs writeback defect by upgrading 7.2.3 to 7.2.6.
   The sustained slowdown remained; neither the kernel fix nor SMART alone
   explained it. No thermal-counter increase accompanied the traced test.
3. Trace NVMe command setup/completion: mean write latency 120 ms, maximum
   6.33 seconds. This located substantial delay below filesystem processing,
   but did not distinguish controller/firmware from driver/completion handling.
4. Compare the same workload on Baldr's encrypted Btrfs Toshiba NVMe: 859 MiB/sec
   across 8 GiB, versus Forseti's 67 MiB/sec. Eldir's encrypted ext4 Samsung 860
   SATA path achieved 427 MiB/sec. These were sequential, private-file tests.
5. Verify discard through every layer. Forseti's NVMe supported it, but its
   LUKS mapping advertised zero discard capability; fstrim.timer was disabled.
   Filesystem free space was not evidence that the SSD knew those blocks were
   disposable. Earlier dismissal of TRIM was incorrect.
6. Enable persistent LUKS discard, trim Btrfs free space, and verify matching
   physical-device counters: 325.6 GiB in 89,411 discard commands. After a short
   recovery interval, unchanged direct and buffered tests reached 900 and
   738 MiB/sec respectively. Then enable Btrfs discard=async for ongoing
   create/delete traffic, keeping weekly fstrim as a backstop.

The dramatic same-machine before/after supports blocked discard as a major
cause. It does not turn sequential bandwidth into a Fireweed throughput
promise. The authoritative log still requires durability; rebuildable
projections omit explicit synchronization but still consume CPU, memory,
writeback bandwidth and checkpoint work.

## Required baseline for future measurements

Before a major benchmark series or after a storage/kernel change:

- Record kernel, CPU topology, physical device model, filesystem/mount options,
  encryption mapping, available space, health/temperature, discard capability
  through the mapping, and TRIM configuration. Record exact CLI/source hashes.
- Run tests serially, without builds or other write benchmarks. Record start/end
  device counters and background activity. Do not use an unrelated filesystem,
  tmpfs, sparse allocation, compressible zeros, or a short burst as the sustained
  storage baseline for Fireweed.
- Run the archived 8 GiB/16 MiB-block direct private-file calibration, followed
  by the normal buffered calibration. Both pre-generate incompressible data,
  include final fdatasync, verify first/last blocks, and report per-GiB behavior.
  NOCOW applies only to the direct Btrfs test file. Record order and idle periods.
- Treat 8 GiB as this reproducible calibration, not proof of indefinite steady
  state. If rates vary or a sustained limit is asserted, extend/repeat a bounded
  test beyond the observed burst and verify device counters. Synchronous random
  I/O and log commit latency require separate matching measurements.
- Keep ongoing async discard part of the normal machine configuration. Do not
  secretly pre-trim before each qualifying run to manufacture a fresh-device
  result. Repeat campaigns consecutively with normal retention and checkpointing.

## Napkin math: resource demand first, qualification second

For the fixed original-row campaign (one million resident recipients, 64 stores,
8 cycles, two enrichment stages, scheduled delivery/retries, progress reads and
purge), count complete recipient lifecycles. Approximately 8 + 2/19 = 8.1053
logical mutations occur per recipient; 10k row mutations/sec is not 10k complete
recipients/sec. Handler and API batch limits remain part of the workload.

Measure the following over the full campaign, including checkpoints:

- `C`: process CPU-seconds / completed recipients.
- `B`: physical host write bytes / completed recipients, with background activity
  explicitly identified. Logical payload bytes miss WAL/checkpoint amplification.
- `P`: usable CPU-seconds per wall second at the observed workload and topology.
  Sixteen logical threads are not sixteen independent physical cores.
- `D`: measured sustained storage-path bytes/sec for the relevant access pattern.
- Log durable batch latency, batch size, independent concurrency, progress latency,
  queue fairness, memory/WAL bounds and final-cycle stability.

At recipient rate R, demand is R*C CPU-seconds/sec and R*B write bytes/sec.
The bandwidth/CPU envelope is min(P/C, D/B), before serial dependencies, durable
commit latency, contention and fairness costs. A rough independent-batch log
bound is concurrent durable batches * events per batch / batch latency, divided
by events per recipient; it must be measured with the actual log publication path.
Do not add overlapping per-phase maxima or sum overlapping I/O latencies as wall
clock time. Never call observed device throughput a device capacity ceiling.

Using the older approximately 3,300 physical bytes/recipient and 1.04 ms CPU
cost only as provisional inputs:

| Complete recipients/sec | Logical mutations/sec | CPU-seconds/sec | Host writes MiB/sec |
|---:|---:|---:|---:|
| 10,000 | 81,053 | 10.4 | 31.5 |
| 12,500 | 101,316 | 13.0 | 39.3 |

The repaired buffered sequential calibration (738 MiB/sec) is about 19 times
that 12.5k bandwidth demand, corresponding to a bandwidth-only envelope near
234k recipients/sec at 3,300 bytes/recipient. This is NOT an achievable Fireweed
prediction: the provisional CPU envelope at 14 usable CPU-seconds/sec is only
about 13.5k recipients/sec. The full repaired campaign must replace C and B and
show whether CPU, serial work, log durability or storage now sets the limit.

Acceptance remains two clean full campaign passes and the primitive floors on
the final candidate, first at 10k and then 12.5k completed recipients/sec. Every
cycle, independent outcome check, progress-read latency/frequency check, due
latency limit, WAL bound, materialized checkpoint and stability check still
applies. Changing the napkin estimate does not lower these acceptance targets.

## First repaired full-campaign measurement

On unchanged CLI `be319723...`, source `4cb870fa`, the full eight-cycle run
completed at **15,064 recipients/sec**, with a slowest-cycle
equivalent rate of **14,342/sec**. Every 10k and 12.5k gate passed. This is one
qualifying run; repetition and refreshed primitive checks remain required.

Measured CPU cost is **0.9332 ms/recipient**. Sampled host writes were
28.547 GiB, or approximately **3832 bytes/recipient**; these are host-wide
counters with a slightly shorter sampling window than the full run. At 12.5k,
this implies **11.67 CPU-seconds/sec** and **45.68 MiB/sec**
of writes. At 10k, it implies **9.33 CPU-seconds/sec** and
**36.54 MiB/sec**. The observed process consumed about 14.04
CPU-seconds/sec. Holding CPU cost and that occupancy fixed gives roughly
15k recipients/sec, consistent with this run, rather than the much higher
sequential-bandwidth-only envelope. This is a retrospective resource budget,
not an independent prediction or proof that further CPU optimization is impossible.

Mean device write-request latency was 2.99 ms, device busy time 31.9%, and
host I/O wait approximately 0.075 CPU-seconds/sec. Peak process RSS was
18.49 GiB. Compared with the previous same-binary fixed-kernel run's 9,971/sec
and 58.3 ms device write latency, the repaired configuration improved full
workflow throughput by about 51%. The 13.4x disk calibration improvement does
not imply a 13.4x application improvement once CPU becomes limiting.

Evidence: `fireweed-campaign-trim-s64-w2-eight*` and
`fireweed-campaign-trim-summary.json` in the workflow-capacity evidence directory.

## Repeatability result and next code investigation

The second identical full campaign, without an intervening manual TRIM, ran at
**14,096 recipients/sec overall**, but its final cycle reached only
**11,548/sec** and the RSS stability gate failed. Every other check passed.
Final-three snapshot maxima were 14.782, 11.157 and 11.983 GiB: the failure
was a decrease/variation, not evidence of a monotonic memory leak. The overall
peak was 17.32 GiB. Device write latency remained 3.3 ms; CPU cost was
0.962 ms/recipient. Increased reads and memory pressure accompanied the repeat,
but do not by themselves establish the cause of its late-cycle slowdown.

Both subsequent million-row varied-payload primitive runs passed all gates:

| Phase | Run 1 records/sec | Run 2 records/sec |
|---|---:|---:|
| Insert | 118,379 | 107,239 |
| Enrich by key | 99,300 | 98,849 |
| Schedule by ID | 98,586 | 102,586 |
| Claim and complete | 74,277 | 75,345 |
| Purge | 159,122 | 162,966 |

These are 1,000-row public API batches across 32 independent stores, not
individual unbatched request rates. The same CLI binary was used throughout;
all write tests ran sequentially. Existing same-binary correctness tests were
not rebuilt or rerun during these performance measurements.

The repeated full-campaign goal remains **unmet**. The next bounded code
investigation is late-cycle memory/cache reclamation and CPU cost, particularly
preparation and delivery, while retaining the same eight-cycle workload and
all gates. Instrument process RSS, anonymous/file memory, swap, major faults
and pressure through the run to distinguish allocator/cache release from
reclamation; correlate with phase timing before changing cache budgets.
Do not weaken the RSS gate or return to speculative SSD settings.

Evidence: `fireweed-campaign-trim-s64-w2-eight-repeat*`,
`fireweed-campaign-trim-repeat-summary.json`, `fireweed-trim-primitives-{1,2}.json.gz`
and `fireweed-trim-primitive-pair.json`. All four post-repair Fireweed runs are
complete. Projection directories remain retained; no release or push was made.
