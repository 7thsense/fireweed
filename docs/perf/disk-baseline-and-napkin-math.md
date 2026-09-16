# Disk baseline and Fireweed capacity estimates

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
