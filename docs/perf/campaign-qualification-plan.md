# Campaign qualification and performance plan

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
one-time maintenance comparison is prepared but awaits explicit host approval;
its contribution to the storage bottleneck remains unproven.
See the [current resource math](workflow-hardware-headroom.md) and evidence below.

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
