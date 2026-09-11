# Campaign qualification and performance plan

2026-09-11. This supersedes treating the original-row saturation test as full
campaign qualification. Historical measurements remain valid for their declared
workload and source. The source-preview v0.31.27 is committed locally; publication
was blocked by GitHub credential scope.

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
