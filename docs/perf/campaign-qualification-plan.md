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
and must be measured, not assumed free. Three 1 KiB body versions represent
29.3 MiB/sec at 10k and 36.6 MiB/sec at 12.5k before encoding/index/WAL overhead.
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
