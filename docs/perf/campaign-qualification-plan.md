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
The implementation's throughput has not yet been measured or qualified.

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
Python gate/monitor tests passed. Release campaign validation and new capacity
measurement remain pending.

[Regression before](../helix/04-build/evidence/workflow-capacity/campaign-receipt-gc-red.log.gz),
[regression after](../helix/04-build/evidence/workflow-capacity/campaign-receipt-gc-green.log.gz),
[relational suite](../helix/04-build/evidence/workflow-capacity/campaign-receipt-gc-suite.log.gz).
