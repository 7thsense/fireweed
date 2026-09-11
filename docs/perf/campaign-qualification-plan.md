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
