# Adversarial review: writer-contention recovery round 34

Review the current plan against round 33 and source; do not implement.

Round 33 was folded by:

- moving Claim/shared driver-cap checks to true driver ingress: new Claim
  compatibility bucket creation/direct Claim admission and new mutation
  generation admission, before queue turns are retained;
- retaining the 8-Claim/24-shared two-wave caps but adding above-cliff lanes at
  nine Claim queues and twenty-five mutation queues; accepted work must progress,
  rejected work must have no effect, and fixed-cadence retry must complete all
  original IDs;
- explaining why the 1,024 Claim caller cap remains meaningful: compatible
  callers attach to admitted drivers, while the eight limit applies only to new
  driver buckets;
- adding symmetric S0/S3s/S3m/S5 four-compatibility-key same-queue Claim
  overload cohorts and the same 2,021.075 s closed-cohort ceiling;
- adding eight-active/eight-queued OutcomeReadAdmission after request-entry
  coverage, with 10 s admission, 5 s work, structural pool p99 <=100 ms, and a
  seventeen-reader above-cliff lane completing within 31.05 s;
- replacing sequencer byte budgets and the oversize loan with zero-copy request
  references, fixed descriptors, active-generation-only rendering, and the
  existing one-request run-alone response path;
- adding S3i: a 1,024-ID deduplicated round-robin callerless reclaim retry queue
  with 10 ms exponential-to-1 s cadence, page isolation, no per-queue task, and
  eventual drain under same-queue saturation;
- separating the 505 s driver-service ceiling, 2,021.075 s closed-cohort
  ceiling, and 4,040 s eight-round suffix-request ceiling;
- bounding and metering transitive KeyedQueueGate delay at 505 s;
- removing the residual wait-before-return on one-attempt failure;
- recording admitted-service p50/p95/p99 in S0;
- making S3m take the real fence only on isolated shadow queues;
- including worst-case reservation split rounds in the 5 s work derivation; and
- adding S3i to the slice and issue dependency graphs.

Use the same findings table, prior-round audit, verdict, convergence, and
summary contract. Audit the whole plan, not only these deltas. `Convergence:
YES` requires no BLOCKING and no WARNING. If a concern is implementable inside
an existing named acceptance criterion without changing architecture or public
contract, classify it as NOTE.
