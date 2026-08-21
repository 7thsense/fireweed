# Adversarial review: writer-contention recovery round 32

Review the current plan against round 31 and source; do not implement.

Round 31 was folded by:

- moving the two-generation/16-request per-queue cap into the mutation
  sequencer shared by direct SelectionFenceAdmission and live KeyedQueueGate
  ingress;
- making the sequencer own exact retained-byte accounting and adding
  cross-ingress capacity tests;
- adding an `SS_INFLIGHT=32` one-queue baseline plus S3s/S5 closed-cohort retry
  lanes that require every original request ID to complete, compare settled
  throughput, and separate capacity rejection, deadline expiry, retry count,
  and original-request age;
- stating one acquisition-bound rule based on 2×p99 and the complete legal
  predecessor hold, then deriving fence 75 s, Claim/shared slot 95 s, and
  sequencer 255 s hard caps from 5 s coverage/work and 30+30 s publication;
- requiring post-slot driver-pool p99 at or below 100 ms with zero expiry;
- specifying one internal attempt for shared mutation and recomputing its
  service ceiling to 505 s;
- bounding Claim precoverage at 5 s, retaining three Claim attempts, and
  recomputing the injected ceiling to 630 s;
- replacing stale Claim protocol constants with 75 s acquisition, 5 s drain,
  and derived/capped 5 s work;
- requiring public reads to wait request-entry high-water before borrowing the
  outcome pool; and
- requiring every candidate mutation to wait both the prior mutation frontier
  and queue-scoped `last_claim` before validation.

Use the same findings table, prior-round audit, verdict, convergence, and
summary contract. Audit the whole plan, not only these deltas. `Convergence:
YES` requires no BLOCKING and no WARNING. If a concern is implementable inside
an existing named acceptance criterion without changing architecture or public
contract, classify it as NOTE.
