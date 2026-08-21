# Adversarial review: writer-contention recovery round 26

Review the current plan against round 25 and source; do not implement.

Round 25 was folded by:

- configuring all sixteen pooled readers with 8 MiB caches, retaining the
  128 MiB writer cache and retiring the old reader, for the same 256 MiB
  configured aggregate page-cache ceiling as the current two 128 MiB
  connections; S3r asserts effective values and M evidence records the ceiling;
- adding a four-slot ClaimDriverReadAdmission before the eight-connection driver
  pool, reserving at least four connections for shared Push/Update/Retry/Purge
  validation under saturated Claim traffic;
- bounding Claim-driver-slot and pool acquisition at 5 s with distinct named
  Backpressure, while returning both resources on each retry;
- testing saturated Claim drivers alongside progressing Push, Update,
  Finalize-Retry, and outcome render;
- adding the one-way apply-worker→writer→commit→high-water-notify dependency:
  derived apply never takes earlier resources, derived fence regions never take
  the writer, and writer holders cannot acquire admission, pools, or fence;
- raising S-0 and S3r liveness coverage to all sixteen concurrent readers plus
  live writer/apply;
- making the S2a carrier a defaulted builder with explicit `NonDerived` generic
  callers and a derived source-audit/test gate rather than claiming false
  whole-workspace compile enforcement;
- splitting pool resources/metrics into driver and outcome names and adding the
  Claim-driver-slot name to S2e;
- scoping zero starvation/expiry to fence, drain, and coverage while separately
  counting expected slot/pool Backpressure; and
- bounding each borrowed driver connection's maximum hold at 10.5 s
  (5 s fence acquisition + 5 s drain + 500 ms work), distinct from the 5 s
  acquisition deadline.

Use the same findings table, prior-round audit, verdict, convergence, and
summary contract. Audit the whole plan, not only these deltas. `Convergence:
YES` requires no BLOCKING and no WARNING. If a concern is implementable inside
an existing named acceptance criterion without changing architecture or public
contract, classify it as NOTE.
