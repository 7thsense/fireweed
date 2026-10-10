---
ddx:
  id: contract-formal-queue-semantics
  type: contract
  depends_on: [prd, adr-s3-turso-public-cell, adr-async-commit-strategy-and-dispatch]
  status: accepted
---

# CONTRACT-006: Formal queue semantics

## Domain and authority

This contract defines proof obligations for the current public cell, not a new
API or storage selector. Let q=(tenant,queue), e be a monotonically advancing
storage fence epoch, Lq the ordered authoritative committed command history,
Pq a serving projection, and aq its applied cursor. Log publication means the
protocol's authoritative conditional manifest/head publication, not merely an
uploaded segment. Unknown I/O outcome is neither proven rejection nor success.
Recovery must resolve that outcome from authority before deciding to re-execute.
Physical storage durability and correct atomic conditional publication are
assumptions to be exercised by public S3 qualification, not proven by a model.

## Safety definitions

**F-ACK**: successful mutation acknowledgment implies its exact command envelope
and replay metadata are in Lq. AsyncProjection does not imply aq has advanced.
A claim polls applied rows; an empty poll before catch-up is legal.

**F-PREFIX**: Pq = Fold(snapshot, Lq[snapshot_cursor:aq]) for a validated snapshot
and contiguous committed suffix. Apply advances the cursor and affected state
atomically; repeat apply is idempotent. A projection can lag, but cannot contain
an effect absent from committed authority. Hints never define Lq. Snapshot and
retention lineage must be validated; missing/corrupt authority fails closed.

**F-FENCE**: append publication checks expected_epoch=e in its atomic authority
update. A stale owner may finish previously published work, but cannot publish a
new command after a successor advances e. Serving requires successful acquire,
durable fence confirmation and an unexpired serving ownership session. Lease
assignment epoch and durable storage epoch are distinct domains; cold ephemeral
control-plane restart reconciles them without same-owner self-fencing.

**F-LEASE**: each item has at most one active queue lease. A lease is active when
its lifecycle permits leased work and its token/generation is current and its
expiry exceeds logical now. The caller's owner epoch is independently checked
for mutations; ownership handoff alone must not silently invalidate item leases
unless the existing contract says so. Finalize/renew/reassign validate the exact
current credential and applicable expiry rules. At expiry, a replacement token
cannot be affected by the old token. Already-running external effects are not
queue leases and may overlap after expiry (FR-28).

**F-REPLAY**: key=(q,operation,request_id) with the existing operation-specific
fingerprint. During the configured retention window, equal key/body replays the
stored disposition, including partial/rejected/empty results where required;
different body conflicts without a new durable effect. Claim replay additionally
obeys its active-lease conditions. Retention expiry permits documented reuse;
this is not an eternal exactly-once guarantee. Fingerprint format and operation
namespace follow production, rather than being changed by this definition.

**F-BATCH**: a compatible microbatch has a serial witness respecting per-queue
command order and nonoverlapping completed calls. Its projected per-request
results, selected items, tokens and replay identities equal that witness.
Compatibility includes tenant/queue, explicit fence semantics, mutation kind,
claim-unit, eligibility lane and all existing grouping/cohort constraints.
Cancellation of a result waiter does not remove dispatcher-owned accepted work.
The witness need not expose not-yet-applied durable effects to an empty poll.

**F-BUDGET**: retained serialized jobs own noncloneable permits. Charged global
and tenant bytes equal owned charged allocations and never exceed configured
caps; dropping a caller does not free a job's bytes. Completion, rejection before
ownership transfer and terminal resolved cleanup release exactly once. Crash
forgets volatile allocations/permits together; durable replay debt is a separate
bounded quantity, not an allocation that survives process death.

**F-KEY**: logical `client_item_key` deduplication is separate from request
receipts. Within its configured retention scope, repeated pushes with that key
converge on one logical item according to current conflict policy. A new request
ID does not bypass item-key deduplication; a changed request fingerprint is not
an instruction to create another logical item.

## Progress definitions

For item i, eligible age A_i(t)=integral from its age origin to t of
1[eligible(i,u)] du. Time under lease, not-before/retry delay or a closed gate
is excluded. Owner absence, serving projection lag and caller-specific filters do
not redefine queue-global eligibility or stop its age. Lease expiry resumes accumulated age; explicit recurrence/rearm
uses the governing FR-49..54 origin. Queue progress is global, not per priority
bucket or group. Strict priority and bounded rank error retain FR-7/8 meaning.

**F-EVENTUAL**: under a stable available authoritative store, eventual live owner,
repairable nonpoisoned history and weak fairness of each labeled Apply action (a continuously enabled action must
eventually execute),
accepted deferred commands eventually become applied. A reachable cycle with
permanently outstanding work and every applicable fairness obligation satisfied
is a liveness violation. Arbitrary perpetual crashes/outages do not satisfy
these premises. Poisoned history must remain observable and cannot be skipped.

**F-DEADLINE**: FR-12 requires a claim before eligible age exceeds
progress_bound_ms. Eventual service does not establish this numeric property.
A bounded timed model must declare arrival/admission envelope, service rate,
selection policy, storage/owner recovery bounds and grouping delays. In the
simple unit-service fixture with N eligible items, no arrivals, round-robin
service each tick and initially zero ages, the bound must be at least N ticks.
This fixture is not a proof for production priority/gate/cohort scheduling.
Insufficient assumptions are an unestablished proof obligation, not permission
to weaken the PRD. Workload evidence and risk observability remain required.

## Evidence boundary

Finite exhaustive checking proves properties only over the enumerated model
and declared domains. Production conformance, deterministic adapter simulations,
public S3 tests and process-kill qualification are independent obligations.
Every model report names bounds, assumptions, invariant IDs, transitions, source
hashes and exclusions. Exhaustion of a resource/time/search cap fails closed.

## Finite frontier and cross-boundary obligations

Monotone epoch/history/request bounds close further input at an explicit frontier;
accepted work can still apply, respond, replay or drain. No wrapping or silent
disabling of required work is allowed. Time-bounded models check safety only.
Liveness is per fixed published command, not eventual queue emptiness under
continuing arrivals. Deadlocks include semantic stuttering. Fairness applies to
service actions; an Apply action that executes without advancing remains a fair
but faulty implementation and must yield a counterexample.

Admission models reserve record bytes plus temporary seal-frame capacity and
separately bound dispatcher/running slots and selection waiters. Conservative
reservations may exceed currently materialized bytes; equality is to owned
reservations, while materialized bytes must never exceed their charge.

Required cross-boundary litmus cases are lost claim response → deferred apply →
owner handoff → retained replay, and caller cancellation → publication response
ambiguity → authoritative resolution/recovery. Group/cohort claim units and mixed
per-entry outcomes need explicit refinement fixtures or named production
conformance obligations; an item-only model does not prove those cases.
