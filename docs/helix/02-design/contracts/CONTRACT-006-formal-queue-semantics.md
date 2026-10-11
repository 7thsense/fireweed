---
ddx:
  id: contract-formal-queue-semantics
  type: contract
  depends_on: [prd, architecture, api-queue-claim-commit-contract]
  status: accepted
---

# CONTRACT-006: Formal queue semantics

## Domain and authority

This contract defines API-007 proof obligations for both durable backends. Let q=(tenant,queue), Hq be an abstract serialized committed history and Sq its logical state. Hq is a mathematical witness, not a requirement to persist a second command log in PostgreSQL. Nonoverlapping successful operations respect real-time order. Unknown I/O outcome is neither proven rejection nor success; original request identity resolves it before re-execution.

PostgreSQL refines this witness through native atomic transactions over state and receipts. S3/Turso refines it through authoritative conditional log publication and atomic projection-prefix application. Physical provider durability, database atomicity and conditional publication are declared assumptions exercised by real fault tests, not facts established by a finite model.

## Safety definitions

**F-ACK**: a successful mutation is durably committed with its exact replay metadata and has one position in Hq. Every successful later consumer read/claim observes a state covering acknowledged authority. A wait, catch-up or explicit failure is permitted; a stale empty claim is not.

**F-PREFIX**: serving state corresponds to a contiguous prefix of Hq. PostgreSQL state and receipts commit in the same native transaction. S3/Turso state and applied cursor advance atomically from original genesis and a validated contiguous log prefix. Reapply is idempotent; no effect absent from committed authority may appear. Missing/corrupt known authority fails closed.

**F-FENCE**: conflicting writes have a valid serial order and cannot commit with superseded authority. PostgreSQL uses its native transaction/isolation preconditions. S3 publication atomically compares acquired authoritative writer epoch and expected head; recovery uses the matching acquisition result, original genesis and contiguous history. Hints, cached epochs and host placement credentials cannot authorize publication or initialize an empty replacement over known authority.

**F-LEASE**: each item has at most one active queue lease. A lease is active when
its lifecycle permits leased work and its token/generation is current and its
expiry exceeds logical now. Finalize/renew/reassign validate the exact
current credential and applicable expiry rules. At expiry, a replacement token
cannot be affected by the old token. Already-running external effects are not
queue leases and may overlap after expiry (FR-28).

**F-REPLAY**: key=(q,operation,request_id) with the existing operation-specific
fingerprint. During the configured retention window, equal key/body replays the
stored disposition, including partial/rejected/empty results where required;
different body conflicts without a new durable effect. Claim replay additionally
obeys its active-lease conditions. After result retention expires, the identity remains an expired tombstone:
equal content returns RequestExpired and changed content conflicts. Reuse cannot
create a fresh effect. This does not promise exactly-once external execution. Fingerprint format and operation
namespace follow production, rather than being changed by this definition.

**F-BATCH**: a compatible microbatch has a serial witness respecting per-queue
command order and nonoverlapping completed calls. Its projected per-request
results, selected items, tokens and replay identities equal that witness.
Compatibility includes full tenant/queue identity, mutation kind, original request/group identity and all API-007 preconditions.
Cancellation of a result waiter does not remove dispatcher-owned accepted work.
Successful consumer observations cover acknowledged effects in that witness;
projection debt does not authorize an empty poll that hides eligible work.

**F-BUDGET**: retained serialized jobs own noncloneable permits. Charged global
and tenant bytes equal owned charged allocations and never exceed configured
caps; dropping a caller does not free a job's bytes. Completion, rejection before
ownership transfer and terminal resolved cleanup release exactly once. Crash
forgets volatile allocations/permits together; durable replay debt is a separate
bounded quantity, not an allocation that survives process death.

**F-KEY**: full work keys and unique-index identities retain their defined insert/rekey/conflict semantics. Digest equality alone is insufficient. A new request identity cannot bypass work-key uniqueness.

**F-GROUP**: claimed updates, workflow/audit state CAS, newly inserted work and
the immutable committed/rejected group result have one atomic boundary. A rejected
group has zero work/state/new-work effects. Successful independent siblings retain
their own identities across rebatching; infrastructure failure never fabricates a
known successful subset.

**F-CURRENT-REPLAY**: the original ordered claim membership must all retain live
current credentials. Completing/deleting/expiring any member expires the whole
claim replay. Renewal keeps the token and returns current record version/expiry;
creation-version equality cannot reject a valid renewal.

**F-CLOCK**: lease ownership samples a fresh independent operational clock at
linearization after authority acquisition. Operator evaluated_at classifies
administration only. A queued caller timestamp or log sequencing clock cannot
make an expired credential live.

**F-ABA**: a key's version never repeats, including after deletion/reinsertion.
Full unsigned version/sequence and complete namespace/key identity survive every
physical representation, hash collision and restart.

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

## Assurance applicability and invalidation

Migration rigor is precise reviewed specification. Executable analysis is required
for the affected lease/fence/replay/group/admission slices before readiness claims.
Each property needs its actual model configuration and completed result, reachable
success/recovery witnesses, targeted guard-weakening counterexamples and named
code/test correspondence. Excluded routine sequential/document work records why
an analyzer does not apply. No solver installation is implied.

Existing model/corpus results that allowed stale empty claims or expired identity
reuse are superseded for F-ACK/F-BATCH/F-REPLAY and cannot qualify API-007.
F-GROUP/F-CURRENT-REPLAY/F-CLOCK/F-ABA require revised or explicitly mapped models
and implementation oracles. Unchanged property results count only after source,
assumption and correspondence review. Old pass counts are not renewed assurance.
Finite safety exploration is not universal Rust refinement; liveness, provider
conditional-write durability, physical process loss, and numeric progress require
their separately stated assumptions and real implementation evidence.
