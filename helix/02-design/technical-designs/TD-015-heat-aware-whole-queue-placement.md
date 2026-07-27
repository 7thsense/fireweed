---
ddx:
  id: td-heat-aware-whole-queue-placement
  depends_on:
    - product-vision
    - prd
    - adr-queue-as-shard-unit-and-projection-families
    - td-sharding-and-shard-ownership
    - tp-scale-substantiation
    - tp-verification-acceptance-criteria
  links:
    - {kind: informed_by, to: product-vision}
    - {kind: informed_by, to: prd}
    - {kind: informed_by, to: adr-queue-as-shard-unit-and-projection-families}
    - {kind: informed_by, to: td-sharding-and-shard-ownership}
    - {kind: informed_by, to: tp-scale-substantiation}
    - {kind: informed_by, to: tp-verification-acceptance-criteria}
  status: proposed
---

# Technical Design: TD-015 Heat-Aware Whole-Queue Placement

**Status**: Proposed; operator review required before any architecture or implementation change.
**Disposition**: No architecture change recommended.
**Scope**: placement-policy evaluation for whole queues only.

## Scope

This proposal evaluates static highest-random-weight (HRW) placement, weighted
HRW, and heat-aware whole-queue relocation. It preserves ADR-008: one whole
queue has exactly one active owner, and streams exceeding one owner's capacity
partition across multiple queues at the application layer.

In scope:

- deterministic queue-to-owner selection across a live owner set;
- measured owner capacity and queue demand as advisory placement inputs;
- relocation stability, overload behavior, noisy-neighbor isolation, and
  control-plane failure;
- the existing TD-003 drain, acquire, durable fence, recovery, and resume path.

Out of scope:

- intra-queue splitting or item-level placement;
- cross-owner scatter-gather claims or cross-owner progress aggregation;
- physical-clock safety decisions; elapsed sampling windows are evaluation
  inputs only, never lease, epoch, or append-fence authority;
- API, schema, configuration, telemetry-contract, deployment, or source changes;
- implementation beads before operator review.

## Decision

**Recommendation**: retain static HRW as the settled placement policy. Do not
amend ADR-008 or TD-003 from this evaluation.

Weighted HRW is the first fallback to evaluate if declared owner-capacity
classes become necessary. It preserves deterministic placement and bounded
movement when owner weights are slow-changing control-plane facts. Heat-aware
relocation is not recommended until replayable TP-002 E2 evidence proves that
it improves the worst-owner load and cold-queue progress without violating the
movement, recovery, or progress budgets below.

Queue heat is an observation, not authority. It may nominate a different
target owner; it cannot grant ownership, advance an epoch, expire a lease, or
permit an append. Every accepted move still uses TD-003's existing sequence.

## Governing Alignment

| Authority | Constraint preserved |
|---|---|
| Product Vision: Scale readiness | Horizontal scale distributes queues across independent owners while preserving queue-global progress, exact recovery, and bounded shared resources. |
| PRD P0-11 | A 10M-item queue remains writable, claimable, observable, and exactly recoverable on its declared one-owner topology. |
| PRD P0-12 | Active-scope discovery remains advisory; it does not reserve work or choose ownership. |
| PRD P0-13 | The queue remains the unit of sharding; a logical stream needing more throughput uses multiple application queues. |
| PRD P0-14 | At least 1,000 cold queues plus one hot queue make progress through bounded shared pools. |
| PRD FR-43 | A hot queue cannot prevent another queue from progressing within its configured bound. |
| ADR-008 | One whole queue has one owner; no intra-queue split, scatter-gather claim, resharding, or client-visible placement key is introduced. |
| TD-003 | Target and active owner remain distinct; graceful drain and the single authoritative epoch fence govern reassignment. |
| TP-002 E2 / TP-003 AC-E2E-6 | Exact outcomes, one-owner isolation, cold-queue progress, bounded resources, and same-run degradation are the portable gates; rates are topology-bound capacity evidence. |

## Current State and Residual Question

`crates/fireweed-engine/src/control_plane.rs` implements a stable unweighted
`rendezvous_weight` and selects the greatest score over live owners. Owner
membership is heartbeat-filtered. The reference implementation records no
capacity weight or queue-demand input. TD-003 already defines the authority
record, graceful drain, epoch fencing, recovery, and bounded per-node
coordination; it explicitly leaves cross-queue capacity placement as P1.

The residual question is narrow: does changing only the target-selection policy
produce enough isolation benefit to repay extra queue movement and recovery?
There is no evidence yet that static HRW fails FR-43 when the existing bounded
shared pools and per-queue progress scheduling are working. A hot queue that
exceeds one owner's capacity also cannot be repaired by placement alone.

## Candidate Comparison

| Candidate | Inputs | Determinism and movement | Overload response | Decision |
|---|---|---|---|---|
| Static HRW | queue key, stable owner identity, live set | Deterministic; membership changes move only the departed/new owner's share | No demand reaction; relies on bounded shared pools and app-level multi-queue partitioning | Retain |
| Weighted HRW | static-HRW inputs plus slow-changing positive capacity class | Deterministic for one versioned owner set and weight vector; weight changes move a bounded but measured share | Places proportionally more queues on larger owners; cannot distinguish hot from cold queues | First experimental fallback |
| Heat-aware relocation | static-HRW baseline plus sampled queue demand, owner saturation, recovery cost, and progress risk | Deterministic only from a versioned measurement snapshot; feedback can oscillate and move otherwise-stable queues | Can move a hot whole queue to spare capacity, but cannot add capacity within that queue | Reject pending evidence |

Static HRW is the control arm. Weighted HRW must use a mathematically weighted
rendezvous score rather than multiplying the current hash by a weight, which
would bias the distribution incorrectly. Exact score encoding belongs in a
reviewed implementation design if this proposal is later accepted.

## Evaluation Signals

All signals are recorded per declared topology and compared within one run.
They are not new public telemetry or configuration contracts.

| Signal | Evaluation meaning | Required treatment |
|---|---|---|
| Queue service demand | Completed push/update/claim/finalize work plus pending work over the observation window | Use exact operation counts and backlog change; do not infer demand from backlog alone. |
| Owner saturation | Maximum of bounded worker, connection, pending-operation, memory, and storage-service utilization | Normalize each resource to the run's declared bound; reject missing or unbounded denominators. |
| Progress pressure | Oldest eligible age divided by the queue's `progress_bound_ms` | Treat `>= 1` as an existing product violation, not a placement-success opportunity. |
| Recovery cost | Snapshot/tail bytes, commands replayed, and time observed for the same queue/backend profile | Capacity evidence only; a proposed move must fit the remaining progress budget. |
| Movement cost | Drains, epoch advances, recoveries, redirected requests, and unavailable-claim intervals | Count exactly per queue and owner generation. |

The evaluation uses a short window for response and a long window for
stability: consecutive 5-minute samples summarized over 30 minutes. These are
experimental observation intervals, not safety clocks. Traces use recorded
sample ordinals so an identical input snapshot produces an identical decision.

## Measurable Decision Criteria

A policy may replace static HRW only if every criterion passes in the same
replayable TP-002 E2 workload over at least the canonical 2-, 4-, and 8-owner
topologies:

1. **Correctness floor**: zero lost or duplicate accepted work, zero concurrent
   active owners per queue, zero stale-epoch commits, and exact lifecycle counts.
2. **Progress floor**: every hot and cold queue remains within its declared
   progress bound; no policy may trade a cold-queue violation for aggregate
   throughput.
3. **Isolation improvement**: the candidate reduces the worst-owner normalized
   saturation or worst cold-queue same-run degradation by at least 20% versus
   interleaved static-HRW controls in three consecutive 5-minute samples.
4. **Destination headroom**: predicted destination saturation remains below 60%
   after adding the queue's long-window demand; the source must exceed 80%.
5. **Movement bound**: membership-stable movement stays at or below 1% of active
   queues per 30-minute window, and a queue moves at most once in that window.
6. **Recovery budget**: measured p99 drain plus recovery capacity stays below
   25% of the queue's remaining progress budget at nomination time.
7. **Stability**: after a move, the source/destination decision must not reverse
   for the next six 5-minute samples. Any reversal fails the candidate run.
8. **Resource bound**: sampling and decision work stays in one bounded shared
   control-plane pass; it creates no per-queue task, connection, or unbounded
   history.

The percentages are proposal gates for evaluation, not product promises. A
future accepted design must justify or replace them from recorded evidence.

## Technical Approach

### Placement and Rebalance Decision

#### Static HRW

Resolve the target from the queue key and live owner identities. Owner joins or
leaves are the only automatic placement changes. Existing TD-003 target-versus-
active behavior handles any resulting handoff.

#### Weighted HRW experiment

Use versioned, slow-changing capacity classes that are identical for every
resolver observing one owner-set revision. A weight update is treated like an
owner-set placement revision and is rate-limited by the movement bound. A
missing, stale, zero, or non-finite weight makes the owner ineligible for the
experiment or falls back to equal weight; it never affects an active lease.

#### Heat-aware experiment

A queue becomes a relocation candidate only when all decision criteria have
complete observations and the source-overloaded/destination-headroom condition
holds for three consecutive short windows. Choose among eligible destinations
by the weighted-HRW order, not by a centralized bin-packing search. This keeps
the proposed result reproducible and avoids a second routing authority.

Hysteresis consists of the three-window admission condition, the 30-minute
per-queue cooldown, the movement budget, and the reversal failure gate. A
queue with incomplete data, an active drain, recovery in progress, or less than
the recovery budget remaining before its progress bound is not moved.

## Overload and Noisy-Neighbor Behavior

- If one owner is overloaded but no destination passes the headroom and recovery
  gates, freeze placement and surface insufficient capacity. Do not churn.
- If one queue alone exceeds every owner's capacity, placement has no valid
  remedy. Preserve ownership and require application-level partitioning across
  multiple queues per ADR-008.
- Per-node bounded workers, connections, tasks, and fair queue scheduling remain
  the first FR-43 defense. Placement is not allowed to replace those controls.
- Hot-queue relief cannot consume a cold queue's progress budget. The worst cold
  queue is a first-class gate, not an aggregate percentile hidden by hot work.
- Backlog size alone never triggers relocation: an idle large backlog may be
  cold, while a small rapidly changing queue may be hot.

## Fencing Sequence

Any future policy may nominate only `target_owner`. It must use this unchanged
TD-003 sequence:

1. Record the desired target while the current active owner and epoch remain
   authoritative.
2. Begin graceful drain; stop new claims and allow bounded finalization.
3. Release or expire the prior owner lease.
4. Acquire a strictly greater epoch on the nominated owner.
5. Durably fence that epoch in the log before serving or appending.
6. Recover snapshot plus log tail and confirm the serving state.
7. Resume claims; stale routes redirect and stale epochs fail closed.

Heat, capacity weights, sample time, backlog age, and HRW score never skip or
authorize a step. There remains exactly one durable fencing authority:
`assignment_epoch`.

## Control-Plane Failure Modes

| Failure | Required behavior |
|---|---|
| Missing or stale heat/capacity snapshot | Keep the current active lease and static-HRW target; do not initiate a placement move. |
| Resolvers observe different measurement revisions | Reject the candidate decision and fall back to the last common static-HRW owner-set revision. |
| Decision pass unavailable | Existing owners continue under live leases; acquisitions and renewals follow TD-003 fail-closed behavior. No heat-driven move starts. |
| Failure during drain | Lease expiry and epoch fencing preserve safety; progress risk remains observable. |
| Failure after acquire but before durable fence | New owner remains non-serving; retry/recovery must confirm the exact epoch before use. |
| Metrics appear healthy while a product invariant fails | The invariant failure wins; placement is not reported successful. |

## Component Changes

None. This is a proposed evaluation and a no-change architecture disposition.
`crates/fireweed-engine/src/control_plane.rs`, control-plane persistence, public
contracts, schemas, configuration, and deployment remain unchanged.

## API/Interface and Data Model Changes

None. Signal names and thresholds in this document define an offline evidence
matrix only. They do not define fields, metrics, endpoints, flags, tables, or
operator controls.

## Security

- Placement observations remain tenant-safe operational aggregates; a resolver
  must not expose queue identifiers or workload shape across tenant boundaries.
- Untrusted queue load cannot grant ownership or weaken authorization.
- Checked normalization rejects invalid capacity denominators and non-finite
  values instead of selecting an arbitrary owner.
- Epoch fencing and per-queue authorization remain the security boundary for
  zombie or spoofed owners.

## Testing

No implementation test is authorized. A future reviewed experiment must add an
offline deterministic policy comparator that replays identical TP-002 E2 traces
through all three candidates and records:

- exact assignment and movement history by sample ordinal;
- exact ownership, epoch, lifecycle, and progress outcomes;
- hot and worst-cold queue degradation against interleaved static controls;
- owner saturation, recovery work, redirects, and unavailable-claim intervals;
- identical decisions on repeated input and safe fallback for incomplete data.

Adversarial traces cover one dominant queue, alternating hot queues, owner join
and leave, unequal capacity classes, delayed observations, conflicting snapshot
revisions, drain failure, acquire-to-fence failure, and insufficient cluster
capacity. Wall-clock rates remain declared-topology capacity evidence.

## Migration, Rollback, and Sequence

There is no migration or rollout. Static HRW remains active, so rollback is not
applicable. If the operator later accepts an architecture change, the next
artifact must amend ADR-008 or TD-003 first, define any exact interfaces in a
Contract, and establish a test plan before implementation work is filed.

## Risks

| Risk | Prob | Impact | Mitigation |
|---|---|---|---|
| Heat feedback oscillates ownership | H | H | Retain static HRW; require consecutive windows, cooldown, movement cap, and reversal gate before reconsideration. |
| Relocation worsens progress during recovery | M | H | Recovery must fit a bounded fraction of remaining progress budget; otherwise freeze placement. |
| Capacity weights become stale or incomparable | M | M | Version the evidence snapshot and fall back to equal-weight static HRW. |
| Placement hides deficient shared-resource fairness | M | H | FR-43 cold-queue progress and bounded-pool gates remain mandatory under every policy. |
| Operators infer single-queue horizontal scale | M | H | State the ADR-008 ceiling: one queue uses one owner; partition logical streams across queues. |

## Operator Review Gate

This artifact recommends no architecture change. It authorizes no source,
schema, API, configuration, deployment, or implementation-bead change. Any
future weighted or heat-aware policy requires operator acceptance of an ADR-008
or TD-003 amendment supported by the complete evidence matrix above.
