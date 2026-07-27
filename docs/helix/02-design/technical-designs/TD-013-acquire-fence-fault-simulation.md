---
ddx:
  id: td-acquire-fence-fault-simulation
  depends_on:
    - prd
    - adr-queue-as-shard-unit-and-projection-families
    - adr-log-single-source-of-truth
    - td-sharding-and-shard-ownership
    - tp-scale-substantiation
    - tp-verification-acceptance-criteria
    - build-sp02-deterministic-storage-simulation
  links:
    - {kind: informed_by, to: prd}
    - {kind: informed_by, to: adr-queue-as-shard-unit-and-projection-families}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: td-sharding-and-shard-ownership}
    - {kind: informed_by, to: tp-scale-substantiation}
    - {kind: informed_by, to: tp-verification-acceptance-criteria}
    - {kind: informed_by, to: build-sp02-deterministic-storage-simulation}
  status: proposed
---

# Technical Design: TD-013 Deterministic Acquire-to-Fence Fault Simulation

**Status**: Proposed; test-design authority only.
**Scope**: queue-owner acquire through fence, reassignment, recovery, and completion under load.

## Scope

This proposal extends the retained SP-02 deterministic storage simulation from
enumerated acquire/fence cases to a generated ownership-and-lease state model.
It drives real control-plane, fencing, log, projection, routing, and lifecycle
seams while an independent model supplies exact oracles.

In scope:

- queue-owner acquire, `PendingFence`, storage-fence confirmation, and serving;
- lease issuance, renewal, expiry, finalize, and redelivery;
- owner loss, logical lease expiry, reassignment, stale-route retry, and recovery;
- snapshot plus log-tail rebuild before the new owner serves;
- ordinary concurrent hot/cold queue load under deterministic scheduling;
- replayable seeds, shrinking, typed fault scripts, and exact invariant oracles.

Out of scope:

- production architecture or harness implementation before review;
- deterministic Tokio task scheduling or physical-clock safety decisions;
- intra-queue sharding, scatter-gather claim, or multiple simultaneous owners;
- quiet-host requirements or absolute throughput/latency pass bars;
- replacing TP-003 process-kill tests with a model.

## Governing Alignment

| Authority | Required behavior |
|---|---|
| PRD P0-11..14 | One queue is placed on one owner, reassignment is fenced, and many queues distribute across owners. |
| PRD P0-15 | Shared resources and mutation admission remain bounded under load. |
| PRD FR-9..12 | Queue-global eligible age advances only while eligible and never exceeds the declared progress bound. |
| PRD FR-23..28 | Each item has one lifecycle state, at most one active lease, durable recovery, and at-least-once execution. |
| PRD FR-43 | Active-scope discovery remains advisory, authorized, and bounded; it never leases or reserves work. |
| ADR-008 | The whole queue is the placement and ownership unit; group/cohort members never fan out across owners. |
| ADR-013 | The durable log, not a serving projection or stale route, is state-machine authority. |
| TP-002 E2 | Failover under load preserves exact work, progress, bounded resources, and topology-scoped capacity evidence. |
| TP-003 AC-OWN-2 / AC-ROUTE-1 / AC-TXN-4 | Stale epochs reject, redirects converge, recovery is exact, and accepted work is neither lost nor duplicated. |

## Existing Seams and Residual Gap

SP-02 already provides a runtime-independent model, deterministic seed and
trace renderer, shrinker, phase-addressed scripted `BlobStore`, and production
adapter over real `SegmentedObjectLog`. Its generated suite covers seal,
manifest CAS, epoch-head publication, retention floor, deletion, crash,
restart, and retry.

The current acquire-to-fence coverage is enumerated. It drives real
`InMemoryControlPlane`, engine `acquire_and_fence`, and object log through
failure-before-effect and effect-then-error, exact same-owner retry, lease
expiry/reassignment, stale returned-session rejection, unrelated-queue
progress, and fresh reopen. It does not yet generate interleavings among owner
membership, acquire/fence, data leases, stale routed requests, projection
rebuild, and concurrent load.

This proposal fills only that residual.

## Deterministic State Model

The independent model owns no production types. It represents:

```text
World {
  logical_time,
  live_owners,
  queues: QueueId -> QueueModel,
  routed_clients,
  pending_fault_script,
  trace,
}

QueueModel {
  target_owner,
  control: Unassigned
         | PendingFence { owner, epoch, deadline }
         | Assigned { owner, epoch, deadline }
         | Draining { owner, epoch, deadline },
  storage_epoch,
  durable_log: [Command],
  snapshot_position,
  projection_position,
  items: ItemId -> ItemModel,
  accepted_request_ids,
  progress_bound_ms,
}

ItemModel {
  lifecycle: Pending | Leased | Retry | Complete | Failed,
  eligible_since,
  lease: None | { token, owner_epoch, worker, expires_at },
  command_position,
}
```

The model advances only through explicit operations. Logical time never moves
implicitly. The system under test receives the same operation and scripted
fault disposition, then exposes a normalized observation compared with the
model after every durable cut.

## Operation Alphabet

| Operation | Model transition | Production seam |
|---|---|---|
| `JoinOwner(o)` / `LeaveOwner(o)` | Change live membership and deterministic HRW target | control-plane membership APIs |
| `Resolve(q, client)` | Record authoritative or stale route observation | `resolve_queue_owner` / routing contract |
| `Acquire(q, o)` | Allocate or reaffirm epoch and enter `PendingFence` | `acquire_queue_lease` through `acquire_and_fence` |
| `PublishFence(q, e)` | Raise storage epoch to `e` | object-log fence publication / authority-head CAS |
| `ConfirmFence(q, o, e)` | `PendingFence -> Assigned` only at exact durable epoch | `confirm_queue_lease_fence` |
| `Push(q, request_id, items, e)` | Append accepted items once | owner-scoped mutating port |
| `Claim(q, worker, max, e)` | Lease eligible items atomically | claim port |
| `Renew(q, token, e)` | Extend only the matching active lease | renewal port |
| `Finalize(q, token, outcome, e)` | Commit one legal lifecycle transition | finalize port |
| `AdvanceTime(delta)` | Expire owner/data leases and accrue eligible age | injected logical clock |
| `CrashOwner(o)` | Remove volatile session/projection state | process/session adapter |
| `RestartOwner(o)` | Start non-serving recovery | production reopen path |
| `Recover(q, snapshot, tail)` | Rebuild exact durable state and high-water | snapshot plus log-tail recovery |
| `RetryStaleRoute(op)` | Reissue a captured old route/epoch | client routing adapter |
| `Observe(q)` | No transition | normalized state/metrics snapshot |

Operation generation is precondition-aware but not implementation-aware. It
deliberately emits stale sessions, expired tokens, repeated request IDs, route
misses, and operations during `PendingFence` so fail-closed behavior is tested.

## Durable Fault Cut Points

Use the existing `FaultCutPoint`, `HybridFaultCutPoint`, `RawCommitFault`, and
`FlushPhase` vocabulary. Add a name only when no existing cut denotes the
durable event.

| Cut | Scripted dispositions | Required observation |
|---|---|---|
| before control-plane acquire effect | error / crash | no new epoch or serving session |
| after acquire record, before response | effect-then-error / response loss | durable `PendingFence`; retry resolves exact epoch |
| after acquire, before storage fence | crash / stall | no new-owner serving; old admitted prefix follows TD-003 rule |
| before storage fence effect | error / CAS loss | queue remains non-serving at new epoch |
| after storage fence, before response | effect-then-error | exact reread confirms fence; stale epoch rejects |
| after storage fence, before CP confirm | crash / owner loss | recovery confirms or safely retries exact fence before serving |
| after CP confirm, before first mutation | crash | new owner may recover and reacquire without epoch regression |
| after lease selection, before durable append | crash | no active lease becomes visible |
| after durable lease append, before response | response loss | same request resolves one lease result; no second active lease |
| after append, before serving projection apply | crash / apply error | no success response; recovery replays once before serving |
| after projection apply, before response | response loss | read/replay returns the committed result once |
| during snapshot write / tail replay | crash / truncation / stale list | no partial projection becomes serving or authoritative |
| during owner reassignment with active leases | old-owner stall / new-owner acquire | tokens from the old epoch cannot mutate after the fence |

Fault scripts distinguish failure-before-effect, durable-effect-then-error,
ambiguous create, CAS loss, stale/incomplete list, delayed response, and process
loss. A retry may occur only as an explicit later operation.

## Seeds and Schedules

One 64-bit seed controls operation selection, queue/owner/worker choice,
logical-time deltas, fault disposition, response loss, retry position, and
load interleaving. A failure prints the seed, harness schema, operation index,
full compact trace, model/SUT observations, violated invariant ID, and minimized
trace.

Required campaigns:

| Lane | Seeds | Operations/seed | Queues | Owners | Workers | Purpose |
|---|---:|---:|---:|---:|---:|---|
| focused CI proposal | fixed corpus + 32 generated | 96 | 3 | 3 | 8 | every cut and invariant at low cost |
| release deterministic | 1,024 generated | 512 | 10 | 4 | 64 | deep acquire/fence/retry combinations |
| process replay | minimized corpus only | trace-defined | trace-defined | >=2 | >=8 | real process boundaries for model-found schedules |

Mandatory fixed seeds include `0x5eed`, every historical SP-02 regression
seed, and one seed each for acquire effect-then-error, fence CAS loss,
`PendingFence` owner death, post-fence stale claim, lost claim response,
snapshot crash, and tail-replay crash. Shrinking preserves the violated
invariant and every durable cut needed to reproduce it.

The scheduler is a deterministic operation scheduler, not a Tokio scheduler.
Async completion order enters the trace as an explicit `Complete(op_id)` step.

## Ordinary Concurrent Load

Each ownership trace carries foreground and background work:

- one hot queue receives push/claim/finalize/retry operations on every eligible
  scheduler round;
- at least two cold queues retain eligible work and receive periodic operations;
- unrelated queues continue while one queue stalls at acquire, fence, or
  recovery;
- shared task, connection, pending-operation, and memory counters have declared
  fixed bounds independent of queue count.

Pass/fail uses exact outcomes, monotonic logical progress, queue-global progress
bounds, and bounded resources. Throughput, elapsed recovery time, and latency
percentiles are recorded only as capacity evidence for the declared topology.
There is no quiet-host requirement and no absolute host-speed threshold.

## Invariants and Exact Failure Oracles

| Oracle | Exact pass condition | Authority mapping |
|---|---|---|
| `OWN-1 single_serving_owner` | At most one `Assigned` owner for a queue; its epoch equals CP and storage authority. | ADR-008; TP-003 INV-1/AC-OWN-2 |
| `OWN-2 monotonic_epoch` | Acquisitions by a different owner strictly increase epoch; same-owner reaffirmation never self-fences. | TD-003; AC-OWN-2 |
| `OWN-3 pending_is_nonserving` | No new mutation or claim is admitted from `PendingFence`. | ADR-013; TP-003 AC-TXN-4 |
| `OWN-4 stale_epoch_rejected` | Every post-fence operation carrying an older epoch returns the typed fence/redirect result and appends nothing. | AC-OWN-2; AC-ROUTE-1 |
| `LEASE-1 single_active_lease` | No item has two lease intervals overlapping in logical time. | FR-23..28; INV-1 |
| `LEASE-2 stale_token_no_effect` | Renew/finalize with expired, replaced, or old-epoch token changes no item or log position. | FR-24..28 |
| `DUR-1 accepted_not_lost` | Every acknowledged request is represented exactly once in recovered log/state. | ADR-013; INV-2/INV-10 |
| `DUR-2 unknown_resolves_once` | Same-body retry returns the committed outcome or creates one fresh transition if none committed; never two. | INV-5/INV-14 |
| `VIS-1 response_barrier` | A success response occurs only after authoritative log commit and serving projection visibility. | ADR-013; INV-12 |
| `REC-1 exact_rebuild` | Recovered state checksum, lifecycle counts, leases, replay records, and high-water equal model state at durable head. | AC-TXN-4 |
| `PROG-1 queue_global_bound` | Every eligible item is claimed before model eligible age exceeds `progress_bound_ms`; ineligible intervals do not accrue age. | FR-9..12 |
| `PROG-2 unrelated_queue_progress` | A queue not targeted by the injected stall completes a non-empty operation within its declared scheduler-round bound. | P0-11..15; TP-002 E2 |
| `ROUTE-1 one_hop_convergence` | Wrong-owner route returns the current owner/epoch; the next correctly routed attempt converges or observes a newer authority. | FR-43; AC-ROUTE-1 |
| `SHARD-1 queue_whole` | All item, group, cohort, lease, replay, and progress state for one queue is owned by the same authority record. | ADR-008 |

After every durable cut, `Observe(q)` compares the complete applicable oracle
set. A final terminal drain additionally requires exact accepted, claimed,
finalized, retry, pending, and failed counts; zero lost items; zero duplicate
transitions; zero stale commits; and identical model/SUT checksums.

## Scenario-to-Requirement Matrix

| Scenario | Requirements and decisions | Primary oracles |
|---|---|---|
| acquire effect-then-error and exact retry | P0-11..14, FR-43, ADR-008 | OWN-1..3, ROUTE-1 |
| `PendingFence` crash before storage fence | P0-11..15, ADR-013 | OWN-3, DUR-1, VIS-1 |
| storage fence succeeds; CP confirm response lost | P0-11..14, FR-43, ADR-008/013 | OWN-1..4, ROUTE-1 |
| old owner claims before fence; finalizes after fence | FR-23..28, ADR-008/013 | LEASE-1/2, OWN-4, DUR-1 |
| claim response lost during reassignment | FR-23..28, ADR-013 | LEASE-1, DUR-2, REC-1 |
| snapshot crash plus tail replay on new owner | FR-23..28, P0-11..15, ADR-013 | DUR-1/2, REC-1, VIS-1 |
| stalled hot queue with cold-queue traffic | FR-9..12, P0-11..15 | PROG-1/2 |
| stale route followed by one-hop redirect | FR-43, P0-11..14, ADR-008 | OWN-4, ROUTE-1, SHARD-1 |
| active group/cohort during owner loss | FR-23..28, ADR-008 | LEASE-1/2, SHARD-1, REC-1 |

Every required PRD range is exercised by at least one scenario; no scenario
introduces per-group or downstream-rate progress policy.

## Test Layers and Evidence

| Layer | Target | Evidence |
|---|---|---|
| Pure model | transition preconditions, invariants, stable rendering, shrinking | byte-identical replay; negative controls trip one named oracle |
| Composed deterministic integration | real CP, `acquire_and_fence`, log, projection, routing adapter | per-cut normalized observations and final checksum |
| Process replay | selected minimized schedules against real service processes | kill/restart trace with exact request IDs, epochs, positions, and results |
| Release load | same schedules under TP-002 E2 topology | exact counts/resources plus capacity-only timings |

The model complements TP-003 AC-TXN-4 and the process-kill harness. It cannot
replace real OS process loss, driver/network behavior, or live object-store CAS
evidence.

## Security and Isolation

- Generated operations include cross-tenant route and replay attempts; they
  must return authorization/not-found behavior without revealing owner state.
- Failure traces contain synthetic IDs and hashes, never payloads, lease-token
  material, credentials, or connection strings.
- The independent model keys all authority and replay state by tenant plus
  queue, preventing a passing global-count comparison from hiding isolation
  errors.

## Risks and Stop Conditions

| Risk | Mitigation / stop condition |
|---|---|
| Model copies production bug | Keep model dependency-free; require mutation tests in the adapter/production transition layer. |
| Scheduler claims determinism it does not own | Model async completion explicitly; make no deterministic Tokio claim. |
| Generated traces duplicate SP-02 storage-only coverage | Require every retained trace to cross an ownership, data-lease, route, or recovery boundary. |
| Host timing leaks into pass/fail | Use logical clocks and scheduler rounds; record wall time only as capacity evidence. |
| State space exceeds bounded lanes | Keep precondition-aware generation, fixed CI corpus, invariant-preserving shrink, and longer explicit release campaign. |
| Process replay diverges from model cut | Record exact durable position and map each model cut to a named production boundary before replay. |

## Review and Rollback

**Proposed**. Operator review decides whether generated ownership-level
simulation is worth adding beyond the existing enumerated cases.

If accepted and the future harness cannot prove deterministic replay,
independent mutation sensitivity, or bounded execution, retain only the design
and existing SP-02 enumerated tests. Removing a proposed generated layer must
not weaken TP-003 AC-OWN-2, AC-ROUTE-1, AC-TXN-4, or the process-kill matrix.

No harness code, architecture change, CI wiring, or implementation bead is
authorized or created by this proposal.
