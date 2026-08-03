---
ddx:
  id: adr-async-commit-strategy-and-dispatch
  depends_on:
    - adr-full-async-storage-boundaries
    - adr-orthogonal-log-projection-composition
    - adr-log-single-source-of-truth
    - concerns
  links:
    - {kind: informed_by, to: adr-full-async-storage-boundaries}
    - {kind: informed_by, to: adr-orthogonal-log-projection-composition}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: concerns}
  status: accepted
  review:
    self_hash: 61bf761b8f8b84581b174eb8f1c64a8893ede0dce9353707fb284f751fb82b5e
    deps:
      adr-full-async-storage-boundaries: 26d2c37c96eb0801dbb99e4a02213ecfa747aa533572acde3917801a13cebfcd
      adr-log-single-source-of-truth: 35052eb1b94371aa8abb8e8b348a21b459522c7d5feaba04b7146745a04bda62
      adr-orthogonal-log-projection-composition: 778fdbadeadce6b52e101bda39921f88b193c5737ea96d4b8ae8e8a424a4e743
      concerns: 52b6bbb92cff001a75227115afb20f4d0a73781ec98f49ab446a6866c17284dc
    reviewed_at: "2026-07-20T00:01:24Z"
---

# ADR-017: Async composition injects commit strategy and owned-task dispatch

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-07-18 | Accepted | Project owner | ADR-012, ADR-013, ADR-015, TD-001, TD-010 | High |

## Context

ADR-015 requires runtime-neutral async storage, per-queue serialization, atomic-store transactions,
eventual-apply repair, and started commits that survive caller cancellation. Shared-receiver async log and
projection traits are necessary for unrelated queues to progress, but they cannot determine whether two
separate `append().await` and `apply().await` calls form one atomic transaction. An ordinary async block
also stops when its caller future is dropped; a queue gate alone does not preserve started work.

The generic composition therefore needs explicit authority for both the commit mechanism and task
ownership. Inferring either from `durability_class()` produces type-correct implementations that can lose
atomicity or cancellation safety.

## Decision

`AsyncComposedBackend` will receive two explicit construction-time capabilities:

1. A typed commit strategy:
   - `UnifiedAtomicCommit` owns one substrate transaction that commits log append, projection state,
     cursor/frontier, and replay outcome together.
   - `SeparateReplayCommit` is legal only for `EventualApply`; it durably appends first, repairs projection
     state from the log, and enforces the selected public response barrier.
2. A runtime-neutral owned-task dispatcher. Already-formed raw commits complete strategy preparation before
   queue gating and submission. State-dependent typed operations acquire their queue gate before submission,
   but currently perform authoritative planning and finite byte admission inside dispatcher-owned work.
   Submission transfers the owned request and commit capability to the dispatcher; the caller awaits only a
   result channel. A dropped caller cannot cancel submitted work.

Async storage axes use `&self`, require `Send + Sync`, and put per-queue or per-connection synchronization
inside adapters. The composition holds a queue-local gate across validation, idempotency planning,
selection, commit, projection visibility, and replay-outcome recording. It does not hold a process-global
storage lock across I/O.

Admission is bounded separately from running capacity. Queue-gate waiters do not consume all running
permits, and keyed gate entries are weak/LRU-reclaimed rather than backed by one permanent task,
connection, or loop per queue. Shutdown closes admission, cancels work that has not been submitted, and
drains submitted tasks within the configured bound.

For object-log mutations, admission has a second, byte-oriented capability. The generic
`PreparedAsyncCommitStrategy` turns an already-formed raw request into an owned prepared request before
`AsyncComposedBackend::submit_commit` enters its queue gate or dispatcher. Object-log preparation serializes
once, applies the configured finite-reject or async-wait policy, and attaches the resulting permit; unified
atomic strategies use identity preparation. A service selecting async waiting races the composed submission
future with its own deadline. Direct `SeparateReplayCommitter::commit_replayable` remains a finite non-waiting
fallback because callers of that lower-level interface may already hold dispatcher ownership. Typed
operations are different: their state-dependent `RawCommitRequest` does not exist until authoritative
planning under the queue gate. They currently plan under that gate inside dispatcher-owned work, serialize
once, and use finite non-waiting `Reject` admission there; they never wait for byte capacity while holding the
gate. Mandatory typed pre-dispatch preparation remains full-async activation work. The adapter acquires a node-global and
optional uniform tenant-scoped byte permit for the
retained records plus the temporary seal frame, and transfers that non-cloneable permit with the accepted
request. The permit remains owned through queue wait and seal/CAS resolution; after actor submission the
accepted actor job owns it independently of the coordinator response future. Caller cancellation cannot
release resident bytes. A per-queue waiting-byte cap prevents one stalled queue
from capturing the node budget. Runtime-neutral budget futures contain no timer: the service races them with
its runtime deadline and maps timeout/exhaustion to typed retryable backpressure; the finite production
default rejects immediately when the budget is exhausted.

Supported product compositions are native async. Raw generic
`AsyncComposedBackend` submission is structurally prepared before queue gating
and dispatch. State-dependent typed operations plan inside dispatcher-owned work
under their queue gate and use finite non-waiting byte admission; moving that
planning to an equivalent pre-dispatch prepared boundary remains optimization
work, not permission to route the product through synchronous composition or a
process-wide facade pool.

Commit strategy and response barrier are independent construction inputs.
`Strict` is required on every one of the 15 log-by-projection cells and does not
return until the serving projection can satisfy the complete operation result.
`AsyncProjection` is additionally available on the six filesystem/S3 cells: it
may defer a durable projection, but the acknowledged result is synchronously
visible through the serving projection and the deferred state remains bounded,
ordered, replayable, and poison-aware. These are provider-neutral barriers, not
additional projection selectors.

Immediate memory implementations may use an immediate dispatcher only when the complete typed commit
resolves in one poll. Blocking stores dispatch one whole transaction to a bounded actor/executor. Native
async stores await their drivers inside the owned task.

## Alternatives

| Option | Pros | Cons | Evaluation |
|--------|------|------|------------|
| Sequential async append then apply | Simple generic composition | Cannot provide atomic-store rollback; cancellation can strand append-only state | Rejected |
| One global async mutex around log and projection | Preserves coarse ordering | Serializes unrelated queues and hides adapter concurrency | Rejected |
| Spawn directly with Tokio inside `fireweed-engine` | Straightforward cancellation ownership | Violates runtime-neutral domain boundary and makes embedded runtimes non-portable | Rejected |
| **Injected commit strategy plus owned-task dispatcher** | Makes atomicity, replay ordering, runtime ownership, and capacity explicit | Adds construction types and adapter-specific implementations | **Selected** |

## Consequences

| Type | Impact |
|------|--------|
| Positive | Atomic and eventual-apply compositions cannot be accidentally assembled through the wrong commit sequence. |
| Positive | Started-commit cancellation semantics are enforceable without a Tokio dependency in `fireweed-engine`. |
| Positive | Shared receivers and adapter-owned concurrency permit unrelated queues to progress. |
| Negative | Memory, blocking, and native-async adapters require explicit strategy and dispatcher wiring. |
| Negative | The residual facade bridge cannot be removed until every supported cell has explicit runtime-safety and progress evidence. |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Queue gate covers only append/apply, allowing duplicate claim planning | M | H | Gate the complete logical mutation and test two concurrent claims. |
| Submitted work outlives shutdown indefinitely | M | H | Bounded admission, dispatcher drain bound, and unresolved-outcome replay. |
| Hot-queue waiters starve unrelated queues | M | H | Queue gate before running permit or fair keyed scheduling; cross-queue heartbeat tests. |
| Adapter claims atomicity while implementing separate transactions | L | H | Construction-time strategy type and atomic rollback/crash conformance. |

## Validation

| Success Metric | Review Trigger |
|----------------|----------------|
| Atomic compositions expose only `UnifiedAtomicCommit` | Any atomic composition calls separate append/apply operations. |
| Eventual compositions recover every durable append and preserve the response barrier | Any lost accepted command or read-after-success gap. |
| Submitted commits resolve after caller cancellation | Any started task stops when its response waiter is dropped. |
| A blocked queue does not stop another queue's read or mutation heartbeat | Any process-global storage lock spans awaited I/O. |
| Buffered-byte permits conserve global and tenant charges through every cancellation/fence/CAS path | Any retained serialized command has no permit, a permit is released before its bytes, or charged bytes exceed a configured cap. |

## Supersession

- **Supersedes**: None. This specializes ADR-015's accepted async-boundary decision.
- **Superseded by**: None.

## Concern Impact

- `concurrency-model`: shared receivers, queue-local critical sections, fair bounded dispatch, and no
  process-global awaited storage lock are mandatory.
- `resilience`: submission is the queued-to-started durability boundary; cancellation and shutdown
  behavior are explicit.
- `rust-cargo`: the engine remains runtime-neutral and unsafe-free.

## References

- `docs/helix/02-design/adr/ADR-015-full-async-storage-boundaries.md`
- `docs/helix/02-design/technical-designs/TD-001-storage-architecture-backend-contracts.md`
- `docs/helix/02-design/technical-designs/TD-010-object-log-turso-projection.md`
- `docs/helix/02-design/contracts/API-005-fireweed-rust-facade.md` — binds
  queue-local serialization and cross-queue progress to the single public
  `Fireweed` type; process-wide blocking dispatch is not the product model.
- `crates/fireweed-engine/src/async_store.rs`
- `crates/fireweed-engine/src/commit.rs`
