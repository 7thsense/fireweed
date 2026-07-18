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
     state from the log, and enforces ADR-013's response barrier.
2. A runtime-neutral owned-task dispatcher. Mutation admission and the queue gate complete before
   submission. Submission transfers the owned request and commit capability to the dispatcher; the caller
   awaits only a result channel. A dropped caller cannot cancel submitted work.

Async storage axes use `&self`, require `Send + Sync`, and put per-queue or per-connection synchronization
inside adapters. The composition holds a queue-local gate across validation, idempotency planning,
selection, commit, projection visibility, and replay-outcome recording. It does not hold a process-global
storage lock across I/O.

Admission is bounded separately from running capacity. Queue-gate waiters do not consume all running
permits, and keyed gate entries are weak/LRU-reclaimed rather than backed by one permanent task,
connection, or loop per queue. Shutdown closes admission, cancels work that has not been submitted, and
drains submitted tasks within the configured bound.

Immediate memory implementations may use an immediate dispatcher only when the complete typed commit
resolves in one poll. Blocking stores dispatch one whole transaction to a bounded actor/executor. Native
async stores await their drivers inside the owned task.

## Alternatives

| Option | Pros | Cons | Evaluation |
|--------|------|------|------------|
| Sequential async append then apply | Simple generic composition | Cannot provide atomic-store rollback; cancellation can strand append-only state | Rejected |
| One global async mutex around log and projection | Preserves coarse ordering | Serializes unrelated queues and hides adapter concurrency | Rejected |
| Spawn directly with Tokio inside `pqueue-engine` | Straightforward cancellation ownership | Violates runtime-neutral domain boundary and makes embedded runtimes non-portable | Rejected |
| **Injected commit strategy plus owned-task dispatcher** | Makes atomicity, replay ordering, runtime ownership, and capacity explicit | Adds construction types and adapter-specific implementations | **Selected** |

## Consequences

| Type | Impact |
|------|--------|
| Positive | Atomic and eventual-apply profiles cannot be accidentally composed through the wrong commit sequence. |
| Positive | Started-commit cancellation semantics are enforceable without a Tokio dependency in `pqueue-engine`. |
| Positive | Shared receivers and adapter-owned concurrency permit unrelated queues to progress. |
| Negative | Memory, blocking, and native-async profiles require explicit strategy and dispatcher wiring. |
| Negative | The additive migration carries legacy composition until every profile has an explicit strategy. |

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
| Atomic profiles expose only `UnifiedAtomicCommit` | Any atomic profile calls separate append/apply operations. |
| Eventual profiles recover every durable append and preserve the response barrier | Any lost accepted command or read-after-success gap. |
| Submitted commits resolve after caller cancellation | Any started task stops when its response waiter is dropped. |
| A blocked queue does not stop another queue's read or mutation heartbeat | Any process-global storage lock spans awaited I/O. |

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
- `crates/pqueue-engine/src/async_store.rs`
- `crates/pqueue-engine/src/commit.rs`
