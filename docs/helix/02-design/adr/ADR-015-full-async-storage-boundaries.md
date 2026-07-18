---
ddx:
  id: adr-full-async-storage-boundaries
  links:
    - {kind: informed_by, to: prd}
    - {kind: informed_by, to: concerns}
    - {kind: informed_by, to: adr-cqrs-log-projection-storage-model}
    - {kind: informed_by, to: adr-rust-workspace-and-toolchain-policy}
    - {kind: informed_by, to: adr-hexagonal-architecture-and-two-interfaces}
    - {kind: informed_by, to: adr-orthogonal-log-projection-composition}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
  status: accepted
  review:
    self_hash: e38b3eaaa639ae1ccfc43cb7430924e4e5f7a35ad79f38d687a538a22030e680
    deps: {}
    reviewed_at: "2026-07-18T02:29:39Z"
---

# ADR-015: Full-async storage boundaries

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-07-18 | Accepted | Project owner | ADR-003, ADR-007, ADR-012, ADR-013, TD-001 | High |

## Context

TD-001 specifies asynchronous storage calls and ADR-003 selects Tokio for asynchronous I/O, but
ADR-012 and the realized engine retain a synchronous write core: `LogWriter`, `ProjectionWriter`, and
the closure passed to `Backend::write` run under `std::sync::Mutex<Inner<L, P>>`. Native-async stores
therefore require blocking bridges, and blocking stores can run work on a runtime worker unless each
composition root adds its own defensive wrapper. The split is observable as duplicated
`spawn_blocking` logic, eager ready futures, and an inability to integrate an async database without
either blocking a worker or nesting a runtime.

The external operation ports are already future-returning. The unresolved decision is whether the
storage axes and atomic commit boundary remain synchronous internally or become asynchronous through
the complete engine path.

## Decision

All storage-facing engine boundaries will be asynchronous. Domain traits remain runtime-neutral: they
return `Send` futures and expose no Tokio type. A native-async adapter awaits its driver directly; an
in-memory adapter may return an immediately-ready future; a blocking adapter executes the entire
transaction on a bounded blocking executor or owned storage actor below the async port.

The generic user-supplied `Backend::write` closure will not become an async closure. It will be replaced
by typed, backend-owned async operations, including a typed raw commit used by conformance and fault
injection. Backend-owned operations preserve the legal suspension points and transaction lifetime and
prevent arbitrary code from awaiting while it holds a transaction or global lock.

The following rules are normative:

1. No `std::sync::MutexGuard` or borrowed blocking transaction is held across `.await`.
2. Blocking adapters offload one complete begin/apply/commit-or-rollback unit, never individual SQL
   statements from the same transaction.
3. Serialization is per queue or per connection where the substrate requires it; network or object-store
   I/O does not hold a process-global storage lock.
4. Dropping a future before commit leaves no durable effect. Cancellation during commit is an
   unknown-outcome case: the owned commit continues to a determinate result and `request_id` replay
   resolves a lost response.
5. Atomic stores commit log append, projection apply, cursor advance, and replay outcome together.
   Eventual-apply stores durably append first and repair the projection from the log, while preserving the
   API response barrier defined by ADR-013.
6. Backend construction, recovery, snapshots, inspection, repair, deferred apply, and shutdown are async
   when they can perform I/O.
7. Capability accessors that are immutable after construction stay synchronous and never acquire an
   async lock.

Migration is additive at first: explicit async axis traits and compatibility wrappers land beside the
legacy synchronous axes, the reference composition and memory backend move first, blocking adapters move
through explicit wrappers, and native-async adapters implement the new axes directly. The legacy traits,
ready-future shims, and composition-root blocking wrappers are removed only after conformance parity.

## Alternatives

| Option | Pros | Cons | Evaluation |
|--------|------|------|------------|
| Keep synchronous storage axes | Smallest code change; natural fit for rusqlite | Blocks native async drivers and spreads reactor-safety wrappers across composition roots | Rejected |
| Make `Backend::write` accept an arbitrary async closure | Superficially preserves the existing seam | Complex lifetimes and boxing; arbitrary suspension while transaction state is borrowed | Rejected |
| Put every store behind a blocking actor | One uniform call shape | Throws away native async I/O and adds scheduling/failure boundaries | Rejected |
| **Typed async storage operations with explicit sync adapters** | Native async path, controlled cancellation, runtime-neutral ports | Staged migration and temporary dual contracts | **Selected** |

## Consequences

| Type | Impact |
|------|--------|
| Positive | Native async databases and clients no longer require nested runtimes or reactor blocking. |
| Positive | Cancellation and unknown outcomes become testable storage contracts instead of wrapper behavior. |
| Negative | The migration touches the generic composition and every storage adapter. |
| Negative | A temporary compatibility layer remains until all synchronous axes and raw-write tests migrate. |
| Neutral | SQLite, object-log filesystem, and other blocking implementations remain supported through bounded whole-transaction offload. |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Cancellation interrupts a commit after its outcome becomes durable | M | H | Owned commit task plus request-id outcome replay tests at every cancellation cut. |
| Async conversion weakens transaction affinity | M | H | Typed operations; whole-transaction blocking wrappers; no per-statement offload. |
| Global async lock serializes unrelated queues | M | H | Per-queue serialization and queue-density/heartbeat tests. |
| Dual traits persist indefinitely | M | M | Beads include explicit call-site inventory and removal gates. |

## Validation

| Success Metric | Review Trigger |
|----------------|----------------|
| No blocking storage call runs directly on a Tokio worker | A single-thread runtime heartbeat stalls during storage conformance. |
| All cancellation cut tests converge to zero or one committed outcome | Any duplicate, lost accepted mutation, or cursor-ahead state. |
| No legacy sync storage seam remains after migration | `rg` finds `Backend::write`, `LogWriter`, `ProjectionWriter`, or eager wrapper-only storage futures. |
| Existing backend conformance remains green | A backend changes API-001 success, replay, fencing, or response-barrier behavior. |

## Supersession

- **Supersedes**: ADR-012's synchronous `Backend::write(f)` and
  `std::sync::Mutex<Inner<L, P>>` unit-of-work mechanism only. ADR-012's orthogonal axes and atomicity
  requirements remain accepted.
- **Superseded by**: None.

## Concern Impact

- `concurrency-model`: storage suspension and serialization are now explicit async contracts.
- `resilience`: cancellation, timeout, rollback, and unknown-outcome recovery are required conformance
  cases.
- No library practice is overridden; the runtime-neutral port shape preserves ADR-007 dependency
  direction.

## References

- `docs/helix/02-design/technical-designs/TD-001-storage-architecture-backend-contracts.md`
- `docs/helix/02-design/adr/ADR-012-orthogonal-log-projection-composition.md`
- `crates/pqueue-engine/src/port.rs`
- `crates/pqueue-engine/src/compose.rs`
