---
ddx:
  id: adr-full-async-storage-boundaries
  depends_on:
    - prd
    - concerns
    - adr-cqrs-log-projection-storage-model
    - api-fireweed-kernel-contract
  links:
    - {kind: informed_by, to: prd}
    - {kind: informed_by, to: concerns}
    - {kind: informed_by, to: adr-cqrs-log-projection-storage-model}
    - {kind: informed_by, to: api-fireweed-kernel-contract}
    - {kind: informed_by, to: adr-rust-workspace-and-toolchain-policy}
    - {kind: informed_by, to: adr-hexagonal-architecture-and-two-interfaces}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
  status: accepted
  review:
    self_hash: 26d2c37c96eb0801dbb99e4a02213ecfa747aa533572acde3917801a13cebfcd
    deps:
      adr-cqrs-log-projection-storage-model: 849c0bd7e15200ab056c2e5fcedb4b04a116aba520993fb4bab63b1195146107
      concerns: 52b6bbb92cff001a75227115afb20f4d0a73781ec98f49ab446a6866c17284dc
      prd: 2d97b05f9c0c0db576149bdfef21c729d66e07dbb674c95f6b7135ddcffa3b91
    reviewed_at: "2026-07-20T00:01:22Z"
---

# ADR-015: Full-async storage boundaries

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-07-18 | Accepted | Project owner | API-006, ADR-003, ADR-007, ADR-013, ADR-016, ADR-017 | High |

## Context

- `Store` is async: every op returns a boxed `Send` future (API-006 §4.0). Behind it, the S3×Turso implementation talks to S3, to Turso and to the control plane, all of which do I/O.
- A synchronous storage core under a mutex forces native-async drivers through blocking bridges, lets blocking work run on a runtime worker, and spreads defensive `spawn_blocking` wrappers across composition roots.
- Cancellation has to be safe: a caller may drop its future at any point, and a dropped future must never leave a half-applied change or lose an acknowledged one.

## Decision

Every storage-facing boundary is asynchronous, from `Store` down through the engine's ports to the object log, the Turso projection and the control plane. The contract crate and the engine's ports are runtime-neutral: they return `Send` futures and expose no Tokio type.

- A native-async adapter awaits its driver directly.
- A blocking adapter runs one whole begin, apply and commit-or-rollback unit on a bounded blocking executor or an owned storage actor below the async port.
- The engine exposes typed, backend-owned async operations, never a user-supplied closure that runs inside a transaction. That keeps the legal suspension points fixed and stops arbitrary code from awaiting while it holds a transaction or a lock.

Normative rules:

1. No `std::sync::MutexGuard` or borrowed blocking transaction is held across `.await`.
2. A blocking adapter offloads a complete transaction, never individual statements of one transaction.
3. Serialization is per queue, per partition or per connection, where the substrate requires it. The Turso writer is one connection per file behind a mutex (TD-017 §12). Network and object-store I/O never holds a process-global storage lock.
4. Dropping a future before its mutation is submitted leaves no effect. After submission the mutation continues to a determinate outcome (ADR-017). A lost response is an unknown outcome (API-006 E2), resolved by resending with the request id or by `read` (RQ2, RQ4).
5. The durable log commit comes first, and the projection is applied from the log afterwards and repaired from it after a crash (ADR-013).
6. Construction, recovery, projection control and shutdown are async whenever they can perform I/O.

## Alternatives

| Option | Evaluation |
|--------|------------|
| Synchronous storage ports | Rejected. They block native async drivers and spread reactor-safety wrappers across composition roots. |
| A generic write that accepts an arbitrary async closure | Rejected. Complex lifetimes, boxing, and arbitrary suspension while transaction state is borrowed. |
| Every store behind a blocking actor | Rejected. It discards native async I/O and adds scheduling and failure boundaries. |
| **Typed async storage operations, with whole-transaction offload for blocking stores** | **Selected.** Native async paths, controlled cancellation and runtime-neutral ports. |

## Consequences

| Type | Impact |
|------|--------|
| Positive | Native async drivers need no nested runtime and never block the reactor. |
| Positive | Cancellation and unknown outcomes are testable storage behavior, not wrapper behavior. |
| Negative | Every adapter must state its own serialization and cancellation behavior. |
| Neutral | Blocking implementations still work through bounded whole-transaction offload. |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Cancellation interrupts a commit after its outcome became durable | M | H | Owned commit tasks (ADR-017) and request-id replay tests at every cancellation point. |
| Async conversion weakens transaction affinity | M | H | Typed operations; whole-transaction offload; no per-statement offload. |
| A global async lock serializes unrelated queues | M | H | Per-queue and per-partition serialization, and queue-density heartbeat tests. |

## Validation

| Success Metric | Review Trigger |
|----------------|----------------|
| No blocking storage call runs directly on a Tokio worker | A single-thread runtime heartbeat stalls during the contract suite. |
| Every cancellation-point test converges to zero or one committed outcome | A duplicate, a lost acknowledged mutation, or a projection position ahead of its state. |
| No synchronous storage seam remains | A search finds a closure-based write seam or a sync log or projection writer trait. |
| The contract suite passes on S3×Turso | Any change to success, replay, fencing or visibility behavior. |

## Concern Impact

- `concurrency-model`: suspension points and serialization are explicit async rules.
- `resilience`: cancellation, timeout, rollback and unknown-outcome recovery are required test cases.
- The runtime-neutral port shape keeps ADR-007's dependency direction.

## References

- `docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`
- `docs/helix/02-design/adr/ADR-017-async-commit-strategy-and-dispatch.md`
- `docs/helix/02-design/technical-designs/TD-017-kernel-projection-storage-layout.md`
