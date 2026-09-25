---
ddx:
  id: adr-async-commit-strategy-and-dispatch
  depends_on:
    - adr-full-async-storage-boundaries
    - adr-log-single-source-of-truth
    - api-fireweed-kernel-contract
    - concerns
  links:
    - {kind: informed_by, to: adr-full-async-storage-boundaries}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: api-fireweed-kernel-contract}
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

# ADR-017: Mutations commit log-first through owned-task dispatch

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-09-24 | Accepted | Project owner | API-006, ADR-002, ADR-013, ADR-015, TD-010, TD-016 | High |

## Context

- ADR-015 makes every storage boundary async and requires that a submitted mutation survive its caller's cancellation. An ordinary async block stops when its caller drops the future, so a started commit could be abandoned halfway.
- The S3×Turso implementation has no transaction that spans the object log and the Turso projection. The log commit is the durable boundary (ADR-013), and the projection applies afterwards.
- One node serves many queues (ADR-002). A hot or stalled queue must not stop unrelated queues, and buffered mutation bytes must stay bounded, whatever callers do.

## Decision

1. **Commit is log-first.** A mutation's commands are durably appended to the object log before the Turso projection applies them, and the projection applies them in log order. The projection records its applied position in the same Turso transaction as the applied state, so after a crash the log replays whatever the projection lacks (ADR-013).
2. **Submitted work is owned by the dispatcher.** Submitting a mutation transfers the owned request and its commit capability to a dispatcher-owned task. The caller awaits only a result channel. Dropping the caller cannot cancel submitted work, and a missing receiver discards only the response, never the outcome. The dispatcher is runtime-neutral: the engine has no Tokio dependency.
3. **Planning is serialized per queue, never globally.** Validation, request-id replay, candidate selection and commit planning for one queue run in order under that queue's sequencer (TD-016). No process-global storage lock is held across awaited I/O, so unrelated queues progress.
4. **Admission is bounded in bytes.** Before a mutation enters the pipeline it takes a permit for its serialized bytes from a node-wide budget and, when configured, from a uniform per-tenant budget. A per-queue cap on waiting bytes keeps one stalled queue from taking the node's budget. The permit moves with the request and is released only when the log commit resolves, so caller cancellation cannot free bytes that are still buffered. Exhaustion is `Busy { retry_after }` (API-006 E1), and the production default rejects at once instead of waiting.
5. **Nothing is allocated per queue for good.** Per-queue sequencer and gate entries are reclaimed when idle; there is no permanent task, loop or connection per queue (ADR-002).
6. **Shutdown is bounded.** Shutdown closes admission, cancels work that was not submitted, and drains submitted tasks within a configured bound. A command that became durable before the process ended is resolved by replay on restart, and its caller resolves the lost response by request id or `read` (API-006 E2).

## Alternatives

| Option | Evaluation |
|--------|------------|
| Await the log append and the projection apply in the caller's future | Rejected. A dropped caller could strand a durable append that no one applies or reports. |
| One global async mutex around log and projection | Rejected. It serializes unrelated queues and hides adapter concurrency. |
| Spawning with Tokio inside the engine | Rejected. It breaks the runtime-neutral engine boundary (ADR-015). |
| Waiting for byte capacity while holding the queue's planning order | Rejected. A full budget would stall the whole queue behind one request. |
| **Log-first commit with an owned-task dispatcher and byte admission** | **Selected.** Atomic ordering, cancellation safety and bounded memory are explicit. |

## Consequences

| Type | Impact |
|------|--------|
| Positive | A submitted mutation always reaches a determinate outcome, even if its caller goes away. |
| Positive | Unrelated queues progress while one queue is hot or stalled. |
| Positive | Buffered mutation bytes are bounded per node, per tenant and per queue. |
| Negative | Callers see `Busy` under load and must retry after the hinted delay. |
| Negative | The response waits for the Turso apply after the log commit (TD-010), so commit latency includes the apply; a failed apply poisons the affected queues until replay (TD-010). |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Planning is serialized for only part of a mutation, so two claims select the same record | M | H | Serialize the whole plan per queue; test two concurrent claims. |
| Submitted work outlives shutdown | M | H | Bounded admission, a drain bound, and replay of unresolved outcomes. |
| A hot queue starves unrelated queues | M | H | Per-queue waiting caps and fair scheduling; cross-queue heartbeat tests. |
| A byte permit is released before its bytes leave memory | L | H | The permit moves with the request through every cancellation, fence and publication path; conservation tests. |

## Validation

| Success Metric | Review Trigger |
|----------------|----------------|
| A submitted commit resolves after its caller is dropped | A started task stops when its response waiter is dropped. |
| Every durable append is applied after a crash | A lost acknowledged command, or an applied position ahead of its state. |
| A blocked queue does not stop another queue's reads or writes | A process-global storage lock spans awaited I/O. |
| Byte permits conserve node and tenant charges on every path | A buffered command without a permit, a permit released before its bytes, or charges above a configured cap. |

## Concern Impact

- `concurrency-model`: per-queue planning order, fair bounded dispatch, and no process-global lock across awaited I/O.
- `resilience`: submission is the point after which a mutation always resolves; cancellation and shutdown behavior are explicit.
- `rust-cargo`: the engine stays runtime-neutral and free of unsafe code.

## References

- `docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`
- `docs/helix/02-design/adr/ADR-013-log-single-source-of-truth.md`
- `docs/helix/02-design/adr/ADR-015-full-async-storage-boundaries.md`
- `docs/helix/02-design/technical-designs/TD-010-object-log-turso-projection.md`
- `docs/helix/02-design/technical-designs/TD-016-unified-packed-mutation-path.md`
