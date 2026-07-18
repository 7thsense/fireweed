---
ddx:
  id: build-sp01-global-buffer-byte-admission
  depends_on: [build-slatedb-pattern-adoption-roadmap, td-s3-object-log-sqlite-projection-mode, adr-async-commit-strategy-and-dispatch]
  links:
    - {kind: part_of, to: build-slatedb-pattern-adoption-roadmap}
    - {kind: informed_by, to: td-s3-object-log-sqlite-projection-mode}
    - {kind: verified_by, to: tp-scale-substantiation}
    - {kind: verified_by, to: tp-verification-acceptance-criteria}
  review:
    self_hash: 6211670110ed7f75c2ffb82a3ba5bde0aad9573d7a1963266b93a2b42065a8f1
    deps:
      adr-async-commit-strategy-and-dispatch: 1e09351095c93363b86f817a1d668adff957393f704eb8850d67894870d0919a
      build-slatedb-pattern-adoption-roadmap: 5f066b91ba58eec79c056ec7cd1922682dbfb5d8f0607920d34273661350a196
      td-s3-object-log-sqlite-projection-mode: f77b249de99163d5b3031b174f2ff1a7833b45d1a68646a1a9da206e847a5fd0
    reviewed_at: "2026-07-18T16:20:32Z"
---

# Implementation Plan: SP-01 Global Buffer Byte Admission

## Scope

Implement node-global and optional per-tenant byte admission for unsealed and sealing object-log commands. The existing
request-count capacity and per-queue seal target remain distinct controls. Exclude projection debt budgets,
network body limits, and generic memory accounting.

## Shared Constraints

- Hoist command serialization before admission and pass the serialized representation through the group-commit
  seam. This preserves the existing single-serialization invariant and makes exact charge bytes available.
- Put the runtime-neutral budget/waiter/permit types in `pqueue-engine` and inject them per ADR-017; object-log
  adapters own wiring and byte classification without creating an engine-to-objectlog dependency.
- Acquire bytes before the queue gate and dispatch. A non-cloneable permit transfers with the accepted request
  through the coordinator and buffer; caller cancellation after acceptance never releases resident bytes.
- Prevent byte-holding queue-gate waiters from capturing the global budget: impose a per-queue waiting-byte cap
  and release/reacquire permits when a request parks beyond that cap, without changing accepted-request order.
- A single command larger than the hard cap is permanently rejected as `invalid-request`; budget exhaustion or
  wait timeout is retryable typed backpressure.
- No tenant can consume the global reserve once its configured share is exhausted; unused tenant shares do
  not strand global capacity unless strict partitioning is explicitly configured.
- Admission waiting is async, cancellation-safe, fair enough to prevent a hot tenant from permanent capture,
  and never holds the queue gate while waiting for bytes.

## Implementation Slices

| Slice | Change | Validation |
|---|---|---|
| 0 | Commit, push, and verify the async/cohort/purge baseline | clean worktree; focused and workspace gates green |
| 1 | Amend TD-004/ADR-017 and TP-002/TP-003 with accounting, errors, metrics, and defaults | HELIX validation; config examples |
| 2 | Hoist serialization and thread pre-serialized commands through coordinator/group-commit seams without behavior change | byte-identical segments; no double serialization |
| 3 | Add runtime-neutral `BufferedByteBudget`, fair waiter, owned permit, tenant classifier, and invariant tests in `pqueue-engine` | proptest/model tests for conservation and cancellation |
| 4 | Integrate permits into pre-dispatch admission, queue gating, coordinator, and segment buffer | no double charge; exact release on every exit |
| 5 | Add cross-queue/tenant stress, close/drain, oversize, and stalled-store tests | bounded resident bytes; unrelated tenant progress |
| 6 | Benchmark request-count-only baseline against byte budget at small/target/oversize payloads, including serialization paid before rejection | throughput and p99 bars from roadmap |

## Issue Decomposition

After the slice-0 gate, land docs/config, serialization refactor, reusable budget, integration, then stress/perf
cleanup. Do not combine this with metadata, retry, or segment-format changes. Reuse the cancellation-safe waiter
registration/de-registration pattern already used by `KeyedQueueGate` rather than creating a second idiom.

### Permit release audit

| Exit | Release point |
|---|---|
| Rejected before acceptance / cancelled while waiting | Admission future removes waiter; no coordinator ownership |
| Accepted then queued, requeued for pre-repair, or caller dropped | No early release while bytes remain resident; if abandon removes the final request/bytes, its owned permit releases by RAII |
| Epoch fence or watermark self-fence | When seal returns and drained/cleared bytes are no longer retained |
| Same-epoch manifest CAS loss/conflict | When seal returns after orphan handling; not at seal start |
| Seal success | After segment PUT and manifest CAS complete and drained bytes/temporary frame are freed |
| Projection apply failure after durable seal | Release at seal completion; apply ownership carries no command bytes |
| Worker failure, close, or drain | Owner that drops the final retained serialized bytes releases the permit |

Configuration requires nonzero caps, global cap at least every tenant hard share, global cap at least
`segment_target_bytes`, and consistency with the deployment's maximum command/payload limit. Strict tenant
partitioning is deferred.

## Validation Plan

- [ ] Permit conservation holds for generated acquire/release/cancel traces.
- [ ] Buffered charged bytes never exceed the global cap and tenant charged bytes never exceed its hard cap.
- [ ] Stalled object storage cannot grow retained buffers without bound.
- [ ] Peak accounting includes the temporary segment-frame copy or reserves one maximum in-flight segment.
- [ ] Queue FIFO ordering and group-commit batching remain unchanged.
- [ ] Metrics expose current/peak bytes, wait/reject count, wait duration, and configured limits without IDs.
- [ ] Multi-queue contention stays within the roadmap bars; tenant attribution is available only through a
      rate-limited diagnostic event, never a metric label.

## Risks and Rollbacks

Incorrect ownership can leak capacity or admit too much. Encapsulate release in a non-cloneable RAII permit and
assert counters at drain. Roll back by reverting the iteration; existing request-count admission remains.

## Exit Criteria

The byte caps are documented, configurable, observable, cancellation-safe, stress-tested, and within the
roadmap performance bars. Seal-target and durable-byte counters remain; duplicate buffered-byte bookkeeping is
centralized behind the admission/accounting helper.
