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
    self_hash: 97d1032e2b1bbd9ecae2df5daed4350d88364b2bb4d9e7b3c643677f665d8280
    deps:
      adr-async-commit-strategy-and-dispatch: 61bf761b8f8b84581b174eb8f1c64a8893ede0dce9353707fb284f751fb82b5e
      build-slatedb-pattern-adoption-roadmap: 55aa54bb9ccb2fe1d905655831b26e3676590c1a88528bba4d9349f63572ad54
      td-s3-object-log-sqlite-projection-mode: 56d80c3e6ad5ab54460e300fdf4ddfe535dc75a47b0a2a0e32d0de46c38c7e49
    reviewed_at: "2026-07-20T20:00:41Z"
---

# Implementation Plan: SP-01 Global Buffer Byte Admission

## Scope

Implement node-global and optional per-tenant byte admission for unsealed and sealing object-log commands. The existing
request-count capacity and per-queue seal target remain distinct controls. Exclude projection debt budgets,
network body limits, and generic memory accounting.

## Shared Constraints

- Hoist command serialization before admission and pass the serialized representation through the group-commit
  seam. This preserves the existing single-serialization invariant and makes a conservative resident-peak
  charge available. Co-batched requests each reserve their own 25-byte fixed frame overhead, so aggregate
  accounting intentionally overcharges relative to the one merged frame rather than undercounting a peak.
- Put the runtime-neutral budget/waiter/permit types in `fireweed-engine` and inject them per ADR-017; object-log
  adapters own wiring and byte classification without creating an engine-to-objectlog dependency.
- For an already-formed raw commit, `AsyncComposedBackend` requires `PreparedAsyncCommitStrategy`, acquires
  bytes before the queue gate and dispatch, and dispatches only the prepared owned request. The direct
  `SeparateReplayCommitter` fallback uses finite `Reject` inside its owned task; configured waiting occurs in
  the composed pre-dispatch preparation future and is raced with a service deadline. A typed operation
  currently performs authoritative state-dependent planning under its queue gate inside dispatcher-owned
  work, then uses finite non-waiting `Reject` admission there; it never waits for bytes while holding the
  gate. Mandatory typed pre-dispatch preparation remains full-async activation work. A
  non-cloneable permit transfers with the accepted request through the coordinator and buffer; caller
  cancellation after acceptance never releases resident bytes.
- Prevent byte-holding queue-gate waiters from capturing the global budget: impose a per-queue waiting-byte cap
  and release/reacquire permits when a request parks beyond that cap, without changing accepted-request order.
- A single command larger than the hard cap is permanently rejected as `invalid-request`; budget exhaustion or
  wait timeout is retryable typed backpressure.
- No tenant can consume the global reserve once the optional uniform tenant limit is exhausted; unused capacity does
  not strand global capacity unless strict partitioning is explicitly configured.
- Admission waiting is async, cancellation-safe, fair enough to prevent a hot tenant from permanent capture,
  and never holds the queue gate while waiting for bytes.

## Implementation Slices

| Slice | Change | Validation |
|---|---|---|
| 0 | Commit, push, and verify the async/cohort/purge baseline | clean worktree; focused and workspace gates green |
| 1 | Amend TD-004/ADR-017 and TP-002/TP-003 with accounting, errors, metrics, and defaults | HELIX validation; config examples |
| 2 | Hoist serialization and thread pre-serialized commands through coordinator/group-commit seams without behavior change | byte-identical segments; no double serialization |
| 3 | Add runtime-neutral `BufferedByteBudget`, fair waiter, owned permit, tenant classifier, and invariant tests in `fireweed-engine` | proptest/model tests for conservation and cancellation |
| 4 | Integrate permits into explicit prepared admission, finite ordinary trait admission, queue gating, coordinator, and segment buffer | no double charge; exact release on every exit |
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
| Seal success | After segment PUT, manifest CAS, and projection apply complete and drained bytes/temporary frame are freed |
| Projection apply failure after durable seal | After the failed apply/repair response barrier resolves; the coordinator retains the permit through the failure path |
| Worker failure, close, or drain | Owner that drops the final retained serialized bytes releases the permit |

Configuration requires nonzero caps, global cap at least every tenant hard share, global cap at least
`segment_target_bytes`, and consistency with the deployment's maximum command/payload limit. Strict tenant
partitioning is deferred.

## Validation Plan

- [x] Permit conservation holds for generated acquire/release/cancel traces.
- [x] Buffered charged bytes never exceed the global cap and tenant charged bytes never exceed its hard cap.
- [x] Stalled object storage cannot grow retained buffers without bound in deterministic non-quiet tests.
- [x] Peak accounting includes the temporary segment-frame copy.
- [x] Queue FIFO ordering and group-commit batching remain unchanged.
- [x] Low-cardinality snapshots expose current/peak bytes, wait/reject count, wait duration, and configured limits without IDs.
- [ ] Multi-queue contention stays within the roadmap bars; tenant attribution is available only through a
      rate-limited diagnostic event, never a metric label.

## Implementation Record

The production composition root consumes the validated global, uniform-tenant, and queue caps for all five
live object-log profiles: in-memory, SQLite, hybrid, hybrid-strict, and hybrid-async projections. Raw
native-async requests pass through generic `AsyncComposedBackend` preparation and serialize/admit before the
queue gate and dispatcher; the direct lower-level trait fallback uses finite `Reject` inside its owned task.
Equivalent prepared integration for state-dependent typed operations remains full-async activation work. The live same-queue path serializes, reserves its queue share, and
uses non-waiting global admission only after acquiring the coordinator lock, so lock waiters cannot capture
global permits. Both paths transfer canonical bytes into `SegmentedObjectLog` and retain a non-cloneable
permit through seal/manifest CAS and projection apply. The native-async actor returns permits with its durable result so the
coordinator, rather than the actor response future, owns them through repair/apply. Caller cancellation does
not release accepted work; rejection creates no pending coordinator state. All five production-profile
flusher paths use weak backend ownership and exit on backend drop, releasing pending permits; direct
programmatic `Config` is revalidated against its selected segment target in `start()`. Opt-in `[seg]` lines
carry the complete low-cardinality byte snapshot for every profile. Full TP-002 contention and p99
evidence remains the one unchecked validation item and is intentionally separate from portable correctness gates.

## Risks and Rollbacks

Incorrect ownership can leak capacity or admit too much. Encapsulate release in a non-cloneable RAII permit and
assert counters at drain. Roll back by reverting the iteration; existing request-count admission remains.

## Exit Criteria

The implementation exit requires documented, configurable, observable, cancellation-safe caps; deterministic
stress coverage; and the checked-in serialization smoke comparison. The complete TP-002 throughput/p99 matrix
remains a release-evidence gate and is not claimed by this iteration. Seal-target and durable-byte counters
remain; duplicate buffered-byte bookkeeping is centralized behind the admission/accounting helper.
