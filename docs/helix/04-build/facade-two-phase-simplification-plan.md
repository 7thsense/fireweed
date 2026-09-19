# Plan — Two-phase facade (acks + packed batches, not snapshots)

**Status:** public Strict/sync serving barrier removed. The only
`ResponseBarrier` value is `AsyncProjection`. Object-log opens always use
the async apply coordinator (default spec when unset). Mutate acks the
object-log. `claim` polls applied Turso rows and may be empty. There is
no process-local unpublished overlay. Public reads wait coverage.
Planning may wait previous apply so leases/unique keys are visible; that
is not a public Strict snapshot. The public product cell is **s3 × turso**
(MinIO via `fireweed-objectlog::test_minio` for local tests). Durable
writes stay on the object-log (`S3CreateOnlyPut` / `LogEngine`), not a
raw S3 SDK put from the facade or workload.

**Does not retract** TD-016 (unified packed mutation path), ADR-015 (async
storage boundaries), or ADR-017 (owned-task dispatch, started work survives
caller drop). This plan makes the **public story** match those designs.

## 0. Correction

This is **not** a plan to remove batching, linger, generation packing, or
vector commit. Those are the fast path.

Fire-and-forget **commit** is also rejected. Every mutating call still
completes with that request’s outcomes (`Committed` / `Conflict` / stale
lease / ids). A dropped caller still must not cancel work already submitted
(ADR-017).

## 1. The decision

One two-phase protocol. Two completions, not one world-cut.

| Phase | Completes when | Returns | Does not mean |
| --- | --- | --- | --- |
| **Mutate** (`commit`, packed push/update/complete) | This generation is on the **log** | Per-entry outcomes for **this request** | Every worker’s next `claim` sees new Pending rows |
| **Discover** (`claim` and other Pending polls) | A **batch of currently selectable work** was admitted (possibly empty) | That batch, with leases | A snapshot of the log, or the causal tail of the last `Committed` on another handle |

Contention stays **leases and fences** on selectable rows.

## 2. Keep (speed)

- One sequencer, linger, item/byte caps, packed append
  (TD-016: fill 8 / 800 items / 4 MiB / 20 ms linger as the packed-path
  target; current Turso compose may still linger 10 ms).
- `commit` / push / complete as **futures that resolve with that call’s
  outcomes**. Coalesce many in-flight callers into one generation; each
  caller still gets its own result.
- Remembered lease tokens so the claiming process can complete/renew without
  waiting Turso apply.
- Multi-worker exclusive claim of a **selectable** row (one lease; fence
  one-winner).
- Background apply bounded by `AsyncProjectionSpec`. Apply debt is not a
  claim stall.

## 3. Stop selling (complexity, not throughput)

- Fire-and-forget mutate (no result, or “submitted” with no `Committed` /
  `Conflict`).
- `claim() -> Vec` as “here is the pending set, consistent with the last
  commit.” Empty claim is a normal poll, not a failed commit and not
  `Conflict`.
- `ResponseBarrier::Strict` on native Turso as “serving is caught up.”
  Turso reports `DurabilityClass::EventualApply`. `commit_capabilities` is
  the visibility document; construction knobs that do not change claim
  must not be documented as if they did.
- Two equal worker products. The crate loop `claim; complete` is a helper
  over `commit`, or it is the same packed write path — not a second
  consistency story. Convenience methods without `RequestId` stay narrower
  bindings (API-005), not a second protocol.
- A process-local unpublished overlay of log-acked rows. `claim` reads
  applied projection rows. Empty claim is the poll when apply has not
  caught up.

## 4. Same-process chained stages

Snorri-style enroll → commit → claim next **in one process** polls applied
rows. The next packed generation may wait for the previous mutation or
claim to apply so unique keys and leases exist in Turso. That is not a
public Strict snapshot and not a second serving copy of unpublished
items.

## 5. Tests that prove this (not a Snorri clone)

Native suites today: isolated methods, `instance_fence: None`, no claim of
the continuation. That does not prove the two-phase protocol.

Required on s3 × Turso (MinIO locally; live S3 when P1s is up):

- Mutate always returns per-entry outcomes; no successful fire-and-forget.
- Same-handle: `commit` with fence + continuation, then `claim` receives
  that continuation from applied rows (empty is a normal poll while apply
  lags).
- Two workers: one lease on a selectable row; concurrent same-fence
  commits → one `Committed`, rest `Conflict`.
- Empty `claim` after an *other-handle* commit that is not yet applied is
  empty or retry, never `Conflict`.
- Packed generation: multiple in-flight commits still coalesce; each
  future still completes with its outcomes.
- Public-interface `exercise_commit` sets a real `InstanceFence` and claims
  the continuation; `None` is not the only commit proof.

Out of scope for this plan: copying Snorri’s DAG/approval-gate suite into
Fireweed. Those stay Snorri external acceptance after the two-phase cell is
honest.

## 6. Phases

1. **Contract** — API-005 / crate rustdoc: EventualApply, mutate-ack,
   claim-as-poll, capabilities as the visibility document.
2. **Proof** — tests in §5 on s3 × Turso.
3. **Helpers** — `complete` / `retry` / `release` documented as packed
   writes that share the commit sequencer, not as a Strict snapshot API.
4. **Knob collapse** — `ResponseBarrier` is `AsyncProjection` only. Do not
   remove `AsyncProjectionSpec` bounds; they still cap apply debt.

## 7. Non-goals

- Removing batching, linger, or vectorized commit.
- Public `await_projection` as a worker primitive.
- Process-wide causal claim (every handle sees every commit before apply).
- Reviving SQLite constructors for Snorri.
- Adding enroll/transition/deliver_effect types.

## 8. Snorri (after Fireweed is honest)

- Consume `commit_capabilities.durability_class == EventualApply`.
- Await **commit outcomes**; do not fire-and-forget.
- Next-stage `claim` on the **same process** polls applied rows; other
  processes retry empty claims.
- Stop mapping every `EngineError::Conflict` to `StateFenceConflict`.
- Pin `StorageConfig::s3_turso` + `projection_control`; no SQLite.

Snorri does not get a faster Fireweed by making claim wait. They get it when
mutate acks quickly (packed log) and claim stays a poll of applied work.
