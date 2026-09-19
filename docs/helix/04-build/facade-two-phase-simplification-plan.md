# Plan — Two-phase facade (acks + packed batches, not snapshots)

**Status:** public Strict/sync serving barrier removed. The only
`ResponseBarrier` value is `AsyncProjection`. Object-log opens always use
the async apply coordinator (default spec when unset). Mutate still acks
the object-log; same-process `claim` uses unpublished overlay. Public
reads wait coverage. Planning may wait previous apply so leases/unique
keys are visible; that is not a public Strict snapshot. Matrix collapse /
MinIO stay in the peer worktree.

**Peer worktree (do not merge blindly):**
`/home/erik/.herdr/worktrees/fireweed/worktree-clear-stone-f732`
(`worktree/clear-stone-f732`). It collapses the *public* product to
**s3 × turso**, retires other `StorageConfig` selectors in `validate`,
adds `StorageConfig::s3_turso`, and starts process-local MinIO via
`fireweed-objectlog::test_minio`. Durable writes stay on the **object-log**
(`S3CreateOnlyPut` / `LogConfig::S3`), not a raw S3 SDK put from the
facade or workload. When that lands:

- Keep object-log as the S3 axis. Do not add `aws-sdk-s3` calls outside
  `fireweed-objectlog`.
- Re-point this work's filesystem×turso proofs at s3×turso (same
  `DerivedObjectLogTursoBackend`) once `validate` rejects filesystem.
- Product default barrier should be `AsyncProjection` (this plan), not
  `s3_turso()`'s current `Strict`.
- Their `ensure_bucket` belongs on `S3CreateOnlyPut` (already the
  create-only object-log adapter).

**Does not retract** TD-016 (unified packed mutation path), ADR-015 (async
storage boundaries), or ADR-017 (owned-task dispatch, started work survives
caller drop). This plan makes the **public story** match those designs.

## 0. Correction

This is **not** a plan to remove batching, linger, generation packing, or
vector commit. Those are the fast path.

Waiting for Turso apply before `claim` returns is the slow path. It was
rejected in TD-016: the next generation merges an **unpublished overlay**
instead of waiting coverage. This plan keeps that.

Fire-and-forget **commit** is also rejected. Every mutating call still
completes with that request’s outcomes (`Committed` / `Conflict` / stale
lease / ids). A dropped caller still must not cancel work already submitted
(ADR-017).

## 1. The decision

One two-phase protocol. Two completions, not one world-cut.

| Phase | Completes when | Returns | Does not mean |
| --- | --- | --- | --- |
| **Mutate** (`commit`, packed push/update/complete) | This generation is on the **log** (and the in-process overlay is updated) | Per-entry outcomes for **this request** | Every worker’s next `claim` sees new Pending rows |
| **Discover** (`claim` and other Pending polls) | A **batch of currently selectable work** was admitted (possibly empty) | That batch, with leases | A snapshot of the log, or the causal tail of the last `Committed` on another handle |

Contention stays **leases and fences** on selectable rows. It is not solved by
making `claim` wait for apply.

Chained stages on **one process** stay fast by TD-016 overlay: the next packed
claim generation sees this process’s unpublished `client_keys` / `leased_ids` /
`terminal_ids` without waiting WAL writeback.

## 2. Keep (speed)

- One sequencer, same-kind overlay, linger, item/byte caps, packed append
  (TD-016: fill 8 / 800 items / 4 MiB / 20 ms linger as the packed-path
  target; current Turso compose may still linger 10 ms — do not regress
  packing to “wait apply”).
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
  Turso reports `DurabilityClass::EventualApply` for either construction
  barrier. `commit_capabilities` is the visibility document; construction
  knobs that do not change claim must not be documented as if they did.
- Two equal worker products. The crate loop `claim; complete` is a helper
  over `commit`, or it is the same packed write path — not a second
  consistency story. Convenience methods without `RequestId` stay narrower
  bindings (API-005), not a second protocol.
- `wait_request_entry_coverage` on ordinary item claim. Public **reads**
  (`side_record`, `live_item`, `metrics`) may still wait coverage; that is
  a read, not discovery of work.

## 4. Fast chained stages (same process)

Snorri-style enroll → commit → claim next **in one process** is overlay, not
Strict-on-claim.

1. `commit` appends and publishes the unpublished identity delta before the
   generation turn is released (already required so the next claim snapshot
   does not omit selected ids).
2. The following `claim` on that handle/process plans against serving rows
   **plus** overlay. Continuation items from (1) are selectable without
   waiting high-water apply.
3. Other processes and reopen may still lag until apply. They never
   double-lease a row the overlay already marked leased.

Do **not** implement (2) by blocking claim on checkpoint/writeback.

## 5. Tests that prove this (not a Snorri clone)

Native suites today: isolated methods, `instance_fence: None`, no claim of
the continuation. That does not prove the two-phase protocol.

Required on filesystem × Turso (and S3 × Turso when live):

- Mutate always returns per-entry outcomes; no successful fire-and-forget.
- Same-handle: `commit` with fence + continuation, then `claim` receives
  that continuation **via overlay**, with apply still allowed to lag.
- Two workers: one lease on an overlay-visible row; concurrent same-fence
  commits → one `Committed`, rest `Conflict`.
- Empty `claim` after a *other-handle* commit that is not yet applied is
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
   claim-as-poll, overlay for same-process discovery, capabilities as the
   visibility document. No code path change required to land the words if
   they match TD-016 + current Turso compose.
2. **Proof** — tests in §5 on filesystem × Turso; S3 when P1s is up.
3. **Helpers** — `complete` / `retry` / `release` documented as packed
   writes that share the commit sequencer, not as a Strict snapshot API.
4. **Knob collapse** (later, explicit) — stop implying Turso Strict equals
   Atomic serving. Do not remove `AsyncProjectionSpec` bounds; they still
   cap apply debt.

Phase 4 is a docs/construction honesty cut. It is not a packing rewrite.

## 7. Non-goals

- Removing batching, linger, or vectorized commit.
- Public `await_projection` as a worker primitive.
- Process-wide causal claim (every handle sees every commit before apply).
- Reviving SQLite constructors for Snorri.
- Adding enroll/transition/deliver_effect types.

## 8. Snorri (after Fireweed is honest)

- Consume `commit_capabilities.durability_class == EventualApply`.
- Await **commit outcomes**; do not fire-and-forget.
- Next-stage `claim` on the **same process** may rely on overlay once §5
  passes; other processes retry empty claims.
- Stop mapping every `EngineError::Conflict` to `StateFenceConflict`.
- Pin `StorageConfig` + Turso + `projection_control`; no SQLite.

Snorri does not get a faster Fireweed by making claim wait. They get it when
mutate acks quickly (packed log) and claim stays a poll of overlay+applied
work.
