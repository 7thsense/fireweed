---
ddx:
  id: td-experimentation-surface
  depends_on:
    - adr-log-single-source-of-truth
    - adr-orthogonal-log-projection-composition
    - td-sharding-and-shard-ownership
    - td-s3-object-log-sqlite-projection-mode
    - td-queue-history-change-records
  review:
    self_hash: 68800e79b6e8e458ebfe383e2a34855c9fd408df9ab328225d8946f3e1585655
    deps:
      adr-log-single-source-of-truth: 35052eb1b94371aa8abb8e8b348a21b459522c7d5feaba04b7146745a04bda62
      adr-orthogonal-log-projection-composition: 46327f801156492ee0a1ad0038b730dea7fcef4ebe00641e8f7d9d5f86f8b3f2
      td-queue-history-change-records: 1a69a5ebd1be38b7f17c3be7a1f1319dc6111581fc905fec2c7a894bb3b77bf0
      td-s3-object-log-sqlite-projection-mode: f77b249de99163d5b3031b174f2ff1a7833b45d1a68646a1a9da206e847a5fd0
      td-sharding-and-shard-ownership: b3983f017f7907e900d79cfb08a8cd7ff66786835e66c5d2c1a87589a9db57db
    reviewed_at: "2026-07-11T00:59:47Z"
---

# TD-009: Experimentation surface — pause, branch-at-position, read-as-of-position

**Status**: Draft
**Decision authority**: ADR-013 (log as single source of truth)
**Motivation**: "What if I changed the delivery workflow from x to y?" — pause a live queue, branch it
at a position, run the variant against the branch, compare, discard.

## 1. Pause (harden the existing verb)

Pause already exists as a durable log command: `QueueCommand::PauseQueue`/`ResumeQueue`
(`crates/pqueue-engine/src/command.rs:59-60,584-585`); the projection carries `paused: bool`
(`crates/pqueue-projection/src/lib.rs:165`), `select_eligible` returns empty while paused (`:1887`),
and the flag round-trips through `ProjectionImage` (`:1266,1286`). This TD pins the semantics that are
currently unspecified:

- **Claims**: stop (already true via empty `select_eligible`).
- **Intake**: `PauseQueue` gains an intake mode — `pause(drain_intake: bool)` or an equivalent
  variant — so a queue can be fully quiesced to a stable position before branching. Intake-blocking
  pause is the recommended mode for the branch workflow.
- **Leases**: in-flight leases continue on their normal clock — pause neither extends nor cancels
  them. Discovery keeps reporting intrinsic eligibility buildup, pause-agnostic
  (`crates/pqueue-engine/src/port.rs:818-819`). Documented so paused-queue metrics are not misread.
- Pause/resume are log commands, so they emit change records (TD-008) and survive failover.

## 2. Read-as-of-position

Primitives exist: `LogStore::high_water` (`crates/pqueue-engine/src/compose.rs:113`) and
snapshot-at-position storage (`compose.rs:116-123`). (`shard` below is a `QueueKey` — the whole queue,
per ADR-008; the name survives from the engine's internal vocabulary.) New read path:

- `current_position(shard) -> CommandPosition` — thin wrapper over `high_water`; the cheap "grab the
  LSN" call.
- `read_as_of(shard, position, query)` — hydrate from the nearest snapshot ≤ P
  (`latest_snapshot`/`read_snapshot`), replay `read_from` up to P into an ephemeral projection, answer
  the bounded query, discard. This is the log-as-truth "materialize up to P" read.
- **Relational family**: returns a structured `capability-unavailable` (`EngineError::Unavailable`).
  The ADR-013 rebuild-from-log migration is now complete (the relational stores persist the log,
  implement `recovery_high_water`, and replay the tail on recovery), but as-of reads additionally
  require reconstructing an ephemeral historical projection from snapshot + replay, which the
  relational projection stores decline (`supports_as_of() = false`,
  `crates/pqueue-engine/src/port.rs:1220-1231`) — so the relational family still serves only "now"
  until that reconstruct path is built.

## 3. Branch-at-position

**Object-log family (natural copy-on-write).** `ManifestEntry` objects and segment objects are
immutable (`crates/pqueue-objectlog/src/segmented.rs:539-549`), namespaced per `(tenant, queue)`
(`shard_prefix`, `:582`). Branching queue Q at position P:

1. Allocate a new queue identity Q′ (new `(tenant, queue_id)`) — a new object prefix and manifest
   series.
2. Write Q′'s manifest referencing **the same immutable segment objects** as Q for all sequences ≤ P
   (copy-on-write share, no data copy); Q′ diverges by appending its own segments.
3. Q′ acquires its **own control-plane lease and epoch**
   (`crates/pqueue-engine/src/control_plane.rs:59-89`). The single-active-lease invariant
   (`control_plane.rs:14`) is preserved because the branch is a distinct queue, never a second owner
   of Q. Q keeps running, or stays paused for a clean comparison baseline.

**Relational family**: branch = create Q′ and replay Q's log ≤ P into fresh projection tables. The
ADR-013 rebuild-from-log prerequisite is now in place, but the replay-into-fresh-tables branch path
itself is not built; until it is, the relational family returns a structured `capability-unavailable`.

**Reconstructed state includes in-flight leases.** A branch materialized at P contains Leased items
whose lease tokens reference workers that will never renew against Q′; they expire on the normal clock
and become claimable in the branch. Acceptable for experimentation — documented so experimenters
expect a burst of lease expiries at branch start.

## 4. Branch lifecycle rules

- **Emission suppression**: branches default `emit_change_records = false` (TD-008) so experimental
  lifecycle never floods production history in niflheim/cayce; explicitly overridable.
- **Segment pinning**: Q's segment retention/expiry (TD-004 §Retention) MUST NOT delete segments still
  referenced by a live branch. Branches carry a TTL; expiring a branch releases its segment pins. This
  is the branch-GC hazard rule.
- **Idempotency namespace**: a branch inherits Q's request-id replay records and client-item-key
  tombstones as of P. Retrying a production `request_id` against the branch replay-converges to the
  branched result. Item ids minted ≤ P are identical in Q and Q′; post-P divergence means the same
  `item_id` can exist in both with different states, disambiguated only by queue identity — which is
  why `tenant_id`/`queue_id` are load-bearing in the TD-008 idempotency key.

## The experiment workflow this enables

1. `pause(Q, drain_intake: true)` → Q quiesces at position P.
2. `branch(Q, at: P) -> Q′` (emission-suppressed, own lease, TTL set).
3. Point the variant workflow at Q′; resume Q with the incumbent workflow (a live-traffic shadow
   comparison) or keep Q paused (offline what-if).
4. Compare via `read_as_of(Q, P, …)` vs live reads of Q′ (and Q).
5. Discard Q′ (TTL or explicit delete) — segment pins release; production history untouched.
