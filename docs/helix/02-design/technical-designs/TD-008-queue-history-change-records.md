---
ddx:
  id: td-queue-history-change-records
  depends_on:
    - adr-log-single-source-of-truth
    - adr-cqrs-log-projection-storage-model
    - adr-fjord-embedded-change-log-consumer-surface
    - td-storage-architecture-backend-contracts
    - td-s3-object-log-sqlite-projection-mode
  review:
    self_hash: 02808f93dee17f6f31facc9719b7c3b534ba871d430255eceafa37b0aea67ddf
    deps:
      adr-cqrs-log-projection-storage-model: ef1295e9f2858b2d286c27e1d571aefc5bf4b1614e848d3c8958e3f6af5f68b8
      adr-fjord-embedded-change-log-consumer-surface: c1e5ff620517f039f2138f76841bf6d51a5d52d86ad05d75c5885c80c1cb96e0
      adr-log-single-source-of-truth: 66130c84cb8e5467f5192066a0446f527672dac2eea83f7eae70b66c1e3b724c
      td-s3-object-log-sqlite-projection-mode: 47f10c9ec69454100ac9250c87805c6a17a893fd81e6be3dfe3c9f3c361b4b5d
      td-storage-architecture-backend-contracts: 430d0dc1f83fa62aeb19948efd2a84f5c31df7d15195e51c8296c93c711919f5
    reviewed_at: "2026-07-06T17:29:43Z"
---

# TD-008: Queue history via change-record emission, plus longer terminal retention

**Status**: Draft
**Decision authority**: ADR-013 (log as single source of truth)
**Cross-repo**: niflheim durable-ingest HTTP endpoint (consumer); cayce CONTRACT-013 uses the same
ingest path for SES exhaust, so delivery history lands beside delivery exhaust; fjord, embedded in
pqueue-server, as the Kafka-protocol change-log interface provider (see "Delivery interfaces" and
ADR-014).

## Scope

pqueue emits **item-lifecycle change records** derived from the committed log — the change log. The
first consumer binding is at-least-once delivery to niflheim's durable-ingest endpoint, default-on
with per-queue opt-out; a Kafka-protocol consumer interface is a required second binding (see
"Delivery interfaces"). niflheim owns history and Delta projection. pqueue does **not** write
Parquet/Delta. The terminal retention default rises so items linger long enough to (a) satisfy
idempotency windows and (b) guarantee a terminal item is never reaped before its terminal change
record is durably emitted.

## Change-log requirements (normative)

These hold for **every** delivery binding, current and future:

- **CL-1 Completeness**: on every queue with `emit_change_records = true` (CL-7), every committed
  mutating command produces its change records ("Which transitions emit"); no acknowledged transition
  on such a queue may be absent from the change log. Opting out (CL-7) disables **delivery** for that
  queue entirely — no records are produced and no emission cursor advances for it; the committed
  command log itself (ADR-013, mandatory) remains complete, so opting back in guarantees records only
  from the opt-in position forward, not retroactively. CL-1 is only possible because the durable
  command log is mandatory — the change log is a pure derivation of the committed log tail.
- **CL-2 Off-commit-path**: emission never blocks, observes, or fails the commit path. Bounded,
  telemetry-surfaced lag is the accepted cost (`emission_lag_commands`,
  `emission_oldest_unemitted_age_ms`).
- **CL-3 At-least-once with a stable idempotency key**: `(tenant_id, queue_id, item_id,
  backend_epoch, sequence)`; consumers dedupe. Exactly-once is never claimed.
- **CL-4 Per-queue order**: records for one queue are delivered in `CommandPosition` order; no
  cross-queue ordering exists or is implied.
- **CL-5 Durable resumability**: the emission cursor is durable per queue; crash/failover re-emits
  from the last durable cursor (never skips), and post-failover re-emission under a new epoch still
  dedupes via CL-3.
- **CL-6 Reap/emission frontier coupling**: on a queue with `emit_change_records = true`, a terminal
  item may be reaped only when **both** hold: (a) its retention time has elapsed
  (`now >= terminal_at + terminal_retention_ms`), and (b) the durable `emission_cursor` is at or past
  the item's terminal record `CommandPosition`. On an opted-out queue only (a) applies.
- **CL-7 Per-queue opt-out**: `emit_change_records` (default `true`; branches default `false`,
  TD-009).
- **CL-8 Tenant isolation**: a consumer binding must be scopeable to `(tenant_id, queue_id)` and
  must not leak other tenants' records (ADR-002 deny-by-default applies to the change log too).

## Emission seam

**Not** on the commit path. `commit_locked_batch` (`crates/pqueue-engine/src/compose.rs:1346-1366`)
never blocks on, observes, or fails because of emission. Emission is a **committed-log tail consumer
with its own durable cursor**, structurally identical to recovery replay (`compose.rs:1207` reads
`LogStore::read_from`) and the hybrid-async SQLite apply worker (TD-004 §"Ordered batching and SQLite
high-water").

New engine port, minimal and runtime-free like the group-commit facet:

```rust
trait ChangeRecordSink: Send + Sync {
    /// At-least-once delivery of an ordered batch for one shard. Idempotent on the receiver.
    // `shard` is the whole queue: under ADR-008 the queue is the unit of sharding, and the
    // parameter name survives from the engine's internal vocabulary (`QueueKey{tenant_id, queue_id}`).
    fn emit(&self, shard: &QueueKey, records: &[ChangeRecord]) -> EngineResult<()>;
}
```

The runtime-bearing crate (`pqueue-server`, which owns tokio — `crates/pqueue-server/Cargo.toml:33`)
drives an interval task (modeled on `flush_tick`/`try_flush_deferred_projection`,
`compose.rs:1061,1099`): read `read_from(shard, emission_cursor, limit)`, map each committed
`CommandEnvelope` to `ChangeRecord`s, call `sink.emit`, and only then advance a **durable** per-queue
`emission_cursor` (persisted like `high_water`). At-least-once falls out: a crash before cursor
advance re-emits; the receiver dedupes.

**HTTP client policy**: consistent with `crates/pqueue-objectlog/Cargo.toml:29` (no reqwest/hyper by
design; the S3 client is hand-rolled SigV4), the niflheim sink is a lean hand-rolled POST over the
existing tokio `net` stack. No heavy SDK.

## Which transitions emit

Every mutating `QueueCommand` is already in the log (`crates/pqueue-engine/src/command.rs:49-60`), so
the tail consumer sees all of them. Emit one `ChangeRecord` per affected item for: `Push` (→Pending),
`Claim` (→Leased), `RenewLease`, `Finalize` (→Terminal complete/fail — the high-value record for cayce
delivery history), `LeaseExpired`, retry/release/rearm (→Pending), `UpdateFields`, `PurgeItems`
(tombstone). Queue-scoped `SetGates`/`PauseQueue`/`ResumeQueue` emit a queue-level record with a null
item.

## Record schema and idempotency key

A single `CommandPosition` can fan out to N items (batch push/claim/finalize), so the record grain is
**per (item, position)**:

```
ChangeRecord {
  tenant_id, queue_id,               // shard identity; branches get distinct identity (TD-009)
  item_id: Option<ItemId>,           // None for queue-scoped records
  position: CommandPosition,         // (shard, backend_epoch, sequence)
  command_kind,                      // Push/Claim/Finalize/…
  new_state: Option<ItemState>,
  item_version, terminal_at, …,      // relevant projected fields
  emitted_at, source_owner_id, source_epoch,
}
idempotency_key = (tenant_id, queue_id, item_id, position.backend_epoch, position.sequence)
```

`tenant_id` is load-bearing: branches (TD-009) can mint colliding `item_id`s post-fork, disambiguated
only by queue identity. Including `backend_epoch` means post-failover re-emission under a new epoch
that references the same logical work still dedupes correctly.

## Delivery interfaces

The change log has one seam (`ChangeRecordSink` over the committed-log tail) and multiple consumer
bindings. Two are in contract:

1. **niflheim durable-ingest (HTTP push)** — the current binding, specified throughout this TD: a
   lean hand-rolled POST driven by the `pqueue-server` interval task.
2. **Kafka-protocol consumer interface (required)** — product requirement (2026-07-05, provider
   decided 2026-07-06): downstream consumers must be able to subscribe to the change log with stock
   Kafka clients, and **pqueue must own the surface**. ADR-014 settles the provider/shape choice:
   **fjord, embedded** in `pqueue-server` behind the delivery seam, serves the change topics
   (metadata, fetch, consumer groups, committed offsets, fan-out) so the surface exists in every
   deployment without operating a second system. Each `(tenant_id, queue_id)` change stream is a
   single-partition topic so per-queue order is preserved (CL-4). The normative record contract
   (record key = `"{item_id}:{backend_epoch}:{sequence}"` — unique across fan-out; `pq-*` headers;
   `ChangeRecord` payload; consumer dedupe-window and offset-commit obligations) is pinned in
   ADR-014. On failover, re-emission may assign a later Kafka offset to the same logical record; the
   offset stream never regresses and the record key is the dedupe identity. Retention: on
   `emit_change_records = true` queues, source segments expire only after snapshot coverage AND the
   durable emission cursor has passed the segment's terminal `CommandPosition`; on opted-out queues
   (including TD-009 branches) only snapshot coverage applies — expiry never waits on a cursor that
   does not exist. Tenant authz (CL-8) is enforced by tenant-prefixed topics and ACLs scoped to the
   caller's `(tenant_id, queue_id)` namespace. A deployment that must publish to an external Kafka
   attaches a producer sink at the same seam; the embedded fjord then sits idle.

   Scope boundary with ADR-005: ADR-005's "consumer-side Kafka APIs are permanently out of scope"
   applies to the **queue data plane** (committed offsets conflict with mutable priority and progress
   bounds). The change log is a different surface — an append-only, per-queue-ordered stream where
   Kafka consumer semantics fit naturally — so this Kafka interface does not reopen ADR-005.

## Delivery semantics and failure isolation

- **At-least-once + idempotent ingest** (niflheim dedupes on `idempotency_key`). Never exactly-once,
  never blocking commit.
- Sink errors advance nothing; retry next tick with backoff. Emission lag is a bounded,
  telemetry-surfaced metric (`emission_lag_commands`, `emission_oldest_unemitted_age_ms`), modeled on
  the TD-004 async-apply debt metrics.
- **Default-on with opt-out**: `emit_change_records: bool` (default `true`) on `QueueDefinition`
  (`crates/pqueue-core/src/domain.rs:629-632` region, `#[serde(default)]` for back-compat like
  `terminal_retention_ms`). Branches default to `false` (TD-009).

## Retention default change

`default_terminal_retention_ms()` rises from `60_000` (1 minute) to `3_600_000` (1 hour)
(`crates/pqueue-core/src/domain.rs:652-653`). Rationale: 1 hour covers request-id/client-item-key
idempotency retry windows and gives the emission consumer a wide catch-up margin before terminal reap;
it deliberately does **not** go to days because terminal items inflate the resident projection set
linearly (in-RAM `ProjectionData` for the log-replay family; table+index rows for relational). With
niflheim owning long-term history, pqueue needs only a short operational tail. Per-queue override
remains.

**Reap/emission frontier coupling (the subtlest rule in this TD)**: on a queue with
`emit_change_records = true`, a terminal item MUST NOT be reaped until **both** conditions hold —
retention elapsed (`now >= terminal_at + terminal_retention_ms`) **and** the durable `emission_cursor`
at or past the item's terminal record `CommandPosition` (CL-6). Retention elapsing never overrides the
emission condition. Otherwise a crash between reap and emit silently drops the terminal transition
from history. On an opted-out queue only the retention condition applies.

## Risks

1. **Retention vs hot-projection memory** — capped at 1h default, per-queue tunable; add a
   resident-terminal-count metric so operators see the cost.
2. **Ordering** — records are **per-queue ordered, globally unordered** (`CommandPosition.sequence` is
   per-queue). niflheim/cayce consumers MUST NOT assume cross-queue order. Contract-level statement.
3. **Re-emission after failover** — a new owner replaying the tail re-emits positions the old owner
   may have delivered; correctness rests on receiver idempotency. niflheim's dedupe retention must
   exceed the worst-case emission outage + failover window; document the requirement on the niflheim
   side.

## Kafka interface decision

The change-log Kafka surface is provided by **fjord, embedded in pqueue-server**
(product-owner decision 2026-07-06): pqueue owns the interface, so the surface
exists in every deployment. The load-bearing rules are the boundary invariants
(feed-forward only, never on the commit path, separate storage namespace,
swappable at the seam with fjord idling when an external Kafka is used), the
offset-to-`CommandPosition` mapping, the pinned per-record consumer contract,
retention scoped to `emit_change_records = true` queues, and CL-8 tenant authz
via tenant-prefixed topics/ACLs. See ADR-014 for the normative decision.
