---
ddx:
  id: td-queue-history-change-records
  depends_on:
    - adr-log-single-source-of-truth
    - adr-cqrs-log-projection-storage-model
    - td-storage-architecture-backend-contracts
    - td-s3-object-log-sqlite-projection-mode
---

# TD-008: Queue history via change-record emission to niflheim, plus longer terminal retention

**Status**: Draft
**Decision authority**: ADR-013 (log as single source of truth)
**Cross-repo**: niflheim durable-ingest HTTP endpoint (consumer); cayce CONTRACT-013 uses the same
ingest path for SES exhaust, so delivery history lands beside delivery exhaust.

## Scope

pqueue emits **item-lifecycle change records** derived from the committed log, delivered at-least-once
to niflheim's durable-ingest endpoint, default-on with per-queue opt-out. niflheim owns history and
Delta projection. pqueue does **not** write Parquet/Delta. The terminal retention default rises so
items linger long enough to (a) satisfy idempotency windows and (b) guarantee a terminal item is never
reaped before its terminal change record is durably emitted.

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

**Reap/emission frontier coupling (the subtlest rule in this TD)**: a terminal item MUST NOT be reaped
until the emission cursor has durably passed its terminal record's position. Reap frontier =
`min(terminal_at + retention, emission_cursor_position)`. Otherwise a crash between reap and emit
silently drops the terminal transition from history.

## Risks

1. **Retention vs hot-projection memory** — capped at 1h default, per-queue tunable; add a
   resident-terminal-count metric so operators see the cost.
2. **Ordering** — records are **per-queue ordered, globally unordered** (`CommandPosition.sequence` is
   per-shard). niflheim/cayce consumers MUST NOT assume cross-queue order. Contract-level statement.
3. **Re-emission after failover** — a new owner replaying the tail re-emits positions the old owner
   may have delivered; correctness rests on receiver idempotency. niflheim's dedupe retention must
   exceed the worst-case emission outage + failover window; document the requirement on the niflheim
   side.
