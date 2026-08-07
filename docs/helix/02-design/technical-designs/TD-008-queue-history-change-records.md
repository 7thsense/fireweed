---
ddx:
  id: td-queue-history-change-records
  depends_on:
    - adr-log-single-source-of-truth
    - adr-cqrs-log-projection-storage-model
    - adr-fjord-embedded-change-log-consumer-surface
    - td-storage-architecture-backend-contracts
    - td-s3-object-log-sqlite-projection-mode
  status: accepted
  review:
    self_hash: 1744898d68be2d75ace9da0b8778dd69c827a518506a0598486e9d2480ad1598
    deps:
      adr-cqrs-log-projection-storage-model: 63ed2521bc7d0e785529aafbd179b3ef22d51cbf3897d51c511540be52ee9ba3
      adr-fjord-embedded-change-log-consumer-surface: ebc28c2a895033a35d04b61aca9f8e0e37338ca96ca3aa0a7636a8b6cd96dcee
      adr-log-single-source-of-truth: c88063a069f43bd90f31e4875ad8b35fca9876de5b52cb777908d314d46abd1b
      td-s3-object-log-sqlite-projection-mode: 7770bb133f4ace189bfc715e3be6472f894f7c62d52adfc051540fea97c6a4b2
      td-storage-architecture-backend-contracts: 2d88d342aac82f23616fdff6d94f4ac88701ab6e70c80a0315003c5e66432c74
    reviewed_at: "2026-08-07T11:25:30Z"
---

# TD-008: Queue history via change-record emission, plus longer terminal retention

**Status**: Accepted for Class A durable-log cells; unavailable for Class B memory-log cells
**Decision authority**: ADR-013 (log as single source of truth)
**Cross-repo**: niflheim durable-ingest HTTP endpoint (consumer); cayce CONTRACT-013 uses the same
ingest path for SES exhaust, so delivery history lands beside delivery exhaust; fjord, embedded in
fireweed-server, as the Kafka-protocol change-log interface provider (see "Delivery interfaces" and
ADR-014).

> **Reconciled with the 5×4 storage product (2026-08-03).** ADR-014 specifies
> that fjord delivery is an **in-process** append to the embedded broker's Rust log (librdkafka
> removed; the optional external-Kafka producer is pure-Rust `rskafka`). The record and delivery
> invariants remain unchanged, but they apply only where a Class A log can reconstruct the tail.

## Scope

For Class A cells, Fireweed emits **item-lifecycle change records** derived from the committed durable
log — the change log. The first consumer binding is at-least-once delivery to niflheim's durable-ingest
endpoint, default-on with per-queue opt-out; a Kafka-protocol consumer interface is a required second binding (see
"Delivery interfaces"). niflheim owns history and Delta projection. fireweed does **not** write
Parquet/Delta. The terminal retention default rises so items linger long enough to (a) satisfy
idempotency windows and (b) guarantee a terminal item is never reaped before its terminal change
record is durably emitted.

## Storage-class eligibility

| Class | Cells | Change-record/history contract |
|---|---|---|
| **Class A** | `sqlite`, `postgres`, `filesystem`, or `s3` log × any public projection | Available when the deployment sink is enabled; the durable log tail and durable emission cursor are authoritative. |
| **Class B** | `memory` log × `memory`, `sqlite`, or `postgres` projection | Unavailable. A surviving durable projection is not a reconstructible command log and cannot provide log-derived history, branch, read-as-of, or backfill. |

`emit_change_records` is a per-queue opt-out only inside an eligible Class A deployment; it cannot grant
the capability to Class B. Enabling a deployment change-record sink for a Class B cell fails during
configuration validation, before any storage I/O, with
`EngineError::ChangeRecordsRequireDurableLog`. Its exact RESP token is
`-ERR fireweed change_records_require_durable_log`.

This error is startup-only: it must never escape a mutation or appear in a production commit outcome.
`CommitRejection::ChangeRecordsRequireDurableLog` exists only as a name-level exhaustive serde/mapping
mirror. The generic mapping retains existing semantic normalizations, including
`EngineError::Backpressure { .. }` to `CommitRejection::Backpressure(String)` and the existing
`QueueDefinitionConflict` semantic class/token. Changing a queue from Class B to Class A starts an
eligible history at the new durable-log frontier; it does not synthesize or claim a Class B backfill.

## Change-log requirements (normative)

These hold for **every** delivery binding, current and future:

- **CL-1 Completeness**: on every Class A queue with `emit_change_records = true` (CL-7), every committed
  mutating command produces its change records ("Which transitions emit"); no acknowledged transition
  on such a queue may be absent from the change log. Opting out (CL-7) disables **delivery** for that
  queue entirely — no records are produced and no emission cursor advances for it; the committed
  Class A command log remains complete, so opting back in guarantees records only from the opt-in
  position forward, not retroactively. CL-1 is possible because that class has a durable command log;
  Class B makes no completeness or history claim.
- **CL-2 Off-commit-path**: emission never blocks, observes, or fails the commit path. Bounded,
  telemetry-surfaced lag is the accepted cost (`emission_lag_commands`,
  `emission_oldest_unemitted_age_ms`).
- **CL-3 At-least-once with a stable idempotency key**: `(tenant_id, queue_id, item_id,
  backend_epoch, sequence)`; consumers dedupe. Exactly-once is never claimed.
- **CL-4 Per-queue order**: records for one queue are delivered in `CommandPosition` order; no
  cross-queue ordering exists or is implied.
- **CL-5 Durable resumability**: on Class A, the emission cursor is durable per queue; crash/failover re-emits
  from the last durable cursor (never skips), and post-failover re-emission under a new epoch still
  dedupes via CL-3.
- **CL-6 Reap/emission frontier coupling**: on an eligible Class A queue with `emit_change_records = true`, a terminal
  item may be reaped only when **both** hold: (a) its retention time has elapsed
  (`now >= terminal_at + terminal_retention_ms`), and (b) the durable `emission_cursor` is at or past
  the item's terminal record `CommandPosition`. On an opted-out queue only (a) applies.
- **CL-7 Per-queue opt-out**: within Class A, `emit_change_records` defaults to `true`; branches default
  to `false` (TD-009). In Class B the field is inert because deployment delivery must be disabled.
- **CL-8 Tenant isolation**: a consumer binding must be scopeable to `(tenant_id, queue_id)` and
  must not leak other tenants' records (ADR-002 deny-by-default applies to the change log too).

## Emission seam

**Not** on the commit path. `commit_locked_batch` (`crates/fireweed-engine/src/compose.rs:1346-1366`)
never blocks on, observes, or fails because of emission. Emission is a **committed-log tail consumer
with its own durable cursor**, structurally identical to recovery replay (`compose.rs:1207` reads
`LogStore::read_from`) and an `AsyncProjection` apply worker (TD-004 §"Ordered batching and SQLite
high-water"). This seam is not constructed for Class B.

New engine port, minimal and runtime-free like the group-commit facet:

```rust
trait ChangeRecordSink: Send + Sync {
    /// At-least-once delivery of an ordered batch for one shard. Idempotent on the receiver.
    // `shard` is the whole queue: under ADR-008 the queue is the unit of sharding, and the
    // parameter name survives from the engine's internal vocabulary (`QueueKey{tenant_id, queue_id}`).
    fn emit(&self, shard: &QueueKey, records: &[ChangeRecord]) -> EngineResult<()>;
}
```

The runtime-bearing crate (`fireweed-server`, which owns tokio — `crates/fireweed-server/Cargo.toml:33`)
drives an interval task (modeled on `flush_tick`/`try_flush_deferred_projection`,
`compose.rs:1061,1099`): read `read_from(shard, emission_cursor, limit)`, map each committed
`CommandEnvelope` to `ChangeRecord`s, call `sink.emit`, and only then advance a **durable** per-queue
`emission_cursor` (persisted like `high_water`). At-least-once falls out: a crash before cursor
advance re-emits; the receiver dedupes.

**HTTP client policy**: consistent with `crates/fireweed-objectlog/Cargo.toml:29` (no reqwest/hyper by
design; the S3 client is hand-rolled SigV4), the niflheim sink is a lean hand-rolled POST over the
existing tokio `net` stack. No heavy SDK.

## Which transitions emit

Every mutating `QueueCommand` in an eligible Class A cell is in the durable log
(`crates/fireweed-engine/src/command.rs:49-60`), so the tail consumer sees all of them. Emit one
`ChangeRecord` per affected item for: `Push` (→Pending),
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

The Class A change log has one seam (`ChangeRecordSink` over the committed-log tail) and multiple consumer
bindings. Two are in contract:

1. **niflheim durable-ingest (HTTP push)** — the current binding, specified throughout this TD: a
   lean hand-rolled POST driven by the `fireweed-server` interval task.
2. **Kafka-protocol consumer interface (required)** — product requirement (2026-07-05, provider
   decided 2026-07-06): downstream consumers must be able to subscribe to the change log with stock
   Kafka clients, and **fireweed must own the surface**. ADR-014 settles the provider/shape choice:
   **fjord, embedded** in `fireweed-server` behind the delivery seam, serves the change topics
   (metadata, fetch, consumer groups, committed offsets, fan-out) so the surface exists in every
   eligible Class A deployment without operating a second system. Each `(tenant_id, queue_id)` change stream is a
   single-partition topic so per-queue order is preserved (CL-4). The normative record contract
   (record key = `"{item_id}:{backend_epoch}:{sequence}"` — unique across fan-out; `fireweed-*` headers;
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
- **Default-on with opt-out inside Class A**: `emit_change_records: bool` (default `true`) on `QueueDefinition`
  (`crates/fireweed-core/src/domain.rs:629-632` region, `#[serde(default)]` for back-compat like
  `terminal_retention_ms`). Branches default to `false` (TD-009).

## Retention default change

`default_terminal_retention_ms()` rises from `60_000` (1 minute) to `3_600_000` (1 hour)
(`crates/fireweed-core/src/domain.rs:652-653`). Rationale: 1 hour covers request-id/client-item-key
idempotency retry windows and gives the emission consumer a wide catch-up margin before terminal reap;
it deliberately does **not** go to days because terminal items inflate the resident projection set
linearly (in-RAM `ProjectionData` for the log-replay family; table+index rows for relational). With
niflheim owning long-term history, fireweed needs only a short operational tail. Per-queue override
remains.

**Reap/emission frontier coupling (the subtlest rule in this TD)**: on an eligible Class A queue with
`emit_change_records = true`, a terminal item MUST NOT be reaped until **both** conditions hold —
retention elapsed (`now >= terminal_at + terminal_retention_ms`) **and** the durable `emission_cursor`
at or past the item's terminal record `CommandPosition` (CL-6). Retention elapsing never overrides the
emission condition. Otherwise a crash between reap and emit silently drops the terminal transition
from history. On an opted-out Class A queue and on every Class B queue only the retention condition
applies; neither path claims a change-record history.

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

The change-log Kafka surface is provided by **fjord, embedded in fireweed-server**
(product-owner decision 2026-07-06): Fireweed owns the interface, so the surface
exists in every eligible Class A deployment. The load-bearing rules are the boundary invariants
(feed-forward only, never on the commit path, separate storage namespace,
swappable at the seam with fjord idling when an external Kafka is used), the
offset-to-`CommandPosition` mapping, the pinned per-record consumer contract,
retention scoped to `emit_change_records = true` queues, and CL-8 tenant authz
via tenant-prefixed topics/ACLs. See ADR-014 for the normative decision.
