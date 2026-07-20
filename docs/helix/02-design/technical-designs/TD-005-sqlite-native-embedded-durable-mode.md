---
ddx:
  id: td-sqlite-native-embedded-durable-mode
  depends_on:
    - prd
    - api-native-client-interface
    - adr-cqrs-log-projection-storage-model
    - adr-embedded-engine-integration-and-public-surface
    - td-storage-architecture-backend-contracts
    - td-s3-object-log-sqlite-projection-mode
    - adr-queue-as-shard-unit-and-projection-families
  review:
    self_hash: 4aaa7514b75207a1b07ef4db38fef4a018161a51bfc1ffd2950f61443b7dde56
    deps:
      adr-cqrs-log-projection-storage-model: 849c0bd7e15200ab056c2e5fcedb4b04a116aba520993fb4bab63b1195146107
      adr-embedded-engine-integration-and-public-surface: e06dc6a96cdcd7293b5ba67e9c17d387cd2bd51c14daef13287bdf62a9e3951e
      adr-queue-as-shard-unit-and-projection-families: 50fb11c85cbf40fa182469b036ef5210b304f330171a17ab371ae485524cb924
      api-native-client-interface: ae6c682dbf6e269b6792351f1677477f2324fb24cb4cc4f85392f6369fd43b0b
      prd: 2d97b05f9c0c0db576149bdfef21c729d66e07dbb674c95f6b7135ddcffa3b91
      td-s3-object-log-sqlite-projection-mode: 56d80c3e6ad5ab54460e300fdf4ddfe535dc75a47b0a2a0e32d0de46c38c7e49
      td-storage-architecture-backend-contracts: b1d17cc3481f52097ea0b2233a4a0e7bfa1512381c0b1fed7b3830fd3f02cc4e
    reviewed_at: "2026-07-20T00:01:27Z"
---

# Technical Design: TD-005 SQLite-Native Embedded Durable Mode

**Contract**: API-001 | **ADR**: ADR-001, ADR-006 | **Depends on**: TD-001, TD-004 | **Scope**: `sqlite` backend profile

## Scope

This technical design defines the third committed durable backend: **`sqlite`** —
a single-file, server-free durable backend for embedded hosts (ADR-006). It is
the embedded-durable option that the embedded integration profile (API-003)
requires and that 7snx needs in place of the non-durable in-memory backend.

The design is an **extension of the `object_log_sqlite_projection` backend**: it
keeps that backend's SQLite projection unchanged and **replaces the object-log /
S3 durable command log with a command-log table in the same SQLite database**.
SQLite is therefore both the durable command-log authority and the rebuildable
projection, in one file, with no object store and no Postgres control plane.

In scope:

- A durable `LogStore` over a SQLite `pqueue_command_log` table (WAL + fsync ack
  boundary), reusing the TD-001 command-envelope / `CommandPosition` model.
- Reuse of the existing full item-lifecycle projection `SqliteProjectionStore`
  (`pqueue-sqlite`, `projection.rs`) as the `ProjectionStore`, unchanged from
  object-log mode. (This is the full `ProjectionStore` — not the group/cohort
  `SqliteProjection` in `lib.rs` used by object-log group-summary materialization.)
- A SQLite `ControlPlaneStore` (queue definitions, queue-owner assignment +
  epoch) in the same database.
- **Single-transaction append+apply**: a command is written to the log table and
  applied to the projection tables in one SQLite transaction, so the projection
  is never inconsistent with the log and the durable ack boundary is one WAL
  fsync.
- Recovery: on reopen the persisted projection is already current with the log
  (atomic append+apply means no committed-but-unapplied window), so committed
  state is read directly — no log-tail replay is required.
- The `sqlite` backend profile, wired into `BackendProfile`.

Out of scope:

- Multi-writer coordination / object-store CAS / manifest fencing (TD-004) — a
  single-file SQLite backend is **single-writer** (one host process owns the
  file), so there is no cross-writer CAS problem; epoch handling is restart
  recovery only.
- The S3 object-log substrate and Postgres control plane (TD-004 / TD-002).
- Horizontal scale-out headline evidence (reserved for `object_log_sqlite_projection`).
- A pure-Rust SQLite/embedded engine — v1 uses `rusqlite` (bundled SQLite);
  evaluating `redb`/`limbo` is a separate, non-blocking item (ADR-006).

## Technical Approach

`sqlite` is a **log-projection backend** (ADR-001) where both halves live in one
SQLite database file:

- `LogStore` — appends serialized `CommandEnvelope`s to `pqueue_command_log`
  `(tenant_id, queue_id, sequence, backend_epoch, checksum, payload,
  created_at)`, `PRIMARY KEY (tenant_id, queue_id, sequence)`. The
  durable ack boundary is the committed SQLite transaction under WAL with
  `synchronous=FULL`, which fsyncs the WAL on every commit so a returned append
  survives process crash AND power loss. (`NORMAL` only fsyncs at checkpoint,
  leaving a power-loss window, so it is NOT the default for a backend whose log
  is the ack boundary; it may be offered later as an explicit throughput opt-in.)
  `read_from` is an indexed range scan over `sequence`.
- `ProjectionStore` — the existing `SqliteProjectionStore` (`projection.rs`,
  the full item-lifecycle projection), applied only from committed log rows
  (unchanged semantics from TD-004).
- `ControlPlaneStore` — `pqueue_queue` (validated `QueueDefinition`) and
  `pqueue_queue_owner` (queue → epoch) tables in the same database.
- `SnapshotStore` — optional/no-op for v1: the projection is persisted in the
  same file and is always current with the log (atomic append+apply), so there is
  no replay to accelerate. A compaction/checkpoint may be added later but is not
  required for durability.

**Key decisions**

- **One database, one transaction for append+apply.** Unlike TD-004 (where the
  object-log commit and the local projection apply are separate steps guarded by
  a response barrier), `sqlite` writes the log row and the projection rows in
  the **same** SQLite transaction. On commit, both are durable together. This
  removes the apply-lag/reservation machinery TD-004 needs and gives strict
  read-after-write: a returned ack means the projection already reflects it.
- **Single-writer ownership = simpler fencing.** The embedded host process is the
  sole writer of the file. `backend_epoch` is recorded per queue and bumped on
  open for restart-fencing/observability; there is no concurrent-writer CAS. A
  second process opening the same file is a misconfiguration the backend rejects.
  v1 enforces this with `PRAGMA locking_mode=EXCLUSIVE` plus an exclusive write
  lock acquired at open (a second opener fails with BUSY/LOCKED, surfaced as an
  `AlreadyOpen` error), not a case to coordinate.
- **Single guarded connection serializes writes.** v1 uses ONE
  `Mutex<Connection>` (not a pool): all claimers/finalizers serialize through it,
  which is exactly the single-active-lease serialization point `claim` needs (the
  SQLite analogue of Postgres `FOR UPDATE SKIP LOCKED`). `locking_mode=EXCLUSIVE`
  is consistent with this single-connection model.
- **Reuses the TD-001 conformance suite.** `sqlite` MUST pass the shared backend
  conformance for the item-lifecycle dimensions it implements (durability, lease,
  replay-of-committed-commands, idempotency, progress), at parity with the
  in-memory reference, plus a reopen-recovery test (reopen the file → committed
  state present). Group-summary/cohort/gate materialization remains the
  responsibility of the `lib.rs` group-summary projection used by object-log mode
  and is out of scope for the standalone item-lifecycle backend.

## Backend Profile

`PQUEUE_BACKEND_PROFILE=sqlite` with a single config value: the database file
path (and a `synchronous` strictness knob). No object store, no Postgres. This is
the profile an embedded host (7snx) selects for durable single-file operation.

## Durability and Recovery

- **Ack boundary**: append returns only after the SQLite transaction commits
  under WAL (fsync). This satisfies the TD-001 durable-ack guarantee that the
  in-memory backend cannot.
- **Recovery**: no log-tail replay is needed. Because the log append and the
  projection apply commit in the **same** SQLite transaction (one file), there is
  no committed-but-unapplied window — the persisted projection is always current
  with the log. On reopen the backend reads committed state directly from the
  projection tables. (This supersedes the earlier snapshot+log-tail replay design
  inherited from TD-004, which a single-file atomic backend does not require.)
  The reopen-recovery guarantee is therefore "reopen the file → committed state is
  present", verified by a reopen test.
- **Retention**: pruning of terminal/applied `pqueue_command_log` rows is
  **deferred** (future work, tracked by a follow-up bead). The log table is not
  pruned in v1; durability and reopen recovery do not depend on retention.

## Completion Evidence (to be produced by build beads)

- `cargo test -p pqueue-sqlite` passes the new durable `LogStore`/`ControlPlaneStore`
  unit tests.
- `sqlite` passes the shared TD-001 backend conformance suite (parity with
  `postgres_native` and `object_log_sqlite_projection`).
- A `sqlite` recovery test: append, drop the backend, reopen the same file, and
  read back the committed state.
- The embedder delivery-adapter conformance suite (ADR-006 §5) passes on the
  `sqlite` backend.

## Validation Checklist

- [ ] `LogStore` over a SQLite log table with a WAL-fsync ack boundary.
- [ ] Append+apply in one transaction; strict read-after-write.
- [ ] `SqliteProjection` reused unchanged as `ProjectionStore`.
- [ ] SQLite `ControlPlaneStore`; single-writer ownership enforced.
- [ ] Reopen-from-file recovery preserves committed state.
- [ ] Passes the shared TD-001 backend conformance suite.
- [ ] `sqlite` `BackendProfile` wired; no object-store / Postgres dependency.
