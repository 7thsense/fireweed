---
ddx:
  id: async-runtime-blocking-matrix-inventory
  depends_on:
    - api-fireweed-rust-facade
    - adr-full-async-storage-boundaries
    - storage-matrix-composition-inventory
    - orthogonal-storage-matrix-brief
  status: accepted
---

# Async runtime blocking matrix inventory (5×3)

**Bead**: `fireweed-bfde6c1e`  
**Governing epic**: `fireweed-0a103d61` (native async; no process-wide blocking offload)  
**Contract**: [API-005](../02-design/contracts/API-005-fireweed-rust-facade.md) · [ADR-015](../02-design/adr/ADR-015-full-async-storage-boundaries.md)  
**Scope**: Inventory only — which public `StorageConfig` cells still rely on
process-wide `BlockingLibBackend`, and whether the composed product would stall
a Tokio worker under port poll/await if that bridge were removed.  
**Out of scope**: Implementing adapter fixes.

## Definitions

| Term | Meaning |
|------|---------|
| **Cell** | One public log × projection pair opened via `fireweed::open` / `open_async` (`StorageConfig`). Logs: `memory`, `sqlite`, `postgres`, `filesystem`, `s3`. Projections: `memory`, `sqlite`, `postgres`. |
| **Uses BlockingLibBackend today** | Product open path wraps the composed backend in `BlockingLibBackend` (`crates/fireweed/src/blocking_backend.rs`) via `wrap_blocking_backend` or direct `BlockingLibBackend::new` in `crates/fireweed/src/lib.rs`. |
| **Blocking-under-poll** | If the **composed product** (inner backend, **without** process-wide `BlockingLibBackend`) is polled/awaited on a Tokio worker, does that worker run blocking work: sync **rusqlite**, sync **`postgres::Client`**, bare **`block_on`**, or **`block_in_place`** on the caller? |
| **Facade with bridge (today)** | With `BlockingLibBackend`, each port op is submitted to the process-wide `fireweed-library-io-*` pool; the pool worker runs `futures::executor::block_on(inner_op)` (`blocking_backend.rs` `dispatch`). The Tokio worker only awaits a oneshot — durable I/O does **not** stall it *while the bridge remains*. |
| **Exit criterion** | Concrete residual work to clear process-wide `BlockingLibBackend` for that cell (adapter-local whole-transaction offload or native-async axes per ADR-015 / API-005 bridge removal). |

**Process-wide bridge (reference):**

- `crates/fireweed/src/blocking_backend.rs` — `BlockingLibBackend`, `shared_worker_pool`, `dispatch` → `block_on` on owned OS workers.
- `crates/fireweed/src/lib.rs` — `wrap_blocking_backend`, `open` / `open_async` / cell open arms.

**Common inner blocking pattern (log-replay cells):**

- `assemble_async_log_replay` builds `AsyncLogReplayBackend` over
  `InProcessLogStore` / `InProcessProjectionStore`
  (`crates/fireweed-engine/src/async_log_replay_product.rs`,
  `async_store.rs`).
- Those bridges call sync `LogStore` / `ProjectionStore` methods and return
  `std::future::ready(...)` — work runs on the **polling thread**, not an
  adapter actor.

**Adapter-local async actors (exist but not used by product open arms today):**

- `AsyncPostgresLog` / `AsyncPostgresRelationalProjection`
  (`fireweed-postgres/src/async_log.rs`, `async_projection.rs`) — mailbox + OS
  worker; poll is non-blocking.
- `AsyncSqliteProjectionStore` (`fireweed-sqlite/src/async_projection.rs`) —
  same pattern for projection only.
- Product matrix open still assembles sync axes through `InProcess*` or wraps
  the whole product in `BlockingLibBackend`.

---

## Summary grid

Legend: **BLB** = uses `BlockingLibBackend` today · **BUP** = blocking-under-poll
on the inner product if the process-wide bridge is removed.

| Log \\ Projection | `memory` | `sqlite` | `postgres` |
|-------------------|----------|----------|------------|
| **memory** | BLB **no** · BUP **no** | BLB **yes** · BUP **yes** | BLB **yes** · BUP **yes** |
| **sqlite** | BLB **yes** · BUP **yes** | BLB **yes** · BUP **yes** | BLB **yes** · BUP **yes** |
| **postgres** | BLB **yes** · BUP **yes** | BLB **yes** · BUP **yes** | BLB **yes** · BUP **yes** |
| **filesystem** | BLB **yes** · BUP **no\*** | BLB **yes** · BUP **yes** | BLB **yes** · BUP **yes** |
| **s3** | BLB **yes** · BUP **no\*** | BLB **yes** · BUP **yes** | BLB **yes** · BUP **yes** |

\*Object-log × memory product ports are native-async (`ObjectLogEngineStore` +
`AsyncInMemoryProjection`). Local FS blob paths may still use adapter-local
`spawn_blocking` / `block_in_place` inside `object-log` (not process-wide BLB).
Product open still wraps BLB. Construction may call `block_on_objectlog`
(open-time; `open_async` offloads selected cells via `spawn_blocking`).

**Counts:** 15 cells · BLB on open: **14** · already BLB-free: **1**
(`memory` × `memory`) · BUP residual if BLB removed: **12 yes** · **3 no**
(`memory`×`memory`, `filesystem`×`memory`, `s3`×`memory` product ports; FS still
has library-local offload notes).

---

## Open dispatcher evidence

| Entry | Path | Behavior |
|-------|------|----------|
| `open` | `lib.rs` | `validate` → `open_validated` match on `(log, projection)`. |
| `open_async` | `lib.rs` | Same composition; when `postgres` feature and cell needs offload (`Postgres` log, `S3`/`Filesystem` log, or `Postgres` projection) **and** a Tokio handle exists → `tokio::task::spawn_blocking(open_validated)`. |
| Memory log cells | `open_memory_log_cell` | `memory`×`memory`: raw `composed_memory_backend()` (**no** BLB). `memory`×`sqlite`/`postgres`: `assemble_async_log_replay` + `wrap_blocking_backend`. |
| Sqlite log cells | `open_sqlite_log_cell` → `open_sqlite*` | Always `BlockingLibBackend::new`. |
| Postgres log cells | `open_postgres_log_cell` → `open_postgres_runtime` / assemble | Always BLB; relational uses `PostgresRelationalBackend`. |
| Object-log cells | `open_object_log_cell` | Memory / sqlite / postgres projection arms; all wrap BLB (postgres via `open_objectlog_postgres_blocking`). |

---

## Cell inventory

### 1. `memory` × `memory`

| Field | Value |
|-------|--------|
| **Uses BlockingLibBackend today** | **No** |
| **Blocking-under-poll** | **No** |
| **Open path** | `open_memory_log_cell` → `fireweed_memory::composed_memory_backend()` → `RuntimeCore` without BLB (`lib.rs`) |
| **Product** | `AsyncLogReplayBackend<MemoryLog, InMemoryProjection>` — pure in-process RAM (`fireweed-memory`, `fireweed-projection`) |
| **Evidence** | `InProcessLogStore`/`InProcessProjectionStore` over memory axes only; no rusqlite / sync postgres client / `block_on` on port path |
| **Exit criterion** | **Already clear.** Keep this cell as the native-async reference; do not re-introduce BLB. |

### 2. `memory` × `sqlite`

| Field | Value |
|-------|--------|
| **Uses BlockingLibBackend today** | **Yes** (`wrap_blocking_backend`) |
| **Blocking-under-poll** | **Yes** (projection) |
| **Open path** | `open_memory_log_cell` → `MemoryLog` + `SqliteProjectionStore::open` → `assemble_async_log_replay` → recover → BLB |
| **Blocking mechanism** | Sync rusqlite on `SqliteProjectionStore` via `InProcessProjectionStore` (`fireweed-sqlite` projection; engine `async_store.rs`) |
| **Exit criterion** | Assemble with `AsyncSqliteProjectionStore` (or `BlockingProjectionStore` / owned actor) for the projection axis; keep memory log in-process; drop BLB when single-thread runtime heartbeat stays live under projection apply. |

### 3. `memory` × `postgres`

| Field | Value |
|-------|--------|
| **Uses BlockingLibBackend today** | **Yes** (`wrap_blocking_backend`) |
| **Blocking-under-poll** | **Yes** (projection) |
| **Open path** | `open_memory_log_cell` → `MemoryLog` + `PostgresRelational::connect_in_schema` → `assemble_async_log_replay` → BLB |
| **Blocking mechanism** | Sync `postgres::Client` on `PostgresRelational` / `ProjectionStore` through `InProcessProjectionStore` (`fireweed-postgres`) |
| **Exit criterion** | Wire `AsyncPostgresRelationalProjection` (mailbox actor already in-tree) as the projection axis for this cell; drop BLB after actor-backed apply passes conformance + heartbeat. |

### 4. `sqlite` × `memory`

| Field | Value |
|-------|--------|
| **Uses BlockingLibBackend today** | **Yes** (`open_sqlite` → `BlockingLibBackend::new`) |
| **Blocking-under-poll** | **Yes** (log) |
| **Open path** | `open_sqlite_log_cell` → `open_sqlite` → `composed_sqlite_backend` → BLB |
| **Product** | `AsyncLogReplayBackend<SqliteLog, InMemoryProjection>` |
| **Blocking mechanism** | Sync rusqlite `SqliteLog` (`compose_log.rs` `impl LogStore`) via `InProcessLogStore` |
| **Exit criterion** | Native `AsyncLogStore` for sqlite **or** `BlockingLogStore` / connection actor for whole log transactions; memory projection stays in-process; drop BLB when log append/read no longer run on the reactor thread. |

### 5. `sqlite` × `sqlite`

| Field | Value |
|-------|--------|
| **Uses BlockingLibBackend today** | **Yes** (`open_sqlite_sqlite_projection`) |
| **Blocking-under-poll** | **Yes** (log + projection) |
| **Open path** | `composed_sqlite_log_sqlite_projection` → BLB |
| **Blocking mechanism** | Sync rusqlite on both `SqliteLog` and `SqliteProjectionStore` via `InProcess*` |
| **Exit criterion** | Adapter-local offload (or async axes) on **both** log and projection; no cross-axis process-wide pool; drop BLB after dual-axis runtime-safety proof. |

### 6. `sqlite` × `postgres`

| Field | Value |
|-------|--------|
| **Uses BlockingLibBackend today** | **Yes** (`open_sqlite_postgres_projection`) |
| **Blocking-under-poll** | **Yes** (log + projection) |
| **Open path** | `SqliteLog::open` + `PostgresRelational::connect_in_schema` → `assemble_async_log_replay` → BLB |
| **Blocking mechanism** | Sync rusqlite log + sync postgres projection through `InProcess*` |
| **Exit criterion** | `BlockingLogStore`/`actor` for sqlite log **and** `AsyncPostgresRelationalProjection` for projection; drop BLB when neither axis blocks the reactor. |

### 7. `postgres` × `memory`

| Field | Value |
|-------|--------|
| **Uses BlockingLibBackend today** | **Yes** (`open_postgres_runtime` LogReplay → BLB) |
| **Blocking-under-poll** | **Yes** (log) |
| **Open path** | `composed_postgres_backend*` → `AsyncLogReplayBackend<PostgresLog, InMemoryProjection>` → BLB |
| **Blocking mechanism** | Sync `postgres::Client` on `PostgresLog` (`compose_log.rs`) via `InProcessLogStore` |
| **Exit criterion** | Product open must assemble `AsyncPostgresLog` (in-tree actor) instead of bare `PostgresLog` + `InProcessLogStore`; drop BLB after log-replay conformance on single-thread runtime. |

### 8. `postgres` × `sqlite`

| Field | Value |
|-------|--------|
| **Uses BlockingLibBackend today** | **Yes** (`open_postgres_log_cell` → `wrap_blocking_backend`) |
| **Blocking-under-poll** | **Yes** (log + projection) |
| **Open path** | `PostgresLog` + `SqliteProjectionStore` → `assemble_async_log_replay` → BLB |
| **Blocking mechanism** | Sync postgres client log + sync rusqlite projection |
| **Exit criterion** | `AsyncPostgresLog` + `AsyncSqliteProjectionStore` (or equivalent whole-tx offload per axis); drop BLB. |

### 9. `postgres` × `postgres`

| Field | Value |
|-------|--------|
| **Uses BlockingLibBackend today** | **Yes** (`open_postgres_runtime` Relational → BLB) |
| **Blocking-under-poll** | **Yes** |
| **Open path** | Unified `PostgresRelationalBackend` (same URL for log + projection) → BLB |
| **Blocking mechanism** | Port impls run sync SQL under mutex then `std::future::ready` (e.g. `PushPort::push` in `relational.rs`) — classic blocking-under-poll |
| **Exit criterion** | Whole-transaction adapter actor / `BlockingLogStore`-style ownership for the unified relational backend **or** native async postgres driver path; drop process-wide BLB; retain per-queue serialization (API-005 / ADR-017). |

### 10. `filesystem` × `memory`

| Field | Value |
|-------|--------|
| **Uses BlockingLibBackend today** | **Yes** (`open_objectlog` / `open_objectlog_memory_projection` → BLB) |
| **Blocking-under-poll** | **No** (product ports; see note) |
| **Open path** | `composed_objectlog_backend` / `AsyncObjectLogMemoryBackend::from_log_store` via `block_on_objectlog` at open, then BLB |
| **Product** | `AsyncObjectLogMemoryBackend` — `ObjectLogEngineStore` (`AsyncLogStore`, true `.await` produce) + `AsyncInMemoryProjection` |
| **Note** | `object-log` `LocalBlobStore` may use `spawn_blocking` / flush `block_in_place` **inside** the library (adapter-local). That is not process-wide BLB, but must stay out of bare reactor poll without offload. Open construction uses `block_on_objectlog` (`compose_log.rs`). |
| **Exit criterion** | Drop BLB wrap for this product after single-thread heartbeat under push/claim/finalize; keep any FS offload **inside** object-log / blob adapter; prefer async open helpers without facade `block_on_objectlog` on Tokio workers (`open_async` already `spawn_blocking`s object-log cells when postgres feature classifies them as needing offload). |

### 11. `filesystem` × `sqlite`

| Field | Value |
|-------|--------|
| **Uses BlockingLibBackend today** | **Yes** (`open_objectlog_sqlite` → `open_composed_sqlite` → BLB) |
| **Blocking-under-poll** | **Yes** (projection) |
| **Open path** | `AsyncObjectLogHybridBackend` / hybrid path: `ObjectLogEngineStore` + `InProcessProjectionStore<HybridProjectionStore>` (`async_product_hybrid.rs`) |
| **Blocking mechanism** | Sync rusqlite hybrid/sqlite projection apply on the polling thread; log axis is native async |
| **Exit criterion** | Replace `InProcessProjectionStore<HybridProjectionStore>` with actor/`BlockingProjectionStore`/async projection apply; drop BLB once projection apply is off-reactor. |

### 12. `filesystem` × `postgres`

| Field | Value |
|-------|--------|
| **Uses BlockingLibBackend today** | **Yes** (`open_objectlog_postgres_blocking` → BLB) |
| **Blocking-under-poll** | **Yes** (projection) |
| **Open path** | `AsyncObjectLogPostgresBackend` with `InProcessProjectionStore<PostgresRelational>` (`async_objectlog_postgres.rs`) |
| **Blocking mechanism** | Sync postgres projection through `InProcessProjectionStore`; log native async |
| **Exit criterion** | Use `AsyncPostgresRelationalProjection` (or equivalent) under the object-log product; drop BLB; keep open construction on `spawn_blocking` until connect is async-safe. |

### 13. `s3` × `memory`

| Field | Value |
|-------|--------|
| **Uses BlockingLibBackend today** | **Yes** (same object-log memory arm as filesystem) |
| **Blocking-under-poll** | **No** (product ports; S3 blob I/O is async HTTP) |
| **Open path** | `open_object_log_cell` → `ObjectLogStorage::S3Compatible` → `AsyncObjectLogMemoryBackend` → BLB |
| **Exit criterion** | Same as filesystem×memory: drop BLB after heartbeat proof; no process-wide pool required for product ports. |

### 14. `s3` × `sqlite`

| Field | Value |
|-------|--------|
| **Uses BlockingLibBackend today** | **Yes** |
| **Blocking-under-poll** | **Yes** (projection; same hybrid/sqlite seam as filesystem×sqlite) |
| **Open path** | `open_objectlog_sqlite` with S3 object-log config |
| **Exit criterion** | Same as filesystem×sqlite — async-safe projection axis; drop BLB. |

### 15. `s3` × `postgres`

| Field | Value |
|-------|--------|
| **Uses BlockingLibBackend today** | **Yes** |
| **Blocking-under-poll** | **Yes** (projection; same as filesystem×postgres) |
| **Open path** | `open_objectlog_postgres_blocking` with S3 log |
| **Exit criterion** | Same as filesystem×postgres — async-safe postgres projection; drop BLB. |

---

## Cross-cutting residual mechanisms

| Mechanism | Where | Cells affected | Role vs process-wide BLB |
|-----------|-------|----------------|---------------------------|
| `BlockingLibBackend` / `shared_worker_pool` | `crates/fireweed/src/blocking_backend.rs` | All except `memory`×`memory` | **Process-wide product bridge** (epic removal target) |
| `InProcessLogStore` / `InProcessProjectionStore` | `fireweed-engine/src/async_store.rs` | Log-replay durable cells; object-log × sqlite/postgres | Eager ready futures → BUP **yes** |
| Sync rusqlite | `fireweed-sqlite` (`SqliteLog`, `SqliteProjectionStore`, `HybridProjectionStore`, relational) | Any cell with sqlite axis | Blocking substrate |
| Sync `postgres::Client` | `fireweed-postgres` (`PostgresLog`, `PostgresRelational`, relational backend ports) | Any cell with postgres axis (except pure object-log×memory) | Blocking substrate |
| `block_on_objectlog` | `fireweed-objectlog/src/compose_log.rs` | Object-log open construction | Open-time bridge (dedicated thread when Tokio present); not a substitute for BLB removal on ports |
| `open_async` + `spawn_blocking` | `lib.rs` `storage_open_needs_blocking_offload` | Postgres log/projection and object-log opens | Construction-only offload |
| Adapter actors (unused by matrix open) | `AsyncPostgresLog`, `AsyncPostgresRelationalProjection`, `AsyncSqliteProjectionStore` | Candidates for exit criteria | **Desired** adapter-local offload (ADR-015) |

---

## Exit priority (planning only)

Order is a suggestion for epic decomposition — not a commitment:

1. **Already clear:** `memory` × `memory`.
2. **Drop BLB first candidates:** `filesystem`/`s3` × `memory` (product ports already native-async).
3. **Swap to in-tree actors:** postgres-axis cells → `AsyncPostgresLog` / `AsyncPostgresRelationalProjection`; sqlite projection cells → `AsyncSqliteProjectionStore`.
4. **Sqlite log / unified relational:** still need connection-affine whole-tx offload or native async driver; highest residual risk.
5. **Global gate (epic AC):** `wrap_blocking_backend` / `BlockingLibBackend::new` absent from product open arms (or test-only); API-005 bridge removal criterion met.

---

## Related documents

- [API-005 Fireweed Rust facade](../02-design/contracts/API-005-fireweed-rust-facade.md) — native-async end-state; BLB residual
- [ADR-015 full-async storage boundaries](../02-design/adr/ADR-015-full-async-storage-boundaries.md)
- [Storage matrix composition inventory](./storage-matrix-composition-inventory.md) — wire-up map (distinct concern)
- [Orthogonal storage matrix brief](../02-design/orthogonal-storage-matrix-brief.md)
