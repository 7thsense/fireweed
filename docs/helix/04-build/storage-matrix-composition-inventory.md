---
ddx:
  id: storage-matrix-composition-inventory
  depends_on:
    - orthogonal-storage-matrix-brief
    - adr-orthogonal-log-projection-composition
  status: accepted
---

# Storage matrix composition inventory (5×3)

**Bead**: `fireweed-7ab8e390` (inventory) · updated by Phase-2 wire beads incl. `fireweed-b89a086d`  
**Scope**: Map each public log × projection cell to composition path; keep grid
aligned with server allowlist + match arms after wiring.  
**Sources read / revalidated** (2026-07-28):

| Surface | Path |
|---------|------|
| Server composition root | `crates/fireweed-server/src/lib.rs` (`match (log, projection)` in run path) |
| Env / product allowlist | `crates/fireweed-server/src/env_config.rs` (`parse_backend` wired matrix) |
| Facade constructors | `crates/fireweed/src/lib.rs` (`open_*`, `StorageConfig`) |
| Adapter constructors | `fireweed-memory`, `fireweed-sqlite`, `fireweed-postgres`, `fireweed-objectlog` |

## Public axes

| Axis | Values |
|------|--------|
| **Log** | `memory`, `sqlite`, `postgres`, `filesystem`, `s3` |
| **Projection** | `memory`, `sqlite`, `postgres` |

`filesystem` and `s3` are first-class log names for the segmented object-log family
(`LogSpec::ObjectLog(ObjectLogSpec::LocalFilesystem | S3)`). Legacy env alias
`objectlog` (+ `FIREWEED_OBJECT_LOG_STORE=local|s3`) still parses to those logs.

**Not public matrix projections** (still parseable on server): `hybrid`,
`hybrid-strict`, `hybrid-async`, `turso`. They are noted only where they share an
object-log code path; they are **not** matrix rows.

**Config vs open**: `StorageConfig` in `crates/fireweed/src/lib.rs` validates all
15 pairs as structurally valid. Opening a cell that is not wired still fails at
composition (server: `unsupported storage configuration…pairing is not wired`;
facade: no single `open(StorageConfig)` dispatcher yet).

## Summary grid (server product wire-up)

| Log \ Projection | `memory` | `sqlite` | `postgres` |
|------------------|----------|----------|------------|
| **memory** | yes | yes | yes (`postgres` feature) |
| **sqlite** | yes | yes | yes (`postgres` feature) |
| **postgres** | yes | yes | yes |
| **filesystem** | yes | yes | yes (`postgres` feature) |
| **s3** | yes | yes | yes (`postgres` feature) |

Object-log cells (`filesystem` / `s3`) share `LogSpec::ObjectLog(_)` match arms;
S3 is selected via `ObjectLogSpec::S3` (env `FIREWEED_LOG_BACKEND=s3` or legacy
`objectlog` + `FIREWEED_OBJECT_LOG_STORE=s3`).

**Counts (server public matrix)**: 15 wired (postgres projection cells require the
`postgres` cargo feature).

---

## Cell inventory

Legend for **wired**:

- **Server**: accepted by `parse_backend` allowlist and assembled in the server
  `match (log, projection)` composition root.
- **Facade**: reachable via a public (or documented feature-gated) `open_*`
  constructor on the `fireweed` crate.

### 1. `memory` × `memory`

| Field | Value |
|-------|--------|
| **Wired** | Server **yes** · Facade **yes** |
| **Server path** | `env_config.rs`: `(LogSpec::Memory, ProjectionSpec::InMemory) => true`; `lib.rs` match arm `(LogSpec::Memory, ProjectionSpec::InMemory)` → `composed_memory_backend().with_node_id(node_id)` |
| **Facade** | `fireweed::open_memory` → `fireweed_memory::composed_memory_backend()` |
| **Constructor** | `fireweed_memory::composed_memory_backend` → `ComposedBackend<MemoryLog, InMemoryProjection, InProcessControlPlane>` |
| **Notes** | Class B reference cell. Env: `FIREWEED_LOG_BACKEND=memory`, `FIREWEED_PROJECTION_BACKEND=memory` (alias `inmemory`). |

### 2. `memory` × `sqlite`

| Field | Value |
|-------|--------|
| **Wired** | Server **yes** · Facade **partial** (no dedicated public `open_*`; server Class B arm) |
| **Server path** | Allowlist `(Memory, Sqlite)`; match arm `MemoryLog` × `SqliteProjectionStore` + recover |
| **Facade** | No dedicated `open_memory_sqlite`; composition available via server / engine adapters |
| **Constructor** | Server Class B assembly in `lib.rs` |
| **Notes** | Class B: durable projection survives process death without log rebuild. |

### 3. `memory` × `postgres`

| Field | Value |
|-------|--------|
| **Wired** | Server **yes** (feature `postgres`) · Facade **partial** |
| **Server path** | Allowlist + match arm `MemoryLog` × `PostgresRelational` |
| **Facade** | No dedicated `open_memory_postgres` |
| **Constructor** | Server Class B assembly in `lib.rs` |
| **Notes** | Class B; feature-gated postgres projection. |

### 4. `sqlite` × `memory`

| Field | Value |
|-------|--------|
| **Wired** | Server **yes** · Facade **yes** |
| **Server path** | Allowlist `(LogSpec::Sqlite, ProjectionSpec::InMemory)`; match arm opens 8-worker pool via `fireweed_sqlite::composed_sqlite_backend_for_worker` |
| **Facade** | `fireweed::open_sqlite(path, clock)` → `composed_sqlite_backend(path)` |
| **Constructor** | `fireweed_sqlite::composed_sqlite_backend` / `_for_worker` → `ComposedBackend<SqliteLog, InMemoryProjection, …>` |
| **Notes** | Class A. Projection rebuilt from durable sqlite command log on open. Env: `FIREWEED_LOG_BACKEND=sqlite`, `FIREWEED_PROJECTION_BACKEND=memory`, log path `FIREWEED_SQLITE_LOG_PATH`. |

### 5. `sqlite` × `sqlite`

| Field | Value |
|-------|--------|
| **Wired** | Server **yes** · Facade **yes** |
| **Server path** | Allowlist + match arm with **distinct** log path vs projection path |
| **Facade** | `open_sqlite_sqlite_projection` (orthogonal log × projection paths) |
| **Constructor** | `ComposedBackend<SqliteLog, SqliteProjectionStore, …>` (not unified relational) |
| **Notes** | Distinct from `open_sqlite_relational` (same-store unified relational). |

### 6. `sqlite` × `postgres`

| Field | Value |
|-------|--------|
| **Wired** | Server **yes** (feature `postgres`) · Facade **yes** (feature `postgres`) |
| **Server path** | Allowlist + match arm `SqliteLog` × `PostgresRelational` (off-reactor connect) |
| **Facade** | `open_sqlite_postgres_projection` |
| **Constructor** | `ComposedBackend<SqliteLog, PostgresRelational, …>` |
| **Notes** | Class A; distinct sqlite log path + postgres projection DSN. |

### 7. `postgres` × `memory`

| Field | Value |
|-------|--------|
| **Wired** | Server **yes** (feature `postgres`) · Facade **yes** (feature `postgres`) |
| **Server path** | Allowlist + match arm `(LogSpec::Postgres, ProjectionSpec::InMemory)` → pool of `composed_postgres_backend_for_worker_with_config` via `blocking_backend_pool` |
| **Facade** | `open_postgres` / `open_postgres_async` / `open_postgres_runtime` (`PostgresMode::LogReplay`) → `composed_postgres_backend*` |
| **Constructor** | `fireweed_postgres::composed_postgres_backend*` → `ComposedBackend<PostgresLog, InMemoryProjection, …>` |
| **Notes** | Class A log-replay. Env: `FIREWEED_LOG_BACKEND=postgres`, `FIREWEED_PROJECTION_BACKEND=memory`. |

### 8. `postgres` × `sqlite`

| Field | Value |
|-------|--------|
| **Wired** | Server **yes** (feature `postgres`) · Facade **no** dedicated public cell |
| **Server path** | Match arm `(LogSpec::Postgres, ProjectionSpec::Sqlite { path })` → `PostgresLog::connect_with_config` + `SqliteProjectionStore::open` + `ComposedBackend::new(...).recover()` |
| **Facade** | **MISSING** as first-class `open_*` / `StorageConfig` open; server-only composition today |
| **Constructor** | Inline in server: `ComposedBackend<PostgresLog, SqliteProjectionStore, InProcessControlPlane>` |
| **Notes** | Env: postgres log DSN + `FIREWEED_PROJECTION_BACKEND=sqlite` + `FIREWEED_SQLITE_PROJECTION_PATH`. |

### 9. `postgres` × `postgres`

| Field | Value |
|-------|--------|
| **Wired** | Server **yes** (feature `postgres`) · Facade **yes** (unified relational / coordinated modes) |
| **Server path** | Match arm `(LogSpec::Postgres, ProjectionSpec::Postgres { url })` → `fixed_postgres_relational_pool` when log URL **equals** projection URL; rejects split URLs |
| **Facade** | `open_postgres_runtime` / `open_postgres_coordinated` (relational / multi-instance modes on postgres crate) |
| **Constructor** | `fireweed_server::fixed_postgres_relational_pool` / `fireweed_postgres::composed_postgres_relational_in_schema` (unified relational backend) |
| **Notes** | Atomic single-DB transaction mode (TD-002). Not a split log-URL + projection-URL composition. |

### 10. `filesystem` × `memory`

| Field | Value |
|-------|--------|
| **Wired** | Server **yes** · Facade **yes** (local object-log convenience) |
| **Server path** | `LogSpec::ObjectLog(LocalFilesystem)` + `ProjectionSpec::InMemory` → `SegmentedObjectLogInMemoryBackend::open_with_blob_store` (8-worker pool + flusher) |
| **Facade** | `open_objectlog(root, clock)` → `fireweed_objectlog::composed_objectlog_backend(root)` (local FS only) |
| **Constructor** | Segmented: `SegmentedObjectLogInMemoryBackend`; composed: `composed_objectlog_backend` |
| **Notes** | Env: `FIREWEED_LOG_BACKEND=filesystem` (or legacy `objectlog` + store local), projection `memory`. Root: `FIREWEED_OBJECT_LOG_ROOT`. |

### 11. `filesystem` × `sqlite`

| Field | Value |
|-------|--------|
| **Wired** | Server **yes** · Facade **yes** |
| **Server path** | `ObjectLog(LocalFilesystem)` + `ProjectionSpec::Sqlite` → `SegmentedObjectLogSqliteBackend::open_with_blob_store_and_projection` |
| **Facade** | `open_objectlog_sqlite(ObjectLogRuntimeConfig)` with `ObjectLogStorage::Local` → `open_composed_sqlite` (`HybridProjectionStore` + group-commit) |
| **Constructor** | Server: segmented sqlite projection backend; facade: `ComposedBackend<ObjectLog, HybridProjectionStore, …>` |
| **Notes** | Public projection name is `sqlite`. Legacy hybrid/hybrid-strict/hybrid-async re-use the same object-log + SQLite-image substrate with different apply barriers; not separate matrix rows. |

### 12. `filesystem` × `postgres`

| Field | Value |
|-------|--------|
| **Wired** | Server **yes** (feature `postgres`) · Facade **yes** (feature `objectlog` + `postgres`) |
| **Server path** | Shared `(LogSpec::ObjectLog(spec), ProjectionSpec::Postgres)` → `open_objectlog_postgres_backend` (authoritative group-commit ObjectLog + `PostgresRelational`) |
| **Facade** | `open_objectlog_postgres` / `_async` with `ObjectLogStorage::Local` |
| **Constructor** | Server: `open_objectlog_postgres_backend`; facade: `open_objectlog_postgres_blocking` |
| **Notes** | Same composition arm as s3×postgres; blob store is `LocalFsBlobStore`. |

### 13. `s3` × `memory`

| Field | Value |
|-------|--------|
| **Wired** | Server **yes** · Facade **partial** (no single-arg `open_s3`; use object-log config or server) |
| **Server path** | Same arm as filesystem×memory: `ObjectLog(S3)` + `InMemory` → `SegmentedObjectLogInMemoryBackend` after `ObjectLogSpec::S3::open_blob_store*` |
| **Facade** | No dedicated `open_s3_memory`; `StorageConfig` accepts `LogConfig::S3` but no full-matrix open dispatcher. Object-log composed helpers historically local-first. |
| **Constructor** | Shared segmented in-memory projection backend over `S3BlobStore` |
| **Notes** | Env: `FIREWEED_LOG_BACKEND=s3` (+ S3 endpoint/bucket/creds env). Server wires S3 credentials as static key pair only today. |

### 14. `s3` × `sqlite`

| Field | Value |
|-------|--------|
| **Wired** | Server **yes** · Facade **yes** |
| **Server path** | Same arm as filesystem×sqlite with `ObjectLogSpec::S3` blob store |
| **Facade** | `open_objectlog_sqlite` with `ObjectLogStorage::S3Compatible { … }` |
| **Constructor** | Segmented object-log + sqlite projection (server); hybrid-composed path (facade) |
| **Notes** | Well-trodden production path (TD-004 evidence lineage). |

### 15. `s3` × `postgres`

| Field | Value |
|-------|--------|
| **Wired** | Server **yes** (feature `postgres`) · Facade **yes** (feature `objectlog` + `postgres`) |
| **Server path** | Shared `(LogSpec::ObjectLog(spec), ProjectionSpec::Postgres)` → `open_objectlog_postgres_backend` over `ObjectLogSpec::S3` (+ optional Postgres publication authority for create-only) |
| **Facade** | `open_objectlog_postgres` with `ObjectLogStorage::S3Compatible` |
| **Constructor** | Server: `open_objectlog_postgres_backend`; facade: `open_objectlog_postgres_blocking` |
| **Notes** | First-class env: `FIREWEED_LOG_BACKEND=s3` + `FIREWEED_PROJECTION_BACKEND=postgres` + S3 + projection DSN. Unit tests cover env parse + composition root; live open when S3+PG fixtures are set. Bead `fireweed-b89a086d`. |

---

## Allowlist source of truth (server)

`parse_backend` in `crates/fireweed-server/src/env_config.rs` only marks these
pairings as wired (public + legacy non-public):

```text
Memory          × InMemory | Sqlite | Postgres(feature postgres)
Sqlite          × InMemory | Sqlite | Postgres(feature postgres)
ObjectLog       × InMemory | Sqlite | Postgres(feature postgres)
                  | Turso | Hybrid | HybridStrict | HybridAsync   (legacy non-public)
Postgres        × InMemory | Sqlite | Postgres   (feature postgres)
```

Everything else returns:

```text
unsupported storage configuration FIREWEED_LOG_BACKEND=… FIREWEED_PROJECTION_BACKEND=…:
this FIREWEED_LOG_BACKEND × FIREWEED_PROJECTION_BACKEND pairing is not wired by fireweed-server
```

Runtime double-check: the catch-all match arm in `lib.rs` errors with
`unsupported backend composition: log=… projection=… (not wired by fireweed-server)`.

---

## Facade `open_*` map (by matrix intent)

| Constructor | Matrix cell(s) | Notes |
|-------------|----------------|-------|
| `open_memory` | memory×memory | |
| `open_sqlite` | sqlite×memory | |
| `open_sqlite_relational` | *(unified sqlite relational)* | Not orthogonal sqlite×sqlite |
| `open_objectlog` | filesystem×memory | Local root convenience only |
| `open_objectlog_sqlite` | filesystem×sqlite, s3×sqlite | Via `ObjectLogRuntimeConfig` storage variant |
| `open_objectlog_postgres` | filesystem×postgres, s3×postgres | Feature-gated; **server-wired** via shared ObjectLog arm |
| `open_postgres` / `_async` | postgres×memory | Log-replay |
| `open_postgres_runtime*` | postgres×memory / postgres relational modes | Schema, node_id, coordination knobs |
| `open_postgres_coordinated` | multi-instance postgres | Coordination-focused |
| `open_sqlite_sqlite_projection` / `open_sqlite_postgres_projection` | sqlite×sqlite, sqlite×postgres | Server-wired; Class B memory cells are server-primary |
| *none* | full `open(StorageConfig)` | Type exists; open dispatcher **MISSING** |

---

## Gap list (post Phase-2 wire beads)

| Priority theme | Status | Remaining work |
|----------------|--------|----------------|
| Class B memory log | Server wired (memory×sqlite, memory×postgres) | Dedicated facade `open_*` optional |
| Sqlite log orthogonal projections | Server + facade wired | Evidence/conformance beads |
| Object-log × postgres (server) | Server wired for filesystem\|s3 × postgres | Live S3×PG CI when fixtures present |
| Product open surface | Typed `StorageConfig` validates all 15 | Single `open(StorageConfig)` dispatcher still optional |

---

## Related non-matrix paths (do not count as matrix rows)

| Path | Why noted |
|------|-----------|
| objectlog × hybrid / hybrid-strict / hybrid-async | Server-wired; SQLite-image + hot memory; implementation profile, not public projection axis |
| objectlog × turso | Feature `turso-projection`; not public projection |
| `open_sqlite_relational` | Unified relational SQLite store, not log×projection pair of distinct stores |
| postgres/postgres identical-URL rule | Atomic relational mode, not independent dual-DSN composition |

---

## Acceptance check for this inventory

- Document path: `docs/helix/04-build/storage-matrix-composition-inventory.md`
- Mentions all five logs: `memory`, `sqlite`, `postgres`, `filesystem`, `s3`
- Mentions all three projections: `memory`, `sqlite`, `postgres`
- Marks remaining optional product-surface gaps (dispatcher / dedicated facade opens)
- Updated for Phase-2 wire beads including `fireweed-b89a086d` (s3 × all projections)
)
