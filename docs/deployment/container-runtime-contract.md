# Fireweed Container Image and Runtime Configuration Contract

The release image entrypoint is `fireweed-service`, the RESP server built from
`crates/fireweed-server`.

## Structured storage model (normative)

Container configuration is an **adapter** into the product storage model. The
product definition is the orthogonal product of **two storage axes** (plus
control plane), not a table of environment variable names.

| Axis | Public values | Notes |
|------|---------------|-------|
| **Log backend** | `memory`, `sqlite`, `postgres`, `filesystem`, `s3` | Command append, authority, replay when durable |
| **Projection** | `memory`, `sqlite`, `postgres` | Serving, claim selection, validation, apply |

Typed **`StorageConfig`** (API-005, `orthogonal-storage-matrix-brief`) is the
normative composition root. The service process must assemble one
`StorageConfig` (or an isomorphic structured config) at startup; composition
uses only that model. Helm `storage.log.*` / `storage.projection.*` is the
Kubernetes deploy document and must converge on the same axes and field shape.

`filesystem` and `s3` are first-class object-log peers (same protocol: segments,
manifest, conditional write / authority, retention). They are not profile SKUs
and are not “fake S3” vs “real S3.” Pair strings (`objectlog/sqlite`, …) may
appear in historical evidence IDs and transitional wiring only.

| Log \ Projection | `memory` | `sqlite` | `postgres` |
|------------------|----------|----------|------------|
| `memory` | Class B | Class B | Class B |
| `sqlite` | Class A | Class A | Class A |
| `postgres` | Class A | Class A | Class A |
| `filesystem` | Class A | Class A | Class A |
| `s3` | Class A | Class A | Class A |

**Durability (summary):** Class A success ⇒ durable on the log and visible in
the serving projection; recovery via high-water + tail when the log remains.
Class B success ⇒ visible in the projection; durable only if the projection is
durable; after process death only the projection remains. The contract **must
not collapse those axes** into a single combined backend name or silent
downgrade.

Non-public implementation paths (for example hybrid / hybrid-async / turso
projections under a durable log) may still appear in transitional runtime
wiring; they are **not** public projection product values and must not be
framed as matrix rows.

### Illustrative structured shape

```text
StorageConfig
  log:
    Memory
    | Sqlite { path }
    | Postgres { url, … }
    | Filesystem { root }
    | S3 { endpoint, bucket, region, credentials, … }
  projection:
    Memory
    | Sqlite { path }
    | Postgres { url, … }
  control_plane: …
  # object-log: segments, authority, recovery where applicable
```

Persistent paths use `/var/lib/fireweed`, and database identifiers use the
`fireweed_*` namespace. The runtime accepts only the documented `FIREWEED_*`
configuration namespace for **injection** (see appendix below).

## Transaction and Storage-Combination Contract

The storage axes are implementation choices, not API variants. Any executable
combination MUST preserve the same native Fireweed transaction contract:

- a successful mutating response means accepted effects are durable and visible
  to subsequent reads, claims, idempotency replay, and restart recovery under
  the selected durability class;
- a rejected envelope has no committed item effect, and a rejected item in a
  partial batch has no committed effect for that item;
- a transport failure, timeout, or service crash after submission is resolved by
  retrying the same `request_id`, never by caller-side log repair;
- local projections, segment buffering, and replay are internal implementation
  details.

Unsupported or not-yet-verified log/projection combinations fail closed at
startup. They must not silently downgrade to a weaker durability or projection
cell.

### Current transitional wiring (not the product definition)

Until the server composition root is fully driven by `StorageConfig` for all
15 cells, the process may still select combinations through legacy env spellings
(`objectlog` for the object-log protocol family, `inmemory` for memory
projection, optional hybrid paths). Map those spellings onto the axes above:

| Legacy / transitional selection | Product axes (intent) |
|---------------------------------|------------------------|
| `objectlog` + local root | log `filesystem` |
| `objectlog` + S3 credentials/endpoint | log `s3` |
| `postgres` / `sqlite` / `memory` log | same public log names |
| `inmemory` projection | projection `memory` |
| `sqlite` / `postgres` projection | same public projection names |
| `hybrid`, `hybrid-async`, `hybrid-strict`, `turso` | non-public implementation paths |

The current server composition root wires `memory`/`inmemory`, `sqlite`/`inmemory`,
`objectlog`/`inmemory`, `objectlog`/`sqlite`, `objectlog`/`hybrid`, and
`objectlog`/`hybrid-async` unconditionally under those legacy names. Only the
object-log log axis pairs with `hybrid-async`; other hybrid-async pairings fail
closed at startup. `postgres` + projection cells are wired through the
blocking-safe `PostgresNativeBackend` wrapper when the binary is built with the
`postgres` cargo feature (`--features postgres`, or `--features postgres,tls`
for native-tls). The default release image does **not** build that feature, so
in the shipped image `postgres` still fails at startup with an explicit message
pointing at the required feature build. Other combinations fail at startup with
an explicit unsupported-storage message.

`filesystem`/`s3` (object-log) cells with a local durable projection treat the
object log as command authority; the local projection is rebuilt from committed
state. Hybrid-style paths add a SQLite-first durable image plus hot in-memory
serving (implementation detail, not a public projection axis). Unsupported
pairings fail closed until intentionally implemented and tested. Multi-owner S3
release claims additionally require manifest-CAS stale-owner fencing and the
TP-003 external transaction-contract matrix for the exact cell.

### Snapshot-tail recovery (filesystem/s3 log × sqlite projection)

The SQLite materialized projection is both the queried state AND a durable
**snapshot**: it persists, per queue, a **high-water** (`relational_cursor.
next_seq`) advanced inside the same transaction that applies each sealed
object-log batch. On reopen the server reads that high-water and replays only
the object-log **tail** at sequences `>= high_water` — segments lying entirely
in the snapshot are never fetched or decoded — rather than replaying the full
genesis log. Because the high-water only advances with the projection apply, it
can never lead what is durably materialized; a crash between an object-log
commit and its projection apply leaves the uncommitted tail to be replayed
(idempotently — an already-applied prefix is skipped, never double-applied), so
restart preserves committed state. The recovery-window budget
(`FIREWEED_RECOVERY_MAX_TAIL_COMMANDS` in the injection map) bounds the expected
tail and warns when exceeded. The `segmented` substrate realizes the genuine
bounded-tail I/O saving; the `file` substrate resumes apply at the high-water
(it remains the per-command smoke reference).

### Hybrid recovery and poisoning (transitional objectlog/hybrid path)

Where still wired, the hybrid implementation path applies each committed
object-log batch to SQLite first and then to the hot in-memory projection. The
in-memory projection serves claim selection, `peek`, `pending`, metrics,
live-item lookup, secondary-index lookup, and pre-commit validation. If SQLite
apply fails, no success response is returned and restart replays the object-log
tail. If SQLite commits but memory apply fails, the process-local projection is
**poisoned**: the current operation returns storage failure, every later
read/validation/write fails closed, and the process must restart.

On restart, the server exports a complete SQLite `ProjectionImage`, hydrates
memory, and only then returns the SQLite high-water so recovery can skip the
historical object-log prefix. If hydration fails, recovery fails closed or
replays from genesis. The local SQLite high-water is not authority for segment
expiry; object-log segments remain retained unless covered by a committed
object-store snapshot and recovery window. Durable push `request_id` replay is
part of the runtime contract: a committed-but-unreturned push retry returns the
original item ids for the same body and `request-id-conflict` for a different
body.

### Helm structured keys

The Helm chart exposes storage as structured axes (isomorphic target:
`StorageConfig`):

- `storage.log.backend` (and object-log store / S3 / filesystem fields)
- `storage.projection.backend` (and projection path / URL fields)
- `storage.controlPlane.backend` where applicable

Kubernetes probes are TCP probes against the RESP port.

### Databricks Lakebase (postgres over TLS)

Lakebase is a **real runtime path**, not render-only: build the service image with the
`tls` feature and point it at the rendered DSN. The Helm postgres/Lakebase profile
renders the DSN Secret into the container injection map as
`FIREWEED_POSTGRES_LOG_DATABASE_URL`, which takes precedence over the
local-development `FIREWEED_PG_URL` fallback. A `key=value` or URL DSN with
`sslmode=require` connects over the native-tls connector; an `sslmode=require`
DSN on a non-tls build fails closed at startup (it never silently downgrades to
plaintext). When `DATABRICKS_HOST` and the service-principal
(`DATABRICKS_CLIENT_ID`/`DATABRICKS_CLIENT_SECRET`) or PAT
(`DATABRICKS_TOKEN`/`FIREWEED_DATABRICKS_POSTGRES_USER`) envs are present, a
credential provider injects the postgres user/password at connect instead of the
DSN password.

Build the TLS image with the documented build arg:

```sh
docker build --build-arg CARGO_FEATURES=tls -t fireweed-service:tls .
```

(or `FIREWEED_FEATURES=tls scripts/release/package-binaries.sh` for the binary tarball).
The runtime image installs `ca-certificates` + `libssl3` so the native-tls connector
can verify the Lakebase server certificate. This wires the connection path only; the
live Lakebase provider-certification run remains separate (`pqueue-ea625701`).

---

## Appendix: container injection map

Environment variables are **not** the product definition of storage. They are
the process injection surface that adapters deserialize into structured
configuration (`StorageConfig` / server `Config`). Prefer Helm structured
`storage.*` values or typed library construction; use this map when injecting
into the stock container image.

| Key | Required | Default | Meaning |
|-----|----------|---------|---------|
| `FIREWEED_LISTEN_ADDR` | no | `0.0.0.0:8080` | RESP listen address. |
| `FIREWEED_LOG_BACKEND` | no | `objectlog` | **Injection** for log axis (transitional spellings): `objectlog`, `postgres`, `sqlite`, or `memory`. Product names: `objectlog` maps to `filesystem` or `s3` via object-log store/credentials; others match public log names. |
| `FIREWEED_PROJECTION_BACKEND` | no | `inmemory` | **Injection** for projection axis (transitional): `inmemory`, `sqlite`, `hybrid`, `hybrid-async`, or `postgres`. Product public values are `memory` (`inmemory`), `sqlite`, `postgres`; hybrid* are non-public paths. |
| `FIREWEED_OBJECT_LOG_ROOT` | when log is object-log / `filesystem` | `/var/lib/fireweed/object-log` | Local/filesystem object-log root. |
| `FIREWEED_SQLITE_LOG_PATH` | when log is `sqlite` | `/var/lib/fireweed/fireweed-log.db` | Local SQLite log path. |
| `FIREWEED_SQLITE_PROJECTION_PATH` | when projection is `sqlite` (or transitional hybrid*) | `/var/lib/fireweed/fireweed-projection.db` | Local SQLite materialized projection path (or hybrid durable image path). |
| `FIREWEED_OBJECT_LOG_MODE` | no | `file` | Object-log substrate: `file` (per-command) or `segmented` (group-commit, the production form). |
| `FIREWEED_SEGMENT_TARGET_BYTES` | no | `262144` | `segmented`: byte-size seal trigger. |
| `FIREWEED_SEGMENT_MAX_LATENCY_MS` | no | `20` | `segmented`: latency seal trigger and implementation of the object-log commit-latency-bound knob (`max_commit_latency_ms`). Lower values reduce mutation latency and increase object/log request cost; higher values improve batch density and increase latency. This knob must not weaken transaction integrity. |

The object-log segment format is fixed by this Fireweed release. It has no runtime format selector; storage
containing any retired pre-release frame or metadata namespace is rejected during open.

| Key | Required | Default | Meaning |
|-----|----------|---------|---------|
| `FIREWEED_RECOVERY_MAX_TAIL_COMMANDS` | no | `1000000` | Object-log × durable local projection recovery-window budget. A reopen recovers from the SQLite projection snapshot/image + its recorded high-water and replays only the object-log tail beyond it (not the full genesis log). A tail longer than this budget is logged as a recovery-window warning (the projection has fallen far behind the durable log). |
| `FIREWEED_HYBRID_ASYNC_APPLY_LAG_MAX_COMMANDS` | no | `100000` | Transitional `hybrid-async`: hard cap on committed command sequences the durable SQLite checkpoint may trail the object-log head before backpressure. Must be `> 0`; a zero bound fails closed at startup. |
| `FIREWEED_HYBRID_ASYNC_APPLY_DEBT_MAX_BYTES` | no | `536870912` | Transitional `hybrid-async`: hard cap on retained object-log bytes not yet trimmable via async apply. Must be `> 0`. |
| `FIREWEED_HYBRID_ASYNC_APPLY_QUEUE_DEPTH_MAX` | no | `1024` | Transitional `hybrid-async`: hard cap on sealed segment batches awaiting async SQLite apply. Must be `> 0`. |
| `FIREWEED_HYBRID_ASYNC_OLDEST_UNAPPLIED_MAX_MS` | no | `60000` | Transitional `hybrid-async`: hard cap on the age of the oldest unapplied committed command. Must be `> 0`. |
| `FIREWEED_HYBRID_ASYNC_APPLY_POISON_RETRY_THRESHOLD` | no | `3` | Transitional `hybrid-async`: consecutive failed SQLite apply attempts for a batch that trip fail-closed poison. Must be `> 0`. |
| `FIREWEED_POSTGRES_LOG_DATABASE_URL` | when log is `postgres` (Helm) | _(none)_ | The DSN the Helm postgres/Lakebase path renders from the log-backend Secret. Takes precedence over `FIREWEED_PG_URL`. A libpq URL **or** `key=value` DSN, with a native password; `sslmode=require` selects the native-tls path. |
| `FIREWEED_PG_URL` | when log is `postgres` (local/dev) | `postgres://postgres@127.0.0.1:5432/postgres` | libpq/postgres connection string (URL or `key=value` DSN); the fallback when `FIREWEED_POSTGRES_LOG_DATABASE_URL` is unset. With `sslmode=require` (or `prefer`) the binary must be built `--features postgres,tls` to connect over native-tls; on a non-tls build an `sslmode=require` DSN fails closed at startup (no plaintext downgrade). |
| `DATABRICKS_HOST`, `DATABRICKS_DATABASE_INSTANCE_NAME`, `DATABRICKS_CLIENT_ID`+`DATABRICKS_CLIENT_SECRET` (service principal) or `DATABRICKS_TOKEN`+`FIREWEED_DATABRICKS_POSTGRES_USER` (PAT) | when using Databricks Lakebase credentials | _(none)_ | Optional Databricks credential injection for the `postgres` backend: when `DATABRICKS_HOST` is set, a service-principal/PAT credential provider supplies the postgres user/password at connect instead of the DSN password. |
| `FIREWEED_BOOTSTRAP_QUEUES` | no | `t1:q1` | Comma-separated `tenant:queue` bootstrap list. A non-empty value takes precedence over generated inventory settings. |
| `FIREWEED_BOOTSTRAP_GENERATED_COUNT` | no | _(none)_ | Generate this many bootstrap queues in deterministic numeric order. Valid range: 1–10,000. When absent, generation is disabled. |
| `FIREWEED_BOOTSTRAP_GENERATED_TENANT` | no | `t1` | Tenant for generated bootstrap queues. |
| `FIREWEED_BOOTSTRAP_GENERATED_PREFIX` | no | `q` | Queue prefix for generated bootstrap queues (`q0`, `q1`, … with the default). |
| `FIREWEED_RECLAIM_INTERVAL_MS` | no | `1000` | Reclaim tick interval. |
