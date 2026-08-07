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
| **Projection** | `memory`, `sqlite`, `turso` (default), `postgres` | Serving, claim selection, validation, apply |

Typed **`StorageConfig`** (API-005, `orthogonal-storage-matrix-brief`) is the
normative composition root. The service process must assemble one
`StorageConfig` (or an isomorphic structured config) at startup; composition
uses only that model. Helm `storage.log.*` / `storage.projection.*` is the
Kubernetes deploy document and must converge on the same axes and field shape.

`filesystem` and `s3` are first-class object-log peers (same protocol: segments,
manifest, conditional write / authority, retention). They are not profile SKUs
and are not “fake S3” vs “real S3.” Historical evidence IDs may still name older
pair strings; public product selection uses only the axes above.

| Log \ Projection | `memory` | `sqlite` | `turso` (default) | `postgres` |
|------------------|----------|----------|-------------------|------------|
| `memory` | Class B | Class B | Class B | Class B |
| `sqlite` | Class A | Class A | Class A | Class A |
| `postgres` | Class A | Class A | Class A | Class A |
| `filesystem` | Class A | Class A | Class A | Class A |
| `s3` | Class A | Class A | Class A | Class A |

**Durability (summary):** Class A success ⇒ durable on the log and visible in
the serving projection; recovery via high-water + tail when the log remains.
Class B success ⇒ visible in the projection; durable only if the projection is
durable; after process death only the projection remains. The contract **must
not collapse those axes** into a single combined backend name or silent
downgrade.

Non-public implementation paths (for example demoted hybrid-style projections
under a durable log) may still exist for direct typed `Config` construction and
internal tests; they are **not** public projection product values and must not
be framed as matrix rows or container injection values.

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
    | Turso { path }          # public default
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

### Current composition wiring

The container injection map accepts **only** public product axis names:

- log: `memory` | `sqlite` | `postgres` | `filesystem` | `s3`
- projection: `memory` | `sqlite` | `turso` | `postgres`

Defaults are `filesystem` × `turso` (TD-010). The documented Turso path env is
`FIREWEED_TURSO_PROJECTION_PATH` (default
`/var/lib/fireweed/fireweed-projection.turso`). Legacy product spellings and
demoted Hybrid projection paths are hard-rejected by the env adapter (no
long-lived aliases). The stock `fireweed-service` binary ships the full public
matrix at runtime (Turso default-on; postgres via the blocking-safe
`PostgresNativeBackend`); selecting a cell is injection only — no rebuild.
Lakebase / cloud postgres with `sslmode=require` still requires a `tls`-built
image (`--features tls`); a non-tls binary fails closed on TLS-requiring DSNs
(no plaintext downgrade). Unsupported pairings fail at startup with an explicit
unsupported-storage message.

`filesystem`/`s3` (object-log) cells with a local durable projection treat the
object log as command authority; the local projection is rebuilt from committed
state. Unsupported pairings fail closed until intentionally implemented and
tested. Multi-owner S3 release claims additionally require manifest-CAS
stale-owner fencing and the TP-003 external transaction-contract matrix for the
exact cell.

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
| `FIREWEED_LOG_BACKEND` | no | `filesystem` | **Injection** for log axis. Public values only: `memory`, `sqlite`, `postgres`, `filesystem`, `s3`. |
| `FIREWEED_PROJECTION_BACKEND` | no | `turso` | **Injection** for projection axis. Public values only: `memory`, `sqlite`, `turso`, `postgres`. |
| `FIREWEED_OBJECT_LOG_ROOT` | when log is `filesystem` | `/var/lib/fireweed/object-log` | Local/filesystem object-log root. |
| `FIREWEED_SQLITE_LOG_PATH` | when log is `sqlite` | `/var/lib/fireweed/fireweed-log.db` | Local SQLite log path. |
| `FIREWEED_TURSO_PROJECTION_PATH` | when projection is `turso` | `/var/lib/fireweed/fireweed-projection.turso` | Local Turso materialized projection path (chart default under the storage volume: `/var/lib/fireweed/projection/projection.turso`). |
| `FIREWEED_SQLITE_PROJECTION_PATH` | when projection is `sqlite` | `/var/lib/fireweed/fireweed-projection.db` | Local SQLite materialized projection path. |
| `FIREWEED_OBJECT_LOG_MODE` | no | `file` | Object-log substrate: `file` (per-command) or `segmented` (group-commit, the production form). |
| `FIREWEED_SEGMENT_TARGET_BYTES` | no | `262144` | `segmented`: byte-size seal trigger. |
| `FIREWEED_SEGMENT_MAX_LATENCY_MS` | no | `20` | `segmented`: latency seal trigger and implementation of the object-log commit-latency-bound knob (`max_commit_latency_ms`). Lower values reduce mutation latency and increase object/log request cost; higher values improve batch density and increase latency. This knob must not weaken transaction integrity. |

The object-log segment format is fixed by this Fireweed release. It has no runtime format selector; storage
containing any retired pre-release frame or metadata namespace is rejected during open.

| Key | Required | Default | Meaning |
|-----|----------|---------|---------|
| `FIREWEED_RECOVERY_MAX_TAIL_COMMANDS` | no | `1000000` | Object-log × durable local projection recovery-window budget. A reopen recovers from the SQLite projection snapshot/image + its recorded high-water and replays only the object-log tail beyond it (not the full genesis log). A tail longer than this budget is logged as a recovery-window warning (the projection has fallen far behind the durable log). |
| `FIREWEED_POSTGRES_LOG_DATABASE_URL` | when log is `postgres` (Helm) | _(none)_ | The DSN the Helm postgres/Lakebase path renders from the log-backend Secret. Takes precedence over `FIREWEED_PG_URL`. A libpq URL **or** `key=value` DSN, with a native password; `sslmode=require` selects the native-tls path. |
| `FIREWEED_PG_URL` | when log is `postgres` (local/dev) | `postgres://postgres@127.0.0.1:5432/postgres` | libpq/postgres connection string (URL or `key=value` DSN); the fallback when `FIREWEED_POSTGRES_LOG_DATABASE_URL` is unset. With `sslmode=require` (or `prefer`) the binary must be built `--features postgres,tls` to connect over native-tls; on a non-tls build an `sslmode=require` DSN fails closed at startup (no plaintext downgrade). |
| `DATABRICKS_HOST`, `DATABRICKS_DATABASE_INSTANCE_NAME`, `DATABRICKS_CLIENT_ID`+`DATABRICKS_CLIENT_SECRET` (service principal) or `DATABRICKS_TOKEN`+`FIREWEED_DATABRICKS_POSTGRES_USER` (PAT) | when using Databricks Lakebase credentials | _(none)_ | Optional Databricks credential injection for the `postgres` backend: when `DATABRICKS_HOST` is set, a service-principal/PAT credential provider supplies the postgres user/password at connect instead of the DSN password. |
| `FIREWEED_BOOTSTRAP_QUEUES` | no | `t1:q1` | Comma-separated `tenant:queue` bootstrap list. A non-empty value takes precedence over generated inventory settings. |
| `FIREWEED_BOOTSTRAP_GENERATED_COUNT` | no | _(none)_ | Generate this many bootstrap queues in deterministic numeric order. Valid range: 1–10,000. When absent, generation is disabled. |
| `FIREWEED_BOOTSTRAP_GENERATED_TENANT` | no | `t1` | Tenant for generated bootstrap queues. |
| `FIREWEED_BOOTSTRAP_GENERATED_PREFIX` | no | `q` | Queue prefix for generated bootstrap queues (`q0`, `q1`, … with the default). |
| `FIREWEED_RECLAIM_INTERVAL_MS` | no | `1000` | Reclaim tick interval. |
