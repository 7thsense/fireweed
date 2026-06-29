# pqueue Container Image and Runtime Configuration Contract

The release image entrypoint is `pqueue-service`, the RESP server built from
`crates/pqueue-server`.

## Environment

| Key | Required | Default | Meaning |
|-----|----------|---------|---------|
| `PQUEUE_LISTEN_ADDR` | no | `0.0.0.0:8080` | RESP listen address. |
| `PQUEUE_LOG_BACKEND` | no | `objectlog` | Log backend axis: `objectlog`, `postgres`, `sqlite`, or `memory`. |
| `PQUEUE_PROJECTION_BACKEND` | no | `inmemory` | Projection backend axis: `inmemory`, `sqlite`, or `postgres`. |
| `PQUEUE_OBJECT_LOG_ROOT` | when log is `objectlog` | `/var/lib/pqueue/object-log` | Local object-log root. |
| `PQUEUE_SQLITE_LOG_PATH` | when log is `sqlite` | `/var/lib/pqueue/pqueue-log.db` | Local SQLite log path. |
| `PQUEUE_SQLITE_PROJECTION_PATH` | when projection is `sqlite` | `/var/lib/pqueue/pqueue-projection.db` | Local SQLite materialized projection path for `objectlog/sqlite`. |
| `PQUEUE_OBJECT_LOG_MODE` | no | `file` | `objectlog` substrate: `file` (per-command) or `segmented` (group-commit, the production form). |
| `PQUEUE_SEGMENT_TARGET_BYTES` | no | `262144` | `segmented`: byte-size seal trigger. |
| `PQUEUE_SEGMENT_MAX_LATENCY_MS` | no | `20` | `segmented`: latency seal trigger. |
| `PQUEUE_RECOVERY_MAX_TAIL_COMMANDS` | no | `1000000` | `objectlog/sqlite` recovery-window budget. A reopen recovers from the SQLite projection snapshot + its recorded high-water and replays only the object-log tail beyond it (not the full genesis log). A tail longer than this budget is logged as a recovery-window warning (the projection has fallen far behind the durable log). |
| `PQUEUE_POSTGRES_LOG_DATABASE_URL` | when log is `postgres` (Helm) | _(none)_ | The DSN the Helm postgres/Lakebase profile renders from the log-backend Secret. Takes precedence over `PQUEUE_PG_URL`. A libpq URL **or** `key=value` DSN, with a native password; `sslmode=require` selects the native-tls path. |
| `PQUEUE_PG_URL` | when log is `postgres` (local/dev) | `postgres://postgres@127.0.0.1:5432/postgres` | libpq/postgres connection string (URL or `key=value` DSN) for the `postgres/inmemory` backend; the fallback when `PQUEUE_POSTGRES_LOG_DATABASE_URL` is unset. With `sslmode=require` (or `prefer`) the binary must be built `--features postgres,tls` to connect over native-tls; on a non-tls build an `sslmode=require` DSN fails closed at startup (no plaintext downgrade). |
| `DATABRICKS_HOST`, `DATABRICKS_DATABASE_INSTANCE_NAME`, `DATABRICKS_CLIENT_ID`+`DATABRICKS_CLIENT_SECRET` (service principal) or `DATABRICKS_TOKEN`+`PQUEUE_DATABRICKS_POSTGRES_USER` (PAT) | when using Databricks Lakebase credentials | _(none)_ | Optional Databricks credential injection for the `postgres` backend: when `DATABRICKS_HOST` is set, a service-principal/PAT credential provider supplies the postgres user/password at connect instead of the DSN password. |
| `PQUEUE_BOOTSTRAP_QUEUES` | no | `t1:q1` | Comma-separated `tenant:queue` bootstrap list. |
| `PQUEUE_RECLAIM_INTERVAL_MS` | no | `1000` | Reclaim tick interval. |

The current server composition root wires `memory/inmemory`, `sqlite/inmemory`,
`objectlog/inmemory`, and `objectlog/sqlite` unconditionally. `postgres/inmemory`
is wired through the blocking-safe `PostgresNativeBackend` wrapper — the sync
postgres client is driven only on Tokio's blocking-thread pool (`spawn_blocking`),
never on a reactor worker — but only when the binary is built with the `postgres`
cargo feature (`--features postgres`, or `--features postgres,tls` for native-tls).
The default release image does **not** build that feature, so in the shipped image
`postgres` still fails at startup with an explicit message pointing at the required
feature build. Other combinations fail at startup with an explicit
unsupported-storage message.

### Databricks Lakebase (postgres over TLS)

Lakebase is a **real runtime path**, not render-only: build the service image with the
`tls` feature and point it at the rendered DSN. The Helm postgres/Lakebase profile
renders the DSN Secret as `PQUEUE_POSTGRES_LOG_DATABASE_URL` (consumed in preference
to `PQUEUE_PG_URL`). A `key=value` or URL DSN with `sslmode=require` connects over the
native-tls connector; an `sslmode=require` DSN on a non-tls build fails closed at
startup (it never silently downgrades to plaintext). When `DATABRICKS_HOST` and the
service-principal (`DATABRICKS_CLIENT_ID`/`DATABRICKS_CLIENT_SECRET`) or PAT
(`DATABRICKS_TOKEN`/`PQUEUE_DATABRICKS_POSTGRES_USER`) envs are present, a credential
provider injects the postgres user/password at connect instead of the DSN password.

Build the TLS image with the documented build arg:

```sh
docker build --build-arg CARGO_FEATURES=tls -t pqueue:tls .
```

(or `PQUEUE_FEATURES=tls scripts/release/package-binaries.sh` for the binary tarball).
The runtime image installs `ca-certificates` + `libssl3` so the native-tls connector
can verify the Lakebase server certificate. This wires the connection path only; the
live Lakebase provider-certification run remains separate (`pqueue-ea625701`).

`objectlog/sqlite` is a local single-owner development/runtime profile: the
object log is the durable command authority and SQLite is rebuilt as a
materialized projection. It is not the multi-owner S3 release profile until the
manifest-CAS stale-owner fence work tracked by `pqueue-e5c6d6fc` lands.

### Snapshot-tail recovery (object_log_sqlite_projection)

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
restart preserves committed state. `PQUEUE_RECOVERY_MAX_TAIL_COMMANDS` bounds the
expected tail and warns when exceeded. The `segmented` substrate realizes the
genuine bounded-tail I/O saving; the `file` substrate resumes apply at the
high-water (it remains the per-command smoke reference).

The Helm chart exposes storage axes as:

- `storage.log.backend`
- `storage.projection.backend`

Kubernetes probes are TCP probes against the RESP port.
