# pqueue Container Image and Runtime Configuration Contract

The release image entrypoint is `pqueue-service`, the RESP server built from
`crates/pqueue-server`.

## Environment

| Key | Required | Default | Meaning |
|-----|----------|---------|---------|
| `PQUEUE_LISTEN_ADDR` | no | `0.0.0.0:8080` | RESP listen address. |
| `PQUEUE_LOG_BACKEND` | no | `objectlog` | Log backend axis: `objectlog`, `postgres`, `sqlite`, or `memory`. |
| `PQUEUE_PROJECTION_BACKEND` | no | `inmemory` | Projection backend axis: `inmemory`, `sqlite`, `hybrid`, `hybrid-async`, or `postgres`. |
| `PQUEUE_OBJECT_LOG_ROOT` | when log is `objectlog` | `/var/lib/pqueue/object-log` | Local object-log root. |
| `PQUEUE_SQLITE_LOG_PATH` | when log is `sqlite` | `/var/lib/pqueue/pqueue-log.db` | Local SQLite log path. |
| `PQUEUE_SQLITE_PROJECTION_PATH` | when projection is `sqlite`, `hybrid`, or `hybrid-async` | `/var/lib/pqueue/pqueue-projection.db` | Local SQLite materialized projection path for `objectlog/sqlite`, the SQLite-first durable image for `objectlog/hybrid`, and the asynchronous durable checkpoint image for `objectlog/hybrid-async`. |
| `PQUEUE_OBJECT_LOG_MODE` | no | `file` | `objectlog` substrate: `file` (per-command) or `segmented` (group-commit, the production form). |
| `PQUEUE_SEGMENT_TARGET_BYTES` | no | `262144` | `segmented`: byte-size seal trigger. |
| `PQUEUE_SEGMENT_MAX_LATENCY_MS` | no | `20` | `segmented`: latency seal trigger and implementation of the object-log commit-latency-bound knob (`max_commit_latency_ms`). Lower values reduce mutation latency and increase object/log request cost; higher values improve batch density and increase latency. This knob must not weaken transaction integrity. |
| `PQUEUE_RECOVERY_MAX_TAIL_COMMANDS` | no | `1000000` | `objectlog/sqlite`, `objectlog/hybrid`, and `objectlog/hybrid-async` recovery-window budget. A reopen recovers from the SQLite projection snapshot/image + its recorded high-water and replays only the object-log tail beyond it (not the full genesis log). A tail longer than this budget is logged as a recovery-window warning (the projection has fallen far behind the durable log). |
| `PQUEUE_HYBRID_ASYNC_APPLY_LAG_MAX_COMMANDS` | no | `100000` | `objectlog/hybrid-async`: hard cap on committed command sequences the durable SQLite checkpoint may trail the object-log head before backpressure. Must be `> 0`; a zero bound fails closed at startup. |
| `PQUEUE_HYBRID_ASYNC_APPLY_DEBT_MAX_BYTES` | no | `536870912` | `objectlog/hybrid-async`: hard cap on retained object-log bytes not yet trimmable via async apply. Must be `> 0`. |
| `PQUEUE_HYBRID_ASYNC_APPLY_QUEUE_DEPTH_MAX` | no | `1024` | `objectlog/hybrid-async`: hard cap on sealed segment batches awaiting async SQLite apply. Must be `> 0`. |
| `PQUEUE_HYBRID_ASYNC_OLDEST_UNAPPLIED_MAX_MS` | no | `60000` | `objectlog/hybrid-async`: hard cap on the age of the oldest unapplied committed command. Must be `> 0`. |
| `PQUEUE_HYBRID_ASYNC_APPLY_POISON_RETRY_THRESHOLD` | no | `3` | `objectlog/hybrid-async`: consecutive failed SQLite apply attempts for a batch that trip fail-closed poison. Must be `> 0`. |
| `PQUEUE_POSTGRES_LOG_DATABASE_URL` | when log is `postgres` (Helm) | _(none)_ | The DSN the Helm postgres/Lakebase profile renders from the log-backend Secret. Takes precedence over `PQUEUE_PG_URL`. A libpq URL **or** `key=value` DSN, with a native password; `sslmode=require` selects the native-tls path. |
| `PQUEUE_PG_URL` | when log is `postgres` (local/dev) | `postgres://postgres@127.0.0.1:5432/postgres` | libpq/postgres connection string (URL or `key=value` DSN) for the `postgres/inmemory` backend; the fallback when `PQUEUE_POSTGRES_LOG_DATABASE_URL` is unset. With `sslmode=require` (or `prefer`) the binary must be built `--features postgres,tls` to connect over native-tls; on a non-tls build an `sslmode=require` DSN fails closed at startup (no plaintext downgrade). |
| `DATABRICKS_HOST`, `DATABRICKS_DATABASE_INSTANCE_NAME`, `DATABRICKS_CLIENT_ID`+`DATABRICKS_CLIENT_SECRET` (service principal) or `DATABRICKS_TOKEN`+`PQUEUE_DATABRICKS_POSTGRES_USER` (PAT) | when using Databricks Lakebase credentials | _(none)_ | Optional Databricks credential injection for the `postgres` backend: when `DATABRICKS_HOST` is set, a service-principal/PAT credential provider supplies the postgres user/password at connect instead of the DSN password. |
| `PQUEUE_BOOTSTRAP_QUEUES` | no | `t1:q1` | Comma-separated `tenant:queue` bootstrap list. A non-empty value takes precedence over generated inventory settings. |
| `PQUEUE_BOOTSTRAP_GENERATED_COUNT` | no | _(none)_ | Generate this many bootstrap queues in deterministic numeric order. Valid range: 1–10,000. When absent, generation is disabled. |
| `PQUEUE_BOOTSTRAP_GENERATED_TENANT` | no | `t1` | Tenant for generated bootstrap queues. |
| `PQUEUE_BOOTSTRAP_GENERATED_PREFIX` | no | `q` | Queue prefix for generated bootstrap queues (`q0`, `q1`, … with the default). |
| `PQUEUE_RECLAIM_INTERVAL_MS` | no | `1000` | Reclaim tick interval. |

The current server composition root wires `memory/inmemory`, `sqlite/inmemory`,
`objectlog/inmemory`, `objectlog/sqlite`, `objectlog/hybrid`, and
`objectlog/hybrid-async` unconditionally. `PQUEUE_LOG_BACKEND=objectlog` with
`PQUEUE_PROJECTION_BACKEND=hybrid` uses the same
`PQUEUE_SQLITE_PROJECTION_PATH` as `sqlite` and the generic segmented
object-log group-commit runtime. `PQUEUE_PROJECTION_BACKEND=hybrid-async` runs
the same object-log + hybrid substrate under its canonical profile name, carrying
the `PQUEUE_HYBRID_ASYNC_*` async-apply debt/backpressure/poison thresholds
(manifest commit + synchronous in-memory apply/render is the success barrier; the
durable SQLite image is an asynchronous checkpoint that may lag). Only the
object-log log axis pairs with `hybrid-async`; `memory/hybrid-async`,
`sqlite/hybrid-async`, and `postgres/hybrid-async` fail closed at startup.
`postgres/inmemory`
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

## Transaction and Storage-Combination Contract

The storage axes are implementation choices, not API variants. Any executable
combination MUST preserve the same native pqueue transaction contract:

- a successful mutating response means accepted effects are durable and visible
  to subsequent reads, claims, idempotency replay, and restart recovery;
- a rejected envelope has no committed item effect, and a rejected item in a
  partial batch has no committed effect for that item;
- a transport failure, timeout, or service crash after submission is resolved by
  retrying the same `request_id`, never by caller-side log repair;
- local projections, segment buffering, and replay are internal implementation
  details.

Unsupported or not-yet-verified log/projection combinations fail closed at
startup. They must not silently downgrade to a weaker durability or projection
profile.

`objectlog/inmemory`, `objectlog/sqlite`, and `objectlog/hybrid` are
object-log local-projection profiles: the object log is the durable command
authority and the local projection is rebuilt from committed state. `hybrid`
adds a SQLite-first durable image plus hot in-memory serving. Unsupported
pairings such as `memory/hybrid`, `sqlite/hybrid`, and `postgres/hybrid` fail
closed at startup until intentionally implemented and tested. These profiles are
not the multi-owner S3 release profile until the manifest-CAS stale-owner fence
work and the TP-003 external transaction-contract matrix land.

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

### Hybrid recovery and poisoning (objectlog/hybrid)

`objectlog/hybrid` applies each committed object-log batch to SQLite first and
then to the hot in-memory projection. The in-memory projection serves claim
selection, `peek`, `pending`, metrics, live-item lookup, secondary-index lookup,
and pre-commit validation. If SQLite apply fails, no success response is
returned and restart replays the object-log tail. If SQLite commits but memory
apply fails, the process-local projection is **poisoned**: the current operation
returns storage failure, every later read/validation/write fails closed, and the
process must restart.

On restart, the server exports a complete SQLite `ProjectionImage`, hydrates
memory, and only then returns the SQLite high-water so recovery can skip the
historical object-log prefix. If hydration fails, recovery fails closed or
replays from genesis. The local SQLite high-water is not authority for segment
expiry; object-log segments remain retained unless covered by a committed
object-store snapshot and recovery window. Durable push `request_id` replay is
part of the runtime contract: a committed-but-unreturned push retry returns the
original item ids for the same body and `request-id-conflict` for a different
body.

The Helm chart exposes storage axes as:

- `storage.log.backend`
- `storage.projection.backend`

Kubernetes probes are TCP probes against the RESP port.
