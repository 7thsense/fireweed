# pqueue Helm Chart

This chart deploys the `pqueue-service` RESP runtime. Storage is configured with
separate log and projection axes.

## Storage Axes

Log backend:

- `objectlog`
- `postgres`

Projection backend:

- `inmemory`
- `sqlite`
- `hybrid`
- `hybrid-async`
- `postgres`

The current `pqueue-server` binary wires `memory/inmemory`, `sqlite/inmemory`,
`objectlog/inmemory`, `objectlog/sqlite`, `objectlog/hybrid`, and
`objectlog/hybrid-async` unconditionally. `postgres/inmemory` is also wired — the sync postgres client runs only on Tokio's blocking-thread pool
via the `PostgresNativeBackend` wrapper, never on a reactor worker — but only when
the binary is built with the `postgres` cargo feature (`--features postgres`, or
`--features postgres,tls` for native-tls). The default release image does **not**
build that feature, so selecting `postgres` against the stock image fails loudly at
startup with a message naming the required feature build. Other unsupported
combinations also fail loudly at startup instead of being hidden behind a synthetic
combined backend name.

`PQUEUE_PROJECTION_BACKEND=hybrid` selects the normative `objectlog/hybrid`
profile. It uses the same `PQUEUE_SQLITE_PROJECTION_PATH` as
`sqlite`, applies committed object-log batches to SQLite first, hydrates the hot
in-memory projection from a SQLite `ProjectionImage` before returning SQLite
high-water on recovery, and fails closed if memory apply fails after a SQLite
commit. Until other pairings are explicitly implemented and tested,
`memory/hybrid`, `sqlite/hybrid`, and `postgres/hybrid` must fail at startup.

`PQUEUE_PROJECTION_BACKEND=hybrid-async` selects the `objectlog/hybrid-async`
profile (TD-004): the same hot-in-memory serving over a durable SQLite checkpoint
image as `hybrid` and the same `PQUEUE_SQLITE_PROJECTION_PATH`, but manifest commit
plus synchronous in-memory apply/render is the success barrier and the durable
SQLite image is an asynchronous checkpoint that MAY lag (caught up by object-log
tail replay on recovery). The deployment carries the async-apply
debt/backpressure/poison thresholds, rendered as `PQUEUE_HYBRID_ASYNC_*` from
`storage.projection.hybridAsync`; each bound MUST be `> 0` (a zero bound is
instantly backpressured) and the server fails closed at startup otherwise. Only
the object-log log axis pairs with `hybrid-async`; `memory/hybrid-async`,
`sqlite/hybrid-async`, and `postgres/hybrid-async` fail at startup.

### Databricks Lakebase (postgres over TLS)

When `storage.log.backend=postgres`, the chart renders the log DSN Secret ref into
`PQUEUE_POSTGRES_LOG_DATABASE_URL`. With a `tls`-built image this is a **real runtime
path** (no longer render-only): a DSN with `sslmode=require` connects to Lakebase /
cloud postgres over native-tls, and a service-principal/PAT credential provider
injects the postgres user/password when the `DATABRICKS_*` envs are supplied. Build the
TLS image with `docker build --build-arg CARGO_FEATURES=tls ...`. An `sslmode=require`
DSN on a non-tls image fails closed at startup (no plaintext downgrade). The live
Lakebase provider-certification run remains tracked separately (`pqueue-ea625701`).

## Default Values

```yaml
storage:
  log:
    backend: objectlog
  projection:
    backend: inmemory
```

The chart renders:

- `PQUEUE_LOG_BACKEND`
- `PQUEUE_PROJECTION_BACKEND`
- `PQUEUE_OBJECT_LOG_ROOT` when `storage.log.backend=objectlog`
- `PQUEUE_SQLITE_PROJECTION_PATH` when `storage.projection.backend=sqlite`,
  `hybrid`, or `hybrid-async`
- `PQUEUE_HYBRID_ASYNC_APPLY_LAG_MAX_COMMANDS`,
  `PQUEUE_HYBRID_ASYNC_APPLY_DEBT_MAX_BYTES`,
  `PQUEUE_HYBRID_ASYNC_APPLY_QUEUE_DEPTH_MAX`,
  `PQUEUE_HYBRID_ASYNC_OLDEST_UNAPPLIED_MAX_MS`, and
  `PQUEUE_HYBRID_ASYNC_APPLY_POISON_RETRY_THRESHOLD` (from
  `storage.projection.hybridAsync`) when `storage.projection.backend=hybrid-async`
- Postgres log/projection database URL Secret refs when the corresponding axis
  uses `postgres`

The service exposes the RESP port and uses TCP liveness/readiness probes.

## Shared S3 Multi-Replica Profile

`values-shared-s3.yaml` selects the replica-safe shared S3 object log, Postgres
ownership control plane, and pod-local rebuildable SQLite projection. Each pod
publishes its Kubernetes `metadata.uid` as the full-width `PQUEUE_OWNER_ID` and
its pod IP as `PQUEUE_ADVERTISE_ADDR`; `PQUEUE_NODE_ID` remains the independent
compact item-ID field. Create the referenced S3 and Postgres Secrets before
installing this profile.

## Lakebase Postgres Native Profile

`charts/pqueue/ci/lakebase-postgres-values.yaml` is the static render profile for
Databricks Lakebase with the postgres log axis selected (`projection.backend:
inmemory` — the only wired postgres combination: durable via the DSN log with a
fast in-memory projection apply). It renders Secret references, but it does not
embed credentials in chart values or manifests.

The binary connects to Lakebase from the log DSN alone. The DSN Secret key
referenced by `storage.log.postgres.databaseUrlKey` must contain a
**self-sufficient** libpq key=value DSN — host, port, db, and `sslmode=require`:

```text
host=<pooler-or-direct-host> port=5432 user=<postgres-user> password=<secret> dbname=databricks_postgres sslmode=require
```

This DSN is rendered to the container as `PQUEUE_POSTGRES_LOG_DATABASE_URL`.
Select the Lakebase pooler or direct endpoint by placing that endpoint host in
the DSN Secret. The `storage.lakebase.endpointMode`, `databaseName`, `port`, and
`sslMode` values are operator documentation for constructing that DSN; the binary
does not read `PQUEUE_LAKEBASE_*` metadata, so the chart does not render it.

For native password auth, set `storage.lakebase.auth.mode=native-password` and
store the password inside the DSN Secret. For service-principal OAuth, set
`storage.lakebase.auth.mode=service-principal-oauth` and provide
`storage.lakebase.auth.existingSecret`; the chart renders Secret refs for
`DATABRICKS_HOST`, `PQUEUE_DATABRICKS_DATABASE_INSTANCE_NAME`,
`DATABRICKS_CLIENT_ID`, and `DATABRICKS_CLIENT_SECRET`. For PAT-backed OAuth set
`pat-oauth`; the chart renders `DATABRICKS_HOST`,
`PQUEUE_DATABRICKS_DATABASE_INSTANCE_NAME`, `DATABRICKS_TOKEN`, and
`PQUEUE_DATABRICKS_POSTGRES_USER`. These names match the binary's
`DatabricksCredentialConfig` contract; when `DATABRICKS_HOST` is present a
service-principal/PAT credential provider supplies the postgres user/password at
connect in place of the DSN password.

When the image is built `--features tls` (`--build-arg CARGO_FEATURES=tls`) this
is a real runtime path: an `sslmode=require` DSN connects over native-tls. On a
non-tls build an `sslmode=require` DSN fails closed at startup (no plaintext
downgrade). `pqueue-ea625701` owns live Lakebase provider certification with real
credentials.
