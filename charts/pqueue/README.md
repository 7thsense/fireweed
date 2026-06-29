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
- `postgres`

The current `pqueue-server` binary wires `memory/inmemory`, `sqlite/inmemory`,
`objectlog/inmemory`, and `objectlog/sqlite` unconditionally. `postgres/inmemory`
is also wired — the sync postgres client runs only on Tokio's blocking-thread pool
via the `PostgresNativeBackend` wrapper, never on a reactor worker — but only when
the binary is built with the `postgres` cargo feature (`--features postgres`, or
`--features postgres,tls` for native-tls). The default release image does **not**
build that feature, so selecting `postgres` against the stock image fails loudly at
startup with a message naming the required feature build. Other unsupported
combinations also fail loudly at startup instead of being hidden behind a synthetic
combined backend name.

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
- `PQUEUE_SQLITE_PROJECTION_PATH` when `storage.projection.backend=sqlite`
- Postgres log/projection database URL Secret refs when the corresponding axis
  uses `postgres`

The service exposes the RESP port and uses TCP liveness/readiness probes.
