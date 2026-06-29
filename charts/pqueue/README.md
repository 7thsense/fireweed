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

The current `pqueue-server` binary only wires a subset of those combinations.
Unsupported combinations fail loudly at startup instead of being hidden behind a
synthetic combined backend name.

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

## Lakebase Postgres Native Profile

`charts/pqueue/ci/lakebase-postgres-values.yaml` is the static render profile for
Databricks Lakebase with the postgres log and postgres projection axes selected.
It renders Secret references and Lakebase connection metadata, but it does not
embed credentials in chart values or manifests.

The DSN Secret key referenced by `storage.log.postgres.databaseUrlKey` and
`storage.projection.postgres.databaseUrlKey` must contain a libpq key=value DSN:

```text
host=<pooler-or-direct-host> port=5432 user=<postgres-user> password=<secret> dbname=databricks_postgres sslmode=require
```

Select the Lakebase pooler or direct endpoint by placing that endpoint host in
the DSN Secret and setting `storage.lakebase.endpointMode` to `pooler` or
`direct`. The chart also renders `PQUEUE_LAKEBASE_DATABASE_NAME`,
`PQUEUE_LAKEBASE_PORT`, and `PQUEUE_LAKEBASE_SSLMODE`; the Lakebase profile
defaults those to `databricks_postgres`, `5432`, and `require`.

For native password auth, set `storage.lakebase.auth.mode=native-password` and
store the password inside the DSN Secret. For service-principal OAuth, set
`storage.lakebase.auth.mode=service-principal-oauth` and provide
`storage.lakebase.auth.existingSecret`; the chart renders Secret refs for
`DATABRICKS_HOST`, `PQUEUE_DATABRICKS_DATABASE_INSTANCE_NAME`,
`DATABRICKS_CLIENT_ID`, and `DATABRICKS_CLIENT_SECRET`. A PAT-backed OAuth mode
is also modeled with `pat-oauth` for the existing Databricks credential-provider
environment contract.

The Lakebase image must be built from the pqueue source with TLS-capable
postgres support before using this profile in a live environment. The current
chart profile is render and Secret-wiring support only; `pqueue-13924b0e` owns
the TLS/runtime work and `pqueue-ea625701` owns live Lakebase provider
certification with real credentials.
