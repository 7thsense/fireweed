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
| `PQUEUE_POSTGRES_LOG_DATABASE_URL` | when log is `postgres` | unset | libpq URL or key=value DSN for the Postgres log axis. Parsed today; live server startup remains gated on `pqueue-558bf933`. |
| `PQUEUE_POSTGRES_PROJECTION_DATABASE_URL` | when projection is `postgres` | unset | libpq URL or key=value DSN for the Postgres projection axis. Parsed today; live server startup remains gated on `pqueue-558bf933`. |
| `PQUEUE_LAKEBASE_ENDPOINT_MODE` | no | `pooler` | Lakebase metadata parsed for the postgres_native profile: `pooler` or `direct`. |
| `PQUEUE_LAKEBASE_DATABASE_NAME` | no | `databricks_postgres` | Lakebase database name metadata for the postgres_native profile. |
| `PQUEUE_LAKEBASE_PORT` | no | `5432` | Lakebase Postgres port metadata. |
| `PQUEUE_LAKEBASE_SSLMODE` | no | `require` | Lakebase TLS metadata; Lakebase requires `sslmode=require`. |
| `PQUEUE_LAKEBASE_AUTH_MODE` | no | unset | `native-password`, `service-principal-oauth`, or `pat-oauth`; selecting an OAuth mode also requires the Databricks Secret-backed env vars below. |
| `DATABRICKS_HOST` | OAuth Lakebase modes | unset | Databricks workspace host for database credential generation. |
| `PQUEUE_DATABRICKS_DATABASE_INSTANCE_NAME` | OAuth Lakebase modes | unset | Lakebase database instance name. |
| `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` | service-principal OAuth | unset | Service-principal credential env vars sourced from a Kubernetes Secret. |
| `DATABRICKS_TOKEN` or `DATABRICKS_PAT`; `PQUEUE_DATABRICKS_POSTGRES_USER` or `DATABRICKS_POSTGRES_USER` | PAT OAuth | unset | PAT-backed credential env vars sourced from a Kubernetes Secret. |
| `PQUEUE_BOOTSTRAP_QUEUES` | no | `t1:q1` | Comma-separated `tenant:queue` bootstrap list. |
| `PQUEUE_RECLAIM_INTERVAL_MS` | no | `1000` | Reclaim tick interval. |

The current server composition root wires `memory/inmemory`, `sqlite/inmemory`,
`objectlog/inmemory`, and `objectlog/sqlite`. Other combinations fail at startup
with an explicit unsupported-storage message. `postgres/postgres` is parsed as
the `postgres_native` shape so deployment config can be validated, but live
startup still fails until `pqueue-558bf933` adds the blocking-safe runtime
wrapper.

`objectlog/sqlite` is a local single-owner development/runtime profile: the
object log is the durable command authority and SQLite is rebuilt as a
materialized projection. It is not the multi-owner S3 release profile until the
manifest-CAS stale-owner fence work tracked by `pqueue-e5c6d6fc` lands.

The Helm chart exposes storage axes as:

- `storage.log.backend`
- `storage.projection.backend`

Kubernetes probes are TCP probes against the RESP port.

The source-build Dockerfile accepts optional pqueue-service cargo features with
`--build-arg CARGO_FEATURES=<features>`. The release-ledger binary is built in a
separate package-scoped command without those service features. Omitting the
build arg keeps the default release-image build unchanged.
