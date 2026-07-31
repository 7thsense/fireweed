# Fireweed Queue Helm Chart

This chart deploys the `fireweed-service` RESP runtime. Storage is configured with
separate **log** and **projection** axes (plus control plane) isomorphic to the
product `StorageConfig` model. There is no public combined-profile product type.

## Fireweed Queue preview version policy

`fireweed-queue` is the authoritative chart coordinate under ADR-023. This
source chart keeps an independent version
and `appVersion` for development packages. For the Fireweed Queue v0.20.0
public preview, `scripts/release/package-helm-chart.sh --version 0.20.0`
overrides both values and produces `fireweed-queue-0.20.0.tgz` plus
`fireweed-queue-helm-chart.txt` release evidence.

The chart path, chart name, rendered Kubernetes names, package names, runtime
environment, and persisted identifiers all use the Fireweed namespace.

## Storage Axes

Public product values:

| Axis | Helm key | Values |
|------|----------|--------|
| **Log** | `storage.log.backend` | `memory`, `sqlite`, `postgres`, `filesystem`, `s3` |
| **Projection** | `storage.projection.backend` | `memory`, `sqlite`, `postgres` |
| **Control plane** | `storage.controlPlane.backend` | `inprocess`, `postgres` |

`filesystem` and `s3` are first-class object-log peers (same segment/manifest
protocol). Configure them with structured fields:

- **Filesystem log** — `storage.log.backend=filesystem` and
  `storage.log.objectLog.root` (local disk or NAS path, e.g. `/tank/fireweed/object-log`)
- **S3 log** — `storage.log.backend=s3` and `storage.log.objectLog.s3.*`
  (endpoint, bucket, region, credentials Secret)
- **SQLite log** — `storage.log.backend=sqlite` and `storage.log.sqlite.path`
- **Postgres log / projection** — Secret-ref DSN blocks under
  `storage.log.postgres` / `storage.projection.postgres`
- **SQLite projection** — `storage.projection.backend=sqlite` and
  `storage.projection.sqlite.path`

### Durability (summary)

- **Class A** (`sqlite` / `postgres` / `filesystem` / `s3` log): success means
  durable on the log and visible in the projection; recovery uses high-water +
  tail when the log remains.
- **Class B** (`memory` log): success means visible in the projection; durable
  only if the projection is durable. After process death only the projection
  remains — no Class A log-rebuild claims for a memory log.

### Public names only

The chart schema and the server env adapter accept **only** the public product
values above. Older spellings and demoted projection paths are hard-rejected
(no long-lived aliases). Prefer `filesystem` or `s3` for the object-log protocol
family, and `memory` for an in-process projection.

### Wiring honesty

Unsupported or not-yet-verified log×projection cells fail loudly at startup
instead of silent downgrade. The stock service binary ships the full public
log×projection matrix (including postgres); selecting a cell is
`storage.log.backend` / `storage.projection.backend` (or the matching
`FIREWEED_*` injection) only — no rebuild. Lakebase / cloud postgres with
`sslmode=require` still needs a `tls`-built image
(`docker build --build-arg CARGO_FEATURES=tls ...`); a non-tls image fails
closed on TLS-requiring DSNs (no plaintext downgrade).

### Databricks Lakebase (postgres over TLS)

When `storage.log.backend=postgres`, the chart renders the log DSN Secret ref into
`FIREWEED_POSTGRES_LOG_DATABASE_URL`. With a `tls`-built image this is a **real runtime
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
    backend: filesystem
    objectLog:
      root: /var/lib/fireweed/projection/object-log
  projection:
    backend: memory
```

The chart renders:

- `FIREWEED_LOG_BACKEND` from `storage.log.backend`
- `FIREWEED_PROJECTION_BACKEND` from `storage.projection.backend`
- `FIREWEED_OBJECT_LOG_ROOT` when the log is `filesystem`
- `FIREWEED_OBJECT_LOG_S3_*` when the log is `s3`
- `FIREWEED_SQLITE_LOG_PATH` when the log is `sqlite`
- `FIREWEED_SQLITE_PROJECTION_PATH` when the projection is `sqlite`
- Postgres log/projection/control-plane database URL Secret refs when those
  axes use `postgres`

The service exposes the RESP port and uses TCP liveness/readiness probes.

## Shared S3 multi-replica values

`values-shared-s3.yaml` selects a replica-safe shared S3 object log, Postgres
ownership control plane, and pod-local rebuildable SQLite projection. Prefer
public spellings in operator-owned values:

```yaml
replicaCount: 3
storage:
  log:
    backend: s3
    objectLog:
      s3:
        endpoint: https://s3.example.com
        bucket: fireweed-shared
        region: us-east-1
        credentials:
          existingSecret: fireweed-objectlog-s3
  controlPlane:
    backend: postgres
  projection:
    backend: sqlite
persistence:
  enabled: false
```

Each pod publishes its Kubernetes `metadata.uid` as the full-width
`FIREWEED_OWNER_ID` and its pod IP as `FIREWEED_ADVERTISE_ADDR`;
`FIREWEED_NODE_ID` remains the independent compact item-ID field. Create the
referenced S3 and Postgres Secrets before installing.

## Lakebase Postgres native values

`charts/fireweed-queue/ci/lakebase-postgres-values.yaml` is the static render
fixture for Databricks Lakebase with the postgres **log** axis selected and a
memory projection (durable via the DSN log with a fast in-memory projection
apply). It renders Secret references, but it does not embed credentials in
chart values or manifests.

The binary connects to Lakebase from the log DSN alone. The DSN Secret key
referenced by `storage.log.postgres.databaseUrlKey` must contain a
**self-sufficient** libpq key=value DSN — host, port, db, and `sslmode=require`:

```text
host=<pooler-or-direct-host> port=5432 user=<postgres-user> password=<secret> dbname=databricks_postgres sslmode=require
```

This DSN is rendered to the container as `FIREWEED_POSTGRES_LOG_DATABASE_URL`.
Select the Lakebase pooler or direct endpoint by placing that endpoint host in
the DSN Secret. The `storage.lakebase.endpointMode`, `databaseName`, `port`, and
`sslMode` values are operator documentation for constructing that DSN; the binary
does not read `FIREWEED_LAKEBASE_*` metadata, so the chart does not render it.

For native password auth, set `storage.lakebase.auth.mode=native-password` and
store the password inside the DSN Secret. For service-principal OAuth, set
`storage.lakebase.auth.mode=service-principal-oauth` and provide
`storage.lakebase.auth.existingSecret`; the chart renders Secret refs for
`DATABRICKS_HOST`, `FIREWEED_DATABRICKS_DATABASE_INSTANCE_NAME`,
`DATABRICKS_CLIENT_ID`, and `DATABRICKS_CLIENT_SECRET`. For PAT-backed OAuth set
`pat-oauth`; the chart renders `DATABRICKS_HOST`,
`FIREWEED_DATABRICKS_DATABASE_INSTANCE_NAME`, `DATABRICKS_TOKEN`, and
`FIREWEED_DATABRICKS_POSTGRES_USER`. These names match the binary's
`DatabricksCredentialConfig` contract; when `DATABRICKS_HOST` is present a
service-principal/PAT credential provider supplies the postgres user/password at
connect in place of the DSN password.

When the image is built `--features tls` (`--build-arg CARGO_FEATURES=tls`) this
is a real runtime path: an `sslmode=require` DSN connects over native-tls. On a
non-tls build an `sslmode=require` DSN fails closed at startup (no plaintext
downgrade). `pqueue-ea625701` owns live Lakebase provider certification with real
credentials.
