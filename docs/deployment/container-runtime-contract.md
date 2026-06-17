# pqueue Container Image and Runtime Configuration Contract

This document is the runtime configuration contract for the pqueue service
container image. It is the interface the Helm chart populates and the
[production deployment readiness contract](../helix/04-build/DEPLOYMENT-READINESS.md)
depends on for the `postgres_native` and `object_log_sqlite_projection` backend
profiles.

It lists the environment keys the v1 service binary consumes today and the Helm
values and Secret keys that populate them. The `object_log_sqlite_projection`
profile is runtime-consumed configuration, not a reserved placeholder: invalid
object-log/S3 or SQLite projection values make `pqueue-service` exit before it
binds, and `/readyz` probes the configured S3-compatible object store plus the
SQLite projection directory.

## Image

- Build: `docker build -t pqueue:dev .`
- Toolchain: pinned to the workspace `rust-version` (Rust 1.92) in the builder
  stage; runtime stage is `debian:bookworm-slim`.
- Build inputs: the full Rust workspace. `.dockerignore` keeps `target/`, VCS,
  tooling, and `.ddx/` execution evidence out of the build context, so the image
  builds reproducibly without local source mounts.
- The image bundles two binaries:
  - `pqueue-service` — the API-001 HTTP service (the image **entrypoint**).
  - `pqueue-verify-ledger` — the verification-ledger validator (available on
    `PATH` for release/CI use).

## Entrypoint

- Entrypoint: `/usr/local/bin/pqueue-service`.
- User: non-root system user `pqueue` (uid 10001).
- `pqueue-service --help` (or `-h`) prints this configuration contract and exits
  0. This is the documented health/help command that proves the image entrypoint
  works:

  ```sh
  docker run --rm pqueue:dev --help
  ```

- `pqueue-service --version` (or `-V`) prints the service version and exits 0.
- With no flags, the binary reads its configuration from the environment, binds
  `PQUEUE_LISTEN_ADDR`, serves the API-001 app plus health probes, and runs until
  it receives `SIGINT`/`Ctrl-C`, at which point it shuts down gracefully.

## Health Endpoint and Port

- Default port: `8080` (the image `EXPOSE`s 8080 and defaults
  `PQUEUE_LISTEN_ADDR=0.0.0.0:8080`).
- Liveness probe: `GET /healthz` → `200 ok`.
- Readiness probe: `GET /readyz` → `200 ready` after profile-specific
  dependencies are ready. For `postgres_native`, readiness opens PostgreSQL and
  runs `SELECT 1`; it returns `503` while the database URL is missing or the
  query fails.
- Both probes share the configured listener with the API-001 routes, so a single
  Kubernetes `containerPort` covers the API and health checks.

## Environment / Config Keys Consumed by the Service Binary

These keys are parsed and validated by `pqueue-service` today
(`crates/pqueue-service/src/runtime.rs`). Invalid values cause the binary to exit
non-zero before binding.

| Key | Required | Default | Meaning |
|-----|----------|---------|---------|
| `PQUEUE_LISTEN_ADDR` | no | `0.0.0.0:8080` | `host:port` the HTTP server binds. Must be a valid socket address. |
| `PQUEUE_BACKEND_PROFILE` | no | `postgres_native` | Backend profile. Must be `postgres_native` or `object_log_sqlite_projection`; any other value is rejected. |
| `PQUEUE_PRINCIPAL_ID` | no | `pqueue-service` | Bootstrap principal id for the service auth context. |
| `PQUEUE_TENANTS` | no | empty | Comma-separated tenant allowlist for the bootstrap principal. Blank entries are ignored. |
| `PQUEUE_POSTGRES_DATABASE_URL` | yes for `postgres_native`; yes for `object_log_sqlite_projection` | none | PostgreSQL connection URL. `postgres_native` uses it for readiness and storage; `object_log_sqlite_projection` uses it for the Postgres control plane / manifest-pointer boundary. |

For `object_log_sqlite_projection`, the service also consumes and validates:

| Key | Required | Default | Meaning |
|-----|----------|---------|---------|
| `PQUEUE_OBJECT_LOG_ENDPOINT` | yes | none | S3-compatible endpoint URL. The kind proof uses `http://minio:9000`. |
| `PQUEUE_OBJECT_LOG_BUCKET` | yes | none | Object-log bucket name, e.g. `pqueue-object-log`. |
| `PQUEUE_OBJECT_LOG_REGION` | yes | none | S3-compatible signing region, e.g. `us-east-1`. |
| `PQUEUE_OBJECT_LOG_ACCESS_KEY_ID` | yes | none | S3-compatible access key id, sourced from a Kubernetes Secret. |
| `PQUEUE_OBJECT_LOG_SECRET_ACCESS_KEY` | yes | none | S3-compatible secret access key, sourced from a Kubernetes Secret. |
| `PQUEUE_OBJECT_LOG_SEGMENT_MAX_COMMANDS` | yes | none | Maximum commands per sealed object-log segment. Production rejects one-command object segments; the chart CI profile uses `1024`. |
| `PQUEUE_SQLITE_PROJECTION_DIR` | yes | none | Local SQLite projection directory. In Kubernetes this path is mounted from a PVC for production-like proofs. |

## Backend-Profile Settings Required by Helm

These keys are the runtime configuration the Helm deployment must supply for each
supported backend profile. Helm values and Kubernetes Secrets populate the
environment contract below.

Shared Helm values render the common service environment for both profiles:

| Helm value | Rendered runtime key |
|------------|----------------------|
| `config.listenAddr` | `PQUEUE_LISTEN_ADDR` |
| `backend.profile` | `PQUEUE_BACKEND_PROFILE` |
| `config.principalId` | `PQUEUE_PRINCIPAL_ID` |
| `config.tenants` | `PQUEUE_TENANTS` |
| `backend.shardCount.min` | `PQUEUE_SHARD_COUNT_MIN` |
| `backend.shardCount.max` | `PQUEUE_SHARD_COUNT_MAX` |

### `postgres_native`

- `PQUEUE_BACKEND_PROFILE=postgres_native`.
- `PQUEUE_POSTGRES_DATABASE_URL`, sourced from a Kubernetes Secret, must point at
  the PostgreSQL control-plane and data-plane database. The existing local proof
  fixture uses a `host=... port=5432 user=pqueue password=pqueue dbname=pqueue`
  shaped DSN (`crates/pqueue-service/tests/fixtures/postgres_native_local.toml`).
- `GET /readyz` opens that database connection and runs `SELECT 1`; Kubernetes
  readiness stays unavailable until the database accepts the query.
- Shard-count bounds per the sharding design (TD-003).
- Resource limits, telemetry, and credentials/secret references per the
  deployment-readiness contract's "Required Artifacts" section.

### `object_log_sqlite_projection`

- `PQUEUE_BACKEND_PROFILE=object_log_sqlite_projection`.
- `PQUEUE_POSTGRES_DATABASE_URL` is still required and is sourced from
  `backend.postgres.existingSecret` / `backend.postgres.databaseUrlKey`. In the
  checked-in CI fixture this is Secret `pqueue-postgres`, key `database-url`.
  For this profile it is the Postgres control-plane connection for queue/shard
  metadata and the manifest-pointer/fencing boundary; it is not replaced by
  SQLite.
- S3-compatible object storage is configured by these Helm values, rendered into
  environment variables:
  - `backend.objectLog.endpoint` -> `PQUEUE_OBJECT_LOG_ENDPOINT`
  - `backend.objectLog.bucket` -> `PQUEUE_OBJECT_LOG_BUCKET`
  - `backend.objectLog.region` -> `PQUEUE_OBJECT_LOG_REGION`
  - `backend.objectLog.segmentMaxCommands` ->
    `PQUEUE_OBJECT_LOG_SEGMENT_MAX_COMMANDS`
- S3 credentials are never chart defaults. They are sourced from
  `backend.objectLog.existingSecret`, `backend.objectLog.accessKeyIdKey`, and
  `backend.objectLog.secretAccessKeyKey`. The checked-in kind fixture uses
  Secret `pqueue-object-log` with keys `access-key-id` and `secret-access-key`.
- Object-log segment settings must keep `segmentMaxCommands > 1` in production.
  The CI values use `1024`, matching the runtime test contract and avoiding the
  forbidden one-object-per-command shape.
- SQLite projection storage is configured by
  `backend.sqliteProjection.mountPath`, rendered as
  `PQUEUE_SQLITE_PROJECTION_DIR`. The default and CI path is
  `/var/lib/pqueue/projection`.
- Production-like Kubernetes deployments must back the SQLite projection path
  with a PVC: `persistence.enabled=true` creates or references the claim named by
  `persistence.existingClaim`, or by default
  `<release-fullname>-sqlite-projection` (for the object-log CI render:
  `pqueue-object-log-sqlite-projection-sqlite-projection`). The default
  `persistence.size` storage request is `8Gi` with
  `persistence.accessModes=["ReadWriteOnce"]`; `persistence.storageClass` and
  `persistence.annotations` pass through to the PVC when set. `emptyDir` is only
  a disposable non-production proof shape.
- Shard-count bounds, resource limits, telemetry, and credentials/secret
  references per the deployment-readiness contract.

Only these two profiles are in BUILD-001 production-readiness scope. Other
profiles named by TD-001 (Kafka/Redpanda, DynamoDB-shaped) are design targets
only and are not selectable production profiles.

## Verification

- `docker build -t pqueue:dev .`
- `docker run --rm pqueue:dev --help` (exits 0, prints this contract)
- `cargo +1.92.0 build --release --workspace`
- `cargo test -p pqueue-objectlog -- --nocapture`
- `cargo test -p pqueue-service --test container_runtime_contract_tests -- --nocapture`
- `bash scripts/ci/helm-gate.sh`
- `bash scripts/ci/kind-helm-test.sh --backend object_log_sqlite_projection`
