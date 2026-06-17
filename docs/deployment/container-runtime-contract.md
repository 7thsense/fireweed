# pqueue Container Image and Runtime Configuration Contract

This document is the runtime configuration contract for the pqueue service
container image. It is the interface the Helm chart populates and the
[production deployment readiness contract](../helix/04-build/DEPLOYMENT-READINESS.md)
depends on for the `postgres_native` and `object_log_sqlite_projection` backend
profiles.

It is intentionally split into what the v1 service binary consumes **today** and
the **reserved** keys the Kubernetes deployment must supply as the backend wiring
lands. This mirrors the forward-looking stance of the deployment-readiness
contract: do not claim more runtime behavior than the binary actually implements.

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
| `PQUEUE_POSTGRES_DATABASE_URL` | yes for `postgres_native` readiness | none | PostgreSQL connection URL used by the `postgres_native` readiness check. |

## Backend-Profile Settings Required by Helm (Reserved Contract)

These keys are the runtime configuration the Helm deployment must supply for each
supported backend profile. Helm values and Kubernetes Secrets populate the
environment contract below.

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
- Postgres control-plane connection (as above) for the manifest pointer / control
  plane.
- S3-compatible object storage endpoint, bucket, region, and credentials
  (MinIO in the local `kind` proof when the runtime supports it).
- Object-log segment settings and SQLite projection storage location/volume.
- Shard-count bounds, resource limits, telemetry, and credentials/secret
  references per the deployment-readiness contract.

Only these two profiles are in BUILD-001 production-readiness scope. Other
profiles named by TD-001 (Kafka/Redpanda, DynamoDB-shaped) are design targets
only and are not selectable production profiles.

## Verification

- `docker build -t pqueue:dev .`
- `docker run --rm pqueue:dev --help` (exits 0, prints this contract)
- `cargo +1.92.0 build --release --workspace`
- `cargo test -p pqueue-service --test container_runtime_contract_tests`
