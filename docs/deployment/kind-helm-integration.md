# kind Helm integration harness

`scripts/ci/kind-helm-test.sh` is the reusable local integration harness for
installing the pqueue Helm chart into a disposable `kind` cluster. It is the
runtime companion to the static chart gate in `scripts/ci/helm-gate.sh`.

The harness supports both BUILD-001 backend profiles:

- `postgres_native`
- `object_log_sqlite_projection`

## Prerequisites

Real runs require these tools on `PATH`:

- `docker`
- `kind`
- `kubectl`
- `helm`

The script checks for those tools before it creates a cluster. `--dry-run`
validates the selected backend and prints the planned commands without checking
tools or touching Docker/Kubernetes.

## Running

```sh
bash scripts/ci/kind-helm-test.sh --backend postgres_native
bash scripts/ci/kind-helm-test.sh --backend object_log_sqlite_projection
```

By default the script:

1. builds `pqueue:ci` from the repository root;
2. creates a disposable `kind` cluster;
3. loads `pqueue:ci` into the cluster;
4. applies `scripts/ci/kind/runtime-secrets.yaml`;
5. for `postgres_native`, installs the disposable PostgreSQL fixture in
   `scripts/ci/kind/postgres.yaml` and waits for its Deployment rollout;
6. installs `charts/pqueue` with the selected CI values file;
7. waits for the Helm release and pqueue Deployment rollout;
8. checks `GET /readyz` through `kubectl port-forward`;
9. deletes the `kind` cluster on exit.

Use `--keep-cluster` to preserve the cluster for debugging.

## Object-log / MinIO proof

The object-log production-readiness smoke command is:

```sh
bash scripts/ci/kind-helm-test.sh --backend object_log_sqlite_projection
```

For `object_log_sqlite_projection`, the Helm CI values render this runtime
contract:

- `backend.profile=object_log_sqlite_projection` ->
  `PQUEUE_BACKEND_PROFILE=object_log_sqlite_projection`.
- `backend.postgres.existingSecret=pqueue-postgres` and
  `backend.postgres.databaseUrlKey=database-url` ->
  `PQUEUE_POSTGRES_DATABASE_URL`, the required Postgres control-plane URL.
- `backend.objectLog.endpoint=http://minio:9000`,
  `backend.objectLog.bucket=pqueue-object-log`,
  `backend.objectLog.region=us-east-1`, and
  `backend.objectLog.segmentMaxCommands=1024` ->
  `PQUEUE_OBJECT_LOG_ENDPOINT`, `PQUEUE_OBJECT_LOG_BUCKET`,
  `PQUEUE_OBJECT_LOG_REGION`, and
  `PQUEUE_OBJECT_LOG_SEGMENT_MAX_COMMANDS`.
- `backend.objectLog.existingSecret=pqueue-object-log` with keys
  `access-key-id` and `secret-access-key` ->
  `PQUEUE_OBJECT_LOG_ACCESS_KEY_ID` and
  `PQUEUE_OBJECT_LOG_SECRET_ACCESS_KEY`.
- `backend.sqliteProjection.mountPath=/var/lib/pqueue/projection` ->
  `PQUEUE_SQLITE_PROJECTION_DIR`; with `persistence.enabled=true`, that path is
  backed by the SQLite projection PVC.

A passing object-log kind proof must exercise a real S3-compatible MinIO
endpoint from inside the cluster: `/readyz` performs object-log storage probes
against the configured bucket, and the deployment must use the same Postgres
control-plane Secret and PVC-backed SQLite projection path that Helm rendered.
The release evidence must also cover the restart/replay boundary for this
profile: after a pqueue pod restart, acknowledged object-log state is recovered
from MinIO segments/snapshots plus the SQLite projection path, and `/readyz`
returns `ready` again.

This is a MinIO S3-compatible proof only. It does not certify AWS S3, GCS S3
interop, IAM policy, provider TLS/certificate configuration, or any
cloud-provider-specific conditional-write behavior.

## Dry run

```sh
bash scripts/ci/kind-helm-test.sh --backend postgres_native --dry-run
bash scripts/ci/kind-helm-test.sh --backend object_log_sqlite_projection --dry-run
```

Dry-run output includes the selected backend, cluster/release/namespace names,
the image, the Helm values file, helper manifests, and the exact command plan.

## Helper manifests

`scripts/ci/kind/runtime-secrets.yaml` creates the Kubernetes Secrets that the
Helm chart expects for the supported backend profiles.

For `postgres_native`, `scripts/ci/kind/postgres.yaml` creates an ephemeral
PostgreSQL Deployment and Service named `postgres`. The pqueue Secret points at
that Service. The service readiness endpoint opens a PostgreSQL connection and
runs `SELECT 1`, so the kind smoke proves Kubernetes rollout plus a working
database dependency instead of only proving template rendering.

For `object_log_sqlite_projection`, `scripts/ci/kind/runtime-secrets.yaml`
creates:

- Secret `pqueue-postgres`, key `database-url`, consumed as
  `PQUEUE_POSTGRES_DATABASE_URL` for the Postgres control plane.
- Secret `pqueue-object-log`, keys `access-key-id` and `secret-access-key`,
  consumed as the S3-compatible MinIO credentials.

The object-log values file points the endpoint at the in-cluster MinIO service
name `minio` on port `9000` and stores the local SQLite projection under
`/var/lib/pqueue/projection` on the projection PVC.

## Required verification commands

The deployment docs preserve these gates for the object-log runtime and proof
boundary:

```sh
cargo test -p pqueue-objectlog -- --nocapture
cargo test -p pqueue-service --test container_runtime_contract_tests -- --nocapture
bash scripts/ci/helm-gate.sh
bash scripts/ci/kind-helm-test.sh --backend object_log_sqlite_projection
```
