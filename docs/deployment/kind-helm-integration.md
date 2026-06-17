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
