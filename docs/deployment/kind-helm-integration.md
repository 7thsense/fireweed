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
5. installs `charts/pqueue` with the selected CI values file;
6. waits for the Helm release and deployment rollout;
7. checks `GET /readyz` through `kubectl port-forward`;
8. deletes the `kind` cluster on exit.

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
Helm chart expects for the supported backend profiles. The current service
binary validates the backend profile and serves health endpoints without opening
backend connections; later backend-specific beads can extend the same harness
with Postgres and MinIO runtime dependencies as those paths become live.
