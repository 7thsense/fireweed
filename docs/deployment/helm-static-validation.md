# Helm static validation gate

`scripts/ci/helm-gate.sh` validates the Fireweed Queue Helm chart without creating a
cluster.

The chart storage contract is expressed as axes isomorphic to `StorageConfig`:

- `storage.log.backend`: public `memory` | `sqlite` | `postgres` | `filesystem` | `s3`; compat alias `objectlog` (+ `objectLog.store` local|s3 → filesystem|s3)
- `storage.log.objectLog.root` / `objectLog.s3.*`: filesystem root and S3 credential blocks (structured fields)
- `storage.projection.backend`: public `memory` | `sqlite` | `postgres`; compat `inmemory` only
- `storage.controlPlane.backend`: `inprocess` or `postgres`

Demoted projection values (`hybrid`, `hybrid-async`, `hybrid-strict`, `turso`) are
**not** part of the chart contract or public support surface. The gate contains a
named negative assertion that requires Helm to reject each demoted value at
`/storage/projection/backend` with the exact public allowed enum; an unrelated
render failure cannot satisfy the assertion. Server-side hybrid runtime code may
still exist for direct `Config` construction / internal tests, but the public
env adapter rejects hybrid selection and Turso remains feature-gated non-public.

`shared-s3-postgres-control-plane` is the replica-safe shared profile. It
renders `FIREWEED_OBJECT_LOG_S3_*`,
`FIREWEED_POSTGRES_CONTROL_PLANE_DATABASE_URL`, and `FIREWEED_ADVERTISE_ADDR` from the pod
IP, uses `replicaCount=3`, and keeps
SQLite projections pod-local via `emptyDir` rather than a shared RWO PVC.
The PostgreSQL DSN is both the shared queue control plane and the atomic
create-only publication authority for S3 implementations without native
conditional PutObject support.
The chart fails closed if a local object-log profile is scaled beyond one
replica.

The gate first proves demoted-projection schema exclusion, then runs
`helm lint --strict`, renders checked-in CI values for selected axis
combinations, asserts the rendered environment variables, and validates the
manifests with `kubeconform`.

```sh
bash scripts/ci/helm-gate.sh
```

Runtime smoke testing is separate:

```sh
bash scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend inmemory
bash scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend sqlite
```
