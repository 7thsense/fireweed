# Helm static validation gate

`scripts/ci/helm-gate.sh` validates the Fireweed Queue Helm chart without creating a
cluster.

The chart storage contract is expressed as axes isomorphic to `StorageConfig`:

- `storage.log.backend`: public `memory` | `sqlite` | `postgres` | `filesystem` | `s3`
- `storage.log.objectLog.root` / `objectLog.s3.*`: filesystem root and S3 credential blocks (structured fields)
- `storage.projection.backend`: public `memory` | `sqlite` | `postgres`
- `storage.controlPlane.backend`: `inprocess` or `postgres`

Only those public product values are chart-selectable. The gate contains named
negative assertions that require Helm to reject demoted or legacy backend names
at `/storage/log/backend` and `/storage/projection/backend` with the exact public
allowed enums; an unrelated render failure cannot satisfy the assertion.
Server-side implementation code may still exist for direct `Config` construction
/ internal tests, but the public env adapter and chart schema hard-reject those
names.

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

The gate first proves demoted/legacy backend schema exclusion, then runs
`helm lint --strict`, renders checked-in CI values for selected axis
combinations, asserts the rendered environment variables, and validates the
manifests with `kubeconform`.

```sh
bash scripts/ci/helm-gate.sh
```

Runtime smoke testing is separate:

```sh
bash scripts/ci/kind-helm-test.sh --log-backend filesystem --projection-backend memory
bash scripts/ci/kind-helm-test.sh --log-backend filesystem --projection-backend sqlite
```
