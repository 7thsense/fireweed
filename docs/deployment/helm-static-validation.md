# Helm static validation gate

`scripts/ci/helm-gate.sh` validates the pqueue Helm chart without creating a
cluster.

The chart storage contract is expressed as axes:

- `storage.log.backend`: `objectlog` or `postgres`
- `storage.log.objectLog.store`: `local` or `s3`
- `storage.projection.backend`: `inmemory`, `sqlite`, `turso`, `hybrid`, `hybrid-async`, or `postgres`
- `storage.controlPlane.backend`: `inprocess` or `postgres`

`objectlog/hybrid-strict` is not part of the chart contract or public support
surface. The server retains an experimental env/direct-config-only runtime path,
but the chart schema intentionally excludes `hybrid-strict`. The gate contains
a named negative assertion that requires Helm to reject that value at
`/storage/projection/backend` with the exact allowed enum; an unrelated render
failure cannot satisfy the assertion.

`hybrid` is the projection value for the normative `objectlog/hybrid` contract:
the runtime renders `PQUEUE_PROJECTION_BACKEND=hybrid`, uses
`PQUEUE_SQLITE_PROJECTION_PATH`, applies SQLite first and then memory, and must
fail closed for unsupported non-objectlog pairings until they are implemented and
tested.

`hybrid-async` is the projection value for the `objectlog/hybrid-async` profile:
the runtime renders `PQUEUE_PROJECTION_BACKEND=hybrid-async`, the same
`PQUEUE_SQLITE_PROJECTION_PATH`, and the async-apply threshold env
`PQUEUE_HYBRID_ASYNC_*` from `storage.projection.hybridAsync`. The chart schema
constrains every threshold to `>= 1`; a checked-in CI values profile,
`charts/pqueue/ci/objectlog-hybrid-async-values.yaml`, renders the combination and
is included in the static gate. Its rendered-contract assertions require the
SQLite path and persistent volume mount plus all five fail-closed controls:
`PQUEUE_HYBRID_ASYNC_APPLY_LAG_MAX_COMMANDS`,
`PQUEUE_HYBRID_ASYNC_APPLY_DEBT_MAX_BYTES`,
`PQUEUE_HYBRID_ASYNC_APPLY_QUEUE_DEPTH_MAX`,
`PQUEUE_HYBRID_ASYNC_OLDEST_UNAPPLIED_MAX_MS`, and
`PQUEUE_HYBRID_ASYNC_APPLY_POISON_RETRY_THRESHOLD`. Only the object-log log
axis pairs with `hybrid-async`; other pairings fail closed at startup.

`shared-s3-postgres-control-plane` is the replica-safe shared profile. It
renders `PQUEUE_OBJECT_LOG_S3_*`, `PQUEUE_POSTGRES_CONTROL_PLANE_DATABASE_URL`,
and `PQUEUE_ADVERTISE_ADDR` from the pod IP, uses `replicaCount=3`, and keeps
SQLite projections pod-local via `emptyDir` rather than a shared RWO PVC.
The chart fails closed if a local object-log profile is scaled beyond one
replica.

The gate first proves the `hybrid-strict` schema exclusion, then runs
`helm lint --strict`, renders checked-in CI values for selected axis
combinations, asserts the rendered environment variables, and validates the
manifests with `kubeconform`.

```sh
bash scripts/ci/helm-gate.sh
```

Runtime smoke testing is separate:

```sh
bash scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend inmemory
bash scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend hybrid-async
```
