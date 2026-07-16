# pqueue Operator Deployment Guide

<!-- markdownlint-disable MD013 -->

This guide covers the v0.9.0 release packaging. The Helm chart configures
storage as separate log, projection, and control-plane axes.

## Release Artifacts

- container image `ghcr.io/OWNER/pqueue-service:<version>`
- image digest evidence `pqueue-service-image.txt`
- Helm chart package `pqueue-<version>.tgz`
- Helm chart evidence `pqueue-helm-chart.txt`
- binary archives `pqueue-<version>-<target-triple>.tar.gz`
- checksum file `SHA256SUMS`

## Storage Axes

| Axis | Helm value | Values |
|------|------------|--------|
| Log backend | `storage.log.backend` | `objectlog`, `postgres` |
| Object-log store | `storage.log.objectLog.store` | `local`, `s3` |
| Projection backend | `storage.projection.backend` | `inmemory`, `sqlite`, `hybrid`, `hybrid-async`, `postgres` |
| Control plane | `storage.controlPlane.backend` | `inprocess`, `postgres` |

The current `pqueue-server` release smoke paths include `objectlog/inmemory`,
`objectlog/sqlite`, `objectlog/hybrid`, and `objectlog/hybrid-async`. The chart
also exposes a replica-safe shared profile that combines `objectlog` + `s3`
with `storage.controlPlane.backend=postgres` and `storage.projection.backend=sqlite`.
Unsupported chart combinations render statically and fail loudly at server startup
until their composition roots are wired.

`objectlog/hybrid` is the SQLite-first plus hot-memory object-log profile. It
uses `PQUEUE_SQLITE_PROJECTION_PATH`, treats the object log as the
authority, hydrates memory from SQLite `ProjectionImage` before serving from a
SQLite high-water, and fails closed on memory-apply poisoning. Non-objectlog
hybrid pairings are unsupported unless a future release explicitly documents and
tests them.

`storage.log.objectLog.store=local` is the legacy filesystem-backed profile.
It stays single-replica only. The chart fails closed if you scale it beyond one
pod because its log and projection persistence are not shared.

`storage.log.objectLog.store=s3` selects the shared S3 object-log profile. Use
it with `storage.controlPlane.backend=postgres`,
`storage.projection.backend=sqlite`, `replicaCount > 1`, and
`persistence.enabled=false`. The chart renders pod-reachable `PQUEUE_ADVERTISE_ADDR`
from the pod IP, uses `emptyDir` for the rebuildable SQLite projection, and does
not render the local object-log path or a shared RWO projection PVC.

`objectlog/hybrid-async` (`storage.projection.backend: hybrid-async`) runs the
same object-log + hybrid substrate under its canonical profile name: manifest
commit plus synchronous in-memory apply/render is the success barrier, and the
durable SQLite image is an asynchronous checkpoint that may lag (caught up by
object-log tail replay on recovery). Set the async-apply
debt/backpressure/poison thresholds under `storage.projection.hybridAsync`
(`applyLagMaxCommands`, `applyDebtMaxBytes`, `applyQueueDepthMax`,
`oldestUnappliedMaxMs`, `applyPoisonRetryThreshold`); the chart renders them as
`PQUEUE_HYBRID_ASYNC_*`. Every bound must be `> 0` (a zero bound is instantly
backpressured) and the server fails closed at startup otherwise. Only the
object-log log axis pairs with `hybrid-async`; `memory/hybrid-async`,
`sqlite/hybrid-async`, and `postgres/hybrid-async` fail at startup.

## Install

```sh
NAMESPACE=pqueue
RELEASE=pqueue

kubectl create namespace "$NAMESPACE"

helm install "$RELEASE" "$DIST_DIR/pqueue-${VERSION}.tgz" \
  --namespace "$NAMESPACE" \
  --set image.repository="$IMAGE" \
  --set image.tag="$VERSION" \
  --set storage.log.backend=objectlog \
  --set storage.log.objectLog.store=local \
  --set storage.projection.backend=inmemory
```

Replica-safe shared profile:

```sh
helm install "$RELEASE" "$DIST_DIR/pqueue-${VERSION}.tgz" \
  --namespace "$NAMESPACE" \
  --set image.repository="$IMAGE" \
  --set image.tag="$VERSION" \
  --set replicaCount=3 \
  --set storage.log.backend=objectlog \
  --set storage.log.objectLog.store=s3 \
  --set storage.controlPlane.backend=postgres \
  --set storage.projection.backend=sqlite
```

## Bootstrap Queue Inventories

The service provisions bootstrap queues before its RESP listener becomes ready.
Small deployments may provide exact queue keys through Helm:

```sh
--set-json 'bootstrap.queues=["tenant-a:work","tenant-a:priority"]'
```

For reproducible density and integration deployments, generate a bounded ordered
inventory instead of embedding a long comma-separated manifest:

```sh
--set bootstrap.generated.count=1001 \
--set bootstrap.generated.tenant=density \
--set bootstrap.generated.prefix=q
```

That contract deterministically creates `density:q0` through `density:q1000`.
The count must be between 1 and 10,000 when generation is enabled; both the Helm
schema and the server enforce the 10,000-queue ceiling. Tenant and prefix must be
non-empty and every generated identifier must pass the normal queue identifier
validation. The same tenant, prefix, and count always produce the same unique
queue keys in numeric order.

An explicit non-empty `bootstrap.queues` list takes precedence over generated
settings. With neither form configured, the server preserves the `t1:q1`
default. The corresponding direct environment variables are
`PQUEUE_BOOTSTRAP_QUEUES`, `PQUEUE_BOOTSTRAP_GENERATED_COUNT`,
`PQUEUE_BOOTSTRAP_GENERATED_TENANT`, and
`PQUEUE_BOOTSTRAP_GENERATED_PREFIX`.

## Verification

```sh
bash scripts/ci/helm-gate.sh
bash scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend inmemory
```

## Reference Documents

- [Operator release artifacts](operator-release-artifacts.md)
- [Helm static validation gate](helm-static-validation.md)
- [kind Helm integration harness](kind-helm-integration.md)
- [Container runtime contract](container-runtime-contract.md)
