# Fireweed Queue Operator Deployment Guide

<!-- markdownlint-disable MD013 -->

This guide covers the v0.20.0 Fireweed release packaging. The Helm chart configures
storage as separate log, projection, and control-plane axes.

## Release Artifacts

- container image `ghcr.io/OWNER/fireweed-service:<version>`
- image digest evidence `fireweed-service-image.txt`
- Helm chart package `fireweed-queue-<version>.tgz`
- Helm chart evidence `fireweed-queue-helm-chart.txt`
- binary archives `fireweed-<version>-<target-triple>.tar.gz`
- checksum file `SHA256SUMS`

## Storage Axes

| Axis | Helm value | Values |
|------|------------|--------|
| Log backend | `storage.log.backend` | `objectlog`, `postgres` |
| Object-log store | `storage.log.objectLog.store` | `local`, `s3` |
| Projection backend | `storage.projection.backend` | `inmemory`, `sqlite`, `hybrid`, `hybrid-async`, `postgres` |
| Control plane | `storage.controlPlane.backend` | `inprocess`, `postgres` |

The current `fireweed-server` release smoke paths include `objectlog/inmemory`,
`objectlog/sqlite`, `objectlog/hybrid`, and `objectlog/hybrid-async`. The chart
also exposes a replica-safe shared profile that combines `objectlog` + `s3`
with `storage.controlPlane.backend=postgres` and `storage.projection.backend=sqlite`.
Unsupported chart combinations render statically and fail loudly at server startup
until their composition roots are wired.

`objectlog/hybrid-strict` is a separate experimental runtime path, not an
unsupported chart combination awaiting an implicit startup check. It is
env/direct-config-only, intentionally omitted from the chart schema, live-kind
matrix, and production support contract. Setting
`storage.projection.backend=hybrid-strict` must fail Helm schema validation; do
not treat the server's `PQUEUE_PROJECTION_BACKEND=hybrid-strict` wiring as a
public deployment claim.

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
NAMESPACE=fireweed
RELEASE=fireweed

kubectl create namespace "$NAMESPACE"

helm install "$RELEASE" "$DIST_DIR/fireweed-queue-${VERSION}.tgz" \
  --namespace "$NAMESPACE" \
  --set image.repository="$IMAGE" \
  --set image.tag="$VERSION" \
  --set storage.log.backend=objectlog \
  --set storage.log.objectLog.store=local \
  --set storage.projection.backend=inmemory
```

Replica-safe shared profile:

```sh
helm install "$RELEASE" "$DIST_DIR/fireweed-queue-${VERSION}.tgz" \
  --namespace "$NAMESPACE" \
  --set image.repository="$IMAGE" \
  --set image.tag="$VERSION" \
  --set replicaCount=3 \
  --set storage.log.backend=objectlog \
  --set storage.log.objectLog.store=s3 \
  --set storage.controlPlane.backend=postgres \
  --set storage.projection.backend=sqlite
```

### Shared-profile operating boundary

Create the S3 and Postgres credentials before installing the shared profile.
`storage.log.objectLog.s3.credentials.existingSecret` must name a Secret with
the configured access-key and secret-key fields (the defaults are
`access-key-id` and `secret-access-key`).
`storage.controlPlane.postgres.existingSecret` must name a Secret whose
configured `databaseUrlKey` (default `database-url`) contains the control-plane
Postgres DSN. Keep credentials out of values files and rendered manifests; the
chart only renders Secret references.

The profile spans three failure domains: S3 is the durable log authority,
Postgres owns shared leases and fencing, and each pod holds only a rebuildable
SQLite projection in `emptyDir`. Losing a pod or its local volume triggers a
projection rebuild. Losing access to S3 or Postgres is an availability event
and must fail closed; neither another pod nor its local SQLite file substitutes
for those shared authorities. Spread replicas across nodes or zones according
to the availability policy of the S3 and Postgres services.

Switching from the local filesystem profile to the shared profile is an
explicit migration boundary, not an in-place Helm upgrade. The chart does not
copy the local object log into S3, migrate projection PVC contents, or create
the Postgres control-plane schema and credentials. Plan and validate those
steps separately before changing profiles. Rolling upgrades within the shared
profile must retain compatible S3 configuration, Postgres schema, Secret keys,
and fencing/lease settings until every replica runs the new image.

### Rename resource-recreation boundary

`fireweed-queue` changes the chart name, helper namespace, default workload
names, selector labels, service account, ConfigMap, Service, and PVC names. Helm
does not adopt resources from an existing `pqueue` release automatically. A
Fireweed install therefore creates a new resource set; it is not an in-place
rename of the old release.

For local persistent storage, stop writes, back up the old PVC, and either copy
its contents into the Fireweed PVC or set `persistence.existingClaim` to a
deliberately migrated claim before directing traffic to the new Service. Shared
S3/Postgres deployments may point at the same durable authorities only after
verifying compatible schemas, fencing settings, and Secrets. Keep the old
release available for rollback until Fireweed recovery and readback checks pass,
then remove it explicitly. The chart publishes no `pqueue` release or resource
alias.

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
