# Fireweed Queue Operator Deployment Guide

<!-- markdownlint-disable MD013 -->

This guide covers the v0.22.0 Fireweed release packaging. The Helm chart configures
storage as separate **log**, **projection**, and **control-plane** axes — not as a
combined profile product type.

The v0.22.0 public preview defers GHCR publication. Container coordinates below
describe the deployment artifact contract; they are available only when a
release explicitly publishes and verifies those assets.

## Release Artifacts

- container image `ghcr.io/OWNER/fireweed-service:<version>`
- image digest evidence `fireweed-service-image.txt`
- Helm chart package `fireweed-queue-<version>.tgz`
- Helm chart evidence `fireweed-queue-helm-chart.txt`
- binary archives `fireweed-<version>-<target-triple>.tar.gz`
- checksum file `SHA256SUMS`

## Storage Axes

Storage is the orthogonal product of a log backend and a projection backend
(plus control plane). There is **no** public profile SKU; operators select each
axis independently. Helm keys are isomorphic to the product `StorageConfig`
shape (see [container-runtime-contract.md](container-runtime-contract.md)).

| Axis | Helm value | Public values |
|------|------------|---------------|
| Log backend | `storage.log.backend` | `memory`, `sqlite`, `postgres`, `filesystem`, `s3` |
| Projection | `storage.projection.backend` | `memory`, `sqlite`, `postgres` |
| Control plane | `storage.controlPlane.backend` | `inprocess`, `postgres` |

### Durability classes

| Class | Log backends | Client contract |
|-------|--------------|-----------------|
| **A — Durable log** | `sqlite`, `postgres`, `filesystem`, `s3` | Success ⇒ durable on the log and visible in the serving projection; recovery via high-water + tail when the log remains; `request_id` resolves ambiguity across crash |
| **B — Memory log** | `memory` | Success ⇒ visible in the projection; durable **only if** the projection is durable (`sqlite` / `postgres`). After process death only the projection remains. **No** Class A log rebuild, branch, read-as-of, or change-record-from-log claims |

**Class B disclaimer:** a `memory` log is an explicit weaker persistence
envelope, not a second architecture. Use it for development and evaluation.
Do not claim Class A recovery or durability for any memory-log combination.

### Full matrix (15 cells)

Every cell is a valid selection. Semantics differ only by durability class.
Runtime wiring and release evidence may still be sparse; unsupported or
not-yet-verified cells fail closed at startup.

| Log \ Projection | `memory` | `sqlite` | `postgres` |
|------------------|----------|----------|------------|
| `memory` | Class B | Class B | Class B |
| `sqlite` | Class A | Class A | Class A |
| `postgres` | Class A | Class A | Class A |
| `filesystem` | Class A | Class A | Class A |
| `s3` | Class A | Class A | Class A |

### Object-log peers: `filesystem` and `s3`

`filesystem` and `s3` are first-class object-log peers (same protocol: segments,
manifest, conditional write / authority, retention). They are not “fake S3” vs
“real S3.” Multi-writer still requires ownership and fencing; a shared path or
bucket is not an automatic free multi-writer free-for-all.

**Filesystem log (local disk or NAS).** Set `storage.log.backend=filesystem` and
point `storage.log.objectLog.root` at a durable directory. NAS examples use a
shared mount such as `/tank/fireweed/object-log` (or any site path under
`/tank/...`). Default chart root is `/var/lib/fireweed/projection/object-log`
for pod-local disk. Single-site shared filesystem deployments stay
single-writer unless control-plane ownership and fencing are configured for
multi-replica use; the chart fails closed if a local filesystem object-log
deployment is scaled beyond one pod without a shared multi-writer design.

**S3 log.** Set `storage.log.backend=s3` and configure
`storage.log.objectLog.s3` (endpoint, bucket, region, credentials Secret).
Use with `storage.controlPlane.backend=postgres` for multi-replica ownership
and atomic create-only publication authority when the S3 implementation lacks
that primitive. Pair with a rebuildable local projection
(`storage.projection.backend=sqlite`, `persistence.enabled=false`) so each pod
rebuilds from the shared log.

### Structured fields

- Filesystem root: `storage.log.objectLog.root` (used when log is `filesystem`)
- S3 block: `storage.log.objectLog.s3.*` (used when log is `s3`)
- SQLite log path: `storage.log.sqlite.path` (when log is `sqlite`)
- Projection SQLite path: `storage.projection.sqlite.path` (when projection is `sqlite`)
- Postgres DSN Secret refs: `storage.log.postgres.*` / `storage.projection.postgres.*`

### Compat aliases (not product identity)

Legacy chart spellings still parse for one minor:

- `storage.log.backend=objectlog` + `objectLog.store=local` → product log `filesystem`
- `storage.log.backend=objectlog` + `objectLog.store=s3` → product log `s3`
- `storage.projection.backend=inmemory` → product projection `memory`

Prefer the five public log names and three public projection names in new
values files and operator runbooks. Non-public projection values (`hybrid`,
`hybrid-async`, `turso`) may still render for transitional wiring; they are not
public matrix rows and must not be treated as product SKUs. Setting
`storage.projection.backend=hybrid-strict` fails Helm schema validation.

Postgres is a first-class log and projection backend. Feature flags or image
builds that omit the adapter are packaging choices and fail closed with a clear
message — not “Postgres unfinished.”

## Install

Default chart axes are `filesystem` log × `memory` projection. Minimal install:

```sh
NAMESPACE=fireweed
RELEASE=fireweed

kubectl create namespace "$NAMESPACE"

helm install "$RELEASE" "$DIST_DIR/fireweed-queue-${VERSION}.tgz" \
  --namespace "$NAMESPACE" \
  --set image.repository="$IMAGE" \
  --set image.tag="$VERSION" \
  --set storage.log.backend=filesystem \
  --set storage.log.objectLog.root=/var/lib/fireweed/projection/object-log \
  --set storage.projection.backend=memory
```

### Filesystem log on NAS (example)

Mount the NAS at a stable path (for example `/tank/fireweed`) and point the
object-log root at a subdirectory on that mount:

```sh
helm install "$RELEASE" "$DIST_DIR/fireweed-queue-${VERSION}.tgz" \
  --namespace "$NAMESPACE" \
  --set image.repository="$IMAGE" \
  --set image.tag="$VERSION" \
  --set storage.log.backend=filesystem \
  --set storage.log.objectLog.root=/tank/fireweed/object-log \
  --set storage.projection.backend=sqlite \
  --set storage.projection.sqlite.path=/var/lib/fireweed/projection/projection.db
```

Ensure the pod can write the `/tank/...` path (hostPath, CSI, or NFS volume).
Keep replica count at one unless multi-writer ownership and fencing are fully
configured for that shared filesystem.

### S3 log multi-replica (example)

```sh
helm install "$RELEASE" "$DIST_DIR/fireweed-queue-${VERSION}.tgz" \
  --namespace "$NAMESPACE" \
  --set image.repository="$IMAGE" \
  --set image.tag="$VERSION" \
  --set replicaCount=3 \
  --set storage.log.backend=s3 \
  --set storage.log.objectLog.s3.endpoint=https://s3.example.com \
  --set storage.log.objectLog.s3.bucket=fireweed-shared \
  --set storage.log.objectLog.s3.region=us-east-1 \
  --set storage.log.objectLog.s3.credentials.existingSecret=fireweed-objectlog-s3 \
  --set storage.controlPlane.backend=postgres \
  --set storage.controlPlane.postgres.existingSecret=fireweed-control-plane \
  --set storage.projection.backend=sqlite \
  --set persistence.enabled=false
```

Or install with the checked-in shared values file
(`charts/fireweed-queue/values-shared-s3.yaml`), which selects the same
replica-safe combination (compat `objectlog`+`store=s3` spelling is equivalent
to public `s3`).

### Shared S3 operating boundary

Create the S3 and Postgres credentials before installing.
`storage.log.objectLog.s3.credentials.existingSecret` must name a Secret with
the configured access-key and secret-key fields (defaults `access-key-id` and
`secret-access-key`).
`storage.controlPlane.postgres.existingSecret` must name a Secret whose
configured `databaseUrlKey` (default `database-url`) contains the control-plane
Postgres DSN. Keep credentials out of values files and rendered manifests; the
chart only renders Secret references.

The deployment spans three failure domains: S3 holds durable log objects,
Postgres owns atomic object publication plus shared leases and fencing, and each
pod holds only a rebuildable SQLite projection in `emptyDir`. Losing a pod or
its local volume triggers a projection rebuild. Losing access to S3 or Postgres
is an availability event and must fail closed; neither another pod nor its local
SQLite file substitutes for those shared authorities. Spread replicas across
nodes or zones according to the availability policy of the S3 and Postgres
services.

Switching from a local filesystem log to an S3 log is an explicit migration
boundary, not an in-place Helm upgrade. The chart does not copy the local
object log into S3, migrate projection PVC contents, or create the Postgres
control-plane schema and credentials. Plan and validate those steps separately
before changing axes. Rolling upgrades within an S3 multi-replica deployment
must retain compatible S3 configuration, Postgres schema, Secret keys, and
fencing/lease settings until every replica runs the new image.

The PostgreSQL control-plane DSN also provides the atomic create-only
publication authority for S3 implementations that do not provide that primitive
themselves. With an in-process control plane, startup probes the S3 endpoint's
native create-only behavior and fails closed when it cannot prove the required
contract.

### Rename resource-recreation boundary

`fireweed-queue` changes the chart name, helper namespace, default workload
names, selector labels, service account, ConfigMap, Service, and PVC names. Helm
does not adopt resources from an existing `fireweed` release automatically. A
Fireweed install therefore creates a new resource set; it is not an in-place
rename of the old release.

For local persistent storage, stop writes, back up the old PVC, and either copy
its contents into the Fireweed PVC or set `persistence.existingClaim` to a
deliberately migrated claim before directing traffic to the new Service. Shared
S3/Postgres deployments may point at the same durable authorities only after
verifying compatible schemas, fencing settings, and Secrets. Keep the old
release available for rollback until Fireweed recovery and readback checks pass,
then remove it explicitly. The chart publishes no `fireweed` release or resource
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
`FIREWEED_BOOTSTRAP_QUEUES`, `FIREWEED_BOOTSTRAP_GENERATED_COUNT`,
`FIREWEED_BOOTSTRAP_GENERATED_TENANT`, and
`FIREWEED_BOOTSTRAP_GENERATED_PREFIX`.

## Verification

```sh
bash scripts/ci/helm-gate.sh
bash scripts/ci/kind-helm-test.sh --log-backend filesystem --projection-backend memory
```

Legacy harness spellings (`objectlog` / `inmemory`) remain accepted as compat
aliases for the same axes.

## Reference Documents

- [Operator release artifacts](operator-release-artifacts.md)
- [Helm static validation gate](helm-static-validation.md)
- [kind Helm integration harness](kind-helm-integration.md)
- [Container runtime contract](container-runtime-contract.md)
