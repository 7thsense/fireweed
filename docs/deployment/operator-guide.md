# pqueue Operator Deployment Guide

<!-- markdownlint-disable MD013 -->

This guide is the operator entry point for installing, upgrading, uninstalling,
and verifying pqueue from the BUILD-001 release line. It keeps the Helm commands,
backend profile choice, release artifacts, and production-readiness boundary in
one place.

Open the static operator microsite directly at:

```text
docs/operator/index.html
```

The microsite is static HTML and has no generated build output.

## Production Readiness Status

| Backend profile                | Status                                                                                                                                                | What is proven                                                                                                                                                                                                                           | What is not proven                                                                                                                                                                                                                                                                                             |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `postgres_native`              | Production-readiness target for BUILD-001 once the Helm and kind gates pass for the release artifact being installed.                                 | Helm render/lint, `kind` install/upgrade/uninstall smoke, PostgreSQL readiness probe, release artifact checksum and image digest verification.                                                                                           | Environment-specific capacity planning, backup/restore policy, managed Postgres hardening, and P1 operator workflows unless a release explicitly includes them.                                                                                                                                                |
| `object_log_sqlite_projection` | Production-readiness target for BUILD-001 once the release artifact gate and MinIO-backed `kind` proof pass for the release artifact being installed. | Helm renders S3-compatible object-log settings, Postgres control-plane Secret references, object-store Secret references, and a PVC-backed SQLite projection path. The local proof exercises MinIO bucket setup, object-log writes, restart/replay, and SQLite projection recovery. | No cloud S3 certification. The current boundary does not claim AWS S3, GCS S3 interop, IAM policy, provider TLS/certificates, or provider-specific conditional-write behavior. |

Do not cite `object_log_sqlite_projection` as cloud-provider S3 certified. The
BUILD-001 production boundary is MinIO S3-compatible `kind` proof only. See
[Production Deployment Readiness Contract](../helix/04-build/DEPLOYMENT-READINESS.md)
for the formal gate.

## Release Artifacts

Every install starts with the published GitHub Release assets:

- container image `ghcr.io/OWNER/pqueue-service:<version>` and
  `ghcr.io/OWNER/pqueue-service:sha-<commit>`;
- image digest evidence `pqueue-service-image.txt`;
- Helm chart package `pqueue-<version>.tgz`;
- Helm chart evidence `pqueue-helm-chart.txt`;
- binary archives `pqueue-<version>-<target-triple>.tar.gz`;
- checksum file `SHA256SUMS`.

Download and verify them before running `helm install`:

```sh
OWNER=<github-owner>
REPO=pqueue
TAG=v0.2.1
VERSION="${TAG#v}"
DIST_DIR="release-${TAG}"

mkdir -p "$DIST_DIR"
gh release download "$TAG" \
  --repo "${OWNER}/${REPO}" \
  --pattern "pqueue-${VERSION}-*.tar.gz" \
  --pattern "pqueue-${VERSION}.tgz" \
  --pattern "pqueue-service-image.txt" \
  --pattern "pqueue-helm-chart.txt" \
  --pattern "SHA256SUMS" \
  --dir "$DIST_DIR"

if command -v sha256sum >/dev/null 2>&1; then
  (cd "$DIST_DIR" && sha256sum -c SHA256SUMS)
else
  (cd "$DIST_DIR" && shasum -a 256 -c SHA256SUMS)
fi
```

Verify the container digest before deployment:

```sh
IMAGE_OWNER="$(printf '%s' "$OWNER" | tr '[:upper:]' '[:lower:]')"
IMAGE="ghcr.io/${IMAGE_OWNER}/pqueue-service"
DIGEST="$(awk -F= '$1 == "digest" { print $2 }' "${DIST_DIR}/pqueue-service-image.txt")"
REMOTE_DIGEST="$(docker buildx imagetools inspect "${IMAGE}:${VERSION}" | awk '/Digest:/ { print $2; exit }')"

test "$REMOTE_DIGEST" = "$DIGEST"
```

For the full release artifacts procedure, see
[Operator Release Artifacts](operator-release-artifacts.md).

## Backend Profile Choice

Choose one profile before installing:

| Profile                        | Use when                                                                                                   | Required dependencies                                                                                                                                |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `postgres_native`              | You want the correctness reference backend and a single PostgreSQL-backed deployment.                      | PostgreSQL database reachable from the cluster and Secret `pqueue-postgres` with key `database-url`.                                                 |
| `object_log_sqlite_projection` | You are validating the object-log plus SQLite projection mode against the documented MinIO proof boundary. | PostgreSQL control-plane database, S3-compatible object storage such as MinIO, Secret `pqueue-object-log`, and PVC-backed SQLite projection storage. |

Other backend names in design docs are not BUILD-001 production profiles.

## Values Reference

Common values:

| Helm value                                          | Purpose                                                                         |
| --------------------------------------------------- | ------------------------------------------------------------------------------- |
| `image.repository`                                  | Container image repository, for example `ghcr.io/OWNER/pqueue-service`.         |
| `image.tag`                                         | Image tag. Prefer digest pinning through the deployment platform when possible. |
| `backend.profile`                                   | `postgres_native` or `object_log_sqlite_projection`.                            |
| `backend.shardCount.min` / `backend.shardCount.max` | Rendered as shard-count bounds for the selected backend.                        |
| `config.listenAddr`                                 | Rendered as `PQUEUE_LISTEN_ADDR`; defaults to `0.0.0.0:8080`.                   |
| `config.principalId`                                | Bootstrap service principal id.                                                 |
| `config.tenants`                                    | Bootstrap tenant allowlist.                                                     |
| `resources`                                         | Kubernetes requests and limits for `pqueue-service`.                            |
| `probes.liveness` / `probes.readiness`              | Kubernetes probe paths and timings.                                             |

`postgres_native` values:

| Helm value                        | Purpose                                                |
| --------------------------------- | ------------------------------------------------------ |
| `backend.postgres.existingSecret` | Secret containing the PostgreSQL connection URL.       |
| `backend.postgres.databaseUrlKey` | Secret key rendered as `PQUEUE_POSTGRES_DATABASE_URL`. |

`object_log_sqlite_projection` values:

| Helm value                                                                                             | Purpose                                                                                                  |
| ------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------- |
| `backend.postgres.existingSecret` / `backend.postgres.databaseUrlKey`                                  | Postgres control-plane URL Secret and key.                                                               |
| `backend.objectLog.endpoint`                                                                           | S3-compatible endpoint, for example `http://minio:9000` in kind/MinIO.                                   |
| `backend.objectLog.bucket`                                                                             | Object-log bucket name.                                                                                  |
| `backend.objectLog.region`                                                                             | S3-compatible signing region.                                                                            |
| `backend.objectLog.segmentMaxCommands`                                                                 | Commands per object-log segment. Production must keep this greater than `1`; the CI profile uses `1024`. |
| `backend.objectLog.existingSecret`                                                                     | Secret containing object-store credentials.                                                              |
| `backend.objectLog.accessKeyIdKey`                                                                     | Secret key rendered as `PQUEUE_OBJECT_LOG_ACCESS_KEY_ID`.                                                |
| `backend.objectLog.secretAccessKeyKey`                                                                 | Secret key rendered as `PQUEUE_OBJECT_LOG_SECRET_ACCESS_KEY`.                                            |
| `backend.sqliteProjection.mountPath`                                                                   | Rendered as `PQUEUE_SQLITE_PROJECTION_DIR`.                                                              |
| `persistence.enabled`                                                                                  | When true, creates or references a PVC for the SQLite projection path.                                   |
| `persistence.existingClaim`, `persistence.storageClass`, `persistence.accessModes`, `persistence.size` | PVC selection and storage request settings.                                                              |

The chart defaults and schema live in `charts/pqueue/values.yaml` and
`charts/pqueue/values.schema.json`.

## Helm Install

Create the runtime namespace and Secrets first. For `postgres_native`:

```sh
NAMESPACE=pqueue
RELEASE=pqueue

kubectl create namespace "$NAMESPACE"
kubectl -n "$NAMESPACE" create secret generic pqueue-postgres \
  --from-literal=database-url='postgres://pqueue:pqueue@postgres.example:5432/pqueue'
```

Install from a verified chart package:

```sh
helm install "$RELEASE" "$DIST_DIR/pqueue-${VERSION}.tgz" \
  --namespace "$NAMESPACE" \
  --set image.repository="$IMAGE" \
  --set image.tag="$VERSION" \
  --set backend.profile=postgres_native \
  --set backend.postgres.existingSecret=pqueue-postgres \
  --set backend.postgres.databaseUrlKey=database-url
```

For `object_log_sqlite_projection`, create both Secrets and install with
object-log values:

```sh
kubectl -n "$NAMESPACE" create secret generic pqueue-object-log \
  --from-literal=access-key-id='<access-key-id>' \
  --from-literal=secret-access-key='<secret-access-key>'

helm install "$RELEASE" "$DIST_DIR/pqueue-${VERSION}.tgz" \
  --namespace "$NAMESPACE" \
  --set image.repository="$IMAGE" \
  --set image.tag="$VERSION" \
  --set backend.profile=object_log_sqlite_projection \
  --set backend.postgres.existingSecret=pqueue-postgres \
  --set backend.objectLog.endpoint='http://minio:9000' \
  --set backend.objectLog.bucket=pqueue-object-log \
  --set backend.objectLog.region=us-east-1 \
  --set backend.objectLog.segmentMaxCommands=1024 \
  --set backend.objectLog.existingSecret=pqueue-object-log \
  --set persistence.enabled=true
```

## Helm Upgrade

Verify the new release artifacts first, then upgrade the chart and image
together:

```sh
NEW_TAG=v0.1.1
NEW_VERSION="${NEW_TAG#v}"
NEW_DIST_DIR="release-${NEW_TAG}"

helm upgrade "$RELEASE" "$NEW_DIST_DIR/pqueue-${NEW_VERSION}.tgz" \
  --namespace "$NAMESPACE" \
  --reuse-values \
  --set image.repository="$IMAGE" \
  --set image.tag="$NEW_VERSION"

kubectl -n "$NAMESPACE" rollout status deploy/"$RELEASE"-pqueue
```

For backend-profile changes, prefer a planned migration runbook. Do not switch
profiles by `helm upgrade` unless the release notes provide explicit data-plane
migration instructions.

## Helm Uninstall

Uninstall the release:

```sh
helm uninstall "$RELEASE" --namespace "$NAMESPACE"
```

Secrets and PVCs are operator-owned. Delete them only after confirming the data
retention and recovery policy for the environment:

```sh
kubectl -n "$NAMESPACE" delete secret pqueue-postgres
kubectl -n "$NAMESPACE" delete secret pqueue-object-log
kubectl -n "$NAMESPACE" delete pvc -l app.kubernetes.io/instance="$RELEASE"
```

## kind Smoke Testing

Static Helm validation:

```sh
bash scripts/ci/helm-gate.sh
```

Disposable `kind` smoke for `postgres_native`:

```sh
bash scripts/ci/kind-helm-test.sh --backend postgres_native
```

Target `kind` smoke for `object_log_sqlite_projection`:

```sh
bash scripts/ci/kind-helm-test.sh --backend object_log_sqlite_projection
```

Current status: the object-log `kind` command is the required target, not
completed release evidence, until the MinIO bucket setup, in-cluster S3 probe,
object-log write path, restart/replay check, and SQLite projection recovery
checks are present and passing.

## Troubleshooting

| Symptom                                              | Check                                                                                              | Likely fix                                                                                                                                              |
| ---------------------------------------------------- | -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `helm install` fails schema validation               | Run `bash scripts/ci/helm-gate.sh` and inspect the values rejected by `values.schema.json`.        | Use only supported `backend.profile` names and valid object-log/PVC values.                                                                             |
| Pods stay unready for `postgres_native`              | `kubectl -n "$NAMESPACE" logs deploy/"$RELEASE"-pqueue` and inspect `/readyz`.                     | Confirm Secret `pqueue-postgres` exists, key `database-url` is present, and PostgreSQL accepts `SELECT 1`.                                              |
| Pods stay unready for `object_log_sqlite_projection` | Check the Postgres Secret, object-log Secret, S3 endpoint/bucket, and SQLite projection PVC mount. | Create the bucket, correct MinIO/S3 credentials, verify `PQUEUE_OBJECT_LOG_SEGMENT_MAX_COMMANDS > 1`, and confirm the projection directory is writable. |
| Release artifact verification fails                  | Re-run checksum verification in a clean `DIST_DIR`.                                                | Do not deploy. Re-download assets from the GitHub Release and compare image digest evidence before retrying.                                            |
| `kind` smoke fails before Helm install               | Confirm `docker`, `kind`, `kubectl`, and `helm` are on `PATH`.                                     | Run the script with `--dry-run` to inspect the planned cluster, namespace, values file, and manifests.                                                  |
| Upgrade rollout stalls                               | `kubectl -n "$NAMESPACE" describe deploy/"$RELEASE"-pqueue` and inspect new pod events/logs.       | Roll back with `helm rollback`, then compare runtime config changes against the container contract.                                                     |

## Known Production Gaps

- `object_log_sqlite_projection` is not production-ready until the MinIO-backed
  `kind` proof records bucket setup, object-log writes, pod restart/replay, and
  SQLite projection recovery.
- The object-log proof is S3-compatible MinIO evidence only. It does not certify
  AWS S3, GCS S3 interop, IAM policy, provider TLS/certificate configuration,
  or provider-specific conditional-write semantics.
- Release artifacts alone do not prove Kubernetes readiness; every production
  claim needs Helm and `kind` evidence for the release artifact being deployed.
- P1 operator API workflows are not part of a P0/core production claim unless
  the release notes explicitly include operator-enabled evidence.
- Environment operations remain deployment-specific: capacity planning,
  backups, credentials rotation, network policy, TLS, monitoring, and incident
  runbooks are outside the repository-local proof.

## Reference Documents

- [Operator release artifacts](operator-release-artifacts.md)
- [Helm static validation gate](helm-static-validation.md)
- [kind Helm integration harness](kind-helm-integration.md)
- [Container runtime contract](container-runtime-contract.md)
- [Production deployment readiness contract](../helix/04-build/DEPLOYMENT-READINESS.md)
