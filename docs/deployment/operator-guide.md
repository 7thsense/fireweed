# pqueue Operator Deployment Guide

<!-- markdownlint-disable MD013 -->

This guide covers the v0.2.5 release packaging. The Helm chart configures
storage as separate log and projection axes.

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
| Projection backend | `storage.projection.backend` | `inmemory`, `sqlite`, `hybrid`, `postgres` |

The current `pqueue-server` release smoke paths include `objectlog/inmemory`,
`objectlog/sqlite`, and `objectlog/hybrid`. Unsupported chart combinations
render statically and fail loudly at server startup until their composition
roots are wired.

`objectlog/hybrid` is the SQLite-first plus hot-memory object-log profile. It
uses `PQUEUE_SQLITE_PROJECTION_PATH`, treats the object log as the
authority, hydrates memory from SQLite `ProjectionImage` before serving from a
SQLite high-water, and fails closed on memory-apply poisoning. Non-objectlog
hybrid pairings are unsupported unless a future release explicitly documents and
tests them.

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
  --set storage.projection.backend=inmemory
```

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
