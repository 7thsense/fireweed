---
ddx:
  id: production-deployment-readiness
  depends_on:
    - build-implementation-plan
    - td-storage-architecture-backend-contracts
    - td-postgres-native-reference-mode
    - td-s3-object-log-sqlite-projection-mode
    - tp-scale-substantiation
    - tp-verification-acceptance-criteria
  review:
    self_hash: f6f9e85aec0b5a2b9db0fbc511fc22189acf93e13b13fbf8969d348838f2bd76
    deps:
      build-implementation-plan: 3234935e1274435e86b9396e2b107230e0142fc12c46c0603b01f0a9965a9fc8
      td-postgres-native-reference-mode: ea91286ed9f810497a7da0dd05f962e0bfe2cb001acb682f3d7b10e1e69cdc64
      td-s3-object-log-sqlite-projection-mode: fde8c520a39579fd2c2e771a3f251d09714bb370db6e2eaf040c2d84e9e7dc0d
      td-storage-architecture-backend-contracts: a0053226d680acddfc3b606ec106c47ffb09167374940dc8282607e46b8df96e
      tp-scale-substantiation: ed173bd7adce26c78059c7d347ecb31bfbea8a7e8f679b11f3d9761ddb4fb3d3
      tp-verification-acceptance-criteria: cda220585cc9e5cf4b660a323298baa0550451a1ee9482ecb9de93c02b8e723e
    reviewed_at: "2026-06-25T04:21:18Z"
---

# Production Deployment Readiness Contract

## Scope

This document is the production deployment readiness contract for the pqueue
BUILD-001 release line. Runtime and Helm configuration are expressed as two
storage axes:

- log backend: `objectlog` or `postgres`
- projection backend: `inmemory`, `sqlite`, or `postgres`

The release contract must not collapse those axes into named deployment modes. A
release artifact can claim only the combinations that its runtime, chart
rendering, and CI evidence actually cover.

## Current Release Boundary

The v0.2.x release packaging ships the `pqueue-service` RESP binary, container
image, Helm chart, binary archive, checksums, and release evidence. The service
runtime currently wires these executable combinations:

| Log backend | Projection backend | Runtime status |
|-------------|--------------------|----------------|
| `objectlog` | `inmemory` | Live container and Helm smoke path. |
| `sqlite` | `inmemory` | Local single-process durable log path. |
| `memory` | `inmemory` | Local development path, not a production claim. |

The Helm chart renders storage-axis values for these release-facing
combinations:

| Log backend | Projection backend | Gate |
|-------------|--------------------|------|
| `objectlog` | `inmemory` | Helm render/lint and live `kind` smoke. |
| `objectlog` | `sqlite` | Helm render/lint only until the service wires the SQLite projection adapter. |
| `postgres` | `inmemory` | Helm render/lint only until the service wires the Postgres log adapter. |
| `postgres` | `sqlite` | Helm render/lint only until both adapters are wired. |
| `postgres` | `postgres` | Helm render/lint only until both adapters are wired. |

Unsupported runtime combinations must fail loudly at process startup with the
requested log/projection pair. They must not be silently mapped onto a synthetic
combined backend name.

## Production Target

The production deployment target is a Kubernetes installation delivered by Helm.
Release readiness requires:

- chart schema and templates that expose `storage.log.backend` and
  `storage.projection.backend`;
- rendered environment variables `PQUEUE_LOG_BACKEND` and
  `PQUEUE_PROJECTION_BACKEND`;
- Secret references for Postgres log and projection URLs when those axes choose
  `postgres`;
- object-log storage path/configuration when the log axis chooses `objectlog`;
- SQLite projection path and persistence when the projection axis chooses
  `sqlite`;
- a live `kind` install smoke for every combination that the service runtime
  claims as executable in Kubernetes.

The `kind` proof is the minimum release-readiness gate. It is not a substitute
for environment-specific capacity planning, credentials, monitoring, backups, or
cloud-provider hardening.

## Required Artifacts

A release must publish:

- container image `ghcr.io/<owner>/pqueue-service:<version>` plus
  `ghcr.io/<owner>/pqueue-service:sha-<commit>`;
- Helm chart package `pqueue-<version>.tgz`;
- binary archives `pqueue-<version>-<target-triple>.tar.gz`;
- `SHA256SUMS`;
- release evidence files `pqueue-service-image.txt`,
  `pqueue-helm-chart.txt`, and deployment proof output.

The binary archive must include the real `pqueue-service` runtime and
`pqueue-verify-ledger`. It must not package placeholder binaries or stale
service names.

## CI Gates

The release CI surface must include:

- Rust quality gates: formatting, clippy, workspace tests, release-gate scripts,
  and strict verification-ledger validation;
- Helm chart lint/render checks for every storage combination listed in the
  chart CI values;
- a negative check that `PQUEUE_BACKEND_PROFILE` is absent from rendered Helm
  output;
- a live `kind` Helm smoke for `objectlog` + `inmemory`, including RESP `PING`,
  `XADD`, `XREADGROUP`, rollout restart, and post-restart readback;
- release artifact verification before publishing.

As more runtime adapters are wired, the live `kind` matrix must grow by storage
combination. Do not introduce single-name shortcuts for that matrix.

## Release Evidence

Release evidence must record:

- exact commit SHA and release artifact versions;
- container image tag, digest, and immutable digest coordinate;
- Helm chart version and rendered storage values;
- checksum verification for release assets and digest verification for the
  container image tag;
- `kind` cluster version, Kubernetes version, and node image for live smoke
  runs;
- command, exit status, environment variables, storage combination, scale, seed,
  and ledger paths for source and deployment validation;
- TP-002 E0-E3 source-backed evidence references;
- any declared exclusions, including storage combinations that are chart-rendered
  but not yet live runtime claims.

## Managed Postgres Boundary

The storage axes reserve Postgres for both the log and projection sides:

- `storage.log.backend=postgres`
- `storage.projection.backend=postgres`

Postgres can target self-managed Postgres or a managed Postgres endpoint such as
Databricks Lakebase when the runtime adapter is wired. Lakebase is
Postgres-wire compatible. Connection setup belongs to
`pqueue-postgres::connect`:

- TLS is required for Lakebase. The current connector parses `sslmode` but
  rejects `sslmode=require` before attempting a `NoTls` connection; the
  TLS-capable connector/runtime work is tracked by `pqueue-13924b0e`.
- Native password through a pooler and OAuth-generated database credentials are
  connection-layer concerns, not new storage combinations.
- A credentialed live acceptance run against a real managed endpoint is required
  before any release claims provider-specific managed-Postgres certification.

Until the TLS/runtime work and live run exist, releases may claim only
chart/render support for the Lakebase Postgres storage axes unless the service
runtime and `kind` gate prove the combination.

## Object-Log Boundary

`storage.log.backend=objectlog` selects the fjord object-log runtime path. In
the current release, the live Kubernetes proof pairs it with
`storage.projection.backend=inmemory`.

The object-log release path must prove:

- the chart renders `PQUEUE_LOG_BACKEND=objectlog`;
- the chart renders `PQUEUE_PROJECTION_BACKEND=<projection backend>`;
- object-log root/configuration is present for the container runtime;
- the deployed service writes through the configured object-log runtime path;
- after a rollout restart, acknowledged state can be read back through RESP.

Provider-specific S3 readiness requires a later acceptance run with a named
provider or provider-compatible endpoint, credentials, conditional-write
semantics, and release evidence separate from the local object-log fixture.

## Verification Commands

Release-readiness verification for the current boundary is:

```sh
bash scripts/ci/release-gate.sh --require-tp002-evidence E0,E1,E2,E3
bash scripts/ci/helm-gate.sh
bash scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend inmemory
bash scripts/ci/deployment-release-gate.sh
```
