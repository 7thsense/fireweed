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
    self_hash: 910cced0926a63434460722e40240dd99e45570608c2e866c7f7e97a9c85c821
    deps:
      build-implementation-plan: 55528ea72af327659536b155d61bda5984387104871c7e38707173f7aad5c542
      td-postgres-native-reference-mode: b58232f3c0b56c50bc1e5f01e13afc71ed1c333987498bbabc88c322f80b36e0
      td-s3-object-log-sqlite-projection-mode: f77b249de99163d5b3031b174f2ff1a7833b45d1a68646a1a9da206e847a5fd0
      td-storage-architecture-backend-contracts: 430d0dc1f83fa62aeb19948efd2a84f5c31df7d15195e51c8296c93c711919f5
      tp-scale-substantiation: eb42f16b7dc36a9316cdafa06921e2d089246ed79f6155212022c533acfc4ae9
      tp-verification-acceptance-criteria: ef7d361e7736e99e509f94bbc0b0d435eef558851bc6272527781efa91e5ec08
    reviewed_at: "2026-07-16T17:24:26Z"
---

# Production Deployment Readiness Contract

## Scope

This document is the production deployment readiness contract for the pqueue
BUILD-001 release line. Runtime and Helm configuration are expressed as two
storage axes:

- log backend: `objectlog` or `postgres` (plus the local `sqlite` and dev-only `memory` log axes)
- projection backend: `inmemory`, `sqlite`, `hybrid`, `hybrid-strict`, `hybrid-async`, or `postgres`

The release contract must not collapse those axes into named deployment modes. A
release artifact can claim only the combinations that its runtime, chart
rendering, and CI evidence actually cover.

## Current Release Boundary

> **Version source of truth:** the workspace `Cargo.toml` `[workspace.package] version`
> (currently **0.15.1**) is canonical for the current release line. Release tags follow it
> (`v0.15.1`, …). Version-specific docs under `docs/releases/` and `docs/perf/` are
> historical snapshots of the version in their filename and are not statements about the current line.

The v0.15.x release packaging ships the `pqueue-service` RESP binary, container
image, Helm chart, binary archive, checksums, and release evidence. The service
runtime (`crates/pqueue-server/src/env_config.rs`) currently wires these
executable combinations:

| Log backend | Projection backend | Runtime status |
|-------------|--------------------|----------------|
| `objectlog` | `inmemory` | Live container and Helm smoke path (in the CI kind matrix). |
| `objectlog` | `sqlite` | Wired (durable SQLite projection over the object log; in the CI kind matrix). |
| `objectlog` | `hybrid` | Wired (TD-004 hot-memory-over-durable-SQLite; shipped v0.6.0). |
| `objectlog` | `hybrid-strict` | Wired (SQLite durable before memory apply). Env-only: not yet chart-selectable (the chart schema's projection enum omits `hybrid-strict`). |
| `objectlog` | `hybrid-async` | Wired (deferred async SQLite checkpoint with `PQUEUE_HYBRID_ASYNC_*` debt/backpressure thresholds). |
| `postgres` | `inmemory` | Wired behind the `postgres` cargo feature (in the CI kind matrix). |
| `postgres` | `sqlite` | Wired behind the `postgres` cargo feature (in the CI kind matrix). |
| `postgres` | `postgres` | Wired behind the `postgres` cargo feature (in the CI kind matrix). |
| `sqlite` | `inmemory` | Local single-process durable log path. |
| `memory` | `inmemory` | Local development path, not a production claim. |

The Helm chart renders storage-axis values for these release-facing
combinations:

| Log backend | Projection backend | Gate |
|-------------|--------------------|------|
| `objectlog` | `inmemory` | Helm render/lint and live `kind` smoke (CI matrix). |
| `objectlog` | `sqlite` | Runtime wired, Helm render/lint, and live `kind` smoke in the CI matrix (`scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend sqlite`). |
| `objectlog` | `hybrid` | Runtime wired, Helm render/lint (`helm-gate.sh` `objectlog-hybrid`), and live `kind` smoke in the CI matrix (`scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend hybrid`). |
| `objectlog` | `hybrid-async` | Runtime wired; chart-schema-selectable with a CI values file (`charts/pqueue/ci/objectlog-hybrid-async-values.yaml`), but not yet in the `helm-gate.sh` static-combination list or the live-`kind` matrix. |
| `objectlog` | `hybrid-strict` | Runtime wired via env only; **not** chart-selectable (projection enum omits it). |
| `postgres` | `inmemory` | Postgres log adapter is wired (behind the `postgres` cargo feature via `PostgresNativeBackend`); live `kind` smoke passes in the CI matrix (`scripts/ci/kind-helm-test.sh --log-backend postgres --projection-backend inmemory`). |
| `postgres` | `sqlite` | Adapters wired; live `kind` smoke passes in the CI matrix (`scripts/ci/kind-helm-test.sh --log-backend postgres --projection-backend sqlite`). Exact-pair TP-003 AC-TXN-1/2/3/6 evidence passes in `tp003-ac-txn-{matrix,parity}-postgres-storage-pairs.jsonl`. |
| `postgres` | `postgres` | Adapters wired; live `kind` smoke passes in the CI matrix (`scripts/ci/kind-helm-test.sh --log-backend postgres --projection-backend postgres`). Exact-pair TP-003 AC-TXN-1/2/3/6 evidence passes in `tp003-ac-txn-{matrix,parity}-postgres-storage-pairs.jsonl`; AC-TXN-3 records `commit_transition` capability-N/A because the shipped two-connection composition is eventual-apply, while proving push request-id replay at every applicable cut. |

Unsupported runtime combinations must fail loudly at process startup with the
requested log/projection pair. They must not be silently mapped onto a synthetic
combined backend name.

Runtime status is not a production transaction claim. A storage combination may
be executable for smoke tests after it starts and preserves restart readback, but
it may be production-claimed only after the TP-003 external transaction-contract
matrix and the applicable TP-002 scale/latency evidence are green for that exact
log/projection pair.

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
- release evidence that every production-claimed storage combination satisfies
  API-001's external transaction contract under fault injection: success is
  durable and visible, rejection has no committed effect, and unknown outcomes
  resolve exactly once by `request_id`.

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
- live `kind` Helm smokes for `objectlog` + `inmemory`, `objectlog` + `sqlite`,
  and `objectlog` + `hybrid`, including RESP `PING`, `XADD`, `XREADGROUP`,
  rollout restart, and post-restart readback;
- the TP-003 `AC-TXN-*` transaction-contract matrix for every production-claimed
  storage combination, including object-log crash points around segment write,
  manifest commit, projection apply, response delivery, snapshot, and owner
  reassignment;
- the TP-002 E3 latency/cost matrix for object-log production claims, including
  the configured commit-latency-bound sweep and object/log request-cost curve;
- release artifact verification before publishing.

As more runtime adapters are wired, the live `kind` matrix must grow by storage
combination. Do not introduce single-name shortcuts for that matrix. The current
CI live-`kind` matrix covers `objectlog-inmemory`, `objectlog-sqlite`,
`objectlog-hybrid`, `postgres-inmemory`, `postgres-sqlite`, and
`postgres-postgres`; `objectlog/hybrid-async` remains static-render-only and
`objectlog/hybrid-strict` remains env-only (see the table above).

Current CI state (v0.15.1): the `ci` workflow is green on `main`; all GitHub
Actions are on their current (Node 24) action majors (`actions/checkout@v5`,
`azure/setup-helm@v5`, `azure/setup-kubectl@v5`, `docker/build-push-action@v7`,
`docker/login-action@v4`, `docker/setup-buildx-action@v4`); and the embedded
fjord broker dependency is the public `github.com/7thsense/fjord` repository
pinned by tag, so CI checkout/build requires no private-repo git credentials.

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
- TP-003 external transaction-contract evidence for the exact storage
  combinations claimed by the release;
- configured object-log commit-latency bound values and measured latency/cost
  curves when an object-log production claim is made;
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

- TLS is required for Lakebase. The connector supports native TLS behind the
  `tls` Cargo feature. The stock release binary is built without optional
  features and therefore rejects `sslmode=require` rather than downgrading to
  plaintext.
- Native password through a pooler and OAuth-generated database credentials are
  connection-layer concerns, not new storage combinations.
- A credentialed live acceptance run against a real managed endpoint is required
  before any release claims provider-specific managed-Postgres certification.

The stock release supports plaintext Postgres only. TLS-capable Postgres is
available to source or custom builds that enable the `tls` feature. Until a
credentialed managed-endpoint run exists, neither build may claim
provider-specific Lakebase certification.

## Postgres Commit-Transition Parity Scope

This section settles the scope for the Snorri authoritative vectorized claimed-work
commit boundary (`CommitTransitionPort`, epic pqueue-2201fd37 — **CLOSED**) on the postgres
storage axis. **Status (2026-07): `PostgresRelationalBackend` now IMPLEMENTS `CommitTransitionPort`**
(`crates/pqueue-postgres/src/relational.rs:3800`, with `commit_transition_*` tests). The log-replay
`PostgresBackend` (`crates/pqueue-postgres/src/lib.rs`) still inherits the `Unavailable` default
(commit-transition is a relational-family capability). The rebuildable-from-log migration bead
`pqueue-3c5aa2e0` is closed.

**(a) Keep the unified backend distinct from the shipped two-axis composition.**

- `PostgresRelationalBackend` implements both storage axes as one unified store
  (mirroring `SqliteRelationalBackend`) and carries the atomic
  `CommitTransitionPort` boundary.
- The shipped `storage.log.backend=postgres` +
  `storage.projection.backend=postgres` composition is not that unified
  backend. The server opens an independent `PostgresLog` connection and
  `PostgresRelational` projection connection, then composes them through
  `ComposedBackend`. It is therefore an eventual-apply pair and correctly
  returns `Unavailable` for `commit_transition`.
- Exact-pair AC-TXN-3 evidence does not turn that unavailable operation into a
  success claim: it records a principled capability-N/A for `commit_transition`
  while proving request-id-bearing pushes at the before-append,
  append-before-apply, apply-before-response, and after-response cuts. A future
  Snorri claim requiring the atomic vectorized commit boundary must explicitly
  wire the unified backend; the storage-axis names alone do not imply it.
- `PostgresBackend` (log-replay) is out of scope. It already refuses every
  relational-only feature at the port default (`SetGatesPort`, `ReschedulePort`,
  `DiscoveryPort` all stay `Unavailable`; `crates/pqueue-postgres/src/lib.rs:906-917`),
  and sqlite's own C9 parity landed only on `SqliteRelationalBackend`, never on
  the plain sqlite log adapter. Postgres mirrors that split.

**(b) Postgres schema for side records and instance fences**, mirroring
sqlite-relational (`crates/pqueue-sqlite/src/relational.rs:234-245`), applying
this crate's existing postgres-vs-sqlite type convention (`BLOB`→`BYTEA`,
`INTEGER`→`BIGINT`, e.g. `pqueue_request_idempotency.expires_at`):

```sql
CREATE TABLE IF NOT EXISTS pqueue_side_records (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, key BYTEA NOT NULL, payload BYTEA NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, key)
);

CREATE TABLE IF NOT EXISTS pqueue_instance_fences (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, instance_key BYTEA NOT NULL, fence BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, instance_key)
);
```

Same primary-key shape and semantics as sqlite: both tables are point
lookup/upsert by primary key only, opaque `key`/`payload`/`instance_key`/`fence`
bytes, no claimable/eligible/peekable surface. No additional indexes.

**(c) Request-id retained idempotency: reuse the existing
`pqueue_request_idempotency` table** (`crates/pqueue-postgres/src/relational.rs:187-197`);
no new table.

- That table is already keyed `(tenant_id, queue_id, operation, request_id)`, so
  commit-transition idempotency is a new `operation` value (mirroring sqlite's
  `IDEMPOTENCY_OPERATION_COMMIT` constant), not a new table.
- Postgres's existing table lacks sqlite's `command_positions` column. That
  column is not required for the commit-transition read path: sqlite's
  `check_commit_idempotency` (`crates/pqueue-sqlite/src/relational.rs:561-593`)
  decodes the retained record from `response_payload` alone. Adding
  `command_positions` (or an equivalent) to the postgres table is deferred to
  whichever later change wires `RecoveryReadPort`'s authoritative recovery reads
  for postgres, if that read needs more than `response_payload` provides.

Update (2026-07): the `commit_transition` implementation for
`PostgresRelationalBackend` has landed (`relational.rs:3800`) and epic
`pqueue-2201fd37` is closed. Exact-pair external transaction evidence for
`postgres/sqlite` and `postgres/postgres` is recorded in
`docs/perf/evidence/tp003-ac-txn-matrix-postgres-storage-pairs.jsonl` and
`docs/perf/evidence/tp003-ac-txn-parity-postgres-storage-pairs.jsonl`; future
unified-backend server wiring, `RecoveryReadPort`, or delayed-timer refinements
require separately scoped work.

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
- the configured `PQUEUE_SEGMENT_MAX_LATENCY_MS` / commit-latency-bound value is
  included in release evidence;
- TP-003 `AC-TXN-*` passes for the claimed projection backend; and
- TP-002 E3 reports the latency/cost/recovery curve for that projection backend.

Provider-specific S3 readiness requires a later acceptance run with a named
provider or provider-compatible endpoint, credentials, conditional-write
semantics, the same transaction-contract matrix, and release evidence separate
from the local object-log fixture.

## Verification Commands

Release-readiness verification for the current boundary is:

```sh
bash scripts/ci/release-gate.sh --require-tp002-evidence E0,E1,E2,E3
bash scripts/ci/helm-gate.sh
bash scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend inmemory
bash scripts/ci/deployment-release-gate.sh
```
