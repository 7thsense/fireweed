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
    self_hash: 3abfceeef2a4198a61529895961f7d0792894ce189182fe96004ff0ddc3e3be7
    deps:
      build-implementation-plan: 55528ea72af327659536b155d61bda5984387104871c7e38707173f7aad5c542
      td-postgres-native-reference-mode: 1b657638258f7d3fa15e46b7536d33d766ade1a0948a32598dc5c9ae65b7828b
      td-s3-object-log-sqlite-projection-mode: 56d80c3e6ad5ab54460e300fdf4ddfe535dc75a47b0a2a0e32d0de46c38c7e49
      td-storage-architecture-backend-contracts: b1d17cc3481f52097ea0b2233a4a0e7bfa1512381c0b1fed7b3830fd3f02cc4e
      tp-scale-substantiation: e0ca180cb81c98e7c451341f1ea912bf152ac2c75d422a3b315516fc9f8ee7d3
      tp-verification-acceptance-criteria: fa0121456931158f03003305b8251bc08dfe43f898051472956df479b2889513
    reviewed_at: "2026-07-20T19:58:48Z"
---

# Production Deployment Readiness Contract

## Scope

This document is the production deployment readiness contract for the pqueue
BUILD-001 release line. Runtime and Helm configuration are expressed as two
storage axes:

- log backend: `objectlog` or `postgres` (plus the local `sqlite` and dev-only `memory` log axes)
- projection backend: `inmemory`, `sqlite`, `turso`, `hybrid`, `hybrid-strict`, `hybrid-async`, or `postgres`

The release contract must not collapse those axes into named deployment modes. A
release artifact can claim only the combinations that its runtime, chart
rendering, and CI evidence actually cover.

## Current Release Boundary

> **Version source of truth:** the workspace `Cargo.toml` `[workspace.package] version`
> (currently **0.19.4**) is canonical for the current release line. Release tags follow it
> (`v0.19.4`, …). Version-specific docs under `docs/releases/` and `docs/perf/` are
> historical snapshots of the version in their filename and are not statements about the current line.

The v0.19.4 release packaging ships the `pqueue-service` RESP binary, container
image, Helm chart, binary archive, checksums, and release evidence. The service
runtime (`crates/pqueue-server/src/env_config.rs`) currently wires these
executable combinations:

| Log backend | Projection backend | Runtime status |
|-------------|--------------------|----------------|
| `objectlog` | `inmemory` | Live container and Helm smoke path (in the CI kind matrix). |
| `objectlog` | `sqlite` | Wired (durable SQLite projection over the object log; in the CI kind matrix). |
| `objectlog` | `turso` | Wired behind the `turso-projection` feature; local-file recovery and authoritative object-log rebuild are covered by the focused Turso lane. |
| `objectlog` | `hybrid` | Wired (TD-004 hot-memory-over-durable-SQLite; shipped v0.6.0). |
| `objectlog` | `hybrid-strict` | Experimental runtime path (SQLite durable before memory apply). Env/direct-config only; intentionally not chart-selectable or production-supported. |
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
| `objectlog` | `turso` | Feature-gated runtime and focused chart render fixture; intentionally outside the broad live-`kind` storage matrix. |
| `objectlog` | `hybrid` | Runtime wired, Helm render/lint (`helm-gate.sh` `objectlog-hybrid`), and live `kind` smoke in the CI matrix (`scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend hybrid`). |
| `objectlog` | `hybrid-async` | Runtime wired, Helm render/lint with exact debt/backpressure-variable assertions, and live `kind` smoke in the CI matrix (`scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend hybrid-async`). |
| `objectlog` | `hybrid-strict` | Experimental env/direct-config-only runtime path; **not** chart-selectable or production-supported (projection enum omits it). |
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

### `objectlog/hybrid-strict` public-support decision

**Decision (2026-07-19): DEFER public deployment support.**
`objectlog/hybrid-strict` remains an experimental env/direct-config-only runtime
path. It is excluded from the Helm schema, chart templates, live-`kind` matrix,
deployment release matrix, release/tag gates, operator support contract, and
public support claims.

Runtime wiring is authorization to exercise and improve the profile; it is not
authorization to advertise support. The current evidence proves only that
`PQUEUE_LOG_BACKEND=objectlog` plus
`PQUEUE_PROJECTION_BACKEND=hybrid-strict` selects the strict runtime with its
SQLite path and that non-object-log pairings fail closed. The chart still omits
`hybrid-strict` from `charts/pqueue/values.schema.json`; the PVC, ConfigMap, and
Deployment templates do not include it; and the Helm, live-`kind`, deployment,
and release/tag matrices do not exercise it.

Revisit the support decision only after all of the following are green on one
release candidate revision:

1. fresh governed TP-003 evidence covers AC-HYB-1 through AC-HYB-6, including
   portable under-load comparative performance and exact 100k/10M recovery;
   wall-clock results may describe a deployment's capacity but cannot be a
   quiet-host or absolute host-speed support gate;
2. the chart schema, templates, SQLite PVC/path handling, and operator controls
   expose the exact `objectlog/hybrid-strict` profile and fail closed for invalid
   pairings;
3. a live-`kind` install proves create/write/read, rollout restart, and exact
   post-restart readback for the profile;
4. release and tag gates bind the chart, live-`kind`, TP-003, and published
   evidence to the same source revision; and
5. manifest fencing plus the applicable TP-002 E2/E3 correctness, progress,
   recovery, cost, and bounded-resource prerequisites are closed.

Until then, documentation and Helm tests preserve the exclusion with a named
negative schema assertion; implementation work does not imply support.
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
- CI and tag releases provision a live Postgres service and run
  `ac_txn_contract_matrix_postgres_storage_pairs` plus
  `ac_txn_6_postgres_storage_pair_parity` with `PQUEUE_PG_TEST_URL` set, so the
  exact `postgres/sqlite` and `postgres/postgres` rows cannot pass by skip. Each
  job deletes both tracked JSONL outputs before the tests, requires both
  regenerated files to be non-empty, and only then invokes the verifier; stale
  repository evidence cannot satisfy the live proof step.
- `pqueue-verify-transaction-evidence` consumes the two exact-pair JSONL files
  and requires AC-TXN-1/2/3/6 for both profiles. Missing, duplicate, failed,
  partial, coverage-GAP, and all whole-row N/A results fail closed. Capability
  limits may be recorded only as assertion context inside a passing AC row. The
  standalone smoke release gate validates the repository-held snapshot;
  freshness is an additional invariant enforced by the CI and tag-release steps
  above.

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
  `objectlog` + `hybrid`, and `objectlog` + `hybrid-async`, including RESP
  `PING`, `XADD`, `XREADGROUP`, rollout restart, and post-restart readback;
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
`objectlog-hybrid`, `objectlog-hybrid-async`, `postgres-inmemory`,
`postgres-sqlite`, and `postgres-postgres`; `objectlog/hybrid-strict` remains
env-only (see the table above).

Current CI state (v0.16.0 release candidate): local release gates must pass at the
exact release commit, and pushed-main CI must be green at that same commit before
the tag is created. All GitHub
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

### Tag-gate evidence contract

The tag workflow has two independent TP-002 lanes, and both are mandatory:

1. `scripts/ci/release-gate.sh` generates a clean smoke ledger and requires fresh
   smoke-tier E2 and E3 rows. It then validates the exact E0-E3 authority files
   listed by `target/tp002-release/composite-contract.json`, including
   `target/tp002-release/e3/e3-contract.json`, against the checked-out source
   revision. These exact-commit outputs are staged by the evidence producers;
   they cannot be checked into the commit whose SHA they bind.
2. The release workflow verifies `target/tp002-release/attestation.json` with
   the resolved release tag and `GITHUB_SHA`. The tag must resolve to that exact
   checked-out commit, and every attested evidence/input digest must match.

The governed lane never scans `docs/perf/evidence` or the staging directory.
TP-003 transaction JSONL may coexist there but is not a TP-002 `LedgerRow`; an unlisted E0-E3 row cannot
replace a missing manifest authority. Missing, duplicate, malformed, smoke-tier,
wrong-profile, false-bar, revision-mismatched, or unattested composite authorities fail
closed. The E3 contract additionally requires explicit portable-gate markers and
rejects quiet-host or absolute machine-speed release criteria; wall-clock
measurements are capacity observations only. A configured `progress_bound_ms`
remains a queue liveness contract: eligible work must make logical progress
within that configured bound under load. It is not a benchmark of host speed,
and slow absolute throughput or latency alone cannot fail a release.

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
bash scripts/ci/release-gate.sh
cargo run -p pqueue-release --bin pqueue-verify-transaction-evidence -- \
  --evidence docs/perf/evidence/tp003-ac-txn-matrix-postgres-storage-pairs.jsonl \
  --evidence docs/perf/evidence/tp003-ac-txn-parity-postgres-storage-pairs.jsonl
bash scripts/ci/helm-gate.sh
bash scripts/ci/kind-helm-test.sh --log-backend objectlog --projection-backend inmemory
bash scripts/ci/deployment-release-gate.sh
```
