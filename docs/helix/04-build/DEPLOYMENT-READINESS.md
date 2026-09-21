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
    self_hash: 490871303329604f1034f3c745859b600dbfe2940488a0b4afdf73b4b78f7056
    deps:
      build-implementation-plan: 4ddbeab6da535522d8253e3ce6018c89b901556e2e179453df6de86b3c02363e
      td-postgres-native-reference-mode: 1b657638258f7d3fa15e46b7536d33d766ade1a0948a32598dc5c9ae65b7828b
      td-s3-object-log-sqlite-projection-mode: 56d80c3e6ad5ab54460e300fdf4ddfe535dc75a47b0a2a0e32d0de46c38c7e49
      td-storage-architecture-backend-contracts: b1d17cc3481f52097ea0b2233a4a0e7bfa1512381c0b1fed7b3830fd3f02cc4e
      tp-scale-substantiation: e0ca180cb81c98e7c451341f1ea912bf152ac2c75d422a3b315516fc9f8ee7d3
      tp-verification-acceptance-criteria: 450177278bfc6a0d50fa4c5395dea18fc6dc7738087d88bef7b062ce5fce81ab
    reviewed_at: "2026-07-20T20:03:42Z"
---

# Production Deployment Readiness Contract

## Scope

This document is the production deployment readiness contract for the fireweed
BUILD-001 release line.

### Product storage model (normative)

ADR-024 is the deployment storage law. The public product is one cell: S3
object-log × Turso projection. `ResponseBarrier` has only `AsyncProjection`.
Typed `StorageConfig` (API-005), including `StorageConfig::s3_turso`, is the
composition root for the library and the target for service/Helm configuration.

| Axis | Public values | Responsibility |
|------|---------------|----------------|
| **Log backend** | `s3` | Command append, epoch/fence authority, Class A replay |
| **Projection** | `turso` | Serving projection, rebuildable through `projection_control`, not the command log |

Pair strings from older evidence are not public product SKUs. Other selectors
reject before I/O.

| Log \ Projection | `turso` |
|------------------|---------|
| `s3` | Class A · `AsyncProjection` · **the release cell** |

### Release storage surface (normative)

The release storage surface is that one cell. A release must not ship a second
public cell or a `Strict` barrier. `StorageConfig::s3_turso` sets
`NativeConditionalWrite` and an `AsyncProjectionSpec`. This document does not
claim a 10M-resident or 1000-queue pass.

| Step | What it checks |
|------|----------------|
| Public cell | `StorageConfig` accepts s3 × turso and rejects retired selectors before I/O |
| Server | `Server::validate_for_start` uses the same retirement rule |
| Legacy product-name ban | `bash scripts/ci/assert-no-legacy-storage-product-names.sh` |

Required product CI that claims the full surface sets
`FIREWEED_STORAGE_MATRIX_REQUIRE_FULL=1` and provisions S3 + Postgres fixtures
so skip is not treated as pass (see **Storage matrix fixture requirements**
below and [`scripts/ci/s3-matrix-job-requirements.md`](../../../scripts/ci/s3-matrix-job-requirements.md)).

**Configuration layering:** structured `StorageConfig` fields and Helm
`storage.log.*` / `storage.projection.*` define storage. Environment variables
are a container injection adapter into that model, not the product vocabulary
(see `docs/deployment/container-runtime-contract.md`).

The release contract **must not collapse those axes** into named deployment
modes. A release artifact can claim only the **cells** that its runtime, chart
rendering, and CI evidence actually cover on that revision.

## Current Release Boundary

> **Version source of truth:** the workspace `Cargo.toml` `[workspace.package] version`
> (currently **0.23.3**) is canonical for the current release line. Release tags follow it
> (`v0.23.3`, …). Version-specific docs under `docs/releases/` and `docs/perf/` are
> historical snapshots of the version in their filename and are not statements about the current line.

The public product is the s3 × turso cell (ADR-024). Helm and env adapters
must deserialize into that `StorageConfig`. Legacy spellings (`objectlog`,
`inmemory`, hybrid, and retired log or projection names) are not public
product SKUs: they must fail closed
(`scripts/ci/assert-no-legacy-storage-product-names.sh`).

| Log \ Projection | `turso` |
|------------------|---------|
| `s3` | Class A · `AsyncProjection` |

crates.io and GHCR publication are deferred by the public-preview checklist; no
registry artifact is available until a later release explicitly publishes and
verifies it.

Unsupported axis combinations must fail loudly at process startup with the
requested log/projection pair. They must not be silently mapped onto a synthetic
combined backend name.

Runtime executability is not a production transaction claim. A storage cell may
be smoke-tested after it starts and preserves reopen readback for its durability
class, but production claims require the applicable TP-003 external
transaction-contract evidence and any required TP-002 scale evidence for that
exact log/projection pair.

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
Helm `storage.*` must deserialize to the public cell: S3 object log × Turso,
`AsyncProjection` only (ADR-024). Release readiness requires:

- chart schema and templates that select `s3` and `turso` and reject other
  storage selectors;
- rendered container injection for that cell (`FIREWEED_LOG_BACKEND` /
  `FIREWEED_PROJECTION_BACKEND` and S3 endpoint settings are an adapter, not a
  second product);
- S3 endpoint, bucket, and conditional-write credentials for the object log;
- a local Turso projection path that rebuilds through `projection_control`;
- a live `kind` install smoke for that cell;
- release evidence that the cell satisfies API-001 under fault injection:
  success is durable on the object log, rejection has no committed effect,
  unknown outcomes resolve by `request_id`, and an empty claim is a poll.

Historical Postgres-pair JSONL verifiers are not the public cell and are not a
pass for v0.31.30. This target does not claim a 10M-resident or 1000-queue result.

The `kind` proof is the minimum release-readiness gate. It is not a substitute
for environment-specific capacity planning, credentials, monitoring, backups, or
cloud-provider hardening.

## Required Artifacts

A release must publish:

- container image `ghcr.io/<owner>/fireweed-service:<version>` plus
  `ghcr.io/<owner>/fireweed-service:sha-<commit>`;
- Helm chart package `fireweed-<version>.tgz`;
- binary archives `fireweed-<version>-<target-triple>.tar.gz`;
- `SHA256SUMS`;
- release evidence files `fireweed-service-image.txt`,
  `fireweed-helm-chart.txt`, and deployment proof output.

The binary archive must include the real `fireweed-service` runtime and
`fireweed-verify-ledger`. It must not package placeholder binaries or stale
service names.

## CI Gates

The release CI surface must include:

- **Public-cell gate** (ADR-024): the release surface is s3 × turso with
  `AsyncProjection` only. A multi-cell matrix is not the product. Historical
  invocations of `scripts/ci/storage-matrix-gate.sh` are not this cell's
  qualification record. **Invoked from** `scripts/ci/release-gate.sh`
  (`--skip-helm`, cargo + legacy ban on every release/tag path that runs the
  release gate, including `.github/workflows/release.yml` and
  `scripts/ci/nightly-gate.sh`) and from
  `scripts/ci/deployment-release-gate.sh` (`--skip-cargo`, Helm fixtures on the
  deployment/tag path). Default PR `ci.yml` stays thin (policy:
  `verify-github-actions-policy.sh`) and does **not** run this gate. Required
  jobs that claim this cell provision live S3. The gate fails non-zero when a
  required step fails or when the cell is opened without its fixture. A
  multi-cell mode is not the product;
- Rust quality gates: formatting, clippy, workspace tests, release-gate scripts,
  and strict verification-ledger validation;
- Helm chart lint/render checks for the public s3 × turso cell and the shared
  chart variants under `charts/fireweed-queue/ci/` that still describe it;
- a negative check that `FIREWEED_BACKEND_PROFILE` is absent from rendered Helm
  output;
- live `kind` Helm smoke for the public cell (s3 × turso, ADR-024), including RESP `PING`, `XADD`, `XREADGROUP`, rollout
  restart, and post-restart readback;
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
- each E0/E1 queue's declared positive `progress_bound_ms`, the persisted
  queue-definition value read back from the release candidate, and zero
  accepted-to-claim or discovery-age violations of that declaration;
- TP-003 external transaction-contract evidence for the exact storage
  combinations claimed by the release;
- configured object-log commit-latency bound values and measured latency/cost
  curves when an object-log production claim is made;
- any declared exclusions, including storage combinations that are chart-rendered
  but not yet live runtime claims.

### Tag-gate evidence contract

The default local release gate and the tag workflow have distinct TP-002
responsibilities:

1. The default local `scripts/ci/release-gate.sh` invocation generates a clean
   smoke ledger and requires fresh
   smoke-tier E2 and E3 rows. It then validates the exact E0-E3 authority files
   listed by `target/tp002-release/composite-contract.json`, including
   `target/tp002-release/e3/e3-contract.json`, against the checked-out source
   revision. Evidence producers write the exact-commit outputs only to an external
   run-owned directory; the tag workflow promotes the verified archive into
   `target/tp002-release`. They cannot be checked into the commit whose SHA they bind.
2. The release workflow invokes `scripts/ci/release-gate.sh
   --governed-performance-only`, which runs functional release checks and the
   exact-revision composite verifier without rerunning scaled local smoke
   workloads on a shared GitHub runner. It then verifies
   `target/tp002-release/attestation.json` with
   `--evidence-root target/tp002-release`, the resolved release tag, and `GITHUB_SHA`.
   The tag must resolve to that exact
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
`fireweed-postgres::connect`:

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
(`crates/fireweed-postgres/src/relational.rs:3800`, with `commit_transition_*` tests). The log-replay
`PostgresBackend` (`crates/fireweed-postgres/src/lib.rs`) still inherits the `Unavailable` default
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
  `DiscoveryPort` all stay `Unavailable`; `crates/fireweed-postgres/src/lib.rs:906-917`),
  and sqlite's own C9 parity landed only on `SqliteRelationalBackend`, never on
  the plain sqlite log adapter. Postgres mirrors that split.

**(b) Postgres schema for side records and instance fences**, mirroring
sqlite-relational (`crates/fireweed-sqlite/src/relational.rs:234-245`), applying
this crate's existing postgres-vs-sqlite type convention (`BLOB`→`BYTEA`,
`INTEGER`→`BIGINT`, e.g. `fireweed_request_idempotency.expires_at`):

```sql
CREATE TABLE IF NOT EXISTS fireweed_side_records (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, key BYTEA NOT NULL, payload BYTEA NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, key)
);

CREATE TABLE IF NOT EXISTS fireweed_instance_fences (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, instance_key BYTEA NOT NULL, fence BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, instance_key)
);
```

Same primary-key shape and semantics as sqlite: both tables are point
lookup/upsert by primary key only, opaque `key`/`payload`/`instance_key`/`fence`
bytes, no claimable/eligible/peekable surface. No additional indexes.

**(c) Request-id retained idempotency: reuse the existing
`fireweed_request_idempotency` table** (`crates/fireweed-postgres/src/relational.rs:187-197`);
no new table.

- That table is already keyed `(tenant_id, queue_id, operation, request_id)`, so
  commit-transition idempotency is a new `operation` value (mirroring sqlite's
  `IDEMPOTENCY_OPERATION_COMMIT` constant), not a new table.
- Postgres's existing table lacks sqlite's `command_positions` column. That
  column is not required for the commit-transition read path: sqlite's
  `check_commit_idempotency` (`crates/fireweed-sqlite/src/relational.rs:561-593`)
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

## Object-Log Boundary (`filesystem` and `s3` log backends)

Public product names for the object-log peers are **`filesystem`** (directory
tree / NAS root) and **`s3`** (S3-compatible API). Both share the same
object-log protocol. Transitional chart/runtime spelling
`storage.log.backend=objectlog` with `storage.log.objectLog.store=local|s3`
selects those peers until Helm is isomorphic to `StorageConfig`. In the current
release, the live Kubernetes proof commonly pairs the local/filesystem object
log with a memory projection (`inmemory` legacy spelling).

The object-log release path for a claimed cell must prove:

- the chart selects the intended log backend (`filesystem` or `s3`, or the
  transitional `objectlog` + store mapping) and projection axis;
- container injection renders the corresponding adapter keys (for example
  `FIREWEED_LOG_BACKEND` and projection backend/path or URL keys);
- object-log root (filesystem) or endpoint/bucket (s3) configuration is present
  for the container runtime;
- the deployed service writes through the configured object-log runtime path;
- after a rollout restart, acknowledged state can be read back through RESP
  (Class A recovery: high-water + tail when a durable projection is used);
- the configured `FIREWEED_SEGMENT_MAX_LATENCY_MS` / commit-latency-bound value is
  included in release evidence;
- TP-003 `AC-TXN-*` passes for the claimed log × projection cell; and
- TP-002 E3 reports the latency/cost/recovery curve for that cell when an
  object-log production claim is made.

Provider-specific S3 readiness requires a later acceptance run with a named
provider or provider-compatible endpoint, credentials, conditional-write
semantics, the same transaction-contract matrix, and release evidence separate
from the local filesystem object-log fixture.

## Storage fixture requirements

The public cell needs an S3-compatible endpoint with native conditional write
and a local Turso projection. Missing fixtures fail the job. They are not a
license to open another cell. This section does not claim a 10M or 1000-queue pass.

| Cell | Fixture | Environment |
|------|---------|-------------|
| `s3` × `turso` | S3-compatible endpoint with native conditional write | `FIREWEED_S3_TEST_ENDPOINT` (+ bucket/keys; see below) |

S3-compatible job contract (endpoint, bucket, keys, create-only, MinIO/Garage
notes) is normative in
[`scripts/ci/s3-matrix-job-requirements.md`](../../../scripts/ci/s3-matrix-job-requirements.md).
Suggested exports for a disposable MinIO:

```sh
export FIREWEED_S3_TEST_ENDPOINT="http://127.0.0.1:9000"
export FIREWEED_S3_TEST_BUCKET=fireweed-test
export FIREWEED_S3_TEST_ACCESS_KEY=minioadmin
export FIREWEED_S3_TEST_SECRET_KEY=minioadmin
export FIREWEED_PG_TEST_URL=postgres://fireweed:fireweed@127.0.0.1:5432/fireweed
export FIREWEED_STORAGE_MATRIX_REQUIRE_FULL=1
```

## Verification Commands

Release-readiness verification for the current boundary is the public cell
(ADR-024), not a multi-cell matrix. Historical commands that open other
selectors are not this contract.

```sh
bash scripts/ci/assert-no-legacy-storage-product-names.sh
```

The library and server tests that accept s3 × turso and reject retired
selectors are the cell gate. They are named with the suites that land with
ADR-024. Do not treat an older postgres or filesystem suite as a pass for
this cell.
