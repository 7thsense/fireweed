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

Runtime and Helm configuration are expressed as **two storage axes** (plus
control plane, composed but not redefined here). The product model is the
orthogonal product of log backend × projection store — not a list of named
deployment profiles. Typed `StorageConfig` (API-005) is the normative
composition root for the library and the target for service/Helm configuration
layering (`orthogonal-storage-matrix-brief`).

| Axis | Public values | Responsibility |
|------|---------------|----------------|
| **Log backend** | `memory`, `sqlite`, `postgres`, `filesystem`, `s3` | Command append, epoch/fence authority, replay when durable |
| **Projection** | `memory`, `sqlite`, `postgres` | Serving, claim selection, validation, apply |

`filesystem` and `s3` are first-class object-log peers (same protocol: segments,
manifest, conditional write / authority, retention). They are not test-only
substitutes for each other. Pair strings such as `objectlog/sqlite` may appear
in historical evidence IDs and transitional runtime wiring; they are **not**
public product SKUs.

Full public matrix (15 cells). Semantics differ by durability class (Class A:
durable log; Class B: memory log — see API-005 and ADR-013), not by a second
architecture:

| Log \ Projection | `memory` | `sqlite` | `postgres` |
|------------------|----------|----------|------------|
| `memory` | Class B | Class B | Class B |
| `sqlite` | Class A | Class A | Class A |
| `postgres` | Class A | Class A | Class A |
| `filesystem` | Class A | Class A | Class A |
| `s3` | Class A | Class A | Class A |

### Release storage surface (normative)

The **release storage surface is this full 15-cell matrix** (typed
`StorageConfig`, API-005 / `orthogonal-storage-matrix-brief`). A release/tag
must not ship with a failed **required** matrix cell. The binding gate is
`scripts/ci/storage-matrix-gate.sh` (Phase 6 of
[`storage-matrix-completion-brief.md`](./storage-matrix-completion-brief.md)):

| Step | Command / artifact |
|------|--------------------|
| 15-cell T0–T2 harness | `cargo test -p fireweed --features memory,sqlite,objectlog,postgres --test storage_matrix_t0_t2` |
| Server Class B + Class A suites | `cargo test -p fireweed-server --features postgres --lib class_b` / `sqlite_log_matrix` / `filesystem_matrix` / `s3_object_log` |
| Legacy product-name ban | `bash scripts/ci/assert-no-legacy-storage-product-names.sh` |
| Helm matrix fixtures | `bash scripts/ci/helm-gate.sh` (all 15 public cells under `charts/fireweed-queue/ci/*-values.yaml`) |

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

The public product is the **5×3 log × projection matrix** (`StorageConfig` +
`open` / `open_async`). Helm and env adapters are isomorphic to those axes
(`storage.log` / `storage.projection`; public log names `memory`, `sqlite`,
`postgres`, `filesystem`, `s3`; public projection names `memory`, `sqlite`,
`postgres`). Legacy spellings (`objectlog`, `inmemory`, hybrid/turso projection
select) are **not** public product SKUs: they must fail closed on public
surfaces (`scripts/ci/assert-no-legacy-storage-product-names.sh`).

| Log \ Projection | `memory` | `sqlite` | `postgres` |
|------------------|----------|----------|------------|
| `memory` | Class B | Class B | Class B |
| `sqlite` | Class A | Class A | Class A |
| `postgres` | Class A | Class A | Class A |
| `filesystem` | Class A | Class A | Class A |
| `s3` | Class A | Class A | Class A |

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
Helm `storage.*` is the deploy document for the structured log × projection
model and must remain isomorphic to typed `StorageConfig` as that surface lands.
Release readiness requires:

- chart schema and templates that expose structured log and projection axes
  (`storage.log.backend` and `storage.projection.backend`, evolving toward the
  five public log values including `filesystem` and `s3` and the three public
  projection values);
- rendered container injection for the selected cell (today:
  `FIREWEED_LOG_BACKEND` / `FIREWEED_PROJECTION_BACKEND` and related path/URL
  keys — adapter only, not the product definition);
- Secret references for Postgres log and projection URLs when those axes choose
  `postgres`;
- object-log root or S3 endpoint/bucket configuration when the log axis is
  `filesystem` or `s3` (legacy chart spelling may still say `objectlog` +
  local/s3 store until the Helm isomorphic cutover);
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
  `ac_txn_6_postgres_storage_pair_parity` with `FIREWEED_PG_TEST_URL` set, so the
  exact `postgres/sqlite` and `postgres/postgres` rows cannot pass by skip. Each
  job deletes both tracked JSONL outputs, reruns the tests, asserts that both
  regenerated files are non-empty, and only then invokes the verifier. Stale
  repository evidence cannot satisfy the live proof step.
- `fireweed-verify-transaction-evidence` consumes the two exact-pair JSONL files
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

- **15-cell storage matrix gate** (`scripts/ci/storage-matrix-gate.sh`): binds
  the full public `StorageConfig` matrix (T0–T2 library harness, server Class B /
  sqlite / filesystem / s3 matrix suites, legacy product-name assert, Helm
  15-cell fixtures). **Invoked from** `scripts/ci/release-gate.sh`
  (`--skip-helm`, cargo + legacy ban on every release/tag path that runs the
  release gate, including `.github/workflows/release.yml` and
  `scripts/ci/nightly-gate.sh`) and from
  `scripts/ci/deployment-release-gate.sh` (`--skip-cargo`, Helm fixtures on the
  deployment/tag path). Default PR `ci.yml` stays thin (policy:
  `verify-github-actions-policy.sh`) and does **not** run this gate. Required
  full-matrix jobs set `FIREWEED_STORAGE_MATRIX_REQUIRE_FULL=1` with live S3 +
  Postgres fixtures; the gate fails non-zero when a required step fails or when
  full-matrix mode is set without fixtures;
- Rust quality gates: formatting, clippy, workspace tests, release-gate scripts,
  and strict verification-ledger validation;
- Helm chart lint/render checks for every storage combination listed in the
  chart CI values (the 15 public cells plus shared multi-replica / lakebase
  variants under `charts/fireweed-queue/ci/`);
- a negative check that `FIREWEED_BACKEND_PROFILE` is absent from rendered Helm
  output;
- live `kind` Helm smokes for deploy-facing cells on the public axes (log ∈
  {memory, sqlite, postgres, filesystem, s3} × projection ∈ {memory, sqlite,
  postgres} as claimed), including RESP `PING`, `XADD`, `XREADGROUP`, rollout
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
   revision. These exact-commit outputs are staged by the evidence producers;
   they cannot be checked into the commit whose SHA they bind.
2. The release workflow invokes `scripts/ci/release-gate.sh
   --governed-performance-only`, which runs functional release checks and the
   exact-revision composite verifier without rerunning scaled local smoke
   workloads on a shared GitHub runner. It then verifies
   `target/tp002-release/attestation.json` with
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

## Storage matrix fixture requirements

Full 15-cell matrix evidence needs external services for axes that cannot be
satisfied with temp dirs alone. Local developer runs may skip those cells
(documented `eprintln!`); **required** release/product jobs must provision
fixtures and set `FIREWEED_STORAGE_MATRIX_REQUIRE_FULL=1` so missing fixtures
fail the gate rather than silently reducing coverage.

| Axis / cells | Fixture | Environment |
|--------------|---------|-------------|
| Local Class A/B (`memory`/`sqlite`/`filesystem` log × `memory`/`sqlite` projection) | Temp dirs only | none |
| Any `postgres` log or projection cell | Live Postgres | `FIREWEED_PG_TEST_URL`; build with `--features postgres` |
| `s3` × {`memory`,`sqlite`} | S3-compatible endpoint with native create-only | `FIREWEED_S3_TEST_ENDPOINT` (+ bucket/keys; see below) |
| `s3` × `postgres` | S3 **and** Postgres | both of the above |

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

Release-readiness verification for the current boundary is:

```sh
# Full public 15-cell StorageConfig matrix (library T0–T2, server suites,
# legacy name ban, Helm fixtures). Fails non-zero on any step failure.
# For required release CI, export FIREWEED_STORAGE_MATRIX_REQUIRE_FULL=1
# and the S3/PG fixtures above first.
bash scripts/ci/storage-matrix-gate.sh

bash scripts/ci/release-gate.sh
cargo run -p fireweed-release --bin fireweed-verify-transaction-evidence -- \
  --evidence docs/perf/evidence/tp003-ac-txn-matrix-postgres-storage-pairs.jsonl \
  --evidence docs/perf/evidence/tp003-ac-txn-parity-postgres-storage-pairs.jsonl
bash scripts/ci/helm-gate.sh
bash scripts/ci/kind-helm-test.sh --log-backend filesystem --projection-backend memory
bash scripts/ci/deployment-release-gate.sh
```

Focused matrix commands (also invoked by the gate):

```sh
cargo test -p fireweed --features memory,sqlite,objectlog,postgres --test storage_matrix_t0_t2
cargo test -p fireweed-server --features postgres --lib class_b
cargo test -p fireweed-server --features postgres --lib sqlite_log_matrix
cargo test -p fireweed-server --features postgres --lib filesystem_matrix
cargo test -p fireweed-server --features postgres --lib s3_object_log
bash scripts/ci/assert-no-legacy-storage-product-names.sh
bash scripts/ci/helm-gate.sh
```
