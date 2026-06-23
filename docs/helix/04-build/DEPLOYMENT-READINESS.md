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
    self_hash: d280ca50b2019dce5e9ee346744307d88ab09cbb7f674ba11df07f6582a800da
    deps:
      build-implementation-plan: 903a5d1277524c550297beafbbef6a88f3e161b0bf319ab703713733f9b28ad9
      td-postgres-native-reference-mode: 443e433bb2fa0ac55f95cb9ad02d35f8486e5e015967fb69807a3a50b97474c3
      td-s3-object-log-sqlite-projection-mode: d346e72f23f5859de62807f41e81b34409b43814faf95db8de237ff1ede895b7
      td-storage-architecture-backend-contracts: 5980a5612e178fc0828f567f21efaafd9d49cf7e62b2d8655bf7b9ef32e97d8d
      tp-scale-substantiation: 1e6b2b70c2f613ac9999e7e295c2c2845c76b2d69eaed81f949785d2ab5d51a7
      tp-verification-acceptance-criteria: 15f28d510bdac36217eeba3ea37849174111de98af410d6a5c59dd296125e6bf
    reviewed_at: "2026-06-23T01:45:57Z"
---

# Production Deployment Readiness Contract

## Scope

This document is the production deployment readiness contract for the pqueue
BUILD-001 release line. It defines the deployment target, supported backend
profiles, required release artifacts, CI gates, release evidence, and the
S3/object-log certification boundary that later implementation beads must
satisfy before pqueue is described as production-deployable.

The contract is intentionally forward-looking. As of this artifact, the repo has
release binaries and Rust/local smoke tests, but it has no Helm chart and no
Kubernetes manifests. Later deployment-readiness beads must add those artifacts
and prove them against this document rather than treating existing local tests as
production installation proof.

## Production Target

The production deployment target is a Kubernetes installation delivered by Helm.
Release readiness requires a reproducible Helm install in `kind` for each
supported backend profile:

| Profile | Required local production proof |
|---------|---------------------------------|
| `postgres_native` | Helm install into `kind` with Postgres-backed control plane, log, projection, idempotency, leases, and metrics. |
| `object_log_sqlite_projection` | Helm install into `kind` with Postgres control plane, PVC-backed SQLite projection, and S3-compatible object storage exercised through MinIO. |

The `kind` proof is the minimum production-readiness gate. It is not a substitute
for environment-specific capacity planning, credentials, monitoring, backups, or
cloud-provider hardening.

## Supported Backends

Only these backend profiles are in the BUILD-001 production-readiness scope:

- `postgres_native`: the reference correctness backend and Tier-1
  single-deployment envelope.
- `object_log_sqlite_projection`: the fjord object-log plus SQLite projection
  mode, with Postgres as control plane and S3-compatible object storage as the
  durable log/snapshot substrate.
- `sqlite`: the standalone single-file durable SQLite backend (TD-005) — the
  embedded-durable option for hosts that need durability without an object store
  or Postgres. Command log and projection live in one file and commit
  atomically (single transaction, WAL fsync ack boundary); it is single-writer
  (one process owns the file). Selected with `PQUEUE_BACKEND_PROFILE=sqlite` and
  `PQUEUE_SQLITE_DB_PATH`. It is NOT part of the horizontal-scale headline
  evidence (that remains `object_log_sqlite_projection`).

`postgres_native` MAY target either a self-managed Postgres or a **managed
Postgres endpoint** (e.g. Databricks Lakebase). The managed-endpoint case is the
same backend and the same SQL (it is Postgres-wire compatible and supports
`FOR UPDATE SKIP LOCKED` / `ON CONFLICT`); it is a *deployment* variant, not a
new backend profile. Its connection requirements are defined in "Managed
Postgres / Lakebase Endpoint" below.

Other profiles named by TD-001, including Kafka/Redpanda and DynamoDB-shaped
profiles, are design targets only. They are not production-supported by this
readiness contract.

## Non-Goals

- Do not claim production deployment readiness from release binaries alone.
- Do not claim Kubernetes readiness from local Rust smoke tests alone.
- Do not add or require a Kubernetes operator for BUILD-001 readiness.
- Do not claim provider-specific live cloud S3 certification unless a later
  bead adds a concrete provider adapter, credentials-backed acceptance run, and
  release evidence for that provider.
- Do not treat multi-shard `postgres_native` as horizontal-scale headline
  evidence. TP-002 reserves the horizontal scale/cost evidence path for
  `object_log_sqlite_projection`.
- Do not block P0/core production readiness on P1 operator APIs unless a release
  explicitly claims the operator-enabled surface.
- Do not claim provider-specific managed-Postgres (Lakebase) certification from
  the connection layer alone. The TLS connector and credential-provider seam are
  implemented and unit-tested, but a credentialed live-Lakebase acceptance run is
  a later bead (see "Managed Postgres / Lakebase Endpoint").

## Required Artifacts

The following artifacts are required before a production-readiness release can
be cut:

- Helm chart and values for pqueue service/worker deployment.
- Backend profile values for `postgres_native` and
  `object_log_sqlite_projection`.
- `kind` deployment proof for both supported profiles.
- Postgres dependency wiring for the control plane and, in `postgres_native`,
  the data plane.
- S3-compatible object storage wiring for `object_log_sqlite_projection`, using
  MinIO in the local `kind` proof.
- Documented runtime configuration for shard count bounds, object-log segment
  settings, SQLite projection storage, credentials/secrets, telemetry, and
  resource limits.
- Verification ledger outputs for product workflow smoke and release gates.
- Release-gate script output proving TP-002 E0-E3 and TP-003 P0/core gates from
  source-backed evidence.

## CI Gates

The release-readiness CI surface must include these gates before a release is
called production-deployable:

- Existing Rust quality gates from BUILD-001: formatting, clippy, workspace
  tests, backend conformance, product smoke, and release-gate scripts.
- Helm chart lint/render checks for every supported backend values profile.
- `kind` install/upgrade/uninstall smoke for `postgres_native`.
- `kind` install/upgrade/uninstall smoke for `object_log_sqlite_projection`,
  including MinIO-backed S3-compatible object storage.
- Strict verification-ledger validation for both backend profiles.
- A negative check that no release evidence cites pre-existing `target/` files
  instead of source-backed DDx evidence.

## Release Evidence

A production-readiness release must record:

- exact commit SHA and release artifact versions;
- operator artifact coordinates for the published container image, Helm chart
  package, binary archive set, and `SHA256SUMS`, following
  [operator release artifacts](../../deployment/operator-release-artifacts.md);
- Helm chart version and rendered values used for each backend profile;
- checksum verification for release assets and digest verification for the
  container image tag that operators deploy;
- `kind` cluster version, Kubernetes version, and node image;
- Postgres and MinIO image/version inputs used by the local proof;
- commands, exit status, environment variables, profiles, scale, seed, and
  ledger paths for product workflow smoke and release validation;
- backend conformance results for `postgres_native` and
  `object_log_sqlite_projection`;
- TP-002 E0-E3 source-backed evidence references;
- TP-003 P0/core release gate output and `product_validation_tests` ledger;
- any declared exclusions, such as P1 operator APIs not included in a P0/core
  release.

## Managed Postgres / Lakebase Endpoint

`postgres_native` can run against a managed Postgres endpoint such as Databricks
Lakebase. Lakebase is Postgres-wire compatible (Postgres 16/17 on the Neon
architecture); pqueue's data-plane SQL is unchanged. Only the connection setup
differs, and the connection layer (`pqueue-postgres::connect`) owns it:

- **TLS is required.** Lakebase mandates `sslmode=require`. The connector reads
  `sslmode` from the connection string and selects rustls vs `NoTls`
  automatically; the binary MUST be built with the `tls` feature
  (`cargo build -p pqueue-service --features tls`). Without it, a
  `sslmode=require` connection fails fast with a clear "built without `tls`"
  error rather than silently downgrading.
- **Two auth modes**, both supported by the connection layer:
  - *Native password via the pooler* — a static password through Lakebase's
    PgBouncer endpoint (transaction pooling, up to 10k client connections). The
    simplest mode; `StaticPassword` credential provider. pqueue uses no
    pooler-incompatible features (no `LISTEN`/`NOTIFY`, no advisory locks), so
    `FOR UPDATE SKIP LOCKED` / `ON CONFLICT` work through transaction pooling.
  - *OAuth on the direct endpoint* — a short-lived (~60 min) Databricks database
    credential as the password, via the `RefreshingCredentialProvider`. The
    provider re-mints the token before expiry and a fresh token is used per new
    connection (Lakebase enforces expiry only at login, so live connections
    survive). The Databricks-specific minting (CLI/SDK/REST
    `generate-database-credential`) is supplied as the provider's fetcher.
- **Connection string.** Use the `key=value` DSN form for OAuth (the username is
  an email containing `@`, which the URL form parses ambiguously); dbname is
  `databricks_postgres`, port `5432`.

Validation boundary (deferred, mirroring the S3 boundary below): the connector
and credential seam are implemented and unit-tested, but a **credentialed live
acceptance run against a real Lakebase instance** (TLS handshake, token rotation,
`SKIP LOCKED` concurrency, and the claim throughput/latency envelope re-measured
on Lakebase's disaggregated storage) is a later bead. Until that runs, releases
MUST NOT claim Lakebase production certification — only that `postgres_native`
*can* target a managed Postgres endpoint.

## S3 / Object-Log Boundary

`object_log_sqlite_projection` means the fjord object-log plus SQLite projection
backend specified by TD-004. Runtime and Helm configuration for this profile is
concrete, not reserved:

- `PQUEUE_BACKEND_PROFILE=object_log_sqlite_projection`.
- `PQUEUE_POSTGRES_DATABASE_URL`, sourced from Secret `pqueue-postgres`, key
  `database-url`, is required for the Postgres control plane and
  manifest-pointer/fencing boundary.
- `PQUEUE_OBJECT_LOG_ENDPOINT`, `PQUEUE_OBJECT_LOG_BUCKET`,
  `PQUEUE_OBJECT_LOG_REGION`, and `PQUEUE_OBJECT_LOG_SEGMENT_MAX_COMMANDS` are
  rendered from `backend.objectLog.endpoint`, `backend.objectLog.bucket`,
  `backend.objectLog.region`, and `backend.objectLog.segmentMaxCommands`.
- `PQUEUE_OBJECT_LOG_ACCESS_KEY_ID` and
  `PQUEUE_OBJECT_LOG_SECRET_ACCESS_KEY` are sourced from Secret
  `pqueue-object-log`, keys `access-key-id` and `secret-access-key`.
- `PQUEUE_SQLITE_PROJECTION_DIR` is rendered from
  `backend.sqliteProjection.mountPath`; the CI/default path is
  `/var/lib/pqueue/projection`.
- `PQUEUE_SHARD_COUNT_MIN` and `PQUEUE_SHARD_COUNT_MAX` are rendered from
  `backend.shardCount.min` and `backend.shardCount.max`; they bound the
  deployment shard-count contract for the selected backend.
- Production-like Kubernetes proofs must use a PVC for the SQLite projection
  path (`persistence.enabled=true`, default claim name
  `<release-fullname>-sqlite-projection`, configured by
  `persistence.existingClaim`, `persistence.accessModes`, `persistence.size`,
  and `persistence.storageClass`) rather than relying on local disk as a
  durability boundary.
- Object-log segment settings must keep
  `PQUEUE_OBJECT_LOG_SEGMENT_MAX_COMMANDS > 1`; the checked-in CI profile uses
  `1024`.

Existing evidence validates object-log behavior at the abstraction/local fixture
layer: group commit, manifest fencing, replay, object-store capability
rejection/fallback, SQLite projection, product smoke, and release ledger
behavior.

That evidence is sufficient for the BUILD-001 S3-compatible object-log semantics
claim. It is not a provider-specific live cloud S3 deployment certification.
Provider-specific S3 readiness requires a later bead with:

- a named provider or provider-compatible endpoint;
- credentials and secret-handling policy;
- conditional-write/CAS behavior documented for that provider;
- a live acceptance run or explicitly approved emulator boundary;
- release evidence separate from the fjord/local fixture proof.

The completed `object_log_sqlite_projection` kind proof must demonstrate:

- `scripts/ci/kind-helm-test.sh` and `scripts/ci/kind/**` must deploy MinIO or
  an equivalent S3-compatible service, create/configure the
  `pqueue-object-log` bucket, and wait for those dependencies before installing
  pqueue.
- The deployed service smoke must write through the configured object-log
  runtime path, not only check `GET /readyz`.
- The proof must restart or roll out the pqueue pod and verify state can be
  recovered from object-log segments/snapshots plus the SQLite projection path.

The target command is:

```sh
bash scripts/ci/kind-helm-test.sh --backend object_log_sqlite_projection
```

The required proof boundary is:

- Helm renders the object-log runtime environment, Postgres control-plane Secret
  reference, object-store credential Secret reference, and PVC-backed SQLite
  projection mount.
- The pqueue pod reaches `/readyz` only after the runtime validates its required
  object-log configuration, verifies the SQLite projection directory, and probes
  the configured S3-compatible MinIO bucket.
- Release evidence must cover restart/replay for this profile: after a pqueue
  pod restart, acknowledged state is recovered from MinIO object-log
  segments/snapshots plus the SQLite projection path, and readiness returns to
  `ready`.

This proves S3-compatible MinIO readiness for the Kubernetes proof. It does not
claim cloud-provider-specific S3 certification, IAM policy validation,
provider-managed TLS/certificate hardening, provider-specific conditional-write
semantics, or production certification for AWS S3, GCS S3 interop, or any other
named cloud object store.

Required verification commands for this boundary:

```sh
cargo test -p pqueue-objectlog -- --nocapture
cargo test -p pqueue-service --test container_runtime_contract_tests -- --nocapture
bash scripts/ci/helm-gate.sh
bash scripts/ci/kind-helm-test.sh --backend object_log_sqlite_projection
```
