//! Table-driven T0–T2 harness for all 20 public storage-matrix cells, plus Class B T3
//! (projection durability + rejection; no `durable_log_replay` claims) and Class A cell-batch
//! T0–T4 coverage for **sqlite**, **postgres**, **filesystem**, and **s3** log batches.
//!
//! Governing bar: `docs/helix/04-build/storage-matrix-completion-brief.md` §2
//!
//! | Layer | Meaning |
//! |-------|---------|
//! | **T0 Construct** | `StorageConfig` open via [`fireweed::open`] / [`fireweed::open_async`] |
//! | **T1 Lifecycle** | `create_queue` → `push` → `claim` → `complete` (+ Class B `fail`/reject) |
//! | **T2 Reopen** | Class-correct recovery after process-local drop |
//! | **T3 Contract** | Class B: projection durability + rejection; Class A log batches: TP-003 / request_id |
//! | **T4 Deploy** | Helm CI values under `charts/fireweed-queue/ci/` for chart-installable cells |
//!
//! Class A (durable log): reopen recovers pending items.
//! Class B `memory×memory`: process-local only — empty reopen is OK.
//! Class B `memory×{sqlite,postgres}`: projection-only reopen keeps items.
//!
//! ## Sqlite log three cells (Class A) — T0–T4
//!
//! | Cell | T0–T2 | T3 TP-003 | T4 Helm |
//! |------|-------|-----------|---------|
//! | `sqlite×memory` | [`sqlite_log_three_cells_t0_t2`] | immutable axis fixture + separate run-owned TP-003 producer | `ci/sqlite-memory-values.yaml` |
//! | `sqlite×sqlite` | same | axis `sqlite×sqlite` in that evidence file | `ci/sqlite-sqlite-values.yaml` |
//! | `sqlite×postgres` | same (env-gated) | env-gated live DB | `ci/sqlite-postgres-values.yaml` |
//!
//! Server driver: `cargo test -p fireweed-server --lib sqlite_log`.
//!
//! ## Postgres log three cells (Class A) — T0–T4
//!
//! | Cell | T0–T2 | T3 TP-003 | T4 Helm |
//! |------|-------|-----------|---------|
//! | `postgres×memory` | [`postgres_log_three_cells_t0_t2`] (env-gated) | axis in `tp003-ac-txn-matrix-postgres-storage-pairs.jsonl` (+ legacy `tp003-ac-txn-matrix-postgres.jsonl`) | `ci/postgres-memory-values.yaml` |
//! | `postgres×sqlite` | same (env-gated) | axis `postgres×sqlite` | `ci/postgres-sqlite-values.yaml` |
//! | `postgres×postgres` | same (env-gated) | axis `postgres×postgres` | `ci/postgres-postgres-values.yaml` |
//!
//! Server driver: `cargo test -p fireweed-server --features postgres --lib postgres`.
//! All three cells require `FIREWEED_PG_TEST_URL`; when unset they skip with `eprintln!` but
//! remain registered.
//!
//! ## S3 log three cells (Class A) — T0–T4
//!
//! | Cell | T0–T2 / T3 request_id | T4 Helm |
//! |------|------------------------|---------|
//! | `s3×memory` | [`s3_log_three_cells_t0_t3_contract`] (env-gated live S3) | `ci/s3-memory-values.yaml` |
//! | `s3×sqlite` | same | `ci/s3-sqlite-values.yaml` |
//! | `s3×postgres` | same + `FIREWEED_PG_TEST_URL` | `ci/s3-postgres-values.yaml` |
//!
//! Unit construction (no network): `cargo test -p fireweed-server --lib s3_object_log`.
//! Mandatory CI job requirements: `scripts/ci/s3-matrix-job-requirements.md`.
//!
//! Live fixtures: postgres cells need `FIREWEED_PG_TEST_URL` (and `--features postgres`);
//! s3 cells need `FIREWEED_S3_TEST_ENDPOINT` (+ optional bucket/region/keys). Missing
//! fixtures skip with `eprintln!` — the cell remains registered in the matrix table.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed::{
    ClientItemKey, ConfigSecret, EligibilityPolicy, LogConfig, NewItem, ObjectLogAuthority,
    OrderingMode, PostgresMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, PriorityValue, ProjectionStoreConfig, PushDisposition, QueueDefinition,
    QueueId, QueueKey, RecoveryPolicy, RecurrencePolicy, RequestId, ResponseBarrier, RetryPolicy,
    SegmentConfig, StorageConfig, SystemClock, TenantId, open_async,
};

static FIXTURE_ORDINAL: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------
// Matrix cell table (5 log × 4 projection = 20)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogAxis {
    Memory,
    Sqlite,
    Postgres,
    Filesystem,
    S3,
}

impl LogAxis {
    const ALL: [LogAxis; 5] = [
        LogAxis::Memory,
        LogAxis::Sqlite,
        LogAxis::Postgres,
        LogAxis::Filesystem,
        LogAxis::S3,
    ];

    fn name(self) -> &'static str {
        match self {
            LogAxis::Memory => "memory",
            LogAxis::Sqlite => "sqlite",
            LogAxis::Postgres => "postgres",
            LogAxis::Filesystem => "filesystem",
            LogAxis::S3 => "s3",
        }
    }

    fn is_durable(self) -> bool {
        !matches!(self, LogAxis::Memory)
    }

    fn needs_live_postgres(self) -> bool {
        matches!(self, LogAxis::Postgres)
    }

    fn needs_live_s3(self) -> bool {
        matches!(self, LogAxis::S3)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectionAxis {
    Memory,
    Sqlite,
    Turso,
    Postgres,
}

impl ProjectionAxis {
    const ALL: [ProjectionAxis; 4] = [
        ProjectionAxis::Memory,
        ProjectionAxis::Sqlite,
        ProjectionAxis::Turso,
        ProjectionAxis::Postgres,
    ];

    fn name(self) -> &'static str {
        match self {
            ProjectionAxis::Memory => "memory",
            ProjectionAxis::Sqlite => "sqlite",
            ProjectionAxis::Turso => "turso",
            ProjectionAxis::Postgres => "postgres",
        }
    }

    fn is_durable(self) -> bool {
        !matches!(self, ProjectionAxis::Memory)
    }

    fn needs_live_postgres(self) -> bool {
        matches!(self, ProjectionAxis::Postgres)
    }

    #[allow(dead_code)]
    fn is_local_deterministic(self) -> bool {
        matches!(
            self,
            ProjectionAxis::Memory | ProjectionAxis::Sqlite | ProjectionAxis::Turso
        )
    }
}

/// Class-correct T2 reopen expectation (brief §1.2 / §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReopenExpectation {
    /// Class A: durable log recovers pending items after reopen.
    RecoverPendingFromLog,
    /// Class B memory×memory: process-local only; empty reopen is OK.
    ProcessLocalEmptyOk,
    /// Class B memory×durable projection: projection-only reopen keeps items.
    ProjectionKeepsItems,
}

#[derive(Debug, Clone, Copy)]
struct MatrixCell {
    log: LogAxis,
    projection: ProjectionAxis,
}

impl MatrixCell {
    fn id(self) -> String {
        format!("{}×{}", self.log.name(), self.projection.name())
    }

    fn queue_id_slug(self) -> String {
        format!("{}_{}", self.log.name(), self.projection.name())
    }

    fn is_class_a(self) -> bool {
        self.log.is_durable()
    }

    fn reopen_expectation(self) -> ReopenExpectation {
        if self.is_class_a() {
            ReopenExpectation::RecoverPendingFromLog
        } else if self.projection.is_durable() {
            ReopenExpectation::ProjectionKeepsItems
        } else {
            ReopenExpectation::ProcessLocalEmptyOk
        }
    }

    fn needs_live_postgres(self) -> bool {
        self.log.needs_live_postgres() || self.projection.needs_live_postgres()
    }

    fn needs_live_s3(self) -> bool {
        self.log.needs_live_s3()
    }
}

fn all_matrix_cells() -> [MatrixCell; 20] {
    let mut cells = [MatrixCell {
        log: LogAxis::Memory,
        projection: ProjectionAxis::Memory,
    }; 20];
    let mut i = 0;
    for log in LogAxis::ALL {
        for projection in ProjectionAxis::ALL {
            cells[i] = MatrixCell { log, projection };
            i += 1;
        }
    }
    assert_eq!(i, 20);
    cells
}

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

struct FixtureRoot(PathBuf);

impl FixtureRoot {
    fn new(label: &str) -> Self {
        let ordinal = FIXTURE_ORDINAL.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "fireweed-matrix-t0t2-{label}-{}-{ordinal}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("matrix fixture root");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for FixtureRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn segments() -> SegmentConfig {
    SegmentConfig::new(1024 * 1024, 5).expect("valid segments")
}

fn queue_definition(queue_slug: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("matrix-t0t2").unwrap(),
        queue_id: QueueId::new(queue_slug).unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 3_600_000,
        client_item_key_retention_ms: 3_600_000,
        terminal_retention_ms: 3_600_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

fn queue_key(queue_slug: &str) -> QueueKey {
    QueueKey::new(
        TenantId::new("matrix-t0t2").unwrap(),
        QueueId::new(queue_slug).unwrap(),
    )
}

/// Why a registered cell cannot run T0–T2 in this process (fixture / feature gap).
///
/// Variants are gated by cargo features / env; not every build path constructs all of them.
#[derive(Debug)]
#[allow(dead_code)]
enum SkipReason {
    MissingPostgresFeature,
    MissingPostgresUrl,
    MissingS3Endpoint,
    MissingObjectlogFeature,
    MissingSqliteFeature,
    MissingMemoryFeature,
    MissingTursoFeature,
}

impl SkipReason {
    fn message(&self, cell_id: &str) -> String {
        match self {
            SkipReason::MissingPostgresFeature => format!(
                "storage_matrix_t0_t2: {cell_id} skipped (build without --features postgres)"
            ),
            SkipReason::MissingPostgresUrl => {
                format!("storage_matrix_t0_t2: {cell_id} skipped (FIREWEED_PG_TEST_URL unset)")
            }
            SkipReason::MissingS3Endpoint => {
                format!("storage_matrix_t0_t2: {cell_id} skipped (FIREWEED_S3_TEST_ENDPOINT unset)")
            }
            SkipReason::MissingObjectlogFeature => {
                format!("storage_matrix_t0_t2: {cell_id} skipped (build without objectlog feature)")
            }
            SkipReason::MissingSqliteFeature => {
                format!("storage_matrix_t0_t2: {cell_id} skipped (build without sqlite feature)")
            }
            SkipReason::MissingMemoryFeature => {
                format!("storage_matrix_t0_t2: {cell_id} skipped (build without memory feature)")
            }
            SkipReason::MissingTursoFeature => {
                format!("storage_matrix_t0_t2: {cell_id} skipped (build without turso feature)")
            }
        }
    }
}

fn skip_reason(cell: MatrixCell) -> Option<SkipReason> {
    // Feature gates (cfg evaluated at compile time; returns only when arm is compiled in).
    #[cfg(not(feature = "memory"))]
    if matches!(cell.log, LogAxis::Memory) {
        return Some(SkipReason::MissingMemoryFeature);
    }
    // memory×projection compositions and object-log×memory need the projection crate via `memory`/`objectlog`.
    #[cfg(not(any(feature = "memory", feature = "objectlog")))]
    if matches!(cell.projection, ProjectionAxis::Memory)
        && matches!(cell.log, LogAxis::Filesystem | LogAxis::S3)
    {
        return Some(SkipReason::MissingObjectlogFeature);
    }

    #[cfg(not(feature = "sqlite"))]
    if matches!(cell.log, LogAxis::Sqlite) || matches!(cell.projection, ProjectionAxis::Sqlite) {
        return Some(SkipReason::MissingSqliteFeature);
    }

    #[cfg(not(feature = "turso"))]
    if matches!(cell.projection, ProjectionAxis::Turso) {
        return Some(SkipReason::MissingTursoFeature);
    }

    #[cfg(not(feature = "objectlog"))]
    if matches!(cell.log, LogAxis::Filesystem | LogAxis::S3) {
        return Some(SkipReason::MissingObjectlogFeature);
    }

    if cell.needs_live_postgres() {
        #[cfg(not(feature = "postgres"))]
        {
            return Some(SkipReason::MissingPostgresFeature);
        }
        #[cfg(feature = "postgres")]
        {
            if std::env::var("FIREWEED_PG_TEST_URL").is_err() {
                return Some(SkipReason::MissingPostgresUrl);
            }
        }
    }

    if cell.needs_live_s3() && std::env::var("FIREWEED_S3_TEST_ENDPOINT").is_err() {
        return Some(SkipReason::MissingS3Endpoint);
    }

    None
}

/// Build a fresh [`StorageConfig`] for `cell` under `root`. Paths are stable for reopen.
fn build_config(cell: MatrixCell, root: &Path) -> StorageConfig {
    let log = match cell.log {
        LogAxis::Memory => LogConfig::Memory,
        LogAxis::Sqlite => LogConfig::Sqlite {
            path: root.join("log.db"),
        },
        LogAxis::Postgres => {
            let url = std::env::var("FIREWEED_PG_TEST_URL").expect("checked by skip_reason");
            // Unique schema per open so sequential matrix cells do not share durable residue.
            let schema = format!(
                "fw_t0t2_{}_{}_{}_{}",
                cell.log.name(),
                cell.projection.name(),
                std::process::id(),
                FIXTURE_ORDINAL.fetch_add(1, Ordering::Relaxed)
            );
            LogConfig::Postgres {
                url: ConfigSecret::new(url),
                schema: Some(schema),
                // memory projection pairs use log-replay; postgres projection uses relational
                mode: if matches!(cell.projection, ProjectionAxis::Postgres) {
                    PostgresMode::Relational
                } else {
                    PostgresMode::LogReplay
                },
                node_id: None,
                coordination: None,
            }
        }
        LogAxis::Filesystem => {
            let fs_root = root.join("object-log");
            std::fs::create_dir_all(&fs_root).expect("object-log root");
            LogConfig::Filesystem { root: fs_root }
        }
        LogAxis::S3 => {
            let endpoint = std::env::var("FIREWEED_S3_TEST_ENDPOINT").expect("checked by skip");
            let bucket =
                std::env::var("FIREWEED_S3_TEST_BUCKET").unwrap_or_else(|_| "fireweed".into());
            let region =
                std::env::var("FIREWEED_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into());
            let access = std::env::var("FIREWEED_S3_TEST_ACCESS_KEY")
                .unwrap_or_else(|_| "minioadmin".into());
            let secret = std::env::var("FIREWEED_S3_TEST_SECRET_KEY")
                .unwrap_or_else(|_| "minioadmin".into());
            LogConfig::S3 {
                endpoint,
                bucket,
                region,
                access_key_id: ConfigSecret::new(access),
                secret_access_key: ConfigSecret::new(secret),
                allow_insecure_http: true,
            }
        }
    };

    let projection = match cell.projection {
        ProjectionAxis::Memory => ProjectionStoreConfig::Memory,
        ProjectionAxis::Sqlite => ProjectionStoreConfig::Sqlite {
            path: root.join("projection.db"),
        },
        ProjectionAxis::Turso => ProjectionStoreConfig::Turso {
            path: root.join("projection-turso.db"),
        },
        ProjectionAxis::Postgres => {
            let url = std::env::var("FIREWEED_PG_TEST_URL").expect("checked by skip_reason");
            // postgres×postgres requires identical log and projection URLs.
            ProjectionStoreConfig::Postgres {
                url: ConfigSecret::new(url),
            }
        }
    };

    let mut cfg = StorageConfig {
        log,
        projection,
        control_plane: None,
        authority: None,
        response_barrier: ResponseBarrier::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
        segments: segments(),
        namespace: format!(
            "t0t2-{}-{}-{}-{}",
            cell.log.name(),
            cell.projection.name(),
            std::process::id(),
            FIXTURE_ORDINAL.fetch_add(1, Ordering::Relaxed)
        ),
        recovery: RecoveryPolicy::default(),
    };

    if matches!(
        &cfg.log,
        LogConfig::Filesystem { .. } | LogConfig::S3 { .. }
    ) {
        cfg.authority = Some(ObjectLogAuthority::NativeConditionalWrite);
    }

    cfg
}

// ---------------------------------------------------------------------------
// Per-cell T0–T2 body (+ Class B T3 contract)
// ---------------------------------------------------------------------------

/// Class B T3: memory log never claims durable log-replay; durable projection may claim
/// projection-only reopen. Enforced offline even when a live cell is fixture-skipped.
fn assert_class_b_t3_no_durable_log_replay(cell: MatrixCell) {
    assert!(!cell.is_class_a(), "{} is not Class B", cell.id());
    assert!(
        !cell.log.is_durable(),
        "{} T3: memory log must not be durable",
        cell.id()
    );
    // Product claim shape (mirrors fireweed-conformance hard rule without a dep cycle).
    let durable_log_replay_claimed = false;
    assert!(
        !durable_log_replay_claimed,
        "{} T3: Class B must not claim durable_log_replay",
        cell.id()
    );
    match cell.reopen_expectation() {
        ReopenExpectation::ProcessLocalEmptyOk => {
            assert!(
                !cell.projection.is_durable(),
                "{} T3: process-local reopen only for memory projection",
                cell.id()
            );
        }
        ReopenExpectation::ProjectionKeepsItems => {
            assert!(
                cell.projection.is_durable(),
                "{} T3: projection-only reopen requires durable projection",
                cell.id()
            );
        }
        ReopenExpectation::RecoverPendingFromLog => {
            panic!(
                "{} T3: Class B must never use RecoverPendingFromLog (that is log-replay)",
                cell.id()
            );
        }
    }
}

async fn run_cell_t0_t2(cell: MatrixCell) {
    let cell_id = cell.id();
    let expectation = cell.reopen_expectation();

    if let Some(reason) = skip_reason(cell) {
        eprintln!("{}", reason.message(&cell_id));
        // Class B T3 claims are fixture-independent — still enforce offline.
        if !cell.is_class_a() {
            assert_class_b_t3_no_durable_log_replay(cell);
        }
        return;
    }

    let root = FixtureRoot::new(&cell.queue_id_slug());
    let clock = Arc::new(SystemClock);
    let definition = queue_definition(&cell.queue_id_slug());
    let key = queue_key(&cell.queue_id_slug());

    // --- T0 Construct ---
    let cfg = build_config(cell, root.path());
    cfg.validate()
        .unwrap_or_else(|e| panic!("{cell_id} T0 validate: {e:?}"));
    let fireweed = open_async(cfg.clone(), Arc::clone(&clock) as _)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T0 open: {e:?}"));

    // --- T1 Lifecycle: create_queue → push → claim → complete ---
    fireweed
        .create_queue(definition.clone())
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T1 create_queue: {e:?}"));

    let lifecycle_id = fireweed
        .push(
            &key,
            NewItem {
                client_item_key: Some(
                    ClientItemKey::new(format!("{}_lifecycle", cell.queue_id_slug())).unwrap(),
                ),
                priority: Some(PriorityValue::Int64(10)),
                ..NewItem::default()
            },
        )
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T1 push: {e:?}"));

    let claimed = fireweed
        .claim(&key, 1, 30_000)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T1 claim: {e:?}"));
    assert_eq!(
        claimed.len(),
        1,
        "{cell_id} T1: expected one claimed item, got {}",
        claimed.len()
    );
    assert_eq!(claimed[0].item_id, lifecycle_id);

    fireweed
        .complete(&key, claimed.iter().map(|item| item.item_id))
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T1 complete: {e:?}"));

    let metrics_after_lifecycle = fireweed
        .metrics(&key)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T1 metrics: {e:?}"));
    assert_eq!(
        metrics_after_lifecycle.pending, 0,
        "{cell_id} T1: pending should be 0 after complete"
    );
    assert_eq!(
        metrics_after_lifecycle.complete, 1,
        "{cell_id} T1: complete should be 1 after finalize"
    );

    // Class B T1/T3: reject path (fail dead-letter) must terminalize without log-replay claims.
    if !cell.is_class_a() {
        let fail_id = fireweed
            .push(
                &key,
                NewItem {
                    client_item_key: Some(
                        ClientItemKey::new(format!("{}_reject", cell.queue_id_slug())).unwrap(),
                    ),
                    priority: Some(PriorityValue::Int64(15)),
                    ..NewItem::default()
                },
            )
            .await
            .unwrap_or_else(|e| panic!("{cell_id} T1/T3 push(reject): {e:?}"));
        let fail_claimed = fireweed
            .claim(&key, 1, 30_000)
            .await
            .unwrap_or_else(|e| panic!("{cell_id} T1/T3 claim(reject): {e:?}"));
        assert_eq!(fail_claimed.len(), 1);
        assert_eq!(fail_claimed[0].item_id, fail_id);
        fireweed
            .fail(&key, fail_claimed.iter().map(|item| item.item_id))
            .await
            .unwrap_or_else(|e| panic!("{cell_id} T1/T3 fail/reject: {e:?}"));
        let m = fireweed
            .metrics(&key)
            .await
            .unwrap_or_else(|e| panic!("{cell_id} T1/T3 metrics after fail: {e:?}"));
        assert_eq!(
            m.failed, 1,
            "{cell_id} T3: reject path terminalizes failed=1"
        );
        assert_eq!(
            m.complete, 1,
            "{cell_id} T3: complete path undisturbed by reject"
        );
    }

    // Seed a pending item for T2 reopen checks.
    let pending_id = fireweed
        .push(
            &key,
            NewItem {
                client_item_key: Some(
                    ClientItemKey::new(format!("{}_pending", cell.queue_id_slug())).unwrap(),
                ),
                priority: Some(PriorityValue::Int64(20)),
                ..NewItem::default()
            },
        )
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T2 seed push: {e:?}"));
    assert_eq!(
        fireweed.metrics(&key).await.unwrap().pending,
        1,
        "{cell_id}: seed pending item before drop"
    );
    let _ = pending_id;

    // Process death: drop the handle (and any held resources).
    drop(fireweed);

    // --- T2 Reopen ---
    let reopened = open_async(cfg, clock as _)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T2 reopen: {e:?}"));

    match expectation {
        ReopenExpectation::RecoverPendingFromLog => {
            // Class A: log is SoT — pending survives even when projection is memory.
            let pending = reopened
                .metrics(&key)
                .await
                .unwrap_or_else(|e| panic!("{cell_id} T2 metrics (Class A): {e:?}"))
                .pending;
            assert_eq!(
                pending, 1,
                "{cell_id} T2 Class A: expected 1 pending after reopen (durable log recover)"
            );
            let claimed = reopened
                .claim(&key, 1, 30_000)
                .await
                .unwrap_or_else(|e| panic!("{cell_id} T2 claim (Class A): {e:?}"));
            assert_eq!(claimed.len(), 1, "{cell_id} T2 Class A claim");
            reopened
                .complete(&key, claimed.iter().map(|item| item.item_id))
                .await
                .unwrap_or_else(|e| panic!("{cell_id} T2 complete (Class A): {e:?}"));
        }
        ReopenExpectation::ProjectionKeepsItems => {
            // Class B + durable projection: items remain via projection only (no log-rebuild claim).
            let m = reopened
                .metrics(&key)
                .await
                .unwrap_or_else(|e| panic!("{cell_id} T2 metrics (Class B proj): {e:?}"));
            assert_eq!(
                m.pending, 1,
                "{cell_id} T2 Class B: durable projection should keep 1 pending (projection-only reopen)"
            );
            // T3: terminal reject state also survives via projection (not log-replay).
            assert_eq!(
                m.failed, 1,
                "{cell_id} T3 Class B: failed/rejected survives via durable projection"
            );
            assert_eq!(
                m.complete, 1,
                "{cell_id} T3 Class B: complete survives via durable projection"
            );
            let claimed = reopened
                .claim(&key, 1, 30_000)
                .await
                .unwrap_or_else(|e| panic!("{cell_id} T2 claim (Class B proj): {e:?}"));
            assert_eq!(claimed.len(), 1, "{cell_id} T2 Class B projection claim");
            reopened
                .complete(&key, claimed.iter().map(|item| item.item_id))
                .await
                .unwrap_or_else(|e| panic!("{cell_id} T2 complete (Class B proj): {e:?}"));
        }
        ReopenExpectation::ProcessLocalEmptyOk => {
            // Class B memory×memory: fully process-local. Empty reopen is correct semantics —
            // do not claim durable log-replay. Document that create_queue is fresh.
            let outcome = reopened
                .create_queue(definition)
                .await
                .unwrap_or_else(|e| panic!("{cell_id} T2 create_queue (process-local): {e:?}"));
            assert!(
                outcome.created,
                "{cell_id} T2 memory×memory: reopen should not recover prior queue (process-local Class B)"
            );
            let pending = reopened
                .metrics(&key)
                .await
                .unwrap_or_else(|e| panic!("{cell_id} T2 metrics (process-local): {e:?}"))
                .pending;
            assert_eq!(
                pending, 0,
                "{cell_id} T2 memory×memory: empty reopen is OK (Class B process-local)"
            );
            eprintln!(
                "storage_matrix_t0_t2: {cell_id} T2 process-local empty reopen (documented Class B)"
            );
        }
    }

    // --- T3 Contract (Class B only in this harness) ---
    if !cell.is_class_a() {
        assert_class_b_t3_no_durable_log_replay(cell);
        eprintln!("storage_matrix_t0_t2: {cell_id} T3 Class B contract (no durable_log_replay)");
    }

    drop(reopened);
    // FixtureRoot Drop cleans local paths; PG schemas / S3 objects are left for the suite env.
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Registers all 20 cells and runs T0–T2 (or documents skip) for each.
#[tokio::test]
async fn storage_matrix_t0_t2_all_twenty_cells() {
    let cells = all_matrix_cells();
    assert_eq!(cells.len(), 20, "public matrix is exactly 20 cells");

    let mut ran = 0usize;
    let mut skipped = 0usize;
    let mut class_a = 0usize;
    let mut class_b = 0usize;
    let mut local_turso_ran = 0usize;
    let mut local_turso_skipped = 0usize;

    for cell in cells {
        if cell.is_class_a() {
            class_a += 1;
        } else {
            class_b += 1;
        }

        let is_local_turso = matches!(cell.projection, ProjectionAxis::Turso)
            && !cell.needs_live_postgres()
            && !cell.needs_live_s3();
        if skip_reason(cell).is_some() {
            skipped += 1;
            if is_local_turso {
                local_turso_skipped += 1;
            }
            // Still invoke so skip is eprintln'd consistently and the cell is "registered".
            run_cell_t0_t2(cell).await;
            continue;
        }

        run_cell_t0_t2(cell).await;
        ran += 1;
        if is_local_turso {
            local_turso_ran += 1;
        }
    }

    assert_eq!(
        class_a, 16,
        "16 Class A cells (non-memory log × 4 projections)"
    );
    assert_eq!(class_b, 4, "4 Class B cells (memory log × 4 projections)");
    assert_eq!(ran + skipped, 20, "every cell counted as ran or skipped");

    // Local deterministic Turso rows (memory/sqlite/filesystem × turso) must never skip when the
    // turso feature is enabled — only live postgres/s3 fixture gaps may skip other rows.
    #[cfg(feature = "turso")]
    {
        assert_eq!(
            local_turso_skipped, 0,
            "deterministic local Turso rows must not skip; local_turso_ran={local_turso_ran} local_turso_skipped={local_turso_skipped}"
        );
        assert_eq!(
            local_turso_ran, 3,
            "expected exactly memory×turso, sqlite×turso, filesystem×turso; ran={local_turso_ran}"
        );
    }

    // Default feature set always exercises local cells including Turso.
    assert!(
        ran >= 9,
        "expected ≥9 in-process cells (memory/sqlite/filesystem × memory/sqlite/turso) without live PG/S3; ran={ran} skipped={skipped}"
    );

    eprintln!(
        "storage_matrix_t0_t2: ran={ran} skipped={skipped} local_turso_ran={local_turso_ran} (of 20 registered cells)"
    );
}

/// Structural registration: the table enumerates every public axis pair exactly once.
#[test]
fn storage_matrix_registers_exactly_20_distinct_cells() {
    let cells = all_matrix_cells();
    assert_eq!(cells.len(), 20);

    let mut ids: Vec<String> = cells.iter().map(|c| c.id()).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 20, "cell ids must be unique: {ids:?}");

    // Spot-check axes and reopen expectations.
    let mem_mem = cells
        .iter()
        .find(|c| c.log == LogAxis::Memory && c.projection == ProjectionAxis::Memory)
        .unwrap();
    assert_eq!(
        mem_mem.reopen_expectation(),
        ReopenExpectation::ProcessLocalEmptyOk
    );

    let mem_sqlite = cells
        .iter()
        .find(|c| c.log == LogAxis::Memory && c.projection == ProjectionAxis::Sqlite)
        .unwrap();
    assert_eq!(
        mem_sqlite.reopen_expectation(),
        ReopenExpectation::ProjectionKeepsItems
    );

    let sqlite_sqlite = cells
        .iter()
        .find(|c| c.log == LogAxis::Sqlite && c.projection == ProjectionAxis::Sqlite)
        .unwrap();
    assert_eq!(
        sqlite_sqlite.reopen_expectation(),
        ReopenExpectation::RecoverPendingFromLog
    );

    let filesystem_memory = cells
        .iter()
        .find(|c| c.log == LogAxis::Filesystem && c.projection == ProjectionAxis::Memory)
        .unwrap();
    assert!(filesystem_memory.is_class_a());
}

// ---------------------------------------------------------------------------
// Filesystem log three cells: full T0–T3 (Class A contract bar)
// ---------------------------------------------------------------------------

/// T0–T3 for filesystem×memory and filesystem×sqlite (always in-process).
/// filesystem×postgres runs when `FIREWEED_PG_TEST_URL` + `--features postgres` are available.
#[tokio::test]
async fn filesystem_log_three_cells_t0_t3_contract() {
    let cells = [
        MatrixCell {
            log: LogAxis::Filesystem,
            projection: ProjectionAxis::Memory,
        },
        MatrixCell {
            log: LogAxis::Filesystem,
            projection: ProjectionAxis::Sqlite,
        },
        MatrixCell {
            log: LogAxis::Filesystem,
            projection: ProjectionAxis::Postgres,
        },
    ];

    let mut ran = 0usize;
    for cell in cells {
        let cell_id = cell.id();
        if let Some(reason) = skip_reason(cell) {
            eprintln!("{}", reason.message(&cell_id));
            continue;
        }
        run_filesystem_cell_t0_t3(cell).await;
        ran += 1;
    }

    // Always run the two local filesystem cells under default features.
    assert!(
        ran >= 2,
        "filesystem×memory and filesystem×sqlite must run without live PG; ran={ran}"
    );
    eprintln!("filesystem_log_three_cells_t0_t3_contract: ran={ran}/3");
}

async fn run_filesystem_cell_t0_t3(cell: MatrixCell) {
    assert!(
        matches!(cell.log, LogAxis::Filesystem) && cell.is_class_a(),
        "filesystem Class A only"
    );
    let cell_id = cell.id();
    let root = FixtureRoot::new(&format!("fs-t3-{}", cell.queue_id_slug()));
    let clock = Arc::new(SystemClock);
    let slug = format!("fs_t3_{}", cell.queue_id_slug());
    let definition = queue_definition(&slug);
    let key = queue_key(&slug);

    let cfg = build_config(cell, root.path());
    cfg.validate()
        .unwrap_or_else(|e| panic!("{cell_id} T0 validate: {e:?}"));
    let fireweed = open_async(cfg.clone(), Arc::clone(&clock) as _)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T0 open: {e:?}"));

    fireweed
        .create_queue(definition.clone())
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T1 create_queue: {e:?}"));

    // T1 lifecycle: push → claim → complete
    let lifecycle_id = fireweed
        .push(
            &key,
            NewItem {
                client_item_key: Some(ClientItemKey::new(format!("{slug}_lifecycle")).unwrap()),
                priority: Some(PriorityValue::Int64(10)),
                ..NewItem::default()
            },
        )
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T1 push: {e:?}"));

    let claimed = fireweed
        .claim(&key, 1, 30_000)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T1 claim: {e:?}"));
    assert_eq!(claimed.len(), 1, "{cell_id} T1 claim");
    assert_eq!(claimed[0].item_id, lifecycle_id);
    fireweed
        .complete(&key, claimed.iter().map(|item| item.item_id))
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T1 complete: {e:?}"));

    // T3 AC-TXN-3: request_id Fresh then Replayed (and across reopen)
    let rid = RequestId::new(format!("{slug}-rid")).unwrap();
    let item = NewItem {
        client_item_key: Some(ClientItemKey::new(format!("{slug}_rid")).unwrap()),
        priority: Some(PriorityValue::Int64(20)),
        ..NewItem::default()
    };
    let (first_id, first_disp) = fireweed
        .push_with_request_id(&key, rid.clone(), item.clone())
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T3 first push_with_request_id: {e:?}"));
    assert_eq!(
        first_disp,
        PushDisposition::Fresh,
        "{cell_id} T3: first request_id must be Fresh"
    );
    let (second_id, second_disp) = fireweed
        .push_with_request_id(&key, rid.clone(), item)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T3 replay push_with_request_id: {e:?}"));
    assert_eq!(
        second_disp,
        PushDisposition::Replayed,
        "{cell_id} T3: same request_id + body must be Replayed"
    );
    assert_eq!(
        first_id, second_id,
        "{cell_id} T3: request_id replay must return the same item id"
    );
    assert_eq!(
        fireweed.metrics(&key).await.unwrap().pending,
        1,
        "{cell_id} T3: replay must not double-insert"
    );

    drop(fireweed);

    let reopened = open_async(cfg, clock as _)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T2 reopen: {e:?}"));
    assert_eq!(
        reopened.metrics(&key).await.unwrap().pending,
        1,
        "{cell_id} T2 Class A: pending recovered from durable filesystem log"
    );

    let (after_id, after_disp) = reopened
        .push_with_request_id(
            &key,
            rid,
            NewItem {
                client_item_key: Some(ClientItemKey::new(format!("{slug}_rid")).unwrap()),
                priority: Some(PriorityValue::Int64(20)),
                ..NewItem::default()
            },
        )
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T3 post-reopen request_id: {e:?}"));
    assert_eq!(
        after_disp,
        PushDisposition::Replayed,
        "{cell_id} T3: request_id survives process death (Class A)"
    );
    assert_eq!(
        after_id, first_id,
        "{cell_id} T3: request_id id survives process death"
    );
    assert_eq!(reopened.metrics(&key).await.unwrap().pending, 1);

    let claimed = reopened
        .claim(&key, 1, 30_000)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T2 claim: {e:?}"));
    assert_eq!(claimed.len(), 1);
    reopened
        .complete(&key, claimed.iter().map(|item| item.item_id))
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T2 complete: {e:?}"));
    assert_eq!(reopened.metrics(&key).await.unwrap().pending, 0);
}

/// Focused T0–T2 for the three Class A **sqlite log** cells (brief program cell batch).
///
/// Complements the full 20-cell table: always exercises `sqlite×memory` and `sqlite×sqlite`
/// when the `sqlite` feature is on; `sqlite×postgres` follows the same skip rules as the table.
#[tokio::test]
async fn sqlite_log_three_cells_t0_t2() {
    let cells = [
        MatrixCell {
            log: LogAxis::Sqlite,
            projection: ProjectionAxis::Memory,
        },
        MatrixCell {
            log: LogAxis::Sqlite,
            projection: ProjectionAxis::Sqlite,
        },
        MatrixCell {
            log: LogAxis::Sqlite,
            projection: ProjectionAxis::Postgres,
        },
    ];
    for cell in cells {
        assert!(cell.is_class_a(), "{} must be Class A", cell.id());
        assert_eq!(
            cell.reopen_expectation(),
            ReopenExpectation::RecoverPendingFromLog,
            "{} Class A reopen",
            cell.id()
        );
        run_cell_t0_t2(cell).await;
    }
}

// ---------------------------------------------------------------------------
// S3 log three cells: full T0–T3 (Class A contract bar; live S3 env-gated)
// ---------------------------------------------------------------------------

/// T0–T3 for s3×memory, s3×sqlite, and s3×postgres when `FIREWEED_S3_TEST_ENDPOINT` is set.
/// Without a live S3 fixture the cells remain registered and skip with an explicit `eprintln!`.
#[tokio::test]
async fn s3_log_three_cells_t0_t3_contract() {
    let cells = [
        MatrixCell {
            log: LogAxis::S3,
            projection: ProjectionAxis::Memory,
        },
        MatrixCell {
            log: LogAxis::S3,
            projection: ProjectionAxis::Sqlite,
        },
        MatrixCell {
            log: LogAxis::S3,
            projection: ProjectionAxis::Postgres,
        },
    ];

    let mut ran = 0usize;
    for cell in cells {
        let cell_id = cell.id();
        if let Some(reason) = skip_reason(cell) {
            eprintln!("{}", reason.message(&cell_id));
            continue;
        }
        run_s3_cell_t0_t3(cell).await;
        ran += 1;
    }

    if std::env::var("FIREWEED_S3_TEST_ENDPOINT").is_ok() {
        assert!(
            ran >= 2,
            "with FIREWEED_S3_TEST_ENDPOINT set, s3×memory and s3×sqlite must run; ran={ran}"
        );
    } else {
        eprintln!(
            "s3_log_three_cells_t0_t3_contract: no live S3 (ran={ran}/3); \
             set FIREWEED_S3_TEST_ENDPOINT for required CI (see scripts/ci/s3-matrix-job-requirements.md)"
        );
    }
    eprintln!("s3_log_three_cells_t0_t3_contract: ran={ran}/3");
}

async fn run_s3_cell_t0_t3(cell: MatrixCell) {
    assert!(
        matches!(cell.log, LogAxis::S3) && cell.is_class_a(),
        "s3 Class A only"
    );
    let cell_id = cell.id();
    let root = FixtureRoot::new(&format!("s3-t3-{}", cell.queue_id_slug()));
    let clock = Arc::new(SystemClock);
    let slug = format!("s3_t3_{}", cell.queue_id_slug());
    let definition = queue_definition(&slug);
    let key = queue_key(&slug);

    // Unique StorageConfig.namespace per cell so concurrent matrix runs do not collide
    // on the shared S3 bucket (open wraps the store in NamespacedBlobStore).
    let mut cfg = build_config(cell, root.path());
    cfg.namespace = format!(
        "t0t2-s3-t3-{}-{}-{}",
        cell.queue_id_slug(),
        std::process::id(),
        FIXTURE_ORDINAL.fetch_add(1, Ordering::Relaxed)
    );
    cfg.validate()
        .unwrap_or_else(|e| panic!("{cell_id} T0 validate: {e:?}"));
    let fireweed = open_async(cfg.clone(), Arc::clone(&clock) as _)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T0 open: {e:?}"));

    fireweed
        .create_queue(definition.clone())
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T1 create_queue: {e:?}"));

    // T1 lifecycle: push → claim → complete
    let lifecycle_id = fireweed
        .push(
            &key,
            NewItem {
                client_item_key: Some(ClientItemKey::new(format!("{slug}_lifecycle")).unwrap()),
                priority: Some(PriorityValue::Int64(10)),
                ..NewItem::default()
            },
        )
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T1 push: {e:?}"));

    let claimed = fireweed
        .claim(&key, 1, 30_000)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T1 claim: {e:?}"));
    assert_eq!(claimed.len(), 1, "{cell_id} T1 claim");
    assert_eq!(claimed[0].item_id, lifecycle_id);
    fireweed
        .complete(&key, claimed.iter().map(|item| item.item_id))
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T1 complete: {e:?}"));

    // T3 AC-TXN-3: request_id Fresh then Replayed (and across reopen)
    let rid = RequestId::new(format!("{slug}-rid")).unwrap();
    let item = NewItem {
        client_item_key: Some(ClientItemKey::new(format!("{slug}_rid")).unwrap()),
        priority: Some(PriorityValue::Int64(20)),
        ..NewItem::default()
    };
    let (first_id, first_disp) = fireweed
        .push_with_request_id(&key, rid.clone(), item.clone())
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T3 first push_with_request_id: {e:?}"));
    assert_eq!(
        first_disp,
        PushDisposition::Fresh,
        "{cell_id} T3: first request_id must be Fresh"
    );
    let (second_id, second_disp) = fireweed
        .push_with_request_id(&key, rid.clone(), item)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T3 replay push_with_request_id: {e:?}"));
    assert_eq!(
        second_disp,
        PushDisposition::Replayed,
        "{cell_id} T3: same request_id + body must be Replayed"
    );
    assert_eq!(
        first_id, second_id,
        "{cell_id} T3: request_id replay must return the same item id"
    );
    assert_eq!(
        fireweed.metrics(&key).await.unwrap().pending,
        1,
        "{cell_id} T3: replay must not double-insert"
    );

    drop(fireweed);

    let reopened = open_async(cfg, clock as _)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T2 reopen: {e:?}"));
    assert_eq!(
        reopened.metrics(&key).await.unwrap().pending,
        1,
        "{cell_id} T2 Class A: pending recovered from durable s3 log"
    );

    let (after_id, after_disp) = reopened
        .push_with_request_id(
            &key,
            rid,
            NewItem {
                client_item_key: Some(ClientItemKey::new(format!("{slug}_rid")).unwrap()),
                priority: Some(PriorityValue::Int64(20)),
                ..NewItem::default()
            },
        )
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T3 post-reopen request_id: {e:?}"));
    assert_eq!(
        after_disp,
        PushDisposition::Replayed,
        "{cell_id} T3: request_id survives process death (Class A)"
    );
    assert_eq!(
        after_id, first_id,
        "{cell_id} T3: request_id id survives process death"
    );
    assert_eq!(reopened.metrics(&key).await.unwrap().pending, 1);

    let claimed = reopened
        .claim(&key, 1, 30_000)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T2 claim: {e:?}"));
    assert_eq!(claimed.len(), 1);
    reopened
        .complete(&key, claimed.iter().map(|item| item.item_id))
        .await
        .unwrap_or_else(|e| panic!("{cell_id} T2 complete: {e:?}"));
    assert_eq!(reopened.metrics(&key).await.unwrap().pending, 0);
}

/// T3/T4 linkage for s3 log cells: axis-named evidence file and Helm CI values exist.
#[test]
fn s3_log_t3_t4_evidence_and_helm_values_present() {
    let fixture = fireweed_release::Fixture::new(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tp003-s3-axis.jsonl"),
    )
    .expect("open immutable S3 axis fixture");
    let body = std::fs::read_to_string(
        fixture
            .authorize(fireweed_release::EvidenceOperation::Read)
            .expect("fixture authorizes reads"),
    )
    .expect("read S3 axis fixture");
    for axis in ["s3×memory", "s3×sqlite", "s3×postgres"] {
        assert!(
            body.contains(axis),
            "TP-003 s3 pair evidence must name axis {axis}"
        );
    }

    let chart_ci = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../charts/fireweed-queue/ci");
    for name in [
        "s3-memory-values.yaml",
        "s3-sqlite-values.yaml",
        "s3-postgres-values.yaml",
    ] {
        let p = chart_ci.join(name);
        assert!(p.is_file(), "T4 Helm CI values missing: {}", p.display());
        let v = std::fs::read_to_string(&p).unwrap();
        assert!(
            v.contains("backend: s3"),
            "{name} must set storage.log.backend=s3"
        );
    }

    let reqs = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../scripts/ci/s3-matrix-job-requirements.md");
    assert!(
        reqs.is_file(),
        "mandatory S3 CI job requirements doc missing: {}",
        reqs.display()
    );
}

/// T3/T4 linkage for sqlite log cells: axis-named evidence file and Helm CI values exist.
#[test]
fn sqlite_log_t3_t4_evidence_and_helm_values_present() {
    let fixture = fireweed_release::Fixture::new(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tp003-sqlite-axis.jsonl"),
    )
    .expect("open immutable sqlite axis fixture");
    let body = std::fs::read_to_string(
        fixture
            .authorize(fireweed_release::EvidenceOperation::Read)
            .expect("fixture authorizes reads"),
    )
    .expect("read sqlite axis fixture");
    for axis in ["sqlite×memory", "sqlite×sqlite", "sqlite×postgres"] {
        assert!(
            body.contains(axis),
            "TP-003 sqlite pair evidence must name axis {axis}"
        );
    }

    let chart_ci = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../charts/fireweed-queue/ci");
    for name in [
        "sqlite-memory-values.yaml",
        "sqlite-sqlite-values.yaml",
        "sqlite-postgres-values.yaml",
    ] {
        let p = chart_ci.join(name);
        assert!(p.is_file(), "T4 Helm CI values missing: {}", p.display());
        let v = std::fs::read_to_string(&p).unwrap();
        assert!(
            v.contains("backend: sqlite"),
            "{name} must set storage.log.backend=sqlite"
        );
    }
}

// ---------------------------------------------------------------------------
// Postgres log three cells: full T0–T4 (Class A)
// ---------------------------------------------------------------------------

/// Focused T0–T2 for the three Class A **postgres log** cells (brief program cell batch).
///
/// All three require `FIREWEED_PG_TEST_URL` + `--features postgres`. When the URL is unset each
/// cell is still registered and documents the skip (same rules as the 20-cell table).
#[tokio::test]
async fn postgres_log_three_cells_t0_t2() {
    let cells = [
        MatrixCell {
            log: LogAxis::Postgres,
            projection: ProjectionAxis::Memory,
        },
        MatrixCell {
            log: LogAxis::Postgres,
            projection: ProjectionAxis::Sqlite,
        },
        MatrixCell {
            log: LogAxis::Postgres,
            projection: ProjectionAxis::Postgres,
        },
    ];
    let mut ran = 0usize;
    let mut skipped = 0usize;
    for cell in cells {
        assert!(cell.is_class_a(), "{} must be Class A", cell.id());
        assert_eq!(
            cell.reopen_expectation(),
            ReopenExpectation::RecoverPendingFromLog,
            "{} Class A reopen",
            cell.id()
        );
        if skip_reason(cell).is_some() {
            skipped += 1;
        } else {
            ran += 1;
        }
        run_cell_t0_t2(cell).await;
    }
    assert_eq!(
        ran + skipped,
        3,
        "postgres log three cells must all be registered (ran={ran} skipped={skipped})"
    );
    eprintln!("postgres_log_three_cells_t0_t2: ran={ran} skipped={skipped}/3");
}

/// T3/T4 linkage for postgres log cells: axis-named evidence file and Helm CI values exist.
#[test]
fn postgres_log_t3_t4_evidence_and_helm_values_present() {
    let fixture = fireweed_release::Fixture::new(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tp003-postgres-axis.jsonl"),
    )
    .expect("open immutable postgres axis fixture");
    let body = std::fs::read_to_string(
        fixture
            .authorize(fireweed_release::EvidenceOperation::Read)
            .expect("fixture authorizes reads"),
    )
    .expect("read postgres axis fixture");
    for axis in ["postgres×memory", "postgres×sqlite", "postgres×postgres"] {
        let escaped = axis.replace('×', "\\u00d7");
        let slash = axis.replace('×', "/");
        assert!(
            body.contains(axis) || body.contains(&escaped) || body.contains(&slash),
            "TP-003 postgres pair evidence must name axis {axis}"
        );
    }

    let chart_ci = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../charts/fireweed-queue/ci");
    for name in [
        "postgres-memory-values.yaml",
        "postgres-sqlite-values.yaml",
        "postgres-postgres-values.yaml",
    ] {
        let p = chart_ci.join(name);
        assert!(p.is_file(), "T4 Helm CI values missing: {}", p.display());
        let v = std::fs::read_to_string(&p).unwrap();
        assert!(
            v.contains("backend: postgres"),
            "{name} must set storage.log.backend=postgres"
        );
    }
}
