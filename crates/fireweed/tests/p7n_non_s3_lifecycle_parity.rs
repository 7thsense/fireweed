//! P7N — claim / append / finalize / lifecycle / races / recurrence on the 12 non-S3 cells.
//!
//! The public 5×3 matrix without S3 is:
//!   log ∈ {memory, sqlite, postgres, filesystem}
//!   × projection ∈ {memory, sqlite, postgres}
//! = 12 cells.
//!
//! This suite is intentionally **P7-method focused** (push/claim/complete/fail/release/retry/
//! rearm + claim race + recurrence cycle). Discovery / projection-control / rich-claim capability
//! gaps are P6 / other plan keys and must not block lifecycle parity.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed::{
    ClientItemKey, Fireweed, NewItem, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, ProjectionStoreConfig, QueueDefinition,
    QueueId, QueueKey, RecurrenceMode, RecurrencePolicy, ResponseBarrier, RetryPolicy,
    SegmentConfig, StorageConfig, SystemClock, TenantId, UtcTimestamp, open,
};

#[cfg(feature = "postgres")]
use fireweed::{ConfigSecret, LogConfig, PostgresMode};

static ORD: AtomicU64 = AtomicU64::new(0);

struct FixtureRoot(PathBuf);

impl FixtureRoot {
    fn new(label: &str) -> Self {
        let n = ORD.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("fireweed-p7n-{}-{}-{n}", label, std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
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
    SegmentConfig {
        target_bytes: 1024 * 1024,
        max_latency_ms: 5,
    }
}

fn queue_def(name: &str, recurring: bool) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("p7n").unwrap(),
        queue_id: QueueId::new(name).unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: fireweed::EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: if recurring {
            RecurrencePolicy {
                mode: RecurrenceMode::Recurring,
                until: Some(UtcTimestamp::new(i64::MAX / 4, 0).unwrap()),
            }
        } else {
            RecurrencePolicy::default()
        },
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 5 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

fn qk(name: &str) -> QueueKey {
    QueueKey::new(TenantId::new("p7n").unwrap(), QueueId::new(name).unwrap())
}

fn item(key: &str, priority: i64) -> NewItem {
    NewItem {
        client_item_key: Some(ClientItemKey::new(key).unwrap()),
        priority: Some(PriorityValue::Int64(priority)),
        ..Default::default()
    }
}

/// Core P7 lifecycle: push → claim → complete / fail / release / retry / rearm + race + recurrence.
///
/// Queue and client-item-key names are cell-prefixed so shared Postgres URLs without schema
/// isolation cannot collide across parallel cells.
async fn exercise_lifecycle(cell: &str, fw: &Fireweed) {
    let slug = cell.replace(['-', '×', 'x'], "_");
    let qname = |suffix: &str| format!("{slug}_{suffix}");
    let key = |suffix: &str| format!("{slug}_{suffix}");

    // --- complete ---
    let name = qname("complete");
    let q = qk(&name);
    fw.create_queue(queue_def(&name, false))
        .await
        .unwrap_or_else(|e| panic!("{cell}: create complete: {e}"));
    let id = fw
        .push(&q, item(&key("complete-1"), 1))
        .await
        .unwrap_or_else(|e| panic!("{cell}: push complete: {e}"));
    let claimed = fw
        .claim(&q, 1, 60_000)
        .await
        .unwrap_or_else(|e| panic!("{cell}: claim complete: {e}"));
    assert_eq!(claimed.len(), 1, "{cell}: expected one claim for complete");
    assert_eq!(claimed[0].item_id, id);
    fw.complete(&q, [id])
        .await
        .unwrap_or_else(|e| panic!("{cell}: complete: {e}"));

    // --- fail ---
    let name = qname("fail");
    let q = qk(&name);
    fw.create_queue(queue_def(&name, false))
        .await
        .unwrap_or_else(|e| panic!("{cell}: create fail: {e}"));
    let id = fw
        .push(&q, item(&key("fail-1"), 1))
        .await
        .unwrap_or_else(|e| panic!("{cell}: push fail: {e}"));
    let claimed = fw.claim(&q, 1, 60_000).await.unwrap();
    assert_eq!(claimed.len(), 1);
    fw.fail(&q, [id])
        .await
        .unwrap_or_else(|e| panic!("{cell}: fail: {e}"));

    // --- release ---
    let name = qname("release");
    let q = qk(&name);
    fw.create_queue(queue_def(&name, false))
        .await
        .unwrap_or_else(|e| panic!("{cell}: create release: {e}"));
    let id = fw
        .push(&q, item(&key("release-1"), 1))
        .await
        .unwrap_or_else(|e| panic!("{cell}: push release: {e}"));
    let claimed = fw.claim(&q, 1, 60_000).await.unwrap();
    assert_eq!(claimed.len(), 1);
    fw.release(&q, [id])
        .await
        .unwrap_or_else(|e| panic!("{cell}: release: {e}"));
    let again = fw.claim(&q, 1, 60_000).await.unwrap();
    assert_eq!(
        again.len(),
        1,
        "{cell}: release must return item to pending"
    );
    assert_eq!(again[0].item_id, id);
    fw.ack(&q, [id]).await.unwrap();

    // --- retry ---
    let name = qname("retry");
    let q = qk(&name);
    fw.create_queue(queue_def(&name, false))
        .await
        .unwrap_or_else(|e| panic!("{cell}: create retry: {e}"));
    let id = fw
        .push(&q, item(&key("retry-1"), 1))
        .await
        .unwrap_or_else(|e| panic!("{cell}: push retry: {e}"));
    let claimed = fw.claim(&q, 1, 60_000).await.unwrap();
    assert_eq!(claimed.len(), 1);
    fw.retry(&q, [id], None)
        .await
        .unwrap_or_else(|e| panic!("{cell}: retry: {e}"));
    let again = fw.claim(&q, 1, 60_000).await.unwrap();
    assert_eq!(again.len(), 1, "{cell}: retry must re-eligibilize");
    assert_eq!(again[0].item_id, id);
    fw.ack(&q, [id]).await.unwrap();

    // --- rearm (recurring) ---
    let name = qname("rearm");
    let q = qk(&name);
    fw.create_queue(queue_def(&name, true))
        .await
        .unwrap_or_else(|e| panic!("{cell}: create rearm: {e}"));
    let id = fw
        .push(&q, item(&key("rearm-1"), 1))
        .await
        .unwrap_or_else(|e| panic!("{cell}: push rearm: {e}"));
    let claimed = fw.claim(&q, 1, 60_000).await.unwrap();
    assert_eq!(claimed.len(), 1);
    fw.rearm(&q, [id])
        .await
        .unwrap_or_else(|e| panic!("{cell}: rearm: {e}"));
    let again = fw.claim(&q, 1, 60_000).await.unwrap();
    assert_eq!(
        again.len(),
        1,
        "{cell}: rearm must return same id to pending"
    );
    assert_eq!(again[0].item_id, id, "{cell}: recurrence keeps same id");
    fw.ack(&q, [id]).await.unwrap();

    // --- race: only one claimer wins a single eligible item ---
    let name = qname("race");
    let q = qk(&name);
    fw.create_queue(queue_def(&name, false))
        .await
        .unwrap_or_else(|e| panic!("{cell}: create race: {e}"));
    let id = fw
        .push(&q, item(&key("race-1"), 1))
        .await
        .unwrap_or_else(|e| panic!("{cell}: push race: {e}"));
    let first = fw.claim(&q, 1, 60_000).await.unwrap();
    let second = fw.claim(&q, 1, 60_000).await.unwrap();
    assert_eq!(first.len(), 1, "{cell}: first claimer wins");
    assert_eq!(first[0].item_id, id);
    assert!(
        second.is_empty(),
        "{cell}: second claimer must observe empty batch"
    );
    fw.ack(&q, [id]).await.unwrap();

    // --- batch append (push_batch) ---
    let name = qname("append");
    let q = qk(&name);
    fw.create_queue(queue_def(&name, false))
        .await
        .unwrap_or_else(|e| panic!("{cell}: create append: {e}"));
    let ids = fw
        .push_batch(
            &q,
            vec![
                item(&key("append-a"), 1),
                item(&key("append-b"), 2),
                item(&key("append-c"), 3),
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("{cell}: push_batch: {e}"));
    assert_eq!(ids.len(), 3, "{cell}: batch append size");
    let claimed = fw.claim(&q, 10, 60_000).await.unwrap();
    assert_eq!(claimed.len(), 3, "{cell}: batch claim after append");
    for c in &claimed {
        fw.ack(&q, [c.item_id]).await.unwrap();
    }
}

async fn run_cell(cell: &str, open_fw: impl FnOnce() -> Fireweed) {
    let fw = open_fw();
    exercise_lifecycle(cell, &fw).await;
    drop(fw);
}

async fn run_cell_async(cell: &str, open_fw: impl std::future::Future<Output = Fireweed>) {
    let fw = open_fw.await;
    exercise_lifecycle(cell, &fw).await;
    drop(fw);
}

fn pg_url() -> Option<String> {
    std::env::var("FIREWEED_PG_TEST_URL")
        .ok()
        .filter(|s| !s.is_empty())
}

// ---------------------------------------------------------------------------
// Local deterministic cells (no live Postgres)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn p7n_memory_memory_lifecycle() {
    run_cell("memory--memory", || {
        fireweed::open_memory(Arc::new(SystemClock))
    })
    .await;
}

#[tokio::test]
async fn p7n_memory_sqlite_lifecycle() {
    let root = FixtureRoot::new("memory_sqlite");
    run_cell("memory--sqlite", || {
        let mut cfg = StorageConfig::memory();
        cfg.projection = ProjectionStoreConfig::Sqlite {
            path: root.path().join("projection.sqlite"),
        };
        open(cfg, Arc::new(SystemClock)).expect("open memory×sqlite")
    })
    .await;
}

#[tokio::test]
async fn p7n_sqlite_memory_lifecycle() {
    let root = FixtureRoot::new("sqlite_memory");
    run_cell("sqlite--memory", || {
        fireweed::open_sqlite(
            root.path().join("log.sqlite").to_str().unwrap(),
            Arc::new(SystemClock),
        )
        .expect("open sqlite×memory")
    })
    .await;
}

#[tokio::test]
async fn p7n_sqlite_sqlite_lifecycle() {
    let root = FixtureRoot::new("sqlite_sqlite");
    run_cell("sqlite--sqlite", || {
        fireweed::open_sqlite_relational(
            root.path().join("relational.sqlite").to_str().unwrap(),
            Arc::new(SystemClock),
        )
        .expect("open sqlite×sqlite")
    })
    .await;
}

#[tokio::test]
async fn p7n_filesystem_memory_lifecycle() {
    let root = FixtureRoot::new("filesystem_memory");
    run_cell("filesystem--memory", || {
        fireweed::open_objectlog(root.path().join("object-log"), Arc::new(SystemClock))
            .expect("open filesystem×memory")
    })
    .await;
}

#[tokio::test]
async fn p7n_filesystem_sqlite_lifecycle() {
    let root = FixtureRoot::new("filesystem_sqlite");
    run_cell("filesystem--sqlite", || {
        let cfg = fireweed::ObjectLogRuntimeConfig {
            object_log: fireweed::ObjectLogStorage::Local {
                root: root.path().join("object-log"),
            },
            authority: fireweed::ObjectLogAuthority::NativeConditionalWrite,
            projection: fireweed::ProjectionConfig::Sqlite {
                path: root.path().join("projection.sqlite"),
            },
            response_barrier: ResponseBarrier::Strict,
            segments: segments(),
            namespace: "p7n-filesystem-sqlite".into(),
            recovery: fireweed::RecoveryPolicy::default(),
        };
        fireweed::open_objectlog_sqlite(cfg, Arc::new(SystemClock)).expect("open filesystem×sqlite")
    })
    .await;
}

// ---------------------------------------------------------------------------
// Live Postgres cells — require `--features postgres` + FIREWEED_PG_TEST_URL
// ---------------------------------------------------------------------------

#[cfg(feature = "postgres")]
mod postgres_cells {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn p7n_memory_postgres_lifecycle() {
        let Some(url) = pg_url() else {
            panic!("p7n_memory_postgres_lifecycle: FIREWEED_PG_TEST_URL unset — skipping");
        };
        let mut cfg = StorageConfig::memory();
        cfg.projection = ProjectionStoreConfig::Postgres {
            url: ConfigSecret::new(url),
        };
        cfg.namespace = format!("p7n_mem_pg_{}", std::process::id());
        run_cell_async("memory--postgres", async {
            fireweed::open_async(cfg, Arc::new(SystemClock))
                .await
                .expect("open memory×postgres")
        })
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p7n_sqlite_postgres_lifecycle() {
        let Some(url) = pg_url() else {
            panic!("p7n_sqlite_postgres_lifecycle: FIREWEED_PG_TEST_URL unset — skipping");
        };
        let root = FixtureRoot::new("sqlite_postgres");
        let mut cfg = StorageConfig::memory();
        cfg.log = LogConfig::Sqlite {
            path: root.path().join("log.sqlite"),
        };
        cfg.projection = ProjectionStoreConfig::Postgres {
            url: ConfigSecret::new(url),
        };
        cfg.namespace = format!("p7n_sqlite_pg_{}", std::process::id());
        run_cell_async("sqlite--postgres", async {
            fireweed::open_async(cfg, Arc::new(SystemClock))
                .await
                .expect("open sqlite×postgres")
        })
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p7n_postgres_memory_lifecycle() {
        let Some(url) = pg_url() else {
            panic!("p7n_postgres_memory_lifecycle: FIREWEED_PG_TEST_URL unset — skipping");
        };
        run_cell_async("postgres--memory", async {
            fireweed::open_postgres_async(&url, Arc::new(SystemClock))
                .await
                .expect("open postgres×memory")
        })
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p7n_postgres_sqlite_lifecycle() {
        let Some(url) = pg_url() else {
            panic!("p7n_postgres_sqlite_lifecycle: FIREWEED_PG_TEST_URL unset — skipping");
        };
        let root = FixtureRoot::new("postgres_sqlite");
        let schema = format!("p7n_pg_sqlite_{}", std::process::id());
        let mut cfg = StorageConfig::memory();
        cfg.log = LogConfig::Postgres {
            url: ConfigSecret::new(url),
            schema: Some(schema.clone()),
            mode: PostgresMode::LogReplay,
            node_id: None,
            coordination: None,
        };
        cfg.projection = ProjectionStoreConfig::Sqlite {
            path: root.path().join("projection.sqlite"),
        };
        cfg.namespace = schema;
        run_cell_async("postgres--sqlite", async {
            fireweed::open_async(cfg, Arc::new(SystemClock))
                .await
                .expect("open postgres×sqlite")
        })
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p7n_postgres_postgres_lifecycle() {
        let Some(url) = pg_url() else {
            panic!("p7n_postgres_postgres_lifecycle: FIREWEED_PG_TEST_URL unset — skipping");
        };
        // Isolate relational schema so concurrent matrix runs do not collide.
        let schema = format!("p7n_pg_pg_{}", std::process::id());
        run_cell_async("postgres--postgres", async {
            fireweed::open_postgres_runtime_async(
                fireweed::PostgresRuntimeConfig {
                    url: ConfigSecret::new(url),
                    schema: Some(schema),
                    mode: PostgresMode::Relational,
                    node_id: None,
                    coordination: None,
                    claim_pool_size: 0,
                },
                Arc::new(SystemClock),
            )
            .await
            .expect("open postgres×postgres")
        })
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p7n_filesystem_postgres_lifecycle() {
        let Some(url) = pg_url() else {
            panic!("p7n_filesystem_postgres_lifecycle: FIREWEED_PG_TEST_URL unset — skipping");
        };
        let root = FixtureRoot::new("filesystem_postgres");
        let cfg = fireweed::ObjectLogRuntimeConfig {
            object_log: fireweed::ObjectLogStorage::Local {
                root: root.path().join("object-log"),
            },
            authority: fireweed::ObjectLogAuthority::NativeConditionalWrite,
            projection: fireweed::ProjectionConfig::Postgres {
                url: ConfigSecret::new(url),
            },
            response_barrier: ResponseBarrier::Strict,
            segments: segments(),
            namespace: format!("p7n-fs-pg-{}", std::process::id()),
            recovery: fireweed::RecoveryPolicy::default(),
        };
        run_cell_async("filesystem--postgres", async {
            fireweed::open_objectlog_postgres_async(cfg, Arc::new(SystemClock))
                .await
                .expect("open filesystem×postgres")
        })
        .await;
    }
}
