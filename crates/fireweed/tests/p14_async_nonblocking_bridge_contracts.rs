//! P14 async nonblocking + bounded sync bridge contracts [ddx-61324c64].
use fireweed::{
    ClientItemKey, ConfigSecret, LogConfig, NewItem, ObjectLogAuthority, OrderingMode,
    PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue,
    ProjectionStoreConfig, QueueDefinition, QueueId, QueueKey, RecoveryAction, RecoveryPolicy,
    RecurrencePolicy, ResponseBarrier, RetryPolicy, SegmentConfig, StorageConfig, SystemClock,
    TenantId, open, open_async,
};
use fireweed_engine::{
    EngineError, MIN_ASSIGNMENT_EPOCH, resolve_bounded_epoch, resolve_write_epoch,
    resolve_write_epoch_async, resolve_write_epoch_sync,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
static ORD: AtomicU64 = AtomicU64::new(0);
struct FixtureRoot(PathBuf);
impl FixtureRoot {
    fn new(label: &str) -> Self {
        let n = ORD.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("fireweed-p14-{}-{}-{n}", label, std::process::id()));
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
fn queue_def(name: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("p14").unwrap(),
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
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
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
fn filesystem_memory_config(root: PathBuf, namespace: String) -> StorageConfig {
    StorageConfig {
        log: LogConfig::Filesystem { root },
        projection: ProjectionStoreConfig::Memory,
        control_plane: None,
        authority: Some(ObjectLogAuthority::NativeConditionalWrite),
        response_barrier: ResponseBarrier::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
        segments: SegmentConfig::new(64 * 1024, 5).unwrap(),
        namespace,
        recovery: RecoveryPolicy {
            incompatible_projection: RecoveryAction::RebuildProjection,
            verify_checksums: true,
            max_tail_commands: 10_000,
        },
    }
}
#[test]
fn p14_bounded_sync_bridge_rejects_epoch_zero_ownership_stamps() {
    assert!(matches!(
        resolve_bounded_epoch(0, Some(0)),
        Err(EngineError::Invalid(_))
    ));
    assert_eq!(resolve_bounded_epoch(3, Some(3)).unwrap(), 3);
    assert_eq!(resolve_write_epoch_sync(None, || Ok(0)).unwrap(), 0);
    const { assert!(MIN_ASSIGNMENT_EPOCH >= 1) };
}
#[tokio::test(flavor = "current_thread")]
async fn p14_async_pre_resolution_is_pure_after_await() {
    assert_eq!(
        resolve_write_epoch_async(None, || async { Ok(0u64) })
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        resolve_write_epoch_async(Some(4), || async { Ok(4u64) })
            .await
            .unwrap(),
        4
    );
    assert!(matches!(
        resolve_write_epoch_async(Some(0), || async { Ok(0u64) }).await,
        Err(EngineError::Invalid(_))
    ));
    assert!(matches!(
        resolve_write_epoch(1, Some(2)),
        Err(EngineError::EpochFenced)
    ));
}
#[tokio::test(flavor = "current_thread")]
async fn p14_filesystem_memory_keeps_single_thread_heartbeat_live() {
    let root = FixtureRoot::new("fs-hb");
    let clock: Arc<dyn fireweed::Clock> = Arc::new(SystemClock);
    let finished = Arc::new(AtomicBool::new(false));
    let ticks = Arc::new(AtomicU64::new(0));
    let hb_done = Arc::clone(&finished);
    let hb_ticks = Arc::clone(&ticks);
    let heartbeat = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(1));
        while !hb_done.load(Ordering::Acquire) {
            interval.tick().await;
            hb_ticks.fetch_add(1, Ordering::Relaxed);
        }
    });
    while ticks.load(Ordering::Acquire) == 0 {
        tokio::task::yield_now().await;
    }
    let ticks_before = ticks.load(Ordering::Relaxed);
    let fw = open_async(
        filesystem_memory_config(root.path().to_path_buf(), "p14-hb".into()),
        Arc::clone(&clock),
    )
    .await
    .unwrap();
    let def = queue_def("hb-fs");
    let key = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    fw.create_queue(def).await.unwrap();
    let item = fw
        .push(
            &key,
            NewItem {
                priority: Some(PriorityValue::Int64(1)),
                client_item_key: Some(ClientItemKey::new("k1").unwrap()),
                ..NewItem::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(fw.claim(&key, 1, 30_000).await.unwrap()[0].item_id, item);
    let _ = fw.metrics(&key).await.unwrap();
    let _ = fw.peek(&key, 8).await.unwrap();
    finished.store(true, Ordering::Release);
    heartbeat.await.unwrap();
    assert!(ticks.load(Ordering::Relaxed) > ticks_before);
}
#[tokio::test(flavor = "current_thread")]
async fn p14_filesystem_memory_recovery_read_keeps_heartbeat_live() {
    let root = FixtureRoot::new("fs-recover");
    let clock: Arc<dyn fireweed::Clock> = Arc::new(SystemClock);
    let cfg = filesystem_memory_config(root.path().to_path_buf(), "p14-recover".into());
    let def = queue_def("recover-fs");
    let key = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    {
        let fw = open(cfg.clone(), Arc::clone(&clock)).unwrap();
        fw.create_queue(def.clone()).await.unwrap();
        fw.push(
            &key,
            NewItem {
                priority: Some(PriorityValue::Int64(7)),
                ..NewItem::default()
            },
        )
        .await
        .unwrap();
    }
    let finished = Arc::new(AtomicBool::new(false));
    let ticks = Arc::new(AtomicU64::new(0));
    let hb_done = Arc::clone(&finished);
    let hb_ticks = Arc::clone(&ticks);
    let heartbeat = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(1));
        while !hb_done.load(Ordering::Acquire) {
            interval.tick().await;
            hb_ticks.fetch_add(1, Ordering::Relaxed);
        }
    });
    while ticks.load(Ordering::Acquire) == 0 {
        tokio::task::yield_now().await;
    }
    let ticks_before = ticks.load(Ordering::Relaxed);
    let reopened = open_async(cfg, Arc::clone(&clock)).await.unwrap();
    reopened.create_queue(def).await.unwrap();
    let m = reopened.metrics(&key).await.unwrap();
    assert!(m.pending + m.leased + m.complete >= 1);
    let _ = reopened.peek(&key, 4).await.unwrap();
    finished.store(true, Ordering::Release);
    heartbeat.await.unwrap();
    assert!(ticks.load(Ordering::Relaxed) > ticks_before);
}
#[tokio::test(flavor = "current_thread")]
async fn p14_s3_memory_keeps_single_thread_heartbeat_live() {
    if std::env::var("FIREWEED_S3_TEST_ENDPOINT").is_err() {
        eprintln!("P14_S3_SKIP: FIREWEED_S3_TEST_ENDPOINT unset");
        return;
    }
    let clock: Arc<dyn fireweed::Clock> = Arc::new(SystemClock);
    let finished = Arc::new(AtomicBool::new(false));
    let ticks = Arc::new(AtomicU64::new(0));
    let hb_done = Arc::clone(&finished);
    let hb_ticks = Arc::clone(&ticks);
    let heartbeat = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(1));
        while !hb_done.load(Ordering::Acquire) {
            interval.tick().await;
            hb_ticks.fetch_add(1, Ordering::Relaxed);
        }
    });
    while ticks.load(Ordering::Acquire) == 0 {
        tokio::task::yield_now().await;
    }
    let ticks_before = ticks.load(Ordering::Relaxed);
    let cfg = StorageConfig {
        log: LogConfig::S3 {
            endpoint: std::env::var("FIREWEED_S3_TEST_ENDPOINT").unwrap(),
            bucket: std::env::var("FIREWEED_S3_TEST_BUCKET").unwrap_or_else(|_| "fireweed".into()),
            region: std::env::var("FIREWEED_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into()),
            access_key_id: ConfigSecret::new(
                std::env::var("FIREWEED_S3_TEST_ACCESS_KEY")
                    .unwrap_or_else(|_| "minioadmin".into()),
            ),
            secret_access_key: ConfigSecret::new(
                std::env::var("FIREWEED_S3_TEST_SECRET_KEY")
                    .unwrap_or_else(|_| "minioadmin".into()),
            ),
            allow_insecure_http: true,
        },
        projection: ProjectionStoreConfig::Memory,
        control_plane: None,
        authority: Some(ObjectLogAuthority::NativeConditionalWrite),
        response_barrier: ResponseBarrier::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
        segments: SegmentConfig::new(64 * 1024, 5).unwrap(),
        namespace: format!("p14-s3-{}", std::process::id()),
        recovery: RecoveryPolicy {
            incompatible_projection: RecoveryAction::RebuildProjection,
            verify_checksums: true,
            max_tail_commands: 10_000,
        },
    };
    let fw = open_async(cfg, Arc::clone(&clock)).await.unwrap();
    let def = queue_def("hb-s3");
    let key = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    fw.create_queue(def).await.unwrap();
    fw.push(
        &key,
        NewItem {
            priority: Some(PriorityValue::Int64(1)),
            ..NewItem::default()
        },
    )
    .await
    .unwrap();
    let _ = fw.claim(&key, 1, 30_000).await.unwrap();
    let _ = fw.metrics(&key).await.unwrap();
    finished.store(true, Ordering::Release);
    heartbeat.await.unwrap();
    assert!(ticks.load(Ordering::Relaxed) > ticks_before);
}
#[test]
fn p14_request_id_ownership_stamp_never_silently_uses_epoch_zero() {
    assert!(matches!(
        resolve_bounded_epoch(0, Some(0)),
        Err(EngineError::Invalid(_))
    ));
    assert_eq!(resolve_write_epoch(0, None).unwrap(), 0);
}
#[tokio::test(flavor = "current_thread")]
async fn p14_filesystem_open_sync_path_uses_bounded_bridge_under_current_thread() {
    let root = FixtureRoot::new("fs-sync-open");
    let clock: Arc<dyn fireweed::Clock> = Arc::new(SystemClock);
    let fw = open(
        filesystem_memory_config(root.path().to_path_buf(), "p14-sync".into()),
        clock,
    )
    .unwrap();
    let def = queue_def("sync-open");
    let key = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    fw.create_queue(def).await.unwrap();
    fw.push(
        &key,
        NewItem {
            priority: Some(PriorityValue::Int64(1)),
            ..NewItem::default()
        },
    )
    .await
    .unwrap();
}
