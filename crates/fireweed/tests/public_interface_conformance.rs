//! Provider-neutral shared API-005 public-interface suite (local cells).
//!
//! Cell IDs use the authority-manifest separator (`log--projection[--variant]`).
//! Provider brand strings are forbidden in fixtures; live S3 provenance is P4s.
//! Method coverage is discovery-derived via `scripts/ci/api005_suite_ownership.py`.

#[path = "support/public_interface.rs"]
mod public_interface;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed::{
    ConfigSecret, Fireweed, LogConfig, ObjectLogAuthority, ProjectionStoreConfig, RecoveryPolicy,
    ResponseBarrier, SegmentConfig, StorageConfig, SystemClock,
};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

struct FixtureRoot(PathBuf);

impl FixtureRoot {
    fn new(cell: &str) -> Self {
        let ordinal = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "fireweed-public-interface-{cell}-{}-{ordinal}",
            std::process::id()
        ));
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

async fn assert_cell(
    cell: &str,
    expect_projection_control: bool,
    expect_atomic_commit: bool,
    fireweed: Fireweed,
) {
    if expect_atomic_commit {
        public_interface::run(cell, &fireweed, expect_projection_control).await;
    } else {
        public_interface::run_with_commit_boundary(
            cell,
            &fireweed,
            expect_projection_control,
            expect_atomic_commit,
        )
        .await;
    }
    drop(fireweed);
}

fn objectlog_storage(
    log: LogConfig,
    projection: ProjectionStoreConfig,
    namespace: &str,
) -> StorageConfig {
    StorageConfig {
        log,
        projection,
        control_plane: None,
        authority: Some(ObjectLogAuthority::NativeConditionalWrite),
        response_barrier: ResponseBarrier::AsyncProjection,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
        segments: SegmentConfig::new(262_144, 20).unwrap(),
        namespace: namespace.into(),
        recovery: RecoveryPolicy::default(),
    }
}

#[test]
fn objectlog_authority_validation_accepts_native_conditional_write() {
    let root = FixtureRoot::new("authority-validation");
    let retired = objectlog_storage(
        LogConfig::Filesystem {
            root: root.path().join("object-log"),
        },
        ProjectionStoreConfig::Memory,
        "authority-validation",
    )
    .validate()
    .expect_err("filesystem × memory is retired");
    assert!(retired.to_string().contains("s3 log"));

    objectlog_storage(
        LogConfig::S3 {
            endpoint: "http://127.0.0.1:9".into(),
            bucket: "fixture".into(),
            region: "us-east-1".into(),
            access_key_id: ConfigSecret::new("fixture-key"),
            secret_access_key: ConfigSecret::new("fixture-secret"),
            allow_insecure_http: true,
        },
        ProjectionStoreConfig::Turso {
            path: root.path().join("projection.db"),
        },
        "authority-validation-s3",
    )
    .validate()
    .unwrap();
}

#[tokio::test]
async fn memory_memory_public_interface() {
    let root = FixtureRoot::new("s3--turso");
    assert_cell(
        "s3--turso",
        true,
        true,
        filesystem_turso(root.path(), ResponseBarrier::AsyncProjection, "s3-turso").await,
    )
    .await;
}

#[cfg(all(feature = "objectlog", feature = "turso"))]
#[tokio::test]
async fn filesystem_turso_strict_public_interface() {
    let root = FixtureRoot::new("s3--turso--strict");
    assert_cell(
        "s3--turso--strict",
        true,
        true,
        filesystem_turso(
            root.path(),
            ResponseBarrier::AsyncProjection,
            "s3-turso-strict",
        )
        .await,
    )
    .await;
}

#[cfg(all(feature = "objectlog", feature = "turso"))]
#[tokio::test]
async fn filesystem_turso_async_public_interface() {
    let root = FixtureRoot::new("s3--turso--async");
    assert_cell(
        "s3--turso--async",
        true,
        true,
        filesystem_turso(
            root.path(),
            ResponseBarrier::AsyncProjection,
            "s3-turso-async",
        )
        .await,
    )
    .await;
}

#[cfg(all(feature = "objectlog", feature = "turso"))]
#[tokio::test]
async fn filesystem_turso_projection_control_rebuilds_from_log() {
    let root = FixtureRoot::new("filesystem--turso--rebuild");
    let fireweed = filesystem_turso(
        root.path(),
        ResponseBarrier::AsyncProjection,
        "filesystem-turso-rebuild",
    )
    .await;
    let definition = fireweed::QueueDefinition {
        tenant_id: fireweed::TenantId::new("rebuild").unwrap(),
        queue_id: fireweed::QueueId::new("work").unwrap(),
        priority_model: fireweed::PriorityModel {
            kind: fireweed::PriorityModelKind::Int64,
            direction: fireweed::PriorityDirection::Ascending,
            tie_breaker: fireweed::PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: fireweed::OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: fireweed::EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: fireweed::RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: fireweed::RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: false,
    };
    let queue = fireweed::QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    assert!(fireweed.create_queue(definition).await.unwrap().created);
    let item_id = fireweed
        .push(
            &queue,
            fireweed::NewItem {
                client_item_key: Some(fireweed::ClientItemKey::new("rebuild-item").unwrap()),
                priority: Some(fireweed::PriorityValue::Int64(1)),
                ..fireweed::NewItem::default()
            },
        )
        .await
        .unwrap();
    let control = fireweed
        .projection_control()
        .expect("filesystem×turso projection control");
    let before = control.verify().await.unwrap();
    assert!(
        before.compatible,
        "projection must match the log before delete"
    );
    control.delete().await.unwrap();
    let rebuilt = control.rebuild().await.unwrap();
    assert!(
        rebuilt.projection_sequence > 0 || rebuilt.tail_commands_replayed > 0,
        "rebuild must replay log commands: {rebuilt:?}"
    );
    let after = control.verify().await.unwrap();
    assert!(after.compatible, "rebuilt projection must match the log");
    assert_eq!(fireweed.metrics(&queue).await.unwrap().pending, 1);
    let live = fireweed
        .live_item(
            &queue,
            fireweed::ClientItemKey::new("rebuild-item").unwrap(),
        )
        .await
        .unwrap()
        .expect("item survives projection rebuild");
    assert_eq!(live.item_id, item_id);
}

#[cfg(all(feature = "objectlog", feature = "turso"))]
#[tokio::test]
async fn filesystem_turso_async_same_handle_claims_commit_continuation() {
    let root = FixtureRoot::new("filesystem--turso--continuation");
    let fw = filesystem_turso(
        root.path(),
        ResponseBarrier::AsyncProjection,
        "filesystem-turso-continuation",
    )
    .await;
    let definition = fireweed::QueueDefinition {
        tenant_id: fireweed::TenantId::new("cont").unwrap(),
        queue_id: fireweed::QueueId::new("work").unwrap(),
        priority_model: fireweed::PriorityModel {
            kind: fireweed::PriorityModelKind::Int64,
            direction: fireweed::PriorityDirection::Ascending,
            tie_breaker: fireweed::PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: fireweed::OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: fireweed::EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: fireweed::RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: fireweed::RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: false,
    };
    let q = fireweed::QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    assert!(fw.create_queue(definition).await.unwrap().created);
    fw.push_batch(
        &q,
        vec![fireweed::NewItem {
            client_item_key: Some(fireweed::ClientItemKey::new("stage-0").unwrap()),
            priority: Some(fireweed::PriorityValue::Int64(1)),
            payload: Some(b"zero".to_vec().into()),
            ..Default::default()
        }],
    )
    .await
    .unwrap();
    let claimed = fw.claim(&q, 1, 60_000).await.unwrap();
    assert_eq!(claimed.len(), 1);
    let caps = fw.commit_capabilities(&q).unwrap();
    assert_eq!(
        caps.durability_class,
        fireweed::DurabilityClass::EventualApply
    );
    let committed = fw
        .commit(
            &q,
            fireweed::CommitRequest {
                request_id: Some(fireweed::RequestId::new("cont-1").unwrap()),
                entries: vec![fireweed::CommitEntry {
                    claim_ref: fireweed::ClaimRef {
                        item_id: claimed[0].item_id,
                        lease_token: claimed[0].lease_token.clone().unwrap(),
                        lease_expires_at: claimed[0].lease_expires_at,
                        item_version: claimed[0].item_version,
                    },
                    finalize: fireweed::FinalizeKind::Complete,
                    side_records: vec![],
                    lifecycle_items: vec![fireweed::NewItem {
                        client_item_key: Some(fireweed::ClientItemKey::new("stage-1").unwrap()),
                        priority: Some(fireweed::PriorityValue::Int64(2)),
                        payload: Some(b"one".to_vec().into()),
                        ..Default::default()
                    }],
                    instance_fence: Some(fireweed::InstanceFence {
                        instance_key: b"inst".to_vec(),
                        expected: 0,
                        next: 1,
                    }),
                }],
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        committed.as_slice(),
        [fireweed::EntryOutcome::Committed { lifecycle_item_ids }] if lifecycle_item_ids.len() == 1
    ));
    let next = fw.claim(&q, 1, 60_000).await.unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].client_item_key.as_str(), "stage-1");
}

#[cfg(all(feature = "objectlog", feature = "turso"))]
#[tokio::test]
async fn filesystem_turso_async_packed_commits_each_return_outcomes() {
    let root = FixtureRoot::new("filesystem--turso--packed-commit");
    let fw = filesystem_turso(
        root.path(),
        ResponseBarrier::AsyncProjection,
        "filesystem-turso-packed-commit",
    )
    .await;
    let definition = fireweed::QueueDefinition {
        tenant_id: fireweed::TenantId::new("pack").unwrap(),
        queue_id: fireweed::QueueId::new("work").unwrap(),
        priority_model: fireweed::PriorityModel {
            kind: fireweed::PriorityModelKind::Int64,
            direction: fireweed::PriorityDirection::Ascending,
            tie_breaker: fireweed::PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: fireweed::OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: fireweed::EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: fireweed::RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: fireweed::RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: false,
    };
    let q = fireweed::QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    assert!(fw.create_queue(definition).await.unwrap().created);
    fw.push_batch(
        &q,
        vec![
            fireweed::NewItem {
                client_item_key: Some(fireweed::ClientItemKey::new("a").unwrap()),
                priority: Some(fireweed::PriorityValue::Int64(1)),
                payload: Some(b"a".to_vec().into()),
                ..Default::default()
            },
            fireweed::NewItem {
                client_item_key: Some(fireweed::ClientItemKey::new("b").unwrap()),
                priority: Some(fireweed::PriorityValue::Int64(2)),
                payload: Some(b"b".to_vec().into()),
                ..Default::default()
            },
        ],
    )
    .await
    .unwrap();
    let claimed = fw.claim(&q, 2, 60_000).await.unwrap();
    assert_eq!(claimed.len(), 2);
    let commit =
        |item: &fireweed::ClaimedItem, rid: &str, next_key: &str, expected: u64, next: u64| {
            fw.commit(
                &q,
                fireweed::CommitRequest {
                    request_id: Some(fireweed::RequestId::new(rid).unwrap()),
                    entries: vec![fireweed::CommitEntry {
                        claim_ref: fireweed::ClaimRef {
                            item_id: item.item_id,
                            lease_token: item.lease_token.clone().unwrap(),
                            lease_expires_at: item.lease_expires_at,
                            item_version: item.item_version,
                        },
                        finalize: fireweed::FinalizeKind::Complete,
                        side_records: vec![],
                        lifecycle_items: vec![fireweed::NewItem {
                            client_item_key: Some(fireweed::ClientItemKey::new(next_key).unwrap()),
                            priority: Some(fireweed::PriorityValue::Int64(3)),
                            payload: Some(next_key.as_bytes().to_vec().into()),
                            ..Default::default()
                        }],
                        instance_fence: Some(fireweed::InstanceFence {
                            instance_key: next_key.as_bytes().to_vec(),
                            expected,
                            next,
                        }),
                    }],
                },
            )
        };
    let (left, right) = tokio::join!(
        commit(&claimed[0], "pack-a", "next-a", 0, 1),
        commit(&claimed[1], "pack-b", "next-b", 0, 1),
    );
    for (name, result) in [("a", left), ("b", right)] {
        let outcomes = result.unwrap_or_else(|error| panic!("{name} commit: {error}"));
        assert!(
            matches!(
                outcomes.as_slice(),
                [fireweed::EntryOutcome::Committed { lifecycle_item_ids }]
                    if lifecycle_item_ids.len() == 1
            ),
            "{name} outcomes: {outcomes:?}"
        );
    }
    let next = fw.claim(&q, 8, 60_000).await.unwrap();
    let keys: std::collections::HashSet<_> = next
        .iter()
        .map(|item| item.client_item_key.as_str().to_string())
        .collect();
    assert!(
        keys.contains("next-a") && keys.contains("next-b"),
        "packed continuations missing from claim: {keys:?}"
    );
}

#[cfg(all(feature = "objectlog", feature = "turso"))]
async fn filesystem_turso(root: &Path, barrier: ResponseBarrier, namespace: &str) -> Fireweed {
    let s3 = fireweed_objectlog::shared_s3_test_env();
    let mut storage = StorageConfig::s3_turso(
        s3.endpoint.clone(),
        s3.bucket.clone(),
        s3.region.clone(),
        s3.access_key.clone(),
        s3.secret_key.clone(),
        s3.allow_insecure_http(),
        root.join("projection.db"),
    );
    storage.namespace = format!(
        "{namespace}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    storage.response_barrier = barrier;
    storage.async_projection =
        (barrier == ResponseBarrier::AsyncProjection).then(fireweed::AsyncProjectionSpec::default);
    fireweed::open_async(storage, Arc::new(SystemClock))
        .await
        .unwrap()
}
