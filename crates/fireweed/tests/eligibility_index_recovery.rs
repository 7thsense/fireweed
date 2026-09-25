//! Reopen recovery must reseed item-id counters and keep the eligibility set
//! free of duplicate item ids after commit-transition lifecycle work.
//!
//! Hosted on the public `filesystem--turso` cell (rusqlite sqlite log retired).

#![cfg(all(feature = "objectlog", feature = "turso"))]

#[path = "support/storage.rs"]
mod storage;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use fireweed::{
    ClaimRef, Clock, CommitEntry, CommitRequest, EngineResult, EntryOutcome, FinalizeKind, NewItem,
    QueueDefinition, QueueKey, RequestId, SegmentConfig, StorageConfig, TenantId, UtcTimestamp,
};
use fireweed_core::{
    ClientItemKey, EligibilityPolicy, Metadata, MetadataValue, OrderingMode, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueId, RecurrencePolicy,
    RetryPolicy,
};

struct ManualClock(AtomicI64);

impl Clock for ManualClock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::new(self.0.load(Ordering::SeqCst), 0).expect("timestamp")
    }
}

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

fn qdef() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t").unwrap(),
        queue_id: QueueId::new("q").unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Timestamp,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 10_000,
        max_claim_batch_size: 10_000,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: false,
    }
}

fn config(root: &std::path::Path, namespace: &str) -> StorageConfig {
    let mut cfg = storage::product_config(root, namespace);
    cfg.segments = SegmentConfig {
        target_bytes: 256 * 1_024,
        max_latency_ms: 20,
    };
    cfg
}

fn unique_ids(items: &[fireweed::ClaimedItem]) {
    let ids: HashSet<_> = items.iter().map(|item| item.item_id).collect();
    assert_eq!(ids.len(), items.len(), "duplicate claimed item ids");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reopen_after_commit_transition_lifecycle_keeps_unique_eligible_and_fresh_ids()
-> EngineResult<()> {
    let root = std::env::temp_dir().join(format!(
        "fw-elig-recovery-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(root.join("log")).unwrap();
    let shard = QueueKey::new(TenantId::new("t").unwrap(), QueueId::new("q").unwrap());
    let clock = Arc::new(ManualClock(AtomicI64::new(10)));
    let cfg = config(&root, &storage::unique_namespace("elig-recovery"));

    let lifecycle_ids = {
        let fw = fireweed::open_async(cfg.clone(), Arc::clone(&clock) as _)
            .await
            .expect("open s3--turso");
        fw.create_queue(qdef()).await.expect("create queue");
        let mut metadata = Metadata::new();
        metadata.insert("native_transition_item", MetadataValue::String("1".into()));
        fw.upsert(
            &shard,
            ClientItemKey::new("stage-0").unwrap(),
            NewItem {
                client_item_key: Some(ClientItemKey::new("stage-0").unwrap()),
                priority: Some(PriorityValue::Timestamp(ts(20_000))),
                metadata,
                ..Default::default()
            },
        )
        .await
        .expect("upsert stage-0");
        let claimed = fw.claim(&shard, 1, 30_000).await.expect("claim stage-0");
        let c = &claimed[0];
        let outcomes = fw
            .commit(
                &shard,
                CommitRequest {
                    request_id: Some(RequestId::new("txn-elig-recovery").unwrap()),
                    entries: vec![CommitEntry {
                        claim_ref: ClaimRef {
                            item_id: c.item_id,
                            lease_token: c.lease_token.clone().unwrap(),
                            lease_expires_at: c.lease_expires_at,
                            item_version: c.item_version,
                        },
                        finalize: FinalizeKind::Complete,
                        side_records: Vec::new(),
                        lifecycle_items: vec![
                            NewItem {
                                priority: Some(PriorityValue::Timestamp(ts(5))),
                                not_before: Some(ts(5)),
                                ..Default::default()
                            },
                            NewItem {
                                priority: Some(PriorityValue::Timestamp(ts(6))),
                                not_before: None,
                                ..Default::default()
                            },
                        ],
                        instance_fence: None,
                    }],
                },
            )
            .await
            .expect("commit lifecycle transition");
        match &outcomes[0] {
            EntryOutcome::Committed { lifecycle_item_ids } => lifecycle_item_ids.clone(),
            other => panic!("expected commit, got {other:?}"),
        }
    };

    clock.0.store(100, Ordering::SeqCst);
    let fw = fireweed::open_async(cfg, Arc::clone(&clock) as _)
        .await
        .expect("reopen s3--turso");
    fw.create_queue(qdef())
        .await
        .expect("re-create queue after reopen");
    let peeked = fw.peek(&shard, 10_000).await.expect("peek after reopen");
    let unique: HashSet<_> = peeked.iter().map(|item| item.item_id).collect();
    assert_eq!(unique.len(), peeked.len(), "peek has duplicate item ids");
    assert_eq!(peeked.len(), 2, "unexpected eligible set after reopen");

    let mut metadata = Metadata::new();
    metadata.insert("native_transition_item", MetadataValue::String("1".into()));
    let upserted = fw
        .upsert(
            &shard,
            ClientItemKey::new("stage-1").unwrap(),
            NewItem {
                client_item_key: Some(ClientItemKey::new("stage-1").unwrap()),
                priority: Some(PriorityValue::Timestamp(ts(20_001))),
                metadata,
                ..Default::default()
            },
        )
        .await
        .expect("upsert stage-1");
    let new_id = match upserted {
        fireweed::UpsertOutcome::Inserted { item_id } => item_id,
        fireweed::UpsertOutcome::Replaced { new_item_id, .. } => new_item_id,
    };
    assert!(
        !lifecycle_ids.contains(&new_id),
        "post-reopen mint re-used a recovered item id {new_id:?}; counters were not reseeded"
    );

    let claimed = fw
        .claim(&shard, 8, 30_000)
        .await
        .expect("claim after reopen");
    assert_eq!(claimed.len(), 3);
    unique_ids(&claimed);

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}
