// B-020: storage trait conformance against the in-memory reference backend.
//
// These tests verify that the in-memory backend skeleton satisfies the
// TD-001 command envelope and position APIs, and provide a conformance harness
// that future backends can run against.

use pqueue_core::{
    CohortPolicy, EligibilityPolicy, OrderingMode, PriorityModel, QueueCreationPolicy,
    QueueId, RecurrencePolicy, RetryPolicy, TenantId, CreateQueue,
};
use pqueue_storage::{
    memory::{
        MemoryControlPlaneStore, MemoryLogStore, MemoryProjectionStore, MemorySnapshotStore,
    },
    traits::{
        ControlPlaneError, ControlPlaneStore, DurabilityProfile, LogStore, LogStoreError,
        ProjectionStore, ProjectionSnapshot, SnapshotStore,
    },
    types::{CommandPosition, QueueKey, ShardId, ShardKey},
    CommandEnvelope, CommandId, QueueCommand,
};
use pqueue_storage::commands::BatchPushCommand;

fn tenant() -> TenantId {
    TenantId::new("test-tenant").unwrap()
}

fn qid(s: &str) -> QueueId {
    QueueId::new(s).unwrap()
}

fn simple_queue_def(tenant: TenantId, queue: QueueId) -> pqueue_core::QueueDefinition {
    let create = CreateQueue {
        tenant_id: tenant,
        queue_id: queue,
        priority_model: PriorityModel::timestamp_ascending(),
        ordering_mode: OrderingMode::Strict,
        group_co_residency: false,
        progress_bound_ms: 30_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: CohortPolicy::disabled(),
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 3_600_000,
        client_item_key_retention_ms: 86_400_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 50,
        max_eligible_group_size: None,
        shard_count: Some(1),
    };
    create.validate(&QueueCreationPolicy::default()).unwrap()
}

fn shard(tenant: TenantId, queue: QueueId, shard_id: u32) -> ShardKey {
    ShardKey { tenant_id: tenant, queue_id: queue, shard_id: ShardId::new(shard_id) }
}

fn dummy_cmd(tenant: TenantId, queue: QueueId, shard_id: u32) -> CommandEnvelope {
    use pqueue_core::UtcTimestamp;
    CommandEnvelope {
        command_id: CommandId::new("cmd-1"),
        request_id: None,
        tenant_id: tenant.clone(),
        queue_id: queue.clone(),
        shard_id: ShardId::new(shard_id),
        item_ids: vec![],
        command: QueueCommand::BatchPush(BatchPushCommand { items: vec![] }),
        checksum: pqueue_storage::types::CommandChecksum(0),
        created_at: UtcTimestamp::new(0, 0).unwrap(),
    }
}

// ---------------------------------------------------------------------------
// ControlPlaneStore conformance
// ---------------------------------------------------------------------------

#[tokio::test]
async fn storage_conformance_control_plane_create_and_read_queue() {
    let store = MemoryControlPlaneStore::new();
    let t = tenant();
    let q = qid("orders");
    let def = simple_queue_def(t.clone(), q.clone());

    let result = store.create_queue(def.clone()).await.unwrap();
    assert!(result.created);
    assert_eq!(result.definition.queue_id, q);

    let key = QueueKey { tenant_id: t.clone(), queue_id: q.clone() };
    let fetched = store.queue_definition(&key).await.unwrap();
    assert_eq!(fetched.queue_id, q);
}

#[tokio::test]
async fn storage_conformance_control_plane_duplicate_create_is_error() {
    let store = MemoryControlPlaneStore::new();
    let t = tenant();
    let q = qid("dups");
    let def = simple_queue_def(t.clone(), q.clone());

    store.create_queue(def.clone()).await.unwrap();
    let err = store.create_queue(def).await.unwrap_err();
    assert_eq!(err, ControlPlaneError::QueueAlreadyExists);
}

#[tokio::test]
async fn storage_conformance_control_plane_missing_queue_is_error() {
    let store = MemoryControlPlaneStore::new();
    let key = QueueKey { tenant_id: tenant(), queue_id: qid("ghost") };
    let err = store.queue_definition(&key).await.unwrap_err();
    assert_eq!(err, ControlPlaneError::QueueNotFound);
}

#[tokio::test]
async fn storage_conformance_control_plane_shard_assignments() {
    let store = MemoryControlPlaneStore::new();
    let t = tenant();
    let q = qid("sharded");
    let def = simple_queue_def(t.clone(), q.clone());

    store.create_queue(def).await.unwrap();
    let key = QueueKey { tenant_id: t, queue_id: q };
    let shards = store.shard_assignments(&key).await.unwrap();
    assert_eq!(shards.len(), 1, "single-shard queue should have one assignment");
    assert_eq!(shards[0].epoch, 1);
}

#[tokio::test]
async fn storage_conformance_control_plane_list_queues() {
    let store = MemoryControlPlaneStore::new();
    let t = tenant();
    let q1 = qid("q1");
    let q2 = qid("q2");
    store.create_queue(simple_queue_def(t.clone(), q1.clone())).await.unwrap();
    store.create_queue(simple_queue_def(t.clone(), q2.clone())).await.unwrap();

    let mut listed = store.list_queues(&t).await.unwrap();
    listed.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let names: Vec<&str> = listed.iter().map(|q| q.as_str()).collect();
    assert_eq!(names, vec!["q1", "q2"]);
}

// ---------------------------------------------------------------------------
// LogStore conformance
// ---------------------------------------------------------------------------

#[tokio::test]
async fn storage_conformance_log_store_append_and_read() {
    let store = MemoryLogStore::new();
    let t = tenant();
    let q = qid("log-test");
    let sk = shard(t.clone(), q.clone(), 0);

    let cmd = dummy_cmd(t, q, 0);
    let result = store.append_batch(&sk, None, vec![cmd]).await.unwrap();
    assert_eq!(result.last_position.sequence, 0);

    let page = store.read_from(&sk, None, 10).await.unwrap();
    assert_eq!(page.commands.len(), 1);
    assert!(page.next_position.is_none());
}

#[tokio::test]
async fn storage_conformance_log_store_multiple_appends_sequential() {
    let store = MemoryLogStore::new();
    let t = tenant();
    let q = qid("seq-test");
    let sk = shard(t.clone(), q.clone(), 0);

    for i in 0..5u32 {
        let cmd = dummy_cmd(t.clone(), q.clone(), 0);
        let res = store.append_batch(&sk, None, vec![cmd]).await.unwrap();
        assert_eq!(res.last_position.sequence, i as u64);
    }

    let page = store.read_from(&sk, None, 10).await.unwrap();
    assert_eq!(page.commands.len(), 5);
}

#[tokio::test]
async fn storage_conformance_log_store_read_from_position() {
    let store = MemoryLogStore::new();
    let t = tenant();
    let q = qid("page-test");
    let sk = shard(t.clone(), q.clone(), 0);

    for _ in 0..4 {
        store.append_batch(&sk, None, vec![dummy_cmd(t.clone(), q.clone(), 0)]).await.unwrap();
    }

    // Read from position 1 (after sequence 0).
    let first_pos = CommandPosition {
        shard_key: sk.clone(),
        sequence: 0,
        backend_epoch: 0,
    };
    let page = store.read_from(&sk, Some(first_pos), 10).await.unwrap();
    // Sequences 1, 2, 3
    assert_eq!(page.commands.len(), 3);
    assert_eq!(page.commands[0].0.sequence, 1);
}

#[tokio::test]
async fn storage_conformance_log_store_stale_epoch_rejected() {
    let store = MemoryLogStore::new();
    let t = tenant();
    let q = qid("epoch-test");
    let sk = shard(t.clone(), q.clone(), 0);

    // First append establishes the shard with epoch 0.
    store.append_batch(&sk, None, vec![dummy_cmd(t.clone(), q.clone(), 0)]).await.unwrap();

    // Now attempt with a wrong expected epoch (epoch 99 != actual 0).
    let err = store
        .append_batch(&sk, Some(99), vec![dummy_cmd(t.clone(), q.clone(), 0)])
        .await
        .unwrap_err();
    assert!(matches!(err, LogStoreError::StalEpoch { .. }));
}

#[tokio::test]
async fn storage_conformance_log_store_uninitialized_shard_read_fails() {
    let store = MemoryLogStore::new();
    let sk = shard(tenant(), qid("ghost"), 0);
    let err = store.read_from(&sk, None, 10).await.unwrap_err();
    assert_eq!(err, LogStoreError::ShardNotFound);
}

#[tokio::test]
async fn storage_conformance_log_store_durability_profile_is_none() {
    let store = MemoryLogStore::new();
    assert_eq!(store.durability_profile(), DurabilityProfile::None);
}

// ---------------------------------------------------------------------------
// SnapshotStore conformance
// ---------------------------------------------------------------------------

#[tokio::test]
async fn storage_conformance_snapshot_write_and_read() {
    let store = MemorySnapshotStore::new();
    let t = tenant();
    let q = qid("snap-test");
    let sk = shard(t.clone(), q.clone(), 0);
    let pos = CommandPosition { shard_key: sk.clone(), sequence: 5, backend_epoch: 1 };

    let snap = ProjectionSnapshot { payload: b"state".to_vec() };
    let ref_ = store.write_snapshot(&sk, pos.clone(), snap).await.unwrap();
    assert_eq!(ref_.shard_key, sk);
    assert_eq!(ref_.position.sequence, 5);

    let latest = store.latest_snapshot(&sk).await.unwrap();
    assert!(latest.is_some());

    let loaded = store.read_snapshot(&ref_).await.unwrap();
    assert_eq!(loaded.payload, b"state");
}

#[tokio::test]
async fn storage_conformance_snapshot_no_snapshot_returns_none() {
    let store = MemorySnapshotStore::new();
    let sk = shard(tenant(), qid("empty"), 0);
    let result = store.latest_snapshot(&sk).await.unwrap();
    assert!(result.is_none());
}

// ---------------------------------------------------------------------------
// ProjectionStore conformance
// ---------------------------------------------------------------------------

#[tokio::test]
async fn storage_conformance_projection_apply_committed_succeeds() {
    let store = MemoryProjectionStore::new();
    let t = tenant();
    let q = qid("proj-test");
    let sk = shard(t.clone(), q.clone(), 0);
    let pos = CommandPosition { shard_key: sk.clone(), sequence: 0, backend_epoch: 0 };

    store.apply_committed(pos, &[]).await.unwrap();
}
