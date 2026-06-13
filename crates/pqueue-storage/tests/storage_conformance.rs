// B-020: storage trait conformance against the in-memory reference backend.
//
// These tests verify that the in-memory backend skeleton satisfies the
// TD-001 command envelope and position APIs, and provide a conformance harness
// that future backends can run against.

use pqueue_core::{
    CohortPolicy, ClientItemKey, EligibilityPolicy, ItemId, OrderingMode, PriorityModel,
    QueueCreationPolicy, QueueId, RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp, CreateQueue,
};
use pqueue_storage::{
    memory::{
        MemoryControlPlaneStore, MemoryLogStore, MemoryProjectionStore, MemorySnapshotStore,
    },
    traits::{
        ClaimRequest, ControlPlaneError, ControlPlaneStore, DurabilityProfile,
        LogStore, LogStoreError, ProjectionStore, ProjectionSnapshot, SnapshotStore,
    },
    types::{CommandPosition, QueueKey, ShardId, ShardKey},
    CommandEnvelope, CommandId, QueueCommand,
};
use pqueue_storage::commands::{
    BatchClaimCommand, BatchFinalizeCommand, BatchPushCommand, FinalizeKind, FinalizeOutcome,
    LeaseExpiredCommand, PushItem,
};

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

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

fn iid(s: &str) -> ItemId {
    ItemId::new(s).unwrap()
}

fn cik(s: &str) -> ClientItemKey {
    ClientItemKey::new(s).unwrap()
}

fn dummy_cmd(tenant: TenantId, queue: QueueId, shard_id: u32) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new("cmd-1"),
        request_id: None,
        tenant_id: tenant.clone(),
        queue_id: queue.clone(),
        shard_id: ShardId::new(shard_id),
        item_ids: vec![],
        command: QueueCommand::BatchPush(BatchPushCommand { items: vec![] }),
        checksum: pqueue_storage::types::CommandChecksum(0),
        created_at: ts(0),
    }
}

fn push_cmd(
    tenant: TenantId,
    queue: QueueId,
    shard_id: u32,
    items: Vec<PushItem>,
    cmd_id: &str,
) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new(cmd_id),
        request_id: None,
        tenant_id: tenant.clone(),
        queue_id: queue.clone(),
        shard_id: ShardId::new(shard_id),
        item_ids: items.iter().map(|i| i.item_id.clone()).collect(),
        command: QueueCommand::BatchPush(BatchPushCommand { items }),
        checksum: pqueue_storage::types::CommandChecksum(0),
        created_at: ts(0),
    }
}

fn claim_cmd(
    tenant: TenantId,
    queue: QueueId,
    shard_id: u32,
    item_ids: Vec<ItemId>,
    token: &str,
    expires_at: UtcTimestamp,
    cmd_id: &str,
) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new(cmd_id),
        request_id: None,
        tenant_id: tenant.clone(),
        queue_id: queue.clone(),
        shard_id: ShardId::new(shard_id),
        item_ids: item_ids.clone(),
        command: QueueCommand::BatchClaim(BatchClaimCommand {
            item_ids,
            lease_token: token.to_string(),
            lease_expires_at: expires_at,
        }),
        checksum: pqueue_storage::types::CommandChecksum(0),
        created_at: ts(0),
    }
}

fn finalize_cmd(
    tenant: TenantId,
    queue: QueueId,
    shard_id: u32,
    outcomes: Vec<FinalizeOutcome>,
    cmd_id: &str,
) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new(cmd_id),
        request_id: None,
        tenant_id: tenant.clone(),
        queue_id: queue.clone(),
        shard_id: ShardId::new(shard_id),
        item_ids: outcomes.iter().map(|o| o.item_id.clone()).collect(),
        command: QueueCommand::BatchFinalize(BatchFinalizeCommand { outcomes }),
        checksum: pqueue_storage::types::CommandChecksum(0),
        created_at: ts(0),
    }
}

fn lease_expired_cmd(
    tenant: TenantId,
    queue: QueueId,
    shard_id: u32,
    item_ids: Vec<ItemId>,
    cmd_id: &str,
) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new(cmd_id),
        request_id: None,
        tenant_id: tenant.clone(),
        queue_id: queue.clone(),
        shard_id: ShardId::new(shard_id),
        item_ids: item_ids.clone(),
        command: QueueCommand::LeaseExpired(LeaseExpiredCommand { item_ids }),
        checksum: pqueue_storage::types::CommandChecksum(0),
        created_at: ts(0),
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

// ---------------------------------------------------------------------------
// Durability conformance (B-020): items survive apply_committed round-trip
// ---------------------------------------------------------------------------

fn make_push_item(id: &str, key: &str, max_attempts: u32) -> PushItem {
    PushItem {
        item_id: iid(id),
        client_item_key: cik(key),
        priority: None,
        not_before: None,
        max_attempts,
        payload: None,
    }
}

#[tokio::test]
async fn storage_conformance_durability_push_items_visible_in_metrics() {
    let store = MemoryProjectionStore::new();
    let t = tenant();
    let q = qid("dur-push");
    let sk = shard(t.clone(), q.clone(), 0);
    let pos = CommandPosition { shard_key: sk.clone(), sequence: 0, backend_epoch: 0 };

    let items = vec![
        make_push_item("i1", "k1", 3),
        make_push_item("i2", "k2", 3),
    ];
    store.apply_committed(pos, &[push_cmd(t.clone(), q.clone(), 0, items, "cmd-1")]).await.unwrap();

    let qk = QueueKey { tenant_id: t, queue_id: q };
    let m = store.metrics(&qk).await.unwrap();
    assert_eq!(m.pending_count, 2);
    assert_eq!(m.leased_count, 0);
}

#[tokio::test]
async fn storage_conformance_durability_empty_batch_is_noop() {
    let store = MemoryProjectionStore::new();
    let t = tenant();
    let q = qid("dur-noop");
    let sk = shard(t.clone(), q.clone(), 0);
    let pos = CommandPosition { shard_key: sk.clone(), sequence: 0, backend_epoch: 0 };

    store.apply_committed(pos, &[push_cmd(t.clone(), q.clone(), 0, vec![], "cmd-1")]).await.unwrap();

    let qk = QueueKey { tenant_id: t, queue_id: q };
    let m = store.metrics(&qk).await.unwrap();
    assert_eq!(m.pending_count, 0);
}

#[tokio::test]
async fn storage_conformance_durability_multi_batch_accumulates() {
    let store = MemoryProjectionStore::new();
    let t = tenant();
    let q = qid("dur-multi");
    let sk = shard(t.clone(), q.clone(), 0);

    let pos0 = CommandPosition { shard_key: sk.clone(), sequence: 0, backend_epoch: 0 };
    store.apply_committed(
        pos0,
        &[push_cmd(t.clone(), q.clone(), 0, vec![make_push_item("i1", "k1", 3)], "cmd-1")],
    ).await.unwrap();

    let pos1 = CommandPosition { shard_key: sk.clone(), sequence: 1, backend_epoch: 0 };
    store.apply_committed(
        pos1,
        &[push_cmd(t.clone(), q.clone(), 0, vec![make_push_item("i2", "k2", 3)], "cmd-2")],
    ).await.unwrap();

    let qk = QueueKey { tenant_id: t, queue_id: q };
    let m = store.metrics(&qk).await.unwrap();
    assert_eq!(m.pending_count, 2);
}

#[tokio::test]
async fn storage_conformance_durability_metrics_missing_queue_is_error() {
    let store = MemoryProjectionStore::new();
    let qk = QueueKey { tenant_id: tenant(), queue_id: qid("ghost") };
    let err = store.metrics(&qk).await.unwrap_err();
    assert!(matches!(err, pqueue_storage::traits::ProjectionError::QueueNotFound));
}

// ---------------------------------------------------------------------------
// Claim conformance (B-020): batch_claim selects eligible Pending items
// ---------------------------------------------------------------------------

async fn push_and_claim_setup(
    store: &MemoryProjectionStore,
    t: TenantId,
    q: QueueId,
    items: Vec<PushItem>,
) -> ShardKey {
    let sk = shard(t.clone(), q.clone(), 0);
    let pos = CommandPosition { shard_key: sk.clone(), sequence: 0, backend_epoch: 0 };
    store.apply_committed(pos, &[push_cmd(t, q, 0, items, "cmd-push")]).await.unwrap();
    sk
}

#[tokio::test]
async fn storage_conformance_claim_returns_pending_items() {
    let store = MemoryProjectionStore::new();
    let t = tenant();
    let q = qid("claim-basic");
    let sk = push_and_claim_setup(
        &store, t.clone(), q.clone(),
        vec![make_push_item("i1", "k1", 3), make_push_item("i2", "k2", 3)],
    ).await;

    let result = store.batch_claim(ClaimRequest {
        shard_key: sk.clone(),
        max_items: 10,
        now: ts(1000),
        lease_token: "tok-1".to_string(),
        lease_expires_at: ts(2000),
    }).await.unwrap();

    assert_eq!(result.claimed_item_ids.len(), 2);
    assert_eq!(result.lease_token, "tok-1");
}

#[tokio::test]
async fn storage_conformance_claim_respects_max_items() {
    let store = MemoryProjectionStore::new();
    let t = tenant();
    let q = qid("claim-max");
    let sk = push_and_claim_setup(
        &store, t.clone(), q.clone(),
        vec![
            make_push_item("i1", "k1", 3),
            make_push_item("i2", "k2", 3),
            make_push_item("i3", "k3", 3),
        ],
    ).await;

    let result = store.batch_claim(ClaimRequest {
        shard_key: sk,
        max_items: 2,
        now: ts(1000),
        lease_token: "tok-1".to_string(),
        lease_expires_at: ts(2000),
    }).await.unwrap();

    assert_eq!(result.claimed_item_ids.len(), 2);
}

#[tokio::test]
async fn storage_conformance_claim_moves_items_to_leased() {
    let store = MemoryProjectionStore::new();
    let t = tenant();
    let q = qid("claim-state");
    let sk = push_and_claim_setup(
        &store, t.clone(), q.clone(),
        vec![make_push_item("i1", "k1", 3)],
    ).await;

    store.batch_claim(ClaimRequest {
        shard_key: sk,
        max_items: 10,
        now: ts(1000),
        lease_token: "tok-1".to_string(),
        lease_expires_at: ts(2000),
    }).await.unwrap();

    let qk = QueueKey { tenant_id: t, queue_id: q };
    let m = store.metrics(&qk).await.unwrap();
    assert_eq!(m.pending_count, 0);
    assert_eq!(m.leased_count, 1);
}

#[tokio::test]
async fn storage_conformance_claim_respects_not_before() {
    let store = MemoryProjectionStore::new();
    let t = tenant();
    let q = qid("claim-notbefore");
    let sk = shard(t.clone(), q.clone(), 0);
    let pos = CommandPosition { shard_key: sk.clone(), sequence: 0, backend_epoch: 0 };

    let future_item = PushItem {
        item_id: iid("i1"),
        client_item_key: cik("k1"),
        priority: None,
        not_before: Some(ts(9999)),
        max_attempts: 3,
        payload: None,
    };
    let ready_item = make_push_item("i2", "k2", 3);
    store.apply_committed(pos, &[push_cmd(t.clone(), q.clone(), 0, vec![future_item, ready_item], "cmd-1")]).await.unwrap();

    let result = store.batch_claim(ClaimRequest {
        shard_key: sk,
        max_items: 10,
        now: ts(1000),
        lease_token: "tok-1".to_string(),
        lease_expires_at: ts(2000),
    }).await.unwrap();

    // Only i2 should be claimed (i1 not_before is in future).
    assert_eq!(result.claimed_item_ids.len(), 1);
    assert_eq!(result.claimed_item_ids[0], iid("i2"));
}

#[tokio::test]
async fn storage_conformance_claim_empty_shard_returns_not_found() {
    let store = MemoryProjectionStore::new();
    let sk = shard(tenant(), qid("ghost"), 0);
    let err = store.batch_claim(ClaimRequest {
        shard_key: sk,
        max_items: 10,
        now: ts(1000),
        lease_token: "tok".to_string(),
        lease_expires_at: ts(2000),
    }).await.unwrap_err();
    assert!(matches!(err, pqueue_storage::traits::ProjectionError::QueueNotFound));
}

#[tokio::test]
async fn storage_conformance_claim_already_leased_items_skipped() {
    let store = MemoryProjectionStore::new();
    let t = tenant();
    let q = qid("claim-skip-leased");
    let sk = push_and_claim_setup(
        &store, t.clone(), q.clone(),
        vec![make_push_item("i1", "k1", 3), make_push_item("i2", "k2", 3)],
    ).await;

    // Claim i1 via apply_committed (simulates log-driven claim).
    let pos = CommandPosition { shard_key: sk.clone(), sequence: 1, backend_epoch: 0 };
    store.apply_committed(pos, &[claim_cmd(t.clone(), q.clone(), 0, vec![iid("i1")], "tok-1", ts(2000), "cmd-claim")]).await.unwrap();

    // batch_claim should only return i2.
    let result = store.batch_claim(ClaimRequest {
        shard_key: sk,
        max_items: 10,
        now: ts(1000),
        lease_token: "tok-2".to_string(),
        lease_expires_at: ts(3000),
    }).await.unwrap();

    assert_eq!(result.claimed_item_ids.len(), 1);
    assert_eq!(result.claimed_item_ids[0], iid("i2"));
}

// ---------------------------------------------------------------------------
// Progress conformance (B-020): finalize lifecycle transitions
// ---------------------------------------------------------------------------

async fn push_and_auto_claim(
    store: &MemoryProjectionStore,
    t: TenantId,
    q: QueueId,
    item_id: &str,
    token: &str,
) -> ShardKey {
    let sk = shard(t.clone(), q.clone(), 0);
    let pos0 = CommandPosition { shard_key: sk.clone(), sequence: 0, backend_epoch: 0 };
    store.apply_committed(pos0, &[push_cmd(t.clone(), q.clone(), 0, vec![make_push_item(item_id, item_id, 3)], "cmd-push")]).await.unwrap();

    let pos1 = CommandPosition { shard_key: sk.clone(), sequence: 1, backend_epoch: 0 };
    store.apply_committed(pos1, &[claim_cmd(t, q, 0, vec![iid(item_id)], token, ts(2000), "cmd-claim")]).await.unwrap();
    sk
}

#[tokio::test]
async fn storage_conformance_progress_finalize_complete() {
    let store = MemoryProjectionStore::new();
    let t = tenant();
    let q = qid("prog-complete");
    let sk = push_and_auto_claim(&store, t.clone(), q.clone(), "i1", "tok").await;

    let pos = CommandPosition { shard_key: sk, sequence: 2, backend_epoch: 0 };
    store.apply_committed(pos, &[finalize_cmd(
        t.clone(), q.clone(), 0,
        vec![FinalizeOutcome { item_id: iid("i1"), kind: FinalizeKind::Complete }],
        "cmd-fin",
    )]).await.unwrap();

    let qk = QueueKey { tenant_id: t, queue_id: q };
    let m = store.metrics(&qk).await.unwrap();
    assert_eq!(m.completed_count, 1);
    assert_eq!(m.leased_count, 0);
}

#[tokio::test]
async fn storage_conformance_progress_finalize_fail() {
    let store = MemoryProjectionStore::new();
    let t = tenant();
    let q = qid("prog-fail");
    let sk = push_and_auto_claim(&store, t.clone(), q.clone(), "i1", "tok").await;

    let pos = CommandPosition { shard_key: sk, sequence: 2, backend_epoch: 0 };
    store.apply_committed(pos, &[finalize_cmd(
        t.clone(), q.clone(), 0,
        vec![FinalizeOutcome { item_id: iid("i1"), kind: FinalizeKind::Fail }],
        "cmd-fin",
    )]).await.unwrap();

    let qk = QueueKey { tenant_id: t, queue_id: q };
    let m = store.metrics(&qk).await.unwrap();
    assert_eq!(m.failed_count, 1);
    assert_eq!(m.leased_count, 0);
}

#[tokio::test]
async fn storage_conformance_progress_finalize_retry_re_enqueues() {
    let store = MemoryProjectionStore::new();
    let t = tenant();
    let q = qid("prog-retry");
    let sk = push_and_auto_claim(&store, t.clone(), q.clone(), "i1", "tok").await;

    let pos = CommandPosition { shard_key: sk.clone(), sequence: 2, backend_epoch: 0 };
    store.apply_committed(pos, &[finalize_cmd(
        t.clone(), q.clone(), 0,
        vec![FinalizeOutcome { item_id: iid("i1"), kind: FinalizeKind::Retry }],
        "cmd-fin",
    )]).await.unwrap();

    let qk = QueueKey { tenant_id: t.clone(), queue_id: q.clone() };
    let m = store.metrics(&qk).await.unwrap();
    assert_eq!(m.pending_count, 1, "retried item should be Pending again");
    assert_eq!(m.leased_count, 0);

    // Should be claimable again.
    let result = store.batch_claim(ClaimRequest {
        shard_key: sk,
        max_items: 10,
        now: ts(1000),
        lease_token: "tok-2".to_string(),
        lease_expires_at: ts(3000),
    }).await.unwrap();
    assert_eq!(result.claimed_item_ids.len(), 1);
}

#[tokio::test]
async fn storage_conformance_progress_finalize_release_re_enqueues() {
    let store = MemoryProjectionStore::new();
    let t = tenant();
    let q = qid("prog-release");
    let sk = push_and_auto_claim(&store, t.clone(), q.clone(), "i1", "tok").await;

    let pos = CommandPosition { shard_key: sk.clone(), sequence: 2, backend_epoch: 0 };
    store.apply_committed(pos, &[finalize_cmd(
        t.clone(), q.clone(), 0,
        vec![FinalizeOutcome { item_id: iid("i1"), kind: FinalizeKind::Release }],
        "cmd-fin",
    )]).await.unwrap();

    let qk = QueueKey { tenant_id: t, queue_id: q };
    let m = store.metrics(&qk).await.unwrap();
    assert_eq!(m.pending_count, 1);
}

#[tokio::test]
async fn storage_conformance_progress_lease_expired_re_enqueues() {
    let store = MemoryProjectionStore::new();
    let t = tenant();
    let q = qid("prog-expired");
    let sk = push_and_auto_claim(&store, t.clone(), q.clone(), "i1", "tok").await;

    let pos = CommandPosition { shard_key: sk.clone(), sequence: 2, backend_epoch: 0 };
    store.apply_committed(pos, &[lease_expired_cmd(t.clone(), q.clone(), 0, vec![iid("i1")], "cmd-exp")]).await.unwrap();

    let qk = QueueKey { tenant_id: t, queue_id: q };
    let m = store.metrics(&qk).await.unwrap();
    assert_eq!(m.pending_count, 1, "expired lease should return item to Pending");
    assert_eq!(m.leased_count, 0);
}

#[tokio::test]
async fn storage_conformance_progress_full_lifecycle_complete() {
    let store = MemoryProjectionStore::new();
    let t = tenant();
    let q = qid("prog-full");
    let sk = shard(t.clone(), q.clone(), 0);

    // Push 2 items.
    let pos0 = CommandPosition { shard_key: sk.clone(), sequence: 0, backend_epoch: 0 };
    store.apply_committed(pos0, &[push_cmd(t.clone(), q.clone(), 0, vec![
        make_push_item("i1", "k1", 3),
        make_push_item("i2", "k2", 3),
    ], "cmd-push")]).await.unwrap();

    // Claim both.
    let pos1 = CommandPosition { shard_key: sk.clone(), sequence: 1, backend_epoch: 0 };
    store.apply_committed(pos1, &[claim_cmd(t.clone(), q.clone(), 0, vec![iid("i1"), iid("i2")], "tok", ts(5000), "cmd-claim")]).await.unwrap();

    // Complete i1, fail i2.
    let pos2 = CommandPosition { shard_key: sk, sequence: 2, backend_epoch: 0 };
    store.apply_committed(pos2, &[finalize_cmd(t.clone(), q.clone(), 0, vec![
        FinalizeOutcome { item_id: iid("i1"), kind: FinalizeKind::Complete },
        FinalizeOutcome { item_id: iid("i2"), kind: FinalizeKind::Fail },
    ], "cmd-fin")]).await.unwrap();

    let qk = QueueKey { tenant_id: t, queue_id: q };
    let m = store.metrics(&qk).await.unwrap();
    assert_eq!(m.pending_count, 0);
    assert_eq!(m.leased_count, 0);
    assert_eq!(m.completed_count, 1);
    assert_eq!(m.failed_count, 1);
}
