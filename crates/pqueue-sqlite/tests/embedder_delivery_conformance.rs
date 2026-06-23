//! Embedder delivery-adapter conformance suite (TD-005 / ADR-006; bead
//! pqueue-9ff01321), driven through the embedded `SqliteBackend` surface.
//!
//! This is distinct from the backend-author conformance (`shared_conformance.rs`,
//! which proves storage-trait parity). This suite exercises the DELIVERY-shaped
//! semantics an embedded host (7snx) relies on: push delivery work, a worker
//! claims it, finalize complete/fail, retry and expired-lease re-pending, and
//! durability across a host restart. It maps to 7snx's
//! `assert_delivery_queue_adapter_conformance` (sevensnx-interfaces testkit):
//! the host's `DeliveryQueueAdapter` over the sqlite backend must satisfy these.
//!
//! client_item_key boundary (bead flag): 7snx's adapter conformance treats a
//! re-push of the same `client_item_key` as a convergent duplicate (returns 0),
//! but that dedupe lives in the 7snx adapter (`pushed_client_keys`), NOT in
//! pqueue. pqueue converges by `item_id` (the delivery work id). The test
//! `pqueue_dedupes_by_item_id_not_client_item_key` pins this boundary so the
//! adapter responsibility is explicit. See bead pqueue-9ff01321 / TP-001.

use pqueue_core::{
    ClientItemKey, CohortPolicy, CreateQueue, EligibilityPolicy, ItemId, OrderingMode,
    PriorityModel, PriorityValue, QueueCreationPolicy, QueueDefinition, QueueId, RecurrencePolicy,
    RetryPolicy, TenantId, UtcTimestamp,
};
use pqueue_sqlite::SqliteBackend;
use pqueue_storage::commands::{FinalizeKind, FinalizeOutcome, PushItem};
use pqueue_storage::types::{QueueKey, ShardId, ShardKey};

fn tenant() -> TenantId {
    TenantId::new("tenant_conformance").unwrap()
}
fn run() -> QueueId {
    QueueId::new("run_conformance").unwrap()
}
fn shard() -> ShardKey {
    ShardKey {
        tenant_id: tenant(),
        queue_id: run(),
        shard_id: ShardId::new(0),
    }
}
fn qk() -> QueueKey {
    QueueKey {
        tenant_id: tenant(),
        queue_id: run(),
    }
}
fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

fn queue_def() -> QueueDefinition {
    CreateQueue {
        tenant_id: tenant(),
        queue_id: run(),
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
    }
    .validate(&QueueCreationPolicy::default())
    .unwrap()
}

/// A delivery work item: `delivery_work_id` is the pqueue `item_id`,
/// `client_item_key` is the adapter's idempotency key.
fn work(delivery_work_id: &str, client_item_key: &str) -> PushItem {
    PushItem {
        client_item_key: ClientItemKey::new(client_item_key).unwrap(),
        item_id: ItemId::new(delivery_work_id).unwrap(),
        priority: Some(PriorityValue::Int64(10)),
        not_before: Some(ts(0)),
        max_attempts: 3,
        payload: None,
    }
}

fn complete(id: &str) -> FinalizeOutcome {
    FinalizeOutcome {
        item_id: ItemId::new(id).unwrap(),
        kind: FinalizeKind::Complete,
    }
}
fn fail(id: &str) -> FinalizeOutcome {
    FinalizeOutcome {
        item_id: ItemId::new(id).unwrap(),
        kind: FinalizeKind::Fail,
    }
}
fn retry(id: &str) -> FinalizeOutcome {
    FinalizeOutcome {
        item_id: ItemId::new(id).unwrap(),
        kind: FinalizeKind::Retry,
    }
}

async fn new_run() -> SqliteBackend {
    let backend = SqliteBackend::open_in_memory().unwrap();
    backend.create_queue(queue_def()).await.unwrap();
    backend
}

/// Mirrors 7snx `assert_delivery_queue_adapter_conformance`: push → claim →
/// retry re-pends → reclaim → complete.
#[tokio::test]
async fn delivery_push_claim_retry_complete_lifecycle() {
    let backend = new_run().await;
    backend
        .push(
            &shard(),
            vec![work("work_1", "tenant/run/recipient_1/email")],
        )
        .await
        .unwrap();

    let pending = backend.metrics(&qk()).await.unwrap();
    assert_eq!(pending.pending_count, 1);
    assert_eq!(pending.leased_count, 0);

    let claimed = backend
        .claim(&shard(), 1, "lease_1", ts(100), ts(200))
        .await
        .unwrap();
    assert_eq!(claimed, vec![ItemId::new("work_1").unwrap()]);
    let leased = backend.metrics(&qk()).await.unwrap();
    assert_eq!(leased.pending_count, 0);
    assert_eq!(leased.leased_count, 1);

    // Transient failure -> retry re-pends the work.
    backend
        .finalize(&shard(), vec![retry("work_1")])
        .await
        .unwrap();
    assert_eq!(backend.metrics(&qk()).await.unwrap().pending_count, 1);

    // A worker reclaims and completes it.
    let again = backend
        .claim(&shard(), 1, "lease_2", ts(200), ts(300))
        .await
        .unwrap();
    assert_eq!(again.len(), 1);
    backend
        .finalize(&shard(), vec![complete("work_1")])
        .await
        .unwrap();
    let done = backend.metrics(&qk()).await.unwrap();
    assert_eq!(done.completed_count, 1);
    assert_eq!(done.failed_count, 0);
    assert_eq!(done.leased_count, 0);
}

#[tokio::test]
async fn delivery_terminal_failure_is_terminal() {
    let backend = new_run().await;
    backend
        .push(&shard(), vec![work("work_1", "k1")])
        .await
        .unwrap();
    backend
        .claim(&shard(), 1, "lease", ts(100), ts(200))
        .await
        .unwrap();
    backend
        .finalize(&shard(), vec![fail("work_1")])
        .await
        .unwrap();

    let m = backend.metrics(&qk()).await.unwrap();
    assert_eq!(m.failed_count, 1);
    assert_eq!(m.leased_count, 0);
    assert_eq!(m.pending_count, 0);
    // Terminal: nothing left to claim.
    let claimed = backend
        .claim(&shard(), 10, "lease-2", ts(300), ts(400))
        .await
        .unwrap();
    assert!(claimed.is_empty());
}

#[tokio::test]
async fn delivery_expired_lease_returns_work_to_pending() {
    let backend = new_run().await;
    backend
        .push(&shard(), vec![work("work_1", "k1")])
        .await
        .unwrap();
    backend
        .claim(&shard(), 1, "lease", ts(100), ts(200))
        .await
        .unwrap();
    backend
        .expire_leases(&shard(), vec![ItemId::new("work_1").unwrap()])
        .await
        .unwrap();

    let m = backend.metrics(&qk()).await.unwrap();
    assert_eq!(m.pending_count, 1, "expired lease returns work to pending");
    assert_eq!(m.leased_count, 0);
}

#[tokio::test]
async fn delivery_duplicate_work_id_converges() {
    let backend = new_run().await;
    backend
        .push(&shard(), vec![work("work_1", "k1")])
        .await
        .unwrap();
    // Re-delivering the same delivery_work_id converges (no duplicate item) —
    // this is what lets an adapter keyed on delivery_work_id report a re-push as
    // a convergent duplicate.
    backend
        .push(&shard(), vec![work("work_1", "k1")])
        .await
        .unwrap();
    assert_eq!(backend.metrics(&qk()).await.unwrap().pending_count, 1);
}

#[tokio::test]
async fn pqueue_dedupes_by_item_id_not_client_item_key() {
    let backend = new_run().await;
    // Two DIFFERENT delivery_work_ids that share ONE client_item_key. pqueue
    // converges by item_id, so it keeps BOTH — client_item_key dedupe is the
    // embedder adapter's responsibility (7snx `pushed_client_keys`), NOT pqueue.
    backend
        .push(
            &shard(),
            vec![
                work("work_a", "same/client/item/key"),
                work("work_b", "same/client/item/key"),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        backend.metrics(&qk()).await.unwrap().pending_count,
        2,
        "pqueue does not dedupe by client_item_key; the adapter must"
    );
}

/// The durability point B6 exists for: delivery state survives a host restart
/// (FEAT-006), because append+apply commit atomically to one file.
#[tokio::test]
async fn delivery_state_survives_host_restart() {
    let path = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("delivery-{}.db", std::process::id()));
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    {
        let backend = SqliteBackend::open_durable(&path).unwrap();
        backend.create_queue(queue_def()).await.unwrap();
        backend
            .push(&shard(), vec![work("work_1", "k1"), work("work_2", "k2")])
            .await
            .unwrap();
        backend
            .claim(&shard(), 1, "lease", ts(100), ts(200))
            .await
            .unwrap();
        // host process exits here (backend dropped, file lock released)
    }
    let restarted = SqliteBackend::open_durable(&path).unwrap();
    let m = restarted.metrics(&qk()).await.unwrap();
    assert_eq!(m.leased_count, 1, "leased work survives restart");
    assert_eq!(m.pending_count, 1, "pending work survives restart");
    drop(restarted);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}
