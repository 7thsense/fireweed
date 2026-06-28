//! Request-id idempotency contract over the memory (atomic-class) backend, exercised through the public
//! `Pqueue` facade. Proves the retained-replay machinery the Snorri authoritative-commit boundary builds
//! on (ddx-pqueue-2201fd37): the caller's `request_id` propagates into the durable command envelope and
//! drives replay / conflict / expired outcomes.
//!
//! - same request id + same body  -> REPLAY the original ids, append nothing (no new item)
//! - same request id + diff body   -> `RequestIdConflict`
//! - retry after the retention win -> treated as a fresh push (push semantics; the prior ids are gone)

use std::sync::Arc;

use pqueue::{EngineError, NewItem, Pqueue, PriorityValue, RequestId};
use pqueue_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use pqueue_engine::QueueKey;
use pqueue_memory::{ManualClock, MemoryBackend};

fn qkey() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}

fn qdef(request_id_retention_ms: u64) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t1").unwrap(),
        queue_id: QueueId::new("q1").unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
    }
}

fn item(priority: i64) -> NewItem {
    NewItem {
        priority: Some(PriorityValue::Int64(priority)),
        ..Default::default()
    }
}

#[tokio::test]
async fn same_request_id_same_body_replays_without_a_second_append() {
    let pq = Pqueue::new(Arc::new(MemoryBackend::new()), Arc::new(ManualClock::at(0)));
    let q = qkey();
    pq.create_queue(qdef(60_000)).await.unwrap();
    let rid = RequestId::new("snorri-txn-1").unwrap();

    let first = pq
        .push_with_request_id(&q, rid.clone(), item(10))
        .await
        .unwrap();
    // Replay: identical body under the same request id returns the SAME id and appends nothing.
    let replay = pq
        .push_with_request_id(&q, rid.clone(), item(10))
        .await
        .unwrap();
    assert_eq!(first, replay, "replay must return the original id");

    // Exactly one item exists (the replay did not enqueue a second one).
    let m = pq.metrics(&q).await.unwrap();
    assert_eq!(m.pending, 1, "replay must not enqueue a duplicate");
}

#[tokio::test]
async fn same_request_id_different_body_conflicts() {
    let pq = Pqueue::new(Arc::new(MemoryBackend::new()), Arc::new(ManualClock::at(0)));
    let q = qkey();
    pq.create_queue(qdef(60_000)).await.unwrap();
    let rid = RequestId::new("snorri-txn-2").unwrap();

    pq.push_with_request_id(&q, rid.clone(), item(10))
        .await
        .unwrap();
    // A different body under the same request id is a structural conflict — nothing appended.
    let err = pq
        .push_with_request_id(&q, rid.clone(), item(99))
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::RequestIdConflict);

    let m = pq.metrics(&q).await.unwrap();
    assert_eq!(
        m.pending, 1,
        "the conflicting body must not enqueue anything"
    );
}

#[tokio::test]
async fn retry_after_retention_window_is_a_fresh_push() {
    let clock = Arc::new(ManualClock::at(0));
    let pq = Pqueue::new(Arc::new(MemoryBackend::new()), clock.clone());
    let q = qkey();
    // Short retention so a clock advance crosses the expiry boundary.
    pq.create_queue(qdef(1_000)).await.unwrap();
    let rid = RequestId::new("snorri-txn-3").unwrap();

    let first = pq
        .push_with_request_id(&q, rid.clone(), item(10))
        .await
        .unwrap();

    // Advance past the retention window (1_000ms): the retained entry is now expired.
    clock.set(5);
    let after_expiry = pq
        .push_with_request_id(&q, rid.clone(), item(10))
        .await
        .unwrap();

    // Push semantics: an expired entry is a genuinely new request, so a fresh item is appended
    // (different id) rather than replaying the old one.
    assert_ne!(
        first, after_expiry,
        "an expired request id must execute fresh, not replay"
    );
    let m = pq.metrics(&q).await.unwrap();
    assert_eq!(m.pending, 2, "expired retry must enqueue a second item");
}

#[tokio::test]
async fn distinct_request_ids_each_append() {
    let pq = Pqueue::new(Arc::new(MemoryBackend::new()), Arc::new(ManualClock::at(0)));
    let q = qkey();
    pq.create_queue(qdef(60_000)).await.unwrap();

    let a = pq
        .push_with_request_id(&q, RequestId::new("a").unwrap(), item(10))
        .await
        .unwrap();
    let b = pq
        .push_with_request_id(&q, RequestId::new("b").unwrap(), item(10))
        .await
        .unwrap();
    assert_ne!(a, b, "distinct request ids are distinct logical requests");
    assert_eq!(pq.metrics(&q).await.unwrap().pending, 2);
}
