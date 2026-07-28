//! Request-id idempotency contract over the memory (atomic-class) backend, exercised through the public
//! `RuntimeCore` facade. Proves the retained-replay machinery the Snorri authoritative-commit boundary builds
//! on (ddx-pqueue-2201fd37): the caller's `request_id` propagates into the durable command envelope and
//! drives replay / conflict / expired outcomes.
//!
//! - same request id + same body  -> REPLAY the original ids, append nothing (no new item), disposition Replayed
//! - same request id + diff body   -> `RequestIdConflict`
//! - retry after the retention win -> treated as a fresh push (push semantics; the prior ids are gone)

use std::sync::Arc;

use fireweed::{EngineError, NewItem, PriorityValue, PushDisposition, RequestId, RuntimeCore};
use fireweed_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use fireweed_engine::QueueKey;
use fireweed_memory::{ManualClock, composed_memory_backend};

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
        max_rank_error: 0,
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
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
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
    let fireweed = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    let q = qkey();
    fireweed.create_queue(qdef(60_000)).await.unwrap();
    let rid = RequestId::new("snorri-txn-1").unwrap();

    let (first, first_disp) = fireweed
        .push_with_request_id(&q, rid.clone(), item(10))
        .await
        .unwrap();
    assert_eq!(first_disp, PushDisposition::Fresh);
    // Replay: identical body under the same request id returns the SAME id and appends nothing.
    let (replay, replay_disp) = fireweed
        .push_with_request_id(&q, rid.clone(), item(10))
        .await
        .unwrap();
    assert_eq!(replay_disp, PushDisposition::Replayed);
    assert_eq!(first, replay, "replay must return the original id");

    // Exactly one item exists (the replay did not enqueue a second one).
    let m = fireweed.metrics(&q).await.unwrap();
    assert_eq!(m.pending, 1, "replay must not enqueue a duplicate");
}

#[tokio::test]
async fn same_request_id_different_body_conflicts() {
    let fireweed = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    let q = qkey();
    fireweed.create_queue(qdef(60_000)).await.unwrap();
    let rid = RequestId::new("snorri-txn-2").unwrap();

    let (_, disp) = fireweed
        .push_with_request_id(&q, rid.clone(), item(10))
        .await
        .unwrap();
    assert_eq!(disp, PushDisposition::Fresh);
    // A different body under the same request id is a structural conflict — nothing appended.
    let err = fireweed
        .push_with_request_id(&q, rid.clone(), item(99))
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::RequestIdConflict);

    let m = fireweed.metrics(&q).await.unwrap();
    assert_eq!(
        m.pending, 1,
        "the conflicting body must not enqueue anything"
    );
}

#[tokio::test]
async fn retry_after_retention_window_is_a_fresh_push() {
    let clock = Arc::new(ManualClock::at(0));
    let fireweed = RuntimeCore::new(Arc::new(composed_memory_backend()), clock.clone());
    let q = qkey();
    // Short retention so a clock advance crosses the expiry boundary.
    fireweed.create_queue(qdef(1_000)).await.unwrap();
    let rid = RequestId::new("snorri-txn-3").unwrap();

    let (first, first_disp) = fireweed
        .push_with_request_id(&q, rid.clone(), item(10))
        .await
        .unwrap();
    assert_eq!(first_disp, PushDisposition::Fresh);

    // Advance past the retention window (1_000ms): the retained entry is now expired.
    clock.set(5);
    let (after_expiry, after_disp) = fireweed
        .push_with_request_id(&q, rid.clone(), item(10))
        .await
        .unwrap();
    assert_eq!(after_disp, PushDisposition::Fresh);

    // Push semantics: an expired entry is a genuinely new request, so a fresh item is appended
    // (different id) rather than replaying the old one.
    assert_ne!(
        first, after_expiry,
        "an expired request id must execute fresh, not replay"
    );
    let m = fireweed.metrics(&q).await.unwrap();
    assert_eq!(m.pending, 2, "expired retry must enqueue a second item");
}

#[tokio::test]
async fn distinct_request_ids_each_append() {
    let fireweed = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    let q = qkey();
    fireweed.create_queue(qdef(60_000)).await.unwrap();

    let (a, a_disp) = fireweed
        .push_with_request_id(&q, RequestId::new("a").unwrap(), item(10))
        .await
        .unwrap();
    let (b, b_disp) = fireweed
        .push_with_request_id(&q, RequestId::new("b").unwrap(), item(10))
        .await
        .unwrap();
    assert_eq!(a_disp, PushDisposition::Fresh);
    assert_eq!(b_disp, PushDisposition::Fresh);
    assert_ne!(a, b, "distinct request ids are distinct logical requests");
    assert_eq!(fireweed.metrics(&q).await.unwrap().pending, 2);
}

#[tokio::test]
async fn batch_push_reports_fresh_then_replayed_disposition() {
    let fireweed = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    let q = qkey();
    fireweed.create_queue(qdef(60_000)).await.unwrap();
    let rid = RequestId::new("snorri-batch-1").unwrap();
    let body = vec![item(10), item(20)];

    let first = fireweed
        .push_batch_with_request_id(&q, rid.clone(), body.clone())
        .await
        .unwrap();
    assert!(first.is_fresh());
    assert_eq!(first.len(), 2);

    let replay = fireweed
        .push_batch_with_request_id(&q, rid, body)
        .await
        .unwrap();
    assert!(replay.is_replayed());
    assert_eq!(replay.item_ids, first.item_ids);
    assert_eq!(fireweed.metrics(&q).await.unwrap().pending, 2);
}
