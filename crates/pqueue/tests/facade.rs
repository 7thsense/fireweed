//! The ergonomic library facade exercised over real backends (memory = atomic class; objectlog =
//! eventual-apply class), proving the singular verbs compose the engine ports correctly.

use std::sync::Arc;

use pqueue::{EngineError, Nack, NewItem, Pqueue};
use pqueue_core::{
    ClientItemKey, EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId, RecurrencePolicy,
    RetryPolicy, TenantId,
};
use pqueue_engine::QueueKey;
use pqueue_memory::{ManualClock, MemoryBackend};

fn qkey() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}

fn qdef() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t1").unwrap(),
        queue_id: QueueId::new("q1").unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        group_co_residency: false,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        shard_count: 1,
    }
}

fn at(priority: i64) -> NewItem {
    NewItem {
        priority: Some(PriorityValue::Int64(priority)),
        ..Default::default()
    }
}

#[tokio::test]
async fn push_claim_ack_nack_lifecycle_over_memory() {
    let backend = Arc::new(MemoryBackend::new());
    let clock = Arc::new(ManualClock::at(0));
    let pq = Pqueue::new(backend, clock);
    let q = qkey();
    pq.create_queue(qdef()).await.unwrap();

    // push out of priority order.
    for p in [30, 10, 20] {
        pq.push(&q, at(p)).await.unwrap();
    }

    // peek is priority-ordered (ascending Int64): 10, 20, 30.
    let peeked: Vec<i64> = pq
        .peek(&q, 10)
        .await
        .unwrap()
        .iter()
        .map(|v| match v.priority {
            Some(PriorityValue::Int64(n)) => n,
            _ => -1,
        })
        .collect();
    assert_eq!(peeked, vec![10, 20, 30]);

    // claim 2 highest-priority → 10, 20; both leased.
    let claimed = pq.claim(&q, 2, 30_000).await.unwrap();
    let claimed_pri: Vec<i64> = claimed
        .iter()
        .map(|c| match c.priority {
            Some(PriorityValue::Int64(n)) => n,
            _ => -1,
        })
        .collect();
    assert_eq!(claimed_pri, vec![10, 20]);
    let m = pq.metrics(&q).await.unwrap();
    assert_eq!((m.pending, m.leased), (1, 2));

    // ack them → complete.
    pq.ack(&q, claimed.iter().map(|c| c.item_id.clone()))
        .await
        .unwrap();
    let m = pq.metrics(&q).await.unwrap();
    assert_eq!((m.complete, m.leased), (2, 0));

    // claim the last (30), nack Retry → back to pending, claimable again with a bumped attempt.
    let last = pq.claim(&q, 1, 30_000).await.unwrap();
    assert_eq!(last.len(), 1);
    assert_eq!(last[0].attempt_count, 1);
    pq.nack(&q, last.iter().map(|c| c.item_id.clone()), Nack::Retry)
        .await
        .unwrap();
    let again = pq.claim(&q, 1, 30_000).await.unwrap();
    assert_eq!(again.len(), 1, "retried item is claimable again");
    assert!(again[0].attempt_count > 1, "redelivery bumps attempt_count");
}

#[tokio::test]
async fn upsert_dedups_on_client_item_key_over_memory() {
    let backend = Arc::new(MemoryBackend::new());
    let clock = Arc::new(ManualClock::at(0));
    let pq = Pqueue::new(backend, clock);
    let q = qkey();
    pq.create_queue(qdef()).await.unwrap();

    let key = ClientItemKey::new("dup").unwrap();
    pq.upsert(&q, key.clone(), at(50)).await.unwrap();
    pq.upsert(&q, key, at(20)).await.unwrap(); // replaces the pending item

    let m = pq.metrics(&q).await.unwrap();
    assert_eq!(m.pending, 1, "same key upserts to a single pending item");
    let peeked: Vec<i64> = pq
        .peek(&q, 10)
        .await
        .unwrap()
        .iter()
        .map(|v| match v.priority {
            Some(PriorityValue::Int64(n)) => n,
            _ => -1,
        })
        .collect();
    assert_eq!(peeked, vec![20], "the replacement's priority survives");
}

#[tokio::test]
async fn objectlog_push_works_but_upsert_is_unavailable() {
    use pqueue_objectlog::ObjectLogBackend;
    let root = std::env::temp_dir().join(format!("pqueue-facade-objlog-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let backend = Arc::new(ObjectLogBackend::open(&root).unwrap());
    let clock = Arc::new(ManualClock::at(0));
    let pq = Pqueue::new(backend, clock);
    let q = qkey();
    pq.create_queue(qdef()).await.unwrap();

    // push (append) works on the eventual-apply class...
    pq.push(&q, at(5)).await.unwrap();
    assert_eq!(pq.metrics(&q).await.unwrap().pending, 1);

    // ...but upsert (atomic XDEL+XADD) is refused with the structured `Unavailable`.
    let err = pq
        .upsert(&q, ClientItemKey::new("k").unwrap(), at(5))
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::Unavailable);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn two_handles_on_one_backend_do_not_collide_ids() {
    // B2 regression: ids are backend-assigned (not a per-handle counter), so two `Pqueue` handles
    // sharing one backend mint DISTINCT item ids and both items coexist.
    let backend = Arc::new(MemoryBackend::new());
    let clock = Arc::new(ManualClock::at(0));
    let a = Pqueue::new(backend.clone(), clock.clone());
    let b = Pqueue::new(backend.clone(), clock);
    let q = qkey();
    a.create_queue(qdef()).await.unwrap();

    let id_a = a.push(&q, at(10)).await.unwrap();
    let id_b = b.push(&q, at(20)).await.unwrap();
    assert_ne!(id_a, id_b, "distinct backend-assigned ids across handles");
    assert_eq!(
        a.metrics(&q).await.unwrap().pending,
        2,
        "both pushes coexist (no silent overwrite)"
    );
}

#[tokio::test]
async fn ack_of_non_leased_id_is_a_structured_error() {
    let backend = Arc::new(MemoryBackend::new());
    let pq = Pqueue::new(backend, Arc::new(ManualClock::at(0)));
    let q = qkey();
    pq.create_queue(qdef()).await.unwrap();
    let id = pq.push(&q, at(5)).await.unwrap(); // pending, never claimed
    let err = pq.ack(&q, [id]).await.unwrap_err();
    assert_eq!(
        err,
        EngineError::Invalid("item is not leased"),
        "ack of a never-leased item is rejected, not a silent success"
    );
}

#[tokio::test]
async fn fail_dead_letters_a_claimed_item() {
    let backend = Arc::new(MemoryBackend::new());
    let pq = Pqueue::new(backend, Arc::new(ManualClock::at(0)));
    let q = qkey();
    pq.create_queue(qdef()).await.unwrap();
    pq.push(&q, at(5)).await.unwrap();
    let claimed = pq.claim(&q, 1, 30_000).await.unwrap();
    pq.fail(&q, claimed.iter().map(|c| c.item_id.clone())).await.unwrap();
    let m = pq.metrics(&q).await.unwrap();
    assert_eq!((m.failed, m.leased), (1, 0), "fail moves the item to terminal failed");
}
