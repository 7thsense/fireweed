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

/// fireweed-01802c42: async sqlite log-replay product must rebuild the push request-id ledger on
/// recovery-on-open so same-body replays and changed-body conflicts survive a close/reopen.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn request_id_conflict_and_replay_survive_sqlite_async_reopen() {
    use fireweed::open_sqlite;
    let path = std::env::temp_dir().join(format!(
        "fw-request-id-reopen-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    let path_str = path.to_str().unwrap();
    let rid = RequestId::new("reopen-rid-1").unwrap();
    let empty_rid = RequestId::new("reopen-empty-1").unwrap();
    let q = qkey();
    let first = {
        let fw = open_sqlite(path_str, Arc::new(ManualClock::at(0))).unwrap();
        fw.create_queue(qdef(60_000)).await.unwrap();
        assert!(
            fw.commit_capabilities(&q)
                .unwrap()
                .retained_commit_idempotency,
            "async sqlite product must advertise retained_commit_idempotency"
        );
        let (id, disp) = fw
            .push_with_request_id(&q, rid.clone(), item(10))
            .await
            .unwrap();
        assert_eq!(disp, PushDisposition::Fresh);
        let (id2, disp2) = fw
            .push_with_request_id(&q, rid.clone(), item(10))
            .await
            .unwrap();
        assert_eq!(disp2, PushDisposition::Replayed);
        assert_eq!(id, id2);
        let err = fw
            .push_with_request_id(&q, rid.clone(), item(99))
            .await
            .unwrap_err();
        assert_eq!(err, EngineError::RequestIdConflict);

        // Empty batch under a request_id is a durable no-op with retained conflict/replay.
        let empty_first = fw
            .push_batch_with_request_id(&q, empty_rid.clone(), vec![])
            .await
            .unwrap();
        assert!(empty_first.is_fresh());
        assert!(empty_first.item_ids.is_empty());
        let empty_replay = fw
            .push_batch_with_request_id(&q, empty_rid.clone(), vec![])
            .await
            .unwrap();
        assert!(empty_replay.is_replayed());
        let empty_conflict = fw
            .push_batch_with_request_id(&q, empty_rid.clone(), vec![item(1)])
            .await
            .unwrap_err();
        assert_eq!(empty_conflict, EngineError::RequestIdConflict);
        id
    };

    let reopened = open_sqlite(path_str, Arc::new(ManualClock::at(0))).unwrap();
    reopened.create_queue(qdef(60_000)).await.unwrap();
    assert!(
        reopened
            .commit_capabilities(&q)
            .unwrap()
            .retained_commit_idempotency
    );
    let (replay_id, replay_disp) = reopened
        .push_with_request_id(&q, rid.clone(), item(10))
        .await
        .unwrap();
    assert_eq!(
        replay_disp,
        PushDisposition::Replayed,
        "same body after reopen must Replayed"
    );
    assert_eq!(replay_id, first);
    let err = reopened
        .push_with_request_id(&q, rid, item(99))
        .await
        .unwrap_err();
    assert_eq!(
        err,
        EngineError::RequestIdConflict,
        "changed body after reopen must RequestIdConflict"
    );

    let empty_after = reopened
        .push_batch_with_request_id(&q, empty_rid.clone(), vec![])
        .await
        .unwrap();
    assert!(
        empty_after.is_replayed(),
        "empty request_id body must Replayed after reopen"
    );
    let empty_nonempty = reopened
        .push_batch_with_request_id(&q, empty_rid, vec![item(1)])
        .await
        .unwrap_err();
    assert_eq!(empty_nonempty, EngineError::RequestIdConflict);

    drop(reopened);
    let _ = std::fs::remove_file(&path);
}

/// fireweed-01802c42: batch push disposition Fresh → Replayed and changed-body conflict across reopen.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn batch_request_id_replay_and_conflict_survive_sqlite_async_reopen() {
    use fireweed::open_sqlite;
    let path = std::env::temp_dir().join(format!(
        "fw-request-id-batch-reopen-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    let path_str = path.to_str().unwrap();
    let rid = RequestId::new("reopen-batch-1").unwrap();
    let q = qkey();
    let body = vec![item(10), item(20)];
    let first = {
        let fw = open_sqlite(path_str, Arc::new(ManualClock::at(0))).unwrap();
        fw.create_queue(qdef(60_000)).await.unwrap();
        let outcome = fw
            .push_batch_with_request_id(&q, rid.clone(), body.clone())
            .await
            .unwrap();
        assert!(outcome.is_fresh());
        outcome.item_ids.clone()
    };
    let reopened = open_sqlite(path_str, Arc::new(ManualClock::at(0))).unwrap();
    reopened.create_queue(qdef(60_000)).await.unwrap();
    let replay = reopened
        .push_batch_with_request_id(&q, rid.clone(), body)
        .await
        .unwrap();
    assert!(
        replay.is_replayed(),
        "batch same body after reopen must Replayed"
    );
    assert_eq!(replay.item_ids, first);
    let conflict = reopened
        .push_batch_with_request_id(&q, rid, vec![item(11), item(22)])
        .await;
    assert_eq!(conflict.unwrap_err(), EngineError::RequestIdConflict);
    drop(reopened);
    let _ = std::fs::remove_file(&path);
}

/// fireweed-6486ed63: empty request_id batch must retain fingerprint so a later non-empty body
/// under the same id returns RequestIdConflict (snorri workflow_enqueue empty→nonempty).
/// Repeated create_queue mirrors snorri's enqueue path.
#[cfg(feature = "memory")]
#[tokio::test]
async fn empty_request_id_then_nonempty_conflicts_on_memory() {
    use fireweed::open_memory;
    let fw = open_memory(Arc::new(ManualClock::at(0)));
    let q = qkey();
    let def = qdef(60_000);
    fw.create_queue(def.clone()).await.unwrap();
    let rid = RequestId::new("empty-then-full").unwrap();
    fw.create_queue(def.clone()).await.unwrap();
    let empty = fw
        .push_batch_with_request_id(&q, rid.clone(), vec![])
        .await
        .unwrap();
    assert!(empty.is_fresh(), "empty first is Fresh");
    assert!(empty.item_ids.is_empty());
    fw.create_queue(def.clone()).await.unwrap();
    let empty_replay = fw
        .push_batch_with_request_id(&q, rid.clone(), vec![])
        .await
        .unwrap();
    assert!(
        empty_replay.is_replayed(),
        "empty same-body must Replayed; got {:?}",
        empty_replay.disposition
    );
    fw.create_queue(def).await.unwrap();
    let err = fw
        .push_batch_with_request_id(&q, rid, vec![item(1)])
        .await
        .unwrap_err();
    assert_eq!(
        err,
        EngineError::RequestIdConflict,
        "empty then nonempty must RequestIdConflict; got {err:?}"
    );
}

/// fireweed-6486ed63: changed-body-across-reopen only (batch), after empty request_id is also retained.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn changed_body_request_id_conflicts_across_sqlite_async_reopen() {
    use fireweed::open_sqlite;
    let path = std::env::temp_dir().join(format!(
        "fw-request-id-changed-body-reopen-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    let path_str = path.to_str().unwrap();
    let rid = RequestId::new("changed-body-reopen").unwrap();
    let empty_rid = RequestId::new("changed-body-empty").unwrap();
    let q = qkey();
    let original = vec![item(10), item(20)];
    let first_ids = {
        let fw = open_sqlite(path_str, Arc::new(ManualClock::at(0))).unwrap();
        fw.create_queue(qdef(60_000)).await.unwrap();
        let outcome = fw
            .push_batch_with_request_id(&q, rid.clone(), original.clone())
            .await
            .unwrap();
        assert!(outcome.is_fresh());
        // Empty request_id is a durable no-op that still occupies the ledger.
        let empty = fw
            .push_batch_with_request_id(&q, empty_rid.clone(), vec![])
            .await
            .unwrap();
        assert!(empty.is_fresh());
        outcome.item_ids.clone()
    };

    let reopened = open_sqlite(path_str, Arc::new(ManualClock::at(0))).unwrap();
    reopened.create_queue(qdef(60_000)).await.unwrap();
    let replay = reopened
        .push_batch_with_request_id(&q, rid.clone(), original)
        .await
        .unwrap();
    assert!(
        replay.is_replayed(),
        "same body after reopen must Replayed"
    );
    assert_eq!(replay.item_ids, first_ids);
    assert_eq!(
        reopened
            .push_batch_with_request_id(&q, rid, vec![item(11), item(22)])
            .await
            .unwrap_err(),
        EngineError::RequestIdConflict,
        "changed body after reopen must RequestIdConflict"
    );
    assert!(
        reopened
            .push_batch_with_request_id(&q, empty_rid.clone(), vec![])
            .await
            .unwrap()
            .is_replayed(),
        "empty same body after reopen must Replayed"
    );
    assert_eq!(
        reopened
            .push_batch_with_request_id(&q, empty_rid, vec![item(1)])
            .await
            .unwrap_err(),
        EngineError::RequestIdConflict,
        "empty then nonempty after reopen must RequestIdConflict"
    );
    drop(reopened);
    let _ = std::fs::remove_file(&path);
}
