#![allow(dead_code, unused_imports)]

use std::sync::Arc;

use fireweed::*;
use fireweed_memory::ManualClock;

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
    assert!(replay.is_replayed(), "same body after reopen must Replayed");
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
