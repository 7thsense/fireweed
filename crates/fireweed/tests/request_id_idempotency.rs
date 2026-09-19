#![allow(dead_code, unused_imports)]

#[path = "support/storage.rs"]
mod storage;

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

/// Retried query claims must recover the original lease, including renewals, from
/// the log after reopen. Empty and rejected claims also retain their disposition.
#[cfg(feature = "turso")]
#[tokio::test]
async fn native_query_claim_receipts_survive_renewal_reopen_and_empty_results() {
    let fixture = storage::Fixture::new();
    let clock = Arc::new(ManualClock::at(0));
    let mut definition = qdef(1_000);
    definition.typed_indexes = vec![QueueIndex {
        name: "by_score".into(),
        declaration: IndexDeclaration::Single(IndexDef {
            field: "score".into(),
            index_type: IndexType::Integer,
            unique: false,
        }),
    }];
    let queue = qkey();
    let query = ClaimByQueryRequest {
        index: Some("by_score".into()),
        filters: vec![],
        order_by: OrderField {
            field: "score".into(),
            direction: SortDirection::Ascending,
        },
        max_items: 1,
        lease_duration_ms: 2_000,
        worker_id: WorkerId::new("receipt-worker").unwrap(),
        request_id: Some(RequestId::new("query-receipt").unwrap()),
    };
    let fw = storage::open_log_turso(fixture.path(), clock.clone()).unwrap();
    fw.create_queue(definition.clone()).await.unwrap();
    let make_item = |score| NewItem {
        entity: Some(serde_json::json!({"score": score})),
        ..item(score)
    };
    let ids = fw
        .push_batch(&queue, vec![make_item(1), make_item(2), make_item(3)])
        .await
        .unwrap();
    let first = fw.claim_by_query(&queue, query.clone()).await.unwrap();
    assert_eq!(first.items.len(), 1);
    assert_eq!(first.items[0].item_id, ids[0]);
    let query_token = first.items[0].lease_token.clone();
    let by_ids = ClaimByItemIdsRequest {
        item_ids: vec![ids[1], ids[1], ItemId::from_u64(u64::MAX)],
        lease_duration_ms: 2_000,
        worker_id: query.worker_id.clone(),
        request_id: RequestId::new("ids-receipt").unwrap(),
        lease_token: None,
    };
    let second = fw.claim_by_item_ids(&queue, by_ids.clone()).await.unwrap();
    assert_eq!(second.items.len(), 1);
    assert_eq!(second.items[0].item_id, ids[1]);
    assert_eq!(second.outcomes.len(), 2);
    let ids_token = second.items[0].lease_token.clone();
    let dispositions = serde_json::to_value(&second.outcomes).unwrap();
    clock.set(1);
    fw.renew(&queue, [ids[0], ids[1]], 10_000).await.unwrap();
    drop(fw);

    // The original 2-second leases and 1-second receipt window have expired.
    // Renewal must extend both durable receipts, without claiming the third row.
    clock.set(3);
    let fw = storage::open_log_turso(fixture.path(), clock.clone()).unwrap();
    fw.create_queue(definition).await.unwrap();
    let replay = fw.claim_by_query(&queue, query.clone()).await.unwrap();
    assert_eq!(replay.items.len(), 1);
    assert_eq!(replay.items[0].item_id, ids[0]);
    assert_eq!(replay.items[0].lease_token, query_token);
    assert_eq!(
        replay.items[0].lease_expires_at,
        UtcTimestamp::new(11, 0).unwrap()
    );
    let replay_ids = fw.claim_by_item_ids(&queue, by_ids.clone()).await.unwrap();
    assert_eq!(replay_ids.items.len(), 1);
    assert_eq!(replay_ids.items[0].lease_token, ids_token);
    assert_eq!(
        serde_json::to_value(&replay_ids.outcomes).unwrap(),
        dispositions
    );
    let mut changed = query.clone();
    changed.max_items = 2;
    assert_eq!(
        fw.claim_by_query(&queue, changed).await.unwrap_err(),
        EngineError::RequestIdConflict
    );
    let mut changed_ids = by_ids.clone();
    changed_ids.item_ids = vec![ids[2]];
    assert_eq!(
        fw.claim_by_item_ids(&queue, changed_ids).await.unwrap_err(),
        EngineError::RequestIdConflict
    );
    let empty = ClaimByQueryRequest {
        request_id: Some(RequestId::new("empty-query-receipt").unwrap()),
        filters: vec![QueryFilter {
            field: "score".into(),
            op: FilterOp::Eq,
            value: TypedValue::Integer(9),
        }],
        ..query.clone()
    };
    assert!(
        fw.claim_by_query(&queue, empty.clone())
            .await
            .unwrap()
            .items
            .is_empty()
    );
    fw.push(&queue, make_item(9)).await.unwrap();
    let rejected = ClaimByItemIdsRequest {
        item_ids: vec![ids[0]],
        request_id: RequestId::new("already-leased-receipt").unwrap(),
        ..by_ids.clone()
    };
    let rejected_outcomes = fw
        .claim_by_item_ids(&queue, rejected.clone())
        .await
        .unwrap();
    assert!(rejected_outcomes.items.is_empty());
    drop(fw);

    let fw = storage::open_log_turso(fixture.path(), clock.clone()).unwrap();
    assert!(
        fw.claim_by_query(&queue, empty.clone())
            .await
            .unwrap()
            .items
            .is_empty()
    );
    let rejected_replay = fw
        .claim_by_item_ids(&queue, rejected.clone())
        .await
        .unwrap();
    assert!(rejected_replay.items.is_empty());
    assert_eq!(
        serde_json::to_value(&rejected_replay.outcomes).unwrap(),
        serde_json::to_value(&rejected_outcomes.outcomes).unwrap()
    );
    // Neither request granted a lease: the requested 2-second lease duration
    // must not extend their 1-second receipt retention after reopen.
    clock.set(4);
    assert_eq!(
        fw.claim_by_query(&queue, empty).await.unwrap_err(),
        EngineError::RequestExpired
    );
    assert_eq!(
        fw.claim_by_item_ids(&queue, rejected).await.unwrap_err(),
        EngineError::RequestExpired
    );
    clock.set(12);
    assert_eq!(
        fw.claim_by_query(&queue, query).await.unwrap_err(),
        EngineError::RequestExpired
    );
    assert_eq!(
        fw.claim_by_item_ids(&queue, by_ids).await.unwrap_err(),
        EngineError::RequestExpired
    );
}

/// fireweed-01802c42: filesystem log-replay product must rebuild the push request-id ledger on
/// recovery-on-open so same-body replays and changed-body conflicts survive a close/reopen.
#[cfg(feature = "turso")]
#[tokio::test]
async fn request_id_conflict_and_replay_survive_filesystem_reopen() {
    use storage::open_log_turso_async;
    let path = std::env::temp_dir().join(format!(
        "fw-request-id-reopen-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = storage::cleanup(&path);
    let path_str = path.to_str().unwrap();
    let rid = RequestId::new("reopen-rid-1").unwrap();
    let empty_rid = RequestId::new("reopen-empty-1").unwrap();
    let q = qkey();
    let first = {
        let fw = open_log_turso_async(path_str, Arc::new(ManualClock::at(0)))
            .await
            .unwrap();
        fw.create_queue(qdef(60_000)).await.unwrap();
        assert!(
            fw.commit_capabilities(&q)
                .unwrap()
                .retained_commit_idempotency,
            "filesystem log-replay product must advertise retained_commit_idempotency"
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

    let reopened = open_log_turso_async(path_str, Arc::new(ManualClock::at(0)))
        .await
        .unwrap();
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
    let _ = storage::cleanup(&path);
}

/// fireweed-01802c42: batch push disposition Fresh → Replayed and changed-body conflict across reopen.
#[cfg(feature = "turso")]
#[tokio::test]
async fn batch_request_id_replay_and_conflict_survive_filesystem_reopen() {
    use storage::open_log_turso_async;
    let path = std::env::temp_dir().join(format!(
        "fw-request-id-batch-reopen-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = storage::cleanup(&path);
    let path_str = path.to_str().unwrap();
    let rid = RequestId::new("reopen-batch-1").unwrap();
    let q = qkey();
    let body = vec![item(10), item(20)];
    let first = {
        let fw = open_log_turso_async(path_str, Arc::new(ManualClock::at(0)))
            .await
            .unwrap();
        fw.create_queue(qdef(60_000)).await.unwrap();
        let outcome = fw
            .push_batch_with_request_id(&q, rid.clone(), body.clone())
            .await
            .unwrap();
        assert!(outcome.is_fresh());
        outcome.item_ids.clone()
    };
    let reopened = open_log_turso_async(path_str, Arc::new(ManualClock::at(0)))
        .await
        .unwrap();
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
    let _ = storage::cleanup(&path);
}

/// fireweed-6486ed63: empty request_id batch must retain fingerprint so a later non-empty body
/// under the same id returns RequestIdConflict (snorri workflow_enqueue empty→nonempty).
/// Repeated create_queue mirrors snorri's enqueue path.
#[cfg(feature = "memory")]
#[tokio::test]
async fn empty_request_id_then_nonempty_conflicts_on_memory() {
    use fireweed::open_product;
    let fw = open_product(Arc::new(ManualClock::at(0)));
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
#[cfg(feature = "turso")]
#[tokio::test]
async fn changed_body_request_id_conflicts_across_filesystem_reopen() {
    use storage::open_log_turso_async;
    let path = std::env::temp_dir().join(format!(
        "fw-request-id-changed-body-reopen-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = storage::cleanup(&path);
    let path_str = path.to_str().unwrap();
    let rid = RequestId::new("changed-body-reopen").unwrap();
    let empty_rid = RequestId::new("changed-body-empty").unwrap();
    let q = qkey();
    let original = vec![item(10), item(20)];
    let first_ids = {
        let fw = open_log_turso_async(path_str, Arc::new(ManualClock::at(0)))
            .await
            .unwrap();
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

    let reopened = open_log_turso_async(path_str, Arc::new(ManualClock::at(0)))
        .await
        .unwrap();
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
    let _ = storage::cleanup(&path);
}

/// Competing ID/key updates must plan under the same queue serialization boundary.
/// Both successful and rejected request receipts must replay without another mutation.
#[cfg(feature = "turso")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn atomic_turso_concurrent_batch_updates_retain_success_and_conflict_receipts() {
    let root = std::env::temp_dir().join(format!(
        "fw-atomic-update-race-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let ns = format!(
        "atomic-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let config = storage::product_config(&root, &ns);
    let fw = fireweed::open_async(config, Arc::new(ManualClock::at(0)))
        .await
        .unwrap();
    let q = qkey();
    fw.create_queue(qdef(60_000)).await.unwrap();
    let key = ClientItemKey::new("same-original-row").unwrap();
    let id = fw
        .push(
            &q,
            NewItem {
                client_item_key: Some(key.clone()),
                ..item(1)
            },
        )
        .await
        .unwrap();
    let version = fw
        .live_item(&q, key.clone())
        .await
        .unwrap()
        .unwrap()
        .item_version;
    let request = |name, target, value| BatchUpdateRequest {
        request_id: RequestId::new(name).unwrap(),
        updates: vec![BatchUpdateEntry {
            item_ref: target,
            expected_item_version: Some(version),
            priority: BatchUpdateValue::Replace(PriorityValue::Int64(value)),
            not_before: BatchUpdateValue::Keep,
            payload: BatchUpdateValue::Keep,
            metadata: BatchUpdateValue::Keep,
            gate_keys: BatchUpdateValue::Keep,
            fields: BatchUpdateValue::Keep,
        }],
    };
    let a = request("by-id", BatchUpdateItemRef::ItemId(id), 10);
    let b = request("by-key", BatchUpdateItemRef::ClientItemKey(key.clone()), 20);
    let (left, right) = tokio::join!(
        fw.batch_update(&q, a.clone()),
        fw.batch_update(&q, b.clone())
    );
    let (left, right) = (left.unwrap(), right.unwrap());
    let results = [&left.results[0], &right.results[0]];
    let updated = results
        .iter()
        .filter(|outcome| matches!(outcome, BatchUpdateOutcome::Updated { .. }))
        .count();
    assert!(
        updated >= 1,
        "at least one concurrent batch update must apply, got {results:?}"
    );
    assert_eq!(fw.batch_update(&q, a).await.unwrap(), left);
    assert_eq!(fw.batch_update(&q, b).await.unwrap(), right);
    let live = fw.live_item(&q, key).await.unwrap().unwrap();
    assert_eq!(live.item_id, id);
    assert!(
        live.item_version >= version + 1,
        "winner must increment version, got {}",
        live.item_version
    );
    assert!(fw.metrics(&q).await.unwrap().pending >= 1);
    drop(fw);
    std::fs::remove_dir_all(root).unwrap();
}
