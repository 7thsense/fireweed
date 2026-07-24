use std::collections::BTreeMap;

use axon_esf::IndexDef;
use fireweed_conformance::{qdef, shard, ts};
use fireweed_core::{
    ClaimByQueryRequest, ClientItemKey, FilterOp, IndexDeclaration, IndexType, LeaseToken,
    OrderField, QueryFilter, QueueDefinition, QueueIndex, SortDirection, TypedValue, WorkerId,
};
use fireweed_engine::{
    ClaimByQueryContext, ClaimPort, ClaimRequest, ControlPlaneStore, EngineError, FinalizeKind,
    FinalizeOutcome, FinalizePort, HotProjectionQueryPort, PushPort, PushSpec, RenewLeasePort,
    UpsertPort,
};
use fireweed_sqlite::SqliteRelationalBackend;

fn query_definition() -> QueueDefinition {
    QueueDefinition {
        typed_indexes: vec![QueueIndex {
            name: "by_rank".to_string(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: "rank".to_string(),
                index_type: IndexType::Integer,
                unique: false,
            }),
        }],
        ..qdef()
    }
}

fn record_kind_definition() -> QueueDefinition {
    QueueDefinition {
        typed_indexes: vec![QueueIndex {
            name: "by_record_kind".to_string(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: "record_kind".to_string(),
                index_type: IndexType::String,
                unique: false,
            }),
        }],
        ..qdef()
    }
}

fn spec(rank: i64) -> PushSpec {
    PushSpec {
        entity: Some(serde_json::json!({"rank": rank})),
        ..Default::default()
    }
}

fn ordinary_claim(max_items: usize, token: &str) -> ClaimRequest {
    ClaimRequest {
        eligibility_time: None,
        shard: shard(),
        worker_id: WorkerId::new("ordinary-worker").unwrap(),
        max_items,
        lease_token: LeaseToken::new(token).unwrap(),
        lease_expires_at: ts(500),
        now: ts(100),
        compatibility: Default::default(),
        expected_epoch: None,
    }
}

fn query_request(request_id: &str) -> ClaimByQueryRequest {
    ClaimByQueryRequest {
        index: Some("by_rank".to_string()),
        filters: vec![QueryFilter {
            field: "rank".to_string(),
            op: FilterOp::Gte,
            value: TypedValue::Integer(0),
        }],
        order_by: OrderField {
            field: "rank".to_string(),
            direction: SortDirection::Ascending,
        },
        max_items: 20,
        lease_duration_ms: 30_000,
        worker_id: WorkerId::new("query-worker").unwrap(),
        request_id: Some(fireweed_core::RequestId::new(request_id).unwrap()),
    }
}

fn record_kind_request(request_id: &str, record_kind: &str) -> ClaimByQueryRequest {
    ClaimByQueryRequest {
        index: Some("by_record_kind".to_string()),
        filters: vec![QueryFilter {
            field: "record_kind".to_string(),
            op: FilterOp::Eq,
            value: TypedValue::String(record_kind.to_string()),
        }],
        order_by: OrderField {
            field: "record_kind".to_string(),
            direction: SortDirection::Ascending,
        },
        max_items: 20,
        lease_duration_ms: 30_000,
        worker_id: WorkerId::new("query-worker").unwrap(),
        request_id: Some(fireweed_core::RequestId::new(request_id).unwrap()),
    }
}

fn query_context(now: i64) -> ClaimByQueryContext {
    ClaimByQueryContext {
        now: ts(now),
        eligibility_time: None,
    }
}

type LifecycleSnapshotRow = (String, String, i64, Option<i64>, Option<String>);

fn lifecycle_snapshot(path: &str) -> Vec<LifecycleSnapshotRow> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT item_id, lifecycle_state, CAST(item_version AS INTEGER), lease_expires_at, worker_id \
             FROM pqueue_items ORDER BY item_id",
        )
        .unwrap();
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })
        .unwrap();
    rows.map(|row| row.unwrap()).collect()
}

#[tokio::test]
async fn claim_by_query_excludes_leased_terminal_superseded_and_future_rows() {
    let backend = SqliteRelationalBackend::in_memory().unwrap();
    backend.create_queue(query_definition()).await.unwrap();

    let terminal = backend
        .push(&shard(), vec![spec(2)], ts(0), None)
        .await
        .unwrap()[0];
    backend
        .claim(ordinary_claim(1, "terminal-lease"))
        .await
        .unwrap();
    backend
        .finalize(
            &shard(),
            vec![FinalizeOutcome::new(terminal, FinalizeKind::Complete)],
            ts(101),
            None,
        )
        .await
        .unwrap();

    let live = backend
        .push(&shard(), vec![spec(1)], ts(2), None)
        .await
        .unwrap()[0];
    let live_claim = backend
        .claim(ordinary_claim(1, "live-lease"))
        .await
        .unwrap();
    assert_eq!(live_claim.items[0].item_id, live);

    let replacement_key = ClientItemKey::new("replacement-key").unwrap();
    backend
        .replace_if_pending(
            &shard(),
            &replacement_key,
            None,
            None,
            None,
            None,
            BTreeMap::new(),
            Default::default(),
            Some(serde_json::json!({"rank": 3})),
            ts(3),
            None,
        )
        .await
        .unwrap();
    let replacement = backend
        .replace_if_pending(
            &shard(),
            &replacement_key,
            None,
            None,
            None,
            None,
            BTreeMap::new(),
            Default::default(),
            Some(serde_json::json!({"rank": 3})),
            ts(4),
            None,
        )
        .await
        .unwrap();
    let replacement_id = match replacement {
        fireweed_engine::UpsertOutcome::Replaced { new_item_id, .. } => new_item_id,
        other => panic!("expected replacement, got {other:?}"),
    };

    backend
        .push(
            &shard(),
            vec![PushSpec {
                not_before: Some(ts(200)),
                ..spec(4)
            }],
            ts(5),
            None,
        )
        .await
        .unwrap();
    let eligible = backend
        .push(&shard(), vec![spec(0)], ts(6), None)
        .await
        .unwrap()[0];

    let claimed = backend
        .claim_by_query(
            &shard(),
            query_request("eligibility-request"),
            query_context(100),
        )
        .await
        .unwrap();
    let ids: Vec<_> = claimed.items.iter().map(|item| item.item_id).collect();
    assert_eq!(ids, vec![eligible, replacement_id]);
}

#[tokio::test]
async fn claim_by_query_reopen_does_not_reissue_terminal_or_live_lease() {
    let path = std::env::temp_dir().join(format!(
        "pqueue-claim-by-query-reopen-{}-{}.db",
        std::process::id(),
        std::thread::current().name().unwrap_or("test")
    ));
    let _ = std::fs::remove_file(&path);
    let path_string = path.to_string_lossy().into_owned();
    {
        let backend = SqliteRelationalBackend::open(&path_string).unwrap();
        backend.create_queue(query_definition()).await.unwrap();
        let terminal = backend
            .push(&shard(), vec![spec(0)], ts(0), None)
            .await
            .unwrap()[0];
        backend
            .claim(ordinary_claim(1, "terminal-before-reopen"))
            .await
            .unwrap();
        backend
            .finalize(
                &shard(),
                vec![FinalizeOutcome::new(terminal, FinalizeKind::Complete)],
                ts(101),
                None,
            )
            .await
            .unwrap();
        backend
            .push(&shard(), vec![spec(1)], ts(2), None)
            .await
            .unwrap();
        backend
            .claim(ordinary_claim(1, "live-before-reopen"))
            .await
            .unwrap();
    }

    let reopened = SqliteRelationalBackend::open(&path_string).unwrap();
    let claimed = reopened
        .claim_by_query(
            &shard(),
            query_request("reopen-request"),
            query_context(100),
        )
        .await
        .unwrap();
    assert!(claimed.items.is_empty());
    drop(reopened);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn record_kind_transition_claim_is_atomic_for_mixed_rows_and_decode_errors() {
    let path = std::env::temp_dir().join(format!(
        "pqueue-claim-by-query-mixed-{}-{}.db",
        std::process::id(),
        std::thread::current().name().unwrap_or("test")
    ));
    let _ = std::fs::remove_file(&path);
    let path_string = path.to_string_lossy().into_owned();

    let backend = SqliteRelationalBackend::open(&path_string).unwrap();
    backend
        .create_queue(record_kind_definition())
        .await
        .unwrap();
    let ids = backend
        .push(
            &shard(),
            ["transition", "effect", "await", "result", "transition"]
                .into_iter()
                .enumerate()
                .map(|(rank, kind)| PushSpec {
                    entity: Some(serde_json::json!({
                        "record_kind": kind,
                        "sequence": rank,
                    })),
                    ..Default::default()
                })
                .collect(),
            ts(0),
            None,
        )
        .await
        .unwrap();

    let pristine = lifecycle_snapshot(&path_string);
    let no_match = backend
        .claim_by_query(
            &shard(),
            record_kind_request("no-match", "missing"),
            query_context(100),
        )
        .await
        .unwrap();
    assert!(no_match.items.is_empty());
    assert_eq!(lifecycle_snapshot(&path_string), pristine);

    let conn = rusqlite::Connection::open(&path_string).unwrap();
    for (request_id, column, corrupt_value, restored_value) in [
        (
            "candidate-row-decode",
            "item_version",
            "'not-an-integer'",
            "1",
        ),
        (
            "entity-decode",
            "entity_document",
            "'not-json'",
            "'{\"record_kind\":\"transition\",\"sequence\":0}'",
        ),
        (
            "index-decode",
            "entity_document",
            "'{\"record_kind\":7,\"sequence\":0}'",
            "'{\"record_kind\":\"transition\",\"sequence\":0}'",
        ),
    ] {
        conn.execute(
            &format!("UPDATE pqueue_items SET {column}={corrupt_value} WHERE item_id=?1"),
            [ids[0].to_string()],
        )
        .unwrap();
        let before_error = lifecycle_snapshot(&path_string);
        let error = backend
            .claim_by_query(
                &shard(),
                record_kind_request(request_id, "transition"),
                query_context(100),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            EngineError::Storage(_) | EngineError::Invalid(_)
        ));
        assert_eq!(
            lifecycle_snapshot(&path_string),
            before_error,
            "{request_id} must roll back before mutating any lease"
        );
        conn.execute(
            &format!("UPDATE pqueue_items SET {column}={restored_value} WHERE item_id=?1"),
            [ids[0].to_string()],
        )
        .unwrap();
    }
    drop(conn);

    let claimed = backend
        .claim_by_query(
            &shard(),
            record_kind_request("transition-only", "transition"),
            query_context(100),
        )
        .await
        .unwrap();
    let mut claimed_ids = claimed
        .items
        .iter()
        .map(|item| item.item_id)
        .collect::<Vec<_>>();
    claimed_ids.sort();
    let mut expected_transition_ids = vec![ids[0], ids[4]];
    expected_transition_ids.sort();
    assert_eq!(claimed_ids, expected_transition_ids);
    let after_claim = lifecycle_snapshot(&path_string);
    for row in &after_claim {
        let item_id = fireweed_core::ItemId::new(row.0.clone()).unwrap();
        if item_id == ids[0] || item_id == ids[4] {
            assert_eq!((&row.1[..], row.2), ("Leased", 2));
            assert_eq!(row.4.as_deref(), Some("query-worker"));
        } else {
            assert_eq!(
                (&row.1[..], row.2, row.3, row.4.as_deref()),
                ("Pending", 1, None, None)
            );
        }
    }

    drop(backend);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn claim_by_query_validates_and_durably_replays_the_api_envelope() {
    let path = std::env::temp_dir().join(format!(
        "pqueue-claim-by-query-replay-{}-{}.db",
        std::process::id(),
        std::thread::current().name().unwrap_or("test")
    ));
    let _ = std::fs::remove_file(&path);
    let path_string = path.to_string_lossy().into_owned();

    let first = {
        let backend = SqliteRelationalBackend::open(&path_string).unwrap();
        let mut definition = query_definition();
        definition.request_id_retention_ms = 10_000;
        backend.create_queue(definition).await.unwrap();
        backend
            .push(
                &shard(),
                vec![PushSpec {
                    not_before: Some(ts(150)),
                    ..spec(0)
                }],
                ts(0),
                None,
            )
            .await
            .unwrap();

        let mut missing_request_id = query_request("unused");
        missing_request_id.request_id = None;
        assert!(matches!(
            backend
                .claim_by_query(&shard(), missing_request_id, query_context(100))
                .await,
            Err(EngineError::Invalid(_))
        ));
        for (request_id, max_items, lease_duration_ms) in [
            ("zero-items", 0, 30_000),
            ("too-many-items", 101, 30_000),
            ("zero-duration", 1, 0),
            ("too-long-duration", 1, 60_001),
        ] {
            let mut invalid = query_request(request_id);
            invalid.max_items = max_items;
            invalid.lease_duration_ms = lease_duration_ms;
            assert!(matches!(
                backend
                    .claim_by_query(&shard(), invalid, query_context(100))
                    .await,
                Err(EngineError::Invalid(_))
            ));
        }

        let context = ClaimByQueryContext {
            now: ts(100),
            eligibility_time: Some(ts(200)),
        };
        let first = backend
            .claim_by_query(&shard(), query_request("durable-replay"), context)
            .await
            .unwrap();
        assert_eq!(first.items.len(), 1);
        assert_eq!(first.items[0].lease_expires_at, ts(130));
        first
    };

    let reopened = SqliteRelationalBackend::open(&path_string).unwrap();
    let replay = reopened
        .claim_by_query(
            &shard(),
            query_request("durable-replay"),
            query_context(101),
        )
        .await
        .unwrap();
    assert_eq!(replay.items.len(), 1);
    assert_eq!(replay.items[0].item_id, first.items[0].item_id);
    assert_eq!(replay.items[0].item_version, first.items[0].item_version);
    assert_eq!(replay.items[0].lease_token, first.items[0].lease_token);
    assert_eq!(
        replay.items[0].lease_expires_at,
        first.items[0].lease_expires_at
    );
    let worker: String = rusqlite::Connection::open(&path)
        .unwrap()
        .query_row(
            "SELECT worker_id FROM pqueue_items WHERE item_id=?1",
            [first.items[0].item_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(worker, "query-worker");

    reopened
        .renew(
            &shard(),
            vec![first.items[0].item_id],
            ts(160),
            ts(105),
            None,
        )
        .await
        .unwrap();
    drop(reopened);
    let reopened = SqliteRelationalBackend::open(&path_string).unwrap();

    let active_after_retention = reopened
        .claim_by_query(
            &shard(),
            query_request("durable-replay"),
            query_context(145),
        )
        .await
        .unwrap();
    assert_eq!(
        active_after_retention.items[0].item_id,
        first.items[0].item_id
    );

    let mut conflict = query_request("durable-replay");
    conflict.worker_id = WorkerId::new("different-worker").unwrap();
    assert_eq!(
        reopened
            .claim_by_query(&shard(), conflict, query_context(145))
            .await
            .unwrap_err(),
        EngineError::RequestIdConflict
    );
    assert_eq!(
        reopened
            .claim_by_query(
                &shard(),
                query_request("durable-replay"),
                query_context(161),
            )
            .await
            .unwrap_err(),
        EngineError::RequestExpired
    );

    drop(reopened);
    let _ = std::fs::remove_file(path);
}
