use std::collections::BTreeMap;

use axon_esf::IndexDef;
use pqueue_conformance::{qdef, shard, ts};
use pqueue_core::{
    ClaimByQueryRequest, ClientItemKey, FilterOp, IndexDeclaration, IndexType, LeaseToken,
    OrderField, QueryFilter, QueueDefinition, QueueIndex, SortDirection, TypedValue, WorkerId,
};
use pqueue_engine::{
    ClaimPort, ClaimRequest, ControlPlaneStore, FinalizeKind, FinalizeOutcome, FinalizePort,
    HotProjectionQueryPort, PushPort, PushSpec, UpsertPort,
};
use pqueue_sqlite::SqliteRelationalBackend;

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

fn query_request(now: i64) -> ClaimByQueryRequest {
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
        now: ts(now),
        worker_id: WorkerId::new("query-worker").unwrap(),
        request_id: None,
    }
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
        pqueue_engine::UpsertOutcome::Replaced { new_item_id, .. } => new_item_id,
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
        .claim_by_query(&shard(), query_request(100))
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
        .claim_by_query(&shard(), query_request(100))
        .await
        .unwrap();
    assert!(claimed.items.is_empty());
    drop(reopened);
    let _ = std::fs::remove_file(path);
}
