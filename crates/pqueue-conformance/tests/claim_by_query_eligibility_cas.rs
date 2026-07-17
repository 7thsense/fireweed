use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};

use pqueue_conformance::{qdef, shard, ts};
use pqueue_core::{
    ClaimByQueryRequest, ClientItemKey, FilterOp, IndexDeclaration, IndexDef, IndexType,
    LeaseToken, OrderField, QueryFilter, QueueDefinition, QueueIndex, SortDirection, TypedValue,
    WorkerId,
};
use pqueue_engine::{
    ClaimPort, ClaimRequest, ControlPlaneStore, FinalizeKind, FinalizeOutcome, FinalizePort,
    HotProjectionQueryPort, PushPort, PushSpec, UpsertOutcome, UpsertPort,
};
use pqueue_sqlite::SqliteRelationalBackend;

fn definition() -> QueueDefinition {
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

fn ordinary_claim(token: &str) -> ClaimRequest {
    ClaimRequest {
        eligibility_time: None,
        shard: shard(),
        worker_id: WorkerId::new("seed-worker").unwrap(),
        max_items: 1,
        lease_token: LeaseToken::new(token).unwrap(),
        lease_expires_at: ts(500),
        now: ts(100),
        compatibility: Default::default(),
        expected_epoch: None,
    }
}

fn query(max_items: u32) -> ClaimByQueryRequest {
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
        max_items,
        lease_duration_ms: 30_000,
        now: ts(100),
        worker_id: WorkerId::new("query-worker").unwrap(),
        request_id: None,
    }
}

#[test]
fn claim_by_query_eligibility_cas() {
    let path = std::env::temp_dir().join(format!(
        "pqueue-claim-by-query-cas-{}.db",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    let path_string = path.to_string_lossy().into_owned();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let (first_expected, remaining_expected) = runtime.block_on(async {
        let backend = SqliteRelationalBackend::open(&path_string).unwrap();
        backend.create_queue(definition()).await.unwrap();

        let terminal = backend
            .push(&shard(), vec![spec(5)], ts(0), None)
            .await
            .unwrap()[0];
        backend
            .claim(ordinary_claim("terminal-seed"))
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
            .push(&shard(), vec![spec(4)], ts(1), None)
            .await
            .unwrap();
        backend.claim(ordinary_claim("live-seed")).await.unwrap();

        let key = ClientItemKey::new("supersession-key").unwrap();
        backend
            .replace_if_pending(
                &shard(),
                &key,
                None,
                None,
                None,
                None,
                BTreeMap::new(),
                Default::default(),
                Some(serde_json::json!({"rank": 3})),
                ts(2),
                None,
            )
            .await
            .unwrap();
        let replacement = backend
            .replace_if_pending(
                &shard(),
                &key,
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
        let replacement_id = match replacement {
            UpsertOutcome::Replaced { new_item_id, .. } => new_item_id,
            other => panic!("expected replacement, got {other:?}"),
        };

        backend
            .push(
                &shard(),
                vec![PushSpec {
                    not_before: Some(ts(200)),
                    ..spec(6)
                }],
                ts(4),
                None,
            )
            .await
            .unwrap();
        let due = backend
            .push(&shard(), vec![spec(2), spec(1)], ts(5), None)
            .await
            .unwrap();

        let first = backend.claim_by_query(&shard(), query(1)).await.unwrap();
        assert_eq!(first.items.len(), 1);
        assert_eq!(first.items[0].item_id, due[1], "declared index order");
        assert_eq!(first.items[0].lease_expires_at, ts(130));
        assert_eq!(first.items[0].item_version, 2, "claim advances version");
        (due[1], vec![due[0], replacement_id])
    });

    let barrier = Arc::new(Barrier::new(2));
    let mut attempts = Vec::new();
    for _ in 0..2 {
        let path = path_string.clone();
        let barrier = Arc::clone(&barrier);
        attempts.push(std::thread::spawn(move || {
            let backend = SqliteRelationalBackend::open(&path).unwrap();
            barrier.wait();
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    backend
                        .claim_by_query(&shard(), query(10))
                        .await
                        .unwrap()
                        .items
                        .into_iter()
                        .map(|item| item.item_id)
                        .collect::<Vec<_>>()
                })
        }));
    }
    let mut concurrent_ids = attempts
        .into_iter()
        .flat_map(|attempt| attempt.join().unwrap())
        .collect::<Vec<_>>();
    concurrent_ids.sort();
    let mut expected = remaining_expected;
    expected.sort();
    assert_eq!(
        concurrent_ids, expected,
        "each eligible version leases once"
    );
    assert!(!concurrent_ids.contains(&first_expected));

    let _ = std::fs::remove_file(path);
}
