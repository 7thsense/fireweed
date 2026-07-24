use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};

use fireweed_conformance::{commit, envelope, qdef, shard, ts};
use fireweed_core::{
    ClaimByQueryRequest, ClientItemKey, FilterOp, GateKeyPolicy, IndexDeclaration, IndexDef,
    IndexType, LeaseToken, OrderField, QueryFilter, QueueDefinition, QueueIndex, SortDirection,
    TypedValue, WorkerId,
};
use fireweed_engine::{
    ClaimByQueryContext, ClaimPort, ClaimRequest, ControlPlaneStore, FinalizeKind, FinalizeOutcome,
    FinalizePort, HotProjectionQueryPort, PauseQueueCommand, PushPort, PushSpec, QueueCommand,
    SetGatesCommand, SetGatesPort, UpsertOutcome, UpsertPort,
};
use fireweed_sqlite::SqliteRelationalBackend;

fn definition() -> QueueDefinition {
    let mut definition = QueueDefinition {
        typed_indexes: vec![QueueIndex {
            name: "by_rank".to_string(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: "rank".to_string(),
                index_type: IndexType::Integer,
                unique: false,
            }),
        }],
        ..qdef()
    };
    definition.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
    definition.eligibility_policy.max_gate_keys_per_item = Some(4);
    definition.eligibility_policy.max_gates_per_request = Some(4);
    definition
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

fn query(max_items: u32, request_id: &str) -> ClaimByQueryRequest {
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

    let (first_expected, stale_id, remaining_expected, gated_id) = runtime.block_on(async {
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
        let gated_id = backend
            .push(
                &shard(),
                vec![PushSpec {
                    gate_keys: vec!["hold".to_string()],
                    ..spec(0)
                }],
                ts(6),
                None,
            )
            .await
            .unwrap()[0];
        backend
            .set_gates(
                &shard(),
                SetGatesCommand {
                    gate_keys: vec!["hold".to_string()],
                    blocked: true,
                },
                ts(7),
                None,
            )
            .await
            .unwrap();

        let first = backend
            .claim_by_query(&shard(), query(1, "first-query"), query_context(100))
            .await
            .unwrap();
        assert_eq!(first.items.len(), 1);
        assert_eq!(first.items[0].item_id, due[1], "declared index order");
        assert_eq!(first.items[0].lease_expires_at, ts(130));
        assert_eq!(first.items[0].item_version, 2, "claim advances version");
        (due[1], due[0], vec![replacement_id], gated_id)
    });

    // Force the candidate's version to change between selection and the lease UPDATE. The trigger
    // makes the outer optimistic-CAS UPDATE report zero changed rows deterministically.
    let connection = rusqlite::Connection::open(&path_string).unwrap();
    connection
        .execute_batch(&format!(
            "CREATE TRIGGER claim_by_query_stale_version
             BEFORE UPDATE OF lifecycle_state ON pqueue_items
             WHEN OLD.item_id = '{}' AND NEW.lifecycle_state = 'Leased'
             BEGIN
               UPDATE pqueue_items SET item_version = item_version + 1
               WHERE tenant_id = 't1' AND queue_id = 'q1' AND item_id = '{}';
               SELECT RAISE(IGNORE);
             END;",
            stale_id, stale_id
        ))
        .unwrap();
    drop(connection);

    let barrier = Arc::new(Barrier::new(2));
    let mut attempts = Vec::new();
    for attempt_id in 0..2 {
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
                        .claim_by_query(
                            &shard(),
                            query(10, &format!("concurrent-query-{attempt_id}")),
                            query_context(100),
                        )
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

    let connection = rusqlite::Connection::open(&path_string).unwrap();
    let stale_id_string = stale_id.to_string();
    let (state, version): (String, i64) = connection
        .query_row(
            "SELECT lifecycle_state, item_version FROM pqueue_items WHERE item_id = ?1",
            [stale_id_string],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, "Pending");
    assert!(version > 1, "trigger must create a stale selected version");
    connection
        .execute_batch("DROP TRIGGER claim_by_query_stale_version")
        .unwrap();
    drop(connection);

    runtime.block_on(async {
        let backend = SqliteRelationalBackend::open(&path_string).unwrap();
        backend
            .set_gates(
                &shard(),
                SetGatesCommand {
                    gate_keys: vec!["hold".to_string()],
                    blocked: false,
                },
                ts(101),
                None,
            )
            .await
            .unwrap();
        let unblocked = backend
            .claim_by_query(&shard(), query(1, "unblocked-query"), query_context(101))
            .await
            .unwrap();
        assert_eq!(unblocked.items[0].item_id, gated_id);

        backend
            .push(&shard(), vec![spec(7)], ts(102), None)
            .await
            .unwrap();
        commit(
            &backend,
            envelope(
                QueueCommand::PauseQueue(PauseQueueCommand::default()),
                vec![],
            ),
        )
        .await;
        let paused = backend
            .claim_by_query(&shard(), query(10, "paused-query"), query_context(103))
            .await
            .unwrap();
        assert!(
            paused.items.is_empty(),
            "paused queues expose no candidates"
        );
    });

    let _ = std::fs::remove_file(path);
}
