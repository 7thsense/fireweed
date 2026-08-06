//! fireweed-9cec8b02 / P7x: concurrent claim_by_query on the async log-replay sqlite product
//! must not append two Claim commands for the same Pending item (Leased + Claim → illegal
//! lifecycle transition). Selecting outside KeyedQueueGate was the residual admission race
//! behind snorri worker-pool/sqlite and campaign-scale/sqlite.

use std::sync::Arc;

use axon_esf::IndexDef;
use fireweed_conformance::{qdef, shard, ts};
use fireweed_core::{
    ClaimByQueryRequest, FilterOp, IndexDeclaration, IndexType, OrderField, QueryFilter,
    QueueDefinition, QueueIndex, SortDirection, TypedValue, WorkerId,
};
use fireweed_engine::{
    ClaimByQueryContext, ControlPlaneStore, FinalizeKind, FinalizeOutcome, FinalizePort,
    HotProjectionQueryPort, PushPort, PushSpec,
};
use fireweed_sqlite::composed_sqlite_backend;

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

fn query_request(request_id: &str, worker: &str) -> ClaimByQueryRequest {
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
        max_items: 4,
        lease_duration_ms: 30_000,
        worker_id: WorkerId::new(worker).unwrap(),
        request_id: Some(fireweed_core::RequestId::new(request_id).unwrap()),
    }
}

fn query_context(now_secs: i64) -> ClaimByQueryContext {
    ClaimByQueryContext {
        now: ts(now_secs),
        eligibility_time: None,
        expected_epoch: None,
    }
}

#[tokio::test]
async fn concurrent_claim_by_query_never_double_claims_on_sqlite_log_replay() {
    const ITEMS: usize = 16;
    const WORKERS: usize = 8;

    let path = std::env::temp_dir().join(format!(
        "fireweed-concurrent-claim-by-query-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let path_s = path.to_str().unwrap();
    let backend = Arc::new(composed_sqlite_backend(path_s).unwrap());
    backend.create_queue(query_definition()).await.unwrap();

    let mut specs = Vec::with_capacity(ITEMS);
    for i in 0..ITEMS {
        specs.push(PushSpec {
            entity: Some(serde_json::json!({"rank": i as i64})),
            payload: Some(bytes::Bytes::from(format!("p{i}"))),
            ..PushSpec::default()
        });
    }
    backend.push(&shard(), specs, ts(50), None).await.unwrap();

    let mut handles = Vec::new();
    for w in 0..WORKERS {
        let backend = Arc::clone(&backend);
        handles.push(tokio::spawn(async move {
            let mut claimed_total = 0usize;
            for tick in 0..ITEMS {
                let claimed = backend
                    .claim_by_query(
                        &shard(),
                        query_request(&format!("w{w}-t{tick}"), &format!("worker-{w}")),
                        query_context(100 + tick as i64),
                    )
                    .await
                    .expect("claim_by_query must not illegal-lifecycle under contention");
                claimed_total += claimed.items.len();
                for item in claimed.items {
                    backend
                        .finalize(
                            &shard(),
                            vec![FinalizeOutcome::new(item.item_id, FinalizeKind::Complete)],
                            ts(200 + tick as i64),
                            None,
                        )
                        .await
                        .expect("finalize after own claim must succeed");
                }
            }
            claimed_total
        }));
    }

    let mut total = 0usize;
    for h in handles {
        total += h.await.expect("worker join");
    }
    assert_eq!(
        total, ITEMS,
        "every item claimed exactly once across concurrent claim_by_query workers"
    );

    let _ = std::fs::remove_file(&path);
}
