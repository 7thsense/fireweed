//! fireweed-cd0e5255: claim_by_query cost is O(claimed + bounded skip), not O(corpus).

use std::collections::BTreeMap;
use std::time::Instant;

use fireweed_conformance::{qdef, shard, ts};
use fireweed_core::{
    ClaimByQueryRequest, ClientItemKey, FilterOp, IndexDeclaration, IndexDef, IndexType, ItemId,
    OrderField, PriorityValue, QueryFilter, QueueDefinition, QueueIndex, RequestId, SortDirection,
    TypedValue, WorkerId,
};
use fireweed_engine::{
    ControlPlaneStore, FinalizeKind, FinalizeOutcome, FinalizePort, HotProjectionQueryPort,
    ProjectionStore, PushPort, PushSpec, assemble_async_log_replay,
};
use fireweed_projection::{InMemoryProjection, MemoryLog};

fn backend() -> fireweed_engine::AsyncLogReplayBackend<MemoryLog, InMemoryProjection> {
    assemble_async_log_replay(MemoryLog::new(), InMemoryProjection::new(), 1).expect("assemble")
}

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
        max_push_batch_size: 100,
        max_claim_batch_size: 10_000,
        ..qdef()
    }
}

fn push_spec(key: &str, rank: i64) -> PushSpec {
    PushSpec {
        client_item_key: Some(ClientItemKey::new(key).unwrap()),
        priority: Some(PriorityValue::Int64(rank)),
        entity: Some(serde_json::json!({"rank": rank})),
        fields: BTreeMap::new(),
        ..Default::default()
    }
}

fn claim_req(request_id: &str, max_items: u32) -> ClaimByQueryRequest {
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
        lease_duration_ms: 60_000,
        worker_id: WorkerId::new("claim-cost").unwrap(),
        request_id: Some(RequestId::new(request_id).unwrap()),
    }
}

fn claim_ctx(now: i64) -> fireweed_engine::ClaimByQueryContext {
    fireweed_engine::ClaimByQueryContext {
        now: ts(now),
        eligibility_time: None,
        expected_epoch: None,
    }
}

async fn measure_claim_after_history(history: usize, pending: usize) -> (u128, usize) {
    let b = backend();
    b.create_queue(definition()).await.unwrap();
    const BATCH: usize = 100;
    let mut rank = 0_i64;
    let mut remaining = history;
    while remaining > 0 {
        let n = remaining.min(BATCH);
        let specs: Vec<_> = (0..n)
            .map(|_| {
                rank += 1;
                push_spec(&format!("h-{history}-{rank}"), rank)
            })
            .collect();
        b.push(&shard(), specs, ts(0), None).await.unwrap();
        remaining -= n;
    }
    let mut drained = 0usize;
    let mut step = 0u64;
    while drained < history {
        let claimed = b
            .claim_by_query(
                &shard(),
                claim_req(&format!("hist-{history}-{step}"), 500),
                claim_ctx(10),
            )
            .await
            .unwrap();
        if claimed.items.is_empty() {
            break;
        }
        let outcomes: Vec<_> = claimed
            .items
            .iter()
            .map(|item| FinalizeOutcome::new(item.item_id, FinalizeKind::Complete))
            .collect();
        b.finalize(&shard(), outcomes, ts(11), None).await.unwrap();
        drained += claimed.items.len();
        step += 1;
    }
    assert_eq!(drained, history, "history must fully drain");
    let pending_specs: Vec<_> = (0..pending)
        .map(|i| {
            rank += 1;
            push_spec(&format!("p-{history}-{i}"), rank)
        })
        .collect();
    let pending_ids = b.push(&shard(), pending_specs, ts(20), None).await.unwrap();
    assert_eq!(pending_ids.len(), pending);
    let start = Instant::now();
    let claimed = b
        .claim_by_query(
            &shard(),
            claim_req(&format!("measure-{history}"), pending as u32),
            claim_ctx(30),
        )
        .await
        .unwrap();
    let elapsed_ms = start.elapsed().as_millis();
    assert_eq!(claimed.items.len(), pending);
    let claimed_ids: std::collections::HashSet<ItemId> =
        claimed.items.iter().map(|i| i.item_id).collect();
    let expected: std::collections::HashSet<ItemId> = pending_ids.into_iter().collect();
    assert_eq!(claimed_ids, expected);
    let empty = b
        .with_projection(|p| {
            <InMemoryProjection as ProjectionStore>::select_claim_by_query(
                p,
                &shard(),
                Some("by_rank"),
                &claim_req("probe", 10).filters,
                &claim_req("probe", 10).order_by,
                10,
                ts(30),
            )
        })
        .unwrap();
    assert!(
        empty.is_empty(),
        "leased items must leave the claim secondary index"
    );
    (elapsed_ms, claimed.items.len())
}

#[test]
fn claim_by_query_cost_independent_of_terminal_corpus_size() {
    futures::executor::block_on(async {
        let pending = 50usize;
        let sizes = [1_000usize, 10_000, 100_000];
        let mut samples = Vec::new();
        for history in sizes {
            let (ms, n) = measure_claim_after_history(history, pending).await;
            assert_eq!(n, pending);
            samples.push((history, ms));
            eprintln!("claim_cost_scale history={history} claim_ms={ms} pending={pending}");
        }
        let base = samples[0].1.max(1);
        let large = samples[2].1;
        assert!(
            large <= base.saturating_mul(20).max(base + 200),
            "claim_by_query after 100k terminal history took {large}ms vs {base}ms at 1k — \
             claim cost still scales with corpus (fireweed-cd0e5255); samples={samples:?}"
        );
    });
}

#[test]
fn claim_secondary_index_drops_leased_and_terminal() {
    futures::executor::block_on(async {
        let b = backend();
        b.create_queue(definition()).await.unwrap();
        let ids = b
            .push(
                &shard(),
                vec![push_spec("a", 1), push_spec("b", 2), push_spec("c", 3)],
                ts(0),
                None,
            )
            .await
            .unwrap();
        let before = b
            .with_projection(|p| {
                <InMemoryProjection as ProjectionStore>::select_claim_by_query(
                    p,
                    &shard(),
                    Some("by_rank"),
                    &claim_req("x", 10).filters,
                    &claim_req("x", 10).order_by,
                    10,
                    ts(1),
                )
            })
            .unwrap();
        assert_eq!(before.len(), 3);
        let claimed = b
            .claim_by_query(&shard(), claim_req("lease-1", 2), claim_ctx(1))
            .await
            .unwrap();
        assert_eq!(claimed.items.len(), 2);
        let after_lease = b
            .with_projection(|p| {
                <InMemoryProjection as ProjectionStore>::select_claim_by_query(
                    p,
                    &shard(),
                    Some("by_rank"),
                    &claim_req("y", 10).filters,
                    &claim_req("y", 10).order_by,
                    10,
                    ts(1),
                )
            })
            .unwrap();
        assert_eq!(after_lease, vec![ids[2]]);
        let outcomes: Vec<_> = claimed
            .items
            .iter()
            .map(|item| FinalizeOutcome::new(item.item_id, FinalizeKind::Complete))
            .collect();
        b.finalize(&shard(), outcomes, ts(2), None).await.unwrap();
        let after_terminal = b
            .with_projection(|p| {
                <InMemoryProjection as ProjectionStore>::select_claim_by_query(
                    p,
                    &shard(),
                    Some("by_rank"),
                    &claim_req("z", 10).filters,
                    &claim_req("z", 10).order_by,
                    10,
                    ts(2),
                )
            })
            .unwrap();
        assert_eq!(after_terminal, vec![ids[2]]);
    });
}
