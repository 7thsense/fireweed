//! End-to-end: an OFF-THE-SHELF Redis client (`redis` crate) drives the pqueue RESP front over real
//! TCP: produce via XADD, drain via XREADGROUP `>`, ack via XACK, and reconcile that every
//! produced item is delivered exactly once, in priority order, with ties broken by insertion order
//! (plan section 3 drain-and-reconcile, validating Invariant 1 through the stock command surface).

use std::sync::Arc;

use pqueue_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use pqueue_engine::ControlPlaneStore;
use pqueue_memory::MemoryBackend;
use pqueue_resp::serve;
use redis::streams::StreamReadReply;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_and_reconcile_with_offtheshelf_client() {
    let backend = Arc::new(MemoryBackend::new());
    backend.create_queue(qdef()).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve(listener, backend.clone()));

    let client = redis::Client::open(format!("redis://{addr}")).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // Produce mixed priorities incl. a DUPLICATE (two 30s) to exercise the CreatedSequence
    // tie-breaker. Record the server-assigned id per insertion so we can check tie order.
    let priorities: Vec<i64> = vec![50, 10, 90, 30, 70, 20, 80, 40, 60, 5, 30];
    let mut produced_ids: Vec<String> = Vec::new();
    for &p in &priorities {
        let id: String = redis::cmd("XADD")
            .arg("t1:q1")
            .arg("*")
            .arg("priority")
            .arg(p)
            .query_async(&mut con)
            .await
            .unwrap();
        produced_ids.push(id);
    }

    // Drain COUNT 3 at a time, acking each batch, until empty.
    let mut delivered: Vec<(i64, String)> = Vec::new();
    let mut round_bounds: Vec<(i64, i64)> = Vec::new(); // (min, max) priority per round
    let mut rounds = 0;
    loop {
        rounds += 1;
        assert!(
            rounds < 100,
            "drain did not terminate (possible orphan/hang)"
        );
        let reply: Option<StreamReadReply> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg("g")
            .arg("c")
            .arg("COUNT")
            .arg(3)
            .arg("STREAMS")
            .arg("t1:q1")
            .arg(">")
            .query_async(&mut con)
            .await
            .unwrap();
        let Some(reply) = reply else { break };
        if reply.keys.iter().all(|k| k.ids.is_empty()) {
            break;
        }
        let mut round: Vec<i64> = Vec::new();
        let mut ack = redis::cmd("XACK");
        ack.arg("t1:q1").arg("g");
        for key in &reply.keys {
            for id in &key.ids {
                let p: i64 = id.get("priority").expect("priority field present");
                delivered.push((p, id.id.clone()));
                round.push(p);
                ack.arg(&id.id);
            }
        }
        round_bounds.push((*round.iter().min().unwrap(), *round.iter().max().unwrap()));
        let _acked: i64 = ack.query_async(&mut con).await.unwrap();
    }

    // (a) exactly-once + global priority order: delivered priorities == sorted(produced).
    let delivered_pri: Vec<i64> = delivered.iter().map(|(p, _)| *p).collect();
    let mut expected = priorities.clone();
    expected.sort();
    assert_eq!(
        delivered_pri, expected,
        "delivered set must equal produced set, in priority order (Invariant 1, exactly once)"
    );

    // (b) cross-batch ordering: each round's max <= the next round's min (so a backend that sorted
    // only WITHIN a batch would fail here).
    for w in round_bounds.windows(2) {
        assert!(
            w[0].1 <= w[1].0,
            "round priority bands must not overlap: {:?} then {:?}",
            w[0],
            w[1]
        );
    }

    // (c) tie-break: the first-inserted priority-30 item is delivered before the second.
    let first_30 = &produced_ids[3];
    let second_30 = &produced_ids[10];
    let pos_first = delivered.iter().position(|(_, id)| id == first_30).unwrap();
    let pos_second = delivered
        .iter()
        .position(|(_, id)| id == second_30)
        .unwrap();
    assert!(
        pos_first < pos_second,
        "equal-priority items must break ties by insertion order (CreatedSequence)"
    );
}
