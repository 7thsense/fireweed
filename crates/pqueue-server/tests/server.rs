//! Composition-root integration: the background ReclaimDriver task recovers orphaned leases with no
//! client traffic, and the wired server is drivable by an off-the-shelf Redis client.

use std::sync::Arc;
use std::time::Duration;

use pqueue_core::{
    EligibilityPolicy, LeaseToken, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp, WorkerId,
};
use pqueue_engine::{ClaimPort, ClaimRequest, Clock, ProjectionRead, PushPort, PushSpec, QueueKey};
use pqueue_memory::{ManualClock, MemoryBackend};
use pqueue_resp::SystemClock;
use pqueue_server::{Backend, Config, start, start_with};
use redis::streams::StreamReadReply;

fn qkey() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}
fn shard() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}
fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}

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
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn background_reclaim_recovers_orphaned_lease_without_client_traffic() {
    let backend = Arc::new(MemoryBackend::new());
    let clock = Arc::new(ManualClock::at(1_000)); // t = 1000s

    // Start the server (provisions the queue) with a fast reclaim ticker + the injected manual clock.
    let server = start_with(
        backend.clone(),
        clock.clone() as Arc<dyn Clock>,
        "127.0.0.1:0",
        Duration::from_millis(5),
        &[qdef()],
    )
    .await
    .unwrap();
    assert!(server.is_running(), "serve + reclaim tasks are alive");

    // Push + claim DIRECTLY on the backend (NO RESP client) — the item is leased until t = 1060s.
    backend
        .push(
            &shard(),
            vec![PushSpec {
                priority: Some(PriorityValue::Int64(5)),
                ..Default::default()
            }],
            clock.now(),
            None,
        )
        .await
        .unwrap();
    let claimed = backend
        .claim(ClaimRequest {
            shard: shard(),
            worker_id: WorkerId::new("w").unwrap(),
            max_items: 1,
            lease_token: LeaseToken::new("L1").unwrap(),
            lease_expires_at: ts(1_060),
            now: clock.now(),
            compatibility: pqueue_engine::ClaimCompatibility::default(),
            expected_epoch: None,
        })
        .await
        .unwrap();
    assert_eq!(claimed.items.len(), 1);
    assert_eq!(backend.metrics(&qkey()).await.unwrap().leased, 1);

    // The worker "crashes": no renew, no ack. Advance the clock past the lease — and DO NOTHING ELSE.
    clock.set(1_061); // 1s past expiry
    // Poll (not a fixed sleep) for the background reclaim task to recover the orphaned lease — no client
    // traffic occurs during this wait, so the ONLY actor that can change state is the reclaim loop.
    let mut reclaimed = false;
    for _ in 0..200 {
        if backend.metrics(&qkey()).await.unwrap().leased == 0 {
            reclaimed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        reclaimed,
        "the orphaned lease was reclaimed by the background task alone (TD-007 §3)"
    );
    let m = backend.metrics(&qkey()).await.unwrap();
    assert_eq!((m.pending, m.leased), (1, 0));
    assert!(
        server.reclaim_stats().leases_reclaimed >= 1,
        "the reclaim is counted/observable, not silently swallowed"
    );
    server.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_provisions_queues_and_serves_end_to_end() {
    // `start()` constructs the backend internally, so the ONLY way it can serve a request is if it
    // provisions the config's queues. Boot it, then drive it with a stock client (no out-of-band setup).
    let server = start(Config {
        backend: Backend::Memory,
        node_id: 0,
        listen: "127.0.0.1:0".to_string(),
        reclaim_interval: Duration::from_secs(60),
        queues: vec![qdef()],
    })
    .await
    .unwrap();

    let client = redis::Client::open(format!("redis://{}", server.addr())).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();
    let _: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(7)
        .query_async(&mut con)
        .await
        .unwrap();
    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(
        reply.keys[0].ids.len(),
        1,
        "provisioned queue serves a real request"
    );
    server.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn boots_and_is_drivable_by_offtheshelf_redis_client() {
    let backend = Arc::new(MemoryBackend::new());
    let server = start_with(
        backend.clone(),
        Arc::new(SystemClock),
        "127.0.0.1:0",
        Duration::from_secs(60),
        &[qdef()],
    )
    .await
    .unwrap();

    let client = redis::Client::open(format!("redis://{}", server.addr())).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(5)
        .query_async(&mut con)
        .await
        .unwrap();
    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    let id = reply.keys[0].ids[0].id.clone();
    let acked: i64 = redis::cmd("XACK")
        .arg("t1:q1")
        .arg("g")
        .arg(&id)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(acked, 1);
    assert_eq!(backend.metrics(&qkey()).await.unwrap().complete, 1);
    server.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_and_drain_drains_in_flight_then_stops_accepting() {
    // Graceful drain (owed-item D): with a client connection still OPEN, `shutdown_and_drain` signals the
    // serve loop, the idle handler exits on the cancel between commands (it is NOT abort-forced), the
    // JoinSet drains, and the call returns FAR under its bound. Afterwards the listener is closed.
    let backend = Arc::new(MemoryBackend::new());
    let server = start_with(
        backend.clone(),
        Arc::new(SystemClock),
        "127.0.0.1:0",
        Duration::from_secs(60),
        &[qdef()],
    )
    .await
    .unwrap();
    let addr = server.addr();

    // A real request succeeds; the connection stays open (idle) afterwards, so a live handler exists to
    // drain.
    let client = redis::Client::open(format!("redis://{addr}")).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();
    let _: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(5)
        .query_async(&mut con)
        .await
        .unwrap();

    // The drain has a 30s internal bound, but the idle handler exits on cancel immediately, so the whole
    // call resolves well within an outer 5s guard (proving it drained gracefully, not via the abort path).
    let drained = tokio::time::timeout(
        Duration::from_secs(5),
        server.shutdown_and_drain(Duration::from_secs(30)),
    )
    .await;
    assert!(
        drained.is_ok(),
        "graceful drain returned within the bound — in-flight handler drained, no abort-forced wait"
    );

    // The listener is closed: a fresh connection cannot complete a request.
    let post = redis::Client::open(format!("redis://{addr}"))
        .unwrap()
        .get_multiplexed_async_connection()
        .await;
    let refused = match post {
        Err(_) => true,
        Ok(mut c) => redis::cmd("PING")
            .query_async::<String>(&mut c)
            .await
            .is_err(),
    };
    assert!(
        refused,
        "server stopped accepting connections after the drain"
    );
}

/// `resolve_node_id` (ADR-009 service seam): a configured small integer is used verbatim; an out-of-range
/// number or an arbitrary string (a hostname / pod identity the deployment wires in) is hashed into a byte;
/// distinct identities map to distinct node ids in the common case, keeping the app infra-agnostic.
#[test]
fn resolve_node_id_uses_small_ints_verbatim_and_hashes_the_rest() {
    use pqueue_server::resolve_node_id;
    assert_eq!(resolve_node_id("0"), 0);
    assert_eq!(resolve_node_id("7"), 7);
    assert_eq!(resolve_node_id("255"), 255);
    assert_eq!(resolve_node_id("  3 "), 3, "trimmed");
    // Out of u8 range / non-numeric -> hashed into range (stable, and distinct here).
    let a = resolve_node_id("256");
    let b = resolve_node_id("pqueue-statefulset-0");
    let c = resolve_node_id("pqueue-statefulset-1");
    assert_ne!(b, c, "distinct pod identities map to distinct node ids");
    let _ = a; // just must not panic / must be in range (u8 by construction)
}
