//! Composition-root integration: the background ReclaimDriver task recovers orphaned leases with no
//! client traffic, and the wired server is drivable by an off-the-shelf Redis client.

use std::sync::Arc;
use std::time::Duration;

use pqueue_core::{
    EligibilityPolicy, LeaseToken, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp, WorkerId,
};
use pqueue_engine::{
    ClaimPort, ClaimRequest, Clock, ControlPlaneStore, EngineError, InMemoryControlPlane,
    ProjectionRead, PushPort, PushSpec, QueueControlPlane, QueueKey,
};
use pqueue_memory::{ManualClock, MemoryBackend};
use pqueue_resp::{RespHooks, RouteDecision, SystemClock, serve_with_shutdown_and_hooks};
use pqueue_server::{
    BackendSpec, Config, ControlPlaneSpec, LogSpec, OwnershipRuntime, ProjectionSpec, start,
    start_with,
};

/// The composed objectlog-LOG + sqlite-PROJECTION spec (replaces the retired `Backend::ObjectLogSqlite` /
/// `Backend::SegmentedObjectLogSqlite` variants — both are now this one composition).
fn objectlog_sqlite_spec(root: std::path::PathBuf, projection: std::path::PathBuf) -> BackendSpec {
    BackendSpec {
        log: LogSpec::ObjectLog { root },
        projection: ProjectionSpec::Sqlite { path: projection },
        control_plane: ControlPlaneSpec::InProcess,
    }
}
use redis::streams::StreamReadReply;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

fn qkey() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}
fn shard() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}
fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}
fn owner(s: &str) -> pqueue_core::OwnerId {
    pqueue_core::OwnerId::new(s).unwrap()
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
        max_rank_error: 0,
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

fn endpoint(addr: std::net::SocketAddr) -> String {
    format!("127.0.0.1:{}", addr.port())
}

fn tmp_runtime_paths(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!("pqueue-server-{tag}-{}-obj", std::process::id()));
    let projection = std::env::temp_dir().join(format!(
        "pqueue-server-{tag}-{}-projection.db",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_file(&projection);
    (root, projection)
}

async fn raw_resp(addr: std::net::SocketAddr, parts: &[&str]) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut request = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        request.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        request.extend_from_slice(part.as_bytes());
        request.extend_from_slice(b"\r\n");
    }
    stream.write_all(&request).await.unwrap();
    let mut buf = vec![0; 512];
    let n = stream.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n]).to_string()
}

#[tokio::test]
async fn ownership_runtime_routes_wrong_node_to_moved() {
    let backend = Arc::new(MemoryBackend::new());
    backend.create_queue(qdef()).await.unwrap();
    let cp = Arc::new(InMemoryControlPlane::default());
    let a = Arc::new(OwnershipRuntime::new(
        backend.clone(),
        cp.clone(),
        owner("node-a"),
        "10.0.0.1:7000".to_string(),
    ));
    let b = OwnershipRuntime::new(
        backend,
        cp.clone(),
        owner("node-b"),
        "10.0.0.2:7000".to_string(),
    );
    b.set_owner_endpoint(owner("node-a"), "10.0.0.1:7000");

    a.register_owner(ts(0)).unwrap();
    a.acquire_queue(&qkey(), ts(0)).await.unwrap();
    b.register_owner(ts(1)).unwrap();

    let decision = b
        .route_command("XADD", &[], b"t1:q1", ts(1), false)
        .await
        .unwrap();
    assert!(matches!(
        decision,
        RouteDecision::Moved { endpoint, .. } if endpoint == "10.0.0.1:7000"
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resp_misrouted_write_emits_moved_to_active_owner() {
    let backend = Arc::new(MemoryBackend::new());
    backend.create_queue(qdef()).await.unwrap();
    let cp = Arc::new(InMemoryControlPlane::default());
    let a = OwnershipRuntime::new(
        backend.clone(),
        cp.clone(),
        owner("node-a"),
        "10.0.0.1:7000".to_string(),
    );
    a.register_owner(ts(0)).unwrap();
    a.acquire_queue(&qkey(), ts(0)).await.unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let clock = Arc::new(ManualClock::at(1));
    let b = Arc::new(OwnershipRuntime::new(
        backend.clone(),
        cp,
        owner("node-b"),
        endpoint(addr),
    ));
    b.set_owner_endpoint(owner("node-a"), "10.0.0.1:7000");
    let cancel = CancellationToken::new();
    let task = tokio::spawn(serve_with_shutdown_and_hooks(
        listener,
        backend,
        b,
        clock as Arc<dyn Clock>,
        cancel.clone(),
    ));

    let response = raw_resp(addr, &["XADD", "t1:q1", "*", "priority", "1"]).await;
    assert!(
        response.starts_with("-MOVED "),
        "expected MOVED, got {response:?}"
    );
    assert!(
        response.contains("10.0.0.1:7000"),
        "redirect must name the active owner endpoint: {response:?}"
    );
    cancel.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn cached_owner_epoch_fences_real_push_path_after_reassignment() {
    let backend = Arc::new(MemoryBackend::new());
    backend.create_queue(qdef()).await.unwrap();
    let cp = Arc::new(InMemoryControlPlane::default());
    let a = OwnershipRuntime::new(
        backend.clone(),
        cp.clone(),
        owner("node-a"),
        "10.0.0.1:7000".to_string(),
    );
    let b = OwnershipRuntime::new(
        backend.clone(),
        cp,
        owner("node-b"),
        "10.0.0.2:7000".to_string(),
    );
    a.register_owner(ts(0)).unwrap();
    a.acquire_queue(&qkey(), ts(0)).await.unwrap();
    let stale_epoch = a
        .expected_epoch_for_write(&qkey(), ts(1), false)
        .await
        .unwrap()
        .unwrap();

    b.register_owner(ts(20)).unwrap();
    b.acquire_queue(&qkey(), ts(20)).await.unwrap();
    let err = backend
        .push(
            &qkey(),
            vec![PushSpec {
                priority: Some(PriorityValue::Int64(5)),
                ..Default::default()
            }],
            ts(21),
            Some(stale_epoch),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::EpochFenced));
}

#[tokio::test]
async fn cached_owner_epoch_fences_real_claim_path_after_reassignment() {
    let backend = Arc::new(MemoryBackend::new());
    backend.create_queue(qdef()).await.unwrap();
    let cp = Arc::new(InMemoryControlPlane::default());
    let a = OwnershipRuntime::new(
        backend.clone(),
        cp.clone(),
        owner("node-a"),
        "10.0.0.1:7000".to_string(),
    );
    let b = OwnershipRuntime::new(
        backend.clone(),
        cp,
        owner("node-b"),
        "10.0.0.2:7000".to_string(),
    );
    a.register_owner(ts(0)).unwrap();
    a.acquire_queue(&qkey(), ts(0)).await.unwrap();
    let stale_epoch = a
        .expected_epoch_for_write(&qkey(), ts(1), false)
        .await
        .unwrap()
        .unwrap();
    backend
        .push(
            &qkey(),
            vec![PushSpec {
                priority: Some(PriorityValue::Int64(5)),
                ..Default::default()
            }],
            ts(1),
            Some(stale_epoch),
        )
        .await
        .unwrap();

    b.register_owner(ts(20)).unwrap();
    b.acquire_queue(&qkey(), ts(20)).await.unwrap();
    let err = backend
        .claim(ClaimRequest {
            shard: qkey(),
            worker_id: WorkerId::new("stale").unwrap(),
            max_items: 1,
            lease_token: LeaseToken::new("stale-lease").unwrap(),
            lease_expires_at: ts(80),
            now: ts(21),
            compatibility: pqueue_engine::ClaimCompatibility::default(),
            expected_epoch: Some(stale_epoch),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::EpochFenced));
}

#[tokio::test]
async fn standby_owner_acquires_managed_queue_after_expiry() {
    let backend = Arc::new(MemoryBackend::new());
    backend.create_queue(qdef()).await.unwrap();
    let cp = Arc::new(InMemoryControlPlane::default());
    let a = OwnershipRuntime::new(
        backend.clone(),
        cp.clone(),
        owner("node-a"),
        "10.0.0.1:7000".to_string(),
    );
    let b = OwnershipRuntime::new(backend, cp, owner("node-b"), "10.0.0.2:7000".to_string());
    a.register_owner(ts(0)).unwrap();
    a.acquire_queue(&qkey(), ts(0)).await.unwrap();
    b.watch_queue(qkey());
    b.register_owner(ts(1)).unwrap();
    b.renew_sessions(ts(1)).await.unwrap();
    assert!(matches!(
        b.expected_epoch_for_write(&qkey(), ts(1), false).await,
        Err(EngineError::Unavailable)
    ));

    b.renew_sessions(ts(20)).await.unwrap();
    assert_eq!(
        b.expected_epoch_for_write(&qkey(), ts(20), false)
            .await
            .unwrap(),
        Some(2)
    );
}

#[tokio::test]
async fn draining_owner_releases_managed_queue_after_inflight_clears() {
    let backend = Arc::new(MemoryBackend::new());
    backend.create_queue(qdef()).await.unwrap();
    let cp = Arc::new(InMemoryControlPlane::default());
    let a = OwnershipRuntime::new(
        backend.clone(),
        cp.clone(),
        owner("node-a"),
        "10.0.0.1:7000".to_string(),
    );
    let b = OwnershipRuntime::new(
        backend,
        cp.clone(),
        owner("node-b"),
        "10.0.0.2:7000".to_string(),
    );
    a.watch_queue(qkey());
    a.register_owner(ts(0)).unwrap();
    a.acquire_queue(&qkey(), ts(0)).await.unwrap();
    b.watch_queue(qkey());
    b.register_owner(ts(1)).unwrap();
    cp.begin_drain(&qkey(), 1, &owner("node-b"), ts(1)).unwrap();

    a.renew_sessions(ts(2)).await.unwrap();
    let released = cp.resolve_queue_owner(&qkey(), ts(2)).unwrap();
    assert_eq!(released.active_owner, None);
    assert_eq!(released.state, pqueue_engine::LeaseState::Unassigned);

    b.renew_sessions(ts(20)).await.unwrap();
    assert_eq!(
        b.expected_epoch_for_write(&qkey(), ts(20), false)
            .await
            .unwrap(),
        Some(2)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resp_xclaim_drain_split_renews_inflight_and_refuses_reassign() {
    let backend = Arc::new(MemoryBackend::new());
    backend.create_queue(qdef()).await.unwrap();
    let cp = Arc::new(InMemoryControlPlane::default());
    let clock = Arc::new(ManualClock::at(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hooks = Arc::new(OwnershipRuntime::new(
        backend.clone(),
        cp.clone(),
        owner("node-a"),
        endpoint(addr),
    ));
    hooks.register_owner(ts(0)).unwrap();
    hooks.acquire_queue(&qkey(), ts(0)).await.unwrap();
    let cancel = CancellationToken::new();
    let task = tokio::spawn(serve_with_shutdown_and_hooks(
        listener,
        backend.clone(),
        hooks,
        clock.clone() as Arc<dyn Clock>,
        cancel.clone(),
    ));
    let client = redis::Client::open(format!("redis://{}", addr)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(1)
        .query_async(&mut con)
        .await
        .unwrap();
    let _: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(2)
        .query_async(&mut con)
        .await
        .unwrap();
    let first: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c1")
        .arg("COUNT")
        .arg(1)
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    let second: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c2")
        .arg("COUNT")
        .arg(1)
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    let id1 = first.keys[0].ids[0].id.clone();
    let id2 = second.keys[0].ids[0].id.clone();
    let before = backend.pending(&qkey()).await.unwrap();
    let id1_before = before
        .iter()
        .find(|lease| lease.item_id.to_string() == id1)
        .unwrap();
    let id1_token = id1_before.lease_token.as_str().to_string();
    let id1_before_expiry = id1_before.lease_expires_at;
    let id2_before = before
        .iter()
        .find(|lease| lease.item_id.to_string() == id2)
        .unwrap();
    let id2_token = id2_before.lease_token.as_str().to_string();
    let id2_before_expiry = id2_before.lease_expires_at;

    cp.begin_drain(&qkey(), 1, &owner("node-b"), ts(1)).unwrap();
    clock.set(10);
    let result: redis::RedisResult<Vec<String>> = redis::cmd("XCLAIM")
        .arg("t1:q1")
        .arg("g")
        .arg(&id1_token)
        .arg(0)
        .arg(&id1)
        .arg(&id2)
        .arg("JUSTID")
        .query_async(&mut con)
        .await;
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("pqueue unavailable"),
        "drain reassign half should be refused, got {err}"
    );

    let after = backend.pending(&qkey()).await.unwrap();
    let id1_after = after
        .iter()
        .find(|lease| lease.item_id.to_string() == id1)
        .unwrap();
    let id2_after = after
        .iter()
        .find(|lease| lease.item_id.to_string() == id2)
        .unwrap();
    assert_eq!(id1_after.lease_token.as_str(), id1_token);
    assert!(
        id1_after.lease_expires_at > id1_before_expiry,
        "same-consumer XCLAIM renew must still commit during drain"
    );
    assert_eq!(id2_after.lease_token.as_str(), id2_token);
    assert_eq!(
        id2_after.lease_expires_at, id2_before_expiry,
        "cross-consumer XCLAIM reassign must not commit during drain"
    );
    drop(con);
    cancel.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn draining_owner_refuses_new_claim_but_serves_inflight_epoch() {
    let backend = Arc::new(MemoryBackend::new());
    backend.create_queue(qdef()).await.unwrap();
    let cp = Arc::new(InMemoryControlPlane::default());
    let a = OwnershipRuntime::new(
        backend,
        cp.clone(),
        owner("node-a"),
        "10.0.0.1:7000".to_string(),
    );
    let b = owner("node-b");
    a.register_owner(ts(0)).unwrap();
    a.acquire_queue(&qkey(), ts(0)).await.unwrap();
    cp.begin_drain(&qkey(), 1, &b, ts(1)).unwrap();

    let new_claim = a.expected_epoch_for_write(&qkey(), ts(2), true).await;
    assert!(matches!(new_claim, Err(EngineError::Unavailable)));
    let in_flight = a
        .expected_epoch_for_write(&qkey(), ts(2), false)
        .await
        .unwrap();
    assert_eq!(in_flight, Some(1));
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
    let server = start(Config::new(
        BackendSpec::memory(),
        0,
        "127.0.0.1:0".to_string(),
        Duration::from_secs(60),
        vec![qdef()],
    ))
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
async fn objectlog_sqlite_runtime_reopens_rebuilds_and_keeps_item_ids_advancing() {
    let (object_root, projection_path) = tmp_runtime_paths("olsqlite");
    let first_id = {
        let server = start(Config::new(
            objectlog_sqlite_spec(object_root.clone(), projection_path.clone()),
            0,
            "127.0.0.1:0".to_string(),
            Duration::from_secs(60),
            vec![qdef()],
        ))
        .await
        .unwrap();
        let client = redis::Client::open(format!("redis://{}", server.addr())).unwrap();
        let mut con = client.get_multiplexed_async_connection().await.unwrap();
        let produced: String = redis::cmd("XADD")
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
        assert_eq!(reply.keys[0].ids[0].id, produced);
        let acked: i64 = redis::cmd("XACK")
            .arg("t1:q1")
            .arg("g")
            .arg(&produced)
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(acked, 1);
        server.shutdown_and_drain(Duration::from_secs(5)).await;
        produced
    };

    let _ = std::fs::remove_file(&projection_path);
    let server = start(Config::new(
        objectlog_sqlite_spec(object_root.clone(), projection_path.clone()),
        0,
        "127.0.0.1:0".to_string(),
        Duration::from_secs(60),
        vec![qdef()],
    ))
    .await
    .unwrap();
    let client = redis::Client::open(format!("redis://{}", server.addr())).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();
    let empty: Option<StreamReadReply> = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(
        empty.is_none(),
        "acked item was not redelivered after rebuild"
    );
    let next_id: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(9)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_ne!(
        next_id, first_id,
        "post-reopen push must not remint an existing item id"
    );
    server.shutdown_and_drain(Duration::from_secs(5)).await;
    let _ = std::fs::remove_dir_all(&object_root);
    let _ = std::fs::remove_file(&projection_path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segmented_objectlog_sqlite_push_claim_finalize_and_recovers_on_reopen() {
    // The composed objectlog-LOG + sqlite-PROJECTION backend (the segmented object log is the composed
    // `ObjectLog` axis); a push acks only after its segment seals (durable) AND applies to the projection.
    let (object_root, projection_path) = tmp_runtime_paths("segolsqlite");
    let first_id = {
        let server = start(Config::new(
            objectlog_sqlite_spec(object_root.clone(), projection_path.clone()),
            0,
            "127.0.0.1:0".to_string(),
            Duration::from_secs(60),
            vec![qdef()],
        ))
        .await
        .unwrap();
        let client = redis::Client::open(format!("redis://{}", server.addr())).unwrap();
        let mut con = client.get_multiplexed_async_connection().await.unwrap();
        // Push acks only after its segment seals (durable) AND applies to the projection.
        let produced: String = redis::cmd("XADD")
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
        assert_eq!(reply.keys[0].ids[0].id, produced);
        let acked: i64 = redis::cmd("XACK")
            .arg("t1:q1")
            .arg("g")
            .arg(&produced)
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(acked, 1);
        server.shutdown_and_drain(Duration::from_secs(5)).await;
        produced
    };

    // Reopen against the SAME durable segment log but a FRESH projection db: recovery must replay the
    // committed segments (via `read_all`) so the acked item is NOT redelivered and ids keep advancing.
    let _ = std::fs::remove_file(&projection_path);
    let server = start(Config::new(
        objectlog_sqlite_spec(object_root.clone(), projection_path.clone()),
        0,
        "127.0.0.1:0".to_string(),
        Duration::from_secs(60),
        vec![qdef()],
    ))
    .await
    .unwrap();
    let client = redis::Client::open(format!("redis://{}", server.addr())).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();
    let empty: Option<StreamReadReply> = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(
        empty.is_none(),
        "acked item was redelivered after segmented recovery replay"
    );
    let next_id: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(9)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_ne!(
        next_id, first_id,
        "post-reopen push must not remint an existing item id"
    );
    server.shutdown_and_drain(Duration::from_secs(5)).await;
    let _ = std::fs::remove_dir_all(&object_root);
    let _ = std::fs::remove_file(&projection_path);
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
