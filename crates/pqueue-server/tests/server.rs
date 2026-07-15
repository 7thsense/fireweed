//! Composition-root integration: the background ReclaimDriver task recovers orphaned leases with no
//! client traffic, and the wired server is drivable by an off-the-shelf Redis client.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use pqueue_core::{
    EligibilityPolicy, LeaseToken, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId,
    RecurrencePolicy, RequestId, RetryPolicy, TenantId, UtcTimestamp, WorkerId,
};
use pqueue_engine::{
    ChangeRecord, ChangeRecordKind, ChangeRecordSink, ClaimPort, ClaimRequest, Clock,
    ComposedBackend, ControlPlaneStore, EngineError, FinalizeKind, FinalizeOutcome, FinalizePort,
    InMemoryControlPlane, InProcessControlPlane, LogStore, ProjectionRead, PushPort, PushSpec,
    QueueControlPlane, QueueKey,
};
use pqueue_memory::{ManualClock, composed_memory_backend};
use pqueue_objectlog::ObjectLog;
use pqueue_resp::{RespHooks, RouteDecision, SystemClock, serve_with_shutdown_and_hooks};
use pqueue_server::{
    BackendSpec, ChangeRecordSinkConfig, Config, ControlPlaneSpec, LogSpec,
    NiflheimChangeRecordSink, OwnershipRuntime, ProjectionSpec, emit_change_record_tick, start,
    start_with,
};
use pqueue_sqlite::{HybridProjectionStore, composed_sqlite_backend_in_memory};

/// The composed objectlog-LOG + sqlite-PROJECTION spec (replaces the retired `Backend::ObjectLogSqlite` /
/// `Backend::SegmentedObjectLogSqlite` variants — both are now this one composition).
fn objectlog_sqlite_spec(root: std::path::PathBuf, projection: std::path::PathBuf) -> BackendSpec {
    BackendSpec {
        log: LogSpec::ObjectLog { root },
        projection: ProjectionSpec::Sqlite { path: projection },
        control_plane: ControlPlaneSpec::InProcess,
    }
}

fn objectlog_hybrid_spec(root: std::path::PathBuf, projection: std::path::PathBuf) -> BackendSpec {
    BackendSpec {
        log: LogSpec::ObjectLog { root },
        projection: ProjectionSpec::Hybrid { path: projection },
        control_plane: ControlPlaneSpec::InProcess,
    }
}

fn objectlog_hybrid_async_spec(
    root: std::path::PathBuf,
    projection: std::path::PathBuf,
) -> BackendSpec {
    BackendSpec {
        log: LogSpec::ObjectLog { root },
        projection: ProjectionSpec::HybridAsync { path: projection },
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
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
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

fn open_direct_objectlog_hybrid(
    root: &std::path::Path,
    projection: &std::path::Path,
) -> ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane> {
    ComposedBackend::new(
        ObjectLog::open_group_commit(
            root,
            pqueue_server::SegmentConfig::new(1024 * 1024, 5).unwrap(),
        )
        .expect("open object log"),
        HybridProjectionStore::open(projection.to_str().expect("utf8 projection path"))
            .expect("open hybrid projection"),
        InProcessControlPlane::new(),
    )
    .with_group_commit(true)
    .recover()
    .expect("recover objectlog/hybrid")
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
    let backend = Arc::new(composed_memory_backend());
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
    let backend = Arc::new(composed_memory_backend());
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
    let backend = Arc::new(composed_memory_backend());
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
    let backend = Arc::new(composed_memory_backend());
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
            eligibility_time: None,
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
    let backend = Arc::new(composed_memory_backend());
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
    let backend = Arc::new(composed_memory_backend());
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
    let backend = Arc::new(composed_memory_backend());
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
    let backend = Arc::new(composed_memory_backend());
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
    let backend = Arc::new(composed_memory_backend());
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
            eligibility_time: None,
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
async fn terminal_emission_metrics_reach_server_surface() {
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
        .arg(11)
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
    let claimed_id = reply.keys[0].ids[0].id.clone();
    let _: i64 = redis::cmd("XACK")
        .arg("t1:q1")
        .arg("g")
        .arg(&claimed_id)
        .query_async(&mut con)
        .await
        .unwrap();

    let info: std::collections::HashMap<String, redis::Value> = redis::cmd("XINFO")
        .arg("STREAM")
        .arg("t1:q1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(
        match &info["resident-terminal-count"] {
            redis::Value::Int(n) => *n,
            other => panic!("XINFO STREAM resident-terminal-count should be an int, got {other:?}"),
        },
        1,
        "the server surface reads terminal emission metrics"
    );
    assert_eq!(
        match &info["length"] {
            redis::Value::Int(n) => *n,
            other => panic!("XINFO STREAM length should be an int, got {other:?}"),
        },
        0,
        "live-count behavior stays unchanged"
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objectlog_hybrid_push_claim_finalize_and_recovers_on_reopen() {
    let (object_root, projection_path) = tmp_runtime_paths("objectlog-hybrid");
    let first_id = {
        let mut config = Config::new(
            objectlog_hybrid_spec(object_root.clone(), projection_path.clone()),
            0,
            "127.0.0.1:0".to_string(),
            Duration::from_secs(60),
            vec![qdef()],
        );
        config.segment_config =
            pqueue_server::SegmentConfig::new(1024 * 1024, 5).expect("valid segment config");
        let server = start(config).await.unwrap();
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

    let server = start(Config::new(
        objectlog_hybrid_spec(object_root.clone(), projection_path.clone()),
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
        "acked item was redelivered after objectlog/hybrid recovery"
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
        "post-reopen hybrid push must not remint an existing item id"
    );
    server.shutdown_and_drain(Duration::from_secs(5)).await;
    let _ = std::fs::remove_dir_all(&object_root);
    let _ = std::fs::remove_file(&projection_path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objectlog_hybrid_async_push_claim_finalize_and_recovers_on_reopen() {
    // The `objectlog/hybrid-async` runtime profile end to end: it selects the object-log + hybrid substrate
    // (manifest commit + synchronous in-memory apply/render is the success barrier; the SQLite image is an
    // asynchronous checkpoint), carries the async-apply thresholds, and recovers acked state on reopen.
    let (object_root, projection_path) = tmp_runtime_paths("objectlog-hybrid-async");
    let first_id = {
        let mut config = Config::new(
            objectlog_hybrid_async_spec(object_root.clone(), projection_path.clone()),
            0,
            "127.0.0.1:0".to_string(),
            Duration::from_secs(60),
            vec![qdef()],
        );
        config.segment_config =
            pqueue_server::SegmentConfig::new(1024 * 1024, 5).expect("valid segment config");
        // A non-default threshold config the async profile carries into `start`.
        config.hybrid_async =
            pqueue_server::HybridAsyncThresholds::new(4096, 8 * 1024 * 1024, 64, 30_000, 5)
                .expect("valid hybrid-async thresholds");
        let server = start(config).await.unwrap();
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

    let server = start(Config::new(
        objectlog_hybrid_async_spec(object_root.clone(), projection_path.clone()),
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
        "acked item was redelivered after objectlog/hybrid-async recovery"
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
        "post-reopen hybrid-async push must not remint an existing item id"
    );
    server.shutdown_and_drain(Duration::from_secs(5)).await;
    let _ = std::fs::remove_dir_all(&object_root);
    let _ = std::fs::remove_file(&projection_path);
}

/// The `objectlog/hybrid-async` config used by the crash/chaos tests below: the async spec plus a non-default
/// threshold set so the profile is exercised end to end (bead pqueue-fed791af).
fn objectlog_hybrid_async_config(
    object_root: std::path::PathBuf,
    projection_path: std::path::PathBuf,
) -> Config {
    let mut config = Config::new(
        objectlog_hybrid_async_spec(object_root, projection_path),
        0,
        "127.0.0.1:0".to_string(),
        Duration::from_secs(60),
        vec![qdef()],
    );
    config.segment_config =
        pqueue_server::SegmentConfig::new(1024 * 1024, 5).expect("valid segment config");
    config.hybrid_async =
        pqueue_server::HybridAsyncThresholds::new(4096, 8 * 1024 * 1024, 64, 30_000, 5)
            .expect("valid hybrid-async thresholds");
    config
}

/// CHAOS — crash MID-LEASE on the `objectlog/hybrid-async` profile: an item is claimed (XREADGROUP) but never
/// acked, then the server is dropped. On restart the recovered lease is neither DUPLICATED (a fresh
/// XREADGROUP does not redeliver it) nor LOST (a subsequently-pushed item is the only thing delivered — the
/// leased item stays in-flight, not re-queued to pending).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objectlog_hybrid_async_chaos_crash_mid_lease_neither_redelivers_nor_loses() {
    let (object_root, projection_path) =
        tmp_runtime_paths("objectlog-hybrid-async-chaos-mid-lease");
    let leased_id = {
        let server = start(objectlog_hybrid_async_config(
            object_root.clone(),
            projection_path.clone(),
        ))
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
        // Claim (deliver) the item but DO NOT ack it — a crash strikes mid-lease.
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
        server.shutdown_and_drain(Duration::from_secs(5)).await;
        id
    };

    let server = start(objectlog_hybrid_async_config(
        object_root.clone(),
        projection_path.clone(),
    ))
    .await
    .unwrap();
    let client = redis::Client::open(format!("redis://{}", server.addr())).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // The recovered lease is still valid, so a fresh read does NOT redeliver it (no duplicate lease).
    let redelivered: Option<StreamReadReply> = redis::cmd("XREADGROUP")
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
        redelivered.is_none(),
        "a still-valid recovered lease must not be redelivered after a mid-lease crash"
    );

    // The leased item was not lost back to pending: pushing a NEW item and reading yields exactly that new
    // item (the old one is held in-flight, not re-queued).
    let fresh: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(9)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_ne!(fresh, leased_id, "post-crash push minted a distinct id");
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
        "exactly the fresh item is delivered; the in-flight lease was neither lost nor duplicated"
    );
    assert_eq!(reply.keys[0].ids[0].id, fresh);
    server.shutdown_and_drain(Duration::from_secs(5)).await;
    let _ = std::fs::remove_dir_all(&object_root);
    let _ = std::fs::remove_file(&projection_path);
}

/// CHAOS — disk-loss of the SQLite projection image on the `objectlog/hybrid-async` profile: after two pushes
/// the server is dropped and the projection db is DELETED. Because the object log is the source of truth, a
/// restart replays the retained log from genesis and both items are delivered exactly once (nothing lost,
/// nothing duplicated).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objectlog_hybrid_async_chaos_disk_loss_replays_retained_object_log() {
    let (object_root, projection_path) =
        tmp_runtime_paths("objectlog-hybrid-async-chaos-disk-loss");
    {
        let server = start(objectlog_hybrid_async_config(
            object_root.clone(),
            projection_path.clone(),
        ))
        .await
        .unwrap();
        let client = redis::Client::open(format!("redis://{}", server.addr())).unwrap();
        let mut con = client.get_multiplexed_async_connection().await.unwrap();
        let first: String = redis::cmd("XADD")
            .arg("t1:q1")
            .arg("*")
            .arg("priority")
            .arg(1)
            .query_async(&mut con)
            .await
            .unwrap();
        let second: String = redis::cmd("XADD")
            .arg("t1:q1")
            .arg("*")
            .arg("priority")
            .arg(2)
            .query_async(&mut con)
            .await
            .unwrap();
        assert_ne!(first, second);
        server.shutdown_and_drain(Duration::from_secs(5)).await;
    }

    // DISK LOSS: the async SQLite projection image is gone; only the durable object log remains.
    std::fs::remove_file(&projection_path).unwrap();

    let server = start(objectlog_hybrid_async_config(
        object_root.clone(),
        projection_path.clone(),
    ))
    .await
    .unwrap();
    let client = redis::Client::open(format!("redis://{}", server.addr())).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();
    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("COUNT")
        .arg(2)
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(
        reply.keys[0].ids.len(),
        2,
        "a fresh hybrid-async projection db replays the retained object log from genesis after disk loss"
    );
    server.shutdown_and_drain(Duration::from_secs(5)).await;
    let _ = std::fs::remove_dir_all(&object_root);
    let _ = std::fs::remove_file(&projection_path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objectlog_hybrid_disk_loss_replays_retained_object_log() {
    let (object_root, projection_path) = tmp_runtime_paths("objectlog-hybrid-disk-loss");
    {
        let mut config = Config::new(
            objectlog_hybrid_spec(object_root.clone(), projection_path.clone()),
            0,
            "127.0.0.1:0".to_string(),
            Duration::from_secs(60),
            vec![qdef()],
        );
        config.segment_config =
            pqueue_server::SegmentConfig::new(1024 * 1024, 5).expect("valid segment config");
        let server = start(config).await.unwrap();
        let client = redis::Client::open(format!("redis://{}", server.addr())).unwrap();
        let mut con = client.get_multiplexed_async_connection().await.unwrap();
        let first: String = redis::cmd("XADD")
            .arg("t1:q1")
            .arg("*")
            .arg("priority")
            .arg(1)
            .query_async(&mut con)
            .await
            .unwrap();
        let second: String = redis::cmd("XADD")
            .arg("t1:q1")
            .arg("*")
            .arg("priority")
            .arg(2)
            .query_async(&mut con)
            .await
            .unwrap();
        assert_ne!(first, second);
        server.shutdown_and_drain(Duration::from_secs(5)).await;
    }

    std::fs::remove_file(&projection_path).unwrap();
    let server = start(Config::new(
        objectlog_hybrid_spec(object_root.clone(), projection_path.clone()),
        0,
        "127.0.0.1:0".to_string(),
        Duration::from_secs(60),
        vec![qdef()],
    ))
    .await
    .unwrap();
    let client = redis::Client::open(format!("redis://{}", server.addr())).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();
    let reply: StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("COUNT")
        .arg(2)
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(
        reply.keys[0].ids.len(),
        2,
        "fresh projection db replays retained object log from genesis"
    );
    server.shutdown_and_drain(Duration::from_secs(5)).await;
    let _ = std::fs::remove_dir_all(&object_root);
    let _ = std::fs::remove_file(&projection_path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn change_record_sink_rejected_on_unwired_profile() {
    let config = Config::new(
        BackendSpec {
            log: LogSpec::Memory,
            projection: ProjectionSpec::InMemory,
            control_plane: ControlPlaneSpec::InProcess,
        },
        0,
        "127.0.0.1:0".to_string(),
        Duration::from_secs(60),
        vec![qdef()],
    );
    let mut config = config;
    config.change_record_sink = ChangeRecordSinkConfig {
        enabled: true,
        endpoint: Some("kafka://127.0.0.1:9092".to_string()),
        ..ChangeRecordSinkConfig::default()
    };

    match start(config).await {
        Ok(_) => panic!("memory backend must refuse sink startup"),
        Err(err) => assert!(
            err.to_string().contains("only wired for objectlog/hybrid"),
            "{}",
            err
        ),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn change_record_sink_rejected_without_durable_cursor_store() {
    let (object_root, projection_path) = tmp_runtime_paths("change-record-sink-no-cursor");
    let mut config = Config::new(
        objectlog_hybrid_spec(object_root.clone(), projection_path.clone()),
        0,
        "127.0.0.1:0".to_string(),
        Duration::from_secs(60),
        vec![qdef()],
    );
    config.segment_config =
        pqueue_server::SegmentConfig::new(1024 * 1024, 5).expect("valid segment config");
    config.change_record_sink = ChangeRecordSinkConfig {
        enabled: true,
        endpoint: Some("kafka://127.0.0.1:9092".to_string()),
        ..ChangeRecordSinkConfig::default()
    };

    match start(config).await {
        Ok(_) => panic!("objectlog/hybrid must refuse sink startup without a durable cursor"),
        Err(err) => assert!(
            err.to_string().contains("durable emission cursor store"),
            "{}",
            err
        ),
    }
    let _ = std::fs::remove_dir_all(&object_root);
    let _ = std::fs::remove_file(&projection_path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn objectlog_hybrid_force_seals_before_claim_and_fences_stale_epoch() {
    let (object_root, projection_path) = tmp_runtime_paths("objectlog-hybrid-force-seal");
    let backend = Arc::new(open_direct_objectlog_hybrid(&object_root, &projection_path));
    backend.create_queue(qdef()).await.unwrap();
    let flusher_backend = backend.clone();
    let flusher = tokio::spawn(async move {
        loop {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
                .unwrap_or(0);
            let _ = flusher_backend.flush_tick(now_ms);
            let _ = flusher_backend.flush_deferred_projection();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });
    let queue = shard();
    let e0 = backend.current_epoch(&queue).await.unwrap();
    let mut push = Box::pin(backend.push(
        &queue,
        vec![PushSpec {
            priority: Some(PriorityValue::Int64(1)),
            ..Default::default()
        }],
        ts(0),
        Some(e0),
    ));
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    assert!(
        matches!(push.as_mut().poll(&mut cx), std::task::Poll::Pending),
        "large-segment push must buffer until a seal"
    );
    let claimed = backend
        .claim(ClaimRequest {
            eligibility_time: None,
            shard: queue.clone(),
            worker_id: WorkerId::new("claimer").unwrap(),
            max_items: 1,
            lease_token: LeaseToken::new("lease-force-seal").unwrap(),
            lease_expires_at: ts(60),
            now: ts(1),
            compatibility: pqueue_engine::ClaimCompatibility::default(),
            expected_epoch: Some(e0),
        })
        .await
        .unwrap();
    assert_eq!(
        claimed.items.len(),
        1,
        "claim must see the force-sealed pending push"
    );
    let ids = tokio::time::timeout(Duration::from_secs(1), push)
        .await
        .expect("force-sealed push acked")
        .unwrap();
    assert_eq!(
        claimed.items[0].item_id, ids[0],
        "claim saw force-sealed push"
    );

    let e1 = backend.acquire_epoch(&queue).await.unwrap();
    assert!(e1 > e0);
    let stale = backend
        .push_with_request_id(
            &queue,
            RequestId::new("stale-writer").unwrap(),
            vec![PushSpec {
                priority: Some(PriorityValue::Int64(2)),
                ..Default::default()
            }],
            ts(2),
            Some(e0),
        )
        .await
        .unwrap_err();
    assert_eq!(stale, EngineError::EpochFenced);

    flusher.abort();
    let _ = std::fs::remove_dir_all(&object_root);
    let _ = std::fs::remove_file(&projection_path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn objectlog_hybrid_request_id_replays_after_reopen() {
    let (object_root, projection_path) = tmp_runtime_paths("objectlog-hybrid-request-id");
    let request_id = RequestId::new("hybrid-request-1").unwrap();
    let body = vec![PushSpec {
        priority: Some(PriorityValue::Int64(5)),
        ..Default::default()
    }];
    let first = {
        let backend = open_direct_objectlog_hybrid(&object_root, &projection_path);
        backend.create_queue(qdef()).await.unwrap();
        backend
            .push_with_request_id(&shard(), request_id.clone(), body.clone(), ts(0), None)
            .await
            .unwrap()
    };

    let reopened = open_direct_objectlog_hybrid(&object_root, &projection_path);
    let replayed = reopened
        .push_with_request_id(&shard(), request_id.clone(), body, ts(1), None)
        .await
        .unwrap();
    assert_eq!(replayed, first);
    assert_eq!(reopened.metrics(&shard()).await.unwrap().pending, 1);
    let err = reopened
        .push_with_request_id(
            &shard(),
            request_id,
            vec![PushSpec {
                priority: Some(PriorityValue::Int64(6)),
                ..Default::default()
            }],
            ts(2),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::RequestIdConflict);

    let _ = std::fs::remove_dir_all(&object_root);
    let _ = std::fs::remove_file(&projection_path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn boots_and_is_drivable_by_offtheshelf_redis_client() {
    let backend = Arc::new(composed_memory_backend());
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
    let backend = Arc::new(composed_memory_backend());
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

async fn accept_change_record_requests(
    listener: TcpListener,
    statuses: Vec<u16>,
    captured: Arc<std::sync::Mutex<Vec<Vec<ChangeRecord>>>>,
) {
    for status in statuses {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        socket.read_to_end(&mut request).await.unwrap();
        let request = String::from_utf8(request).unwrap();
        let body = request.split("\r\n\r\n").nth(1).unwrap_or("");
        let records: Vec<ChangeRecord> = serde_json::from_str(body).unwrap();
        captured.lock().unwrap().push(records);
        let reason = if (200..300).contains(&status) {
            "OK"
        } else {
            "ERROR"
        };
        let response =
            format!("HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        socket.write_all(response.as_bytes()).await.unwrap();
    }
}

fn sink_config(addr: std::net::SocketAddr) -> ChangeRecordSinkConfig {
    let mut headers = std::collections::BTreeMap::new();
    headers.insert("authorization".to_string(), "Bearer test".to_string());
    ChangeRecordSinkConfig {
        enabled: true,
        endpoint: Some(format!("http://127.0.0.1:{}/ingest", addr.port())),
        headers,
        tick_interval: Duration::from_millis(1),
        batch_size: 16,
    }
}

#[tokio::test]
async fn change_record_sink_delivers() {
    let backend = Arc::new(composed_memory_backend());
    let shard = qkey();
    backend.create_queue(qdef()).await.unwrap();
    let pushed = backend
        .push(&shard, vec![PushSpec::default()], ts(0), None)
        .await
        .unwrap();
    let claim = backend
        .claim(ClaimRequest {
            eligibility_time: None,
            shard: shard.clone(),
            worker_id: WorkerId::new("worker-1").unwrap(),
            max_items: 1,
            lease_token: LeaseToken::new("lease-1").unwrap(),
            lease_expires_at: ts(60),
            now: ts(1),
            compatibility: Default::default(),
            expected_epoch: None,
        })
        .await
        .unwrap();
    assert_eq!(pushed[0], claim.items[0].item_id);
    backend
        .finalize(
            &shard,
            vec![FinalizeOutcome::new(
                claim.items[0].item_id,
                FinalizeKind::Complete,
            )],
            ts(2),
            None,
        )
        .await
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
    let receiver = tokio::spawn(accept_change_record_requests(
        listener,
        vec![200],
        captured.clone(),
    ));
    let config = sink_config(addr);
    let sink = NiflheimChangeRecordSink::new(&config).unwrap();

    emit_change_record_tick(backend.as_ref(), &sink, &[qdef()], config.batch_size).unwrap();
    receiver.await.unwrap();

    let received = captured.lock().unwrap().clone();
    assert_eq!(received.len(), 1);
    let records = &received[0];
    assert_eq!(records.len(), 3);
    assert_eq!(
        records.iter().map(|r| r.command_kind).collect::<Vec<_>>(),
        vec![
            ChangeRecordKind::Push,
            ChangeRecordKind::Claim,
            ChangeRecordKind::Finalize,
        ]
    );
    assert!(records[0].position.sequence < records[1].position.sequence);
    assert!(records[1].position.sequence < records[2].position.sequence);
    let keys: Vec<_> = records
        .iter()
        .map(|record| record.idempotency_key())
        .collect();
    assert_eq!(keys[0].2, records[0].item_id);
    assert_eq!(keys[1].2, records[1].item_id);
    assert_eq!(keys[2].2, records[2].item_id);
    assert!(keys[0] != keys[1] && keys[1] != keys[2] && keys[0] != keys[2]);
}

#[tokio::test]
async fn change_record_sink_failure_isolation() {
    let backend = Arc::new(composed_memory_backend());
    let shard = qkey();
    backend.create_queue(qdef()).await.unwrap();
    let pushed = backend
        .push(&shard, vec![PushSpec::default()], ts(0), None)
        .await
        .unwrap();
    let claim = backend
        .claim(ClaimRequest {
            eligibility_time: None,
            shard: shard.clone(),
            worker_id: WorkerId::new("worker-1").unwrap(),
            max_items: 1,
            lease_token: LeaseToken::new("lease-1").unwrap(),
            lease_expires_at: ts(60),
            now: ts(1),
            compatibility: Default::default(),
            expected_epoch: None,
        })
        .await
        .unwrap();
    assert_eq!(pushed[0], claim.items[0].item_id);
    backend
        .finalize(
            &shard,
            vec![FinalizeOutcome::new(
                claim.items[0].item_id,
                FinalizeKind::Complete,
            )],
            ts(2),
            None,
        )
        .await
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
    let receiver = tokio::spawn(accept_change_record_requests(
        listener,
        vec![500, 200],
        captured.clone(),
    ));
    let config = sink_config(addr);
    let sink = NiflheimChangeRecordSink::new(&config).unwrap();

    emit_change_record_tick(backend.as_ref(), &sink, &[qdef()], config.batch_size).unwrap();
    emit_change_record_tick(backend.as_ref(), &sink, &[qdef()], config.batch_size).unwrap();
    receiver.await.unwrap();

    let received = captured.lock().unwrap().clone();
    assert_eq!(received.len(), 2);
    assert_eq!(received[0].len(), 3);
    assert_eq!(received[1].len(), 3);
    let stable = |records: &Vec<ChangeRecord>| {
        records
            .iter()
            .map(|record| {
                (
                    record.item_id,
                    record.position,
                    record.command_kind,
                    record.new_state,
                    record.terminal_at,
                    record.source_owner_id.clone(),
                    record.source_epoch,
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(stable(&received[0]), stable(&received[1]));
}

#[derive(Default)]
struct RecordingChangeRecordSink {
    batches: std::sync::Mutex<Vec<Vec<ChangeRecordKind>>>,
}

impl RecordingChangeRecordSink {
    fn batches(&self) -> Vec<Vec<ChangeRecordKind>> {
        self.batches.lock().expect("sink poisoned").clone()
    }
}

impl ChangeRecordSink for RecordingChangeRecordSink {
    fn emit(&self, _shard: &QueueKey, records: &[ChangeRecord]) -> pqueue_engine::EngineResult<()> {
        self.batches
            .lock()
            .expect("sink poisoned")
            .push(records.iter().map(|record| record.command_kind).collect());
        Ok(())
    }
}

#[tokio::test]
async fn emit_enabled_queues_reap_terminal_items_only_after_cursor_reaches_terminal_record() {
    let backend = Arc::new(composed_sqlite_backend_in_memory().unwrap());
    let shard = qkey();
    backend.create_queue(qdef()).await.unwrap();

    let ids = backend
        .push(&shard, vec![PushSpec::default()], ts(0), None)
        .await
        .unwrap();
    let claim = backend
        .claim(ClaimRequest {
            eligibility_time: None,
            shard: shard.clone(),
            worker_id: WorkerId::new("worker-1").unwrap(),
            max_items: 1,
            lease_token: LeaseToken::new("lease-1").unwrap(),
            lease_expires_at: ts(60),
            now: ts(1),
            compatibility: Default::default(),
            expected_epoch: None,
        })
        .await
        .unwrap();
    assert_eq!(claim.items[0].item_id, ids[0]);
    backend
        .finalize(
            &shard,
            vec![FinalizeOutcome::new(ids[0], FinalizeKind::Complete)],
            ts(2),
            None,
        )
        .await
        .unwrap();

    let sink = RecordingChangeRecordSink::default();
    let queues = vec![qdef()];

    emit_change_record_tick(backend.as_ref(), &sink, &queues, 1).unwrap();
    assert_eq!(
        backend.with_log(|log| log.emission_cursor(&shard).unwrap()),
        Some(pqueue_engine::CommandPosition::new(shard.clone(), 0, 0))
    );
    assert_eq!(backend.metrics(&shard).await.unwrap().complete, 1);

    emit_change_record_tick(backend.as_ref(), &sink, &queues, 1).unwrap();
    assert_eq!(
        backend.with_log(|log| log.emission_cursor(&shard).unwrap()),
        Some(pqueue_engine::CommandPosition::new(shard.clone(), 0, 1))
    );
    assert_eq!(backend.metrics(&shard).await.unwrap().complete, 1);

    emit_change_record_tick(backend.as_ref(), &sink, &queues, 1).unwrap();
    assert_eq!(
        backend.with_log(|log| log.emission_cursor(&shard).unwrap()),
        Some(pqueue_engine::CommandPosition::new(shard.clone(), 0, 2))
    );
    assert_eq!(backend.metrics(&shard).await.unwrap().complete, 0);
    assert_eq!(
        sink.batches(),
        vec![
            vec![ChangeRecordKind::Push],
            vec![ChangeRecordKind::Claim],
            vec![ChangeRecordKind::Finalize],
        ]
    );
}
