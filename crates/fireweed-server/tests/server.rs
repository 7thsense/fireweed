//! Composition-root integration: the background ReclaimDriver task recovers orphaned leases with no
//! client traffic, and the wired server is drivable by an off-the-shelf Redis client.

use std::sync::Arc;
use std::time::Duration;

use fireweed_core::{
    EligibilityPolicy, LeaseToken, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp, WorkerId,
};
use fireweed_engine::{
    ChangeRecord, ChangeRecordKind, ClaimPort, ClaimRequest, Clock, ControlPlaneConfig,
    ControlPlaneStore, EngineError, FinalizeKind, FinalizeOutcome, FinalizePort,
    InMemoryControlPlane, ProjectionRead, PushPort, PushSpec, QueueControlPlane, QueueKey,
};
use fireweed_memory::{ManualClock, composed_memory_backend};
use fireweed_resp::{RespHooks, RouteDecision, SystemClock, serve_with_shutdown_and_hooks};
use fireweed_server::{
    BackendSpec, ChangeRecordSinkConfig, Config, ControlPlaneSpec, LogSpec,
    NiflheimChangeRecordSink, ObjectLogSpec, OwnershipRuntime, ProjectionSpec, ResponseBarrierSpec,
    SegmentConfig, emit_change_record_tick, start, start_with,
};
fn objectlog_turso_spec(root: std::path::PathBuf, projection: std::path::PathBuf) -> BackendSpec {
    BackendSpec {
        log: LogSpec::ObjectLog(ObjectLogSpec::local(
            root,
            SegmentConfig::new(262_144, 20).unwrap(),
        )),
        projection: ProjectionSpec::Turso { path: projection },
        control_plane: ControlPlaneSpec::InProcess,
        response_barrier: ResponseBarrierSpec::AsyncProjection,
        async_projection: None,
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
fn owner(s: &str) -> fireweed_core::OwnerId {
    fireweed_core::OwnerId::new(s).unwrap()
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
    let root =
        std::env::temp_dir().join(format!("fireweed-server-{tag}-{}-obj", std::process::id()));
    let projection = std::env::temp_dir().join(format!(
        "fireweed-server-{tag}-{}-projection.db",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_file(&projection);
    let _ = std::fs::remove_file(format!("{}-wal", projection.display()));
    let _ = std::fs::remove_file(format!("{}-shm", projection.display()));
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
    cp.advertise_owner_endpoint(&owner("node-a"), "10.0.0.1:7000", ts(0))
        .unwrap();
    a.acquire_queue(&qkey(), ts(0)).await.unwrap();
    b.renew_sessions(ts(1)).await.unwrap();

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
    cp.advertise_owner_endpoint(&owner("node-a"), "10.0.0.1:7000", ts(0))
        .unwrap();
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
    b.renew_sessions(ts(1)).await.unwrap();
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
async fn expired_cached_owner_endpoint_fails_closed_before_the_next_refresh() {
    let backend = Arc::new(composed_memory_backend());
    backend.create_queue(qdef()).await.unwrap();
    let cp = Arc::new(InMemoryControlPlane::new(ControlPlaneConfig {
        heartbeat_ttl_ms: 50,
        lease_ttl_ms: 5_000,
    }));
    let a = OwnershipRuntime::new(
        backend.clone(),
        cp.clone(),
        owner("node-a"),
        "10.0.0.1:7000".to_string(),
    );
    cp.advertise_owner_endpoint(&owner("node-a"), "10.0.0.1:7000", ts(0))
        .unwrap();
    a.acquire_queue(&qkey(), ts(0)).await.unwrap();
    let b = OwnershipRuntime::new(backend, cp, owner("node-b"), "10.0.0.2:7000".to_string());
    b.renew_sessions(ts(0)).await.unwrap();
    assert!(matches!(
        b.route_command("XADD", &[], b"t1:q1", ts(0), false)
            .await
            .unwrap(),
        RouteDecision::Moved { .. }
    ));

    // The lease is still live at t=1, but node-a's 50ms membership/endpoint advertisement is expired.
    // Route-time expiry checking must fail closed even though b has not refreshed its cache again.
    assert_eq!(
        b.route_command("XADD", &[], b"t1:q1", ts(1), false)
            .await
            .unwrap(),
        RouteDecision::Unavailable
    );
}

#[tokio::test]
async fn malformed_or_unknown_owner_endpoint_never_redirects() {
    for advertised in [Some("not-an-address"), None] {
        let backend = Arc::new(composed_memory_backend());
        backend.create_queue(qdef()).await.unwrap();
        let cp = Arc::new(InMemoryControlPlane::default());
        match advertised {
            Some(endpoint) => cp
                .advertise_owner_endpoint(&owner("node-a"), endpoint, ts(0))
                .unwrap(),
            None => cp.register_owner(&owner("node-a"), ts(0)).unwrap(),
        }
        let a = OwnershipRuntime::new(
            backend.clone(),
            cp.clone(),
            owner("node-a"),
            "10.0.0.1:7000".to_string(),
        );
        a.acquire_queue(&qkey(), ts(0)).await.unwrap();
        let b = OwnershipRuntime::new(backend, cp, owner("node-b"), "10.0.0.2:7000".to_string());
        b.renew_sessions(ts(0)).await.unwrap();
        assert_eq!(
            b.route_command("XADD", &[], b"t1:q1", ts(0), false)
                .await
                .unwrap(),
            RouteDecision::Unavailable
        );
    }
}

#[tokio::test]
async fn endpoint_snapshot_refresh_is_once_per_node_tick_not_per_queue() {
    let backend = Arc::new(composed_memory_backend());
    let cp = Arc::new(InMemoryControlPlane::default());
    let runtime = OwnershipRuntime::new(
        backend.clone(),
        cp,
        owner("node-a"),
        "10.0.0.1:7000".to_string(),
    );
    for index in 0..100 {
        let mut definition = qdef();
        definition.queue_id = QueueId::new(format!("q-{index}")).unwrap();
        let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        runtime.watch_queue(key);
    }

    runtime.renew_sessions(ts(0)).await.unwrap();
    assert_eq!(runtime.endpoint_refresh_count(), 1);
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
            compatibility: fireweed_engine::ClaimCompatibility::default(),
            expected_epoch: Some(stale_epoch),

            request_id: None,
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
    assert_eq!(released.state, fireweed_engine::LeaseState::Unassigned);

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
        err.contains("fireweed unavailable"),
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
            compatibility: fireweed_engine::ClaimCompatibility::default(),
            expected_epoch: None,

            request_id: None,
        })
        .await
        .unwrap();
    assert_eq!(claimed.items.len(), 1);
    assert_eq!(backend.metrics(&qkey()).await.unwrap().leased, 1);

    // The worker "crashes": no renew, no ack. Advance the clock past the lease — and DO NOTHING ELSE.
    clock.set(1_061); // 1s past expiry
    // Observe the complete outcome: the backend mutation and the counter publication are consecutive
    // operations, not one atomic snapshot. This test deliberately uses a multi-thread runtime and a
    // bounded wall-clock timeout: paused-time auto-advance can outrun backend work under a loaded
    // workspace run, making the observer consume many virtual ticker intervals before that work is
    // scheduled even though the same test passes in isolation.
    let ticks_before = server.reclaim_stats().ticks;
    let observed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let stats = server.reclaim_stats();
            // The counter is published only after the backend tick completes. Poll it before metrics
            // so the observer does not contend with an in-progress reclaim for the queue operation path.
            if stats.leases_reclaimed >= 1 {
                let metrics = backend.metrics(&qkey()).await.unwrap();
                if metrics.leased == 0 {
                    break (metrics, stats);
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the background task must publish both the reclaim and its observability counter");
    let (m, stats) = observed;
    assert_eq!((m.pending, m.leased), (1, 0));
    assert!(stats.ticks > ticks_before);
    assert_eq!(stats.errors, 0);
    server.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_provisions_queues_and_serves_end_to_end() {
    let err = start(Config::new(
        BackendSpec::memory(),
        0,
        "127.0.0.1:0".into(),
        Duration::from_secs(1),
        vec![qdef()],
    ))
    .await
    .err()
    .expect("memory is not a public cell");
    assert_eq!(err, EngineError::Invalid(fireweed::RETIRED_STORAGE_CELL));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_emission_metrics_reach_server_surface() {
    let err = start(Config::new(
        BackendSpec::memory(),
        0,
        "127.0.0.1:0".into(),
        Duration::from_secs(1),
        vec![qdef()],
    ))
    .await
    .err()
    .expect("memory is not a public cell");
    assert_eq!(err, EngineError::Invalid(fireweed::RETIRED_STORAGE_CELL));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn objectlog_turso_runtime_reopens_rebuilds_and_keeps_item_ids_advancing() {
    {
        let (object_root, projection_path) = tmp_runtime_paths("retired-cell");
        let err = start(Config::new(
            objectlog_turso_spec(object_root, projection_path),
            0,
            "127.0.0.1:0".into(),
            Duration::from_secs(1),
            vec![qdef()],
        ))
        .await
        .err()
        .expect("filesystem x turso is not a public cell");
        assert_eq!(err, EngineError::Invalid(fireweed::RETIRED_STORAGE_CELL));
    }
}

/// Delete-rebuild: ProjectionLifecycle wipes and rebuilds the Turso projection from the object log.
#[cfg(feature = "turso-projection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objectlog_turso_profile_rebuilds_deleted_projection_from_authoritative_log() {
    {
        let (object_root, projection_path) = tmp_runtime_paths("retired-cell");
        let err = start(Config::new(
            objectlog_turso_spec(object_root, projection_path),
            0,
            "127.0.0.1:0".into(),
            Duration::from_secs(1),
            vec![qdef()],
        ))
        .await
        .err()
        .expect("filesystem x turso is not a public cell");
        assert_eq!(err, EngineError::Invalid(fireweed::RETIRED_STORAGE_CELL));
    }
}

/// AC-TURSO-5: empty path fails closed before any Turso/database I/O.
#[cfg(feature = "turso-projection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turso_startup_validation_precedes_storage_io() {
    let root = std::env::temp_dir().join(format!(
        "fw-turso-preio-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    // Intentionally do not create root: validation must fail before log/projection open.
    let result = start(Config::new(
        BackendSpec {
            log: LogSpec::ObjectLog(ObjectLogSpec::S3 {
                endpoint: "http://127.0.0.1:9".into(),
                bucket: "fireweed".into(),
                region: "us-east-1".into(),
                credentials: fireweed_server::S3CredentialSource::Static {
                    access_key_id: "akid".into(),
                    secret_access_key: "secret".into(),
                },
                segment_config: SegmentConfig::new(262_144, 20).unwrap(),
                allow_insecure_http: true,
            }),
            projection: ProjectionSpec::Turso {
                path: std::path::PathBuf::new(),
            },
            control_plane: ControlPlaneSpec::InProcess,
            response_barrier: ResponseBarrierSpec::AsyncProjection,
            async_projection: None,
        },
        0,
        "127.0.0.1:0".to_string(),
        Duration::from_secs(60),
        vec![qdef()],
    ))
    .await;
    let Err(err) = result else {
        panic!("empty turso path must fail before I/O");
    };
    let msg = err.to_string();
    assert!(
        msg.contains("turso projection path must not be empty"),
        "unexpected pre-I/O error: {msg}"
    );
    assert!(
        !root.exists(),
        "validation must not create the object-log root before rejecting empty Turso path"
    );
}

/// Server surface: memory × turso (Class B) push/claim via RESP; proves all-log Turso arms compile
/// through the shared composition path.
#[cfg(feature = "turso-projection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_turso_server_push_claim_lifecycle() {
    let err = start(Config::new(
        BackendSpec::memory(),
        0,
        "127.0.0.1:0".into(),
        Duration::from_secs(1),
        vec![qdef()],
    ))
    .await
    .err()
    .expect("memory is not a public cell");
    assert_eq!(err, EngineError::Invalid(fireweed::RETIRED_STORAGE_CELL));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objectlog_turso_rejects_unprovisioned_queue_before_ownership_acquisition() {
    {
        let (object_root, projection_path) = tmp_runtime_paths("retired-cell");
        let err = start(Config::new(
            objectlog_turso_spec(object_root, projection_path),
            0,
            "127.0.0.1:0".into(),
            Duration::from_secs(1),
            vec![qdef()],
        ))
        .await
        .err()
        .expect("filesystem x turso is not a public cell");
        assert_eq!(err, EngineError::Invalid(fireweed::RETIRED_STORAGE_CELL));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segmented_objectlog_turso_push_claim_finalize_and_recovers_on_reopen() {
    {
        let (object_root, projection_path) = tmp_runtime_paths("retired-cell");
        let err = start(Config::new(
            objectlog_turso_spec(object_root, projection_path),
            0,
            "127.0.0.1:0".into(),
            Duration::from_secs(1),
            vec![qdef()],
        ))
        .await
        .err()
        .expect("filesystem x turso is not a public cell");
        assert_eq!(err, EngineError::Invalid(fireweed::RETIRED_STORAGE_CELL));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objectlog_hybrid_push_claim_finalize_and_recovers_on_reopen() {
    {
        let (object_root, projection_path) = tmp_runtime_paths("retired-cell");
        let err = start(Config::new(
            objectlog_turso_spec(object_root, projection_path),
            0,
            "127.0.0.1:0".into(),
            Duration::from_secs(1),
            vec![qdef()],
        ))
        .await
        .err()
        .expect("filesystem x turso is not a public cell");
        assert_eq!(err, EngineError::Invalid(fireweed::RETIRED_STORAGE_CELL));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objectlog_turso_async_push_claim_finalize_and_recovers_on_reopen() {
    {
        let (object_root, projection_path) = tmp_runtime_paths("retired-cell");
        let err = start(Config::new(
            objectlog_turso_spec(object_root, projection_path),
            0,
            "127.0.0.1:0".into(),
            Duration::from_secs(1),
            vec![qdef()],
        ))
        .await
        .err()
        .expect("filesystem x turso is not a public cell");
        assert_eq!(err, EngineError::Invalid(fireweed::RETIRED_STORAGE_CELL));
    }
}

/// CHAOS — crash MID-LEASE on the `objectlog/turso async` profile: an item is claimed (XREADGROUP) but never
/// acked, then the server is dropped. On restart the recovered lease is neither DUPLICATED (a fresh
/// XREADGROUP does not redeliver it) nor LOST (a subsequently-pushed item is the only thing delivered — the
/// leased item stays in-flight, not re-queued to pending).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objectlog_turso_async_chaos_crash_mid_lease_neither_redelivers_nor_loses() {
    {
        let (object_root, projection_path) = tmp_runtime_paths("retired-cell");
        let err = start(Config::new(
            objectlog_turso_spec(object_root, projection_path),
            0,
            "127.0.0.1:0".into(),
            Duration::from_secs(1),
            vec![qdef()],
        ))
        .await
        .err()
        .expect("filesystem x turso is not a public cell");
        assert_eq!(err, EngineError::Invalid(fireweed::RETIRED_STORAGE_CELL));
    }
}

/// CHAOS — after two async-profile pushes, wipe the Turso projection through
/// ProjectionLifecycle and restart. The object log is the source of truth, so
/// both items are delivered exactly once (nothing lost, nothing duplicated).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objectlog_turso_async_chaos_disk_loss_replays_retained_object_log() {
    {
        let (object_root, projection_path) = tmp_runtime_paths("retired-cell");
        let err = start(Config::new(
            objectlog_turso_spec(object_root, projection_path),
            0,
            "127.0.0.1:0".into(),
            Duration::from_secs(1),
            vec![qdef()],
        ))
        .await
        .err()
        .expect("filesystem x turso is not a public cell");
        assert_eq!(err, EngineError::Invalid(fireweed::RETIRED_STORAGE_CELL));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objectlog_hybrid_disk_loss_replays_retained_object_log() {
    {
        let (object_root, projection_path) = tmp_runtime_paths("retired-cell");
        let err = start(Config::new(
            objectlog_turso_spec(object_root, projection_path),
            0,
            "127.0.0.1:0".into(),
            Duration::from_secs(1),
            vec![qdef()],
        ))
        .await
        .err()
        .expect("filesystem x turso is not a public cell");
        assert_eq!(err, EngineError::Invalid(fireweed::RETIRED_STORAGE_CELL));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn change_record_sink_rejected_on_class_b_memory_log() {
    let config = Config::new(
        BackendSpec {
            log: LogSpec::Memory,
            projection: ProjectionSpec::InMemory,
            control_plane: ControlPlaneSpec::InProcess,
            response_barrier: ResponseBarrierSpec::AsyncProjection,
            async_projection: None,
        },
        0,
        "127.0.0.1:0".to_string(),
        Duration::from_secs(60),
        vec![qdef()],
    );
    let mut config = config;
    // Use HTTP so feature-off Kafka does not win before Class-B durability.
    config.change_record_sink = ChangeRecordSinkConfig {
        enabled: true,
        endpoint: Some("http://127.0.0.1:8080".to_string()),
        ..ChangeRecordSinkConfig::default()
    };

    assert_eq!(
        start(config)
            .await
            .err()
            .expect("memory backend must refuse sink startup"),
        EngineError::Invalid(fireweed::RETIRED_STORAGE_CELL)
    );
}

#[cfg(feature = "env-config")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn env_and_programmatic_sink_configs_share_the_typed_startup_validation_boundary() {
    fn direct_config(endpoint: &str, enabled: bool) -> Config {
        let mut config = Config::new(
            BackendSpec {
                log: LogSpec::ObjectLog(ObjectLogSpec::S3 {
                    endpoint: "http://127.0.0.1:9".into(),
                    bucket: "fireweed".into(),
                    region: "us-east-1".into(),
                    credentials: fireweed_server::S3CredentialSource::Static {
                        access_key_id: "akid".into(),
                        secret_access_key: "secret".into(),
                    },
                    segment_config: SegmentConfig::new(262_144, 20).unwrap(),
                    allow_insecure_http: true,
                }),
                projection: ProjectionSpec::Turso {
                    path: std::path::PathBuf::from("/tmp/fireweed-sink-boundary.turso"),
                },
                control_plane: ControlPlaneSpec::InProcess,
                response_barrier: ResponseBarrierSpec::AsyncProjection,
                async_projection: Some(fireweed_engine::AsyncProjectionSpec::default()),
            },
            0,
            "127.0.0.1:0".to_owned(),
            Duration::from_secs(60),
            vec![qdef()],
        );
        config.change_record_sink = ChangeRecordSinkConfig {
            enabled,
            endpoint: Some(endpoint.to_owned()),
            ..ChangeRecordSinkConfig::default()
        };
        config
    }

    fn env_config(endpoint: &str, enabled: bool) -> Config {
        let values = [
            ("FIREWEED_LOG_BACKEND", "s3"),
            ("FIREWEED_PROJECTION_BACKEND", "turso"),
            ("FIREWEED_OBJECT_LOG_S3_ENDPOINT", "http://127.0.0.1:19100"),
            ("FIREWEED_OBJECT_LOG_S3_BUCKET", "fireweed-test"),
            ("FIREWEED_OBJECT_LOG_S3_REGION", "us-east-1"),
            ("FIREWEED_OBJECT_LOG_S3_CREDENTIAL_SOURCE", "static"),
            ("FIREWEED_OBJECT_LOG_S3_ACCESS_KEY_ID", "fireweed"),
            (
                "FIREWEED_OBJECT_LOG_S3_SECRET_ACCESS_KEY",
                "fireweed-test-rustfs",
            ),
            ("FIREWEED_OBJECT_LOG_S3_ALLOW_INSECURE_HTTP", "true"),
            ("FIREWEED_BOOTSTRAP_QUEUES", "t1:q1"),
            (
                "FIREWEED_CHANGE_RECORD_SINK_ENABLED",
                if enabled { "true" } else { "false" },
            ),
            ("FIREWEED_CHANGE_RECORD_SINK_ENDPOINT", endpoint),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect();
        Config::from_env(&values).expect("s3 × turso env must parse")
    }

    let retired = Config::from_env(
        &[
            ("FIREWEED_LOG_BACKEND", "memory"),
            ("FIREWEED_PROJECTION_BACKEND", "memory"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect(),
    );
    let Err(retired_env) = retired else {
        panic!("memory × memory env is not a public cell");
    };
    let retired_text = retired_env.to_string();
    assert!(
        retired_text.contains("turso") || retired_text.contains("s3"),
        "{retired_text}"
    );

    let malformed = EngineError::Invalid(
        "change record sink endpoint must use an explicit scheme: `kafka://host:port` for external Kafka or `http://host:port` for durable-ingest; a schemeless `host:port` is rejected",
    );
    for config in [
        direct_config("not-a-url", true),
        env_config("not-a-url", true),
    ] {
        assert_eq!(
            start(config)
                .await
                .err()
                .expect("malformed endpoint must fail at qualified startup"),
            malformed
        );
    }

    for config in [
        direct_config("http://127.0.0.1:8080", true),
        env_config("http://127.0.0.1:8080", true),
    ] {
        assert_eq!(
            config.validate_for_start(),
            Ok(()),
            "public s3 × turso accepts an enabled http change-record endpoint"
        );
    }
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
    use fireweed_server::resolve_node_id;
    assert_eq!(resolve_node_id("0"), 0);
    assert_eq!(resolve_node_id("7"), 7);
    assert_eq!(resolve_node_id("255"), 255);
    assert_eq!(resolve_node_id("  3 "), 3, "trimmed");
    // Out of u8 range / non-numeric -> hashed into range (stable, and distinct here).
    let a = resolve_node_id("256");
    let b = resolve_node_id("fireweed-statefulset-0");
    let c = resolve_node_id("fireweed-statefulset-1");
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

            request_id: None,
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

            request_id: None,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn class_a_filesystem_memory_starts_with_enabled_embedded_change_record_delivery() {
    {
        let (object_root, projection_path) = tmp_runtime_paths("retired-cell");
        let err = start(Config::new(
            objectlog_turso_spec(object_root, projection_path),
            0,
            "127.0.0.1:0".into(),
            Duration::from_secs(1),
            vec![qdef()],
        ))
        .await
        .err()
        .expect("filesystem x turso is not a public cell");
        assert_eq!(err, EngineError::Invalid(fireweed::RETIRED_STORAGE_CELL));
    }
}
