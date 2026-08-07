//! P14 change-record mode nonblocking proofs [ddx-61324c64].
use fireweed_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use fireweed_engine::{ControlPlaneStore, EngineError};
use fireweed_memory::composed_memory_backend;
use fireweed_server::{
    BackendSpec, ChangeRecordSinkConfig, ChangeRecordSinkMode, Config, ControlPlaneSpec, LogSpec,
    ObjectLogSpec, PostgresWholeOperationAdapter, ProjectionSpec, ResponseBarrierSpec,
    SegmentConfig, start,
};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
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
fn unique_tag(tag: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "p14-{}-{}-{}",
        tag,
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}
fn tmp_root(tag: &str) -> PathBuf {
    let r = std::env::temp_dir().join(unique_tag(tag));
    let _ = std::fs::remove_dir_all(&r);
    let _ = std::fs::create_dir_all(&r);
    r
}
fn segments() -> SegmentConfig {
    SegmentConfig::new(262_144, 20).unwrap()
}
fn base_config(backend: BackendSpec) -> Config {
    Config::new(
        backend,
        0,
        "127.0.0.1:0".into(),
        Duration::from_secs(60),
        vec![qdef()],
    )
}
fn fs_backend(root: PathBuf) -> BackendSpec {
    BackendSpec {
        log: LogSpec::ObjectLog(ObjectLogSpec::local(root, segments())),
        projection: ProjectionSpec::InMemory,
        control_plane: ControlPlaneSpec::InProcess,
        response_barrier: ResponseBarrierSpec::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
    }
}
fn embedded_sink() -> ChangeRecordSinkConfig {
    ChangeRecordSinkConfig {
        enabled: true,
        endpoint: None,
        tick_interval: Duration::from_millis(20),
        batch_size: 16,
        ..Default::default()
    }
}
fn http_sink(port: u16) -> ChangeRecordSinkConfig {
    ChangeRecordSinkConfig {
        enabled: true,
        endpoint: Some(format!("http://127.0.0.1:{port}/ingest")),
        tick_interval: Duration::from_millis(20),
        batch_size: 16,
        ..Default::default()
    }
}
fn kafka_sink() -> ChangeRecordSinkConfig {
    ChangeRecordSinkConfig {
        enabled: true,
        endpoint: Some("kafka://127.0.0.1:9092".into()),
        tick_interval: Duration::from_millis(20),
        batch_size: 16,
        ..Default::default()
    }
}
#[cfg(not(feature = "external-kafka"))]
const EXTERNAL_KAFKA_FEATURE_REQUIRED: &str = "external-kafka change record sink requires the `external-kafka` cargo feature (pure-Rust rskafka); \
     the default in-process embedded surface needs no endpoint";
async fn redis_xadd(addr: SocketAddr) {
    let client = redis::Client::open(format!("redis://{addr}")).unwrap();
    let mut con = client
        .get_multiplexed_async_connection_with_config(
            &redis::AsyncConnectionConfig::new()
                .set_response_timeout(Some(Duration::from_secs(10))),
        )
        .await
        .unwrap();
    let _: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(1)
        .query_async(&mut con)
        .await
        .unwrap();
}
async fn accept_one_http_ok(listener: TcpListener) {
    let (mut s, _) = listener.accept().await.unwrap();
    let mut buf = Vec::new();
    let _ = s.read_to_end(&mut buf).await;
    let _ = s
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        .await;
}
async fn with_heartbeat<F, Fut>(work: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let finished = Arc::new(AtomicBool::new(false));
    let ticks = Arc::new(AtomicU64::new(0));
    let hb_done = Arc::clone(&finished);
    let hb_ticks = Arc::clone(&ticks);
    let heartbeat = tokio::spawn(async move {
        let mut i = tokio::time::interval(Duration::from_millis(1));
        while !hb_done.load(Ordering::Acquire) {
            i.tick().await;
            hb_ticks.fetch_add(1, Ordering::Relaxed);
        }
    });
    while ticks.load(Ordering::Acquire) == 0 {
        tokio::task::yield_now().await;
    }
    let before = ticks.load(Ordering::Relaxed);
    work().await;
    finished.store(true, Ordering::Release);
    heartbeat.await.unwrap();
    assert!(ticks.load(Ordering::Relaxed) > before);
}
#[test]
fn p14_delivery_modes_resolve_independently() {
    assert_eq!(
        ChangeRecordSinkConfig::default().mode(),
        ChangeRecordSinkMode::Disabled
    );
    assert_eq!(embedded_sink().mode(), ChangeRecordSinkMode::Embedded);
    assert_eq!(http_sink(9).mode(), ChangeRecordSinkMode::Http);
    assert_eq!(kafka_sink().mode(), ChangeRecordSinkMode::ExternalKafka);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p14_embedded_change_record_emission_keeps_heartbeat_live() {
    let root = tmp_root("embedded");
    with_heartbeat(|| async {
        let mut c = base_config(fs_backend(root.clone()));
        c.change_record_sink = embedded_sink();
        let s = start(c).await.unwrap();
        redis_xadd(s.addr()).await;
        tokio::time::sleep(Duration::from_millis(80)).await;
        s.shutdown_and_drain(Duration::from_secs(5)).await;
    })
    .await;
    let _ = std::fs::remove_dir_all(&root);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p14_http_change_record_emission_keeps_heartbeat_live() {
    let root = tmp_root("http");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = tokio::spawn(accept_one_http_ok(listener));
    with_heartbeat(|| async {
        let mut c = base_config(fs_backend(root.clone()));
        c.change_record_sink = http_sink(port);
        let s = start(c).await.unwrap();
        redis_xadd(s.addr()).await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        s.shutdown_and_drain(Duration::from_secs(5)).await;
    })
    .await;
    let _ = tokio::time::timeout(Duration::from_secs(2), acceptor).await;
    let _ = std::fs::remove_dir_all(&root);
}
#[cfg(not(feature = "external-kafka"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p14_external_kafka_feature_off_rejects_class_a_enabled_sink() {
    let root = tmp_root("kafka-off");
    let mut c = base_config(fs_backend(root.clone()));
    c.change_record_sink = kafka_sink();
    assert_eq!(
        start(c).await.err(),
        Some(EngineError::Invalid(EXTERNAL_KAFKA_FEATURE_REQUIRED))
    );
    let _ = std::fs::remove_dir_all(&root);
}
#[cfg(feature = "external-kafka")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p14_external_kafka_feature_on_composition_accepts_class_a() {
    let root = tmp_root("kafka-on");
    let mut c = base_config(fs_backend(root.clone()));
    c.change_record_sink = kafka_sink();
    match start(c).await {
        Ok(s) => {
            with_heartbeat(|| async {
                redis_xadd(s.addr()).await;
                tokio::time::sleep(Duration::from_millis(80)).await;
                s.shutdown_and_drain(Duration::from_secs(5)).await;
            })
            .await;
        }
        Err(e) => {
            let m = format!("{e:?}");
            assert!(!m.contains("external-kafka change record sink requires the `external-kafka`"));
            eprintln!("P14_EXTERNAL_KAFKA_START: {m}");
        }
    }
    let _ = std::fs::remove_dir_all(&root);
}
#[tokio::test(flavor = "current_thread")]
async fn p14_bounded_runtime_bridge_returns_backpressure_when_saturated() {
    let (_, _, pt) = fireweed_resp::runtime_task_resource_counts();
    let (_, _, pc) = fireweed_resp::connection_resource_counts();
    fireweed_resp::set_max_live_connections(usize::MAX);
    fireweed_resp::set_max_runtime_tasks(1);
    let (release, wait) = oneshot::channel::<()>();
    let holder = fireweed_resp::spawn_governed(async move {
        let _ = wait.await;
    });
    let adapter = PostgresWholeOperationAdapter::new(composed_memory_backend());
    let err = adapter
        .list_queues(&TenantId::new("tenant").unwrap())
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::Backpressure {
            resource: "runtime task slots"
        }
    ));
    release.send(()).unwrap();
    holder.await.unwrap();
    fireweed_resp::set_max_runtime_tasks(pt);
    fireweed_resp::set_max_live_connections(pc);
}
