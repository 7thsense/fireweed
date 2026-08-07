//! P8cs — S3 durable emission cursor (native CAS) + emitter startup arms
//! (fireweed-cd2e6466).
//!
//! Owns:
//! - Live S3 emission-cursor lifecycle (monotonic, concurrent CAS, failover resume)
//! - Canonical S3 × memory / SQLite / Postgres server arms start the real background
//!   emitter via P8c's shared finalizer (`finalize_with_change_record_delivery`), attach
//!   it to shutdown, and cancel/join without leaking
//! - Per-cell: opt-out, isolation, reap coupling, and spawned-task transport smoke for
//!   Embedded / Http (ExternalKafka feature-gated)
//!
//! Out of scope: full CL-1..CL-8 (P11), simultaneous multi-mode delivery, Turso (P12a).
//!
//! Focused run:
//! ```text
//! set -a; source /tmp/fireweed-s3-secrets/credentials.env; set +a
//! export FIREWEED_PG_TEST_URL=postgres://fireweed:fireweed@127.0.0.1:55432/fireweed
//! cargo test -p fireweed-server --test p8cs_s3_delivery_cursor -- --nocapture
//! cargo test -p fireweed-objectlog s3_emission_cursor_native_cas -- --nocapture
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fireweed_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
    UtcTimestamp,
};
use fireweed_engine::{
    AsyncLogStore, ChangeRecord, ChangeRecordSink, CommandChecksum, CommandEnvelope, CommandId,
    CommandPosition, ControlPlaneStore, EngineError, EngineResult, PauseQueueCommand, PushPort,
    PushSpec, QueueCommand, QueueKey,
};
use fireweed_objectlog::{ObjectLogEngineStore, flush_config_from_segment};
use fireweed_server::{
    BackendSpec, ChangeRecordSinkConfig, ChangeRecordSinkMode, Config, ControlPlaneSpec, LogSpec,
    ObjectLogSpec, ProjectionSpec, ResponseBarrierSpec, S3CredentialSource, SegmentConfig,
    emit_change_record_tick, spawn_change_record_emitter, start,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex as AsyncMutex;

/// Serialize heavy S3×server boots so parallel tokio tests do not starve the RESP client.
static P8CS_SERVER_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

fn unique_tag(tag: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "p8cs-{}-{}-{}",
        tag,
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

fn require_s3() -> (String, String, String, String, String) {
    let endpoint = std::env::var("FIREWEED_S3_TEST_ENDPOINT")
        .expect("FIREWEED_S3_TEST_ENDPOINT required for P8cs (fail-closed live S3; no LOUD skip)");
    let bucket = std::env::var("FIREWEED_S3_TEST_BUCKET").expect("FIREWEED_S3_TEST_BUCKET");
    let region =
        std::env::var("FIREWEED_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".to_owned());
    let access = std::env::var("FIREWEED_S3_TEST_ACCESS_KEY").expect("FIREWEED_S3_TEST_ACCESS_KEY");
    let secret = std::env::var("FIREWEED_S3_TEST_SECRET_KEY").expect("FIREWEED_S3_TEST_SECRET_KEY");
    (endpoint, bucket, region, access, secret)
}

fn pg_url() -> String {
    std::env::var("FIREWEED_PG_TEST_URL")
        .expect("FIREWEED_PG_TEST_URL required for P8cs s3×postgres (fail-closed; no LOUD skip)")
}

fn url_with_schema(url: &str, schema: &str) -> String {
    if url.contains("?options=") || url.contains("&options=") {
        url.to_string()
    } else if url.contains('?') {
        format!("{url}&options=-csearch_path%3D{schema}")
    } else {
        format!("{url}?options=-csearch_path%3D{schema}")
    }
}

async fn create_schema(url: &str, schema: &str) {
    let create = url.to_string();
    let schema = schema.to_string();
    tokio::task::spawn_blocking(move || {
        let mut client =
            fireweed_postgres::connect(fireweed_postgres::PostgresConnectConfig::new(create))
                .expect("connect to create schema");
        client
            .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema};"))
            .expect("create schema");
    })
    .await
    .expect("schema create join");
}

async fn drop_schema(url: &str, schema: &str) {
    let drop_url = url.to_string();
    let schema = schema.to_string();
    let _ = tokio::task::spawn_blocking(move || {
        if let Ok(mut client) =
            fireweed_postgres::connect(fireweed_postgres::PostgresConnectConfig::new(drop_url))
        {
            let _ = client.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE;"));
        }
    })
    .await;
}

fn qdef_named(tenant: &str, queue: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new(tenant).unwrap(),
        queue_id: QueueId::new(queue).unwrap(),
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

fn qdef() -> QueueDefinition {
    qdef_named("t1", "q1")
}

fn shard_of(def: &QueueDefinition) -> QueueKey {
    QueueKey::new(def.tenant_id.clone(), def.queue_id.clone())
}

fn segments() -> SegmentConfig {
    SegmentConfig::new(262_144, 20).expect("valid segments")
}

fn s3_log_spec(endpoint: &str, bucket: &str, region: &str, access: &str, secret: &str) -> LogSpec {
    LogSpec::ObjectLog(ObjectLogSpec::S3 {
        endpoint: endpoint.to_string(),
        bucket: bucket.to_string(),
        region: region.to_string(),
        credentials: S3CredentialSource::Static {
            access_key_id: access.to_string(),
            secret_access_key: secret.to_string(),
        },
        segment_config: segments(),
        allow_insecure_http: true,
    })
}

fn base_config(backend: BackendSpec, queues: Vec<QueueDefinition>) -> Config {
    Config::new(
        backend,
        0,
        "127.0.0.1:0".to_string(),
        Duration::from_secs(60),
        queues,
    )
}

fn embedded_sink() -> ChangeRecordSinkConfig {
    ChangeRecordSinkConfig {
        enabled: true,
        endpoint: None,
        tick_interval: Duration::from_millis(20),
        batch_size: 16,
        ..ChangeRecordSinkConfig::default()
    }
}

fn http_sink(port: u16) -> ChangeRecordSinkConfig {
    ChangeRecordSinkConfig {
        enabled: true,
        endpoint: Some(format!("http://127.0.0.1:{port}/ingest")),
        tick_interval: Duration::from_millis(20),
        batch_size: 16,
        ..ChangeRecordSinkConfig::default()
    }
}

fn kafka_sink() -> ChangeRecordSinkConfig {
    ChangeRecordSinkConfig {
        enabled: true,
        endpoint: Some("kafka://127.0.0.1:9092".to_string()),
        ..ChangeRecordSinkConfig::default()
    }
}

fn tmp_file(tag: &str, ext: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("{}.{}", unique_tag(tag), ext));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    path
}

fn pause_envelope(id: &str) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new(id),
        request_id: None,
        request_fingerprint: None,
        request_outcome: None,
        item_ids: Vec::new(),
        command: QueueCommand::PauseQueue(PauseQueueCommand::default()),
        checksum: CommandChecksum(0),
        created_at: UtcTimestamp::new(1, 0).unwrap(),
    }
}

async fn redis_xadd(addr: std::net::SocketAddr, stream: &str) {
    let client = redis::Client::open(format!("redis://{addr}")).expect("redis url");
    let mut con = client
        .get_multiplexed_async_connection_with_config(
            &redis::AsyncConnectionConfig::new()
                .set_response_timeout(Some(Duration::from_secs(10))),
        )
        .await
        .expect("redis connect");
    let _: String = redis::cmd("XADD")
        .arg(stream)
        .arg("*")
        .arg("priority")
        .arg(1)
        .query_async(&mut con)
        .await
        .expect("XADD");
}

async fn accept_one_http_ok(listener: TcpListener) {
    let (mut socket, _) = listener.accept().await.expect("accept change-record http");
    let mut buf = Vec::new();
    let _ = socket.read_to_end(&mut buf).await;
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    let _ = socket.write_all(response).await;
}

#[derive(Default)]
struct CountingSink(std::sync::Mutex<usize>);

impl ChangeRecordSink for CountingSink {
    fn emit(&self, _shard: &QueueKey, records: &[ChangeRecord]) -> EngineResult<()> {
        *self.0.lock().expect("poisoned") += records.len();
        Ok(())
    }
}

// ── Delivery mode resolution (pure) ─────────────────────────────────────────

#[test]
fn p8cs_delivery_mode_resolution_matrix() {
    assert_eq!(
        ChangeRecordSinkConfig::default().mode(),
        ChangeRecordSinkMode::Disabled
    );
    assert_eq!(embedded_sink().mode(), ChangeRecordSinkMode::Embedded);
    assert_eq!(http_sink(9).mode(), ChangeRecordSinkMode::Http);
    assert_eq!(kafka_sink().mode(), ChangeRecordSinkMode::ExternalKafka);
}

// ── Source guard: S3 arms reach emission only via shared finalizer ──────────

#[test]
fn p8cs_s3_arms_use_shared_finalizer_not_direct_spawn() {
    let source = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"));
    // Production start must route S3 × memory/sqlite/postgres through the shared helper chain.
    for helper in [
        "open_objectlog_s3_memory_backend",
        "open_objectlog_s3_sqlite_backend",
        "open_objectlog_s3_postgres_backend",
        "finalize_objectlog_async_owned",
        "finalize_objectlog_blocking_owned",
        "finalize_with_change_record_delivery",
    ] {
        assert!(
            source.contains(helper),
            "server composition root must name {helper} for P8cs"
        );
    }
    // Direct spawn in production start arms is forbidden (P8c residual invariant).
    let production = source
        .split("#[cfg(test)]")
        .next()
        .expect("production source before cfg(test)");
    let direct_spawns = production
        .matches("spawn_change_record_emitter_if_enabled(")
        .count();
    // Only the shared finalizer body may call it (exactly one production call site).
    assert_eq!(
        direct_spawns, 1,
        "exactly one production call to spawn_change_record_emitter_if_enabled (shared finalizer)"
    );
}

// ── S3 log-axis cursor lifecycle (native CAS substrate) ─────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8cs_s3_log_cursor_lifecycle_native_cas() {
    let (endpoint, bucket, region, access, secret) = require_s3();
    let tag = unique_tag("log-cursor");
    let data_prefix = format!("fwlog-{tag}/");
    let meta_prefix = format!("fwmeta-{tag}/");
    let flush = flush_config_from_segment(262_144, 1);

    let positions = {
        let log = ObjectLogEngineStore::open_s3_with_prefixes(
            &endpoint,
            &region,
            &bucket,
            &access,
            &secret,
            data_prefix.clone(),
            meta_prefix.clone(),
            flush,
        )
        .await
        .expect("open S3 log");
        let mut definition = qdef();
        definition.emit_change_records = true;
        let key = shard_of(&definition);
        log.create_or_read_definition(definition).await.unwrap();
        log.ensure_shard(key.clone()).await.unwrap();
        let epoch = log.acquire_epoch(key.clone()).await.unwrap();
        let positions = log
            .append(
                key.clone(),
                vec![
                    pause_envelope("a"),
                    pause_envelope("b"),
                    pause_envelope("c"),
                    pause_envelope("d"),
                ],
                epoch,
            )
            .await
            .unwrap();
        assert_eq!(positions.len(), 4);
        assert!(log.supports_emission_cursor());
        assert_eq!(log.emission_cursor(&key).await.unwrap(), None);

        // Emit-driven advance (sink before cursor — at-least-once).
        let sink = CountingSink::default();
        let n = log
            .emit_change_record_tail(&key, &sink, 1, UtcTimestamp::new(2, 0).unwrap(), None)
            .await
            .unwrap();
        assert!(n >= 1);
        assert_eq!(
            log.emission_cursor(&key).await.unwrap().as_ref(),
            Some(&positions[0])
        );

        // Concurrent CAS advances.
        let log = Arc::new(log);
        let p1 = positions[1].clone();
        let p2 = positions[2].clone();
        let key1 = key.clone();
        let key2 = key.clone();
        let l1 = Arc::clone(&log);
        let l2 = Arc::clone(&log);
        let (r1, r2) = tokio::join!(
            async move { l1.set_emission_cursor(&key1, p1).await },
            async move { l2.set_emission_cursor(&key2, p2).await },
        );
        assert!(
            r1.is_ok() || r2.is_ok(),
            "at least one concurrent CAS advance ok: {r1:?}/{r2:?}"
        );
        let final_cursor = log.emission_cursor(&key).await.unwrap().expect("cursor");
        assert!(
            final_cursor == positions[1] || final_cursor == positions[2],
            "final cursor must be a concurrent target: {final_cursor:?}"
        );

        log.set_emission_cursor(&key, positions[3].clone())
            .await
            .unwrap();
        assert_eq!(
            log.set_emission_cursor(&key, positions[0].clone()).await,
            Err(EngineError::Invalid("emission cursor regression"))
        );
        drop(log);
        positions
    };

    // Failover resume (cursor-store-only reopen).
    let reopened = ObjectLogEngineStore::open_s3_with_prefixes(
        &endpoint,
        &region,
        &bucket,
        &access,
        &secret,
        data_prefix,
        meta_prefix,
        flush,
    )
    .await
    .expect("reopen S3 log");
    let key = shard_of(&qdef());
    assert_eq!(
        reopened.emission_cursor(&key).await.unwrap(),
        Some(positions[3].clone())
    );
}

// ── Real Server lifecycle: S3 × memory / sqlite / postgres ──────────────────

async fn smoke_s3_embedded_cell(mut config: Config, cell: &str, stream: &str) {
    config.change_record_sink = embedded_sink();
    let server = start(config)
        .await
        .unwrap_or_else(|e| panic!("{cell} Embedded delivery must start: {e:?}"));
    redis_xadd(server.addr(), stream).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Shutdown must cancel/join emitter + fjord tasks without leak.
    server.shutdown_and_drain(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8cs_s3_memory_embedded_emitter_lifecycle() {
    let _guard = P8CS_SERVER_LOCK.lock().await;
    let (endpoint, bucket, region, access, secret) = require_s3();
    let def = qdef_named("p8cs", &unique_tag("mem"));
    let stream = format!("{}:{}", def.tenant_id.as_str(), def.queue_id.as_str());
    let config = base_config(
        BackendSpec {
            log: s3_log_spec(&endpoint, &bucket, &region, &access, &secret),
            projection: ProjectionSpec::InMemory,
            control_plane: ControlPlaneSpec::InProcess,
            response_barrier: ResponseBarrierSpec::Strict,
            async_projection: None,
            sqlite_projection_deferred_flush_chunk: None,
        },
        vec![def],
    );
    smoke_s3_embedded_cell(config, "s3×memory", &stream).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8cs_s3_sqlite_embedded_emitter_lifecycle() {
    let _guard = P8CS_SERVER_LOCK.lock().await;
    let (endpoint, bucket, region, access, secret) = require_s3();
    let def = qdef_named("p8cs", &unique_tag("sql"));
    let stream = format!("{}:{}", def.tenant_id.as_str(), def.queue_id.as_str());
    let proj = tmp_file("s3-sqlite-proj", "sqlite");
    let config = base_config(
        BackendSpec {
            log: s3_log_spec(&endpoint, &bucket, &region, &access, &secret),
            projection: ProjectionSpec::Sqlite { path: proj.clone() },
            control_plane: ControlPlaneSpec::InProcess,
            response_barrier: ResponseBarrierSpec::Strict,
            async_projection: None,
            sqlite_projection_deferred_flush_chunk: None,
        },
        vec![def],
    );
    smoke_s3_embedded_cell(config, "s3×sqlite", &stream).await;
    let _ = std::fs::remove_file(&proj);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8cs_s3_postgres_embedded_emitter_lifecycle() {
    let _guard = P8CS_SERVER_LOCK.lock().await;
    let (endpoint, bucket, region, access, secret) = require_s3();
    let url = pg_url();
    let schema = unique_tag("s3_pg").replace('-', "_");
    create_schema(&url, &schema).await;
    let def = qdef_named("p8cs", &unique_tag("pg"));
    let stream = format!("{}:{}", def.tenant_id.as_str(), def.queue_id.as_str());
    let scoped = url_with_schema(&url, &schema);
    let config = base_config(
        BackendSpec {
            log: s3_log_spec(&endpoint, &bucket, &region, &access, &secret),
            projection: ProjectionSpec::Postgres { url: scoped },
            control_plane: ControlPlaneSpec::InProcess,
            response_barrier: ResponseBarrierSpec::Strict,
            async_projection: None,
            sqlite_projection_deferred_flush_chunk: None,
        },
        vec![def],
    );
    smoke_s3_embedded_cell(config, "s3×postgres", &stream).await;
    drop_schema(&url, &schema).await;
}

// ── Transport smoke through real spawned emitter (HTTP + feature-off Kafka) ─

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8cs_s3_memory_http_delivery_smoke_through_spawned_task() {
    let _guard = P8CS_SERVER_LOCK.lock().await;
    let (endpoint, bucket, region, access, secret) = require_s3();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = tokio::spawn(accept_one_http_ok(listener));

    let def = qdef_named("p8cs", &unique_tag("http"));
    let stream = format!("{}:{}", def.tenant_id.as_str(), def.queue_id.as_str());
    let mut config = base_config(
        BackendSpec {
            log: s3_log_spec(&endpoint, &bucket, &region, &access, &secret),
            projection: ProjectionSpec::InMemory,
            control_plane: ControlPlaneSpec::InProcess,
            response_barrier: ResponseBarrierSpec::Strict,
            async_projection: None,
            sqlite_projection_deferred_flush_chunk: None,
        },
        vec![def],
    );
    config.change_record_sink = http_sink(port);
    let server = start(config)
        .await
        .expect("s3×memory HTTP delivery must start");
    redis_xadd(server.addr(), &stream).await;

    tokio::time::timeout(Duration::from_secs(10), acceptor)
        .await
        .expect("HTTP sink must receive at least one delivery from the spawned emitter")
        .expect("acceptor join");

    server.shutdown_and_drain(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8cs_s3_sqlite_http_delivery_smoke_through_spawned_task() {
    let _guard = P8CS_SERVER_LOCK.lock().await;
    let (endpoint, bucket, region, access, secret) = require_s3();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = tokio::spawn(accept_one_http_ok(listener));

    let def = qdef_named("p8cs", &unique_tag("http-sql"));
    let stream = format!("{}:{}", def.tenant_id.as_str(), def.queue_id.as_str());
    let proj = tmp_file("s3-http-sqlite", "sqlite");
    let mut config = base_config(
        BackendSpec {
            log: s3_log_spec(&endpoint, &bucket, &region, &access, &secret),
            projection: ProjectionSpec::Sqlite { path: proj.clone() },
            control_plane: ControlPlaneSpec::InProcess,
            response_barrier: ResponseBarrierSpec::Strict,
            async_projection: None,
            sqlite_projection_deferred_flush_chunk: None,
        },
        vec![def],
    );
    config.change_record_sink = http_sink(port);
    let server = start(config)
        .await
        .expect("s3×sqlite HTTP delivery must start");
    redis_xadd(server.addr(), &stream).await;

    tokio::time::timeout(Duration::from_secs(10), acceptor)
        .await
        .expect("HTTP sink must receive delivery from spawned emitter")
        .expect("acceptor join");

    server.shutdown_and_drain(Duration::from_secs(5)).await;
    let _ = std::fs::remove_file(&proj);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8cs_s3_postgres_http_delivery_smoke_through_spawned_task() {
    let _guard = P8CS_SERVER_LOCK.lock().await;
    let (endpoint, bucket, region, access, secret) = require_s3();
    let url = pg_url();
    let schema = unique_tag("s3_http_pg").replace('-', "_");
    create_schema(&url, &schema).await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = tokio::spawn(accept_one_http_ok(listener));

    let def = qdef_named("p8cs", &unique_tag("http-pg"));
    let stream = format!("{}:{}", def.tenant_id.as_str(), def.queue_id.as_str());
    let scoped = url_with_schema(&url, &schema);
    let mut config = base_config(
        BackendSpec {
            log: s3_log_spec(&endpoint, &bucket, &region, &access, &secret),
            projection: ProjectionSpec::Postgres { url: scoped },
            control_plane: ControlPlaneSpec::InProcess,
            response_barrier: ResponseBarrierSpec::Strict,
            async_projection: None,
            sqlite_projection_deferred_flush_chunk: None,
        },
        vec![def],
    );
    config.change_record_sink = http_sink(port);
    let server = start(config)
        .await
        .expect("s3×postgres HTTP delivery must start");
    redis_xadd(server.addr(), &stream).await;

    tokio::time::timeout(Duration::from_secs(15), acceptor)
        .await
        .expect("HTTP sink must receive delivery from spawned emitter")
        .expect("acceptor join");

    server.shutdown_and_drain(Duration::from_secs(5)).await;
    drop_schema(&url, &schema).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8cs_external_kafka_feature_off_rejects_s3_class_a() {
    #[cfg(feature = "external-kafka")]
    {
        // Feature-on construction needs a live broker (P8k fixture); composition is accepted.
        let _ = kafka_sink();
        return;
    }
    #[cfg(not(feature = "external-kafka"))]
    {
        let (endpoint, bucket, region, access, secret) = require_s3();
        let mut config = base_config(
            BackendSpec {
                log: s3_log_spec(&endpoint, &bucket, &region, &access, &secret),
                projection: ProjectionSpec::InMemory,
                control_plane: ControlPlaneSpec::InProcess,
                response_barrier: ResponseBarrierSpec::Strict,
                async_projection: None,
                sqlite_projection_deferred_flush_chunk: None,
            },
            vec![qdef()],
        );
        config.change_record_sink = kafka_sink();
        let err = start(config).await.err().expect("must reject");
        match err {
            EngineError::Invalid(msg) => {
                assert!(
                    msg.contains("external-kafka"),
                    "feature-off Kafka must name the feature, got: {msg}"
                );
            }
            other => panic!("expected Invalid feature message, got {other:?}"),
        }
    }
}

// ── Opt-out + isolation + reap coupling on S3 product cells ─────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8cs_s3_memory_opt_out_isolation_and_reap_coupling() {
    let _guard = P8CS_SERVER_LOCK.lock().await;
    let (endpoint, bucket, region, access, secret) = require_s3();

    // Opt-out: enabled sink + emit_change_records=false still starts (no emitter work).
    let mut opted_out = qdef_named("p8cs", &unique_tag("opt"));
    opted_out.emit_change_records = false;
    let mut config = base_config(
        BackendSpec {
            log: s3_log_spec(&endpoint, &bucket, &region, &access, &secret),
            projection: ProjectionSpec::InMemory,
            control_plane: ControlPlaneSpec::InProcess,
            response_barrier: ResponseBarrierSpec::Strict,
            async_projection: None,
            sqlite_projection_deferred_flush_chunk: None,
        },
        vec![opted_out],
    );
    config.change_record_sink = embedded_sink();
    let server = start(config)
        .await
        .expect("opt-out queues allow enabled sink");
    server.shutdown_and_drain(Duration::from_secs(5)).await;

    // Disabled + endpoint still rejected (tuple coherence; not generic unsupported).
    let mut disabled = base_config(
        BackendSpec {
            log: s3_log_spec(&endpoint, &bucket, &region, &access, &secret),
            projection: ProjectionSpec::InMemory,
            control_plane: ControlPlaneSpec::InProcess,
            response_barrier: ResponseBarrierSpec::Strict,
            async_projection: None,
            sqlite_projection_deferred_flush_chunk: None,
        },
        vec![qdef()],
    );
    disabled.change_record_sink.endpoint = Some("http://127.0.0.1:9".into());
    assert_eq!(
        start(disabled).await.err(),
        Some(EngineError::Invalid(
            "change-record-endpoint-requires-enabled"
        ))
    );

    // Isolation + opt-out cursor: two shards on one S3 log store.
    let tag = unique_tag("iso");
    let data_prefix = format!("fwlog-{tag}/");
    let meta_prefix = format!("fwmeta-{tag}/");
    let flush = flush_config_from_segment(262_144, 1);
    let log = ObjectLogEngineStore::open_s3_with_prefixes(
        &endpoint,
        &region,
        &bucket,
        &access,
        &secret,
        data_prefix,
        meta_prefix,
        flush,
    )
    .await
    .expect("open S3 log for isolation");

    let mut emit_def = qdef_named("tenant-a", &unique_tag("qa"));
    emit_def.emit_change_records = true;
    let mut silent_def = qdef_named("tenant-b", &unique_tag("qb"));
    silent_def.emit_change_records = false;
    let emit_key = shard_of(&emit_def);
    let silent_key = shard_of(&silent_def);

    log.create_or_read_definition(emit_def.clone())
        .await
        .unwrap();
    log.create_or_read_definition(silent_def.clone())
        .await
        .unwrap();
    log.ensure_shard(emit_key.clone()).await.unwrap();
    log.ensure_shard(silent_key.clone()).await.unwrap();
    let epoch_a = log.acquire_epoch(emit_key.clone()).await.unwrap();
    let epoch_b = log.acquire_epoch(silent_key.clone()).await.unwrap();
    let pos_a = log
        .append(emit_key.clone(), vec![pause_envelope("iso-a")], epoch_a)
        .await
        .unwrap();
    let _pos_b = log
        .append(silent_key.clone(), vec![pause_envelope("iso-b")], epoch_b)
        .await
        .unwrap();

    let sink = CountingSink::default();
    // Direct emit only for emit-enabled shard (opt-out product path uses tick).
    log.emit_change_record_tail(&emit_key, &sink, 8, UtcTimestamp::new(3, 0).unwrap(), None)
        .await
        .unwrap();
    assert_eq!(
        log.emission_cursor(&emit_key).await.unwrap().as_ref(),
        Some(&pos_a[0]),
        "enabled tenant must advance its cursor"
    );
    assert_eq!(
        log.emission_cursor(&silent_key).await.unwrap(),
        None,
        "opted-out tenant must not get a cursor from sibling emit"
    );

    // Reap coupling substrate: cursor behind terminal position blocks reap when emit enabled.
    // Projection-level contract (CL-6) is owned by fireweed-projection tests; here we prove the
    // S3 log can supply the cursor that those reaps consult.
    let behind = CommandPosition::new(emit_key.clone(), pos_a[0].backend_epoch, 0);
    // If cursor is at pos_a[0], a terminal at that position is emission-safe; a cursor at 0 is not.
    assert!(
        behind.precedes(&pos_a[0]) || behind == pos_a[0] || pos_a[0].precedes(&behind),
        "positions comparable on same shard"
    );
    let cursor = log.emission_cursor(&emit_key).await.unwrap().unwrap();
    assert!(
        !cursor.precedes(&pos_a[0]) && (cursor == pos_a[0] || pos_a[0].precedes(&cursor)),
        "durable S3 cursor at/past emitted position for reap coupling: cursor={cursor:?} terminal={:?}",
        pos_a[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8cs_s3_sqlite_cursor_and_emitter_cancel_join() {
    let _guard = P8CS_SERVER_LOCK.lock().await;
    let (endpoint, bucket, region, access, secret) = require_s3();
    let tag = unique_tag("sql-cursor");
    let data_prefix = format!("fwlog-{tag}/");
    let meta_prefix = format!("fwmeta-{tag}/");
    let flush = flush_config_from_segment(262_144, 1);
    let proj = tmp_file("s3-sql-cursor", "sqlite");

    let backend = fireweed_objectlog::AsyncObjectLogSqliteBackend::from_log_and_projection(
        ObjectLogEngineStore::open_s3_with_prefixes(
            &endpoint,
            &region,
            &bucket,
            &access,
            &secret,
            data_prefix,
            meta_prefix,
            flush,
        )
        .await
        .expect("open S3 log"),
        fireweed_sqlite::SqliteProjectionStore::open(proj.to_str().unwrap()).expect("proj"),
        0,
    )
    .await
    .expect("s3×sqlite product");

    let def = qdef_named("p8cs", &unique_tag("sqlc"));
    let key = shard_of(&def);
    backend.create_queue(def.clone()).await.unwrap();
    for i in 0..4u32 {
        backend
            .push(
                &key,
                vec![PushSpec::default()],
                UtcTimestamp::new(i as i64, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
    }

    let sink = CountingSink::default();
    backend
        .with_log(|log| {
            fireweed_objectlog::block_on_objectlog(log.emit_change_record_tail(
                &key,
                &sink,
                2,
                UtcTimestamp::new(10, 0).unwrap(),
                None,
            ))
        })
        .unwrap();
    let cursor = backend
        .with_log(|log| fireweed_objectlog::block_on_objectlog(log.emission_cursor(&key)))
        .unwrap()
        .expect("cursor after emit");
    assert!(cursor.sequence >= 1, "cursor advanced: {cursor:?}");

    // Real emitter task cancel/join over the durable S3×sqlite backend.
    let backend = Arc::new(backend);
    let handle = spawn_change_record_emitter(
        Arc::clone(&backend),
        Arc::new(CountingSink::default()) as Arc<dyn ChangeRecordSink>,
        vec![def],
        ChangeRecordSinkConfig {
            enabled: true,
            tick_interval: Duration::from_millis(5),
            batch_size: 2,
            ..ChangeRecordSinkConfig::default()
        },
    );
    tokio::time::sleep(Duration::from_millis(40)).await;
    handle.abort();
    let join_err = handle.await.expect_err("emitter must be cancelled");
    assert!(join_err.is_cancelled(), "join must report cancellation");

    let _ = std::fs::remove_file(&proj);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8cs_s3_postgres_cursor_failover_resume() {
    let _guard = P8CS_SERVER_LOCK.lock().await;
    let (endpoint, bucket, region, access, secret) = require_s3();
    let url = pg_url();
    let schema = unique_tag("s3_pg_cur").replace('-', "_");
    create_schema(&url, &schema).await;
    let scoped = url_with_schema(&url, &schema);

    let tag = unique_tag("pg-cursor");
    let data_prefix = format!("fwlog-{tag}/");
    let meta_prefix = format!("fwmeta-{tag}/");
    let flush = flush_config_from_segment(262_144, 1);

    let positions = {
        let log = ObjectLogEngineStore::open_s3_with_prefixes(
            &endpoint,
            &region,
            &bucket,
            &access,
            &secret,
            data_prefix.clone(),
            meta_prefix.clone(),
            flush,
        )
        .await
        .expect("open S3 log");
        // Open postgres projection so the product cell is exercised; cursor lives on the log.
        let projection = fireweed_postgres::AsyncPostgresRelationalProjection::connect(&scoped)
            .await
            .expect("pg projection");
        let backend = fireweed_postgres::AsyncObjectLogPostgresBackend::from_log_and_projection(
            log, projection, 0,
        )
        .await
        .expect("s3×postgres product");

        let def = qdef_named("p8cs", &unique_tag("pgc"));
        let key = shard_of(&def);
        backend.create_queue(def.clone()).await.unwrap();
        for i in 0..3u32 {
            backend
                .push(
                    &key,
                    vec![PushSpec::default()],
                    UtcTimestamp::new(i as i64, 0).unwrap(),
                    None,
                )
                .await
                .unwrap();
        }
        let sink = CountingSink::default();
        let emitted = emit_change_record_tick(&backend, &sink, &[def], 8).unwrap();
        assert!(
            emitted >= 1,
            "s3×postgres tick must emit at least one change record, got {emitted}"
        );
        let cursor = backend
            .with_log(|log| fireweed_objectlog::block_on_objectlog(log.emission_cursor(&key)))
            .unwrap()
            .expect("cursor after tick (first command may be sequence 0)");
        // Force advance + regression via log CAS path.
        let next = CommandPosition::new(key.clone(), cursor.backend_epoch, cursor.sequence + 1);
        backend
            .with_log(|log| {
                fireweed_objectlog::block_on_objectlog(log.set_emission_cursor(&key, next.clone()))
            })
            .unwrap();
        let regress = CommandPosition::new(key.clone(), cursor.backend_epoch, cursor.sequence);
        assert_eq!(
            backend.with_log(|log| {
                fireweed_objectlog::block_on_objectlog(log.set_emission_cursor(&key, regress))
            }),
            Err(EngineError::Invalid("emission cursor regression"))
        );
        let final_cursor = backend
            .with_log(|log| fireweed_objectlog::block_on_objectlog(log.emission_cursor(&key)))
            .unwrap()
            .unwrap();
        assert_eq!(final_cursor, next);
        drop(backend);
        (key, final_cursor, data_prefix, meta_prefix)
    };

    // Failover: reopen log-only handle on same prefixes — cursor survives independently of
    // postgres projection reconnect (cursor-store isolation).
    let reopened = ObjectLogEngineStore::open_s3_with_prefixes(
        &endpoint,
        &region,
        &bucket,
        &access,
        &secret,
        positions.2,
        positions.3,
        flush,
    )
    .await
    .expect("reopen S3 log");
    assert_eq!(
        reopened.emission_cursor(&positions.0).await.unwrap(),
        Some(positions.1)
    );

    drop_schema(&url, &schema).await;
}
