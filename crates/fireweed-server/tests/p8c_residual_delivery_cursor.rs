//! P8c residual fixtures (fireweed-c0019c7e).
//!
//! Owns delivery-mode smokes and per-cell emission-cursor lifecycle fixtures left after the core
//! P8c land (fireweed-610f2245). Out of scope here:
//! - Turso public projection selectors → P12a
//! - S3 Class A arms → P8cs
//! - Barrier-relative claims → P3v (already on main)
//!
//! Focused run:
//!   cargo test -p fireweed-server --test p8c_residual_delivery_cursor -- --nocapture
//! With Postgres cells:
//!   FIREWEED_PG_TEST_URL=postgres://fireweed:fireweed@127.0.0.1:55432/fireweed \
//!     cargo test -p fireweed-server --test p8c_residual_delivery_cursor -- --nocapture

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
    CommandPosition, ControlPlaneStore, EngineError, EngineResult, LogStore, PauseQueueCommand,
    PushPort, PushSpec, QueueCommand, QueueKey,
};
use fireweed_objectlog::{ObjectLogEngineStore, flush_config_from_segment};
use fireweed_server::{
    BackendSpec, ChangeRecordSinkConfig, ChangeRecordSinkMode, Config, ControlPlaneSpec, LogSpec,
    ObjectLogSpec, ProjectionSpec, ResponseBarrierSpec, SegmentConfig, emit_change_record_tick,
    spawn_change_record_emitter, start,
};
use fireweed_sqlite::{SqliteLog, composed_sqlite_backend};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex as AsyncMutex;

// Serialize heavy residual server boots (object-log + multi-cell matrix) so parallel tokio tests
// do not starve redis clients under the default multiplexed response deadline.
static RESIDUAL_SERVER_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

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

fn shard() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}

fn unique_tag(tag: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "p8c-residual-{}-{}-{}",
        tag,
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

fn tmp_root(tag: &str) -> PathBuf {
    let root = std::env::temp_dir().join(unique_tag(tag));
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::create_dir_all(&root);
    root
}

fn tmp_file(tag: &str, ext: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("{}.{}", unique_tag(tag), ext));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    path
}

fn segments() -> SegmentConfig {
    SegmentConfig::new(262_144, 20).expect("valid segments")
}

fn base_config(backend: BackendSpec) -> Config {
    Config::new(
        backend,
        0,
        "127.0.0.1:0".to_string(),
        Duration::from_secs(60),
        vec![qdef()],
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

const EXTERNAL_KAFKA_FEATURE_REQUIRED: &str = "external-kafka change record sink requires the `external-kafka` cargo feature (pure-Rust rskafka); \
     the default in-process embedded surface needs no endpoint";

fn pg_url() -> Option<String> {
    std::env::var("FIREWEED_PG_TEST_URL").ok()
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

async fn redis_xadd(addr: std::net::SocketAddr) {
    let client = redis::Client::open(format!("redis://{addr}")).expect("redis url");
    let mut con = client
        .get_multiplexed_async_connection_with_config(
            &redis::AsyncConnectionConfig::new()
                .set_response_timeout(Some(Duration::from_secs(10))),
        )
        .await
        .expect("redis connect");
    let _: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(1)
        .query_async(&mut con)
        .await
        .expect("XADD");
}

async fn smoke_embedded_cell(mut config: Config, cell: &str) {
    config.change_record_sink = embedded_sink();
    let server = start(config)
        .await
        .unwrap_or_else(|e| panic!("{cell} Embedded delivery must start: {e:?}"));
    redis_xadd(server.addr()).await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    server.shutdown_and_drain(Duration::from_secs(5)).await;
}

async fn accept_one_http_ok(listener: TcpListener) {
    let (mut socket, _) = listener.accept().await.expect("accept change-record http");
    let mut buf = Vec::new();
    let _ = socket.read_to_end(&mut buf).await;
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    let _ = socket.write_all(response).await;
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

// ── Delivery mode resolution (pure) ─────────────────────────────────────────

#[test]
fn p8c_residual_delivery_mode_resolution_matrix() {
    let disabled = ChangeRecordSinkConfig::default();
    assert_eq!(disabled.mode(), ChangeRecordSinkMode::Disabled);

    let embedded = embedded_sink();
    assert_eq!(embedded.mode(), ChangeRecordSinkMode::Embedded);

    let http = http_sink(9);
    assert_eq!(http.mode(), ChangeRecordSinkMode::Http);

    let kafka = kafka_sink();
    assert_eq!(kafka.mode(), ChangeRecordSinkMode::ExternalKafka);
}

// ── Delivery-mode composition smokes via start() ────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8c_residual_class_b_delivery_mode_negatives_and_disabled() {
    // Disabled Class B always starts.
    let mut disabled = base_config(BackendSpec {
        log: LogSpec::Memory,
        projection: ProjectionSpec::InMemory,
        control_plane: ControlPlaneSpec::InProcess,
        response_barrier: ResponseBarrierSpec::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
    });
    disabled.change_record_sink = ChangeRecordSinkConfig::default();
    let server = start(disabled)
        .await
        .expect("Class B Strict+Disabled delivery must start");
    server.shutdown_and_drain(Duration::from_secs(5)).await;

    // Enabled Embedded on Class B → durability rejection.
    let mut embedded = base_config(BackendSpec {
        log: LogSpec::Memory,
        projection: ProjectionSpec::InMemory,
        control_plane: ControlPlaneSpec::InProcess,
        response_barrier: ResponseBarrierSpec::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
    });
    embedded.change_record_sink = embedded_sink();
    assert_eq!(
        start(embedded).await.err(),
        Some(EngineError::ChangeRecordsRequireDurableLog)
    );

    // Enabled HTTP on Class B → same durability rejection (feature-off Kafka would win first).
    let mut http = base_config(BackendSpec {
        log: LogSpec::Memory,
        projection: ProjectionSpec::InMemory,
        control_plane: ControlPlaneSpec::InProcess,
        response_barrier: ResponseBarrierSpec::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
    });
    http.change_record_sink = http_sink(8080);
    assert_eq!(
        start(http).await.err(),
        Some(EngineError::ChangeRecordsRequireDurableLog)
    );

    // Disabled + present endpoint → tuple coherence before durability/feature.
    let mut tuple = base_config(BackendSpec {
        log: LogSpec::Memory,
        projection: ProjectionSpec::InMemory,
        control_plane: ControlPlaneSpec::InProcess,
        response_barrier: ResponseBarrierSpec::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
    });
    tuple.change_record_sink.endpoint = Some("http://127.0.0.1:9".into());
    assert_eq!(
        start(tuple).await.err(),
        Some(EngineError::Invalid(
            "change-record-endpoint-requires-enabled"
        ))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8c_residual_external_kafka_feature_off_rejects_class_a_and_class_b() {
    #[cfg(feature = "external-kafka")]
    {
        // Feature-on builds still accept composition; construction needs a live broker and is owned
        // by the hermetic ExternalKafka fixture (P8k), not this residual bead.
        return;
    }
    #[cfg(not(feature = "external-kafka"))]
    {
        let class_b = {
            let mut c = base_config(BackendSpec {
                log: LogSpec::Memory,
                projection: ProjectionSpec::InMemory,
                control_plane: ControlPlaneSpec::InProcess,
                response_barrier: ResponseBarrierSpec::Strict,
                async_projection: None,
                sqlite_projection_deferred_flush_chunk: None,
            });
            c.change_record_sink = kafka_sink();
            c
        };
        assert_eq!(
            start(class_b).await.err(),
            Some(EngineError::Invalid(EXTERNAL_KAFKA_FEATURE_REQUIRED)),
            "Class B + kafka + feature-off must name the feature, not durable-log"
        );

        let log_path = tmp_file("kafka-class-a", "sqlite");
        let class_a = {
            let mut c = base_config(BackendSpec {
                log: LogSpec::Sqlite {
                    path: log_path.clone(),
                },
                projection: ProjectionSpec::InMemory,
                control_plane: ControlPlaneSpec::InProcess,
                response_barrier: ResponseBarrierSpec::Strict,
                async_projection: None,
                sqlite_projection_deferred_flush_chunk: None,
            });
            c.change_record_sink = kafka_sink();
            c
        };
        assert_eq!(
            start(class_a).await.err(),
            Some(EngineError::Invalid(EXTERNAL_KAFKA_FEATURE_REQUIRED))
        );
        let _ = std::fs::remove_file(&log_path);
    }
}

/// Non-S3 Class A cells without Postgres axes (Turso → P12a, S3 → P8cs): Embedded delivery smokes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8c_residual_class_a_non_pg_embedded_delivery_smokes() {
    let _guard = RESIDUAL_SERVER_LOCK.lock().await;

    // sqlite × memory
    {
        let log_path = tmp_file("sqlite-mem", "sqlite");
        let config = base_config(BackendSpec {
            log: LogSpec::Sqlite {
                path: log_path.clone(),
            },
            projection: ProjectionSpec::InMemory,
            control_plane: ControlPlaneSpec::InProcess,
            response_barrier: ResponseBarrierSpec::Strict,
            async_projection: None,
            sqlite_projection_deferred_flush_chunk: None,
        });
        smoke_embedded_cell(config, "sqlite×memory").await;
        let _ = std::fs::remove_file(&log_path);
    }

    // sqlite × sqlite
    {
        let log_path = tmp_file("sqlite-sqlite-log", "sqlite");
        let proj_path = tmp_file("sqlite-sqlite-proj", "sqlite");
        let config = base_config(BackendSpec {
            log: LogSpec::Sqlite {
                path: log_path.clone(),
            },
            projection: ProjectionSpec::Sqlite {
                path: proj_path.clone(),
            },
            control_plane: ControlPlaneSpec::InProcess,
            response_barrier: ResponseBarrierSpec::Strict,
            async_projection: None,
            sqlite_projection_deferred_flush_chunk: None,
        });
        smoke_embedded_cell(config, "sqlite×sqlite").await;
        let _ = std::fs::remove_file(&log_path);
        let _ = std::fs::remove_file(&proj_path);
    }

    // filesystem × memory / sqlite
    {
        let root = tmp_root("fs-mem");
        let config = base_config(BackendSpec {
            log: LogSpec::ObjectLog(ObjectLogSpec::local(root.clone(), segments())),
            projection: ProjectionSpec::InMemory,
            control_plane: ControlPlaneSpec::InProcess,
            response_barrier: ResponseBarrierSpec::Strict,
            async_projection: None,
            sqlite_projection_deferred_flush_chunk: None,
        });
        smoke_embedded_cell(config, "filesystem×memory").await;
        let _ = std::fs::remove_dir_all(&root);
    }
    {
        let root = tmp_root("fs-sqlite");
        let proj = tmp_file("fs-sqlite-proj", "sqlite");
        let config = base_config(BackendSpec {
            log: LogSpec::ObjectLog(ObjectLogSpec::local(root.clone(), segments())),
            projection: ProjectionSpec::Sqlite { path: proj.clone() },
            control_plane: ControlPlaneSpec::InProcess,
            response_barrier: ResponseBarrierSpec::Strict,
            async_projection: None,
            sqlite_projection_deferred_flush_chunk: None,
        });
        smoke_embedded_cell(config, "filesystem×sqlite").await;
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_file(&proj);
    }
}

/// Postgres-axis Class A cells (env-gated): Embedded delivery smokes through Server lifecycle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8c_residual_class_a_postgres_axis_embedded_delivery_smokes() {
    let Some(url) = pg_url() else {
        eprintln!(
            "P8C RESIDUAL PG CELLS SKIPPED (embedded delivery smokes) — set FIREWEED_PG_TEST_URL"
        );
        return;
    };
    let _guard = RESIDUAL_SERVER_LOCK.lock().await;

    // sqlite × postgres
    {
        let schema = unique_tag("sql_pg").replace('-', "_");
        let scoped = url_with_schema(&url, &schema);
        create_schema(&url, &schema).await;
        let log_path = tmp_file("sqlite-pg-log", "sqlite");
        let config = base_config(BackendSpec {
            log: LogSpec::Sqlite {
                path: log_path.clone(),
            },
            projection: ProjectionSpec::Postgres { url: scoped },
            control_plane: ControlPlaneSpec::InProcess,
            response_barrier: ResponseBarrierSpec::Strict,
            async_projection: None,
            sqlite_projection_deferred_flush_chunk: None,
        });
        smoke_embedded_cell(config, "sqlite×postgres").await;
        let _ = std::fs::remove_file(&log_path);
        drop_schema(&url, &schema).await;
    }

    // postgres × memory
    {
        let schema = unique_tag("pg_mem").replace('-', "_");
        let scoped = url_with_schema(&url, &schema);
        create_schema(&url, &schema).await;
        let config = base_config(BackendSpec {
            log: LogSpec::Postgres {
                url: scoped,
                credentials: None,
            },
            projection: ProjectionSpec::InMemory,
            control_plane: ControlPlaneSpec::InProcess,
            response_barrier: ResponseBarrierSpec::Strict,
            async_projection: None,
            sqlite_projection_deferred_flush_chunk: None,
        });
        smoke_embedded_cell(config, "postgres×memory").await;
        drop_schema(&url, &schema).await;
    }

    // postgres × sqlite
    {
        let schema = unique_tag("pg_sql").replace('-', "_");
        let scoped = url_with_schema(&url, &schema);
        create_schema(&url, &schema).await;
        let proj = tmp_file("pg-sql-proj", "sqlite");
        let config = base_config(BackendSpec {
            log: LogSpec::Postgres {
                url: scoped,
                credentials: None,
            },
            projection: ProjectionSpec::Sqlite { path: proj.clone() },
            control_plane: ControlPlaneSpec::InProcess,
            response_barrier: ResponseBarrierSpec::Strict,
            async_projection: None,
            sqlite_projection_deferred_flush_chunk: None,
        });
        smoke_embedded_cell(config, "postgres×sqlite").await;
        let _ = std::fs::remove_file(&proj);
        drop_schema(&url, &schema).await;
    }

    // postgres × postgres (atomic same URL)
    {
        let schema = unique_tag("pg_pg").replace('-', "_");
        let scoped = url_with_schema(&url, &schema);
        create_schema(&url, &schema).await;
        let config = base_config(BackendSpec {
            log: LogSpec::Postgres {
                url: scoped.clone(),
                credentials: None,
            },
            projection: ProjectionSpec::Postgres { url: scoped },
            control_plane: ControlPlaneSpec::InProcess,
            response_barrier: ResponseBarrierSpec::Strict,
            async_projection: None,
            sqlite_projection_deferred_flush_chunk: None,
        });
        smoke_embedded_cell(config, "postgres×postgres").await;
        drop_schema(&url, &schema).await;
    }

    // filesystem × postgres
    {
        let schema = unique_tag("fs_pg").replace('-', "_");
        let scoped = url_with_schema(&url, &schema);
        create_schema(&url, &schema).await;
        let root = tmp_root("fs-pg");
        let config = base_config(BackendSpec {
            log: LogSpec::ObjectLog(ObjectLogSpec::local(root.clone(), segments())),
            projection: ProjectionSpec::Postgres { url: scoped },
            control_plane: ControlPlaneSpec::InProcess,
            response_barrier: ResponseBarrierSpec::Strict,
            async_projection: None,
            sqlite_projection_deferred_flush_chunk: None,
        });
        smoke_embedded_cell(config, "filesystem×postgres").await;
        let _ = std::fs::remove_dir_all(&root);
        drop_schema(&url, &schema).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8c_residual_class_a_http_delivery_smoke_through_spawned_task() {
    let _guard = RESIDUAL_SERVER_LOCK.lock().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = tokio::spawn(accept_one_http_ok(listener));

    let log_path = tmp_file("http-sqlite-mem", "sqlite");
    let mut config = base_config(BackendSpec {
        log: LogSpec::Sqlite {
            path: log_path.clone(),
        },
        projection: ProjectionSpec::InMemory,
        control_plane: ControlPlaneSpec::InProcess,
        response_barrier: ResponseBarrierSpec::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
    });
    config.change_record_sink = http_sink(port);
    let server = start(config)
        .await
        .expect("Class A sqlite×memory HTTP delivery must start");
    redis_xadd(server.addr()).await;

    // Wait for at least one emitter tick to attempt HTTP delivery.
    tokio::time::timeout(Duration::from_secs(5), acceptor)
        .await
        .expect("HTTP sink must receive at least one delivery from the spawned emitter")
        .expect("acceptor join");

    server.shutdown_and_drain(Duration::from_secs(5)).await;
    let _ = std::fs::remove_file(&log_path);
}

// ── Per-axis cursor lifecycle fixtures (synthetic durable-log, no catalog replay) ─

#[derive(Default)]
struct CountingSink(std::sync::Mutex<usize>);

impl ChangeRecordSink for CountingSink {
    fn emit(&self, _shard: &QueueKey, records: &[ChangeRecord]) -> EngineResult<()> {
        *self.0.lock().expect("poisoned") += records.len();
        Ok(())
    }
}

/// SQLite-log cursor: monotonic advance, concurrent emit-driven advance, cancel/join, crash/reopen.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8c_residual_sqlite_log_cursor_lifecycle() {
    let path = tmp_file("cursor-sqlite", "sqlite");
    let path_str = path.to_str().expect("utf8");

    // Seed durable commands via a composed backend (queue catalog + log).
    {
        let backend = composed_sqlite_backend(path_str).expect("open sqlite composed");
        backend.create_queue(qdef()).await.unwrap();
        for i in 0..6 {
            backend
                .push(
                    &shard(),
                    vec![PushSpec::default()],
                    UtcTimestamp::new(i, 0).unwrap(),
                    None,
                )
                .await
                .unwrap();
        }
        // Cursor-only: emit two records, then drop without replaying catalog.
        let sink = CountingSink::default();
        assert_eq!(
            backend.with_log(|log| log.emission_cursor(&shard()).unwrap()),
            None
        );
        backend
            .emit_change_record_tail(&shard(), &sink, 2, UtcTimestamp::new(10, 0).unwrap(), None)
            .unwrap();
        let cursor = backend
            .with_log(|log| log.emission_cursor(&shard()).unwrap())
            .expect("cursor after emit");
        assert!(cursor.sequence >= 1, "cursor must advance: {cursor:?}");
        drop(backend);
    }

    // Crash/reopen: cursor-store-only survival independent of queue catalog replay claims.
    {
        let reopened = composed_sqlite_backend(path_str).expect("reopen sqlite composed");
        let cursor = reopened
            .with_log(|log| log.emission_cursor(&shard()).unwrap())
            .expect("cursor survives reopen");
        assert!(cursor.sequence >= 1);

        // Concurrent emit-driven advances (multiple ticks race the same shard).
        let sink = Arc::new(CountingSink::default());
        let backend = Arc::new(reopened);
        let mut handles = Vec::new();
        for _ in 0..4 {
            let b = Arc::clone(&backend);
            let s = Arc::clone(&sink);
            handles.push(tokio::task::spawn_blocking(move || {
                b.emit_change_record_tail(
                    &shard(),
                    s.as_ref(),
                    1,
                    UtcTimestamp::new(20, 0).unwrap(),
                    None,
                )
            }));
        }
        for h in handles {
            h.await
                .expect("join concurrent emit")
                .expect("emit should not fail");
        }
        let advanced = backend
            .with_log(|log| log.emission_cursor(&shard()).unwrap())
            .expect("cursor after concurrent advance");
        assert!(
            advanced.sequence >= cursor.sequence,
            "concurrent emit must not regress cursor ({advanced:?} vs {cursor:?})"
        );

        // Lifecycle cancel/join of the real emitter task over this durable backend.
        let handle = spawn_change_record_emitter(
            Arc::clone(&backend),
            Arc::new(CountingSink::default()) as Arc<dyn ChangeRecordSink>,
            vec![qdef()],
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
    }

    // Direct SqliteLog monotonic + regression guard (cursor-store only).
    {
        let mut log = SqliteLog::open(path_str).expect("open raw sqlite log");
        assert!(log.supports_emission_cursor());
        let cur = log.emission_cursor(&shard()).unwrap().expect("cursor row");
        let next = CommandPosition::new(shard(), cur.backend_epoch, cur.sequence + 10);
        log.set_emission_cursor(&shard(), next.clone()).unwrap();
        assert_eq!(log.emission_cursor(&shard()).unwrap(), Some(next));
        let regress = CommandPosition::new(shard(), cur.backend_epoch, cur.sequence);
        assert_eq!(
            log.set_emission_cursor(&shard(), regress),
            Err(EngineError::Invalid("emission cursor regression"))
        );
    }

    let _ = std::fs::remove_file(&path);
}

/// Filesystem object-log cursor lifecycle (synthetic envelopes; no queue catalog replay).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8c_residual_filesystem_log_cursor_lifecycle() {
    let root = tmp_root("cursor-fs");
    let flush = flush_config_from_segment(262_144, 1);

    let positions = {
        let log = ObjectLogEngineStore::open_local(&root, flush.clone())
            .await
            .expect("open filesystem log");
        let mut definition = qdef();
        definition.emit_change_records = true;
        let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
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

        // Concurrent monotonic set_emission_cursor (metadata-permit serializes regressions).
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
            "at least one concurrent advance ok"
        );
        // The loser may be Ok (if it ran second with higher seq) or Invalid regression.
        let final_cursor = log.emission_cursor(&key).await.unwrap().expect("cursor");
        assert!(
            final_cursor == positions[1] || final_cursor == positions[2],
            "final cursor must be one of the concurrent targets: {final_cursor:?}"
        );

        // Force to last and prove regression rejects.
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

    // Cursor-store-only crash/reopen.
    let reopened = ObjectLogEngineStore::open_local(&root, flush)
        .await
        .expect("reopen filesystem log");
    let key = shard();
    assert_eq!(
        reopened.emission_cursor(&key).await.unwrap(),
        Some(positions[3].clone())
    );

    // Emitter lifecycle cancel/join is covered for sqlite; filesystem product path is
    // exercised by the Embedded delivery smokes above (Server maintenance_tasks).
    let _ = std::fs::remove_dir_all(&root);
}

/// Postgres-log cursor lifecycle (env-gated; synthetic, cursor-store only).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8c_residual_postgres_log_cursor_lifecycle() {
    let Some(url) = pg_url() else {
        eprintln!(
            "P8C RESIDUAL PG CURSOR LIFECYCLE SKIPPED — set FIREWEED_PG_TEST_URL to a live DB"
        );
        return;
    };
    let schema = unique_tag("pg_cursor").replace('-', "_");
    create_schema(&url, &schema).await;

    let positions = tokio::task::spawn_blocking({
        let url = url.clone();
        let schema = schema.clone();
        move || {
            let mut log = fireweed_postgres::PostgresLog::connect_in_schema(&url, &schema)
                .expect("connect postgres log");
            let key = shard();
            log.ensure_shard(&key).unwrap();
            let epoch = log.acquire_epoch(&key).unwrap();
            let commands = vec![
                pause_envelope("pg-a"),
                pause_envelope("pg-b"),
                pause_envelope("pg-c"),
            ];
            let positions = log.append(&key, &commands, epoch).unwrap();
            assert!(log.supports_emission_cursor());
            assert_eq!(log.emission_cursor(&key).unwrap(), None);
            log.set_emission_cursor(&key, positions[0].clone()).unwrap();
            assert_eq!(
                log.emission_cursor(&key).unwrap(),
                Some(positions[0].clone())
            );
            // Concurrent-style sequential advances + regression.
            log.set_emission_cursor(&key, positions[1].clone()).unwrap();
            assert_eq!(
                log.set_emission_cursor(&key, positions[0].clone()),
                Err(EngineError::Invalid("emission cursor regression"))
            );
            log.set_emission_cursor(&key, positions[2].clone()).unwrap();
            positions
        }
    })
    .await
    .expect("spawn_blocking join");

    // Crash/reopen (new connection, same schema) — cursor-store only.
    // Emitter cancel/join for postgres cells is proven via Server.shutdown_and_drain in the
    // Embedded delivery smokes (blocking_backend pool path); direct composed open cannot nest
    // the sync postgres client runtime under tokio.
    let reopened_cursor = tokio::task::spawn_blocking({
        let url = url.clone();
        let schema = schema.clone();
        move || {
            let log = fireweed_postgres::PostgresLog::connect_in_schema(&url, &schema)
                .expect("reopen postgres log");
            log.emission_cursor(&shard()).unwrap()
        }
    })
    .await
    .expect("reopen join");
    assert_eq!(reopened_cursor, Some(positions[2].clone()));

    drop_schema(&url, &schema).await;
}

/// Tick-level opt-out + disabled endpoint tuple (complements server.rs residual seeds).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p8c_residual_opt_out_and_disabled_endpoint_tuple_on_class_a() {
    let log_path = tmp_file("opt-out", "sqlite");

    let mut opted_out = qdef();
    opted_out.emit_change_records = false;
    let mut config = base_config(BackendSpec {
        log: LogSpec::Sqlite {
            path: log_path.clone(),
        },
        projection: ProjectionSpec::InMemory,
        control_plane: ControlPlaneSpec::InProcess,
        response_barrier: ResponseBarrierSpec::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
    });
    config.queues = vec![opted_out];
    config.change_record_sink = embedded_sink();
    let server = start(config)
        .await
        .expect("opt-out queues allow enabled sink without emitter work");
    server.shutdown_and_drain(Duration::from_secs(5)).await;

    // Disabled + valid endpoint is still rejected (tuple coherence).
    let mut disabled = base_config(BackendSpec {
        log: LogSpec::Sqlite {
            path: log_path.clone(),
        },
        projection: ProjectionSpec::InMemory,
        control_plane: ControlPlaneSpec::InProcess,
        response_barrier: ResponseBarrierSpec::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
    });
    disabled.change_record_sink.endpoint = Some("http://127.0.0.1:9".into());
    assert_eq!(
        start(disabled).await.err(),
        Some(EngineError::Invalid(
            "change-record-endpoint-requires-enabled"
        ))
    );

    // Synthetic tick on opted-out queue must not advance cursor.
    let backend = composed_sqlite_backend(log_path.to_str().unwrap()).expect("open");
    let mut opt_out_def = qdef();
    opt_out_def.emit_change_records = false;
    backend.create_queue(opt_out_def.clone()).await.unwrap();
    backend
        .push(
            &shard(),
            vec![PushSpec::default()],
            UtcTimestamp::new(0, 0).unwrap(),
            None,
        )
        .await
        .unwrap();
    let sink = CountingSink::default();
    emit_change_record_tick(&backend, &sink, &[opt_out_def], 16).unwrap();
    assert_eq!(
        backend.with_log(|log| log.emission_cursor(&shard()).unwrap()),
        None,
        "opt-out must not advance emission cursor"
    );

    let _ = std::fs::remove_file(&log_path);
}
