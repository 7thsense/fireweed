#![forbid(unsafe_code)]
//! # pqueue-server
//!
//! The **composition root**: the single place that selects a concrete backend (memory / sqlite /
//! objectlog) and wires it to the two faces of pqueue. It binds the RESP front ([`pqueue_resp::serve`])
//! and runs a **background [`ReclaimDriver`] task** that periodically `tick`s the engine so expired
//! leases are reclaimed on a *quiet* queue with no client traffic — closing the orphan-on-quiet-queue
//! gap (TD-007 §3) that the client-triggered `XAUTOCLAIM` alone leaves open.
//!
//! Hexagonal: this is the ONLY crate that names concrete adapters; everything else depends only inward.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fjord::{
    FjordClusterView, FjordGroupCoordinator, FjordLog, FjordOffsetStore, FjordTopicRegistry,
};
use heimq::config::Config as HeimqConfig;
use heimq::server::Server as HeimqServer;
use heimq_broker::storage::{ClusterView, LogBackend, OffsetStore, RecordBatchView};
use pqueue_core::{OwnerId, QueueDefinition, QueueId, TenantId, UtcTimestamp};
use pqueue_engine::{
    AcquireOutcome, AuthContext, Clock, ComposedBackend, EngineError, EngineResult,
    InMemoryControlPlane, InProcessControlPlane, LeaseState, OwnedSession, QueueControlPlane,
    QueueKey,
};
use pqueue_memory::composed_memory_backend;
use pqueue_objectlog::ObjectLog;
use pqueue_resp::{
    RespBackend, RespHooks, RouteDecision, SystemClock, route, serve_with_shutdown,
    serve_with_shutdown_and_hooks,
};
use pqueue_sqlite::{HybridProjectionStore, composed_sqlite_backend};
// Re-exported: it is the type of the public `Config::hybrid_async` field, so composition-root callers and
// tests that construct a `Config` directly can name the async-apply threshold config.
pub use pqueue_sqlite::HybridAsyncThresholds;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

mod change_record_sink;
mod object_log_sqlite;
pub use change_record_sink::{
    ChangeRecordSinkConfig, ChangeRecordSinkMode, FjordChangeRecordSink, NiflheimChangeRecordSink,
    emit_change_record_tick, spawn_change_record_emitter,
};
pub use object_log_sqlite::{
    DEFAULT_RECOVERY_MAX_TAIL, ObjectLogSqliteBackend, SegmentedObjectLogInMemoryBackend,
    SegmentedObjectLogSqliteBackend,
};
pub use pqueue_objectlog::segmented::SegmentConfig;

/// The single optional env-var populator for [`Config`] (`Config::from_env`) plus its [`ConfigError`]. Pure
/// over a caller-supplied env map; the only process-env read lives in the `pqueue-service` bin's `main`.
#[cfg(feature = "env-config")]
mod env_config;
#[cfg(feature = "env-config")]
pub use env_config::ConfigError;

#[cfg(feature = "postgres")]
mod postgres_native;
#[cfg(feature = "postgres")]
pub use postgres_native::{BlockingBackend, PostgresNativeBackend};

/// The durable command-LOG axis (ADR-012): which substrate holds the command log + the co-located
/// epoch/fence authority. One half of a [`BackendSpec`].
pub enum LogSpec {
    /// In-memory reference log (atomic class; non-durable).
    Memory,
    /// Durable sqlite command log at `path` (atomic class).
    Sqlite { path: PathBuf },
    /// Segmented group-commit object log (`SegmentedObjectLog<LocalFsBlobStore>`) rooted at `root` over the
    /// local filesystem (eventual-apply class). This is the object log's ONLY production form — the
    /// per-command-file mode and the `PQUEUE_OBJECT_LOG_MODE` pseudo-axis are retired (ADR-012 P2).
    ObjectLog { root: PathBuf },
    /// SYNC postgres durable-log adapter (atomic class), driven through the blocking-safe
    /// [`PostgresNativeBackend`] wrapper so no sync postgres client call runs on a Tokio worker thread.
    /// `url` is a libpq/postgres connection string (URL or `key=value` DSN, with a native password); with the
    /// `tls` feature an `sslmode=require|prefer` url connects over native-tls (Lakebase / cloud postgres).
    /// `credentials` optionally injects a Databricks service-principal/PAT credential at connect (the
    /// user/password is set from the provider instead of the DSN). Requires the `postgres` cargo feature.
    #[cfg(feature = "postgres")]
    Postgres {
        url: String,
        credentials: Option<pqueue_postgres::CredentialProvider>,
    },
}

impl LogSpec {
    fn label(&self) -> &'static str {
        match self {
            LogSpec::Memory => "memory",
            LogSpec::Sqlite { .. } => "sqlite",
            LogSpec::ObjectLog { .. } => "objectlog",
            #[cfg(feature = "postgres")]
            LogSpec::Postgres { .. } => "postgres",
        }
    }
}

/// The materialized-PROJECTION axis (ADR-012): the read model the composition renders from. The other half
/// of a [`BackendSpec`].
pub enum ProjectionSpec {
    /// In-memory `ProjectionData` projection, rebuilt by log replay on open.
    InMemory,
    /// Derived relational sqlite projection (`pqueue_items` is the read model) at `path`.
    Sqlite { path: PathBuf },
    /// SQLite-first durable projection image plus hot in-memory serving at `path`.
    Hybrid { path: PathBuf },
    /// The `objectlog/hybrid-strict` profile (TD-004): the SAME hot-in-memory serving + durable SQLite
    /// projection image at `path` as [`Self::Hybrid`], but the group-commit write path commits the sealed
    /// batch DURABLY to SQLite BEFORE applying it to hot memory (`apply_durable_then_memory`). A SQLite
    /// failure returns no success and replays the object-log tail; a SQLite-commit-then-memory-apply failure
    /// poisons the store fail-closed until restart, when memory rehydrates from the SQLite `ProjectionImage`.
    HybridStrict { path: PathBuf },
    /// The `objectlog/hybrid-async` profile (TD-004): the SAME hot-in-memory serving + durable SQLite
    /// projection image at `path` as [`Self::Hybrid`], selected under its canonical `hybrid-async` name so
    /// the deployment carries the async-apply debt/backpressure/poison threshold config
    /// ([`Config::hybrid_async`]). Manifest commit + synchronous in-memory apply/render is the success
    /// barrier; the durable SQLite image is an asynchronous checkpoint that MAY lag and is caught up by
    /// object-log tail replay on recovery.
    HybridAsync { path: PathBuf },
    /// SYNC postgres relational projection (`PostgresRelational`, atomic class) at `url`, composed against
    /// the [`LogSpec::Postgres`] durable log. `url` is a libpq/postgres connection string; connect + recover
    /// MUST run off the reactor (the composition root drives it through `spawn_blocking`, same as the log
    /// axis). Requires the `postgres` cargo feature.
    #[cfg(feature = "postgres")]
    Postgres { url: String },
}

impl ProjectionSpec {
    fn label(&self) -> &'static str {
        match self {
            ProjectionSpec::InMemory => "inmemory",
            ProjectionSpec::Sqlite { .. } => "sqlite",
            ProjectionSpec::Hybrid { .. } => "hybrid",
            ProjectionSpec::HybridStrict { .. } => "hybrid-strict",
            ProjectionSpec::HybridAsync { .. } => "hybrid-async",
            #[cfg(feature = "postgres")]
            ProjectionSpec::Postgres { .. } => "postgres",
        }
    }
}

type ObjectLogHybridBackend =
    ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>;

/// The control-plane axis (queue definitions + placement). The in-process plane is the only wired one.
pub enum ControlPlaneSpec {
    InProcess,
}

fn change_record_sink_profile_is_wired(log: &LogSpec, projection: &ProjectionSpec) -> bool {
    matches!(
        (log, projection),
        (LogSpec::ObjectLog { .. }, ProjectionSpec::Hybrid { .. })
            | (
                LogSpec::ObjectLog { .. },
                ProjectionSpec::HybridStrict { .. }
            )
            | (
                LogSpec::ObjectLog { .. },
                ProjectionSpec::HybridAsync { .. }
            )
    )
}

/// Typed configuration for the embedded fjord surface that pqueue-server boots behind the composition
/// root seam. The namespace root is isolated from pqueue's own queue storage roots so the Kafka surface
/// state never shares a directory with the queue commit path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddedFjordConfig {
    pub namespace_root: PathBuf,
    pub cluster_id: String,
    /// Optional TCP listen address (`host:port` or `kafka://host:port`) for the embedded broker's
    /// EXTERNAL-consumer Kafka surface. `None` (the default) keeps the change log purely in-process: pqueue
    /// appends change records directly to the shared log and the write path binds no socket (ADR-014). Set
    /// this only when a deployment wants external Kafka consumers to read the change log over TCP; the
    /// surface then serves fetches from the SAME in-process log the sink appends to.
    pub broker_listen: Option<String>,
}

impl Default for EmbeddedFjordConfig {
    fn default() -> Self {
        Self {
            namespace_root: PathBuf::from("/var/lib/pqueue/fjord"),
            cluster_id: "pqueue-fjord".to_string(),
            broker_listen: None,
        }
    }
}

/// The in-process fjord surface materialized from [`EmbeddedFjordConfig`].
pub struct EmbeddedFjordSurface {
    node_id: i32,
    namespace_root: PathBuf,
    cluster_id: String,
    pub topic_registry: Arc<FjordTopicRegistry>,
    pub log: Arc<FjordLog>,
    pub offset_store: Arc<FjordOffsetStore>,
    pub cluster_view: Arc<FjordClusterView>,
    pub group_coordinator: Arc<FjordGroupCoordinator>,
}

impl EmbeddedFjordSurface {
    pub fn namespace_root(&self) -> &PathBuf {
        &self.namespace_root
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub fn node_id(&self) -> i32 {
        self.node_id
    }

    /// The shared in-process log handle, typed as the broker's [`LogBackend`]. This is the SAME handle the
    /// embedded `HeimqServer` serves external Kafka consumers from and the change-record sink appends to, so
    /// in-process appends are immediately visible to broker fetches.
    pub fn log_backend(&self) -> Arc<dyn LogBackend> {
        Arc::clone(&self.log) as Arc<dyn LogBackend>
    }
}

/// Construct the embedded fjord surface from typed config. This stays separate from the queue commit path:
/// the returned surface owns its own namespace root and state objects, and the queue backend never shares
/// those directories or handles.
fn embedded_fjord_namespace_root(node_id: i32, config: &EmbeddedFjordConfig) -> PathBuf {
    config.namespace_root.join(format!("node-{node_id}"))
}

pub fn build_embedded_fjord_surface(
    node_id: i32,
    config: &EmbeddedFjordConfig,
) -> EmbeddedFjordSurface {
    let namespace_root = embedded_fjord_namespace_root(node_id, config);
    let topic_registry = FjordTopicRegistry::new(node_id);
    let log = Arc::new(FjordLog::new_with_registry(Arc::clone(&topic_registry)));
    let offset_store = FjordOffsetStore::new();
    let cluster_view = Arc::new(FjordClusterView::new_with_registry(
        node_id,
        "127.0.0.1",
        0,
        config.cluster_id.clone(),
        Arc::clone(&topic_registry),
    ));

    EmbeddedFjordSurface {
        node_id,
        namespace_root,
        cluster_id: config.cluster_id.clone(),
        topic_registry,
        log,
        offset_store,
        cluster_view,
        group_coordinator: Arc::new(FjordGroupCoordinator::new()),
    }
}

/// Canonical Kafka topic name for a queue namespace.
///
/// ADR-014 scopes each change stream to a tenant-prefixed topic so the embedded surface can
/// authorize reads by exact `(tenant_id, queue_id)` identity instead of broad tenant-only access.
pub fn fjord_topic_name(queue: &QueueKey) -> EngineResult<String> {
    validate_fjord_topic_segment(queue.tenant_id.as_str())?;
    validate_fjord_topic_segment(queue.queue_id.as_str())?;

    let topic = format!("{}.{}", queue.tenant_id.as_str(), queue.queue_id.as_str());
    if topic.len() > 249 {
        return Err(EngineError::Invalid(
            "fjord topic name exceeds Kafka's 249-character limit",
        ));
    }
    Ok(topic)
}

/// Register the tenant-prefixed change-log topics owned by the configured queues.
pub fn register_embedded_fjord_topics(
    topic_registry: &FjordTopicRegistry,
    queues: &[QueueDefinition],
) -> EngineResult<()> {
    for queue in queues {
        let key = QueueKey::new(queue.tenant_id.clone(), queue.queue_id.clone());
        topic_registry.register_topic(&fjord_topic_name(&key)?, 1);
    }
    Ok(())
}

fn parse_fjord_topic_name(topic: &str) -> EngineResult<QueueKey> {
    if topic.len() > 249 {
        return Err(EngineError::Invalid(
            "fjord topic name exceeds Kafka's 249-character limit",
        ));
    }
    let (tenant, queue) = topic
        .split_once('.')
        .ok_or(EngineError::Invalid("fjord topic must be tenant-prefixed"))?;
    validate_fjord_topic_segment(tenant)?;
    validate_fjord_topic_segment(queue)?;
    Ok(QueueKey::new(
        TenantId::new(tenant).map_err(|_| EngineError::Invalid("bad tenant"))?,
        QueueId::new(queue).map_err(|_| EngineError::Invalid("bad queue"))?,
    ))
}

/// In-process topic readiness for the shared embedded surface.
///
/// Because pqueue now owns the surface, topic existence is a synchronous in-process property: we create
/// each queue topic in the shared `FjordLog` and register it in the shared `FjordTopicRegistry` before the
/// broker starts serving. This replaces the former loopback metadata poll — no Kafka client, no
/// socket round-trip — and it fails closed if any expected topic is missing after creation.
fn embedded_fjord_topics_ready(
    surface: &EmbeddedFjordSurface,
    topics: &[String],
) -> EngineResult<()> {
    for topic in topics {
        // Idempotent: create the single-partition topic in the shared log so external-consumer fetches
        // (and in-process appends) find it, and register it for Metadata responses.
        surface.log.get_or_create_topic(topic, 1);
        if surface.log.topic(topic).is_none() {
            return Err(EngineError::Storage(format!(
                "embedded fjord broker could not create change-log topic {topic}"
            )));
        }
    }
    Ok(())
}

fn validate_fjord_topic_segment(value: &str) -> EngineResult<()> {
    if value.is_empty() {
        return Err(EngineError::Invalid(
            "fjord topic components must not be empty",
        ));
    }
    if value.contains('.') {
        return Err(EngineError::Invalid(
            "fjord topic components must not contain '.' because fjord topics use '.' as the tenant/queue separator",
        ));
    }
    if !value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-'))
    {
        return Err(EngineError::Invalid(
            "fjord topic components must contain only ASCII alphanumerics, '_' or '-'",
        ));
    }
    Ok(())
}

/// Deny-by-default tenant/queue ACL bridge for the embedded change-log surface.
///
/// The caller is authorized only for its exact namespace. A different tenant or a different queue in
/// the same tenant is rejected.
pub fn authorize_fjord_topic_read(
    auth: &AuthContext,
    allowed_queue: &QueueKey,
    requested_topic: &str,
) -> EngineResult<()> {
    auth.authorize_tenant(allowed_queue.tenant_id.as_str())?;
    let requested_queue = parse_fjord_topic_name(requested_topic)?;
    if requested_queue == *allowed_queue {
        Ok(())
    } else {
        Err(EngineError::Forbidden(
            "principal is not authorized for the requested queue namespace",
        ))
    }
}

fn parse_kafka_bootstrap(input: &str) -> EngineResult<(String, u16)> {
    let trimmed = input.trim();
    let without_scheme = trimmed
        .strip_prefix("kafka://")
        .or_else(|| trimmed.strip_prefix("tcp://"))
        .unwrap_or(trimmed);
    let (host, port) = without_scheme.rsplit_once(':').ok_or(EngineError::Invalid(
        "kafka endpoint must include host:port",
    ))?;
    let port = port
        .parse::<u16>()
        .map_err(|_| EngineError::Invalid("kafka endpoint port must be a u16"))?;
    if host.is_empty() {
        return Err(EngineError::Invalid("kafka endpoint host is required"));
    }
    Ok((host.to_string(), port))
}

/// Spawn the embedded fjord broker (the external-consumer Kafka surface over the SHARED in-process log).
///
/// The broker is built over the caller's `surface` — the same `Arc<dyn LogBackend>` / offset store the
/// change-record sink appends to — so change records written in-process are immediately fetchable by
/// external Kafka consumers over this TCP surface. There is no loopback socket on the write path: pqueue
/// appends directly to `surface.log`; only external consumers use this socket (ADR-014).
pub async fn spawn_embedded_fjord_broker(
    surface: &EmbeddedFjordSurface,
    endpoint: &str,
    queues: &[QueueDefinition],
) -> EngineResult<JoinHandle<()>> {
    let (host, port) = parse_kafka_bootstrap(endpoint)?;
    let node_id = surface.node_id();
    let cluster_id = surface.cluster_id().to_string();
    let topics = queues
        .iter()
        .map(|queue| {
            fjord_topic_name(&QueueKey::new(
                queue.tenant_id.clone(),
                queue.queue_id.clone(),
            ))
        })
        .collect::<EngineResult<Vec<_>>>()?;
    register_embedded_fjord_topics(&surface.topic_registry, queues)?;
    // Create the queue topics in the shared log synchronously so external fetches and in-process appends
    // both find them before the broker starts serving.
    embedded_fjord_topics_ready(surface, &topics)?;
    let namespace_root = surface.namespace_root().clone();

    let broker_config = HeimqConfig {
        host: host.clone(),
        port,
        data_dir: namespace_root,
        memory_only: true,
        segment_size: 1024 * 1024 * 1024,
        retention_ms: 7 * 24 * 60 * 60 * 1000,
        max_memory_bytes: 0,
        default_partitions: 1,
        broker_id: node_id,
        cluster_id: cluster_id.clone(),
        metrics: false,
        metrics_port: 9093,
        create_topics: topics.iter().map(|topic| format!("{topic}:1")).collect(),
        // The embedded change-log surface only serves the exact queue topics we pre-register.
        // Unknown topics must not be auto-created, otherwise Metadata can leak or mint
        // namespaces outside the configured tenant/queue set.
        auto_create_topics: false,
        storage_log: "memory://".to_string(),
        storage_offsets: "memory://".to_string(),
        storage_groups: "memory://".to_string(),
        advertised_host: Some(host.clone()),
    };

    let cluster_view = Arc::new(FjordClusterView::new_with_registry(
        node_id,
        host.clone(),
        port,
        cluster_id,
        Arc::clone(&surface.topic_registry),
    ));
    let server = HeimqServer::with_backends_and_cluster_view(
        broker_config,
        Arc::clone(&surface.log) as Arc<dyn LogBackend>,
        Arc::clone(&surface.offset_store) as Arc<dyn OffsetStore>,
        cluster_view as Arc<dyn ClusterView>,
    )
    .map_err(|e| EngineError::Storage(format!("build embedded fjord server: {e}")))?;

    let bootstrap = format!("{host}:{port}");
    let handle = tokio::spawn(async move {
        if let Err(e) = server.run().await {
            eprintln!("[fjord] embedded broker terminated: {e}");
        }
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match tokio::net::TcpStream::connect(&bootstrap).await {
            Ok(_) => break,
            Err(_) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => {
                handle.abort();
                return Err(EngineError::Storage(format!(
                    "embedded fjord broker did not bind at {bootstrap}: {e}"
                )));
            }
        }
    }

    Ok(handle)
}

/// A single change record decoded back out of the embedded fjord log (partition 0). The consumer-contract
/// introspection type: it exposes the exact record shape an external Kafka consumer would observe — the
/// broker-assigned `offset`, the TD-008 idempotency `key`, the `pq-*` `headers`, and the `ChangeRecord`
/// JSON `value` — read through the same in-process log the embedded `HeimqServer` fetches from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddedChangeRecord {
    pub offset: i64,
    pub partition: i32,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
}

/// Read and decode every change record on a queue topic's partition 0 from the shared embedded log, in
/// offset order. This verifies the consumer contract in-process (no Kafka client, no socket): the bytes are
/// the exact Kafka v2 record batches the embedded broker serves external consumers.
pub fn read_embedded_change_records(
    surface: &EmbeddedFjordSurface,
    topic: &str,
) -> EngineResult<Vec<EmbeddedChangeRecord>> {
    let (bytes, _hwm) = surface
        .log
        .fetch(topic, 0, 0, i32::MAX)
        .map_err(|e| EngineError::Storage(format!("fetch embedded change records: {e}")))?;
    decode_change_record_batches(&bytes)
}

fn decode_change_record_batches(mut data: &[u8]) -> EngineResult<Vec<EmbeddedChangeRecord>> {
    let mut out = Vec::new();
    // The fetch payload is a concatenation of Kafka v2 record batches. Each batch's `batch_length` field
    // (bytes 8..12) counts the bytes AFTER that field, so a batch occupies `12 + batch_length` bytes.
    while data.len() >= 12 {
        let batch_len = i32::from_be_bytes(
            data[8..12]
                .try_into()
                .map_err(|_| EngineError::Storage("truncated record batch length".into()))?,
        ) as usize;
        let total = 12 + batch_len;
        if data.len() < total {
            break;
        }
        let (batch, rest) = data.split_at(total);
        let view = RecordBatchView::from_bytes(batch)
            .map_err(|e| EngineError::Storage(format!("decode change-record batch: {e}")))?;
        let base = view.base_offset();
        for record in view.records() {
            out.push(EmbeddedChangeRecord {
                offset: base + i64::from(record.offset_delta),
                partition: 0,
                key: record.key.map(|b| b.as_ref().to_vec()),
                value: record.value.map(|b| b.as_ref().to_vec()),
                headers: record
                    .headers()
                    .map(|(k, v)| (k.to_string(), v.map(|b| b.to_vec())))
                    .collect(),
            });
        }
        data = rest;
    }
    Ok(out)
}

/// Spawn the embedded broker's external-consumer TCP surface only for the in-process `Embedded` sink mode
/// AND only when the deployment configured `embedded_fjord.broker_listen`. In-process appends happen
/// regardless (the sink holds the shared log); this merely exposes the SAME log to external Kafka consumers
/// over TCP. `Http`/`ExternalKafka`/`Disabled` modes never bind the embedded surface.
async fn maybe_spawn_embedded_broker(
    surface: &EmbeddedFjordSurface,
    broker_listen: Option<&str>,
    change_record_sink: &ChangeRecordSinkConfig,
    queues: &[QueueDefinition],
) -> EngineResult<Option<JoinHandle<()>>> {
    if !change_record_sink::change_record_sink_is_embedded(change_record_sink) {
        return Ok(None);
    }
    match broker_listen {
        Some(listen) => Ok(Some(
            spawn_embedded_fjord_broker(surface, listen, queues).await?,
        )),
        None => Ok(None),
    }
}

/// A backend selected as the orthogonal product `LogSpec × ProjectionSpec × ControlPlaneSpec` (ADR-012).
/// [`start`] assembles the concrete backend from this spec.
///
/// NOTE (ADR-012 P2 status): the spec is the one composition axis the server selects on, and [`start`] now
/// assembles the wired families from the ONE generic `ComposedBackend` — `Memory → composed_memory_backend`,
/// `Sqlite → composed_sqlite_backend`, `Postgres → composed_postgres_backend_with_config` (the durable
/// `PostgresLog` command log + in-memory projection, driven blocking-safe through [`BlockingBackend`]). The
/// generic `ComposedBackend::recover` rebuilds the projection + counters + cmd-seq from the durable
/// log/projection on open (honored across every durable composition by the reopen/recovery conformance
/// dimension), so no composed family regresses restart durability. The object-log families
/// (`ObjectLog × {InMemory, Sqlite}`) still run on the segmented group-commit backends because their
/// production env contract — concurrent segment co-buffering + the latency-seal flusher + segment-config /
/// debug-segments / recovery-tail knobs — is not yet expressed by the per-append-seal composed `ObjectLog`
/// axis; `objectlog/hybrid` is the first runtime that uses that generic group-commit axis directly.
pub struct BackendSpec {
    pub log: LogSpec,
    pub projection: ProjectionSpec,
    pub control_plane: ControlPlaneSpec,
}

impl BackendSpec {
    /// The single-node in-memory reference composition (`Memory × InMemory × InProcess`).
    pub fn memory() -> Self {
        Self {
            log: LogSpec::Memory,
            projection: ProjectionSpec::InMemory,
            control_plane: ControlPlaneSpec::InProcess,
        }
    }
}

/// Resolve the postgres [`LogSpec`] from the runtime environment, using the env names the Helm Lakebase
/// profile renders. The DSN secret is `PQUEUE_POSTGRES_LOG_DATABASE_URL` (the chart's log-backend Secret
/// ref); `PQUEUE_PG_URL` is the local/dev fallback, and the documented default is the last resort. A
/// Databricks service-principal/PAT credential provider is attached when `DATABRICKS_HOST` is present.
///
/// No plaintext fallback: if the DSN demands `sslmode=require` but this binary was built WITHOUT the `tls`
/// feature, this fails at config time rather than letting the runtime silently downgrade to `NoTls`.
///
/// This is a pure function over an env map (no live DB, no process env) so the composition-root config
/// layer is unit-testable.
#[cfg(feature = "postgres")]
pub fn resolve_postgres_log(
    env: &std::collections::BTreeMap<String, String>,
) -> Result<LogSpec, String> {
    let nonempty = |key: &str| env.get(key).filter(|s| !s.is_empty()).cloned();
    let url = nonempty("PQUEUE_POSTGRES_LOG_DATABASE_URL")
        .or_else(|| nonempty("PQUEUE_PG_URL"))
        .unwrap_or_else(|| "postgres://postgres@127.0.0.1:5432/postgres".to_string());

    // Fail closed before connecting if the DSN requires TLS but this build cannot provide it.
    let ssl_mode = pqueue_postgres::PostgresConnectConfig::new(&url)
        .parsed_ssl_mode()
        .map_err(|e| format!("invalid postgres DSN: {e}"))?;
    #[cfg(not(feature = "tls"))]
    if matches!(ssl_mode, pqueue_postgres::PostgresSslMode::Require) {
        return Err(
            "DSN requests sslmode=require but this binary was built without the `tls` feature; rebuild \
             `--features postgres,tls` (no plaintext downgrade)"
                .to_string(),
        );
    }
    let _ = ssl_mode;

    // Databricks service-principal / PAT credential injection: present iff DATABRICKS_HOST is set. The
    // provider supersedes any DSN password (and sets the postgres user for service-principal OAuth).
    let credentials = if nonempty("DATABRICKS_HOST").is_some() {
        let config = pqueue_postgres::DatabricksCredentialConfig::from_env_map(env.clone())
            .map_err(|e| format!("invalid Databricks credential configuration: {e}"))?;
        let provider = pqueue_postgres::DatabricksCredentialProvider::from_config(config)
            .map_err(|e| format!("could not build Databricks credential provider: {e}"))?;
        Some(pqueue_postgres::CredentialProvider::Databricks(provider))
    } else {
        None
    };

    Ok(LogSpec::Postgres { url, credentials })
}

/// Resolve the postgres [`ProjectionSpec`] from the runtime environment, using the env name the Helm chart's
/// `storage.projection.postgres` axis renders. The DSN secret is `PQUEUE_POSTGRES_PROJECTION_DATABASE_URL`;
/// `PQUEUE_PG_PROJECTION_URL` is the local/dev fallback, and the documented default is the last resort.
///
/// No plaintext fallback: if the DSN demands `sslmode=require` but this binary was built WITHOUT the `tls`
/// feature, this fails at config time rather than letting the runtime silently downgrade to `NoTls`.
///
/// This is a pure function over an env map (no live DB, no process env) so the composition-root config
/// layer is unit-testable.
#[cfg(feature = "postgres")]
pub fn resolve_postgres_projection(
    env: &std::collections::BTreeMap<String, String>,
) -> Result<ProjectionSpec, String> {
    let nonempty = |key: &str| env.get(key).filter(|s| !s.is_empty()).cloned();
    let url = nonempty("PQUEUE_POSTGRES_PROJECTION_DATABASE_URL")
        .or_else(|| nonempty("PQUEUE_PG_PROJECTION_URL"))
        .unwrap_or_else(|| "postgres://postgres@127.0.0.1:5432/postgres".to_string());

    // Fail closed before connecting if the DSN requires TLS but this build cannot provide it.
    let ssl_mode = pqueue_postgres::PostgresConnectConfig::new(&url)
        .parsed_ssl_mode()
        .map_err(|e| format!("invalid postgres DSN: {e}"))?;
    #[cfg(not(feature = "tls"))]
    if matches!(ssl_mode, pqueue_postgres::PostgresSslMode::Require) {
        return Err(
            "DSN requests sslmode=require but this binary was built without the `tls` feature; rebuild \
             `--features postgres,tls` (no plaintext downgrade)"
                .to_string(),
        );
    }
    let _ = ssl_mode;

    Ok(ProjectionSpec::Postgres { url })
}

/// The single authoritative, fully-typed runtime configuration for a pqueue server. Every knob the server
/// needs lives here as a typed field; there is exactly ONE optional env populator (`Config::from_env`, in
/// the `pqueue-service` bin) that maps the documented `PQUEUE_*`/`DATABRICKS_*` env names onto these fields.
/// A pure-library embedder constructs this struct directly and never touches the process environment — the
/// library reads no env vars at all.
pub struct Config {
    pub backend: BackendSpec,
    /// Typed configuration for the embedded fjord surface. The queue commit path does not share this
    /// namespace; it is only booted as the Kafka-facing surface behind the composition root seam.
    pub embedded_fjord: EmbeddedFjordConfig,
    /// This instance's node id, packed into the disambiguation byte of every minted `ItemId` (ADR-009) so
    /// distinct replicas over a shared store never mint a colliding id. It is a *configured* value: the
    /// deployment is responsible for handing each replica a distinct one (e.g. the Helm chart maps a
    /// StatefulSet ordinal or pod identity into it) — the application stays infrastructure-agnostic. Build
    /// it from a configured string via [`resolve_node_id`]. `0` is the single-instance default.
    pub node_id: u8,
    /// Listen address, e.g. `"127.0.0.1:6380"` (use `":0"` for an ephemeral port in tests).
    pub listen: String,
    /// How often the background reclaim task ticks the engine.
    pub reclaim_interval: Duration,
    /// Queues to provision at startup. The RESP front has no create-queue command, so a server started
    /// with no queues here (and no out-of-band creation) would reject every request with `no such
    /// queue` — provision them up front.
    pub queues: Vec<QueueDefinition>,
    /// Group-commit segment configuration (byte-size + latency seal triggers) for the segmented object-log
    /// families. The typed form of `PQUEUE_SEGMENT_TARGET_BYTES` / `PQUEUE_SEGMENT_MAX_LATENCY_MS`; applied by
    /// [`start`] when the [`LogSpec::ObjectLog`] log axis is selected.
    pub segment_config: SegmentConfig,
    /// Recovery-window budget (max object-log tail commands) before an object-log+SQLite reopen logs a
    /// recovery-window warning. Parsed from `PQUEUE_RECOVERY_MAX_TAIL_COMMANDS` (default
    /// [`DEFAULT_RECOVERY_MAX_TAIL`]); applied by [`start`] to the objectlog+sqlite backend.
    pub recovery_max_tail: u64,
    /// Opt-in group-commit telemetry for the segmented+SQLite object-log backend (the typed form of
    /// `PQUEUE_DEBUG_SEGMENTS`).
    pub debug_segments: bool,
    /// Tokio worker-thread cap (the typed form of `PQUEUE_WORKER_THREADS`). `None` = one worker per core.
    /// Consumed by the bin when building the runtime, not by [`start`].
    pub worker_threads: Option<usize>,
    /// Per-queue bounds on `objectlog/hybrid-async` async SQLite apply debt (bead pqueue-6da52695): the
    /// hard lag/bytes/depth/age limits and the apply-retry poison threshold that drive backpressure and
    /// fail-closed poison (TD-004 §"Async apply debt, backpressure, and poison thresholds"). The typed form
    /// of the `PQUEUE_HYBRID_ASYNC_*` env names; applied by the hybrid-async projection's apply pipeline.
    pub hybrid_async: HybridAsyncThresholds,
    /// Cap on how many deferred SQLite-checkpoint commands one `objectlog/hybrid` or
    /// `objectlog/hybrid-async` deferred-flush call applies (bead pqueue-8e5e7846). `flush_deferred` runs
    /// under the composed backend's unit-of-work mutex, so bounding this bounds the worst-case time one
    /// call can block concurrent push/claim callers; the periodic flusher's 250ms cadence drains a larger
    /// backlog over several calls instead of one unbounded transaction. The typed form of
    /// `PQUEUE_HYBRID_DEFERRED_FLUSH_CHUNK`, defaulting to
    /// [`pqueue_sqlite::DEFAULT_DEFERRED_FLUSH_CHUNK`]; applied to the hybrid projection store on open.
    pub deferred_flush_chunk: usize,
    // Background change-record emission settings (TD-008). Disabled by default; enabled deployments
    // can point at a niflheim durable-ingest endpoint and configure tick/batch cadence.
    pub change_record_sink: ChangeRecordSinkConfig,
}

impl Config {
    /// Construct a config with the in-code defaults for the env-only knobs (recovery budget, debug-segments,
    /// worker-threads). The composition root / embedder supplies the core fields; the optional knobs default
    /// to their library defaults. Keeps call sites that don't care about the env knobs concise.
    pub fn new(
        backend: BackendSpec,
        node_id: u8,
        listen: String,
        reclaim_interval: Duration,
        queues: Vec<QueueDefinition>,
    ) -> Self {
        Self {
            backend,
            embedded_fjord: EmbeddedFjordConfig::default(),
            node_id,
            listen,
            reclaim_interval,
            queues,
            segment_config: SegmentConfig::new(262_144, 20).expect("valid default segment config"),
            recovery_max_tail: DEFAULT_RECOVERY_MAX_TAIL,
            debug_segments: false,
            worker_threads: None,
            hybrid_async: HybridAsyncThresholds::default(),
            deferred_flush_chunk: pqueue_sqlite::DEFAULT_DEFERRED_FLUSH_CHUNK,
            change_record_sink: ChangeRecordSinkConfig::default(),
        }
    }
}

pub struct OwnershipRuntime<B, CP> {
    backend: Arc<B>,
    control_plane: Arc<CP>,
    owner: OwnerId,
    endpoint: String,
    owner_endpoints: Mutex<std::collections::HashMap<OwnerId, String>>,
    managed_queues: Mutex<std::collections::HashSet<QueueKey>>,
    sessions: Mutex<std::collections::HashMap<QueueKey, OwnedSession>>,
    /// Per-queue gate serializing COLD-START acquisition. `acquire_queue_lease` is non-idempotent (it bumps
    /// the epoch on every call), so two concurrent first-writes to an unowned queue would each acquire,
    /// double-bumping the epoch and fencing the laggard. This gate (taken only on the unowned path, never on
    /// the hot already-owned path) lets the first acquirer win and the rest reuse its session.
    acquire_gates: Mutex<std::collections::HashMap<QueueKey, Arc<tokio::sync::Mutex<()>>>>,
}

impl<B, CP> OwnershipRuntime<B, CP>
where
    B: RespBackend,
    CP: QueueControlPlane + 'static,
{
    pub fn new(backend: Arc<B>, control_plane: Arc<CP>, owner: OwnerId, endpoint: String) -> Self {
        let mut endpoints = std::collections::HashMap::new();
        endpoints.insert(owner.clone(), endpoint.clone());
        Self {
            backend,
            control_plane,
            owner,
            endpoint,
            owner_endpoints: Mutex::new(endpoints),
            managed_queues: Mutex::new(std::collections::HashSet::new()),
            sessions: Mutex::new(std::collections::HashMap::new()),
            acquire_gates: Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub fn owner(&self) -> &OwnerId {
        &self.owner
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn set_owner_endpoint(&self, owner: OwnerId, endpoint: impl Into<String>) {
        self.owner_endpoints
            .lock()
            .expect("poisoned")
            .insert(owner, endpoint.into());
    }

    pub fn watch_queue(&self, queue: QueueKey) {
        self.managed_queues.lock().expect("poisoned").insert(queue);
    }

    pub fn register_owner(&self, now: UtcTimestamp) -> EngineResult<()> {
        self.control_plane.register_owner(&self.owner, now)
    }

    pub async fn acquire_queue(&self, queue: &QueueKey, now: UtcTimestamp) -> EngineResult<()> {
        // Read prior active owner before acquire (for restart-reconciliation with ephemeral CP).
        let prior_owner = if self.control_plane.is_ephemeral() {
            self.control_plane
                .lease(queue)
                .ok()
                .and_then(|l| l.active_owner_id)
        } else {
            None
        };

        match self
            .cp_acquire(queue.clone(), self.owner.clone(), now)
            .await?
        {
            AcquireOutcome::Acquired(lease) => {
                let current_epoch = self.backend.current_epoch(queue).await?;
                let fence_epoch = if current_epoch == lease.assignment_epoch {
                    current_epoch
                } else if current_epoch < lease.assignment_epoch {
                    self.backend.acquire_epoch(queue).await?
                } else {
                    // current_epoch > lease.assignment_epoch
                    if prior_owner.as_ref() == Some(&self.owner) {
                        // Same-owner re-affirm after a prior restart-reconciliation:
                        // use current storage epoch without re-advancing.
                        current_epoch
                    } else if self.control_plane.is_ephemeral() {
                        // Ephemeral CP reset on restart: catch up storage fence.
                        self.backend.acquire_epoch(queue).await?
                    } else {
                        return Err(EngineError::EpochFenced);
                    }
                };
                let session = OwnedSession {
                    owner: self.owner.clone(),
                    queue: queue.clone(),
                    lease_epoch: lease.assignment_epoch,
                    fence_epoch,
                };
                self.sessions
                    .lock()
                    .expect("poisoned")
                    .insert(queue.clone(), session);
                Ok(())
            }
            AcquireOutcome::Rejected(_) => Err(EngineError::Unavailable),
        }
    }

    pub async fn renew_sessions(&self, now: UtcTimestamp) -> EngineResult<()> {
        self.cp_heartbeat(self.owner.clone(), now).await?;
        let mut queues: std::collections::BTreeSet<QueueKey> = self
            .managed_queues
            .lock()
            .expect("poisoned")
            .iter()
            .cloned()
            .collect();
        queues.extend(self.sessions.lock().expect("poisoned").keys().cloned());
        for queue in queues {
            let resolution = self.cp_resolve(queue.clone(), now).await?;
            if resolution.active_owner.as_ref() == Some(&self.owner)
                && resolution
                    .target_owner
                    .as_ref()
                    .is_some_and(|target| target != &self.owner)
                && resolution.state == LeaseState::Assigned
                && let Some(active_epoch) = resolution.assignment_epoch
            {
                let _ = self
                    .cp_begin_drain(
                        queue.clone(),
                        active_epoch,
                        resolution.target_owner.as_ref().expect("checked").clone(),
                        now,
                    )
                    .await?;
            }
            let session = self.sessions.lock().expect("poisoned").get(&queue).cloned();
            match (resolution.state, resolution.active_owner.as_ref(), session) {
                (LeaseState::Assigned, Some(owner), Some(session)) if owner == &self.owner => {
                    match self
                        .cp_renew(queue.clone(), self.owner.clone(), session.lease_epoch, now)
                        .await
                    {
                        Ok(_) => {}
                        Err(EngineError::EpochFenced) => {
                            self.sessions.lock().expect("poisoned").remove(&queue);
                        }
                        Err(e) => return Err(e),
                    }
                }
                (LeaseState::Draining, Some(owner), Some(session)) if owner == &self.owner => {
                    let metrics = self.backend.metrics(&queue).await?;
                    if metrics.leased == 0 {
                        self.cp_release(
                            queue.clone(),
                            self.owner.clone(),
                            session.lease_epoch,
                            now,
                        )
                        .await?;
                        self.sessions.lock().expect("poisoned").remove(&queue);
                    } else {
                        match self
                            .cp_renew(queue.clone(), self.owner.clone(), session.lease_epoch, now)
                            .await
                        {
                            Ok(_) => {}
                            Err(EngineError::EpochFenced) => {
                                self.sessions.lock().expect("poisoned").remove(&queue);
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
                (LeaseState::Unassigned, None, _)
                    if resolution.target_owner.as_ref() == Some(&self.owner) =>
                {
                    match self.acquire_queue(&queue, now).await {
                        Ok(()) | Err(EngineError::Unavailable) => {}
                        Err(e) => return Err(e),
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn ensure_epoch(
        &self,
        queue: &QueueKey,
        now: UtcTimestamp,
        is_new_claim: bool,
    ) -> EngineResult<Option<u64>> {
        self.cp_register(self.owner.clone(), now).await?;
        let mut resolution = self.cp_resolve(queue.clone(), now).await?;
        if resolution.active_owner.as_ref() == Some(&self.owner)
            && resolution
                .target_owner
                .as_ref()
                .is_some_and(|target| target != &self.owner)
            && resolution.state == LeaseState::Assigned
            && let (Some(epoch), Some(target)) = (
                resolution.assignment_epoch,
                resolution.target_owner.as_ref(),
            )
        {
            self.cp_begin_drain(queue.clone(), epoch, target.clone(), now)
                .await?;
            resolution = self.cp_resolve(queue.clone(), now).await?;
        }
        match (resolution.state, resolution.active_owner.as_ref()) {
            (LeaseState::Assigned, Some(owner)) | (LeaseState::Draining, Some(owner))
                if owner == &self.owner =>
            {
                if resolution.state == LeaseState::Draining && is_new_claim {
                    return Err(EngineError::Unavailable);
                }
                let epoch = resolution
                    .assignment_epoch
                    .ok_or(EngineError::Unavailable)?;
                // Hot path: already own this queue → no gate, just (re)validate the cached session.
                self.establish_owned_session(queue, epoch).await
            }
            (_, Some(_)) => Err(EngineError::Unavailable),
            (LeaseState::Unassigned, None)
                if resolution.target_owner.as_ref() == Some(&self.owner) =>
            {
                // Cold start: serialize acquisition per queue (the non-idempotent epoch bump must happen at
                // most once across concurrent first-writes).
                let gate = self.acquire_gate_for(queue);
                let _g = gate.lock().await;
                // Re-resolve under the gate: a peer first-write may have acquired while we waited.
                let resolution = self.cp_resolve(queue.clone(), now).await?;
                match (resolution.state, resolution.active_owner.as_ref()) {
                    (LeaseState::Assigned, Some(owner)) | (LeaseState::Draining, Some(owner))
                        if owner == &self.owner =>
                    {
                        if resolution.state == LeaseState::Draining && is_new_claim {
                            return Err(EngineError::Unavailable);
                        }
                        let epoch = resolution
                            .assignment_epoch
                            .ok_or(EngineError::Unavailable)?;
                        self.establish_owned_session(queue, epoch).await
                    }
                    (_, Some(_)) => Err(EngineError::Unavailable),
                    (LeaseState::Unassigned, None)
                        if resolution.target_owner.as_ref() == Some(&self.owner) =>
                    {
                        self.acquire_queue(queue, now).await?;
                        let sessions = self.sessions.lock().expect("poisoned");
                        Ok(sessions.get(queue).map(|session| session.fence_epoch))
                    }
                    _ => Err(EngineError::Unavailable),
                }
            }
            _ => Err(EngineError::Unavailable),
        }
    }

    /// (Re)establish the cached fence session for a queue this owner holds at `epoch`, returning the fence
    /// epoch passed to write commands. Reuses a still-valid cached session; otherwise reads the backend's
    /// authoritative epoch and caches it (reconciling the restart gap for ephemeral control planes).
    async fn establish_owned_session(
        &self,
        queue: &QueueKey,
        epoch: u64,
    ) -> EngineResult<Option<u64>> {
        let existing = {
            let sessions = self.sessions.lock().expect("poisoned");
            sessions
                .get(queue)
                .and_then(|session| (session.lease_epoch == epoch).then_some(session.fence_epoch))
        };
        if existing.is_some() {
            return Ok(existing);
        }
        let fence_epoch = self.backend.current_epoch(queue).await?;
        let fence_epoch = if fence_epoch == epoch {
            fence_epoch
        } else if fence_epoch < epoch {
            self.backend.acquire_epoch(queue).await?
        } else if self.control_plane.is_ephemeral() {
            // Ephemeral CP reset on restart: storage epoch is ahead of CP epoch.
            // Advance storage to fence stale pre-restart writers.
            self.backend.acquire_epoch(queue).await?
        } else {
            return Err(EngineError::EpochFenced);
        };
        let session = OwnedSession {
            owner: self.owner.clone(),
            queue: queue.clone(),
            lease_epoch: epoch,
            fence_epoch,
        };
        self.sessions
            .lock()
            .expect("poisoned")
            .insert(queue.clone(), session);
        Ok(Some(fence_epoch))
    }

    fn acquire_gate_for(&self, queue: &QueueKey) -> Arc<tokio::sync::Mutex<()>> {
        self.acquire_gates
            .lock()
            .expect("poisoned")
            .entry(queue.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    async fn cp_register(&self, owner: OwnerId, now: UtcTimestamp) -> EngineResult<()> {
        let cp = self.control_plane.clone();
        blocking_control_plane(move || cp.register_owner(&owner, now)).await
    }

    async fn cp_heartbeat(&self, owner: OwnerId, now: UtcTimestamp) -> EngineResult<()> {
        let cp = self.control_plane.clone();
        blocking_control_plane(move || cp.heartbeat(&owner, now)).await
    }

    async fn cp_resolve(
        &self,
        queue: QueueKey,
        now: UtcTimestamp,
    ) -> EngineResult<pqueue_engine::OwnerResolution> {
        let cp = self.control_plane.clone();
        blocking_control_plane(move || cp.resolve_queue_owner(&queue, now)).await
    }

    async fn cp_acquire(
        &self,
        queue: QueueKey,
        owner: OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<AcquireOutcome> {
        let cp = self.control_plane.clone();
        blocking_control_plane(move || cp.acquire_queue_lease(&queue, &owner, now)).await
    }

    async fn cp_renew(
        &self,
        queue: QueueKey,
        owner: OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<pqueue_engine::QueueLease> {
        let cp = self.control_plane.clone();
        blocking_control_plane(move || cp.renew_queue_lease(&queue, &owner, expected_epoch, now))
            .await
    }

    async fn cp_begin_drain(
        &self,
        queue: QueueKey,
        expected_epoch: u64,
        target_owner: OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<pqueue_engine::QueueLease> {
        let cp = self.control_plane.clone();
        blocking_control_plane(move || cp.begin_drain(&queue, expected_epoch, &target_owner, now))
            .await
    }

    async fn cp_release(
        &self,
        queue: QueueKey,
        owner: OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        let cp = self.control_plane.clone();
        blocking_control_plane(move || cp.release_queue_lease(&queue, &owner, expected_epoch, now))
            .await
    }
}

async fn blocking_control_plane<T, F>(f: F) -> EngineResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> EngineResult<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| EngineError::Storage(format!("control-plane task failed: {e}")))?
}

impl<B, CP> RespHooks for OwnershipRuntime<B, CP>
where
    B: RespBackend,
    CP: QueueControlPlane + 'static,
{
    async fn route_command(
        &self,
        _name: &str,
        _args: &[Vec<u8>],
        routing_key: &[u8],
        now: UtcTimestamp,
        is_new_claim: bool,
    ) -> EngineResult<RouteDecision> {
        let Ok(queue) = parse_resp_queue_key(routing_key) else {
            return Ok(RouteDecision::Serve);
        };
        let auth = AuthContext::new("resp", [queue.tenant_id.as_str()]);
        if auth.authorize_tenant(queue.tenant_id.as_str()).is_err() {
            return Ok(RouteDecision::NoPerm);
        }
        self.cp_register(self.owner.clone(), now).await?;
        let resolution = self.cp_resolve(queue.clone(), now).await?;
        if resolution.state == LeaseState::Unassigned
            && resolution.target_owner.as_ref() == Some(&self.owner)
        {
            self.acquire_queue(&queue, now).await?;
            return Ok(RouteDecision::Serve);
        }
        let endpoints = self.owner_endpoints.lock().expect("poisoned").clone();
        Ok(route(
            &self.owner,
            &queue,
            routing_key,
            &auth,
            &resolution,
            |owner| endpoints.get(owner).cloned(),
            is_new_claim,
        ))
    }

    async fn expected_epoch_for_write(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        is_new_claim: bool,
    ) -> EngineResult<Option<u64>> {
        self.ensure_epoch(shard, now, is_new_claim).await
    }
}

fn parse_resp_queue_key(key: &[u8]) -> EngineResult<QueueKey> {
    let s = std::str::from_utf8(key).map_err(|_| EngineError::Invalid("non-utf8 key"))?;
    let (tenant, queue) = match s.split_once(':') {
        Some((t, q)) => (t, q),
        None => ("default", s),
    };
    Ok(QueueKey::new(
        TenantId::new(tenant).map_err(|_| EngineError::Invalid("bad tenant"))?,
        QueueId::new(queue).map_err(|_| EngineError::Invalid("bad queue"))?,
    ))
}

/// Observable counters for the background reclaim loop (so a swallowed tick error is countable, not
/// silent, and the reclaim work is surfaced for ops).
#[derive(Default)]
struct ReclaimCounters {
    ticks: AtomicU64,
    errors: AtomicU64,
    leases_reclaimed: AtomicU64,
    cohorts_expired: AtomicU64,
}

#[derive(Default)]
struct OwnershipCounters {
    ticks: AtomicU64,
    errors: AtomicU64,
}

/// A point-in-time snapshot of the reclaim loop's counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimStats {
    pub ticks: u64,
    pub errors: u64,
    pub leases_reclaimed: u64,
    pub cohorts_expired: u64,
}

/// A point-in-time snapshot of the ownership loop's counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnershipStats {
    pub ticks: u64,
    pub errors: u64,
}

/// A running server: the bound address + the two background tasks (RESP accept loop + reclaim ticker).
/// The task handles are `Option` so [`Server::shutdown_and_drain`] can take ownership to await the serve
/// task; [`Drop`] aborts whatever remains.
pub struct Server {
    addr: SocketAddr,
    serve_task: Option<JoinHandle<()>>,
    reclaim_task: Option<JoinHandle<()>>,
    ownership_task: Option<JoinHandle<()>>,
    fjord_task: Option<JoinHandle<()>>,
    /// Signals the RESP serve loop to stop accepting and drain in-flight connection handlers.
    cancel: CancellationToken,
    reclaim: Arc<ReclaimCounters>,
    ownership: Arc<OwnershipCounters>,
}

impl Server {
    /// The actually-bound listen address (resolves `:0` to the OS-assigned port).
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Liveness probe: neither background task has panicked/aborted/finished. NOTE: this is task
    /// liveness, not deep readiness — it does not prove the listener accepts or that reclaim ticks
    /// succeed. Pair with [`Server::reclaim_stats`] to detect a tick that is erroring every cycle.
    pub fn is_running(&self) -> bool {
        self.serve_task.as_ref().is_some_and(|t| !t.is_finished())
            && self.reclaim_task.as_ref().is_some_and(|t| !t.is_finished())
            && self
                .ownership_task
                .as_ref()
                .is_none_or(|t| !t.is_finished())
    }

    /// A snapshot of the background reclaim loop's counters (ticks run, tick errors, leases reclaimed,
    /// cohorts expired).
    pub fn reclaim_stats(&self) -> ReclaimStats {
        ReclaimStats {
            ticks: self.reclaim.ticks.load(Ordering::Relaxed),
            errors: self.reclaim.errors.load(Ordering::Relaxed),
            leases_reclaimed: self.reclaim.leases_reclaimed.load(Ordering::Relaxed),
            cohorts_expired: self.reclaim.cohorts_expired.load(Ordering::Relaxed),
        }
    }

    /// A snapshot of the ownership loop's counters (renew ticks and renewal/resolve errors).
    pub fn ownership_stats(&self) -> OwnershipStats {
        OwnershipStats {
            ticks: self.ownership.ticks.load(Ordering::Relaxed),
            errors: self.ownership.errors.load(Ordering::Relaxed),
        }
    }

    /// Stop serving and stop the reclaim ticker, synchronously. Signals the drain token (so the serve
    /// loop stops accepting) and then **aborts** both background tasks immediately — it does NOT wait for
    /// in-flight connection handlers to drain. Being sync, it is safe to call from [`Drop`] and from the
    /// existing non-async call sites. For a bounded graceful drain, use [`Server::shutdown_and_drain`].
    pub fn shutdown(&self) {
        self.cancel.cancel();
        if let Some(t) = &self.serve_task {
            t.abort();
        }
        if let Some(t) = &self.reclaim_task {
            t.abort();
        }
        if let Some(t) = &self.ownership_task {
            t.abort();
        }
        if let Some(t) = &self.fjord_task {
            t.abort();
        }
    }

    /// Gracefully stop: signal the serve loop to stop accepting and **drain** in-flight connection
    /// handlers (each finishes its current command, then exits), awaiting them up to `timeout`. Past the
    /// bound the serve task is aborted; because the serve loop owns the handlers in a `JoinSet`, aborting
    /// it drops the set and hard-aborts any handler still running — so the bound is real, not best-effort.
    /// The reclaim ticker is aborted (it holds no client work). Consumes the server.
    pub async fn shutdown_and_drain(mut self, timeout: Duration) {
        self.cancel.cancel();
        if let Some(mut serve) = self.serve_task.take()
            && tokio::time::timeout(timeout, &mut serve).await.is_err()
        {
            serve.abort();
        }
        if let Some(reclaim) = self.reclaim_task.take() {
            reclaim.abort();
        }
        if let Some(ownership) = self.ownership_task.take() {
            ownership.abort();
        }
        if let Some(fjord) = self.fjord_task.take() {
            fjord.abort();
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn io_err(e: std::io::Error) -> EngineError {
    EngineError::Storage(e.to_string())
}

/// Resolve a *configured* node-identity string into the 8-bit `node_id` packed into every minted `ItemId`.
/// A plain integer already in `0..=255` is used verbatim (the clean operator-assigned case); anything else
/// — an out-of-range number, a hostname, or a pod name/UID the deployment wired in — is hashed into a `u8`.
/// This keeps the application infrastructure-agnostic: the deployment decides what identity to pass, and
/// this only guarantees it lands in range. (NOTE: the hash path lives in a 256-value space, so for very
/// large fleets prefer configuring distinct small integers directly; `node_id` is defense-in-depth anyway.)
pub fn resolve_node_id(configured: &str) -> u8 {
    match configured.trim().parse::<u8>() {
        Ok(n) => n,
        Err(_) => {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            configured.trim().hash(&mut h);
            (h.finish() & 0xFF) as u8
        }
    }
}

/// Construct the configured backend + a `SystemClock`, provision the config's queues, then run the
/// server. After this returns the server is ready to serve requests against the provisioned queues.
pub async fn start(config: Config) -> EngineResult<Server> {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let node_id = config.node_id;
    // B0.1b: construct the ONE embedded fjord surface here. Its shared `Arc<dyn LogBackend>` is handed BOTH
    // to the change-record sink (in-process appends) AND — when a deployment opts into an external TCP
    // surface via `embedded_fjord.broker_listen` — to the embedded `HeimqServer`, so in-process appends are
    // immediately visible to broker fetches. No separate/discarded surface, no loopback socket for writes.
    let fjord_surface = build_embedded_fjord_surface(node_id as i32, &config.embedded_fjord);
    register_embedded_fjord_topics(&fjord_surface.topic_registry, &config.queues)?;
    let fjord_log = fjord_surface.log_backend();
    let fjord_broker_listen = config.embedded_fjord.broker_listen.clone();
    let listen = config.listen.clone();
    let interval = config.reclaim_interval;
    let queues = config.queues.clone();
    let segment_config = config.segment_config;
    let recovery_max_tail = config.recovery_max_tail;
    let debug_segments = config.debug_segments;
    let hybrid_async = config.hybrid_async;
    let deferred_flush_chunk = config.deferred_flush_chunk;
    let change_record_sink = config.change_record_sink.clone();
    let BackendSpec {
        log,
        projection,
        control_plane,
    } = config.backend;
    // Only the in-process control plane (queue definitions + placement) is wired today.
    let ControlPlaneSpec::InProcess = control_plane;
    if config.change_record_sink.enabled
        && config.queues.iter().any(|queue| queue.emit_change_records)
        && !change_record_sink_profile_is_wired(&log, &projection)
    {
        return Err(EngineError::Invalid(
            "change record sink is only wired for objectlog/hybrid, objectlog/hybrid-strict, and objectlog/hybrid-async",
        ));
    }

    // ADR-012 P2: the server selects on the two-axis [`BackendSpec`] and assembles every wired family from
    // the ONE generic recovery-capable `ComposedBackend` (the monoliths are gone). The memory family needs
    // no crash recovery; the durable sqlite/postgres families run `ComposedBackend::recover` on open. The
    // object-log families still carry their own segmented group-commit + flusher + segment-config /
    // debug-segments / recovery-tail env contract (which the per-append-seal composed `ObjectLog` axis does
    // not express), so they remain on the segmented backends until that contract is folded into the axis.
    match (log, projection) {
        (LogSpec::Memory, ProjectionSpec::InMemory) => {
            let backend = Arc::new(composed_memory_backend().with_node_id(node_id));
            run_owned(backend, node_id, clock, &listen, interval, &queues).await
        }
        (LogSpec::Sqlite { path }, ProjectionSpec::InMemory) => {
            let p = path
                .to_str()
                .ok_or_else(|| EngineError::Storage("non-utf8 path".into()))?;
            let backend = Arc::new(composed_sqlite_backend(p)?.with_node_id(node_id));
            run_owned(backend, node_id, clock, &listen, interval, &queues).await
        }
        (LogSpec::ObjectLog { root }, ProjectionSpec::InMemory) => {
            // The segmented group-commit object log (the object log's only production form) over an in-memory
            // projection rebuilt by `read_all` replay on open.
            let backend = Arc::new(
                SegmentedObjectLogInMemoryBackend::open(root, segment_config)?
                    .with_node_id(node_id),
            );
            // The flusher seals latency-due segments so a buffer below `target_bytes` still acks promptly.
            backend.spawn_flusher();
            run_owned(backend, node_id, clock, &listen, interval, &queues).await
        }
        (LogSpec::ObjectLog { root }, ProjectionSpec::Sqlite { path }) => {
            // The segmented group-commit object log driving the derived SQLite projection: concurrent pushes
            // co-buffer into one sealed segment (one durable object + one manifest-CAS + one batched SQLite
            // apply), and a reopen replays the object-log tail beyond the projection snapshot high-water.
            let p = path
                .to_str()
                .ok_or_else(|| EngineError::Storage("non-utf8 path".into()))?;
            let backend = Arc::new(
                SegmentedObjectLogSqliteBackend::open(root, p, segment_config)?
                    .with_node_id(node_id)
                    .with_recovery_max_tail(recovery_max_tail)
                    .with_debug_segments(debug_segments),
            );
            backend.spawn_flusher();
            run_owned(backend, node_id, clock, &listen, interval, &queues).await
        }
        (LogSpec::ObjectLog { root }, ProjectionSpec::Hybrid { path }) => {
            let backend = open_objectlog_hybrid_backend(
                &root,
                &path,
                segment_config,
                recovery_max_tail,
                node_id,
                deferred_flush_chunk,
                false,
                None,
            )?;
            spawn_hybrid_flusher(backend.clone(), debug_segments);
            let fjord_task = maybe_spawn_embedded_broker(
                &fjord_surface,
                fjord_broker_listen.as_deref(),
                &change_record_sink,
                &queues,
            )
            .await?;
            let _change_record_emitter =
                change_record_sink::spawn_change_record_emitter_if_enabled(
                    backend.clone(),
                    &queues,
                    &change_record_sink,
                    fjord_log.clone(),
                )?;
            run_owned_with_fjord_task(
                backend, node_id, clock, &listen, interval, &queues, fjord_task,
            )
            .await
        }
        (LogSpec::ObjectLog { root }, ProjectionSpec::HybridStrict { path }) => {
            // The `objectlog/hybrid-strict` profile (TD-004): the same object-log group-commit substrate as
            // `objectlog/hybrid`, but the projection commits every sealed batch DURABLY to SQLite BEFORE
            // applying it to hot memory (`apply_durable_then_memory`, selected by `with_strict_apply(true)`).
            // This puts the SQLite-durable-before-visible barrier and the SQLite-commit-then-memory-fail
            // poison cut on the real server write pipeline: a SQLite failure returns no success and replays
            // the object-log tail, and a poisoned store fails closed until a restart rehydrates memory from
            // the durable SQLite `ProjectionImage`.
            let backend = open_objectlog_hybrid_backend(
                &root,
                &path,
                segment_config,
                recovery_max_tail,
                node_id,
                deferred_flush_chunk,
                true,
                None,
            )?;
            spawn_hybrid_flusher(backend.clone(), debug_segments);
            let fjord_task = maybe_spawn_embedded_broker(
                &fjord_surface,
                fjord_broker_listen.as_deref(),
                &change_record_sink,
                &queues,
            )
            .await?;
            let _change_record_emitter =
                change_record_sink::spawn_change_record_emitter_if_enabled(
                    backend.clone(),
                    &queues,
                    &change_record_sink,
                    fjord_log.clone(),
                )?;
            run_owned_with_fjord_task(
                backend, node_id, clock, &listen, interval, &queues, fjord_task,
            )
            .await
        }
        (LogSpec::ObjectLog { root }, ProjectionSpec::HybridAsync { path }) => {
            // The `objectlog/hybrid-async` profile runs the same object-log + hybrid (hot-memory serving,
            // durable SQLite checkpoint) substrate as `objectlog/hybrid`; the distinction is the profile's
            // async-apply debt/backpressure/poison threshold config, validated fail-closed at config time
            // (see `Config::hybrid_async` / `HybridAsyncThresholds`). Log the resolved thresholds, then WIRE
            // them into the composed write path: the `HybridAsyncMonitor` armed inside the hybrid store
            // observes the real deferred-checkpoint backlog on every live apply / flush, so `admit_mutation`
            // gates real mutating pushes closed under Hard debt and `recovery_high_water` withholds the
            // lagging high-water until the backlog drains (TD-004:361).
            eprintln!(
                "[objectlog/hybrid-async] async-apply thresholds: lag_max_commands={} debt_max_bytes={} \
                 queue_depth_max={} oldest_unapplied_max_ms={} poison_retry_threshold={}",
                hybrid_async.apply_lag_max_commands,
                hybrid_async.apply_debt_max_bytes,
                hybrid_async.apply_queue_depth_max,
                hybrid_async.oldest_unapplied_max_ms,
                hybrid_async.apply_poison_retry_threshold,
            );
            let backend = open_objectlog_hybrid_backend(
                &root,
                &path,
                segment_config,
                recovery_max_tail,
                node_id,
                deferred_flush_chunk,
                false,
                Some(hybrid_async),
            )?;
            spawn_hybrid_flusher(backend.clone(), debug_segments);
            let fjord_task = maybe_spawn_embedded_broker(
                &fjord_surface,
                fjord_broker_listen.as_deref(),
                &change_record_sink,
                &queues,
            )
            .await?;
            let _change_record_emitter =
                change_record_sink::spawn_change_record_emitter_if_enabled(
                    backend.clone(),
                    &queues,
                    &change_record_sink,
                    fjord_log.clone(),
                )?;
            run_owned_with_fjord_task(
                backend, node_id, clock, &listen, interval, &queues, fjord_task,
            )
            .await
        }
        #[cfg(feature = "postgres")]
        (LogSpec::Postgres { url, credentials }, ProjectionSpec::InMemory) => {
            // ADR-012 P2: the composed postgres backend (`ComposedBackend<PostgresLog, InMemoryProjection,
            // InProcessControlPlane>`) — the durable postgres command log + in-memory projection, assembled
            // by the one generic composition with recovery-on-open. The sync postgres `connect` (client
            // handshake + log replay) MUST run off the reactor: the postgres client drives its own internal
            // runtime per call, so connecting on a Tokio worker would panic ("cannot start a runtime from
            // within a runtime"). Connect + recover inside `spawn_blocking`, then drive the composition only
            // through the blocking-safe `BlockingBackend` wrapper so no sync postgres call hits a reactor
            // worker.
            let backend = tokio::task::spawn_blocking(move || {
                let mut connect_config = pqueue_postgres::PostgresConnectConfig::new(url);
                if let Some(provider) = credentials {
                    connect_config = connect_config.with_credential_provider(provider);
                }
                pqueue_postgres::composed_postgres_backend_with_config(connect_config)
                    .map(|b| b.with_node_id(node_id))
            })
            .await
            .map_err(|e| {
                EngineError::Storage(format!("postgres connect task join failed: {e}"))
            })??;
            let backend = Arc::new(BlockingBackend::from_arc(Arc::new(backend)));
            run_owned(backend, node_id, clock, &listen, interval, &queues).await
        }
        #[cfg(feature = "postgres")]
        (LogSpec::Postgres { url, credentials }, ProjectionSpec::Sqlite { path }) => {
            // The composed postgres-log + sqlite-projection backend (`ComposedBackend<PostgresLog,
            // SqliteProjectionStore, InProcessControlPlane>`): the durable postgres command log paired with a
            // derived SQLite relational projection, recovery-on-open. Same off-reactor discipline as
            // postgres/inmemory above: connect BOTH axes and recover inside `spawn_blocking`, then drive the
            // composition only through the blocking-safe `BlockingBackend` wrapper.
            let p = path
                .to_str()
                .ok_or_else(|| EngineError::Storage("non-utf8 path".into()))?
                .to_string();
            let backend = tokio::task::spawn_blocking(move || {
                let mut connect_config = pqueue_postgres::PostgresConnectConfig::new(url);
                if let Some(provider) = credentials {
                    connect_config = connect_config.with_credential_provider(provider);
                }
                let log = pqueue_postgres::PostgresLog::connect_with_config(connect_config)?;
                let projection = pqueue_sqlite::SqliteProjectionStore::open(&p)?;
                ComposedBackend::new(log, projection, InProcessControlPlane::new())
                    .recover()
                    .map(|b| b.with_node_id(node_id))
            })
            .await
            .map_err(|e| {
                EngineError::Storage(format!("postgres/sqlite connect task join failed: {e}"))
            })??;
            let backend = Arc::new(BlockingBackend::from_arc(Arc::new(backend)));
            run_owned(backend, node_id, clock, &listen, interval, &queues).await
        }
        #[cfg(feature = "postgres")]
        (
            LogSpec::Postgres { url, credentials },
            ProjectionSpec::Postgres {
                url: projection_url,
            },
        ) => {
            // The composed postgres-log + postgres-projection backend (`ComposedBackend<PostgresLog,
            // PostgresRelational, InProcessControlPlane>`): the durable postgres command log paired with a
            // SEPARATE postgres connection driving the relational projection (distinct table sets, no
            // collision — see `pqueue_postgres::compose_log`'s `log_entries`/`queue_defs` vs
            // `pqueue_postgres::relational`'s `pqueue_items`/`queues`), recovery-on-open. Same off-reactor
            // discipline: connect BOTH axes and recover inside `spawn_blocking`.
            let backend = tokio::task::spawn_blocking(move || {
                let mut connect_config = pqueue_postgres::PostgresConnectConfig::new(url);
                if let Some(provider) = credentials {
                    connect_config = connect_config.with_credential_provider(provider);
                }
                let log = pqueue_postgres::PostgresLog::connect_with_config(connect_config)?;
                let projection = pqueue_postgres::PostgresRelational::connect(&projection_url)?;
                ComposedBackend::new(log, projection, InProcessControlPlane::new())
                    .recover()
                    .map(|b| b.with_node_id(node_id))
            })
            .await
            .map_err(|e| {
                EngineError::Storage(format!("postgres/postgres connect task join failed: {e}"))
            })??;
            let backend = Arc::new(BlockingBackend::from_arc(Arc::new(backend)));
            run_owned(backend, node_id, clock, &listen, interval, &queues).await
        }
        (log, projection) => Err(EngineError::Storage(format!(
            "unsupported backend composition: log={} projection={} (not wired by pqueue-server)",
            log.label(),
            projection.label()
        ))),
    }
}

/// Assemble the object-log + hybrid (hot-memory serving over a durable SQLite checkpoint image) composed
/// backend shared by the `objectlog/hybrid` and `objectlog/hybrid-async` profiles: open the group-commit
/// object log at `root` + the hybrid projection store at `path`, run the ack-after-seal group-commit write
/// path, and recover-on-open (hydrate memory from the validated SQLite image + replay the object-log tail).
// Composition-root builder: threads the object-log + hybrid-projection knobs (strict apply, async
// backpressure monitor) into one backend. The arity reflects the wiring surface, not incidental complexity.
#[allow(clippy::too_many_arguments)]
fn open_objectlog_hybrid_backend(
    root: &std::path::Path,
    path: &std::path::Path,
    segment_config: SegmentConfig,
    recovery_max_tail: u64,
    node_id: u8,
    deferred_flush_chunk: usize,
    strict: bool,
    async_monitor: Option<HybridAsyncThresholds>,
) -> EngineResult<Arc<ObjectLogHybridBackend>> {
    let p = path
        .to_str()
        .ok_or_else(|| EngineError::Storage("non-utf8 path".into()))?;
    let mut projection = HybridProjectionStore::open(p)?
        .with_deferred_flush_chunk(deferred_flush_chunk)
        .with_strict_apply(strict);
    // `objectlog/hybrid-async` ONLY: arm the TD-004 debt/backpressure/poison monitor with the operator's
    // configured thresholds so observed async-apply debt gates real mutating admission (`admit_mutation`) and
    // withholds the lagging recovery high-water (`recovery_high_water`). `objectlog/hybrid` and
    // `objectlog/hybrid-strict` pass `None` and are behaviorally unchanged (no monitor, no gating).
    if let Some(thresholds) = async_monitor {
        projection = projection.with_async_monitor(thresholds);
    }
    Ok(Arc::new(
        ComposedBackend::new(
            ObjectLog::open_group_commit(root, segment_config)?,
            projection,
            InProcessControlPlane::new(),
        )
        .with_group_commit(true)
        .with_recovery_max_tail(recovery_max_tail)
        .recover()?
        .with_node_id(node_id),
    ))
}

fn spawn_hybrid_flusher(
    backend: Arc<ObjectLogHybridBackend>,
    debug_segments: bool,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let interval_ms = backend.group_commit_flush_interval_ms();
        let mut tick = tokio::time::interval(Duration::from_millis(interval_ms));
        let mut deferred_tick = tokio::time::interval(Duration::from_millis(250));
        let mut dbg_last = std::time::Instant::now();
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    let now_ms = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
                        Ok(d) => d.as_millis().min(i64::MAX as u128) as i64,
                        Err(_) => 0,
                    };
                    if let Err(e) = backend.flush_tick(now_ms) {
                        eprintln!("[objectlog/hybrid] group-commit flush failed: {e}");
                    }
                }
                _ = deferred_tick.tick() => {
                    if let Err(e) = backend.try_flush_deferred_projection() {
                        eprintln!("[objectlog/hybrid] deferred projection flush failed: {e}");
                    }
                }
            }
            if debug_segments && dbg_last.elapsed() >= Duration::from_secs(1) {
                dbg_last = std::time::Instant::now();
                let c = backend.with_log(|log| log.counters());
                eprintln!(
                    "[seg] profile=objectlog/hybrid sealed={} commands={} mean_batch={:.1} max_batch={} objects_put={}",
                    c.segments_sealed,
                    c.commands_committed,
                    c.mean_batch_size(),
                    c.max_batch_size(),
                    c.objects_put
                );
            }
        }
    })
}

/// Wrap an already-`Arc`-shared backend in the single-node ownership runtime and run it: a per-node
/// in-memory lease control plane + a `node-{id}` owner, then [`start_with_ownership`]. The shared tail of
/// every `start` arm (each arm builds a different concrete backend type, monomorphized here).
async fn run_owned<B: RespBackend>(
    backend: Arc<B>,
    node_id: u8,
    clock: Arc<dyn Clock>,
    listen: &str,
    reclaim_interval: Duration,
    queues: &[QueueDefinition],
) -> EngineResult<Server> {
    let cp = Arc::new(InMemoryControlPlane::default());
    let owner =
        OwnerId::new(format!("node-{node_id}")).map_err(|e| EngineError::Storage(e.to_string()))?;
    start_with_ownership(backend, cp, owner, clock, listen, reclaim_interval, queues).await
}

async fn run_owned_with_fjord_task<B: RespBackend>(
    backend: Arc<B>,
    node_id: u8,
    clock: Arc<dyn Clock>,
    listen: &str,
    reclaim_interval: Duration,
    queues: &[QueueDefinition],
    fjord_task: Option<JoinHandle<()>>,
) -> EngineResult<Server> {
    let mut server = run_owned(backend, node_id, clock, listen, reclaim_interval, queues).await?;
    server.fjord_task = fjord_task;
    Ok(server)
}

/// Run the server over an already-constructed backend + clock (the generic core; tests inject a
/// controllable clock and keep a handle to the backend). `queues` are created before serving.
pub async fn start_with<B: RespBackend>(
    backend: Arc<B>,
    clock: Arc<dyn Clock>,
    listen: &str,
    reclaim_interval: Duration,
    queues: &[QueueDefinition],
) -> EngineResult<Server> {
    // Provision queues up front (idempotent create), so the wire surface — which has no create-queue
    // command — has something to serve. A definition conflict surfaces as a structured error.
    for def in queues {
        backend.create_queue(def.clone()).await?;
    }
    let listener = TcpListener::bind(listen).await.map_err(io_err)?;
    let addr = listener.local_addr().map_err(io_err)?;
    let reclaim = Arc::new(ReclaimCounters::default());
    let ownership = Arc::new(OwnershipCounters::default());
    let cancel = CancellationToken::new();
    let serve_task = tokio::spawn(serve_with_shutdown(
        listener,
        backend.clone(),
        clock.clone(),
        cancel.clone(),
    ));
    let reclaim_task = tokio::spawn(reclaim_loop(
        backend,
        clock,
        reclaim_interval,
        reclaim.clone(),
    ));
    Ok(Server {
        addr,
        serve_task: Some(serve_task),
        reclaim_task: Some(reclaim_task),
        ownership_task: None,
        fjord_task: None,
        cancel,
        reclaim,
        ownership,
    })
}

pub async fn start_with_ownership<B, CP>(
    backend: Arc<B>,
    control_plane: Arc<CP>,
    owner: OwnerId,
    clock: Arc<dyn Clock>,
    listen: &str,
    reclaim_interval: Duration,
    queues: &[QueueDefinition],
) -> EngineResult<Server>
where
    B: RespBackend,
    CP: QueueControlPlane + 'static,
{
    for def in queues {
        backend.create_queue(def.clone()).await?;
    }
    let listener = TcpListener::bind(listen).await.map_err(io_err)?;
    let addr = listener.local_addr().map_err(io_err)?;
    let endpoint = {
        let ip = addr.ip();
        let host = if ip.is_unspecified() {
            "127.0.0.1".to_string()
        } else {
            ip.to_string()
        };
        format!("{host}:{}", addr.port())
    };
    let hooks = Arc::new(OwnershipRuntime::new(
        backend.clone(),
        control_plane,
        owner,
        endpoint,
    ));
    let now = clock.now();
    hooks.register_owner(now)?;
    for def in queues {
        let queue = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        hooks.watch_queue(queue.clone());
        match hooks.acquire_queue(&queue, now).await {
            Ok(()) | Err(EngineError::Unavailable) => {}
            Err(e) => return Err(e),
        }
    }

    let reclaim = Arc::new(ReclaimCounters::default());
    let ownership = Arc::new(OwnershipCounters::default());
    let cancel = CancellationToken::new();
    let serve_task = tokio::spawn(serve_with_shutdown_and_hooks(
        listener,
        backend.clone(),
        hooks.clone(),
        clock.clone(),
        cancel.clone(),
    ));
    let reclaim_task = tokio::spawn(reclaim_loop(
        backend,
        clock.clone(),
        reclaim_interval,
        reclaim.clone(),
    ));
    let ownership_task = tokio::spawn(ownership_loop(
        hooks,
        clock,
        reclaim_interval,
        ownership.clone(),
    ));
    Ok(Server {
        addr,
        serve_task: Some(serve_task),
        reclaim_task: Some(reclaim_task),
        ownership_task: Some(ownership_task),
        fjord_task: None,
        cancel,
        reclaim,
        ownership,
    })
}

/// The background reclaim driver: every `interval`, `tick(now)` so expired leases are reclaimed without
/// any client traffic (TD-007 §3). Best-effort + idempotent (the engine's `tick` makes no transitions at
/// the same/earlier `now`). A tick error is COUNTED (not silently dropped) so a persistently-failing
/// reclaim is observable via [`Server::reclaim_stats`] rather than hiding behind a green liveness probe.
async fn reclaim_loop<B: RespBackend>(
    backend: Arc<B>,
    clock: Arc<dyn Clock>,
    interval: Duration,
    counters: Arc<ReclaimCounters>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        counters.ticks.fetch_add(1, Ordering::Relaxed);
        match backend.tick(clock.now()).await {
            Ok(report) => {
                if report.leases_reclaimed > 0 {
                    counters
                        .leases_reclaimed
                        .fetch_add(report.leases_reclaimed, Ordering::Relaxed);
                }
                if report.cohorts_expired > 0 {
                    counters
                        .cohorts_expired
                        .fetch_add(report.cohorts_expired, Ordering::Relaxed);
                }
            }
            Err(_) => {
                counters.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

async fn ownership_loop<B, CP>(
    hooks: Arc<OwnershipRuntime<B, CP>>,
    clock: Arc<dyn Clock>,
    interval: Duration,
    counters: Arc<OwnershipCounters>,
) where
    B: RespBackend,
    CP: QueueControlPlane + 'static,
{
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        counters.ticks.fetch_add(1, Ordering::Relaxed);
        if hooks.renew_sessions(clock.now()).await.is_err() {
            counters.errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}
