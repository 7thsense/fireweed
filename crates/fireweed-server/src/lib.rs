#![forbid(unsafe_code)]
//! # fireweed-server
//!
//! The **composition root**: the single place that selects a concrete backend (memory / sqlite /
//! objectlog) and wires it to the two faces of fireweed. It binds the RESP front ([`fireweed_resp::serve`])
//! and runs a **background [`ReclaimDriver`] task** that periodically `tick`s the engine so expired
//! leases are reclaimed on a *quiet* queue with no client traffic — closing the orphan-on-quiet-queue
//! gap (TD-007 §3) that the client-triggered `XAUTOCLAIM` alone leaves open.
//!
//! Hexagonal: this is the ONLY crate that names concrete adapters; everything else depends only inward.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fireweed_core::{OwnerId, QueueDefinition, QueueId, TenantId, UtcTimestamp};
use fireweed_engine::{
    AcquireOutcome, AuthContext, BufferedByteBudget, BufferedByteBudgetConfig, Clock,
    ControlPlaneConfig, EngineError, EngineResult, InMemoryControlPlane, LeaseState, OwnedSession,
    QueueControlPlane, QueueKey, assemble_async_log_replay,
};
use fireweed_memory::composed_memory_backend;
use fireweed_resp::{
    RespBackend, RespHooks, RouteDecision, SystemClock, route, serve_with_shutdown,
    serve_with_shutdown_and_hooks,
};
use fireweed_sqlite::HybridProjectionStore;
use fjord::{
    FjordClusterView, FjordGroupCoordinator, FjordLog, FjordOffsetStore, FjordTopicRegistry,
};
use heimq::config::Config as HeimqConfig;
use heimq::server::Server as HeimqServer;
use heimq_broker::storage::{ClusterView, LogBackend, OffsetStore, RecordBatchView};
// Re-exported: it is the type of the public `Config::hybrid_async` field, so composition-root callers and
// tests that construct a `Config` directly can name the async-apply threshold config.
pub use fireweed_sqlite::HybridAsyncThresholds;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

mod change_record_sink;
mod tokio_dispatcher;
pub use change_record_sink::{
    ChangeRecordSinkConfig, ChangeRecordSinkMode, FjordChangeRecordSink, NiflheimChangeRecordSink,
    emit_change_record_tick, spawn_change_record_emitter,
};
/// Segment flush knobs for object-log product cells (maps onto LogEngine FlushConfig).
pub use fireweed_objectlog::SegmentConfig;
/// Recovery-window default for object-log reopen budgets (FIREWEED_RECOVERY_MAX_TAIL_COMMANDS).
pub const DEFAULT_RECOVERY_MAX_TAIL: u64 = 1_000_000;
pub use tokio_dispatcher::TokioTaskDispatcher;

/// The single optional env-var populator for [`Config`] (`Config::from_env`) plus its [`ConfigError`]. Pure
/// over a caller-supplied env map; the only process-env read lives in the `fireweed-service` bin's `main`.
#[cfg(feature = "env-config")]
mod env_config;
#[cfg(feature = "env-config")]
pub use env_config::ConfigError;

mod postgres_native;
pub use postgres_native::{
    PostgresBlockingLifecycle, PostgresNativeBackend, PostgresWholeOperationAdapter,
    fixed_postgres_relational_pool,
};

/// The durable command-LOG axis (ADR-012): which substrate holds the command log + the co-located
/// epoch/fence authority. One half of a [`BackendSpec`].
pub enum LogSpec {
    /// In-memory reference log (atomic class; non-durable).
    Memory,
    /// Durable sqlite command log at `path` (atomic class).
    Sqlite { path: PathBuf },
    /// Segmented group-commit object log over the explicitly selected local or shared object-store profile.
    ObjectLog(ObjectLogSpec),
    /// SYNC postgres durable-log adapter (atomic class), driven through the blocking-safe
    /// [`PostgresNativeBackend`] wrapper so no sync postgres client call runs on a Tokio worker thread.
    /// `url` is a libpq/postgres connection string (URL or `key=value` DSN, with a native password); with the
    /// `tls` feature an `sslmode=require|prefer` url connects over native-tls (Lakebase / cloud postgres).
    /// `credentials` optionally injects a Databricks service-principal/PAT credential at connect (the
    /// user/password is set from the provider instead of the DSN). Requires the `postgres` cargo feature.
    #[cfg(feature = "postgres")]
    Postgres {
        url: String,
        credentials: Option<fireweed_postgres::CredentialProvider>,
    },
}

impl LogSpec {
    /// Public product axis name (orthogonal storage matrix). Object-log local/s3 map to
    /// `filesystem` / `s3`; legacy env aliases are rejected by the public adapter.
    fn label(&self) -> &'static str {
        match self {
            LogSpec::Memory => "memory",
            LogSpec::Sqlite { .. } => "sqlite",
            LogSpec::ObjectLog(ObjectLogSpec::LocalFilesystem { .. }) => "filesystem",
            LogSpec::ObjectLog(ObjectLogSpec::S3 { .. }) => "s3",
            #[cfg(feature = "postgres")]
            LogSpec::Postgres { .. } => "postgres",
        }
    }
}

/// How S3 credentials are supplied to the production object-log client.
///
/// Only explicit static credentials are wired today. The enum keeps credential acquisition typed so a
/// future workload-identity provider cannot be confused with a partially populated static pair.
#[derive(Clone, PartialEq, Eq)]
pub enum S3CredentialSource {
    Static {
        access_key_id: String,
        secret_access_key: String,
    },
}

impl std::fmt::Debug for S3CredentialSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Static { access_key_id, .. } => formatter
                .debug_struct("Static")
                .field("access_key_id", access_key_id)
                .field("secret_access_key", &"[REDACTED]")
                .finish(),
        }
    }
}

/// Typed segmented object-log configuration. The variants are the deployment boundary:
/// [`LocalFilesystem`](Self::LocalFilesystem) is supported only for a single replica/local process, while
/// [`S3`](Self::S3) is a shared store suitable for multiple replicas (subject to the separate ownership
/// lease contract). Both carry the exact segment seal settings used by the runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectLogSpec {
    LocalFilesystem {
        root: PathBuf,
        segment_config: SegmentConfig,
    },
    S3 {
        endpoint: String,
        bucket: String,
        region: String,
        credentials: S3CredentialSource,
        segment_config: SegmentConfig,
        /// Plain HTTP is rejected unless this is explicitly true. It exists for local MinIO and must not
        /// be enabled for production shared-store traffic.
        allow_insecure_http: bool,
    },
}

pub const DEFAULT_OBJECTLOG_BUFFERED_BYTES_GLOBAL: usize = 64 * 1024 * 1024;
pub const DEFAULT_OBJECTLOG_QUEUE_WAITING_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_POSTGRES_POOL_SIZE: usize = 8;
pub const MAX_POSTGRES_POOL_SIZE: usize = 64;

/// Node-wide buffered-byte limits for object-log commit composition. The optional tenant limit is uniform;
/// tenant-specific policy belongs outside the storage engine and must not create an unbounded override map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectLogByteLimits {
    pub global: usize,
    pub tenant: Option<usize>,
    pub queue_waiting: usize,
}

impl ObjectLogByteLimits {
    pub fn validate(self, segment_target_bytes: usize) -> Result<Self, &'static str> {
        if self.global == 0 {
            return Err("object-log global buffered-byte limit must be positive");
        }
        if self.queue_waiting == 0 || self.queue_waiting > self.global {
            return Err(
                "object-log queue waiting-byte limit must be positive and no larger than global",
            );
        }
        if self
            .tenant
            .is_some_and(|limit| limit == 0 || limit > self.global)
        {
            return Err(
                "object-log tenant buffered-byte limit must be positive and no larger than global",
            );
        }
        if segment_target_bytes > self.global {
            return Err("object-log segment target must not exceed the global buffered-byte limit");
        }
        Ok(self)
    }
}

impl Default for ObjectLogByteLimits {
    fn default() -> Self {
        Self {
            global: DEFAULT_OBJECTLOG_BUFFERED_BYTES_GLOBAL,
            tenant: None,
            queue_waiting: DEFAULT_OBJECTLOG_QUEUE_WAITING_BYTES,
        }
    }
}

fn build_objectlog_byte_budget(limits: ObjectLogByteLimits) -> EngineResult<BufferedByteBudget> {
    let mut config = BufferedByteBudgetConfig::new(limits.global).map_err(EngineError::Invalid)?;
    if let Some(tenant) = limits.tenant {
        config = config
            .with_uniform_tenant_limit(tenant)
            .map_err(EngineError::Invalid)?;
    }
    Ok(BufferedByteBudget::new(config))
}

impl ObjectLogSpec {
    pub fn local(root: impl Into<PathBuf>, segment_config: SegmentConfig) -> Self {
        Self::LocalFilesystem {
            root: root.into(),
            segment_config,
        }
    }

    /// Whether the selected profile is backed by a shared object store rather than node-local files.
    pub fn is_shared(&self) -> bool {
        matches!(self, Self::S3 { .. })
    }

    pub fn segment_config(&self) -> SegmentConfig {
        match self {
            Self::LocalFilesystem { segment_config, .. } | Self::S3 { segment_config, .. } => {
                *segment_config
            }
        }
    }

    /// Replace the segment seal settings while preserving the selected local/shared storage profile.
    pub fn set_segment_config(&mut self, config: SegmentConfig) {
        match self {
            Self::LocalFilesystem { segment_config, .. } | Self::S3 { segment_config, .. } => {
                *segment_config = config;
            }
        }
    }

    fn validate(&self) -> EngineResult<()> {
        self.segment_config().validate_for_production()?;
        match self {
            Self::LocalFilesystem { root, .. } => {
                if root.as_os_str().is_empty() {
                    return Err(EngineError::Invalid(
                        "local object-log root must not be empty",
                    ));
                }
            }
            Self::S3 {
                endpoint,
                bucket,
                region,
                credentials,
                allow_insecure_http,
                ..
            } => {
                if endpoint.starts_with("http://") && !allow_insecure_http {
                    return Err(EngineError::Invalid(
                        "plaintext S3 endpoint requires explicit allow_insecure_http=true (local MinIO only)",
                    ));
                }
                if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
                    return Err(EngineError::Invalid(
                        "S3 endpoint must use https:// (or explicitly allowed http:// for local MinIO)",
                    ));
                }
                if bucket.trim().is_empty() || bucket.contains('/') {
                    return Err(EngineError::Invalid(
                        "S3 bucket must be a non-empty bucket name",
                    ));
                }
                if region.trim().is_empty() {
                    return Err(EngineError::Invalid("S3 region must not be empty"));
                }
                let S3CredentialSource::Static {
                    access_key_id,
                    secret_access_key,
                } = credentials;
                if access_key_id.trim().is_empty() || secret_access_key.trim().is_empty() {
                    return Err(EngineError::Invalid(
                        "static S3 credentials require both access key id and secret access key",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// The materialized-PROJECTION axis (ADR-012): the read model the composition renders from. The other half
/// of a [`BackendSpec`].
pub enum ProjectionSpec {
    /// In-memory `ProjectionData` projection, rebuilt by log replay on open.
    InMemory,
    /// Derived relational sqlite projection (`fireweed_items` is the read model) at `path`.
    Sqlite { path: PathBuf },
    /// Native-async local Turso derived projection. Selection is accepted only by builds carrying the
    /// `turso-projection` feature and only with an object-log authority.
    Turso { path: PathBuf },
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
    /// a durable log axis ([`LogSpec::Postgres`], [`LogSpec::Sqlite`], or Class B memory log). `url` is a
    /// libpq/postgres connection string; connect + recover MUST run off the reactor (the composition root
    /// drives it through `spawn_blocking`, same as the log axis). Requires the `postgres` cargo feature.
    #[cfg(feature = "postgres")]
    Postgres { url: String },
}

impl ProjectionSpec {
    /// Public product axis name when the value is one of the three matrix projections;
    /// legacy hybrid/turso names remain for non-public implementation profiles.
    fn label(&self) -> &'static str {
        match self {
            // Public matrix name is `memory`.
            ProjectionSpec::InMemory => "memory",
            ProjectionSpec::Sqlite { .. } => "sqlite",
            ProjectionSpec::Turso { .. } => "turso",
            ProjectionSpec::Hybrid { .. } => "hybrid",
            ProjectionSpec::HybridStrict { .. } => "hybrid-strict",
            ProjectionSpec::HybridAsync { .. } => "hybrid-async",
            #[cfg(feature = "postgres")]
            ProjectionSpec::Postgres { .. } => "postgres",
        }
    }
}

type ObjectLogHybridBackend = fireweed_objectlog::AsyncObjectLogHybridBackend;

/// Object-log (LogEngine) × durable Postgres relational projection product (server matrix cell).
#[cfg(feature = "postgres")]
type ObjectLogPostgresBackend = fireweed_postgres::AsyncObjectLogPostgresBackend;

/// The queue-ownership control-plane axis. `InProcess` is an explicit single-process development profile;
/// production replicas use the shared transactional Postgres authority.
#[derive(Debug, Clone)]
pub enum ControlPlaneSpec {
    /// Development/test only. Environment parsing rejects this profile when `FIREWEED_REPLICA_COUNT > 1`.
    InProcess,
    /// Shared production membership, queue-lease, and monotonic assignment-epoch authority.
    Postgres {
        url: String,
        config: ControlPlaneConfig,
    },
}

fn change_record_sink_profile_is_wired(log: &LogSpec, projection: &ProjectionSpec) -> bool {
    matches!(
        (log, projection),
        (LogSpec::ObjectLog(_), ProjectionSpec::Hybrid { .. })
            | (LogSpec::ObjectLog(_), ProjectionSpec::HybridStrict { .. })
            | (LogSpec::ObjectLog(_), ProjectionSpec::HybridAsync { .. })
    )
}

/// Typed configuration for the embedded fjord surface that fireweed-server boots behind the composition
/// root seam. The namespace root is isolated from fireweed's own queue storage roots so the Kafka surface
/// state never shares a directory with the queue commit path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddedFjordConfig {
    pub namespace_root: PathBuf,
    pub cluster_id: String,
    /// Optional TCP listen address (`host:port` or `kafka://host:port`) for the embedded broker's
    /// EXTERNAL-consumer Kafka surface. `None` (the default) keeps the change log purely in-process: fireweed
    /// appends change records directly to the shared log and the write path binds no socket (ADR-014). Set
    /// this only when a deployment wants external Kafka consumers to read the change log over TCP; the
    /// surface then serves fetches from the SAME in-process log the sink appends to.
    pub broker_listen: Option<String>,
}

impl Default for EmbeddedFjordConfig {
    fn default() -> Self {
        Self {
            namespace_root: PathBuf::from("/var/lib/fireweed/fjord"),
            cluster_id: "fireweed-fjord".to_string(),
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
/// Because fireweed now owns the surface, topic existence is a synchronous in-process property: we create
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
/// external Kafka consumers over this TCP surface. There is no loopback socket on the write path: fireweed
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
    let handle = fireweed_resp::spawn_governed(async move {
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
/// broker-assigned `offset`, the TD-008 idempotency `key`, the `fireweed-*` `headers`, and the `ChangeRecord`
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
/// Object-log cells (`filesystem`/`s3` × memory|sqlite|hybrid|postgres) open via crates.io LogEngine
/// products ([`fireweed_objectlog::ObjectLogEngineStore`] + async projection composition). Segment seal
/// knobs on [`ObjectLogSpec`] map to [`object_log::FlushConfig`] through
/// [`fireweed_objectlog::flush_config_from_segment`]. Memory/sqlite/postgres log axes use async
/// log-replay composition.
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
/// profile renders. The DSN secret is `FIREWEED_POSTGRES_LOG_DATABASE_URL` (the chart's log-backend Secret
/// ref); `FIREWEED_PG_URL` is the local/dev fallback, and the documented default is the last resort. A
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
    let nonempty = |suffix: &str| {
        env.get(&format!("FIREWEED_{suffix}"))
            .filter(|s| !s.is_empty())
            .cloned()
    };
    let url = nonempty("POSTGRES_LOG_DATABASE_URL")
        .or_else(|| nonempty("PG_URL"))
        .unwrap_or_else(|| "postgres://postgres@127.0.0.1:5432/postgres".to_string());

    // Fail closed before connecting if the DSN requires TLS but this build cannot provide it.
    let ssl_mode = fireweed_postgres::PostgresConnectConfig::new(&url)
        .parsed_ssl_mode()
        .map_err(|e| format!("invalid postgres DSN: {e}"))?;
    #[cfg(not(feature = "tls"))]
    if matches!(ssl_mode, fireweed_postgres::PostgresSslMode::Require) {
        return Err(
            "DSN requests sslmode=require but this binary was built without the `tls` feature; rebuild \
             `--features postgres,tls` (no plaintext downgrade)"
                .to_string(),
        );
    }
    let _ = ssl_mode;

    // Databricks service-principal / PAT credential injection: present iff DATABRICKS_HOST is set. The
    // provider supersedes any DSN password (and sets the postgres user for service-principal OAuth).
    let credentials = if env
        .get("DATABRICKS_HOST")
        .is_some_and(|value| !value.is_empty())
    {
        let config = fireweed_postgres::DatabricksCredentialConfig::from_env_map(env.clone())
            .map_err(|e| format!("invalid Databricks credential configuration: {e}"))?;
        let provider = fireweed_postgres::DatabricksCredentialProvider::from_config(config)
            .map_err(|e| format!("could not build Databricks credential provider: {e}"))?;
        Some(fireweed_postgres::CredentialProvider::Databricks(provider))
    } else {
        None
    };

    Ok(LogSpec::Postgres { url, credentials })
}

/// Resolve the postgres [`ProjectionSpec`] from the runtime environment, using the env name the Helm chart's
/// `storage.projection.postgres` axis renders. The DSN secret is `FIREWEED_POSTGRES_PROJECTION_DATABASE_URL`;
/// `FIREWEED_PG_PROJECTION_URL` is the local/dev fallback, and the documented default is the last resort.
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
    let nonempty = |suffix: &str| {
        env.get(&format!("FIREWEED_{suffix}"))
            .filter(|s| !s.is_empty())
            .cloned()
    };
    let url = nonempty("POSTGRES_PROJECTION_DATABASE_URL")
        .or_else(|| nonempty("PG_PROJECTION_URL"))
        .unwrap_or_else(|| "postgres://postgres@127.0.0.1:5432/postgres".to_string());

    // Fail closed before connecting if the DSN requires TLS but this build cannot provide it.
    let ssl_mode = fireweed_postgres::PostgresConnectConfig::new(&url)
        .parsed_ssl_mode()
        .map_err(|e| format!("invalid postgres DSN: {e}"))?;
    #[cfg(not(feature = "tls"))]
    if matches!(ssl_mode, fireweed_postgres::PostgresSslMode::Require) {
        return Err(
            "DSN requests sslmode=require but this binary was built without the `tls` feature; rebuild \
             `--features postgres,tls` (no plaintext downgrade)"
                .to_string(),
        );
    }
    let _ = ssl_mode;

    Ok(ProjectionSpec::Postgres { url })
}

/// The single authoritative, fully-typed runtime configuration for a fireweed server. Every knob the server
/// needs lives here as a typed field; there is exactly ONE optional env populator (`Config::from_env`, in
/// the `fireweed-service` bin) that maps the documented `FIREWEED_*`/`DATABRICKS_*` env names onto these fields.
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
    /// Full-width identity published to the queue ownership control plane. This is deliberately
    /// independent of the 8-bit `node_id`, whose bounded namespace is suitable for item IDs but not
    /// for collision-free replica membership.
    pub owner_id: OwnerId,
    /// Listen address, e.g. `"127.0.0.1:6380"` (use `":0"` for an ephemeral port in tests).
    pub listen: String,
    /// Client-reachable RESP address advertised through the shared control plane. Multi-replica env
    /// profiles require this explicitly so an unspecified listen bind is never rewritten to loopback and
    /// published as if it were pod-reachable.
    pub advertise_addr: Option<String>,
    /// How often the background reclaim task ticks the engine.
    pub reclaim_interval: Duration,
    /// Queues to provision at startup. The RESP front has no create-queue command, so a server started
    /// with no queues here (and no out-of-band creation) would reject every request with `no such
    /// queue` — provision them up front.
    pub queues: Vec<QueueDefinition>,
    /// Recovery-window budget (max object-log tail commands) before an object-log+SQLite reopen logs a
    /// recovery-window warning. Parsed from `FIREWEED_RECOVERY_MAX_TAIL_COMMANDS` (default
    /// [`DEFAULT_RECOVERY_MAX_TAIL`]); applied by [`start`] to the objectlog+sqlite backend.
    pub recovery_max_tail: u64,
    /// Opt-in group-commit telemetry for the segmented+SQLite object-log backend (the typed form of
    /// `FIREWEED_DEBUG_SEGMENTS`).
    pub debug_segments: bool,
    /// Validated finite admission bounds shared by every object-log commit profile on this node.
    pub objectlog_byte_limits: ObjectLogByteLimits,
    /// Tokio worker-thread cap (the typed form of `FIREWEED_WORKER_THREADS`). `None` = one worker per core.
    /// Consumed by the bin when building the runtime, not by [`start`].
    pub worker_threads: Option<usize>,
    /// Fixed number of sync PostgreSQL connections owned by the `postgres/inmemory` production backend.
    /// Queue affinity multiplexes any number of queues over this bounded pool; the value never grows from
    /// queue creation or load. Parsed from `FIREWEED_POSTGRES_POOL_SIZE`.
    pub postgres_pool_size: usize,
    /// Optional path for the service binary's atomic Tokio worker/live-task gauge snapshot. `None`
    /// disables the reporter. The env-config form requires an absolute, non-empty path.
    pub runtime_resource_metrics_path: Option<std::path::PathBuf>,
    /// Per-queue bounds on `objectlog/hybrid-async` async SQLite apply debt (bead pqueue-6da52695): the
    /// hard lag/bytes/depth/age limits and the apply-retry poison threshold that drive backpressure and
    /// fail-closed poison (TD-004 §"Async apply debt, backpressure, and poison thresholds"). The typed form
    /// of the `FIREWEED_HYBRID_ASYNC_*` env names; applied by the hybrid-async projection's apply pipeline.
    pub hybrid_async: HybridAsyncThresholds,
    /// Cap on how many deferred SQLite-checkpoint commands one `objectlog/hybrid` or
    /// `objectlog/hybrid-async` deferred-flush call applies (bead pqueue-8e5e7846). `flush_deferred` runs
    /// under the composed backend's unit-of-work mutex, so bounding this bounds the worst-case time one
    /// call can block concurrent push/claim callers; the periodic flusher's 250ms cadence drains a larger
    /// backlog over several calls instead of one unbounded transaction. The typed form of
    /// `FIREWEED_HYBRID_DEFERRED_FLUSH_CHUNK`, defaulting to
    /// [`fireweed_sqlite::DEFAULT_DEFERRED_FLUSH_CHUNK`]; applied to the hybrid projection store on open.
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
        let owner_id = OwnerId::new(format!("node-{node_id}"))
            .expect("a numeric node id always forms a valid owner id");
        Self {
            backend,
            embedded_fjord: EmbeddedFjordConfig::default(),
            node_id,
            owner_id,
            listen,
            advertise_addr: None,
            reclaim_interval,
            queues,
            recovery_max_tail: DEFAULT_RECOVERY_MAX_TAIL,
            debug_segments: false,
            objectlog_byte_limits: ObjectLogByteLimits::default(),
            worker_threads: None,
            postgres_pool_size: DEFAULT_POSTGRES_POOL_SIZE,
            runtime_resource_metrics_path: None,
            hybrid_async: HybridAsyncThresholds::default(),
            deferred_flush_chunk: fireweed_sqlite::DEFAULT_DEFERRED_FLUSH_CHUNK,
            change_record_sink: ChangeRecordSinkConfig::default(),
        }
    }
}

pub struct OwnershipRuntime<B, CP: ?Sized> {
    backend: Arc<B>,
    control_plane: Arc<CP>,
    owner: OwnerId,
    endpoint: String,
    owner_endpoints: Mutex<std::collections::HashMap<OwnerId, CachedOwnerEndpoint>>,
    endpoint_refreshes: AtomicU64,
    renewal_batch_tasks: AtomicU64,
    resolution_batch_tasks: AtomicU64,
    managed_queues: Mutex<std::collections::HashSet<QueueKey>>,
    sessions: Mutex<std::collections::HashMap<QueueKey, OwnedSession>>,
    /// Per-queue gate serializing COLD-START acquisition. `acquire_queue_lease` is non-idempotent (it bumps
    /// the epoch on every call), so two concurrent first-writes to an unowned queue would each acquire,
    /// double-bumping the epoch and fencing the laggard. This gate (taken only on the unowned path, never on
    /// the hot already-owned path) lets the first acquirer win and the rest reuse its session.
    acquire_gates: Mutex<std::collections::HashMap<QueueKey, Arc<tokio::sync::Mutex<()>>>>,
    control_plane_executor: ControlPlaneExecutor,
}

const CONTROL_PLANE_RUNNING: usize = 8;
const CONTROL_PLANE_OUTSTANDING: usize = 256;

struct ControlPlaneExecutorState {
    running: Arc<tokio::sync::Semaphore>,
    outstanding: Arc<tokio::sync::Semaphore>,
    start_gate: Mutex<()>,
    closed: AtomicBool,
    started: AtomicUsize,
    drained: tokio::sync::Notify,
}

#[derive(Clone)]
struct ControlPlaneExecutor {
    state: Arc<ControlPlaneExecutorState>,
}

#[derive(Clone)]
struct ControlPlaneLifecycle {
    state: Arc<ControlPlaneExecutorState>,
}

struct StartedControlPlaneOperation {
    state: Arc<ControlPlaneExecutorState>,
}

impl Drop for StartedControlPlaneOperation {
    fn drop(&mut self) {
        if self.state.started.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.state.drained.notify_waiters();
        }
    }
}

impl ControlPlaneExecutor {
    fn new() -> Self {
        Self::with_limits(CONTROL_PLANE_RUNNING, CONTROL_PLANE_OUTSTANDING)
    }

    fn with_limits(running: usize, outstanding: usize) -> Self {
        assert!(running > 0 && outstanding >= running);
        Self {
            state: Arc::new(ControlPlaneExecutorState {
                running: Arc::new(tokio::sync::Semaphore::new(running)),
                outstanding: Arc::new(tokio::sync::Semaphore::new(outstanding)),
                start_gate: Mutex::new(()),
                closed: AtomicBool::new(false),
                started: AtomicUsize::new(0),
                drained: tokio::sync::Notify::new(),
            }),
        }
    }

    fn lifecycle(&self) -> ControlPlaneLifecycle {
        ControlPlaneLifecycle {
            state: Arc::clone(&self.state),
        }
    }

    async fn execute<T, F>(&self, operation: F) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> EngineResult<T> + Send + 'static,
    {
        if self.state.closed.load(Ordering::Acquire) {
            return Err(EngineError::Unavailable);
        }
        let outstanding = self
            .state
            .outstanding
            .clone()
            .try_acquire_owned()
            .map_err(|_| EngineError::Backpressure {
                resource: "control-plane operations",
            })?;
        let state = Arc::clone(&self.state);
        let (sender, receiver) = tokio::sync::oneshot::channel();
        // Admission transfers the operation into an owned task. Caller cancellation cannot discard an
        // accepted lease mutation; shutdown rejects queued work and drains every operation that started.
        let admitted = fireweed_resp::try_spawn_governed(async move {
            let running = match state.running.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => {
                    let _ = sender.send(Err(EngineError::Unavailable));
                    return;
                }
            };
            {
                let _start_gate = state
                    .start_gate
                    .lock()
                    .expect("control-plane start gate poisoned");
                if state.closed.load(Ordering::Acquire) {
                    let _ = sender.send(Err(EngineError::Unavailable));
                    return;
                }
                state.started.fetch_add(1, Ordering::AcqRel);
            }
            let started = StartedControlPlaneOperation {
                state: Arc::clone(&state),
            };
            let result = tokio::task::spawn_blocking(operation)
                .await
                .map_err(|error| {
                    EngineError::Storage(format!("control-plane task failed: {error}"))
                })
                .and_then(|result| result);
            drop(started);
            drop(running);
            drop(outstanding);
            let _ = sender.send(result);
        });
        if admitted.is_none() {
            return Err(EngineError::Backpressure {
                resource: "runtime tasks",
            });
        }
        receiver
            .await
            .map_err(|_| EngineError::Storage("control-plane operation responder dropped".into()))?
    }
}

impl ControlPlaneLifecycle {
    fn close(&self) {
        let _start_gate = self
            .state
            .start_gate
            .lock()
            .expect("control-plane start gate poisoned");
        self.state.closed.store(true, Ordering::Release);
        self.state.running.close();
        self.state.outstanding.close();
    }

    async fn drain_started(&self) {
        loop {
            let notified = self.state.drained.notified();
            if self.state.started.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Clone)]
struct CachedOwnerEndpoint {
    endpoint: String,
    expires_at: UtcTimestamp,
}

/// Accept only canonical, dialable IP socket addresses. The control-plane column is intentionally opaque,
/// so validation happens both before publication and after every durable read. Unspecified addresses and
/// port zero are listener bindings, not peer-reachable redirect targets.
fn validated_owner_endpoint(endpoint: &str) -> Option<String> {
    let address = endpoint.parse::<SocketAddr>().ok()?;
    if address.ip().is_unspecified() || address.port() == 0 {
        return None;
    }
    Some(address.to_string())
}

impl<B, CP: ?Sized> OwnershipRuntime<B, CP>
where
    B: RespBackend,
    CP: QueueControlPlane + 'static,
{
    pub fn new(backend: Arc<B>, control_plane: Arc<CP>, owner: OwnerId, endpoint: String) -> Self {
        Self {
            backend,
            control_plane,
            owner,
            endpoint,
            owner_endpoints: Mutex::new(std::collections::HashMap::new()),
            endpoint_refreshes: AtomicU64::new(0),
            renewal_batch_tasks: AtomicU64::new(0),
            resolution_batch_tasks: AtomicU64::new(0),
            managed_queues: Mutex::new(std::collections::HashSet::new()),
            sessions: Mutex::new(std::collections::HashMap::new()),
            acquire_gates: Mutex::new(std::collections::HashMap::new()),
            control_plane_executor: ControlPlaneExecutor::new(),
        }
    }

    pub fn owner(&self) -> &OwnerId {
        &self.owner
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Number of node-wide endpoint snapshot polls attempted by this runtime.
    pub fn endpoint_refresh_count(&self) -> u64 {
        self.endpoint_refreshes.load(Ordering::Relaxed)
    }

    pub fn ownership_batch_task_counts(&self) -> (u64, u64) {
        (
            self.renewal_batch_tasks.load(Ordering::Relaxed),
            self.resolution_batch_tasks.load(Ordering::Relaxed),
        )
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
            self.cp_lease(queue.clone())
                .await
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
                let current_epoch = match self.backend.current_epoch(queue).await {
                    Ok(epoch) => epoch,
                    Err(fence_error) => {
                        self.sessions.lock().expect("poisoned").remove(queue);
                        let _ = self
                            .cp_release(
                                queue.clone(),
                                self.owner.clone(),
                                lease.assignment_epoch,
                                now,
                            )
                            .await;
                        return Err(fence_error);
                    }
                };
                let fence_result = if current_epoch <= lease.assignment_epoch {
                    self.backend
                        .fence_epoch(queue, lease.assignment_epoch)
                        .await
                } else {
                    // current_epoch > lease.assignment_epoch
                    if prior_owner.as_ref() == Some(&self.owner) {
                        // Same-owner re-affirm after a prior restart-reconciliation:
                        // use current storage epoch without re-advancing.
                        Ok(current_epoch)
                    } else if self.control_plane.is_ephemeral() {
                        // Ephemeral CP reset on restart: catch up storage fence.
                        self.backend.acquire_epoch(queue).await
                    } else {
                        Err(EngineError::EpochFenced)
                    }
                };
                let fence_epoch = match fence_result {
                    Ok(epoch) => epoch,
                    Err(fence_error) => {
                        self.sessions.lock().expect("poisoned").remove(queue);
                        // Best-effort immediate compensation. Safety does not depend on it: a failed
                        // release leaves the durable lease PendingFence, which cannot route or renew and
                        // whose same-epoch reacquire is rejected until expiry.
                        let _ = self
                            .cp_release(
                                queue.clone(),
                                self.owner.clone(),
                                lease.assignment_epoch,
                                now,
                            )
                            .await;
                        return Err(fence_error);
                    }
                };
                // A pod-local projection may have been initialized while this node was a non-owner and
                // therefore be behind the shared log. Hydrate only after the durable epoch fence has made
                // old writers stale, but before confirming/publishing this lease as serving. Failure keeps
                // the lease non-serving and takes the same compensation path as a failed fence.
                if let Err(hydration_error) =
                    self.backend.hydrate_projection_for_ownership(queue).await
                {
                    self.sessions.lock().expect("poisoned").remove(queue);
                    let _ = self
                        .cp_release(
                            queue.clone(),
                            self.owner.clone(),
                            lease.assignment_epoch,
                            now,
                        )
                        .await;
                    return Err(hydration_error);
                }
                if lease.state == LeaseState::PendingFence
                    && let Err(confirm_error) = self
                        .cp_confirm(
                            queue.clone(),
                            self.owner.clone(),
                            lease.assignment_epoch,
                            now,
                        )
                        .await
                {
                    self.sessions.lock().expect("poisoned").remove(queue);
                    let _ = self
                        .cp_release(
                            queue.clone(),
                            self.owner.clone(),
                            lease.assignment_epoch,
                            now,
                        )
                        .await;
                    return Err(confirm_error);
                }
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
        // Endpoint refresh and lease renewal are independent node-level work. Preserve the first
        // error for the caller, but never let a discovery failure suppress healthy lease renewals.
        let mut first_error = self.advertise_and_refresh_owner_endpoints(now).await.err();
        // Renew cached sessions FIRST. Assignment polling and drain maintenance are independently fallible;
        // one unhealthy queue must never suppress every healthy queue's lease extension.
        let renewals: Vec<fireweed_engine::LeaseRenewal> = self
            .sessions
            .lock()
            .expect("poisoned")
            .values()
            .map(|session| fireweed_engine::LeaseRenewal {
                queue: session.queue.clone(),
                owner: self.owner.clone(),
                expected_epoch: session.lease_epoch,
            })
            .collect();
        if !renewals.is_empty() {
            let outcomes = self.cp_renew_batch(renewals.clone(), now).await?;
            if outcomes.len() != renewals.len() {
                return Err(EngineError::Storage(format!(
                    "control-plane batch renewal returned {} outcomes for {} inputs",
                    outcomes.len(),
                    renewals.len()
                )));
            }
            if let Some(error) = self.apply_session_renewal_outcomes(renewals, outcomes)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }

        let mut queues: std::collections::BTreeSet<QueueKey> = self
            .managed_queues
            .lock()
            .expect("poisoned")
            .iter()
            .cloned()
            .collect();
        queues.extend(self.sessions.lock().expect("poisoned").keys().cloned());
        let queues: Vec<QueueKey> = queues.into_iter().collect();
        let resolutions = self.cp_resolve_batch(queues.clone(), now).await?;
        if resolutions.len() != queues.len() {
            return Err(EngineError::Storage(format!(
                "control-plane batch resolution returned {} outcomes for {} inputs",
                resolutions.len(),
                queues.len()
            )));
        }
        for (queue, resolution) in queues.into_iter().zip(resolutions) {
            if resolution.active_owner.as_ref() == Some(&self.owner)
                && resolution
                    .target_owner
                    .as_ref()
                    .is_some_and(|target| target != &self.owner)
                && resolution.state == LeaseState::Assigned
                && let Some(active_epoch) = resolution.assignment_epoch
                && let Err(error) = self
                    .cp_begin_drain(
                        queue.clone(),
                        active_epoch,
                        resolution.target_owner.as_ref().expect("checked").clone(),
                        now,
                    )
                    .await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
            let session = self.sessions.lock().expect("poisoned").get(&queue).cloned();
            match (resolution.state, resolution.active_owner.as_ref(), session) {
                (LeaseState::Assigned, Some(owner), Some(_)) if owner == &self.owner => {}
                (LeaseState::Draining, Some(owner), Some(session)) if owner == &self.owner => {
                    let metrics = match self.backend.metrics(&queue).await {
                        Ok(metrics) => metrics,
                        Err(error) => {
                            if first_error.is_none() {
                                first_error = Some(error);
                            }
                            continue;
                        }
                    };
                    if metrics.leased == 0 {
                        if let Err(error) = self
                            .cp_release(queue.clone(), self.owner.clone(), session.lease_epoch, now)
                            .await
                        {
                            if first_error.is_none() {
                                first_error = Some(error);
                            }
                        } else {
                            self.sessions.lock().expect("poisoned").remove(&queue);
                        }
                    }
                }
                (LeaseState::Unassigned, None, _)
                    if resolution.target_owner.as_ref() == Some(&self.owner) =>
                {
                    match self.acquire_queue(&queue, now).await {
                        Ok(()) | Err(EngineError::Unavailable) => {}
                        Err(error) => {
                            if first_error.is_none() {
                                first_error = Some(error);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    fn apply_session_renewal_outcomes(
        &self,
        renewals: Vec<fireweed_engine::LeaseRenewal>,
        outcomes: Vec<fireweed_engine::LeaseRenewalOutcome>,
    ) -> Option<EngineError> {
        let mut first_error = None;
        for (renewal, outcome) in renewals.into_iter().zip(outcomes) {
            match outcome {
                fireweed_engine::LeaseRenewalOutcome::Renewed(_) => {}
                fireweed_engine::LeaseRenewalOutcome::Fenced
                | fireweed_engine::LeaseRenewalOutcome::Missing => {
                    self.sessions
                        .lock()
                        .expect("poisoned")
                        .remove(&renewal.queue);
                }
                fireweed_engine::LeaseRenewalOutcome::Error(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        first_error
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
                self.establish_owned_session(queue, epoch, now).await
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
                        self.establish_owned_session(queue, epoch, now).await
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
        now: UtcTimestamp,
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
        let current_epoch = match self.backend.current_epoch(queue).await {
            Ok(epoch) => epoch,
            Err(error) => {
                self.sessions.lock().expect("poisoned").remove(queue);
                let _ = self
                    .cp_release(queue.clone(), self.owner.clone(), epoch, now)
                    .await;
                return Err(error);
            }
        };
        let fence_result = if current_epoch == epoch {
            Ok(current_epoch)
        } else if current_epoch < epoch {
            self.backend.fence_epoch(queue, epoch).await
        } else if self.control_plane.is_ephemeral() {
            // Ephemeral CP reset on restart: storage epoch is ahead of CP epoch.
            // Advance storage to fence stale pre-restart writers.
            self.backend.acquire_epoch(queue).await
        } else {
            Err(EngineError::EpochFenced)
        };
        let fence_epoch = match fence_result {
            Ok(epoch) => epoch,
            Err(error) => {
                self.sessions.lock().expect("poisoned").remove(queue);
                let _ = self
                    .cp_release(queue.clone(), self.owner.clone(), epoch, now)
                    .await;
                return Err(error);
            }
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
        self.control_plane_executor
            .execute(move || cp.register_owner(&owner, now))
            .await
    }

    async fn cp_advertise_endpoint(
        &self,
        owner: OwnerId,
        endpoint: String,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        let endpoint = validated_owner_endpoint(&endpoint).ok_or(EngineError::Invalid(
            "owner endpoint must be a dialable IP socket address with a nonzero port",
        ))?;
        let cp = self.control_plane.clone();
        self.control_plane_executor
            .execute(move || cp.advertise_owner_endpoint(&owner, &endpoint, now))
            .await
    }

    async fn cp_live_owner_endpoints(
        &self,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<fireweed_engine::OwnerEndpointAdvertisement>> {
        let cp = self.control_plane.clone();
        self.control_plane_executor
            .execute(move || cp.live_owner_endpoints(now))
            .await
    }

    /// One advertisement write and one bounded live-owner read per node-level ownership tick. The entire
    /// cache is replaced so unknown, expired, or removed advertisements cannot survive a successful poll.
    async fn advertise_and_refresh_owner_endpoints(&self, now: UtcTimestamp) -> EngineResult<()> {
        self.endpoint_refreshes.fetch_add(1, Ordering::Relaxed);
        self.cp_advertise_endpoint(self.owner.clone(), self.endpoint.clone(), now)
            .await?;
        let advertisements = match self.cp_live_owner_endpoints(now).await {
            Ok(advertisements) => advertisements,
            Err(error) => {
                self.owner_endpoints.lock().expect("poisoned").clear();
                return Err(error);
            }
        };
        let endpoints = advertisements
            .into_iter()
            .filter(|advertisement| now < advertisement.expires_at)
            .filter_map(|advertisement| {
                validated_owner_endpoint(&advertisement.endpoint).map(|endpoint| {
                    (
                        advertisement.owner,
                        CachedOwnerEndpoint {
                            endpoint,
                            expires_at: advertisement.expires_at,
                        },
                    )
                })
            })
            .collect();
        *self.owner_endpoints.lock().expect("poisoned") = endpoints;
        Ok(())
    }

    async fn cp_resolve(
        &self,
        queue: QueueKey,
        now: UtcTimestamp,
    ) -> EngineResult<fireweed_engine::OwnerResolution> {
        let cp = self.control_plane.clone();
        self.control_plane_executor
            .execute(move || cp.resolve_queue_owner(&queue, now))
            .await
    }

    async fn cp_lease(&self, queue: QueueKey) -> EngineResult<fireweed_engine::QueueLease> {
        let cp = self.control_plane.clone();
        self.control_plane_executor
            .execute(move || cp.lease(&queue))
            .await
    }

    async fn cp_resolve_batch(
        &self,
        queues: Vec<QueueKey>,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<fireweed_engine::OwnerResolution>> {
        self.resolution_batch_tasks.fetch_add(1, Ordering::Relaxed);
        let cp = self.control_plane.clone();
        self.control_plane_executor
            .execute(move || cp.resolve_queue_owners(&queues, now))
            .await
    }

    async fn cp_acquire(
        &self,
        queue: QueueKey,
        owner: OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<AcquireOutcome> {
        let cp = self.control_plane.clone();
        self.control_plane_executor
            .execute(move || cp.acquire_queue_lease(&queue, &owner, now))
            .await
    }

    async fn cp_renew_batch(
        &self,
        renewals: Vec<fireweed_engine::LeaseRenewal>,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<fireweed_engine::LeaseRenewalOutcome>> {
        self.renewal_batch_tasks.fetch_add(1, Ordering::Relaxed);
        let cp = self.control_plane.clone();
        self.control_plane_executor
            .execute(move || cp.renew_queue_leases(&renewals, now))
            .await
    }

    async fn cp_confirm(
        &self,
        queue: QueueKey,
        owner: OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<fireweed_engine::QueueLease> {
        let cp = self.control_plane.clone();
        self.control_plane_executor
            .execute(move || cp.confirm_queue_lease_fence(&queue, &owner, expected_epoch, now))
            .await
    }

    async fn cp_begin_drain(
        &self,
        queue: QueueKey,
        expected_epoch: u64,
        target_owner: OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<fireweed_engine::QueueLease> {
        let cp = self.control_plane.clone();
        self.control_plane_executor
            .execute(move || cp.begin_drain(&queue, expected_epoch, &target_owner, now))
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
        self.control_plane_executor
            .execute(move || cp.release_queue_lease(&queue, &owner, expected_epoch, now))
            .await
    }
}

impl<B, CP: ?Sized> RespHooks for OwnershipRuntime<B, CP>
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
        if resolution.active_owner.as_ref() == Some(&self.owner)
            && matches!(
                resolution.state,
                LeaseState::Assigned | LeaseState::Draining
            )
        {
            if resolution.state == LeaseState::Draining && is_new_claim {
                return Ok(RouteDecision::Unavailable);
            }
            let epoch = resolution
                .assignment_epoch
                .ok_or(EngineError::Unavailable)?;
            self.establish_owned_session(&queue, epoch, now).await?;
        }
        let endpoints = self.owner_endpoints.lock().expect("poisoned").clone();
        Ok(route(
            &self.owner,
            &queue,
            routing_key,
            &auth,
            &resolution,
            |owner| {
                endpoints
                    .get(owner)
                    .filter(|advertisement| now < advertisement.expires_at)
                    .map(|advertisement| advertisement.endpoint.clone())
            },
            is_new_claim,
        ))
    }

    async fn expected_epoch_for_write(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        is_new_claim: bool,
    ) -> EngineResult<Option<u64>> {
        if !is_new_claim
            && let Some(epoch) = self
                .sessions
                .lock()
                .expect("poisoned")
                .get(shard)
                .map(|session| session.fence_epoch)
        {
            // RESP routing established this session from the same command's authoritative resolution.
            // Renewal removes fenced/missing sessions. New deliveries deliberately re-resolve because an
            // already-cached session can transition to Draining between commands; in-flight mutations can
            // reuse the fence while new claims must observe and refuse that transition.
            return Ok(Some(epoch));
        }
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
    /// Storage maintenance tasks (segment sealers and deferred projection flushers). Kept so shutdown
    /// cannot silently detach accepted durable work from the server lifecycle.
    maintenance_tasks: Vec<JoinHandle<()>>,
    blocking_lifecycles: Vec<PostgresBlockingLifecycle>,
    control_plane_lifecycles: Vec<ControlPlaneLifecycle>,
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
        for lifecycle in &self.blocking_lifecycles {
            lifecycle.close();
        }
        for lifecycle in &self.control_plane_lifecycles {
            lifecycle.close();
        }
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
        for task in &self.maintenance_tasks {
            task.abort();
        }
    }

    /// Gracefully stop: signal the serve loop to stop accepting and **drain** in-flight connection
    /// handlers (each finishes its current command, then exits), awaiting them up to `timeout`. Past the
    /// bound the serve task is aborted; because the serve loop owns the handlers in a `JoinSet`, aborting
    /// it drops the set and hard-aborts any handler still running — so the bound is real, not best-effort.
    /// The reclaim ticker is aborted (it holds no client work). Consumes the server.
    pub async fn shutdown_and_drain(mut self, timeout: Duration) {
        for lifecycle in &self.blocking_lifecycles {
            lifecycle.close();
        }
        for lifecycle in &self.control_plane_lifecycles {
            lifecycle.close();
        }
        self.cancel.cancel();
        if let Some(mut serve) = self.serve_task.take()
            && tokio::time::timeout(timeout, &mut serve).await.is_err()
        {
            serve.abort();
        }
        if let Some(reclaim) = self.reclaim_task.take() {
            reclaim.abort();
            let _ = reclaim.await;
        }
        if let Some(ownership) = self.ownership_task.take() {
            ownership.abort();
            let _ = ownership.await;
        }
        if let Some(fjord) = self.fjord_task.take() {
            fjord.abort();
            let _ = fjord.await;
        }
        // Segment sealers remain live until every accepted mutation has crossed its response barrier.
        // Otherwise a graceful shutdown could abort the only task capable of resolving a started push.
        for lifecycle in &self.blocking_lifecycles {
            let _ = tokio::time::timeout(timeout, lifecycle.drain_started()).await;
        }
        for lifecycle in &self.control_plane_lifecycles {
            let _ = tokio::time::timeout(timeout, lifecycle.drain_started()).await;
        }
        for task in self.maintenance_tasks.drain(..) {
            task.abort();
            let _ = task.await;
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
    if !(1..=MAX_POSTGRES_POOL_SIZE).contains(&config.postgres_pool_size) {
        return Err(EngineError::Invalid(
            "postgres pool size must be between 1 and 64",
        ));
    }
    if let LogSpec::ObjectLog(spec) = &config.backend.log {
        spec.validate()?;
        config
            .objectlog_byte_limits
            .validate(spec.segment_config().target_bytes)
            .map_err(EngineError::Invalid)?;
    }
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let node_id = config.node_id;
    let owner_id = config.owner_id.clone();
    // B0.1b: construct the ONE embedded fjord surface here. Its shared `Arc<dyn LogBackend>` is handed BOTH
    // to the change-record sink (in-process appends) AND — when a deployment opts into an external TCP
    // surface via `embedded_fjord.broker_listen` — to the embedded `HeimqServer`, so in-process appends are
    // immediately visible to broker fetches. No separate/discarded surface, no loopback socket for writes.
    let fjord_surface = build_embedded_fjord_surface(node_id as i32, &config.embedded_fjord);
    register_embedded_fjord_topics(&fjord_surface.topic_registry, &config.queues)?;
    let fjord_log = fjord_surface.log_backend();
    let fjord_broker_listen = config.embedded_fjord.broker_listen.clone();
    let listen = config.listen.clone();
    let advertise_addr = config.advertise_addr.clone();
    let interval = config.reclaim_interval;
    let queues = config.queues.clone();
    let recovery_max_tail = config.recovery_max_tail;
    let debug_segments = config.debug_segments;
    let hybrid_async = config.hybrid_async;
    let deferred_flush_chunk = config.deferred_flush_chunk;
    let config_objectlog_queue_limit = config.objectlog_byte_limits.queue_waiting;
    let objectlog_byte_budget = build_objectlog_byte_budget(config.objectlog_byte_limits)?;
    let change_record_sink = config.change_record_sink.clone();
    #[cfg(feature = "postgres")]
    let postgres_pool_size = config.postgres_pool_size;
    let BackendSpec {
        log,
        projection,
        control_plane,
    } = config.backend;
    // The sync Postgres client owns an internal runtime, so connect off the Tokio reactor. Erase the
    // concrete implementation only after construction; every backend arm receives this same selected
    // queue-ownership authority instead of manufacturing a private per-process control plane.
    let control_plane: Arc<dyn QueueControlPlane> = match control_plane {
        ControlPlaneSpec::InProcess => Arc::new(InMemoryControlPlane::default()),
        ControlPlaneSpec::Postgres { url, config } => Arc::new(
            tokio::task::spawn_blocking(move || {
                fireweed_postgres::PostgresControlPlane::connect(&url, config)
            })
            .await
            .map_err(|e| {
                EngineError::Storage(format!("postgres control-plane connect task failed: {e}"))
            })??,
        ),
    };
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
            run_owned(
                backend,
                control_plane,
                advertise_addr.as_deref(),
                owner_id.clone(),
                clock,
                &listen,
                interval,
                &queues,
            )
            .await
        }
        (LogSpec::Memory, ProjectionSpec::Sqlite { path }) => {
            // Class B (ADR-013): in-process MemoryLog for ordering while alive × durable SQLite
            // projection. Reopen/recovery is projection-only — no Class A log-replay claims.
            // Async product assemble + recover: MemoryLog is non-durable, so recover walks an empty
            // log catalog (no-op). The durable projection image is the reopen source of truth.
            let p = path
                .into_os_string()
                .into_string()
                .map_err(|_| EngineError::Storage("non-utf8 path".into()))?;
            let backend = tokio::task::spawn_blocking(move || {
                let log = fireweed_projection::MemoryLog::new();
                let projection = fireweed_sqlite::SqliteProjectionStore::open(&p)?;
                assemble_async_log_replay(log, projection, node_id)?.recover()
            })
            .await
            .map_err(|e| EngineError::Storage(format!("memory/sqlite open task failed: {e}")))??;
            // Single-member pool: SQLite projection is blocking-safe via whole-operation adapter.
            let (backend, lifecycle) = blocking_backend_pool(vec![Arc::new(backend)]);
            run_owned_with_blocking_lifecycle(
                backend,
                lifecycle,
                control_plane,
                advertise_addr.as_deref(),
                owner_id.clone(),
                clock,
                &listen,
                interval,
                &queues,
            )
            .await
        }
        #[cfg(feature = "postgres")]
        (LogSpec::Memory, ProjectionSpec::Postgres { url }) => {
            // Class B (ADR-013): in-process MemoryLog × durable postgres relational projection.
            // Connect + assemble off-reactor (sync postgres client must not run on a Tokio worker).
            // Reopen is projection-only; no Class A log-rebuild claims. MemoryLog recover is a no-op.
            let backend = tokio::task::spawn_blocking(move || {
                let log = fireweed_projection::MemoryLog::new();
                let projection = fireweed_postgres::PostgresRelational::connect(&url)?;
                assemble_async_log_replay(log, projection, node_id)?.recover()
            })
            .await
            .map_err(|e| {
                EngineError::Storage(format!("memory/postgres connect task join failed: {e}"))
            })??;
            let (backend, lifecycle) = blocking_backend(Arc::new(backend));
            run_owned_with_blocking_lifecycle(
                backend,
                lifecycle,
                control_plane,
                advertise_addr.as_deref(),
                owner_id.clone(),
                clock,
                &listen,
                interval,
                &queues,
            )
            .await
        }
        (LogSpec::Sqlite { path }, ProjectionSpec::InMemory) => {
            let p = path
                .into_os_string()
                .into_string()
                .map_err(|_| EngineError::Storage("non-utf8 path".into()))?;
            let backends = tokio::task::spawn_blocking(move || {
                (0..8)
                    .map(|index| {
                        fireweed_sqlite::composed_sqlite_backend_for_worker(&p, index, 8)
                            .map(|backend| backend.with_node_id(node_id))
                    })
                    .collect::<EngineResult<Vec<_>>>()
            })
            .await
            .map_err(|e| EngineError::Storage(format!("sqlite open task failed: {e}")))??;
            let (backend, lifecycle) =
                blocking_backend_pool(backends.into_iter().map(Arc::new).collect());
            run_owned_with_blocking_lifecycle(
                backend,
                lifecycle,
                control_plane,
                advertise_addr.as_deref(),
                owner_id.clone(),
                clock,
                &listen,
                interval,
                &queues,
            )
            .await
        }
        (
            LogSpec::Sqlite { path },
            ProjectionSpec::Sqlite {
                path: projection_path,
            },
        ) => {
            // Class A: durable sqlite command LOG × derived sqlite PROJECTION at distinct paths.
            // Uses the adapter `composed_sqlite_log_sqlite_projection` (snapshot-tail recovery on open).
            // Same off-reactor + whole-operation adapter discipline as sqlite/inmemory.
            let log_p = path
                .into_os_string()
                .into_string()
                .map_err(|_| EngineError::Storage("non-utf8 sqlite log path".into()))?;
            let proj_p = projection_path
                .into_os_string()
                .into_string()
                .map_err(|_| EngineError::Storage("non-utf8 sqlite projection path".into()))?;
            if log_p == proj_p {
                return Err(EngineError::Invalid(
                    "sqlite/sqlite requires distinct log and projection paths \
                     (FIREWEED_SQLITE_LOG_PATH ≠ FIREWEED_SQLITE_PROJECTION_PATH)",
                ));
            }
            let backend = tokio::task::spawn_blocking(move || {
                fireweed_sqlite::composed_sqlite_log_sqlite_projection(&log_p, &proj_p)
                    .map(|b| b.with_node_id(node_id))
            })
            .await
            .map_err(|e| EngineError::Storage(format!("sqlite/sqlite open task failed: {e}")))??;
            // Single-member pool: whole-operation adapter is always available (unlike
            // `blocking_backend`, which is gated on the `postgres` feature for historical reasons).
            let (backend, lifecycle) = blocking_backend_pool(vec![Arc::new(backend)]);
            run_owned_with_blocking_lifecycle(
                backend,
                lifecycle,
                control_plane,
                advertise_addr.as_deref(),
                owner_id.clone(),
                clock,
                &listen,
                interval,
                &queues,
            )
            .await
        }
        #[cfg(feature = "postgres")]
        (LogSpec::Sqlite { path }, ProjectionSpec::Postgres { url }) => {
            // Class A: durable sqlite command LOG × derived postgres relational PROJECTION.
            // Distinct stores: sqlite log path vs postgres projection URL. Connect + recover off-reactor
            // (sync postgres client must not run on a Tokio worker). Async log-replay product
            // (`assemble_async_log_replay`) replaces the retired sync dual-stack open.
            let log_p = path
                .to_str()
                .ok_or_else(|| EngineError::Storage("non-utf8 sqlite log path".into()))?
                .to_string();
            let backend = tokio::task::spawn_blocking(move || {
                let log = fireweed_sqlite::SqliteLog::open(&log_p)?;
                let projection = fireweed_postgres::PostgresRelational::connect(&url)?;
                assemble_async_log_replay(log, projection, node_id)?.recover()
            })
            .await
            .map_err(|e| {
                EngineError::Storage(format!("sqlite/postgres connect task join failed: {e}"))
            })??;
            let (backend, lifecycle) = blocking_backend(Arc::new(backend));
            run_owned_with_blocking_lifecycle(
                backend,
                lifecycle,
                control_plane,
                advertise_addr.as_deref(),
                owner_id.clone(),
                clock,
                &listen,
                interval,
                &queues,
            )
            .await
        }
        (LogSpec::ObjectLog(spec), ProjectionSpec::InMemory) => {
            // Program A: crates.io object-log LogEngine × in-memory projection (async composition).
            // LogEngine owns group-commit flush; no segmented flusher task.
            let _ = (
                objectlog_byte_budget,
                config_objectlog_queue_limit,
                debug_segments,
            );
            let segment = spec.segment_config();
            let flush = fireweed_objectlog::flush_config_from_segment(
                segment.target_bytes,
                segment.max_latency_ms,
            );
            let backend = match spec {
                ObjectLogSpec::LocalFilesystem { root, .. } => {
                    fireweed_objectlog::AsyncObjectLogMemoryBackend::open_local_with_node_id(
                        root, flush, node_id,
                    )
                    .await?
                }
                ObjectLogSpec::S3 {
                    endpoint,
                    bucket,
                    region,
                    credentials:
                        S3CredentialSource::Static {
                            access_key_id,
                            secret_access_key,
                        },
                    ..
                } => {
                    let log = fireweed_objectlog::ObjectLogEngineStore::open_s3(
                        &endpoint,
                        &region,
                        &bucket,
                        &access_key_id,
                        &secret_access_key,
                        flush,
                    )
                    .await?;
                    fireweed_objectlog::AsyncObjectLogMemoryBackend::from_log_store(log, node_id)
                        .await?
                }
            };
            let backend = Arc::new(backend);
            run_owned(
                backend,
                control_plane,
                advertise_addr.as_deref(),
                owner_id.clone(),
                clock,
                &listen,
                interval,
                &queues,
            )
            .await
        }
        (LogSpec::ObjectLog(spec), ProjectionSpec::Sqlite { path }) => {
            // Program A: LogEngine × durable sqlite projection (async composition).
            let _ = (
                objectlog_byte_budget,
                config_objectlog_queue_limit,
                debug_segments,
                recovery_max_tail,
            );
            let p = path
                .into_os_string()
                .into_string()
                .map_err(|_| EngineError::Storage("non-utf8 path".into()))?;
            let segment = spec.segment_config();
            let flush = fireweed_objectlog::flush_config_from_segment(
                segment.target_bytes,
                segment.max_latency_ms,
            );
            let backend = match spec {
                ObjectLogSpec::LocalFilesystem { root, .. } => {
                    fireweed_objectlog::AsyncObjectLogSqliteBackend::open(root, &p, flush, node_id)
                        .await?
                }
                ObjectLogSpec::S3 {
                    endpoint,
                    bucket,
                    region,
                    credentials:
                        S3CredentialSource::Static {
                            access_key_id,
                            secret_access_key,
                        },
                    ..
                } => {
                    let log = fireweed_objectlog::ObjectLogEngineStore::open_s3(
                        &endpoint,
                        &region,
                        &bucket,
                        &access_key_id,
                        &secret_access_key,
                        flush,
                    )
                    .await?;
                    let projection = fireweed_sqlite::SqliteProjectionStore::open(&p)?;
                    fireweed_objectlog::AsyncObjectLogSqliteBackend::from_log_and_projection(
                        log, projection, node_id,
                    )
                    .await?
                }
            };
            run_owned(
                Arc::new(backend),
                control_plane,
                advertise_addr.as_deref(),
                owner_id.clone(),
                clock,
                &listen,
                interval,
                &queues,
            )
            .await
        }
        (LogSpec::ObjectLog(_), ProjectionSpec::Turso { .. }) => Err(EngineError::Invalid(
            "FIREWEED_PROJECTION_BACKEND=turso is not supported after LogEngine cutover; use memory|sqlite|hybrid|postgres projections",
        )),
        (LogSpec::ObjectLog(spec), ProjectionSpec::Hybrid { path }) => {
            // Program A: LogEngine × hybrid projection (async product). LogEngine owns group-commit
            // flush; the maintenance task only drains deferred SQLite checkpoint work.
            let _ = (
                objectlog_byte_budget,
                config_objectlog_queue_limit,
                recovery_max_tail,
            );
            let backend = open_objectlog_hybrid_backend(
                spec,
                &path,
                node_id,
                deferred_flush_chunk,
                false,
                None,
            )
            .await?;
            let flusher = spawn_hybrid_flusher(&backend, debug_segments);
            let fjord_task = maybe_spawn_embedded_broker(
                &fjord_surface,
                fjord_broker_listen.as_deref(),
                &change_record_sink,
                &queues,
            )
            .await?;
            let emitter_backend = Arc::clone(&backend);
            let mut server = run_owned_with_fjord_task(
                backend,
                control_plane,
                advertise_addr.as_deref(),
                owner_id.clone(),
                clock,
                &listen,
                interval,
                &queues,
                fjord_task,
            )
            .await?;
            server.maintenance_tasks.push(flusher);
            let change_record_emitter = change_record_sink::spawn_change_record_emitter_if_enabled(
                emitter_backend,
                &queues,
                &change_record_sink,
                fjord_log.clone(),
            )?;
            if let Some(task) = change_record_emitter {
                server.maintenance_tasks.push(task);
            }
            Ok(server)
        }
        (LogSpec::ObjectLog(spec), ProjectionSpec::HybridStrict { path }) => {
            // The `objectlog/hybrid-strict` profile (TD-004): LogEngine × hybrid with strict
            // durable-SQLite-before-hot-memory apply (`with_strict_apply(true)`).
            let _ = (
                objectlog_byte_budget,
                config_objectlog_queue_limit,
                recovery_max_tail,
            );
            let backend = open_objectlog_hybrid_backend(
                spec,
                &path,
                node_id,
                deferred_flush_chunk,
                true,
                None,
            )
            .await?;
            let flusher = spawn_hybrid_flusher(&backend, debug_segments);
            let fjord_task = maybe_spawn_embedded_broker(
                &fjord_surface,
                fjord_broker_listen.as_deref(),
                &change_record_sink,
                &queues,
            )
            .await?;
            let emitter_backend = Arc::clone(&backend);
            let mut server = run_owned_with_fjord_task(
                backend,
                control_plane,
                advertise_addr.as_deref(),
                owner_id.clone(),
                clock,
                &listen,
                interval,
                &queues,
                fjord_task,
            )
            .await?;
            server.maintenance_tasks.push(flusher);
            let change_record_emitter = change_record_sink::spawn_change_record_emitter_if_enabled(
                emitter_backend,
                &queues,
                &change_record_sink,
                fjord_log.clone(),
            )?;
            if let Some(task) = change_record_emitter {
                server.maintenance_tasks.push(task);
            }
            Ok(server)
        }
        (LogSpec::ObjectLog(spec), ProjectionSpec::HybridAsync { path }) => {
            // The `objectlog/hybrid-async` profile: LogEngine × hybrid with async-apply debt monitor.
            eprintln!(
                "[objectlog/hybrid-async] async-apply thresholds: lag_max_commands={} debt_max_bytes={} \
                 queue_depth_max={} oldest_unapplied_max_ms={} poison_retry_threshold={}",
                hybrid_async.apply_lag_max_commands,
                hybrid_async.apply_debt_max_bytes,
                hybrid_async.apply_queue_depth_max,
                hybrid_async.oldest_unapplied_max_ms,
                hybrid_async.apply_poison_retry_threshold,
            );
            let _ = (
                objectlog_byte_budget,
                config_objectlog_queue_limit,
                recovery_max_tail,
            );
            let backend = open_objectlog_hybrid_backend(
                spec,
                &path,
                node_id,
                deferred_flush_chunk,
                false,
                Some(hybrid_async),
            )
            .await?;
            let flusher = spawn_hybrid_flusher(&backend, debug_segments);
            let fjord_task = maybe_spawn_embedded_broker(
                &fjord_surface,
                fjord_broker_listen.as_deref(),
                &change_record_sink,
                &queues,
            )
            .await?;
            let emitter_backend = Arc::clone(&backend);
            let mut server = run_owned_with_fjord_task(
                backend,
                control_plane,
                advertise_addr.as_deref(),
                owner_id.clone(),
                clock,
                &listen,
                interval,
                &queues,
                fjord_task,
            )
            .await?;
            server.maintenance_tasks.push(flusher);
            let change_record_emitter = change_record_sink::spawn_change_record_emitter_if_enabled(
                emitter_backend,
                &queues,
                &change_record_sink,
                fjord_log.clone(),
            )?;
            if let Some(task) = change_record_emitter {
                server.maintenance_tasks.push(task);
            }
            Ok(server)
        }
        #[cfg(feature = "postgres")]
        (LogSpec::ObjectLog(spec), ProjectionSpec::Postgres { url }) => {
            // Class A: filesystem|s3 LogEngine × durable Postgres projection (async product).
            // Sync Postgres client work is confined inside projection applies; open uses spawn_blocking
            // for the connect half via the product factory. LogEngine owns flush (no segmented flusher).
            let _ = (
                objectlog_byte_budget,
                config_objectlog_queue_limit,
                recovery_max_tail,
                debug_segments,
            );
            let backend = open_objectlog_postgres_backend(spec, &url, node_id).await?;
            let (backend, lifecycle) = blocking_backend(backend);
            run_owned_with_blocking_lifecycle(
                backend,
                lifecycle,
                control_plane,
                advertise_addr.as_deref(),
                owner_id.clone(),
                clock,
                &listen,
                interval,
                &queues,
            )
            .await
        }
        #[cfg(feature = "postgres")]
        (LogSpec::Postgres { url, credentials }, ProjectionSpec::InMemory) => {
            // ADR-015: one production wrapper owns a configured fixed pool of recovered composed postgres
            // workers. Stable queue affinity keeps a queue's projection, ordering gate, and complete SQL
            // transaction on one connection while unrelated queues use other pool members. Pool size is
            // independent of queue count. Sync connect/recovery and every complete operation stay off the
            // reactor because postgres' sync client drives an internal runtime per call.
            let backends = tokio::task::spawn_blocking(move || {
                let mut connect_config = fireweed_postgres::PostgresConnectConfig::new(url);
                if let Some(provider) = credentials {
                    connect_config = connect_config.with_credential_provider(provider);
                }
                (0..postgres_pool_size)
                    .map(|index| {
                        fireweed_postgres::composed_postgres_backend_for_worker_with_config(
                            connect_config.clone(),
                            index,
                            postgres_pool_size,
                        )
                        .map(|backend| backend.with_node_id(node_id))
                    })
                    .collect::<EngineResult<Vec<_>>>()
            })
            .await
            .map_err(|e| {
                EngineError::Storage(format!("postgres connect task join failed: {e}"))
            })??;
            let (backend, lifecycle) =
                blocking_backend_pool(backends.into_iter().map(Arc::new).collect());
            run_owned_with_blocking_lifecycle(
                backend,
                lifecycle,
                control_plane,
                advertise_addr.as_deref(),
                owner_id.clone(),
                clock,
                &listen,
                interval,
                &queues,
            )
            .await
        }
        #[cfg(feature = "postgres")]
        (LogSpec::Postgres { url, credentials }, ProjectionSpec::Sqlite { path }) => {
            // Class A: durable postgres command log × derived SQLite relational projection.
            // Same off-reactor discipline as postgres/inmemory: connect BOTH axes and recover inside
            // `spawn_blocking`, then drive the composition through the whole-operation adapter.
            // Async log-replay product replaces the retired sync dual-stack open.
            let p = path
                .to_str()
                .ok_or_else(|| EngineError::Storage("non-utf8 path".into()))?
                .to_string();
            let backend = tokio::task::spawn_blocking(move || {
                let mut connect_config = fireweed_postgres::PostgresConnectConfig::new(url);
                if let Some(provider) = credentials {
                    connect_config = connect_config.with_credential_provider(provider);
                }
                let log = fireweed_postgres::PostgresLog::connect_with_config(connect_config)?;
                let projection = fireweed_sqlite::SqliteProjectionStore::open(&p)?;
                assemble_async_log_replay(log, projection, node_id)?.recover()
            })
            .await
            .map_err(|e| {
                EngineError::Storage(format!("postgres/sqlite connect task join failed: {e}"))
            })??;
            let (backend, lifecycle) = blocking_backend(Arc::new(backend));
            run_owned_with_blocking_lifecycle(
                backend,
                lifecycle,
                control_plane,
                advertise_addr.as_deref(),
                owner_id.clone(),
                clock,
                &listen,
                interval,
                &queues,
            )
            .await
        }
        #[cfg(feature = "postgres")]
        (
            LogSpec::Postgres { url, credentials },
            ProjectionSpec::Postgres {
                url: projection_url,
            },
        ) => {
            // TD-002: the selectable postgres/postgres profile is the unified relational backend. Every
            // command envelope and projection mutation commits in the same database transaction. A fixed
            // pool gives stable queue affinity: one hot queue is serialized on one member, while unrelated
            // queues make progress on other members. Distinct log/projection URLs cannot provide this
            // atomic boundary and are rejected rather than silently selecting the legacy split store.
            if url != projection_url {
                return Err(EngineError::Invalid(
                    "postgres/postgres atomic mode requires identical log and projection URLs",
                ));
            }
            let backend = tokio::task::spawn_blocking(move || {
                let mut connect_config = fireweed_postgres::PostgresConnectConfig::new(url);
                if let Some(provider) = credentials {
                    connect_config = connect_config.with_credential_provider(provider);
                }
                fixed_postgres_relational_pool(connect_config, None, postgres_pool_size, node_id)
            })
            .await
            .map_err(|e| {
                EngineError::Storage(format!("postgres/postgres connect task join failed: {e}"))
            })??;
            let lifecycle = backend.lifecycle();
            run_owned_with_blocking_lifecycle(
                backend,
                lifecycle,
                control_plane,
                advertise_addr.as_deref(),
                owner_id.clone(),
                clock,
                &listen,
                interval,
                &queues,
            )
            .await
        }
        (log, projection) => Err(EngineError::Storage(format!(
            "unsupported backend composition: log={} projection={} (not wired by fireweed-server)",
            log.label(),
            projection.label()
        ))),
    }
}

/// Open LogEngine × hybrid projection for `objectlog/hybrid{,-strict,-async}` product cells.
async fn open_objectlog_hybrid_backend(
    spec: ObjectLogSpec,
    path: &std::path::Path,
    node_id: u8,
    deferred_flush_chunk: usize,
    strict: bool,
    async_monitor: Option<HybridAsyncThresholds>,
) -> EngineResult<Arc<ObjectLogHybridBackend>> {
    let p = path
        .to_str()
        .ok_or_else(|| EngineError::Storage("non-utf8 path".into()))?
        .to_string();
    let segment = spec.segment_config();
    let flush =
        fireweed_objectlog::flush_config_from_segment(segment.target_bytes, segment.max_latency_ms);
    let hybrid = fireweed_objectlog::HybridProductConfig {
        deferred_flush_chunk,
        strict,
        async_monitor,
    };
    let backend = match spec {
        ObjectLogSpec::LocalFilesystem { root, .. } => {
            fireweed_objectlog::AsyncObjectLogHybridBackend::open(root, &p, flush, node_id, hybrid)
                .await?
        }
        ObjectLogSpec::S3 {
            endpoint,
            bucket,
            region,
            credentials:
                S3CredentialSource::Static {
                    access_key_id,
                    secret_access_key,
                },
            ..
        } => {
            let log = fireweed_objectlog::ObjectLogEngineStore::open_s3(
                &endpoint,
                &region,
                &bucket,
                &access_key_id,
                &secret_access_key,
                flush,
            )
            .await?;
            let mut projection = HybridProjectionStore::open(&p)?
                .with_deferred_flush_chunk(hybrid.deferred_flush_chunk)
                .with_strict_apply(hybrid.strict);
            if let Some(thresholds) = hybrid.async_monitor {
                projection = projection.with_async_monitor(thresholds);
            }
            fireweed_objectlog::AsyncObjectLogHybridBackend::from_log_and_projection(
                log, projection, node_id,
            )
            .await?
        }
    };
    Ok(Arc::new(backend))
}

/// Open LogEngine × Postgres relational projection for the objectlog/postgres product cell.
#[cfg(feature = "postgres")]
async fn open_objectlog_postgres_backend(
    spec: ObjectLogSpec,
    projection_url: &str,
    node_id: u8,
) -> EngineResult<Arc<ObjectLogPostgresBackend>> {
    let segment = spec.segment_config();
    let flush =
        fireweed_objectlog::flush_config_from_segment(segment.target_bytes, segment.max_latency_ms);
    let url = projection_url.to_string();
    let backend = match spec {
        ObjectLogSpec::LocalFilesystem { root, .. } => {
            let log = fireweed_objectlog::ObjectLogEngineStore::open_local(root, flush).await?;
            let projection =
                fireweed_postgres::AsyncPostgresRelationalProjection::connect(&url).await?;
            fireweed_postgres::AsyncObjectLogPostgresBackend::from_log_and_projection(
                log, projection, node_id,
            )
            .await?
        }
        ObjectLogSpec::S3 {
            endpoint,
            bucket,
            region,
            credentials:
                S3CredentialSource::Static {
                    access_key_id,
                    secret_access_key,
                },
            ..
        } => {
            let log = fireweed_objectlog::ObjectLogEngineStore::open_s3(
                &endpoint,
                &region,
                &bucket,
                &access_key_id,
                &secret_access_key,
                flush,
            )
            .await?;
            let projection =
                fireweed_postgres::AsyncPostgresRelationalProjection::connect(&url).await?;
            fireweed_postgres::AsyncObjectLogPostgresBackend::from_log_and_projection(
                log, projection, node_id,
            )
            .await?
        }
    };
    Ok(Arc::new(backend))
}

/// Drain deferred hybrid SQLite checkpoint work. LogEngine owns segment flush internally.
fn spawn_hybrid_flusher(
    backend: &Arc<ObjectLogHybridBackend>,
    debug_segments: bool,
) -> JoinHandle<()> {
    let weak = Arc::downgrade(backend);
    fireweed_resp::spawn_governed(async move {
        let deferred_interval = Duration::from_millis(250);
        let now = tokio::time::Instant::now();
        let mut deferred_tick =
            tokio::time::interval_at(now + deferred_interval, deferred_interval);
        let mut dbg_last = std::time::Instant::now();
        loop {
            deferred_tick.tick().await;
            if weak.strong_count() == 0 {
                break;
            }
            let emit_debug = debug_segments && dbg_last.elapsed() >= Duration::from_secs(1);
            if emit_debug {
                dbg_last = std::time::Instant::now();
            }
            let job_backend = weak.clone();
            let join = tokio::task::spawn_blocking(move || {
                let backend = job_backend.upgrade()?;
                let result = backend.try_flush_deferred_projection().map(|_| ());
                if emit_debug {
                    eprintln!(
                        "[seg] profile=objectlog/hybrid deferred_flush ok={}",
                        result.is_ok()
                    );
                }
                Some(result)
            });
            match join.await {
                Ok(Some(Ok(()))) => {}
                Ok(Some(Err(e))) => {
                    eprintln!("[objectlog/hybrid] maintenance flush failed: {e}")
                }
                Ok(None) => break,
                Err(e) => eprintln!("[objectlog/hybrid] maintenance task failed: {e}"),
            }
        }
    })
}

/// Wrap an already-`Arc`-shared backend in the selected ownership runtime and run it. [`start`] constructs
/// the control plane once and passes it through every backend arm instead of manufacturing private state.
#[allow(clippy::too_many_arguments)]
async fn run_owned<B: RespBackend>(
    backend: Arc<B>,
    control_plane: Arc<dyn QueueControlPlane>,
    advertise_addr: Option<&str>,
    owner: OwnerId,
    clock: Arc<dyn Clock>,
    listen: &str,
    reclaim_interval: Duration,
    queues: &[QueueDefinition],
) -> EngineResult<Server> {
    start_with_ownership_advertised(
        backend,
        control_plane,
        owner,
        clock,
        listen,
        advertise_addr,
        reclaim_interval,
        queues,
    )
    .await
}

#[cfg(feature = "postgres")]
fn blocking_backend<B: RespBackend>(
    inner: Arc<B>,
) -> (
    Arc<PostgresWholeOperationAdapter<B>>,
    PostgresBlockingLifecycle,
) {
    let adapter = PostgresWholeOperationAdapter::from_arc(inner);
    let lifecycle = adapter.lifecycle();
    (Arc::new(adapter), lifecycle)
}

fn blocking_backend_pool<B: RespBackend>(
    inner: Vec<Arc<B>>,
) -> (
    Arc<PostgresWholeOperationAdapter<B>>,
    PostgresBlockingLifecycle,
) {
    let adapter = PostgresWholeOperationAdapter::from_arcs(inner);
    let lifecycle = adapter.lifecycle();
    (Arc::new(adapter), lifecycle)
}

#[allow(clippy::too_many_arguments)]
async fn run_owned_with_blocking_lifecycle<B: RespBackend>(
    backend: Arc<B>,
    lifecycle: PostgresBlockingLifecycle,
    control_plane: Arc<dyn QueueControlPlane>,
    advertise_addr: Option<&str>,
    owner: OwnerId,
    clock: Arc<dyn Clock>,
    listen: &str,
    reclaim_interval: Duration,
    queues: &[QueueDefinition],
) -> EngineResult<Server> {
    let mut server = run_owned(
        backend,
        control_plane,
        advertise_addr,
        owner,
        clock,
        listen,
        reclaim_interval,
        queues,
    )
    .await?;
    server.blocking_lifecycles.push(lifecycle);
    Ok(server)
}

// The Fjord variant carries the same explicit composition-root inputs as `run_owned`, plus the
// lifecycle task that must be attached to the returned server.
#[allow(clippy::too_many_arguments)]
async fn run_owned_with_fjord_task<B: RespBackend>(
    backend: Arc<B>,
    control_plane: Arc<dyn QueueControlPlane>,
    advertise_addr: Option<&str>,
    owner: OwnerId,
    clock: Arc<dyn Clock>,
    listen: &str,
    reclaim_interval: Duration,
    queues: &[QueueDefinition],
    fjord_task: Option<JoinHandle<()>>,
) -> EngineResult<Server> {
    let mut server = run_owned(
        backend,
        control_plane,
        advertise_addr,
        owner,
        clock,
        listen,
        reclaim_interval,
        queues,
    )
    .await?;
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
    let serve_task = fireweed_resp::spawn_governed(serve_with_shutdown(
        listener,
        backend.clone(),
        clock.clone(),
        cancel.clone(),
    ));
    let reclaim_task = fireweed_resp::spawn_governed(reclaim_loop(
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
        maintenance_tasks: Vec::new(),
        blocking_lifecycles: Vec::new(),
        control_plane_lifecycles: Vec::new(),
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
    CP: QueueControlPlane + ?Sized + 'static,
{
    start_with_ownership_advertised(
        backend,
        control_plane,
        owner,
        clock,
        listen,
        None,
        reclaim_interval,
        queues,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn start_with_ownership_advertised<B, CP>(
    backend: Arc<B>,
    control_plane: Arc<CP>,
    owner: OwnerId,
    clock: Arc<dyn Clock>,
    listen: &str,
    advertise_addr: Option<&str>,
    reclaim_interval: Duration,
    queues: &[QueueDefinition],
) -> EngineResult<Server>
where
    B: RespBackend,
    CP: QueueControlPlane + ?Sized + 'static,
{
    for def in queues {
        backend.create_queue(def.clone()).await?;
    }
    let listener = TcpListener::bind(listen).await.map_err(io_err)?;
    let addr = listener.local_addr().map_err(io_err)?;
    let endpoint = if let Some(advertise_addr) = advertise_addr {
        validated_owner_endpoint(advertise_addr).ok_or(EngineError::Invalid(
            "advertise address must be a dialable IP socket address with a nonzero port",
        ))?
    } else {
        let ip = addr.ip();
        let advertised_ip = if ip.is_unspecified() {
            "127.0.0.1".parse().expect("loopback IP is valid")
        } else {
            ip
        };
        SocketAddr::new(advertised_ip, addr.port()).to_string()
    };
    let hooks = Arc::new(OwnershipRuntime::new(
        backend.clone(),
        control_plane,
        owner,
        endpoint,
    ));
    let control_plane_lifecycle = hooks.control_plane_executor.lifecycle();
    let now = clock.now();
    hooks.advertise_and_refresh_owner_endpoints(now).await?;
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
    let serve_task = fireweed_resp::spawn_governed(serve_with_shutdown_and_hooks(
        listener,
        backend.clone(),
        hooks.clone(),
        clock.clone(),
        cancel.clone(),
    ));
    let reclaim_task = fireweed_resp::spawn_governed(reclaim_loop(
        backend,
        clock.clone(),
        reclaim_interval,
        reclaim.clone(),
    ));
    let ownership_task = fireweed_resp::spawn_governed(ownership_loop(
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
        maintenance_tasks: Vec::new(),
        blocking_lifecycles: Vec::new(),
        control_plane_lifecycles: vec![control_plane_lifecycle],
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
    CP: QueueControlPlane + ?Sized + 'static,
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

#[cfg(test)]
mod byte_admission_wiring_tests {
    use super::*;
    use bytes::Bytes;
    use fireweed_core::{
        EligibilityPolicy, Metadata, OrderingMode, PriorityDirection, PriorityModel,
        PriorityModelKind, PriorityTieBreaker, RecurrencePolicy, RetryPolicy,
    };
    use fireweed_engine::{
        ControlPlaneStore, InMemoryControlPlane, LeaseRenewal, LeaseRenewalOutcome, LeaseState,
        PushPort, PushSpec, QueueControlPlane, QueueLease,
    };
    use std::sync::mpsc;

    #[test]
    fn every_objectlog_projection_arm_uses_native_create_only_composition() {
        let source = include_str!("lib.rs");
        let production_start = source
            .split("pub async fn start(config: Config)")
            .nth(1)
            .expect("server composition root")
            .split("#[cfg(test)]")
            .next()
            .expect("production composition boundary");

        // LogEngine product cells open S3 via ObjectLogEngineStore::open_s3.
        assert!(
            !production_start.contains("spec.open_blob_store()?"),
            "no object-log projection arm may bypass the S3 authority boundary"
        );
        assert!(
            production_start.contains("ObjectLogEngineStore::open_s3"),
            "object-log product cells must open S3 through ObjectLogEngineStore::open_s3"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn control_plane_executor_bounds_admission_and_owns_cancelled_calls() {
        let executor = ControlPlaneExecutor::with_limits(1, 2);
        let lifecycle = executor.lifecycle();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let first = {
            let executor = executor.clone();
            tokio::spawn(async move {
                executor
                    .execute(move || {
                        entered_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                        Ok::<_, EngineError>(1)
                    })
                    .await
            })
        };
        entered_rx.recv().unwrap();
        let (completed_tx, completed_rx) = mpsc::sync_channel(1);
        let second = {
            let executor = executor.clone();
            tokio::spawn(async move {
                executor
                    .execute(move || {
                        completed_tx.send(()).unwrap();
                        Ok::<_, EngineError>(2)
                    })
                    .await
            })
        };
        while executor.state.outstanding.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            executor.execute(|| Ok::<_, EngineError>(3)).await,
            Err(EngineError::Backpressure { .. })
        ));
        second.abort();
        release_tx.send(()).unwrap();
        assert_eq!(first.await.unwrap().unwrap(), 1);
        completed_rx.recv().unwrap();
        lifecycle.close();
        lifecycle.drain_started().await;
        assert!(matches!(
            executor.execute(|| Ok::<_, EngineError>(4)).await,
            Err(EngineError::Unavailable)
        ));
    }

    fn queue_definition() -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("tenant").unwrap(),
            queue_id: QueueId::new("queue").unwrap(),
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
            secondary_indexes: Vec::new(),
            entity_schema: None,
            typed_indexes: Vec::new(),
            emit_change_records: false,
        }
    }

    fn push_spec() -> PushSpec {
        PushSpec {
            client_item_key: None,
            priority: None,
            not_before: None,
            group_key: None,
            payload: Some(Bytes::from_static(b"resident")),
            fields: Default::default(),
            metadata: Metadata::default(),
            cohort_size: None,
            gate_keys: Vec::new(),
            entity: None,
        }
    }

    #[tokio::test]
    async fn production_hybrid_constructor_opens_log_engine_product() {
        let path = std::env::temp_dir().join(format!(
            "fireweed-hybrid-open-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let root = std::env::temp_dir().join(format!(
            "fireweed-hybrid-log-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&root);
        let backend = open_objectlog_hybrid_backend(
            ObjectLogSpec::local(&root, SegmentConfig::new(1_024, 100).unwrap()),
            &path,
            0,
            fireweed_sqlite::DEFAULT_DEFERRED_FLUSH_CHUNK,
            false,
            None,
        )
        .await
        .expect("open hybrid LogEngine product");
        drop(backend);
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn hybrid_flusher_does_not_retain_backend_on_shutdown() {
        let path = std::env::temp_dir().join(format!(
            "fireweed-hybrid-drop-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let root = std::env::temp_dir().join(format!(
            "fireweed-hybrid-drop-log-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&root);
        let backend = open_objectlog_hybrid_backend(
            ObjectLogSpec::local(&root, SegmentConfig::new(8_192, 60_000).unwrap()),
            &path,
            0,
            fireweed_sqlite::DEFAULT_DEFERRED_FLUSH_CHUNK,
            false,
            None,
        )
        .await
        .expect("open hybrid product");
        let definition = queue_definition();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        let flusher = spawn_hybrid_flusher(&backend, false);
        let push = {
            let backend = Arc::clone(&backend);
            let shard = shard.clone();
            tokio::spawn(async move {
                backend
                    .push(
                        &shard,
                        vec![push_spec()],
                        UtcTimestamp::new(1_700_000_000, 0).unwrap(),
                        None,
                    )
                    .await
            })
        };
        // Yield so the push can land before we drop the backend/flusher.
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        push.abort();
        let _ = push.await;
        assert_eq!(
            Arc::strong_count(&backend),
            1,
            "the flusher must hold only Weak ownership between maintenance deadlines"
        );
        let weak = Arc::downgrade(&backend);
        drop(backend);
        tokio::time::timeout(Duration::from_secs(1), flusher)
            .await
            .expect("weak hybrid flusher did not exit")
            .expect("hybrid flusher task failed");
        assert!(weak.upgrade().is_none());
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn start_rejects_programmatic_objectlog_limits_below_segment_target() {
        let path = std::env::temp_dir().join(format!(
            "fireweed-invalid-byte-config-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let mut config = Config::new(
            BackendSpec {
                log: LogSpec::ObjectLog(ObjectLogSpec::local(
                    path,
                    SegmentConfig::new(4_096, 100).unwrap(),
                )),
                projection: ProjectionSpec::InMemory,
                control_plane: ControlPlaneSpec::InProcess,
            },
            0,
            "127.0.0.1:0".to_string(),
            Duration::from_secs(1),
            Vec::new(),
        );
        config.objectlog_byte_limits = ObjectLogByteLimits {
            global: 2_048,
            tenant: Some(1_024),
            queue_waiting: 1_024,
        };
        let error = start(config)
            .await
            .err()
            .expect("direct Config must not bypass segment-target validation");
        assert!(matches!(error, EngineError::Invalid(_)));
    }

    #[tokio::test]
    async fn object_log_commit_recovery_tests_rejects_production_one_object_per_command_config() {
        fn config(log: ObjectLogSpec) -> Config {
            Config::new(
                BackendSpec {
                    log: LogSpec::ObjectLog(log),
                    projection: ProjectionSpec::InMemory,
                    control_plane: ControlPlaneSpec::InProcess,
                },
                0,
                "127.0.0.1:0".to_owned(),
                Duration::from_secs(1),
                Vec::new(),
            )
        }

        let root = std::env::temp_dir().join(format!(
            "fireweed-p3o-server-pre-io-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let unsafe_segments = SegmentConfig::new(1, 20).expect("structurally valid test shape");

        let local_error = start(config(ObjectLogSpec::local(root.clone(), unsafe_segments)))
            .await
            .err()
            .expect("local production config must fail closed");
        assert_eq!(
            local_error,
            EngineError::Invalid(fireweed_engine::PRODUCTION_ONE_OBJECT_PER_COMMAND_ERROR)
        );
        assert!(!root.exists(), "local guard must run before filesystem I/O");

        let s3_error = tokio::time::timeout(
            Duration::from_millis(100),
            start(config(ObjectLogSpec::S3 {
                endpoint: "http://127.0.0.1:1".to_owned(),
                bucket: "fireweed".to_owned(),
                region: "us-east-1".to_owned(),
                credentials: S3CredentialSource::Static {
                    access_key_id: "akid".to_owned(),
                    secret_access_key: "secret".to_owned(),
                },
                segment_config: unsafe_segments,
                allow_insecure_http: true,
            })),
        )
        .await
        .expect("S3 config rejection must not wait for network I/O")
        .err()
        .expect("S3 production config must fail closed");
        assert_eq!(
            s3_error,
            EngineError::Invalid(fireweed_engine::PRODUCTION_ONE_OBJECT_PER_COMMAND_ERROR)
        );

        let neighboring_segments =
            SegmentConfig::new(2, 1).expect("neighboring group-commit shape");
        assert!(
            ObjectLogSpec::local("/tmp/fireweed-p3o-neighbor", neighboring_segments)
                .validate()
                .is_ok()
        );
        assert!(
            ObjectLogSpec::S3 {
                endpoint: "https://s3.example".to_owned(),
                bucket: "fireweed".to_owned(),
                region: "us-east-1".to_owned(),
                credentials: S3CredentialSource::Static {
                    access_key_id: "akid".to_owned(),
                    secret_access_key: "secret".to_owned(),
                },
                segment_config: neighboring_segments,
                allow_insecure_http: false,
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn server_mixed_renewal_outcomes_evict_only_fenced_and_missing_sessions() {
        let backend = Arc::new(fireweed_memory::composed_memory_backend());
        let cp = Arc::new(InMemoryControlPlane::default());
        let owner = OwnerId::new("node").unwrap();
        let runtime = OwnershipRuntime::new(backend, cp, owner.clone(), "127.0.0.1:7000".into());
        let queues: Vec<QueueKey> = ["assigned", "draining", "error", "fenced", "missing"]
            .into_iter()
            .map(|name| {
                QueueKey::new(
                    TenantId::new("tenant").unwrap(),
                    QueueId::new(name).unwrap(),
                )
            })
            .collect();
        for queue in &queues {
            runtime.sessions.lock().unwrap().insert(
                queue.clone(),
                OwnedSession {
                    owner: owner.clone(),
                    queue: queue.clone(),
                    lease_epoch: 1,
                    fence_epoch: 1,
                },
            );
        }
        let renewals = queues
            .iter()
            .cloned()
            .map(|queue| LeaseRenewal {
                queue,
                owner: owner.clone(),
                expected_epoch: 1,
            })
            .collect();
        let lease = |state| QueueLease {
            state,
            active_owner_id: Some(owner.clone()),
            target_owner_id: None,
            assignment_epoch: 1,
            lease_expires_at: Some(UtcTimestamp::new(15, 0).unwrap()),
        };
        let error = EngineError::Storage("transient row".into());
        let result = runtime.apply_session_renewal_outcomes(
            renewals,
            vec![
                LeaseRenewalOutcome::Renewed(lease(LeaseState::Assigned)),
                LeaseRenewalOutcome::Renewed(lease(LeaseState::Draining)),
                LeaseRenewalOutcome::Error(error.clone()),
                LeaseRenewalOutcome::Fenced,
                LeaseRenewalOutcome::Missing,
            ],
        );
        assert_eq!(result, Some(error));
        let sessions = runtime.sessions.lock().unwrap();
        assert!(sessions.contains_key(&queues[0]));
        assert!(sessions.contains_key(&queues[1]));
        assert!(
            sessions.contains_key(&queues[2]),
            "transient error retains session"
        );
        assert!(!sessions.contains_key(&queues[3]));
        assert!(!sessions.contains_key(&queues[4]));
    }

    #[tokio::test]
    async fn node_renewal_uses_one_renewal_and_one_resolution_task_at_1_100_1000() {
        for size in [1usize, 100, 1_000] {
            let backend = Arc::new(fireweed_memory::composed_memory_backend());
            let cp = Arc::new(InMemoryControlPlane::default());
            let owner = OwnerId::new(format!("node-{size}")).unwrap();
            cp.register_owner(&owner, UtcTimestamp::new(0, 0).unwrap())
                .unwrap();
            let runtime =
                OwnershipRuntime::new(backend, cp.clone(), owner.clone(), "127.0.0.1:7000".into());
            for index in 0..size {
                let queue = QueueKey::new(
                    TenantId::new("tenant").unwrap(),
                    QueueId::new(format!("q-{index:04}")).unwrap(),
                );
                let AcquireOutcome::Acquired(lease) = cp
                    .acquire_queue_lease(&queue, &owner, UtcTimestamp::new(0, 0).unwrap())
                    .unwrap()
                else {
                    panic!("owner acquires queue");
                };
                cp.confirm_queue_lease_fence(
                    &queue,
                    &owner,
                    lease.assignment_epoch,
                    UtcTimestamp::new(0, 0).unwrap(),
                )
                .unwrap();
                runtime.sessions.lock().unwrap().insert(
                    queue.clone(),
                    OwnedSession {
                        owner: owner.clone(),
                        queue,
                        lease_epoch: lease.assignment_epoch,
                        fence_epoch: lease.assignment_epoch,
                    },
                );
            }
            let before = runtime.ownership_batch_task_counts();
            runtime
                .renew_sessions(UtcTimestamp::new(1, 0).unwrap())
                .await
                .unwrap();
            let after = runtime.ownership_batch_task_counts();
            assert_eq!(after.0 - before.0, 1, "{size} queues use one renewal task");
            assert_eq!(
                after.1 - before.1,
                1,
                "{size} queues use one assignment-poll task"
            );
            assert_eq!(runtime.sessions.lock().unwrap().len(), size);
        }
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_postgres_factory_selects_fixed_unified_atomic_pool() {
        let source = include_str!("lib.rs");
        let arm = source
            .split("// TD-002: the selectable postgres/postgres profile")
            .nth(1)
            .expect("postgres/postgres factory arm")
            .split("(log, projection) =>")
            .next()
            .expect("factory arm boundary");
        assert!(arm.contains("fixed_postgres_relational_pool"));
        assert!(!arm.contains("PostgresLog::connect_with_config"));
        assert!(
            !arm.contains("ComposedBackend") || !arm.contains("::new"),
            "postgres/postgres product arm must not open the retired sync dual-stack backend"
        );
    }

    /// Class A cell: construct a durable sqlite log × sqlite projection via the product adapter
    /// used by the server composition root (`composed_sqlite_log_sqlite_projection`), with distinct
    /// paths for log vs projection.
    #[test]
    fn sqlite_log_sqlite_projection_constructs_with_distinct_paths() {
        let uniq = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        );
        let log_path =
            std::env::temp_dir().join(format!("fireweed-server-sqlite-sqlite-log-{uniq}.db"));
        let proj_path =
            std::env::temp_dir().join(format!("fireweed-server-sqlite-sqlite-proj-{uniq}.db"));
        let _ = std::fs::remove_file(&log_path);
        let _ = std::fs::remove_file(&proj_path);

        let log_s = log_path.to_str().expect("utf8 log path");
        let proj_s = proj_path.to_str().expect("utf8 proj path");
        assert_ne!(log_s, proj_s);

        // Mirror the BackendSpec the server match arm receives.
        let spec = BackendSpec {
            log: LogSpec::Sqlite {
                path: log_path.clone(),
            },
            projection: ProjectionSpec::Sqlite {
                path: proj_path.clone(),
            },
            control_plane: ControlPlaneSpec::InProcess,
        };
        match (&spec.log, &spec.projection) {
            (LogSpec::Sqlite { path: lp }, ProjectionSpec::Sqlite { path: pp }) => {
                assert_ne!(lp, pp);
            }
            _ => panic!("expected sqlite × sqlite BackendSpec"),
        }

        let backend = fireweed_sqlite::composed_sqlite_log_sqlite_projection(log_s, proj_s)
            .expect("open sqlite log × sqlite projection");
        // Recovery-on-open succeeds on empty stores (no panic / storage error).
        drop(backend);

        // Source contract: composition root selects the distinct-path adapter.
        let source = include_str!("lib.rs");
        assert!(
            source.contains("composed_sqlite_log_sqlite_projection"),
            "server must wire composed_sqlite_log_sqlite_projection for sqlite×sqlite"
        );
        assert!(
            source.contains("LogSpec::Sqlite { path }, ProjectionSpec::Sqlite"),
            "server match arm for sqlite×sqlite must exist"
        );

        let _ = std::fs::remove_file(&log_path);
        let _ = std::fs::remove_file(&proj_path);
    }

    /// Class A cell: construct a durable sqlite log × postgres projection composition (same shape as
    /// the server match arm). Live connect is env-gated; without a DB we still assert BackendSpec
    /// shape and that the composition root names both axes.
    #[test]
    fn sqlite_log_postgres_projection_backend_spec_and_composition_root() {
        let log_path = std::env::temp_dir().join(format!(
            "fireweed-server-sqlite-postgres-log-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let _ = std::fs::remove_file(&log_path);

        // Always construct the sqlite log axis used by the pairing.
        let log = fireweed_sqlite::SqliteLog::open(log_path.to_str().unwrap())
            .expect("open sqlite log for sqlite×postgres cell");
        drop(log);

        let source = include_str!("lib.rs");
        assert!(
            source.contains("LogSpec::Sqlite { path }, ProjectionSpec::Postgres"),
            "server match arm for sqlite×postgres must exist (feature postgres)"
        );
        assert!(
            source.contains("PostgresRelational::connect"),
            "sqlite×postgres must compose SqliteLog with PostgresRelational"
        );

        let _ = std::fs::remove_file(&log_path);
    }

    /// When a live postgres is available, open the full sqlite-log × postgres-projection composed
    /// backend (mirrors the server `spawn_blocking` body).
    #[cfg(feature = "postgres")]
    #[test]
    fn sqlite_log_postgres_projection_constructs_when_pg_available() {
        let Ok(url) = std::env::var("FIREWEED_PG_TEST_URL") else {
            eprintln!("SQLITE/POSTGRES CONSTRUCT SKIPPED — set FIREWEED_PG_TEST_URL to a live DB");
            return;
        };
        let schema = format!("fireweed_sqlite_pg_{}", std::process::id());

        let mut client =
            fireweed_postgres::connect(fireweed_postgres::PostgresConnectConfig::new(&url))
                .expect("connect to create schema");
        client
            .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema};"))
            .expect("create schema");
        drop(client);

        let scoped = if url.contains('?') {
            format!("{url}&options=-csearch_path%3D{schema}")
        } else {
            format!("{url}?options=-csearch_path%3D{schema}")
        };

        let log_path =
            std::env::temp_dir().join(format!("fireweed-server-sqlite-pg-construct-{schema}.db"));
        let _ = std::fs::remove_file(&log_path);

        let log =
            fireweed_sqlite::SqliteLog::open(log_path.to_str().unwrap()).expect("open sqlite log");
        let projection = fireweed_postgres::PostgresRelational::connect(&scoped)
            .expect("connect postgres projection");
        let backend = assemble_async_log_replay(log, projection, 0)
            .expect("assemble async log-replay")
            .recover()
            .expect("recover sqlite×postgres composition");
        drop(backend);

        let _ = std::fs::remove_file(&log_path);
        if let Ok(mut client) =
            fireweed_postgres::connect(fireweed_postgres::PostgresConnectConfig::new(&url))
        {
            let _ = client.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE;"));
        }
    }
    fn filesystem_tmp_root(tag: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "fireweed-server-fs-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create filesystem object-log root");
        root
    }

    /// BackendSpec + composition-root contract for filesystem × postgres.
    #[test]
    fn filesystem_object_log_postgres_projection_backend_spec_and_composition_root() {
        let root = filesystem_tmp_root("pg-spec");
        let segment_config = SegmentConfig::new(262_144, 20).unwrap();
        let spec = BackendSpec {
            log: LogSpec::ObjectLog(ObjectLogSpec::local(&root, segment_config)),
            #[cfg(feature = "postgres")]
            projection: ProjectionSpec::Postgres {
                url: "postgres://fireweed:fireweed@127.0.0.1:5432/fireweed".into(),
            },
            #[cfg(not(feature = "postgres"))]
            projection: ProjectionSpec::InMemory,
            control_plane: ControlPlaneSpec::InProcess,
        };
        assert_eq!(spec.log.label(), "filesystem");
        match &spec.log {
            LogSpec::ObjectLog(ObjectLogSpec::LocalFilesystem { root: r, .. }) => {
                assert_eq!(r, &root);
            }
            _ => panic!("expected LocalFilesystem log"),
        }

        let source = include_str!("lib.rs");
        assert!(
            source.contains("open_objectlog_postgres_backend"),
            "server must own open_objectlog_postgres_backend for filesystem|s3 × postgres"
        );
        assert!(
            source.contains("ObjectLogEngineStore::open_local")
                || source.contains("ObjectLogEngineStore::open_s3")
                || source.contains("open_group_commit_authoritative_with_blob_store"),
            "object-log×postgres must open LogEngine (or legacy authoritative group-commit ObjectLog)"
        );
        assert!(
            source.contains("PostgresRelational::connect"),
            "object-log×postgres must compose with PostgresRelational"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    /// When a live postgres is available, open filesystem object-log × postgres projection.
    #[cfg(feature = "postgres")]
    #[test]
    fn filesystem_object_log_postgres_projection_constructs_when_pg_available() {
        let Ok(url) = std::env::var("FIREWEED_PG_TEST_URL") else {
            eprintln!(
                "FILESYSTEM/POSTGRES CONSTRUCT SKIPPED — set FIREWEED_PG_TEST_URL to a live DB"
            );
            return;
        };
        let schema = format!("fireweed_fs_pg_{}", std::process::id());
        let mut client =
            fireweed_postgres::connect(fireweed_postgres::PostgresConnectConfig::new(&url))
                .expect("connect to create schema");
        client
            .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema};"))
            .expect("create schema");
        drop(client);

        let scoped = if url.contains('?') {
            format!("{url}&options=-csearch_path%3D{schema}")
        } else {
            format!("{url}?options=-csearch_path%3D{schema}")
        };

        let root = filesystem_tmp_root(&schema);
        let segment_config = SegmentConfig::new(262_144, 20).unwrap();
        let log_spec = ObjectLogSpec::local(&root, segment_config);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build object-log PostgreSQL operation runtime");
        let backend = runtime
            .block_on(open_objectlog_postgres_backend(log_spec, &scoped, 0))
            .expect("construct filesystem×postgres");
        drop(backend);
        drop(runtime);

        let _ = std::fs::remove_dir_all(&root);
        if let Ok(mut client) =
            fireweed_postgres::connect(fireweed_postgres::PostgresConnectConfig::new(&url))
        {
            let _ = client.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE;"));
        }
    }

    fn s3_unit_spec(segment_config: SegmentConfig) -> ObjectLogSpec {
        // S3BlobStore::new is client-only (no network). Unit construction of segmented backends
        // over that client does not contact the endpoint until a first blob op.
        ObjectLogSpec::S3 {
            endpoint: "http://127.0.0.1:9".into(),
            bucket: "fireweed-unit-s3".into(),
            region: "us-east-1".into(),
            credentials: S3CredentialSource::Static {
                access_key_id: "unit-access".into(),
                secret_access_key: "unit-secret".into(),
            },
            segment_config,
            allow_insecure_http: true,
        }
    }

    /// BackendSpec + composition-root contract for s3 × postgres (shared ObjectLog arm).
    #[test]
    fn s3_object_log_postgres_projection_backend_spec_and_composition_root() {
        let segment_config = SegmentConfig::new(262_144, 20).unwrap();
        let spec = BackendSpec {
            log: LogSpec::ObjectLog(s3_unit_spec(segment_config)),
            #[cfg(feature = "postgres")]
            projection: ProjectionSpec::Postgres {
                url: "postgres://fireweed:fireweed@127.0.0.1:5432/fireweed".into(),
            },
            #[cfg(not(feature = "postgres"))]
            projection: ProjectionSpec::InMemory,
            control_plane: ControlPlaneSpec::InProcess,
        };
        assert_eq!(spec.log.label(), "s3");
        match &spec.log {
            LogSpec::ObjectLog(ObjectLogSpec::S3 {
                endpoint,
                bucket,
                allow_insecure_http,
                ..
            }) => {
                assert_eq!(endpoint, "http://127.0.0.1:9");
                assert_eq!(bucket, "fireweed-unit-s3");
                assert!(*allow_insecure_http);
            }
            _ => panic!("expected S3 object log"),
        }

        let source = include_str!("lib.rs");
        assert!(
            source.contains("open_objectlog_postgres_backend"),
            "server must own open_objectlog_postgres_backend for filesystem|s3 × postgres"
        );
        assert!(
            source.contains("Class A: filesystem|s3 object-log × durable Postgres projection"),
            "postgres composition arm documents shared filesystem|s3 coverage"
        );
        assert!(
            source.contains("ObjectLogEngineStore::open_local")
                || source.contains("ObjectLogEngineStore::open_s3")
                || source.contains("open_group_commit_authoritative_with_blob_store"),
            "object-log×postgres must open LogEngine (or legacy authoritative group-commit ObjectLog)"
        );
        assert!(
            source.contains("PostgresRelational::connect"),
            "object-log×postgres must compose with PostgresRelational"
        );
    }

    /// Live s3 × postgres construction when S3-compatible + Postgres test envs are set.
    /// Without live services this is a no-op skip (unit coverage lives in the construction tests above).
    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn s3_object_log_postgres_projection_constructs_when_s3_and_pg_available() {
        let required = [
            "FIREWEED_S3_TEST_ENDPOINT",
            "FIREWEED_S3_TEST_BUCKET",
            "FIREWEED_S3_TEST_REGION",
            "FIREWEED_S3_TEST_ACCESS_KEY",
            "FIREWEED_S3_TEST_SECRET_KEY",
            "FIREWEED_PG_TEST_URL",
        ];
        let values: Option<Vec<_>> = required
            .iter()
            .map(|name| std::env::var(name).ok().map(|value| (*name, value)))
            .collect();
        let Some(values) = values else {
            eprintln!(
                "S3/POSTGRES CONSTRUCT SKIPPED — set {} for live s3×postgres open",
                required.join(", ")
            );
            return;
        };
        let lookup = |name: &str| {
            values
                .iter()
                .find_map(|(key, value)| (*key == name).then_some(value.as_str()))
                .expect("required live-test variable")
        };

        let url = lookup("FIREWEED_PG_TEST_URL").to_owned();
        let schema = format!("fireweed_s3_pg_{}", std::process::id());
        let mut client =
            fireweed_postgres::connect(fireweed_postgres::PostgresConnectConfig::new(&url))
                .expect("connect to create schema");
        client
            .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema};"))
            .expect("create schema");
        drop(client);

        let scoped = if url.contains('?') {
            format!("{url}&options=-csearch_path%3D{schema}")
        } else {
            format!("{url}?options=-csearch_path%3D{schema}")
        };

        // S3 requires native create-only (If-None-Match / equivalent); open probes the endpoint.
        let segment_config = SegmentConfig::new(262_144, 20).unwrap();
        let log_spec = ObjectLogSpec::S3 {
            endpoint: lookup("FIREWEED_S3_TEST_ENDPOINT").to_owned(),
            bucket: lookup("FIREWEED_S3_TEST_BUCKET").to_owned(),
            region: lookup("FIREWEED_S3_TEST_REGION").to_owned(),
            credentials: S3CredentialSource::Static {
                access_key_id: lookup("FIREWEED_S3_TEST_ACCESS_KEY").to_owned(),
                secret_access_key: lookup("FIREWEED_S3_TEST_SECRET_KEY").to_owned(),
            },
            segment_config,
            allow_insecure_http: lookup("FIREWEED_S3_TEST_ENDPOINT").starts_with("http://"),
        };
        let backend = open_objectlog_postgres_backend(log_spec, &scoped, 0)
            .await
            .expect("construct s3×postgres");
        drop(backend);

        if let Ok(mut client) =
            fireweed_postgres::connect(fireweed_postgres::PostgresConnectConfig::new(&url))
        {
            let _ = client.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE;"));
        }
    }
}

/// Class B (ADR-013) memory-log cells: full T0–T3 via [`fireweed::open`] / [`fireweed::open_async`].
///
/// Cells: `memory×memory`, `memory×sqlite`, `memory×postgres`.
///
/// | Layer | Meaning (Class B) |
/// |-------|-------------------|
/// | **T0 Construct** | `StorageConfig` validate + open succeeds |
/// | **T1 Lifecycle** | create_queue → push → claim → complete; claim → fail (reject) |
/// | **T2 Reopen** | process death → projection-only recover (empty OK for memory×memory) |
/// | **T3 Contract** | projection durability + rejection; `durable_log_replay` must not be claimed |
///
/// Postgres cell runs when `FIREWEED_PG_TEST_URL` is set (and fireweed is built with `postgres`);
/// otherwise it is skipped with an explicit `eprintln!`.
#[cfg(test)]
mod class_b_memory_log_tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use fireweed::{
        ClientItemKey, ConfigSecret, EligibilityPolicy, LogConfig, NewItem, OrderingMode,
        PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue,
        ProjectionStoreConfig, QueueDefinition, QueueId, QueueKey, RecoveryPolicy,
        RecurrencePolicy, ResponseBarrier, RetryPolicy, SegmentConfig, StorageConfig, SystemClock,
        TenantId, open, open_async,
    };
    use fireweed_conformance::matrix_classes::{
        CellConformanceClaims, MatrixCell, MatrixLog, MatrixProjection, ProductDurabilityClass,
        register_suite_claims, validate_claims_for_cell,
    };

    static FIXTURE_ORDINAL: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ClassBProjection {
        Memory,
        Sqlite,
        Postgres,
    }

    impl ClassBProjection {
        fn name(self) -> &'static str {
            match self {
                Self::Memory => "memory",
                Self::Sqlite => "sqlite",
                Self::Postgres => "postgres",
            }
        }

        fn is_durable(self) -> bool {
            !matches!(self, Self::Memory)
        }

        fn matrix_projection(self) -> MatrixProjection {
            match self {
                Self::Memory => MatrixProjection::Memory,
                Self::Sqlite => MatrixProjection::Sqlite,
                Self::Postgres => MatrixProjection::Postgres,
            }
        }
    }

    struct FixtureRoot(PathBuf);

    impl FixtureRoot {
        fn new(label: &str) -> Self {
            let ordinal = FIXTURE_ORDINAL.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "fireweed-class-b-t0t3-{label}-{}-{ordinal}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).expect("class B fixture root");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for FixtureRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn segments() -> SegmentConfig {
        SegmentConfig::new(1024 * 1024, 5).expect("valid segments")
    }

    fn queue_slug(proj: ClassBProjection) -> String {
        format!(
            "cb_{}_{}",
            proj.name(),
            FIXTURE_ORDINAL.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn queue_definition(slug: &str) -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("class-b-t0t3").unwrap(),
            queue_id: QueueId::new(slug).unwrap(),
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
            request_id_retention_ms: 3_600_000,
            client_item_key_retention_ms: 3_600_000,
            terminal_retention_ms: 3_600_000,
            max_lease_duration_ms: 60_000,
            retry_policy: RetryPolicy { max_attempts: 3 },
            max_push_batch_size: 100,
            max_claim_batch_size: 100,
            max_eligible_group_size: None,
            secondary_indexes: vec![],
            entity_schema: None,
            typed_indexes: vec![],
            emit_change_records: false,
        }
    }

    fn queue_key(slug: &str) -> QueueKey {
        QueueKey::new(
            TenantId::new("class-b-t0t3").unwrap(),
            QueueId::new(slug).unwrap(),
        )
    }

    fn build_class_b_config(proj: ClassBProjection, root: &Path, slug: &str) -> StorageConfig {
        let projection = match proj {
            ClassBProjection::Memory => ProjectionStoreConfig::Memory,
            ClassBProjection::Sqlite => ProjectionStoreConfig::Sqlite {
                path: root.join("projection.db"),
            },
            ClassBProjection::Postgres => {
                let url = std::env::var("FIREWEED_PG_TEST_URL")
                    .expect("postgres skip_reason must gate this path");
                ProjectionStoreConfig::Postgres {
                    url: ConfigSecret::new(url),
                }
            }
        };
        StorageConfig {
            log: LogConfig::Memory,
            projection,
            control_plane: None,
            authority: None,
            response_barrier: ResponseBarrier::Strict,
            segments: segments(),
            namespace: format!("class-b-{}-{}-{}", proj.name(), slug, std::process::id()),
            recovery: RecoveryPolicy::default(),
        }
    }

    /// T3 hard rule: Class B must never register or validate `durable_log_replay`.
    fn assert_class_b_t3_claims(proj: ClassBProjection) {
        let cell = MatrixCell::new(MatrixLog::Memory, proj.matrix_projection());
        assert_eq!(
            cell.product_durability_class(),
            ProductDurabilityClass::ClassB,
            "{} must be product Class B",
            cell.id()
        );
        let max = cell.claims();
        assert!(
            !max.durable_log_replay,
            "Class B {} must not allow durable_log_replay in max claims",
            cell.id()
        );
        // Allowed claims for Class B (core + optional projection_reopen; never log-replay).
        let allowed = CellConformanceClaims {
            core: true,
            durable_log_replay: false,
            projection_reopen: proj.is_durable(),
            relational_reconnect: false,
            eventual_apply: false,
            in_process_log_read: true,
        };
        validate_claims_for_cell(cell, &allowed)
            .unwrap_or_else(|e| panic!("{} T3 validate_claims: {e}", cell.id()));
        register_suite_claims(cell, allowed)
            .unwrap_or_else(|e| panic!("{} T3 register_suite_claims: {e}", cell.id()));

        // Explicit ban: attempting durable_log_replay must fail registration.
        let illegal = CellConformanceClaims {
            durable_log_replay: true,
            ..allowed
        };
        let err = validate_claims_for_cell(cell, &illegal)
            .expect_err("Class B must reject durable_log_replay claim");
        assert_eq!(err.flag, "durable_log_replay");
        assert!(
            err.reason.contains("Class B") || err.reason.contains("memory log"),
            "ban reason should name Class B / memory log: {}",
            err.reason
        );
    }

    /// Full T0–T3 body for one Class B cell via [`fireweed::open`] / [`fireweed::open_async`].
    async fn run_class_b_cell_t0_t3(proj: ClassBProjection) {
        let cell_id = format!("memory×{}", proj.name());

        if matches!(proj, ClassBProjection::Postgres)
            && std::env::var("FIREWEED_PG_TEST_URL").is_err()
        {
            eprintln!(
                "class_b T0-T3: {cell_id} skipped (FIREWEED_PG_TEST_URL unset; rebuild with --features postgres when live PG is available)"
            );
            // T3 claims still enforced offline — no durable_log_replay even when the live cell skips.
            assert_class_b_t3_claims(proj);
            return;
        }

        let root = FixtureRoot::new(proj.name());
        let slug = queue_slug(proj);
        let definition = queue_definition(&slug);
        let key = queue_key(&slug);
        let clock = Arc::new(SystemClock);

        // --- T0 Construct ---
        let cfg = build_class_b_config(proj, root.path(), &slug);
        assert!(
            !cfg.log.is_durable_log(),
            "{cell_id} T0: memory log is Class B (non-durable)"
        );
        cfg.validate()
            .unwrap_or_else(|e| panic!("{cell_id} T0 validate: {e:?}"));
        // Normative construct path: `fireweed::open(StorageConfig)` (sync). Postgres projection
        // may touch the sync client under an active Tokio runtime — use open_async there.
        let fireweed = if matches!(proj, ClassBProjection::Postgres) {
            open_async(cfg.clone(), Arc::clone(&clock) as _)
                .await
                .unwrap_or_else(|e| panic!("{cell_id} T0 open_async(StorageConfig): {e:?}"))
        } else {
            open(cfg.clone(), Arc::clone(&clock) as _)
                .unwrap_or_else(|e| panic!("{cell_id} T0 open(StorageConfig): {e:?}"))
        };

        // --- T1 Lifecycle: create_queue → push → claim → complete; push → claim → fail ---
        fireweed
            .create_queue(definition.clone())
            .await
            .unwrap_or_else(|e| panic!("{cell_id} T1 create_queue: {e:?}"));

        let complete_id = fireweed
            .push(
                &key,
                NewItem {
                    client_item_key: Some(ClientItemKey::new(format!("{slug}_complete")).unwrap()),
                    priority: Some(PriorityValue::Int64(10)),
                    ..NewItem::default()
                },
            )
            .await
            .unwrap_or_else(|e| panic!("{cell_id} T1 push(complete path): {e:?}"));

        let claimed = fireweed
            .claim(&key, 1, 30_000)
            .await
            .unwrap_or_else(|e| panic!("{cell_id} T1 claim: {e:?}"));
        assert_eq!(claimed.len(), 1, "{cell_id} T1 claim batch");
        assert_eq!(claimed[0].item_id, complete_id);

        fireweed
            .complete(&key, claimed.iter().map(|item| item.item_id))
            .await
            .unwrap_or_else(|e| panic!("{cell_id} T1 complete: {e:?}"));

        // Reject path: fail dead-letters a second claimed item.
        let fail_id = fireweed
            .push(
                &key,
                NewItem {
                    client_item_key: Some(ClientItemKey::new(format!("{slug}_fail")).unwrap()),
                    priority: Some(PriorityValue::Int64(11)),
                    ..NewItem::default()
                },
            )
            .await
            .unwrap_or_else(|e| panic!("{cell_id} T1 push(fail path): {e:?}"));
        let fail_claimed = fireweed
            .claim(&key, 1, 30_000)
            .await
            .unwrap_or_else(|e| panic!("{cell_id} T1 claim(fail path): {e:?}"));
        assert_eq!(fail_claimed.len(), 1);
        assert_eq!(fail_claimed[0].item_id, fail_id);
        fireweed
            .fail(&key, fail_claimed.iter().map(|item| item.item_id))
            .await
            .unwrap_or_else(|e| panic!("{cell_id} T1 fail/reject: {e:?}"));

        let metrics_after_t1 = fireweed
            .metrics(&key)
            .await
            .unwrap_or_else(|e| panic!("{cell_id} T1 metrics: {e:?}"));
        assert_eq!(
            metrics_after_t1.complete, 1,
            "{cell_id} T1: one completed item"
        );
        assert_eq!(
            metrics_after_t1.failed, 1,
            "{cell_id} T1: one failed (rejected) item"
        );
        assert_eq!(
            metrics_after_t1.pending, 0,
            "{cell_id} T1: no pending after finalize+fail"
        );
        assert_eq!(
            metrics_after_t1.leased, 0,
            "{cell_id} T1: no leased after finalize+fail"
        );

        // Seed a pending item for T2 reopen (not claimed).
        let _pending_id = fireweed
            .push(
                &key,
                NewItem {
                    client_item_key: Some(ClientItemKey::new(format!("{slug}_pending")).unwrap()),
                    priority: Some(PriorityValue::Int64(20)),
                    ..NewItem::default()
                },
            )
            .await
            .unwrap_or_else(|e| panic!("{cell_id} T2 seed push: {e:?}"));
        assert_eq!(
            fireweed.metrics(&key).await.unwrap().pending,
            1,
            "{cell_id}: seed pending before process death"
        );

        drop(fireweed);

        // --- T2 Reopen (class-correct) ---
        let reopened = open_async(cfg, clock as _)
            .await
            .unwrap_or_else(|e| panic!("{cell_id} T2 reopen open(StorageConfig): {e:?}"));

        if proj.is_durable() {
            // Projection-only recover: pending + terminal states survive; no log rebuild claim.
            let m = reopened
                .metrics(&key)
                .await
                .unwrap_or_else(|e| panic!("{cell_id} T2 metrics (durable proj): {e:?}"));
            assert_eq!(
                m.pending, 1,
                "{cell_id} T2: durable projection keeps 1 pending (projection-only reopen, not log replay)"
            );
            assert_eq!(
                m.complete, 1,
                "{cell_id} T2: complete survives via projection"
            );
            assert_eq!(
                m.failed, 1,
                "{cell_id} T2: failed/rejected survives via projection"
            );

            let claimed = reopened
                .claim(&key, 1, 30_000)
                .await
                .unwrap_or_else(|e| panic!("{cell_id} T2 claim: {e:?}"));
            assert_eq!(claimed.len(), 1, "{cell_id} T2 claim pending");
            reopened
                .complete(&key, claimed.iter().map(|item| item.item_id))
                .await
                .unwrap_or_else(|e| panic!("{cell_id} T2 complete: {e:?}"));
        } else {
            // memory×memory: fully process-local. Empty reopen is correct Class B semantics.
            let outcome = reopened
                .create_queue(definition)
                .await
                .unwrap_or_else(|e| panic!("{cell_id} T2 create_queue (process-local): {e:?}"));
            assert!(
                outcome.created,
                "{cell_id} T2: reopen must not recover prior queue (process-local Class B)"
            );
            let m = reopened
                .metrics(&key)
                .await
                .unwrap_or_else(|e| panic!("{cell_id} T2 metrics (process-local): {e:?}"));
            assert_eq!(
                m.pending, 0,
                "{cell_id} T2 memory×memory: empty reopen is OK (Class B process-local)"
            );
            assert_eq!(m.complete, 0, "{cell_id} T2: no durable complete state");
            assert_eq!(m.failed, 0, "{cell_id} T2: no durable failed state");
            eprintln!(
                "class_b T0-T3: {cell_id} T2 process-local empty reopen (documented Class B)"
            );
        }

        drop(reopened);

        // --- T3 Contract: claims ban + projection durability already exercised in T2 ---
        assert_class_b_t3_claims(proj);
        eprintln!("class_b T0-T3: {cell_id} passed (no durable_log_replay claim)");
    }

    #[test]
    fn class_b_memory_projection_arms_exist_in_composition_root() {
        let source = include_str!("lib.rs");
        assert!(
            source.contains("LogSpec::Memory, ProjectionSpec::Sqlite"),
            "server match arm for memory×sqlite (Class B) must exist"
        );
        assert!(
            source.contains("LogSpec::Memory, ProjectionSpec::Postgres"),
            "server match arm for memory×postgres (Class B) must exist (feature postgres)"
        );
        assert!(
            source.contains("fireweed_projection::MemoryLog::new()"),
            "Class B arms must assemble MemoryLog (in-process ordering), not a durable log"
        );
        let mem_sqlite = source
            .split("LogSpec::Memory, ProjectionSpec::Sqlite")
            .nth(1)
            .expect("memory×sqlite arm")
            .split("LogSpec::")
            .next()
            .expect("arm boundary");
        assert!(
            mem_sqlite.contains("Class B"),
            "memory×sqlite arm must document Class B semantics"
        );
        assert!(
            !mem_sqlite.contains("rebuilds the in-memory projection by replaying"),
            "Class B must not claim log-replay rebuild"
        );
        // Composition must not advertise Class A log-replay product claims for memory log arms.
        assert!(
            !mem_sqlite.contains("durable_log_replay"),
            "Class B arm must not mention durable_log_replay"
        );
    }

    /// T3 offline: every Class B cell's max claim set bans `durable_log_replay`.
    #[test]
    fn class_b_three_cells_never_claim_durable_log_replay() {
        for proj in [
            ClassBProjection::Memory,
            ClassBProjection::Sqlite,
            ClassBProjection::Postgres,
        ] {
            assert_class_b_t3_claims(proj);
        }
    }

    #[tokio::test]
    async fn class_b_memory_memory_t0_t3() {
        run_class_b_cell_t0_t3(ClassBProjection::Memory).await;
    }

    #[tokio::test]
    async fn class_b_memory_sqlite_t0_t3() {
        run_class_b_cell_t0_t3(ClassBProjection::Sqlite).await;
    }

    /// `memory×postgres` Class B: requires live `FIREWEED_PG_TEST_URL` (and fireweed `postgres` feature).
    /// Skips with `eprintln!` when the fixture is absent; T3 claim ban still runs offline.
    #[tokio::test]
    async fn class_b_memory_postgres_t0_t3() {
        run_class_b_cell_t0_t3(ClassBProjection::Postgres).await;
    }

    /// Table registration: all three Class B cells are covered by the T0–T3 harness.
    #[tokio::test]
    async fn class_b_all_three_cells_t0_t3() {
        for proj in [
            ClassBProjection::Memory,
            ClassBProjection::Sqlite,
            ClassBProjection::Postgres,
        ] {
            run_class_b_cell_t0_t3(proj).await;
        }
    }
}

/// Class A **sqlite log** matrix cells (brief §1.1 / §2): `sqlite×memory`, `sqlite×sqlite`,
/// `sqlite×postgres`.
///
/// | Layer | Coverage in this module |
/// |-------|-------------------------|
/// | **T0 Construct** | composition-root arms + open via product adapters |
/// | **T1 Lifecycle** | create_queue → push → claim → finalize |
/// | **T2 Reopen** | Class A: pending survives process-local drop+reopen via durable log |
/// | **T3 Contract** | TP-003 AC-TXN-1/2/3 for exact pairs → explicit run-owned JSONL |
/// | **T4 Deploy** | Helm CI values under `charts/fireweed-queue/ci/sqlite-*-values.yaml` (+ helm-gate) |
#[cfg(test)]
mod sqlite_log_matrix_tests {
    use super::*;
    use fireweed_conformance::fault::{
        AcEvidence, TxnCaps, ac_txn_1_success_durable_visible, ac_txn_2_rejection_no_effect,
        ac_txn_3_unknown_outcome_replay, render_evidence,
    };
    use fireweed_conformance::{claim_req, qdef, shard, ts};
    use fireweed_engine::{
        ClaimPort, FinalizeKind, FinalizeOutcome, FinalizePort, ProjectionRead, PushPort, PushSpec,
    };
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    static SQLITE_LOG_FIXTURE: AtomicU64 = AtomicU64::new(0);

    fn fixture_root(label: &str) -> PathBuf {
        let n = SQLITE_LOG_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "fireweed-sqlite-log-{label}-{}-{n}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("sqlite log fixture root");
        path
    }

    fn cleanup_sqlite_files(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    fn cleanup_root(root: &Path) {
        let _ = std::fs::remove_dir_all(root);
    }

    fn evidence_output(base: &Path, variable: &str, filename: &str) -> fireweed_release::RunOwned {
        let repository_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("resolve repository root");
        let requested = std::env::var_os(variable)
            .map(PathBuf::from)
            .unwrap_or_else(|| base.join(filename));
        let run_root = requested
            .parent()
            .expect("TP-003 output requires a parent directory");
        fireweed_release::RunOwned::new(repository_root, run_root, &requested)
            .expect("authorize run-owned TP-003 output")
    }

    /// T0: composition root wires all three sqlite-log × projection cells.
    #[test]
    fn sqlite_log_composition_root_wires_three_projection_cells() {
        let source = include_str!("lib.rs");
        assert!(
            source.contains("LogSpec::Sqlite { path }, ProjectionSpec::InMemory"),
            "server match arm for sqlite×memory must exist"
        );
        assert!(
            source.contains("composed_sqlite_backend_for_worker"),
            "sqlite×memory must use composed sqlite log + in-memory projection pool"
        );
        assert!(
            source.contains("LogSpec::Sqlite { path }, ProjectionSpec::Sqlite"),
            "server match arm for sqlite×sqlite must exist"
        );
        assert!(
            source.contains("composed_sqlite_log_sqlite_projection"),
            "sqlite×sqlite must use composed_sqlite_log_sqlite_projection"
        );
        assert!(
            source.contains("LogSpec::Sqlite { path }, ProjectionSpec::Postgres"),
            "server match arm for sqlite×postgres must exist (feature postgres)"
        );
        assert!(
            source.contains("PostgresRelational::connect"),
            "sqlite×postgres must compose SqliteLog with PostgresRelational"
        );
    }

    /// Shared T1 lifecycle body: create → push → claim → finalize → metrics.
    async fn lifecycle_push_claim_complete<B>(backend: &B, cell: &str)
    where
        B: fireweed_engine::Backend
            + fireweed_engine::ControlPlaneStore
            + ProjectionRead
            + PushPort
            + ClaimPort
            + FinalizePort,
    {
        backend
            .create_queue(qdef())
            .await
            .unwrap_or_else(|e| panic!("{cell} T1 create_queue: {e:?}"));
        let pushed = backend
            .push(&shard(), vec![PushSpec::default()], ts(1), None)
            .await
            .unwrap_or_else(|e| panic!("{cell} T1 push: {e:?}"));
        assert_eq!(pushed.len(), 1, "{cell} T1 push count");
        let claimed = backend
            .claim(claim_req(1, 30_000, 1))
            .await
            .unwrap_or_else(|e| panic!("{cell} T1 claim: {e:?}"));
        assert_eq!(claimed.items.len(), 1, "{cell} T1 claim count");
        assert_eq!(claimed.items[0].item_id, pushed[0]);
        backend
            .finalize(
                &shard(),
                vec![FinalizeOutcome::new(
                    claimed.items[0].item_id,
                    FinalizeKind::Complete,
                )],
                ts(2),
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{cell} T1 finalize: {e:?}"));
        let m = backend
            .metrics(&shard())
            .await
            .unwrap_or_else(|e| panic!("{cell} T1 metrics: {e:?}"));
        assert_eq!(m.pending, 0, "{cell} T1 pending after complete");
        assert_eq!(m.complete, 1, "{cell} T1 complete count");
    }

    /// T0–T2: sqlite×memory — durable log, in-memory projection; reopen recovers via log.
    #[tokio::test]
    async fn sqlite_log_memory_lifecycle_and_reopen() {
        let root = fixture_root("memory");
        let log_path = root.join("log.db");
        let log_s = log_path.to_str().unwrap();
        let cell = "sqlite×memory";

        {
            let backend = fireweed_sqlite::composed_sqlite_backend(log_s)
                .unwrap_or_else(|e| panic!("{cell} T0 open: {e:?}"));
            lifecycle_push_claim_complete(&backend, cell).await;
            // Seed pending for T2.
            let pending = backend
                .push(&shard(), vec![PushSpec::default()], ts(10), None)
                .await
                .expect("T2 seed push");
            assert_eq!(pending.len(), 1);
            assert_eq!(
                backend.metrics(&shard()).await.unwrap().pending,
                1,
                "{cell}: seed pending before drop"
            );
            drop(backend);
        }

        // T2 Class A reopen: same durable log path, fresh in-memory projection rebuilt from log.
        let reopened = fireweed_sqlite::composed_sqlite_backend(log_s)
            .unwrap_or_else(|e| panic!("{cell} T2 reopen: {e:?}"));
        assert_eq!(
            reopened.metrics(&shard()).await.unwrap().pending,
            1,
            "{cell} T2 Class A: durable log recovers 1 pending"
        );
        let claimed = reopened
            .claim(claim_req(1, 40_000, 20))
            .await
            .expect("T2 claim");
        assert_eq!(claimed.items.len(), 1, "{cell} T2 claim");
        reopened
            .finalize(
                &shard(),
                vec![FinalizeOutcome::new(
                    claimed.items[0].item_id,
                    FinalizeKind::Complete,
                )],
                ts(21),
                None,
            )
            .await
            .expect("T2 finalize");
        assert_eq!(reopened.metrics(&shard()).await.unwrap().pending, 0);
        drop(reopened);
        cleanup_root(&root);
    }

    /// T0–T2: sqlite×sqlite — distinct log + projection paths; reopen recovers via log (+ projection HW).
    #[tokio::test]
    async fn sqlite_log_sqlite_lifecycle_and_reopen() {
        let root = fixture_root("sqlite");
        let log_path = root.join("log.db");
        let proj_path = root.join("projection.db");
        let log_s = log_path.to_str().unwrap();
        let proj_s = proj_path.to_str().unwrap();
        assert_ne!(log_s, proj_s);
        let cell = "sqlite×sqlite";

        {
            let backend = fireweed_sqlite::composed_sqlite_log_sqlite_projection(log_s, proj_s)
                .unwrap_or_else(|e| panic!("{cell} T0 open: {e:?}"));
            lifecycle_push_claim_complete(&backend, cell).await;
            let pending = backend
                .push(&shard(), vec![PushSpec::default()], ts(10), None)
                .await
                .expect("T2 seed push");
            assert_eq!(pending.len(), 1);
            assert_eq!(backend.metrics(&shard()).await.unwrap().pending, 1);
            drop(backend);
        }

        let reopened = fireweed_sqlite::composed_sqlite_log_sqlite_projection(log_s, proj_s)
            .unwrap_or_else(|e| panic!("{cell} T2 reopen: {e:?}"));
        assert_eq!(
            reopened.metrics(&shard()).await.unwrap().pending,
            1,
            "{cell} T2 Class A: expected 1 pending after reopen"
        );
        let claimed = reopened
            .claim(claim_req(1, 40_000, 20))
            .await
            .expect("T2 claim");
        assert_eq!(claimed.items.len(), 1);
        reopened
            .finalize(
                &shard(),
                vec![FinalizeOutcome::new(
                    claimed.items[0].item_id,
                    FinalizeKind::Complete,
                )],
                ts(21),
                None,
            )
            .await
            .expect("T2 finalize");
        drop(reopened);
        cleanup_root(&root);
    }

    /// T0–T2: sqlite×postgres via product `open_async(StorageConfig)` (Tokio-safe).
    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn sqlite_log_postgres_lifecycle_and_reopen() {
        let Ok(url) = std::env::var("FIREWEED_PG_TEST_URL") else {
            eprintln!(
                "sqlite_log_postgres_lifecycle_and_reopen SKIPPED — set FIREWEED_PG_TEST_URL"
            );
            return;
        };
        use fireweed::{
            ConfigSecret, LogConfig, NewItem, ProjectionStoreConfig, RecoveryPolicy,
            ResponseBarrier, SegmentConfig, StorageConfig, SystemClock, open_async,
        };
        let cell = "sqlite×postgres";
        let root = fixture_root("postgres");
        let clock = Arc::new(SystemClock);
        let cfg = StorageConfig {
            log: LogConfig::Sqlite {
                path: root.join("log.db"),
            },
            projection: ProjectionStoreConfig::Postgres {
                url: ConfigSecret::new(url),
            },
            control_plane: None,
            authority: None,
            response_barrier: ResponseBarrier::Strict,
            segments: SegmentConfig::new(1024 * 1024, 5).expect("segments"),
            namespace: format!("server_sqlite_pg_life_{}", std::process::id()),
            recovery: RecoveryPolicy::default(),
        };
        cfg.validate().expect("validate");
        let def = qdef();
        let key = shard();

        {
            let fireweed = open_async(cfg.clone(), Arc::clone(&clock) as _)
                .await
                .unwrap_or_else(|e| panic!("{cell} T0 open: {e:?}"));
            fireweed.create_queue(def.clone()).await.expect("create");
            let id = fireweed.push(&key, NewItem::default()).await.expect("push");
            let claimed = fireweed.claim(&key, 1, 30_000).await.expect("claim");
            assert_eq!(claimed.len(), 1);
            assert_eq!(claimed[0].item_id, id);
            fireweed
                .complete(&key, claimed.iter().map(|c| c.item_id))
                .await
                .expect("complete");
            let _ = fireweed.push(&key, NewItem::default()).await.expect("seed");
            assert_eq!(fireweed.metrics(&key).await.expect("m").pending, 1);
            std::thread::spawn(move || drop(fireweed))
                .join()
                .expect("drop open handle");
        }

        {
            let reopened = open_async(cfg, Arc::clone(&clock) as _)
                .await
                .unwrap_or_else(|e| panic!("{cell} T2 reopen: {e:?}"));
            assert_eq!(
                reopened.metrics(&key).await.expect("m").pending,
                1,
                "{cell} T2 Class A: pending recovers via durable sqlite log"
            );
            let claimed = reopened.claim(&key, 1, 30_000).await.expect("claim");
            assert_eq!(claimed.len(), 1);
            reopened
                .complete(&key, claimed.iter().map(|c| c.item_id))
                .await
                .expect("finalize");
            std::thread::spawn(move || drop(reopened))
                .join()
                .expect("drop reopen handle");
        }

        cleanup_root(&root);
    }

    fn record_outcome(
        records: &mut Vec<AcEvidence>,
        failures: &mut Vec<String>,
        ac: &'static str,
        backend: &str,
        outcome: Result<Vec<String>, String>,
    ) {
        match outcome {
            Ok(assertions) => {
                let partial = assertions.iter().any(|a| a.contains("GAP"));
                records.push(AcEvidence {
                    ac,
                    backend: backend.to_string(),
                    result: if partial { "partial" } else { "pass" },
                    detail: String::new(),
                    assertions,
                });
            }
            Err(reason) => {
                failures.push(format!("{ac} [{backend}]: {reason}"));
                records.push(AcEvidence {
                    ac,
                    backend: backend.to_string(),
                    result: "fail",
                    detail: reason,
                    assertions: vec![],
                });
            }
        }
    }

    /// T3: TP-003 AC-TXN-1/2/3 for exact sqlite-log storage pairs; writes axis-named evidence.
    ///
    /// - `sqlite×memory` — product adapter `composed_sqlite_backend` (server sqlite×InMemory arm)
    /// - `sqlite×sqlite` — product adapter `composed_sqlite_log_sqlite_projection`
    /// - `sqlite×postgres` — env-gated live Postgres projection (same as server arm)
    #[test]
    fn sqlite_log_t3_tp003_ac_txn_exact_pairs() {
        const DURABLE: TxnCaps = TxnCaps {
            durable_reopen: true,
        };
        let mut records: Vec<AcEvidence> = Vec::new();
        let mut failures: Vec<String> = Vec::new();
        let base = fixture_root("t3");

        // --- sqlite×memory ---
        {
            let cell = "sqlite×memory";
            let cell_base = base.join("memory");
            std::fs::create_dir_all(&cell_base).unwrap();
            let make = |tag: &str| {
                let path = cell_base.join(format!("{tag}.db"));
                fireweed_sqlite::composed_sqlite_backend(path.to_str().unwrap())
                    .expect("open sqlite×memory")
            };
            record_outcome(
                &mut records,
                &mut failures,
                "AC-TXN-1",
                cell,
                futures::executor::block_on(ac_txn_1_success_durable_visible(make)),
            );
            let make = |tag: &str| {
                let path = cell_base.join(format!("{tag}.db"));
                fireweed_sqlite::composed_sqlite_backend(path.to_str().unwrap())
                    .expect("open sqlite×memory")
            };
            record_outcome(
                &mut records,
                &mut failures,
                "AC-TXN-2",
                cell,
                futures::executor::block_on(ac_txn_2_rejection_no_effect(make, DURABLE)),
            );
            let make = |tag: &str| {
                let path = cell_base.join(format!("{tag}.db"));
                fireweed_sqlite::composed_sqlite_backend(path.to_str().unwrap())
                    .expect("open sqlite×memory")
            };
            record_outcome(
                &mut records,
                &mut failures,
                "AC-TXN-3",
                cell,
                futures::executor::block_on(ac_txn_3_unknown_outcome_replay(make, DURABLE)),
            );
        }

        // --- sqlite×sqlite ---
        {
            let cell = "sqlite×sqlite";
            let cell_base = base.join("sqlite");
            std::fs::create_dir_all(&cell_base).unwrap();
            let make = |tag: &str| {
                let log = cell_base.join(format!("{tag}-log.db"));
                let proj = cell_base.join(format!("{tag}-proj.db"));
                fireweed_sqlite::composed_sqlite_log_sqlite_projection(
                    log.to_str().unwrap(),
                    proj.to_str().unwrap(),
                )
                .expect("open sqlite×sqlite")
            };
            record_outcome(
                &mut records,
                &mut failures,
                "AC-TXN-1",
                cell,
                futures::executor::block_on(ac_txn_1_success_durable_visible(make)),
            );
            let make = |tag: &str| {
                let log = cell_base.join(format!("{tag}-log.db"));
                let proj = cell_base.join(format!("{tag}-proj.db"));
                fireweed_sqlite::composed_sqlite_log_sqlite_projection(
                    log.to_str().unwrap(),
                    proj.to_str().unwrap(),
                )
                .expect("open sqlite×sqlite")
            };
            record_outcome(
                &mut records,
                &mut failures,
                "AC-TXN-2",
                cell,
                futures::executor::block_on(ac_txn_2_rejection_no_effect(make, DURABLE)),
            );
            let make = |tag: &str| {
                let log = cell_base.join(format!("{tag}-log.db"));
                let proj = cell_base.join(format!("{tag}-proj.db"));
                fireweed_sqlite::composed_sqlite_log_sqlite_projection(
                    log.to_str().unwrap(),
                    proj.to_str().unwrap(),
                )
                .expect("open sqlite×sqlite")
            };
            record_outcome(
                &mut records,
                &mut failures,
                "AC-TXN-3",
                cell,
                futures::executor::block_on(ac_txn_3_unknown_outcome_replay(make, DURABLE)),
            );
        }

        // --- sqlite×postgres (live fixture) ---
        #[cfg(feature = "postgres")]
        if let Ok(url) = std::env::var("FIREWEED_PG_TEST_URL") {
            let cell = "sqlite×postgres";
            let cell_base = base.join("postgres");
            std::fs::create_dir_all(&cell_base).unwrap();
            let run = SQLITE_LOG_FIXTURE.fetch_add(1, Ordering::Relaxed);
            let url_c = url.clone();
            let make = |tag: &str| {
                let log_path = cell_base.join(format!("{tag}-log.db"));
                let schema = format!(
                    "fw_sqlite_pg_t3_{}_{}_{}",
                    std::process::id(),
                    run,
                    tag.replace('-', "_")
                );
                let mut client = fireweed_postgres::connect(
                    fireweed_postgres::PostgresConnectConfig::new(&url_c),
                )
                .expect("connect for schema");
                client
                    .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema};"))
                    .expect("create schema");
                drop(client);
                let scoped = if url_c.contains('?') {
                    format!("{url_c}&options=-csearch_path%3D{schema}")
                } else {
                    format!("{url_c}?options=-csearch_path%3D{schema}")
                };
                let log = fireweed_sqlite::SqliteLog::open(log_path.to_str().unwrap())
                    .expect("open sqlite log");
                let projection = fireweed_postgres::PostgresRelational::connect(&scoped)
                    .expect("connect pg projection");
                assemble_async_log_replay(log, projection, 0)
                    .expect("assemble async log-replay")
                    .recover()
                    .expect("recover sqlite×postgres")
            };
            record_outcome(
                &mut records,
                &mut failures,
                "AC-TXN-1",
                cell,
                futures::executor::block_on(ac_txn_1_success_durable_visible(make)),
            );
            let make = |tag: &str| {
                let log_path = cell_base.join(format!("{tag}-log.db"));
                let schema = format!(
                    "fw_sqlite_pg_t3_{}_{}_{}",
                    std::process::id(),
                    run,
                    tag.replace('-', "_")
                );
                let mut client = fireweed_postgres::connect(
                    fireweed_postgres::PostgresConnectConfig::new(&url_c),
                )
                .expect("connect for schema");
                client
                    .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema};"))
                    .expect("create schema");
                drop(client);
                let scoped = if url_c.contains('?') {
                    format!("{url_c}&options=-csearch_path%3D{schema}")
                } else {
                    format!("{url_c}?options=-csearch_path%3D{schema}")
                };
                let log = fireweed_sqlite::SqliteLog::open(log_path.to_str().unwrap())
                    .expect("open sqlite log");
                let projection = fireweed_postgres::PostgresRelational::connect(&scoped)
                    .expect("connect pg projection");
                assemble_async_log_replay(log, projection, 0)
                    .expect("assemble async log-replay")
                    .recover()
                    .expect("recover sqlite×postgres")
            };
            record_outcome(
                &mut records,
                &mut failures,
                "AC-TXN-2",
                cell,
                futures::executor::block_on(ac_txn_2_rejection_no_effect(make, DURABLE)),
            );
            let make = |tag: &str| {
                let log_path = cell_base.join(format!("{tag}-log.db"));
                let schema = format!(
                    "fw_sqlite_pg_t3_{}_{}_{}",
                    std::process::id(),
                    run,
                    tag.replace('-', "_")
                );
                let mut client = fireweed_postgres::connect(
                    fireweed_postgres::PostgresConnectConfig::new(&url_c),
                )
                .expect("connect for schema");
                client
                    .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema};"))
                    .expect("create schema");
                drop(client);
                let scoped = if url_c.contains('?') {
                    format!("{url_c}&options=-csearch_path%3D{schema}")
                } else {
                    format!("{url_c}?options=-csearch_path%3D{schema}")
                };
                let log = fireweed_sqlite::SqliteLog::open(log_path.to_str().unwrap())
                    .expect("open sqlite log");
                let projection = fireweed_postgres::PostgresRelational::connect(&scoped)
                    .expect("connect pg projection");
                assemble_async_log_replay(log, projection, 0)
                    .expect("assemble async log-replay")
                    .recover()
                    .expect("recover sqlite×postgres")
            };
            record_outcome(
                &mut records,
                &mut failures,
                "AC-TXN-3",
                cell,
                futures::executor::block_on(ac_txn_3_unknown_outcome_replay(make, DURABLE)),
            );
        } else {
            eprintln!(
                "sqlite_log T3 ordinary route did not receive a PostgreSQL fixture; \
                 AC-TXN-1/2/3 executed for sqlite×memory and sqlite×sqlite"
            );
            assert!(
                std::env::var_os("FIREWEED_TP003_SQLITE_EVIDENCE_OUT").is_none(),
                "governed sqlite TP-003 evidence requires FIREWEED_PG_TEST_URL"
            );
        }

        #[cfg(not(feature = "postgres"))]
        {
            assert!(
                std::env::var_os("FIREWEED_TP003_SQLITE_EVIDENCE_OUT").is_none(),
                "governed sqlite TP-003 evidence requires the postgres feature"
            );
        }

        let output = evidence_output(
            &base,
            "FIREWEED_TP003_SQLITE_EVIDENCE_OUT",
            "tp003-ac-txn-matrix-sqlite-storage-pairs.jsonl",
        );
        output
            .write(render_evidence(&records))
            .expect("write run-owned sqlite storage-pair TP-003 evidence");
        eprintln!(
            "sqlite_log T3 TP-003 evidence written to {} ({} rows)",
            output.path().display(),
            records.len()
        );
        cleanup_root(&base);
        assert!(
            failures.is_empty(),
            "sqlite log TP-003 exact-pair failures:\n{}",
            failures.join("\n")
        );
    }

    /// T3 linkage: the immutable test fixture uses exact sqlite axis names. Live evidence is produced
    /// separately into a run-owned output by `sqlite_log_t3_tp003_ac_txn_exact_pairs`.
    #[test]
    fn sqlite_log_t3_evidence_axis_names_file_contract() {
        let fixture = fireweed_release::Fixture::new(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/tp003-sqlite-axis.jsonl"),
        )
        .expect("open immutable sqlite axis fixture");
        let body = std::fs::read_to_string(
            fixture
                .authorize(fireweed_release::EvidenceOperation::Read)
                .expect("fixture authorizes reads"),
        )
        .expect("read sqlite axis fixture");
        assert!(
            body.contains("sqlite×memory") || body.contains("sqlite\\u00d7memory"),
            "evidence must name sqlite×memory axis"
        );
        assert!(
            body.contains("sqlite×sqlite") || body.contains("sqlite\\u00d7sqlite"),
            "evidence must name sqlite×sqlite axis"
        );
        assert!(
            body.contains("sqlite×postgres") || body.contains("sqlite\\u00d7postgres"),
            "evidence must name sqlite×postgres axis"
        );
    }

    /// T4: chart-installable sqlite-log cells have CI values files and helm-gate registration.
    #[test]
    fn sqlite_log_t4_helm_ci_values_and_gate() {
        let chart_ci =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../charts/fireweed-queue/ci");
        for name in [
            "sqlite-memory-values.yaml",
            "sqlite-sqlite-values.yaml",
            "sqlite-postgres-values.yaml",
        ] {
            let p = chart_ci.join(name);
            assert!(
                p.is_file(),
                "T4: missing Helm CI values for sqlite log cell: {}",
                p.display()
            );
            let body = std::fs::read_to_string(&p).unwrap();
            assert!(
                body.contains("backend: sqlite"),
                "{} must select storage.log.backend=sqlite",
                name
            );
        }

        let gate = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../scripts/ci/helm-gate.sh");
        let gate_body = std::fs::read_to_string(&gate).expect("read helm-gate.sh");
        for combo in ["sqlite-memory", "sqlite-sqlite", "sqlite-postgres"] {
            assert!(
                gate_body.contains(combo),
                "helm-gate.sh must register combination {combo}"
            );
        }

        // Optional live helm template when helm is on PATH (kind smoke is CI-side; static render here).
        if std::process::Command::new("helm")
            .arg("version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
        {
            let chart =
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../charts/fireweed-queue");
            for (combo, values) in [
                ("sqlite-memory", "sqlite-memory-values.yaml"),
                ("sqlite-sqlite", "sqlite-sqlite-values.yaml"),
                ("sqlite-postgres", "sqlite-postgres-values.yaml"),
            ] {
                let values_path = chart.join("ci").join(values);
                let out = std::process::Command::new("helm")
                    .args([
                        "template",
                        &format!("fireweed-{combo}"),
                        chart.to_str().unwrap(),
                        "--values",
                        values_path.to_str().unwrap(),
                    ])
                    .output()
                    .expect("helm template");
                assert!(
                    out.status.success(),
                    "T4 helm template {combo} failed:\n{}",
                    String::from_utf8_lossy(&out.stderr)
                );
                let rendered = String::from_utf8_lossy(&out.stdout);
                assert!(
                    rendered.contains("FIREWEED_LOG_BACKEND: \"sqlite\""),
                    "{combo} render must set FIREWEED_LOG_BACKEND=sqlite"
                );
            }
        } else {
            eprintln!(
                "sqlite_log T4 helm template skipped (helm not on PATH); values+gate checked"
            );
        }
    }

    // Silence unused when postgres feature off / cleanup helper retained for all arms.
    #[allow(dead_code)]
    fn _sqlite_log_cleanup_helpers() {
        let p = PathBuf::from("/tmp/__none__");
        cleanup_sqlite_files(&p);
    }
}

/// Class A **postgres log** matrix cells (brief §1.1 / §2): `postgres×memory`, `postgres×sqlite`,
/// `postgres×postgres`.
///
/// | Layer | Coverage in this module |
/// |-------|-------------------------|
/// | **T0 Construct** | composition-root arms + open via product adapters |
/// | **T1 Lifecycle** | create_queue → push → claim → finalize |
/// | **T2 Reopen** | Class A: pending survives process-local drop+reopen via durable log |
/// | **T3 Contract** | TP-003 AC-TXN-1/2/3 for exact pairs → explicit run-owned JSONL |
/// | **T4 Deploy** | Helm CI values under `charts/fireweed-queue/ci/postgres-*-values.yaml` (+ helm-gate) |
///
/// Live fixtures: every cell needs `FIREWEED_PG_TEST_URL` (and `--features postgres`). An explicit
/// governed evidence output fails closed without that URL; the ordinary route asserts the missing-input
/// boundary and the immutable axis fixture keeps name-contract coverage deterministic.
#[cfg(test)]
mod postgres_log_matrix_tests {
    // Helpers and AC-TXN imports are only exercised under `--features postgres`.
    // Default clippy/test builds still compile the module (composition-root source
    // asserts + not-postgres stubs) and must not trip unused/dead-code lint.
    #![allow(dead_code, unused_imports)]

    use super::*;
    use fireweed_conformance::fault::{
        AcEvidence, TxnCaps, ac_txn_1_success_durable_visible, ac_txn_2_rejection_no_effect,
        ac_txn_3_unknown_outcome_replay, render_evidence,
    };
    use fireweed_conformance::{claim_req, qdef, shard, ts};
    use fireweed_engine::{
        ClaimPort, FinalizeKind, FinalizeOutcome, FinalizePort, ProjectionRead, PushPort, PushSpec,
    };
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    static POSTGRES_LOG_FIXTURE: AtomicU64 = AtomicU64::new(0);

    fn fixture_root(label: &str) -> PathBuf {
        let n = POSTGRES_LOG_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "fireweed-postgres-log-{label}-{}-{n}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("postgres log fixture root");
        path
    }

    fn cleanup_root(root: &Path) {
        let _ = std::fs::remove_dir_all(root);
    }

    fn evidence_output(base: &Path, variable: &str, filename: &str) -> fireweed_release::RunOwned {
        let repository_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("resolve repository root");
        let requested = std::env::var_os(variable)
            .map(PathBuf::from)
            .unwrap_or_else(|| base.join(filename));
        let run_root = requested
            .parent()
            .expect("TP-003 output requires a parent directory");
        fireweed_release::RunOwned::new(repository_root, run_root, &requested)
            .expect("authorize run-owned TP-003 output")
    }

    fn pg_url() -> Option<String> {
        std::env::var("FIREWEED_PG_TEST_URL").ok()
    }

    fn schema_name(prefix: &str) -> String {
        let n = POSTGRES_LOG_FIXTURE.fetch_add(1, Ordering::Relaxed);
        format!("fw_pg_log_{}_{}_{}", prefix, std::process::id(), n)
    }

    /// T0: composition root wires all three postgres-log × projection cells.
    #[test]
    fn postgres_log_composition_root_wires_three_projection_cells() {
        let source = include_str!("lib.rs");
        assert!(
            source.contains("LogSpec::Postgres { url, credentials }, ProjectionSpec::InMemory"),
            "server match arm for postgres×memory must exist"
        );
        assert!(
            source.contains("composed_postgres_backend_for_worker_with_config"),
            "postgres×memory must use composed postgres log + in-memory projection pool"
        );
        assert!(
            source.contains("LogSpec::Postgres { url, credentials }, ProjectionSpec::Sqlite"),
            "server match arm for postgres×sqlite must exist"
        );
        assert!(
            source.contains("PostgresLog::connect_with_config")
                && source.contains("SqliteProjectionStore::open"),
            "postgres×sqlite must compose PostgresLog with SqliteProjectionStore"
        );
        assert!(
            source.contains("ProjectionSpec::Postgres")
                && source.contains("fixed_postgres_relational_pool"),
            "server match arm for postgres×postgres must use fixed_postgres_relational_pool"
        );
    }

    /// Shared T1 lifecycle body: create → push → claim → finalize → metrics.
    async fn lifecycle_push_claim_complete<B>(backend: &B, cell: &str)
    where
        B: fireweed_engine::Backend
            + fireweed_engine::ControlPlaneStore
            + ProjectionRead
            + PushPort
            + ClaimPort
            + FinalizePort,
    {
        backend
            .create_queue(qdef())
            .await
            .unwrap_or_else(|e| panic!("{cell} T1 create_queue: {e:?}"));
        let pushed = backend
            .push(&shard(), vec![PushSpec::default()], ts(1), None)
            .await
            .unwrap_or_else(|e| panic!("{cell} T1 push: {e:?}"));
        assert_eq!(pushed.len(), 1, "{cell} T1 push count");
        let claimed = backend
            .claim(claim_req(1, 30_000, 1))
            .await
            .unwrap_or_else(|e| panic!("{cell} T1 claim: {e:?}"));
        assert_eq!(claimed.items.len(), 1, "{cell} T1 claim count");
        assert_eq!(claimed.items[0].item_id, pushed[0]);
        backend
            .finalize(
                &shard(),
                vec![FinalizeOutcome::new(
                    claimed.items[0].item_id,
                    FinalizeKind::Complete,
                )],
                ts(2),
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{cell} T1 finalize: {e:?}"));
        let m = backend
            .metrics(&shard())
            .await
            .unwrap_or_else(|e| panic!("{cell} T1 metrics: {e:?}"));
        assert_eq!(m.pending, 0, "{cell} T1 pending after complete");
        assert_eq!(m.complete, 1, "{cell} T1 complete count");
    }

    /// T0–T2: postgres×memory — durable log, in-memory projection; reopen recovers via log.
    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_log_memory_lifecycle_and_reopen() {
        let Some(url) = pg_url() else {
            eprintln!(
                "postgres_log_memory_lifecycle_and_reopen SKIPPED — set FIREWEED_PG_TEST_URL \
                 (cell postgres×memory remains registered)"
            );
            return;
        };
        let cell = "postgres×memory";
        let schema = schema_name("mem");
        {
            let backend = fireweed_postgres::composed_postgres_backend_in_schema(&url, &schema)
                .unwrap_or_else(|e| panic!("{cell} T0 open: {e:?}"));
            futures::executor::block_on(async {
                lifecycle_push_claim_complete(&backend, cell).await;
                let pending = backend
                    .push(&shard(), vec![PushSpec::default()], ts(10), None)
                    .await
                    .expect("T2 seed push");
                assert_eq!(pending.len(), 1);
                assert_eq!(
                    backend.metrics(&shard()).await.unwrap().pending,
                    1,
                    "{cell}: seed pending before drop"
                );
            });
            drop(backend);
        }

        // T2 Class A reopen: same durable log schema, fresh in-memory projection rebuilt from log.
        let reopened = fireweed_postgres::composed_postgres_backend_in_schema(&url, &schema)
            .unwrap_or_else(|e| panic!("{cell} T2 reopen: {e:?}"));
        futures::executor::block_on(async {
            assert_eq!(
                reopened.metrics(&shard()).await.unwrap().pending,
                1,
                "{cell} T2 Class A: durable log recovers 1 pending"
            );
            let claimed = reopened
                .claim(claim_req(1, 40_000, 20))
                .await
                .expect("T2 claim");
            assert_eq!(claimed.items.len(), 1, "{cell} T2 claim");
            reopened
                .finalize(
                    &shard(),
                    vec![FinalizeOutcome::new(
                        claimed.items[0].item_id,
                        FinalizeKind::Complete,
                    )],
                    ts(21),
                    None,
                )
                .await
                .expect("T2 finalize");
            assert_eq!(reopened.metrics(&shard()).await.unwrap().pending, 0);
        });
        drop(reopened);

        if let Ok(mut client) =
            fireweed_postgres::connect(fireweed_postgres::PostgresConnectConfig::new(&url))
        {
            let _ = client.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE;"));
        }
    }

    /// T0–T2: postgres×sqlite — durable postgres log + file-backed sqlite projection.
    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_log_sqlite_lifecycle_and_reopen() {
        let Some(url) = pg_url() else {
            eprintln!(
                "postgres_log_sqlite_lifecycle_and_reopen SKIPPED — set FIREWEED_PG_TEST_URL \
                 (cell postgres×sqlite remains registered)"
            );
            return;
        };
        let cell = "postgres×sqlite";
        let schema = schema_name("sqlite");
        let root = fixture_root("sqlite");
        let proj_path = root.join("projection.db");
        let proj_s = proj_path.to_str().unwrap().to_string();
        {
            let log = fireweed_postgres::PostgresLog::connect_in_schema(&url, &schema)
                .expect("connect postgres log");
            let projection = fireweed_sqlite::SqliteProjectionStore::open(&proj_s)
                .expect("open sqlite projection");
            let backend = assemble_async_log_replay(log, projection, 0)
                .expect("assemble async log-replay")
                .recover()
                .unwrap_or_else(|e| panic!("{cell} T0 recover: {e:?}"));
            futures::executor::block_on(async {
                lifecycle_push_claim_complete(&backend, cell).await;
                let pending = backend
                    .push(&shard(), vec![PushSpec::default()], ts(10), None)
                    .await
                    .expect("T2 seed");
                assert_eq!(pending.len(), 1);
                assert_eq!(backend.metrics(&shard()).await.unwrap().pending, 1);
            });
            drop(backend);
        }

        {
            let log = fireweed_postgres::PostgresLog::connect_in_schema(&url, &schema)
                .expect("reconnect postgres log");
            let projection = fireweed_sqlite::SqliteProjectionStore::open(&proj_s)
                .expect("reopen sqlite projection");
            let reopened = assemble_async_log_replay(log, projection, 0)
                .expect("assemble async log-replay")
                .recover()
                .unwrap_or_else(|e| panic!("{cell} T2 reopen: {e:?}"));
            futures::executor::block_on(async {
                assert_eq!(
                    reopened.metrics(&shard()).await.unwrap().pending,
                    1,
                    "{cell} T2 Class A: pending recovers via durable postgres log"
                );
                let claimed = reopened
                    .claim(claim_req(1, 40_000, 20))
                    .await
                    .expect("T2 claim");
                assert_eq!(claimed.items.len(), 1);
                reopened
                    .finalize(
                        &shard(),
                        vec![FinalizeOutcome::new(
                            claimed.items[0].item_id,
                            FinalizeKind::Complete,
                        )],
                        ts(21),
                        None,
                    )
                    .await
                    .expect("T2 finalize");
            });
            drop(reopened);
        }

        cleanup_root(&root);
        if let Ok(mut client) =
            fireweed_postgres::connect(fireweed_postgres::PostgresConnectConfig::new(&url))
        {
            let _ = client.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE;"));
        }
    }

    /// T0–T2: postgres×postgres — product unified relational backend (server arm).
    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_log_postgres_lifecycle_and_reopen() {
        let Some(url) = pg_url() else {
            eprintln!(
                "postgres_log_postgres_lifecycle_and_reopen SKIPPED — set FIREWEED_PG_TEST_URL \
                 (cell postgres×postgres remains registered)"
            );
            return;
        };
        let cell = "postgres×postgres";
        let schema = schema_name("pgpg");
        {
            let backend =
                fireweed_postgres::PostgresRelationalBackend::connect_in_schema(&url, &schema)
                    .unwrap_or_else(|e| panic!("{cell} T0 open: {e:?}"));
            futures::executor::block_on(async {
                lifecycle_push_claim_complete(&backend, cell).await;
                let pending = backend
                    .push(&shard(), vec![PushSpec::default()], ts(10), None)
                    .await
                    .expect("T2 seed");
                assert_eq!(pending.len(), 1);
                assert_eq!(backend.metrics(&shard()).await.unwrap().pending, 1);
            });
            drop(backend);
        }

        {
            let reopened =
                fireweed_postgres::PostgresRelationalBackend::connect_in_schema(&url, &schema)
                    .unwrap_or_else(|e| panic!("{cell} T2 reopen: {e:?}"));
            futures::executor::block_on(async {
                assert_eq!(
                    reopened.metrics(&shard()).await.unwrap().pending,
                    1,
                    "{cell} T2 Class A: pending recovers via durable postgres relational store"
                );
                let claimed = reopened
                    .claim(claim_req(1, 40_000, 20))
                    .await
                    .expect("T2 claim");
                assert_eq!(claimed.items.len(), 1);
                reopened
                    .finalize(
                        &shard(),
                        vec![FinalizeOutcome::new(
                            claimed.items[0].item_id,
                            FinalizeKind::Complete,
                        )],
                        ts(21),
                        None,
                    )
                    .await
                    .expect("T2 finalize");
            });
            drop(reopened);
        }

        if let Ok(mut client) =
            fireweed_postgres::connect(fireweed_postgres::PostgresConnectConfig::new(&url))
        {
            let _ = client.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE;"));
        }
    }

    fn record_outcome(
        records: &mut Vec<AcEvidence>,
        failures: &mut Vec<String>,
        ac: &'static str,
        backend: &str,
        outcome: Result<Vec<String>, String>,
    ) {
        match outcome {
            Ok(assertions) => {
                let partial = assertions.iter().any(|a| a.contains("GAP"));
                records.push(AcEvidence {
                    ac,
                    backend: backend.to_string(),
                    result: if partial { "partial" } else { "pass" },
                    detail: String::new(),
                    assertions,
                });
            }
            Err(reason) => {
                failures.push(format!("{ac} [{backend}]: {reason}"));
                records.push(AcEvidence {
                    ac,
                    backend: backend.to_string(),
                    result: "fail",
                    detail: reason,
                    assertions: vec![],
                });
            }
        }
    }

    /// T3: TP-003 AC-TXN-1/2/3 for exact postgres-log storage pairs; writes axis-named evidence.
    ///
    /// - `postgres×memory` — product adapter `composed_postgres_backend_in_schema`
    /// - `postgres×sqlite` — product composition `PostgresLog` + `SqliteProjectionStore`
    /// - `postgres×postgres` — product composition matching exact-pair conformance factory
    ///   (independent log + projection schemas; same types as server-facing pair evidence)
    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_log_t3_tp003_ac_txn_exact_pairs() {
        const DURABLE: TxnCaps = TxnCaps {
            durable_reopen: true,
        };
        let mut records: Vec<AcEvidence> = Vec::new();
        let mut failures: Vec<String> = Vec::new();
        let base = fixture_root("t3");

        let Some(url) = pg_url() else {
            assert!(
                std::env::var_os("FIREWEED_TP003_POSTGRES_EVIDENCE_OUT").is_none(),
                "governed postgres TP-003 evidence requires FIREWEED_PG_TEST_URL"
            );
            cleanup_root(&base);
            return;
        };

        // --- postgres×memory ---
        {
            let cell = "postgres×memory";
            let run = POSTGRES_LOG_FIXTURE.fetch_add(1, Ordering::Relaxed);
            let url_c = url.clone();
            let make = |tag: &str| {
                let schema = format!(
                    "fw_pg_mem_t3_{}_{}_{}",
                    std::process::id(),
                    run,
                    tag.replace('-', "_")
                );
                fireweed_postgres::composed_postgres_backend_in_schema(&url_c, &schema)
                    .expect("open postgres×memory")
            };
            record_outcome(
                &mut records,
                &mut failures,
                "AC-TXN-1",
                cell,
                futures::executor::block_on(ac_txn_1_success_durable_visible(make)),
            );
            let make = |tag: &str| {
                let schema = format!(
                    "fw_pg_mem_t3_{}_{}_{}",
                    std::process::id(),
                    run,
                    tag.replace('-', "_")
                );
                fireweed_postgres::composed_postgres_backend_in_schema(&url_c, &schema)
                    .expect("open postgres×memory")
            };
            record_outcome(
                &mut records,
                &mut failures,
                "AC-TXN-2",
                cell,
                futures::executor::block_on(ac_txn_2_rejection_no_effect(make, DURABLE)),
            );
            let make = |tag: &str| {
                let schema = format!(
                    "fw_pg_mem_t3_{}_{}_{}",
                    std::process::id(),
                    run,
                    tag.replace('-', "_")
                );
                fireweed_postgres::composed_postgres_backend_in_schema(&url_c, &schema)
                    .expect("open postgres×memory")
            };
            record_outcome(
                &mut records,
                &mut failures,
                "AC-TXN-3",
                cell,
                futures::executor::block_on(ac_txn_3_unknown_outcome_replay(make, DURABLE)),
            );
        }

        // --- postgres×sqlite ---
        {
            let cell = "postgres×sqlite";
            let cell_base = base.join("sqlite");
            std::fs::create_dir_all(&cell_base).unwrap();
            let run = POSTGRES_LOG_FIXTURE.fetch_add(1, Ordering::Relaxed);
            let url_c = url.clone();
            let make = |tag: &str| {
                let log_schema = format!(
                    "fw_pg_sql_t3_{}_{}_{}",
                    std::process::id(),
                    run,
                    tag.replace('-', "_")
                );
                let proj = cell_base.join(format!("{tag}-proj.db"));
                let log = fireweed_postgres::PostgresLog::connect_in_schema(&url_c, &log_schema)
                    .expect("connect postgres log");
                let projection =
                    fireweed_sqlite::SqliteProjectionStore::open(proj.to_str().unwrap())
                        .expect("open sqlite projection");
                assemble_async_log_replay(log, projection, 0)
                    .expect("assemble async log-replay")
                    .recover()
                    .expect("recover postgres×sqlite")
                    .with_node_id(1)
            };
            record_outcome(
                &mut records,
                &mut failures,
                "AC-TXN-1",
                cell,
                futures::executor::block_on(ac_txn_1_success_durable_visible(make)),
            );
            let make = |tag: &str| {
                let log_schema = format!(
                    "fw_pg_sql_t3_{}_{}_{}",
                    std::process::id(),
                    run,
                    tag.replace('-', "_")
                );
                let proj = cell_base.join(format!("{tag}-proj.db"));
                let log = fireweed_postgres::PostgresLog::connect_in_schema(&url_c, &log_schema)
                    .expect("connect postgres log");
                let projection =
                    fireweed_sqlite::SqliteProjectionStore::open(proj.to_str().unwrap())
                        .expect("open sqlite projection");
                assemble_async_log_replay(log, projection, 0)
                    .expect("assemble async log-replay")
                    .recover()
                    .expect("recover postgres×sqlite")
                    .with_node_id(1)
            };
            record_outcome(
                &mut records,
                &mut failures,
                "AC-TXN-2",
                cell,
                futures::executor::block_on(ac_txn_2_rejection_no_effect(make, DURABLE)),
            );
            let make = |tag: &str| {
                let log_schema = format!(
                    "fw_pg_sql_t3_{}_{}_{}",
                    std::process::id(),
                    run,
                    tag.replace('-', "_")
                );
                let proj = cell_base.join(format!("{tag}-proj.db"));
                let log = fireweed_postgres::PostgresLog::connect_in_schema(&url_c, &log_schema)
                    .expect("connect postgres log");
                let projection =
                    fireweed_sqlite::SqliteProjectionStore::open(proj.to_str().unwrap())
                        .expect("open sqlite projection");
                assemble_async_log_replay(log, projection, 0)
                    .expect("assemble async log-replay")
                    .recover()
                    .expect("recover postgres×sqlite")
                    .with_node_id(1)
            };
            record_outcome(
                &mut records,
                &mut failures,
                "AC-TXN-3",
                cell,
                futures::executor::block_on(ac_txn_3_unknown_outcome_replay(make, DURABLE)),
            );
        }

        // --- postgres×postgres (exact-pair composed log+projection schemas) ---
        {
            let cell = "postgres×postgres";
            let run = POSTGRES_LOG_FIXTURE.fetch_add(1, Ordering::Relaxed);
            let url_c = url.clone();
            let make = |tag: &str| {
                let log_schema = format!(
                    "fw_pg_pg_t3_{}_{}_{}_log",
                    std::process::id(),
                    run,
                    tag.replace('-', "_")
                );
                let proj_schema = format!(
                    "fw_pg_pg_t3_{}_{}_{}_proj",
                    std::process::id(),
                    run,
                    tag.replace('-', "_")
                );
                let log = fireweed_postgres::PostgresLog::connect_in_schema(&url_c, &log_schema)
                    .expect("connect postgres log axis");
                let projection =
                    fireweed_postgres::PostgresRelational::connect_in_schema(&url_c, &proj_schema)
                        .expect("connect postgres projection axis");
                assemble_async_log_replay(log, projection, 0)
                    .expect("assemble async log-replay")
                    .recover()
                    .expect("recover postgres×postgres")
                    .with_node_id(1)
            };
            record_outcome(
                &mut records,
                &mut failures,
                "AC-TXN-1",
                cell,
                futures::executor::block_on(ac_txn_1_success_durable_visible(make)),
            );
            let make = |tag: &str| {
                let log_schema = format!(
                    "fw_pg_pg_t3_{}_{}_{}_log",
                    std::process::id(),
                    run,
                    tag.replace('-', "_")
                );
                let proj_schema = format!(
                    "fw_pg_pg_t3_{}_{}_{}_proj",
                    std::process::id(),
                    run,
                    tag.replace('-', "_")
                );
                let log = fireweed_postgres::PostgresLog::connect_in_schema(&url_c, &log_schema)
                    .expect("connect postgres log axis");
                let projection =
                    fireweed_postgres::PostgresRelational::connect_in_schema(&url_c, &proj_schema)
                        .expect("connect postgres projection axis");
                assemble_async_log_replay(log, projection, 0)
                    .expect("assemble async log-replay")
                    .recover()
                    .expect("recover postgres×postgres")
                    .with_node_id(1)
            };
            record_outcome(
                &mut records,
                &mut failures,
                "AC-TXN-2",
                cell,
                futures::executor::block_on(ac_txn_2_rejection_no_effect(make, DURABLE)),
            );
            let make = |tag: &str| {
                let log_schema = format!(
                    "fw_pg_pg_t3_{}_{}_{}_log",
                    std::process::id(),
                    run,
                    tag.replace('-', "_")
                );
                let proj_schema = format!(
                    "fw_pg_pg_t3_{}_{}_{}_proj",
                    std::process::id(),
                    run,
                    tag.replace('-', "_")
                );
                let log = fireweed_postgres::PostgresLog::connect_in_schema(&url_c, &log_schema)
                    .expect("connect postgres log axis");
                let projection =
                    fireweed_postgres::PostgresRelational::connect_in_schema(&url_c, &proj_schema)
                        .expect("connect postgres projection axis");
                assemble_async_log_replay(log, projection, 0)
                    .expect("assemble async log-replay")
                    .recover()
                    .expect("recover postgres×postgres")
                    .with_node_id(1)
            };
            record_outcome(
                &mut records,
                &mut failures,
                "AC-TXN-3",
                cell,
                futures::executor::block_on(ac_txn_3_unknown_outcome_replay(make, DURABLE)),
            );
        }

        let output = evidence_output(
            &base,
            "FIREWEED_TP003_POSTGRES_EVIDENCE_OUT",
            "tp003-ac-txn-matrix-postgres-storage-pairs.jsonl",
        );
        output
            .write(render_evidence(&records))
            .expect("write run-owned postgres storage-pair TP-003 evidence");
        eprintln!(
            "postgres_log T3 TP-003 evidence written to {} ({} rows)",
            output.path().display(),
            records.len()
        );
        cleanup_root(&base);
        assert!(
            failures.is_empty(),
            "postgres log TP-003 exact-pair failures:\n{}",
            failures.join("\n")
        );
    }

    /// T3 linkage: the immutable test fixture uses exact postgres axis names. Live evidence is produced
    /// separately into a run-owned output by `postgres_log_t3_tp003_ac_txn_exact_pairs`.
    #[test]
    fn postgres_log_t3_evidence_axis_names_file_contract() {
        let fixture = fireweed_release::Fixture::new(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/tp003-postgres-axis.jsonl"),
        )
        .expect("open immutable postgres axis fixture");
        let body = std::fs::read_to_string(
            fixture
                .authorize(fireweed_release::EvidenceOperation::Read)
                .expect("fixture authorizes reads"),
        )
        .expect("read postgres axis fixture");
        // Accept either the axis multiplication sign or unicode-escaped form.
        for axis in ["postgres×memory", "postgres×sqlite", "postgres×postgres"] {
            let escaped = axis.replace('×', "\\u00d7");
            assert!(
                body.contains(axis)
                    || body.contains(&escaped)
                    || body.contains(&axis.replace('×', "/")),
                "evidence must name axis {axis} (or slash alias); body head: {}",
                body.chars().take(200).collect::<String>()
            );
        }
    }

    /// T4: chart-installable postgres-log cells have CI values files and helm-gate registration.
    #[test]
    fn postgres_log_t4_helm_ci_values_and_gate() {
        let chart_ci =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../charts/fireweed-queue/ci");
        for name in [
            "postgres-memory-values.yaml",
            "postgres-sqlite-values.yaml",
            "postgres-postgres-values.yaml",
        ] {
            let p = chart_ci.join(name);
            assert!(
                p.is_file(),
                "T4: missing Helm CI values for postgres log cell: {}",
                p.display()
            );
            let body = std::fs::read_to_string(&p).unwrap();
            assert!(
                body.contains("backend: postgres"),
                "{} must select storage.log.backend=postgres",
                name
            );
        }

        let gate = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../scripts/ci/helm-gate.sh");
        let gate_body = std::fs::read_to_string(&gate).expect("read helm-gate.sh");
        for combo in ["postgres-memory", "postgres-sqlite", "postgres-postgres"] {
            assert!(
                gate_body.contains(combo),
                "helm-gate.sh must register combination {combo}"
            );
        }

        if std::process::Command::new("helm")
            .arg("version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
        {
            let chart =
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../charts/fireweed-queue");
            for (combo, values) in [
                ("postgres-memory", "postgres-memory-values.yaml"),
                ("postgres-sqlite", "postgres-sqlite-values.yaml"),
                ("postgres-postgres", "postgres-postgres-values.yaml"),
            ] {
                let values_path = chart.join("ci").join(values);
                let out = std::process::Command::new("helm")
                    .args([
                        "template",
                        &format!("fireweed-{combo}"),
                        chart.to_str().unwrap(),
                        "--values",
                        values_path.to_str().unwrap(),
                    ])
                    .output()
                    .expect("helm template");
                assert!(
                    out.status.success(),
                    "T4 helm template {combo} failed:\n{}",
                    String::from_utf8_lossy(&out.stderr)
                );
                let rendered = String::from_utf8_lossy(&out.stdout);
                assert!(
                    rendered.contains("FIREWEED_LOG_BACKEND: \"postgres\""),
                    "{combo} render must set FIREWEED_LOG_BACKEND=postgres"
                );
            }
        } else {
            eprintln!(
                "postgres_log T4 helm template skipped (helm not on PATH); values+gate checked"
            );
        }
    }

    /// Without the postgres feature the three cells remain registered (composition + T3/T4 contracts).
    #[cfg(not(feature = "postgres"))]
    #[test]
    fn postgres_log_cells_registered_without_postgres_feature() {
        // Composition root arms are feature-gated in source, but T3/T4 contracts still hold.
        postgres_log_t3_evidence_axis_names_file_contract();
        postgres_log_t4_helm_ci_values_and_gate();
    }
}
