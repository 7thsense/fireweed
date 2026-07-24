#![forbid(unsafe_code)]
//! # fireweed-server
//!
//! The **composition root**: the single place that selects a concrete backend (memory / sqlite /
//! objectlog) and wires it to the two faces of pqueue. It binds the RESP front ([`fireweed_resp::serve`])
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
    ComposedBackend, ControlPlaneConfig, EngineError, EngineResult, InMemoryControlPlane,
    InProcessControlPlane, LeaseState, OwnedSession, QueueControlPlane, QueueKey,
};
use fireweed_memory::composed_memory_backend;
use fireweed_objectlog::ObjectLog;
use fireweed_objectlog::segmented::{BlobStore, LocalFsBlobStore, S3BlobStore};
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
mod object_log_sqlite;
#[cfg(feature = "turso-projection")]
mod object_log_turso;
mod tokio_dispatcher;
pub use change_record_sink::{
    ChangeRecordSinkConfig, ChangeRecordSinkMode, FjordChangeRecordSink, NiflheimChangeRecordSink,
    emit_change_record_tick, spawn_change_record_emitter,
};
pub use fireweed_objectlog::segmented::{SegmentConfig, SegmentWriterFormat};
pub use object_log_sqlite::{
    DEFAULT_RECOVERY_MAX_TAIL, ObjectLogSqliteBackend, RecoveryStats,
    SegmentedObjectLogInMemoryBackend, SegmentedObjectLogSqliteBackend,
};
#[cfg(feature = "turso-projection")]
pub use object_log_turso::ObjectLogTursoBackend;
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
    fn label(&self) -> &'static str {
        match self {
            LogSpec::Memory => "memory",
            LogSpec::Sqlite { .. } => "sqlite",
            LogSpec::ObjectLog(_) => "objectlog",
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
                S3BlobStore::new(endpoint, bucket, access_key_id, secret_access_key, region)?;
            }
        }
        Ok(())
    }

    fn open_blob_store(&self) -> EngineResult<Arc<dyn BlobStore>> {
        self.validate()?;
        match self {
            Self::LocalFilesystem { root, .. } => Ok(Arc::new(LocalFsBlobStore::open(root)?)),
            Self::S3 {
                endpoint,
                bucket,
                region,
                credentials,
                ..
            } => {
                let S3CredentialSource::Static {
                    access_key_id,
                    secret_access_key,
                } = credentials;
                Ok(Arc::new(S3BlobStore::new(
                    endpoint,
                    bucket,
                    access_key_id,
                    secret_access_key,
                    region,
                )?))
            }
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
            ProjectionSpec::Turso { .. } => "turso",
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

/// The queue-ownership control-plane axis. `InProcess` is an explicit single-process development profile;
/// production replicas use the shared transactional Postgres authority.
#[derive(Debug, Clone)]
pub enum ControlPlaneSpec {
    /// Development/test only. Environment parsing rejects this profile when `PQUEUE_REPLICA_COUNT > 1`.
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
/// `PostgresLog` command log + in-memory projection, driven through [`PostgresWholeOperationAdapter`]). The
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
    let nonempty = |suffix: &str| {
        env.get(&format!("FIREWEED_{suffix}"))
            .or_else(|| env.get(&format!("PQUEUE_{suffix}")))
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
    let nonempty = |suffix: &str| {
        env.get(&format!("FIREWEED_{suffix}"))
            .or_else(|| env.get(&format!("PQUEUE_{suffix}")))
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

/// The single authoritative, fully-typed runtime configuration for a pqueue server. Every knob the server
/// needs lives here as a typed field; there is exactly ONE optional env populator (`Config::from_env`, in
/// the `fireweed-service` bin) that maps the documented `PQUEUE_*`/`DATABRICKS_*` env names onto these fields.
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
    /// recovery-window warning. Parsed from `PQUEUE_RECOVERY_MAX_TAIL_COMMANDS` (default
    /// [`DEFAULT_RECOVERY_MAX_TAIL`]); applied by [`start`] to the objectlog+sqlite backend.
    pub recovery_max_tail: u64,
    /// Opt-in group-commit telemetry for the segmented+SQLite object-log backend (the typed form of
    /// `PQUEUE_DEBUG_SEGMENTS`).
    pub debug_segments: bool,
    /// Validated finite admission bounds shared by every object-log commit profile on this node.
    pub objectlog_byte_limits: ObjectLogByteLimits,
    /// Tokio worker-thread cap (the typed form of `PQUEUE_WORKER_THREADS`). `None` = one worker per core.
    /// Consumed by the bin when building the runtime, not by [`start`].
    pub worker_threads: Option<usize>,
    /// Fixed number of sync PostgreSQL connections owned by the `postgres/inmemory` production backend.
    /// Queue affinity multiplexes any number of queues over this bounded pool; the value never grows from
    /// queue creation or load. Parsed from `PQUEUE_POSTGRES_POOL_SIZE`.
    pub postgres_pool_size: usize,
    /// Optional path for the service binary's atomic Tokio worker/live-task gauge snapshot. `None`
    /// disables the reporter. The env-config form requires an absolute, non-empty path.
    pub runtime_resource_metrics_path: Option<std::path::PathBuf>,
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
        (LogSpec::ObjectLog(spec), ProjectionSpec::InMemory) => {
            // The segmented group-commit object log (the object log's only production form) over an in-memory
            // projection rebuilt by `read_all` replay on open.
            let backends = tokio::task::spawn_blocking(move || {
                let segment_config = spec.segment_config();
                let store = spec.open_blob_store()?;
                (0..8)
                    .map(|index| {
                        SegmentedObjectLogInMemoryBackend::open_with_blob_store(
                            Arc::clone(&store),
                            segment_config,
                        )
                        .map(|backend| {
                            backend
                                .with_byte_admission(
                                    objectlog_byte_budget.clone(),
                                    config_objectlog_queue_limit,
                                )
                                .with_debug_segments(debug_segments)
                                .with_node_id(node_id)
                                .with_worker_partition(index, 8)
                        })
                    })
                    .collect::<EngineResult<Vec<_>>>()
            })
            .await
            .map_err(|e| EngineError::Storage(format!("object-log open task failed: {e}")))??;
            let backends: Vec<_> = backends.into_iter().map(Arc::new).collect();
            // The flusher seals latency-due segments so a buffer below `target_bytes` still acks promptly.
            let flushers: Vec<_> = backends
                .iter()
                .map(|backend| backend.spawn_flusher())
                .collect();
            let (backend, lifecycle) = blocking_backend_pool(backends);
            let mut server = run_owned_with_blocking_lifecycle(
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
            .await?;
            server.maintenance_tasks.extend(flushers);
            Ok(server)
        }
        (LogSpec::ObjectLog(spec), ProjectionSpec::Sqlite { path }) => {
            // The segmented group-commit object log driving the derived SQLite projection: concurrent pushes
            // co-buffer into one sealed segment (one durable object + one manifest-CAS + one batched SQLite
            // apply), and a reopen replays the object-log tail beyond the projection snapshot high-water.
            let p = path
                .into_os_string()
                .into_string()
                .map_err(|_| EngineError::Storage("non-utf8 path".into()))?;
            let backends = tokio::task::spawn_blocking(move || {
                let segment_config = spec.segment_config();
                let store = spec.open_blob_store()?;
                let projection = Arc::new(fireweed_sqlite::SqliteProjectionStore::open(&p)?);
                (0..8)
                    .map(|index| {
                        SegmentedObjectLogSqliteBackend::open_with_blob_store_and_projection(
                            Arc::clone(&store),
                            Arc::clone(&projection),
                            segment_config,
                        )
                        .map(|backend| {
                            backend
                                .with_byte_admission(
                                    objectlog_byte_budget.clone(),
                                    config_objectlog_queue_limit,
                                )
                                .with_node_id(node_id)
                                .with_recovery_max_tail(recovery_max_tail)
                                .with_debug_segments(debug_segments)
                                .with_worker_partition(index, 8)
                        })
                    })
                    .collect::<EngineResult<Vec<_>>>()
            })
            .await
            .map_err(|e| {
                EngineError::Storage(format!("object-log/sqlite open task failed: {e}"))
            })??;
            let backends: Vec<_> = backends.into_iter().map(Arc::new).collect();
            let flushers: Vec<_> = backends
                .iter()
                .map(|backend| backend.spawn_flusher())
                .collect();
            let (backend, lifecycle) = blocking_backend_pool(backends);
            let mut server = run_owned_with_blocking_lifecycle(
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
            .await?;
            server.maintenance_tasks.extend(flushers);
            Ok(server)
        }
        #[cfg(feature = "turso-projection")]
        (LogSpec::ObjectLog(spec), ProjectionSpec::Turso { path }) => {
            let segment_config = spec.segment_config();
            let store = spec.open_blob_store()?;
            let backend = Arc::new(
                ObjectLogTursoBackend::open_with_blob_store(store, &path, segment_config).await?,
            );
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
        #[cfg(not(feature = "turso-projection"))]
        (LogSpec::ObjectLog(_), ProjectionSpec::Turso { .. }) => Err(EngineError::Invalid(
            "PQUEUE_PROJECTION_BACKEND=turso requires a fireweed-server build with the `turso-projection` cargo feature",
        )),
        (LogSpec::ObjectLog(spec), ProjectionSpec::Hybrid { path }) => {
            let backends = tokio::task::spawn_blocking(move || {
                let segment_config = spec.segment_config();
                let store = spec.open_blob_store()?;
                (0..8)
                    .map(|index| {
                        open_objectlog_hybrid_backend(
                            Arc::clone(&store),
                            &path,
                            segment_config,
                            recovery_max_tail,
                            node_id,
                            deferred_flush_chunk,
                            false,
                            None,
                            objectlog_byte_budget.clone(),
                            config_objectlog_queue_limit,
                            Some((index, 8)),
                        )
                    })
                    .collect::<EngineResult<Vec<_>>>()
            })
            .await
            .map_err(|e| EngineError::Storage(format!("hybrid open task failed: {e}")))??;
            let flushers: Vec<_> = backends
                .iter()
                .map(|backend| spawn_hybrid_flusher(backend, debug_segments))
                .collect();
            let (backend, lifecycle) = blocking_backend_pool(backends);
            let fjord_task = maybe_spawn_embedded_broker(
                &fjord_surface,
                fjord_broker_listen.as_deref(),
                &change_record_sink,
                &queues,
            )
            .await?;
            let emitter_backend = backend.clone();
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
            server.blocking_lifecycles.push(lifecycle);
            server.maintenance_tasks.extend(flushers);
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
            // The `objectlog/hybrid-strict` profile (TD-004): the same object-log group-commit substrate as
            // `objectlog/hybrid`, but the projection commits every sealed batch DURABLY to SQLite BEFORE
            // applying it to hot memory (`apply_durable_then_memory`, selected by `with_strict_apply(true)`).
            // This puts the SQLite-durable-before-visible barrier and the SQLite-commit-then-memory-fail
            // poison cut on the real server write pipeline: a SQLite failure returns no success and replays
            // the object-log tail, and a poisoned store fails closed until a restart rehydrates memory from
            // the durable SQLite `ProjectionImage`.
            let backends = tokio::task::spawn_blocking(move || {
                let segment_config = spec.segment_config();
                let store = spec.open_blob_store()?;
                (0..8)
                    .map(|index| {
                        open_objectlog_hybrid_backend(
                            Arc::clone(&store),
                            &path,
                            segment_config,
                            recovery_max_tail,
                            node_id,
                            deferred_flush_chunk,
                            true,
                            None,
                            objectlog_byte_budget.clone(),
                            config_objectlog_queue_limit,
                            Some((index, 8)),
                        )
                    })
                    .collect::<EngineResult<Vec<_>>>()
            })
            .await
            .map_err(|e| EngineError::Storage(format!("hybrid-strict open task failed: {e}")))??;
            let flushers: Vec<_> = backends
                .iter()
                .map(|backend| spawn_hybrid_flusher(backend, debug_segments))
                .collect();
            let (backend, lifecycle) = blocking_backend_pool(backends);
            let fjord_task = maybe_spawn_embedded_broker(
                &fjord_surface,
                fjord_broker_listen.as_deref(),
                &change_record_sink,
                &queues,
            )
            .await?;
            let emitter_backend = backend.clone();
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
            server.blocking_lifecycles.push(lifecycle);
            server.maintenance_tasks.extend(flushers);
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
            let backends = tokio::task::spawn_blocking(move || {
                let segment_config = spec.segment_config();
                let store = spec.open_blob_store()?;
                (0..8)
                    .map(|index| {
                        open_objectlog_hybrid_backend(
                            Arc::clone(&store),
                            &path,
                            segment_config,
                            recovery_max_tail,
                            node_id,
                            deferred_flush_chunk,
                            false,
                            Some(hybrid_async),
                            objectlog_byte_budget.clone(),
                            config_objectlog_queue_limit,
                            Some((index, 8)),
                        )
                    })
                    .collect::<EngineResult<Vec<_>>>()
            })
            .await
            .map_err(|e| EngineError::Storage(format!("hybrid-async open task failed: {e}")))??;
            let flushers: Vec<_> = backends
                .iter()
                .map(|backend| spawn_hybrid_flusher(backend, debug_segments))
                .collect();
            let (backend, lifecycle) = blocking_backend_pool(backends);
            let fjord_task = maybe_spawn_embedded_broker(
                &fjord_surface,
                fjord_broker_listen.as_deref(),
                &change_record_sink,
                &queues,
            )
            .await?;
            let emitter_backend = backend.clone();
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
            server.blocking_lifecycles.push(lifecycle);
            server.maintenance_tasks.extend(flushers);
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
            // The composed postgres-log + sqlite-projection backend (`ComposedBackend<PostgresLog,
            // SqliteProjectionStore, InProcessControlPlane>`): the durable postgres command log paired with a
            // derived SQLite relational projection, recovery-on-open. Same off-reactor discipline as
            // postgres/inmemory above: connect BOTH axes and recover inside `spawn_blocking`, then drive the
            // composition only through the bounded whole-operation adapter.
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
                ComposedBackend::new(log, projection, InProcessControlPlane::new())
                    .recover()
                    .map(|b| b.with_node_id(node_id))
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

/// Assemble the object-log + hybrid (hot-memory serving over a durable SQLite checkpoint image) composed
/// backend shared by the `objectlog/hybrid` and `objectlog/hybrid-async` profiles: open the group-commit
/// object log at `root` + the hybrid projection store at `path`, run the ack-after-seal group-commit write
/// path, and recover-on-open (hydrate memory from the validated SQLite image + replay the object-log tail).
// Composition-root builder: threads the object-log + hybrid-projection knobs (strict apply, async
// backpressure monitor) into one backend. The arity reflects the wiring surface, not incidental complexity.
#[allow(clippy::too_many_arguments)]
fn open_objectlog_hybrid_backend(
    store: Arc<dyn BlobStore>,
    path: &std::path::Path,
    segment_config: SegmentConfig,
    recovery_max_tail: u64,
    node_id: u8,
    deferred_flush_chunk: usize,
    strict: bool,
    async_monitor: Option<HybridAsyncThresholds>,
    byte_budget: BufferedByteBudget,
    queue_byte_limit: usize,
    worker_partition: Option<(usize, usize)>,
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
    let backend = ComposedBackend::new(
        ObjectLog::open_group_commit_with_blob_store(store, segment_config)?,
        projection,
        InProcessControlPlane::new(),
    )
    .with_group_commit(true)
    .with_byte_admission(byte_budget, queue_byte_limit)
    .with_recovery_max_tail(recovery_max_tail)
    .recover()?
    .with_node_id(node_id);
    let backend = if let Some((index, count)) = worker_partition {
        backend.with_worker_partition(index, count)
    } else {
        backend
    };
    Ok(Arc::new(backend))
}

fn spawn_hybrid_flusher(
    backend: &Arc<ObjectLogHybridBackend>,
    debug_segments: bool,
) -> JoinHandle<()> {
    let interval_ms = backend.group_commit_flush_interval_ms();
    let weak = Arc::downgrade(backend);
    fireweed_resp::spawn_governed(async move {
        let group_interval = Duration::from_millis(interval_ms);
        let deferred_interval = Duration::from_millis(250);
        // `tokio::time::interval` fires immediately. That eager tick both defeats the configured
        // co-buffering window and can enqueue a blocking job that retains the last backend Arc while the
        // blocking pool is busy. Start each cadence at its first real deadline instead.
        let now = tokio::time::Instant::now();
        let mut tick = tokio::time::interval_at(now + group_interval, group_interval);
        let mut deferred_tick =
            tokio::time::interval_at(now + deferred_interval, deferred_interval);
        let mut dbg_last = std::time::Instant::now();
        loop {
            enum FlushKind {
                GroupCommit,
                DeferredProjection,
            }
            let kind = tokio::select! {
                _ = tick.tick() => FlushKind::GroupCommit,
                _ = deferred_tick.tick() => FlushKind::DeferredProjection,
            };
            if weak.strong_count() == 0 {
                break;
            }
            let emit_debug = debug_segments && dbg_last.elapsed() >= Duration::from_secs(1);
            if emit_debug {
                dbg_last = std::time::Instant::now();
            }
            // Keep only a Weak reference while this job waits for blocking-pool admission. If server
            // shutdown drops the production owners under load, a queued maintenance job must not prolong
            // the backend or its resident byte permits.
            let job_backend = weak.clone();
            let join = tokio::task::spawn_blocking(move || {
                let backend = job_backend.upgrade()?;
                let result = match kind {
                    FlushKind::GroupCommit => {
                        let now_ms = match std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                        {
                            Ok(d) => d.as_millis().min(i64::MAX as u128) as i64,
                            Err(_) => 0,
                        };
                        backend.flush_tick(now_ms).map(|_| ())
                    }
                    FlushKind::DeferredProjection => {
                        backend.try_flush_deferred_projection().map(|_| ())
                    }
                };
                if emit_debug {
                    let c = backend.with_log(|log| log.counters());
                    let admission = hybrid_byte_admission_telemetry(&backend);
                    eprintln!(
                        "[seg] profile=objectlog/hybrid sealed={} commands={} mean_batch={:.1} max_batch={} objects_put={} {}",
                        c.segments_sealed,
                        c.commands_committed,
                        c.mean_batch_size(),
                        c.max_batch_size(),
                        c.objects_put,
                        admission,
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

fn hybrid_byte_admission_telemetry(backend: &ObjectLogHybridBackend) -> String {
    let stats = backend
        .byte_admission_stats()
        .expect("production hybrid byte admission is configured");
    let (global, tenant, queue) = backend
        .byte_admission_limits()
        .expect("production hybrid byte admission limits are configured");
    format!(
        "admission_current={} admission_peak={} admission_waiters={} admission_waits={} admission_rejects={} admission_total_wait_nanos={} admission_max_wait_nanos={} admission_global_limit={} admission_tenant_limit={} admission_queue_limit={}",
        stats.charged_bytes,
        stats.peak_charged_bytes,
        stats.waiting_requests,
        stats.wait_count,
        stats.rejection_count,
        stats.total_wait_nanos,
        stats.max_wait_nanos,
        global,
        tenant.map_or_else(|| "none".to_string(), |value| value.to_string()),
        queue,
    )
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
        AcquireOutcome, ControlPlaneConfig, ControlPlaneStore, InMemoryControlPlane, LeaseRenewal,
        LeaseRenewalOutcome, LeaseState, OwnerEndpointAdvertisement, OwnerResolution,
        ProjectionRead, PushPort, PushSpec, QueueControlPlane, QueueLease,
    };
    use fireweed_objectlog::segmented::InMemoryBlobStore;
    use std::sync::mpsc;

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

    struct PauseAfterAcquire {
        inner: Arc<InMemoryControlPlane>,
        entered: mpsc::Sender<()>,
        resume: Mutex<mpsc::Receiver<()>>,
    }

    impl QueueControlPlane for PauseAfterAcquire {
        fn register_owner(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()> {
            self.inner.register_owner(owner, now)
        }
        fn advertise_owner_endpoint(
            &self,
            owner: &OwnerId,
            endpoint: &str,
            now: UtcTimestamp,
        ) -> EngineResult<()> {
            self.inner.advertise_owner_endpoint(owner, endpoint, now)
        }
        fn live_owner_endpoints(
            &self,
            now: UtcTimestamp,
        ) -> EngineResult<Vec<OwnerEndpointAdvertisement>> {
            self.inner.live_owner_endpoints(now)
        }
        fn heartbeat(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()> {
            self.inner.heartbeat(owner, now)
        }
        fn resolve_queue_owner(
            &self,
            queue: &QueueKey,
            now: UtcTimestamp,
        ) -> EngineResult<OwnerResolution> {
            self.inner.resolve_queue_owner(queue, now)
        }
        fn acquire_queue_lease(
            &self,
            queue: &QueueKey,
            owner: &OwnerId,
            now: UtcTimestamp,
        ) -> EngineResult<AcquireOutcome> {
            let outcome = self.inner.acquire_queue_lease(queue, owner, now)?;
            self.entered
                .send(())
                .map_err(|_| EngineError::Unavailable)?;
            self.resume
                .lock()
                .expect("pause receiver poisoned")
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| EngineError::Unavailable)?;
            Ok(outcome)
        }
        fn confirm_queue_lease_fence(
            &self,
            queue: &QueueKey,
            owner: &OwnerId,
            expected_epoch: u64,
            now: UtcTimestamp,
        ) -> EngineResult<QueueLease> {
            self.inner
                .confirm_queue_lease_fence(queue, owner, expected_epoch, now)
        }
        fn renew_queue_lease(
            &self,
            queue: &QueueKey,
            owner: &OwnerId,
            expected_epoch: u64,
            now: UtcTimestamp,
        ) -> EngineResult<QueueLease> {
            self.inner
                .renew_queue_lease(queue, owner, expected_epoch, now)
        }
        fn begin_drain(
            &self,
            queue: &QueueKey,
            expected_epoch: u64,
            target_owner: &OwnerId,
            now: UtcTimestamp,
        ) -> EngineResult<QueueLease> {
            self.inner
                .begin_drain(queue, expected_epoch, target_owner, now)
        }
        fn release_queue_lease(
            &self,
            queue: &QueueKey,
            owner: &OwnerId,
            expected_epoch: u64,
            now: UtcTimestamp,
        ) -> EngineResult<()> {
            self.inner
                .release_queue_lease(queue, owner, expected_epoch, now)
        }
        fn lease(&self, queue: &QueueKey) -> EngineResult<QueueLease> {
            self.inner.lease(queue)
        }
        fn is_ephemeral(&self) -> bool {
            true
        }
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

    #[test]
    fn production_hybrid_constructor_consumes_node_and_queue_caps() {
        let path = std::env::temp_dir().join(format!(
            "pqueue-byte-admission-hybrid-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let budget = BufferedByteBudget::new(
            BufferedByteBudgetConfig::new(8_192)
                .unwrap()
                .with_uniform_tenant_limit(4_096)
                .unwrap(),
        );
        let backend = open_objectlog_hybrid_backend(
            Arc::new(InMemoryBlobStore::new()),
            &path,
            SegmentConfig::new(1_024, 100).unwrap(),
            DEFAULT_RECOVERY_MAX_TAIL,
            0,
            fireweed_sqlite::DEFAULT_DEFERRED_FLUSH_CHUNK,
            false,
            None,
            budget,
            2_048,
            None,
        )
        .unwrap();
        assert_eq!(
            backend.byte_admission_limits(),
            Some((8_192, Some(4_096), 2_048))
        );
        assert_eq!(backend.byte_admission_stats().unwrap().charged_bytes, 0);
        let telemetry = hybrid_byte_admission_telemetry(&backend);
        assert!(telemetry.contains("admission_global_limit=8192"));
        assert!(telemetry.contains("admission_tenant_limit=4096"));
        assert!(telemetry.contains("admission_queue_limit=2048"));
        drop(backend);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn hybrid_flusher_does_not_retain_backend_or_resident_permits_on_shutdown() {
        let path = std::env::temp_dir().join(format!(
            "pqueue-byte-admission-hybrid-drop-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let budget = BufferedByteBudget::new(BufferedByteBudgetConfig::new(16_384).unwrap());
        let backend = open_objectlog_hybrid_backend(
            Arc::new(InMemoryBlobStore::new()),
            &path,
            SegmentConfig::new(8_192, 60_000).unwrap(),
            DEFAULT_RECOVERY_MAX_TAIL,
            0,
            fireweed_sqlite::DEFAULT_DEFERRED_FLUSH_CHUNK,
            false,
            None,
            budget.clone(),
            8_192,
            None,
        )
        .unwrap();
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
        for _ in 0..100 {
            if budget.stats().charged_bytes > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(budget.stats().charged_bytes > 0);
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
        assert_eq!(budget.stats().charged_bytes, 0);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn start_rejects_programmatic_objectlog_limits_below_segment_target() {
        let path = std::env::temp_dir().join(format!(
            "pqueue-invalid-byte-config-{}-{}",
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pending_fence_gap_has_one_safe_old_prefix_then_fences_stale_retry() {
        let store = Arc::new(InMemoryBlobStore::new());
        let config = SegmentConfig::new(4_096, 1).unwrap();
        let a_backend = Arc::new(
            SegmentedObjectLogInMemoryBackend::open_with_blob_store(store.clone(), config).unwrap(),
        );
        let b_backend = Arc::new(
            SegmentedObjectLogInMemoryBackend::open_with_blob_store(store, config).unwrap(),
        );
        let a_flusher = a_backend.spawn_flusher();
        let b_flusher = b_backend.spawn_flusher();
        let cp = Arc::new(InMemoryControlPlane::new(ControlPlaneConfig {
            heartbeat_ttl_ms: 5_000,
            lease_ttl_ms: 10_000,
        }));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let a_owner = OwnerId::new("owner-a").unwrap();
        let b_owner = OwnerId::new("owner-b").unwrap();
        let a = Arc::new(OwnershipRuntime::new(
            a_backend.clone(),
            cp.clone(),
            a_owner,
            "127.0.0.1:7101".into(),
        ));
        let b = Arc::new(OwnershipRuntime::new(
            b_backend.clone(),
            Arc::new(PauseAfterAcquire {
                inner: cp.clone(),
                entered: entered_tx,
                resume: Mutex::new(resume_rx),
            }),
            b_owner,
            "127.0.0.1:7102".into(),
        ));
        let definition = queue_definition();
        let queue = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        a_backend.create_queue(definition.clone()).await.unwrap();
        b_backend.create_queue(definition).await.unwrap();
        a_backend.fence_epoch(&queue, 0).await.unwrap();
        a.register_owner(UtcTimestamp::new(0, 0).unwrap()).unwrap();
        a.acquire_queue(&queue, UtcTimestamp::new(0, 0).unwrap())
            .await
            .unwrap();
        b.register_owner(UtcTimestamp::new(20, 0).unwrap()).unwrap();

        let acquiring = {
            let b = b.clone();
            let queue = queue.clone();
            tokio::spawn(async move {
                b.acquire_queue(&queue, UtcTimestamp::new(20, 0).unwrap())
                    .await
            })
        };
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(cp.lease(&queue).unwrap().state, LeaseState::PendingFence);
        let routing_key = format!("{}:{}", queue.tenant_id.as_str(), queue.queue_id.as_str());
        assert_eq!(
            a.route_command(
                "GET",
                &[],
                routing_key.as_bytes(),
                UtcTimestamp::new(20, 0).unwrap(),
                false,
            )
            .await
            .unwrap(),
            RouteDecision::Unavailable
        );
        a_backend
            .push(
                &queue,
                vec![push_spec()],
                UtcTimestamp::new(20, 0).unwrap(),
                Some(1),
            )
            .await
            .unwrap();
        resume_tx.send(()).unwrap();
        acquiring.await.unwrap().unwrap();
        assert_eq!(
            a_backend
                .push(
                    &queue,
                    vec![push_spec()],
                    UtcTimestamp::new(21, 0).unwrap(),
                    Some(1),
                )
                .await,
            Err(EngineError::EpochFenced)
        );
        assert_eq!(b_backend.metrics(&queue).await.unwrap().pending, 1);
        assert_eq!(b_backend.current_epoch(&queue).await.unwrap(), 2);
        a_flusher.abort();
        b_flusher.abort();
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
        assert!(!arm.contains("ComposedBackend::new"));
    }
}
