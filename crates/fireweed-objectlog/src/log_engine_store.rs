//! Native-async [`AsyncLogStore`] over crates.io [`object_log::LogEngine`] (program A).
//!
//! Opaque payload bytes carry a Fireweed batch frame (`backend_epoch` + envelopes). Offsets from
//! the engine sequencer map 1:1 onto [`CommandPosition::sequence`]. Epoch and high-water metadata
//! live in dedicated blob keys so they survive reopen with a [`object_log::ManifestSequencer`].

#![allow(
    clippy::manual_async_fn,
    reason = "AsyncLogStore deliberately exposes explicit Send future return types"
)]

use std::collections::HashMap;
use std::fmt::Write as _;
use std::io::Write as IoWrite;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use fireweed_core::QueueDefinition;
use fireweed_engine::{
    AsyncLogStore, CommandEnvelope, CommandPage, CommandPosition, CreateQueueOutcome,
    DurabilityClass, EngineError, EngineResult, ProjectionSnapshot, QueueCommand, QueueKey,
    SnapshotRef,
};
use object_log::{
    BlobStore, Durability, FlushConfig, LocalBlobStore, LogEngine, ManifestSequencer,
    MemoryBlobStore, PartitionKey, Sequencer,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, oneshot};

use crate::s3_create_only::S3CreateOnlyPut;

fn store_err(e: impl std::fmt::Display) -> EngineError {
    EngineError::Storage(e.to_string())
}

/// Map an open-time blob/sequencer failure into an operator-facing storage error.
///
/// When the underlying store cannot prove create-only / conditional-write preconditions
/// (fireweed-2aefefbb), the message must name the authority requirement and the endpoint so
/// operators do not treat a permanent S3-compat gap as a transient `Unavailable`.
fn open_store_err(endpoint: &str, e: impl std::fmt::Display) -> EngineError {
    let detail = e.to_string();
    let lower = detail.to_ascii_lowercase();
    let precondition = lower.contains("if-none-match")
        || lower.contains("precondition")
        || lower.contains("create-only")
        || lower.contains("put_if_absent")
        || lower.contains("conditional")
        || lower.contains("412");
    if precondition {
        EngineError::Storage(format!(
            "object-log open failed on endpoint {endpoint}: NativeConditionalWrite requires \
             create-only PutObject (If-None-Match: * / put-if-absent) to be *enforced* by the \
             endpoint; probe or first create-only write did not prove that precondition. \
             Garage v2.2.0 and similar non-enforcing stores are unsupported for multi-writer \
             object-log authority (see docs/operator/object-log-authority-compatibility.md). \
             detail: {detail}"
        ))
    } else {
        EngineError::Storage(format!(
            "object-log open failed on endpoint {endpoint}: {detail}"
        ))
    }
}

fn partition_key(shard: &QueueKey) -> PartitionKey {
    PartitionKey(format!(
        "{}\u{0}{}",
        shard.tenant_id.as_str(),
        shard.queue_id.as_str()
    ))
}

fn parse_partition(key: &PartitionKey) -> Option<QueueKey> {
    let (tenant, queue) = key.0.split_once('\0')?;
    Some(QueueKey::new(
        fireweed_core::TenantId::new(tenant).ok()?,
        fireweed_core::QueueId::new(queue).ok()?,
    ))
}

#[derive(Serialize, Deserialize, Default)]
struct EpochDoc {
    epoch: u64,
}

#[derive(Serialize, Deserialize, Default)]
struct HighWaterDoc {
    backend_epoch: u64,
    sequence: u64,
}

#[derive(Serialize, Deserialize)]
struct SnapshotMetaDoc {
    backend_epoch: u64,
    sequence: u64,
    ref_id: String,
}

#[derive(Clone, Serialize, Deserialize, Default)]
struct CatalogDoc {
    definitions: Vec<QueueDefinition>,
}

static SNAPSHOT_ORDINAL: AtomicU64 = AtomicU64::new(0);
static DEFINITION_ORDINAL: AtomicU64 = AtomicU64::new(0);

type LocalEngine = LogEngine<ManifestSequencer>;
type EpochCache = Mutex<HashMap<String, u64>>;
type HighWaterCache = Mutex<HashMap<String, CommandPosition>>;
type MetadataPermits = Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>;

/// Coalesce concurrent same-shard appends into one object PUT.
///
/// The composed admit permit allows only one `submit_commit` per queue, so
/// LogEngine linger never sees a second produce. Ports that call
/// [`ObjectLogEngineStore::packed_append`] bypass that permit and wait here.
const PACK_TARGET_BYTES: usize = 4 * 1024 * 1024;
const PACK_MAX_BATCHES: usize = 8;
/// Gather window for concurrent produces. Seal immediately once a full window
/// is waiting; otherwise wait this long for more callers to join the PUT.
const PACK_LINGER: Duration = Duration::from_millis(20);
/// Pre-position budget covering linger, produce-lock queueing, encode, and leader election.
pub const OBJECT_LOG_PRE_POSITION_TIMEOUT: Duration = Duration::from_secs(30);
/// Post-position budget covering `engine.produce` plus periodic high-water `put_json`.
pub const OBJECT_LOG_POST_POSITION_TIMEOUT: Duration = Duration::from_secs(30);

/// Signaled after the pack leader has published apply (or skipped it).
/// Followers wait on this before cancelling their apply reservation.
pub struct ApplyPublish {
    done: Notify,
    finished: std::sync::atomic::AtomicBool,
}

impl ApplyPublish {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            done: Notify::new(),
            finished: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub fn already_done() -> Arc<Self> {
        let publish = Self::new();
        publish.notify();
        publish
    }

    pub fn notify(&self) {
        self.finished.store(true, Ordering::Release);
        self.done.notify_waiters();
    }

    pub async fn wait(&self) {
        if self.finished.load(Ordering::Acquire) {
            return;
        }
        let notified = self.done.notified();
        if self.finished.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }
}

impl std::fmt::Debug for ApplyPublish {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ApplyPublish")
    }
}

/// One packed object-log produce. `apply_batch` is set on exactly one waiter so
/// the projection apply happens once per PUT, not once per public call.
#[derive(Debug)]
pub struct PackedAppendOutcome {
    pub positions: Vec<CommandPosition>,
    pub apply_batch: Option<PackedApplyBatch>,
    pub apply_published: Arc<ApplyPublish>,
}

#[derive(Debug, Clone)]
pub struct PackedApplyBatch {
    pub positions: Vec<CommandPosition>,
    pub commands: Vec<CommandEnvelope>,
    /// Follower reservation ids the leader must absorb before enqueue.
    pub transferred_reservation_ids: Vec<u64>,
}

/// Typed packed-append disposition broadcast to every co-sealed waiter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackedAppendError {
    /// Linger/lock/encode/leader-election failure. Reservation may be cancelled.
    BeforePosition(EngineError),
    /// `engine.produce` or later `advance_high_water`→`put_json` failed or timed out.
    /// Positions must not be reused; the shard is poisoned.
    PostPositionAmbiguous { shard: QueueKey, reason: String },
}

impl PackedAppendError {
    fn before_timeout() -> Self {
        Self::BeforePosition(EngineError::Backpressure {
            resource: "object-log-append-pre-position",
        })
    }

    pub fn into_engine(self) -> EngineError {
        match self {
            Self::BeforePosition(error) => error,
            Self::PostPositionAmbiguous { reason, .. } => {
                EngineError::Storage(format!("object-log post-position ambiguous: {reason}"))
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum PackLane {
    Claim,
    Mutate,
}

fn pack_lane(commands: &[CommandEnvelope]) -> PackLane {
    if commands.iter().all(|envelope| {
        matches!(
            envelope.command,
            QueueCommand::Claim(_) | QueueCommand::CohortClaim(_)
        )
    }) {
        PackLane::Claim
    } else {
        PackLane::Mutate
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct PackGroupKey {
    shard: QueueKey,
    epoch: u64,
    lane: PackLane,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PackGroupPhase {
    PrePosition,
    PostPosition,
}

struct PackWaiter {
    shard: QueueKey,
    epoch: u64,
    lane: PackLane,
    commands: Vec<CommandEnvelope>,
    bytes: usize,
    reservation_id: Option<u64>,
    joined_at: Instant,
    tx: oneshot::Sender<Result<PackedAppendOutcome, PackedAppendError>>,
}

struct PackState {
    pending: Vec<PackWaiter>,
    bytes: usize,
    oldest: Option<Instant>,
    groups: HashMap<PackGroupKey, PackGroupPhase>,
}

struct PackedProduceGate {
    group: PackGroupKey,
    pre_deadline: Instant,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PackerStats {
    pub seals: u64,
    pub waiters: u64,
    pub commands: u64,
    pub bytes: u64,
}

/// Time spent waiting on the metadata permit and/or produce lock.
///
/// `append_wait` covers produce-path permit then produce-lock acquires.
/// Epoch-acquire and emission-cursor waits are permit-only.
#[derive(Debug, Default, Clone, Copy)]
pub struct LockWaitStats {
    pub append_waits: u64,
    pub append_wait: Duration,
    pub epoch_acquire_waits: u64,
    pub epoch_acquire_wait: Duration,
    pub emission_cursor_waits: u64,
    pub emission_cursor_wait: Duration,
}

struct LockWaitCounters {
    append_waits: AtomicU64,
    append_wait_nanos: AtomicU64,
    epoch_acquire_waits: AtomicU64,
    epoch_acquire_wait_nanos: AtomicU64,
    emission_cursor_waits: AtomicU64,
    emission_cursor_wait_nanos: AtomicU64,
}

impl LockWaitCounters {
    fn new() -> Self {
        Self {
            append_waits: AtomicU64::new(0),
            append_wait_nanos: AtomicU64::new(0),
            epoch_acquire_waits: AtomicU64::new(0),
            epoch_acquire_wait_nanos: AtomicU64::new(0),
            emission_cursor_waits: AtomicU64::new(0),
            emission_cursor_wait_nanos: AtomicU64::new(0),
        }
    }

    fn record_append(&self, wait: Duration) {
        self.append_waits.fetch_add(1, Ordering::Relaxed);
        self.append_wait_nanos
            .fetch_add(wait.as_nanos() as u64, Ordering::Relaxed);
    }

    fn record_epoch_acquire(&self, wait: Duration) {
        self.epoch_acquire_waits.fetch_add(1, Ordering::Relaxed);
        self.epoch_acquire_wait_nanos
            .fetch_add(wait.as_nanos() as u64, Ordering::Relaxed);
    }

    fn record_emission_cursor(&self, wait: Duration) {
        self.emission_cursor_waits.fetch_add(1, Ordering::Relaxed);
        self.emission_cursor_wait_nanos
            .fetch_add(wait.as_nanos() as u64, Ordering::Relaxed);
    }

    fn snapshot(&self) -> LockWaitStats {
        LockWaitStats {
            append_waits: self.append_waits.load(Ordering::Relaxed),
            append_wait: Duration::from_nanos(self.append_wait_nanos.load(Ordering::Relaxed)),
            epoch_acquire_waits: self.epoch_acquire_waits.load(Ordering::Relaxed),
            epoch_acquire_wait: Duration::from_nanos(
                self.epoch_acquire_wait_nanos.load(Ordering::Relaxed),
            ),
            emission_cursor_waits: self.emission_cursor_waits.load(Ordering::Relaxed),
            emission_cursor_wait: Duration::from_nanos(
                self.emission_cursor_wait_nanos.load(Ordering::Relaxed),
            ),
        }
    }
}

struct PackerCounters {
    seals: AtomicU64,
    waiters: AtomicU64,
    commands: AtomicU64,
    bytes: AtomicU64,
}

struct ObjectLogPacker {
    state: Mutex<PackState>,
    notify: Notify,
    counters: PackerCounters,
}

impl ObjectLogPacker {
    fn new() -> Self {
        Self {
            state: Mutex::new(PackState {
                pending: Vec::new(),
                bytes: 0,
                oldest: None,
                groups: HashMap::new(),
            }),
            notify: Notify::new(),
            counters: PackerCounters {
                seals: AtomicU64::new(0),
                waiters: AtomicU64::new(0),
                commands: AtomicU64::new(0),
                bytes: AtomicU64::new(0),
            },
        }
    }

    fn snapshot(&self) -> PackerStats {
        PackerStats {
            seals: self.counters.seals.load(Ordering::Relaxed),
            waiters: self.counters.waiters.load(Ordering::Relaxed),
            commands: self.counters.commands.load(Ordering::Relaxed),
            bytes: self.counters.bytes.load(Ordering::Relaxed),
        }
    }

    fn ready_locked(state: &PackState) -> bool {
        if state.pending.is_empty() {
            return false;
        }
        state.bytes >= PACK_TARGET_BYTES
            || state.pending.len() >= PACK_MAX_BATCHES
            || state.oldest.is_some_and(|t| t.elapsed() >= PACK_LINGER)
    }
}

struct LocalEngineRegistration {
    engine: Weak<LocalEngine>,
    epochs: Weak<EpochCache>,
    high_water: Weak<HighWaterCache>,
    metadata_permits: Weak<MetadataPermits>,
    flush_config: String,
}

fn local_engine_registry() -> &'static Mutex<HashMap<std::path::PathBuf, LocalEngineRegistration>> {
    static REGISTRY: OnceLock<Mutex<HashMap<std::path::PathBuf, LocalEngineRegistration>>> =
        OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

enum DefinitionAuthority {
    /// Durable create-only files are the authority. The process registry only shares the
    /// LogEngine sequencer; it is neither consulted nor required to select a definition.
    Local { root: std::path::PathBuf },
    /// One store owns this in-memory blob namespace. A short catalog-only permit makes the
    /// get/put pair atomic without serializing append, read, projection, or unrelated I/O.
    ProcessLocal,
    /// S3 PutObject with `If-None-Match: *` (enforced by the endpoint, e.g. P1s MinIO).
    /// Owned by Fireweed because `object_log::BlobStore` is overwrite-only `put`.
    S3CreateOnly { put: Arc<S3CreateOnlyPut> },
    /// Generic/custom BlobStore path without a create-only publisher: fail closed rather
    /// than pretend read-then-put is authoritative.
    ConditionalCreateUnavailable,
}

fn partition_component(shard: &QueueKey) -> String {
    let partition = partition_key(shard);
    let mut encoded = String::with_capacity(partition.0.len() * 2);
    for byte in partition.0.as_bytes() {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

fn next_snapshot_ref_id() -> String {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let ordinal = SNAPSHOT_ORDINAL.fetch_add(1, Ordering::Relaxed);
    format!("{timestamp:x}-{:x}-{ordinal:x}", std::process::id())
}

fn valid_snapshot_ref_id(ref_id: &str) -> bool {
    !ref_id.is_empty()
        && ref_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

/// Fireweed log axis backed by crates.io object-log.
pub struct ObjectLogEngineStore<S: Sequencer = ManifestSequencer> {
    engine: Arc<LogEngine<S>>,
    blob: Arc<dyn BlobStore>,
    /// In-process epoch cache (also written to blob for reopen).
    epochs: Arc<EpochCache>,
    high_water: Arc<HighWaterCache>,
    /// Appends since last durable high-water PUT, per partition. The produce is
    /// already sequenced; the high-water blob is reopen acceleration only.
    high_water_appends: Mutex<HashMap<String, u64>>,
    packer: Arc<ObjectLogPacker>,
    /// One sequenced produce at a time. Concurrent seals otherwise assign
    /// overlapping or gapped offsets and the apply coordinator holds forever.
    /// Always acquired after the per-shard metadata permit (never inverted).
    produce_lock: tokio::sync::Mutex<()>,
    lock_wait: LockWaitCounters,
    metadata_permits: Arc<MetadataPermits>,
    catalog: Mutex<CatalogDoc>,
    meta_prefix: String,
    definition_authority: DefinitionAuthority,
    definition_permit: tokio::sync::Mutex<()>,
    pre_position_timeout_ms: AtomicU64,
    post_position_timeout_ms: AtomicU64,
    fail_high_water_puts: AtomicU32,
    pre_position_stall_ms: AtomicU64,
}

impl ObjectLogEngineStore<ManifestSequencer> {
    /// Open a durable local-filesystem log under `root` (FIREWEED_OBJECT_LOG_ROOT).
    pub async fn open_local(root: impl AsRef<Path>, flush: FlushConfig) -> EngineResult<Self> {
        let root = root.as_ref();
        std::fs::create_dir_all(root).map_err(store_err)?;
        let root = std::fs::canonicalize(root).map_err(store_err)?;
        let blob: Arc<dyn BlobStore> = Arc::new(LocalBlobStore::new(&root));
        crate::storage_generation::reject_incompatible_storage_generation(
            &blob, "fwlog/", "fwmeta/",
        )
        .await?;
        let sequencer = ManifestSequencer::open(Arc::clone(&blob), "fwmeta/manifest/")
            .await
            .map_err(store_err)?;
        let candidate = Arc::new(LogEngine::new(
            Arc::clone(&blob),
            Arc::new(sequencer),
            flush,
            "fwlog/",
        ));
        let candidate_epochs = Arc::new(Mutex::new(HashMap::new()));
        let candidate_high_water = Arc::new(Mutex::new(HashMap::new()));
        let candidate_metadata_permits = Arc::new(Mutex::new(HashMap::new()));
        let flush_config = format!("{flush:?}");
        let (engine, epochs, high_water, metadata_permits) = {
            let mut registry = local_engine_registry()
                .lock()
                .expect("local engine registry");
            match registry.get(&root).and_then(|entry| {
                Some((
                    entry.engine.upgrade()?,
                    entry.epochs.upgrade()?,
                    entry.high_water.upgrade()?,
                    entry.metadata_permits.upgrade()?,
                    &entry.flush_config,
                ))
            }) {
                Some((engine, epochs, high_water, metadata_permits, existing_config))
                    if existing_config == &flush_config =>
                {
                    (engine, epochs, high_water, metadata_permits)
                }
                Some((_engine, _epochs, _high_water, _metadata_permits, _)) => {
                    return Err(EngineError::Invalid(
                        "object-log namespace is already open with a different flush configuration",
                    ));
                }
                None => {
                    registry.insert(
                        root.clone(),
                        LocalEngineRegistration {
                            engine: Arc::downgrade(&candidate),
                            epochs: Arc::downgrade(&candidate_epochs),
                            high_water: Arc::downgrade(&candidate_high_water),
                            metadata_permits: Arc::downgrade(&candidate_metadata_permits),
                            flush_config,
                        },
                    );
                    (
                        candidate,
                        candidate_epochs,
                        candidate_high_water,
                        candidate_metadata_permits,
                    )
                }
            }
        };
        let blob = Arc::clone(engine.blob_store());
        let store = Self {
            engine,
            blob,
            epochs,
            high_water,
            high_water_appends: Mutex::new(HashMap::new()),
            packer: Arc::new(ObjectLogPacker::new()),
            produce_lock: tokio::sync::Mutex::new(()),
            lock_wait: LockWaitCounters::new(),
            metadata_permits,
            catalog: Mutex::new(CatalogDoc::default()),
            meta_prefix: "fwmeta/".to_string(),
            definition_authority: DefinitionAuthority::Local { root },
            definition_permit: tokio::sync::Mutex::new(()),
            pre_position_timeout_ms: AtomicU64::new(
                OBJECT_LOG_PRE_POSITION_TIMEOUT.as_millis() as u64
            ),
            post_position_timeout_ms: AtomicU64::new(
                OBJECT_LOG_POST_POSITION_TIMEOUT.as_millis() as u64
            ),
            fail_high_water_puts: AtomicU32::new(0),
            pre_position_stall_ms: AtomicU64::new(0),
        };
        store.load_meta().await?;
        Ok(store)
    }

    /// In-memory substrate for tests (sequencer + blob are process-local).
    pub async fn open_memory(flush: FlushConfig) -> EngineResult<Self> {
        let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
        Self::open_with_blob_and_authority(
            blob,
            "fwlog/",
            "fwmeta/",
            flush,
            DefinitionAuthority::ProcessLocal,
        )
        .await
    }

    /// Open against an S3-compatible endpoint (crates.io `object_log::S3BlobStore`).
    ///
    /// Queue-definition authority uses a Fireweed-owned create-only PutObject path
    /// (`If-None-Match: *`) because the BlobStore port is overwrite-only.
    pub async fn open_s3(
        endpoint: &str,
        region: &str,
        bucket: &str,
        access_key_id: &str,
        secret_access_key: &str,
        flush: FlushConfig,
    ) -> EngineResult<Self> {
        Self::open_s3_with_prefixes(
            endpoint,
            region,
            bucket,
            access_key_id,
            secret_access_key,
            "fwlog/",
            "fwmeta/",
            flush,
        )
        .await
    }

    /// S3 open with explicit data/meta prefixes (namespaced product cells).
    #[allow(clippy::too_many_arguments)]
    pub async fn open_s3_with_prefixes(
        endpoint: &str,
        region: &str,
        bucket: &str,
        access_key_id: &str,
        secret_access_key: &str,
        data_prefix: impl Into<String>,
        meta_prefix: impl Into<String>,
        flush: FlushConfig,
    ) -> EngineResult<Self> {
        let blob: Arc<dyn BlobStore> = Arc::new(object_log::S3BlobStore::new(
            endpoint,
            region,
            bucket,
            access_key_id,
            secret_access_key,
        ));
        let put = Arc::new(S3CreateOnlyPut::new(
            endpoint,
            region,
            bucket,
            access_key_id,
            secret_access_key,
        ));
        // fireweed-1d17e656: prove the endpoint enforces If-None-Match:* before accepting
        // NativeConditionalWrite. Probe once per open, not per create_queue.
        let meta_prefix = meta_prefix.into();
        let probe_key = format!(
            "{meta_prefix}create-only-probe/{pid}-{nanos}",
            pid = std::process::id(),
            nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        );
        put.probe_enforced_create_only(&probe_key)
            .await
            .map_err(|err| match err {
                EngineError::Storage(detail) => open_store_err(endpoint, detail),
                other => other,
            })?;
        Self::open_with_blob_and_authority(
            blob,
            data_prefix,
            meta_prefix,
            flush,
            DefinitionAuthority::S3CreateOnly { put },
        )
        .await
        .map_err(|err| match err {
            EngineError::Storage(detail)
                if !detail.contains("INCOMPATIBLE_OBJECT_LOG_GENERATION")
                    && !detail.contains("MIXED_OBJECT_LOG_GENERATION") =>
            {
                open_store_err(endpoint, detail)
            }
            other => other,
        })
    }

    /// Open over an existing blob store (local, memory, or S3) with durable manifest sequencing.
    ///
    /// Fails closed with a stable [`EngineError::Storage`] message containing
    /// [`crate::INCOMPATIBLE_OBJECT_LOG_GENERATION`] (or
    /// [`crate::MIXED_OBJECT_LOG_GENERATION`]) when the blob namespace still holds
    /// retired FWSG segment/manifest layout. See
    /// `docs/operator/object-log-storage-generation.md`.
    pub async fn open_with_blob(
        blob: Arc<dyn BlobStore>,
        data_prefix: impl Into<String>,
        meta_prefix: impl Into<String>,
        flush: FlushConfig,
    ) -> EngineResult<Self> {
        Self::open_with_blob_and_authority(
            blob,
            data_prefix,
            meta_prefix,
            flush,
            DefinitionAuthority::ConditionalCreateUnavailable,
        )
        .await
    }

    async fn open_with_blob_and_authority(
        blob: Arc<dyn BlobStore>,
        data_prefix: impl Into<String>,
        meta_prefix: impl Into<String>,
        flush: FlushConfig,
        definition_authority: DefinitionAuthority,
    ) -> EngineResult<Self> {
        let data_prefix = data_prefix.into();
        let meta_prefix = meta_prefix.into();
        crate::storage_generation::reject_incompatible_storage_generation(
            &blob,
            &data_prefix,
            &meta_prefix,
        )
        .await?;
        let sequencer =
            ManifestSequencer::open(Arc::clone(&blob), format!("{meta_prefix}manifest/"))
                .await
                .map_err(store_err)?;
        let engine = Arc::new(LogEngine::new(
            Arc::clone(&blob),
            Arc::new(sequencer),
            flush,
            data_prefix,
        ));
        let store = Self {
            engine,
            blob,
            epochs: Arc::new(Mutex::new(HashMap::new())),
            high_water: Arc::new(Mutex::new(HashMap::new())),
            high_water_appends: Mutex::new(HashMap::new()),
            packer: Arc::new(ObjectLogPacker::new()),
            produce_lock: tokio::sync::Mutex::new(()),
            lock_wait: LockWaitCounters::new(),
            metadata_permits: Arc::new(Mutex::new(HashMap::new())),
            catalog: Mutex::new(CatalogDoc::default()),
            meta_prefix,
            definition_authority,
            definition_permit: tokio::sync::Mutex::new(()),
            pre_position_timeout_ms: AtomicU64::new(
                OBJECT_LOG_PRE_POSITION_TIMEOUT.as_millis() as u64
            ),
            post_position_timeout_ms: AtomicU64::new(
                OBJECT_LOG_POST_POSITION_TIMEOUT.as_millis() as u64
            ),
            fail_high_water_puts: AtomicU32::new(0),
            pre_position_stall_ms: AtomicU64::new(0),
        };
        store.load_meta().await?;
        Ok(store)
    }
}

impl<S: Sequencer<Meta = ()>> ObjectLogEngineStore<S> {
    fn epoch_key(&self, shard: &QueueKey) -> String {
        format!(
            "{}epochs/{}",
            self.meta_prefix,
            partition_key(shard).0.replace('\0', "/")
        )
    }

    fn high_water_key(&self, shard: &QueueKey) -> String {
        format!(
            "{}high_water/{}",
            self.meta_prefix,
            partition_key(shard).0.replace('\0', "/")
        )
    }

    fn emission_cursor_key(&self, shard: &QueueKey) -> String {
        format!(
            "{}emission_cursor/{}",
            self.meta_prefix,
            partition_key(shard).0.replace('\0', "/")
        )
    }

    /// Durable TD-008 emission cursor for one queue (blob-backed; Class A filesystem/S3 log).
    pub async fn emission_cursor(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        let key = self.emission_cursor_key(shard);
        match self.blob.get(&key).await.map_err(store_err)? {
            Some(bytes) => {
                let doc: HighWaterDoc = serde_json::from_slice(&bytes).map_err(store_err)?;
                Ok(Some(CommandPosition::new(
                    shard.clone(),
                    doc.backend_epoch,
                    doc.sequence,
                )))
            }
            None => Ok(None),
        }
    }

    /// Persist a monotonic emission cursor after a successful sink emit.
    ///
    /// On S3 (`DefinitionAuthority::S3CreateOnly`), advances use **native CAS**:
    /// create-only (`If-None-Match: *`) for the first cursor write and compare-and-swap
    /// (`If-Match: <etag>`) for subsequent advances (P8cs / P1s-attested conditional update).
    /// Filesystem and process-local paths keep the P8c metadata-permit + overwrite put.
    pub async fn set_emission_cursor(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
    ) -> EngineResult<()> {
        if position.queue != *shard {
            return Err(EngineError::Invalid("emission cursor queue mismatch"));
        }
        let permit = self.metadata_permit(shard);
        let wait_started = Instant::now();
        let _guard = permit.lock().await;
        self.lock_wait
            .record_emission_cursor(wait_started.elapsed());
        match &self.definition_authority {
            DefinitionAuthority::S3CreateOnly { put } => {
                self.set_emission_cursor_s3_cas(put.as_ref(), shard, position)
                    .await
            }
            DefinitionAuthority::Local { .. }
            | DefinitionAuthority::ProcessLocal
            | DefinitionAuthority::ConditionalCreateUnavailable => {
                if let Some(current) = self.emission_cursor(shard).await?
                    && !current.precedes(&position)
                    && current != position
                {
                    return Err(EngineError::Invalid("emission cursor regression"));
                }
                self.put_json(
                    &self.emission_cursor_key(shard),
                    &HighWaterDoc {
                        backend_epoch: position.backend_epoch,
                        sequence: position.sequence,
                    },
                )
                .await
            }
        }
    }

    /// S3 native CAS advance for the emission cursor (P8cs).
    ///
    /// Retry budget covers concurrent writers that lose an If-Match race; each attempt
    /// re-reads the durable cursor and re-checks monotonicity before writing.
    async fn set_emission_cursor_s3_cas(
        &self,
        put: &S3CreateOnlyPut,
        shard: &QueueKey,
        position: CommandPosition,
    ) -> EngineResult<()> {
        let key = self.emission_cursor_key(shard);
        let payload = Bytes::from(
            serde_json::to_vec(&HighWaterDoc {
                backend_epoch: position.backend_epoch,
                sequence: position.sequence,
            })
            .map_err(store_err)?,
        );
        for _attempt in 0..16u8 {
            match put.get_with_etag(&key).await? {
                None => {
                    // First durable cursor: create-only so two writers cannot both invent
                    // a cursor under a lost-update overwrite.
                    if put.put_if_absent(&key, payload.clone()).await? {
                        return Ok(());
                    }
                    // Lost the create race — re-read and decide idempotent vs advance.
                    continue;
                }
                Some((bytes, etag)) => {
                    let doc: HighWaterDoc = serde_json::from_slice(&bytes).map_err(store_err)?;
                    let current =
                        CommandPosition::new(shard.clone(), doc.backend_epoch, doc.sequence);
                    if current == position {
                        return Ok(());
                    }
                    if !current.precedes(&position) {
                        return Err(EngineError::Invalid("emission cursor regression"));
                    }
                    if put.put_if_match(&key, payload.clone(), &etag).await? {
                        return Ok(());
                    }
                    // Lost CAS race — another writer advanced; retry with a fresh ETag.
                }
            }
        }
        Err(EngineError::Storage(
            "emission cursor CAS exhausted after concurrent S3 conditional-write races".into(),
        ))
    }

    pub fn supports_emission_cursor(&self) -> bool {
        true
    }

    fn latest_snapshot_key(&self, shard: &QueueKey) -> String {
        format!(
            "{}snapshots/{}/latest.json",
            self.meta_prefix,
            partition_component(shard)
        )
    }

    fn snapshot_payload_key(&self, shard: &QueueKey, ref_id: &str) -> String {
        format!(
            "{}snapshots/{}/objects/{ref_id}.bin",
            self.meta_prefix,
            partition_component(shard)
        )
    }

    fn catalog_key(&self) -> String {
        format!("{}catalog.json", self.meta_prefix)
    }

    fn definition_prefix(&self) -> String {
        format!("{}queue_definitions/", self.meta_prefix)
    }

    fn metadata_permit(&self, shard: &QueueKey) -> Arc<tokio::sync::Mutex<()>> {
        self.metadata_permits
            .lock()
            .expect("metadata permits")
            .entry(partition_key(shard).0)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    fn definition_key(&self, shard: &QueueKey) -> String {
        format!(
            "{}{}.json",
            self.definition_prefix(),
            partition_component(shard)
        )
    }

    fn cache_definition(&self, definition: QueueDefinition) {
        let mut catalog = self.catalog.lock().expect("catalog");
        if let Some(existing) = catalog.definitions.iter_mut().find(|existing| {
            existing.tenant_id == definition.tenant_id && existing.queue_id == definition.queue_id
        }) {
            *existing = definition;
        } else {
            catalog.definitions.push(definition);
        }
    }

    async fn load_meta(&self) -> EngineResult<()> {
        // Read the retired aggregate catalog for backward-compatible reopen, then merge every
        // per-queue authority record. New writes never update catalog.json: independent queues
        // therefore cannot erase one another through a stale read/modify/write cycle.
        if let Some(bytes) = self
            .blob
            .get(&self.catalog_key())
            .await
            .map_err(store_err)?
        {
            let doc: CatalogDoc = serde_json::from_slice(&bytes).map_err(store_err)?;
            *self.catalog.lock().expect("catalog mutex") = doc;
        }
        let mut keys = self
            .blob
            .list(&self.definition_prefix())
            .await
            .map_err(store_err)?;
        keys.sort();
        for key in keys {
            let bytes = self
                .blob
                .get(&key)
                .await
                .map_err(store_err)?
                .ok_or_else(|| EngineError::Storage(format!("definition disappeared: {key}")))?;
            let definition: QueueDefinition = serde_json::from_slice(&bytes).map_err(store_err)?;
            self.cache_definition(definition);
        }
        Ok(())
    }

    async fn put_json(&self, key: &str, value: &impl Serialize) -> EngineResult<()> {
        if key.contains("high_water/") {
            let remaining = self.fail_high_water_puts.load(Ordering::SeqCst);
            if remaining > 0
                && self
                    .fail_high_water_puts
                    .compare_exchange(remaining, remaining - 1, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                return Err(EngineError::Storage(
                    "injected high-water put_json failure".into(),
                ));
            }
        }
        let bytes = Bytes::from(serde_json::to_vec(value).map_err(store_err)?);
        self.blob.put(key, bytes).await.map_err(store_err)
    }

    async fn load_epoch(&self, shard: &QueueKey) -> EngineResult<u64> {
        let pk = partition_key(shard).0;
        if let Some(e) = self.epochs.lock().expect("epochs").get(&pk).copied() {
            return Ok(e);
        }
        let key = self.epoch_key(shard);
        let epoch = match self.blob.get(&key).await.map_err(store_err)? {
            Some(bytes) => {
                let doc: EpochDoc = serde_json::from_slice(&bytes).map_err(store_err)?;
                doc.epoch
            }
            None => 0,
        };
        self.epochs.lock().expect("epochs").insert(pk, epoch);
        Ok(epoch)
    }

    async fn store_epoch(&self, shard: &QueueKey, epoch: u64) -> EngineResult<()> {
        let pk = partition_key(shard).0;
        self.epochs.lock().expect("epochs").insert(pk, epoch);
        self.put_json(&self.epoch_key(shard), &EpochDoc { epoch })
            .await
    }

    /// Atomically publish or read the durable first-writer definition for one queue.
    ///
    /// The returned winner is cached even when the caller later rejects it as incompatible.
    /// Local files use an immutable hard-link publication after syncing a unique temporary file;
    /// no process-local mutex decides the winner. The process-local memory adapter uses the short
    /// catalog permit because it has no cross-process durability contract.
    pub async fn create_or_read_definition(
        &self,
        definition: QueueDefinition,
    ) -> EngineResult<CreateQueueOutcome> {
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let key = self.definition_key(&shard);
        // A definition found only in the retired aggregate catalog is still an existing durable
        // winner. Publish that exact value into the immutable per-queue layout before considering
        // the caller's candidate; migration must never let a later create rewrite legacy truth.
        let legacy_winner = self
            .catalog
            .lock()
            .expect("catalog")
            .definitions
            .iter()
            .find(|existing| {
                existing.tenant_id == definition.tenant_id
                    && existing.queue_id == definition.queue_id
            })
            .cloned();
        let publication = legacy_winner.clone().unwrap_or_else(|| definition.clone());
        let bytes = serde_json::to_vec(&publication).map_err(store_err)?;
        let physically_created = match &self.definition_authority {
            DefinitionAuthority::Local { root } => {
                let root = root.clone();
                let key = key.clone();
                tokio::task::spawn_blocking(move || publish_local_definition(&root, &key, &bytes))
                    .await
                    .map_err(|error| {
                        EngineError::Storage(format!(
                            "local definition publication worker failed: {error}"
                        ))
                    })??
            }
            DefinitionAuthority::ProcessLocal => {
                let _permit = self.definition_permit.lock().await;
                if self.blob.get(&key).await.map_err(store_err)?.is_some() {
                    false
                } else {
                    self.blob
                        .put(&key, Bytes::from(bytes))
                        .await
                        .map_err(store_err)?;
                    true
                }
            }
            DefinitionAuthority::S3CreateOnly { put } => {
                put.put_if_absent(&key, Bytes::from(bytes)).await?
            }
            DefinitionAuthority::ConditionalCreateUnavailable => {
                return Err(EngineError::Storage(
                    "NativeConditionalWrite queue-definition authority is unavailable: the \
                     configured BlobStore exposes overwrite-only put and cannot prove create-only \
                     publication; use the local filesystem adapter or the S3 open path (which \
                     issues If-None-Match: * PutObject for definitions)"
                        .into(),
                ));
            }
        };
        let created = physically_created && legacy_winner.is_none();
        let winner_bytes = self
            .blob
            .get(&key)
            .await
            .map_err(store_err)?
            .ok_or_else(|| {
                EngineError::Storage("definition authority vanished after publish".into())
            })?;
        let winner: QueueDefinition = serde_json::from_slice(&winner_bytes).map_err(store_err)?;
        if winner.tenant_id != definition.tenant_id || winner.queue_id != definition.queue_id {
            return Err(EngineError::Storage(
                "queue-definition authority key contains a different queue identity".into(),
            ));
        }
        self.cache_definition(winner.clone());
        Ok(CreateQueueOutcome {
            created,
            definition: winner,
        })
    }
}

fn publish_local_definition(root: &Path, key: &str, bytes: &[u8]) -> EngineResult<bool> {
    let final_path = root.join(key);
    let parent = final_path
        .parent()
        .ok_or_else(|| EngineError::Storage("definition authority path has no parent".into()))?;
    std::fs::create_dir_all(parent).map_err(store_err)?;

    let ordinal = DEFINITION_ORDINAL.fetch_add(1, Ordering::Relaxed);
    let temp_path = parent.join(format!(".definition-{}-{ordinal}.tmp", std::process::id()));
    let mut temp = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp_path)
        .map_err(store_err)?;
    IoWrite::write_all(&mut temp, bytes).map_err(store_err)?;
    temp.sync_all().map_err(store_err)?;
    drop(temp);

    let publish = std::fs::hard_link(&temp_path, &final_path);
    let created = match publish {
        Ok(()) => {
            std::fs::File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(store_err)?;
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => false,
        Err(error) => {
            let _ = std::fs::remove_file(&temp_path);
            return Err(store_err(format!(
                "local create-only definition publication failed: {error}"
            )));
        }
    };
    std::fs::remove_file(&temp_path).map_err(store_err)?;
    Ok(created)
}

/// Map env-style segment knobs onto object-log flush config (names unchanged at product edge).
///
/// `target_bytes` is a packing *preference*, not the engine hard ceiling.
/// Stuffing it into `FlushConfig::max_bytes` (historical) forced a seal on every
/// large command and stopped linger from co-buffering concurrent produces.
/// `max_bytes` stays the object-log physics ceiling (default 1 GiB) unless the
/// caller asks for a larger one.
///
/// Packing of concurrent Fireweed appends is owned by [`ObjectLogPacker`].
/// The engine linger is 1 ms so a already-packed produce seals immediately.
pub fn flush_config_from_segment(target_bytes: usize, max_latency_ms: u64) -> FlushConfig {
    let mut cfg = FlushConfig::default();
    if target_bytes > cfg.max_bytes {
        cfg.max_bytes = target_bytes;
    }
    cfg.max_batches = fireweed_engine::PRODUCTION_OBJECT_LOG_MAX_BATCHES;
    let _ = max_latency_ms;
    // Packer already grouped commands. Engine must not add another linger.
    cfg.linger = Duration::from_millis(1);
    cfg.max_inflight_flushes = 8;
    cfg.budget.enabled = false;
    cfg
}

impl<S: Sequencer<Meta = ()> + 'static> ObjectLogEngineStore<S> {
    /// Emit durable change-record tail from the emission cursor (TD-008 / P8c filesystem, P8cs S3 CAS).
    pub async fn emit_change_record_tail<Sk>(
        &self,
        shard: &QueueKey,
        sink: &Sk,
        limit: usize,
        emitted_at: fireweed_core::UtcTimestamp,
        source_owner_id: Option<fireweed_core::OwnerId>,
    ) -> EngineResult<usize>
    where
        Sk: fireweed_engine::ChangeRecordSink + ?Sized,
    {
        use fireweed_engine::command_envelope_change_records;

        let cursor = self.emission_cursor(shard).await?;
        let page = AsyncLogStore::read_from(self, shard.clone(), cursor, limit).await?;
        if page.entries.is_empty() {
            return Ok(0);
        }
        let mut records = Vec::new();
        for (position, env) in &page.entries {
            records.extend(command_envelope_change_records(
                shard,
                position,
                env,
                emitted_at,
                source_owner_id.clone(),
            ));
        }
        sink.emit(shard, &records)?;
        if let Some((position, _)) = page.entries.last() {
            self.set_emission_cursor(shard, position.clone()).await?;
        }
        Ok(records.len())
    }

    /// Exclusive produce: skip the packer. Push/Claim/Finalize already hold the
    /// per-queue admit permit, so a linger can never attract a second waiter.
    pub async fn append_exclusive(
        &self,
        shard: QueueKey,
        commands: Vec<CommandEnvelope>,
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        let epoch = self.load_epoch(&shard).await?;
        if epoch != expected_epoch {
            return Err(EngineError::EpochFenced);
        }
        self.produce_immediate(&shard, commands, expected_epoch, None)
            .await
            .map_err(PackedAppendError::into_engine)
    }

    pub fn packer_stats(&self) -> PackerStats {
        self.packer.snapshot()
    }

    pub fn lock_wait_stats(&self) -> LockWaitStats {
        self.lock_wait.snapshot()
    }

    /// Group-commit path for ports that do not hold the per-queue admit permit
    /// (BatchUpdate / upsert). Concurrent callers of the same shard share one
    /// object PUT when they arrive within [`PACK_LINGER`] or fill [`PACK_TARGET_BYTES`].
    pub async fn packed_append(
        &self,
        shard: QueueKey,
        commands: Vec<CommandEnvelope>,
        expected_epoch: u64,
    ) -> Result<PackedAppendOutcome, PackedAppendError> {
        self.packed_append_owned(shard, commands, expected_epoch, None, false)
            .await
    }

    /// Force-seal only this shard/epoch/lane group, charging exact envelope bytes.
    pub async fn packed_append_force_seal(
        &self,
        shard: QueueKey,
        commands: Vec<CommandEnvelope>,
        expected_epoch: u64,
        reservation_id: Option<u64>,
    ) -> Result<PackedAppendOutcome, PackedAppendError> {
        self.packed_append_owned(shard, commands, expected_epoch, reservation_id, true)
            .await
    }

    /// Packed append that carries a reservation id so co-sealed followers transfer to the leader.
    pub async fn packed_append_owned(
        &self,
        shard: QueueKey,
        commands: Vec<CommandEnvelope>,
        expected_epoch: u64,
        reservation_id: Option<u64>,
        force_seal: bool,
    ) -> Result<PackedAppendOutcome, PackedAppendError> {
        if commands.is_empty() {
            return Ok(PackedAppendOutcome {
                positions: Vec::new(),
                apply_batch: None,
                apply_published: ApplyPublish::already_done(),
            });
        }
        let epoch = self
            .load_epoch(&shard)
            .await
            .map_err(PackedAppendError::BeforePosition)?;
        if epoch != expected_epoch {
            return Err(PackedAppendError::BeforePosition(EngineError::EpochFenced));
        }
        let bytes =
            exact_envelope_bytes(&commands).map_err(PackedAppendError::BeforePosition)? as usize;
        let lane = pack_lane(&commands);
        let group = PackGroupKey {
            shard: shard.clone(),
            epoch: expected_epoch,
            lane,
        };
        let (tx, rx) = oneshot::channel();
        let joined_at = Instant::now();
        let should_seal = {
            let mut state = self.packer.state.lock().expect("packer");
            if !state.pending.is_empty()
                && state
                    .pending
                    .iter()
                    .any(|w| w.shard != shard || w.epoch != expected_epoch)
            {
                self.packer.notify.notify_waiters();
            }
            state.pending.push(PackWaiter {
                shard,
                epoch: expected_epoch,
                lane,
                commands,
                bytes,
                reservation_id,
                joined_at,
                tx,
            });
            state.bytes = state.bytes.saturating_add(bytes);
            if state.oldest.is_none() {
                state.oldest = Some(joined_at);
            }
            force_seal || ObjectLogPacker::ready_locked(&state)
        };
        if should_seal {
            self.packer.notify.notify_waiters();
            if force_seal {
                self.seal_own_group(group.clone()).await;
            } else {
                self.seal_packed(true).await;
            }
            return self.await_packed_result(rx, &group).await;
        }
        tokio::pin!(rx);
        tokio::select! {
            result = &mut rx => {
                return self.unpack_packed_result(result.map_err(|_| ()), &group);
            }
            _ = tokio::time::sleep(PACK_LINGER) => {
                self.seal_packed(true).await;
            }
        }
        self.unpack_packed_result(rx.await.map_err(|_| ()), &group)
    }

    async fn await_packed_result(
        &self,
        rx: oneshot::Receiver<Result<PackedAppendOutcome, PackedAppendError>>,
        group: &PackGroupKey,
    ) -> Result<PackedAppendOutcome, PackedAppendError> {
        self.unpack_packed_result(rx.await.map_err(|_| ()), group)
    }

    fn unpack_packed_result(
        &self,
        result: Result<Result<PackedAppendOutcome, PackedAppendError>, ()>,
        group: &PackGroupKey,
    ) -> Result<PackedAppendOutcome, PackedAppendError> {
        match result {
            Ok(outcome) => outcome,
            Err(_) => {
                let phase = self
                    .packer
                    .state
                    .lock()
                    .expect("packer")
                    .groups
                    .get(group)
                    .copied();
                if phase == Some(PackGroupPhase::PostPosition) {
                    Err(PackedAppendError::PostPositionAmbiguous {
                        shard: group.shard.clone(),
                        reason: "object-log packer waiter dropped after position".into(),
                    })
                } else {
                    Err(PackedAppendError::BeforePosition(EngineError::Storage(
                        "object-log packer waiter dropped".into(),
                    )))
                }
            }
        }
    }

    async fn seal_own_group(&self, group: PackGroupKey) {
        let waiters = {
            let mut state = self.packer.state.lock().expect("packer");
            take_group(&mut state, &group)
        };
        if waiters.is_empty() {
            return;
        }
        self.seal_group(group, waiters).await;
    }

    async fn seal_packed(&self, force: bool) {
        let pending = {
            let mut state = self.packer.state.lock().expect("packer");
            if state.pending.is_empty() {
                return;
            }
            if !force
                && !ObjectLogPacker::ready_locked(&state)
                && state.oldest.is_some_and(|t| t.elapsed() < PACK_LINGER)
            {
                return;
            }
            state.bytes = 0;
            state.oldest = None;
            std::mem::take(&mut state.pending)
        };
        if pending.is_empty() {
            return;
        }
        let mut groups: HashMap<PackGroupKey, Vec<PackWaiter>> = HashMap::new();
        for w in pending {
            groups
                .entry(PackGroupKey {
                    shard: w.shard.clone(),
                    epoch: w.epoch,
                    lane: w.lane,
                })
                .or_default()
                .push(w);
        }
        for (group, waiters) in groups {
            self.seal_group(group, waiters).await;
        }
    }

    async fn seal_group(&self, group: PackGroupKey, waiters: Vec<PackWaiter>) {
        let waiter_n = waiters.len() as u64;
        let command_n = waiters.iter().map(|w| w.commands.len() as u64).sum::<u64>();
        let byte_n = waiters.iter().map(|w| w.bytes as u64).sum::<u64>();
        self.packer.counters.seals.fetch_add(1, Ordering::Relaxed);
        self.packer
            .counters
            .waiters
            .fetch_add(waiter_n, Ordering::Relaxed);
        self.packer
            .counters
            .commands
            .fetch_add(command_n, Ordering::Relaxed);
        self.packer
            .counters
            .bytes
            .fetch_add(byte_n, Ordering::Relaxed);
        let pre_deadline = waiters
            .iter()
            .map(|w| w.joined_at)
            .min()
            .unwrap_or_else(Instant::now)
            + Duration::from_millis(self.pre_position_timeout_ms.load(Ordering::Relaxed));
        {
            let mut state = self.packer.state.lock().expect("packer");
            state
                .groups
                .insert(group.clone(), PackGroupPhase::PrePosition);
        }
        let counts: Vec<usize> = waiters.iter().map(|w| w.commands.len()).collect();
        let mut all = Vec::with_capacity(counts.iter().sum());
        for w in &waiters {
            all.extend(w.commands.iter().cloned());
        }
        let gate = PackedProduceGate {
            group: group.clone(),
            pre_deadline,
        };
        let result = self
            .produce_immediate(&group.shard, all.clone(), group.epoch, Some(gate))
            .await;
        {
            let mut state = self.packer.state.lock().expect("packer");
            state.groups.remove(&group);
        }
        match result {
            Ok(positions) => {
                let mut offset = 0usize;
                let mut leader = true;
                let apply_published = ApplyPublish::new();
                let transferred_reservation_ids: Vec<u64> = waiters
                    .iter()
                    .skip(1)
                    .filter_map(|w| w.reservation_id)
                    .collect();
                for (w, n) in waiters.into_iter().zip(counts) {
                    let slice = positions
                        .get(offset..offset + n)
                        .map(|s| s.to_vec())
                        .unwrap_or_default();
                    offset += n;
                    let apply_batch = if leader {
                        leader = false;
                        Some(PackedApplyBatch {
                            positions: positions.clone(),
                            commands: all.clone(),
                            transferred_reservation_ids: transferred_reservation_ids.clone(),
                        })
                    } else {
                        None
                    };
                    let _ = w.tx.send(Ok(PackedAppendOutcome {
                        positions: slice,
                        apply_batch,
                        apply_published: Arc::clone(&apply_published),
                    }));
                }
            }
            Err(error) => {
                for w in waiters {
                    let _ = w.tx.send(Err(error.clone()));
                }
            }
        }
    }

    async fn produce_immediate(
        &self,
        shard: &QueueKey,
        commands: Vec<CommandEnvelope>,
        expected_epoch: u64,
        gate: Option<PackedProduceGate>,
    ) -> Result<Vec<CommandPosition>, PackedAppendError> {
        if commands.is_empty() {
            return Ok(Vec::new());
        }
        let pre_budget =
            Duration::from_millis(self.pre_position_timeout_ms.load(Ordering::Relaxed));
        let pre_deadline = gate
            .as_ref()
            .map(|gate| gate.pre_deadline)
            .unwrap_or_else(|| Instant::now() + pre_budget);
        let post_timeout =
            Duration::from_millis(self.post_position_timeout_ms.load(Ordering::Relaxed));
        let permit = self.metadata_permit(shard);
        let pre = async {
            let stall_ms = self.pre_position_stall_ms.load(Ordering::Relaxed);
            if stall_ms > 0 {
                tokio::time::sleep(Duration::from_millis(stall_ms)).await;
            }
            // Terminal suffix: metadata-permit then produce-lock. The permit is held
            // across produce and the permit-held high-water decision/advance.
            let wait_started = Instant::now();
            let metadata = permit.lock().await;
            self.lock_wait.record_append(wait_started.elapsed());
            let epoch = self
                .load_epoch(shard)
                .await
                .map_err(PackedAppendError::BeforePosition)?;
            if epoch != expected_epoch {
                return Err(PackedAppendError::BeforePosition(EngineError::EpochFenced));
            }
            let wait_started = Instant::now();
            let produce = self.produce_lock.lock().await;
            self.lock_wait.record_append(wait_started.elapsed());
            let payload = Bytes::from(
                fireweed_engine::command_codec::encode_log_batch(expected_epoch, &commands)
                    .map_err(|error| PackedAppendError::BeforePosition(store_err(error)))?,
            );
            let record_count = i32::try_from(commands.len()).map_err(|_| {
                PackedAppendError::BeforePosition(EngineError::Invalid(
                    "batch too large for object-log record_count",
                ))
            })?;
            Ok((metadata, produce, payload, record_count))
        };
        let remaining = pre_deadline.saturating_duration_since(Instant::now());
        let (metadata, produce, payload, record_count) =
            match tokio::time::timeout(remaining, pre).await {
                Ok(Ok(parts)) => parts,
                Ok(Err(error)) => return Err(error),
                Err(_) => return Err(PackedAppendError::before_timeout()),
            };
        let _metadata = metadata;
        let _produce = produce;
        if let Some(gate) = &gate {
            let mut state = self.packer.state.lock().expect("packer");
            match state.groups.get_mut(&gate.group) {
                Some(phase) if *phase == PackGroupPhase::PrePosition => {
                    *phase = PackGroupPhase::PostPosition;
                }
                Some(_) | None => return Err(PackedAppendError::before_timeout()),
            }
        }
        let produced = async {
            let outcome = self
                .engine
                .produce(
                    partition_key(shard),
                    payload,
                    record_count,
                    (),
                    Durability::Sequenced,
                )
                .await
                .map_err(store_err)?;
            let base = outcome.base_offset.ok_or_else(|| {
                EngineError::Storage("sequenced produce missing base_offset".into())
            })? as u64;
            let positions: Vec<CommandPosition> = (0..commands.len() as u64)
                .map(|i| CommandPosition::new(shard.clone(), expected_epoch, base + i))
                .collect();
            if let Some(last) = positions.last() {
                self.advance_high_water_held(shard, last).await?;
            }
            EngineResult::Ok(positions)
        };
        match tokio::time::timeout(post_timeout, produced).await {
            Ok(Ok(positions)) => Ok(positions),
            Ok(Err(error)) => Err(PackedAppendError::PostPositionAmbiguous {
                shard: shard.clone(),
                reason: error.to_string(),
            }),
            Err(_) => Err(PackedAppendError::PostPositionAmbiguous {
                shard: shard.clone(),
                reason: "object-log post-position produce timed out".into(),
            }),
        }
    }

    /// Advance high-water after a sequenced produce.
    ///
    /// The caller must already hold this shard's metadata permit; this helper
    /// never re-acquires it (tokio's mutex is not reentrant).
    async fn advance_high_water_held(
        &self,
        shard: &QueueKey,
        last: &CommandPosition,
    ) -> EngineResult<()> {
        let should_advance = self
            .high_water
            .lock()
            .expect("high_water")
            .get(&partition_key(shard).0)
            .is_none_or(|current| current.precedes(last));
        if !should_advance {
            return Ok(());
        }
        let pk = partition_key(shard).0;
        self.high_water
            .lock()
            .expect("high_water")
            .insert(pk.clone(), last.clone());
        let appends = {
            let mut counts = self.high_water_appends.lock().expect("high_water_appends");
            let n = counts.entry(pk).or_insert(0);
            *n += 1;
            *n
        };
        if appends == 1 || appends.is_multiple_of(64) {
            self.put_json(
                &self.high_water_key(shard),
                &HighWaterDoc {
                    backend_epoch: last.backend_epoch,
                    sequence: last.sequence,
                },
            )
            .await?;
        }
        Ok(())
    }
}

pub(crate) fn exact_envelope_bytes(commands: &[CommandEnvelope]) -> EngineResult<u64> {
    let mut total = 0_u64;
    for command in commands {
        let encoded = fireweed_engine::command_codec::encode_command_envelope(command)?;
        total = total.saturating_add(encoded.len() as u64);
    }
    Ok(total)
}

fn take_group(state: &mut PackState, group: &PackGroupKey) -> Vec<PackWaiter> {
    let mut taken = Vec::new();
    let mut remain = Vec::new();
    for waiter in state.pending.drain(..) {
        if waiter.shard == group.shard && waiter.epoch == group.epoch && waiter.lane == group.lane {
            taken.push(waiter);
        } else {
            remain.push(waiter);
        }
    }
    state.pending = remain;
    state.bytes = state.pending.iter().map(|waiter| waiter.bytes).sum();
    state.oldest = state.pending.iter().map(|waiter| waiter.joined_at).min();
    taken
}

impl<S: Sequencer<Meta = ()> + 'static> AsyncLogStore for ObjectLogEngineStore<S> {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::EventualApply
    }

    fn is_durable_log(&self) -> bool {
        true
    }

    fn ensure_shard(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            let _ = self.load_epoch(&shard).await?;
            Ok(())
        }
    }

    fn current_epoch(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        async move { self.load_epoch(&shard).await }
    }

    fn acquire_epoch(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        async move {
            let permit = self.metadata_permit(&shard);
            let wait_started = Instant::now();
            let _guard = permit.lock().await;
            self.lock_wait.record_epoch_acquire(wait_started.elapsed());
            let next = self.load_epoch(&shard).await?.saturating_add(1);
            self.store_epoch(&shard, next).await?;
            Ok(next)
        }
    }

    fn append(
        &self,
        shard: QueueKey,
        commands: Vec<CommandEnvelope>,
        expected_epoch: u64,
    ) -> impl std::future::Future<Output = EngineResult<Vec<CommandPosition>>> + Send {
        async move {
            let epoch = self.load_epoch(&shard).await?;
            if epoch != expected_epoch {
                return Err(EngineError::EpochFenced);
            }
            self.produce_immediate(&shard, commands, expected_epoch, None)
                .await
                .map_err(PackedAppendError::into_engine)
        }
    }

    fn read_from(
        &self,
        shard: QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<CommandPage>> + Send {
        async move {
            let from_seq = from
                .as_ref()
                .map(|p| p.sequence.saturating_add(1))
                .unwrap_or(0);
            let batches = self
                .engine
                .fetch(&partition_key(&shard), from_seq as i64, 4 * 1024 * 1024)
                .await
                .map_err(store_err)?;
            let mut entries = Vec::new();
            for batch in batches {
                let (backend_epoch, decoded) =
                    fireweed_engine::command_codec::decode_log_batch(&batch.payload)
                        .map_err(store_err)?;
                for (i, env) in decoded.into_iter().enumerate() {
                    let seq = batch.base_offset as u64 + i as u64;
                    if seq < from_seq {
                        continue;
                    }
                    entries.push((CommandPosition::new(shard.clone(), backend_epoch, seq), env));
                    if entries.len() >= limit {
                        break;
                    }
                }
                if entries.len() >= limit {
                    break;
                }
            }
            // Always return a cursor when this page is non-empty. A 4 MiB engine fetch may
            // return fewer than `limit` entries while more history remains; stopping early would
            // truncate genesis recovery (observed as a short tail under multi-million loads —
            // fireweed-3aaa3ebc / TestE3RecoveryExactGenesisReplay).
            let next = entries
                .last()
                .map(|(p, _)| CommandPosition::new(shard.clone(), p.backend_epoch, p.sequence));
            Ok(CommandPage { entries, next })
        }
    }

    fn high_water(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        async move {
            let pk = partition_key(&shard).0;
            if let Some(p) = self
                .high_water
                .lock()
                .expect("high_water")
                .get(&pk)
                .cloned()
            {
                return Ok(Some(p));
            }
            let key = self.high_water_key(&shard);
            match self.blob.get(&key).await.map_err(store_err)? {
                Some(bytes) => {
                    let doc: HighWaterDoc = serde_json::from_slice(&bytes).map_err(store_err)?;
                    let pos = CommandPosition::new(shard, doc.backend_epoch, doc.sequence);
                    self.high_water
                        .lock()
                        .expect("high_water")
                        .insert(pk, pos.clone());
                    Ok(Some(pos))
                }
                None => Ok(None),
            }
        }
    }

    fn set_high_water(
        &self,
        shard: QueueKey,
        position: CommandPosition,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            if position.queue != shard {
                return Err(EngineError::Invalid("high-water queue mismatch"));
            }
            let permit = self.metadata_permit(&shard);
            let _guard = permit.lock().await;
            if let Some(current) = AsyncLogStore::high_water(self, shard.clone()).await?
                && position.precedes(&current)
            {
                return Err(EngineError::Invalid("high-water regression"));
            }
            self.high_water
                .lock()
                .expect("high_water")
                .insert(partition_key(&shard).0, position.clone());
            self.put_json(
                &self.high_water_key(&shard),
                &HighWaterDoc {
                    backend_epoch: position.backend_epoch,
                    sequence: position.sequence,
                },
            )
            .await
        }
    }

    fn write_snapshot(
        &self,
        shard: QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> impl std::future::Future<Output = EngineResult<SnapshotRef>> + Send {
        async move {
            if position.queue != shard {
                return Err(EngineError::Invalid("snapshot position queue mismatch"));
            }

            let ref_id = next_snapshot_ref_id();
            self.blob
                .put(
                    &self.snapshot_payload_key(&shard, &ref_id),
                    Bytes::from(snapshot.payload),
                )
                .await
                .map_err(store_err)?;
            self.put_json(
                &self.latest_snapshot_key(&shard),
                &SnapshotMetaDoc {
                    backend_epoch: position.backend_epoch,
                    sequence: position.sequence,
                    ref_id: ref_id.clone(),
                },
            )
            .await?;

            Ok(SnapshotRef {
                queue: shard,
                position,
                ref_id,
            })
        }
    }

    fn latest_snapshot(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<SnapshotRef>>> + Send {
        async move {
            let Some(bytes) = self
                .blob
                .get(&self.latest_snapshot_key(&shard))
                .await
                .map_err(store_err)?
            else {
                return Ok(None);
            };
            let doc: SnapshotMetaDoc = serde_json::from_slice(&bytes).map_err(store_err)?;
            if !valid_snapshot_ref_id(&doc.ref_id) {
                return Err(EngineError::Storage(
                    "invalid object-log snapshot reference metadata".into(),
                ));
            }
            Ok(Some(SnapshotRef {
                position: CommandPosition::new(shard.clone(), doc.backend_epoch, doc.sequence),
                queue: shard,
                ref_id: doc.ref_id,
            }))
        }
    }

    fn read_snapshot(
        &self,
        snapshot_ref: SnapshotRef,
    ) -> impl std::future::Future<Output = EngineResult<ProjectionSnapshot>> + Send {
        async move {
            if snapshot_ref.position.queue != snapshot_ref.queue {
                return Err(EngineError::Invalid("snapshot position queue mismatch"));
            }
            if !valid_snapshot_ref_id(&snapshot_ref.ref_id) {
                return Err(EngineError::Invalid("invalid snapshot reference"));
            }
            let payload = self
                .blob
                .get(&self.snapshot_payload_key(&snapshot_ref.queue, &snapshot_ref.ref_id))
                .await
                .map_err(store_err)?
                .ok_or(EngineError::NotFound)?;
            Ok(ProjectionSnapshot {
                payload: payload.to_vec(),
            })
        }
    }

    fn recover_definitions(
        &self,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueDefinition>>> + Send {
        async move {
            self.load_meta().await?;
            Ok(self.catalog.lock().expect("catalog").definitions.clone())
        }
    }
}

// Silence unused helper until S3 open path is wired.
#[allow(dead_code)]
fn _parse_partition_smoke(key: &PartitionKey) -> Option<QueueKey> {
    parse_partition(key)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    use fireweed_core::{
        EligibilityPolicy, ItemId, OrderingMode, PriorityModel, QueueDefinition, QueueId,
        RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
    };
    use fireweed_engine::{
        AsyncLogStore, AsyncProjectionSpec, CommandChecksum, CommandEnvelope, CommandId,
        CommandPosition, EngineError, FinalizeCommand, FinalizeKind, FinalizeOutcome,
        ProjectionSnapshot, QueueCommand, QueueKey, SnapshotRef,
    };
    use fireweed_projection::{AsyncInMemoryProjection, InMemoryProjection};

    use super::{
        OBJECT_LOG_POST_POSITION_TIMEOUT, OBJECT_LOG_PRE_POSITION_TIMEOUT, ObjectLogEngineStore,
        PackedAppendError,
    };
    use crate::async_projection_apply::AsyncProjectionApplyCoordinator;
    use object_log::FlushConfig;

    fn temp_root(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "fireweed-olog-{tag}-{}-{}",
            std::process::id(),
            super::DEFINITION_ORDINAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ))
    }

    fn zero_linger() -> FlushConfig {
        FlushConfig {
            linger: std::time::Duration::ZERO,
            ..FlushConfig::default()
        }
    }

    fn qdef() -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("t").unwrap(),
            queue_id: QueueId::new("q").unwrap(),
            priority_model: PriorityModel::timestamp_ascending(),
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

    #[tokio::test]
    async fn local_definition_authority_returns_preopen_winner_and_full_definition() {
        let root = temp_root("definition-preopen");
        let _ = std::fs::remove_dir_all(&root);
        let winner = ObjectLogEngineStore::open_local(&root, zero_linger())
            .await
            .unwrap();
        let compatible_loser = ObjectLogEngineStore::open_local(&root, zero_linger())
            .await
            .unwrap();
        let incompatible_loser = ObjectLogEngineStore::open_local(&root, zero_linger())
            .await
            .unwrap();
        let definition = qdef();
        let mut incompatible = definition.clone();
        incompatible.request_id_retention_ms += 1;

        assert!(
            winner
                .create_or_read_definition(definition.clone())
                .await
                .unwrap()
                .created
        );
        let compatible = compatible_loser
            .create_or_read_definition(definition.clone())
            .await
            .unwrap();
        assert!(!compatible.created);
        assert_eq!(compatible.definition, definition);
        let incompatible_outcome = incompatible_loser
            .create_or_read_definition(incompatible)
            .await
            .unwrap();
        assert!(!incompatible_outcome.created);
        assert_eq!(incompatible_outcome.definition, definition);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn local_definition_authority_preserves_concurrent_different_queues() {
        let root = temp_root("definition-different-queues");
        let _ = std::fs::remove_dir_all(&root);
        let first = ObjectLogEngineStore::open_local(&root, zero_linger())
            .await
            .unwrap();
        let second = ObjectLogEngineStore::open_local(&root, zero_linger())
            .await
            .unwrap();
        let first_definition = qdef();
        let mut second_definition = qdef();
        second_definition.queue_id = QueueId::new("q2").unwrap();

        let (first_outcome, second_outcome) = tokio::join!(
            first.create_or_read_definition(first_definition.clone()),
            second.create_or_read_definition(second_definition.clone())
        );
        assert!(first_outcome.unwrap().created);
        assert!(second_outcome.unwrap().created);
        drop(first);
        drop(second);

        let reopened = ObjectLogEngineStore::open_local(&root, zero_linger())
            .await
            .unwrap();
        let definitions = reopened.recover_definitions().await.unwrap();
        assert!(definitions.contains(&first_definition));
        assert!(definitions.contains(&second_definition));
        assert_eq!(definitions.len(), 2);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn local_definition_authority_has_exactly_one_concurrent_creator() {
        let root = temp_root("definition-same-queue");
        let _ = std::fs::remove_dir_all(&root);
        let first = ObjectLogEngineStore::open_local(&root, zero_linger())
            .await
            .unwrap();
        let second = ObjectLogEngineStore::open_local(&root, zero_linger())
            .await
            .unwrap();
        let definition = qdef();

        let (first_outcome, second_outcome) = tokio::join!(
            first.create_or_read_definition(definition.clone()),
            second.create_or_read_definition(definition.clone())
        );
        let outcomes = [first_outcome.unwrap(), second_outcome.unwrap()];
        assert_eq!(outcomes.iter().filter(|outcome| outcome.created).count(), 1);
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.definition == definition)
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn local_namespace_rejects_conflicting_live_flush_configuration() {
        let root = temp_root("flush-config-conflict");
        let _ = std::fs::remove_dir_all(&root);
        let _first = ObjectLogEngineStore::open_local(&root, zero_linger())
            .await
            .unwrap();
        let mut conflicting = zero_linger();
        conflicting.linger = std::time::Duration::from_millis(1);
        assert!(matches!(
            ObjectLogEngineStore::open_local(&root, conflicting).await,
            Err(EngineError::Invalid(
                "object-log namespace is already open with a different flush configuration"
            ))
        ));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn local_preopen_handles_share_epoch_authority_cache() {
        let root = temp_root("epoch-handoff");
        let _ = std::fs::remove_dir_all(&root);
        let first = ObjectLogEngineStore::open_local(&root, zero_linger())
            .await
            .unwrap();
        let second = ObjectLogEngineStore::open_local(&root, zero_linger())
            .await
            .unwrap();
        let definition = qdef();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());

        assert_eq!(first.acquire_epoch(shard.clone()).await.unwrap(), 1);
        assert_eq!(second.current_epoch(shard).await.unwrap(), 1);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn legacy_catalog_definition_migrates_without_candidate_overwrite() {
        let root = temp_root("legacy-catalog-migration");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("fwmeta")).unwrap();
        let durable = qdef();
        std::fs::write(
            root.join("fwmeta/catalog.json"),
            serde_json::to_vec(&super::CatalogDoc {
                definitions: vec![durable.clone()],
            })
            .unwrap(),
        )
        .unwrap();
        let store = ObjectLogEngineStore::open_local(&root, zero_linger())
            .await
            .unwrap();
        let mut candidate = durable.clone();
        candidate.request_id_retention_ms += 1;

        let outcome = store.create_or_read_definition(candidate).await.unwrap();
        assert!(!outcome.created);
        assert_eq!(outcome.definition, durable);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn log_engine_append_and_read_round_trip() {
        let log = ObjectLogEngineStore::open_memory(FlushConfig {
            linger: std::time::Duration::ZERO,
            ..FlushConfig::default()
        })
        .await
        .unwrap();
        let def = qdef();
        let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        log.ensure_shard(shard.clone()).await.unwrap();
        let epoch = log.acquire_epoch(shard.clone()).await.unwrap();
        let env = CommandEnvelope {
            command_id: CommandId::new("c1"),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: Vec::new(),
            command: QueueCommand::PauseQueue(Default::default()),
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(1, 0).unwrap(),
        };
        let positions = log
            .append(shard.clone(), vec![env.clone()], epoch)
            .await
            .unwrap();
        assert_eq!(positions.len(), 1);
        let page = log.read_from(shard.clone(), None, 16).await.unwrap();
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].0, positions[0]);
        assert_eq!(page.entries[0].1.command_id, env.command_id);
    }

    #[tokio::test]
    async fn local_snapshots_are_distinct_readable_and_durable_across_reopen() {
        let root = std::env::temp_dir().join(format!(
            "fireweed-olog-snapshot-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0)
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();

        let def = qdef();
        let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        let position = CommandPosition::new(shard.clone(), 3, 17);
        let flush = FlushConfig {
            linger: std::time::Duration::ZERO,
            ..FlushConfig::default()
        };

        let (first, second) = {
            let log = ObjectLogEngineStore::open_local(&root, flush)
                .await
                .unwrap();
            log.ensure_shard(shard.clone()).await.unwrap();
            assert!(log.latest_snapshot(shard.clone()).await.unwrap().is_none());
            let other_shard = QueueKey::new(
                TenantId::new("other").unwrap(),
                QueueId::new("queue").unwrap(),
            );
            assert!(matches!(
                log.write_snapshot(
                    other_shard.clone(),
                    position.clone(),
                    ProjectionSnapshot {
                        payload: b"wrong-queue".to_vec(),
                    },
                )
                .await,
                Err(EngineError::Invalid("snapshot position queue mismatch"))
            ));

            let first = log
                .write_snapshot(
                    shard.clone(),
                    position.clone(),
                    ProjectionSnapshot {
                        payload: b"first".to_vec(),
                    },
                )
                .await
                .unwrap();
            let second = log
                .write_snapshot(
                    shard.clone(),
                    position.clone(),
                    ProjectionSnapshot {
                        payload: b"second".to_vec(),
                    },
                )
                .await
                .unwrap();

            assert_ne!(first.ref_id, second.ref_id);
            assert_eq!(
                log.latest_snapshot(shard.clone())
                    .await
                    .unwrap()
                    .unwrap()
                    .ref_id,
                second.ref_id
            );
            assert_eq!(
                log.read_snapshot(first.clone()).await.unwrap().payload,
                b"first"
            );
            assert_eq!(
                log.read_snapshot(second.clone()).await.unwrap().payload,
                b"second"
            );
            assert!(matches!(
                log.read_snapshot(SnapshotRef {
                    queue: shard.clone(),
                    position: position.clone(),
                    ref_id: "missing".into(),
                })
                .await,
                Err(EngineError::NotFound)
            ));
            assert!(matches!(
                log.read_snapshot(SnapshotRef {
                    queue: shard.clone(),
                    position: CommandPosition::new(other_shard, 3, 17),
                    ref_id: second.ref_id.clone(),
                })
                .await,
                Err(EngineError::Invalid("snapshot position queue mismatch"))
            ));
            (first, second)
        };

        let reopened = ObjectLogEngineStore::open_local(&root, flush)
            .await
            .unwrap();
        assert_eq!(
            reopened
                .latest_snapshot(shard.clone())
                .await
                .unwrap()
                .unwrap()
                .ref_id,
            second.ref_id
        );
        assert_eq!(
            reopened.read_snapshot(first).await.unwrap().payload,
            b"first"
        );
        assert_eq!(
            reopened.read_snapshot(second).await.unwrap().payload,
            b"second"
        );

        drop(reopened);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// fireweed-481d3e43: reopen must not overwrite sealed data objects (object-log v0.3.1).
    /// Large multi-command frames (~20KiB) are the snorri transition-commit shape.
    #[tokio::test]
    async fn local_log_reopen_preserves_large_batches_across_process_boundary() {
        let root = std::env::temp_dir().join(format!(
            "fireweed-olog-reopen-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();

        let def = qdef();
        let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        let flush = FlushConfig {
            linger: std::time::Duration::ZERO,
            ..FlushConfig::default()
        };

        // Generation 1: large side-record-like pause batches (inflate command_id payload).
        {
            let log = ObjectLogEngineStore::open_local(&root, flush)
                .await
                .unwrap();
            log.create_or_read_definition(def.clone()).await.unwrap();
            log.ensure_shard(shard.clone()).await.unwrap();
            let epoch = log.acquire_epoch(shard.clone()).await.unwrap();
            let big = "X".repeat(20_000);
            let env = CommandEnvelope {
                command_id: CommandId::new(big),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: Vec::new(),
                command: QueueCommand::PauseQueue(Default::default()),
                checksum: CommandChecksum(0),
                created_at: UtcTimestamp::new(1, 0).unwrap(),
            };
            log.append(shard.clone(), vec![env], epoch)
                .await
                .expect("gen1 append");
            drop(log);
        }

        // Generation 2: reopen and append again; both generations must parse.
        {
            let log = ObjectLogEngineStore::open_local(&root, flush)
                .await
                .unwrap();
            log.ensure_shard(shard.clone()).await.unwrap();
            let epoch = log.current_epoch(shard.clone()).await.unwrap();
            let env = CommandEnvelope {
                command_id: CommandId::new("gen2"),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: Vec::new(),
                command: QueueCommand::PauseQueue(Default::default()),
                checksum: CommandChecksum(0),
                created_at: UtcTimestamp::new(2, 0).unwrap(),
            };
            log.append(shard.clone(), vec![env], epoch)
                .await
                .expect("gen2 append must not clobber gen1 objects");
            let mut from = None;
            let mut n = 0usize;
            loop {
                let page = log
                    .read_from(shard.clone(), from.clone(), 64)
                    .await
                    .expect("reopen recovery must parse sealed batches");
                n += page.entries.len();
                match page.next {
                    Some(next) if !page.entries.is_empty() => from = Some(next),
                    _ => break,
                }
            }
            assert!(n >= 2, "expected both generations, got {n}");
            drop(log);
        }

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn emission_cursor_advances_monotonically_and_survives_reopen() {
        use fireweed_engine::{ChangeRecord, ChangeRecordSink, EngineResult};

        let root = temp_root("emission-cursor");
        let _ = std::fs::remove_dir_all(&root);
        let log = ObjectLogEngineStore::open_local(&root, zero_linger())
            .await
            .unwrap();
        let mut definition = qdef();
        definition.emit_change_records = true;
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        log.create_or_read_definition(definition).await.unwrap();
        log.ensure_shard(shard.clone()).await.unwrap();
        let epoch = log.acquire_epoch(shard.clone()).await.unwrap();
        let env = |id: &str| CommandEnvelope {
            command_id: CommandId::new(id),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: Vec::new(),
            command: QueueCommand::PauseQueue(Default::default()),
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(1, 0).unwrap(),
        };
        let positions = log
            .append(shard.clone(), vec![env("a"), env("b")], epoch)
            .await
            .unwrap();
        assert_eq!(positions.len(), 2);
        assert!(log.supports_emission_cursor());
        assert_eq!(log.emission_cursor(&shard).await.unwrap(), None);

        #[derive(Default)]
        struct Sink(std::sync::Mutex<usize>);
        impl ChangeRecordSink for Sink {
            fn emit(&self, _shard: &QueueKey, records: &[ChangeRecord]) -> EngineResult<()> {
                *self.0.lock().unwrap() += records.len();
                Ok(())
            }
        }
        let sink = Sink::default();
        let n = log
            .emit_change_record_tail(&shard, &sink, 1, UtcTimestamp::new(2, 0).unwrap(), None)
            .await
            .unwrap();
        assert!(n >= 1);
        let cursor = log.emission_cursor(&shard).await.unwrap();
        assert_eq!(cursor.as_ref(), Some(&positions[0]));

        // Advance past first, then reject a regression to the first position.
        log.set_emission_cursor(&shard, positions[1].clone())
            .await
            .unwrap();
        assert_eq!(
            log.set_emission_cursor(&shard, positions[0].clone()).await,
            Err(EngineError::Invalid("emission cursor regression"))
        );

        drop(log);
        let reopened = ObjectLogEngineStore::open_local(&root, zero_linger())
            .await
            .unwrap();
        assert_eq!(
            reopened.emission_cursor(&shard).await.unwrap(),
            Some(positions[1].clone())
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// P8cs: live S3 emission cursor uses native CAS (create-only + If-Match) and survives reopen.
    #[tokio::test]
    async fn s3_emission_cursor_native_cas_monotonic_and_reopen() {
        let endpoint = std::env::var("FIREWEED_S3_TEST_ENDPOINT").expect(
            "FIREWEED_S3_TEST_ENDPOINT required for P8cs S3 emission-cursor CAS (fail-closed; no LOUD skip)",
        );
        let bucket = std::env::var("FIREWEED_S3_TEST_BUCKET").expect("FIREWEED_S3_TEST_BUCKET");
        let region =
            std::env::var("FIREWEED_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into());
        let access =
            std::env::var("FIREWEED_S3_TEST_ACCESS_KEY").expect("FIREWEED_S3_TEST_ACCESS_KEY");
        let secret =
            std::env::var("FIREWEED_S3_TEST_SECRET_KEY").expect("FIREWEED_S3_TEST_SECRET_KEY");
        let tag = format!(
            "p8cs-cursor-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        );
        let data_prefix = format!("fwlog-{tag}/");
        let meta_prefix = format!("fwmeta-{tag}/");
        let log = ObjectLogEngineStore::open_s3_with_prefixes(
            &endpoint,
            &region,
            &bucket,
            &access,
            &secret,
            data_prefix.clone(),
            meta_prefix.clone(),
            zero_linger(),
        )
        .await
        .expect("open S3 log with unique prefixes");
        let mut definition = qdef();
        definition.emit_change_records = true;
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        log.create_or_read_definition(definition).await.unwrap();
        log.ensure_shard(shard.clone()).await.unwrap();
        let epoch = log.acquire_epoch(shard.clone()).await.unwrap();
        let env = |id: &str| CommandEnvelope {
            command_id: CommandId::new(id),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: Vec::new(),
            command: QueueCommand::PauseQueue(Default::default()),
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(1, 0).unwrap(),
        };
        let positions = log
            .append(
                shard.clone(),
                vec![env("s3-a"), env("s3-b"), env("s3-c"), env("s3-d")],
                epoch,
            )
            .await
            .unwrap();
        assert_eq!(positions.len(), 4);
        assert!(log.supports_emission_cursor());
        assert_eq!(log.emission_cursor(&shard).await.unwrap(), None);

        // Concurrent CAS advances: both targets are valid monotonic steps.
        let log = Arc::new(log);
        let p1 = positions[1].clone();
        let p2 = positions[2].clone();
        let key1 = shard.clone();
        let key2 = shard.clone();
        let l1 = Arc::clone(&log);
        let l2 = Arc::clone(&log);
        let (r1, r2) = tokio::join!(
            async move { l1.set_emission_cursor(&key1, p1).await },
            async move { l2.set_emission_cursor(&key2, p2).await },
        );
        assert!(
            r1.is_ok() || r2.is_ok(),
            "at least one concurrent S3 CAS advance must succeed: {r1:?} / {r2:?}"
        );
        let final_cursor = log.emission_cursor(&shard).await.unwrap().expect("cursor");
        assert!(
            final_cursor == positions[1] || final_cursor == positions[2],
            "final cursor must be one of the concurrent targets: {final_cursor:?}"
        );

        log.set_emission_cursor(&shard, positions[3].clone())
            .await
            .unwrap();
        assert_eq!(
            log.set_emission_cursor(&shard, positions[0].clone()).await,
            Err(EngineError::Invalid("emission cursor regression"))
        );

        // Failover resume: drop handle, reopen same prefixes, cursor survives (CL-5 substrate).
        drop(log);
        let reopened = ObjectLogEngineStore::open_s3_with_prefixes(
            &endpoint,
            &region,
            &bucket,
            &access,
            &secret,
            data_prefix,
            meta_prefix,
            zero_linger(),
        )
        .await
        .expect("reopen S3 log");
        assert_eq!(
            reopened.emission_cursor(&shard).await.unwrap(),
            Some(positions[3].clone())
        );
    }

    fn production_source() -> &'static str {
        include_str!("log_engine_store.rs")
            .split_once("#[cfg(test)]\nmod tests")
            .expect("log_engine_store test module")
            .0
    }

    fn between<'a>(source: &'a str, start: &str, end: &str) -> &'a str {
        let (_, tail) = source
            .split_once(start)
            .unwrap_or_else(|| panic!("missing source-audit start marker: {start}"));
        let (body, _) = tail
            .split_once(end)
            .unwrap_or_else(|| panic!("missing source-audit end marker: {end}"));
        body
    }

    fn assert_no_committed_pool_or_selection_fence(region: &str, label: &str) {
        for needle in [
            "SelectionFence",
            "SelectionFenceAdmission",
            "SelectionFenceAcquire",
            "OutcomeReadAdmission",
            "ClaimDriverReadAdmission",
            "SharedDriverReadAdmission",
            "committed driver read pool",
            "committed outcome read pool",
            "borrow_committed",
            "CommittedPool",
            "driver_pool",
            "outcome_pool",
        ] {
            assert!(
                !region.contains(needle),
                "{label} must not {needle} while holding metadata permit or produce_lock"
            );
        }
    }

    fn pause_env(id: &str) -> CommandEnvelope {
        CommandEnvelope {
            command_id: CommandId::new(id),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: Vec::new(),
            command: QueueCommand::PauseQueue(Default::default()),
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(1, 0).unwrap(),
        }
    }

    fn complete_env(id: &str, item: u32) -> CommandEnvelope {
        let item_id = ItemId::mint(1, 0, item);
        CommandEnvelope {
            command_id: CommandId::new(id),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: vec![item_id],
            command: QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome::new(item_id, FinalizeKind::Complete)],
            }),
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(1, 0).unwrap(),
        }
    }

    /// S3a: every produce path is metadata-permit → produce-lock with permit-held high-water.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn objectlog_metadata_produce_lock_order_is_global() {
        let production = production_source();
        let produce_immediate = between(
            production,
            "async fn produce_immediate(",
            "async fn advance_high_water_held(",
        );
        let permit_idx = produce_immediate
            .find("self.metadata_permit(shard)")
            .expect("produce_immediate must acquire the metadata permit");
        let permit_lock_idx = produce_immediate[permit_idx..]
            .find(".lock()")
            .map(|offset| permit_idx + offset)
            .expect("produce_immediate must lock the metadata permit");
        let produce_lock_idx = produce_immediate
            .find("self.produce_lock.lock()")
            .expect("produce_immediate must acquire produce_lock");
        assert!(
            permit_lock_idx < produce_lock_idx,
            "produce_immediate must acquire metadata-permit before produce-lock; \
             inverted order deadlocks Complete vs fenced produce"
        );
        assert!(
            produce_immediate.contains("advance_high_water_held("),
            "produce_immediate must use the permit-held high-water helper"
        );
        assert!(
            !produce_immediate
                .replace("advance_high_water_held", "")
                .contains("advance_high_water"),
            "produce_immediate must not re-lock metadata via advance_high_water"
        );
        assert_eq!(
            production.matches("self.produce_lock.lock()").count(),
            1,
            "produce_lock must be acquired in exactly one produce path"
        );
        assert_eq!(
            production.matches(".produce_immediate(").count(),
            3,
            "append, append_exclusive, and seal_group must share produce_immediate"
        );
        assert_eq!(
            production
                .lines()
                .filter(|line| line.contains(".produce(") && !line.contains("produce_immediate"))
                .count(),
            1,
            "engine.produce must exist only inside produce_immediate"
        );

        let held = between(
            production,
            "async fn advance_high_water_held(",
            "fn exact_envelope_bytes(",
        );
        assert!(
            !held.contains("metadata_permit") && !held.contains("produce_lock"),
            "permit-held high-water helper must not re-acquire metadata permit or produce_lock"
        );

        for (label, start, end) in [
            ("acquire_epoch", "fn acquire_epoch(", "fn append("),
            ("set_high_water", "fn set_high_water(", "fn write_snapshot("),
            (
                "set_emission_cursor",
                "pub async fn set_emission_cursor(",
                "async fn set_emission_cursor_s3_cas(",
            ),
        ] {
            let body = between(production, start, end);
            assert!(
                body.contains("metadata_permit"),
                "{label} must take the metadata permit"
            );
            assert!(
                !body.contains("produce_lock"),
                "{label} must not acquire produce_lock (would invert vs produce)"
            );
            assert_no_committed_pool_or_selection_fence(body, label);
        }
        assert_no_committed_pool_or_selection_fence(produce_immediate, "produce_immediate");
        assert_no_committed_pool_or_selection_fence(held, "advance_high_water_held");
        assert_no_committed_pool_or_selection_fence(production, "log_engine_store production");

        let log = Arc::new(
            ObjectLogEngineStore::open_memory(FlushConfig {
                linger: std::time::Duration::ZERO,
                ..FlushConfig::default()
            })
            .await
            .unwrap(),
        );
        let produce_def = qdef();
        let mut epoch_def = qdef();
        epoch_def.queue_id = QueueId::new("q-epoch").unwrap();
        let produce_shard =
            QueueKey::new(produce_def.tenant_id.clone(), produce_def.queue_id.clone());
        let epoch_shard = QueueKey::new(epoch_def.tenant_id.clone(), epoch_def.queue_id.clone());
        log.create_or_read_definition(produce_def).await.unwrap();
        log.create_or_read_definition(epoch_def).await.unwrap();
        log.ensure_shard(produce_shard.clone()).await.unwrap();
        log.ensure_shard(epoch_shard.clone()).await.unwrap();
        let epoch = log.acquire_epoch(produce_shard.clone()).await.unwrap();
        let warmup = log
            .append(produce_shard.clone(), vec![pause_env("warmup")], epoch)
            .await
            .unwrap();
        assert_eq!(warmup.len(), 1);

        let raced = async {
            let mut handles = Vec::new();
            for i in 0..8u32 {
                {
                    let log = Arc::clone(&log);
                    let shard = produce_shard.clone();
                    handles.push(tokio::spawn(async move {
                        log.append(
                            shard,
                            vec![complete_env(&format!("complete-{i}"), i)],
                            epoch,
                        )
                        .await
                        .map(|_| ())
                    }));
                }
                {
                    let log = Arc::clone(&log);
                    let shard = produce_shard.clone();
                    handles.push(tokio::spawn(async move {
                        log.append_exclusive(shard, vec![pause_env(&format!("excl-{i}"))], epoch)
                            .await
                            .map(|_| ())
                    }));
                }
                {
                    let log = Arc::clone(&log);
                    let shard = produce_shard.clone();
                    handles.push(tokio::spawn(async move {
                        log.packed_append(shard, vec![pause_env(&format!("pack-{i}"))], epoch)
                            .await
                            .map(|_| ())
                            .map_err(PackedAppendError::into_engine)
                    }));
                }
                {
                    let log = Arc::clone(&log);
                    let shard = epoch_shard.clone();
                    handles.push(tokio::spawn(async move {
                        log.acquire_epoch(shard).await.map(|_| ())
                    }));
                }
            }
            let mut completed = 0usize;
            for handle in handles {
                match handle.await.expect("join") {
                    Ok(()) => completed += 1,
                    Err(EngineError::EpochFenced) => {}
                    Err(error) => panic!("concurrent produce/epoch failed: {error}"),
                }
            }
            completed
        };
        let completed = tokio::time::timeout(std::time::Duration::from_secs(15), raced)
            .await
            .expect(
                "metadata-permit → produce-lock inversion hung under Complete/acquire-epoch/produce",
            );
        assert!(
            completed > 0,
            "concurrent Complete/acquire-epoch/produce must complete at least one lock-order path"
        );

        log.set_emission_cursor(&produce_shard, warmup[0].clone())
            .await
            .unwrap();
        let waits = log.lock_wait_stats();
        eprintln!(
            "S3a lock waits: append={} ({:?}) epoch_acquire={} ({:?}) emission_cursor={} ({:?})",
            waits.append_waits,
            waits.append_wait,
            waits.epoch_acquire_waits,
            waits.epoch_acquire_wait,
            waits.emission_cursor_waits,
            waits.emission_cursor_wait
        );
        assert!(
            waits.append_waits >= 2,
            "append must record metadata-permit and produce-lock waits, got {}",
            waits.append_waits
        );
        assert!(
            waits.epoch_acquire_waits >= 1,
            "epoch-acquire must record permit wait, got {}",
            waits.epoch_acquire_waits
        );
        assert_eq!(
            waits.emission_cursor_waits, 1,
            "emission-cursor must record permit wait"
        );
        assert!(
            AsyncLogStore::high_water(log.as_ref(), produce_shard.clone())
                .await
                .unwrap()
                .is_some()
        );
    }

    fn qdef_named(queue: &str) -> QueueDefinition {
        let mut definition = qdef();
        definition.queue_id = QueueId::new(queue).unwrap();
        definition
    }

    async fn test_coordinator() -> AsyncProjectionApplyCoordinator<AsyncInMemoryProjection> {
        AsyncProjectionApplyCoordinator::new(
            Arc::new(AsyncInMemoryProjection::new(InMemoryProjection::new())),
            AsyncProjectionSpec::default(),
        )
        .unwrap()
    }

    #[test]
    fn object_log_append_phase_timeouts_default_to_thirty_seconds() {
        assert_eq!(OBJECT_LOG_PRE_POSITION_TIMEOUT, Duration::from_secs(30));
        assert_eq!(OBJECT_LOG_POST_POSITION_TIMEOUT, Duration::from_secs(30));
        let production = production_source();
        assert!(
            production.contains("OBJECT_LOG_PRE_POSITION_TIMEOUT.as_millis() as u64"),
            "constructors must store the 30s pre-position default"
        );
        assert!(
            production.contains("OBJECT_LOG_POST_POSITION_TIMEOUT.as_millis() as u64"),
            "constructors must store the 30s post-position default"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn driver_vector_force_seals_and_charges_own_group() {
        let log = Arc::new(
            ObjectLogEngineStore::open_memory(FlushConfig {
                linger: Duration::ZERO,
                ..FlushConfig::default()
            })
            .await
            .unwrap(),
        );
        let coordinator = test_coordinator().await;
        let own = qdef_named("own");
        let other = qdef_named("other");
        let own_shard = QueueKey::new(own.tenant_id.clone(), own.queue_id.clone());
        let other_shard = QueueKey::new(other.tenant_id.clone(), other.queue_id.clone());
        log.create_or_read_definition(own).await.unwrap();
        log.create_or_read_definition(other).await.unwrap();
        log.ensure_shard(own_shard.clone()).await.unwrap();
        log.ensure_shard(other_shard.clone()).await.unwrap();
        let own_epoch = log.acquire_epoch(own_shard.clone()).await.unwrap();
        let other_epoch = log.acquire_epoch(other_shard.clone()).await.unwrap();

        let follower_cmds = vec![pause_env("follower")];
        let driver_cmds = vec![pause_env("driver-a"), pause_env("driver-b")];
        let exact_own = super::exact_envelope_bytes(
            &follower_cmds
                .iter()
                .cloned()
                .chain(driver_cmds.iter().cloned())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let follower_res = coordinator
            .reserve(own_shard.clone(), &follower_cmds)
            .await
            .unwrap();
        let driver_res = coordinator
            .reserve(own_shard.clone(), &driver_cmds)
            .await
            .unwrap();
        let other_res = coordinator
            .reserve(other_shard.clone(), &[pause_env("other")])
            .await
            .unwrap();

        let other_log = Arc::clone(&log);
        let other_handle = tokio::spawn(async move {
            other_log
                .packed_append_owned(
                    other_shard,
                    vec![pause_env("other")],
                    other_epoch,
                    Some(other_res.id()),
                    false,
                )
                .await
        });
        let follower_log = Arc::clone(&log);
        let follower_shard = own_shard.clone();
        let follower_id = follower_res.id();
        let follower_handle = tokio::spawn(async move {
            follower_log
                .packed_append_owned(
                    follower_shard,
                    follower_cmds,
                    own_epoch,
                    Some(follower_id),
                    false,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(5)).await;

        let started = Instant::now();
        let driver_outcome = log
            .packed_append_force_seal(
                own_shard.clone(),
                driver_cmds,
                own_epoch,
                Some(driver_res.id()),
            )
            .await
            .unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(20),
            "force-seal must not wait pack linger, elapsed {:?}",
            started.elapsed()
        );
        let follower_outcome = follower_handle.await.unwrap().unwrap();
        assert!(
            !other_handle.is_finished(),
            "force-seal must not steal a different shard/lane group"
        );

        let stats = log.packer_stats();
        assert_eq!(stats.seals, 1);
        assert_eq!(stats.waiters, 2);
        assert_eq!(stats.commands, 3);
        assert_eq!(stats.bytes, exact_own);

        let (leader_outcome, leader_res, follower_wait) = if driver_outcome.apply_batch.is_some() {
            (driver_outcome, driver_res, follower_res)
        } else {
            (follower_outcome, follower_res, driver_res)
        };
        let batch = leader_outcome.apply_batch.expect("leader owns publication");
        assert_eq!(batch.commands.len(), 3);
        assert_eq!(batch.positions.len(), 3);
        assert_eq!(batch.transferred_reservation_ids, vec![follower_wait.id()]);
        coordinator
            .transfer_followers_and_recharge(
                &leader_res,
                &batch.transferred_reservation_ids,
                &batch.commands,
            )
            .await
            .unwrap();
        assert!(
            !coordinator.reservation_outstanding(&follower_wait).await,
            "followers transfer into the leader instead of remaining independent debt"
        );
        assert!(coordinator.reservation_outstanding(&leader_res).await);
        coordinator
            .enqueue_reserved(leader_res, batch.positions, batch.commands)
            .await
            .unwrap();
        let snap = coordinator.snapshot(&own_shard).await;
        assert_eq!(snap.apply_queue_depth, 1);
        assert_eq!(snap.apply_lag_commands, 3);
        assert_eq!(snap.apply_debt_bytes, exact_own);

        let other_outcome = tokio::time::timeout(Duration::from_secs(2), other_handle)
            .await
            .expect("other group linger")
            .unwrap()
            .unwrap();
        assert_eq!(other_outcome.positions.len(), 1);
        let stats = log.packer_stats();
        assert_eq!(stats.seals, 2);
        assert_eq!(stats.waiters, 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn post_produce_high_water_failure_never_cancels_or_reuses_position() {
        let log = Arc::new(
            ObjectLogEngineStore::open_memory(FlushConfig {
                linger: Duration::ZERO,
                ..FlushConfig::default()
            })
            .await
            .unwrap(),
        );
        let coordinator = test_coordinator().await;
        let def = qdef_named("hw-fail");
        let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        log.create_or_read_definition(def).await.unwrap();
        log.ensure_shard(shard.clone()).await.unwrap();
        let epoch = log.acquire_epoch(shard.clone()).await.unwrap();
        let commands = vec![pause_env("ambiguous")];
        let reservation = coordinator.reserve(shard.clone(), &commands).await.unwrap();
        log.fail_high_water_puts.store(1, Ordering::SeqCst);
        let error = log
            .packed_append_force_seal(shard.clone(), commands, epoch, Some(reservation.id()))
            .await
            .expect_err("high-water put_json failure is post-position");
        assert!(
            matches!(
                error,
                PackedAppendError::PostPositionAmbiguous { ref reason, .. }
                    if reason.contains("injected high-water put_json failure")
            ),
            "typed disposition must be post-position, got {error:?}"
        );
        assert!(
            coordinator.reservation_outstanding(&reservation).await,
            "post-position failure must not cancel the reservation"
        );
        coordinator
            .latch_poison(shard.clone(), "high-water put_json failure".into())
            .await;
        assert!(coordinator.snapshot(&shard).await.poison_reason.is_some());
        assert!(
            coordinator
                .reserve(shard.clone(), &[pause_env("next")])
                .await
                .is_err(),
            "poisoned shard must reject new reservations"
        );
        let page = log.read_from(shard.clone(), None, 16).await.unwrap();
        assert_eq!(
            page.entries.len(),
            1,
            "produce allocated a durable position that must remain occupied"
        );
        let used = page.entries[0].0.clone();
        let later = log
            .append_exclusive(shard.clone(), vec![pause_env("after")], epoch)
            .await
            .unwrap();
        assert_eq!(later.len(), 1);
        assert!(
            later[0].sequence > used.sequence,
            "later produce must not reuse the ambiguous position {} vs {}",
            later[0].sequence,
            used.sequence
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn all_waiters_receive_same_typed_append_disposition() {
        let log = Arc::new(
            ObjectLogEngineStore::open_memory(FlushConfig {
                linger: Duration::ZERO,
                ..FlushConfig::default()
            })
            .await
            .unwrap(),
        );
        log.pre_position_timeout_ms.store(100, Ordering::SeqCst);
        log.post_position_timeout_ms.store(100, Ordering::SeqCst);
        let def = qdef_named("typed");
        let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        log.create_or_read_definition(def).await.unwrap();
        log.ensure_shard(shard.clone()).await.unwrap();
        let epoch = log.acquire_epoch(shard.clone()).await.unwrap();

        async fn join_three(
            log: &Arc<ObjectLogEngineStore>,
            shard: &QueueKey,
            epoch: u64,
            tag: &str,
        ) -> Vec<PackedAppendError> {
            let mut handles = Vec::new();
            for i in 0..2 {
                let log = Arc::clone(log);
                let shard = shard.clone();
                let env = pause_env(&format!("{tag}-{i}"));
                handles.push(tokio::spawn(async move {
                    log.packed_append_owned(shard, vec![env], epoch, None, false)
                        .await
                }));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
            {
                let log = Arc::clone(log);
                let shard = shard.clone();
                let env = pause_env(&format!("{tag}-leader"));
                handles.push(tokio::spawn(async move {
                    log.packed_append_owned(shard, vec![env], epoch, None, true)
                        .await
                }));
            }
            let mut errors = Vec::new();
            for handle in handles {
                errors.push(handle.await.unwrap().expect_err("injected failure"));
            }
            errors
        }

        log.fail_high_water_puts.store(1, Ordering::SeqCst);
        let post = join_three(&log, &shard, epoch, "post").await;
        assert_eq!(post.len(), 3);
        for error in &post {
            assert!(
                matches!(error, PackedAppendError::PostPositionAmbiguous { .. }),
                "every co-sealed waiter must see post-position, got {error:?}"
            );
            assert_eq!(error, &post[0]);
        }

        let def_pre = qdef_named("typed-pre");
        let pre_shard = QueueKey::new(def_pre.tenant_id.clone(), def_pre.queue_id.clone());
        log.create_or_read_definition(def_pre).await.unwrap();
        log.ensure_shard(pre_shard.clone()).await.unwrap();
        let pre_epoch = log.acquire_epoch(pre_shard.clone()).await.unwrap();
        log.pre_position_stall_ms.store(250, Ordering::SeqCst);
        let pre = join_three(&log, &pre_shard, pre_epoch, "pre").await;
        log.pre_position_stall_ms.store(0, Ordering::SeqCst);
        assert_eq!(pre.len(), 3);
        for error in &pre {
            assert!(
                matches!(
                    error,
                    PackedAppendError::BeforePosition(EngineError::Backpressure {
                        resource: "object-log-append-pre-position"
                    })
                ),
                "every co-sealed waiter must see retryable pre-position, got {error:?}"
            );
            assert_eq!(error, &pre[0]);
        }
        assert!(
            !matches!(pre[0], PackedAppendError::PostPositionAmbiguous { .. }),
            "followers must not independently poison a live pre-position leader"
        );
    }
}
