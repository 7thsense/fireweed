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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use bytes::Bytes;
use fireweed_core::QueueDefinition;
use fireweed_engine::{
    AsyncLogStore, CommandEnvelope, CommandPage, CommandPosition, CreateQueueOutcome,
    DurabilityClass, EngineError, EngineResult, ProjectionSnapshot, QueueKey, SnapshotRef,
};
use object_log::{
    BlobStore, Durability, FlushConfig, LocalBlobStore, LogEngine, ManifestSequencer,
    MemoryBlobStore, PartitionKey, Sequencer,
};
use serde::{Deserialize, Serialize};

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

#[derive(Serialize, Deserialize)]
struct BatchFrame {
    backend_epoch: u64,
    commands: Vec<CommandEnvelope>,
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
    /// The crates.io BlobStore port has overwrite-only `put`; an S3/custom adapter cannot claim
    /// multi-writer authority until it exposes an enforced conditional-create operation.
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
    metadata_permits: Arc<MetadataPermits>,
    catalog: Mutex<CatalogDoc>,
    meta_prefix: String,
    definition_authority: DefinitionAuthority,
    definition_permit: tokio::sync::Mutex<()>,
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
            metadata_permits,
            catalog: Mutex::new(CatalogDoc::default()),
            meta_prefix: "fwmeta/".to_string(),
            definition_authority: DefinitionAuthority::Local { root },
            definition_permit: tokio::sync::Mutex::new(()),
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
    pub async fn open_s3(
        endpoint: &str,
        region: &str,
        bucket: &str,
        access_key_id: &str,
        secret_access_key: &str,
        flush: FlushConfig,
    ) -> EngineResult<Self> {
        let blob: Arc<dyn BlobStore> = Arc::new(object_log::S3BlobStore::new(
            endpoint,
            region,
            bucket,
            access_key_id,
            secret_access_key,
        ));
        Self::open_with_blob(blob, "fwlog/", "fwmeta/", flush)
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
            metadata_permits: Arc::new(Mutex::new(HashMap::new())),
            catalog: Mutex::new(CatalogDoc::default()),
            meta_prefix,
            definition_authority,
            definition_permit: tokio::sync::Mutex::new(()),
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
    pub async fn set_emission_cursor(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
    ) -> EngineResult<()> {
        if position.queue != *shard {
            return Err(EngineError::Invalid("emission cursor queue mismatch"));
        }
        let permit = self.metadata_permit(shard);
        let _guard = permit.lock().await;
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
            DefinitionAuthority::ConditionalCreateUnavailable => {
                return Err(EngineError::Storage(
                    "NativeConditionalWrite queue-definition authority is unavailable: the \
                     configured BlobStore exposes overwrite-only put and cannot prove create-only \
                     publication; use the local filesystem adapter or an S3 adapter with enforced \
                     If-None-Match: * support"
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
pub fn flush_config_from_segment(target_bytes: usize, max_latency_ms: u64) -> FlushConfig {
    let mut cfg = FlushConfig::default();
    if target_bytes > 0 {
        cfg.max_bytes = target_bytes;
    }
    cfg.max_batches = fireweed_engine::PRODUCTION_OBJECT_LOG_MAX_BATCHES;
    cfg.linger = Duration::from_millis(max_latency_ms);
    cfg
}

impl<S: Sequencer<Meta = ()> + 'static> ObjectLogEngineStore<S> {
    /// Emit durable change-record tail from the emission cursor (TD-008 / P8c filesystem cursor).
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
            let _guard = permit.lock().await;
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
            if commands.is_empty() {
                return Ok(Vec::new());
            }
            let epoch = self.load_epoch(&shard).await?;
            if epoch != expected_epoch {
                return Err(EngineError::EpochFenced);
            }
            let frame = BatchFrame {
                backend_epoch: expected_epoch,
                commands: commands.clone(),
            };
            let payload = Bytes::from(serde_json::to_vec(&frame).map_err(store_err)?);
            let record_count = i32::try_from(commands.len())
                .map_err(|_| EngineError::Invalid("batch too large for object-log record_count"))?;
            let outcome = self
                .engine
                .produce(
                    partition_key(&shard),
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
                let permit = self.metadata_permit(&shard);
                let _guard = permit.lock().await;
                let should_advance = self
                    .high_water
                    .lock()
                    .expect("high_water")
                    .get(&partition_key(&shard).0)
                    .is_none_or(|current| current.precedes(last));
                if should_advance {
                    self.high_water
                        .lock()
                        .expect("high_water")
                        .insert(partition_key(&shard).0, last.clone());
                    self.put_json(
                        &self.high_water_key(&shard),
                        &HighWaterDoc {
                            backend_epoch: last.backend_epoch,
                            sequence: last.sequence,
                        },
                    )
                    .await?;
                }
            }
            Ok(positions)
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
                let frame: BatchFrame =
                    serde_json::from_slice(&batch.payload).map_err(store_err)?;
                for (i, env) in frame.commands.into_iter().enumerate() {
                    let seq = batch.base_offset as u64 + i as u64;
                    if seq < from_seq {
                        continue;
                    }
                    entries.push((
                        CommandPosition::new(shard.clone(), frame.backend_epoch, seq),
                        env,
                    ));
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
    use fireweed_core::{
        EligibilityPolicy, OrderingMode, PriorityModel, QueueDefinition, QueueId, RecurrencePolicy,
        RetryPolicy, TenantId, UtcTimestamp,
    };
    use fireweed_engine::{
        AsyncLogStore, CommandChecksum, CommandEnvelope, CommandId, CommandPosition, EngineError,
        ProjectionSnapshot, QueueCommand, QueueKey, SnapshotRef,
    };

    use super::ObjectLogEngineStore;
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
}
