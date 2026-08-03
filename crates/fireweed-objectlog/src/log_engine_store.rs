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
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use fireweed_core::QueueDefinition;
use fireweed_engine::{
    AsyncLogStore, CommandEnvelope, CommandPage, CommandPosition, DurabilityClass, EngineError,
    EngineResult, ProjectionSnapshot, QueueKey, SnapshotRef,
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
    engine: LogEngine<S>,
    blob: Arc<dyn BlobStore>,
    /// In-process epoch cache (also written to blob for reopen).
    epochs: Mutex<HashMap<String, u64>>,
    high_water: Mutex<HashMap<String, CommandPosition>>,
    catalog: Mutex<CatalogDoc>,
    meta_prefix: String,
}

impl ObjectLogEngineStore<ManifestSequencer> {
    /// Open a durable local-filesystem log under `root` (FIREWEED_OBJECT_LOG_ROOT).
    pub async fn open_local(root: impl AsRef<Path>, flush: FlushConfig) -> EngineResult<Self> {
        let root = root.as_ref();
        std::fs::create_dir_all(root).map_err(store_err)?;
        let blob: Arc<dyn BlobStore> = Arc::new(LocalBlobStore::new(root));
        Self::open_with_blob(blob, "fwlog/", "fwmeta/", flush).await
    }

    /// In-memory substrate for tests (sequencer + blob are process-local).
    pub async fn open_memory(flush: FlushConfig) -> EngineResult<Self> {
        let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
        Self::open_with_blob(blob, "fwlog/", "fwmeta/", flush).await
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
        let engine = LogEngine::new(Arc::clone(&blob), Arc::new(sequencer), flush, data_prefix);
        let store = Self {
            engine,
            blob,
            epochs: Mutex::new(HashMap::new()),
            high_water: Mutex::new(HashMap::new()),
            catalog: Mutex::new(CatalogDoc::default()),
            meta_prefix,
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

    async fn load_meta(&self) -> EngineResult<()> {
        // Catalog
        if let Some(bytes) = self
            .blob
            .get(&self.catalog_key())
            .await
            .map_err(store_err)?
        {
            let doc: CatalogDoc = serde_json::from_slice(&bytes).map_err(store_err)?;
            *self.catalog.lock().expect("catalog mutex") = doc;
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

    /// Persist a queue definition into the durable catalog (survives reopen).
    ///
    /// Product `create_queue` paths call this so projection recovery can discover
    /// shards without requiring a separate CreateQueue log envelope.
    pub async fn register_definition(&self, definition: QueueDefinition) -> EngineResult<()> {
        let doc =
            {
                let mut catalog = self.catalog.lock().expect("catalog");
                if let Some(existing) = catalog.definitions.iter_mut().find(|d| {
                    d.tenant_id == definition.tenant_id && d.queue_id == definition.queue_id
                }) {
                    *existing = definition;
                } else {
                    catalog.definitions.push(definition);
                }
                catalog.clone()
            };
        self.put_json(&self.catalog_key(), &doc).await
    }
}

/// Map env-style segment knobs onto object-log flush config (names unchanged at product edge).
pub fn flush_config_from_segment(target_bytes: usize, max_latency_ms: u64) -> FlushConfig {
    let mut cfg = FlushConfig::default();
    if target_bytes > 0 {
        cfg.max_bytes = target_bytes;
    }
    cfg.linger = Duration::from_millis(max_latency_ms);
    cfg
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
            // Persist catalog updates from CreateQueue / definition-carrying commands.
            let catalog_dirty =
                {
                    let mut catalog = self.catalog.lock().expect("catalog");
                    let mut dirty = false;
                    for env in &commands {
                        if let fireweed_engine::QueueCommand::CreateQueue(cmd) = &env.command {
                            let def = cmd.definition.clone();
                            if let Some(existing) = catalog.definitions.iter_mut().find(|d| {
                                d.tenant_id == def.tenant_id && d.queue_id == def.queue_id
                            }) {
                                *existing = def;
                            } else {
                                catalog.definitions.push(def);
                            }
                            dirty = true;
                        }
                    }
                    dirty.then(|| catalog.clone())
                };
            if let Some(doc) = catalog_dirty {
                self.put_json(&self.catalog_key(), &doc).await?;
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
        async move { Ok(self.catalog.lock().expect("catalog").definitions.clone()) }
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
            log.register_definition(def.clone()).await.unwrap();
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
}
