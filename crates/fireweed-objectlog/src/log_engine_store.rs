//! Native-async [`AsyncLogStore`] over crates.io [`object_log::LogEngine`] (program A).
//!
//! Opaque payload bytes carry a Fireweed batch frame (`backend_epoch` + envelopes). Offsets from
//! the engine sequencer map 1:1 onto [`CommandPosition::sequence`]. Epoch and high-water metadata
//! live in dedicated blob keys so they survive reopen with a [`object_log::ManifestSequencer`].

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use fireweed_core::QueueDefinition;
use fireweed_engine::{
    AsyncLogStore, CommandEnvelope, CommandPage, CommandPosition, DurabilityClass, EngineError,
    EngineResult, QueueKey,
};
use object_log::{
    BlobStore, Durability, FlushConfig, LocalBlobStore, LogEngine, ManifestSequencer,
    MemoryBlobStore, PartitionKey, Sequencer,
};
use serde::{Deserialize, Serialize};

fn store_err(e: impl std::fmt::Display) -> EngineError {
    EngineError::Storage(e.to_string())
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

#[derive(Clone, Serialize, Deserialize, Default)]
struct CatalogDoc {
    definitions: Vec<QueueDefinition>,
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
        Self::open_with_blob(blob, "fwlog/", "fwmeta/", flush).await
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
        let doc = {
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
        AsyncLogStore, CommandChecksum, CommandEnvelope, CommandId, QueueCommand, QueueKey,
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
}
