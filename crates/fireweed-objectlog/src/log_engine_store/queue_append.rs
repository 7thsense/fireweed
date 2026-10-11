//! Admitted native raw append. Queue owners still establish freshness, unique
//! genesis, poison/recovery, projection coverage and receipt semantics.
use super::*;
use fireweed_core::queue_contract::{QueueError, QueueResult};
use fireweed_engine::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_log_record::OwnedQueueLogRecord,
};
use fireweed_queue_spool::ScratchWorkers;
fn unknown(error: impl std::fmt::Display) -> QueueError {
    QueueError::OutcomeUnknown(format!("API-007 append: {error}"))
}
struct AppendLifetime {
    _charge: OwnedQueueBufferCharge,
}
/// Result storage retains its own operation admission through delivery/drop.
pub struct AdmittedApi007Append {
    outcome: Result<CommandPosition, PackedAppendError>,
    _charge: OwnedQueueBufferCharge,
}
impl AdmittedApi007Append {
    pub fn outcome(&self) -> &Result<CommandPosition, PackedAppendError> {
        &self.outcome
    }
}
struct AppendWorker<S: Sequencer> {
    store: Arc<ObjectLogEngineStore<S>>,
    namespace: VerifiedApi007Namespace,
    payload: Bytes,
    epoch: u64,
    expected_head: i64,
    lifetime: object_log::AppendOwner,
    job: Arc<QueueReservation>,
    output_charge: Option<OwnedQueueBufferCharge>,
}
impl<S: Sequencer<Meta = ()> + 'static> ObjectLogEngineStore<S> {
    /// Local/S3 only: persistent alias caches cannot carry per-operation owners.
    /// Bind authoritative_next_offset to the fully applied authoritative log,
    /// never insertion sequence or a sparse metadata hint.
    /// Dispatched work completes independently of caller cancellation. Owner
    /// terminal cleanup remains separate from both ACK and scheduler drain.
    pub async fn append_api007_record(
        self: &Arc<Self>,
        namespace: VerifiedApi007Namespace,
        record: OwnedQueueLogRecord,
        authoritative_next_offset: u64,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<AdmittedApi007Append> {
        if !self.raw_io_qualified
            || !matches!(
                self.definition_authority,
                DefinitionAuthority::Local { .. } | DefinitionAuthority::S3CreateOnly { .. }
            )
        {
            return Err(unknown(
                "raw append requires a local or S3 authority without persistent payload aliases",
            ));
        }
        let expected_head = i64::try_from(authoritative_next_offset).map_err(unknown)?;
        self.validate_api007_namespace(&namespace)?;
        if !record.belongs_to(job) || !namespace.belongs_to(job) {
            return Err(unknown("append buffers belong to another operation"));
        }
        let view = record.view()?;
        if view.address() != namespace.address()? {
            return Err(unknown(
                "record full address differs from namespace authority",
            ));
        }
        let epoch = view.epoch();
        let root_bytes = match &self.definition_authority {
            DefinitionAuthority::Local { root } => root.as_os_str().as_encoded_bytes().len(),
            _ => 0,
        };
        let headers = [
            self.meta_prefix.len(),
            self.store_tag.len(),
            self.engine.data_prefix().len(),
            root_bytes,
        ]
        .into_iter()
        .try_fold(1024u64, |total, bytes| {
            total
                .checked_add(u64::try_from(bytes).map_err(unknown)?)
                .ok_or(QueueError::TooLarge)
        })?
        .checked_mul(16)
        .ok_or(QueueError::TooLarge)?;
        let capacity = u64::try_from(record.bytes().len())
            .map_err(unknown)?
            .checked_mul(4)
            .and_then(|n| n.checked_add(headers))
            .and_then(|n| n.checked_add(65536))
            .ok_or(QueueError::TooLarge)?;
        let transport = job.owned_buffer(capacity)?;
        let output_charge =
            job.owned_buffer(headers.checked_add(4096).ok_or(QueueError::TooLarge)?)?;
        let worker = AppendWorker {
            store: Arc::clone(self),
            namespace,
            payload: record.into_admitted_bytes(),
            epoch,
            expected_head,
            lifetime: Arc::new(AppendLifetime { _charge: transport }),
            job: Arc::clone(job),
            output_charge: Some(output_charge),
        };
        workers.run(move || worker.execute()).await
    }
}
impl<S: Sequencer<Meta = ()> + 'static> AppendWorker<S> {
    fn execute(mut self) -> QueueResult<AdmittedApi007Append> {
        crate::block_on_objectlog_future(self.execute_inner())
    }
    async fn execute_inner(&mut self) -> QueueResult<AdmittedApi007Append> {
        let shard = self.store.checked_api007_shard(&self.namespace)?;
        let outcome = self
            .store
            .produce_payload_with_owner(
                shard,
                self.epoch,
                || Ok((self.payload.clone(), 1)),
                Some(Arc::clone(&self.lifetime)),
                Some(&self.job),
                Some(self.expected_head),
            )
            .await;
        let outcome = match outcome {
            Ok(mut positions) if positions.len() == 1 => Ok(positions.remove(0)),
            Ok(_) => {
                return Err(unknown(
                    "single-record append returned wrong position count",
                ));
            }
            Err(error) => Err(error),
        };
        Ok(AdmittedApi007Append {
            outcome,
            _charge: self.output_charge.take().expect("one output charge"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_core::queue_contract::{CollectionDefinition, QueueAddress, QueueSchema};
    use fireweed_engine::{
        AsyncLogStore,
        queue_admission::QueueAdmission,
        queue_log_record::encode_genesis,
        queue_schema_codec::{AdmittedQueueDefinition, encode_definition},
    };
    fn address() -> QueueAddress {
        QueueAddress {
            tenant: "t\0β".into(),
            queue: "q/λ".into(),
        }
    }
    fn schema() -> QueueSchema {
        QueueSchema {
            work: CollectionDefinition {
                fields: Default::default(),
                indexes: Default::default(),
                unique_indexes: Default::default(),
                additional_field_prefixes: Default::default(),
            },
            state: Default::default(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        }
    }
    #[tokio::test]
    async fn admitted_local_append_reopens_and_keeps_definite_vs_unknown_outcomes() {
        let root = super::super::tests::temp_root("native-append");
        let store = Arc::new(
            ObjectLogEngineStore::open_local(
                &root,
                FlushConfig {
                    linger: Duration::ZERO,
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        );
        let admission = QueueAdmission::new(2, 16 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let namespace = store.bind_api007_namespace(&address(), &job).await.unwrap();
        let shard = store.checked_api007_shard(&namespace).unwrap().clone();
        store.acquire_epoch(shard.clone()).await.unwrap();
        let epoch = store.acquire_epoch(shard).await.unwrap();
        let raw = encode_definition(&schema()).unwrap();
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let record = encode_genesis(epoch, &address(), &definition, &[11; 32], 4096, &job).unwrap();
        let workers = ScratchWorkers::new(1, 2).unwrap();
        let result = store
            .append_api007_record(namespace, record, 0, &job, &workers)
            .await
            .unwrap();
        assert_eq!(result.outcome().as_ref().unwrap().sequence, 0);
        drop(result);
        let namespace = store.bind_api007_namespace(&address(), &job).await.unwrap();
        let stale =
            encode_genesis(epoch - 1, &address(), &definition, &[11; 32], 4096, &job).unwrap();
        let result = store
            .append_api007_record(namespace, stale, 1, &job, &workers)
            .await
            .unwrap();
        assert!(matches!(
            result.outcome(),
            Err(PackedAppendError::BeforePosition(EngineError::EpochFenced))
        ));
        drop(result);
        let namespace = store.bind_api007_namespace(&address(), &job).await.unwrap();
        let before = fireweed_engine::queue_planner::RecordSnapshot::default();
        let delta = fireweed_engine::queue_planner::combine_deltas(&[], 0).unwrap();
        let frame = fireweed_engine::queue_publication::encode_publication_pinned(
            &address(),
            definition.pin(),
            &before,
            &delta,
            &[],
            4096,
            &job,
        )
        .unwrap();
        // Insertion sequence is still zero, but authoritative log next offset
        // is one after genesis. Using insertion sequence must not publish.
        let wrong = fireweed_engine::queue_log_record::encode_publication_record(
            epoch,
            frame.as_bytes(),
            4096,
            &job,
        )
        .unwrap();
        let wrong_namespace = store.bind_api007_namespace(&address(), &job).await.unwrap();
        let rejected = store
            .append_api007_record(wrong_namespace, wrong, 0, &job, &workers)
            .await
            .unwrap();
        assert!(matches!(
            rejected.outcome(),
            Err(PackedAppendError::HeadRejected {
                expected: 0,
                current: 1
            })
        ));
        drop(rejected);
        let record = fireweed_engine::queue_log_record::encode_publication_record(
            epoch,
            frame.as_bytes(),
            4096,
            &job,
        )
        .unwrap();
        drop(frame);
        store.fail_high_water_puts.store(1, Ordering::SeqCst);
        // Force this append to publish a sparse high-water hint.
        *store
            .high_water_appends
            .lock()
            .unwrap()
            .values_mut()
            .next()
            .unwrap() = 63;
        let result = store
            .append_api007_record(namespace, record, 1, &job, &workers)
            .await
            .unwrap();
        assert!(matches!(
            result.outcome(),
            Err(PackedAppendError::PostPositionAmbiguous { .. })
        ));
        drop(result);
        drop(definition);
        drop(job);
        workers.close_and_drain().await.unwrap();
        drop(store);
        let reopened = ObjectLogEngineStore::open_local(
            &root,
            FlushConfig {
                linger: Duration::ZERO,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let namespace = reopened
            .bind_api007_namespace(&address(), &job)
            .await
            .unwrap();
        for offset in 0..2 {
            let read = reopened
                .plan_api007_read(&namespace, offset, 65536, &job)
                .unwrap();
            let workers = ScratchWorkers::new(1, 1).unwrap();
            let value = read.read(&workers, 4096).await.unwrap().unwrap();
            assert_eq!(value.view().unwrap().epoch(), epoch);
            workers.close_and_drain().await.unwrap();
        }
        assert!(
            reopened
                .plan_api007_read(&namespace, 2, 65536, &job)
                .unwrap()
                .is_eof()
        );
        drop(namespace);
        drop(job);
        drop(reopened);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn persistent_memory_alias_authority_is_rejected_before_append() {
        let store = Arc::new(
            ObjectLogEngineStore::open_memory(FlushConfig::default())
                .await
                .unwrap(),
        );
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let namespace = store.bind_api007_namespace(&address(), &job).await.unwrap();
        let shard = store.checked_api007_shard(&namespace).unwrap().clone();
        let epoch = store.acquire_epoch(shard.clone()).await.unwrap();
        let raw = encode_definition(&schema()).unwrap();
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let record = encode_genesis(epoch, &address(), &definition, &[11; 32], 4096, &job).unwrap();
        drop(definition);
        let workers = ScratchWorkers::new(1, 1).unwrap();
        assert!(
            store
                .append_api007_record(namespace, record, 0, &job, &workers)
                .await
                .is_err()
        );
        assert!(
            store
                .engine
                .plan_fetch(&partition_key(&shard), 0, 1, 65536, 1)
                .unwrap()
                .entries()
                .is_empty()
        );
        drop(job);
        workers.close_and_drain().await.unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }

    #[tokio::test]
    async fn custom_io_rejects_and_native_append_ignores_legacy_epoch_hint() {
        let root = super::super::tests::temp_root("bounded-epoch");
        std::fs::create_dir_all(&root).unwrap();
        let mut store = Arc::new(
            ObjectLogEngineStore::open_with_blob_and_authority(
                Arc::new(LocalBlobStore::new(&root)),
                "fwlog/",
                "fwmeta/",
                FlushConfig {
                    linger: Duration::ZERO,
                    ..Default::default()
                },
                DefinitionAuthority::Local { root: root.clone() },
            )
            .await
            .unwrap(),
        );
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let terminal = Arc::new(tokio::sync::Notify::new());
        let notify = terminal.clone();
        let job = Arc::new(
            admission
                .reserve(0)
                .unwrap()
                .with_terminal_callback(move || notify.notify_one())
                .unwrap(),
        );
        let namespace = store.bind_api007_namespace(&address(), &job).await.unwrap();
        let shard = store.checked_api007_shard(&namespace).unwrap().clone();
        let epoch = store.acquire_epoch(shard.clone()).await.unwrap();
        let raw = encode_definition(&schema()).unwrap();
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let record = encode_genesis(epoch, &address(), &definition, &[11; 32], 4096, &job).unwrap();
        let workers = ScratchWorkers::new(1, 1).unwrap();
        assert!(!store.raw_io_qualified);
        assert!(
            store
                .append_api007_record(namespace, record, 0, &job, &workers)
                .await
                .is_err()
        );
        // Explicit test-only qualification of this known local delegate.
        Arc::get_mut(&mut store).unwrap().raw_io_qualified = true;
        let epoch_path = root.join(store.epoch_key(&shard));
        std::fs::OpenOptions::new()
            .write(true)
            .open(epoch_path)
            .unwrap()
            .set_len(2 << 20)
            .unwrap();
        store.epochs.lock().unwrap().clear();
        assert!(store.load_epoch_admitted(&shard, &job).await.is_err());
        let namespace = store.bind_api007_namespace(&address(), &job).await.unwrap();
        let record = encode_genesis(epoch, &address(), &definition, &[11; 32], 4096, &job).unwrap();
        let result = store
            .append_api007_record(namespace, record, 0, &job, &workers)
            .await
            .unwrap();
        assert!(result.outcome().is_ok(), "{:?}", result.outcome());
        assert!(store.epochs.lock().unwrap().is_empty());
        assert_eq!(
            store
                .engine
                .plan_fetch(&partition_key(&shard), 0, 1, 65536, 1)
                .unwrap()
                .entries()
                .len(),
            1
        );
        drop(result);
        drop(definition);
        drop(job);
        workers.close_and_drain().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), terminal.notified())
            .await
            .unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn same_epoch_physical_genesis_race_indexes_one_original_secret() {
        let root = super::super::tests::temp_root("genesis-head-race");
        let flush = FlushConfig {
            linger: Duration::from_millis(10),
            ..Default::default()
        };
        let first = Arc::new(
            ObjectLogEngineStore::open_local(&root, flush)
                .await
                .unwrap(),
        );
        let second = Arc::new(
            ObjectLogEngineStore::open_local(&root, flush)
                .await
                .unwrap(),
        );
        let admission = QueueAdmission::new(2, 16 << 20).unwrap();
        let job_a = Arc::new(admission.reserve(0).unwrap());
        let job_b = Arc::new(admission.reserve(0).unwrap());
        let ns_a = first
            .bind_api007_namespace(&address(), &job_a)
            .await
            .unwrap();
        let ns_b = second
            .bind_api007_namespace(&address(), &job_b)
            .await
            .unwrap();
        let shard = first.checked_api007_shard(&ns_a).unwrap().clone();
        let epoch = first.acquire_epoch(shard).await.unwrap();
        let raw = encode_definition(&schema()).unwrap();
        let definition_a = AdmittedQueueDefinition::from_raw(&raw, &job_a).unwrap();
        let definition_b = AdmittedQueueDefinition::from_raw(&raw, &job_b).unwrap();
        let record_a =
            encode_genesis(epoch, &address(), &definition_a, &[1; 32], 4096, &job_a).unwrap();
        let record_b =
            encode_genesis(epoch, &address(), &definition_b, &[2; 32], 4096, &job_b).unwrap();
        drop(definition_a);
        drop(definition_b);
        let workers = ScratchWorkers::new(2, 2).unwrap();
        let (a, b) = tokio::join!(
            first.append_api007_record(ns_a, record_a, 0, &job_a, &workers),
            second.append_api007_record(ns_b, record_b, 0, &job_b, &workers)
        );
        let a = a.unwrap();
        let b = b.unwrap();
        assert_eq!(
            usize::from(a.outcome().is_ok()) + usize::from(b.outcome().is_ok()),
            1
        );
        let winner = if a.outcome().is_ok() { 1 } else { 2 };
        let loser = if a.outcome().is_ok() {
            b.outcome()
        } else {
            a.outcome()
        };
        assert!(matches!(
            loser,
            Err(PackedAppendError::HeadRejected {
                expected: 0,
                current: 1
            })
        ));
        drop(a);
        drop(b);
        let ns = first
            .bind_api007_namespace(&address(), &job_a)
            .await
            .unwrap();
        let read = first
            .plan_api007_read(&ns, 0, 65536, &job_a)
            .unwrap()
            .read(&workers, 4096)
            .await
            .unwrap()
            .unwrap();
        let view = read.view().unwrap();
        let fireweed_engine::queue_log_record::QueueLogPayload::Genesis(genesis) = view.payload()
        else {
            panic!("expected genesis")
        };
        assert_eq!(genesis.cursor_secret(), &[winner; 32]);
        assert_eq!(genesis.definition_bytes(), raw);
        assert!(
            first
                .plan_api007_read(&ns, 1, 65536, &job_a)
                .unwrap()
                .is_eof()
        );
        drop(read);
        drop(ns);
        drop(job_a);
        drop(job_b);
        workers.close_and_drain().await.unwrap();
        drop(first);
        drop(second);
        std::fs::remove_dir_all(root).unwrap();
    }

    struct PausedIo {
        inner: LocalBlobStore,
        phase: std::sync::atomic::AtomicU8,
        entered: tokio::sync::Notify,
        release: tokio::sync::watch::Sender<bool>,
    }
    impl PausedIo {
        async fn pause(&self, phase: u8) {
            if self.phase.load(Ordering::SeqCst) == phase {
                let mut release = self.release.subscribe();
                self.entered.notify_one();
                release.wait_for(|ready| *ready).await.unwrap();
            }
        }
    }
    #[async_trait::async_trait]
    impl BlobStore for PausedIo {
        async fn put(&self, key: &str, value: Bytes) -> Result<(), object_log::ObjectLogError> {
            if key.starts_with("fwmeta/high_water/") {
                self.pause(3).await;
            }
            self.inner.put(key, value).await
        }
        async fn put_chunks(
            &self,
            key: &str,
            value: Vec<Bytes>,
        ) -> Result<(), object_log::ObjectLogError> {
            if key.starts_with("fwlog/") {
                self.pause(2).await;
            }
            self.inner.put_chunks(key, value).await
        }
        async fn get(&self, key: &str) -> Result<Option<Bytes>, object_log::ObjectLogError> {
            if key.starts_with("fwmeta/epochs/") {
                self.pause(1).await;
            }
            self.inner.get(key).await
        }
        async fn get_range(
            &self,
            key: &str,
            range: std::ops::Range<u64>,
        ) -> Result<Option<Bytes>, object_log::ObjectLogError> {
            self.inner.get_range(key, range).await
        }
        async fn get_range_exact(
            &self,
            key: &str,
            range: std::ops::Range<u64>,
        ) -> Result<Option<Bytes>, object_log::ObjectLogError> {
            self.inner.get_range_exact(key, range).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, object_log::ObjectLogError> {
            self.inner.list(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<(), object_log::ObjectLogError> {
            self.inner.delete(key).await
        }
        async fn compare_and_swap(
            &self,
            key: &str,
            expected: Option<Bytes>,
            value: Bytes,
        ) -> Result<object_log::CasOutcome, object_log::ObjectLogError> {
            self.inner.compare_and_swap(key, expected, value).await
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn owned_deadlines_drain_started_io_and_survive_caller_drop() {
        // Phase 4 drains a late head rejection after the caller deadline.
        for phase in [1, 2, 3, 4] {
            for cancel in [false, true] {
                let root = super::super::tests::temp_root("owned-deadline");
                std::fs::create_dir_all(&root).unwrap();
                let blob = Arc::new(PausedIo {
                    inner: LocalBlobStore::new(&root),
                    phase: std::sync::atomic::AtomicU8::new(0),
                    entered: tokio::sync::Notify::new(),
                    release: tokio::sync::watch::channel(false).0,
                });
                let mut store = ObjectLogEngineStore::open_with_blob_and_authority(
                    blob.clone(),
                    "fwlog/",
                    "fwmeta/",
                    FlushConfig {
                        linger: Duration::ZERO,
                        ..Default::default()
                    },
                    DefinitionAuthority::Local { root: root.clone() },
                )
                .await
                .unwrap();
                // Test-only qualification of a known local delegate with pauses.
                store.raw_io_qualified = true;
                let store = Arc::new(store);
                let admission = QueueAdmission::new(1, 16 << 20).unwrap();
                let terminal = Arc::new(tokio::sync::Notify::new());
                let notify = terminal.clone();
                let job = Arc::new(
                    admission
                        .reserve(0)
                        .unwrap()
                        .with_terminal_callback(move || notify.notify_one())
                        .unwrap(),
                );
                let namespace = store.bind_api007_namespace(&address(), &job).await.unwrap();
                let shard = store.checked_api007_shard(&namespace).unwrap().clone();
                let epoch = store.acquire_epoch(shard.clone()).await.unwrap();
                let raw = encode_definition(&schema()).unwrap();
                let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
                let record =
                    encode_genesis(epoch, &address(), &definition, &[11; 32], 4096, &job).unwrap();
                drop(definition);
                if phase == 4 {
                    let seed_namespace =
                        store.bind_api007_namespace(&address(), &job).await.unwrap();
                    let seed_definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
                    let seed =
                        encode_genesis(epoch, &address(), &seed_definition, &[12; 32], 4096, &job)
                            .unwrap();
                    let seed_workers = ScratchWorkers::new(1, 1).unwrap();
                    let seeded = store
                        .append_api007_record(seed_namespace, seed, 0, &job, &seed_workers)
                        .await
                        .unwrap();
                    assert!(seeded.outcome().is_ok());
                    drop(seeded);
                    drop(seed_definition);
                    seed_workers.close_and_drain().await.unwrap();
                }
                if phase == 1 {
                    store.epochs.lock().unwrap().clear();
                }
                store
                    .pre_position_timeout_ms
                    .store(if phase == 1 { 1 } else { 5000 }, Ordering::SeqCst);
                store.post_position_timeout_ms.store(
                    if phase == 2 || phase == 4 { 1 } else { 1000 },
                    Ordering::SeqCst,
                );
                if phase == 1 {
                    store.pre_position_stall_ms.store(100, Ordering::SeqCst);
                }
                blob.phase
                    .store(if phase == 4 { 2 } else { phase }, Ordering::SeqCst);
                let workers = ScratchWorkers::new(1, 1).unwrap();
                let owned_workers = workers.clone();
                let writer = store.clone();
                let caller = tokio::spawn(async move {
                    writer
                        .append_api007_record(namespace, record, 0, &job, &owned_workers)
                        .await
                });
                if phase != 1 {
                    tokio::time::timeout(Duration::from_secs(5), blob.entered.notified())
                        .await
                        .unwrap();
                }
                tokio::time::sleep(Duration::from_millis(if phase == 3 { 1100 } else { 25 })).await;
                assert!(!caller.is_finished());
                let charged = admission.usage().unwrap();
                assert_eq!(charged.0, 1);
                assert!(charged.1 > 65536);
                if cancel {
                    caller.abort();
                }
                assert_eq!(admission.usage().unwrap(), charged);
                blob.release.send_replace(true);
                if cancel {
                    match caller.await {
                        Err(error) => assert!(error.is_cancelled()),
                        Ok(_) => panic!("caller was not cancelled"),
                    }
                } else {
                    let result = caller.await.unwrap().unwrap();
                    if phase == 1 {
                        assert!(matches!(
                            result.outcome(),
                            Err(PackedAppendError::BeforePosition(
                                EngineError::Backpressure { .. }
                            ))
                        ));
                    } else {
                        assert!(
                            matches!(result.outcome(), Err(PackedAppendError::PostPositionAmbiguous { reason, .. }) if reason.contains(if phase == 2 || phase == 4 { "produce timed out" } else { "high-water metadata timed out" }))
                        );
                    }
                    drop(result);
                }
                workers.close_and_drain().await.unwrap();
                tokio::time::timeout(Duration::from_secs(5), terminal.notified())
                    .await
                    .unwrap();
                assert_eq!(admission.usage().unwrap(), (0, 0));
                let plan = store
                    .engine
                    .plan_fetch(&partition_key(&shard), 0, 1, 65536, 1)
                    .unwrap();
                assert_eq!(plan.entries().len(), usize::from(phase != 1));
                if phase == 4 {
                    let eof = store
                        .engine
                        .plan_fetch(&partition_key(&shard), 1, 1, 65536, 1)
                        .unwrap();
                    assert!(eof.entries().is_empty());
                }
                *super::super::namespace::METADATA_READ_HOOK.lock().unwrap() = None;
                drop(store);
                std::fs::remove_dir_all(root).unwrap();
            }
        }
    }
}
