//! Operation-scoped, one-record bounded replay. No projection or cursor writes.
use super::*;
use fireweed_core::queue_contract::{QueueError, QueueResult};
use fireweed_engine::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_log_record::{OwnedQueueLogRecord, QueueLogRecordView, view_queue_log_record},
    queue_namespace::{encode_address, view_address},
};
use fireweed_queue_spool::ScratchWorkers;
use object_log::{BoundedSequencer, FetchPlan};

fn unknown(error: impl std::fmt::Display) -> QueueError {
    QueueError::OutcomeUnknown(format!("API-007 replay: {error}"))
}
/// Operation-owned point-in-time authority. The manifest's resident index is
/// not covered by this wrapper charge and remains a backend qualification gate.
pub struct AdmittedApi007State {
    state: object_log::AuthoritativePartitionState,
    namespace: VerifiedApi007Namespace,
    _charge: OwnedQueueBufferCharge,
}
impl AdmittedApi007State {
    pub fn into_namespace(self) -> VerifiedApi007Namespace {
        self.namespace
    }
    pub fn namespace(&self) -> &VerifiedApi007Namespace {
        &self.namespace
    }
    pub fn state(&self) -> object_log::AuthoritativePartitionState {
        self.state
    }
}
enum StateAction {
    Read,
    Acquire,
}
struct StateWorker<S: Sequencer> {
    action: StateAction,
    store: Arc<ObjectLogEngineStore<S>>,
    namespace: VerifiedApi007Namespace,
    charge: Option<OwnedQueueBufferCharge>,
}
impl<S: Sequencer<Meta = ()> + 'static> StateWorker<S> {
    fn execute(mut self) -> QueueResult<AdmittedApi007State> {
        let shard = self.store.checked_api007_shard(&self.namespace)?;
        let partition = partition_key(shard);
        let state = match self.action {
            StateAction::Read => self
                .store
                .engine
                .sequencer()
                .authoritative_partition_state(&partition),
            StateAction::Acquire => self
                .store
                .engine
                .sequencer()
                .acquire_authoritative_epoch(&partition),
        }
        .map_err(unknown)?;
        Ok(AdmittedApi007State {
            state,
            namespace: self.namespace,
            _charge: self.charge.take().expect("one result charge"),
        })
    }
}
impl<S: Sequencer<Meta = ()> + 'static> ObjectLogEngineStore<S> {
    /// Native fresh authority only. No cached epoch document or legacy refresh
    /// fallback. This result does not grant writer exclusivity or a pinned view.
    pub async fn read_api007_state(
        self: &Arc<Self>,
        namespace: VerifiedApi007Namespace,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<AdmittedApi007State> {
        self.dispatch_api007_state(namespace, job, workers, StateAction::Read)
            .await
    }
    /// Acquire a native fence and matching replay bounds in one durable CAS.
    /// Errors may follow a durable advance. Once dispatched, caller cancellation
    /// does not cancel the operation. A cancelled pre-dispatch waiter may stop
    /// without mutation. No legacy epoch metadata write or ownership fallback.
    pub async fn acquire_api007_state(
        self: &Arc<Self>,
        namespace: VerifiedApi007Namespace,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<AdmittedApi007State> {
        self.dispatch_api007_state(namespace, job, workers, StateAction::Acquire)
            .await
    }
    async fn dispatch_api007_state(
        self: &Arc<Self>,
        namespace: VerifiedApi007Namespace,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
        action: StateAction,
    ) -> QueueResult<AdmittedApi007State> {
        if !self.raw_io_qualified {
            return Err(unknown(
                "native state requires qualified local or S3 authority",
            ));
        }
        self.validate_api007_namespace(&namespace)?;
        if !namespace.belongs_to(job) {
            return Err(unknown("state namespace belongs to another operation"));
        }
        let address = namespace.address()?;
        let root_bytes = match &self.definition_authority {
            DefinitionAuthority::Local { root } => root.as_os_str().as_encoded_bytes().len(),
            _ => 0,
        };
        let capacity = [
            address.tenant.len(),
            address.queue.len(),
            self.meta_prefix.len(),
            self.store_tag.len(),
            self.engine.data_prefix().len(),
            root_bytes,
        ]
        .into_iter()
        .try_fold(65536u64, |n, bytes| {
            n.checked_add(u64::try_from(bytes).map_err(unknown)?)
                .ok_or(QueueError::TooLarge)
        })?
        .checked_mul(16)
        .ok_or(QueueError::TooLarge)?;
        let worker = StateWorker {
            action,
            store: Arc::clone(self),
            namespace,
            charge: Some(job.owned_buffer(capacity)?),
        };
        workers.run(move || worker.execute()).await
    }
}

/// Empty means installed sequencer snapshot EOF, never budget truncation.
/// Recovery owners must refresh/acquire the durable fence before trusting coverage.
pub struct Api007ReadPlan<S: Sequencer> {
    plan: FetchPlan<S>,
    engine: Arc<LogEngine<S>>,
    address: Bytes,
    offset: u64,
    _charge: OwnedQueueBufferCharge,
}
/// Exact bytes retain admission until the consumer drops them.
pub struct AdmittedApi007Read {
    bytes: Bytes,
    offset: u64,
    _charge: OwnedQueueBufferCharge,
}
impl AdmittedApi007Read {
    /// Zero-copy transfer of the original read capacity into the engine owner.
    pub fn into_record(
        self,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<(u64, OwnedQueueLogRecord)> {
        let record = OwnedQueueLogRecord::from_admitted_bytes(self.bytes, self._charge, job)?;
        Ok((self.offset, record))
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn view(&self) -> QueueResult<QueueLogRecordView<'_>> {
        view_queue_log_record(&self.bytes)
    }
}
impl<S: BoundedSequencer<Meta = ()> + 'static> ObjectLogEngineStore<S> {
    /// Reserve declared metadata before native lookup; one logical record is one
    /// object-log batch. This API intentionally cannot read legacy command logs.
    pub fn plan_api007_read(
        &self,
        namespace: &VerifiedApi007Namespace,
        offset: u64,
        metadata_bytes: usize,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Api007ReadPlan<S>> {
        let shard = self.checked_api007_shard(namespace)?;
        let address = namespace.address()?;
        let address_size = 24usize
            .checked_add(address.tenant.len())
            .and_then(|n| n.checked_add(address.queue.len()))
            .ok_or(QueueError::TooLarge)?;
        let budget = u64::try_from(metadata_bytes).map_err(unknown)?;
        let address_charge = u64::try_from(address_size)
            .map_err(unknown)?
            .checked_mul(4)
            .ok_or(QueueError::TooLarge)?;
        let charge = job.owned_buffer(
            budget
                .checked_mul(4)
                .and_then(|n| n.checked_add(address_charge))
                .and_then(|n| n.checked_add(65536))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let encoded = encode_address(&fireweed_core::queue_contract::QueueAddress {
            tenant: address.tenant.into(),
            queue: address.queue.into(),
        })?;
        let offset_i64 = i64::try_from(offset).map_err(unknown)?;
        let plan = self
            .engine
            .plan_fetch(&partition_key(shard), offset_i64, 1, metadata_bytes, 1)
            .map_err(unknown)?;
        if let Some(entry) = plan.entries().first()
            && (entry.record_count != 1 || entry.base_offset != offset_i64)
        {
            return Err(unknown(
                "queue batch must contain exactly one record at requested offset",
            ));
        }
        Ok(Api007ReadPlan {
            plan,
            engine: Arc::clone(&self.engine),
            address: Bytes::from(encoded),
            offset,
            _charge: charge,
        })
    }
}
impl<S: BoundedSequencer<Meta = ()> + 'static> Api007ReadPlan<S> {
    pub fn is_eof(&self) -> bool {
        self.plan.entries().is_empty()
    }
    pub fn payload_bytes(&self) -> u64 {
        self.plan.payload_bytes()
    }
    /// Consume the plan. After dispatch, the bounded worker owns all resources
    /// through exact read and physical local worker completion, even if the
    /// awaiting caller disappears. The returned bytes keep their own charge.
    pub async fn read(
        self,
        workers: &ScratchWorkers,
        hard_bytes: u64,
    ) -> QueueResult<Option<AdmittedApi007Read>> {
        if self.is_eof() {
            return Ok(None);
        }
        let bytes = self.payload_bytes();
        if bytes > hard_bytes {
            return Err(QueueError::TooLarge);
        }
        let charge = self._charge.child_buffer(
            bytes
                .checked_mul(4)
                .and_then(|n| n.checked_add(65536))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let worker = ReadWorker {
            plan: self,
            payload_charge: Some(charge),
        };
        workers
            .run_on_runtime(
                crate::compose_log::objectlog_shared_runtime()
                    .handle()
                    .clone(),
                move || worker.execute(),
            )
            .await
    }
}
// Capture this container as a whole. Its consuming method retains the metadata
// guard through physical completion; Rust cannot split closure captures by field.
struct ReadWorker<S: Sequencer> {
    plan: Api007ReadPlan<S>,
    payload_charge: Option<OwnedQueueBufferCharge>,
}
impl<S: BoundedSequencer<Meta = ()> + 'static> ReadWorker<S> {
    async fn execute(mut self) -> QueueResult<Option<AdmittedApi007Read>> {
        self.execute_inner().await
    }
    async fn execute_inner(&mut self) -> QueueResult<Option<AdmittedApi007Read>> {
        let batch = self
            .plan
            .engine
            .read_planned_batch(&self.plan.plan, 0, self.plan.payload_bytes())
            .await
            .map_err(unknown)?;
        let result = AdmittedApi007Read {
            bytes: batch.payload,
            offset: self.plan.offset,
            _charge: self.payload_charge.take().expect("one output charge"),
        };
        let record = result.view()?;
        if record.address() != view_address(&self.plan.address)? {
            return Err(unknown(
                "record full address differs from namespace authority",
            ));
        }
        Ok(Some(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_core::queue_contract::{CollectionDefinition, QueueAddress, QueueSchema};
    use fireweed_engine::AsyncLogStore;
    use fireweed_engine::{
        queue_admission::QueueAdmission,
        queue_log_record::encode_genesis,
        queue_schema_codec::{AdmittedQueueDefinition, encode_definition},
    };
    async fn independent_local(root: &Path) -> Arc<ObjectLogEngineStore> {
        let mut store = ObjectLogEngineStore::open_with_blob_and_authority(
            Arc::new(LocalBlobStore::new(root)),
            "fwlog/",
            "fwmeta/",
            FlushConfig {
                linger: Duration::ZERO,
                ..Default::default()
            },
            DefinitionAuthority::Local {
                root: root.to_owned(),
            },
        )
        .await
        .unwrap();
        // Test-only qualification of an exact native local delegate.
        store.raw_io_qualified = true;
        Arc::new(store)
    }
    #[tokio::test]
    async fn native_state_refreshes_independent_handle_and_retains_binding() {
        let root = super::super::tests::temp_root("api007-state");
        std::fs::create_dir_all(&root).unwrap();
        let first = independent_local(&root).await;
        let second = independent_local(&root).await;
        assert!(!Arc::ptr_eq(&first.engine, &second.engine));
        let admission = QueueAdmission::new(1, 16 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let address = QueueAddress {
            tenant: "t".into(),
            queue: "q".into(),
        };
        let ns = first.bind_api007_namespace(&address, &job).await.unwrap();
        let shard = first.checked_api007_shard(&ns).unwrap().clone();
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let second_ns = second.bind_api007_namespace(&address, &job).await.unwrap();
        let initial = second
            .read_api007_state(second_ns, &job, &workers)
            .await
            .unwrap();
        assert_eq!(initial.state().next_offset, 0);
        assert_eq!(initial.state().epoch, 0);
        drop(initial);
        drop(ns);
        let epoch = first.acquire_epoch(shard).await.unwrap();
        let ns = second.bind_api007_namespace(&address, &job).await.unwrap();
        let state = second.read_api007_state(ns, &job, &workers).await.unwrap();
        assert_eq!(state.state().epoch, epoch);
        second.validate_api007_namespace(state.namespace()).unwrap();
        assert_eq!(state.namespace().address().unwrap().queue, "q");
        drop(job);
        assert_eq!(admission.usage().unwrap().0, 1);
        drop(state);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        workers.close_and_drain().await.unwrap();
        drop(first);
        drop(second);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn native_acquisition_appends_without_legacy_epoch_metadata() {
        let root = super::super::tests::temp_root("api007-native-bootstrap");
        std::fs::create_dir_all(&root).unwrap();
        let store = independent_local(&root).await;
        let admission = QueueAdmission::new(1, 16 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let address = QueueAddress {
            tenant: "t".into(),
            queue: "q".into(),
        };
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let ns = store.bind_api007_namespace(&address, &job).await.unwrap();
        let shard = store.checked_api007_shard(&ns).unwrap().clone();
        let state = store
            .acquire_api007_state(ns, &job, &workers)
            .await
            .unwrap();
        let bounds = state.state();
        assert_eq!((bounds.epoch, bounds.next_offset), (1, 0));
        assert!(!root.join(store.epoch_key(&shard)).exists());
        assert!(store.epochs.lock().unwrap().is_empty());
        let schema = QueueSchema {
            work: CollectionDefinition {
                fields: Default::default(),
                indexes: Default::default(),
                unique_indexes: Default::default(),
                additional_field_prefixes: Default::default(),
            },
            state: Default::default(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        };
        let raw = encode_definition(&schema).unwrap();
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let record =
            encode_genesis(bounds.epoch, &address, &definition, &[17; 32], 4096, &job).unwrap();
        let appended = store
            .append_api007_record(state.into_namespace(), record, 0, &job, &workers)
            .await
            .unwrap();
        assert!(appended.outcome().is_ok(), "{:?}", appended.outcome());
        drop(appended);
        let ns = store.bind_api007_namespace(&address, &job).await.unwrap();
        let newer = store
            .acquire_api007_state(ns, &job, &workers)
            .await
            .unwrap();
        assert_eq!((newer.state().epoch, newer.state().next_offset), (2, 1));
        let stale = encode_genesis(1, &address, &definition, &[18; 32], 4096, &job).unwrap();
        let rejected = store
            .append_api007_record(newer.into_namespace(), stale, 1, &job, &workers)
            .await
            .unwrap();
        assert!(matches!(
            rejected.outcome(),
            Err(PackedAppendError::BeforePosition(EngineError::EpochFenced))
        ));
        drop(rejected);
        assert!(store.epochs.lock().unwrap().is_empty());
        assert!(!root.join(store.epoch_key(&shard)).exists());
        assert!(
            store
                .engine
                .plan_fetch(&partition_key(&shard), 1, 1, 65536, 1)
                .unwrap()
                .entries()
                .is_empty()
        );
        drop(definition);
        drop(job);
        workers.close_and_drain().await.unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn queued_native_state_keeps_reservation_after_caller_drop() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            for acquire in [false, true] {
                let root = super::super::tests::temp_root("api007-state-cancel");
                std::fs::create_dir_all(&root).unwrap();
                let store = independent_local(&root).await;
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
                let address = QueueAddress {
                    tenant: "t".into(),
                    queue: "q".into(),
                };
                let namespace = store.bind_api007_namespace(&address, &job).await.unwrap();
                let shard = store.checked_api007_shard(&namespace).unwrap().clone();
                let baseline = admission.usage().unwrap();
                let workers = ScratchWorkers::new(1, 1).unwrap();
                let (started_tx, started_rx) = std::sync::mpsc::channel();
                let (release_tx, release_rx) = std::sync::mpsc::channel();
                let blocker = tokio::task::spawn_blocking(move || {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                });
                started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                let reader = store.clone();
                let owned_workers = workers.clone();
                let caller = tokio::spawn(async move {
                    if acquire {
                        reader
                            .acquire_api007_state(namespace, &job, &owned_workers)
                            .await
                    } else {
                        reader
                            .read_api007_state(namespace, &job, &owned_workers)
                            .await
                    }
                });
                tokio::time::timeout(Duration::from_secs(5), async {
                    while admission.usage().unwrap().1 <= baseline.1 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                let held = admission.usage().unwrap();
                caller.abort();
                match caller.await {
                    Err(error) => assert!(error.is_cancelled()),
                    Ok(_) => panic!("caller survived cancellation"),
                }
                assert_eq!(admission.usage().unwrap(), held);
                release_tx.send(()).unwrap();
                blocker.await.unwrap();
                workers.close_and_drain().await.unwrap();
                tokio::time::timeout(Duration::from_secs(5), terminal.notified())
                    .await
                    .unwrap();
                assert_eq!(admission.usage().unwrap(), (0, 0));
                let check = store.clone();
                let observed = tokio::task::spawn_blocking(move || {
                    check
                        .engine
                        .sequencer()
                        .authoritative_partition_state(&partition_key(&shard))
                        .unwrap()
                })
                .await
                .unwrap();
                assert_eq!(observed.epoch, u64::from(acquire));
                drop(store);
                std::fs::remove_dir_all(root).unwrap();
            }
        });
    }

    #[tokio::test]
    async fn physical_genesis_replay_checks_address_count_bounds_and_eof() {
        let root = super::super::tests::temp_root("api007-replay");
        let store = ObjectLogEngineStore::open_local(
            &root,
            FlushConfig {
                linger: Duration::ZERO,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let admission = QueueAdmission::new(2, 16 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let address = QueueAddress {
            tenant: "tenant\0β".into(),
            queue: "work/λ".into(),
        };
        let namespace = store.bind_api007_namespace(&address, &job).await.unwrap();
        let shard = store.checked_api007_shard(&namespace).unwrap();
        let epoch = store.acquire_epoch(shard.clone()).await.unwrap();
        let schema = QueueSchema {
            work: CollectionDefinition {
                fields: Default::default(),
                indexes: Default::default(),
                unique_indexes: Default::default(),
                additional_field_prefixes: Default::default(),
            },
            state: Default::default(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        };
        let raw = encode_definition(&schema).unwrap();
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let record = encode_genesis(epoch, &address, &definition, &[7; 32], 4096, &job).unwrap();
        let len = record.bytes().len() as u64;
        store
            .produce_payload_immediate(shard, epoch, || {
                Ok((Bytes::copy_from_slice(record.bytes()), 1))
            })
            .await
            .unwrap();
        let workers = ScratchWorkers::new(1, 2).unwrap();
        let plan = store.plan_api007_read(&namespace, 0, 65536, &job).unwrap();
        assert_eq!(plan.payload_bytes(), len);
        assert!(matches!(
            plan.read(&workers, len - 1).await,
            Err(QueueError::TooLarge)
        ));
        let value = store
            .plan_api007_read(&namespace, 0, 65536, &job)
            .unwrap()
            .read(&workers, len)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value.offset(), 0);
        assert_eq!(value.view().unwrap().address().tenant, address.tenant);
        let pointer = value.bytes.as_ptr();
        let (offset, owned) = value.into_record(&job).unwrap();
        assert_eq!(offset, 0);
        assert_eq!(owned.bytes().as_ptr(), pointer);
        assert!(owned.belongs_to(&job));
        assert_eq!(owned.view().unwrap().address().tenant, address.tenant);
        drop(owned);
        let eof = store.plan_api007_read(&namespace, 1, 65536, &job).unwrap();
        assert!(eof.is_eof());
        assert!(eof.read(&workers, 0).await.unwrap().is_none());
        assert!(store.plan_api007_read(&namespace, 0, 1, &job).is_err());
        // A structurally valid record for a different full address is corruption.
        let other = QueueAddress {
            tenant: address.tenant.clone(),
            queue: "other".into(),
        };
        let bad = encode_genesis(epoch, &other, &definition, &[7; 32], 4096, &job).unwrap();
        store
            .produce_payload_immediate(shard, epoch, || {
                Ok((Bytes::copy_from_slice(bad.bytes()), 1))
            })
            .await
            .unwrap();
        assert!(
            store
                .plan_api007_read(&namespace, 1, 65536, &job)
                .unwrap()
                .read(&workers, 4096)
                .await
                .is_err()
        );
        store
            .produce_payload_immediate(shard, epoch, || {
                Ok((Bytes::copy_from_slice(record.bytes()), 2))
            })
            .await
            .unwrap();
        assert!(store.plan_api007_read(&namespace, 2, 65536, &job).is_err());
        drop(record);
        drop(bad);
        drop(definition);
        drop(namespace);
        drop(job);
        workers.close_and_drain().await.unwrap();
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    use fireweed_core::queue_contract::{CollectionDefinition, QueueAddress, QueueSchema};
    use fireweed_engine::{
        AsyncLogStore,
        queue_admission::QueueAdmission,
        queue_log_record::encode_genesis,
        queue_schema_codec::{AdmittedQueueDefinition, encode_definition},
    };
    async fn seed(
        store: &ObjectLogEngineStore,
    ) -> (
        Arc<QueueAdmission>,
        Arc<QueueReservation>,
        VerifiedApi007Namespace,
        Arc<tokio::sync::Notify>,
    ) {
        let admission = QueueAdmission::new(1, 16 << 20).unwrap();
        let terminal = Arc::new(tokio::sync::Notify::new());
        let notify = Arc::clone(&terminal);
        let job = Arc::new(
            admission
                .reserve(0)
                .unwrap()
                .with_terminal_callback(move || notify.notify_one())
                .unwrap(),
        );
        let address = QueueAddress {
            tenant: "tenant".into(),
            queue: "queue".into(),
        };
        let namespace = store.bind_api007_namespace(&address, &job).await.unwrap();
        let shard = store.checked_api007_shard(&namespace).unwrap();
        let epoch = store.acquire_epoch(shard.clone()).await.unwrap();
        let schema = QueueSchema {
            work: CollectionDefinition {
                fields: Default::default(),
                indexes: Default::default(),
                unique_indexes: Default::default(),
                additional_field_prefixes: Default::default(),
            },
            state: Default::default(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        };
        let raw = encode_definition(&schema).unwrap();
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let record = encode_genesis(epoch, &address, &definition, &[7; 32], 4096, &job).unwrap();
        store
            .produce_payload_immediate(shard, epoch, || {
                Ok((Bytes::copy_from_slice(record.bytes()), 1))
            })
            .await
            .unwrap();
        (admission, job, namespace, terminal)
    }
    #[test]
    fn queued_read_keeps_all_charges_after_caller_drop() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let store = ObjectLogEngineStore::open_memory(FlushConfig {
                linger: Duration::ZERO,
                ..Default::default()
            })
            .await
            .unwrap();
            let (admission, job, namespace, terminal) = seed(&store).await;
            let plan = store.plan_api007_read(&namespace, 0, 65536, &job).unwrap();
            drop(namespace);
            drop(job);
            let metadata_charge = admission.usage().unwrap().1;
            let workers = ScratchWorkers::new(1, 1).unwrap();
            let (started_tx, started_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let owned_workers = workers.clone();
            let caller = tokio::spawn(async move { plan.read(&owned_workers, 4096).await });
            tokio::time::timeout(Duration::from_secs(5), async {
                while admission.usage().unwrap().1 <= metadata_charge {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let all_charges = admission.usage().unwrap();
            caller.abort();
            let _ = caller.await;
            assert_eq!(admission.usage().unwrap(), all_charges);
            release_tx.send(()).unwrap();
            blocker.await.unwrap();
            workers.close_and_drain().await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), terminal.notified())
                .await
                .unwrap();
            assert_eq!(admission.usage().unwrap(), (0, 0));
        });
    }
    struct PausedRange {
        inner: MemoryBlobStore,
        entered: tokio::sync::Notify,
        release: tokio::sync::watch::Sender<bool>,
    }
    #[async_trait::async_trait]
    impl BlobStore for PausedRange {
        async fn put(&self, key: &str, value: Bytes) -> Result<(), object_log::ObjectLogError> {
            self.inner.put(key, value).await
        }
        async fn get(&self, key: &str) -> Result<Option<Bytes>, object_log::ObjectLogError> {
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
            let mut release = self.release.subscribe();
            self.entered.notify_one();
            release.wait_for(|ready| *ready).await.unwrap();
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
    #[tokio::test]
    async fn running_read_keeps_all_charges_after_caller_drop() {
        let blob = Arc::new(PausedRange {
            inner: MemoryBlobStore::new(),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::watch::channel(false).0,
        });
        let store = ObjectLogEngineStore::open_with_blob_and_authority(
            blob.clone(),
            "fwlog/",
            "fwmeta/",
            FlushConfig {
                linger: Duration::ZERO,
                ..Default::default()
            },
            DefinitionAuthority::ProcessLocal,
        )
        .await
        .unwrap();
        let (admission, job, namespace, terminal) = seed(&store).await;
        let plan = store.plan_api007_read(&namespace, 0, 65536, &job).unwrap();
        drop(namespace);
        drop(job);
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let owned_workers = workers.clone();
        let caller = tokio::spawn(async move { plan.read(&owned_workers, 4096).await });
        tokio::time::timeout(Duration::from_secs(5), blob.entered.notified())
            .await
            .unwrap();
        let all_charges = admission.usage().unwrap();
        assert!(all_charges.1 > 4 * 65536);
        caller.abort();
        let _ = caller.await;
        assert_eq!(admission.usage().unwrap(), all_charges);
        blob.release.send_replace(true);
        workers.close_and_drain().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), terminal.notified())
            .await
            .unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
}
