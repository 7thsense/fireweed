//! Owned first-genesis provisioning. This establishes genesis authority only,
//! not full-history validation, projection coverage, or a ready QueueStore.
use super::*;
use fireweed_core::queue_contract::{QueueAddress, QueueError, QueueResult};
use fireweed_engine::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_log_record::{
        QueueLogFormat, QueueLogPayload, checked_genesis_record_len, encode_checked_genesis,
    },
    queue_schema_codec::AdmittedQueueDefinition,
};
use fireweed_queue_spool::ScratchWorkers;
use object_log::BoundedSequencer;
fn unknown(error: impl std::fmt::Display) -> QueueError {
    QueueError::OutcomeUnknown(format!("API-007 provision: {error}"))
}
/// Original durable genesis authority with operation-local replay bounds.
/// This is not a fully recovered or projected queue and must not be cached.
pub struct ProvisionedApi007Genesis {
    namespace: VerifiedApi007Namespace,
    definition: AdmittedQueueDefinition,
    cursor_secret: [u8; 32],
    target: object_log::AuthoritativePartitionState,
    format: QueueLogFormat,
    _charge: OwnedQueueBufferCharge,
}
impl ProvisionedApi007Genesis {
    pub fn namespace(&self) -> &VerifiedApi007Namespace {
        &self.namespace
    }
    pub fn definition(&self) -> &AdmittedQueueDefinition {
        &self.definition
    }
    pub fn cursor_secret(&self) -> &[u8; 32] {
        &self.cursor_secret
    }
    pub fn format(&self) -> QueueLogFormat {
        self.format
    }
    pub fn target(&self) -> object_log::AuthoritativePartitionState {
        self.target
    }
}
/// Owned internal provisioning inputs; not a public queue transaction handle.
pub struct Api007GenesisRequest {
    pub namespace: VerifiedApi007Namespace,
    pub definition: AdmittedQueueDefinition,
    pub candidate_secret: [u8; 32],
    pub max_record_bytes: u64,
    pub metadata_bytes: usize,
}
struct ProvisionTask<S: Sequencer> {
    store: Arc<ObjectLogEngineStore<S>>,
    namespace: Option<VerifiedApi007Namespace>,
    address: QueueAddress,
    incoming: AdmittedQueueDefinition,
    candidate_secret: [u8; 32],
    max_record_bytes: u64,
    metadata_bytes: usize,
    workers: ScratchWorkers,
    _task_slot: tokio::sync::OwnedSemaphorePermit,
    job: Arc<QueueReservation>,
    charge: Option<OwnedQueueBufferCharge>,
}
impl<S: BoundedSequencer<Meta = ()> + 'static> ObjectLogEngineStore<S> {
    /// Once the whole task is spawned, caller cancellation cannot stop the
    /// bounded protocol between physical stages. Job admission bounds task count.
    pub async fn provision_api007_genesis(
        self: &Arc<Self>,
        request: Api007GenesisRequest,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<ProvisionedApi007Genesis> {
        let Api007GenesisRequest {
            namespace,
            definition: incoming,
            candidate_secret,
            max_record_bytes,
            metadata_bytes,
        } = request;
        if !self.raw_io_qualified {
            return Err(unknown("native provisioning requires qualified authority"));
        }
        self.validate_api007_namespace(&namespace)?;
        if !namespace.belongs_to(job) || !incoming.belongs_to(job) {
            return Err(unknown("provision buffers belong to another operation"));
        }
        if max_record_bytes == 0 || metadata_bytes == 0 {
            return Err(QueueError::TooLarge);
        }
        let task_slot = Arc::clone(&self.native_provision_tasks)
            .try_acquire_owned()
            .map_err(|_| QueueError::Busy)?;
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
        let charge = job.owned_buffer(capacity)?;
        let task = ProvisionTask {
            store: Arc::clone(self),
            address: QueueAddress {
                tenant: address.tenant.into(),
                queue: address.queue.into(),
            },
            namespace: Some(namespace),
            incoming,
            candidate_secret,
            max_record_bytes,
            metadata_bytes,
            workers: workers.clone(),
            job: Arc::clone(job),
            charge: Some(charge),
            _task_slot: task_slot,
        };
        if checked_genesis_record_len(&task.address, &task.incoming)? > max_record_bytes {
            return Err(QueueError::TooLarge);
        }
        tokio::spawn(async move { task.execute().await })
            .await
            .map_err(unknown)?
    }
}
impl<S: BoundedSequencer<Meta = ()> + 'static> ProvisionTask<S> {
    async fn execute(mut self) -> QueueResult<ProvisionedApi007Genesis> {
        for _ in 0..8 {
            let namespace = self.namespace.take().expect("one namespace per attempt");
            let acquired = self
                .store
                .acquire_api007_state(namespace, &self.job, &self.workers)
                .await?;
            let mut target = acquired.state();
            if target.log_start != 0 {
                return Err(unknown("genesis history was retained away"));
            }
            let mut namespace = acquired.into_namespace();
            if target.next_offset == 0 {
                let record = encode_checked_genesis(
                    target.epoch,
                    &self.address,
                    &self.incoming,
                    &self.candidate_secret,
                    self.max_record_bytes,
                    &self.job,
                )?;
                let appended = self
                    .store
                    .append_api007_record(namespace, record, 0, &self.job, &self.workers)
                    .await?;
                match appended.outcome() {
                    Ok(position)
                        if position.sequence == 0 && position.backend_epoch == target.epoch =>
                    {
                        target.next_offset = 1;
                    }
                    Ok(_) => return Err(unknown("genesis append returned unexpected position")),
                    Err(
                        PackedAppendError::HeadRejected { .. }
                        | PackedAppendError::PostPositionAmbiguous { .. }
                        | PackedAppendError::BeforePosition(EngineError::EpochFenced),
                    ) => {
                        drop(appended);
                        self.namespace = Some(
                            self.store
                                .bind_api007_namespace(&self.address, &self.job)
                                .await?,
                        );
                        continue;
                    }
                    Err(error) => return Err(unknown(format!("{error:?}"))),
                }
                drop(appended);
                namespace = self
                    .store
                    .bind_api007_namespace(&self.address, &self.job)
                    .await?;
            }
            let read = self
                .store
                .plan_api007_read(&namespace, 0, self.metadata_bytes, &self.job)?
                .read(&self.workers, self.max_record_bytes)
                .await?
                .ok_or_else(|| unknown("durable head has no genesis record"))?;
            let record = read.view()?;
            if record.epoch() > target.epoch {
                return Err(unknown("genesis epoch exceeds acquired replay target"));
            }
            let QueueLogPayload::Genesis(genesis) = record.payload() else {
                return Err(unknown("offset zero is not genesis"));
            };
            let definition = genesis.validate_definition(&self.job)?;
            if definition.schema() != self.incoming.schema() {
                return Err(QueueError::DefinitionConflict);
            }
            let cursor_secret = *genesis.cursor_secret();
            return Ok(ProvisionedApi007Genesis {
                namespace,
                definition,
                cursor_secret,
                target,
                format: record.format(),
                _charge: self.charge.take().expect("one authority charge"),
            });
        }
        Err(unknown("genesis provisioning races exhausted"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_core::queue_contract::{CollectionDefinition, QueueSchema};
    use fireweed_engine::{queue_admission::QueueAdmission, queue_schema_codec::encode_definition};
    async fn open(root: &Path) -> Arc<ObjectLogEngineStore> {
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
        store.raw_io_qualified = true; // Exact native local delegate; test-only.
        Arc::new(store)
    }
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
    async fn request(
        store: &ObjectLogEngineStore,
        raw: &[u8],
        secret: u8,
        job: &Arc<QueueReservation>,
    ) -> Api007GenesisRequest {
        Api007GenesisRequest {
            namespace: store.bind_api007_namespace(&address(), job).await.unwrap(),
            definition: AdmittedQueueDefinition::from_raw(raw, job).unwrap(),
            candidate_secret: [secret; 32],
            max_record_bytes: 65536,
            metadata_bytes: 65536,
        }
    }
    #[tokio::test]
    async fn independent_provisioners_converge_and_reopen_original_legacy_authority() {
        let root = super::super::tests::temp_root("api007-provision-race");
        std::fs::create_dir_all(&root).unwrap();
        let first = open(&root).await;
        let second = open(&root).await;
        assert!(!Arc::ptr_eq(&first.engine, &second.engine));
        let admission = QueueAdmission::new(2, 32 << 20).unwrap();
        let a = Arc::new(admission.reserve(0).unwrap());
        let b = Arc::new(admission.reserve(0).unwrap());
        let legacy = [0, 0, 0, 0, 1, 3];
        let ra = request(&first, &legacy, 1, &a).await;
        let rb = request(&second, &legacy, 2, &b).await;
        let workers = ScratchWorkers::new(2, 4).unwrap();
        let (ga, gb) = tokio::join!(
            first.provision_api007_genesis(ra, &a, &workers),
            second.provision_api007_genesis(rb, &b, &workers)
        );
        let ga = ga.unwrap();
        let gb = gb.unwrap();
        assert_eq!(ga.cursor_secret(), gb.cursor_secret());
        assert!([1, 2].contains(&ga.cursor_secret()[0]));
        assert_eq!(ga.format(), QueueLogFormat::ChecksummedV2);
        assert_eq!(ga.definition().raw(), legacy);
        assert_eq!(gb.definition().pin(), ga.definition().pin());
        assert_eq!(ga.target().next_offset, 1);
        assert_eq!(gb.target().next_offset, 1);
        let original_secret = *ga.cursor_secret();
        let original_pin = ga.definition().pin();
        assert_eq!(first.native_provision_tasks.available_permits(), 64);
        drop(ga);
        drop(gb);
        drop(a);
        drop(b);
        drop(first);
        drop(second);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        let reopened = open(&root).await;
        let job = Arc::new(admission.reserve(0).unwrap());
        let normalized = encode_definition(&schema()).unwrap();
        let current = reopened
            .provision_api007_genesis(
                request(&reopened, &normalized, 99, &job).await,
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(current.format(), QueueLogFormat::ChecksummedV2);
        assert_eq!(current.definition().raw(), legacy);
        assert_eq!(current.definition().pin(), original_pin);
        assert_eq!(*current.cursor_secret(), original_secret);
        assert_eq!(current.target().next_offset, 1);
        drop(current);
        let mut conflicting = schema();
        conflicting.max_attempts = 4;
        let raw = encode_definition(&conflicting).unwrap();
        assert!(matches!(
            reopened
                .provision_api007_genesis(request(&reopened, &raw, 77, &job).await, &job, &workers)
                .await,
            Err(QueueError::DefinitionConflict)
        ));
        drop(job);
        workers.close_and_drain().await.unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
        drop(reopened);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn actual_legacy_record_is_identified_and_never_reframed_on_reopen() {
        let root = super::super::tests::temp_root("api007-legacy-record");
        std::fs::create_dir_all(&root).unwrap();
        let store = open(&root).await;
        let admission = QueueAdmission::new(1, 16 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let legacy = [0, 0, 0, 0, 1, 3];
        let req = request(&store, &legacy, 71, &job).await;
        let state = store
            .acquire_api007_state(req.namespace, &job, &workers)
            .await
            .unwrap();
        let record = fireweed_engine::queue_log_record::encode_genesis(
            state.state().epoch,
            &address(),
            &req.definition,
            &[71; 32],
            65536,
            &job,
        )
        .unwrap();
        assert_eq!(record.view().unwrap().format(), QueueLogFormat::LegacyV1);
        let appended = store
            .append_api007_record(state.into_namespace(), record, 0, &job, &workers)
            .await
            .unwrap();
        assert!(appended.outcome().is_ok());
        drop(appended);
        drop(req.definition);
        drop(job);
        drop(store);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        let reopened = open(&root).await;
        let job = Arc::new(admission.reserve(0).unwrap());
        let normalized = encode_definition(&schema()).unwrap();
        let recovered = reopened
            .provision_api007_genesis(
                request(&reopened, &normalized, 99, &job).await,
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(recovered.format(), QueueLogFormat::LegacyV1);
        assert_eq!(recovered.definition().raw(), legacy);
        assert_eq!(recovered.cursor_secret(), &[71; 32]);
        assert_eq!(recovered.target().next_offset, 1);
        drop(recovered);
        drop(job);
        workers.close_and_drain().await.unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
        drop(reopened);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn different_schema_creators_have_one_original_winner() {
        let root = super::super::tests::temp_root("api007-provision-definition-race");
        std::fs::create_dir_all(&root).unwrap();
        let first = open(&root).await;
        let second = open(&root).await;
        let admission = QueueAdmission::new(2, 32 << 20).unwrap();
        let a = Arc::new(admission.reserve(0).unwrap());
        let b = Arc::new(admission.reserve(0).unwrap());
        let raw_a = encode_definition(&schema()).unwrap();
        let mut other = schema();
        other.max_attempts = 4;
        let raw_b = encode_definition(&other).unwrap();
        let ra = request(&first, &raw_a, 31, &a).await;
        let rb = request(&second, &raw_b, 32, &b).await;
        let workers = ScratchWorkers::new(2, 4).unwrap();
        let (left, right) = tokio::join!(
            first.provision_api007_genesis(ra, &a, &workers),
            second.provision_api007_genesis(rb, &b, &workers)
        );
        let (winner, raw, secret) = match (left, right) {
            (Ok(winner), Err(QueueError::DefinitionConflict)) => (winner, raw_a, 31),
            (Err(QueueError::DefinitionConflict), Ok(winner)) => (winner, raw_b, 32),
            _ => panic!("expected one success and one definition conflict"),
        };
        assert_eq!(winner.target().next_offset, 1);
        assert_eq!(winner.definition().raw(), raw);
        assert_eq!(winner.cursor_secret(), &[secret; 32]);
        drop(winner);
        drop(a);
        drop(b);
        drop(first);
        drop(second);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        let reopened = open(&root).await;
        let job = Arc::new(admission.reserve(0).unwrap());
        let recovered = reopened
            .provision_api007_genesis(request(&reopened, &raw, 99, &job).await, &job, &workers)
            .await
            .unwrap();
        assert_eq!(recovered.target().next_offset, 1);
        assert_eq!(recovered.definition().raw(), raw);
        assert_eq!(recovered.cursor_secret(), &[secret; 32]);
        drop(recovered);
        drop(job);
        workers.close_and_drain().await.unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
        drop(reopened);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn malformed_or_missing_genesis_is_never_replaced_and_task_slots_are_independent() {
        use std::io::{Seek, SeekFrom, Write};
        let root = super::super::tests::temp_root("api007-provision-corrupt");
        std::fs::create_dir_all(&root).unwrap();
        let store = open(&root).await;
        let admission = QueueAdmission::new(1, 16 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let raw = encode_definition(&schema()).unwrap();
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let mut too_small = request(&store, &raw, 1, &job).await;
        let physical = store
            .checked_api007_shard(&too_small.namespace)
            .unwrap()
            .clone();
        too_small.max_record_bytes = 1;
        assert!(matches!(
            store
                .provision_api007_genesis(too_small, &job, &workers)
                .await,
            Err(QueueError::TooLarge)
        ));
        let check = store.clone();
        assert_eq!(
            tokio::task::spawn_blocking(move || check
                .engine
                .sequencer()
                .authoritative_partition_state(&partition_key(&physical))
                .unwrap())
            .await
            .unwrap()
            .epoch,
            0
        );
        let all_slots = store
            .native_provision_tasks
            .clone()
            .acquire_many_owned(64)
            .await
            .unwrap();
        assert!(matches!(
            store
                .provision_api007_genesis(request(&store, &raw, 1, &job).await, &job, &workers)
                .await,
            Err(QueueError::Busy)
        ));
        drop(all_slots);
        // Indexed genesis with a failed acceleration-hint write is ambiguous.
        // Provision must reacquire and recover it rather than insert a second.
        store.fail_high_water_puts.store(1, Ordering::SeqCst);
        let created = store
            .provision_api007_genesis(request(&store, &raw, 1, &job).await, &job, &workers)
            .await
            .unwrap();
        assert!(created.target().epoch >= 2);
        assert_eq!(created.target().next_offset, 1);
        assert_eq!(created.cursor_secret(), &[1; 32]);
        let shard = store
            .checked_api007_shard(created.namespace())
            .unwrap()
            .clone();
        let plan = store
            .engine
            .plan_fetch(&partition_key(&shard), 0, 1, 65536, 1)
            .unwrap();
        let location = plan.entries()[0].location.clone();
        drop(created);
        let path = root.join(location.object_id);
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(
            u64::from(location.byte_start) + u64::from(location.byte_len) - 33,
        ))
        .unwrap();
        file.write_all(b"X").unwrap();
        drop(file);
        assert!(matches!(
            store
                .provision_api007_genesis(request(&store, &raw, 2, &job).await, &job, &workers)
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        std::fs::remove_file(path).unwrap();
        assert!(matches!(
            store
                .provision_api007_genesis(request(&store, &raw, 3, &job).await, &job, &workers)
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert!(
            store
                .engine
                .plan_fetch(&partition_key(&shard), 1, 1, 65536, 1)
                .unwrap()
                .entries()
                .is_empty()
        );
        drop(plan);
        drop(job);
        workers.close_and_drain().await.unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn caller_cancellation_cannot_stop_provision_between_owned_stages() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let root = super::super::tests::temp_root("api007-provision-cancel");
            std::fs::create_dir_all(&root).unwrap();
            let store = open(&root).await;
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
            let raw = encode_definition(&schema()).unwrap();
            let req = request(&store, &raw, 23, &job).await;
            let shard = store.checked_api007_shard(&req.namespace).unwrap().clone();
            let baseline = admission.usage().unwrap();
            let workers = ScratchWorkers::new(1, 1).unwrap();
            let (started_tx, started_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let writer = store.clone();
            let owned_workers = workers.clone();
            let caller = tokio::spawn(async move {
                writer
                    .provision_api007_genesis(req, &job, &owned_workers)
                    .await
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                while store.native_provision_tasks.available_permits() == 64
                    || admission.usage().unwrap().1 <= baseline.1
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            caller.abort();
            match caller.await {
                Err(error) => assert!(error.is_cancelled()),
                Ok(_) => panic!("caller survived cancellation"),
            }
            assert_eq!(admission.usage().unwrap().0, 1);
            assert_eq!(store.native_provision_tasks.available_permits(), 63);
            release_tx.send(()).unwrap();
            blocker.await.unwrap();
            // Reservation drain precedes worker shutdown: the task still owns later stages.
            tokio::time::timeout(Duration::from_secs(5), terminal.notified())
                .await
                .unwrap();
            workers.close_and_drain().await.unwrap();
            assert_eq!(admission.usage().unwrap(), (0, 0));
            assert_eq!(store.native_provision_tasks.available_permits(), 64);
            let check = store.clone();
            let state = tokio::task::spawn_blocking(move || {
                check
                    .engine
                    .sequencer()
                    .authoritative_partition_state(&partition_key(&shard))
                    .unwrap()
            })
            .await
            .unwrap();
            assert_eq!(state.next_offset, 1);
            drop(store);
            std::fs::remove_dir_all(root).unwrap();
        });
    }
}
