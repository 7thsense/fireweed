//! Complete addressed workflow-state reads in a covered, owned native snapshot.
use super::*;
use fireweed_engine::queue_memory::incoming_headers;
pub struct StateReadInput {
    pub authority: Option<Arc<fireweed_engine::queue_state_authority::AdmittedStateAuthority>>,
    pub genesis: OwnedQueueLogRecord,
    pub request: StateReadRequest,
    pub limits: QueueLimits,
    pub required_next_offset: u64,
    pub required_insertion_sequence: u64,
    pub required_last_record_epoch: u64,
}
pub struct AdmittedStateRecords {
    records: Vec<Option<StateRecord>>,
    _charges: Vec<OwnedQueueBufferCharge>,
}
impl AdmittedStateRecords {
    pub fn into_owned(
        self,
    ) -> fireweed_engine::queue_contract::OwnedQueueValue<
        Vec<Option<StateRecord>>,
        Vec<OwnedQueueBufferCharge>,
    > {
        fireweed_engine::queue_contract::OwnedQueueValue::new(self.records, self._charges)
    }

    pub fn records(&self) -> &[Option<StateRecord>] {
        &self.records
    }
}
struct Worker {
    projection: Arc<QueueProjection>,
    input: StateReadInput,
    job: Arc<QueueReservation>,
    charges: Vec<OwnedQueueBufferCharge>,
}
impl QueueProjection {
    /// Every state value is complete; requested order, duplicates and absent
    /// identities are preserved. Orchestration must confirm authority afterward.
    pub async fn read_state_records(
        self: &Arc<Self>,
        input: StateReadInput,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<AdmittedStateRecords> {
        if !input.genesis.belongs_to(job) {
            return Err(unknown("state genesis belongs to another operation"));
        }
        input.request.preflight(&input.limits)?;
        let headers = incoming_headers(&input.request)?;
        let containers = (input.request.keys.len() as u64)
            .checked_mul(std::mem::size_of::<Option<StateRecord>>() as u64)
            .and_then(|n| n.checked_mul(3))
            .ok_or(QueueError::TooLarge)?;
        let repeated_collection = (input.request.keys.len() as u64)
            .checked_mul(input.request.collection.len() as u64)
            .and_then(|n| n.checked_mul(4))
            .ok_or(QueueError::TooLarge)?;
        let capacity = encoded_len(&input.request)?
            .checked_mul(16)
            .and_then(|n| n.checked_add(headers))
            .and_then(|n| n.checked_add(containers))
            .and_then(|n| n.checked_add(repeated_collection))
            .and_then(|n| n.checked_add(65536))
            .ok_or(QueueError::TooLarge)?;
        let mut worker = Worker {
            projection: self.clone(),
            input,
            job: job.clone(),
            charges: vec![job.owned_buffer(capacity)?],
        };
        workers
            .run(move || {
                Ok(crate::tx::block_on_owned_apply(async {
                    let projection = worker.projection.clone();
                    let records = owned_native_transaction(
                        &projection,
                        TransactionBehavior::Deferred,
                        &mut worker,
                    )
                    .await?;
                    Ok(AdmittedStateRecords {
                        records,
                        _charges: std::mem::take(&mut worker.charges),
                    })
                }))
            })
            .await?
    }
}
impl NativeTransactionWork for Worker {
    type Output = Vec<Option<StateRecord>>;
    async fn before_commit(&mut self) {
        #[cfg(test)]
        {
            self.projection.pause_apply(false).await;
            assert!(!self.charges.is_empty());
            if self
                .projection
                .panic_after_command
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                panic!("injected state read precommit panic");
            }
        }
    }
    async fn run(&mut self, connection: &Connection) -> QueueResult<Self::Output> {
        let snapshot = super::snapshot::read_snapshot(connection, &self.input.genesis, &self.job)
            .await?
            .ok_or_else(|| unknown("state projection missing"))?;
        snapshot.validate_point(
            &self.input.genesis,
            &self.input.request.queue,
            (
                self.input.required_next_offset,
                self.input.required_insertion_sequence,
                self.input.required_last_record_epoch,
            ),
        )?;
        if let Some(authority) = &self.input.authority {
            authority.validate(
                &self.input.request,
                &self.input.genesis,
                (
                    self.input.required_next_offset,
                    self.input.required_insertion_sequence,
                    self.input.required_last_record_epoch,
                ),
                &self.job,
            )?;
        }
        self.input
            .request
            .validate(snapshot.definition.schema(), &self.input.limits)?;
        if let Some(authority) = &self.input.authority
            && authority.response_bytes()? > self.input.request.max_bytes
        {
            return Err(QueueError::TooLarge);
        }
        let collection = collection_number(
            snapshot.definition.schema(),
            Some(&self.input.request.collection),
        )?;
        let definition = collection_definition(snapshot.definition.schema(), collection)?;
        let mut records = Vec::new();
        let mut total = 0;
        let mut exceeded = false;
        let payload_bound = self
            .input
            .authority
            .as_ref()
            .map(|authority| authority.payload_bound())
            .transpose()?;
        for (position, key) in self.input.request.keys.iter().enumerate() {
            use super::state_authority::{StateAuthority, load_state_authority};
            use fireweed_engine::queue_state_authority::StateDescriptor;
            let descriptor = self
                .input
                .authority
                .as_ref()
                .map(|authority| authority.descriptor(position, key))
                .transpose()?;
            let cached =
                load_state_authority(connection, snapshot.queue_id, collection, key).await?;
            let (id, version) = match (descriptor, cached) {
                (Some(StateDescriptor::NeverSeen), StateAuthority::Absent)
                | (None, StateAuthority::Absent)
                | (None, StateAuthority::Deleted { .. }) => {
                    records.push(None);
                    continue;
                }
                (
                    Some(StateDescriptor::Deleted { highwater }),
                    StateAuthority::Deleted { version },
                ) if highwater == version => {
                    records.push(None);
                    continue;
                }
                (
                    Some(source @ StateDescriptor::Present { highwater, .. }),
                    StateAuthority::Present { id, version },
                ) if highwater == version => {
                    super::state_source::validate_payload_metadata(connection, id, source).await?;
                    (id, version)
                }
                (None, StateAuthority::Present { id, version }) => (id, version),
                _ => {
                    return Err(unknown(
                        "state source/cache presence or highwater disagreement",
                    ));
                }
            };
            let mut fields = Fields::new();
            let mut value = bytes::Bytes::new();
            payload::load_payload(
                connection,
                id,
                definition,
                payload::PayloadSelection {
                    fields: None,
                    include_body: true,
                    max_bytes: payload_bound.unwrap_or(self.input.request.max_bytes),
                },
                &mut payload::NativePayloadBudget {
                    job: &self.job,
                    charges: &mut self.charges,
                    total: &mut total,
                    response_limit: &mut exceeded,
                },
                &mut fields,
                &mut value,
            )
            .await
            .map_err(|error| {
                if payload_bound.is_some() && matches!(error, QueueError::TooLarge) {
                    unknown("cached state payload exceeds source-derived bound")
                } else {
                    error
                }
            })?;
            let record = StateRecord {
                collection: self.input.request.collection.clone(),
                key: key.clone(),
                version,
                fields,
                value,
            };
            if let Some(descriptor) = descriptor {
                descriptor.verify_record(&record)?;
            }
            records.push(Some(record));
        }
        let response = encoded_len(&records)?;
        if response > self.input.request.max_bytes {
            return Err(QueueError::TooLarge);
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{
        address, apply_group_record, bootstrap, execute_for_test, group, seed,
    };
    use super::*;
    use fireweed_engine::{
        queue_admission::QueueAdmission, queue_log_record::encode_checked_genesis,
    };
    fn input(
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
        keys: Vec<String>,
        max_bytes: u64,
        head: u64,
    ) -> StateReadInput {
        StateReadInput {
            authority: None,
            genesis: encode_checked_genesis(1, &address(), definition, &[31; 32], 1 << 20, job)
                .unwrap(),
            request: StateReadRequest {
                queue: address(),
                collection: "workflow".into(),
                keys,
                max_bytes,
            },
            limits: QueueLimits::default(),
            required_next_offset: head,
            required_insertion_sequence: 2,
            required_last_record_epoch: 1,
        }
    }
    fn checked_input(
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
        max_bytes: u64,
        value: &[u8],
    ) -> (StateReadInput, Vec<OwnedQueueLogRecord>) {
        use super::super::tests::{group_record, seed_record};
        use fireweed_engine::{
            queue_recovery::ColdQueueHistory, queue_state_authority::ColdStateTracker,
        };
        let mut input = input(
            definition,
            job,
            vec!["run".into(), "missing".into(), "run".into()],
            max_bytes,
            3,
        );
        let (mut snapshot, seed) = seed_record(definition, job, false);
        let mut group = group("state-source");
        group.state_updates.push(StateUpdate {
            collection: "workflow".into(),
            key: "run".into(),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: [("phase".into(), FieldValue::Text("running".into()))].into(),
                value: bytes::Bytes::copy_from_slice(value),
            },
        });
        let record = group_record(definition, &mut snapshot, &group, job);
        let records = vec![seed, record];
        let mut tracker = ColdStateTracker::new(&input.request, job).unwrap();
        let mut history = ColdQueueHistory::new(&input.request.queue, job).unwrap();
        for (offset, record) in std::iter::once(&input.genesis)
            .chain(records.iter())
            .enumerate()
        {
            history.observe(offset as u64, record.bytes()).unwrap();
            tracker.observe(offset as u64, record).unwrap();
        }
        input.authority = Some(Arc::new(
            tracker.finish(&history, &input.genesis, (3, 2, 1)).unwrap(),
        ));
        (input, records)
    }
    #[tokio::test]
    async fn source_state_size_precedes_payload_admission_and_cache_inflation() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(2, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 2).unwrap();
        let (projection, definition) =
            bootstrap(&directory.path().join("source-size.db"), &job, &workers).await;
        let value = vec![7; 1 << 20];
        let (mut input, records) = checked_input(&definition, &job, 1, &value);
        for (index, record) in records.into_iter().enumerate() {
            projection
                .apply(record, index as u64 + 1, &job, &workers)
                .await
                .unwrap();
        }
        let baseline = admission.usage().unwrap();
        let ballast = job
            .owned_buffer((64 << 20) - baseline.1 - (128 << 10))
            .unwrap();
        assert!(
            matches!(
                projection.read_state_records(input, &job, &workers).await,
                Err(QueueError::TooLarge)
            ),
            "source oversize must precede payload admission failure"
        );
        drop(ballast);
        let record = StateRecord {
            collection: "workflow".into(),
            key: "run".into(),
            version: 1,
            fields: [("phase".into(), FieldValue::Text("running".into()))].into(),
            value: bytes::Bytes::copy_from_slice(&value),
        };
        let expected = vec![Some(record.clone()), None, Some(record)];
        let exact = encoded_len(&expected).unwrap();
        let (next, records) = checked_input(&definition, &job, exact, &value);
        drop(records);
        input = next;
        let result = projection
            .read_state_records(input, &job, &workers)
            .await
            .unwrap();
        assert_eq!(result.records(), expected);
        drop(result);
        let (input, records) = checked_input(&definition, &job, exact - 1, &value);
        drop(records);
        assert!(matches!(
            projection.read_state_records(input, &job, &workers).await,
            Err(QueueError::TooLarge)
        ));
        execute_for_test(&projection, "UPDATE q7_bodies SET value=zeroblob(8388608) WHERE record_id IN (SELECT id FROM q7_records WHERE collection=1 AND key=?1)", vec![blob(b"run")]).await;
        let (input, records) = checked_input(&definition, &job, exact, &value);
        drop(records);
        assert!(
            matches!(
                projection.read_state_records(input, &job, &workers).await,
                Err(QueueError::OutcomeUnknown(_))
            ),
            "fitting source plus inflated cache is uncertain, not an oversized response"
        );
        workers.close_and_drain().await.unwrap();
    }

    #[tokio::test]
    async fn state_native_proof_scope_and_same_length_cache_corruption() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(2, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 2).unwrap();
        let (projection, definition) =
            bootstrap(&directory.path().join("source-scope.db"), &job, &workers).await;
        let (input, records) = checked_input(&definition, &job, 65536, b"exact");
        for (index, record) in records.into_iter().enumerate() {
            projection
                .apply(record, index as u64 + 1, &job, &workers)
                .await
                .unwrap();
        }
        let proof = input.authority.unwrap();
        for keys in [
            vec![],
            vec!["run".into()],
            vec!["missing".into(), "run".into(), "run".into()],
        ] {
            let mut input = input_for_proof(&definition, &job, keys, proof.clone());
            input.request.max_bytes = 65536;
            assert!(matches!(
                projection.read_state_records(input, &job, &workers).await,
                Err(QueueError::OutcomeUnknown(_))
            ));
        }
        let mut different_cut = input_for_proof(&definition, &job, vec![], proof.clone());
        use fireweed_engine::{
            queue_recovery::ColdQueueHistory, queue_state_authority::ColdStateTracker,
        };
        let mut tracker = ColdStateTracker::new(&different_cut.request, &job).unwrap();
        let mut history = ColdQueueHistory::new(&different_cut.request.queue, &job).unwrap();
        history.observe(0, different_cut.genesis.bytes()).unwrap();
        tracker.observe(0, &different_cut.genesis).unwrap();
        different_cut.authority = Some(Arc::new(
            tracker
                .finish(&history, &different_cut.genesis, (1, 0, 1))
                .unwrap(),
        ));
        assert!(
            matches!(
                projection
                    .read_state_records(different_cut, &job, &workers)
                    .await,
                Err(QueueError::OutcomeUnknown(_))
            ),
            "valid native cut must reject a different genuine empty proof cut"
        );
        drop(history);
        let other = Arc::new(admission.reserve(0).unwrap());
        let other_definition = AdmittedQueueDefinition::from_raw(definition.raw(), &other).unwrap();
        let (mut other_input, records) = checked_input(&other_definition, &other, 65536, b"exact");
        drop(records);
        let foreign = input_for_proof(
            &definition,
            &job,
            vec!["run".into(), "missing".into(), "run".into()],
            other_input.authority.take().unwrap(),
        );
        assert!(
            matches!(
                projection.read_state_records(foreign, &job, &workers).await,
                Err(QueueError::OutcomeUnknown(_))
            ),
            "foreign proof owner cannot borrow a valid native input"
        );
        drop(other_input);
        drop(other_definition);
        drop(other);
        execute_for_test(&projection,"UPDATE q7_bodies SET value=?2 WHERE record_id IN (SELECT id FROM q7_records WHERE collection=1 AND key=?1)",vec![blob(b"run"),blob(b"wrong")]).await;
        let input = input_for_proof(
            &definition,
            &job,
            vec!["run".into(), "missing".into(), "run".into()],
            proof,
        );
        assert!(matches!(
            projection.read_state_records(input, &job, &workers).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        execute_for_test(&projection,"UPDATE q7_bodies SET value=?2 WHERE record_id IN (SELECT id FROM q7_records WHERE collection=1 AND key=?1)",vec![blob(b"run"),blob(b"exact")]).await;
        let encoded = encode(&FieldValue::Text("waiting".into())).unwrap();
        execute_for_test(&projection,"UPDATE q7_fields SET value=?2 WHERE record_id IN (SELECT id FROM q7_records WHERE collection=1 AND key=?1)",vec![blob(b"run"),blob(&encoded)]).await;
        let (input, records) = checked_input(&definition, &job, 65536, b"exact");
        drop(records);
        assert!(
            matches!(
                projection.read_state_records(input, &job, &workers).await,
                Err(QueueError::OutcomeUnknown(_))
            ),
            "same-length valid field change must not pass source digest"
        );
        let writer = projection.writer.lock().await;
        assert!(
            writer
                .as_ref()
                .is_none_or(|connection| matches!(connection.is_autocommit(), Ok(true)))
        );
        drop(writer);
        workers.close_and_drain().await.unwrap();
    }
    fn input_for_proof(
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
        keys: Vec<String>,
        proof: Arc<fireweed_engine::queue_state_authority::AdmittedStateAuthority>,
    ) -> StateReadInput {
        let mut input = input(definition, job, keys, 65536, 3);
        input.authority = Some(proof);
        input
    }

    #[tokio::test]
    async fn state_proof_owner_survives_native_panic_and_cancel_cleanup() {
        use fireweed_engine::{
            queue_recovery::ColdQueueHistory, queue_state_authority::ColdStateTracker,
        };
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(2, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 2).unwrap();
        let (projection, definition) =
            bootstrap(&directory.path().join("owner.db"), &job, &workers).await;
        let (initial, records) = checked_input(&definition, &job, 65536, b"exact");
        for (index, record) in records.into_iter().enumerate() {
            projection
                .apply(record, index as u64 + 1, &job, &workers)
                .await
                .unwrap();
        }
        drop(initial);
        let baseline = admission.usage().unwrap();
        for panic in [true, false] {
            let terminal = Arc::new(tokio::sync::Notify::new());
            let signal = terminal.clone();
            let owned_job = Arc::new(
                admission
                    .reserve(0)
                    .unwrap()
                    .with_terminal_callback(move || signal.notify_one())
                    .unwrap(),
            );
            let owned_definition =
                AdmittedQueueDefinition::from_raw(definition.raw(), &owned_job).unwrap();
            let (mut input, records) =
                checked_input(&owned_definition, &owned_job, 65536, b"exact");
            input.authority = None;
            input.request.keys.clear();
            let mut tracker = ColdStateTracker::new(&input.request, &owned_job).unwrap();
            let mut history = ColdQueueHistory::new(&input.request.queue, &owned_job).unwrap();
            for (offset, record) in std::iter::once(&input.genesis)
                .chain(records.iter())
                .enumerate()
            {
                history.observe(offset as u64, record.bytes()).unwrap();
                tracker.observe(offset as u64, record).unwrap();
            }
            let proof = Arc::new(tracker.finish(&history, &input.genesis, (3, 2, 1)).unwrap());
            let weak = Arc::downgrade(&proof);
            input.authority = Some(proof);
            drop(history);
            drop(records);
            drop(owned_definition);
            let pause = Arc::new(ApplyPause {
                after_commit: false,
                fail_reply: false,
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::watch::channel(false).0,
            });
            *projection.pause.lock().await = Some(pause.clone());
            projection
                .panic_after_command
                .store(panic, std::sync::atomic::Ordering::Release);
            let caller = tokio::spawn({
                let projection = projection.clone();
                let workers = workers.clone();
                async move {
                    projection
                        .read_state_records(input, &owned_job, &workers)
                        .await
                }
            });
            tokio::time::timeout(std::time::Duration::from_secs(30), pause.entered.notified())
                .await
                .unwrap();
            assert!(weak.upgrade().is_some());
            assert!(admission.usage().unwrap().1 > baseline.1);
            if panic {
                pause.release.send(true).unwrap();
                assert!(matches!(
                    caller.await.unwrap(),
                    Err(QueueError::OutcomeUnknown(_))
                ));
            } else {
                caller.abort();
                assert!(matches!(caller.await,Err(error) if error.is_cancelled()));
                assert!(
                    weak.upgrade().is_some(),
                    "physical state reader still owns proof after caller cancellation"
                );
                pause.release.send(true).unwrap();
            }
            tokio::time::timeout(std::time::Duration::from_secs(30), terminal.notified())
                .await
                .unwrap();
            assert!(weak.upgrade().is_none());
            assert_eq!(admission.usage().unwrap(), baseline);
            let writer = projection.writer.lock().await;
            assert!(
                writer
                    .as_ref()
                    .is_none_or(|connection| matches!(connection.is_autocommit(), Ok(true)))
            );
            drop(writer);
        }
        workers.close_and_drain().await.unwrap();
    }

    #[tokio::test]
    async fn complete_state_reads_preserve_order_duplicates_unsigned_versions_and_exact_budget() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state-read.db");
        let admission = QueueAdmission::new(2, 128 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 2).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        let mut snapshot = seed(&projection, &definition, &job, &workers, false).await;
        let key = "state/\0λ".repeat(40);
        let mut put = group("create-state-read");
        put.state_updates.push(StateUpdate {
            collection: "workflow".into(),
            key: key.clone(),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: [("phase".into(), FieldValue::Text("running".into()))].into(),
                value: vec![7, 8, 9].into(),
            },
        });
        apply_group_record(
            &projection,
            &definition,
            &mut snapshot,
            &put,
            2,
            &job,
            &workers,
        )
        .await
        .unwrap();
        let keys = vec![key.clone(), "absent".into(), key.clone()];
        let result = projection
            .read_state_records(
                input(&definition, &job, keys.clone(), 65536, 3),
                &job,
                &workers,
            )
            .await
            .unwrap();
        let expected = result.records().to_vec();
        assert_eq!(expected[0], expected[2]);
        assert_eq!(expected[1], None);
        assert_eq!(expected[0].as_ref().unwrap().value.as_ref(), [7, 8, 9]);
        assert_eq!(
            expected[0].as_ref().unwrap().fields,
            [("phase".into(), FieldValue::Text("running".into()))].into()
        );
        let exact = encoded_len(&result.records()).unwrap();
        drop(result);
        let result = projection
            .read_state_records(
                input(&definition, &job, keys.clone(), exact, 3),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(result.records(), expected);
        drop(result);
        assert!(matches!(
            projection
                .read_state_records(
                    input(&definition, &job, keys.clone(), exact - 1, 3),
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::TooLarge)
        ));
        execute_for_test(
            &projection,
            "UPDATE q7_records SET version=?1 WHERE collection=1 AND key=?2",
            vec![blob(&u64::MAX.to_be_bytes()), blob(key.as_bytes())],
        )
        .await;
        let result = projection
            .read_state_records(
                input(&definition, &job, vec![key.clone()], 65536, 3),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(result.records()[0].as_ref().unwrap().version, u64::MAX);
        drop(result);
        execute_for_test(
            &projection,
            "UPDATE q7_records SET active=2 WHERE collection=1 AND key=?1",
            vec![blob(key.as_bytes())],
        )
        .await;
        assert!(matches!(
            projection
                .read_state_records(
                    input(&definition, &job, keys.clone(), 65536, 3),
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        execute_for_test(
            &projection,
            "UPDATE q7_records SET active=0 WHERE collection=1 AND key=?1",
            vec![blob(key.as_bytes())],
        )
        .await;
        execute_for_test(&projection,"DELETE FROM q7_bodies WHERE record_id IN (SELECT id FROM q7_records WHERE collection=1)",vec![]).await;
        let result = projection
            .read_state_records(input(&definition, &job, keys, 65536, 3), &job, &workers)
            .await
            .unwrap();
        assert!(result.records().iter().all(Option::is_none));
        drop(result);
        drop(projection);
        workers.close_and_drain().await.unwrap();
        drop(snapshot);
        drop(definition);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
}
