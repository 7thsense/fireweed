//! Selective, provisional insert planning. Only conditional durable publication
//! and covered immutable replay can acknowledge these results to a consumer.
use super::receipt_metadata::{
    ReceiptDomain, ReceiptMetadata, read_receipt_metadata_with_authority,
};
use super::*;
use fireweed_engine::{
    Clock,
    queue_log_record::encode_checked_publication_record,
    queue_memory::{group_expansion_budget, incoming_headers},
    queue_planner::{
        RecordIdentity, RecordSnapshot, combine_deltas, insert_unique_probes, plan_insert_vector,
        required_work_fields,
    },
    queue_publication::{PublicationReceipt, encode_publication_pinned},
};
pub type PreparedInsert = super::preparation::PreparedPublication<AdmittedInsertReplay>;
impl PreparedInsert {
    pub fn outcomes(&self) -> &[InsertOutcome] {
        self.response().outcomes()
    }
}
struct Parts {
    record: OwnedQueueLogRecord,
    outcomes: Vec<InsertOutcome>,
    head: u64,
    epoch: u64,
}
struct Worker {
    projection: Arc<QueueProjection>,
    input: InsertReplayInput,
    epoch: u64,
    max_record_bytes: u64,
    clock: Arc<dyn Clock>,
    job: Arc<QueueReservation>,
    charges: Vec<OwnedQueueBufferCharge>,
}
impl QueueProjection {
    /// None means an existing original identity requires immutable insert replay.
    /// This method neither mutates the projection nor publishes to the log.
    pub async fn prepare_insert(
        self: &Arc<Self>,
        input: InsertReplayInput,
        epoch: u64,
        max_record_bytes: u64,
        clock: Arc<dyn Clock>,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<Option<PreparedInsert>> {
        if !input.genesis.belongs_to(job) {
            return Err(unknown(
                "insert preparation genesis belongs to another operation",
            ));
        }
        input.request.preflight(&input.limits)?;
        if epoch == 0 || max_record_bytes == 0 {
            return Err(QueueError::Invalid(
                "invalid insert preparation authority/capacity".into(),
            ));
        }
        let capacity = encoded_len(&input.request)?
            .checked_mul(16)
            .and_then(|n| n.checked_add(incoming_headers(&input.request).ok()?))
            .and_then(|n| n.checked_add(65536))
            .ok_or(QueueError::TooLarge)?;
        let mut worker = Worker {
            projection: self.clone(),
            input,
            epoch,
            max_record_bytes,
            clock,
            job: job.clone(),
            charges: vec![job.owned_buffer(capacity)?],
        };
        workers
            .run(move || {
                Ok(crate::tx::block_on_owned_apply(async {
                    let projection = worker.projection.clone();
                    let parts = owned_native_transaction(
                        &projection,
                        TransactionBehavior::Deferred,
                        &mut worker,
                    )
                    .await?;
                    Ok(parts.map(|parts| {
                        PreparedInsert::new(
                            parts.record,
                            AdmittedInsertReplay::from_outcomes(
                                parts.outcomes,
                                std::mem::take(&mut worker.charges),
                            ),
                            parts.head,
                            parts.epoch,
                        )
                    }))
                }))
            })
            .await?
    }
}
impl NativeTransactionWork for Worker {
    type Output = Option<Parts>;
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
                panic!("injected insert preparation precommit panic");
            }
        }
    }
    async fn run(&mut self, connection: &Connection) -> QueueResult<Self::Output> {
        let native = super::snapshot::read_snapshot(connection, &self.input.genesis, &self.job)
            .await?
            .ok_or_else(|| unknown("insert preparation projection missing"))?;
        native.validate_point(
            &self.input.genesis,
            &self.input.request.queue,
            (
                self.input.required_next_offset,
                self.input.required_insertion_sequence,
                self.input.required_last_record_epoch,
            ),
        )?;
        if self.epoch < native.last_record_epoch {
            return Err(unknown("insert writer epoch precedes recovered history"));
        }
        let request = &self.input.request;
        let schema = native.definition.schema();
        request.validate(schema, &self.input.limits)?;
        match read_receipt_metadata_with_authority(
            connection,
            native.queue_id,
            ReceiptDomain::Insert,
            &request.request_id,
            (3, fingerprint("insert", request)?),
            self.input.original_descriptor(&self.job)?,
        )
        .await?
        {
            ReceiptMetadata::Absent => {}
            ReceiptMetadata::Expired => return Err(QueueError::RequestExpired),
            ReceiptMetadata::Present { .. } => return Ok(None),
        }
        let group = CommitGroup {
            administration: None,
            request_id: request.request_id.clone(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: request.items.clone(),
            work_updates: vec![],
        };
        self.charges.push(
            self.job.owned_buffer(
                (native.definition.raw().len() as u64)
                    .checked_mul(16)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(QueueError::TooLarge)?,
            )?,
        );
        let names = required_work_fields(&group, schema)?;
        let mut snapshot = RecordSnapshot {
            next_sequence: native.insertion_sequence,
            ..Default::default()
        };
        // Every addressed key is already admitted from the incoming vector. Old
        // bodies and nonunique fields are never fetched for complete replacement.
        for item in &request.items {
            let identity = RecordIdentity {
                collection: None,
                key: item.key.clone(),
            };
            match super::work::load_work_authority(
                connection,
                native.queue_id,
                &item.key,
                native.insertion_sequence,
            )
            .await?
            {
                super::work::WorkAuthority::Absent => {}
                super::work::WorkAuthority::Deleted { version } => {
                    snapshot.versions.insert(identity, version);
                }
                super::work::WorkAuthority::Present { id, mut work } => {
                    let loaded =
                        load_selected_fields(connection, id, &schema.work, &names, &self.job)
                            .await?;
                    // Certificate names, including absent components, remain
                    // owned for each addressed record and its planner copies.
                    let certificates = names.iter().try_fold(1024u64, |n, name| {
                        n.checked_add(name.len() as u64)
                            .and_then(|n| n.checked_add(256))
                            .ok_or(QueueError::TooLarge)
                    })?;
                    self.charges.push(
                        self.job.owned_buffer(
                            certificates.checked_mul(16).ok_or(QueueError::TooLarge)?,
                        )?,
                    );
                    self.charges.extend(loaded._charges);
                    work.record.work.fields = loaded.fields;
                    work.field_coverage = FieldCoverage::Selected {
                        names: names.clone(),
                        prefixes: Default::default(),
                    };
                    snapshot.versions.insert(identity, work.record.version);
                    snapshot.work.insert(item.key.clone(), *work);
                }
            }
        }
        self.charges.push(
            self.job.owned_buffer(
                group_expansion_budget(&group, &snapshot, schema)?
                    .checked_mul(8)
                    .ok_or(QueueError::TooLarge)?,
            )?,
        );
        let probes = insert_unique_probes(&request.items, &snapshot, schema)?;
        super::unique_authority::load_unique_authority(
            connection,
            native.queue_id,
            schema,
            &probes,
            &mut snapshot,
            &self.job,
            &mut self.charges,
        )
        .await?;
        snapshot.unique_probes_loaded = probes;
        let before = snapshot.clone();
        let clock = &self.clock;
        let (deltas, outcomes) = plan_insert_vector(request, &mut snapshot, schema, || {
            timestamp_nanos(clock.now())
        })?;
        if encoded_len(&outcomes)? > self.input.limits.max_response_bytes {
            return Err(QueueError::TooLarge);
        }
        let delta = combine_deltas(&deltas, before.next_sequence)?;
        let receipt = PublicationReceipt::insert(request, &outcomes).map_err(publication_error)?;
        let frame = encode_publication_pinned(
            &request.queue,
            native.definition.pin(),
            &before,
            &delta,
            &[receipt],
            self.max_record_bytes,
            &self.job,
        )
        .map_err(publication_error)?;
        let record = encode_checked_publication_record(
            self.epoch,
            frame.as_bytes(),
            self.max_record_bytes,
            &self.job,
        )?;
        Ok(Some(Parts {
            record,
            outcomes,
            head: native.next_offset,
            epoch: self.epoch,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{address, apply_group_record, bootstrap, group, seed, work};
    use super::*;
    use fireweed_conformance::queue_contract::TestClock;
    use fireweed_engine::{
        queue_admission::QueueAdmission, queue_log_record::encode_checked_genesis,
    };
    fn input(
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
        request: InsertRequest,
        head: u64,
        sequence: u64,
    ) -> InsertReplayInput {
        let definition = AdmittedQueueDefinition::from_raw(definition.raw(), job).unwrap();
        InsertReplayInput {
            original: None,
            genesis: encode_checked_genesis(1, &address(), &definition, &[31; 32], 16 << 20, job)
                .unwrap(),
            request,
            limits: QueueLimits::default(),
            required_next_offset: head,
            required_insertion_sequence: sequence,
            required_last_record_epoch: 1,
        }
    }
    #[tokio::test]
    async fn selective_insert_preparation_preserves_order_unique_owners_and_deleted_highwater() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 128 << 20).unwrap();
        let seed_job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(
            &directory.path().join("insert-preparation.db"),
            &seed_job,
            &workers,
        )
        .await;
        let mut image = seed(&projection, &definition, &seed_job, &workers, true).await;
        let clock = Arc::new(TestClock::default());
        clock.set(100);
        let small_admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(small_admission.reserve(0).unwrap());
        let request = InsertRequest {
            queue: address(),
            request_id: "upsert-ordered".into(),
            mode: InsertMode::Upsert,
            items: vec![work("a", 3, false), work("b", 3, false)],
        };
        let prepared = projection
            .prepare_insert(
                input(&definition, &job, request.clone(), 2, 2),
                1,
                16 << 20,
                clock.clone(),
                &job,
                &workers,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(prepared.expected_next_offset(), 2);
        assert_eq!(prepared.writer_epoch(), 1);
        assert!(
            matches!(prepared.outcomes(), [InsertOutcome::Replaced { key, version: 2 }, InsertOutcome::Rejected { key: rejected, reason: Rejection::UniqueConflict }] if key=="a" && rejected=="b")
        );
        let expected = prepared.outcomes().to_vec();
        // Planning is read-only; original seed results and source records persist
        // until canonical publication applies at the exact recovered offset.
        assert!(
            projection
                .replay_insert(
                    input(&definition, &job, request.clone(), 2, 2),
                    &job,
                    &workers
                )
                .await
                .unwrap()
                .is_none()
        );
        let (record, provisional, head) = prepared.into_parts();
        projection
            .apply(record, head, &job, &workers)
            .await
            .unwrap();
        drop(provisional);
        let replay = projection
            .replay_insert(
                input(&definition, &job, request.clone(), 3, 3),
                &job,
                &workers,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.outcomes(), expected);
        drop(replay);
        assert!(
            projection
                .prepare_insert(
                    input(&definition, &job, request.clone(), 3, 3),
                    1,
                    16 << 20,
                    clock.clone(),
                    &job,
                    &workers
                )
                .await
                .unwrap()
                .is_none()
        );
        let mut changed = request.clone();
        changed.items[0].priority += 1;
        assert!(matches!(
            projection
                .prepare_insert(
                    input(&definition, &job, changed, 3, 3),
                    1,
                    16 << 20,
                    clock.clone(),
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::RequestConflict)
        ));
        // Reconstruct the same image with the shared planner for a real deletion.
        let probes = insert_unique_probes(&request.items, &image, definition.schema()).unwrap();
        image.unique_probes_loaded.extend(probes);
        plan_insert_vector(&request, &mut image, definition.schema(), || Ok(100)).unwrap();
        let mut deletion = group("delete-upserted-a");
        deletion.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(2),
            change: UnclaimedChange::Delete,
        });
        apply_group_record(
            &projection,
            &definition,
            &mut image,
            &deletion,
            3,
            &seed_job,
            &workers,
        )
        .await
        .unwrap();
        let reinsert = InsertRequest {
            queue: address(),
            request_id: "reinsert-highwater".into(),
            mode: InsertMode::InsertOnly,
            items: vec![work("a", 4, false)],
        };
        let prepared = projection
            .prepare_insert(
                input(&definition, &job, reinsert.clone(), 4, 3),
                1,
                16 << 20,
                clock.clone(),
                &job,
                &workers,
            )
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(prepared.outcomes(),[InsertOutcome::Inserted { key,version:4 }] if key=="a")
        );
        let (record, response, head) = prepared.into_parts();
        projection
            .apply(record, head, &job, &workers)
            .await
            .unwrap();
        drop(response);
        let replay = projection
            .replay_insert(input(&definition, &job, reinsert, 5, 4), &job, &workers)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            replay.outcomes(),
            [InsertOutcome::Inserted { version: 4, .. }]
        ));
        drop(replay);
        let fresh = InsertRequest {
            queue: address(),
            request_id: "panic-fresh".into(),
            mode: InsertMode::InsertOnly,
            items: vec![work("c", 5, false)],
        };
        projection
            .panic_after_command
            .store(true, std::sync::atomic::Ordering::Release);
        assert!(matches!(
            projection
                .prepare_insert(
                    input(&definition, &job, fresh.clone(), 5, 4),
                    1,
                    16 << 20,
                    clock.clone(),
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        let prepared = projection
            .prepare_insert(
                input(&definition, &job, fresh, 5, 4),
                1,
                16 << 20,
                clock,
                &job,
                &workers,
            )
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            prepared.outcomes(),
            [InsertOutcome::Inserted { version: 1, .. }]
        ));
        drop(prepared);
        workers.close_and_drain().await.unwrap();
    }

    #[tokio::test]
    async fn corrupt_unique_owner_and_stored_type_fail_unknown_without_prepared_record() {
        use super::super::tests::{execute_for_test, inspect};
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 128 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) =
            bootstrap(&directory.path().join("corrupt-owner.db"), &job, &workers).await;
        seed(&projection, &definition, &job, &workers, false).await;
        let clock = Arc::new(TestClock::default());
        let metadata = inspect(
            &projection,
            "SELECT id,queue_id FROM q7_records WHERE key=?1",
            vec![blob(b"a")],
            2,
        )
        .await;
        let id = integer(&metadata[0]).unwrap();
        let queue_id = integer(&metadata[1]).unwrap();
        let fresh = InsertRequest {
            queue: address(),
            request_id: "probe-original-owner".into(),
            mode: InsertMode::InsertOnly,
            items: vec![work("c", 1, false)],
        };
        for (sql, params, restore, restore_params) in [
            (
                "UPDATE q7_indexes SET record_id=-99 WHERE record_id=?1",
                vec![Value::Integer(id)],
                "UPDATE q7_indexes SET record_id=?1 WHERE record_id=-99",
                vec![Value::Integer(id)],
            ),
            (
                "UPDATE q7_records SET queue_id=-99 WHERE id=?1",
                vec![Value::Integer(id)],
                "UPDATE q7_records SET queue_id=?1 WHERE id=?2",
                vec![Value::Integer(queue_id), Value::Integer(id)],
            ),
            (
                "UPDATE q7_records SET collection=1 WHERE id=?1",
                vec![Value::Integer(id)],
                "UPDATE q7_records SET collection=0 WHERE id=?1",
                vec![Value::Integer(id)],
            ),
        ] {
            execute_for_test(&projection, sql, params).await;
            assert!(matches!(
                projection
                    .prepare_insert(
                        input(&definition, &job, fresh.clone(), 2, 2),
                        1,
                        16 << 20,
                        clock.clone(),
                        &job,
                        &workers
                    )
                    .await,
                Err(QueueError::OutcomeUnknown(_))
            ));
            execute_for_test(&projection, restore, restore_params).await;
            let prepared = projection
                .prepare_insert(
                    input(&definition, &job, fresh.clone(), 2, 2),
                    1,
                    16 << 20,
                    clock.clone(),
                    &job,
                    &workers,
                )
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(
                prepared.outcomes(),
                [InsertOutcome::Rejected {
                    reason: Rejection::UniqueConflict,
                    ..
                }]
            ));
            drop(prepared);
        }
        let field = field_number(&definition.schema().work, "u").unwrap();
        execute_for_test(
            &projection,
            "UPDATE q7_fields SET value=?1 WHERE record_id=?2 AND field=?3",
            vec![
                blob(&encode(&FieldValue::Text("wrong-type".into())).unwrap()),
                Value::Integer(id),
                Value::Integer(i64::from(field)),
            ],
        )
        .await;
        let upsert = InsertRequest {
            queue: address(),
            request_id: "wrong-stored-type".into(),
            mode: InsertMode::Upsert,
            items: vec![work("a", 3, false)],
        };
        assert!(matches!(
            projection
                .prepare_insert(
                    input(&definition, &job, upsert.clone(), 2, 2),
                    1,
                    16 << 20,
                    clock.clone(),
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        execute_for_test(
            &projection,
            "UPDATE q7_fields SET value=?1 WHERE record_id=?2 AND field=?3",
            vec![
                blob(&encode(&FieldValue::Integer(1)).unwrap()),
                Value::Integer(id),
                Value::Integer(i64::from(field)),
            ],
        )
        .await;
        let prepared = projection
            .prepare_insert(
                input(&definition, &job, upsert, 2, 2),
                1,
                16 << 20,
                clock,
                &job,
                &workers,
            )
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            prepared.outcomes(),
            [InsertOutcome::Replaced { version: 2, .. }]
        ));
        drop(prepared);
        let point=inspect(&projection,"SELECT applied_next_offset,next_sequence,(SELECT COUNT(*) FROM q7_requests) FROM q7_queues",vec![],3).await;
        assert_eq!(integer(&point[0]).unwrap(), 2);
        assert_eq!(unsigned(&point[1]).unwrap(), 2);
        assert_eq!(integer(&point[2]).unwrap(), 1);
        workers.close_and_drain().await.unwrap();
    }
}
