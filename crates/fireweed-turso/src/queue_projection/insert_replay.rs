//! Original insert results, independent of current work versions/lifecycle.
use super::receipt_metadata::{ReceiptDomain, ReceiptMetadata, read_receipt_metadata};
use super::*;
use fireweed_engine::{
    queue_memory::incoming_headers,
    queue_publication::{
        ReceiptOutcomeRef, receipt_outcome_heap_bound, validate_insert_receipt,
        view_receipt_outcome,
    },
};
pub struct InsertReplayInput {
    pub genesis: OwnedQueueLogRecord,
    pub request: InsertRequest,
    pub limits: QueueLimits,
    pub required_next_offset: u64,
    pub required_insertion_sequence: u64,
    pub required_last_record_epoch: u64,
}
pub struct AdmittedInsertReplay {
    outcomes: Vec<InsertOutcome>,
    _charges: Vec<OwnedQueueBufferCharge>,
}
impl AdmittedInsertReplay {
    pub(super) fn from_outcomes(
        outcomes: Vec<InsertOutcome>,
        charges: Vec<OwnedQueueBufferCharge>,
    ) -> Self {
        Self {
            outcomes,
            _charges: charges,
        }
    }
    pub fn outcomes(&self) -> &[InsertOutcome] {
        &self.outcomes
    }
}
struct InsertReplayWorker {
    projection: Arc<QueueProjection>,
    input: InsertReplayInput,
    job: Arc<QueueReservation>,
    charges: Vec<OwnedQueueBufferCharge>,
}
impl QueueProjection {
    pub async fn replay_insert(
        self: &Arc<Self>,
        input: InsertReplayInput,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<Option<AdmittedInsertReplay>> {
        if !input.genesis.belongs_to(job) {
            return Err(unknown(
                "insert replay genesis belongs to another operation",
            ));
        }
        input.request.preflight(&input.limits)?;
        let headers = incoming_headers(&input.request)?;
        let capacity = encoded_len(&input.request)?
            .checked_mul(16)
            .and_then(|n| n.checked_add(headers))
            .and_then(|n| n.checked_add(65536))
            .ok_or(QueueError::TooLarge)?;
        let mut worker = InsertReplayWorker {
            projection: self.clone(),
            input,
            job: job.clone(),
            charges: vec![job.owned_buffer(capacity)?],
        };
        workers
            .run(move || {
                Ok(crate::tx::block_on_owned_apply(async {
                    let projection = worker.projection.clone();
                    let outcomes = owned_native_transaction(
                        &projection,
                        TransactionBehavior::Deferred,
                        &mut worker,
                    )
                    .await?;
                    Ok(outcomes.map(|outcomes| AdmittedInsertReplay {
                        outcomes,
                        _charges: std::mem::take(&mut worker.charges),
                    }))
                }))
            })
            .await?
    }
}
impl NativeTransactionWork for InsertReplayWorker {
    type Output = Option<Vec<InsertOutcome>>;
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
                panic!("injected insert replay precommit panic");
            }
        }
    }
    async fn run(&mut self, connection: &Connection) -> QueueResult<Self::Output> {
        let snapshot = super::snapshot::read_snapshot(connection, &self.input.genesis, &self.job)
            .await?
            .ok_or_else(|| unknown("insert replay projection missing"))?;
        self.input
            .request
            .validate(snapshot.definition.schema(), &self.input.limits)?;
        snapshot.validate_point(
            &self.input.genesis,
            &self.input.request.queue,
            (
                self.input.required_next_offset,
                self.input.required_insertion_sequence,
                self.input.required_last_record_epoch,
            ),
        )?;
        let request = &self.input.request;
        let (format, size) = match read_receipt_metadata(
            connection,
            snapshot.queue_id,
            ReceiptDomain::Insert,
            &request.request_id,
            Some((3, fingerprint("insert", request)?)),
        )
        .await?
        {
            ReceiptMetadata::Absent => return Ok(None),
            ReceiptMetadata::Expired => return Err(QueueError::RequestExpired),
            ReceiptMetadata::Present { format, size } => (format, size),
        };
        self.charges.push(
            self.job.owned_buffer(
                size.checked_mul(4)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(QueueError::TooLarge)?,
            )?,
        );
        let hash = discovery_digest(request.request_id.as_bytes());
        let row=one(connection,"SELECT outcome FROM q7_requests WHERE queue_id=?1 AND domain=0 AND identity_hash=?2 AND identity=?3",vec![Value::Integer(snapshot.queue_id),blob(&hash),blob(request.request_id.as_bytes())],1).await?.ok_or_else(||unknown("insert receipt vanished in snapshot"))?;
        let raw = bytes(&row[0])?;
        if raw.len() as u64 != size {
            return Err(unknown("insert receipt length changed in snapshot"));
        }
        let outcome = view_receipt_outcome(0, format, raw).map_err(publication_error)?;
        let ReceiptOutcomeRef::Insert(outcomes) = outcome else {
            return Err(unknown("insert domain contains commit outcome"));
        };
        validate_insert_receipt(request, outcomes).map_err(publication_error)?;
        let size = encoded_len(&outcomes)?;
        if size > self.input.limits.max_response_bytes {
            return Err(QueueError::TooLarge);
        }
        let conversion = receipt_outcome_heap_bound(outcome, raw.len() as u64)
            .map_err(publication_error)?
            .checked_add(size.checked_mul(4).ok_or(QueueError::TooLarge)?)
            .ok_or(QueueError::TooLarge)?;
        self.charges.push(self.job.owned_buffer(conversion)?);
        Ok(Some(decode(&encode(&outcomes)?)?))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{
        address, apply_group_record, bootstrap, execute_for_test, group, inspect, patch,
        publication, seed, work,
    };
    use super::*;
    use fireweed_engine::{
        queue_admission::QueueAdmission,
        queue_log_record::encode_checked_genesis,
        queue_planner::{combine_deltas, insert_unique_probes, plan_insert_vector},
        queue_publication::PublicationReceipt,
    };
    fn input(
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
        request: InsertRequest,
        head: u64,
        sequence: u64,
    ) -> InsertReplayInput {
        InsertReplayInput {
            genesis: encode_checked_genesis(1, &address(), definition, &[31; 32], 16 << 20, job)
                .unwrap(),
            request,
            limits: QueueLimits::default(),
            required_next_offset: head,
            required_insertion_sequence: sequence,
            required_last_record_epoch: 1,
        }
    }
    #[tokio::test]
    async fn insert_replay_is_original_positional_and_fingerprint_first_after_deletion() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("insert.db");
        let admission = QueueAdmission::new(1, 128 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        let mut snapshot = seed(&projection, &definition, &job, &workers, true).await;
        let request = InsertRequest {
            queue: address(),
            request_id: "seed".into(),
            mode: InsertMode::InsertOnly,
            items: vec![work("a", 1, true), work("b", 2, false)],
        };
        let original = vec![
            InsertOutcome::Inserted {
                key: "a".into(),
                version: 1,
            },
            InsertOutcome::Inserted {
                key: "b".into(),
                version: 1,
            },
        ];
        let raw = inspect(
            &projection,
            "SELECT outcome FROM q7_requests WHERE domain=0 AND identity=?1",
            vec![blob(b"seed")],
            1,
        )
        .await;
        let mut deletion = group("delete-a");
        deletion.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Delete,
        });
        apply_group_record(
            &projection,
            &definition,
            &mut snapshot,
            &deletion,
            2,
            &job,
            &workers,
        )
        .await
        .unwrap();
        let mut change = group("change-b");
        change.work_updates.push(patch("b", 1, Some(33), None));
        apply_group_record(
            &projection,
            &definition,
            &mut snapshot,
            &change,
            3,
            &job,
            &workers,
        )
        .await
        .unwrap();
        let replay = projection
            .replay_insert(
                input(&definition, &job, request.clone(), 4, 2),
                &job,
                &workers,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.outcomes(), original);
        drop(replay);
        let upsert = InsertRequest {
            queue: address(),
            request_id: "upsert".into(),
            mode: InsertMode::Upsert,
            items: vec![work("a", 3, false), work("b", 2, false)],
        };
        snapshot
            .unique_probes_loaded
            .extend(insert_unique_probes(&upsert.items, &snapshot, definition.schema()).unwrap());
        let before = snapshot.clone();
        let (deltas, expected) =
            plan_insert_vector(&upsert, &mut snapshot, definition.schema(), || Ok(40)).unwrap();
        let delta = combine_deltas(&deltas, before.next_sequence).unwrap();
        let receipt = PublicationReceipt::insert(&upsert, &expected).unwrap();
        projection
            .apply(
                publication(&before, &delta, &[receipt], &definition, &job),
                4,
                &job,
                &workers,
            )
            .await
            .unwrap();
        let replay = projection
            .replay_insert(
                input(&definition, &job, upsert.clone(), 5, 4),
                &job,
                &workers,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.outcomes(), expected);
        drop(replay);
        let mut rejected = upsert.clone();
        rejected.mode = InsertMode::InsertOnly;
        rejected.request_id = "rejected".into();
        snapshot
            .unique_probes_loaded
            .extend(insert_unique_probes(&rejected.items, &snapshot, definition.schema()).unwrap());
        let before = snapshot.clone();
        let (deltas, expected_rejected) =
            plan_insert_vector(&rejected, &mut snapshot, definition.schema(), || Ok(50)).unwrap();
        let delta = combine_deltas(&deltas, before.next_sequence).unwrap();
        let receipt = PublicationReceipt::insert(&rejected, &expected_rejected).unwrap();
        projection
            .apply(
                publication(&before, &delta, &[receipt], &definition, &job),
                5,
                &job,
                &workers,
            )
            .await
            .unwrap();
        let replay = projection
            .replay_insert(input(&definition, &job, rejected, 6, 4), &job, &workers)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.outcomes(), expected_rejected);
        drop(replay);
        let mut absent = request.clone();
        absent.request_id = "absent".into();
        assert!(
            projection
                .replay_insert(input(&definition, &job, absent, 6, 4), &job, &workers)
                .await
                .unwrap()
                .is_none()
        );
        let mut invalid = request.clone();
        invalid.items.clear();
        assert!(matches!(
            projection
                .replay_insert(input(&definition, &job, invalid, 6, 4), &job, &workers)
                .await,
            Err(QueueError::Invalid(_))
        ));
        let mut duplicate = request.clone();
        duplicate.items[1] = duplicate.items[0].clone();
        assert!(matches!(
            projection
                .replay_insert(input(&definition, &job, duplicate, 6, 4), &job, &workers)
                .await,
            Err(QueueError::Invalid(_))
        ));
        let mut below_floor = input(&definition, &job, request.clone(), 6, 4);
        below_floor.limits.max_response_bytes = encoded_len(&original).unwrap();
        assert!(matches!(
            projection.replay_insert(below_floor, &job, &workers).await,
            Err(QueueError::TooLarge)
        ));
        let wrong_mode = inspect(
            &projection,
            "SELECT outcome FROM q7_requests WHERE domain=0 AND identity=?1",
            vec![blob(b"upsert")],
            1,
        )
        .await;
        execute_for_test(
            &projection,
            "UPDATE q7_requests SET outcome=?1 WHERE domain=0 AND identity=?2",
            vec![wrong_mode[0].clone(), blob(b"seed")],
        )
        .await;
        assert!(matches!(
            projection
                .replay_insert(
                    input(&definition, &job, request.clone(), 6, 4),
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        execute_for_test(&projection,"UPDATE q7_requests SET expired=1,outcome=zeroblob(8388608) WHERE domain=0 AND identity=?1",vec![blob(b"seed")]).await;
        let mut changed = request.clone();
        changed.items[0].priority = 99;
        assert!(matches!(
            projection
                .replay_insert(input(&definition, &job, changed, 6, 4), &job, &workers)
                .await,
            Err(QueueError::RequestConflict)
        ));
        assert!(matches!(
            projection
                .replay_insert(
                    input(&definition, &job, request.clone(), 6, 4),
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::RequestExpired)
        ));
        execute_for_test(
            &projection,
            "UPDATE q7_requests SET expired=0,outcome=?1 WHERE domain=0 AND identity=?2",
            vec![raw[0].clone(), blob(b"seed")],
        )
        .await;
        projection
            .panic_after_command
            .store(true, std::sync::atomic::Ordering::Release);
        assert!(matches!(
            projection
                .replay_insert(
                    input(&definition, &job, request.clone(), 6, 4),
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        drop(projection);
        let projection = QueueProjection::open(&path, &job, &workers).await.unwrap();
        let replay = projection
            .replay_insert(input(&definition, &job, request, 6, 4), &job, &workers)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.outcomes(), original);
        drop(replay);
        workers.close_and_drain().await.unwrap();
    }
}
