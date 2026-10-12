//! Validate the complete batch and classify all original identities in one
//! native snapshot. Fresh execution begins only after covered orchestration.
use super::*;
use fireweed_engine::{
    queue_memory::incoming_headers,
    queue_publication::{
        CommitReceiptProjection, ReceiptOutcomeRef, receipt_outcome_heap_bound,
        validate_original_commit_receipt, view_receipt_outcome,
    },
};

pub struct CommitBatchReplayInput {
    pub genesis: OwnedQueueLogRecord,
    pub request: CommitBatch,
    pub limits: QueueLimits,
    /// Public receipt-vector budget; independent of internal classification tags.
    pub response_max_bytes: u64,
    pub required_next_offset: u64,
    pub required_insertion_sequence: u64,
    pub required_last_record_epoch: u64,
}
pub struct AdmittedCommitBatchReplay {
    known: Vec<Option<CommitReceipt>>,
    _charges: Vec<OwnedQueueBufferCharge>,
}
impl AdmittedCommitBatchReplay {
    pub fn known(&self) -> &[Option<CommitReceipt>] {
        &self.known
    }
    pub fn take_known(&mut self, position: usize) -> QueueResult<Option<CommitReceipt>> {
        self.known
            .get_mut(position)
            .map(Option::take)
            .ok_or_else(|| unknown("invalid batch replay position"))
    }
}
struct Worker {
    projection: Arc<QueueProjection>,
    input: CommitBatchReplayInput,
    job: Arc<QueueReservation>,
    charges: Vec<OwnedQueueBufferCharge>,
}
impl QueueProjection {
    pub async fn replay_commit_batch(
        self: &Arc<Self>,
        input: CommitBatchReplayInput,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<AdmittedCommitBatchReplay> {
        if !input.genesis.belongs_to(job) {
            return Err(unknown("batch genesis belongs to another operation"));
        }
        input.request.preflight(&input.limits)?;
        if input.response_max_bytes == 0
            || input.response_max_bytes > input.limits.max_response_bytes
        {
            return Err(QueueError::TooLarge);
        }
        fireweed_engine::queue_memory::administrative_envelope_preflight(
            &input.request,
            &input.limits,
        )?;
        let containers = (input.request.groups.len() as u64)
            .checked_mul(
                (std::mem::size_of::<Option<CommitReceipt>>()
                    + std::mem::size_of::<OwnedQueueBufferCharge>() * 4) as u64,
            )
            .and_then(|n| n.checked_mul(3))
            .ok_or(QueueError::TooLarge)?;
        let headers = incoming_headers(&input.request)?;
        let capacity = encoded_len(&input.request)?
            .checked_mul(16)
            .and_then(|n| n.checked_add(headers))
            .and_then(|n| n.checked_add(containers))
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
                    let known = owned_native_transaction(
                        &projection,
                        TransactionBehavior::Deferred,
                        &mut worker,
                    )
                    .await?;
                    Ok(AdmittedCommitBatchReplay {
                        known,
                        _charges: std::mem::take(&mut worker.charges),
                    })
                }))
            })
            .await?
    }
}
impl NativeTransactionWork for Worker {
    type Output = Vec<Option<CommitReceipt>>;
    async fn before_commit(&mut self) {}
    async fn run(&mut self, connection: &Connection) -> QueueResult<Self::Output> {
        let native = super::snapshot::read_snapshot(connection, &self.input.genesis, &self.job)
            .await?
            .ok_or_else(|| unknown("batch projection missing"))?;
        native.validate_point(
            &self.input.genesis,
            &self.input.request.queue,
            (
                self.input.required_next_offset,
                self.input.required_insertion_sequence,
                self.input.required_last_record_epoch,
            ),
        )?;
        // This includes every later sibling's schema/type/credential/target checks.
        self.input
            .request
            .validate(native.definition.schema(), &self.input.limits)?;
        let mut known = Vec::new();
        for group in &self.input.request.groups {
            let format = if group.administration.is_some() { 4 } else { 3 };
            let raw = super::receipt_authority::load_raw(
                connection,
                native.queue_id,
                &group.request_id,
                Some((format, group.fingerprint(&self.input.request.queue)?)),
                &self.job,
                &mut self.charges,
            )
            .await;
            let receipt = match raw {
                Ok(None) => None,
                Ok(Some((_, None))) | Err(QueueError::RequestExpired) => Some(CommitReceipt {
                    request_id: group.request_id.clone(),
                    outcome: GroupOutcome::RequestExpired,
                }),
                Err(QueueError::RequestConflict) => Some(CommitReceipt {
                    request_id: group.request_id.clone(),
                    outcome: GroupOutcome::RequestConflict,
                }),
                Err(error) => return Err(error),
                Ok(Some((format, Some(raw)))) => {
                    let outcome =
                        view_receipt_outcome(2, format, &raw).map_err(publication_error)?;
                    let ReceiptOutcomeRef::Commit(commit) = outcome else {
                        return Err(unknown("batch receipt has wrong domain"));
                    };
                    self.charges.push(
                        self.job.owned_buffer(
                            receipt_outcome_heap_bound(outcome, raw.len() as u64)
                                .map_err(publication_error)?,
                        )?,
                    );
                    let projection = CommitReceiptProjection {
                        request_id: &group.request_id,
                        outcome: commit,
                    };
                    let bytes = encoded_len(&projection)?;
                    self.charges.push(
                        self.job.owned_buffer(
                            bytes
                                .checked_mul(4)
                                .and_then(|n| n.checked_add(4096))
                                .ok_or(QueueError::TooLarge)?,
                        )?,
                    );
                    let encoded = encode(&projection)?;
                    let receipt: CommitReceipt = decode(&encoded)?;
                    validate_original_commit_receipt(
                        &self.input.request.queue,
                        group,
                        &receipt,
                        &self.job,
                    )
                    .map_err(publication_error)?;
                    Some(receipt)
                }
            };
            known.push(receipt);
        }
        // The public sequence is Vec<CommitReceipt>. Internal Option/Lookup tags
        // must not turn an exact-boundary valid public response into TooLarge.
        let mut bytes = encoded_len(&(known.len() as u64))?;
        for receipt in known.iter().flatten() {
            bytes = bytes
                .checked_add(encoded_len(receipt)?)
                .ok_or(QueueError::TooLarge)?;
        }
        if bytes > self.input.response_max_bytes {
            return Err(QueueError::TooLarge);
        }
        Ok(known)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{
        address, apply_group_record, bootstrap, execute_for_test, group, patch, seed,
    };
    use super::*;
    use fireweed_engine::{
        queue_admission::QueueAdmission, queue_log_record::encode_checked_genesis,
    };
    #[tokio::test]
    async fn batch_original_classification_keeps_positional_fates_exact_public_bytes_and_late_errors()
     {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 128 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) =
            bootstrap(&directory.path().join("batch.db"), &job, &workers).await;
        let mut image = seed(&projection, &definition, &job, &workers, false).await;
        let mut first = group("first");
        first.work_updates.push(patch("a", 1, Some(7), None));
        apply_group_record(
            &projection,
            &definition,
            &mut image,
            &first,
            2,
            &job,
            &workers,
        )
        .await
        .unwrap();
        let mut second = group("expired");
        second.work_updates.push(patch("b", 1, Some(8), None));
        apply_group_record(
            &projection,
            &definition,
            &mut image,
            &second,
            3,
            &job,
            &workers,
        )
        .await
        .unwrap();
        use fireweed_core::queue_administration::{
            AdministrativeAction, AdministrativeEntry, AdministrativeIntent,
            AdministrativeOperation, WorkSelector,
        };
        let mut administrative = group("fp4-original");
        administrative.administration = Some(AdministrativeCommit {
            max_result_bytes: 65536,
            intent: AdministrativeIntent {
                evaluated_at: 20,
                before_fields: Some(["u".into()].into()),
                operation: AdministrativeOperation::Addressed(vec![AdministrativeEntry {
                    key: "a".into(),
                    expected_version: Some(2),
                    precondition: WorkSelector::all(vec![]),
                    action: AdministrativeAction::Update {
                        patch: WorkPatch {
                            priority: Some(13),
                            ..Default::default()
                        },
                        lifecycle: AdministrativeLifecycle::Keep,
                        lease_guard: AdministrativeLeaseGuard::RejectActive,
                        remove_field_prefixes: Default::default(),
                    },
                }]),
            },
        });
        let root =
            fireweed_queue_spool::ScratchRoot::open(directory.path().to_path_buf(), &workers)
                .await
                .unwrap();
        let prepared = projection
            .prepare_administration(
                AdministrativePreparationInput {
                    commit: CommitPreparationInput {
                        genesis: encode_checked_genesis(
                            1,
                            &address(),
                            &definition,
                            &[31; 32],
                            16 << 20,
                            &job,
                        )
                        .unwrap(),
                        queue: address(),
                        group: administrative.clone(),
                        limits: QueueLimits::default(),
                        required_next_offset: 4,
                        required_insertion_sequence: 2,
                        required_last_record_epoch: 1,
                    },
                    scratch_root: root,
                    scratch_quota: fireweed_queue_spool::ScratchDiskQuota::new(64 << 20).unwrap(),
                },
                1,
                16 << 20,
                Arc::new(fireweed_conformance::queue_contract::TestClock::default()),
                &job,
                &workers,
            )
            .await
            .unwrap()
            .unwrap();
        let (record, response, head) = prepared.into_parts();
        projection
            .apply(record, head, &job, &workers)
            .await
            .unwrap();
        drop(response);
        let input = |groups: Vec<CommitGroup>, max: u64| CommitBatchReplayInput {
            genesis: encode_checked_genesis(1, &address(), &definition, &[31; 32], 16 << 20, &job)
                .unwrap(),
            request: CommitBatch {
                queue: address(),
                groups,
            },
            limits: QueueLimits::default(),
            response_max_bytes: max,
            required_next_offset: 5,
            required_insertion_sequence: 2,
            required_last_record_epoch: 1,
        };
        let reply = projection
            .replay_commit_batch(
                input(
                    vec![first.clone(), second.clone(), administrative.clone()],
                    1 << 20,
                ),
                &job,
                &workers,
            )
            .await
            .unwrap();
        let expected: Vec<_> = reply.known().iter().map(|x| x.clone().unwrap()).collect();
        let exact = encoded_len(&expected).unwrap();
        drop(reply);
        let reply = projection
            .replay_commit_batch(
                input(
                    vec![first.clone(), second.clone(), administrative.clone()],
                    exact,
                ),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(
            reply.known(),
            expected.iter().cloned().map(Some).collect::<Vec<_>>()
        );
        drop(reply);
        assert!(matches!(
            projection
                .replay_commit_batch(
                    input(
                        vec![first.clone(), second.clone(), administrative.clone()],
                        exact - 1
                    ),
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::TooLarge)
        ));
        execute_for_test(
            &projection,
            "UPDATE q7_requests SET expired=1,outcome=x'ff' WHERE identity=?1",
            vec![blob(b"expired")],
        )
        .await;
        let mut conflict = first.clone();
        conflict.work_updates[0].change = UnclaimedChange::Patch(WorkPatch {
            priority: Some(9),
            ..Default::default()
        });
        let mut fresh = group("fresh");
        fresh.work_updates.push(patch("a", 2, Some(10), None));
        let reply = projection
            .replay_commit_batch(
                input(
                    vec![
                        fresh.clone(),
                        conflict,
                        second.clone(),
                        administrative.clone(),
                    ],
                    1 << 20,
                ),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert!(reply.known()[0].is_none());
        assert_eq!(
            reply.known()[1].as_ref().unwrap().outcome,
            GroupOutcome::RequestConflict
        );
        assert_eq!(
            reply.known()[2].as_ref().unwrap().outcome,
            GroupOutcome::RequestExpired
        );
        assert!(matches!(
            reply.known()[3].as_ref().unwrap().outcome,
            GroupOutcome::AdministrativeCommitted { .. }
        ));
        drop(reply);
        let mut invalid = fresh.clone();
        invalid.request_id = "invalid-later".into();
        invalid.work_updates[0].change = UnclaimedChange::Patch(WorkPatch {
            fields: [("u".into(), Some(FieldValue::Text("wrong".into())))].into(),
            ..Default::default()
        });
        assert!(matches!(
            projection
                .replay_commit_batch(input(vec![fresh.clone(), invalid], 1 << 20), &job, &workers)
                .await,
            Err(QueueError::Invalid(_))
        ));
        execute_for_test(
            &projection,
            "UPDATE q7_requests SET outcome=x'ff' WHERE identity=?1",
            vec![blob(b"first")],
        )
        .await;
        assert!(matches!(
            projection
                .replay_commit_batch(input(vec![fresh, first], 1 << 20), &job, &workers)
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
    }
}
