//! Covered ordinary claim-group commit preparation. Business decisions are the
//! shared planner's; only durable original-receipt resolution can acknowledge it.
use super::receipt_metadata::{ReceiptDomain, ReceiptMetadata, read_receipt_metadata};
use super::*;
use fireweed_engine::{
    Clock,
    queue_log_record::encode_checked_publication_record,
    queue_memory::{group_expansion_budget, incoming_headers},
    queue_planner::{combine_deltas, plan_commit_group, prospective_unique_probes},
    queue_publication::{PublicationReceipt, encode_publication_pinned},
};
pub struct CommitPreparationInput {
    pub genesis: OwnedQueueLogRecord,
    pub queue: QueueAddress,
    pub group: CommitGroup,
    pub limits: QueueLimits,
    pub required_next_offset: u64,
    pub required_insertion_sequence: u64,
    pub required_last_record_epoch: u64,
}
pub struct AdmittedCommitReceipt {
    receipt: CommitReceipt,
    _charges: Vec<OwnedQueueBufferCharge>,
}
impl AdmittedCommitReceipt {
    pub fn receipt(&self) -> &CommitReceipt {
        &self.receipt
    }
}
pub type PreparedOrdinaryCommit = super::preparation::PreparedPublication<AdmittedCommitReceipt>;
impl PreparedOrdinaryCommit {
    pub fn receipt(&self) -> &CommitReceipt {
        self.response().receipt()
    }
}
struct Parts {
    record: OwnedQueueLogRecord,
    receipt: CommitReceipt,
    head: u64,
    epoch: u64,
}
struct Worker {
    projection: Arc<QueueProjection>,
    input: CommitPreparationInput,
    epoch: u64,
    max_record_bytes: u64,
    clock: Arc<dyn Clock>,
    job: Arc<QueueReservation>,
    charges: Vec<OwnedQueueBufferCharge>,
}
impl QueueProjection {
    /// Existing identities require immutable receipt replay. Administration uses
    /// its separately resolved producer; this internal method is ordinary-only.
    pub async fn prepare_ordinary_commit(
        self: &Arc<Self>,
        input: CommitPreparationInput,
        epoch: u64,
        max_record_bytes: u64,
        clock: Arc<dyn Clock>,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<Option<PreparedOrdinaryCommit>> {
        if !input.genesis.belongs_to(job) {
            return Err(unknown("commit genesis belongs to another operation"));
        }
        input.group.preflight_for(&input.queue, &input.limits)?;
        if input.group.administration.is_some() || epoch == 0 || max_record_bytes == 0 {
            return Err(QueueError::Invalid(
                "invalid ordinary commit preparation authority/shape".into(),
            ));
        }
        let headers = incoming_headers(&input.group)?;
        let capacity = encoded_len(&(&input.queue, &input.group))?
            .checked_mul(16)
            .and_then(|n| n.checked_add(headers))
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
                        PreparedOrdinaryCommit::new(
                            parts.record,
                            AdmittedCommitReceipt {
                                receipt: parts.receipt,
                                _charges: std::mem::take(&mut worker.charges),
                            },
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
                panic!("injected commit preparation precommit panic");
            }
        }
    }
    async fn run(&mut self, connection: &Connection) -> QueueResult<Self::Output> {
        let native = super::snapshot::read_snapshot(connection, &self.input.genesis, &self.job)
            .await?
            .ok_or_else(|| unknown("commit preparation projection missing"))?;
        native.validate_point(
            &self.input.genesis,
            &self.input.queue,
            (
                self.input.required_next_offset,
                self.input.required_insertion_sequence,
                self.input.required_last_record_epoch,
            ),
        )?;
        if self.epoch < native.last_record_epoch {
            return Err(unknown("commit writer epoch precedes recovered history"));
        }
        let schema = native.definition.schema();
        let group = &self.input.group;
        group.validate_for(&self.input.queue, schema, &self.input.limits)?;
        match read_receipt_metadata(
            connection,
            native.queue_id,
            ReceiptDomain::Commit,
            &group.request_id,
            Some((3, group.fingerprint(&self.input.queue)?)),
        )
        .await?
        {
            ReceiptMetadata::Absent => {}
            ReceiptMetadata::Expired => return Err(QueueError::RequestExpired),
            ReceiptMetadata::Present { .. } => return Ok(None),
        }
        self.charges.push(
            self.job.owned_buffer(
                (native.definition.raw().len() as u64)
                    .checked_mul(16)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(QueueError::TooLarge)?,
            )?,
        );
        let mut snapshot = super::group_authority::load_group(
            connection,
            native.queue_id,
            schema,
            group,
            native.insertion_sequence,
            &self.job,
            &mut self.charges,
        )
        .await?;
        self.charges.push(
            self.job.owned_buffer(
                group_expansion_budget(group, &snapshot, schema)?
                    .checked_mul(8)
                    .ok_or(QueueError::TooLarge)?,
            )?,
        );
        let probes = prospective_unique_probes(group, &snapshot, schema)?;
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
        // Sample only after every addressed metadata/field/unique read completes.
        let now = timestamp_nanos(self.clock.now())?;
        let delta = plan_commit_group(group, &mut snapshot, now, schema)?;
        if encoded_len(&delta.receipt)? > self.input.limits.max_response_bytes {
            return Err(QueueError::TooLarge);
        }
        let receipt = delta.receipt.clone();
        let combined = combine_deltas(&[delta], before.next_sequence)?;
        let publication_receipt = PublicationReceipt::commit(&self.input.queue, group, &receipt)
            .map_err(publication_error)?;
        let frame = encode_publication_pinned(
            &self.input.queue,
            native.definition.pin(),
            &before,
            &combined,
            &[publication_receipt],
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
            receipt,
            head: native.next_offset,
            epoch: self.epoch,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{address, bootstrap, execute_for_test, group, inspect, seed, work};
    use super::*;
    use fireweed_conformance::queue_contract::TestClock;
    use fireweed_engine::{
        queue_admission::QueueAdmission, queue_log_record::encode_checked_genesis,
    };
    fn input(
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
        group: CommitGroup,
        head: u64,
        sequence: u64,
    ) -> CommitPreparationInput {
        let definition = AdmittedQueueDefinition::from_raw(definition.raw(), job).unwrap();
        CommitPreparationInput {
            genesis: encode_checked_genesis(1, &address(), &definition, &[31; 32], 16 << 20, job)
                .unwrap(),
            queue: address(),
            group,
            limits: QueueLimits::default(),
            required_next_offset: head,
            required_insertion_sequence: sequence,
            required_last_record_epoch: 1,
        }
    }
    #[tokio::test]
    async fn ordinary_commit_preparation_is_atomic_for_claim_state_new_work_and_original_receipts()
    {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("ordinary-commit.db");
        let admission = QueueAdmission::new(2, 128 << 20).unwrap();
        let seed_job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 2).unwrap();
        let (projection, definition) = bootstrap(&path, &seed_job, &workers).await;
        seed(&projection, &definition, &seed_job, &workers, true).await;
        let small = QueueAdmission::new(1, 16 << 20).unwrap();
        let job = Arc::new(small.reserve(0).unwrap());
        let clock = Arc::new(TestClock::default());
        clock.set(1000);
        let claim_request = ClaimRequest {
            queue: address(),
            request_id: "claim-a-before-commit".into(),
            filter: Filter {
                keys: Some(["a".into()].into()),
                ..Default::default()
            },
            order: ClaimOrder::PriorityAscending,
            eligible_at: 0,
            lease_nanos: 1_000_000,
            max_records: 1,
            max_bytes: 65536,
            fields: Some(["u".into()].into()),
            include_body: false,
        };
        let claim_input = |head| {
            let definition = AdmittedQueueDefinition::from_raw(definition.raw(), &job).unwrap();
            ClaimReplayInput {
                genesis: encode_checked_genesis(
                    1,
                    &address(),
                    &definition,
                    &[31; 32],
                    16 << 20,
                    &job,
                )
                .unwrap(),
                request: claim_request.clone(),
                now: 1000,
                limits: QueueLimits::default(),
                required_next_offset: head,
                required_insertion_sequence: 2,
                required_last_record_epoch: 1,
            }
        };
        let claim = projection
            .prepare_claim(claim_input(2), 1, 16 << 20, clock.clone(), &job, &workers)
            .await
            .unwrap()
            .unwrap();
        let (record, provisional, head) = claim.into_parts();
        projection
            .apply(record, head, &job, &workers)
            .await
            .unwrap();
        drop(provisional);
        let claim = projection
            .replay_claim(claim_input(3), &job, &workers)
            .await
            .unwrap()
            .unwrap();
        let claimed = claim.records()[0].clone();
        drop(claim);
        let mut transition = group("finish-original");
        transition.claimed_updates.push(ClaimedUpdate {
            key: "a".into(),
            lease: claimed.lease.clone(),
            expected_version: claimed.record.version,
            patch: WorkPatch {
                fields: [("u".into(), Some(FieldValue::Integer(3)))].into(),
                ..Default::default()
            },
            disposition: Disposition::Complete,
        });
        transition.state_updates.push(StateUpdate {
            collection: "workflow".into(),
            key: "run".into(),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: [("phase".into(), FieldValue::Text("ready".into()))].into(),
                value: vec![7, 8].into(),
            },
        });
        transition.work_inserts.push(work("c", 4, false));
        let prepared = projection
            .prepare_ordinary_commit(
                input(&definition, &job, transition.clone(), 3, 2),
                1,
                16 << 20,
                clock.clone(),
                &job,
                &workers,
            )
            .await
            .unwrap()
            .unwrap();
        let expected = prepared.receipt().clone();
        assert!(
            matches!(&expected.outcome,GroupOutcome::Committed {versions} if versions.len()==3 && versions.iter().any(|v|v.key=="a"&&v.version==3)&&versions.iter().any(|v|v.key=="run"&&v.version==1)&&versions.iter().any(|v|v.key=="c"&&v.version==1))
        );
        let point=inspect(&projection,"SELECT applied_next_offset,(SELECT COUNT(*) FROM q7_requests),(SELECT COUNT(*) FROM q7_records WHERE collection=1) FROM q7_queues",vec![],3).await;
        assert_eq!(integer(&point[0]).unwrap(), 3);
        assert_eq!(integer(&point[1]).unwrap(), 2);
        assert_eq!(integer(&point[2]).unwrap(), 0);
        let (record, response, head) = prepared.into_parts();
        projection
            .apply(record, head, &job, &workers)
            .await
            .unwrap();
        drop(response);
        let state=inspect(&projection,"SELECT r.version,b.value FROM q7_records r JOIN q7_bodies b ON b.record_id=r.id WHERE r.collection=1 AND r.key=?1",vec![blob(b"run")],2).await;
        assert_eq!(unsigned(&state[0]).unwrap(), 1);
        assert_eq!(bytes(&state[1]).unwrap(), [7, 8]);
        let old=inspect(&projection,"SELECT length(b.value),r.lifecycle,r.lease_until,r.lease_hash FROM q7_records r JOIN q7_bodies b ON b.record_id=r.id WHERE r.key=?1",vec![blob(b"a")],4).await;
        assert_eq!(integer(&old[0]).unwrap(), 1 << 20);
        assert_eq!(integer(&old[1]).unwrap(), 2);
        assert!(matches!(old[2], Value::Null));
        assert!(matches!(old[3], Value::Null));
        assert!(
            projection
                .prepare_ordinary_commit(
                    input(&definition, &job, transition.clone(), 4, 3),
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
        let mut changed = transition.clone();
        changed.work_inserts[0].priority += 1;
        assert!(matches!(
            projection
                .prepare_ordinary_commit(
                    input(&definition, &job, changed, 4, 3),
                    1,
                    16 << 20,
                    clock.clone(),
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::RequestConflict)
        ));
        let lookups = projection
            .read_commit_receipts(
                ReceiptReadInput {
                    genesis: input(&definition, &job, transition.clone(), 4, 3).genesis,
                    request: ReceiptReadRequest {
                        queue: address(),
                        request_ids: vec![transition.request_id.clone()],
                        max_bytes: 65536,
                    },
                    limits: QueueLimits::default(),
                    required_next_offset: 4,
                    required_insertion_sequence: 3,
                    required_last_record_epoch: 1,
                },
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(lookups.receipts(), [ReceiptLookup::Found(expected)]);
        drop(lookups);
        let b_input = |head| {
            let mut input = claim_input(head);
            input.request.request_id = "claim-b-before-late-cas".into();
            input.request.filter.keys = Some(["b".into()].into());
            input.required_insertion_sequence = 3;
            input
        };
        let prepared = projection
            .prepare_claim(b_input(4), 1, 16 << 20, clock.clone(), &job, &workers)
            .await
            .unwrap()
            .unwrap();
        let (record, response, head) = prepared.into_parts();
        projection
            .apply(record, head, &job, &workers)
            .await
            .unwrap();
        drop(response);
        let replay = projection
            .replay_claim(b_input(5), &job, &workers)
            .await
            .unwrap()
            .unwrap();
        let b_claim = replay.records()[0].clone();
        drop(replay);
        let mut rejected = transition.clone();
        rejected.request_id = "reject-whole-group".into();
        rejected.claimed_updates[0].key = "b".into();
        rejected.claimed_updates[0].lease = b_claim.lease;
        rejected.claimed_updates[0].expected_version = b_claim.record.version;
        rejected.claimed_updates[0].patch = WorkPatch::default();
        rejected.state_updates[0].expect = StateExpect::Version(999);
        rejected.work_inserts[0] = work("d", 5, false);
        let prepared = projection
            .prepare_ordinary_commit(
                input(&definition, &job, rejected, 5, 3),
                1,
                16 << 20,
                clock.clone(),
                &job,
                &workers,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            prepared.receipt().outcome,
            GroupOutcome::Rejected(Rejection::VersionConflict)
        );
        let (record, response, head) = prepared.into_parts();
        projection
            .apply(record, head, &job, &workers)
            .await
            .unwrap();
        drop(response);
        let untouched=inspect(&projection,"SELECT (SELECT COUNT(*) FROM q7_records WHERE key=?1),next_sequence,(SELECT version FROM q7_records WHERE collection=1 AND key=?2) FROM q7_queues",vec![blob(b"d"),blob(b"run")],3).await;
        assert_eq!(integer(&untouched[0]).unwrap(), 0);
        assert_eq!(unsigned(&untouched[1]).unwrap(), 3);
        assert_eq!(unsigned(&untouched[2]).unwrap(), 1);
        let b = inspect(
            &projection,
            "SELECT version,lifecycle FROM q7_records WHERE key=?1",
            vec![blob(b"b")],
            2,
        )
        .await;
        assert_eq!(unsigned(&b[0]).unwrap(), 2);
        assert_eq!(integer(&b[1]).unwrap(), 1);
        // State replacement needs old indexed fields/version, never the opaque
        // old value. A valid 64-MiB value must not be fetched for this tiny put.
        execute_for_test(&projection,"UPDATE q7_bodies SET value=zeroblob(67108864) WHERE record_id=(SELECT id FROM q7_records WHERE collection=1 AND key=?1)",vec![blob(b"run")]).await;
        let tiny = QueueAdmission::new(1, 1 << 20).unwrap();
        let tiny_job = Arc::new(tiny.reserve(0).unwrap());
        let mut state_only = group("state-replacement-without-old-value");
        state_only.state_updates.push(StateUpdate {
            collection: "workflow".into(),
            key: "run".into(),
            expect: StateExpect::Version(1),
            change: StateChange::Put {
                fields: Fields::new(),
                value: vec![9].into(),
            },
        });
        projection
            .panic_after_command
            .store(true, std::sync::atomic::Ordering::Release);
        assert!(matches!(
            projection
                .prepare_ordinary_commit(
                    input(&definition, &tiny_job, state_only.clone(), 6, 3),
                    1,
                    16 << 20,
                    clock.clone(),
                    &tiny_job,
                    &workers
                )
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        let prepared = projection
            .prepare_ordinary_commit(
                input(&definition, &tiny_job, state_only, 6, 3),
                1,
                16 << 20,
                clock,
                &tiny_job,
                &workers,
            )
            .await
            .unwrap()
            .unwrap();
        let (record, response, head) = prepared.into_parts();
        projection
            .apply(record, head, &tiny_job, &workers)
            .await
            .unwrap();
        drop(response);
        let state=inspect(&projection,"SELECT r.version,b.value FROM q7_records r JOIN q7_bodies b ON b.record_id=r.id WHERE r.collection=1 AND r.key=?1",vec![blob(b"run")],2).await;
        assert_eq!(unsigned(&state[0]).unwrap(), 2);
        assert_eq!(bytes(&state[1]).unwrap(), [9]);
        drop(projection);
        let reopened = QueueProjection::open(&path, &job, &workers).await.unwrap();
        assert!(
            reopened
                .prepare_ordinary_commit(
                    input(&definition, &job, transition, 7, 3),
                    1,
                    16 << 20,
                    Arc::new(TestClock::default()),
                    &job,
                    &workers
                )
                .await
                .unwrap()
                .is_none()
        );
        workers.close_and_drain().await.unwrap();
    }

    async fn publish(
        projection: &Arc<QueueProjection>,
        input: CommitPreparationInput,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
        clock: Arc<dyn Clock>,
    ) -> CommitReceipt {
        let prepared = projection
            .prepare_ordinary_commit(input, 1, 16 << 20, clock, job, workers)
            .await
            .unwrap()
            .unwrap();
        let expected = prepared.receipt().clone();
        let (record, response, head) = prepared.into_parts();
        projection.apply(record, head, job, workers).await.unwrap();
        drop(response);
        expected
    }
    fn state_group(id: &str, key: &str, expect: StateExpect, phase: &str) -> CommitGroup {
        let mut group = group(id);
        group.state_updates.push(StateUpdate {
            collection: "workflow".into(),
            key: key.into(),
            expect,
            change: StateChange::Put {
                fields: [("phase".into(), FieldValue::Text(phase.into()))].into(),
                value: vec![4].into(),
            },
        });
        group
    }
    #[tokio::test]
    async fn state_unique_ownership_swaps_deleted_highwater_and_scope_corruption_are_exact() {
        use super::super::tests::schema;
        use fireweed_engine::queue_schema_codec::encode_definition;
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let projection =
            QueueProjection::open(&directory.path().join("state-unique.db"), &job, &workers)
                .await
                .unwrap();
        let mut schema = schema();
        let state = schema.state.get_mut("workflow").unwrap();
        state
            .indexes
            .insert("phase-unique".into(), vec!["phase".into()]);
        state.unique_indexes.insert("phase-unique".into());
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job).unwrap();
        let genesis =
            encode_checked_genesis(1, &address(), &definition, &[31; 32], 16 << 20, &job).unwrap();
        projection.apply(genesis, 0, &job, &workers).await.unwrap();
        seed(&projection, &definition, &job, &workers, false).await;
        let clock = Arc::new(TestClock::default());
        let first = state_group("state-first", "alpha", StateExpect::Absent, "one");
        assert!(matches!(
            publish(
                &projection,
                input(&definition, &job, first, 2, 2),
                &job,
                &workers,
                clock.clone()
            )
            .await
            .outcome,
            GroupOutcome::Committed { .. }
        ));
        let conflict = state_group("state-conflict", "beta", StateExpect::Absent, "one");
        assert_eq!(
            publish(
                &projection,
                input(&definition, &job, conflict, 3, 2),
                &job,
                &workers,
                clock.clone()
            )
            .await
            .outcome,
            GroupOutcome::Rejected(Rejection::UniqueConflict)
        );
        let mut swap = state_group(
            "state-owner-transfer",
            "alpha",
            StateExpect::Version(1),
            "two",
        );
        swap.state_updates
            .extend(state_group("unused", "beta", StateExpect::Absent, "one").state_updates);
        let receipt = publish(
            &projection,
            input(&definition, &job, swap, 4, 2),
            &job,
            &workers,
            clock.clone(),
        )
        .await;
        assert!(
            matches!(receipt.outcome,GroupOutcome::Committed {versions} if versions.len()==2&&versions.iter().any(|v|v.key=="alpha"&&v.version==2)&&versions.iter().any(|v|v.key=="beta"&&v.version==1))
        );
        let mut deletion = group("state-delete-alpha");
        deletion.state_updates.push(StateUpdate {
            collection: "workflow".into(),
            key: "alpha".into(),
            expect: StateExpect::Version(2),
            change: StateChange::Delete,
        });
        publish(
            &projection,
            input(&definition, &job, deletion, 5, 2),
            &job,
            &workers,
            clock.clone(),
        )
        .await;
        let reinsert = state_group(
            "state-reinsert-alpha",
            "alpha",
            StateExpect::Absent,
            "three",
        );
        let receipt = publish(
            &projection,
            input(&definition, &job, reinsert, 6, 2),
            &job,
            &workers,
            clock.clone(),
        )
        .await;
        assert!(
            matches!(receipt.outcome,GroupOutcome::Committed {versions} if versions.len()==1&&versions[0].version==4)
        );
        let metadata = inspect(
            &projection,
            "SELECT id,version FROM q7_records WHERE collection=1 AND key=?1",
            vec![blob(b"beta")],
            2,
        )
        .await;
        let beta_id = integer(&metadata[0]).unwrap();
        assert_eq!(unsigned(&metadata[1]).unwrap(), 1);
        execute_for_test(
            &projection,
            "UPDATE q7_records SET collection=0 WHERE id=?1",
            vec![Value::Integer(beta_id)],
        )
        .await;
        let outsider = state_group("state-corrupt-owner", "gamma", StateExpect::Absent, "one");
        assert!(matches!(
            projection
                .prepare_ordinary_commit(
                    input(&definition, &job, outsider.clone(), 7, 2),
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
            "UPDATE q7_records SET collection=1 WHERE id=?1",
            vec![Value::Integer(beta_id)],
        )
        .await;
        let prepared = projection
            .prepare_ordinary_commit(
                input(&definition, &job, outsider, 7, 2),
                1,
                16 << 20,
                clock,
                &job,
                &workers,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            prepared.receipt().outcome,
            GroupOutcome::Rejected(Rejection::UniqueConflict)
        );
        drop(prepared);
        let point=inspect(&projection,"SELECT applied_next_offset,next_sequence,(SELECT version FROM q7_records WHERE collection=1 AND key=?1),(SELECT COUNT(*) FROM q7_records WHERE collection=1 AND active=1) FROM q7_queues",vec![blob(b"alpha")],4).await;
        assert_eq!(integer(&point[0]).unwrap(), 7);
        assert_eq!(unsigned(&point[1]).unwrap(), 2);
        assert_eq!(unsigned(&point[2]).unwrap(), 4);
        assert_eq!(integer(&point[3]).unwrap(), 2);
        workers.close_and_drain().await.unwrap();
    }
}
