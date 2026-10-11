//! Resolved new-claim preparation in one covered native snapshot. Publication
//! still requires the independently acquired durable epoch/head; this does not
//! mutate the projection, own that fence or implement the consumer QueueStore.
use super::*;
use fireweed_engine::{
    Clock,
    queue_body::ResolvedBody,
    queue_claim::{ClaimContext, ClaimResponseBudget, claim_expiry, plan_claim},
    queue_claim_publication::{ClaimPublicationInput, encode_claim_publication},
    queue_claim_receipt::{ClaimLease, encode_claim_receipt},
    queue_fields::{FieldCoverage, ResolvedFields},
    queue_log_record::encode_checked_claim_record,
    queue_planner::{CommitDelta, RecordIdentity, RecordSnapshot, combine_deltas},
    queue_publication::encode_publication_pinned,
};
/// All records and tokens are PROVISIONAL until matching epoch/head conditional
/// publication resolves and covered current-record replay validates ownership
/// with a fresh clock before consumer ACK. A lost race resolves original identity
/// and cannot acknowledge a second token set.
/// Moving the publication out never moves response allocations out of their
/// admitted owner. The public response remains available only by borrowing.
pub struct PreparedClaim {
    record: OwnedQueueLogRecord,
    response: AdmittedClaimReplay,
    expected_next_offset: u64,
    writer_epoch: u64,
}
impl PreparedClaim {
    pub fn records(&self) -> &[ClaimedWork] {
        self.response.records()
    }
    pub fn expected_next_offset(&self) -> u64 {
        self.expected_next_offset
    }
    pub fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }
    pub fn into_parts(self) -> (OwnedQueueLogRecord, AdmittedClaimReplay, u64) {
        (self.record, self.response, self.expected_next_offset)
    }
}
struct PreparedParts {
    record: OwnedQueueLogRecord,
    records: Vec<ClaimedWork>,
    expected_next_offset: u64,
    writer_epoch: u64,
}
struct PrepareWorker {
    projection: Arc<QueueProjection>,
    input: ClaimReplayInput,
    epoch: u64,
    max_record_bytes: u64,
    clock: Arc<dyn Clock>,
    job: Arc<QueueReservation>,
    charges: Vec<OwnedQueueBufferCharge>,
}
impl QueueProjection {
    /// None means the original identity already exists and must be resolved
    /// through current-record replay. This method never republishes a receipt.
    /// `input.now` is deliberately ignored: selection samples `clock` only after
    /// acquiring the native snapshot. The acquired log epoch is not its historic
    /// last-record epoch; publication must also compare the recovered head.
    pub async fn prepare_claim(
        self: &Arc<Self>,
        input: ClaimReplayInput,
        epoch: u64,
        max_record_bytes: u64,
        clock: Arc<dyn Clock>,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<Option<PreparedClaim>> {
        if !input.genesis.belongs_to(job) {
            return Err(unknown("prepare genesis belongs to another operation"));
        }
        input.request.preflight(&input.limits)?;
        if epoch == 0 || max_record_bytes == 0 {
            return Err(QueueError::Invalid(
                "invalid claim preparation authority/capacity".into(),
            ));
        }
        let charge = super::selection::selection_charge(&input)?
            .checked_add(
                input
                    .request
                    .max_bytes
                    .checked_mul(4)
                    .ok_or(QueueError::TooLarge)?,
            )
            .and_then(|n| n.checked_add(u64::from(input.request.max_records).checked_mul(4096)?))
            .ok_or(QueueError::TooLarge)?;
        let mut worker = PrepareWorker {
            projection: Arc::clone(self),
            input,
            epoch,
            max_record_bytes,
            clock,
            job: Arc::clone(job),
            charges: vec![job.owned_buffer(charge)?],
        };
        workers
            .run(move || {
                Ok(crate::tx::block_on_owned_apply(async {
                    let projection = Arc::clone(&worker.projection);
                    let result = owned_native_transaction(
                        &projection,
                        TransactionBehavior::Deferred,
                        &mut worker,
                    )
                    .await?;
                    Ok(result.map(|parts| PreparedClaim {
                        record: parts.record,
                        response: AdmittedClaimReplay::from_records(
                            parts.records,
                            std::mem::take(&mut worker.charges),
                        ),
                        expected_next_offset: parts.expected_next_offset,
                        writer_epoch: parts.writer_epoch,
                    }))
                }))
            })
            .await?
    }
}
impl NativeTransactionWork for PrepareWorker {
    type Output = Option<PreparedParts>;
    async fn before_commit(&mut self) {
        #[cfg(test)]
        {
            self.projection.pause_apply(false).await;
            assert!(
                !self.charges.is_empty(),
                "prepare input charges released before transaction terminal"
            );
            if self
                .projection
                .panic_after_command
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                panic!("injected prepare precommit panic");
            }
        }
    }
    async fn run(&mut self, connection: &Connection) -> QueueResult<Self::Output> {
        let snapshot = super::snapshot::read_snapshot(connection, &self.input.genesis, &self.job)
            .await?
            .ok_or_else(|| unknown("prepare projection missing"))?;
        snapshot.validate_claim_input(&self.input)?;
        if self.epoch < snapshot.last_record_epoch {
            return Err(unknown("prepare writer epoch precedes recovered history"));
        }
        if super::replay::claim_receipt_size(connection, snapshot.queue_id, &self.input.request)
            .await?
            .is_some()
        {
            return Ok(None);
        }
        let now = timestamp_nanos(self.clock.now())?;
        let request = &self.input.request;
        let expiry = claim_expiry(now, request.lease_nanos)?;
        let (sql, pool) = super::selection::candidate_sql(
            request,
            snapshot.definition.schema(),
            snapshot.queue_id,
            now,
        )?;
        let params = if pool.is_empty() {
            vec![]
        } else {
            vec![Value::Blob(pool)]
        };
        let mut rows = connection.query(&sql, params).await.map_err(unknown)?;
        let mut candidates = vec![];
        let mut key_bytes = 0u64;
        while let Some(row) = rows.next().await.map_err(unknown)? {
            let id = integer(&row.get_value(0).map_err(unknown)?)?;
            let length =
                u64::try_from(integer(&row.get_value(1).map_err(unknown)?)?).map_err(unknown)?;
            let next = key_bytes.checked_add(length).ok_or(QueueError::TooLarge)?;
            if next > request.max_bytes {
                if candidates.is_empty() {
                    return Err(QueueError::TooLarge);
                }
                break;
            }
            // Actual selected key expansion covers predecessor/map identities,
            // response, membership, delta and simultaneous canonical containers.
            // Charge BEFORE the SDK fetch or any full-key copies; base response
            // admission separately covers payloads and initial SDK/candidate keys.
            self.charges.push(
                self.job.owned_buffer(
                    length
                        .checked_mul(16)
                        .and_then(|n| n.checked_add(1024))
                        .ok_or(QueueError::TooLarge)?,
                )?,
            );
            let key = one(
                connection,
                "SELECT key FROM q7_records WHERE id=?1 AND queue_id=?2 AND collection=0",
                vec![Value::Integer(id), Value::Integer(snapshot.queue_id)],
                1,
            )
            .await?
            .ok_or_else(|| unknown("prepare selected key vanished"))?;
            let key = bytes(&key[0])?;
            if key.len() as u64 != length {
                return Err(unknown("prepare selected key length changed"));
            }
            let key = std::str::from_utf8(key).map_err(unknown)?;
            if key.trim().is_empty() {
                return Err(unknown("invalid prepare selected key"));
            }
            candidates.push((id, key.to_owned()));
            key_bytes = next;
        }
        drop(rows);
        let mut before = RecordSnapshot {
            next_sequence: snapshot.insertion_sequence,
            ..Default::default()
        };
        let mut output = vec![];
        let mut response_budget = ClaimResponseBudget::new(request.max_bytes)?;
        let mut leases = vec![];
        let mut changed = vec![];
        let mut versions = vec![];
        let mut field_effects = BTreeMap::new();
        let mut body_effects = BTreeMap::new();
        let mut payload_total = 0u64;
        for (selected_id, key) in candidates {
            let (id, mut stored) = super::work::load_current_work(
                connection,
                snapshot.queue_id,
                &key,
                snapshot.insertion_sequence,
            )
            .await?
            .ok_or_else(|| unknown("prepare selected work vanished"))?;
            if id != selected_id {
                return Err(unknown("prepare selected identity changed"));
            }
            // The native predicate already selected this record; the common
            // planner validates raw lease shape and eligibility before payload.
            let token = fireweed_engine::generate_query_lease_token()
                .map_err(unknown)?
                .as_str()
                .as_bytes()
                .to_vec();
            let metadata_transition = plan_claim(
                stored.clone(),
                token.clone(),
                ClaimContext {
                    now,
                    eligible_at: request.eligible_at,
                    expires_at: expiry,
                    insertion_head: snapshot.insertion_sequence,
                },
            )?;
            let raw_next = payload_total
                .checked_add(key.len() as u64)
                .and_then(|n| n.checked_add(token.len() as u64))
                .ok_or(QueueError::TooLarge)?;
            if raw_next > request.max_bytes {
                if output.is_empty() {
                    return Err(QueueError::TooLarge);
                }
                break;
            }
            payload_total = raw_next;
            let mut response_limit = false;
            let loaded = super::replay::load_replay_payload(
                connection,
                id,
                &snapshot.definition.schema().work,
                request,
                &mut super::replay::ReplayPayloadBudget {
                    job: &self.job,
                    charges: &mut self.charges,
                    total: &mut payload_total,
                    response_limit: &mut response_limit,
                },
                &mut stored.record.work,
            )
            .await;
            if let Err(error) = loaded {
                if response_limit && matches!(error, QueueError::TooLarge) && !output.is_empty() {
                    break;
                }
                return Err(error);
            }
            stored.body_loaded = request.include_body;
            if request.fields.is_none() {
                stored.field_coverage = FieldCoverage::Complete;
            }
            let mut transitioned = metadata_transition.work;
            transitioned.record.work = stored.record.work.clone();
            transitioned.field_coverage = stored.field_coverage.clone();
            transitioned.body_loaded = stored.body_loaded;
            let claimed = ClaimedWork {
                record: transitioned.record.clone(),
                lease: LeaseCredential {
                    token: token.clone(),
                },
            };
            if !response_budget.try_include(&claimed)? {
                if output.is_empty() {
                    return Err(QueueError::TooLarge);
                }
                break;
            }
            output.push(claimed);
            before.versions.insert(
                RecordIdentity {
                    collection: None,
                    key: key.clone(),
                },
                stored.record.version,
            );
            before.work.insert(key.clone(), stored);
            leases.push(ClaimLease {
                key: key.clone(),
                token,
                version: transitioned.record.version,
            });
            versions.push(RecordVersion {
                collection: None,
                key: key.clone(),
                version: transitioned.record.version,
            });
            changed.push(transitioned);
            field_effects.insert(key.clone(), ResolvedFields::Preserve);
            body_effects.insert(key, ResolvedBody::Preserve);
        }
        if encoded_len(&output)? != response_budget.encoded_bytes()? {
            return Err(unknown("claim response framing mismatch"));
        }
        // Frame metadata is independent of consumer payload coverage. Neither
        // omitted bodies/fields nor loaded values are replacement commands.
        let delta = CommitDelta {
            work: changed,
            work_fields: field_effects,
            work_bodies: body_effects,
            deleted_work: vec![],
            state: vec![],
            deleted_state: vec![],
            versions: versions.clone(),
            next_sequence: before.next_sequence,
            receipt: CommitReceipt {
                request_id: request.request_id.clone(),
                outcome: GroupOutcome::Committed { versions },
            },
        };
        let delta = combine_deltas(&[delta], before.next_sequence)?;
        let commands = encode_publication_pinned(
            &request.queue,
            snapshot.definition.pin(),
            &before,
            &delta,
            &[],
            self.max_record_bytes,
            &self.job,
        )
        .map_err(|error| unknown(format!("{error:?}")))?;
        let receipt = encode_claim_receipt(&leases, self.max_record_bytes, &self.job)?;
        let publication = encode_claim_publication(
            ClaimPublicationInput {
                request,
                definition: &snapshot.definition,
                commands: commands.as_bytes(),
                receipt: &receipt,
                claimed_at: now,
                limits: &self.input.limits,
            },
            self.max_record_bytes,
            &self.job,
        )?;
        let record = encode_checked_claim_record(
            self.epoch,
            &publication,
            self.max_record_bytes,
            &self.job,
        )?;
        Ok(Some(PreparedParts {
            record,
            records: output,
            expected_next_offset: snapshot.next_offset,
            writer_epoch: self.epoch,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{address, bootstrap, execute_for_test, inspect, seed};
    use super::*;
    use fireweed_conformance::queue_contract::TestClock;
    use fireweed_engine::{
        queue_admission::QueueAdmission, queue_log_record::encode_checked_genesis,
    };
    struct Fixture {
        directory: tempfile::TempDir,
        projection: Arc<QueueProjection>,
        definition: AdmittedQueueDefinition,
        admission: Arc<QueueAdmission>,
        job: Arc<QueueReservation>,
        workers: Arc<ScratchWorkers>,
        clock: Arc<TestClock>,
        snapshot: RecordSnapshot,
    }
    impl Fixture {
        async fn new(opaque: bool) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let admission = QueueAdmission::new(1, 128 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let workers = Arc::new(ScratchWorkers::new(1, 1).unwrap());
            let (projection, definition) =
                bootstrap(&directory.path().join("prepare.db"), &job, &workers).await;
            let snapshot = seed(&projection, &definition, &job, &workers, opaque).await;
            let clock = Arc::new(TestClock::default());
            clock.set(100);
            Self {
                directory,
                projection,
                definition,
                admission,
                job,
                workers,
                clock,
                snapshot,
            }
        }
        fn request(&self, id: &str) -> ClaimRequest {
            ClaimRequest {
                queue: address(),
                request_id: id.into(),
                filter: Filter::default(),
                order: ClaimOrder::PriorityAscending,
                eligible_at: 0,
                lease_nanos: 1_000_000,
                max_records: 2,
                max_bytes: 4 << 20,
                fields: None,
                include_body: true,
            }
        }
        fn input(&self, request: ClaimRequest, offset: u64, epoch: u64) -> ClaimReplayInput {
            ClaimReplayInput {
                genesis: encode_checked_genesis(
                    1,
                    &address(),
                    &self.definition,
                    &[31; 32],
                    16 << 20,
                    &self.job,
                )
                .unwrap(),
                request,
                now: -500,
                limits: QueueLimits::default(),
                required_next_offset: offset,
                required_insertion_sequence: 2,
                required_last_record_epoch: epoch,
            }
        }
        async fn prepare(&self, request: ClaimRequest) -> QueueResult<Option<PreparedClaim>> {
            self.projection
                .prepare_claim(
                    self.input(request, 2, 1),
                    7,
                    16 << 20,
                    self.clock.clone(),
                    &self.job,
                    &self.workers,
                )
                .await
        }
        async fn unchanged(&self) {
            let row=inspect(&self.projection,"SELECT applied_next_offset,(SELECT COUNT(*) FROM q7_requests WHERE domain=1),(SELECT COUNT(*) FROM q7_records WHERE lifecycle=1) FROM q7_queues",vec![],3).await;
            assert_eq!(
                row,
                vec![Value::Integer(2), Value::Integer(0), Value::Integer(0)]
            );
        }
        async fn finish(self) {
            drop(self.projection);
            drop(self.definition);
            drop(self.job);
            self.workers.close_and_drain().await.unwrap();
            assert_eq!(self.admission.usage().unwrap(), (0, 0));
        }
    }
    #[tokio::test]
    async fn native_preparation_preserves_selection_order_and_applies_replays_reopens_exactly() {
        let mut f = Fixture::new(true).await;
        execute_for_test(
            &f.projection,
            "UPDATE q7_records SET priority=8 WHERE key=?1",
            vec![blob(b"b")],
        )
        .await;
        let mut request = f.request("prepared");
        request.order = ClaimOrder::PriorityDescending;
        let prepared = f.prepare(request.clone()).await.unwrap().unwrap();
        assert_eq!(prepared.expected_next_offset(), 2);
        assert_eq!(prepared.writer_epoch(), 7);
        assert_eq!(
            prepared
                .records()
                .iter()
                .map(|r| r.record.work.key.as_str())
                .collect::<Vec<_>>(),
            ["b", "a"]
        );
        assert_eq!(prepared.records()[1].record.work.body.len(), 1 << 20);
        assert_eq!(
            prepared.records()[1].record.work.fields["opaque"],
            FieldValue::Bytes(vec![7; 1 << 20])
        );
        assert!(prepared.records().iter().all(|r| r.record.version == 2
            && r.record.attempts == 1
            && r.record.lease_expires_at == Some(1_000_100)));
        f.unchanged().await;
        let expected = prepared.records().to_vec();
        let (record, response, head) = prepared.into_parts();
        let view = record.view().unwrap();
        let QueueLogPayload::Claim(claim) = view.payload() else {
            panic!("not claim")
        };
        assert_eq!(claim.claimed_at(), 100);
        assert_eq!(
            claim
                .receipt()
                .entries()
                .map(|v| v.unwrap().key.to_owned())
                .collect::<Vec<_>>(),
            ["b", "a"]
        );
        f.projection
            .apply(record, head, &f.job, &f.workers)
            .await
            .unwrap();
        assert_eq!(response.records(), expected);
        drop(response);
        let replay = f
            .projection
            .replay_claim(f.input(request.clone(), 3, 7), &f.job, &f.workers)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.records(), expected);
        drop(replay);
        assert!(
            f.projection
                .prepare_claim(
                    f.input(request.clone(), 3, 7),
                    8,
                    16 << 20,
                    f.clock.clone(),
                    &f.job,
                    &f.workers
                )
                .await
                .unwrap()
                .is_none()
        );
        let path = f.directory.path().join("prepare.db");
        drop(f.projection);
        f.projection = QueueProjection::open(&path, &f.job, &f.workers)
            .await
            .unwrap();
        let replay = f
            .projection
            .replay_claim(f.input(request, 3, 7), &f.job, &f.workers)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.records(), expected);
        drop(replay);
        drop(expected);
        f.finish().await;
    }
    #[tokio::test]
    async fn exact_response_prefix_stops_at_first_nonfitting_work_without_skipping() {
        let f = Fixture::new(true).await;
        execute_for_test(
            &f.projection,
            "UPDATE q7_records SET priority=8 WHERE key=?1",
            vec![blob(b"b")],
        )
        .await;
        let mut request = f.request("budget-probe");
        request.order = ClaimOrder::PriorityDescending;
        let probe = f.prepare(request.clone()).await.unwrap().unwrap();
        let exact = encoded_len(&&probe.records()[..1]).unwrap();
        drop(probe);
        request.request_id = "fits-one".into();
        request.max_bytes = exact;
        let fitted = f.prepare(request.clone()).await.unwrap().unwrap();
        assert_eq!(fitted.records().len(), 1);
        assert_eq!(fitted.records()[0].record.work.key, "b");
        assert_eq!(encoded_len(&fitted.records()).unwrap(), exact);
        drop(fitted);
        f.unchanged().await;
        request.request_id = "first-too-large".into();
        request.max_bytes = exact - 1;
        assert!(matches!(
            f.prepare(request).await,
            Err(QueueError::TooLarge)
        ));
        f.unchanged().await;
        let mut omitted = f.request("selective");
        omitted.max_bytes = 4096;
        omitted.fields = Some(["u".into()].into());
        omitted.include_body = false;
        let prepared = f.prepare(omitted).await.unwrap().unwrap();
        assert_eq!(prepared.records().len(), 2);
        assert!(
            prepared
                .records()
                .iter()
                .all(|r| r.record.work.body.is_empty() && r.record.work.fields.len() == 1)
        );
        let (record, response, head) = prepared.into_parts();
        f.projection
            .apply(record, head, &f.job, &f.workers)
            .await
            .unwrap();
        drop(response);
        let row=inspect(&f.projection,"SELECT length(value) FROM q7_bodies WHERE record_id=(SELECT id FROM q7_records WHERE key=?1)",vec![blob(b"a")],1).await;
        assert_eq!(integer(&row[0]).unwrap(), 1 << 20);
        f.finish().await;
    }
    #[tokio::test]
    async fn empty_receipts_conflicts_expiration_and_frame_failures_do_not_create_new_claims() {
        let f = Fixture::new(false).await;
        let mut request = f.request("empty");
        request.filter.keys = Some(["missing".into()].into());
        assert!(
            f.projection
                .prepare_claim(
                    f.input(request.clone(), 2, 1),
                    7,
                    1,
                    f.clock.clone(),
                    &f.job,
                    &f.workers
                )
                .await
                .is_err()
        );
        f.unchanged().await;
        let prepared = f.prepare(request.clone()).await.unwrap().unwrap();
        assert!(prepared.records().is_empty());
        let (record, response, head) = prepared.into_parts();
        f.projection
            .apply(record, head, &f.job, &f.workers)
            .await
            .unwrap();
        drop(response);
        assert!(
            f.projection
                .prepare_claim(
                    f.input(request.clone(), 3, 7),
                    8,
                    16 << 20,
                    f.clock.clone(),
                    &f.job,
                    &f.workers
                )
                .await
                .unwrap()
                .is_none()
        );
        let mut changed = request.clone();
        changed.order = ClaimOrder::PriorityDescending;
        assert!(matches!(
            f.projection
                .prepare_claim(
                    f.input(changed, 3, 7),
                    8,
                    16 << 20,
                    f.clock.clone(),
                    &f.job,
                    &f.workers
                )
                .await,
            Err(QueueError::RequestConflict)
        ));
        execute_for_test(
            &f.projection,
            "UPDATE q7_requests SET expired=1 WHERE domain=1",
            vec![],
        )
        .await;
        assert!(matches!(
            f.projection
                .prepare_claim(
                    f.input(request, 3, 7),
                    8,
                    16 << 20,
                    f.clock.clone(),
                    &f.job,
                    &f.workers
                )
                .await,
            Err(QueueError::RequestExpired)
        ));
        f.finish().await;
    }
    #[tokio::test]
    async fn queued_current_replay_samples_clock_after_snapshot_and_rejects_expiry() {
        let f = Fixture::new(false).await;
        let request = f.request("fresh-replay");
        let prepared = f
            .projection
            .prepare_claim(
                f.input(request.clone(), 2, 1),
                7,
                16 << 20,
                f.clock.clone(),
                &f.job,
                &f.workers,
            )
            .await
            .unwrap()
            .unwrap();
        let (record, provisional, head) = prepared.into_parts();
        f.projection
            .apply(record, head, &f.job, &f.workers)
            .await
            .unwrap();
        drop(provisional);
        let guard = f.projection.writer.lock().await;
        let call = tokio::spawn({
            let projection = f.projection.clone();
            let job = f.job.clone();
            let workers = f.workers.clone();
            let clock = f.clock.clone();
            let input = f.input(request.clone(), 3, 7);
            async move {
                projection
                    .replay_claim_with_clock(input, clock, &job, &workers)
                    .await
            }
        });
        tokio::task::yield_now().await;
        f.clock.set(1_000_100);
        drop(guard);
        assert!(matches!(call.await.unwrap(), Err(QueueError::ClaimExpired)));
        let mut changed = request;
        changed.include_body = false;
        assert!(matches!(
            f.projection
                .replay_claim_with_clock(
                    f.input(changed, 3, 7),
                    f.clock.clone(),
                    &f.job,
                    &f.workers,
                )
                .await,
            Err(QueueError::RequestConflict)
        ));
    }
    #[tokio::test]
    async fn queued_preparation_samples_clock_after_native_snapshot_and_rejects_bad_metadata() {
        let f = Fixture::new(false).await;
        execute_for_test(
            &f.projection,
            "UPDATE q7_records SET lifecycle=1,lease_until=500,lease_hash=?1 WHERE key=?2",
            vec![blob(&[8; 32]), blob(b"a")],
        )
        .await;
        let mut request = f.request("fresh");
        request.filter.keys = Some(["a".into()].into());
        let guard = f.projection.writer.lock().await;
        let call = tokio::spawn({
            let projection = f.projection.clone();
            let job = f.job.clone();
            let workers = f.workers.clone();
            let clock = f.clock.clone();
            let input = f.input(request.clone(), 2, 1);
            async move {
                projection
                    .prepare_claim(input, 7, 16 << 20, clock, &job, &workers)
                    .await
            }
        });
        tokio::task::yield_now().await;
        f.clock.set(1000);
        drop(guard);
        let prepared = call.await.unwrap().unwrap().unwrap();
        assert_eq!(prepared.records().len(), 1);
        assert_eq!(
            prepared.records()[0].record.lease_expires_at,
            Some(1_001_000)
        );
        drop(prepared);
        execute_for_test(
            &f.projection,
            "UPDATE q7_records SET lease_hash=?1 WHERE key=?2",
            vec![blob(&vec![0; 4 << 20]), blob(b"a")],
        )
        .await;
        request.max_bytes = 1;
        assert!(matches!(
            f.prepare(request).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert!(
            f.projection
                .prepare_claim(
                    f.input(f.request("stale-epoch"), 2, 1),
                    0,
                    16 << 20,
                    f.clock.clone(),
                    &f.job,
                    &f.workers
                )
                .await
                .is_err()
        );
        f.finish().await;
    }
    #[tokio::test]
    async fn full_unsigned_metadata_and_admission_failure_never_shorten_a_valid_prefix() {
        let f = Fixture::new(false).await;
        let mut too_much = f.request("admission");
        too_much.max_bytes = 32 << 20;
        assert!(matches!(
            f.prepare(too_much).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        f.unchanged().await;
        execute_for_test(&f.projection,"UPDATE q7_records SET version=?1,insertion_sequence=?2,attempts=7,lifecycle=1,lease_until=50,lease_hash=?3 WHERE key=?4",
            vec![number(u64::MAX-1),number(u64::MAX),blob(&[8;32]),blob(b"a")]).await;
        execute_for_test(
            &f.projection,
            "UPDATE q7_queues SET next_sequence=?1",
            vec![number(u64::MAX)],
        )
        .await;
        let mut request = f.request("unsigned");
        request.filter.keys = Some(["a".into()].into());
        let mut input = f.input(request.clone(), 2, 1);
        input.required_insertion_sequence = u64::MAX;
        let prepared = f
            .projection
            .prepare_claim(input, 7, 16 << 20, f.clock.clone(), &f.job, &f.workers)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(prepared.records()[0].record.version, u64::MAX);
        assert_eq!(prepared.records()[0].record.insertion_sequence, u64::MAX);
        assert_eq!(prepared.records()[0].record.attempts, 8);
        let expected = prepared.records().to_vec();
        let (record, response, head) = prepared.into_parts();
        f.projection
            .apply(record, head, &f.job, &f.workers)
            .await
            .unwrap();
        drop(response);
        let mut input = f.input(request, 3, 7);
        input.required_insertion_sequence = u64::MAX;
        input.now = 100;
        let replay = f
            .projection
            .replay_claim(input, &f.job, &f.workers)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.records(), expected);
        drop(replay);
        drop(expected);
        f.finish().await;
    }
    #[tokio::test]
    async fn cancelled_started_preparation_awaits_worker_and_reservation_terminal() {
        let f = Fixture::new(false).await;
        let request = f.request("cancel");
        let raw = f.definition.raw().to_vec();
        let projection = f.projection;
        let workers = f.workers;
        let admission = f.admission;
        let clock = f.clock;
        drop(f.definition);
        drop(f.job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        let terminal = Arc::new(tokio::sync::Notify::new());
        let notify = terminal.clone();
        let job = Arc::new(
            admission
                .reserve(0)
                .unwrap()
                .with_terminal_callback(move || notify.notify_one())
                .unwrap(),
        );
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let input = ClaimReplayInput {
            genesis: encode_checked_genesis(1, &address(), &definition, &[31; 32], 16 << 20, &job)
                .unwrap(),
            request,
            now: -500,
            limits: QueueLimits::default(),
            required_next_offset: 2,
            required_insertion_sequence: 2,
            required_last_record_epoch: 1,
        };
        drop(definition);
        let pause = Arc::new(ApplyPause {
            after_commit: false,
            fail_reply: false,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::watch::channel(false).0,
        });
        *projection.pause.lock().await = Some(pause.clone());
        let call = tokio::spawn({
            let projection = projection.clone();
            let workers = workers.clone();
            async move {
                projection
                    .prepare_claim(input, 7, 16 << 20, clock, &job, &workers)
                    .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), pause.entered.notified())
            .await
            .unwrap();
        let retained = admission.usage().unwrap();
        assert!(retained.1 > 1000);
        call.abort();
        assert!(matches!(call.await,Err(error) if error.is_cancelled()));
        assert_eq!(admission.usage().unwrap(), retained);
        pause.release.send(true).unwrap();
        workers.close_and_drain().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), terminal.notified())
            .await
            .unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
        *projection.pause.lock().await = None;
        let clone = projection.clone();
        assert!(
            tokio::task::spawn_blocking(move || crate::tx::block_on_owned_apply(async {
                clone
                    .writer
                    .lock()
                    .await
                    .as_ref()
                    .unwrap()
                    .is_autocommit()
                    .unwrap()
            }))
            .await
            .unwrap()
        );
        let row=inspect(&projection,"SELECT applied_next_offset,(SELECT COUNT(*) FROM q7_requests WHERE domain=1),(SELECT COUNT(*) FROM q7_records WHERE lifecycle=1) FROM q7_queues",vec![],3).await;
        assert_eq!(
            row,
            vec![Value::Integer(2), Value::Integer(0), Value::Integer(0)]
        );
        drop(projection);
        drop(f.directory);
    }
    #[tokio::test]
    async fn precommit_panic_rolls_back_with_input_charges_retained_and_connection_reusable() {
        let f = Fixture::new(false).await;
        let baseline = f.admission.usage().unwrap();
        f.projection
            .panic_after_command
            .store(true, std::sync::atomic::Ordering::Release);
        assert!(matches!(
            f.prepare(f.request("panic")).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        f.workers.close_and_drain().await.unwrap();
        // Drain and output drop are distinct. No prepared result is returned on
        // this path; request/genesis and all scratch remain worker-owned until
        // cleanup. A subsequent owner observes a clean native connection.
        let projection = f.projection.clone();
        assert!(
            tokio::task::spawn_blocking(move || crate::tx::block_on_owned_apply(async {
                projection
                    .writer
                    .lock()
                    .await
                    .as_ref()
                    .unwrap()
                    .is_autocommit()
                    .unwrap()
            }))
            .await
            .unwrap()
        );
        f.unchanged().await;
        assert_eq!(f.admission.usage().unwrap(), baseline);
        f.finish().await;
    }
    #[tokio::test]
    async fn long_stored_key_is_precharged_without_request_key_filter_under_tight_admission() {
        use fireweed_engine::{
            queue_planner::{combine_deltas, insert_unique_probes, plan_insert_vector},
            queue_publication::PublicationReceipt,
        };
        let f = Fixture::new(false).await;
        // Build a valid native insertion rather than manufacturing cached rows.
        let mut snapshot = f.snapshot.clone();
        let long = "λ\0".repeat(65536);
        let insert = InsertRequest {
            queue: address(),
            request_id: "long-key".into(),
            items: vec![NewWork {
                key: long.clone(),
                priority: -10,
                available_at: 0,
                fields: Fields::from([("u".into(), FieldValue::Integer(3))]),
                body: bytes::Bytes::new(),
            }],
            mode: InsertMode::InsertOnly,
        };
        snapshot.unique_probes_loaded =
            insert_unique_probes(&insert.items, &snapshot, f.definition.schema()).unwrap();
        let before = snapshot.clone();
        let (deltas, outcomes) =
            plan_insert_vector(&insert, &mut snapshot, f.definition.schema(), || Ok(0)).unwrap();
        let delta = combine_deltas(&deltas, 2).unwrap();
        let receipt = PublicationReceipt::insert(&insert, &outcomes).unwrap();
        let record =
            super::super::tests::publication(&before, &delta, &[receipt], &f.definition, &f.job);
        f.projection
            .apply(record, 2, &f.job, &f.workers)
            .await
            .unwrap();
        let mut request = f.request("tight-key");
        request.max_records = 1;
        request.max_bytes = 256 << 10;
        assert!(request.filter.keys.is_none());
        for (capacity, should_fit) in [(4 << 20, false), (16 << 20, true)] {
            let admission = QueueAdmission::new(1, capacity).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let definition = AdmittedQueueDefinition::from_raw(f.definition.raw(), &job).unwrap();
            let input = ClaimReplayInput {
                genesis: encode_checked_genesis(
                    1,
                    &address(),
                    &definition,
                    &[31; 32],
                    16 << 20,
                    &job,
                )
                .unwrap(),
                request: request.clone(),
                now: -500,
                limits: QueueLimits::default(),
                required_next_offset: 3,
                required_insertion_sequence: 3,
                required_last_record_epoch: 1,
            };
            let result = f
                .projection
                .prepare_claim(input, 7, 16 << 20, f.clock.clone(), &job, &f.workers)
                .await;
            if should_fit {
                let prepared = result.unwrap().unwrap();
                assert_eq!(prepared.records().len(), 1);
                assert_eq!(prepared.records()[0].record.work.key, long);
                drop(prepared);
            } else {
                assert!(matches!(result, Err(QueueError::OutcomeUnknown(_))));
            }
            drop(definition);
            drop(job);
            assert_eq!(admission.usage().unwrap(), (0, 0));
        }
        let row=inspect(&f.projection,"SELECT applied_next_offset,(SELECT COUNT(*) FROM q7_requests WHERE domain=1),(SELECT COUNT(*) FROM q7_records WHERE lifecycle=1) FROM q7_queues",vec![],3).await;
        assert_eq!(
            row,
            vec![Value::Integer(3), Value::Integer(0), Value::Integer(0)]
        );
        f.finish().await;
    }
}
