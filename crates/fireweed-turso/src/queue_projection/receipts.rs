//! Immutable commit receipts in one admitted, covered native snapshot.
//! Public orchestration must validate current authoritative coverage afterward.
use super::*;
use fireweed_engine::{
    queue_memory::incoming_headers,
    queue_publication::{
        CommitReceiptProjection, ReceiptLookupProjection, ReceiptOutcomeRef,
        receipt_outcome_heap_bound, view_receipt_outcome,
    },
};

pub struct ReceiptReadInput {
    pub genesis: OwnedQueueLogRecord,
    pub request: ReceiptReadRequest,
    pub limits: QueueLimits,
    pub required_next_offset: u64,
    pub required_insertion_sequence: u64,
    pub required_last_record_epoch: u64,
}
pub struct AdmittedCommitReceipts {
    receipts: Vec<ReceiptLookup>,
    _charges: Vec<OwnedQueueBufferCharge>,
}
impl AdmittedCommitReceipts {
    pub fn receipts(&self) -> &[ReceiptLookup] {
        &self.receipts
    }
}
struct ReceiptWorker {
    projection: Arc<QueueProjection>,
    input: ReceiptReadInput,
    job: Arc<QueueReservation>,
    charges: Vec<OwnedQueueBufferCharge>,
}
impl QueueProjection {
    /// No clock/current-row reevaluation: retained outcomes preserve original
    /// producer order. Requested lookup order and duplicates remain observable.
    pub async fn read_commit_receipts(
        self: &Arc<Self>,
        input: ReceiptReadInput,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<AdmittedCommitReceipts> {
        if !input.genesis.belongs_to(job) {
            return Err(unknown("receipt genesis belongs to another operation"));
        }
        input.request.validate(&input.limits)?;
        let headers = incoming_headers(&input.request)?;
        // All positions own large enums even for Absent/Expired. Include all
        // overlapping raw, borrowed, decoded and guard vectors. Three times
        // each element covers growth plus old/new allocation overlap during a
        // realloc; this bound is acquired before any result Vec can grow.
        type RawReceipt = Option<(u8, Option<Vec<u8>>)>;
        let position_size = std::mem::size_of::<RawReceipt>()
            .checked_add(std::mem::size_of::<ReceiptLookupProjection<'_>>())
            .and_then(|n| n.checked_add(std::mem::size_of::<ReceiptLookup>()))
            .and_then(|n| {
                n.checked_add(std::mem::size_of::<OwnedQueueBufferCharge>().checked_mul(2)?)
            })
            .ok_or(QueueError::TooLarge)?;
        let containers = (input.request.request_ids.len() as u64)
            .checked_mul(position_size as u64)
            .and_then(|n| n.checked_mul(3))
            .ok_or(QueueError::TooLarge)?;
        let capacity = encoded_len(&input.request)?
            .checked_mul(16)
            .and_then(|n| n.checked_add(headers))
            .and_then(|n| n.checked_add(containers))
            .and_then(|n| n.checked_add(65536))
            .ok_or(QueueError::TooLarge)?;
        let mut worker = ReceiptWorker {
            projection: self.clone(),
            input,
            job: job.clone(),
            charges: vec![job.owned_buffer(capacity)?],
        };
        workers
            .run(move || {
                Ok(crate::tx::block_on_owned_apply(async {
                    let projection = worker.projection.clone();
                    let receipts = owned_native_transaction(
                        &projection,
                        TransactionBehavior::Deferred,
                        &mut worker,
                    )
                    .await?;
                    Ok(AdmittedCommitReceipts {
                        receipts,
                        _charges: std::mem::take(&mut worker.charges),
                    })
                }))
            })
            .await?
    }
}
impl NativeTransactionWork for ReceiptWorker {
    type Output = Vec<ReceiptLookup>;
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
                panic!("injected receipt precommit panic");
            }
        }
    }
    async fn run(&mut self, connection: &Connection) -> QueueResult<Self::Output> {
        let snapshot = super::snapshot::read_snapshot(connection, &self.input.genesis, &self.job)
            .await?
            .ok_or_else(|| unknown("receipt projection missing"))?;
        snapshot.validate_point(
            &self.input.genesis,
            &self.input.request.queue,
            (
                self.input.required_next_offset,
                self.input.required_insertion_sequence,
                self.input.required_last_record_epoch,
            ),
        )?;
        // Retain independently charged raw outcomes first. FWQP fixed lengths
        // cannot decide the public Postcard response budget.
        let mut raw = Vec::new();
        for identity in &self.input.request.request_ids {
            let hash = discovery_digest(identity.as_bytes());
            let params = || {
                vec![
                    Value::Integer(snapshot.queue_id),
                    blob(&hash),
                    blob(identity.as_bytes()),
                ]
            };
            let metadata=one(connection,"SELECT CASE WHEN typeof(fingerprint_format)='integer' THEN fingerprint_format ELSE NULL END,CASE WHEN typeof(fingerprint)='blob' AND length(fingerprint)=32 THEN 1 ELSE 0 END,CASE WHEN typeof(expired)='integer' THEN expired ELSE NULL END,CASE WHEN typeof(outcome)='blob' THEN length(outcome) ELSE -1 END FROM q7_requests WHERE queue_id=?1 AND domain=2 AND identity_hash=?2 AND identity=?3",params(),4).await?;
            let Some(metadata) = metadata else {
                raw.push(None);
                continue;
            };
            let format = u8::try_from(integer(&metadata[0])?).map_err(unknown)?;
            if !matches!(format, 3 | 4) || integer(&metadata[1])? != 1 {
                return Err(unknown("invalid stored receipt fingerprint"));
            }
            match integer(&metadata[2])? {
                1 => {
                    raw.push(Some((format, None)));
                    continue;
                }
                0 => {}
                _ => return Err(unknown("invalid stored receipt expiry")),
            }
            let size = u64::try_from(integer(&metadata[3])?).map_err(unknown)?;
            self.charges.push(
                self.job.owned_buffer(
                    size.checked_mul(4)
                        .and_then(|n| n.checked_add(4096))
                        .ok_or(QueueError::TooLarge)?,
                )?,
            );
            let row=one(connection,"SELECT outcome FROM q7_requests WHERE queue_id=?1 AND domain=2 AND identity_hash=?2 AND identity=?3",params(),1).await?.ok_or_else(||unknown("receipt vanished in snapshot"))?;
            let bytes = bytes(&row[0])?;
            if bytes.len() as u64 != size {
                return Err(unknown("receipt length changed in snapshot"));
            }
            raw.push(Some((format, Some(bytes.to_vec()))));
        }
        let mut public = Vec::new();
        for (identity, raw) in self.input.request.request_ids.iter().zip(&raw) {
            public.push(match raw {
                None => ReceiptLookupProjection::Absent,
                Some((_, None)) => ReceiptLookupProjection::Expired,
                Some((format, Some(bytes))) => {
                    let outcome =
                        view_receipt_outcome(2, *format, bytes).map_err(publication_error)?;
                    let ReceiptOutcomeRef::Commit(commit) = outcome else {
                        return Err(unknown("commit domain contains insert outcome"));
                    };
                    self.charges.push(
                        self.job.owned_buffer(
                            receipt_outcome_heap_bound(outcome, bytes.len() as u64)
                                .map_err(publication_error)?,
                        )?,
                    );
                    ReceiptLookupProjection::Found(CommitReceiptProjection {
                        request_id: identity,
                        outcome: commit,
                    })
                }
            });
        }
        let size = encoded_len(&public)?;
        if size > self.input.request.max_bytes {
            return Err(QueueError::TooLarge);
        }
        self.charges.push(
            self.job.owned_buffer(
                size.checked_mul(4)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(QueueError::TooLarge)?,
            )?,
        );
        // One shared DTO projection/conversion; no second outcome semantics in
        // this adapter. Both transient bytes and final headers stay admitted.
        let encoded = encode(&public)?;
        decode(&encoded)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{
        address, apply_group_record, bootstrap, execute_for_test, group, inspect, patch, seed,
    };
    use super::*;
    use fireweed_engine::{
        queue_admission::QueueAdmission, queue_log_record::encode_checked_genesis,
    };
    fn input(
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
        ids: Vec<String>,
        max_bytes: u64,
        head: u64,
    ) -> ReceiptReadInput {
        ReceiptReadInput {
            genesis: encode_checked_genesis(1, &address(), definition, &[31; 32], 1 << 20, job)
                .unwrap(),
            request: ReceiptReadRequest {
                queue: address(),
                request_ids: ids,
                max_bytes,
            },
            limits: QueueLimits::default(),
            required_next_offset: head,
            required_insertion_sequence: 2,
            required_last_record_epoch: 1,
        }
    }
    #[tokio::test]
    async fn original_receipts_survive_deletion_preserve_duplicates_and_fit_exact_public_budget() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("receipts.db");
        let admission = QueueAdmission::new(2, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 2).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        let mut snapshot = seed(&projection, &definition, &job, &workers, true).await;
        let mut changed = group("change");
        changed.work_updates.push(patch("a", 1, Some(33), None));
        apply_group_record(
            &projection,
            &definition,
            &mut snapshot,
            &changed,
            2,
            &job,
            &workers,
        )
        .await
        .unwrap();
        let original = CommitReceipt {
            request_id: "change".into(),
            outcome: GroupOutcome::Committed {
                versions: vec![RecordVersion {
                    collection: None,
                    key: "a".into(),
                    version: 2,
                }],
            },
        };
        let mut deleted = group("delete");
        deleted.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(2),
            change: UnclaimedChange::Delete,
        });
        apply_group_record(
            &projection,
            &definition,
            &mut snapshot,
            &deleted,
            3,
            &job,
            &workers,
        )
        .await
        .unwrap();
        let mut rejected = group("rejected");
        rejected
            .work_updates
            .push(patch("missing", 999, None, None));
        apply_group_record(
            &projection,
            &definition,
            &mut snapshot,
            &rejected,
            4,
            &job,
            &workers,
        )
        .await
        .unwrap();
        execute_for_test(&projection,"UPDATE q7_requests SET expired=1,outcome=zeroblob(8388608) WHERE domain=2 AND identity=?1",vec![blob(b"delete")]).await;
        let ids = vec![
            "change".into(),
            "absent".into(),
            "delete".into(),
            "change".into(),
            "rejected".into(),
        ];
        let expected = vec![
            ReceiptLookup::Found(original.clone()),
            ReceiptLookup::Absent,
            ReceiptLookup::Expired,
            ReceiptLookup::Found(original),
            ReceiptLookup::Found(CommitReceipt {
                request_id: "rejected".into(),
                outcome: GroupOutcome::Rejected(Rejection::NotFound),
            }),
        ];
        let exact = encoded_len(&expected).unwrap();
        let result = projection
            .read_commit_receipts(
                input(&definition, &job, ids.clone(), exact, 5),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(result.receipts(), expected);
        drop(result);
        assert!(matches!(
            projection
                .read_commit_receipts(
                    input(&definition, &job, ids.clone(), exact - 1, 5),
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::TooLarge)
        ));
        let public_one = encoded_len(&vec![expected[0].clone()]).unwrap();
        let stored_size = inspect(
            &projection,
            "SELECT length(outcome) FROM q7_requests WHERE domain=2 AND identity=?1",
            vec![blob(b"change")],
            1,
        )
        .await;
        assert!(
            u64::try_from(integer(&stored_size[0]).unwrap()).unwrap() > public_one,
            "fixed FWQP size must not reject a fitting public response"
        );
        let result = projection
            .read_commit_receipts(
                input(
                    &definition,
                    &job,
                    vec!["change".into()],
                    encoded_len(&vec![expected[0].clone()]).unwrap(),
                    5,
                ),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(result.receipts(), &expected[..1]);
        drop(result);
        drop(projection);
        let projection = QueueProjection::open(&path, &job, &workers).await.unwrap();
        let result = projection
            .read_commit_receipts(input(&definition, &job, ids, exact, 5), &job, &workers)
            .await
            .unwrap();
        assert_eq!(result.receipts(), expected);
        drop(result);
        let mut bad = input(&definition, &job, vec!["change".into()], exact, 5);
        bad.required_next_offset = 4;
        assert!(matches!(
            projection.read_commit_receipts(bad, &job, &workers).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        // A power-of-two growth boundary exposes status-vector headers even
        // when every public result serializes to a single byte. Holding the
        // writer proves rejection precedes native dispatch/snapshot allocation.
        let writer_guard = projection.writer.lock().await;
        for identity in ["x", "delete"] {
            let tight = QueueAdmission::new(1, 1 << 20).unwrap();
            let tight_job = Arc::new(tight.reserve(0).unwrap());
            let tight_definition =
                AdmittedQueueDefinition::from_raw(definition.raw(), &tight_job).unwrap();
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(3),
                projection.read_commit_receipts(
                    input(
                        &tight_definition,
                        &tight_job,
                        vec![identity.into(); 2049],
                        65536,
                        5,
                    ),
                    &tight_job,
                    &workers,
                ),
            )
            .await
            .expect("container admission must precede writer wait");
            assert!(
                matches!(result,Err(QueueError::OutcomeUnknown(message)) if message.contains("admitted buffer capacity exhausted"))
            );
        }
        drop(writer_guard);
        let baseline = admission.usage().unwrap();
        projection
            .panic_after_command
            .store(true, std::sync::atomic::Ordering::Release);
        assert!(matches!(
            projection
                .read_commit_receipts(
                    input(&definition, &job, vec!["change".into()], exact, 5),
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        let healthy = projection
            .read_commit_receipts(
                input(&definition, &job, vec!["change".into()], exact, 5),
                &job,
                &workers,
            )
            .await
            .unwrap();
        drop(healthy);
        assert_eq!(admission.usage().unwrap(), baseline);
        let terminal = Arc::new(tokio::sync::Notify::new());
        let signal = terminal.clone();
        let owned_job = Arc::new(
            admission
                .reserve(0)
                .unwrap()
                .with_terminal_callback(move || signal.notify_one())
                .unwrap(),
        );
        let pause = Arc::new(ApplyPause {
            after_commit: false,
            fail_reply: false,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::watch::channel(false).0,
        });
        *projection.pause.lock().await = Some(pause.clone());
        let caller = tokio::spawn({
            let projection = projection.clone();
            let workers = workers.clone();
            let owned_definition =
                AdmittedQueueDefinition::from_raw(definition.raw(), &owned_job).unwrap();
            let input = input(
                &owned_definition,
                &owned_job,
                vec!["change".into()],
                exact,
                5,
            );
            drop(owned_definition);
            async move {
                projection
                    .read_commit_receipts(input, &owned_job, &workers)
                    .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(30), pause.entered.notified())
            .await
            .unwrap();
        caller.abort();
        assert!(matches!(caller.await,Err(error) if error.is_cancelled()));
        assert_eq!(
            admission.usage().unwrap().0,
            2,
            "started reader owns its reservation"
        );
        pause.release.send(true).unwrap();
        workers.close_and_drain().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(30), terminal.notified())
            .await
            .unwrap();
        assert_eq!(admission.usage().unwrap(), baseline);
    }
    #[tokio::test]
    async fn repeated_large_administrative_before_fields_are_admitted_before_materialization() {
        use fireweed_core::queue_administration::*;
        use fireweed_engine::{
            queue_log_record::encode_checked_publication_record,
            queue_planner::plan_commit_group,
            queue_publication::{
                AdministrativePublicationBuilder, AdministrativePublicationReceipt,
                AdministrativeReceiptOutcome,
            },
        };
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) =
            bootstrap(&directory.path().join("large.db"), &job, &workers).await;
        let mut snapshot = seed(&projection, &definition, &job, &workers, true).await;
        let fields: Fields = (0..5000)
            .map(|n| (format!("meta/tiny/{n}"), FieldValue::Boolean(n % 2 == 0)))
            .collect();
        let mut update = group("large-fields");
        update.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                fields: fields
                    .iter()
                    .map(|(k, v)| (k.clone(), Some(v.clone())))
                    .collect(),
                ..Default::default()
            }),
        });
        apply_group_record(
            &projection,
            &definition,
            &mut snapshot,
            &update,
            2,
            &job,
            &workers,
        )
        .await
        .unwrap();
        let stored = &snapshot.work["a"].record;
        let before = AdministrativeBefore {
            version: stored.version,
            lifecycle: stored.lifecycle,
            available_at: stored.work.available_at,
            eligible_since: stored.eligible_since,
            attempts: stored.attempts,
            lease_expires_at: stored.lease_expires_at,
            fields: fields.clone(),
        };
        let mut histogram = AdministrativeHistogram::default();
        histogram.record(&AdministrativeOutcome::NoChange).unwrap();
        let result = AdministrativeResult {
            histogram,
            items: vec![AdministrativeItemResult {
                key: "a".into(),
                outcome: AdministrativeOutcome::NoChange,
                before: Some(before),
            }],
            clauses: vec![],
        };
        let intent = AdministrativeIntent {
            evaluated_at: 20,
            before_fields: Some(fields.keys().cloned().collect()),
            operation: AdministrativeOperation::Addressed(vec![AdministrativeEntry {
                key: "a".into(),
                expected_version: Some(2),
                precondition: WorkSelector::all(vec![]),
                action: AdministrativeAction::Update {
                    patch: WorkPatch::default(),
                    lifecycle: AdministrativeLifecycle::Keep,
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                    remove_field_prefixes: Default::default(),
                },
            }]),
        };
        let admin_group = group("large-before");
        let queue = address();
        let mut unchanged = snapshot.clone();
        let ordinary =
            plan_commit_group(&group("empty"), &mut unchanged, 20, definition.schema()).unwrap();
        let outcome = AdministrativeReceiptOutcome::Committed {
            versions: &[],
            administration: &result,
        };
        let receipt = AdministrativePublicationReceipt::new(
            &queue,
            &admin_group,
            &intent,
            1 << 20,
            &outcome,
            &job,
        )
        .unwrap();
        let builder = AdministrativePublicationBuilder::new(
            &receipt,
            definition.pin(),
            &snapshot,
            &ordinary,
            0,
            0,
            1 << 20,
            &job,
        )
        .unwrap();
        let frame = builder.finish().unwrap();
        let record = encode_checked_publication_record(1, frame.as_bytes(), 1 << 20, &job).unwrap();
        projection.apply(record, 3, &job, &workers).await.unwrap();
        drop(frame);
        let expected_item = ReceiptLookup::Found(CommitReceipt {
            request_id: "large-before".into(),
            outcome: GroupOutcome::AdministrativeCommitted {
                versions: vec![],
                administration: result,
            },
        });
        let expected = vec![expected_item.clone(), expected_item];
        let exact = encoded_len(&expected).unwrap();
        let ids = vec!["large-before".into(), "large-before".into()];
        let read = projection
            .read_commit_receipts(
                input(&definition, &job, ids.clone(), exact, 4),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(read.receipts(), expected);
        drop(read);
        assert!(matches!(
            projection
                .read_commit_receipts(
                    input(&definition, &job, ids.clone(), exact - 1, 4),
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::TooLarge)
        ));
        let tight = QueueAdmission::new(1, 1 << 20).unwrap();
        let tight_job = Arc::new(tight.reserve(0).unwrap());
        let tight_definition =
            AdmittedQueueDefinition::from_raw(definition.raw(), &tight_job).unwrap();
        assert!(matches!(
            projection
                .read_commit_receipts(
                    input(&tight_definition, &tight_job, ids, exact, 4),
                    &tight_job,
                    &workers
                )
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        let healthy = projection
            .read_commit_receipts(
                input(&definition, &job, vec!["large-before".into()], exact, 4),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(healthy.receipts(), &expected[..1]);
        drop(healthy);
        workers.close_and_drain().await.unwrap();
    }
}
