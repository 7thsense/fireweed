//! Full producer qualification against an independent cold history/snapshot fold.
use super::*;
use fireweed_core::queue_contract::*;
use fireweed_engine::{
    queue_planner::{RecordSnapshot, combine_deltas, plan_commit_group},
    queue_publication::{PublicationReceipt, encode_publication_pinned},
    queue_source_index::QueueSourceCompileLimits,
};
use std::{collections::BTreeMap, ops::Bound};

fn limits() -> QueueNativeSourcePreparationLimits {
    QueueNativeSourcePreparationLimits {
        compile: QueueSourceCompileLimits {
            edits: 1000,
            input_bytes: 1 << 20,
        },
        read: QueueNativeSourceReadLimits {
            page: QueueNativeScanLimits {
                rows: 256,
                output_bytes: 1 << 20,
                work: work(),
            },
            range_pages: 8,
        },
        prepare: QueueNativeEditLimits {
            edits: 1000,
            input_bytes: 1 << 20,
            work: work(),
        },
    }
}
fn budget(job: &Arc<QueueReservation>, calls: u64) -> Arc<QueueNativeSourceBudget> {
    QueueNativeSourceBudget::new(
        QueueNativeSourceWork {
            semantic: QueueNativeSemanticWork {
                pages: work().pages * calls,
                hydrated_bytes: work().hydrated_bytes * calls,
            },
            record_attempts: 0,
            hydration_reads: 0,
            hydration_body_bytes: 0,
        },
        job,
    )
    .unwrap()
}
fn group(id: &str) -> CommitGroup {
    CommitGroup {
        request_id: id.into(),
        administration: None,
        claimed_updates: vec![],
        work_updates: vec![],
        work_inserts: vec![],
        state_updates: vec![],
    }
}
fn ordinary(
    acquired: &QueueNativeAcquired,
    definition: &AdmittedQueueDefinition,
    before: &RecordSnapshot,
    groups: &[CommitGroup],
    job: &Arc<QueueReservation>,
) -> (OwnedQueueLogRecord, RecordSnapshot) {
    let mut after = before.clone();
    let mut deltas = Vec::new();
    let address = QueueAddress {
        tenant: "tenant".into(),
        queue: "producer".into(),
    };
    for group in groups {
        after.unique_probes_loaded.extend(
            fireweed_engine::queue_planner::required_unique_probes(
                group,
                &after,
                10,
                definition.schema(),
            )
            .unwrap(),
        );
        let delta = plan_commit_group(group, &mut after, 10, definition.schema()).unwrap();
        if matches!(group.request_id.as_str(), "rejected" | "source-rejected") {
            assert!(
                matches!(delta.receipt.outcome, GroupOutcome::Rejected(_)),
                "intentional rejection: {:?}",
                delta.receipt.outcome
            );
        } else {
            assert!(
                matches!(delta.receipt.outcome, GroupOutcome::Committed { .. }),
                "{}: {:?}",
                group.request_id,
                delta.receipt.outcome
            );
        }
        deltas.push(delta);
    }
    let mut receipts: Vec<_> = groups
        .iter()
        .zip(&deltas)
        .map(|(group, delta)| PublicationReceipt::commit(&address, group, &delta.receipt).unwrap())
        .collect();
    receipts.sort_by(|a, b| a.identity().cmp(&b.identity()));
    let combined = combine_deltas(&deltas, before.next_sequence).unwrap();
    let frame = encode_publication_pinned(
        &address,
        definition.pin(),
        before,
        &combined,
        &receipts,
        1 << 20,
        job,
    )
    .unwrap();
    (
        fireweed_engine::queue_log_record::encode_checked_publication_record(
            acquired.as_source().head().epoch(),
            frame.as_bytes(),
            1 << 20,
            job,
        )
        .unwrap(),
        after,
    )
}
// Exercise the real exact-cut source reader as a planner input, independently
// of the compiler that later publishes facts. The reference remains the cold
// full-image shared planner and no projection/cache rows are consulted.
async fn check_source_group(
    log: &QueueNativeLog,
    acquired: &QueueNativeAcquired,
    definition: &AdmittedQueueDefinition,
    snapshot: &RecordSnapshot,
    group: &CommitGroup,
    ledger: Arc<QueueNativeSourceBudget>,
    job: &Arc<QueueReservation>,
) {
    use fireweed_engine::queue_source_index::{QueueSourceReader, load_group_preimage};
    struct Clock;
    impl fireweed_engine::Clock for Clock {
        fn now(&self) -> fireweed_core::UtcTimestamp {
            fireweed_core::UtcTimestamp {
                seconds: 0,
                nanoseconds: 10,
            }
        }
    }
    let reader = QueueNativeSourceReader::new(
        acquired.as_source(),
        log,
        job,
        Arc::clone(&ledger),
        limits().read,
    )
    .unwrap()
    .with_records(super::record::record_limits())
    .unwrap();
    let queue = QueueAddress {
        tenant: "tenant".into(),
        queue: "producer".into(),
    };
    let before = acquired.as_source().head();
    let plan = load_group_preimage(
        &reader,
        &queue,
        group,
        definition,
        &QueueLimits::default(),
        job,
    )
    .await
    .unwrap()
    .plan(&Clock)
    .unwrap();
    let mut image = snapshot.clone();
    image.unique_probes_loaded.extend(
        fireweed_engine::queue_planner::prospective_unique_probes(
            group,
            &image,
            definition.schema(),
        )
        .unwrap(),
    );
    let expected = plan_commit_group(group, &mut image, 10, definition.schema()).unwrap();
    assert_eq!(plan.receipt(), &expected.receipt);
    assert!(plan.cut().same_cut(&reader.cut().unwrap()));
    let publication = plan
        .encode(
            fireweed_engine::queue_source_index::QueueSourceGroupEncodingLimits {
                frame_bytes: 1 << 20,
                record_bytes: 1 << 20,
            },
        )
        .unwrap();
    let (cold_record, _) = ordinary(
        acquired,
        definition,
        snapshot,
        std::slice::from_ref(group),
        job,
    );
    assert_eq!(
        publication
            .record_for(&reader.cut().unwrap(), definition, job)
            .unwrap()
            .bytes(),
        cold_record.bytes(),
    );
    // Independently captured read bundle at the same numeric head. Reusing the
    // sealed writer here is test construction only; preparation must reject its
    // distinct semantic owner before reaching that writer or any source read.
    let namespace = acquired
        .as_source()
        .retained_namespace(&queue, job)
        .unwrap();
    let foreign_cut = log.capture(&namespace).await.unwrap();
    let foreign_bundle = QueueNativeAcquired {
        writer: QueueNativeWriter {
            writer: acquired.writer.writer.clone(),
            namespace: acquired.writer.namespace.clone(),
            epoch: acquired.writer.epoch,
            next_offset: acquired.writer.next_offset,
        },
        source: QueueNativeCaptured {
            log_identity: acquired.source.log_identity.clone(),
            namespace,
            cut: foreign_cut,
            semantic_identity: QueueNativeCaptured::identity_charge(job).unwrap(),
        },
    };
    assert_eq!(
        foreign_bundle.as_source().head(),
        acquired.as_source().head()
    );
    let mut changed_schema = definition.schema().clone();
    changed_schema.max_attempts += 1;
    let changed_definition = AdmittedQueueDefinition::from_raw(
        &fireweed_engine::queue_schema_codec::encode_definition(&changed_schema).unwrap(),
        job,
    )
    .unwrap();
    let foreign_admission = QueueAdmission::new(1, 1 << 20).unwrap();
    let foreign_job = Arc::new(foreign_admission.reserve(0).unwrap());
    let directory = tempfile::tempdir().unwrap();
    let foreign_log =
        QueueNativeLog::local(directory.path(), "foreign-binding".into(), options(1 << 20))
            .unwrap();
    for (target, target_log, target_definition, target_job, message) in [
        (
            &foreign_bundle,
            log,
            definition,
            job,
            "planned publication cut, definition or operation differs",
        ),
        (
            acquired,
            log,
            &changed_definition,
            job,
            "planned publication cut, definition or operation differs",
        ),
        (
            acquired,
            log,
            definition,
            &foreign_job,
            "semantic namespace, cut or operation differs",
        ),
        (
            acquired,
            &foreign_log,
            definition,
            job,
            "semantic namespace, cut or operation differs",
        ),
    ] {
        let probe = budget(job, 1);
        let error = target
            .prepare_group_publication(
                target_log,
                &publication,
                target_definition,
                probe.clone(),
                limits(),
                target_job,
            )
            .await
            .err()
            .unwrap();
        assert!(
            matches!(error, QueueError::OutcomeUnknown(ref actual) if actual.contains(message)),
            "{error:?}"
        );
        // Every compiler source read requires a ledger grant. The full original
        // grant remaining proves these failures did no compilation/source work.
        probe.grant(work()).unwrap();
        assert!(
            probe
                .grant(QueueNativeSemanticWork {
                    pages: 1,
                    hydrated_bytes: 1
                })
                .is_err()
        );
        assert_eq!(
            log.capture(&acquired.source.namespace)
                .await
                .unwrap()
                .head(),
            before
        );
    }
    foreign_log.shutdown().await.unwrap();
    let prepared = acquired
        .prepare_group_publication(log, &publication, definition, ledger, limits(), job)
        .await
        .unwrap();
    drop(prepared);
    assert_eq!(
        log.capture(&acquired.source.namespace)
            .await
            .unwrap()
            .head(),
        before
    );
}

async fn accept(
    log: &QueueNativeLog,
    acquired: QueueNativeAcquired,
    record: OwnedQueueLogRecord,
    definition: &AdmittedQueueDefinition,
    state: (&mut Vec<OwnedQueueLogRecord>, &RecordSnapshot),
    ledger: Arc<QueueNativeSourceBudget>,
    job: &Arc<QueueReservation>,
) -> QueueNativeAcquired {
    let original = acquired
        .compile_and_prepare(log, &record, definition, ledger, limits(), job)
        .await
        .unwrap();
    assert!(matches!(
        original.submit(log).await.unwrap(),
        QueueNativeSubmit::Stored(_)
    ));
    // Resolve against the retained original token before adopting another cut.
    assert_eq!(
        original.resolve(log, 1).await.unwrap(),
        QueueNativeResolution::DurableContent
    );
    verify_published(log, &acquired, record, definition, state, job).await
}

async fn verify_published(
    log: &QueueNativeLog,
    acquired: &QueueNativeAcquired,
    record: OwnedQueueLogRecord,
    definition: &AdmittedQueueDefinition,
    state: (&mut Vec<OwnedQueueLogRecord>, &RecordSnapshot),
    job: &Arc<QueueReservation>,
) -> QueueNativeAcquired {
    let (history, snapshot) = state;
    history.push(record);
    let current = fresh(log, acquired, "producer", job).await;
    assert_eq!(
        current.as_source().head().bounds(),
        (0, history.len() as u64)
    );
    assert!(
        current
            .as_source()
            .read(log, history.len() as u64, 1 << 20, job)
            .await
            .unwrap()
            .is_none()
    );
    let page = current
        .as_source()
        .semantic_scan(
            log,
            Bound::Unbounded,
            Bound::Unbounded,
            limits().read.page,
            job,
        )
        .await
        .unwrap();
    assert_eq!(page.status(), QueueNativeScanStatus::Complete);
    let mut persisted = Vec::new();
    for (offset, expected) in history.iter().enumerate() {
        let record = current
            .as_source()
            .read(log, offset as u64, 1 << 20, job)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.bytes(), expected.bytes());
        persisted.push(record);
    }
    let actual: BTreeMap<_, _> = page
        .rows()
        .map(|r| (r.key().to_vec(), r.value().to_vec()))
        .collect();
    assert_eq!(
        actual,
        fireweed_conformance::queue_source_oracle::fold(&persisted, snapshot, definition.schema())
    );
    // Reopen the namespace independently: equal values cannot depend on a local carrier.
    let ns = log
        .open(
            &QueueAddress {
                tenant: "tenant".into(),
                queue: "producer".into(),
            },
            job,
        )
        .await
        .unwrap()
        .unwrap();
    let reopened = log.acquire_bundle(ns, job).await.unwrap();
    assert_eq!(
        reopened.as_source().head().bounds(),
        (0, history.len() as u64)
    );
    assert!(
        reopened
            .as_source()
            .read(log, history.len() as u64, 1 << 20, job)
            .await
            .unwrap()
            .is_none()
    );
    let page = reopened
        .as_source()
        .semantic_scan(
            log,
            Bound::Unbounded,
            Bound::Unbounded,
            limits().read.page,
            job,
        )
        .await
        .unwrap();
    assert_eq!(page.status(), QueueNativeScanStatus::Complete);
    assert_eq!(
        page.rows()
            .map(|r| (r.key().to_vec(), r.value().to_vec()))
            .collect::<BTreeMap<_, _>>(),
        actual
    );
    reopened
}
async fn accept_source_group(
    log: &QueueNativeLog,
    acquired: QueueNativeAcquired,
    definition: &AdmittedQueueDefinition,
    group: &CommitGroup,
    state: (&mut Vec<OwnedQueueLogRecord>, &RecordSnapshot),
    ledger: Arc<QueueNativeSourceBudget>,
    job: &Arc<QueueReservation>,
) -> (QueueNativeAcquired, RecordSnapshot) {
    use fireweed_engine::queue_source_index::{
        QueueSourceGroupEncodingLimits, load_group_preimage,
    };
    struct Clock;
    impl fireweed_engine::Clock for Clock {
        fn now(&self) -> fireweed_core::UtcTimestamp {
            fireweed_core::UtcTimestamp {
                seconds: 0,
                nanoseconds: 10,
            }
        }
    }
    let (history, before) = state;
    let (expected, after) = ordinary(
        &acquired,
        definition,
        before,
        std::slice::from_ref(group),
        job,
    );
    let reader = QueueNativeSourceReader::new(
        acquired.as_source(),
        log,
        job,
        ledger.clone(),
        limits().read,
    )
    .unwrap()
    .with_records(super::record::record_limits())
    .unwrap();
    let queue = QueueAddress {
        tenant: "tenant".into(),
        queue: "producer".into(),
    };
    let queue_limits = QueueLimits::default();
    let publication = load_group_preimage(&reader, &queue, group, definition, &queue_limits, job)
        .await
        .unwrap()
        .plan(&Clock)
        .unwrap()
        .encode(QueueSourceGroupEncodingLimits {
            frame_bytes: 1 << 20,
            record_bytes: 1 << 20,
        })
        .unwrap();
    let prepared = acquired
        .prepare_group_publication(log, &publication, definition, ledger, limits(), job)
        .await
        .unwrap();
    assert!(matches!(
        prepared.submit(log).await.unwrap(),
        QueueNativeSubmit::Stored(_)
    ));
    assert_eq!(
        prepared.resolve(log, 1).await.unwrap(),
        QueueNativeResolution::DurableContent
    );
    let current =
        verify_published(log, &acquired, expected, definition, (history, &after), job).await;
    (current, after)
}

async fn producer_contract(log: QueueNativeLog) {
    let admission = QueueAdmission::new(2, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let collection = CollectionDefinition {
        fields: Default::default(),
        indexes: Default::default(),
        unique_indexes: Default::default(),
        additional_field_prefixes: Default::default(),
    };
    let mut work_definition = collection.clone();
    work_definition.fields = BTreeMap::from([
        ("tag".into(), FieldType::Integer),
        ("kind".into(), FieldType::Text),
    ]);
    work_definition
        .indexes
        .insert("by_tag".into(), vec!["tag".into()]);
    work_definition.unique_indexes.insert("by_tag".into());
    let schema = QueueSchema {
        work: work_definition,
        state: BTreeMap::from([("workflow".into(), collection)]),
        progress_bound_nanos: 1,
        max_attempts: 3,
    };
    let (mut acquired, genesis) = bundle_for_schema(&log, "producer", &job, &schema).await;
    let genesis_view = genesis.view().unwrap();
    let QueueLogPayload::Genesis(view) = genesis_view.payload() else {
        panic!()
    };
    let definition = view.validate_definition(&job).unwrap();
    let ledger = budget(&job, 10_000);
    let mut history = Vec::new();
    let mut snapshot = RecordSnapshot::default();
    acquired = accept(
        &log,
        acquired,
        genesis,
        &definition,
        (&mut history, &snapshot),
        ledger.clone(),
        &job,
    )
    .await;
    let (empty, after) = ordinary(
        &acquired,
        &definition,
        &snapshot,
        &[group("receipt-only")],
        &job,
    );
    snapshot = after;
    acquired = accept(
        &log,
        acquired,
        empty,
        &definition,
        (&mut history, &snapshot),
        ledger.clone(),
        &job,
    )
    .await;
    let mut seed = group("seed");
    seed.work_inserts.push(NewWork {
        key: "claim".into(),
        priority: -1,
        available_at: 0,
        fields: Fields::from([
            ("tag".into(), FieldValue::Integer(7)),
            ("kind".into(), FieldValue::Text("active".into())),
        ]),
        body: vec![0, 1, 2].into(),
    });
    check_source_group(
        &log,
        &acquired,
        &definition,
        &snapshot,
        &seed,
        ledger.clone(),
        &job,
    )
    .await;
    (acquired, snapshot) = accept_source_group(
        &log,
        acquired,
        &definition,
        &seed,
        (&mut history, &snapshot),
        ledger.clone(),
        &job,
    )
    .await;
    let (record, after) = claim_record(&acquired, &definition, &snapshot, &job);
    snapshot = after;
    acquired = accept(
        &log,
        acquired,
        record,
        &definition,
        (&mut history, &snapshot),
        ledger.clone(),
        &job,
    )
    .await;
    let mut release = group("release");
    release.claimed_updates.push(ClaimedUpdate {
        key: "claim".into(),
        lease: LeaseCredential { token: vec![7; 32] },
        expected_version: 2,
        patch: WorkPatch::default(),
        disposition: Disposition::Complete,
    });
    let mut rejected = group("rejected");
    rejected.work_updates.push(UnclaimedUpdate {
        key: "absent".into(),
        expect: StateExpect::Version(99),
        change: UnclaimedChange::Delete,
    });
    check_source_group(
        &log,
        &acquired,
        &definition,
        &snapshot,
        &release,
        ledger.clone(),
        &job,
    )
    .await;
    check_source_group(
        &log,
        &acquired,
        &definition,
        &snapshot,
        &rejected,
        ledger.clone(),
        &job,
    )
    .await;
    let mut deletion = release.clone();
    deletion.request_id = "complete-field-range".into();
    deletion.claimed_updates[0].disposition = Disposition::Delete;
    let (range_record, _) = ordinary(&acquired, &definition, &snapshot, &[deletion], &job);
    let (record, after) = ordinary(
        &acquired,
        &definition,
        &snapshot,
        &[release, rejected, group("zero-commands")],
        &job,
    );
    let generous = acquired
        .compile_and_prepare(
            &log,
            &range_record,
            &definition,
            ledger.clone(),
            limits(),
            &job,
        )
        .await
        .unwrap();
    drop(generous);
    let mut incomplete = limits();
    incomplete.read.page.rows = 1;
    incomplete.read.range_pages = 1;
    let baseline = admission.usage().unwrap();
    let error = acquired
        .compile_and_prepare(
            &log,
            &range_record,
            &definition,
            ledger.clone(),
            incomplete,
            &job,
        )
        .await
        .err()
        .unwrap();
    assert!(
        matches!(error, QueueError::OutcomeUnknown(message) if message.contains("source range page admission exhausted"))
    );
    assert_eq!(admission.usage().unwrap(), baseline);
    assert_eq!(
        log.capture(&acquired.source.namespace)
            .await
            .unwrap()
            .head(),
        acquired.as_source().head()
    );
    drop(range_record);
    snapshot = after;
    acquired = accept(
        &log,
        acquired,
        record,
        &definition,
        (&mut history, &snapshot),
        ledger.clone(),
        &job,
    )
    .await;
    assert_eq!(
        snapshot.work["claim"].record.lifecycle,
        Lifecycle::Completed
    );
    let (record, after) = fp4_record(&acquired, &definition, &snapshot, &job);
    snapshot = after;
    acquired = accept(
        &log,
        acquired,
        record,
        &definition,
        (&mut history, &snapshot),
        ledger.clone(),
        &job,
    )
    .await;
    assert_eq!(snapshot.work["claim"].record.lifecycle, Lifecycle::Pending);
    assert_eq!(
        snapshot.state[&("workflow".into(), "run".into())].version,
        1
    );

    let mut atomic = group("source-preview-state-and-next");
    atomic.state_updates.push(StateUpdate {
        collection: "workflow".into(),
        key: "run".into(),
        expect: StateExpect::Version(1),
        change: StateChange::Put {
            fields: Fields::new(),
            value: vec![8, 9].into(),
        },
    });
    atomic.work_inserts.push(NewWork {
        key: "next".into(),
        priority: 1,
        available_at: 0,
        fields: Fields::from([("tag".into(), FieldValue::Integer(99))]),
        body: vec![4].into(),
    });
    check_source_group(
        &log,
        &acquired,
        &definition,
        &snapshot,
        &atomic,
        ledger.clone(),
        &job,
    )
    .await;

    (acquired, snapshot) = accept_source_group(
        &log,
        acquired,
        &definition,
        &atomic,
        (&mut history, &snapshot),
        ledger.clone(),
        &job,
    )
    .await;
    let mut source_rejected = group("source-rejected");
    source_rejected.work_updates.push(UnclaimedUpdate {
        key: "absent".into(),
        expect: StateExpect::Version(99),
        change: UnclaimedChange::Delete,
    });
    (acquired, snapshot) = accept_source_group(
        &log,
        acquired,
        &definition,
        &source_rejected,
        (&mut history, &snapshot),
        ledger.clone(),
        &job,
    )
    .await;

    for duplicate in [false, true] {
        let id = if duplicate {
            "receipt-only"
        } else {
            "late-quota"
        };
        let (proposed, _) = ordinary(&acquired, &definition, &snapshot, &[group(id)], &job);
        let mut attempt = limits();
        if !duplicate {
            attempt.compile.input_bytes = 1;
        }
        let error = acquired
            .compile_and_prepare(
                &log,
                &proposed,
                &definition,
                budget(&job, 1000),
                attempt,
                &job,
            )
            .await
            .err()
            .unwrap();
        let expected = if duplicate {
            "duplicate source original identity"
        } else {
            "source edit output exceeds admitted limits"
        };
        assert!(
            matches!(error, QueueError::OutcomeUnknown(message) if message.contains(expected)),
            "intended {expected}"
        );
        assert_eq!(
            log.capture(&acquired.source.namespace)
                .await
                .unwrap()
                .head(),
            acquired.as_source().head()
        );
    }
    // Deliberately forge an incomplete source map through the test-only raw-edit
    // escape hatch. A real producer must still check the fourth aggregate even
    // for a receipt-only record, before it can return a prepared publication.
    let (poison, _) = ordinary(
        &acquired,
        &definition,
        &snapshot,
        &[group("poison-map")],
        &job,
    );
    let fourth =
        fireweed_engine::queue_source_index::QueueSourceKey::LifecycleCount(Lifecycle::Failed)
            .encode()
            .unwrap();
    let forged = acquired
        .prepare_with_edits(
            &log,
            &poison,
            "test-only-incomplete-source",
            &[QueueNativeSemanticEdit {
                key: &fourth,
                value: None,
            }],
            limits().prepare,
            &job,
        )
        .await
        .unwrap();
    assert!(matches!(
        forged.submit(&log).await.unwrap(),
        QueueNativeSubmit::Stored(_)
    ));
    acquired = fresh(&log, &acquired, "producer", &job).await;
    let (proposed, _) = ordinary(
        &acquired,
        &definition,
        &snapshot,
        &[group("after-missing-fourth")],
        &job,
    );
    let error = acquired
        .compile_and_prepare(
            &log,
            &proposed,
            &definition,
            budget(&job, 1000),
            limits(),
            &job,
        )
        .await
        .err()
        .unwrap();
    assert!(
        matches!(error, QueueError::OutcomeUnknown(message) if message.contains("source lifecycle count missing"))
    );
    assert_eq!(
        log.capture(&acquired.source.namespace)
            .await
            .unwrap()
            .head(),
        acquired.as_source().head()
    );
    drop((forged, poison, proposed));
    drop((acquired, history, definition, ledger));
    log.shutdown().await.unwrap();
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
}
fn claim_record(
    acquired: &QueueNativeAcquired,
    definition: &AdmittedQueueDefinition,
    before: &RecordSnapshot,
    job: &Arc<QueueReservation>,
) -> (OwnedQueueLogRecord, RecordSnapshot) {
    use fireweed_engine::{
        queue_body::ResolvedBody,
        queue_fields::ResolvedFields,
        queue_planner::{CommitDelta, RecordIdentity},
    };
    let claimed = fireweed_engine::queue_claim::plan_claim(
        before.work["claim"].clone(),
        vec![7; 32],
        fireweed_engine::queue_claim::ClaimContext {
            now: 10,
            eligible_at: 0,
            expires_at: 1_000_010,
            insertion_head: before.next_sequence,
        },
    )
    .unwrap();
    let version = claimed.work.record.version;
    let versions = vec![RecordVersion {
        collection: None,
        key: "claim".into(),
        version,
    }];
    let mut after = before.clone();
    after.versions.insert(
        RecordIdentity {
            collection: None,
            key: "claim".into(),
        },
        version,
    );
    after.work.insert("claim".into(), claimed.work.clone());
    let delta = CommitDelta {
        work: vec![claimed.work],
        work_fields: BTreeMap::from([("claim".into(), ResolvedFields::Preserve)]),
        work_bodies: BTreeMap::from([("claim".into(), ResolvedBody::Preserve)]),
        deleted_work: vec![],
        state: vec![],
        deleted_state: vec![],
        versions: versions.clone(),
        next_sequence: before.next_sequence,
        receipt: CommitReceipt {
            request_id: String::new(),
            outcome: GroupOutcome::Committed { versions },
        },
    };
    let request = ClaimRequest {
        queue: QueueAddress {
            tenant: "tenant".into(),
            queue: "producer".into(),
        },
        request_id: "nonempty-claim".into(),
        filter: Filter::default(),
        order: ClaimOrder::PriorityAscending,
        eligible_at: 0,
        lease_nanos: 1_000_000,
        max_records: 1,
        max_bytes: 1 << 20,
        fields: Some(Default::default()),
        include_body: false,
    };
    let receipt = fireweed_engine::queue_claim_receipt::encode_claim_receipt(
        &[fireweed_engine::queue_claim_receipt::ClaimLease {
            key: "claim".into(),
            token: vec![7; 32],
            version,
        }],
        1 << 20,
        job,
    )
    .unwrap();
    let commands = encode_publication_pinned(
        &request.queue,
        definition.pin(),
        before,
        &delta,
        &[],
        1 << 20,
        job,
    )
    .unwrap();
    let publication = fireweed_engine::queue_claim_publication::encode_claim_publication(
        fireweed_engine::queue_claim_publication::ClaimPublicationInput {
            request: &request,
            definition,
            commands: commands.as_bytes(),
            receipt: &receipt,
            claimed_at: 10,
            limits: &QueueLimits::default(),
        },
        1 << 20,
        job,
    )
    .unwrap();
    (
        fireweed_engine::queue_log_record::encode_checked_claim_record(
            acquired.as_source().head().epoch(),
            &publication,
            1 << 20,
            job,
        )
        .unwrap(),
        after,
    )
}
#[tokio::test]
async fn source_bridge_complete_producer_matches_cold_oracle_local() {
    let dir = tempfile::tempdir().unwrap();
    producer_contract(
        QueueNativeLog::local(dir.path(), "compiled-producer".into(), options(1 << 20)).unwrap(),
    )
    .await;
}
#[tokio::test]
async fn source_bridge_complete_producer_matches_cold_oracle_real_s3() {
    use object_log::BlobStore;
    let env = crate::shared_s3_test_env();
    for disabled in [false, true] {
        let prefix = format!(
            "native-compiled-producer/{}/{disabled}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let log = QueueNativeLog::s3(
            QueueNativeS3Connection {
                endpoint: &env.endpoint,
                region: &env.region,
                bucket: &env.bucket,
                access_key: &env.access_key,
                secret_key: &env.secret_key,
            },
            prefix.clone(),
            options(1 << 20),
            QueueNativeS3TransportOptions {
                disable_payload_signing: disabled,
                ..Default::default()
            },
        )
        .unwrap();
        producer_contract(log).await;
        let cleanup = object_log::S3BlobStore::new_owned_with_options(
            &env.endpoint,
            &env.region,
            &env.bucket,
            &env.access_key,
            &env.secret_key,
            object_log::S3ClientOptions::default(),
        )
        .unwrap();
        for key in cleanup.list(&prefix).await.unwrap() {
            cleanup.delete(&key).await.unwrap();
        }
    }
}

#[tokio::test]
async fn source_bridge_genesis_scope_limits_and_nonrefunding_preparation_work() {
    let dir = tempfile::tempdir().unwrap();
    let log =
        QueueNativeLog::local(dir.path(), "bridge-admission".into(), options(1 << 20)).unwrap();
    let admission = QueueAdmission::new(2, 32 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let foreign = Arc::new(admission.reserve(0).unwrap());
    let (acquired, record) = bundle(&log, "producer", &job).await;
    let view = record.view().unwrap();
    let QueueLogPayload::Genesis(genesis) = view.payload() else {
        panic!()
    };
    let definition = genesis.validate_definition(&job).unwrap();
    let foreign_definition = genesis.validate_definition(&foreign).unwrap();
    let ledger = budget(&job, 1);
    let baseline = admission.usage().unwrap();
    assert!(
        acquired
            .compile_and_prepare(
                &log,
                &record,
                &foreign_definition,
                ledger.clone(),
                limits(),
                &job
            )
            .await
            .is_err()
    );
    assert!(
        acquired
            .compile_and_prepare(
                &log,
                &record,
                &definition,
                budget(&foreign, 1),
                limits(),
                &job
            )
            .await
            .is_err()
    );
    for field in 0..12 {
        let mut short = limits();
        match field {
            0 => short.compile.edits = 0,
            1 => short.compile.input_bytes = 0,
            2 => short.read.page.rows = 0,
            3 => short.read.page.output_bytes = 0,
            4 => short.read.page.work.pages = 0,
            5 => short.read.page.work.hydrated_bytes = 0,
            6 => short.read.range_pages = 0,
            7 => short.prepare.edits = 0,
            8 => short.prepare.input_bytes = 0,
            9 => short.prepare.work.pages = 0,
            10 => short.prepare.work.hydrated_bytes = 0,
            11 => short.compile.edits = 5, // sixth mandatory fact fails late
            _ => unreachable!(),
        }
        assert!(
            acquired
                .compile_and_prepare(&log, &record, &definition, ledger.clone(), short, &job)
                .await
                .is_err(),
            "invalid limit {field}"
        );
        assert_eq!(admission.usage().unwrap(), baseline);
    }
    assert_eq!(
        log.capture(&acquired.source.namespace)
            .await
            .unwrap()
            .head()
            .bounds()
            .1,
        0
    );
    let original = acquired
        .compile_and_prepare(&log, &record, &definition, ledger.clone(), limits(), &job)
        .await
        .unwrap();
    // Preparation consumes the only grant even though no submission occurred.
    let retained = admission.usage().unwrap();
    assert!(
        acquired
            .compile_and_prepare(&log, &record, &definition, ledger.clone(), limits(), &job)
            .await
            .is_err()
    );
    assert_eq!(admission.usage().unwrap(), retained);
    assert_eq!(
        log.capture(&acquired.source.namespace)
            .await
            .unwrap()
            .head()
            .bounds()
            .1,
        0
    );
    assert!(matches!(
        original.submit(&log).await.unwrap(),
        QueueNativeSubmit::Stored(_)
    ));
    drop((
        ledger,
        original,
        acquired,
        record,
        definition,
        foreign_definition,
        job,
        foreign,
    ));
    log.shutdown().await.unwrap();
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

fn fp4_record(
    acquired: &QueueNativeAcquired,
    definition: &AdmittedQueueDefinition,
    before: &RecordSnapshot,
    job: &Arc<QueueReservation>,
) -> (OwnedQueueLogRecord, RecordSnapshot) {
    use fireweed_core::queue_administration::*;
    use fireweed_engine::queue_publication::*;
    let patch = WorkPatch {
        priority: Some(17),
        ..Default::default()
    };
    let mut request = group("fp4-admin-plan");
    request.work_updates.push(UnclaimedUpdate {
        key: "claim".into(),
        expect: StateExpect::Version(before.work["claim"].record.version),
        change: UnclaimedChange::Administrative {
            patch: patch.clone(),
            lifecycle: AdministrativeLifecycle::Pending,
            lease_guard: AdministrativeLeaseGuard::InvalidateActive,
        },
    });
    let admin = plan_commit_group(&request, &mut before.clone(), 10, definition.schema()).unwrap();
    let mut logical = group("fp4-state-and-work");
    logical.state_updates.push(StateUpdate {
        collection: "workflow".into(),
        key: "run".into(),
        expect: StateExpect::Absent,
        change: StateChange::Put {
            fields: Fields::new(),
            value: vec![9, 0].into(),
        },
    });
    let mut after = before.clone();
    let ordinary = plan_commit_group(&logical, &mut after, 10, definition.schema()).unwrap();
    let GroupOutcome::Committed { versions } = &ordinary.receipt.outcome else {
        panic!()
    };
    for row in &admin.work {
        after.work.insert(row.record.work.key.clone(), row.clone());
    }
    for version in &admin.versions {
        after.versions.insert(
            fireweed_engine::queue_planner::RecordIdentity {
                collection: version.collection.clone(),
                key: version.key.clone(),
            },
            version.version,
        );
    }
    let intent = AdministrativeIntent {
        evaluated_at: 10,
        before_fields: None,
        operation: AdministrativeOperation::SelectFirst(vec![AdministrativeClause {
            id: "all".into(),
            selector: WorkSelector::all(vec![]),
            precondition: WorkSelector::all(vec![]),
            action: AdministrativeAction::Update {
                patch,
                lifecycle: AdministrativeLifecycle::Pending,
                lease_guard: AdministrativeLeaseGuard::InvalidateActive,
                remove_field_prefixes: Default::default(),
            },
        }]),
    };
    logical.administration = Some(AdministrativeCommit {
        intent: intent.clone(),
        max_result_bytes: 1 << 20,
    });
    let mut histogram = AdministrativeHistogram::default();
    histogram
        .record(&AdministrativeOutcome::Changed {
            version: admin.versions[0].version,
        })
        .unwrap();
    let result = AdministrativeResult {
        histogram: histogram.clone(),
        items: vec![],
        clauses: vec![AdministrativeClauseResult {
            id: "all".into(),
            histogram,
        }],
    };
    let outcome = AdministrativeReceiptOutcome::Committed {
        versions,
        administration: &result,
    };
    let address = QueueAddress {
        tenant: "tenant".into(),
        queue: "producer".into(),
    };
    let receipt =
        AdministrativePublicationReceipt::new(&address, &logical, &intent, 1 << 20, &outcome, job)
            .unwrap();
    let fragment = encode_administrative_work_fragment(before, &admin, job).unwrap();
    let frame = encode_publication_pinned(
        &address,
        definition.pin(),
        before,
        &ordinary,
        &[],
        1 << 20,
        job,
    )
    .unwrap();
    let view = view_publication(frame.as_bytes()).unwrap();
    let commands = view
        .command_fragments()
        .collect::<PublicationResult<Vec<_>>>()
        .unwrap();
    let command_bytes = fragment.as_bytes().len() as u64
        + commands
            .iter()
            .map(|c| c.as_bytes().len() as u64)
            .sum::<u64>();
    let mut builder = AdministrativePublicationBuilder::new(
        &receipt,
        definition.pin(),
        before,
        &ordinary,
        1 + commands.len() as u64,
        command_bytes,
        1 << 20,
        job,
    )
    .unwrap();
    builder.append_administrative(fragment.as_bytes()).unwrap();
    for command in &commands {
        builder.append_ordinary(command.as_bytes()).unwrap();
    }
    let frame = builder.finish().unwrap();
    let record = fireweed_engine::queue_log_record::encode_checked_publication_record(
        acquired.as_source().head().epoch(),
        frame.as_bytes(),
        1 << 20,
        job,
    )
    .unwrap();
    let view = record.view().unwrap();
    let QueueLogPayload::Publication(publication) = view.payload() else {
        panic!()
    };
    assert_eq!(
        publication
            .receipts()
            .next()
            .unwrap()
            .unwrap()
            .fingerprint_format,
        4
    );
    (record, after)
}

mod batch;

mod original_batch;

mod body;

mod administrative;

mod state;
