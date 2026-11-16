//! Owned preimages retain original request/cut identity across worker movement.
use super::*;
use fireweed_core::queue_administration::{
    AdministrativeAction, AdministrativeEntry, AdministrativeIntent, AdministrativeOperation,
    SelectorNode, WorkSelector,
};
fn request() -> CommitGroup {
    let mut group = group("owned-ordinary");
    group.work_updates.push(UnclaimedUpdate {
        key: "a".into(),
        expect: StateExpect::Version(1),
        change: UnclaimedChange::Patch(WorkPatch {
            priority: Some(9),
            ..Default::default()
        }),
    });
    group.state_updates.push(StateUpdate {
        collection: "s".into(),
        key: "state".into(),
        expect: StateExpect::Version(1),
        change: StateChange::Check,
    });
    group.administration = Some(AdministrativeCommit {
        max_result_bytes: 65536,
        intent: AdministrativeIntent {
            evaluated_at: 0,
            before_fields: None,
            operation: AdministrativeOperation::Addressed(vec![AdministrativeEntry {
                key: "b".into(),
                expected_version: Some(1),
                precondition: WorkSelector::all(vec![]),
                action: AdministrativeAction::Delete {
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                },
            }]),
        },
    });
    group
}
#[test]
fn owned_authority_crosses_worker_and_rejects_retargeting() {
    futures::executor::block_on(async {
        let f = fixture();
        let reader = BodyReader {
            fixture: &f,
            reads: AtomicUsize::new(0),
            header_reads: AtomicUsize::new(0),
            lookups: AtomicUsize::new(0),
            fail_at: 1,
        };
        let mut group = request();
        let baseline = f.admission.usage().unwrap();
        let authority = load_administrative_ordinary_authority(
            &reader,
            reader.cut().unwrap(),
            &f.queue,
            &group,
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await
        .unwrap();
        let authority = std::thread::spawn(move || {
            assert_eq!(authority.snapshot().work["a"].record.work.priority, 0);
            assert!(!authority.snapshot().work["a"].body_loaded);
            assert!(!authority.snapshot().work.contains_key("b"));
            let state = &authority.snapshot().state[&("s".into(), "state".into())];
            assert!(state.fields.is_empty());
            assert!(state.value.is_empty());
            authority
        })
        .join()
        .unwrap();
        authority
            .validate(
                reader.cut().unwrap(),
                &f.queue,
                &group,
                &f.definition,
                &f.job,
            )
            .unwrap();
        assert!(authority.cut().same_cut(&reader.cut().unwrap()));
        let original = group.clone();
        group.administration.as_mut().unwrap().max_result_bytes += 1;
        assert!(
            authority
                .validate(
                    reader.cut().unwrap(),
                    &f.queue,
                    &group,
                    &f.definition,
                    &f.job
                )
                .is_err()
        );
        group = original.clone();
        group.administration.as_mut().unwrap().intent.evaluated_at = 1;
        assert!(
            authority
                .validate(
                    reader.cut().unwrap(),
                    &f.queue,
                    &group,
                    &f.definition,
                    &f.job
                )
                .is_err()
        );
        group = original.clone();
        group.request_id.push('x');
        assert!(
            authority
                .validate(
                    reader.cut().unwrap(),
                    &f.queue,
                    &group,
                    &f.definition,
                    &f.job
                )
                .is_err()
        );
        group = original.clone();
        group.state_updates[0].change = StateChange::Put {
            fields: Fields::new(),
            value: vec![9].into(),
        };
        assert!(
            matches!(authority.validate(reader.cut().unwrap(), &f.queue, &group, &f.definition, &f.job), Err(QueueError::OutcomeUnknown(message)) if message == "source group preimage: owned ordinary request, definition or queue differs")
        );
        group = original;
        let foreign_identity = Arc::new(f.job.owned_buffer(128).unwrap());
        let original_cut = reader.cut().unwrap();
        let foreign_cut = QueueSourceCut::from_captured(
            &foreign_identity,
            original_cut.address(),
            original_cut.epoch(),
            original_cut.through(),
            &f.job,
        )
        .unwrap();
        assert!(
            authority
                .validate(foreign_cut, &f.queue, &group, &f.definition, &f.job)
                .is_err()
        );
        let altered_cut = QueueSourceCut::from_captured(
            &f.reader.identity,
            original_cut.address(),
            original_cut.epoch() + 1,
            original_cut.through(),
            &f.job,
        )
        .unwrap();
        assert!(
            authority
                .validate(altered_cut, &f.queue, &group, &f.definition, &f.job)
                .is_err()
        );
        let foreign_job = Arc::new(f.admission.reserve(0).unwrap());
        assert!(
            authority
                .validate(original_cut, &f.queue, &group, &f.definition, &foreign_job)
                .is_err()
        );
        drop(foreign_job);
        drop(foreign_identity);
        let mut schema = f.definition.schema().clone();
        schema.max_attempts += 1;
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &f.job)
                .unwrap();
        assert!(
            authority
                .validate(original_cut, &f.queue, &group, &definition, &f.job)
                .is_err()
        );
        drop(definition);
        assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
        let binding = authority.retain_binding(&f.job).unwrap();
        drop(authority);
        binding
            .validate(
                reader.cut().unwrap(),
                &f.queue,
                &group,
                &f.definition,
                &f.job,
            )
            .unwrap();
        assert!(f.admission.usage().unwrap().1 > baseline.1);
        drop(binding);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}
#[test]
fn administrative_validation_is_funded_before_source_reads() {
    futures::executor::block_on(async {
        let f = fixture();
        let reader = BodyReader {
            fixture: &f,
            reads: AtomicUsize::new(0),
            header_reads: AtomicUsize::new(0),
            lookups: AtomicUsize::new(0),
            fail_at: 1,
        };
        let mut group = request();
        let AdministrativeOperation::Addressed(entries) =
            &mut group.administration.as_mut().unwrap().intent.operation
        else {
            panic!()
        };
        entries[0].precondition.nodes = vec![SelectorNode::Not; 40000];
        let baseline = f.admission.usage().unwrap();
        let result = load_administrative_ordinary_authority(
            &reader,
            reader.cut().unwrap(),
            &f.queue,
            &group,
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await;
        assert!(
            matches!(result, Err(QueueError::OutcomeUnknown(message)) if message == "admitted buffer capacity exhausted; resolve owned operation")
        );
        assert_eq!(reader.header_reads.load(Ordering::SeqCst), 0);
        assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}

#[test]
fn source_failure_after_owned_binding_restores_every_charge() {
    futures::executor::block_on(async {
        let mut f = fixture();
        assert!(
            f.reader
                .facts
                .remove(&QueueSourceKey::Sequence.encode().unwrap())
                .is_some()
        );
        let reader = BodyReader {
            fixture: &f,
            reads: AtomicUsize::new(0),
            header_reads: AtomicUsize::new(0),
            lookups: AtomicUsize::new(0),
            fail_at: 1,
        };
        let baseline = f.admission.usage().unwrap();
        let result = load_administrative_ordinary_authority(
            &reader,
            reader.cut().unwrap(),
            &f.queue,
            &request(),
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await;
        assert!(
            matches!(result, Err(QueueError::OutcomeUnknown(message)) if message == "source header: covered source has no sequence fact")
        );
        assert_eq!(reader.header_reads.load(Ordering::SeqCst), 2);
        assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}

#[test]
fn shared_header_rejects_foreign_definition_admission_before_row_reads() {
    futures::executor::block_on(async {
        let f = fixture();
        let reader = BodyReader {
            fixture: &f,
            reads: AtomicUsize::new(0),
            header_reads: AtomicUsize::new(0),
            lookups: AtomicUsize::new(0),
            fail_at: 1,
        };
        let baseline = f.admission.usage().unwrap();
        let source = open_administrative_source(
            &reader,
            reader.cut().unwrap(),
            &f.queue,
            &f.definition,
            &f.job,
        )
        .await
        .unwrap();
        let mut schema = f.definition.schema().clone();
        schema.max_attempts += 1;
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &f.job)
                .unwrap();
        let group = request();
        let admission = admit_administrative_group(
            &f.queue,
            &group,
            &definition,
            &QueueLimits::default(),
            &f.job,
        )
        .unwrap();
        let before = reader.header_reads.load(Ordering::SeqCst);
        let lookup_before = reader.lookups.load(Ordering::SeqCst);
        let result = source.load_admitted_ordinary_authority(admission).await;
        assert!(
            matches!(result, Err(QueueError::OutcomeUnknown(message)) if message == "source group preimage: ordinary header source or request differs")
        );
        assert_eq!(reader.header_reads.load(Ordering::SeqCst), before);
        assert_eq!(reader.lookups.load(Ordering::SeqCst), lookup_before);
        assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
        drop(definition);
        drop(source);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}

#[test]
fn ordinary_and_addressed_rows_share_one_original_header() {
    futures::executor::block_on(async {
        let f = fixture();
        let reader = BodyReader {
            fixture: &f,
            reads: AtomicUsize::new(0),
            header_reads: AtomicUsize::new(0),
            lookups: AtomicUsize::new(0),
            fail_at: 1,
        };
        let baseline = f.admission.usage().unwrap();
        let group = request();
        let admission = admit_administrative_group(
            &f.queue,
            &group,
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .unwrap();
        let source = open_administrative_source(
            &reader,
            reader.cut().unwrap(),
            &f.queue,
            &f.definition,
            &f.job,
        )
        .await
        .unwrap();
        let authority = source
            .load_admitted_ordinary_authority(admission)
            .await
            .unwrap();
        let AdministrativeOperation::Addressed(entries) =
            &group.administration.as_ref().unwrap().intent.operation
        else {
            panic!()
        };
        let row = source
            .load_addressed_row(0, &entries[0], None)
            .await
            .unwrap();
        assert_eq!(reader.header_reads.load(Ordering::SeqCst), 2);
        drop(row);
        drop(authority);
        drop(source);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}
