//! Source scans cannot be relabelled after reading or substituted by shape.
use super::*;
use fireweed_core::queue_administration::{
    AdministrativeAction, AdministrativeEntry, AdministrativeIntent, AdministrativeOperation,
    WorkSelector,
};
use std::cell::Cell;
fn request(keys: &[&str], priority: i64, body: bool) -> CommitGroup {
    let mut result = group("source-scan");
    result.administration = Some(AdministrativeCommit {
        max_result_bytes: 65536,
        intent: AdministrativeIntent {
            evaluated_at: 0,
            before_fields: None,
            operation: AdministrativeOperation::Addressed(
                keys.iter()
                    .map(|key| AdministrativeEntry {
                        key: (*key).into(),
                        expected_version: Some(1),
                        precondition: WorkSelector::all(vec![]),
                        action: AdministrativeAction::Update {
                            patch: WorkPatch {
                                priority: Some(priority),
                                body: body.then(|| vec![9].into()),
                                ..Default::default()
                            },
                            lifecycle: AdministrativeLifecycle::Keep,
                            lease_guard: AdministrativeLeaseGuard::RejectActive,
                            remove_field_prefixes: BTreeSet::new(),
                        },
                    })
                    .collect(),
            ),
        },
    });
    result
}
fn reader(f: &Fixture, fail_at: usize) -> BodyReader<'_> {
    BodyReader {
        fixture: f,
        reads: AtomicUsize::new(0),
        header_reads: AtomicUsize::new(0),
        lookups: AtomicUsize::new(0),
        fail_at,
    }
}
#[test]
fn source_scan_binds_actual_intent_budget_and_group_before_row_dispatch() {
    futures::executor::block_on(async {
        let f = fixture();
        let reader = reader(&f, 0);
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
        let group = request(&["a"], 3, false);
        let limits = QueueLimits::default();
        let ordinary = source
            .load_ordinary_authority(&f.queue, &group, &limits)
            .await
            .unwrap();
        let admin = group.administration.as_ref().unwrap();
        let different = request(&["a"], 9, false);
        let before = reader.lookups.load(Ordering::SeqCst);
        let usage = f.admission.usage().unwrap();
        let delivered = Cell::new(0);
        for (request, intent, budget) in [
            (
                &group,
                &different.administration.as_ref().unwrap().intent,
                admin.max_result_bytes,
            ),
            (&group, &admin.intent, admin.max_result_bytes + 1),
            (
                &different,
                &different.administration.as_ref().unwrap().intent,
                admin.max_result_bytes,
            ),
        ] {
            assert!(
                source
                    .scan_addressed_to_sink(
                        request,
                        &f.queue,
                        intent,
                        budget,
                        &ordinary,
                        &limits,
                        |_| {
                            delivered.set(delivered.get() + 1);
                            futures::future::ready(Ok(()))
                        }
                    )
                    .await
                    .is_err()
            );
            assert_eq!(reader.lookups.load(Ordering::SeqCst), before);
            assert_eq!(f.admission.usage().unwrap(), usage);
        }
        assert_eq!(delivered.get(), 0);
        drop(ordinary);
        drop(source);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}
#[test]
fn source_scan_rejects_same_shape_summary_foreign_cut_job_definition_and_request() {
    futures::executor::block_on(async {
        let f = fixture();
        let reader = reader(&f, 0);
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
        let group = request(&["a"], 3, false);
        let limits = QueueLimits::default();
        let ordinary = source
            .load_ordinary_authority(&f.queue, &group, &limits)
            .await
            .unwrap();
        let admin = group.administration.as_ref().unwrap();
        let mut completed = Vec::new();
        for _ in 0..2 {
            let scan = source
                .scan_addressed_to_sink(
                    &group,
                    &f.queue,
                    &admin.intent,
                    admin.max_result_bytes,
                    &ordinary,
                    &limits,
                    |_| futures::future::ready(Ok(())),
                )
                .await
                .unwrap();
            completed.push(scan.into_parts());
        }
        let (context, summary) = completed.pop().unwrap();
        let (other_context, other_summary) = completed.pop().unwrap();
        assert_eq!(summary.result(), other_summary.result());
        assert_eq!(
            summary.result().unwrap().items[0].outcome,
            fireweed_core::queue_administration::AdministrativeOutcome::Changed { version: 2 }
        );
        context
            .validate(
                reader.cut().unwrap(),
                &f.queue,
                &group,
                &f.definition,
                &ordinary,
                &summary,
                &f.job,
            )
            .unwrap();
        assert!(
            context
                .validate(
                    reader.cut().unwrap(),
                    &f.queue,
                    &group,
                    &f.definition,
                    &ordinary,
                    &other_summary,
                    &f.job
                )
                .is_err()
        );
        let identity = Arc::new(f.job.owned_buffer(128).unwrap());
        let cut = reader.cut().unwrap();
        let foreign = QueueSourceCut::from_captured(
            &identity,
            cut.address(),
            cut.epoch(),
            cut.through(),
            &f.job,
        )
        .unwrap();
        assert!(
            context
                .validate(
                    foreign,
                    &f.queue,
                    &group,
                    &f.definition,
                    &ordinary,
                    &summary,
                    &f.job
                )
                .is_err()
        );
        let different = request(&["a"], 9, false);
        assert!(
            context
                .validate(
                    cut,
                    &f.queue,
                    &different,
                    &f.definition,
                    &ordinary,
                    &summary,
                    &f.job
                )
                .is_err()
        );
        let foreign_reader = Reader {
            through: f.reader.through,
            identity: Arc::new(f.job.owned_buffer(128).unwrap()),
            facts: f.reader.facts.clone(),
        };
        let foreign_ordinary = load_administrative_ordinary_authority(
            &foreign_reader,
            foreign_reader.cut().unwrap(),
            &f.queue,
            &group,
            &f.definition,
            &limits,
            &f.job,
        )
        .await
        .unwrap();
        let foreign_cut = foreign_reader.cut().unwrap();
        assert_eq!(foreign_cut.epoch(), cut.epoch());
        assert_eq!(foreign_cut.through(), cut.through());
        assert_eq!(
            foreign_ordinary.snapshot().next_sequence,
            ordinary.snapshot().next_sequence
        );
        foreign_ordinary
            .validate(foreign_cut, &f.queue, &group, &f.definition, &f.job)
            .unwrap();
        assert!(
            context
                .validate(
                    foreign_cut,
                    &f.queue,
                    &group,
                    &f.definition,
                    &foreign_ordinary,
                    &summary,
                    &f.job
                )
                .is_err()
        );
        drop(foreign_ordinary);
        drop(foreign_reader);
        let mut changed = group.clone();
        changed.request_id.push_str("-other");
        let changed_ordinary = source
            .load_ordinary_authority(&f.queue, &changed, &limits)
            .await
            .unwrap();
        changed_ordinary
            .validate(cut, &f.queue, &changed, &f.definition, &f.job)
            .unwrap();
        assert!(
            context
                .validate(
                    cut,
                    &f.queue,
                    &changed,
                    &f.definition,
                    &changed_ordinary,
                    &summary,
                    &f.job
                )
                .is_err()
        );
        drop(changed_ordinary);
        assert!(
            context
                .validate(
                    cut,
                    &f.queue,
                    &changed,
                    &f.definition,
                    &ordinary,
                    &summary,
                    &f.job
                )
                .is_err()
        );
        let other_job = Arc::new(f.admission.reserve(0).unwrap());
        assert!(
            context
                .validate(
                    cut,
                    &f.queue,
                    &group,
                    &f.definition,
                    &ordinary,
                    &summary,
                    &other_job
                )
                .is_err()
        );
        let mut schema = f.definition.schema().clone();
        schema.max_attempts += 1;
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &f.job)
                .unwrap();
        assert!(
            context
                .validate(
                    cut,
                    &f.queue,
                    &group,
                    &definition,
                    &ordinary,
                    &summary,
                    &f.job
                )
                .is_err()
        );
        drop(definition);
        drop(other_job);
        drop(identity);
        drop(other_context);
        drop(other_summary);
        drop(context);
        drop(summary);
        drop(ordinary);
        drop(source);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}
#[test]
fn source_scan_late_read_and_sink_failure_never_mint_context_or_retain_credit() {
    futures::executor::block_on(async {
        for sink_failure in [false, true] {
            let f = fixture();
            let reader = reader(&f, if sink_failure { 0 } else { 2 });
            let source = open_administrative_source(
                &reader,
                reader.cut().unwrap(),
                &f.queue,
                &f.definition,
                &f.job,
            )
            .await
            .unwrap();
            let group = request(&["a", "b"], 3, true);
            let limits = QueueLimits::default();
            let ordinary = source
                .load_ordinary_authority(&f.queue, &group, &limits)
                .await
                .unwrap();
            let admin = group.administration.as_ref().unwrap();
            let baseline = f.admission.usage().unwrap();
            let delivered = Cell::new(0);
            let result = source
                .scan_addressed_to_sink(
                    &group,
                    &f.queue,
                    &admin.intent,
                    admin.max_result_bytes,
                    &ordinary,
                    &limits,
                    |_| {
                        delivered.set(delivered.get() + 1);
                        futures::future::ready(if sink_failure && delivered.get() == 2 {
                            Err(QueueError::TooLarge)
                        } else {
                            Ok(())
                        })
                    },
                )
                .await;
            assert!(result.is_err());
            assert_eq!(delivered.get(), if sink_failure { 2 } else { 1 });
            assert_eq!(reader.reads.load(Ordering::SeqCst), 2);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
    });
}
#[test]
fn empty_source_scan_still_binds_foreign_scope_and_admission_failure_does_not_dispatch() {
    futures::executor::block_on(async {
        let f = fixture();
        let reader = reader(&f, 0);
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
        let group = request(&[], 3, false);
        let limits = QueueLimits::default();
        let ordinary = source
            .load_ordinary_authority(&f.queue, &group, &limits)
            .await
            .unwrap();
        let admin = group.administration.as_ref().unwrap();
        let calls = reader.lookups.load(Ordering::SeqCst);
        let scan = source
            .scan_addressed_to_sink(
                &group,
                &f.queue,
                &admin.intent,
                admin.max_result_bytes,
                &ordinary,
                &limits,
                |_| -> futures::future::Ready<QueueResult<()>> {
                    panic!("empty source cannot deliver")
                },
            )
            .await
            .unwrap();
        let (context, summary) = scan.into_parts();
        assert_eq!(summary.packet_count(), 0);
        context
            .validate(
                reader.cut().unwrap(),
                &f.queue,
                &group,
                &f.definition,
                &ordinary,
                &summary,
                &f.job,
            )
            .unwrap();
        let identity = Arc::new(f.job.owned_buffer(128).unwrap());
        let cut = reader.cut().unwrap();
        let foreign = QueueSourceCut::from_captured(
            &identity,
            cut.address(),
            cut.epoch(),
            cut.through(),
            &f.job,
        )
        .unwrap();
        assert!(
            context
                .validate(
                    foreign,
                    &f.queue,
                    &group,
                    &f.definition,
                    &ordinary,
                    &summary,
                    &f.job
                )
                .is_err()
        );
        assert_eq!(reader.lookups.load(Ordering::SeqCst), calls);
        drop(identity);
        drop(context);
        drop(summary);
        let nonempty = request(&["a"], 3, false);
        let nonempty_ordinary = source
            .load_ordinary_authority(&f.queue, &nonempty, &limits)
            .await
            .unwrap();
        let nonempty_admin = nonempty.administration.as_ref().unwrap();
        let calls = reader.lookups.load(Ordering::SeqCst);
        let reads = reader.reads.load(Ordering::SeqCst);
        let usage = f.admission.usage().unwrap();
        let fill = f.job.owned_buffer((32u64 << 20) - usage.1).unwrap();
        let full = f.admission.usage().unwrap();
        assert!(
            source
                .scan_addressed_to_sink(
                    &nonempty,
                    &f.queue,
                    &nonempty_admin.intent,
                    nonempty_admin.max_result_bytes,
                    &nonempty_ordinary,
                    &limits,
                    |_| -> futures::future::Ready<QueueResult<()>> {
                        panic!("admission failure cannot deliver")
                    }
                )
                .await
                .is_err()
        );
        assert_eq!(reader.lookups.load(Ordering::SeqCst), calls);
        assert_eq!(reader.reads.load(Ordering::SeqCst), reads);
        assert_eq!(f.admission.usage().unwrap(), full);
        drop(fill);
        assert_eq!(f.admission.usage().unwrap(), usage);
        drop(nonempty_ordinary);
        drop(ordinary);
        drop(source);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}
#[test]
fn abandoned_source_scan_retires_provisional_context_and_row_owners() {
    use futures::{FutureExt, task::noop_waker_ref};
    use std::task::{Context, Poll};
    futures::executor::block_on(async {
        let f = fixture();
        let reader = reader(&f, 0);
        let source = open_administrative_source(
            &reader,
            reader.cut().unwrap(),
            &f.queue,
            &f.definition,
            &f.job,
        )
        .await
        .unwrap();
        let group = request(&["a", "b"], 3, true);
        let limits = QueueLimits::default();
        let ordinary = source
            .load_ordinary_authority(&f.queue, &group, &limits)
            .await
            .unwrap();
        let admin = group.administration.as_ref().unwrap();
        let baseline = f.admission.usage().unwrap();
        let delivered = Cell::new(0);
        let mut operation = source
            .scan_addressed_to_sink(
                &group,
                &f.queue,
                &admin.intent,
                admin.max_result_bytes,
                &ordinary,
                &limits,
                |_| {
                    delivered.set(delivered.get() + 1);
                    async {
                        futures::future::pending::<()>().await;
                        Ok(())
                    }
                },
            )
            .boxed_local();
        assert!(matches!(
            operation
                .as_mut()
                .poll(&mut Context::from_waker(noop_waker_ref())),
            Poll::Pending
        ));
        assert_eq!(delivered.get(), 1);
        assert_eq!(reader.reads.load(Ordering::SeqCst), 1);
        assert!(f.admission.usage().unwrap().1 > baseline.1);
        drop(operation);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}
