//! Consumer-side controls exercise the private producer before any submission.
use super::*;
use fireweed_engine::queue_publication::decode_original_commit_outcome;
use fireweed_queue_spool::{FinalizationProbe, FinalizationStage, SourceAdministrativePublication};
use std::{
    sync::atomic::{AtomicUsize, Ordering::SeqCst},
    time::Duration,
};
fn spent(context: &setup::Context<'_>) -> (u64, u64, u64, u64, u64) {
    let value = context.ledger.remaining_work();
    (
        value.semantic.pages,
        value.semantic.hydrated_bytes,
        value.record_attempts,
        value.hydration_reads,
        value.hydration_body_bytes,
    )
}
fn rejected(
    record: &OwnedQueueLogRecord,
    context: &setup::Context<'_>,
    request: &CommitGroup,
) -> Rejection {
    let checked = record.checked().unwrap();
    let QueueLogPayload::Publication(view) = checked.view().payload() else {
        panic!()
    };
    assert_eq!(view.summary().commands, 0);
    assert_eq!(
        view.summary().initial_sequence,
        view.summary().final_sequence
    );
    let mut receipts = view.receipts();
    let receipt = receipts.next().unwrap().unwrap();
    assert!(receipts.next().is_none());
    assert_eq!(receipt.fingerprint_format, 4);
    assert_eq!(receipt.domain, 2);
    assert_eq!(receipt.request_id, request.request_id);
    assert_eq!(
        receipt.fingerprint,
        &request.fingerprint(context.queue).unwrap()
    );
    let (dto, _owner) = decode_original_commit_outcome(
        context.queue,
        request,
        4,
        receipt.encoded_outcome(),
        context.job,
    )
    .unwrap();
    let GroupOutcome::Rejected(reason) = dto.outcome else {
        panic!()
    };
    reason
}
pub(super) async fn check(
    context: &setup::Context<'_>,
    request: &CommitGroup,
    publication: &SourceAdministrativePublication,
    admission: &QueueAdmission,
) {
    let original = context.source.source_cut(context.log, context.job).unwrap();
    let successful = publication
        .record_for(
            &original,
            context.queue,
            request,
            context.definition,
            context.job,
        )
        .unwrap();
    let baseline = admission.usage().unwrap();
    let disk = context.quota.usage().unwrap();
    let mut other = request.clone();
    let AdministrativeOperation::Addressed(entries) =
        &mut other.administration.as_mut().unwrap().intent.operation
    else {
        panic!()
    };
    let AdministrativeAction::Update { patch, .. } = &mut entries[0].action else {
        panic!()
    };
    patch.priority = Some(3);
    let (sink_a, scan_a, ordinary_a) = context.scan(request, 1 << 20).await;
    let (sink_b, scan_b, ordinary_b) = context.scan(&other, 1 << 20).await;
    let clock = AtomicUsize::new(0);
    assert!(
        context
            .finish(request, 1 << 20, (sink_b, scan_a, ordinary_a), || {
                clock.fetch_add(1, SeqCst);
                Ok(10)
            })
            .await
            .is_err()
    );
    assert_eq!(
        clock.load(SeqCst),
        0,
        "sink substitution refuses before planning"
    );
    drop((sink_a, scan_b, ordinary_b));
    assert_eq!(admission.usage().unwrap(), baseline);
    assert_eq!(context.quota.usage().unwrap(), disk);
    let exact = successful.bytes().len() as u64;
    for cap in [exact, exact - 1] {
        let resolution = context.resolution(request, cap).await;
        let actual = resolution
            .publication(context.queue, request, context.job)
            .unwrap();
        let record = actual
            .record_for(
                &original,
                context.queue,
                request,
                context.definition,
                context.job,
            )
            .unwrap();
        if cap == exact {
            assert_eq!(record.bytes(), successful.bytes());
        } else {
            assert_eq!(rejected(record, context, request), Rejection::TooLarge);
        }
        drop(actual);
        assert_eq!(admission.usage().unwrap(), baseline);
        assert_eq!(context.quota.usage().unwrap(), disk);
    }
    let mut missing = request.clone();
    missing.state_updates[0].expect = StateExpect::Version(99);
    let resolution = context.resolution(&missing, 1 << 20).await;
    let actual = resolution
        .publication(context.queue, &missing, context.job)
        .unwrap();
    assert_eq!(
        rejected(
            actual
                .record_for(
                    &original,
                    context.queue,
                    &missing,
                    context.definition,
                    context.job
                )
                .unwrap(),
            context,
            &missing
        ),
        Rejection::NotFound
    );
    drop(actual);
    assert_eq!(admission.usage().unwrap(), baseline);
    assert_eq!(context.quota.usage().unwrap(), disk);
    for stage in [
        FinalizationStage::AssembleFrame,
        FinalizationStage::EncodeRecord,
    ] {
        for mode in 0..if stage == FinalizationStage::EncodeRecord {
            3
        } else {
            2
        } {
            let (probe, release) = FinalizationProbe::new(stage, Duration::from_secs(20));
            let resolution = context.probed(request, probe.clone()).await;
            let before = spent(context);
            let workers = context.workers.clone();
            let job = context.job.clone();
            let queue = context.queue.clone();
            let group = request.clone();
            let running = tokio::spawn(async move {
                workers
                    .run(move || resolution.publication(&queue, &group, &job))
                    .await
            });
            tokio::time::timeout(Duration::from_secs(10), probe.entered())
                .await
                .unwrap();
            let usage = admission.usage().unwrap();
            let available = match (stage, mode) {
                (_, 0) => None,
                (FinalizationStage::AssembleFrame, _) => Some(0),
                (FinalizationStage::EncodeRecord, mode) => {
                    let n = 4 * exact + 4096;
                    Some(if mode == 1 { n - 1 } else { n })
                }
                _ => unreachable!(),
            };
            let fill = available.map(|available| {
                context
                    .job
                    .owned_buffer((64u64 << 20) - usage.1 - available)
                    .unwrap()
            });
            release.release();
            drop(release);
            let result = tokio::time::timeout(Duration::from_secs(10), running)
                .await
                .unwrap()
                .unwrap();
            assert!(
                probe.controller_released(),
                "timeout is not a successful controller release"
            );
            if mode == 1 {
                let expected = if stage == FinalizationStage::AssembleFrame {
                    "queue scratch: publication receipt binding failed"
                } else {
                    "admitted buffer capacity exhausted; resolve owned operation"
                };
                assert!(
                    matches!(result, Err(QueueError::OutcomeUnknown(ref message)) if message == expected)
                );
            } else {
                let actual = result.unwrap();
                assert_eq!(
                    actual
                        .record_for(
                            &original,
                            context.queue,
                            request,
                            context.definition,
                            context.job
                        )
                        .unwrap()
                        .bytes(),
                    successful.bytes()
                );
                drop(actual);
            }
            drop(fill);
            assert_eq!(
                admission.usage().unwrap(),
                baseline,
                "all provisional owners retire"
            );
            assert_eq!(context.quota.usage().unwrap(), disk);
            assert_eq!(
                spent(context),
                before,
                "assembly does not spend source work"
            );
        }
    }
}
