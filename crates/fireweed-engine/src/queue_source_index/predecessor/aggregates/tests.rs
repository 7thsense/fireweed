use super::super::tests::{Reader, group, record};
use super::*;
use crate::{queue_log_record::OwnedQueueLogRecord, queue_schema_codec::encode_definition};
use fireweed_core::queue_contract::*;

use super::super::test_fixture::{Fixture, fixture, request};
fn limits() -> QueueSourceCompileLimits {
    QueueSourceCompileLimits {
        edits: 5,
        input_bytes: 4096,
    }
}
async fn accumulate<'a, R: QueueSourceReader>(
    context: &QueueSourcePublicationContext<'a, R>,
    limits: QueueSourceCompileLimits,
) -> QueueResult<QueueSourceAggregateEdits<'a>> {
    let mut builder = context.aggregates(limits)?;
    for command in context.commands() {
        let command = command?;
        let predecessor = context.predecessor(&command).await?;
        let transition = context.record_transition(&predecessor).await?;
        builder.observe(&transition)?;
    }
    builder.finish().await
}
#[test]
fn aggregates_match_complete_planner_counts_without_expiry_reclassification() {
    futures::executor::block_on(async {
        for raw in LIFECYCLES {
            let f = fixture(raw);
            for kind in [
                "delete", "preserve", "replace", "complete", "fail", "pending", "create", "state",
                "rejected",
            ] {
                let (proposed, after) =
                    record(&f.queue, &f.schema, &f.snapshot, &request(kind), &f.job);
                let checked = proposed.checked().unwrap();
                let context = QueueSourcePublicationContext::begin(
                    &f.reader,
                    &checked,
                    &f.definition,
                    &f.job,
                )
                .await
                .unwrap();
                let baseline = f.admission.usage().unwrap();
                super::super::intrinsics::tests::assert_inventory_delta(
                    &context,
                    &f.snapshot,
                    &after,
                )
                .await;
                assert_eq!(f.admission.usage().unwrap(), baseline);
                let output = accumulate(&context, limits()).await.unwrap();
                output.verify(&proposed, &context.cut, &f.job).unwrap();
                let mut actual = std::collections::BTreeMap::new();
                let mut expected = std::collections::BTreeMap::new();
                for lifecycle in LIFECYCLES {
                    let key = QueueSourceKey::LifecycleCount(lifecycle).encode().unwrap();
                    actual.insert(key.clone(), if raw == lifecycle { 2u64 } else { 0 });
                    expected.insert(
                        key,
                        after
                            .work
                            .values()
                            .filter(|row| row.record.lifecycle == lifecycle)
                            .count() as u64,
                    );
                }
                let key = QueueSourceKey::Sequence.encode().unwrap();
                actual.insert(key.clone(), f.snapshot.next_sequence);
                expected.insert(key, after.next_sequence);
                for (key, value) in output.edits() {
                    actual.insert(key.to_vec(), postcard::from_bytes(value.unwrap()).unwrap());
                }
                assert_eq!(actual, expected, "{raw:?}/{kind}");
                if matches!(kind, "state" | "rejected" | "preserve") && raw != Lifecycle::Leased {
                    assert_eq!(output.edits().len(), 0);
                }
                drop(output);
                assert_eq!(f.admission.usage().unwrap(), baseline);
            }
        }
    });
}
#[test]
fn contradictory_source_counts_fail_even_for_net_zero_transitions() {
    futures::executor::block_on(async {
        for (kind, count) in [("preserve", 0u64), ("state", 3u64), ("delete", 0u64)] {
            let mut f = fixture(Lifecycle::Pending);
            f.reader.facts.insert(
                QueueSourceKey::LifecycleCount(Lifecycle::Pending)
                    .encode()
                    .unwrap(),
                queue_storage_layout::encode(&count).unwrap(),
            );
            let (proposed, _) = record(&f.queue, &f.schema, &f.snapshot, &request(kind), &f.job);
            let checked = proposed.checked().unwrap();
            let context =
                QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                    .await
                    .unwrap();
            let baseline = f.admission.usage().unwrap();
            let error = accumulate(&context, limits()).await.err().unwrap();
            assert!(
                matches!(error, QueueError::OutcomeUnknown(message) if message.contains("contradicts observed work") || message.contains("exceeds insertion authority"))
            );
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
        for corrupt in [None, Some(vec![]), Some(vec![255; 20])] {
            let mut f = fixture(Lifecycle::Pending);
            let key = QueueSourceKey::LifecycleCount(Lifecycle::Failed)
                .encode()
                .unwrap();
            if let Some(value) = corrupt {
                f.reader.facts.insert(key, value);
            } else {
                f.reader.facts.remove(&key);
            }
            let (proposed, _) = record(&f.queue, &f.schema, &f.snapshot, &request("state"), &f.job);
            let checked = proposed.checked().unwrap();
            let context =
                QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                    .await
                    .unwrap();
            let baseline = f.admission.usage().unwrap();
            assert!(accumulate(&context, limits()).await.is_err());
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
    });
}

fn packed_record(f: &Fixture, requests: &[CommitGroup]) -> OwnedQueueLogRecord {
    let mut after = f.snapshot.clone();
    let deltas: Vec<_> = requests
        .iter()
        .map(|request| {
            crate::queue_planner::plan_commit_group(request, &mut after, 0, &f.schema).unwrap()
        })
        .collect();
    let canonical =
        crate::queue_planner::combine_deltas(&deltas, f.snapshot.next_sequence).unwrap();
    let receipts: Vec<_> = requests
        .iter()
        .zip(&deltas)
        .map(|(request, delta)| {
            crate::queue_publication::PublicationReceipt::commit(&f.queue, request, &delta.receipt)
                .unwrap()
        })
        .collect();
    let frame = crate::queue_publication::encode_publication(
        &f.queue,
        &encode_definition(&f.schema).unwrap(),
        &f.snapshot,
        &canonical,
        &receipts,
        1 << 20,
        &f.job,
    )
    .unwrap();
    crate::queue_log_record::encode_checked_publication_record(1, frame.as_bytes(), 1 << 20, &f.job)
        .unwrap()
}
#[test]
fn sequence_gaps_and_maximum_are_preserved_without_count_overflow() {
    futures::executor::block_on(async {
        let f = fixture(Lifecycle::Pending);
        let create = request("create");
        let mut delete = group("packed-delete");
        delete.work_updates.push(UnclaimedUpdate {
            key: "c\0γ".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Delete,
        });
        let proposed = packed_record(&f, &[create, delete]);
        let checked = proposed.checked().unwrap();
        let context =
            QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                .await
                .unwrap();
        let output = accumulate(&context, limits()).await.unwrap();
        let actual: Vec<_> = output
            .edits()
            .map(|(key, value)| {
                (
                    key.to_vec(),
                    postcard::from_bytes::<u64>(value.unwrap()).unwrap(),
                )
            })
            .collect();
        assert_eq!(actual, [(QueueSourceKey::Sequence.encode().unwrap(), 3)]);
        drop(output);
        for overflow in [false, true] {
            let mut f = fixture(Lifecycle::Pending);
            f.snapshot.next_sequence = u64::MAX - 1;
            f.reader.facts.insert(
                QueueSourceKey::Sequence.encode().unwrap(),
                queue_storage_layout::encode(&f.snapshot.next_sequence).unwrap(),
            );
            if overflow {
                f.reader.facts.insert(
                    QueueSourceKey::LifecycleCount(Lifecycle::Pending)
                        .encode()
                        .unwrap(),
                    queue_storage_layout::encode(&u64::MAX).unwrap(),
                );
            }
            let (proposed, after) =
                record(&f.queue, &f.schema, &f.snapshot, &request("create"), &f.job);
            assert_eq!(after.next_sequence, u64::MAX);
            let checked = proposed.checked().unwrap();
            let context =
                QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                    .await
                    .unwrap();
            let baseline = f.admission.usage().unwrap();
            let result = accumulate(&context, limits()).await;
            if overflow {
                assert!(
                    matches!(result, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("count overflow"))
                );
            } else {
                let output = result.unwrap();
                assert!(output.edits().any(|(key, value)| key
                    == QueueSourceKey::Sequence.encode().unwrap()
                    && postcard::from_bytes::<u64>(value.unwrap()).unwrap() == u64::MAX));
                drop(output);
            }
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
        // A net-zero transition at MAX is legitimate; coalesce before range checks.
        let mut f = fixture(Lifecycle::Pending);
        f.snapshot.next_sequence = u64::MAX;
        for key in [
            QueueSourceKey::Sequence,
            QueueSourceKey::LifecycleCount(Lifecycle::Pending),
        ] {
            f.reader.facts.insert(
                key.encode().unwrap(),
                queue_storage_layout::encode(&u64::MAX).unwrap(),
            );
        }
        let (proposed, _) = record(
            &f.queue,
            &f.schema,
            &f.snapshot,
            &request("preserve"),
            &f.job,
        );
        let checked = proposed.checked().unwrap();
        let context =
            QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                .await
                .unwrap();
        assert_eq!(
            accumulate(&context, limits()).await.unwrap().edits().len(),
            0
        );
    });
}
#[test]
fn duplicate_fresh_allocations_and_incomplete_command_coverage_fail_sticky() {
    futures::executor::block_on(async {
        let f = fixture(Lifecycle::Pending);
        let mut request = request("create");
        request.work_inserts.push(NewWork {
            key: "d\0δ".into(),
            priority: 0,
            available_at: 0,
            fields: Default::default(),
            body: Default::default(),
        });
        let mut after = f.snapshot.clone();
        let mut delta =
            crate::queue_planner::plan_commit_group(&request, &mut after, 0, &f.schema).unwrap();
        assert_eq!(delta.work.len(), 2);
        delta.work[1].record.insertion_sequence = delta.work[0].record.insertion_sequence;
        let receipt = crate::queue_publication::PublicationReceipt::commit(
            &f.queue,
            &request,
            &delta.receipt,
        )
        .unwrap();
        let frame = crate::queue_publication::encode_publication(
            &f.queue,
            &encode_definition(&f.schema).unwrap(),
            &f.snapshot,
            &delta,
            &[receipt],
            1 << 20,
            &f.job,
        )
        .unwrap();
        let proposed = crate::queue_log_record::encode_checked_publication_record(
            1,
            frame.as_bytes(),
            1 << 20,
            &f.job,
        )
        .unwrap();
        let checked = proposed.checked().unwrap();
        let context =
            QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                .await
                .unwrap();
        let other =
            QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                .await
                .unwrap();
        let commands = context.commands().collect::<QueueResult<Vec<_>>>().unwrap();
        let p0 = context.predecessor(&commands[0]).await.unwrap();
        let p1 = context.predecessor(&commands[1]).await.unwrap();
        let t0 = context.record_transition(&p0).await.unwrap();
        let t1 = context.record_transition(&p1).await.unwrap();
        let baseline = f.admission.usage().unwrap();
        let mut duplicate_allocation = context.aggregates(limits()).unwrap();
        duplicate_allocation.observe(&t0).unwrap();
        let error = duplicate_allocation.observe(&t1).unwrap_err();
        assert!(
            matches!(error, QueueError::OutcomeUnknown(message) if message.contains("duplicate fresh work insertion allocation"))
        );
        assert!(duplicate_allocation.observe(&t1).is_err());
        assert!(duplicate_allocation.finish().await.is_err());
        assert!(
            context
                .aggregates(limits())
                .unwrap()
                .finish()
                .await
                .is_err()
        );
        let mut omitted = context.aggregates(limits()).unwrap();
        omitted.observe(&t0).unwrap();
        assert!(omitted.finish().await.is_err());
        let mut duplicate = context.aggregates(limits()).unwrap();
        duplicate.observe(&t0).unwrap();
        assert!(duplicate.observe(&t0).is_err());
        assert!(duplicate.finish().await.is_err());
        let mut reordered = context.aggregates(limits()).unwrap();
        reordered.observe(&t1).unwrap();
        assert!(reordered.observe(&t0).is_err());
        assert!(reordered.finish().await.is_err());
        let mut foreign = other.aggregates(limits()).unwrap();
        assert!(foreign.observe(&t0).is_err());
        assert!(foreign.finish().await.is_err());
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}

struct CounterReader<'a> {
    base: &'a Reader,
    selected: Vec<u8>,
    wrong_cut: bool,
    foreign: Arc<OwnedQueueBufferCharge>,
    reads: std::sync::atomic::AtomicUsize,
}
impl QueueSourceReader for CounterReader<'_> {
    type Point = Vec<u8>;
    type Range = (Vec<u8>, Vec<u8>);
    fn cut(&self) -> QueueResult<QueueSourceCut<'_>> {
        self.base.cut()
    }
    async fn lookup(&self, key: &[u8]) -> QueueResult<Self::Point> {
        if key.starts_with(&[1, 8]) {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        if key == self.selected && !self.wrong_cut {
            return Err(unknown("injected fourth counter source failure"));
        }
        self.base.lookup(key).await
    }
    fn point<'a>(
        &'a self,
        owner: &'a Self::Point,
    ) -> QueueResult<crate::queue_source_index::QueueSourcePoint<'a>> {
        if owner == &self.selected && self.wrong_cut {
            let cut = QueueSourceCut::from_captured(
                &self.foreign,
                self.cut()?.address(),
                1,
                self.cut()?.through(),
                self.foreign.reservation(),
            )?;
            return Ok(cut.point_from_source(owner, self.base.facts.get(owner).map(Vec::as_slice)));
        }
        self.base.point(owner)
    }
    async fn complete_range(&self, lower: &[u8], upper: &[u8]) -> QueueResult<Self::Range> {
        self.base.complete_range(lower, upper).await
    }
    fn visit_range(
        &self,
        owner: &Self::Range,
        lower: &[u8],
        upper: &[u8],
        visitor: &mut dyn for<'a> FnMut(
            crate::queue_source_index::QueueSourcePoint<'a>,
        ) -> QueueResult<()>,
    ) -> QueueResult<()> {
        self.base.visit_range(owner, lower, upper, visitor)
    }
}
#[test]
fn fourth_counter_failure_or_foreign_cut_never_returns_partial_output() {
    futures::executor::block_on(async {
        let f = fixture(Lifecycle::Pending);
        for kind in ["complete", "state", "rejected"] {
            let (proposed, _) = record(&f.queue, &f.schema, &f.snapshot, &request(kind), &f.job);
            let checked = proposed.checked().unwrap();
            for wrong_cut in [false, true] {
                let reader = CounterReader {
                    base: &f.reader,
                    selected: QueueSourceKey::LifecycleCount(Lifecycle::Failed)
                        .encode()
                        .unwrap(),
                    wrong_cut,
                    foreign: Arc::new(f.job.owned_buffer(128).unwrap()),
                    reads: std::sync::atomic::AtomicUsize::new(0),
                };
                let context =
                    QueueSourcePublicationContext::begin(&reader, &checked, &f.definition, &f.job)
                        .await
                        .unwrap();
                let baseline = f.admission.usage().unwrap();
                assert!(matches!(
                    accumulate(&context, limits()).await,
                    Err(QueueError::OutcomeUnknown(_))
                ));
                assert_eq!(reader.reads.load(std::sync::atomic::Ordering::SeqCst), 4);
                assert_eq!(f.admission.usage().unwrap(), baseline);
            }
        }
    });
}
async fn build<'c, 'a, R: QueueSourceReader>(
    context: &'c QueueSourcePublicationContext<'a, R>,
    limits: QueueSourceCompileLimits,
) -> QueueSourceAggregateAccumulator<'c, 'a, R> {
    let mut builder = context.aggregates(limits).unwrap();
    for command in context.commands() {
        let command = command.unwrap();
        let predecessor = context.predecessor(&command).await.unwrap();
        let transition = context.record_transition(&predecessor).await.unwrap();
        builder.observe(&transition).unwrap();
    }
    builder
}
#[test]
fn aggregate_output_limits_and_late_admission_fail_without_leaking_credit() {
    futures::executor::block_on(async {
        let f = fixture(Lifecycle::Pending);
        let mut request = request("complete");
        let mut second = request.work_updates[0].clone();
        second.key = "b\0β".into();
        request.work_updates.push(second);
        request.work_inserts.push(NewWork {
            key: "c\0γ".into(),
            priority: 0,
            available_at: 0,
            fields: Default::default(),
            body: Default::default(),
        });
        let (proposed, _) = record(&f.queue, &f.schema, &f.snapshot, &request, &f.job);
        let checked = proposed.checked().unwrap();
        let context =
            QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                .await
                .unwrap();
        let baseline = f.admission.usage().unwrap();
        let complete = accumulate(&context, limits()).await.unwrap();
        let edits = complete.edits().len() as u64;
        assert_eq!(edits, 3);
        let bytes = complete
            .edits()
            .map(|(key, value)| (key.len() + value.unwrap().len()) as u64)
            .sum::<u64>();
        drop(complete);
        assert_eq!(f.admission.usage().unwrap(), baseline);
        for (limit, success) in [
            (
                QueueSourceCompileLimits {
                    edits,
                    input_bytes: bytes,
                },
                true,
            ),
            (
                QueueSourceCompileLimits {
                    edits: edits - 1,
                    input_bytes: bytes,
                },
                false,
            ),
            (
                QueueSourceCompileLimits {
                    edits,
                    input_bytes: bytes - 1,
                },
                false,
            ),
        ] {
            let result = accumulate(&context, limit).await;
            assert_eq!(result.is_ok(), success);
            drop(result);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
        async fn probe(
            f: &Fixture,
            context: &QueueSourcePublicationContext<'_, Reader>,
            available: u64,
        ) -> Option<String> {
            let baseline = f.admission.usage().unwrap();
            let builder = build(context, limits()).await;
            let used = f.admission.usage().unwrap().1;
            let filler = f.job.owned_buffer((16 << 20) - used - available).unwrap();
            let error = match builder.finish().await {
                Ok(output) => {
                    drop(output);
                    None
                }
                Err(error) => Some(format!("{error:?}")),
            };
            drop(filler);
            assert_eq!(f.admission.usage().unwrap(), baseline);
            error
        }
        let (mut lower, mut upper) = (0, 1 << 20);
        assert!(probe(&f, &context, lower).await.is_some());
        assert!(probe(&f, &context, upper).await.is_none());
        while upper - lower > 1 {
            let middle = lower + (upper - lower) / 2;
            if probe(&f, &context, middle).await.is_none() {
                upper = middle;
            } else {
                lower = middle;
            }
        }
        assert!(probe(&f, &context, upper).await.is_none());
        assert!(
            probe(&f, &context, upper - 1)
                .await
                .unwrap()
                .contains("aggregate output admission")
        );
    });
}
