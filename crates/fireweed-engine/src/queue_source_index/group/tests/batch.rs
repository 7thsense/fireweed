use super::*;
use crate::queue_publication::{PublicationReceipt, encode_publication_pinned};

#[test]
fn source_batch_overlay_preserves_released_unique_absence_and_state_highwaters() {
    futures::executor::block_on(async {
        let f = fixture();
        let mut remove = group("z-remove");
        remove.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Delete,
        });
        let mut transfer = group("y-transfer");
        transfer.work_inserts.push(work("c", "a"));
        let mut rejected = group("x-rejected");
        rejected.work_inserts.push(work("d", "a"));
        let mut check = group("w-check");
        check.state_updates.push(StateUpdate {
            collection: "s".into(),
            key: "state".into(),
            expect: StateExpect::Version(1),
            change: StateChange::Check,
        });
        let mut put = group("v-put");
        put.state_updates.push(StateUpdate {
            collection: "s".into(),
            key: "state".into(),
            expect: StateExpect::Version(1),
            change: StateChange::Put {
                fields: work("", "changed").fields,
                value: vec![9].into(),
            },
        });
        let mut delete = group("u-delete");
        delete.state_updates.push(StateUpdate {
            collection: "s".into(),
            key: "state".into(),
            expect: StateExpect::Version(2),
            change: StateChange::Delete,
        });
        let mut recreate = group("t-recreate");
        recreate.state_updates.push(StateUpdate {
            collection: "s".into(),
            key: "state".into(),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: work("", "recreated").fields,
                value: vec![10].into(),
            },
        });
        let batch = CommitBatch {
            queue: f.queue.clone(),
            groups: vec![remove, transfer, rejected, check, put, delete, recreate],
        };
        let positions: Vec<_> = (0..batch.groups.len()).collect();
        let baseline = f.admission.usage().unwrap();
        let clock = Clock::default();
        let input = load_batch_preimage(
            &f.reader,
            &batch,
            &positions,
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await
        .unwrap();
        assert_eq!(clock.0.load(Ordering::SeqCst), 0);
        let plan = input.plan(&f.reader, &clock).await.unwrap();
        assert_eq!(clock.0.load(Ordering::SeqCst), batch.groups.len());
        let mut expected = f.snapshot.clone();
        let mut deltas = Vec::new();
        for group in &batch.groups {
            expected.unique_probes_loaded.extend(
                queue_planner::prospective_unique_probes(group, &expected, f.definition.schema())
                    .unwrap(),
            );
            deltas.push(
                queue_planner::plan_commit_group(
                    group,
                    &mut expected,
                    1_000_000_000,
                    f.definition.schema(),
                )
                .unwrap(),
            );
        }
        assert_eq!(
            plan.receipts().collect::<Vec<_>>(),
            deltas.iter().map(|d| &d.receipt).collect::<Vec<_>>()
        );
        assert!(matches!(
            deltas[1].receipt.outcome,
            GroupOutcome::Committed { .. }
        ));
        assert!(matches!(
            deltas[2].receipt.outcome,
            GroupOutcome::Rejected(Rejection::UniqueConflict)
        ));
        assert_eq!(expected.state[&("s".into(), "state".into())].version, 4);
        assert!(expected.work.contains_key("c"));
        assert!(!expected.work.contains_key("a"));
        assert!(!expected.work.contains_key("d"));
        let canonical = queue_planner::combine_deltas(&deltas, f.snapshot.next_sequence).unwrap();
        let mut receipts = batch
            .groups
            .iter()
            .zip(&deltas)
            .map(|(g, d)| PublicationReceipt::commit(&f.queue, g, &d.receipt).unwrap())
            .collect::<Vec<_>>();
        receipts.sort_unstable_by(|a, b| a.identity().cmp(&b.identity()));
        let frame = encode_publication_pinned(
            &f.queue,
            f.definition.pin(),
            &f.snapshot,
            &canonical,
            &receipts,
            1 << 20,
            &f.job,
        )
        .unwrap();
        let expected_record = crate::queue_log_record::encode_checked_publication_record(
            1,
            frame.as_bytes(),
            1 << 20,
            &f.job,
        )
        .unwrap();
        let publication = plan
            .encode_prefix(QueueSourceGroupEncodingLimits {
                frame_bytes: 1 << 20,
                record_bytes: 1 << 20,
            })
            .unwrap();
        assert_eq!(publication.positions(), positions);
        let cut = f.reader.cut().unwrap();
        assert_eq!(
            publication
                .record_for(&cut, &f.definition, &batch, &f.job)
                .unwrap()
                .bytes(),
            expected_record.bytes()
        );
        assert!(
            publication
                .record_for(&cut, &f.definition, &batch.clone(), &f.job)
                .is_err()
        );
        {
            let foreign_owner = Arc::new(f.job.owned_buffer(128).unwrap());
            let foreign_cut = QueueSourceCut::from_captured(
                &foreign_owner,
                cut.address(),
                cut.epoch(),
                cut.through(),
                &f.job,
            )
            .unwrap();
            assert!(
                publication
                    .record_for(&foreign_cut, &f.definition, &batch, &f.job)
                    .is_err(),
                "equal numeric heads do not establish the original opaque capture"
            );
            let foreign_job = f.admission.reserve(0).unwrap();
            assert!(
                publication
                    .record_for(&cut, &f.definition, &batch, &foreign_job)
                    .is_err()
            );
            let mut schema = f.definition.schema().clone();
            schema.progress_bound_nanos += 1;
            let foreign_definition =
                AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &f.job)
                    .unwrap();
            assert!(
                publication
                    .record_for(&cut, &foreign_definition, &batch, &f.job)
                    .is_err()
            );
        }
        assert_eq!(
            publication
                .record_for(&cut, &f.definition, &batch, &f.job)
                .unwrap()
                .bytes(),
            expected_record.bytes(),
            "failed foreign handoffs must not invalidate the original capability"
        );
        drop(publication);
        drop(expected_record);
        drop(frame);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}

#[test]
fn source_batch_physical_overflow_retains_fitting_prefix_without_resampling() {
    futures::executor::block_on(async {
        let f = fixture();
        let mut first = group("z-first");
        let mut item = work("new-one", "new-one");
        item.body = vec![7; 4096].into();
        first.work_inserts.push(item);
        let mut second = group("a-second");
        let mut item = work("new-two", "new-two");
        item.body = vec![8; 4096].into();
        second.work_inserts.push(item);
        let batch = CommitBatch {
            queue: f.queue.clone(),
            groups: vec![first, second],
        };
        // A fresh independent plan at the retained sample has exactly this prefix.
        let mut before = f.snapshot.clone();
        before.unique_probes_loaded.extend(
            queue_planner::prospective_unique_probes(
                &batch.groups[0],
                &before,
                f.definition.schema(),
            )
            .unwrap(),
        );
        let delta = queue_planner::plan_commit_group(
            &batch.groups[0],
            &mut before,
            1_000_000_000,
            f.definition.schema(),
        )
        .unwrap();
        let receipt =
            PublicationReceipt::commit(&f.queue, &batch.groups[0], &delta.receipt).unwrap();
        let frame = encode_publication_pinned(
            &f.queue,
            f.definition.pin(),
            &f.snapshot,
            &delta,
            &[receipt],
            1 << 20,
            &f.job,
        )
        .unwrap();
        let expected = crate::queue_log_record::encode_checked_publication_record(
            1,
            frame.as_bytes(),
            1 << 20,
            &f.job,
        )
        .unwrap();
        let frame_bytes = frame.as_bytes().len() as u64;
        let clock = Clock::default();
        let plan = load_batch_preimage(
            &f.reader,
            &batch,
            &[0, 1],
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await
        .unwrap()
        .plan(&f.reader, &clock)
        .await
        .unwrap();
        assert_eq!(clock.0.load(Ordering::SeqCst), 2);
        let output = plan
            .encode_prefix(QueueSourceGroupEncodingLimits {
                frame_bytes,
                record_bytes: 1 << 20,
            })
            .unwrap();
        assert_eq!(output.positions(), &[0]);
        assert_eq!(
            clock.0.load(Ordering::SeqCst),
            2,
            "packing cannot resample logical execution"
        );
        assert_eq!(
            output
                .record_for(&f.reader.cut().unwrap(), &f.definition, &batch, &f.job)
                .unwrap()
                .bytes(),
            expected.bytes()
        );
    });
}

struct UnreadSource(AtomicUsize);
impl QueueSourceReader for UnreadSource {
    type Point = ();
    type Range = ();
    fn cut(&self) -> QueueResult<QueueSourceCut<'_>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(unknown("validation reached source"))
    }
    async fn lookup(&self, _: &[u8]) -> QueueResult<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(unknown("validation reached source"))
    }
    fn point<'a>(&'a self, _: &'a ()) -> QueueResult<QueueSourcePoint<'a>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(unknown("validation reached source"))
    }
    async fn complete_range(&self, _: &[u8], _: &[u8]) -> QueueResult<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(unknown("validation reached source"))
    }
    fn visit_range(
        &self,
        _: &(),
        _: &[u8],
        _: &[u8],
        _: &mut dyn for<'a> FnMut(QueueSourcePoint<'a>) -> QueueResult<()>,
    ) -> QueueResult<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(unknown("validation reached source"))
    }
}
#[test]
fn source_batch_validates_aggregate_positional_response_before_any_source_call() {
    futures::executor::block_on(async {
        let f = fixture();
        let limits = QueueLimits {
            max_response_bytes: 400,
            ..Default::default()
        };
        let groups = ["first", "second"]
            .into_iter()
            .map(|id| {
                let mut group = group(id);
                group.work_updates.push(UnclaimedUpdate {
                    key: "a".into(),
                    expect: StateExpect::Version(1),
                    change: UnclaimedChange::Delete,
                });
                group
            })
            .collect();
        let batch = CommitBatch {
            queue: f.queue.clone(),
            groups,
        };
        // Each single-group envelope fits. Their cumulative reserved response
        // does not, even when only the first position is selected for execution.
        for group in &batch.groups {
            group
                .validate_for(&batch.queue, f.definition.schema(), &limits)
                .unwrap();
        }
        assert_eq!(
            batch.validate(f.definition.schema(), &limits),
            Err(QueueError::TooLarge)
        );
        let reader = UnreadSource(AtomicUsize::new(0));
        let clock = Clock::default();
        let result = match load_batch_preimage(
            &reader,
            &batch,
            &[0],
            &f.definition,
            &limits,
            &f.job,
        )
        .await
        {
            Ok(image) => image.plan(&reader, &clock).await.map(|_| ()),
            Err(error) => Err(error),
        };
        assert_eq!(result, Err(QueueError::TooLarge));
        assert_eq!(reader.0.load(Ordering::SeqCst), 0);
        assert_eq!(clock.0.load(Ordering::SeqCst), 0);
    });
}

struct TrackedSource<'a> {
    inner: &'a Reader,
    clock: &'a Clock,
    forbidden: BTreeSet<Vec<u8>>,
    points: std::sync::Mutex<Vec<(Vec<u8>, usize)>>,
}
impl QueueSourceReader for TrackedSource<'_> {
    type Point = Vec<u8>;
    type Range = (Vec<u8>, Vec<u8>);
    fn cut(&self) -> QueueResult<QueueSourceCut<'_>> {
        self.inner.cut()
    }
    async fn lookup(&self, key: &[u8]) -> QueueResult<Vec<u8>> {
        self.points
            .lock()
            .unwrap()
            .push((key.to_vec(), self.clock.0.load(Ordering::SeqCst)));
        if self.forbidden.contains(key) {
            return Err(QueueError::TooLarge);
        }
        self.inner.lookup(key).await
    }
    fn point<'a>(&'a self, owner: &'a Vec<u8>) -> QueueResult<QueueSourcePoint<'a>> {
        self.inner.point(owner)
    }
    async fn complete_range(&self, lower: &[u8], upper: &[u8]) -> QueueResult<Self::Range> {
        self.inner.complete_range(lower, upper).await
    }
    fn visit_range(
        &self,
        owner: &Self::Range,
        lower: &[u8],
        upper: &[u8],
        visitor: &mut dyn for<'a> FnMut(QueueSourcePoint<'a>) -> QueueResult<()>,
    ) -> QueueResult<()> {
        self.inner.visit_range(owner, lower, upper, visitor)
    }
}
#[test]
fn source_batch_fields_are_unioned_per_identity_and_late_probes_precede_its_clock() {
    futures::executor::block_on(async {
        let huge = "u".repeat(64 << 10);
        let f = fixture_with_work(vec![work("a", &huge), work("b", "b")]);
        let mut a = group("a-patch-n");
        a.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                fields: BTreeMap::from([("n".into(), Some(FieldValue::Integer(7)))]),
                ..Default::default()
            }),
        });
        let mut b = group("b-patch-u");
        b.work_updates.push(UnclaimedUpdate {
            key: "b".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                fields: BTreeMap::from([("u".into(), Some(FieldValue::Text("new-b".into())))]),
                ..Default::default()
            }),
        });
        let batch = CommitBatch {
            queue: f.queue.clone(),
            groups: vec![a, b],
        };
        let field = |key, name| {
            QueueSourceKey::Field {
                collection: None,
                key,
                field: name,
            }
            .encode()
            .unwrap()
        };
        let clock = Clock::default();
        let reader = TrackedSource {
            inner: &f.reader,
            clock: &clock,
            forbidden: BTreeSet::from([field("a", "u"), field("b", "n")]),
            points: Default::default(),
        };
        let plan = load_batch_preimage(
            &reader,
            &batch,
            &[0, 1],
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await
        .unwrap()
        .plan(&reader, &clock)
        .await
        .unwrap();
        assert!(
            plan.receipts()
                .all(|r| matches!(r.outcome, GroupOutcome::Committed { .. }))
        );
        assert_eq!(clock.0.load(Ordering::SeqCst), 2);
        let points = reader.points.lock().unwrap();
        let fields: BTreeSet<_> = [
            field("a", "n"),
            field("a", "u"),
            field("b", "n"),
            field("b", "u"),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            points
                .iter()
                .filter(|(key, _)| fields.contains(key))
                .cloned()
                .collect::<Vec<_>>(),
            vec![(field("a", "n"), 0), (field("b", "u"), 0)]
        );
        let probes = queue_planner::prospective_unique_probes(
            &batch.groups[1],
            &f.snapshot,
            f.definition.schema(),
        )
        .unwrap();
        for probe in probes {
            let key = QueueSourceKey::UniqueOwner {
                collection: probe.collection.as_deref(),
                index: &probe.index,
                sort: &probe.full_sort_key,
            }
            .encode()
            .unwrap();
            assert_eq!(
                points
                    .iter()
                    .filter(|(actual, _)| actual == &key)
                    .map(|(_, sample)| *sample)
                    .collect::<Vec<_>>(),
                vec![1]
            );
        }
    });
}

#[test]
fn source_batch_complete_nonmonotonic_run_fits_even_when_first_write_does_not() {
    futures::executor::block_on(async {
        let f = fixture();
        let mut insert = group("z-large-first");
        let mut item = work("temporary", "temporary");
        item.body = vec![7; 8192].into();
        insert.work_inserts.push(item);
        let mut remove = group("a-delete-second");
        remove.work_updates.push(UnclaimedUpdate {
            key: "temporary".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Delete,
        });
        let batch = CommitBatch {
            queue: f.queue.clone(),
            groups: vec![insert, remove],
        };
        let (first, _) = record(
            &f.queue,
            f.definition.schema(),
            &f.snapshot,
            &batch.groups[0],
            &f.job,
        );
        assert!(first.bytes().len() > 4096);
        let plan = load_batch_preimage(
            &f.reader,
            &batch,
            &[0, 1],
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await
        .unwrap()
        .plan(&f.reader, &ZeroClock)
        .await
        .unwrap();
        assert!(
            plan.receipts()
                .all(|receipt| matches!(receipt.outcome, GroupOutcome::Committed { .. })),
            "coalescing requires both logical executions to succeed"
        );
        let mut expected = f.snapshot.clone();
        let mut deltas = Vec::new();
        for group in &batch.groups {
            expected.unique_probes_loaded.extend(
                queue_planner::prospective_unique_probes(group, &expected, f.definition.schema())
                    .unwrap(),
            );
            deltas.push(
                queue_planner::plan_commit_group(group, &mut expected, 0, f.definition.schema())
                    .unwrap(),
            );
        }
        assert_eq!(
            expected.versions[&crate::queue_planner::RecordIdentity {
                collection: None,
                key: "temporary".into()
            }],
            2
        );
        assert!(!expected.work.contains_key("temporary"));
        let canonical = queue_planner::combine_deltas(&deltas, f.snapshot.next_sequence).unwrap();
        let mut receipts = batch
            .groups
            .iter()
            .zip(&deltas)
            .map(|(g, d)| PublicationReceipt::commit(&f.queue, g, &d.receipt).unwrap())
            .collect::<Vec<_>>();
        receipts.sort_unstable_by(|a, b| a.identity().cmp(&b.identity()));
        let frame = encode_publication_pinned(
            &f.queue,
            f.definition.pin(),
            &f.snapshot,
            &canonical,
            &receipts,
            1 << 20,
            &f.job,
        )
        .unwrap();
        let expected_record = crate::queue_log_record::encode_checked_publication_record(
            1,
            frame.as_bytes(),
            1 << 20,
            &f.job,
        )
        .unwrap();
        let output = plan
            .encode_prefix(QueueSourceGroupEncodingLimits {
                frame_bytes: 4096,
                record_bytes: 4096,
            })
            .unwrap();
        assert_eq!(output.positions(), &[0, 1]);
        assert_eq!(
            output
                .record_for(&f.reader.cut().unwrap(), &f.definition, &batch, &f.job)
                .unwrap()
                .bytes(),
            expected_record.bytes()
        );
        assert!(
            output
                .record_for(&f.reader.cut().unwrap(), &f.definition, &batch, &f.job)
                .unwrap()
                .bytes()
                .len()
                < 4096
        );
    });
}

#[test]
fn source_batch_outer_record_limit_exact_and_one_short_are_independent_of_frame_cap() {
    futures::executor::block_on(async {
        let f = fixture();
        let mut insert = group("outer-boundary");
        let mut item = work("new", "new");
        item.body = vec![7; 1024].into();
        insert.work_inserts.push(item);
        let batch = CommitBatch {
            queue: f.queue.clone(),
            groups: vec![insert],
        };
        let (expected, _) = record(
            &f.queue,
            f.definition.schema(),
            &f.snapshot,
            &batch.groups[0],
            &f.job,
        );
        let exact = expected.bytes().len() as u64;
        for cap in [exact, exact - 1] {
            let plan = load_batch_preimage(
                &f.reader,
                &batch,
                &[0],
                &f.definition,
                &QueueLimits::default(),
                &f.job,
            )
            .await
            .unwrap()
            .plan(&f.reader, &ZeroClock)
            .await
            .unwrap();
            let result = plan.encode_prefix(QueueSourceGroupEncodingLimits {
                frame_bytes: 1 << 20,
                record_bytes: cap,
            });
            if cap == exact {
                assert_eq!(
                    result
                        .unwrap()
                        .record_for(&f.reader.cut().unwrap(), &f.definition, &batch, &f.job)
                        .unwrap()
                        .bytes(),
                    expected.bytes()
                );
            } else {
                assert!(matches!(result, Err(QueueError::TooLarge)));
            }
        }
    });
}
