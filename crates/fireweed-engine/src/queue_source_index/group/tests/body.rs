//! Exact body correspondence is stronger than a checksum-valid byte interval.
use super::*;
use crate::queue_publication::{BodyCommandRef, EffectRef};
use crate::queue_source_index::{QueueSourceWorkBodyEvidence, verify_source_work_bodies};
fn evidence<'a>(f: &'a Fixture, key: &str) -> QueueSourceWorkBodyEvidence<'a> {
    let scratch = f.job.owned_buffer(4096 + key.len() as u64 * 4).unwrap();
    let encoded = QueueSourceKey::Record {
        collection: None,
        key,
    }
    .encode()
    .unwrap();
    let point = f
        .reader
        .cut()
        .unwrap()
        .point_from_source(&encoded, f.reader.facts.get(&encoded).map(Vec::as_slice));
    let result =
        QueueSourceWorkBodyEvidence::from_source(f.reader.cut().unwrap(), key, point, &f.job)
            .unwrap()
            .unwrap();
    drop(encoded);
    drop(scratch);
    result
}
fn replace_descriptor(f: &mut Fixture, key: &str, payload: QueueSourcePayload) {
    let encoded = QueueSourceKey::Record {
        collection: None,
        key,
    }
    .encode()
    .unwrap();
    let mut fact: QueueSourceRecord = decode(&f.reader.facts[&encoded]).unwrap();
    let Some(QueueSourceLive::Work { body, .. }) = fact.live.as_mut() else {
        panic!()
    };
    *body = payload;
    f.reader.facts.insert(encoded, encode(&fact).unwrap());
}
#[test]
fn source_work_bodies_preserve_positions_aliases_empty_and_frame_ownership() {
    let mut empty = work("empty", "empty");
    empty.body = Default::default();
    let mut distinct = work("b", "b");
    distinct.body = vec![9, 8, 7].into();
    let f = fixture_with_work(vec![work("a", "a"), distinct, empty]);
    let baseline = f.admission.usage().unwrap();
    let a = evidence(&f, "a");
    let b = evidence(&f, "b");
    let empty = evidence(&f, "empty");
    let checked = f.history[1].checked().unwrap();
    let bodies = verify_source_work_bodies(
        &[&b, &empty, &a, &a],
        &checked,
        1,
        f.definition.pin(),
        &f.job,
    )
    .unwrap();
    assert_eq!(bodies.body(0).unwrap(), [9, 8, 7]);
    assert!(bodies.body(1).unwrap().is_empty());
    assert_eq!(bodies.body(2).unwrap(), [1, 2]);
    assert_eq!(bodies.body(3).unwrap(), [1, 2]);
    assert!(bodies.body(4).is_err());
    // Verified raw bodies borrow the actual retained frame, not the facts table.
    let frame = f.history[1].bytes();
    let raw = bodies.body(0).unwrap();
    assert!(raw.as_ptr().addr() >= frame.as_ptr().addr());
    assert!(raw.as_ptr().addr() + raw.len() <= frame.as_ptr().addr() + frame.len());
    drop(a);
    drop(b);
    drop(empty);
    assert_eq!(bodies.body(0).unwrap(), [9, 8, 7]);
    drop(bodies);
    assert_eq!(f.admission.usage().unwrap(), baseline);
}
#[test]
fn source_work_body_rejects_other_key_equal_bytes_valid_subspan_state_and_receipt() {
    for variant in 0..4 {
        let mut f = fixture();
        let payload = {
            let checked = f.history[1].checked().unwrap();
            let QueueLogPayload::Publication(publication) = checked.view().payload() else {
                panic!()
            };
            let a = publication
                .commands()
                .map(Result::unwrap)
                .find_map(|c| match c.effect {
                    EffectRef::WorkPut {
                        body: BodyCommandRef::Replace(bytes),
                        ..
                    } if c.identity.key == "a" => Some(bytes),
                    _ => None,
                })
                .unwrap();
            let raw = match variant {
                0 => publication
                    .commands()
                    .map(Result::unwrap)
                    .find_map(|c| match c.effect {
                        EffectRef::WorkPut {
                            body: BodyCommandRef::Replace(bytes),
                            ..
                        } if c.identity.key == "b" => Some(bytes),
                        _ => None,
                    })
                    .unwrap(),
                1 => &a[1..],
                2 => publication
                    .commands()
                    .map(Result::unwrap)
                    .find_map(|c| match c.effect {
                        EffectRef::StatePut { value, .. } => Some(value),
                        _ => None,
                    })
                    .unwrap(),
                _ => publication
                    .receipts()
                    .next()
                    .unwrap()
                    .unwrap()
                    .encoded_outcome(),
            };
            if variant == 0 {
                assert_eq!(raw, a);
                assert_ne!(raw.as_ptr(), a.as_ptr());
            }
            QueueSourcePayload::from_checked(&checked, 1, raw).unwrap()
        };
        replace_descriptor(&mut f, "a", payload);
        let a = evidence(&f, "a");
        let checked = f.history[1].checked().unwrap();
        let baseline = f.admission.usage().unwrap();
        assert!(matches!(
            verify_source_work_bodies(&[&a], &checked, 1, f.definition.pin(), &f.job),
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(f.admission.usage().unwrap(), baseline);
    }
}
#[test]
fn source_work_body_rejects_foreign_cut_job_offset_and_definition() {
    let f = fixture();
    let other = fixture();
    assert_eq!(f.history[1].bytes(), other.history[1].bytes());
    let a = evidence(&f, "a");
    let foreign = evidence(&other, "a");
    let checked = f.history[1].checked().unwrap();
    let baseline = f.admission.usage().unwrap();
    assert!(
        verify_source_work_bodies(&[&a, &foreign], &checked, 1, f.definition.pin(), &f.job)
            .is_err()
    );
    assert!(
        verify_source_work_bodies(
            &[&a],
            &other.history[1].checked().unwrap(),
            1,
            f.definition.pin(),
            &f.job
        )
        .is_err()
    );
    assert!(verify_source_work_bodies(&[&a], &checked, 0, f.definition.pin(), &f.job).is_err());
    let mut schema = f.definition.schema().clone();
    schema.max_attempts += 1;
    let definition =
        AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &f.job).unwrap();
    assert!(verify_source_work_bodies(&[&a], &checked, 1, definition.pin(), &f.job).is_err());
    drop(definition);
    assert_eq!(f.admission.usage().unwrap(), baseline);
    // Equal numeric cut/job with a different opaque owner is still foreign.
    let identity = Arc::new(f.job.owned_buffer(128).unwrap());
    let original = f.reader.cut().unwrap();
    let cut = QueueSourceCut::from_captured(
        &identity,
        original.address(),
        original.epoch(),
        original.through(),
        &f.job,
    )
    .unwrap();
    let key = QueueSourceKey::Record {
        collection: None,
        key: "a",
    }
    .encode()
    .unwrap();
    let point = original.point_from_source(&key, f.reader.facts.get(&key).map(Vec::as_slice));
    assert!(QueueSourceWorkBodyEvidence::from_source(cut, "a", point, &f.job).is_err());
}
#[test]
fn source_work_body_rejects_independently_valid_same_job_mixed_cuts() {
    let f = fixture();
    let a = evidence(&f, "a");
    let identity = Arc::new(f.job.owned_buffer(128).unwrap());
    let original = f.reader.cut().unwrap();
    let cut = QueueSourceCut::from_captured(
        &identity,
        original.address(),
        original.epoch(),
        original.through(),
        &f.job,
    )
    .unwrap();
    let key = QueueSourceKey::Record {
        collection: None,
        key: "a",
    }
    .encode()
    .unwrap();
    let point = cut.point_from_source(&key, f.reader.facts.get(&key).map(Vec::as_slice));
    let b = QueueSourceWorkBodyEvidence::from_source(cut, "a", point, &f.job)
        .unwrap()
        .unwrap();
    let checked = f.history[1].checked().unwrap();
    assert!(verify_source_work_bodies(&[&a], &checked, 1, f.definition.pin(), &f.job).is_ok());
    assert!(verify_source_work_bodies(&[&b], &checked, 1, f.definition.pin(), &f.job).is_ok());
    assert!(verify_source_work_bodies(&[&a, &b], &checked, 1, f.definition.pin(), &f.job).is_err());
}
fn append(f: &mut Fixture, group: CommitGroup) {
    let (publication, snapshot) =
        record(&f.queue, f.definition.schema(), &f.snapshot, &group, &f.job);
    {
        let checked = publication.checked().unwrap();
        let QueueLogPayload::Publication(view) = checked.view().payload() else {
            panic!()
        };
        let stored = view
            .receipts()
            .map(Result::unwrap)
            .find(|r| r.request_id == group.request_id)
            .unwrap();
        let (receipt, owner) = crate::queue_publication::decode_original_commit_outcome(
            &f.queue,
            &group,
            3,
            stored.encoded_outcome(),
            &f.job,
        )
        .unwrap();
        assert!(matches!(receipt.outcome, GroupOutcome::Committed { .. }));
        drop(receipt);
        drop(owner);
    }
    f.history.push(publication);
    f.snapshot = snapshot;
    f.reader.facts = oracle::fold(&f.history, &f.snapshot, f.definition.schema());
    f.reader.through = f.history.len() as u64;
}
#[test]
fn source_work_body_preserve_accepts_old_replacement_with_current_newer_metadata() {
    let mut f = fixture();
    let mut update = group("metadata-only");
    update.work_updates.push(UnclaimedUpdate {
        key: "a".into(),
        expect: StateExpect::Version(1),
        change: UnclaimedChange::Patch(WorkPatch {
            priority: Some(9),
            ..Default::default()
        }),
    });
    append(&mut f, update);
    assert_eq!(f.snapshot.work["a"].record.version, 2);
    assert_eq!(f.snapshot.work["a"].record.work.priority, 9);
    let key = QueueSourceKey::Record {
        collection: None,
        key: "a",
    }
    .encode()
    .unwrap();
    let fact: QueueSourceRecord = decode(&f.reader.facts[&key]).unwrap();
    assert_eq!(fact.highwater, 2);
    let newer = f.history[2].checked().unwrap();
    let QueueLogPayload::Publication(view) = newer.view().payload() else {
        panic!()
    };
    assert!(
        view.commands()
            .map(Result::unwrap)
            .any(|c| c.identity.key == "a"
                && matches!(
                    c.effect,
                    EffectRef::WorkPut {
                        body: BodyCommandRef::Preserve,
                        ..
                    }
                ))
    );
    let a = evidence(&f, "a");
    assert_eq!(a.source_offset(), 1);
    let bodies = verify_source_work_bodies(
        &[&a],
        &f.history[1].checked().unwrap(),
        1,
        f.definition.pin(),
        &f.job,
    )
    .unwrap();
    assert_eq!(bodies.body(0).unwrap(), [1, 2]);
    assert!(
        verify_source_work_bodies(
            &[&a],
            &f.history[2].checked().unwrap(),
            2,
            f.definition.pin(),
            &f.job
        )
        .is_err()
    );
}
#[test]
fn source_work_body_rejects_old_incarnation_and_future_command_version() {
    for future in [false, true] {
        let mut f = fixture();
        if future {
            let mut update = group("replace");
            update.work_updates.push(UnclaimedUpdate {
                key: "a".into(),
                expect: StateExpect::Version(1),
                change: UnclaimedChange::Patch(WorkPatch {
                    body: Some(vec![9].into()),
                    ..Default::default()
                }),
            });
            append(&mut f, update);
        }
        let key = QueueSourceKey::Record {
            collection: None,
            key: "a",
        }
        .encode()
        .unwrap();
        let mut fact: QueueSourceRecord = decode(&f.reader.facts[&key]).unwrap();
        let Some(QueueSourceLive::Work { metadata, .. }) = fact.live.as_mut() else {
            panic!()
        };
        if future {
            metadata.version = 1;
            fact.highwater = 1;
        } else {
            metadata.insertion_sequence += 100;
        }
        f.reader.facts.insert(key, encode(&fact).unwrap());
        let a = evidence(&f, "a");
        let offset = a.source_offset();
        assert!(
            verify_source_work_bodies(
                &[&a],
                &f.history[offset as usize].checked().unwrap(),
                offset,
                f.definition.pin(),
                &f.job
            )
            .is_err()
        );
    }
}

#[test]
fn source_work_body_second_alias_cannot_replace_complete_fact() {
    let f = fixture();
    let valid = evidence(&f, "a");
    let checked = f.history[1].checked().unwrap();
    let QueueLogPayload::Publication(view) = checked.view().payload() else {
        panic!()
    };
    let raw = view
        .commands()
        .map(Result::unwrap)
        .find_map(|c| match c.effect {
            EffectRef::WorkPut {
                body: BodyCommandRef::Replace(bytes),
                ..
            } if c.identity.key == "b" => Some(bytes),
            _ => None,
        })
        .unwrap();
    let scratch = f.job.owned_buffer(8192).unwrap();
    let key = QueueSourceKey::Record {
        collection: None,
        key: "a",
    }
    .encode()
    .unwrap();
    let mut fact: QueueSourceRecord = decode(&f.reader.facts[&key]).unwrap();
    let Some(QueueSourceLive::Work { body, .. }) = fact.live.as_mut() else {
        panic!()
    };
    *body = QueueSourcePayload::from_checked(&checked, 1, raw).unwrap();
    let forged = encode(&fact).unwrap();
    // Deliberately contradictory same-cut port output, independently admitted
    // as a second alias. Agreement with the first alias cannot substitute for
    // validating each declaration against the actual literal body command.
    let cut = f.reader.cut().unwrap();
    let alias = QueueSourceWorkBodyEvidence::from_source(
        cut,
        "a",
        cut.point_from_source(&key, Some(&forged)),
        &f.job,
    )
    .unwrap()
    .unwrap();
    assert!(
        verify_source_work_bodies(&[&valid, &valid], &checked, 1, f.definition.pin(), &f.job)
            .is_ok()
    );
    assert!(
        verify_source_work_bodies(&[&valid, &alias], &checked, 1, f.definition.pin(), &f.job)
            .is_err()
    );
    assert!(
        verify_source_work_bodies(&[&alias, &valid], &checked, 1, f.definition.pin(), &f.job)
            .is_err()
    );
    drop(alias);
    drop(forged);
    drop(key);
    drop(scratch);
}

struct BodyReader<'a> {
    fixture: &'a Fixture,
    reads: AtomicUsize,
    fail_at: usize,
}
impl QueueSourceReader for BodyReader<'_> {
    type Point = Vec<u8>;
    type Range = (Vec<u8>, Vec<u8>);
    fn cut(&self) -> QueueResult<QueueSourceCut<'_>> {
        self.fixture.reader.cut()
    }
    async fn lookup(&self, key: &[u8]) -> QueueResult<Self::Point> {
        self.fixture.reader.lookup(key).await
    }
    fn point<'a>(&'a self, owner: &'a Self::Point) -> QueueResult<QueueSourcePoint<'a>> {
        self.fixture.reader.point(owner)
    }
    async fn complete_range(&self, lower: &[u8], upper: &[u8]) -> QueueResult<Self::Range> {
        self.fixture.reader.complete_range(lower, upper).await
    }
    fn visit_range(
        &self,
        owner: &Self::Range,
        lower: &[u8],
        upper: &[u8],
        visitor: &mut dyn for<'a> FnMut(QueueSourcePoint<'a>) -> QueueResult<()>,
    ) -> QueueResult<()> {
        self.fixture
            .reader
            .visit_range(owner, lower, upper, visitor)
    }
}
impl<'a> QueueSourceRecordReader for BodyReader<'a> {
    type Record = &'a OwnedQueueLogRecord;
    async fn read_record(
        &self,
        cut: &QueueSourceCut<'_>,
        offset: u64,
    ) -> QueueResult<Self::Record> {
        if !self.cut()?.same_cut(cut) {
            return Err(unknown("fixture record cut differs"));
        }
        let ordinal = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        if ordinal == self.fail_at {
            return Err(QueueError::TooLarge);
        }
        let history: &'a [OwnedQueueLogRecord] = &self.fixture.history;
        history
            .get(offset as usize)
            .ok_or_else(|| unknown("fixture record missing"))
    }
    fn checked_record<'r>(
        &'r self,
        owner: &'r Self::Record,
        cut: &QueueSourceCut<'_>,
        offset: u64,
    ) -> QueueResult<crate::queue_log_record::CheckedQueueLogRecord<'r>> {
        if !self.cut()?.same_cut(cut)
            || !self
                .fixture
                .history
                .get(offset as usize)
                .is_some_and(|expected| std::ptr::eq(expected, *owner))
        {
            return Err(unknown("fixture returned carrier differs"));
        }
        owner.checked()
    }
}
fn administrative_body(id: &str, key: &str, version: u64, body: &[u8]) -> CommitGroup {
    let mut request = group(id);
    request.work_updates.push(UnclaimedUpdate {
        key: key.into(),
        expect: StateExpect::Version(version),
        change: UnclaimedChange::Administrative {
            patch: WorkPatch {
                body: Some(body.to_vec().into()),
                ..Default::default()
            },
            lifecycle: AdministrativeLifecycle::Keep,
            lease_guard: AdministrativeLeaseGuard::RejectActive,
        },
    });
    request
}
#[test]
fn source_work_body_materialization_matches_baseline_equal_changed_and_empty() {
    futures::executor::block_on(async {
        for (old, new) in [
            (vec![1, 2], vec![1, 2]),
            (vec![1, 2], vec![9]),
            (vec![], vec![]),
            (vec![], vec![1]),
        ] {
            let mut item = work("a", "a");
            item.body = old.clone().into();
            let f = fixture_with_work(vec![item]);
            let reader = BodyReader {
                fixture: &f,
                reads: AtomicUsize::new(0),
                fail_at: 0,
            };
            let baseline = f.admission.usage().unwrap();
            let request = administrative_body("body-change", "a", 1, &new);
            let preimage = load_group_preimage(
                &reader,
                &f.queue,
                &request,
                &f.definition,
                &QueueLimits::default(),
                &f.job,
            )
            .await
            .unwrap();
            assert!(preimage.snapshot.work["a"].body_loaded);
            assert_eq!(preimage.snapshot.work["a"].record.work.body.as_ref(), old);
            assert_eq!(reader.reads.load(Ordering::SeqCst), 1);
            let plan = preimage.plan(&ZeroClock).unwrap();
            let mut full = f.snapshot.clone();
            let expected = crate::queue_planner::plan_commit_group(
                &request,
                &mut full,
                0,
                f.definition.schema(),
            )
            .unwrap();
            assert_eq!(plan.receipt(), &expected.receipt);
            let GroupOutcome::Committed { versions } = &plan.receipt().outcome else {
                panic!()
            };
            assert_eq!(versions.len(), 1);
            assert_eq!(versions[0].version, if old == new { 1 } else { 2 });
            assert_eq!(
                matches!(
                    plan.delta.work_bodies["a"],
                    crate::queue_body::ResolvedBody::Preserve
                ),
                old == new
            );
            drop(plan);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
    });
}
#[test]
fn source_work_body_batch_reads_shared_frame_once_and_preserves_evolving_overlay() {
    futures::executor::block_on(async {
        let f = fixture();
        let reader = BodyReader {
            fixture: &f,
            reads: AtomicUsize::new(0),
            fail_at: 0,
        };
        let batch = CommitBatch {
            queue: f.queue.clone(),
            groups: vec![
                administrative_body("a-change", "a", 1, &[5]),
                administrative_body("a-noop", "a", 2, &[5]),
                administrative_body("b-empty", "b", 1, &[]),
            ],
        };
        let baseline = f.admission.usage().unwrap();
        let preimage = load_batch_preimage(
            &reader,
            &batch,
            &[0, 1, 2],
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await
        .unwrap();
        assert_eq!(reader.reads.load(Ordering::SeqCst), 1);
        let plan = preimage.plan(&reader, &ZeroClock).await.unwrap();
        let mut full = f.snapshot.clone();
        let expected = batch
            .groups
            .iter()
            .map(|g| {
                crate::queue_planner::plan_commit_group(g, &mut full, 0, f.definition.schema())
                    .unwrap()
                    .receipt
            })
            .collect::<Vec<_>>();
        assert_eq!(plan.receipts().cloned().collect::<Vec<_>>(), expected);
        let GroupOutcome::Committed { versions } = &expected[1].outcome else {
            panic!()
        };
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].version, 2);
        assert_eq!(reader.reads.load(Ordering::SeqCst), 1);
        drop(plan);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}
#[test]
fn source_work_body_late_record_failure_retires_partial_copies_without_refunding_reads() {
    futures::executor::block_on(async {
        let mut f = fixture();
        append(&mut f, administrative_body("seed-later-body", "a", 1, &[8]));
        let reader = BodyReader {
            fixture: &f,
            reads: AtomicUsize::new(0),
            fail_at: 2,
        };
        let batch = CommitBatch {
            queue: f.queue.clone(),
            groups: vec![
                administrative_body("a-next", "a", 2, &[9]),
                administrative_body("b-next", "b", 1, &[9]),
            ],
        };
        let baseline = f.admission.usage().unwrap();
        assert!(matches!(
            load_batch_preimage(
                &reader,
                &batch,
                &[0, 1],
                &f.definition,
                &QueueLimits::default(),
                &f.job
            )
            .await,
            Err(QueueError::TooLarge)
        ));
        assert_eq!(reader.reads.load(Ordering::SeqCst), 2);
        assert_eq!(f.admission.usage().unwrap(), baseline);
        let recovered = load_batch_preimage(
            &reader,
            &batch,
            &[0, 1],
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await
        .unwrap();
        assert_eq!(reader.reads.load(Ordering::SeqCst), 4);
        drop(recovered);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}
#[test]
fn source_work_body_metadata_only_update_does_not_hydrate() {
    futures::executor::block_on(async {
        let f = fixture();
        let reader = BodyReader {
            fixture: &f,
            reads: AtomicUsize::new(0),
            fail_at: 1,
        };
        let mut request = group("metadata-only");
        request.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                priority: Some(9),
                ..Default::default()
            }),
        });
        let preimage = load_group_preimage(
            &reader,
            &f.queue,
            &request,
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await
        .unwrap();
        assert!(!preimage.snapshot.work["a"].body_loaded);
        assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
    });
}
