use super::super::tests::{address, bootstrap, group, inspect, seed_record};
use super::*;
use fireweed_conformance::queue_contract::TestClock;
use fireweed_engine::{
    queue_admission::QueueAdmission, queue_group_state_authority::ColdGroupStateTracker,
    queue_log_record::encode_checked_genesis, queue_original_authority::ColdOriginalTracker,
    queue_recovery::ColdQueueHistory,
};
fn input(
    request: Arc<CommitBatch>,
    definition: &AdmittedQueueDefinition,
    records: &[OwnedQueueLogRecord],
    job: &Arc<QueueReservation>,
) -> CommitPackPreparationInput {
    let mut history = ColdQueueHistory::new(&request.queue, job).unwrap();
    let mut originals = ColdOriginalTracker::new(&request, job).unwrap();
    let mut states = ColdGroupStateTracker::new(&request.queue, &request.groups, job).unwrap();
    for (offset, record) in records.iter().enumerate() {
        let checked = record.checked().unwrap();
        history.observe_record(offset as u64, &checked).unwrap();
        originals.observe_record(offset as u64, &checked).unwrap();
        states.observe_record(offset as u64, &checked).unwrap();
    }
    let progress = (
        history.next_offset(),
        history.insertion_sequence(),
        history.historical_epoch(),
    );
    let originals = Arc::new(originals.finish(&history, &records[0], progress).unwrap());
    let states = Arc::new(states.finish(&history, &records[0], progress).unwrap());
    CommitPackPreparationInput {
        positions: (0..request.groups.len()).collect(),
        request,
        originals,
        states,
        genesis: encode_checked_genesis(1, &address(), definition, &[31; 32], 16 << 20, job)
            .unwrap(),
        limits: QueueLimits::default(),
        required_progress: progress,
    }
}
fn patch(id: &str, version: u64, priority: i64) -> CommitGroup {
    let mut group = group(id);
    group.work_updates.push(UnclaimedUpdate {
        key: "a".into(),
        expect: StateExpect::Version(version),
        change: UnclaimedChange::Patch(WorkPatch {
            priority: Some(priority),
            ..Default::default()
        }),
    });
    group
}
fn state(group: &mut CommitGroup, expect: StateExpect, change: StateChange) {
    group.state_updates.push(StateUpdate {
        collection: "workflow".into(),
        key: "run".into(),
        expect,
        change,
    });
}
#[tokio::test]
async fn caller_order_unicode_rejections_state_aba_and_body_preservation_pack_atomically() {
    let directory = tempfile::tempdir().unwrap();
    let admission = QueueAdmission::new(2, 256 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let (projection, definition) =
        bootstrap(&directory.path().join("projection.db"), &job, &workers).await;
    let genesis =
        encode_checked_genesis(1, &address(), &definition, &[31; 32], 16 << 20, &job).unwrap();
    let (_, seed) = seed_record(&definition, &job, true);
    let records = vec![genesis, seed];
    let seed_copy = OwnedQueueLogRecord::from_admitted_bytes(
        bytes::Bytes::copy_from_slice(records[1].bytes()),
        job.owned_buffer((records[1].bytes().len() as u64) * 4 + 4096)
            .unwrap(),
        &job,
    )
    .unwrap();
    projection
        .apply(seed_copy, 1, &job, &workers)
        .await
        .unwrap();
    let mut first = patch("z/λ", 1, 7);
    state(
        &mut first,
        StateExpect::Absent,
        StateChange::Put {
            fields: [("phase".into(), FieldValue::Text("first".into()))].into(),
            value: bytes::Bytes::from_static(b"first"),
        },
    );
    let mut rejected = patch("a", 1, 8);
    state(&mut rejected, StateExpect::Version(1), StateChange::Delete);
    let mut deleted = patch("β", 2, 9);
    state(&mut deleted, StateExpect::Version(1), StateChange::Delete);
    let mut recreated = patch("b", 3, 10);
    state(
        &mut recreated,
        StateExpect::Absent,
        StateChange::Put {
            fields: [("phase".into(), FieldValue::Text("recreated".into()))].into(),
            value: bytes::Bytes::from_static(b"recreated"),
        },
    );
    let mut check = patch("Check", 4, 11);
    state(&mut check, StateExpect::Version(3), StateChange::Check);
    let request = Arc::new(CommitBatch {
        queue: address(),
        groups: vec![first, rejected, deleted, recreated, check],
    });
    let clock = Arc::new(TestClock::default());
    clock.set(100);
    let preparation = projection
        .prepare_ordinary_pack(
            input(request.clone(), &definition, &records, &job),
            2,
            16 << 20,
            clock,
            &job,
            &workers,
        )
        .await
        .unwrap();
    let OrdinaryPackPreparation::Prepared(prepared) = preparation else {
        panic!("expected a complete ordinary pack")
    };
    assert_eq!(prepared.response().positions(), [0, 1, 2, 3, 4]);
    assert_eq!(
        prepared
            .response()
            .receipts()
            .iter()
            .map(|receipt| receipt.request_id.as_str())
            .collect::<Vec<_>>(),
        ["z/λ", "a", "β", "b", "Check"]
    );
    assert_eq!(
        prepared.response().receipts()[1].outcome,
        GroupOutcome::Rejected(Rejection::VersionConflict)
    );
    for position in [0, 2, 3, 4] {
        assert!(matches!(
            prepared.response().receipts()[position].outcome,
            GroupOutcome::Committed { .. }
        ));
    }
    for (position, expected) in [
        (0, (Some(1), 2)),
        (2, (Some(2), 3)),
        (3, (Some(3), 4)),
        (4, (None, 5)),
    ] {
        let GroupOutcome::Committed { versions } =
            &prepared.response().receipts()[position].outcome
        else {
            panic!("committed")
        };
        let mut wanted = Vec::new();
        if let Some(version) = expected.0 {
            wanted.push(RecordVersion {
                collection: Some("workflow".into()),
                key: "run".into(),
                version,
            });
        }
        wanted.push(RecordVersion {
            collection: None,
            key: "a".into(),
            version: expected.1,
        });
        assert_eq!(versions, &wanted);
    }
    let row = inspect(
        &projection,
        "SELECT applied_next_offset,(SELECT COUNT(*) FROM q7_requests) FROM q7_queues",
        vec![],
        2,
    )
    .await;
    assert_eq!(integer(&row[0]).unwrap(), 2);
    assert_eq!(integer(&row[1]).unwrap(), 1);
    let (record, provisional, head) = prepared.into_parts();
    assert_eq!(head, 2);
    let physical_ids = match record.view().unwrap().payload() {
        QueueLogPayload::Publication(frame) => frame
            .receipts()
            .map(|receipt| receipt.unwrap().request_id.to_owned())
            .collect::<Vec<_>>(),
        _ => panic!("publication"),
    };
    assert_eq!(physical_ids, ["Check", "a", "b", "z/λ", "β"]);
    projection
        .apply(record, head, &job, &workers)
        .await
        .unwrap();
    let row = inspect(
        &projection,
        "SELECT applied_next_offset,(SELECT COUNT(*) FROM q7_requests) FROM q7_queues",
        vec![],
        2,
    )
    .await;
    assert_eq!(integer(&row[0]).unwrap(), 3);
    assert_eq!(integer(&row[1]).unwrap(), 6);
    let row=inspect(&projection,"SELECT version,priority,(SELECT length(b.value) FROM q7_bodies b WHERE b.record_id=r.id) FROM q7_records r WHERE collection=0 AND key=?1",vec![blob(b"a")],3).await;
    assert_eq!(unsigned(&row[0]).unwrap(), 5);
    assert_eq!(integer(&row[1]).unwrap(), 11);
    assert_eq!(integer(&row[2]).unwrap(), 1 << 20);
    let row=inspect(&projection,"SELECT version,(SELECT b.value FROM q7_bodies b WHERE b.record_id=r.id) FROM q7_records r WHERE collection=1 AND key=?1",vec![blob(b"run")],2).await;
    assert_eq!(unsigned(&row[0]).unwrap(), 3);
    assert_eq!(bytes(&row[1]).unwrap(), b"recreated");
    drop(provisional);
    drop(request);
    drop(records);
    drop(definition);
    drop(projection);
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

#[tokio::test]
async fn packed_unique_release_claim_reject_delete_and_reclaim_preserve_overlay() {
    let directory = tempfile::tempdir().unwrap();
    let admission = QueueAdmission::new(2, 256 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let (projection, definition) =
        bootstrap(&directory.path().join("projection.db"), &job, &workers).await;
    let genesis =
        encode_checked_genesis(1, &address(), &definition, &[31; 32], 16 << 20, &job).unwrap();
    let (_, seed) = seed_record(&definition, &job, true);
    let records = vec![genesis, seed];
    let seed_copy = OwnedQueueLogRecord::from_admitted_bytes(
        bytes::Bytes::copy_from_slice(records[1].bytes()),
        job.owned_buffer((records[1].bytes().len() as u64) * 4 + 4096)
            .unwrap(),
        &job,
    )
    .unwrap();
    projection
        .apply(seed_copy, 1, &job, &workers)
        .await
        .unwrap();
    let mut release = group("release");
    release
        .work_updates
        .push(super::super::tests::patch("a", 1, None, Some(None)));
    let mut claim = group("claim");
    claim
        .work_inserts
        .push(super::super::tests::work("c", 1, false));
    let mut reject = group("reject");
    reject
        .work_updates
        .push(super::super::tests::patch("a", 2, None, Some(Some(1))));
    let mut delete = group("delete");
    delete.work_updates.push(UnclaimedUpdate {
        key: "c".into(),
        expect: StateExpect::Version(1),
        change: UnclaimedChange::Delete,
    });
    let mut reclaim = group("reclaim");
    reclaim
        .work_updates
        .push(super::super::tests::patch("a", 2, None, Some(Some(1))));
    let request = Arc::new(CommitBatch {
        queue: address(),
        groups: vec![release, claim, reject, delete, reclaim],
    });
    let clock = Arc::new(TestClock::default());
    clock.set(100);
    let result = projection
        .prepare_ordinary_pack(
            input(request.clone(), &definition, &records, &job),
            2,
            16 << 20,
            clock,
            &job,
            &workers,
        )
        .await
        .unwrap();
    let OrdinaryPackPreparation::Prepared(prepared) = result else {
        panic!("pack")
    };
    assert_eq!(prepared.response().positions(), [0, 1, 2, 3, 4]);
    assert_eq!(
        prepared.response().receipts()[2].outcome,
        GroupOutcome::Rejected(Rejection::UniqueConflict)
    );
    for position in [0, 1, 3, 4] {
        assert!(matches!(
            prepared.response().receipts()[position].outcome,
            GroupOutcome::Committed { .. }
        ));
    }
    let (record, provisional, head) = prepared.into_parts();
    projection
        .apply(record, head, &job, &workers)
        .await
        .unwrap();
    let row = inspect(
        &projection,
        "SELECT COUNT(*) FROM q7_records WHERE collection=0 AND key=?1 AND active=1",
        vec![blob(b"c")],
        1,
    )
    .await;
    assert_eq!(integer(&row[0]).unwrap(), 0);
    let row=inspect(&projection,"SELECT r.key FROM q7_indexes i JOIN q7_records r ON r.id=i.record_id WHERE i.is_unique=1 AND i.full_key=?1",vec![blob(&fireweed_engine::queue_index::ordered_key(&[Some(&FieldValue::Integer(1))]).unwrap())],1).await;
    assert_eq!(bytes(&row[0]).unwrap(), b"a");
    drop(provisional);
    drop(request);
    drop(records);
    drop(definition);
    drop(projection);
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

struct Fixture {
    projection: Arc<QueueProjection>,
    definition: AdmittedQueueDefinition,
    records: Vec<OwnedQueueLogRecord>,
    workers: ScratchWorkers,
    job: Arc<QueueReservation>,
    admission: Arc<QueueAdmission>,
    _directory: tempfile::TempDir,
}
impl Fixture {
    async fn new(opaque: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(2, 256 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) =
            bootstrap(&directory.path().join("projection.db"), &job, &workers).await;
        let genesis =
            encode_checked_genesis(1, &address(), &definition, &[31; 32], 16 << 20, &job).unwrap();
        let (_, seed) = seed_record(&definition, &job, opaque);
        let records = vec![genesis, seed];
        let copy = OwnedQueueLogRecord::from_admitted_bytes(
            bytes::Bytes::copy_from_slice(records[1].bytes()),
            job.owned_buffer((records[1].bytes().len() as u64) * 4 + 4096)
                .unwrap(),
            &job,
        )
        .unwrap();
        projection.apply(copy, 1, &job, &workers).await.unwrap();
        Self {
            projection,
            definition,
            records,
            workers,
            job,
            admission,
            _directory: directory,
        }
    }
    fn input(&self, groups: Vec<CommitGroup>) -> CommitPackPreparationInput {
        input(
            Arc::new(CommitBatch {
                queue: address(),
                groups,
            }),
            &self.definition,
            &self.records,
            &self.job,
        )
    }
    async fn finish(self) {
        drop(self.projection);
        drop(self.definition);
        drop(self.records);
        drop(self.job);
        self.workers.close_and_drain().await.unwrap();
        assert_eq!(self.admission.usage().unwrap(), (0, 0));
    }
}
struct CountClock(std::sync::atomic::AtomicUsize);
impl Clock for CountClock {
    fn now(&self) -> fireweed_core::UtcTimestamp {
        timestamp_from_nanos(100 + self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) as i64)
    }
}
#[tokio::test]
async fn whole_group_wrapper_split_preserves_accepted_clock_and_exact_fit() {
    let mut fixture = Fixture::new(false).await;
    let clock = Arc::new(CountClock(Default::default()));
    let result = fixture
        .projection
        .prepare_ordinary_pack(
            fixture.input(vec![patch("first", 1, 7)]),
            2,
            65536,
            clock.clone(),
            &fixture.job,
            &fixture.workers,
        )
        .await
        .unwrap();
    let OrdinaryPackPreparation::Prepared(single) = result else {
        panic!("single")
    };
    let (record, provisional, _) = single.into_parts();
    let exact = record.bytes().len() as u64;
    drop(record);
    drop(provisional);
    assert_eq!(clock.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    let clock = Arc::new(CountClock(Default::default()));
    let result = fixture
        .projection
        .prepare_ordinary_pack(
            fixture.input(vec![patch("first", 1, 7), patch("second", 2, 8)]),
            2,
            exact,
            clock.clone(),
            &fixture.job,
            &fixture.workers,
        )
        .await
        .unwrap();
    let OrdinaryPackPreparation::Prepared(prefix) = result else {
        panic!("whole first prefix must fit")
    };
    assert_eq!(prefix.response().positions(), [0]);
    assert_eq!(prefix.response().receipts().len(), 1);
    assert_eq!(
        clock.0.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "the accepted first group never replans/resamples"
    );
    let (prefix_record, prefix_provisional, prefix_head) = prefix.into_parts();
    assert_eq!(prefix_record.bytes().len() as u64, exact);
    let result = fixture
        .projection
        .prepare_ordinary_pack(
            fixture.input(vec![patch("first", 1, 7)]),
            2,
            exact - 1,
            clock,
            &fixture.job,
            &fixture.workers,
        )
        .await
        .unwrap();
    assert!(matches!(result, OrdinaryPackPreparation::SingleGroup));
    let row = inspect(
        &fixture.projection,
        "SELECT applied_next_offset,(SELECT COUNT(*) FROM q7_requests) FROM q7_queues",
        vec![],
        2,
    )
    .await;
    assert_eq!(integer(&row[0]).unwrap(), 2);
    assert_eq!(integer(&row[1]).unwrap(), 1);
    let frozen = OwnedQueueLogRecord::from_admitted_bytes(
        bytes::Bytes::copy_from_slice(prefix_record.bytes()),
        fixture
            .job
            .owned_buffer(prefix_record.bytes().len() as u64 * 4 + 4096)
            .unwrap(),
        &fixture.job,
    )
    .unwrap();
    fixture
        .projection
        .apply(prefix_record, prefix_head, &fixture.job, &fixture.workers)
        .await
        .unwrap();
    fixture.records.push(frozen);
    drop(prefix_provisional);
    let row = inspect(
        &fixture.projection,
        "SELECT priority,version FROM q7_records WHERE collection=0 AND key=?1",
        vec![blob(b"a")],
        2,
    )
    .await;
    assert_eq!(integer(&row[0]).unwrap(), 7);
    assert_eq!(unsigned(&row[1]).unwrap(), 2);
    let row = inspect(&fixture.projection, "SELECT applied_next_offset,(SELECT COUNT(*) FROM q7_requests WHERE identity=?1),(SELECT COUNT(*) FROM q7_requests WHERE identity=?2) FROM q7_queues", vec![blob(b"first"),blob(b"second")], 3).await;
    assert_eq!(integer(&row[0]).unwrap(), 3);
    assert_eq!(integer(&row[1]).unwrap(), 1);
    assert_eq!(integer(&row[2]).unwrap(), 0);
    let mut remainder = fixture.input(vec![patch("first", 1, 7), patch("second", 2, 8)]);
    remainder.positions = vec![1];
    let clock = Arc::new(CountClock(Default::default()));
    let next = fixture
        .projection
        .prepare_ordinary_pack(remainder, 3, 65536, clock, &fixture.job, &fixture.workers)
        .await
        .unwrap();
    let OrdinaryPackPreparation::Prepared(next) = next else {
        panic!("unpublished suffix replans after confirmed prefix")
    };
    assert_eq!(next.response().positions(), [1]);
    let (record, provisional, head) = next.into_parts();
    fixture
        .projection
        .apply(record, head, &fixture.job, &fixture.workers)
        .await
        .unwrap();
    drop(provisional);
    let row = inspect(
        &fixture.projection,
        "SELECT priority,version FROM q7_records WHERE collection=0 AND key=?1",
        vec![blob(b"a")],
        2,
    )
    .await;
    assert_eq!(integer(&row[0]).unwrap(), 8);
    assert_eq!(unsigned(&row[1]).unwrap(), 3);
    fixture.finish().await;
}
#[tokio::test]
async fn pack_rejects_skipped_fresh_prefix_noncontiguous_positions_and_foreign_cut() {
    let fixture = Fixture::new(false).await;
    let clock = Arc::new(TestClock::default());
    clock.set(100);
    for positions in [vec![1], vec![0, 2]] {
        let mut input = fixture.input(vec![
            patch("first", 1, 7),
            patch("second", 2, 8),
            patch("third", 3, 9),
        ]);
        input.positions = positions;
        let result = fixture
            .projection
            .prepare_ordinary_pack(
                input,
                2,
                65536,
                clock.clone(),
                &fixture.job,
                &fixture.workers,
            )
            .await;
        assert!(matches!(result, Err(QueueError::Invalid(_))));
    }
    let mut input = fixture.input(vec![patch("first", 1, 7)]);
    input.required_progress.0 += 1;
    assert!(matches!(
        fixture
            .projection
            .prepare_ordinary_pack(input, 2, 65536, clock, &fixture.job, &fixture.workers)
            .await,
        Err(QueueError::OutcomeUnknown(_))
    ));
    let row = inspect(
        &fixture.projection,
        "SELECT applied_next_offset,(SELECT COUNT(*) FROM q7_requests) FROM q7_queues",
        vec![],
        2,
    )
    .await;
    assert_eq!(integer(&row[0]).unwrap(), 2);
    assert_eq!(integer(&row[1]).unwrap(), 1);
    fixture.finish().await;
}
#[tokio::test]
async fn check_sibling_cannot_suppress_mutation_field_hydration() {
    let fixture = Fixture::new(true).await;
    let mut check = group("check-first");
    check.claimed_updates.push(ClaimedUpdate {
        key: "a".into(),
        lease: LeaseCredential { token: vec![1; 32] },
        expected_version: 1,
        patch: Default::default(),
        disposition: Disposition::Check,
    });
    let mut mutate = group("mutate-next");
    mutate
        .work_updates
        .push(super::super::tests::patch("a", 1, None, Some(Some(3))));
    let clock = Arc::new(TestClock::default());
    clock.set(100);
    let result = fixture
        .projection
        .prepare_ordinary_pack(
            fixture.input(vec![check, mutate]),
            2,
            65536,
            clock,
            &fixture.job,
            &fixture.workers,
        )
        .await
        .unwrap();
    let OrdinaryPackPreparation::Prepared(prepared) = result else {
        panic!("pack")
    };
    assert!(matches!(
        prepared.response().receipts()[0].outcome,
        GroupOutcome::Rejected(_)
    ));
    assert!(matches!(
        prepared.response().receipts()[1].outcome,
        GroupOutcome::Committed { .. }
    ));
    let (record, provisional, head) = prepared.into_parts();
    fixture
        .projection
        .apply(record, head, &fixture.job, &fixture.workers)
        .await
        .unwrap();
    let row=inspect(&fixture.projection,"SELECT r.key FROM q7_indexes i JOIN q7_records r ON r.id=i.record_id WHERE i.is_unique=1 AND i.full_key=?1",vec![blob(&fireweed_engine::queue_index::ordered_key(&[Some(&FieldValue::Integer(3))]).unwrap())],1).await;
    assert_eq!(bytes(&row[0]).unwrap(), b"a");
    let row=inspect(&fixture.projection,"SELECT length(b.value) FROM q7_bodies b JOIN q7_records r ON r.id=b.record_id WHERE r.collection=0 AND r.key=?1",vec![blob(b"a")],1).await;
    assert_eq!(integer(&row[0]).unwrap(), 1 << 20);
    drop(provisional);
    fixture.finish().await;
}

#[tokio::test]
async fn later_state_check_cannot_hide_corrupt_old_fields_but_unused_body_is_ignored() {
    let mut fixture = Fixture::new(false).await;
    let clock = Arc::new(TestClock::default());
    clock.set(100);
    let mut seed = patch("seed-state", 1, 7);
    state(
        &mut seed,
        StateExpect::Absent,
        StateChange::Put {
            fields: [("phase".into(), FieldValue::Text("old".into()))].into(),
            value: bytes::Bytes::from_static(b"old"),
        },
    );
    let result = fixture
        .projection
        .prepare_ordinary_pack(
            fixture.input(vec![seed]),
            2,
            65536,
            clock.clone(),
            &fixture.job,
            &fixture.workers,
        )
        .await
        .unwrap();
    let OrdinaryPackPreparation::Prepared(prepared) = result else {
        panic!("seed")
    };
    let (record, provisional, head) = prepared.into_parts();
    let frozen = OwnedQueueLogRecord::from_admitted_bytes(
        bytes::Bytes::copy_from_slice(record.bytes()),
        fixture
            .job
            .owned_buffer(record.bytes().len() as u64 * 4 + 4096)
            .unwrap(),
        &fixture.job,
    )
    .unwrap();
    fixture
        .projection
        .apply(record, head, &fixture.job, &fixture.workers)
        .await
        .unwrap();
    fixture.records.push(frozen);
    drop(provisional);
    super::super::tests::execute_for_test(&fixture.projection,
        "UPDATE q7_fields SET value=?1 WHERE record_id IN (SELECT id FROM q7_records WHERE collection=1 AND key=?2)",
        vec![blob(&encode(&FieldValue::Text("bad".into())).unwrap()),blob(b"run")]).await;
    super::super::tests::execute_for_test(&fixture.projection,
        "UPDATE q7_bodies SET value='unneeded-corrupt-body' WHERE record_id IN (SELECT id FROM q7_records WHERE collection=1 AND key=?1)",vec![blob(b"run")]).await;
    let mut replace = patch("replace", 2, 8);
    state(
        &mut replace,
        StateExpect::Version(1),
        StateChange::Put {
            fields: [("phase".into(), FieldValue::Text("new".into()))].into(),
            value: bytes::Bytes::from_static(b"new"),
        },
    );
    let mut check = patch("later-check", 3, 9);
    state(&mut check, StateExpect::Version(2), StateChange::Check);
    let result = fixture
        .projection
        .prepare_ordinary_pack(
            fixture.input(vec![replace.clone(), check.clone()]),
            3,
            65536,
            clock.clone(),
            &fixture.job,
            &fixture.workers,
        )
        .await;
    assert!(matches!(result, Err(QueueError::OutcomeUnknown(_))));
    super::super::tests::execute_for_test(&fixture.projection,
        "UPDATE q7_fields SET value=?1 WHERE record_id IN (SELECT id FROM q7_records WHERE collection=1 AND key=?2)",
        vec![blob(&encode(&FieldValue::Text("old".into())).unwrap()),blob(b"run")]).await;
    let result = fixture
        .projection
        .prepare_ordinary_pack(
            fixture.input(vec![replace, check]),
            3,
            65536,
            clock,
            &fixture.job,
            &fixture.workers,
        )
        .await
        .unwrap();
    let OrdinaryPackPreparation::Prepared(prepared) = result else {
        panic!("replacement")
    };
    assert!(
        prepared
            .response()
            .receipts()
            .iter()
            .all(|receipt| matches!(receipt.outcome, GroupOutcome::Committed { .. }))
    );
    let (record, provisional, head) = prepared.into_parts();
    fixture
        .projection
        .apply(record, head, &fixture.job, &fixture.workers)
        .await
        .unwrap();
    let row=inspect(&fixture.projection,"SELECT b.value FROM q7_bodies b JOIN q7_records r ON r.id=b.record_id WHERE r.collection=1 AND r.key=?1",vec![blob(b"run")],1).await;
    assert_eq!(bytes(&row[0]).unwrap(), b"new");
    drop(provisional);
    fixture.finish().await;
}

#[tokio::test]
async fn known_conflicting_prefix_classifies_before_unused_current_state_fields() {
    let mut fixture = Fixture::new(false).await;
    let clock = Arc::new(TestClock::default());
    clock.set(100);
    let mut first = patch("known", 1, 7);
    state(
        &mut first,
        StateExpect::Absent,
        StateChange::Put {
            fields: [("phase".into(), FieldValue::Text("old".into()))].into(),
            value: bytes::Bytes::from_static(b"old"),
        },
    );
    let result = fixture
        .projection
        .prepare_ordinary_pack(
            fixture.input(vec![first.clone()]),
            2,
            65536,
            clock.clone(),
            &fixture.job,
            &fixture.workers,
        )
        .await
        .unwrap();
    let OrdinaryPackPreparation::Prepared(prepared) = result else {
        panic!("seed")
    };
    let (record, provisional, head) = prepared.into_parts();
    let frozen = OwnedQueueLogRecord::from_admitted_bytes(
        bytes::Bytes::copy_from_slice(record.bytes()),
        fixture
            .job
            .owned_buffer(record.bytes().len() as u64 * 4 + 4096)
            .unwrap(),
        &fixture.job,
    )
    .unwrap();
    fixture
        .projection
        .apply(record, head, &fixture.job, &fixture.workers)
        .await
        .unwrap();
    fixture.records.push(frozen);
    drop(provisional);
    super::super::tests::execute_for_test(&fixture.projection,
        "UPDATE q7_fields SET value=?1 WHERE record_id IN (SELECT id FROM q7_records WHERE collection=1 AND key=?2)",
        vec![blob(&encode(&FieldValue::Text("bad".into())).unwrap()),blob(b"run")]).await;
    first.work_updates[0] = super::super::tests::patch("a", 1, Some(999), None);
    let mut fresh = patch("fresh", 2, 8);
    state(&mut fresh, StateExpect::Version(1), StateChange::Check);
    let mut input = fixture.input(vec![first, fresh]);
    input.positions = vec![1];
    let result = fixture
        .projection
        .prepare_ordinary_pack(input, 3, 65536, clock, &fixture.job, &fixture.workers)
        .await
        .unwrap();
    let OrdinaryPackPreparation::Prepared(prepared) = result else {
        panic!("known conflict does not hydrate old state or suppress fresh sibling")
    };
    assert_eq!(prepared.response().positions(), [1]);
    assert!(matches!(
        prepared.response().receipts()[0].outcome,
        GroupOutcome::Committed { .. }
    ));
    drop(prepared);
    fixture.finish().await;
}
#[tokio::test]
async fn pack_proof_owners_survive_cancel_and_panic_until_owned_transaction_cleanup() {
    let fixture = Fixture::new(false).await;
    let baseline = fixture.admission.usage().unwrap();
    for panic in [true, false] {
        let terminal = Arc::new(tokio::sync::Notify::new());
        let signal = terminal.clone();
        let owned_job = Arc::new(
            fixture
                .admission
                .reserve(0)
                .unwrap()
                .with_terminal_callback(move || signal.notify_one())
                .unwrap(),
        );
        let definition =
            AdmittedQueueDefinition::from_raw(fixture.definition.raw(), &owned_job).unwrap();
        let records = fixture
            .records
            .iter()
            .map(|record| {
                OwnedQueueLogRecord::from_admitted_bytes(
                    bytes::Bytes::copy_from_slice(record.bytes()),
                    owned_job
                        .owned_buffer(record.bytes().len() as u64 * 4 + 4096)
                        .unwrap(),
                    &owned_job,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let input = input(
            Arc::new(CommitBatch {
                queue: address(),
                groups: vec![patch("one", 1, 7), patch("two", 2, 8)],
            }),
            &definition,
            &records,
            &owned_job,
        );
        let weak = Arc::downgrade(&input.states);
        drop(records);
        drop(definition);
        let pause = Arc::new(ApplyPause {
            after_commit: false,
            fail_reply: false,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::watch::channel(false).0,
        });
        *fixture.projection.pause.lock().await = Some(pause.clone());
        fixture
            .projection
            .panic_after_command
            .store(panic, std::sync::atomic::Ordering::Release);
        let caller = tokio::spawn({
            let projection = fixture.projection.clone();
            let workers = fixture.workers.clone();
            async move {
                let clock = Arc::new(TestClock::default());
                clock.set(100);
                projection
                    .prepare_ordinary_pack(input, 2, 65536, clock, &owned_job, &workers)
                    .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(30), pause.entered.notified())
            .await
            .unwrap();
        assert!(weak.upgrade().is_some());
        if panic {
            pause.release.send(true).unwrap();
            assert!(matches!(
                caller.await.unwrap(),
                Err(QueueError::OutcomeUnknown(_))
            ));
        } else {
            caller.abort();
            assert!(matches!(caller.await,Err(error) if error.is_cancelled()));
            assert!(weak.upgrade().is_some());
            pause.release.send(true).unwrap();
        }
        tokio::time::timeout(std::time::Duration::from_secs(30), terminal.notified())
            .await
            .unwrap();
        assert!(weak.upgrade().is_none());
        assert_eq!(fixture.admission.usage().unwrap(), baseline);
        let writer = fixture.projection.writer.lock().await;
        assert!(
            writer
                .as_ref()
                .is_none_or(|connection| matches!(connection.is_autocommit(), Ok(true)))
        );
    }
    fixture.finish().await;
}

struct ExpiryClock(std::sync::atomic::AtomicUsize);
impl Clock for ExpiryClock {
    fn now(&self) -> fireweed_core::UtcTimestamp {
        timestamp_from_nanos(
            1_000_049 + self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) as i64,
        )
    }
}
#[tokio::test]
async fn each_group_samples_its_own_clock_across_real_lease_expiry() {
    let mut fixture = Fixture::new(false).await;
    let clock = Arc::new(TestClock::default());
    clock.set(50);
    let prepared = fixture
        .projection
        .prepare_claim(
            ClaimReplayInput {
                original: None,
                genesis: encode_checked_genesis(
                    1,
                    &address(),
                    &fixture.definition,
                    &[31; 32],
                    65536,
                    &fixture.job,
                )
                .unwrap(),
                request: ClaimRequest {
                    queue: address(),
                    request_id: "clock-lease".into(),
                    filter: Filter::default(),
                    order: ClaimOrder::PriorityAscending,
                    eligible_at: 50,
                    lease_nanos: 1_000_000,
                    max_records: 2,
                    max_bytes: 65536,
                    fields: None,
                    include_body: false,
                },
                now: 0,
                limits: QueueLimits::default(),
                required_next_offset: 2,
                required_insertion_sequence: 2,
                required_last_record_epoch: 1,
            },
            2,
            65536,
            clock,
            &fixture.job,
            &fixture.workers,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(prepared.records().len(), 2);
    assert!(
        prepared
            .records()
            .iter()
            .all(|record| record.record.lease_expires_at == Some(1_000_050))
    );
    let groups = prepared
        .records()
        .iter()
        .enumerate()
        .map(|(position, record)| {
            let mut group = group(if position == 0 {
                "before-expiry"
            } else {
                "at-expiry"
            });
            group.claimed_updates.push(ClaimedUpdate {
                key: record.record.work.key.clone(),
                lease: record.lease.clone(),
                expected_version: record.record.version,
                patch: Default::default(),
                disposition: Disposition::Check,
            });
            group
        })
        .collect();
    let (record, provisional, head) = prepared.into_parts();
    let frozen = OwnedQueueLogRecord::from_admitted_bytes(
        bytes::Bytes::copy_from_slice(record.bytes()),
        fixture
            .job
            .owned_buffer(record.bytes().len() as u64 * 4 + 4096)
            .unwrap(),
        &fixture.job,
    )
    .unwrap();
    fixture
        .projection
        .apply(record, head, &fixture.job, &fixture.workers)
        .await
        .unwrap();
    fixture.records.push(frozen);
    drop(provisional);
    let clock = Arc::new(ExpiryClock(Default::default()));
    let result = fixture
        .projection
        .prepare_ordinary_pack(
            fixture.input(groups),
            3,
            65536,
            clock.clone(),
            &fixture.job,
            &fixture.workers,
        )
        .await
        .unwrap();
    let OrdinaryPackPreparation::Prepared(prepared) = result else {
        panic!("lease checks pack")
    };
    assert_eq!(
        prepared.response().receipts()[0].outcome,
        GroupOutcome::Committed { versions: vec![] }
    );
    assert_eq!(
        prepared.response().receipts()[1].outcome,
        GroupOutcome::Rejected(Rejection::StaleLease)
    );
    assert_eq!(clock.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    let (record, provisional, head) = prepared.into_parts();
    fixture
        .projection
        .apply(record, head, &fixture.job, &fixture.workers)
        .await
        .unwrap();
    drop(provisional);
    let row = inspect(
        &fixture.projection,
        "SELECT COUNT(*) FROM q7_records WHERE collection=0 AND version=?1",
        vec![blob(&2u64.to_be_bytes())],
        1,
    )
    .await;
    assert_eq!(integer(&row[0]).unwrap(), 2);
    fixture.finish().await;
}

#[tokio::test]
async fn administration_is_a_barrier_and_late_fallback_fit_precedes_any_pack() {
    use fireweed_core::queue_administration::{
        AdministrativeAction, AdministrativeEntry, AdministrativeIntent, AdministrativeOperation,
        WorkSelector,
    };
    let fixture = Fixture::new(false).await;
    let mut admin = group("administration");
    admin.administration = Some(AdministrativeCommit {
        intent: AdministrativeIntent {
            evaluated_at: 100,
            before_fields: None,
            operation: AdministrativeOperation::Addressed(vec![AdministrativeEntry {
                key: "a".into(),
                expected_version: None,
                precondition: WorkSelector::all(vec![]),
                action: AdministrativeAction::Delete {
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                },
            }]),
        },
        max_result_bytes: 65536,
    });
    let clock = Arc::new(TestClock::default());
    clock.set(100);
    let mut ordinary_prefix = fixture.input(vec![
        patch("first", 1, 7),
        admin.clone(),
        patch("third", 2, 8),
    ]);
    ordinary_prefix.positions = vec![0];
    let positive = fixture
        .projection
        .prepare_ordinary_pack(
            ordinary_prefix,
            2,
            65536,
            clock.clone(),
            &fixture.job,
            &fixture.workers,
        )
        .await
        .unwrap();
    let OrdinaryPackPreparation::Prepared(positive) = positive else {
        panic!("ordinary prefix before a valid administration barrier is allowed")
    };
    assert_eq!(positive.response().positions(), [0]);
    drop(positive);
    let result = fixture
        .projection
        .prepare_ordinary_pack(
            fixture.input(vec![
                patch("first", 1, 7),
                admin.clone(),
                patch("third", 2, 8),
            ]),
            2,
            65536,
            clock.clone(),
            &fixture.job,
            &fixture.workers,
        )
        .await;
    assert!(matches!(result, Err(QueueError::Invalid(_))));
    admin.administration.as_mut().unwrap().max_result_bytes = 1;
    let mut input = fixture.input(vec![patch("first", 1, 7), admin]);
    input.positions = vec![0];
    let result = fixture
        .projection
        .prepare_ordinary_pack(input, 2, 65536, clock, &fixture.job, &fixture.workers)
        .await;
    assert!(matches!(result, Err(QueueError::TooLarge)));
    let row = inspect(
        &fixture.projection,
        "SELECT applied_next_offset,(SELECT COUNT(*) FROM q7_requests) FROM q7_queues",
        vec![],
        2,
    )
    .await;
    assert_eq!(integer(&row[0]).unwrap(), 2);
    assert_eq!(integer(&row[1]).unwrap(), 1);
    fixture.finish().await;
}
