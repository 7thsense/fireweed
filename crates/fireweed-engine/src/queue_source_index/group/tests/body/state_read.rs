//! Full state hydration shares field loading and reads each physical record once.
use super::*;
use crate::queue_source_index::load_source_state_records;
fn prepared() -> (Fixture, StateReadRequest, Vec<Option<StateRecord>>) {
    let mut f = fixture();
    let mut first = group("first-states");
    for key in ["state", "peer"] {
        first.state_updates.push(StateUpdate {
            collection: "s".into(),
            key: key.into(),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: work("", key).fields,
                value: vec![5].into(),
            },
        });
    }
    let (publication, snapshot) = record(
        &f.queue,
        f.definition.schema(),
        &RecordSnapshot::default(),
        &first,
        &f.job,
    );
    f.history[1] = publication;
    f.snapshot = snapshot;
    f.reader.facts = oracle::fold(&f.history, &f.snapshot, f.definition.schema());
    let mut change = group("second-state");
    change.state_updates.push(StateUpdate {
        collection: "s".into(),
        key: "later".into(),
        expect: StateExpect::Absent,
        change: StateChange::Put {
            fields: work("", "later").fields,
            value: vec![7, 8].into(),
        },
    });
    append(&mut f, change);
    let expected = [
        Some("later"),
        Some("peer"),
        None,
        Some("state"),
        Some("later"),
        Some("peer"),
    ]
    .into_iter()
    .map(|key| key.map(|key| f.snapshot.state[&("s".into(), key.into())].clone()))
    .collect::<Vec<_>>();
    let request = StateReadRequest {
        queue: f.queue.clone(),
        collection: "s".into(),
        keys: ["later", "peer", "missing", "state", "later", "peer"]
            .map(String::from)
            .to_vec(),
        max_bytes: encoded_len(&expected).unwrap(),
    };
    (f, request, expected)
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
fn source_state_read_preserves_order_duplicates_exact_cap_and_owned_retirement() {
    futures::executor::block_on(async {
        let (f, mut request, expected) = prepared();
        let baseline = f.admission.usage().unwrap();
        let reader = reader(&f, 0);
        let cut = reader.cut().unwrap();
        let result = load_source_state_records(
            &reader,
            cut,
            &request,
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await
        .unwrap();
        assert_eq!(reader.reads.load(Ordering::SeqCst), 2);
        assert_eq!(reader.header_reads.load(Ordering::SeqCst), 2);
        assert_eq!(
            reader.lookups.load(Ordering::SeqCst),
            6,
            "two header points and four distinct addressed keys"
        );
        assert!(f.admission.usage().unwrap().1 > baseline.1);
        let actual = result.into_value();
        assert_eq!(actual, expected);
        drop(actual);
        assert_eq!(f.admission.usage().unwrap(), baseline);
        request.max_bytes -= 1;
        assert!(matches!(
            load_source_state_records(
                &reader,
                cut,
                &request,
                &f.definition,
                &QueueLimits::default(),
                &f.job
            )
            .await,
            Err(QueueError::TooLarge)
        ));
        assert_eq!(f.admission.usage().unwrap(), baseline);
        request.max_bytes += 1;
        let abandoned = load_source_state_records(
            &reader,
            cut,
            &request,
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await
        .unwrap();
        assert!(f.admission.usage().unwrap().1 > baseline.1);
        drop(abandoned);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}
#[test]
fn source_state_read_late_record_and_field_failure_retire_all_provisional_output() {
    futures::executor::block_on(async {
        let (mut f, request, _) = prepared();
        {
            let reader = reader(&f, 2);
            let baseline = f.admission.usage().unwrap();
            assert!(matches!(
                load_source_state_records(
                    &reader,
                    reader.cut().unwrap(),
                    &request,
                    &f.definition,
                    &QueueLimits::default(),
                    &f.job
                )
                .await,
                Err(QueueError::TooLarge)
            ));
            assert_eq!(reader.reads.load(Ordering::SeqCst), 2);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
        let encoded = QueueSourceKey::Field {
            collection: Some("s"),
            key: "state",
            field: "u",
        }
        .encode()
        .unwrap();
        assert!(f.reader.facts.remove(&encoded).is_some());
        let reader = reader(&f, 0);
        let baseline = f.admission.usage().unwrap();
        assert!(matches!(
            load_source_state_records(
                &reader,
                reader.cut().unwrap(),
                &request,
                &f.definition,
                &QueueLimits::default(),
                &f.job
            )
            .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(
            reader.reads.load(Ordering::SeqCst),
            0,
            "incomplete fields refuse before historical hydration"
        );
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}
#[test]
fn source_state_read_original_scope_and_upfront_admission_precede_source_dispatch() {
    futures::executor::block_on(async {
        let (f, request, _) = prepared();
        let reader = reader(&f, 0);
        let original = reader.cut().unwrap();
        let identity = Arc::new(f.job.owned_buffer(128).unwrap());
        let foreign = QueueSourceCut::from_captured(
            &identity,
            original.address(),
            original.epoch(),
            original.through(),
            &f.job,
        )
        .unwrap();
        let baseline = f.admission.usage().unwrap();
        assert!(
            load_source_state_records(
                &reader,
                foreign,
                &request,
                &f.definition,
                &QueueLimits::default(),
                &f.job
            )
            .await
            .is_err()
        );
        assert_eq!(reader.lookups.load(Ordering::SeqCst), 0);
        let mut empty = request.clone();
        empty.keys.clear();
        assert!(
            load_source_state_records(
                &reader,
                foreign,
                &empty,
                &f.definition,
                &QueueLimits::default(),
                &f.job
            )
            .await
            .is_err()
        );
        assert_eq!(reader.lookups.load(Ordering::SeqCst), 0);
        let fill = f.job.owned_buffer((32u64 << 20) - baseline.1).unwrap();
        assert!(matches!(
            load_source_state_records(
                &reader,
                original,
                &request,
                &f.definition,
                &QueueLimits::default(),
                &f.job
            )
            .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(reader.lookups.load(Ordering::SeqCst), 0);
        assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
        drop(fill);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}

#[test]
fn source_state_read_empty_absent_tombstone_and_recreated_versions() {
    futures::executor::block_on(async {
        let mut f = fixture();
        for mode in 0..4 {
            if mode == 2 {
                let mut change = group("delete-state");
                change.state_updates.push(StateUpdate {
                    collection: "s".into(),
                    key: "state".into(),
                    expect: StateExpect::Version(1),
                    change: StateChange::Delete,
                });
                append(&mut f, change);
            } else if mode == 3 {
                let mut change = group("recreate-state");
                change.state_updates.push(StateUpdate {
                    collection: "s".into(),
                    key: "state".into(),
                    expect: StateExpect::Absent,
                    change: StateChange::Put {
                        fields: work("", "state").fields,
                        value: Default::default(),
                    },
                });
                append(&mut f, change);
            }
            let reader = reader(&f, 0);
            let keys: Vec<String> = match mode {
                0 => vec![],
                1 => vec!["missing".into(), "missing".into()],
                _ => vec!["state".into()],
            };
            let expected: Vec<_> = keys
                .iter()
                .map(|key| f.snapshot.state.get(&("s".into(), key.clone())).cloned())
                .collect();
            let request = StateReadRequest {
                queue: f.queue.clone(),
                collection: "s".into(),
                keys,
                max_bytes: encoded_len(&expected).unwrap(),
            };
            let baseline = f.admission.usage().unwrap();
            let value = load_source_state_records(
                &reader,
                reader.cut().unwrap(),
                &request,
                &f.definition,
                &QueueLimits::default(),
                &f.job,
            )
            .await
            .unwrap();
            assert_eq!(value.into_value(), expected);
            assert_eq!(
                reader.reads.load(Ordering::SeqCst),
                if mode == 3 { 1 } else { 0 }
            );
            if mode == 3 {
                assert_eq!(expected[0].as_ref().unwrap().version, 3);
            }
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
    });
}

mod admission;
