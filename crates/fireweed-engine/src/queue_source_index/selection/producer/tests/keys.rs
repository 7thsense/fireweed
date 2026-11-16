use super::*;
use fireweed_core::queue_administration::{SelectorNode, WorkSelector};
fn selected(keys: &[&str]) -> WorkSelector {
    WorkSelector::leaf(SelectorNode::Keys(
        keys.iter().map(|key| (*key).into()).collect(),
    ))
}
macro_rules! start_keys {
    ($f:ident, $intent:ident) => {
        QueueSourceCandidateProducer::begin(
            $f.reader.take().unwrap(),
            $f.scratch.take().unwrap(),
            &$f.definition,
            QueueSourceSelectionRequest::Administration {
                queue: &$f.queue,
                intent: &$intent,
            },
            0,
            &QueueLimits::default(),
            $f.job.clone(),
            $f.budget.clone(),
        )
        .await
        .unwrap()
    };
}
fn tombstone(f: &mut Fixture, key: &str, highwater: u64, field_count: u64) {
    let fact = QueueSourceRecord {
        highwater,
        field_count,
        live: None,
    };
    let key = QueueSourceKey::Record {
        collection: None,
        key,
    }
    .encode()
    .unwrap();
    f.reader
        .as_mut()
        .unwrap()
        .inner
        .facts
        .insert(key, queue_storage_layout::encode(&fact).unwrap());
}
#[test]
fn keys_require_authenticated_live_points_and_retain_nonmember_observations() {
    futures::executor::block_on(async {
        let mut f = fixture_contents(Signals::default(), false, FieldType::Integer, false, true);
        tombstone(&mut f, "d", 2, 0);
        let intent = boolean::intent(vec![selected(&["a", "b", "c", "d", "missing"])]);
        let (producer, proof) = start_keys!(f, intent).boolean_clause(0).await.unwrap();
        assert_eq!(
            boolean::rows(&proof),
            &BTreeSet::from([(None, "a".into()), (None, "b".into()), (None, "c".into())])
        );
        let points = &proof.keys[0].observations;
        assert_eq!(points.len(), 5);
        assert!(points[0].live && points[1].live && points[2].live);
        assert_eq!(
            (points[3].key, points[3].highwater, points[3].live),
            ("d", Some(2), false)
        );
        assert_eq!(
            (points[4].key, points[4].highwater, points[4].live),
            ("missing", None, false)
        );
        assert_eq!(producer.boolean_clause_len(&proof, 0).unwrap(), 3);
        assert_eq!(f.signals.lookups.load(Ordering::SeqCst), 7); // two headers + five points
        assert_eq!(f.signals.opens.load(Ordering::SeqCst), 0);
        assert_eq!(f.signals.writes.load(Ordering::SeqCst), 3);
        drop(proof);
        drop(producer);
        assert!(f.reader_alive.upgrade().is_none());
        assert!(f.scratch_alive.upgrade().is_none());
    });
}
#[test]
fn empty_keys_is_a_bound_sealed_empty_set_without_universe_or_point_reads() {
    futures::executor::block_on(async {
        let mut f = fixture_contents(Signals::default(), false, FieldType::Integer, false, true);
        let intent = boolean::intent(vec![selected(&[]), selected(&[])]);
        let (producer, first) = start_keys!(f, intent).keys(0, 0).await.unwrap();
        let (producer, second) = producer.keys(1, 0).await.unwrap();
        assert_eq!(producer.keys_len(&first, 0, 0).unwrap(), 0);
        assert_eq!(producer.keys_len(&second, 1, 0).unwrap(), 0);
        let before = f.budget.remaining(&f.job).unwrap();
        assert!(producer.validate_keys(&first, 1, 0).is_err());
        assert_eq!(f.budget.remaining(&f.job).unwrap(), before);
        assert_eq!(f.signals.lookups.load(Ordering::SeqCst), 2);
        assert_eq!(f.signals.opens.load(Ordering::SeqCst), 0);
    });
}
#[test]
fn malformed_tombstone_and_late_point_failure_never_return_a_partial_key_leaf() {
    futures::executor::block_on(async {
        for (highwater, count) in [(0, 0), (2, 1)] {
            let mut f =
                fixture_contents(Signals::default(), false, FieldType::Integer, false, true);
            tombstone(&mut f, "d", highwater, count);
            let intent = boolean::intent(vec![selected(&["a", "d"])]);
            assert!(start_keys!(f, intent).keys(0, 0).await.is_err());
            assert_eq!(f.signals.writes.load(Ordering::SeqCst), 1);
            assert!(f.reader_alive.upgrade().is_none());
            assert!(f.scratch_alive.upgrade().is_none());
        }
        let mut f = fixture_contents(
            Signals {
                fail_lookup: 4,
                ..Default::default()
            },
            false,
            FieldType::Integer,
            false,
            true,
        );
        let intent = boolean::intent(vec![selected(&["a", "b"])]);
        let producer = start_keys!(f, intent);
        let before = f.budget.remaining(&f.job).unwrap();
        assert!(
            matches!(producer.keys(0,0).await,Err(QueueError::OutcomeUnknown(reason)) if reason=="injected metadata point failure")
        );
        assert_eq!(f.signals.writes.load(Ordering::SeqCst), 1);
        assert!(f.budget.remaining(&f.job).unwrap().rows < before.rows);
        assert!(f.reader_alive.upgrade().is_none());
        assert!(f.scratch_alive.upgrade().is_none());
    });
}

#[test]
fn identical_key_occurrences_nonmembers_and_foreign_producers_are_bound() {
    futures::executor::block_on(async {
        let mut f = fixture_contents(Signals::default(), false, FieldType::Integer, false, true);
        let original = &f.reader.as_ref().unwrap().inner;
        let other_reader = Stream {
            inner: Reader {
                through: original.through,
                identity: original.identity.clone(),
                facts: original.facts.clone(),
            },
            foreign_absent: None,
            signals: f.signals.clone(),
            _alive: Arc::new(()),
            job: f.job.clone(),
            admission: f.admission.clone(),
        };
        let other_scratch = Scratch {
            scope: Arc::new(()),
            job: f.job.clone(),
            budget: f.budget.clone(),
            signals: f.signals.clone(),
            _alive: Arc::new(()),
        };
        let intent = boolean::intent(vec![WorkSelector::all(vec![
            selected(&["a", "missing"]),
            selected(&["a", "missing"]),
        ])]);
        let (producer, mut first) = start_keys!(f, intent).keys(0, 0).await.unwrap();
        let (producer, second) = producer.keys(0, 1).await.unwrap();
        let other = QueueSourceCandidateProducer::begin(
            other_reader,
            other_scratch,
            &f.definition,
            QueueSourceSelectionRequest::Administration {
                queue: &f.queue,
                intent: &intent,
            },
            0,
            &QueueLimits::default(),
            f.job.clone(),
            f.budget.clone(),
        )
        .await
        .unwrap();
        let before = f.budget.remaining(&f.job).unwrap();
        assert!(producer.validate_keys(&first, 0, 1).is_err());
        assert!(other.validate_keys(&first, 0, 0).is_err());
        let absent = first.observations.pop().unwrap();
        assert!(!absent.live && absent.highwater.is_none());
        assert!(producer.validate_keys(&first, 0, 0).is_err());
        first.observations.push(absent);
        assert_eq!(producer.keys_len(&first, 0, 0).unwrap(), 1);
        assert_eq!(producer.keys_len(&second, 0, 1).unwrap(), 1);
        assert_eq!(f.budget.remaining(&f.job).unwrap(), before);
    });
}
#[test]
fn foreign_cut_absence_and_decisive_empty_cannot_hide_unresolved_points() {
    futures::executor::block_on(async {
        let mut f = fixture_contents(Signals::default(), false, FieldType::Integer, false, true);
        f.reader.as_mut().unwrap().foreign_absent =
            Some(Arc::new(f.job.owned_buffer(128).unwrap()));
        let intent = boolean::intent(vec![selected(&["a", "missing"])]);
        assert!(
            matches!(start_keys!(f,intent).keys(0,0).await,Err(QueueError::OutcomeUnknown(reason))
            if reason=="queue source cut: lookup evidence belongs to another cut or full key")
        );
        assert_eq!(f.signals.writes.load(Ordering::SeqCst), 1);
        assert!(f.reader_alive.upgrade().is_none());
        assert!(f.scratch_alive.upgrade().is_none());
        let mut f = fixture_contents(
            Signals {
                fail_lookup: 4,
                ..Default::default()
            },
            false,
            FieldType::Integer,
            false,
            true,
        );
        let intent = boolean::intent(vec![WorkSelector::all(vec![
            WorkSelector::any(vec![]),
            selected(&["a", "b"]),
        ])]);
        assert!(
            matches!(start_keys!(f,intent).boolean_clause(0).await,Err(QueueError::OutcomeUnknown(reason)) if reason=="injected metadata point failure")
        );
        assert_eq!(f.signals.lookups.load(Ordering::SeqCst), 4);
        assert_eq!(f.signals.writes.load(Ordering::SeqCst), 1);
        assert!(f.reader_alive.upgrade().is_none());
        assert!(f.scratch_alive.upgrade().is_none());
        let mut f = fixture_contents(Signals::default(), false, FieldType::Integer, false, true);
        let intent = boolean::intent(vec![WorkSelector::negate(selected(&["a", "missing"]))]);
        let (producer, proof) = start_keys!(f, intent).boolean_clause(0).await.unwrap();
        assert_eq!(
            boolean::rows(&proof),
            &BTreeSet::from([(None, "b".into()), (None, "c".into()), (None, "d".into())])
        );
        assert_eq!(producer.boolean_clause_len(&proof, 0).unwrap(), 3);
    });
}
#[test]
fn matching_nonzero_and_extreme_administrative_times_are_accepted() {
    futures::executor::block_on(async {
        for time in [i64::MIN, -7, i64::MAX] {
            let mut f =
                fixture_contents(Signals::default(), false, FieldType::Integer, false, true);
            let mut intent = boolean::intent(vec![selected(&[])]);
            intent.evaluated_at = time;
            let producer = QueueSourceCandidateProducer::begin(
                f.reader.take().unwrap(),
                f.scratch.take().unwrap(),
                &f.definition,
                QueueSourceSelectionRequest::Administration {
                    queue: &f.queue,
                    intent: &intent,
                },
                time,
                &QueueLimits::default(),
                f.job.clone(),
                f.budget.clone(),
            )
            .await
            .unwrap();
            let (producer, proof) = producer.keys(0, 0).await.unwrap();
            assert_eq!(producer._evaluated_at, time);
            assert_eq!(producer.keys_len(&proof, 0, 0).unwrap(), 0);
        }
    });
}
#[test]
fn keys_cumulative_work_is_exact_and_each_one_short_retires_without_refunds() {
    futures::executor::block_on(async {
        let mut f = fixture_contents(Signals::default(), false, FieldType::Integer, false, true);
        let intent = boolean::intent(vec![selected(&["a", "b"])]);
        let (producer, leaf) = start_keys!(f, intent).keys(0, 0).await.unwrap();
        let cost = used(model_limits(), f.budget.remaining(&f.job).unwrap());
        assert_eq!(
            cost.rows,
            2 + f.signals.lookups.load(Ordering::SeqCst) as u64
                + f.signals.writes.load(Ordering::SeqCst) as u64
        );
        assert_eq!(cost.scratch_steps, 2);
        drop(leaf);
        drop(producer);
        for dimension in 0..4 {
            let mut f =
                fixture_contents(Signals::default(), false, FieldType::Integer, false, true);
            let mut limit = cost;
            match dimension {
                1 => limit.rows -= 1,
                2 => limit.copied_bytes -= 1,
                3 => limit.scratch_steps -= 1,
                _ => {}
            }
            let budget = QueueSourceSelectionBudget::new(limit, &f.job).unwrap();
            f.scratch.as_mut().unwrap().budget = budget.clone();
            f.budget = budget;
            let baseline = f.admission.usage().unwrap().1;
            let source = f.reader.as_ref().unwrap().inner.identity.admitted_bytes();
            let intent = boolean::intent(vec![selected(&["a", "b"])]);
            let result = start_keys!(f, intent).keys(0, 0).await;
            if dimension == 0 {
                let (producer, leaf) = result.unwrap();
                assert_eq!(producer.keys_len(&leaf, 0, 0).unwrap(), 2);
                assert_eq!(
                    f.budget.remaining(&f.job).unwrap(),
                    QueueSourceSelectionWork::default()
                );
                drop(leaf);
                drop(producer);
            } else {
                assert!(
                    matches!(result,Err(QueueError::OutcomeUnknown(reason)) if reason=="source selection: request selection work exhausted")
                );
                if dimension == 3 {
                    assert_eq!(f.signals.writes.load(Ordering::SeqCst), 1);
                    let after = f.budget.remaining(&f.job).unwrap();
                    assert_eq!(
                        (after.rows, after.copied_bytes, after.scratch_steps),
                        (1, 1, 0)
                    );
                }
            }
            assert!(f.reader_alive.upgrade().is_none());
            assert!(f.scratch_alive.upgrade().is_none());
            assert_eq!(f.admission.usage().unwrap().1, baseline - source);
        }
    });
}
async fn empty_manifest_trial(headroom: u64, check: bool) -> bool {
    let mut f = fixture_contents(Signals::default(), false, FieldType::Integer, false, true);
    let baseline = f.admission.usage().unwrap().1;
    let source = f.reader.as_ref().unwrap().inner.identity.admitted_bytes();
    let intent = boolean::intent(vec![selected(&[])]);
    let producer = start_keys!(f, intent);
    let filler = f
        .job
        .owned_buffer((32u64 << 20) - f.admission.usage().unwrap().1 - headroom)
        .unwrap();
    let before = f.budget.remaining(&f.job).unwrap();
    let result = producer.keys(0, 0).await;
    let success = result.is_ok();
    if check && !success {
        assert!(
            matches!(&result,Err(QueueError::OutcomeUnknown(reason)) if reason.contains("admitted buffer capacity exhausted"))
        );
        assert_eq!(f.budget.remaining(&f.job).unwrap(), before);
    }
    drop(result);
    drop(filler);
    assert_eq!(f.signals.lookups.load(Ordering::SeqCst), 2);
    assert_eq!(f.signals.opens.load(Ordering::SeqCst), 0);
    assert!(f.reader_alive.upgrade().is_none());
    assert!(f.scratch_alive.upgrade().is_none());
    assert_eq!(f.admission.usage().unwrap().1, baseline - source);
    success
}
#[test]
fn keys_manifest_root_exact_and_one_short_never_dispatch_unfunded_points() {
    futures::executor::block_on(async {
        let (mut low, mut high) = (0, 65536);
        assert!(!empty_manifest_trial(low, false).await);
        assert!(empty_manifest_trial(high, false).await);
        while low + 1 < high {
            let middle = (low + high) / 2;
            if empty_manifest_trial(middle, false).await {
                high = middle
            } else {
                low = middle
            }
        }
        assert!(empty_manifest_trial(high, true).await);
        assert!(!empty_manifest_trial(high - 1, true).await);
    });
}

#[test]
fn nonempty_keys_fund_the_actual_retained_observation_capacity_before_points() {
    futures::executor::block_on(async {
        let requested = || {
            WorkSelector::leaf(SelectorNode::Keys(
                (0..128).map(|n| format!("absent-{n:04}")).collect(),
            ))
        };
        let mut f = fixture_contents(Signals::default(), false, FieldType::Integer, false, true);
        let intent = boolean::intent(vec![selected(&[]), requested()]);
        let (producer, empty) = start_keys!(f, intent).keys(0, 0).await.unwrap();
        let (producer, nonempty) = producer.keys(1, 0).await.unwrap();
        let actual_storage = nonempty
            .observations
            .capacity()
            .checked_mul(std::mem::size_of::<KeyObservation<'_>>())
            .unwrap() as u64;
        assert!(actual_storage > 1024);
        assert_eq!(nonempty.observations.len(), 128);
        assert!(
            nonempty
                .observations
                .iter()
                .all(|point| !point.live && point.highwater.is_none())
        );
        assert!(
            nonempty._memory.admitted_bytes()
                >= actual_storage + std::mem::size_of_val(&nonempty) as u64
        );
        assert!(
            nonempty._memory.admitted_bytes() - empty._memory.admitted_bytes() >= actual_storage
        );
        assert_eq!(producer.keys_len(&nonempty, 1, 0).unwrap(), 0);
        assert_eq!(f.signals.lookups.load(Ordering::SeqCst), 130);
        drop(nonempty);
        drop(empty);
        drop(producer);
        assert!(f.reader_alive.upgrade().is_none());
        assert!(f.scratch_alive.upgrade().is_none());

        // Less than the observed variable storage alone cannot fund this leaf,
        // regardless of record_at's later temporary lookup peak.
        let mut f = fixture_contents(Signals::default(), false, FieldType::Integer, false, true);
        let baseline = f.admission.usage().unwrap().1;
        let source = f.reader.as_ref().unwrap().inner.identity.admitted_bytes();
        let intent = boolean::intent(vec![requested()]);
        let producer = start_keys!(f, intent);
        let filler = f
            .job
            .owned_buffer((32u64 << 20) - f.admission.usage().unwrap().1 - (actual_storage - 1))
            .unwrap();
        let before = f.budget.remaining(&f.job).unwrap();
        assert!(
            matches!(producer.keys(0,0).await,Err(QueueError::OutcomeUnknown(reason)) if reason.contains("admitted buffer capacity exhausted"))
        );
        assert_eq!(f.budget.remaining(&f.job).unwrap(), before);
        assert_eq!(f.signals.lookups.load(Ordering::SeqCst), 2);
        assert_eq!(f.signals.writes.load(Ordering::SeqCst), 0);
        assert!(f.reader_alive.upgrade().is_none());
        assert!(f.scratch_alive.upgrade().is_none());
        drop(filler);
        assert_eq!(f.admission.usage().unwrap().1, baseline - source);
    });
}
