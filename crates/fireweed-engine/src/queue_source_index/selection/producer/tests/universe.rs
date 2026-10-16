use super::*;
fn fixture(signals: Signals) -> Fixture {
    fixture_contents(signals, false, FieldType::Integer, false, true)
}
fn identities(leaf: &QueueSourceUniverseLeaf<'_, Completion, Set>) -> BTreeSet<Vec<u8>> {
    leaf.set
        .0
        .rows
        .iter()
        .map(|(collection, key)| {
            QueueSourceKey::Record {
                collection: collection.as_deref(),
                key,
            }
            .encode()
            .unwrap()
        })
        .collect()
}
#[test]
fn universe_includes_zero_field_and_terminal_records_from_the_independent_history_fold() {
    futures::executor::block_on(async {
        let mut f = fixture(Signals::default());
        let request = self::super::request(&f.queue, Predicate::Equal(FieldValue::Integer(1)));
        let producer = start!(f, request);
        let (producer, leaf) = producer.universe().await.unwrap();
        let expected = ["a", "b", "c", "d"]
            .into_iter()
            .map(|key| {
                QueueSourceKey::Record {
                    collection: None,
                    key,
                }
                .encode()
                .unwrap()
            })
            .collect();
        assert_eq!(identities(&leaf), expected);
        assert_eq!(producer.universe_len(&leaf).unwrap(), 4);
        assert_eq!(f.signals.visits.load(Ordering::SeqCst), 4);
        assert_eq!(f.signals.finishes.load(Ordering::SeqCst), 1);
        drop(leaf);
        drop(producer);
        assert!(f.reader_alive.upgrade().is_none());
        assert!(f.scratch_alive.upgrade().is_none());
    });
}
#[test]
fn late_universe_write_failure_and_foreign_completion_cannot_mint_a_universe() {
    futures::executor::block_on(async {
        for foreign in [false, true] {
            let mut f = fixture(Signals {
                fail_write: if foreign { 0 } else { 4 },
                foreign_completion: foreign,
                ..Default::default()
            });
            let request = self::super::request(&f.queue, Predicate::Equal(FieldValue::Integer(1)));
            let producer = start!(f, request);
            assert!(producer.universe().await.is_err());
            assert_eq!(f.signals.writes.load(Ordering::SeqCst), 4);
            assert_eq!(
                f.signals.finishes.load(Ordering::SeqCst),
                usize::from(foreign)
            );
            assert!(f.reader_alive.upgrade().is_none());
            assert!(f.scratch_alive.upgrade().is_none());
        }
    });
}
#[test]
fn universe_is_the_actual_requested_collection_and_accepts_leased_work_metadata() {
    futures::executor::block_on(async {
        let mut f = fixture(Signals::default());
        let key = QueueSourceKey::Record {
            collection: None,
            key: "a",
        }
        .encode()
        .unwrap();
        // Driven-port metadata control, not a claim publication correspondence
        // proof: leased membership must not be prematurely eligibility-filtered.
        let facts = &mut f.reader.as_mut().unwrap().inner.facts;
        let mut record: QueueSourceRecord = queue_storage_layout::decode(&facts[&key]).unwrap();
        if let Some(QueueSourceLive::Work { metadata, .. }) = record.live.as_mut() {
            metadata.lifecycle = Lifecycle::Leased;
            metadata.lease_expires_at = Some(100);
            metadata.lease_hash = Some([7; 32]);
            metadata.attempts = 1;
        } else {
            panic!()
        }
        record.validate(None).unwrap();
        facts.insert(key, queue_storage_layout::encode(&record).unwrap());
        let request = self::super::request(&f.queue, Predicate::Equal(FieldValue::Integer(1)));
        let (producer, leaf) = start!(f, request).universe().await.unwrap();
        assert_eq!(producer.universe_len(&leaf).unwrap(), 4);
        drop(leaf);
        drop(producer);

        for (collection, keys) in [("state", ["a", "z"]), ("other", ["a", "y"])] {
            let mut f = fixture(Signals::default());
            let request = QueryRequest {
                queue: f.queue.clone(),
                source: RecordSource::State(collection.into()),
                index: "$key".into(),
                filter: Filter::default(),
                descending: false,
                cursor: None,
                max_records: 3,
                max_bytes: 1 << 20,
            };
            let producer = QueueSourceCandidateProducer::begin(
                f.reader.take().unwrap(),
                f.scratch.take().unwrap(),
                &f.definition,
                QueueSourceSelectionRequest::Query(&request),
                0,
                &QueueLimits::default(),
                f.job.clone(),
                f.budget.clone(),
            )
            .await
            .unwrap();
            let (producer, leaf) = producer.universe().await.unwrap();
            let expected = keys
                .into_iter()
                .map(|key| {
                    QueueSourceKey::Record {
                        collection: Some(collection),
                        key,
                    }
                    .encode()
                    .unwrap()
                })
                .collect();
            assert_eq!(identities(&leaf), expected);
            assert_eq!(producer.universe_len(&leaf).unwrap(), 2);
        }
    });
}
#[test]
fn empty_universe_requires_its_real_completion_and_a_late_record_contradiction_retires_everything()
{
    futures::executor::block_on(async {
        let mut f = fixture(Signals::default());
        f._history.truncate(1);
        let reader = f.reader.as_mut().unwrap();
        reader.inner.through = 1;
        reader.inner.facts = oracle::fold(
            &f._history,
            &RecordSnapshot::default(),
            f.definition.schema(),
        );
        let request = self::super::request(&f.queue, Predicate::Equal(FieldValue::Integer(1)));
        let (producer, mut leaf) = start!(f, request).universe().await.unwrap();
        assert_eq!(producer.universe_len(&leaf).unwrap(), 0);
        assert_eq!(f.signals.visits.load(Ordering::SeqCst), 0);
        assert_eq!(f.signals.finishes.load(Ordering::SeqCst), 1);
        let before = f.budget.remaining(&f.job).unwrap();
        leaf.completion.lower.clear();
        assert!(producer.validate_universe(&leaf).is_err());
        assert_eq!(f.budget.remaining(&f.job).unwrap(), before);
        drop(leaf);
        drop(producer);
        for tombstone in [true, false] {
            let mut f = fixture(Signals::default());
            let key = QueueSourceKey::Record {
                collection: None,
                key: "d",
            }
            .encode()
            .unwrap();
            let facts = &mut f.reader.as_mut().unwrap().inner.facts;
            let mut record: QueueSourceRecord = queue_storage_layout::decode(&facts[&key]).unwrap();
            if tombstone {
                record.live = None;
                record.field_count = 0;
            } else if let Some(QueueSourceLive::Work { metadata, .. }) = record.live.as_mut() {
                metadata.insertion_sequence = u64::MAX;
            }
            facts.insert(key, queue_storage_layout::encode(&record).unwrap());
            let request = self::super::request(&f.queue, Predicate::Equal(FieldValue::Integer(1)));
            assert!(matches!(
                start!(f, request).universe().await,
                Err(QueueError::OutcomeUnknown(_))
            ));
            assert_eq!(f.signals.writes.load(Ordering::SeqCst), 3);
            assert_eq!(f.signals.visits.load(Ordering::SeqCst), 4);
            assert_eq!(f.signals.finishes.load(Ordering::SeqCst), 0);
            assert!(f.reader_alive.upgrade().is_none());
            assert!(f.scratch_alive.upgrade().is_none());
        }
    });
}
#[test]
fn universe_work_has_independent_counter_correspondence_and_exact_one_short_limits() {
    futures::executor::block_on(async {
        let mut f = fixture(Signals::default());
        let request = self::super::request(&f.queue, Predicate::Equal(FieldValue::Integer(1)));
        let (producer, leaf) = start!(f, request).universe().await.unwrap();
        let cost = used(model_limits(), f.budget.remaining(&f.job).unwrap());
        let writes = f.signals.writes.load(Ordering::SeqCst) as u64;
        assert_eq!(
            cost.rows,
            writes
                + f.signals.visits.load(Ordering::SeqCst) as u64
                + f.signals.lookups.load(Ordering::SeqCst) as u64
        );
        assert_eq!(cost.scratch_steps, writes);
        drop(leaf);
        drop(producer);
        for dimension in 0..4 {
            let mut f = fixture(Signals::default());
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
            let request = self::super::request(&f.queue, Predicate::Equal(FieldValue::Integer(1)));
            let result = start!(f, request).universe().await;
            assert_eq!(result.is_ok(), dimension == 0);
            if let Ok((producer, leaf)) = result {
                assert_eq!(producer.universe_len(&leaf).unwrap(), 4);
                assert_eq!(
                    f.budget.remaining(&f.job).unwrap(),
                    QueueSourceSelectionWork::default()
                );
                drop(leaf);
                drop(producer);
            }
            assert!(f.reader_alive.upgrade().is_none());
            assert!(f.scratch_alive.upgrade().is_none());
        }
    });
}
#[test]
fn same_job_cut_and_budget_do_not_authorize_a_leaf_from_another_producer() {
    futures::executor::block_on(async {
        let mut f = fixture(Signals::default());
        let source = &f.reader.as_ref().unwrap().inner;
        let other_reader = Stream {
            inner: Reader {
                through: source.through,
                identity: source.identity.clone(),
                facts: source.facts.clone(),
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
        let request = self::super::request(&f.queue, Predicate::Equal(FieldValue::Integer(1)));
        let (producer, leaf) = start!(f, request).universe().await.unwrap();
        let other = QueueSourceCandidateProducer::begin(
            other_reader,
            other_scratch,
            &f.definition,
            QueueSourceSelectionRequest::Claim(&request),
            0,
            &QueueLimits::default(),
            f.job.clone(),
            f.budget.clone(),
        )
        .await
        .unwrap();
        let before = f.budget.remaining(&f.job).unwrap();
        assert!(
            matches!(other.validate_universe(&leaf), Err(QueueError::OutcomeUnknown(reason)) if reason == "source candidate producer: universe producer or collection differs")
        );
        assert_eq!(f.budget.remaining(&f.job).unwrap(), before);
        assert_eq!(producer.universe_len(&leaf).unwrap(), 4);
    });
}
#[test]
fn foreign_completion_of_a_genuinely_empty_universe_is_not_accepted() {
    futures::executor::block_on(async {
        let mut f = fixture(Signals {
            foreign_completion: true,
            ..Default::default()
        });
        f._history.truncate(1);
        let source = f.reader.as_mut().unwrap();
        source.inner.through = 1;
        source.inner.facts = oracle::fold(
            &f._history,
            &RecordSnapshot::default(),
            f.definition.schema(),
        );
        let request = self::super::request(&f.queue, Predicate::Equal(FieldValue::Integer(1)));
        assert!(start!(f, request).universe().await.is_err());
        assert_eq!(f.signals.visits.load(Ordering::SeqCst), 0);
        assert_eq!(f.signals.writes.load(Ordering::SeqCst), 0);
        assert_eq!(f.signals.finishes.load(Ordering::SeqCst), 1);
        assert!(f.reader_alive.upgrade().is_none());
        assert!(f.scratch_alive.upgrade().is_none());
    });
}
async fn batch_trial(headroom: u64, assert_boundary: bool) -> bool {
    let mut f = fixture(Signals {
        late_page_headroom: Some(headroom),
        ..Default::default()
    });
    let baseline = f.admission.usage().unwrap().1;
    let source_charge = f.reader.as_ref().unwrap().inner.identity.admitted_bytes();
    let request = self::super::request(&f.queue, Predicate::Equal(FieldValue::Integer(1)));
    let result = start!(f, request).universe().await;
    let success = result.is_ok();
    if let Ok((producer, leaf)) = &result {
        assert_eq!(producer.universe_len(leaf).unwrap(), 4);
        assert_eq!(
            identities(leaf),
            ["a", "b", "c", "d"]
                .into_iter()
                .map(|key| QueueSourceKey::Record {
                    collection: None,
                    key
                }
                .encode()
                .unwrap())
                .collect()
        );
    }
    if assert_boundary && !success {
        assert_eq!(f.signals.visits.load(Ordering::SeqCst), 2);
        assert_eq!(f.signals.writes.load(Ordering::SeqCst), 1);
        assert_eq!(f.signals.lookups.load(Ordering::SeqCst), 3); // headers + first record
        assert_eq!(f.signals.finishes.load(Ordering::SeqCst), 0);
        let visits = f.signals.visits.load(Ordering::SeqCst)
            + f.signals.lookups.load(Ordering::SeqCst)
            + f.signals.writes.load(Ordering::SeqCst);
        assert_eq!(
            used(model_limits(), f.budget.remaining(&f.job).unwrap()).rows,
            visits as u64
        );
    }
    drop(result);
    assert!(f.reader_alive.upgrade().is_none());
    assert!(f.scratch_alive.upgrade().is_none());
    assert_eq!(f.admission.usage().unwrap().1, baseline - source_charge);
    success
}
#[test]
fn universe_late_copy_admission_exact_and_one_short_spend_the_visit_and_retire_all_owners() {
    futures::executor::block_on(async {
        // Owned-accounting boundary, not an allocator or physical SDK peak.
        let (mut low, mut high) = (0, 4096);
        assert!(batch_trial(high, false).await);
        while low < high {
            let middle = low + (high - low) / 2;
            if batch_trial(middle, false).await {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        assert!(low > std::mem::size_of::<Batch>() as u64);
        assert!(batch_trial(low, true).await);
        assert!(!batch_trial(low - 1, true).await);
    });
}
#[test]
fn dropping_a_universe_source_or_scratch_await_retires_both_ports_without_refund() {
    use std::future::Future;
    futures::executor::block_on(async {
        for source in [true, false] {
            let mut f = fixture(Signals {
                pause_source: source,
                pause_scratch: !source,
                ..Default::default()
            });
            let request = self::super::request(&f.queue, Predicate::Equal(FieldValue::Integer(1)));
            let producer = start!(f, request);
            let before = f.budget.remaining(&f.job).unwrap();
            let mut pending = Box::pin(producer.universe());
            let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
            assert!(pending.as_mut().poll(&mut cx).is_pending());
            assert_eq!(f.signals.source_entered.load(Ordering::SeqCst), source);
            assert_eq!(f.signals.scratch_entered.load(Ordering::SeqCst), !source);
            let spent = f.budget.remaining(&f.job).unwrap();
            assert!(spent.copied_bytes < before.copied_bytes);
            drop(pending);
            assert_eq!(f.budget.remaining(&f.job).unwrap(), spent);
            assert_eq!(f.signals.finishes.load(Ordering::SeqCst), 0);
            assert!(f.reader_alive.upgrade().is_none());
            assert!(f.scratch_alive.upgrade().is_none());
        }
    });
}
