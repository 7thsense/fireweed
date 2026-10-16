//! Driven-port causal models, not physical SDK memory/I/O qualification. Real
//! Turso ownership, SQL plans and quotas are qualified in queue_scratch tests.
use super::*;
use crate::{
    queue_admission::QueueAdmission,
    queue_log_record::{OwnedQueueLogRecord, encode_checked_genesis},
    queue_planner::RecordSnapshot,
    queue_schema_codec::encode_definition,
    queue_source_index::{
        QueueSourcePoint, QueueSourceReader,
        predecessor::tests::{Reader, group, record},
    },
};
use fireweed_core::queue_contract::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
mod oracle {
    fireweed_conformance::define_queue_source_oracle!(crate);
}
#[derive(Default)]
struct Signals {
    writes: AtomicUsize,
    lookups: AtomicUsize,
    visits: AtomicUsize,
    late_page_headroom: Option<u64>,
    finishes: AtomicUsize,
    opens: AtomicUsize,
    source_entered: AtomicBool,
    scratch_entered: AtomicBool,
    pause_source: bool,
    pause_scratch: bool,
    fail_write: usize,
    foreign_completion: bool,
}
struct Stream {
    inner: Reader,
    signals: Arc<Signals>,
    _alive: Arc<()>,
    job: Arc<QueueReservation>,
    admission: Arc<QueueAdmission>,
}
struct Cursor {
    lower: Vec<u8>,
    upper: Vec<u8>,
    after: Option<Vec<u8>>,
    issued: bool,
    complete: bool,
}
struct Page {
    keys: Vec<Vec<u8>>,
    complete: bool,
    _filler: Option<OwnedQueueBufferCharge>,
}
#[derive(Clone)]
struct Completion {
    lower: Vec<u8>,
    upper: Vec<u8>,
    source: Arc<OwnedQueueBufferCharge>,
}
impl QueueSourceReader for Stream {
    type Point = Vec<u8>;
    type Range = (Vec<u8>, Vec<u8>);
    fn cut(&self) -> QueueResult<QueueSourceCut<'_>> {
        self.inner.cut()
    }
    async fn lookup(&self, key: &[u8]) -> QueueResult<Self::Point> {
        self.signals.lookups.fetch_add(1, Ordering::SeqCst);
        self.inner.lookup(key).await
    }
    fn point<'a>(&'a self, owner: &'a Self::Point) -> QueueResult<QueueSourcePoint<'a>> {
        self.inner.point(owner)
    }
    async fn complete_range(&self, lo: &[u8], hi: &[u8]) -> QueueResult<Self::Range> {
        self.inner.complete_range(lo, hi).await
    }
    fn visit_range(
        &self,
        owner: &Self::Range,
        lo: &[u8],
        hi: &[u8],
        visitor: &mut dyn for<'a> FnMut(QueueSourcePoint<'a>) -> QueueResult<()>,
    ) -> QueueResult<()> {
        self.inner.visit_range(owner, lo, hi, visitor)
    }
}
impl QueueSourceStreamingReader for Stream {
    type Cursor = Cursor;
    type Page = Page;
    type Completion = Completion;
    fn open_stream(&self, lower: &[u8], upper: &[u8]) -> QueueResult<Cursor> {
        self.signals.opens.fetch_add(1, Ordering::SeqCst);
        Ok(Cursor {
            lower: lower.to_vec(),
            upper: upper.to_vec(),
            after: None,
            issued: false,
            complete: false,
        })
    }
    async fn next_stream_page(&self, cursor: &mut Cursor) -> QueueResult<Option<Page>> {
        assert!(!cursor.issued);
        if cursor.complete {
            return Ok(None);
        }
        cursor.issued = true;
        if self.signals.pause_source {
            self.signals.source_entered.store(true, Ordering::SeqCst);
            std::future::pending::<()>().await;
        }
        let keys = self
            .inner
            .facts
            .keys()
            .filter(|key| {
                key.as_slice() >= cursor.lower.as_slice()
                    && key.as_slice() < cursor.upper.as_slice()
                    && cursor.after.as_ref().is_none_or(|after| *key > after)
            })
            .take(1)
            .cloned()
            .collect::<Vec<_>>();
        // Small, real final pages: the next source key determines completion.
        let after = keys.last().or(cursor.after.as_ref());
        let more = after.is_some_and(|after| {
            self.inner
                .facts
                .keys()
                .any(|key| key > after && key.as_slice() < cursor.upper.as_slice())
        });
        let filler = if cursor.after.is_some() {
            if let Some(headroom) = self.signals.late_page_headroom {
                let used = self.admission.usage()?.1;
                Some(
                    self.job.owned_buffer(
                        (32u64 << 20)
                            .checked_sub(used)
                            .and_then(|n| n.checked_sub(headroom))
                            .ok_or(QueueError::TooLarge)?,
                    )?,
                )
            } else {
                None
            }
        } else {
            None
        };
        Ok(Some(Page {
            keys,
            complete: !more,
            _filler: filler,
        }))
    }
    fn visit_stream_page(
        &self,
        cursor: &mut Cursor,
        page: Page,
        visitor: &mut dyn for<'a> FnMut(QueueSourcePoint<'a>) -> QueueResult<()>,
    ) -> QueueResult<()> {
        assert!(cursor.issued);
        for key in &page.keys {
            self.signals.visits.fetch_add(1, Ordering::SeqCst);
            visitor(
                self.inner
                    .cut()?
                    .point_from_source(key, self.inner.facts.get(key).map(Vec::as_slice)),
            )?;
        }
        if let Some(last) = page.keys.last() {
            cursor.after = Some(last.clone());
        }
        cursor.complete = page.complete;
        cursor.issued = false;
        Ok(())
    }
    fn finish_stream(&self, cursor: Cursor) -> QueueResult<Completion> {
        self.signals.finishes.fetch_add(1, Ordering::SeqCst);
        if cursor.issued || !cursor.complete {
            return Err(unknown("model cursor incomplete"));
        }
        let source = if self.signals.foreign_completion {
            Arc::new(self.job.owned_buffer(1)?)
        } else {
            self.inner.identity.clone()
        };
        Ok(Completion {
            lower: cursor.lower,
            upper: cursor.upper,
            source,
        })
    }
    fn validate_stream_completion(
        &self,
        completion: &Completion,
        lower: &[u8],
        upper: &[u8],
    ) -> QueueResult<()> {
        if !Arc::ptr_eq(&completion.source, &self.inner.identity)
            || completion.lower != lower
            || completion.upper != upper
        {
            return Err(unknown("model completion differs"));
        }
        Ok(())
    }
}
struct Builder {
    scope: Arc<()>,
    rows: BTreeSet<(Option<String>, String)>,
}
struct Set(Builder);
struct IdentityRow {
    scope: Arc<()>,
    key: Vec<u8>,
}
struct Scratch {
    scope: Arc<()>,
    job: Arc<QueueReservation>,
    budget: Arc<QueueSourceSelectionBudget>,
    signals: Arc<Signals>,
    _alive: Arc<()>,
}
impl QueueSourceCandidateScratch for Scratch {
    type Builder = Builder;
    type Set = Set;
    type Row = IdentityRow;
    fn validate_operation(
        &self,
        job: &QueueReservation,
        budget: &Arc<QueueSourceSelectionBudget>,
    ) -> QueueResult<()> {
        if !std::ptr::eq(self.job.as_ref(), job) || !Arc::ptr_eq(&self.budget, budget) {
            return Err(unknown("model operation differs"));
        }
        Ok(())
    }
    fn begin_set(&mut self) -> QueueResult<Builder> {
        Ok(Builder {
            scope: self.scope.clone(),
            rows: BTreeSet::new(),
        })
    }
    async fn insert(
        &mut self,
        builder: &mut Builder,
        collection: Option<&str>,
        key: &str,
    ) -> QueueResult<()> {
        assert!(Arc::ptr_eq(&builder.scope, &self.scope));
        self.budget.grant(
            &self.job,
            QueueSourceSelectionWork {
                rows: 1,
                copied_bytes: key.len() as u64,
                scratch_steps: 1,
            },
        )?;
        let write = self.signals.writes.fetch_add(1, Ordering::SeqCst) + 1;
        if write == self.signals.fail_write {
            return Err(unknown("model final write failed"));
        }
        builder
            .rows
            .insert((collection.map(str::to_owned), key.to_owned()));
        if self.signals.pause_scratch {
            self.signals.scratch_entered.store(true, Ordering::SeqCst);
            std::future::pending::<()>().await;
        }
        Ok(())
    }
    fn seal(&self, builder: Builder) -> QueueResult<Set> {
        Ok(Set(builder))
    }
    async fn union(&mut self, _: &Set, _: &Set) -> QueueResult<Set> {
        Err(unknown("unexpected algebra in scalar model"))
    }
    async fn intersection(&mut self, _: &Set, _: &Set) -> QueueResult<Set> {
        Err(unknown("unexpected algebra in scalar model"))
    }
    async fn difference(&mut self, _: &Set, _: &Set) -> QueueResult<Set> {
        Err(unknown("unexpected algebra in scalar model"))
    }
    async fn next(&mut self, _: &Set, _: Option<&IdentityRow>) -> QueueResult<Option<IdentityRow>> {
        Err(unknown("unexpected physical iteration in scalar model"))
    }
    fn encoded_identity<'a>(&self, row: &'a IdentityRow) -> QueueResult<&'a [u8]> {
        assert!(Arc::ptr_eq(&row.scope, &self.scope));
        Ok(&row.key)
    }
    fn count(&self, set: &Set) -> QueueResult<u64> {
        assert!(Arc::ptr_eq(&set.0.scope, &self.scope));
        Ok(set.0.rows.len() as u64)
    }
}
struct Fixture {
    admission: Arc<QueueAdmission>,
    reader: Option<Stream>,
    scratch: Option<Scratch>,
    definition: AdmittedQueueDefinition,
    queue: QueueAddress,
    job: Arc<QueueReservation>,
    budget: Arc<QueueSourceSelectionBudget>,
    signals: Arc<Signals>,
    reader_alive: Weak<()>,
    scratch_alive: Weak<()>,
    _history: Vec<OwnedQueueLogRecord>,
}
fn fixture(signals: Signals, corrupt_last: bool) -> Fixture {
    fixture_type(signals, corrupt_last, FieldType::Integer)
}
fn fixture_type(signals: Signals, corrupt_last: bool, kind: FieldType) -> Fixture {
    fixture_indexed(signals, corrupt_last, kind, false)
}
fn fixture_indexed(
    signals: Signals,
    corrupt_last: bool,
    kind: FieldType,
    indexed: bool,
) -> Fixture {
    let admission = QueueAdmission::new(2, 32 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let budget = QueueSourceSelectionBudget::new(
        QueueSourceSelectionWork {
            rows: 1_000_000,
            copied_bytes: 1 << 30,
            scratch_steps: 1_000_000,
        },
        &job,
    )
    .unwrap();
    let schema = QueueSchema {
        work: CollectionDefinition {
            fields: BTreeMap::from([("n".into(), kind)]),
            indexes: if indexed {
                BTreeMap::from([("by_n".into(), vec!["n".into()])])
            } else {
                BTreeMap::new()
            },
            unique_indexes: BTreeSet::new(),
            additional_field_prefixes: BTreeSet::new(),
        },
        state: BTreeMap::new(),
        progress_bound_nanos: 1,
        max_attempts: 3,
    };
    let definition =
        AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job).unwrap();
    let queue = QueueAddress {
        tenant: "t".into(),
        queue: "q".into(),
    };
    let genesis = encode_checked_genesis(1, &queue, &definition, &[7; 32], 1 << 20, &job).unwrap();
    let mut seed = group("seed");
    for key in ["a", "b", "c"] {
        seed.work_inserts.push(NewWork {
            key: key.into(),
            priority: 0,
            available_at: 0,
            fields: Fields::from([(
                "n".into(),
                if kind == FieldType::Float {
                    FieldValue::Float(if key == "c" { 2.0 } else { 1.0 })
                } else {
                    FieldValue::Integer(if key == "c" { 2 } else { 1 })
                },
            )]),
            body: vec![1].into(),
        });
    }
    let (publication, snapshot) = record(&queue, &schema, &RecordSnapshot::default(), &seed, &job);
    let history = vec![genesis, publication];
    let mut facts = oracle::fold(&history, &snapshot, &schema);
    if corrupt_last {
        facts.insert(
            QueueSourceKey::Field {
                collection: None,
                key: "b",
                field: "n",
            }
            .encode()
            .unwrap(),
            queue_storage_layout::encode(&FieldValue::Integer(9)).unwrap(),
        );
    }
    let signals = Arc::new(signals);
    let reader_alive = Arc::new(());
    let scratch_alive = Arc::new(());
    Fixture {
        admission: admission.clone(),
        reader_alive: Arc::downgrade(&reader_alive),
        scratch_alive: Arc::downgrade(&scratch_alive),
        reader: Some(Stream {
            inner: Reader {
                through: 2,
                identity: Arc::new(job.owned_buffer(128).unwrap()),
                facts,
            },
            signals: signals.clone(),
            _alive: reader_alive,
            job: job.clone(),
            admission: admission.clone(),
        }),
        scratch: Some(Scratch {
            scope: Arc::new(()),
            job: job.clone(),
            budget: budget.clone(),
            signals: signals.clone(),
            _alive: scratch_alive,
        }),
        definition,
        queue,
        job,
        budget,
        signals,
        _history: history,
    }
}
fn request(queue: &QueueAddress, predicate: Predicate) -> ClaimRequest {
    ClaimRequest {
        queue: queue.clone(),
        request_id: "claim".into(),
        filter: Filter {
            fields: BTreeMap::from([("n".into(), predicate)]),
            ..Default::default()
        },
        order: ClaimOrder::PriorityAscending,
        eligible_at: 0,
        lease_nanos: 1_000_000,
        max_records: 3,
        max_bytes: 1 << 20,
        fields: None,
        include_body: false,
    }
}
#[test]
fn complete_scalar_leaf_comes_from_the_actual_request_and_full_original_range() {
    futures::executor::block_on(async {
        let mut f = fixture(Signals::default(), false);
        let request = self::request(&f.queue, Predicate::Equal(FieldValue::Integer(1)));
        let producer = QueueSourceCandidateProducer::begin(
            f.reader.take().unwrap(),
            f.scratch.take().unwrap(),
            &f.definition,
            QueueSourceSelectionRequest::Claim(&request),
            0,
            &QueueLimits::default(),
            f.job.clone(),
            f.budget.clone(),
        )
        .await
        .unwrap();
        let (producer, leaf) = producer.scalar(0).await.unwrap();
        assert_eq!(producer.scalar_len(&leaf, 0).unwrap(), 2);
        assert_eq!(
            leaf.set.0.rows,
            BTreeSet::from([(None, "a".into()), (None, "b".into())])
        );
        assert_eq!(f.signals.finishes.load(Ordering::SeqCst), 1);
        assert!(producer.validate_scalar(&leaf, 1).is_err());
        drop(leaf);
        drop(producer);
        assert!(f.reader_alive.upgrade().is_none());
        assert!(f.scratch_alive.upgrade().is_none());
    });
}

macro_rules! start {
    ($f:ident, $request:ident) => {
        QueueSourceCandidateProducer::begin(
            $f.reader.take().unwrap(),
            $f.scratch.take().unwrap(),
            &$f.definition,
            QueueSourceSelectionRequest::Claim(&$request),
            0,
            &QueueLimits::default(),
            $f.job.clone(),
            $f.budget.clone(),
        )
        .await
        .unwrap()
    };
}
#[test]
fn final_page_write_and_late_correspondence_failures_return_no_producer_or_leaf() {
    futures::executor::block_on(async {
        for corruption in [false, true] {
            let signals = Signals {
                fail_write: if corruption { 0 } else { 2 },
                ..Default::default()
            };
            let mut f = fixture(signals, corruption);
            let request = self::request(&f.queue, Predicate::Equal(FieldValue::Integer(1)));
            let producer = start!(f, request);
            let before = f.budget.remaining(&f.job).unwrap();
            assert!(producer.scalar(0).await.is_err());
            assert_eq!(f.signals.finishes.load(Ordering::SeqCst), 0);
            assert_eq!(
                f.signals.writes.load(Ordering::SeqCst),
                if corruption { 1 } else { 2 }
            );
            assert!(f.reader_alive.upgrade().is_none());
            assert!(f.scratch_alive.upgrade().is_none());
            assert!(f.budget.remaining(&f.job).unwrap().rows < before.rows);
        }
    });
}
#[test]
fn actual_empty_range_completion_is_distinct_from_a_validated_empty_plan() {
    futures::executor::block_on(async {
        for empty_plan in [false, true] {
            let mut f = fixture(Signals::default(), false);
            let predicate = if empty_plan {
                Predicate::Range {
                    lower: Some(Bound::Included(FieldValue::Integer(1))),
                    upper: Some(Bound::Excluded(FieldValue::Integer(1))),
                }
            } else {
                Predicate::Equal(FieldValue::Integer(99))
            };
            let request = self::request(&f.queue, predicate);
            let producer = start!(f, request);
            let (producer, mut leaf) = producer.scalar(0).await.unwrap();
            assert_eq!(producer.scalar_len(&leaf, 0).unwrap(), 0);
            assert_eq!(leaf.validated_empty, empty_plan);
            assert_eq!(leaf.completions.len(), usize::from(!empty_plan));
            assert_eq!(
                f.signals.opens.load(Ordering::SeqCst),
                usize::from(!empty_plan)
            );
            if !empty_plan {
                leaf.completions.clear();
                assert!(producer.scalar_len(&leaf, 0).is_err());
            }
        }
    });
}
#[test]
fn foreign_completion_with_identical_bounds_and_job_is_not_a_leaf() {
    futures::executor::block_on(async {
        let mut f = fixture(
            Signals {
                foreign_completion: true,
                ..Default::default()
            },
            false,
        );
        let request = self::request(&f.queue, Predicate::Equal(FieldValue::Integer(1)));
        let producer = start!(f, request);
        assert!(producer.scalar(0).await.is_err());
        assert_eq!(f.signals.writes.load(Ordering::SeqCst), 2);
        assert_eq!(f.signals.finishes.load(Ordering::SeqCst), 1);
        assert!(f.reader_alive.upgrade().is_none());
        assert!(f.scratch_alive.upgrade().is_none());
    });
}
#[test]
fn cancellation_during_source_and_scratch_awaits_drops_both_ports_without_refunding() {
    use std::future::Future;
    futures::executor::block_on(async {
        for source in [true, false] {
            let mut f = fixture(
                Signals {
                    pause_source: source,
                    pause_scratch: !source,
                    ..Default::default()
                },
                false,
            );
            let request = self::request(&f.queue, Predicate::Equal(FieldValue::Integer(1)));
            let producer = start!(f, request);
            let before = f.budget.remaining(&f.job).unwrap();
            let mut pending = Box::pin(producer.scalar(0));
            let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
            assert!(pending.as_mut().poll(&mut cx).is_pending());
            assert_eq!(f.signals.source_entered.load(Ordering::SeqCst), source);
            assert_eq!(f.signals.scratch_entered.load(Ordering::SeqCst), !source);
            let spent = f.budget.remaining(&f.job).unwrap();
            assert!(spent.copied_bytes < before.copied_bytes);
            assert!(f.reader_alive.upgrade().is_some());
            assert!(f.scratch_alive.upgrade().is_some());
            drop(pending);
            assert!(f.reader_alive.upgrade().is_none());
            assert!(f.scratch_alive.upgrade().is_none());
            assert_eq!(f.budget.remaining(&f.job).unwrap(), spent);
            assert_eq!(f.signals.finishes.load(Ordering::SeqCst), 0);
        }
    });
}
#[test]
fn same_family_different_predicates_are_bound_to_their_actual_declaration_occurrences() {
    use fireweed_core::queue_administration::{
        AdministrativeAction, AdministrativeClause, WorkSelector,
    };
    futures::executor::block_on(async {
        let mut f = fixture(Signals::default(), false);
        let intent = AdministrativeIntent {
            evaluated_at: 0,
            before_fields: None,
            operation: AdministrativeOperation::SelectFirst(
                [1, 2]
                    .into_iter()
                    .map(|n| AdministrativeClause {
                        id: format!("clause-{n}"),
                        selector: WorkSelector::leaf(SelectorNode::Field {
                            name: "n".into(),
                            predicate: Predicate::Equal(FieldValue::Integer(n)),
                        }),
                        precondition: WorkSelector::all(vec![]),
                        action: AdministrativeAction::Delete {
                            lease_guard: AdministrativeLeaseGuard::RejectActive,
                        },
                    })
                    .collect(),
            ),
        };
        let producer = QueueSourceCandidateProducer::begin(
            f.reader.take().unwrap(),
            f.scratch.take().unwrap(),
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
        assert_eq!(producer.scalar_declarations(), 2);
        let (producer, first) = producer.scalar(0).await.unwrap();
        let (producer, second) = producer.scalar(1).await.unwrap();
        assert_eq!(producer.scalar_len(&first, 0).unwrap(), 2);
        assert_eq!(producer.scalar_len(&second, 1).unwrap(), 1);
        let before = f.budget.remaining(&f.job).unwrap();
        assert!(producer.scalar_len(&first, 1).is_err());
        assert!(producer.scalar_len(&second, 0).is_err());
        assert_eq!(f.budget.remaining(&f.job).unwrap(), before);
    });
}

#[test]
fn malformed_persisted_float_is_unknown_and_resource_errors_keep_their_classification() {
    futures::executor::block_on(async {
        let mut f = fixture_type(Signals::default(), false, FieldType::Float);
        f.reader.as_mut().unwrap().inner.facts.insert(
            QueueSourceKey::Field {
                collection: None,
                key: "a",
                field: "n",
            }
            .encode()
            .unwrap(),
            queue_storage_layout::encode(&FieldValue::Float(f64::NAN)).unwrap(),
        );
        let request = self::request(&f.queue, Predicate::Equal(FieldValue::Float(1.0)));
        let producer = start!(f, request);
        assert!(matches!(
            producer.scalar(0).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(f.signals.writes.load(Ordering::SeqCst), 0);
        assert!(f.reader_alive.upgrade().is_none());
        assert!(f.scratch_alive.upgrade().is_none());
        assert!(matches!(
            persisted_codec::<()>(Err(QueueError::TooLarge)),
            Err(QueueError::TooLarge)
        ));
    });
}
#[test]
fn every_in_range_and_its_exact_completion_are_required() {
    futures::executor::block_on(async {
        let mut f = fixture(Signals::default(), false);
        let request = self::request(
            &f.queue,
            Predicate::In(vec![FieldValue::Integer(1), FieldValue::Integer(2)]),
        );
        let producer = start!(f, request);
        let (producer, mut leaf) = producer.scalar(0).await.unwrap();
        assert_eq!(producer.scalar_len(&leaf, 0).unwrap(), 3);
        assert_eq!(
            leaf.set.0.rows,
            BTreeSet::from([(None, "a".into()), (None, "b".into()), (None, "c".into())])
        );
        assert_eq!(leaf.completions.len(), 2);
        let second = leaf.completions.pop().unwrap();
        assert!(producer.validate_scalar(&leaf, 0).is_err());
        leaf.completions.push(leaf.completions[0].clone());
        assert!(producer.validate_scalar(&leaf, 0).is_err());
        leaf.completions[1] = second;
        assert_eq!(producer.scalar_len(&leaf, 0).unwrap(), 3);
    });
}
#[test]
fn failure_in_second_in_range_cannot_turn_first_range_into_successful_leaf() {
    futures::executor::block_on(async {
        let mut f = fixture(
            Signals {
                fail_write: 3,
                ..Default::default()
            },
            false,
        );
        let request = self::request(
            &f.queue,
            Predicate::In(vec![FieldValue::Integer(1), FieldValue::Integer(2)]),
        );
        let producer = start!(f, request);
        assert!(producer.scalar(0).await.is_err());
        assert_eq!(f.signals.writes.load(Ordering::SeqCst), 3);
        assert_eq!(f.signals.finishes.load(Ordering::SeqCst), 1);
        assert!(f.reader_alive.upgrade().is_none());
        assert!(f.scratch_alive.upgrade().is_none());
    });
}

fn model_limits() -> QueueSourceSelectionWork {
    QueueSourceSelectionWork {
        rows: 1_000_000,
        copied_bytes: 1 << 30,
        scratch_steps: 1_000_000,
    }
}
fn used(
    before: QueueSourceSelectionWork,
    after: QueueSourceSelectionWork,
) -> QueueSourceSelectionWork {
    QueueSourceSelectionWork {
        rows: before.rows - after.rows,
        copied_bytes: before.copied_bytes - after.copied_bytes,
        scratch_steps: before.scratch_steps - after.scratch_steps,
    }
}
#[test]
fn cumulative_multi_range_work_is_exact_and_each_one_short_returns_no_partial_leaf() {
    futures::executor::block_on(async {
        let mut f = fixture(Signals::default(), false);
        let request = self::request(
            &f.queue,
            Predicate::In(vec![FieldValue::Integer(1), FieldValue::Integer(2)]),
        );
        let producer = start!(f, request);
        let (producer, leaf) = producer.scalar(0).await.unwrap();
        let cost = used(model_limits(), f.budget.remaining(&f.job).unwrap());
        assert!(cost.rows > 0 && cost.copied_bytes > 0 && cost.scratch_steps > 0);
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
            let mut f = fixture(Signals::default(), false);
            let mut limit = cost;
            match dimension {
                1 => limit.rows -= 1,
                2 => limit.copied_bytes -= 1,
                3 => limit.scratch_steps -= 1,
                _ => {}
            }
            // A new request trial, never a replacement in a running producer.
            let budget = QueueSourceSelectionBudget::new(limit, &f.job).unwrap();
            f.scratch.as_mut().unwrap().budget = budget.clone();
            f.budget = budget;
            let request = self::request(
                &f.queue,
                Predicate::In(vec![FieldValue::Integer(1), FieldValue::Integer(2)]),
            );
            let producer = start!(f, request);
            let result = producer.scalar(0).await;
            assert_eq!(result.is_ok(), dimension == 0);
            if let Ok((producer, leaf)) = result {
                assert_eq!(producer.scalar_len(&leaf, 0).unwrap(), 3);
                assert_eq!(
                    f.budget.remaining(&f.job).unwrap(),
                    QueueSourceSelectionWork::default()
                );
                drop(leaf);
                drop(producer);
            }
            assert!(f.reader_alive.upgrade().is_none());
            assert!(f.scratch_alive.upgrade().is_none());
            let remaining = f.budget.remaining(&f.job).unwrap();
            assert!(remaining.rows < limit.rows);
            assert!(remaining.copied_bytes < limit.copied_bytes);
        }
    });
}
#[test]
fn validation_entry_admission_is_exact_before_validation_and_header_dispatch() {
    futures::executor::block_on(async {
        for headroom in [4096u64, 4095] {
            let mut f = fixture(Signals::default(), false);
            let baseline = f.admission.usage().unwrap().1;
            let source_charge = f.reader.as_ref().unwrap().inner.identity.admitted_bytes();
            let filler = f
                .job
                .owned_buffer((32 << 20) - baseline - headroom)
                .unwrap();
            let before = f.budget.remaining(&f.job).unwrap();
            let request = self::request(&f.queue, Predicate::Equal(FieldValue::Integer(1)));
            let result = QueueSourceCandidateProducer::begin(
                f.reader.take().unwrap(),
                f.scratch.take().unwrap(),
                &f.definition,
                QueueSourceSelectionRequest::Claim(&request),
                0,
                &QueueLimits::default(),
                f.job.clone(),
                f.budget.clone(),
            )
            .await;
            assert_eq!(result.is_ok(), headroom == 4096);
            assert_eq!(
                f.signals.lookups.load(Ordering::SeqCst),
                if headroom == 4096 { 2 } else { 0 }
            );
            if headroom == 4095 {
                assert_eq!(f.budget.remaining(&f.job).unwrap(), before);
            }
            drop(result);
            assert!(f.reader_alive.upgrade().is_none());
            assert!(f.scratch_alive.upgrade().is_none());
            assert_eq!(
                f.admission.usage().unwrap().1,
                baseline + filler.admitted_bytes() - source_charge
            );
            drop(filler);
            assert_eq!(f.admission.usage().unwrap().1, baseline - source_charge);
        }
    });
}
#[test]
fn administrative_validation_is_admitted_before_invalid_selector_with_indexed_schema() {
    use fireweed_core::queue_administration::{
        AdministrativeAction, AdministrativeClause, WorkSelector,
    };
    futures::executor::block_on(async {
        for headroom in [4096u64, 4095] {
            let mut f = fixture_indexed(Signals::default(), false, FieldType::Integer, true);
            let baseline = f.admission.usage().unwrap().1;
            let filler = f
                .job
                .owned_buffer((32 << 20) - baseline - headroom)
                .unwrap();
            let before = f.budget.remaining(&f.job).unwrap();
            let intent = AdministrativeIntent {
                evaluated_at: 0,
                before_fields: None,
                operation: AdministrativeOperation::SelectFirst(vec![AdministrativeClause {
                    id: "invalid-selector".into(),
                    selector: WorkSelector {
                        nodes: vec![SelectorNode::Not],
                    },
                    precondition: WorkSelector::all(vec![]),
                    action: AdministrativeAction::Delete {
                        lease_guard: AdministrativeLeaseGuard::RejectActive,
                    },
                }]),
            };
            let result = QueueSourceCandidateProducer::begin(
                f.reader.take().unwrap(),
                f.scratch.take().unwrap(),
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
            .await;
            match result {
                Err(QueueError::OutcomeUnknown(message)) if headroom == 4095 => {
                    assert!(message.contains("admitted buffer capacity exhausted"));
                }
                Err(QueueError::Invalid(_)) if headroom == 4096 => {}
                _ => panic!("validation must follow its Root admission"),
            }
            assert_eq!(f.signals.lookups.load(Ordering::SeqCst), 0);
            if headroom == 4095 {
                assert_eq!(f.budget.remaining(&f.job).unwrap(), before);
            }
            assert!(f.reader_alive.upgrade().is_none());
            assert!(f.scratch_alive.upgrade().is_none());
            drop(filler);
        }
    });
}
async fn late_batch_trial(headroom: u64, assert_boundary: bool) -> bool {
    let mut f = fixture(
        Signals {
            late_page_headroom: Some(headroom),
            ..Default::default()
        },
        false,
    );
    let baseline = f.admission.usage().unwrap().1;
    let source_charge = f.reader.as_ref().unwrap().inner.identity.admitted_bytes();
    let request = self::request(&f.queue, Predicate::Equal(FieldValue::Integer(1)));
    let producer = start!(f, request);
    let result = producer.scalar(0).await;
    let success = result.is_ok();
    if let Ok((producer, leaf)) = &result {
        assert_eq!(producer.scalar_len(leaf, 0).unwrap(), 2);
        assert_eq!(
            leaf.set.0.rows,
            BTreeSet::from([(None, "a".into()), (None, "b".into())])
        );
    }
    if assert_boundary && !success {
        assert_eq!(f.signals.visits.load(Ordering::SeqCst), 2);
        assert_eq!(f.signals.writes.load(Ordering::SeqCst), 1);
        assert_eq!(f.signals.lookups.load(Ordering::SeqCst), 4); // header + first row only
        assert_eq!(f.signals.finishes.load(Ordering::SeqCst), 0);
        let visited = f.signals.visits.load(Ordering::SeqCst)
            + f.signals.lookups.load(Ordering::SeqCst)
            + f.signals.writes.load(Ordering::SeqCst);
        assert_eq!(
            used(model_limits(), f.budget.remaining(&f.job).unwrap()).rows,
            visited as u64
        );
    }
    drop(result);
    assert!(f.reader_alive.upgrade().is_none());
    assert!(f.scratch_alive.upgrade().is_none());
    assert_eq!(f.admission.usage().unwrap().1, baseline - source_charge);
    success
}
#[test]
fn late_batch_copy_admission_exact_limit_and_one_less_release_every_owned_charge() {
    futures::executor::block_on(async {
        // Find the actual owned-accounting boundary with fresh independent
        // trials. This does not measure an allocator or physical SDK peak.
        let (mut lower, mut upper) = (0, 4096);
        assert!(late_batch_trial(upper, false).await);
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            if late_batch_trial(middle, false).await {
                upper = middle;
            } else {
                lower = middle + 1;
            }
        }
        assert!(lower > std::mem::size_of::<Batch>() as u64);
        assert!(late_batch_trial(lower, true).await);
        assert!(!late_batch_trial(lower - 1, true).await);
    });
}
