mod batch;
mod body;
use super::*;
use crate::{
    queue_admission::QueueAdmission,
    queue_log_record::{OwnedQueueLogRecord, QueueLogPayload, encode_checked_genesis},
    queue_schema_codec::encode_definition,
    queue_source_index::predecessor::tests::{Reader, group, record},
    queue_storage_layout::{decode, encode},
};
use std::sync::atomic::{AtomicUsize, Ordering};
mod oracle {
    fireweed_conformance::define_queue_source_oracle!(crate);
}
struct Fixture {
    admission: Arc<QueueAdmission>,
    history: Vec<OwnedQueueLogRecord>,
    reader: Reader,
    queue: QueueAddress,
    definition: AdmittedQueueDefinition,
    snapshot: RecordSnapshot,
    job: Arc<QueueReservation>,
}
fn fixture() -> Fixture {
    fixture_with_work(vec![work("a", "a"), work("b", "b")])
}
fn fixture_with_work(items: Vec<NewWork>) -> Fixture {
    let admission = QueueAdmission::new(2, 32 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let collection = CollectionDefinition {
        fields: BTreeMap::from([
            ("u".into(), FieldType::Text),
            ("n".into(), FieldType::Integer),
        ]),
        indexes: BTreeMap::from([("u".into(), vec!["u".into()])]),
        unique_indexes: BTreeSet::from(["u".into()]),
        additional_field_prefixes: Default::default(),
    };
    let schema = QueueSchema {
        work: collection.clone(),
        state: BTreeMap::from([("s".into(), collection)]),
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
    seed.work_inserts = items;
    seed.state_updates.push(StateUpdate {
        collection: "s".into(),
        key: "state".into(),
        expect: StateExpect::Absent,
        change: StateChange::Put {
            fields: work("", "state").fields,
            value: vec![5].into(),
        },
    });
    let (publication, snapshot) = record(&queue, &schema, &RecordSnapshot::default(), &seed, &job);
    let history = vec![genesis, publication];
    let facts = oracle::fold(&history, &snapshot, &schema);
    assert!(!oracle::inventory(&snapshot).is_empty());
    Fixture {
        admission,
        history,
        reader: Reader {
            through: 2,
            identity: Arc::new(job.owned_buffer(128).unwrap()),
            facts,
        },
        queue,
        definition,
        snapshot,
        job,
    }
}
fn work(key: &str, unique: &str) -> NewWork {
    NewWork {
        key: key.into(),
        priority: 0,
        available_at: 0,
        fields: Fields::from([
            ("u".into(), FieldValue::Text(unique.into())),
            ("n".into(), FieldValue::Integer(1)),
        ]),
        body: vec![1, 2].into(),
    }
}
#[derive(Default)]
struct Clock(AtomicUsize);
impl crate::Clock for Clock {
    fn now(&self) -> fireweed_core::UtcTimestamp {
        self.0.fetch_add(1, Ordering::SeqCst);
        fireweed_core::UtcTimestamp {
            seconds: 1,
            nanoseconds: 0,
        }
    }
}
#[test]
fn atomic_state_and_next_work_match_shared_full_image_planner() {
    futures::executor::block_on(async {
        let f = fixture();
        let mut request = group("transition");
        request.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Delete,
        });
        request.work_inserts.push(work("next", "a"));
        request.state_updates.push(StateUpdate {
            collection: "s".into(),
            key: "state".into(),
            expect: StateExpect::Version(1),
            change: StateChange::Put {
                fields: work("", "changed").fields,
                value: vec![6].into(),
            },
        });
        let clock = Clock::default();
        let input = load_group_preimage(
            &f.reader,
            &f.queue,
            &request,
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await
        .unwrap();
        assert_eq!(clock.0.load(Ordering::SeqCst), 0);
        let plan = input.plan(&clock).unwrap();
        let mut expected = f.snapshot.clone();
        expected.unique_probes_loaded.extend(
            queue_planner::prospective_unique_probes(&request, &expected, f.definition.schema())
                .unwrap(),
        );
        let expected = queue_planner::plan_commit_group(
            &request,
            &mut expected,
            1_000_000_000,
            f.definition.schema(),
        )
        .unwrap();
        assert!(matches!(
            plan.receipt().outcome,
            GroupOutcome::Committed { .. }
        ));
        assert_eq!(plan.delta.state.len(), 1);
        assert_eq!(plan.delta.state[0].key, "state");
        assert_eq!(plan.delta.state[0].value.as_ref(), &[6]);
        assert_eq!(plan.delta.deleted_work, vec!["a"]);
        assert_eq!(plan.delta.work.len(), 1);
        assert_eq!(plan.delta.work[0].record.work.key, "next");
        assert_eq!(plan.receipt(), &expected.receipt);
        assert_eq!(plan.delta.state, expected.state);
        assert_eq!(plan.delta.deleted_work, expected.deleted_work);
        assert_eq!(plan.delta.work, expected.work);
        assert_eq!(clock.0.load(Ordering::SeqCst), 1);
    });
}
#[test]
fn state_check_uses_metadata_but_put_requires_complete_valid_fields() {
    futures::executor::block_on(async {
        let mut f = fixture();
        f.reader.facts.insert(
            QueueSourceKey::Field {
                collection: Some("s"),
                key: "state",
                field: "n",
            }
            .encode()
            .unwrap(),
            vec![255],
        );
        let mut request = group("check");
        request.state_updates.push(StateUpdate {
            collection: "s".into(),
            key: "state".into(),
            expect: StateExpect::Version(1),
            change: StateChange::Check,
        });
        let clock = Clock::default();
        let plan = load_group_preimage(
            &f.reader,
            &f.queue,
            &request,
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await
        .unwrap()
        .plan(&clock)
        .unwrap();
        assert!(
            matches!(plan.receipt().outcome, GroupOutcome::Committed { ref versions } if versions.is_empty())
        );
        drop(plan);
        request.state_updates[0].change = StateChange::Delete;
        assert!(matches!(
            load_group_preimage(
                &f.reader,
                &f.queue,
                &request,
                &f.definition,
                &QueueLimits::default(),
                &f.job
            )
            .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
    });
}
#[test]
fn missing_released_unique_owner_cannot_turn_into_success_or_business_rejection() {
    futures::executor::block_on(async {
        let mut f = fixture();
        let mut request = group("release");
        request.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Delete,
        });
        let tuple =
            queue_planner::prospective_unique_probes(&request, &f.snapshot, f.definition.schema())
                .unwrap()
                .into_iter()
                .next()
                .unwrap();
        let key = QueueSourceKey::UniqueOwner {
            collection: None,
            index: &tuple.index,
            sort: &tuple.full_sort_key,
        }
        .encode()
        .unwrap();
        assert!(f.reader.facts.remove(&key).is_some());
        let preimage = load_group_preimage(
            &f.reader,
            &f.queue,
            &request,
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await
        .unwrap();
        assert!(matches!(
            preimage.plan(&Clock::default()),
            Err(QueueError::OutcomeUnknown(_))
        ));
    });
}
#[test]
fn selected_field_count_and_future_sequence_are_rejected_before_clock() {
    futures::executor::block_on(async {
        let mut f = fixture();
        let mut request = group("patch");
        request.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                fields: BTreeMap::from([("n".into(), Some(FieldValue::Integer(2)))]),
                ..Default::default()
            }),
        });
        let key = QueueSourceKey::Record {
            collection: None,
            key: "a",
        }
        .encode()
        .unwrap();
        let mut fact: QueueSourceRecord = decode(&f.reader.facts[&key]).unwrap();
        fact.field_count = 0;
        f.reader.facts.insert(key.clone(), encode(&fact).unwrap());
        assert!(matches!(
            load_group_preimage(
                &f.reader,
                &f.queue,
                &request,
                &f.definition,
                &QueueLimits::default(),
                &f.job
            )
            .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        fact.field_count = 2;
        if let Some(QueueSourceLive::Work {
            ref mut metadata, ..
        }) = fact.live
        {
            metadata.insertion_sequence = 999;
        }
        f.reader.facts.insert(key, encode(&fact).unwrap());
        assert!(matches!(
            load_group_preimage(
                &f.reader,
                &f.queue,
                &request,
                &f.definition,
                &QueueLimits::default(),
                &f.job
            )
            .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
    });
}

#[test]
fn mixed_work_check_never_reads_another_keys_mutation_fields() {
    futures::executor::block_on(async {
        let mut f = fixture();
        let key = QueueSourceKey::Record {
            collection: None,
            key: "a",
        }
        .encode()
        .unwrap();
        let mut fact: QueueSourceRecord = decode(&f.reader.facts[&key]).unwrap();
        let Some(QueueSourceLive::Work {
            ref mut metadata, ..
        }) = fact.live
        else {
            panic!()
        };
        let claimed = crate::queue_claim::plan_claim(
            f.snapshot.work["a"].clone(),
            vec![9; 32],
            crate::queue_claim::ClaimContext {
                now: 0,
                eligible_at: 0,
                expires_at: 2_000_000_000,
                insertion_head: f.snapshot.next_sequence,
            },
        )
        .unwrap();
        let record = &claimed.work.record;
        metadata.version = record.version;
        metadata.lifecycle = record.lifecycle;
        metadata.attempts = record.attempts;
        metadata.lease_expires_at = record.lease_expires_at;
        metadata.lease_hash = claimed.work.lease_hash;
        fact.highwater = record.version;
        f.reader.facts.insert(key, encode(&fact).unwrap());
        f.reader.facts.insert(
            QueueSourceKey::Field {
                collection: None,
                key: "a",
                field: "n",
            }
            .encode()
            .unwrap(),
            vec![255],
        );
        let mut request = group("mixed-check");
        request.claimed_updates.push(ClaimedUpdate {
            key: "a".into(),
            lease: LeaseCredential { token: vec![9; 32] },
            expected_version: claimed.work.record.version,
            disposition: Disposition::Check,
            patch: WorkPatch::default(),
        });
        request.work_updates.push(UnclaimedUpdate {
            key: "b".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                fields: BTreeMap::from([("n".into(), Some(FieldValue::Integer(2)))]),
                ..Default::default()
            }),
        });
        let plan = load_group_preimage(
            &f.reader,
            &f.queue,
            &request,
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await
        .unwrap()
        .plan(&Clock::default())
        .unwrap();
        assert!(
            matches!(plan.receipt().outcome, GroupOutcome::Committed { ref versions } if versions.len() == 1 && versions[0].key == "b")
        );
        assert!(!plan.preimage.snapshot.work["a"].body_loaded);
        assert!(
            plan.preimage.snapshot.work["a"]
                .record
                .work
                .fields
                .is_empty()
        );
    });
}
#[test]
fn future_body_and_state_spans_are_rejected_before_any_plan() {
    futures::executor::block_on(async {
        let mut f = fixture();
        let checked = f.history[1].checked().unwrap();
        let QueueLogPayload::Publication(publication) = checked.view().payload() else {
            panic!()
        };
        let bytes = publication
            .commands()
            .map(Result::unwrap)
            .find_map(|command| match command.effect {
                crate::queue_publication::EffectRef::WorkPut {
                    body: crate::queue_publication::BodyCommandRef::Replace(bytes),
                    ..
                } => Some(bytes),
                _ => None,
            })
            .unwrap();
        let future = QueueSourcePayload::from_checked(&checked, f.reader.through, bytes).unwrap();
        for (collection, key) in [(None, "a"), (Some("s"), "state")] {
            let source_key = QueueSourceKey::Record { collection, key }.encode().unwrap();
            let mut fact: QueueSourceRecord = decode(&f.reader.facts[&source_key]).unwrap();
            match fact.live.as_mut().unwrap() {
                QueueSourceLive::Work { body, .. } => *body = future,
                QueueSourceLive::State { value } => *value = future,
            }
            let original = f
                .reader
                .facts
                .insert(source_key.clone(), encode(&fact).unwrap())
                .unwrap();
            let mut request = group("future-reference");
            if let Some(collection) = collection {
                request.state_updates.push(StateUpdate {
                    collection: collection.into(),
                    key: key.into(),
                    expect: StateExpect::Version(1),
                    change: StateChange::Check,
                });
            } else {
                request.work_updates.push(UnclaimedUpdate {
                    key: key.into(),
                    expect: StateExpect::Version(1),
                    change: UnclaimedChange::Delete,
                });
            }
            assert!(matches!(
                load_group_preimage(
                    &f.reader,
                    &f.queue,
                    &request,
                    &f.definition,
                    &QueueLimits::default(),
                    &f.job
                )
                .await,
                Err(QueueError::OutcomeUnknown(_))
            ));
            f.reader.facts.insert(source_key, original);
        }
    });
}
#[test]
fn preimage_plan_and_failed_admission_release_all_child_credit() {
    futures::executor::block_on(async {
        let f = fixture();
        let mut request = group("credit");
        request.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Delete,
        });
        let baseline = f.admission.usage().unwrap();
        let preimage = load_group_preimage(
            &f.reader,
            &f.queue,
            &request,
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await
        .unwrap();
        assert!(f.admission.usage().unwrap().1 > baseline.1);
        let plan = preimage.plan(&Clock::default()).unwrap();
        assert!(f.admission.usage().unwrap().1 > baseline.1);
        drop(plan);
        assert_eq!(f.admission.usage().unwrap(), baseline);
        let starver = f.job.owned_buffer((32 << 20) - baseline.1 - 1).unwrap();
        let occupied = f.admission.usage().unwrap();
        assert!(matches!(
            load_group_preimage(
                &f.reader,
                &f.queue,
                &request,
                &f.definition,
                &QueueLimits::default(),
                &f.job
            )
            .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(f.admission.usage().unwrap(), occupied);
        drop(starver);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}

// A deliberately unfaithful port demonstrates that equal numeric heads and an
// A→B→A reader response cannot replace the original opaque cut. The old helper
// recapture would accept these foreign field points under its temporary B cut.
struct AlternatingFields<'a> {
    inner: &'a Reader,
    foreign: Arc<OwnedQueueBufferCharge>,
    cuts: AtomicUsize,
    field: Vec<u8>,
}
impl QueueSourceRecordReader for AlternatingFields<'_> {
    type Record = OwnedQueueLogRecord;
    async fn read_record(
        &self,
        cut: &QueueSourceCut<'_>,
        offset: u64,
    ) -> QueueResult<Self::Record> {
        self.inner.read_record(cut, offset).await
    }
    fn checked_record<'a>(
        &'a self,
        owner: &'a Self::Record,
        cut: &QueueSourceCut<'_>,
        offset: u64,
    ) -> QueueResult<crate::queue_log_record::CheckedQueueLogRecord<'a>> {
        self.inner.checked_record(owner, cut, offset)
    }
}
impl QueueSourceReader for AlternatingFields<'_> {
    type Point = Vec<u8>;
    type Range = (Vec<u8>, Vec<u8>);
    fn cut(&self) -> QueueResult<QueueSourceCut<'_>> {
        if self.cuts.fetch_add(1, Ordering::SeqCst) == 1 {
            self.foreign_cut()
        } else {
            self.inner.cut()
        }
    }
    async fn lookup(&self, key: &[u8]) -> QueueResult<Self::Point> {
        self.inner.lookup(key).await
    }
    fn point<'a>(&'a self, owner: &'a Self::Point) -> QueueResult<QueueSourcePoint<'a>> {
        if owner == &self.field {
            Ok(self
                .foreign_cut()?
                .point_from_source(owner, self.inner.facts.get(owner).map(Vec::as_slice)))
        } else {
            self.inner.point(owner)
        }
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
impl AlternatingFields<'_> {
    fn foreign_cut(&self) -> QueueResult<QueueSourceCut<'_>> {
        let original = self.inner.cut()?;
        QueueSourceCut::from_captured(
            &self.foreign,
            original.address(),
            original.epoch(),
            original.through(),
            self.foreign.reservation(),
        )
    }
}
#[test]
fn fields_cannot_recapture_foreign_cut_then_return_to_original_head() {
    futures::executor::block_on(async {
        let f = fixture();
        let mut request = group("aba-fields");
        request.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                fields: BTreeMap::from([("n".into(), Some(FieldValue::Integer(2)))]),
                ..Default::default()
            }),
        });
        let reader = AlternatingFields {
            inner: &f.reader,
            foreign: Arc::new(f.job.owned_buffer(128).unwrap()),
            cuts: AtomicUsize::new(0),
            field: QueueSourceKey::Field {
                collection: None,
                key: "a",
                field: "n",
            }
            .encode()
            .unwrap(),
        };
        assert!(
            matches!(load_group_preimage(&reader, &f.queue, &request, &f.definition, &QueueLimits::default(), &f.job).await,
            Err(QueueError::OutcomeUnknown(message)) if message.contains("lookup evidence belongs to another cut"))
        );
        assert_eq!(
            reader.cuts.load(Ordering::SeqCst),
            1,
            "field loading must never recapture"
        );
    });
}

#[test]
fn borrowed_payload_sizes_do_not_multiply_preimage_scratch() {
    let f = fixture();
    let mut request = group("borrowed-payload");
    request.work_inserts.push(work("next", "next"));
    let before = request_scratch(&request, f.definition.schema()).unwrap();
    request.work_inserts[0].body = vec![0; 1 << 20].into();
    assert_eq!(
        request_scratch(&request, f.definition.schema()).unwrap(),
        before
    );
}

struct ZeroClock;
impl crate::Clock for ZeroClock {
    fn now(&self) -> fireweed_core::UtcTimestamp {
        fireweed_core::UtcTimestamp {
            seconds: 0,
            nanoseconds: 0,
        }
    }
}

#[test]
fn sealed_encoding_matches_cold_publication_and_rejects_foreign_binding() {
    futures::executor::block_on(async {
        let f = fixture();
        let mut request = group("sealed-transition");
        request.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Delete,
        });
        request.work_inserts.push(work("next", "a"));
        request.state_updates.push(StateUpdate {
            collection: "s".into(),
            key: "state".into(),
            expect: StateExpect::Version(1),
            change: StateChange::Put {
                fields: work("", "changed").fields,
                value: vec![6].into(),
            },
        });
        let (expected, _) = record(
            &f.queue,
            f.definition.schema(),
            &f.snapshot,
            &request,
            &f.job,
        );
        let limits = QueueLimits::default();
        let plan = load_group_preimage(
            &f.reader,
            &f.queue,
            &request,
            &f.definition,
            &limits,
            &f.job,
        )
        .await
        .unwrap()
        .plan(&ZeroClock)
        .unwrap();
        let publication = plan
            .encode(QueueSourceGroupEncodingLimits {
                frame_bytes: 1 << 20,
                record_bytes: 1 << 20,
            })
            .unwrap();
        let cut = f.reader.cut().unwrap();
        assert_eq!(
            publication
                .record_for(&cut, &f.definition, &f.job)
                .unwrap()
                .bytes(),
            expected.bytes()
        );
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
                .record_for(&foreign_cut, &f.definition, &f.job)
                .is_err()
        );
        let foreign_job = f.admission.reserve(0).unwrap();
        assert!(
            publication
                .record_for(&cut, &f.definition, &foreign_job)
                .is_err()
        );
        // Rejected binding attempts must not invalidate the original capability.
        assert_eq!(
            publication
                .record_for(&cut, &f.definition, &f.job)
                .unwrap()
                .bytes(),
            expected.bytes()
        );
    });
}

#[test]
fn sealed_encoding_enforces_exact_inner_and_outer_limits() {
    futures::executor::block_on(async {
        let f = fixture();
        let mut request = group("sealed-limits");
        request.work_inserts.push(work("next", "new"));
        let (expected, _) = record(
            &f.queue,
            f.definition.schema(),
            &f.snapshot,
            &request,
            &f.job,
        );
        let frame_bytes = expected
            .checked()
            .unwrap()
            .payload_interval()
            .unwrap()
            .len() as u64;
        let record_bytes = expected.bytes().len() as u64;
        for (inner, outer, succeeds) in [
            (frame_bytes, record_bytes, true),
            (frame_bytes - 1, record_bytes, false),
            (frame_bytes, record_bytes - 1, false),
        ] {
            let limits = QueueLimits::default();
            let plan = load_group_preimage(
                &f.reader,
                &f.queue,
                &request,
                &f.definition,
                &limits,
                &f.job,
            )
            .await
            .unwrap()
            .plan(&ZeroClock)
            .unwrap();
            let result = plan.encode(QueueSourceGroupEncodingLimits {
                frame_bytes: inner,
                record_bytes: outer,
            });
            if succeeds {
                let publication = result.unwrap();
                assert_eq!(
                    publication
                        .record_for(&f.reader.cut().unwrap(), &f.definition, &f.job)
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

#[test]
fn sealed_encoding_failure_and_output_drop_release_owned_credit() {
    futures::executor::block_on(async {
        let f = fixture();
        let mut request = group("encoding-credit");
        request.work_inserts.push(work("next", "new"));
        let baseline = f.admission.usage().unwrap();
        let limits = QueueLimits::default();
        let plan = load_group_preimage(
            &f.reader,
            &f.queue,
            &request,
            &f.definition,
            &limits,
            &f.job,
        )
        .await
        .unwrap()
        .plan(&ZeroClock)
        .unwrap();
        let occupied = f.admission.usage().unwrap().1;
        let starver = f.job.owned_buffer((32 << 20) - occupied - 1).unwrap();
        let starver_bytes = starver.admitted_bytes();
        let result = plan.encode(QueueSourceGroupEncodingLimits {
            frame_bytes: 1 << 20,
            record_bytes: 1 << 20,
        });
        assert!(matches!(result, Err(QueueError::OutcomeUnknown(_))));
        assert_eq!(f.admission.usage().unwrap().1, baseline.1 + starver_bytes);
        drop(starver);
        assert_eq!(f.admission.usage().unwrap(), baseline);
        let plan = load_group_preimage(
            &f.reader,
            &f.queue,
            &request,
            &f.definition,
            &limits,
            &f.job,
        )
        .await
        .unwrap()
        .plan(&ZeroClock)
        .unwrap();
        let publication = plan
            .encode(QueueSourceGroupEncodingLimits {
                frame_bytes: 1 << 20,
                record_bytes: 1 << 20,
            })
            .unwrap();
        assert!(f.admission.usage().unwrap().1 > baseline.1);
        drop(publication);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}

#[test]
fn sealed_encoding_later_admission_failures_restore_credit() {
    futures::executor::block_on(async {
        let f = fixture();
        let mut request = group(&"literal-request-".repeat(512));
        request.work_inserts.push(work(
            &"literal-key-".repeat(256),
            &"unique-value-".repeat(128),
        ));
        let (expected, _) = record(
            &f.queue,
            f.definition.schema(),
            &f.snapshot,
            &request,
            &f.job,
        );
        let frame_charge = expected
            .checked()
            .unwrap()
            .payload_interval()
            .unwrap()
            .len() as u64
            + 128;
        let outer_charge = expected.bytes().len() as u64 * 4 + 4096;
        let baseline = f.admission.usage().unwrap();
        let limits = QueueLimits::default();
        for fail_outer in [false, true] {
            let plan = load_group_preimage(
                &f.reader,
                &f.queue,
                &request,
                &f.definition,
                &limits,
                &f.job,
            )
            .await
            .unwrap()
            .plan(&ZeroClock)
            .unwrap();
            let canonical_charge = crate::queue_memory::group_expansion_budget(
                &request,
                &plan.preimage.snapshot,
                f.definition.schema(),
            )
            .unwrap()
                + 4096;
            // Receipt-bound validation scratch is smaller than the long literal
            // frame, so this reaches the intended inner or outer allocation.
            assert!(frame_charge > plan.delta.versions.len() as u64 * 192 + 1024);
            let remaining =
                canonical_charge + frame_charge - 1 + if fail_outer { outer_charge } else { 0 };
            let occupied = f.admission.usage().unwrap().1;
            let starver = f
                .job
                .owned_buffer((32 << 20) - occupied - remaining)
                .unwrap();
            let starver_bytes = starver.admitted_bytes();
            let result = plan.encode(QueueSourceGroupEncodingLimits {
                frame_bytes: 1 << 20,
                record_bytes: 1 << 20,
            });
            assert!(matches!(result, Err(QueueError::OutcomeUnknown(_))));
            assert_eq!(f.admission.usage().unwrap().1, baseline.1 + starver_bytes);
            drop(starver);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
        let plan = load_group_preimage(
            &f.reader,
            &f.queue,
            &request,
            &f.definition,
            &limits,
            &f.job,
        )
        .await
        .unwrap()
        .plan(&ZeroClock)
        .unwrap();
        let publication = plan
            .encode(QueueSourceGroupEncodingLimits {
                frame_bytes: 1 << 20,
                record_bytes: 1 << 20,
            })
            .unwrap();
        // Only the returned checked record remains funded; plan, canonical
        // containers, receipt validation scratch and inner frame are all gone.
        assert_eq!(f.admission.usage().unwrap().1, baseline.1 + outer_charge);
        assert_eq!(
            publication
                .record_for(&f.reader.cut().unwrap(), &f.definition, &f.job)
                .unwrap()
                .bytes(),
            expected.bytes()
        );
        drop(publication);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}

#[test]
fn preview_consumes_source_plan_and_retains_independent_output_funding() {
    futures::executor::block_on(async {
        let f = fixture();
        let admission = f.admission.clone();
        let mut request = group("preview-independent");
        request.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Delete,
        });
        let clock = Clock::default();
        let before = admission.usage().unwrap();
        let output = load_group_preimage(
            &f.reader,
            &f.queue,
            &request,
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await
        .unwrap()
        .plan(&clock)
        .unwrap()
        .into_preview(1 << 20)
        .unwrap();
        assert_eq!(clock.0.load(Ordering::SeqCst), 1);
        assert!(admission.usage().unwrap().1 > before.1);
        drop(f);
        assert_eq!(
            admission.usage().unwrap().0,
            1,
            "output retains its reservation after all source owners retire"
        );
        assert!(admission.usage().unwrap().1 > 0);
        // A caller that disappears before handoff still destroys the DTO before
        // its final output guard; no naked result escapes this branch.
        drop(output);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    });
}

#[test]
fn preview_response_limit_drops_consumed_plan_without_publishing_an_original() {
    futures::executor::block_on(async {
        let f = fixture();
        let mut request = group("preview-too-large");
        request.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Delete,
        });
        let before = f.admission.usage().unwrap();
        let facts = f.reader.facts.clone();
        let output = load_group_preimage(
            &f.reader,
            &f.queue,
            &request,
            &f.definition,
            &QueueLimits::default(),
            &f.job,
        )
        .await
        .unwrap()
        .plan(&Clock::default())
        .unwrap()
        .into_preview(0);
        assert_eq!(output.err().unwrap(), QueueError::TooLarge);
        assert_eq!(f.admission.usage().unwrap(), before);
        assert_eq!(f.reader.facts, facts);
    });
}

#[test]
fn source_planning_accepts_exact_signed_timestamp_boundaries_and_rejects_adjacent_overflow() {
    struct BoundaryClock(fireweed_core::UtcTimestamp);
    impl crate::Clock for BoundaryClock {
        fn now(&self) -> fireweed_core::UtcTimestamp {
            self.0
        }
    }
    futures::executor::block_on(async {
        let f = fixture();
        let mut request = group("signed-clock");
        request.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Delete,
        });
        for (seconds, nanoseconds, expected) in [
            (-9_223_372_037, 145_224_192, Some(i64::MIN)),
            (9_223_372_036, 854_775_807, Some(i64::MAX)),
            (-9_223_372_037, 145_224_191, None),
            (9_223_372_036, 854_775_808, None),
        ] {
            let clock = BoundaryClock(fireweed_core::UtcTimestamp {
                seconds,
                nanoseconds,
            });
            let plan = load_group_preimage(
                &f.reader,
                &f.queue,
                &request,
                &f.definition,
                &QueueLimits::default(),
                &f.job,
            )
            .await
            .unwrap()
            .plan(&clock);
            if let Some(exact) = expected {
                assert_eq!(timestamp_nanos(clock.0).unwrap(), exact);
                assert!(matches!(
                    plan.unwrap().receipt().outcome,
                    GroupOutcome::Committed { .. }
                ));
            } else {
                assert_eq!(
                    plan.err().unwrap(),
                    QueueError::OutcomeUnknown(
                        "source group preimage: clock outside queue time domain".into()
                    )
                );
            }
        }
    });
}
