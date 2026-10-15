use super::super::tests::{Reader, group, record};
use super::*;
use crate::{
    queue_admission::QueueAdmission,
    queue_log_record::encode_checked_genesis,
    queue_planner::RecordSnapshot,
    queue_publication::EffectRef,
    queue_schema_codec::encode_definition,
    queue_source_index::{QueueSourceGenesis, QueueSourcePayload, QueueSourceSpan},
};
use fireweed_core::queue_contract::*;
use std::collections::BTreeSet;

const COLLECTION: &str = "state\0α";
struct Fixture {
    admission: Arc<QueueAdmission>,
    job: Arc<QueueReservation>,
    schema: QueueSchema,
    definition: AdmittedQueueDefinition,
    queue: QueueAddress,
    reader: Reader,
    snapshot: RecordSnapshot,
    keys: [String; 2],
    _genesis: OwnedQueueLogRecord,
    _seed: OwnedQueueLogRecord,
}
fn owner_inventory(snapshot: &RecordSnapshot, schema: &QueueSchema) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut output = BTreeMap::new();
    for ((collection, key), record) in &snapshot.state {
        let definition = &schema.state[collection];
        for index in &definition.unique_indexes {
            let values: Vec<_> = definition.indexes[index]
                .iter()
                .map(|name| record.fields.get(name))
                .collect();
            if values.iter().any(Option::is_none) {
                continue;
            }
            let sort = crate::queue_index::ordered_key(&values).unwrap();
            let encoded_key = QueueSourceKey::UniqueOwner {
                collection: Some(collection),
                index,
                sort: &sort,
            }
            .encode()
            .unwrap();
            let owner = RecordIdentity {
                collection: Some(collection.clone()),
                key: key.clone(),
            };
            assert!(
                output
                    .insert(encoded_key, queue_storage_layout::encode(&owner).unwrap())
                    .is_none()
            );
        }
    }
    output
}
fn fixture() -> Fixture {
    fixture_with_keys(["a\0β", "b\0β"])
}
fn fixture_with_keys(keys: [&str; 2]) -> Fixture {
    fixture_with_keys_and_float(keys, None)
}
fn fixture_with_keys_and_float(keys: [&str; 2], float: Option<f64>) -> Fixture {
    let admission = QueueAdmission::new(2, 16 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let empty = CollectionDefinition {
        fields: Default::default(),
        indexes: Default::default(),
        unique_indexes: Default::default(),
        additional_field_prefixes: Default::default(),
    };
    let schema = QueueSchema {
        work: empty,
        state: BTreeMap::from([(
            COLLECTION.into(),
            CollectionDefinition {
                fields: BTreeMap::from([
                    ("f".into(), FieldType::Float),
                    ("u".into(), FieldType::Integer),
                    ("v".into(), FieldType::Text),
                ]),
                indexes: BTreeMap::from([
                    ("by_f".into(), vec!["f".into()]),
                    ("by_u".into(), vec!["u".into()]),
                    ("by_v".into(), vec!["v".into()]),
                ]),
                unique_indexes: BTreeSet::from(["by_u".into(), "by_v".into()]),
                additional_field_prefixes: Default::default(),
            },
        )]),
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
    let checked_genesis = genesis.checked().unwrap();
    let genesis_fact = QueueSourceGenesis {
        source: QueueSourceSpan::from_checked(
            &checked_genesis,
            0,
            &genesis.bytes()[checked_genesis.payload_interval().unwrap()],
        )
        .unwrap(),
        definition_pin: definition.pin(),
    };
    let mut seed = group("seed-owners");
    for (key, number) in [(keys[0], 1), (keys[1], 2)] {
        seed.state_updates.push(StateUpdate {
            collection: COLLECTION.into(),
            key: key.into(),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: {
                    let mut fields = Fields::from([
                        ("u".into(), FieldValue::Integer(number)),
                        ("v".into(), FieldValue::Text(key.into())),
                    ]);
                    if let Some(float) = float {
                        fields.insert("f".into(), FieldValue::Float(float));
                    }
                    fields
                },
                value: vec![number as u8].into(),
            },
        });
    }
    let (seed_record, snapshot) = record(&queue, &schema, &RecordSnapshot::default(), &seed, &job);
    let checked_seed = seed_record.checked().unwrap();
    let QueueLogPayload::Publication(publication) = checked_seed.view().payload() else {
        panic!()
    };
    let mut facts = owner_inventory(&snapshot, &schema);
    facts.insert(
        QueueSourceKey::Genesis.encode().unwrap(),
        queue_storage_layout::encode(&genesis_fact).unwrap(),
    );
    facts.insert(
        QueueSourceKey::Sequence.encode().unwrap(),
        queue_storage_layout::encode(&0u64).unwrap(),
    );
    for command in publication.commands() {
        let command = command.unwrap();
        let EffectRef::StatePut { value, .. } = command.effect else {
            panic!()
        };
        let state = &snapshot.state[&(COLLECTION.into(), command.identity.key.into())];
        let fact = QueueSourceRecord {
            highwater: state.version,
            field_count: state.fields.len() as u64,
            live: Some(QueueSourceLive::State {
                value: QueueSourcePayload::from_checked(&checked_seed, 1, value).unwrap(),
            }),
        };
        facts.insert(
            QueueSourceKey::Record {
                collection: Some(COLLECTION),
                key: command.identity.key,
            }
            .encode()
            .unwrap(),
            queue_storage_layout::encode(&fact).unwrap(),
        );
        for (field, value) in &state.fields {
            facts.insert(
                QueueSourceKey::Field {
                    collection: Some(COLLECTION),
                    key: command.identity.key,
                    field,
                }
                .encode()
                .unwrap(),
                queue_storage_layout::encode(value).unwrap(),
            );
        }
    }
    Fixture {
        admission,
        reader: Reader {
            through: 2,
            identity: Arc::new(job.owned_buffer(128).unwrap()),
            facts,
        },
        job,
        schema,
        definition,
        queue,
        snapshot,
        keys: keys.map(str::to_string),
        _genesis: genesis,
        _seed: seed_record,
    }
}
fn changes(f: &Fixture, swap: bool) -> CommitGroup {
    let mut request = group(if swap { "swap" } else { "preserve-owners" });
    for (position, key) in f.keys.iter().enumerate() {
        let other = &f.keys[1 - position];
        request.state_updates.push(StateUpdate {
            collection: COLLECTION.into(),
            key: key.into(),
            expect: StateExpect::Version(1),
            change: StateChange::Put {
                fields: f.snapshot.state
                    [&(COLLECTION.into(), if swap { other } else { key }.clone())]
                    .fields
                    .clone(),
                value: Default::default(),
            },
        });
    }
    request
}
async fn accumulate<'a, R: QueueSourceReader>(
    context: &QueueSourcePublicationContext<'a, R>,
) -> QueueResult<QueueSourceUniqueOwnerEdits<'a>> {
    let mut owners = context.unique_owners()?;
    for command in context.commands() {
        let command = command?;
        let predecessor = context.predecessor(&command).await?;
        let transition = context.record_transition(&predecessor).await?;
        owners.observe(&transition)?;
    }
    owners.finish().await
}
#[test]
fn source_owners_swap_and_noop_match_complete_planner_inventory() {
    futures::executor::block_on(async {
        let f = fixture();
        for swap in [false, true] {
            let (proposed, after) =
                record(&f.queue, &f.schema, &f.snapshot, &changes(&f, swap), &f.job);
            let checked = proposed.checked().unwrap();
            let context =
                QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                    .await
                    .unwrap();
            let baseline = f.admission.usage().unwrap();
            let edits = accumulate(&context).await.unwrap();
            edits.verify(&proposed, &context.cut, &f.job).unwrap();
            {
                let (twin, _) =
                    record(&f.queue, &f.schema, &f.snapshot, &changes(&f, swap), &f.job);
                assert!(edits.verify(&twin, &context.cut, &f.job).is_err());
                let identity = Arc::new(f.job.owned_buffer(128).unwrap());
                let foreign_cut =
                    QueueSourceCut::from_captured(&identity, context.cut.address(), 1, 2, &f.job)
                        .unwrap();
                assert!(edits.verify(&proposed, &foreign_cut, &f.job).is_err());
                let foreign_job = f.admission.reserve(0).unwrap();
                assert!(edits.verify(&proposed, &context.cut, &foreign_job).is_err());
            }

            let mut actual = owner_inventory(&f.snapshot, &f.schema);
            for (key, value) in edits.edits() {
                match value {
                    Some(value) => {
                        actual.insert(key.to_vec(), value.to_vec());
                    }
                    None => {
                        assert!(actual.remove(key).is_some());
                    }
                }
            }
            assert_eq!(actual, owner_inventory(&after, &f.schema));
            assert_eq!(edits.edits().len(), if swap { 4 } else { 0 });
            drop(edits);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
    });
}
#[test]
fn source_owner_command_coverage_is_exact_and_failure_is_sticky() {
    futures::executor::block_on(async {
        let f = fixture();
        let (proposed, _) = record(
            &f.queue,
            &f.schema,
            &f.snapshot,
            &changes(&f, false),
            &f.job,
        );
        let checked = proposed.checked().unwrap();
        let context =
            QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                .await
                .unwrap();
        let other =
            QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                .await
                .unwrap();
        let baseline = f.admission.usage().unwrap();
        let commands = context.commands().collect::<QueueResult<Vec<_>>>().unwrap();
        let p0 = context.predecessor(&commands[0]).await.unwrap();
        let p1 = context.predecessor(&commands[1]).await.unwrap();
        let t0 = context.record_transition(&p0).await.unwrap();
        let t1 = context.record_transition(&p1).await.unwrap();
        let retained = f.admission.usage().unwrap();
        assert!(context.unique_owners().unwrap().finish().await.is_err());
        let mut omitted = context.unique_owners().unwrap();
        omitted.observe(&t0).unwrap();
        assert!(omitted.finish().await.is_err());
        let mut duplicate = context.unique_owners().unwrap();
        duplicate.observe(&t0).unwrap();
        assert!(duplicate.observe(&t0).is_err());
        assert!(duplicate.observe(&t1).is_err());
        assert!(duplicate.finish().await.is_err());
        let mut reordered = context.unique_owners().unwrap();
        reordered.observe(&t1).unwrap();
        assert!(reordered.observe(&t0).is_err());
        assert!(reordered.finish().await.is_err());
        let mut foreign = other.unique_owners().unwrap();
        assert!(foreign.observe(&t0).is_err());
        assert!(foreign.finish().await.is_err());
        assert_eq!(f.admission.usage().unwrap(), retained);
        drop((t0, t1));
        drop((p0, p1));
        drop(commands);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}

#[derive(Clone, Copy)]
enum Fault {
    None,
    Source,
    Cut,
}
struct FaultReader<'a> {
    base: &'a Reader,
    selected: Vec<u8>,
    fault: Fault,
    foreign: Arc<OwnedQueueBufferCharge>,
    owner_reads: std::sync::atomic::AtomicUsize,
}
impl QueueSourceReader for FaultReader<'_> {
    type Point = Vec<u8>;
    type Range = (Vec<u8>, Vec<u8>);
    fn cut(&self) -> QueueResult<QueueSourceCut<'_>> {
        self.base.cut()
    }
    async fn lookup(&self, key: &[u8]) -> QueueResult<Self::Point> {
        if key.starts_with(&[1, 6]) {
            self.owner_reads
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        if key == self.selected && matches!(self.fault, Fault::Source) {
            return Err(unknown("injected owner source failure"));
        }
        self.base.lookup(key).await
    }
    fn point<'a>(
        &'a self,
        owner: &'a Self::Point,
    ) -> QueueResult<super::super::super::QueueSourcePoint<'a>> {
        if owner == &self.selected && matches!(self.fault, Fault::Cut) {
            let cut = QueueSourceCut::from_captured(
                &self.foreign,
                self.cut()?.address(),
                1,
                2,
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
            super::super::super::QueueSourcePoint<'a>,
        ) -> QueueResult<()>,
    ) -> QueueResult<()> {
        self.base.visit_range(owner, lower, upper, visitor)
    }
}
fn fault_reader<'a>(f: &'a Fixture, selected: Vec<u8>, fault: Fault) -> FaultReader<'a> {
    FaultReader {
        base: &f.reader,
        selected,
        fault,
        foreign: Arc::new(f.job.owned_buffer(128).unwrap()),
        owner_reads: std::sync::atomic::AtomicUsize::new(0),
    }
}
#[test]
fn late_owner_source_failure_and_wrong_cut_discard_the_whole_result() {
    futures::executor::block_on(async {
        let f = fixture();
        let (proposed, _) = record(&f.queue, &f.schema, &f.snapshot, &changes(&f, true), &f.job);
        let checked = proposed.checked().unwrap();
        let owner_keys = owner_inventory(&f.snapshot, &f.schema);
        let selected = owner_keys.keys().nth(1).unwrap().clone();
        for fault in [Fault::Source, Fault::Cut] {
            let reader = fault_reader(&f, selected.clone(), fault);
            let context =
                QueueSourcePublicationContext::begin(&reader, &checked, &f.definition, &f.job)
                    .await
                    .unwrap();
            let baseline = f.admission.usage().unwrap();
            let failure = accumulate(&context).await;
            assert!(matches!(failure, Err(QueueError::OutcomeUnknown(_))));
            assert_eq!(
                reader.owner_reads.load(std::sync::atomic::Ordering::SeqCst),
                2
            );
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
    });
}
#[test]
fn only_source_absence_means_nonownership_and_present_owner_bytes_are_strict() {
    futures::executor::block_on(async {
        let mut f = fixture();
        let (proposed, _) = record(&f.queue, &f.schema, &f.snapshot, &changes(&f, true), &f.job);
        let checked = proposed.checked().unwrap();
        let owner_keys = owner_inventory(&f.snapshot, &f.schema);
        let selected = owner_keys.keys().nth(1).unwrap().clone();
        let original = owner_keys[&selected].clone();
        let mut trailing = original.clone();
        trailing.push(0);
        let mut utf8 = original.clone();
        *utf8.last_mut().unwrap() = 255;
        let mut overlong = original.clone();
        assert!(overlong[1] < 128);
        overlong[1] |= 128;
        overlong.insert(2, 0);
        for corrupt in [
            Vec::new(),
            trailing,
            utf8,
            overlong,
            queue_storage_layout::encode(&RecordIdentity {
                collection: None,
                key: f.keys[1].clone(),
            })
            .unwrap(),
            queue_storage_layout::encode(&RecordIdentity {
                collection: Some(COLLECTION.into()),
                key: " ".into(),
            })
            .unwrap(),
        ] {
            f.reader.facts.insert(selected.clone(), corrupt);
            let reader = fault_reader(&f, selected.clone(), Fault::None);
            let context =
                QueueSourcePublicationContext::begin(&reader, &checked, &f.definition, &f.job)
                    .await
                    .unwrap();
            let baseline = f.admission.usage().unwrap();
            let failure = accumulate(&context).await;
            assert!(
                matches!(failure, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("unique owner"))
            );
            assert_eq!(
                reader.owner_reads.load(std::sync::atomic::Ordering::SeqCst),
                2
            );
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
        f.reader.facts.remove(&selected);
        let context =
            QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                .await
                .unwrap();
        let failure = accumulate(&context).await;
        assert!(
            matches!(failure, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("released tuple lacks exact initial owner"))
        );
    });
}
#[test]
fn long_literal_owners_keep_complete_identity_and_output_admission() {
    futures::executor::block_on(async {
        let keys = ["a\0β".repeat(2048), "b\0β".repeat(2048)];
        let f = fixture_with_keys([&keys[0], &keys[1]]);
        let (proposed, after) =
            record(&f.queue, &f.schema, &f.snapshot, &changes(&f, true), &f.job);
        let checked = proposed.checked().unwrap();
        let context =
            QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                .await
                .unwrap();
        let baseline = f.admission.usage().unwrap();
        let edits = accumulate(&context).await.unwrap();
        let retained = f.admission.usage().unwrap();
        assert!(retained.1 > baseline.1);
        let mut actual = owner_inventory(&f.snapshot, &f.schema);
        for (key, value) in edits.edits() {
            actual.insert(key.to_vec(), value.unwrap().to_vec());
        }
        assert_eq!(actual, owner_inventory(&after, &f.schema));
        drop(edits);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}

#[test]
fn source_owner_creation_and_deletion_match_complete_inventory() {
    futures::executor::block_on(async {
        let f = fixture();
        for delete in [false, true] {
            let mut request = group(if delete {
                "delete-owner"
            } else {
                "create-owner"
            });
            request.state_updates.push(StateUpdate {
                collection: COLLECTION.into(),
                key: if delete {
                    f.keys[0].clone()
                } else {
                    "new\0γ".into()
                },
                expect: if delete {
                    StateExpect::Version(1)
                } else {
                    StateExpect::Absent
                },
                change: if delete {
                    StateChange::Delete
                } else {
                    StateChange::Put {
                        fields: Fields::from([
                            ("u".into(), FieldValue::Integer(3)),
                            ("v".into(), FieldValue::Text("new\0γ".into())),
                        ]),
                        value: Default::default(),
                    }
                },
            });
            let (proposed, after) = record(&f.queue, &f.schema, &f.snapshot, &request, &f.job);
            let checked = proposed.checked().unwrap();
            let context =
                QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                    .await
                    .unwrap();
            let baseline = f.admission.usage().unwrap();
            let output = accumulate(&context).await.unwrap();
            assert_eq!(output.edits().len(), 2);
            assert!(f.admission.usage().unwrap().1 > baseline.1);
            let mut actual = owner_inventory(&f.snapshot, &f.schema);
            for (key, value) in output.edits() {
                assert_eq!(value.is_none(), delete);
                if let Some(value) = value {
                    assert!(actual.insert(key.to_vec(), value.to_vec()).is_none());
                } else {
                    assert!(actual.remove(key).is_some());
                }
            }
            assert_eq!(actual, owner_inventory(&after, &f.schema));
            drop(output);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
    });
}

#[test]
fn receipt_only_publication_has_no_owner_reads() {
    futures::executor::block_on(async {
        let f = fixture();
        let mut request = changes(&f, true);
        request.state_updates[0].expect = StateExpect::Version(999);
        let (proposed, after) = record(&f.queue, &f.schema, &f.snapshot, &request, &f.job);
        assert_eq!(
            owner_inventory(&after, &f.schema),
            owner_inventory(&f.snapshot, &f.schema)
        );
        let checked = proposed.checked().unwrap();
        let reader = fault_reader(&f, Vec::new(), Fault::None);
        let context =
            QueueSourcePublicationContext::begin(&reader, &checked, &f.definition, &f.job)
                .await
                .unwrap();
        assert_eq!(context.publication().summary().commands, 0);
        let baseline = f.admission.usage().unwrap();
        let empty = context
            .record_edits(super::super::super::QueueSourceCompileLimits {
                edits: 0,
                input_bytes: 0,
            })
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(empty.edits().len(), 0);
        drop(empty);
        assert_eq!(f.admission.usage().unwrap(), baseline);
        let output = accumulate(&context).await.unwrap();
        assert_eq!(output.edits().len(), 0);
        assert_eq!(
            reader.owner_reads.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        drop(output);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}

#[test]
fn ordinary_claim_collision_is_rejected_after_valid_predecessors() {
    futures::executor::block_on(async {
        let f = fixture();
        let mut request = changes(&f, false);
        for (i, update) in request.state_updates.iter_mut().enumerate() {
            update.change = StateChange::Put {
                fields: Fields::from([
                    ("u".into(), FieldValue::Integer(3 + i as i64)),
                    ("v".into(), FieldValue::Text(format!("new-{i}"))),
                ]),
                value: Default::default(),
            };
        }
        // Produce a genuine accepted plan, then simulate a structurally valid
        // publication carrying conflicting claims. Source reduction must catch
        // this even when both predecessor records and fields authenticate.
        let mut after = f.snapshot.clone();
        after.unique_probes_loaded.extend(
            crate::queue_planner::required_unique_probes(&request, &after, 0, &f.schema).unwrap(),
        );
        let mut delta =
            crate::queue_planner::plan_commit_group(&request, &mut after, 0, &f.schema).unwrap();
        assert_eq!(delta.state.len(), 2);
        delta.state[1]
            .fields
            .insert("u".into(), FieldValue::Integer(3));
        let receipt = crate::queue_publication::PublicationReceipt::commit(
            &f.queue,
            &request,
            &delta.receipt,
        )
        .unwrap();
        let canonical = crate::queue_planner::combine_deltas(
            std::slice::from_ref(&delta),
            f.snapshot.next_sequence,
        )
        .unwrap();
        let frame = crate::queue_publication::encode_publication(
            &f.queue,
            &encode_definition(&f.schema).unwrap(),
            &f.snapshot,
            &canonical,
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
        let baseline = f.admission.usage().unwrap();
        let mut owners = context.unique_owners().unwrap();
        for command in context.commands() {
            let command = command.unwrap();
            let predecessor = context.predecessor(&command).await.unwrap();
            let transition = context.record_transition(&predecessor).await.unwrap();
            owners.observe(&transition).unwrap();
        }
        let result = owners.finish().await;
        let error = result.err().expect("ordinary collision must fail");
        assert!(
            matches!(&error, QueueError::OutcomeUnknown(message)
            if message == "source predecessor: resolved unique owner effects collide"),
            "{error:?}"
        );
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}

#[test]
fn observe_admission_has_an_exact_boundary_and_releases_partial_groups() {
    futures::executor::block_on(async {
        let f = fixture();
        let (proposed, _) = record(&f.queue, &f.schema, &f.snapshot, &changes(&f, true), &f.job);
        let checked = proposed.checked().unwrap();
        let context =
            QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                .await
                .unwrap();
        let commands = context.commands().collect::<QueueResult<Vec<_>>>().unwrap();
        let p0 = context.predecessor(&commands[0]).await.unwrap();
        let p1 = context.predecessor(&commands[1]).await.unwrap();
        let t0 = context.record_transition(&p0).await.unwrap();
        let t1 = context.record_transition(&p1).await.unwrap();
        let baseline = f.admission.usage().unwrap();
        let probe = |available: u64| {
            let mut owners = context.unique_owners().unwrap();
            let used = f.admission.usage().unwrap().1;
            let filler = f.job.owned_buffer((16 << 20) - used - available).unwrap();
            let result = owners.observe(&t0).and_then(|()| owners.observe(&t1));
            if let Err(error) = &result {
                assert!(
                    matches!(error, QueueError::OutcomeUnknown(message)
                    if message.contains("capacity exhausted")),
                    "{error:?}"
                );
                assert!(owners.observe(&t1).is_err());
            }
            drop(owners);
            drop(filler);
            assert_eq!(f.admission.usage().unwrap(), baseline);
            result.is_ok()
        };
        let (mut lower, mut upper) = (0, 1 << 20);
        assert!(!probe(lower));
        assert!(probe(upper));
        while upper - lower > 1 {
            let middle = lower + (upper - lower) / 2;
            if probe(middle) {
                upper = middle;
            } else {
                lower = middle;
            }
        }
        assert!(!probe(upper - 1));
        assert!(probe(upper));
    });
}

async fn finish_probe(
    f: &Fixture,
    context: &QueueSourcePublicationContext<'_, FaultReader<'_>>,
    available: u64,
) -> (Option<String>, usize) {
    let baseline = f.admission.usage().unwrap();
    let mut owners = context.unique_owners().unwrap();
    for command in context.commands() {
        let command = command.unwrap();
        let predecessor = context.predecessor(&command).await.unwrap();
        let transition = context.record_transition(&predecessor).await.unwrap();
        owners.observe(&transition).unwrap();
    }
    let used = f.admission.usage().unwrap().1;
    let filler = f.job.owned_buffer((16 << 20) - used - available).unwrap();
    context
        .reader
        .owner_reads
        .store(0, std::sync::atomic::Ordering::SeqCst);
    let error = match owners.finish().await {
        Ok(output) => {
            drop(output);
            None
        }
        Err(error) => {
            assert!(
                matches!(&error, QueueError::OutcomeUnknown(message)
                if message.contains("capacity exhausted")),
                "{error:?}"
            );
            Some(format!("{error:?}"))
        }
    };
    let reads = context
        .reader
        .owner_reads
        .load(std::sync::atomic::Ordering::SeqCst);
    drop(filler);
    assert_eq!(f.admission.usage().unwrap(), baseline);
    (error, reads)
}

#[test]
fn finish_admission_has_an_exact_boundary_and_discards_late_output() {
    futures::executor::block_on(async {
        let f = fixture();
        let (proposed, _) = record(&f.queue, &f.schema, &f.snapshot, &changes(&f, true), &f.job);
        let checked = proposed.checked().unwrap();
        let reader = fault_reader(&f, Vec::new(), Fault::None);
        let context =
            QueueSourcePublicationContext::begin(&reader, &checked, &f.definition, &f.job)
                .await
                .unwrap();
        let (mut lower, mut upper) = (0, 1 << 20);
        assert!(finish_probe(&f, &context, lower).await.0.is_some());
        assert!(finish_probe(&f, &context, upper).await.0.is_none());
        while upper - lower > 1 {
            let middle = lower + (upper - lower) / 2;
            if finish_probe(&f, &context, middle).await.0.is_none() {
                upper = middle;
            } else {
                lower = middle;
            }
        }
        assert!(finish_probe(&f, &context, upper).await.0.is_none());
        let (error, reads) = finish_probe(&f, &context, upper - 1).await;
        assert!(error.is_some());
        // This exact budget fails while retaining a changed final owner after
        // at least one earlier tuple group completed, never returning a prefix.
        assert!(reads >= 2, "{reads}: {error:?}");
        assert!(error.unwrap().contains("unique owner output admission"));
    });
}

fn record_inventory(
    f: &Fixture,
    snapshot: &RecordSnapshot,
    records: &BTreeMap<Vec<u8>, Vec<u8>>,
) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut inventory = records.clone();
    for ((collection, key), record) in &snapshot.state {
        for (field, value) in &record.fields {
            inventory.insert(
                QueueSourceKey::Field {
                    collection: Some(collection),
                    key,
                    field,
                }
                .encode()
                .unwrap(),
                queue_storage_layout::encode(value).unwrap(),
            );
            let sort = crate::queue_index::ordered_key(&[Some(value)]).unwrap();
            inventory.insert(
                QueueSourceKey::ScalarPosting {
                    collection: Some(collection),
                    field,
                    sort: &sort,
                    key,
                }
                .encode()
                .unwrap(),
                Vec::new(),
            );
        }
        for (index, columns) in &f.schema.state[collection].indexes {
            let values: Vec<_> = columns
                .iter()
                .map(|field| record.fields.get(field))
                .collect();
            let sort = crate::queue_index::ordered_key(&values).unwrap();
            inventory.insert(
                QueueSourceKey::NamedPosting {
                    collection: Some(collection),
                    index,
                    sort: &sort,
                    key,
                }
                .encode()
                .unwrap(),
                Vec::new(),
            );
        }
    }
    inventory
}
#[test]
fn provisional_record_edits_match_complete_state_inventory_and_retain_tombstones() {
    futures::executor::block_on(async {
        let mut f = fixture();
        // Swap, delete, then recreate the same literal identity. Deletion must
        // retain its version so recreation cannot reset the ABA high-water mark.
        let mut requests = vec![changes(&f, true)];
        let mut delete = group("delete-record");
        delete.state_updates.push(StateUpdate {
            collection: COLLECTION.into(),
            key: f.keys[0].clone(),
            expect: StateExpect::Version(2),
            change: StateChange::Delete,
        });
        requests.push(delete);
        let mut recreate = group("recreate-record");
        recreate.state_updates.push(StateUpdate {
            collection: COLLECTION.into(),
            key: f.keys[0].clone(),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: Fields::from([("u".into(), FieldValue::Integer(3))]),
                value: b"recreated".to_vec().into(),
            },
        });
        requests.push(recreate);
        for request in requests {
            let records: BTreeMap<_, _> = f
                .reader
                .facts
                .iter()
                .filter(|(key, _)| key.starts_with(&[1, 2]))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let mut actual = record_inventory(&f, &f.snapshot, &records);
            let (proposed, after) = record(&f.queue, &f.schema, &f.snapshot, &request, &f.job);
            let checked = proposed.checked().unwrap();
            let context =
                QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                    .await
                    .unwrap();
            let baseline = f.admission.usage().unwrap();
            let mut builder = context
                .record_edits(super::super::super::QueueSourceCompileLimits {
                    edits: 1000,
                    input_bytes: 1 << 20,
                })
                .unwrap();
            let mut expected_records = records;
            for command in context.commands() {
                let command = command.unwrap();
                let predecessor = context.predecessor(&command).await.unwrap();
                let transition = context.record_transition(&predecessor).await.unwrap();
                builder.observe(&transition).unwrap();
                let resolved = transition.resolved();
                let identity = RecordIdentity {
                    collection: resolved.identity.collection.map(str::to_string),
                    key: resolved.identity.key.into(),
                };
                let live = match resolved.effect {
                    EffectRef::StatePut { value, .. } => Some(QueueSourceLive::State {
                        value: QueueSourcePayload::from_checked(
                            &checked,
                            context.cut.through(),
                            value,
                        )
                        .unwrap(),
                    }),
                    EffectRef::Delete => None,
                    _ => panic!("state fixture"),
                };
                let fields = after
                    .state
                    .get(&(COLLECTION.into(), identity.key.clone()))
                    .map_or(0, |record| record.fields.len());
                let fact = QueueSourceRecord {
                    highwater: after.versions[&identity],
                    field_count: fields as u64,
                    live,
                };
                expected_records.insert(
                    QueueSourceKey::Record {
                        collection: Some(COLLECTION),
                        key: &identity.key,
                    }
                    .encode()
                    .unwrap(),
                    queue_storage_layout::encode(&fact).unwrap(),
                );
            }
            let output = builder.finish().unwrap();
            output.verify(&proposed, &context.cut, &f.job).unwrap();
            assert!(
                output
                    .edits()
                    .map(|(key, _)| key)
                    .collect::<Vec<_>>()
                    .windows(2)
                    .all(|keys| keys[0] < keys[1])
            );
            for (key, value) in output.edits() {
                if let Some(value) = value {
                    actual.insert(key.to_vec(), value.to_vec());
                } else {
                    assert!(actual.remove(key).is_some());
                }
            }
            assert_eq!(actual, record_inventory(&f, &after, &expected_records));
            drop(output);
            assert_eq!(f.admission.usage().unwrap(), baseline);
            drop(context);
            // Advance the test source's authenticated facts; production readers
            // keep immutable cuts and publish this inventory atomically instead.
            f.reader
                .facts
                .retain(|key, _| !matches!(key.get(1), Some(2..=5)));
            f.reader.facts.extend(actual);
            f.snapshot = after;
            f.reader.through += 1;
        }
        assert_eq!(
            f.snapshot.state[&(COLLECTION.into(), f.keys[0].clone())].version,
            4
        );
    });
}

#[test]
fn record_edit_coverage_and_limits_fail_closed_without_partial_results() {
    futures::executor::block_on(async {
        let f = fixture();
        let (proposed, _) = record(&f.queue, &f.schema, &f.snapshot, &changes(&f, true), &f.job);
        let checked = proposed.checked().unwrap();
        let context =
            QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                .await
                .unwrap();
        let limits = super::super::super::QueueSourceCompileLimits {
            edits: 1000,
            input_bytes: 1 << 20,
        };
        let commands = context.commands().collect::<QueueResult<Vec<_>>>().unwrap();
        let p0 = context.predecessor(&commands[0]).await.unwrap();
        let p1 = context.predecessor(&commands[1]).await.unwrap();
        let t0 = context.record_transition(&p0).await.unwrap();
        let t1 = context.record_transition(&p1).await.unwrap();
        let baseline = f.admission.usage().unwrap();
        assert!(context.record_edits(limits).unwrap().finish().is_err());
        let mut omitted = context.record_edits(limits).unwrap();
        omitted.observe(&t0).unwrap();
        assert!(omitted.finish().is_err());
        let mut duplicate = context.record_edits(limits).unwrap();
        duplicate.observe(&t0).unwrap();
        assert!(duplicate.observe(&t0).is_err());
        assert!(duplicate.observe(&t1).is_err());
        assert!(duplicate.finish().is_err());
        let mut reordered = context.record_edits(limits).unwrap();
        reordered.observe(&t1).unwrap();
        assert!(reordered.observe(&t0).is_err());
        assert!(reordered.finish().is_err());
        // The first complete record has eleven edits. An eleven-edit limit lets it
        // finish and rejects the next record; no successful prefix can escape.
        let mut late = context
            .record_edits(super::super::super::QueueSourceCompileLimits {
                edits: 11,
                ..limits
            })
            .unwrap();
        late.observe(&t0).unwrap();
        assert!(late.observe(&t1).is_err());
        assert!(late.finish().is_err());
        assert_eq!(f.admission.usage().unwrap(), baseline);
        let mut complete = context.record_edits(limits).unwrap();
        complete.observe(&t0).unwrap();
        complete.observe(&t1).unwrap();
        let complete = complete.finish().unwrap();
        let exact_bytes = complete
            .edits()
            .map(|(key, value)| (key.len() + value.map_or(0, <[u8]>::len)) as u64)
            .sum::<u64>();
        let exact_edits = complete.edits().len() as u64;
        drop(complete);
        for (available, success) in [(exact_bytes, true), (exact_bytes - 1, false)] {
            let mut builder = context
                .record_edits(super::super::super::QueueSourceCompileLimits {
                    edits: exact_edits,
                    input_bytes: available,
                })
                .unwrap();
            let result = builder.observe(&t0).and_then(|()| builder.observe(&t1));
            assert_eq!(result.is_ok(), success);
            assert_eq!(builder.finish().is_ok(), success);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
        let probe = |available: u64| {
            let mut builder = context.record_edits(limits).unwrap();
            let used = f.admission.usage().unwrap().1;
            let filler = f.job.owned_buffer((16 << 20) - used - available).unwrap();
            let first = builder.observe(&t0).is_ok();
            let error = if first {
                builder.observe(&t1).err().map(|error| format!("{error:?}"))
            } else {
                Some("first command exhausted admission".into())
            };
            if error.is_none() {
                drop(builder.finish().unwrap());
            } else {
                assert!(builder.finish().is_err());
            }
            drop(filler);
            assert_eq!(f.admission.usage().unwrap(), baseline);
            (error, first)
        };
        let (mut lower, mut upper) = (0, 1 << 20);
        assert!(probe(lower).0.is_some());
        assert!(probe(upper).0.is_none());
        while upper - lower > 1 {
            let middle = lower + (upper - lower) / 2;
            if probe(middle).0.is_none() {
                upper = middle;
            } else {
                lower = middle;
            }
        }
        assert!(probe(upper).0.is_none());
        let (error, first) = probe(upper - 1);
        assert!(
            first,
            "earlier complete record must precede admission failure"
        );
        assert!(error.unwrap().contains("record output admission"));
    });
}

#[test]
fn shared_edit_buffer_enforces_exact_wire_limits_duplicates_and_scope() {
    let f = fixture();
    let record = &f._seed;
    let cut = f.reader.cut().unwrap();
    let key = QueueSourceKey::Field {
        collection: Some(COLLECTION),
        key: &f.keys[0],
        field: "u\0γ",
    };
    let value = FieldValue::Bytes(vec![9; 16384]);
    let bytes = key.encoded_len().unwrap() as u64
        + fireweed_core::queue_contract::encoded_len(&value).unwrap();
    let limits = super::super::super::QueueSourceCompileLimits {
        edits: 1,
        input_bytes: bytes,
    };
    let baseline = f.admission.usage().unwrap();
    {
        let mut short = super::super::super::edits::SourceEdits::new(
            record,
            cut,
            &f.job,
            super::super::super::QueueSourceCompileLimits {
                input_bytes: bytes - 1,
                ..limits
            },
            "test",
        )
        .unwrap();
        assert!(short.insert(key, Some(&value)).is_err());
        assert_eq!(short.edits().len(), 0);
    }
    assert_eq!(f.admission.usage().unwrap(), baseline);
    {
        let mut output =
            super::super::super::edits::SourceEdits::new(record, cut, &f.job, limits, "test")
                .unwrap();
        output.insert(key, Some(&value)).unwrap();
        output.verify(record, &cut, &f.job).unwrap();
        assert!(output.verify(&f._genesis, &cut, &f.job).is_err());
        let foreign = f.admission.reserve(0).unwrap();
        assert!(output.verify(record, &cut, &foreign).is_err());
        let identity = Arc::new(f.job.owned_buffer(128).unwrap());
        let foreign_cut =
            QueueSourceCut::from_captured(&identity, cut.address(), 1, 2, &f.job).unwrap();
        assert!(output.verify(record, &foreign_cut, &f.job).is_err());
        assert!(output.insert(key, Some(&value)).is_err());
        assert_eq!(output.edits().len(), 1);
    }
    assert_eq!(f.admission.usage().unwrap(), baseline);
    // Isolate duplicate-key rejection from the separate one-edit limit.
    {
        let mut output = super::super::super::edits::SourceEdits::new(
            record,
            cut,
            &f.job,
            super::super::super::QueueSourceCompileLimits {
                edits: 2,
                input_bytes: bytes * 2,
            },
            "test",
        )
        .unwrap();
        output.insert(key, Some(&value)).unwrap();
        let error = output.insert::<FieldValue>(key, None).unwrap_err();
        assert!(
            matches!(error, QueueError::OutcomeUnknown(message) if message.contains("duplicate provisional source edit"))
        );
    }
    assert_eq!(f.admission.usage().unwrap(), baseline);
}

#[test]
fn signed_zero_changes_field_bytes_without_index_posting_churn() {
    futures::executor::block_on(async {
        for (before, after) in [(0.0f64, -0.0f64), (-0.0f64, 0.0f64)] {
            let f = fixture_with_keys_and_float(["a\0β", "b\0β"], Some(before));
            let mut fields = f.snapshot.state[&(COLLECTION.into(), f.keys[0].clone())]
                .fields
                .clone();
            fields.insert("f".into(), FieldValue::Float(after));
            let mut request = group("signed-zero-field");
            request.state_updates.push(StateUpdate {
                collection: COLLECTION.into(),
                key: f.keys[0].clone(),
                expect: StateExpect::Version(1),
                change: StateChange::Put {
                    fields,
                    value: Default::default(),
                },
            });
            let (proposed, snapshot) = record(&f.queue, &f.schema, &f.snapshot, &request, &f.job);
            let checked = proposed.checked().unwrap();
            let context =
                QueueSourcePublicationContext::begin(&f.reader, &checked, &f.definition, &f.job)
                    .await
                    .unwrap();
            let baseline = f.admission.usage().unwrap();
            let mut builder = context
                .record_edits(super::super::super::QueueSourceCompileLimits {
                    edits: 1000,
                    input_bytes: 1 << 20,
                })
                .unwrap();
            for command in context.commands() {
                let command = command.unwrap();
                let predecessor = context.predecessor(&command).await.unwrap();
                builder
                    .observe(&context.record_transition(&predecessor).await.unwrap())
                    .unwrap();
            }
            let output = builder.finish().unwrap();
            let edits: Vec<_> = output.edits().collect();
            assert_eq!(edits.len(), 2, "only record and exact field bytes change");
            let field_key = QueueSourceKey::Field {
                collection: Some(COLLECTION),
                key: &f.keys[0],
                field: "f",
            }
            .encode()
            .unwrap();
            let value = edits
                .iter()
                .find(|(key, _)| *key == field_key)
                .unwrap()
                .1
                .unwrap();
            let expected = &snapshot.state[&(COLLECTION.into(), f.keys[0].clone())].fields["f"];
            assert_eq!(value, queue_storage_layout::encode(expected).unwrap());
            assert_ne!(
                value,
                queue_storage_layout::encode(&FieldValue::Float(before)).unwrap()
            );
            assert!(
                edits
                    .iter()
                    .all(|(key, _)| !matches!(key.get(1), Some(4 | 5)))
            );
            drop(edits);
            drop(output);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
    });
}
