use super::*;
use crate::{
    queue_admission::QueueAdmission,
    queue_namespace::AddressRef,
    queue_planner::{RecordSnapshot, plan_commit_group},
    queue_publication::{
        BodyCommandRef, IdentityRef, PublicationReceipt, WorkMetadataRef, encode_publication,
        view_publication,
    },
    queue_schema_codec::encode_definition,
    queue_storage_layout,
};
use fireweed_core::queue_contract::*;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicUsize, Ordering},
};
struct Reader {
    identity: Arc<OwnedQueueBufferCharge>,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    lookups: AtomicUsize,
    ranges: AtomicUsize,
    fail_range: bool,
}
impl QueueSourceReader for Reader {
    type Point = (Vec<u8>, Option<usize>);
    type Range = (Vec<u8>, Vec<u8>);
    fn cut(&self) -> QueueResult<super::super::QueueSourceCut<'_>> {
        super::super::QueueSourceCut::from_captured(
            &self.identity,
            AddressRef {
                tenant: "t",
                queue: "q",
            },
            1,
            1,
            self.identity.reservation(),
        )
    }
    async fn lookup(&self, full_key: &[u8]) -> QueueResult<Self::Point> {
        self.lookups.fetch_add(1, Ordering::Relaxed);
        Ok((
            full_key.to_vec(),
            self.rows.iter().position(|(key, _)| key == full_key),
        ))
    }
    fn point<'a>(
        &'a self,
        owner: &'a Self::Point,
    ) -> QueueResult<super::super::QueueSourcePoint<'a>> {
        Ok(self
            .cut()?
            .point_from_source(&owner.0, owner.1.map(|i| self.rows[i].1.as_slice())))
    }
    async fn complete_range(&self, lower: &[u8], upper: &[u8]) -> QueueResult<Self::Range> {
        self.ranges.fetch_add(1, Ordering::Relaxed);
        if self.fail_range {
            return Err(unknown("injected incomplete range"));
        }
        Ok((lower.to_vec(), upper.to_vec()))
    }
    fn visit_range(
        &self,
        owner: &Self::Range,
        lower: &[u8],
        upper: &[u8],
        visitor: &mut dyn for<'a> FnMut(super::super::QueueSourcePoint<'a>) -> QueueResult<()>,
    ) -> QueueResult<()> {
        assert_eq!((&owner.0[..], &owner.1[..]), (lower, upper));
        for (key, value) in &self.rows {
            if key.as_slice() >= lower && key.as_slice() < upper {
                visitor(self.cut()?.point_from_source(key, Some(value)))?;
            }
        }
        Ok(())
    }
}
struct AdversarialFields<'a> {
    inner: &'a Reader,
    foreign: Arc<OwnedQueueBufferCharge>,
    cuts: AtomicUsize,
    late_fail: bool,
    alternating: bool,
    visited: AtomicUsize,
}
impl AdversarialFields<'_> {
    fn foreign_cut(&self) -> QueueResult<super::super::QueueSourceCut<'_>> {
        let original = self.inner.cut()?;
        super::super::QueueSourceCut::from_captured(
            &self.foreign,
            original.address(),
            original.epoch(),
            original.through(),
            self.foreign.reservation(),
        )
    }
}
impl QueueSourceReader for AdversarialFields<'_> {
    type Point = <Reader as QueueSourceReader>::Point;
    type Range = <Reader as QueueSourceReader>::Range;
    fn cut(&self) -> QueueResult<super::super::QueueSourceCut<'_>> {
        if self.cuts.fetch_add(1, Ordering::SeqCst) == 1 && self.alternating {
            self.foreign_cut()
        } else {
            self.inner.cut()
        }
    }
    async fn lookup(&self, key: &[u8]) -> QueueResult<Self::Point> {
        self.inner.lookup(key).await
    }
    fn point<'a>(
        &'a self,
        owner: &'a Self::Point,
    ) -> QueueResult<super::super::QueueSourcePoint<'a>> {
        Ok(self
            .foreign_cut()?
            .point_from_source(&owner.0, owner.1.map(|i| self.inner.rows[i].1.as_slice())))
    }
    async fn complete_range(&self, lower: &[u8], upper: &[u8]) -> QueueResult<Self::Range> {
        self.inner.complete_range(lower, upper).await
    }
    fn visit_range(
        &self,
        owner: &Self::Range,
        lower: &[u8],
        upper: &[u8],
        visitor: &mut dyn for<'a> FnMut(super::super::QueueSourcePoint<'a>) -> QueueResult<()>,
    ) -> QueueResult<()> {
        self.inner.visit_range(owner, lower, upper, &mut |point| {
            self.visited.fetch_add(1, Ordering::Relaxed);
            if self.late_fail {
                visitor(point)?;
                if self.visited.load(Ordering::Relaxed) >= 2 {
                    Err(unknown("injected failure after retained prefix"))
                } else {
                    Ok(())
                }
            } else {
                visitor(self.foreign_cut()?.point_from_source(
                    point.key(),
                    point.value_for(&self.inner.cut()?, point.key())?,
                ))
            }
        })
    }
}
#[test]
fn selective_hydration_keeps_original_cut_and_drops_provisional_values() {
    futures::executor::block_on(async {
        let admission = QueueAdmission::new(1, 4 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let mut reader = Reader {
            identity: Arc::new(job.owned_buffer(128).unwrap()),
            rows: vec![(
                QueueSourceKey::Field {
                    collection: None,
                    key: "record",
                    field: "b",
                }
                .encode()
                .unwrap(),
                queue_storage_layout::encode(&FieldValue::Text("value".into())).unwrap(),
            )],
            lookups: AtomicUsize::new(0),
            ranges: AtomicUsize::new(0),
            fail_range: false,
        };
        reader.rows.push((
            QueueSourceKey::Field {
                collection: None,
                key: "record",
                field: "a",
            }
            .encode()
            .unwrap(),
            queue_storage_layout::encode(&FieldValue::Integer(7)).unwrap(),
        ));
        reader.rows.sort();
        for (prefix_mode, alternating) in
            [(false, true), (true, true), (false, false), (true, false)]
        {
            let mut source = AdversarialFields {
                inner: &reader,
                foreign: Arc::new(job.owned_buffer(128).unwrap()),
                cuts: AtomicUsize::new(0),
                late_fail: false,
                alternating,
                visited: AtomicUsize::new(0),
            };
            let cut = source.cut().unwrap();
            let names = if prefix_mode {
                BTreeSet::new()
            } else {
                BTreeSet::from(["b".into()])
            };
            let prefixes = if prefix_mode {
                BTreeSet::from(["a".into(), "b".into()])
            } else {
                BTreeSet::new()
            };
            let selection = QueueSourceFieldSelection {
                names: &names,
                prefixes: &prefixes,
            };
            let baseline = admission.usage().unwrap();
            let error = load_selected_fields(
                &source,
                cut,
                IdentityRef {
                    collection: None,
                    key: "record",
                },
                2,
                &definition(),
                selection,
                &job,
            )
            .await
            .err()
            .unwrap();
            let expected = if alternating {
                "source fields: reader cut differs from original field authority"
            } else {
                "queue source cut: lookup evidence belongs to another cut or full key"
            };
            assert!(matches!(error, QueueError::OutcomeUnknown(message) if message == expected));
            assert_eq!(
                source.cuts.load(Ordering::SeqCst),
                2,
                "one entry validation, never replace original cut"
            );
            assert_eq!(admission.usage().unwrap(), baseline);
            assert!(source.cut().unwrap().same_cut(&cut));
            if prefix_mode {
                source.late_fail = true;
                source.alternating = false;
                source.visited.store(0, Ordering::Relaxed);
                let baseline = admission.usage().unwrap();
                let error = load_selected_fields(
                    &source,
                    reader.cut().unwrap(),
                    IdentityRef {
                        collection: None,
                        key: "record",
                    },
                    2,
                    &definition(),
                    selection,
                    &job,
                )
                .await
                .err()
                .unwrap();
                assert!(
                    matches!(error, QueueError::OutcomeUnknown(message) if message == "source fields: injected failure after retained prefix")
                );
                assert_eq!(source.visited.load(Ordering::Relaxed), 2);
                assert_eq!(
                    admission.usage().unwrap(),
                    baseline,
                    "provisional values must release on late failure"
                );
            }
        }
        let source = AdversarialFields {
            inner: &reader,
            foreign: Arc::new(job.owned_buffer(128).unwrap()),
            cuts: AtomicUsize::new(1),
            late_fail: false,
            alternating: true,
            visited: AtomicUsize::new(0),
        };
        let names = BTreeSet::new();
        let prefixes = BTreeSet::from(["z".into()]);
        let before = (
            reader.lookups.load(Ordering::Relaxed),
            reader.ranges.load(Ordering::Relaxed),
        );
        let baseline = admission.usage().unwrap();
        let error = load_selected_fields(
            &source,
            reader.cut().unwrap(),
            IdentityRef {
                collection: None,
                key: "record",
            },
            2,
            &definition(),
            QueueSourceFieldSelection {
                names: &names,
                prefixes: &prefixes,
            },
            &job,
        )
        .await
        .err()
        .unwrap();
        assert!(
            matches!(error, QueueError::OutcomeUnknown(message) if message == "source fields: reader cut differs from original field authority")
        );
        assert_eq!(
            before,
            (
                reader.lookups.load(Ordering::Relaxed),
                reader.ranges.load(Ordering::Relaxed)
            )
        );
        assert_eq!(admission.usage().unwrap(), baseline);
    });
}
fn definition() -> CollectionDefinition {
    CollectionDefinition {
        fields: BTreeMap::from([
            ("a".into(), FieldType::Integer),
            ("b".into(), FieldType::Text),
            ("untouched".into(), FieldType::Bytes),
        ]),
        indexes: BTreeMap::from([
            ("ab".into(), vec!["a".into(), "b".into()]),
            ("bc".into(), vec!["b".into(), "untouched".into()]),
        ]),
        unique_indexes: Default::default(),
        additional_field_prefixes: Default::default(),
    }
}
fn group(id: &str) -> CommitGroup {
    CommitGroup {
        request_id: id.into(),
        administration: None,
        claimed_updates: vec![],
        state_updates: vec![],
        work_updates: vec![],
        work_inserts: vec![],
    }
}
fn preserve() -> CommandRef<'static> {
    CommandRef {
        identity: IdentityRef {
            collection: None,
            key: "record\0α",
        },
        expected_version: Some(1),
        prior_highwater: 1,
        output_highwater: 2,
        effect: EffectRef::WorkPut {
            metadata: WorkMetadataRef {
                priority: 0,
                available_at: 0,
                insertion_sequence: 1,
                lifecycle: Lifecycle::Pending,
                eligible_since: 0,
                attempts: 0,
                lease_expires_at: None,
                lease_hash: None,
            },
            fields: FieldsCommandRef::Preserve,
            body: BodyCommandRef::Preserve,
        },
    }
}
#[test]
fn field_codec_borrows_every_owned_scalar_without_changing_representation() {
    for owned in [
        FieldValue::Integer(i64::MIN),
        FieldValue::Float(-0.0),
        FieldValue::Boolean(true),
        FieldValue::Text("\0α".into()),
        FieldValue::Bytes(vec![0, 255]),
    ] {
        let encoded = queue_storage_layout::encode(&owned).unwrap();
        assert_eq!(value(&encoded).unwrap().owned(), owned);
        if let (FieldValue::Float(before), Value::Float(after)) = (&owned, value(&encoded).unwrap())
        {
            assert_eq!(before.to_bits(), after.to_bits());
        }
        assert_eq!(
            crate::queue_index::ordered_key_ref(&[Some(source_field_value(&encoded).unwrap())])
                .unwrap(),
            crate::queue_index::ordered_key(&[Some(&owned)]).unwrap()
        );
        let mut trailing = encoded;
        trailing.push(0);
        assert!(source_field_value(&trailing).is_err());
    }
    for encoded in [
        vec![],
        vec![3, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1],
        vec![4, 100, 1],
        vec![2, 2],
    ] {
        assert!(source_field_value(&encoded).is_err());
    }
}
#[test]
fn membership_names_require_complete_canonical_literals() {
    assert_eq!(field_name(b"a\0\xff\xce\xb1\0\0").unwrap(), "a\0α");
    for suffix in [
        b"a".as_slice(),
        b"a\0",
        b"a\0\x01",
        b"a\0\0tail",
        b"\xff\0\0",
    ] {
        assert!(field_name(suffix).is_err());
    }
}
#[test]
fn output_credit_boundary_and_drop_cover_the_actual_retained_field() {
    let admission = QueueAdmission::new(1, 1 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let make = || QueueSourceFields {
        fields: Fields::new(),
        coverage: FieldCoverage::Complete,
        _charge: job
            .owned_buffer((std::mem::size_of::<QueueSourceFields>() + 1024) as u64)
            .unwrap(),
    };
    let encoded = queue_storage_layout::encode(&FieldValue::Bytes(vec![7; 8192])).unwrap();
    let base = admission.usage().unwrap();
    let mut measured = make();
    let before = admission.usage().unwrap().1;
    measured
        .retain("untouched", &encoded, &definition(), &job)
        .unwrap();
    let needed = admission.usage().unwrap().1 - before;
    assert!(needed > encoded.len() as u64);
    drop(measured);
    assert_eq!(admission.usage().unwrap(), base);
    for spare in [needed - 1, needed] {
        let mut output = make();
        let padding = job
            .owned_buffer((1 << 20) - admission.usage().unwrap().1 - spare)
            .unwrap();
        let result = output.retain("untouched", &encoded, &definition(), &job);
        assert_eq!(result.is_ok(), spare == needed);
        assert_eq!(output.present_count(), usize::from(spare == needed));
        drop(output);
        drop(padding);
        assert_eq!(admission.usage().unwrap(), base);
    }
}
#[test]
fn selective_prefix_hydration_certifies_absence_without_reading_unrelated_values() {
    futures::executor::block_on(async {
        let admission = QueueAdmission::new(2, 4 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let foreign = Arc::new(admission.reserve(0).unwrap());
        let identity = IdentityRef {
            collection: None,
            key: "record\0α",
        };
        let fields = Fields::from([
            ("a".into(), FieldValue::Integer(7)),
            ("b".into(), FieldValue::Text("old".into())),
            ("b\0x".into(), FieldValue::Text("nul".into())),
            ("bé".into(), FieldValue::Text("unicode".into())),
        ]);
        let mut schema = definition();
        schema.additional_field_prefixes.insert("b".into());
        let mut reader = Reader {
            identity: Arc::new(job.owned_buffer(128).unwrap()),
            rows: fields
                .iter()
                .map(|(name, value)| {
                    (
                        QueueSourceKey::Field {
                            collection: None,
                            key: identity.key,
                            field: name,
                        }
                        .encode()
                        .unwrap(),
                        queue_storage_layout::encode(value).unwrap(),
                    )
                })
                .collect(),
            lookups: AtomicUsize::new(0),
            ranges: AtomicUsize::new(0),
            fail_range: false,
        };
        // An unrelated malformed/large field is never hydrated or parsed.
        reader.rows.push((
            QueueSourceKey::Field {
                collection: None,
                key: identity.key,
                field: "untouched",
            }
            .encode()
            .unwrap(),
            vec![255; 8192],
        ));
        reader.rows.sort();
        let names = BTreeSet::from(["a".into(), "b".into(), "b-missing".into()]);
        let prefixes = BTreeSet::from([
            "a".into(),
            "b".into(),
            "b\0".into(),
            "bé".into(),
            "z".into(),
        ]);
        let loaded = load_selected_fields(
            &reader,
            reader.cut().unwrap(),
            identity,
            5,
            &schema,
            QueueSourceFieldSelection {
                names: &names,
                prefixes: &prefixes,
            },
            &job,
        )
        .await
        .unwrap();
        let checked = loaded.checked().unwrap();
        assert_eq!(loaded.present_count(), 4);
        for (name, value) in &fields {
            assert_eq!(checked.get(name).unwrap(), Some(value));
        }
        assert_eq!(checked.get("b-missing").unwrap(), None);
        assert_eq!(checked.prefix("b").unwrap().count(), 3);
        assert_eq!(checked.prefix("b\0").unwrap().count(), 1);
        assert!(checked.get("untouched").is_err());
        assert!(checked.get("unknown").is_err());
        assert_eq!(reader.lookups.load(Ordering::Relaxed), 0);
        assert_eq!(reader.ranges.load(Ordering::Relaxed), 3);
        assert_eq!(checked.get("z-missing").unwrap(), None);
        assert_eq!(checked.prefix("z").unwrap().count(), 0);
        drop(loaded);
        assert!(
            load_selected_fields(
                &reader,
                reader.cut().unwrap(),
                identity,
                5,
                &schema,
                QueueSourceFieldSelection {
                    names: &names,
                    prefixes: &prefixes
                },
                &foreign
            )
            .await
            .is_err()
        );
        assert!(
            load_selected_fields(
                &reader,
                reader.cut().unwrap(),
                identity,
                3,
                &schema,
                QueueSourceFieldSelection {
                    names: &names,
                    prefixes: &prefixes
                },
                &job
            )
            .await
            .is_err()
        );
        reader.fail_range = true;
        assert!(
            load_selected_fields(
                &reader,
                reader.cut().unwrap(),
                identity,
                5,
                &schema,
                QueueSourceFieldSelection {
                    names: &names,
                    prefixes: &prefixes
                },
                &job
            )
            .await
            .is_err()
        );
        reader.fail_range = false;
        let empty_prefix = BTreeSet::from([" ".into()]);
        let before = (
            reader.lookups.load(Ordering::Relaxed),
            reader.ranges.load(Ordering::Relaxed),
        );
        assert!(
            load_selected_fields(
                &reader,
                reader.cut().unwrap(),
                identity,
                5,
                &schema,
                QueueSourceFieldSelection {
                    names: &names,
                    prefixes: &empty_prefix
                },
                &job
            )
            .await
            .is_err()
        );
        assert_eq!(
            before,
            (
                reader.lookups.load(Ordering::Relaxed),
                reader.ranges.load(Ordering::Relaxed)
            )
        );
    });
}
#[test]
fn selective_preimages_follow_existing_index_closure_and_complete_membership() {
    futures::executor::block_on(async {
        let admission = QueueAdmission::new(2, 4 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let foreign = Arc::new(admission.reserve(0).unwrap());
        let fields = Fields::from([
            ("a".into(), FieldValue::Integer(7)),
            ("b".into(), FieldValue::Text("old\0α".into())),
            ("untouched".into(), FieldValue::Bytes(vec![9; 8192])),
        ]);
        let mut reader = Reader {
            identity: Arc::new(job.owned_buffer(128).unwrap()),
            rows: fields
                .iter()
                .map(|(name, value)| {
                    (
                        QueueSourceKey::Field {
                            collection: None,
                            key: "record\0α",
                            field: name,
                        }
                        .encode()
                        .unwrap(),
                        queue_storage_layout::encode(value).unwrap(),
                    )
                })
                .collect(),
            lookups: AtomicUsize::new(0),
            ranges: AtomicUsize::new(0),
            fail_range: false,
        };
        reader.rows.sort();
        let schema = QueueSchema {
            work: definition(),
            state: Default::default(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        };
        let mut seed = group("seed");
        seed.work_inserts.push(NewWork {
            key: "record\0α".into(),
            priority: 0,
            available_at: 0,
            fields: fields.clone(),
            body: Default::default(),
        });
        let mut snapshot = RecordSnapshot::default();
        plan_commit_group(&seed, &mut snapshot, 0, &schema).unwrap();
        let mut patch = group("patch");
        patch.work_updates.push(UnclaimedUpdate {
            key: "record\0α".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                fields: BTreeMap::from([("a".into(), Some(FieldValue::Integer(8)))]),
                ..Default::default()
            }),
        });
        let mut after = snapshot.clone();
        let delta = plan_commit_group(&patch, &mut after, 0, &schema).unwrap();
        let queue = QueueAddress {
            tenant: "t".into(),
            queue: "q".into(),
        };
        let receipt = PublicationReceipt::commit(&queue, &patch, &delta.receipt).unwrap();
        let frame = encode_publication(
            &queue,
            &encode_definition(&schema).unwrap(),
            &snapshot,
            &delta,
            &[receipt],
            1 << 20,
            &job,
        )
        .unwrap();
        let view = view_publication(frame.as_bytes()).unwrap();
        let command = view.commands().next().unwrap().unwrap();
        let baseline = admission.usage().unwrap();
        let selected = load_command_fields(&reader, command, 3, &schema.work, &job)
            .await
            .unwrap();
        assert_eq!(selected.present_count(), 2);
        assert_eq!(
            selected.checked().unwrap().get("a").unwrap(),
            fields.get("a")
        );
        assert_eq!(
            selected.checked().unwrap().get("b").unwrap(),
            fields.get("b")
        );
        assert!(selected.checked().unwrap().get("untouched").is_err());
        assert_eq!(reader.lookups.load(Ordering::Relaxed), 2);
        assert_eq!(reader.ranges.load(Ordering::Relaxed), 0);
        drop(selected);
        assert_eq!(admission.usage().unwrap(), baseline);
        let preserved = load_command_fields(&reader, preserve(), 3, &schema.work, &job)
            .await
            .unwrap();
        assert_eq!(preserved.present_count(), 0);
        assert_eq!(reader.lookups.load(Ordering::Relaxed), 2);
        assert_eq!(reader.ranges.load(Ordering::Relaxed), 0);
        drop(preserved);
        let deleted = CommandRef {
            effect: EffectRef::Delete,
            ..preserve()
        };
        let complete = load_command_fields(&reader, deleted, 3, &schema.work, &job)
            .await
            .unwrap();
        assert_eq!(complete.present_count(), 3);
        assert_eq!(
            complete.checked().unwrap().get("untouched").unwrap(),
            fields.get("untouched")
        );
        assert_eq!(complete.checked().unwrap().get("missing").unwrap(), None);
        drop(complete);
        assert_eq!(admission.usage().unwrap(), baseline);
        assert!(
            load_command_fields(&reader, deleted, 2, &schema.work, &job)
                .await
                .is_err()
        );
        assert!(
            load_command_fields(&reader, deleted, 3, &schema.work, &foreign)
                .await
                .is_err()
        );
        assert!(
            load_command_fields(&reader, deleted, 4, &schema.work, &job)
                .await
                .is_err()
        );
        let original_rows = reader.rows.clone();
        reader.rows.push(reader.rows[0].clone());
        reader.rows.sort();
        assert!(
            load_command_fields(&reader, deleted, 3, &schema.work, &job)
                .await
                .is_err()
        );
        reader.rows = original_rows.clone();
        reader.rows[0].1 = queue_storage_layout::encode(&FieldValue::Boolean(false)).unwrap();
        assert!(
            load_command_fields(&reader, deleted, 3, &schema.work, &job)
                .await
                .is_err()
        );
        reader.rows = original_rows.clone();
        reader.rows.push((
            QueueSourceKey::Field {
                collection: None,
                key: "record\0α",
                field: "rogue",
            }
            .encode()
            .unwrap(),
            queue_storage_layout::encode(&FieldValue::Integer(0)).unwrap(),
        ));
        reader.rows.sort();
        assert!(
            load_command_fields(&reader, deleted, 4, &schema.work, &job)
                .await
                .is_err()
        );
        reader.rows = original_rows;
        assert_eq!(admission.usage().unwrap(), baseline);
        // Sparse composite columns are authenticated absence, while an
        // unrelated large field remains outside selected coverage.
        let b_key = QueueSourceKey::Field {
            collection: None,
            key: "record\0α",
            field: "b",
        }
        .encode()
        .unwrap();
        reader.rows.retain(|(key, _)| key != &b_key);
        let sparse = load_command_fields(&reader, command, 2, &schema.work, &job)
            .await
            .unwrap();
        assert_eq!(sparse.present_count(), 1);
        assert_eq!(sparse.checked().unwrap().get("b").unwrap(), None);
        assert!(sparse.checked().unwrap().get("untouched").is_err());
        drop(sparse);
        let complete_sparse = load_command_fields(&reader, deleted, 2, &schema.work, &job)
            .await
            .unwrap();
        drop(complete_sparse);
        reader.fail_range = true;
        let failure = load_command_fields(&reader, deleted, 2, &schema.work, &job).await;
        assert!(
            matches!(failure, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("injected incomplete range"))
        );
        assert_eq!(admission.usage().unwrap(), baseline);
    });
}
