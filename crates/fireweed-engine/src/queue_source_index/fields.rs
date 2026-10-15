//! Admitted selective field preimages for the resolved-command compiler.
use super::{
    QueueSourceKey, QueueSourceReader, prefix_successor, record_fields_prefix,
    record_fields_prefix_len,
};
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_fields::{CheckedFields, FieldCoverage},
    queue_publication::{CommandRef, EffectRef, FieldsCommandRef, ValueRef},
};
use fireweed_core::queue_contract::{
    CollectionDefinition, FieldValue, Fields, QueueError, QueueResult,
};
use serde::Deserialize;
use std::{collections::BTreeSet, sync::Arc};
fn unknown(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("source fields: {message}"))
}

// Same Postcard variant order as FieldValue, with borrowed payloads. Malformed
// lengths cannot allocate an oversized String/Vec before Root admits output.
#[derive(Deserialize)]
enum Value<'a> {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Text(&'a str),
    Bytes(&'a [u8]),
}
impl Value<'_> {
    fn owned(self) -> FieldValue {
        match self {
            Self::Integer(v) => FieldValue::Integer(v),
            Self::Float(v) => FieldValue::Float(v),
            Self::Boolean(v) => FieldValue::Boolean(v),
            Self::Text(v) => FieldValue::Text(v.into()),
            Self::Bytes(v) => FieldValue::Bytes(v.into()),
        }
    }
}
fn value(bytes: &[u8]) -> QueueResult<Value<'_>> {
    let (value, rest) =
        postcard::take_from_bytes(bytes).map_err(|_| unknown("invalid field fact"))?;
    if !rest.is_empty() {
        return Err(unknown("trailing field fact bytes"));
    }
    Ok(value)
}

pub struct QueueSourceFields {
    fields: Fields,
    coverage: FieldCoverage,
    // Last guards outlive both maps and strings throughout destruction.
    _charge: OwnedQueueBufferCharge,
}
impl QueueSourceFields {
    pub fn checked(&self) -> QueueResult<CheckedFields<'_>> {
        CheckedFields::new(&self.fields, &self.coverage)
    }
    pub(super) fn complete_entries(
        &self,
    ) -> QueueResult<impl Iterator<Item = (&str, &FieldValue)>> {
        if !matches!(self.coverage, FieldCoverage::Complete) {
            return Err(unknown("complete enumeration outside loaded authority"));
        }
        Ok(self
            .fields
            .iter()
            .map(|(name, value)| (name.as_str(), value)))
    }
    pub fn present_count(&self) -> usize {
        self.fields.len()
    }
    fn charge(&mut self, bytes: usize, job: &Arc<QueueReservation>) -> QueueResult<()> {
        let bytes = bytes
            .checked_mul(4)
            .and_then(|n| n.checked_add(1024))
            .ok_or(QueueError::TooLarge)?;
        if !std::ptr::eq(self._charge.reservation(), job.as_ref()) {
            return Err(unknown("field output operation differs"));
        }
        self._charge
            .grow(u64::try_from(bytes).map_err(|_| QueueError::TooLarge)?)
    }
    fn retain(
        &mut self,
        name: &str,
        bytes: &[u8],
        definition: &CollectionDefinition,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<()> {
        // Parse borrowing first; owned payloads and both tree nodes are admitted.
        let parsed = value(bytes)?;
        self.charge(
            name.len()
                .checked_add(bytes.len())
                .ok_or(QueueError::TooLarge)?,
            job,
        )?;
        let owned = parsed.owned();
        definition
            .validate_value(name, &owned)
            .map_err(|_| unknown("field fact violates queue schema"))?;
        if self.fields.insert(name.into(), owned).is_some() {
            return Err(unknown("duplicate field fact"));
        }
        Ok(())
    }
    fn name(&mut self, name: &str, job: &Arc<QueueReservation>) -> QueueResult<()> {
        let FieldCoverage::Selected { names, .. } = &self.coverage else {
            return Ok(());
        };
        if names.contains(name) {
            return Ok(());
        }
        self.charge(name.len(), job)?;
        let FieldCoverage::Selected { names, .. } = &mut self.coverage else {
            unreachable!()
        };
        names.insert(name.into());
        Ok(())
    }
}
/// No query/cache facts enter here. Replace and delete require complete source
/// membership; Edit reads only explicit names and columns of affected indexes;
/// Preserve performs no field I/O. The caller retains the record fact proving
/// expected_count at this same reader cut before compiling any output.
pub async fn load_command_fields<R: QueueSourceReader>(
    reader: &R,
    command: CommandRef<'_>,
    expected_count: u64,
    definition: &CollectionDefinition,
    job: &Arc<QueueReservation>,
) -> QueueResult<QueueSourceFields> {
    let cut = reader.cut()?;
    if !cut.belongs_to(job) {
        return Err(unknown("reader operation differs"));
    }
    let charge = job.owned_buffer((std::mem::size_of::<QueueSourceFields>() + 1024) as u64)?;
    let mut output = QueueSourceFields {
        fields: Fields::new(),
        coverage: FieldCoverage::Selected {
            names: BTreeSet::new(),
            prefixes: BTreeSet::new(),
        },
        _charge: charge,
    };
    let fields = match command.effect {
        EffectRef::Delete | EffectRef::StatePut { .. } => None,
        EffectRef::WorkPut {
            fields: FieldsCommandRef::Replace(_),
            ..
        } => None,
        EffectRef::WorkPut {
            fields: FieldsCommandRef::Preserve,
            ..
        } => return Ok(output),
        EffectRef::WorkPut {
            fields: FieldsCommandRef::Edit(edits),
            ..
        } => Some(edits),
    };
    if let Some(edits) = fields {
        for entry in edits.iter() {
            output.name(
                entry
                    .map_err(|_| unknown("invalid resolved field command"))?
                    .0,
                job,
            )?;
        }
        let FieldCoverage::Selected { names, .. } = &output.coverage else {
            unreachable!()
        };
        let explicit_bytes = names.iter().try_fold(0usize, |n, name| {
            n.checked_add(name.len())
                .and_then(|n| n.checked_add(1024))
                .ok_or(QueueError::TooLarge)
        })?;
        output.charge(explicit_bytes, job)?;
        let FieldCoverage::Selected { names, .. } = &output.coverage else {
            unreachable!()
        };
        let explicit = names.clone();
        for columns in definition.indexes.values() {
            if columns.iter().any(|name| explicit.contains(name)) {
                for name in columns {
                    output.name(name, job)?;
                }
            }
        }
        // Names are already charged. Borrow them independently of the output
        // field tree so retaining values cannot invalidate the traversal.
        let FieldCoverage::Selected { names, .. } = &output.coverage else {
            unreachable!()
        };
        let names_bytes = names.iter().try_fold(0usize, |n, name| {
            n.checked_add(name.len())
                .and_then(|n| n.checked_add(std::mem::size_of::<String>()))
                .ok_or(QueueError::TooLarge)
        })?;
        output.charge(names_bytes, job)?;
        let FieldCoverage::Selected { names, .. } = &output.coverage else {
            unreachable!()
        };
        let names: Vec<_> = names.iter().cloned().collect();
        for name in names {
            let key = QueueSourceKey::Field {
                collection: command.identity.collection,
                key: command.identity.key,
                field: &name,
            };
            let _key_charge = job.owned_buffer(key.encoded_len()? as u64)?;
            let key = key.encode()?;
            let owner = reader.lookup(&key).await?;
            let point = reader.point(&owner)?;
            if let Some(bytes) = point.value_for(&cut, &key)? {
                output.retain(&name, bytes, definition, job)?;
            }
        }
    } else {
        let len = record_fields_prefix_len(command.identity.collection, command.identity.key)?;
        let _bounds = job.owned_buffer(len.checked_mul(2).ok_or(QueueError::TooLarge)? as u64)?;
        let lower = record_fields_prefix(command.identity.collection, command.identity.key)?;
        let mut upper = lower.clone();
        if !prefix_successor(&mut upper) {
            return Err(unknown("field family has no upper bound"));
        }
        let range = reader.complete_range(&lower, &upper).await?;
        reader.visit_range(&range, &lower, &upper, &mut |point| {
            let bytes = point
                .value_for(&cut, point.key())?
                .ok_or_else(|| unknown("absent field inside membership range"))?;
            let suffix = point
                .key()
                .strip_prefix(lower.as_slice())
                .ok_or_else(|| unknown("field range prefix differs"))?;
            let _name_charge = job.owned_buffer(
                suffix
                    .len()
                    .checked_add(std::mem::size_of::<String>())
                    .ok_or(QueueError::TooLarge)? as u64,
            )?;
            let name = field_name(suffix)?;
            output.retain(&name, bytes, definition, job)
        })?;
        if output.fields.len() as u64 != expected_count {
            return Err(unknown("complete field membership count differs"));
        }
        output.coverage = FieldCoverage::Complete;
    }
    Ok(output)
}
fn field_name(mut suffix: &[u8]) -> QueueResult<String> {
    let mut bytes = Vec::with_capacity(suffix.len());
    while let Some((&byte, rest)) = suffix.split_first() {
        suffix = rest;
        if byte != 0 {
            bytes.push(byte);
            continue;
        }
        match suffix.split_first() {
            Some((&255, rest)) => {
                bytes.push(0);
                suffix = rest;
            }
            Some((&0, [])) => {
                return String::from_utf8(bytes).map_err(|_| unknown("invalid field name UTF-8"));
            }
            _ => return Err(unknown("noncanonical field name terminator")),
        }
    }
    Err(unknown("unterminated field name"))
}

/// Borrow the source scalar without a second allocation when compiling postings.
pub fn source_field_value(bytes: &[u8]) -> QueueResult<ValueRef<'_>> {
    Ok(match value(bytes)? {
        Value::Integer(v) => ValueRef::Integer(v),
        Value::Float(v) => ValueRef::Float(v),
        Value::Boolean(v) => ValueRef::Boolean(v),
        Value::Text(v) => ValueRef::Text(v),
        Value::Bytes(v) => ValueRef::Bytes(v),
    })
}

#[cfg(test)]
mod tests {
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
            if let (FieldValue::Float(before), Value::Float(after)) =
                (&owned, value(&encoded).unwrap())
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
}
