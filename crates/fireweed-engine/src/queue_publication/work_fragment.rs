//! Disposable one-work-command preparation. This reuses the publication command
//! writer/parser; these bytes have no frame, receipt or publication authority.
use super::*;

pub struct ResolvedWorkFragment<'r> {
    bytes: Vec<u8>,
    _charge: QueueBufferCharge<'r>,
}
impl ResolvedWorkFragment<'_> {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}
/// An administrative row preserves insertion order and resolves against initial
/// live authority. The initial sequence is a fragment validation bound; ordinary
/// inserts may subsequently advance the containing group's final sequence.
pub fn encode_administrative_work_fragment<'r>(
    before: &RecordSnapshot,
    delta: &CommitDelta,
    reservation: &'r QueueReservation,
) -> PublicationResult<ResolvedWorkFragment<'r>> {
    let size = measure_administrative_work_fragment(before, delta)?;
    let charge = reservation
        .buffer(size.checked_add(128).ok_or(PublicationError::Overflow)?)
        .map_err(PublicationError::Admission)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(usize::try_from(size).map_err(|_| PublicationError::Overflow)?)
        .map_err(|_| PublicationError::Overflow)?;
    write_administrative_work_fragment(&mut Sink::Buffer(&mut bytes), before, delta)?;
    if bytes.len() as u64 != size {
        return invalid("work fragment size changed");
    }
    parse_administrative_work_fragment(&bytes, before.next_sequence)?;
    Ok(ResolvedWorkFragment {
        bytes,
        _charge: charge,
    })
}

/// Exact command validation/counting without retaining its payload bytes.
pub fn measure_administrative_work_fragment(
    before: &RecordSnapshot,
    delta: &CommitDelta,
) -> PublicationResult<u64> {
    let mut sink = Sink::Count(0);
    write_administrative_work_fragment(&mut sink, before, delta)?;
    let Sink::Count(size) = sink else {
        unreachable!()
    };
    Ok(size)
}
fn write_administrative_work_fragment(
    sink: &mut Sink<'_>,
    before: &RecordSnapshot,
    delta: &CommitDelta,
) -> PublicationResult<()> {
    if before.work.len() != 1
        || before.versions.len() != 1
        || !before.state.is_empty()
        || delta.versions.len() != 1
        || !delta.state.is_empty()
        || !delta.deleted_state.is_empty()
        || delta.next_sequence != before.next_sequence
        || delta.versions[0].collection.is_some()
    {
        return invalid("not one initial administrative work command");
    }
    validate_delta(before, delta)?;
    let version = &delta.versions[0];
    let prior = before
        .work
        .get(&version.key)
        .ok_or(PublicationError::Invalid(
            "administrative predecessor missing",
        ))?;
    let highwater = before
        .versions
        .iter()
        .next()
        .filter(|(id, _)| id.collection.is_none() && id.key == version.key)
        .map(|(_, value)| *value)
        .ok_or(PublicationError::Invalid(
            "administrative highwater missing",
        ))?;
    let row = delta.work.first().map(|row| {
        (
            row,
            &delta.work_fields[&version.key],
            &delta.work_bodies[&version.key],
        )
    });
    if measure_work_mutation(version, Some(prior.record.version), highwater, row)? == 0 {
        return invalid("administrative fragment cannot be a no-op");
    }
    write_mutation_header(sink, version, Some(prior.record.version), highwater)?;
    if let Some((row, fields, body)) = row {
        write_work_value(sink, row, fields, body)?;
    } else {
        sink.tag(0)?;
    }
    Ok(())
}

/// One admitted resolved command. This is not a business decision or receipt:
/// its placeholder receipt is never persisted. The enclosing publication owns
/// the complete result and atomic boundary.
pub struct MaterializedAdministrativeWork<'r> {
    delta: CommitDelta,
    _charge: QueueBufferCharge<'r>,
}
impl MaterializedAdministrativeWork<'_> {
    pub fn delta(&self) -> &CommitDelta {
        &self.delta
    }
}
fn owned_value(value: ValueRef<'_>) -> FieldValue {
    value.into_owned()
}
/// Materialize a parsed administrative command against selectively reloaded
/// physical authority. Never reevaluate selectors, user CAS, leases or time.
/// Field coverage must include exact edited names and affected index components.
pub fn materialize_administrative_work<'r>(
    command: CommandRef<'_>,
    before: &RecordSnapshot,
    definition: &CollectionDefinition,
    reservation: &'r QueueReservation,
) -> PublicationResult<MaterializedAdministrativeWork<'r>> {
    if command.identity.collection.is_some()
        || before.work.len() != 1
        || before.versions.len() != 1
        || !before.state.is_empty()
        || command.expected_version != Some(command.prior_highwater)
        || command.prior_highwater == 0
        || command.output_highwater <= command.prior_highwater
    {
        return invalid("invalid existing administrative command authority");
    }
    let old = before
        .work
        .get(command.identity.key)
        .ok_or(PublicationError::Invalid(
            "administrative physical predecessor missing",
        ))?;
    let prior = before
        .versions
        .iter()
        .next()
        .filter(|(id, _)| id.collection.is_none() && id.key == command.identity.key)
        .map(|(_, version)| *version);
    if prior != Some(command.prior_highwater)
        || old.record.version != command.prior_highwater
        || old.record.work.key != command.identity.key
    {
        return invalid("administrative physical predecessor mismatch");
    }
    let mut bytes = crate::queue_memory::fields_expansion(
        old.record
            .work
            .fields
            .iter()
            .map(|(name, value)| (name.as_str(), Some(value))),
        definition,
    )
    .map_err(PublicationError::Admission)?;
    bytes = bytes
        .checked_add(old.record.work.body.len() as u64)
        .and_then(|n| n.checked_add(command.identity.key.len() as u64))
        .and_then(|n| n.checked_add(8192))
        .ok_or(PublicationError::Overflow)?;
    if let crate::queue_fields::FieldCoverage::Selected { names, prefixes } = &old.field_coverage {
        for name in names.iter().chain(prefixes.iter()) {
            bytes = bytes
                .checked_add(name.len() as u64)
                .and_then(|n| n.checked_add(256))
                .ok_or(PublicationError::Overflow)?;
        }
    }
    if let EffectRef::WorkPut { fields, body, .. } = command.effect {
        if let BodyCommandRef::Replace(body) = body {
            bytes = bytes
                .checked_add(body.len() as u64)
                .ok_or(PublicationError::Overflow)?;
        }
        if let FieldsCommandRef::Replace(fields) | FieldsCommandRef::Edit(fields) = fields {
            for entry in fields.iter() {
                let (name, value) = entry?;
                let value_bytes = match value {
                    Some(ValueRef::Text(value)) => value.len() as u64,
                    Some(ValueRef::Bytes(value)) => value.len() as u64,
                    _ => 8,
                };
                let copies = 1 + definition
                    .indexes
                    .values()
                    .map(|columns| {
                        columns
                            .iter()
                            .filter(|column| column.as_str() == name)
                            .count() as u64
                    })
                    .sum::<u64>();
                let expanded = value_bytes
                    .checked_mul(copies)
                    .ok_or(PublicationError::Overflow)?;
                bytes = bytes
                    .checked_add(name.len() as u64)
                    .and_then(|n| n.checked_add(expanded))
                    .and_then(|n| n.checked_add(1024))
                    .ok_or(PublicationError::Overflow)?;
            }
        }
    }
    let charge = reservation
        .buffer(bytes.checked_mul(8).ok_or(PublicationError::Overflow)?)
        .map_err(PublicationError::Admission)?;
    let key = command.identity.key.to_owned();
    let mut delta = CommitDelta {
        work: vec![],
        work_fields: Default::default(),
        work_bodies: Default::default(),
        deleted_work: vec![],
        state: vec![],
        deleted_state: vec![],
        versions: vec![RecordVersion {
            collection: None,
            key: key.clone(),
            version: command.output_highwater,
        }],
        next_sequence: before.next_sequence,
        receipt: CommitReceipt {
            request_id: String::new(),
            outcome: GroupOutcome::Committed { versions: vec![] },
        },
    };
    match command.effect {
        EffectRef::Delete => delta.deleted_work.push(key),
        EffectRef::StatePut { .. } => return invalid("state in administrative work command"),
        EffectRef::WorkPut {
            metadata,
            fields,
            body,
        } => {
            if metadata.insertion_sequence != old.record.insertion_sequence {
                return invalid("administrative insertion sequence changed");
            }
            let change = match fields {
                FieldsCommandRef::Preserve => ResolvedFields::Preserve,
                FieldsCommandRef::Replace(fields) => {
                    let mut result = Fields::new();
                    for entry in fields.iter() {
                        let (name, value) = entry?;
                        let value = owned_value(
                            value.ok_or(PublicationError::Invalid("absent replacement field"))?,
                        );
                        definition
                            .validate_value(name, &value)
                            .map_err(PublicationError::Admission)?;
                        result.insert(name.into(), value);
                    }
                    ResolvedFields::Replace(result)
                }
                FieldsCommandRef::Edit(fields) => {
                    let mut result = crate::queue_fields::FieldEdits::new();
                    for entry in fields.iter() {
                        let (name, value) = entry?;
                        if !definition.admits_field(name) {
                            return invalid("undeclared resolved edit");
                        }
                        let value = value.map(owned_value);
                        if let Some(value) = &value {
                            definition
                                .validate_value(name, value)
                                .map_err(PublicationError::Admission)?;
                        }
                        result.insert(name.into(), value);
                    }
                    ResolvedFields::Edit(result)
                }
            };
            let mut row = old.clone();
            crate::queue_fields::apply_to_view(
                &mut row.record.work.fields,
                &mut row.field_coverage,
                change.clone(),
            );
            let body = match body {
                BodyCommandRef::Preserve => ResolvedBody::Preserve,
                BodyCommandRef::Replace(bytes) => {
                    let bytes: bytes::Bytes = bytes.to_vec().into();
                    row.record.work.body = bytes.clone();
                    row.body_loaded = true;
                    ResolvedBody::Replace(bytes)
                }
            };
            row.record.version = command.output_highwater;
            row.record.work.priority = metadata.priority;
            row.record.work.available_at = metadata.available_at;
            row.record.eligible_since = metadata.eligible_since;
            row.record.lifecycle = metadata.lifecycle;
            row.record.attempts = metadata.attempts;
            row.record.lease_expires_at = metadata.lease_expires_at;
            row.lease_hash = metadata.lease_hash.copied();
            crate::queue_planner::validate_work_write_view(Some(old), &row, &change, &body)
                .map_err(PublicationError::Admission)?;
            // Existing-component coverage is needed only for an exact edit.
            // Complete replacement regenerates indexes from its new full bag.
            if matches!(change, ResolvedFields::Edit(_)) {
                crate::queue_fields::resolve_index_effects(
                    &crate::queue_fields::CheckedFields::new(
                        &old.record.work.fields,
                        &old.field_coverage,
                    )
                    .map_err(PublicationError::Admission)?,
                    &change,
                    definition,
                )
                .map_err(PublicationError::Admission)?;
            }
            delta.work_fields.insert(key.clone(), change);
            delta.work_bodies.insert(key, body);
            delta.work.push(row);
        }
    }
    validate_delta(before, &delta)?;
    Ok(MaterializedAdministrativeWork {
        delta,
        _charge: charge,
    })
}

#[cfg(test)]
mod tests {
    use super::super::tests::{group, schema, seed};
    use super::*;
    use crate::{
        queue_admission::QueueAdmission, queue_fields::FieldCoverage,
        queue_planner::plan_commit_group,
    };
    use std::collections::{BTreeMap, BTreeSet};
    fn initial() -> (QueueSchema, RecordSnapshot, String) {
        let schema = schema();
        let mut create = seed();
        create.state_updates.clear();
        let mut before = RecordSnapshot::default();
        plan_commit_group(&create, &mut before, 0, &schema).unwrap();
        (schema, before, create.work_inserts[0].key.clone())
    }
    #[test]
    fn resolved_materialization_preserves_unknown_body_and_edits_exact_certified_view() {
        let (mut schema, mut before, key) = initial();
        schema
            .work
            .indexes
            .insert("compound".into(), vec!["text".into(), "gate".into()]);
        let mut update = group("change");
        update.work_updates.push(UnclaimedUpdate {
            key: key.clone(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                priority: Some(i64::MAX),
                fields: BTreeMap::from([("text".into(), None)]),
                body: Some(Vec::new().into()),
                ..Default::default()
            }),
        });
        let mut after = before.clone();
        let delta = plan_commit_group(&update, &mut after, 0, &schema).unwrap();
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let raw = encode_administrative_work_fragment(&before, &delta, &job).unwrap();
        let command = parse_administrative_work_fragment(raw.as_bytes(), 1).unwrap();
        let row = before.work.get_mut(&key).unwrap();
        row.record
            .work
            .fields
            .retain(|name, _| name == "text" || name == "gate");
        row.field_coverage = FieldCoverage::Selected {
            names: BTreeSet::from(["text".into(), "gate".into()]),
            prefixes: BTreeSet::new(),
        };
        row.record.work.body = Default::default();
        row.body_loaded = false;
        let result = materialize_administrative_work(command, &before, &schema.work, &job).unwrap();
        let output = &result.delta().work[0];
        assert_eq!(output.record.work.priority, i64::MAX);
        assert_eq!(output.record.version, 2);
        assert_eq!(output.record.insertion_sequence, 1);
        assert!(!output.record.work.fields.contains_key("text"));
        assert_eq!(output.record.work.fields.len(), 1);
        assert!(output.body_loaded && output.record.work.body.is_empty());
        assert!(matches!(
            result.delta().work_bodies[&key],
            ResolvedBody::Replace(_)
        ));
        drop(result);
        let row = before.work.get_mut(&key).unwrap();
        row.record.work.fields.remove("gate");
        row.field_coverage = FieldCoverage::Selected {
            names: BTreeSet::from(["text".into()]),
            prefixes: BTreeSet::new(),
        };
        let usage = admission.usage().unwrap();
        assert!(materialize_administrative_work(command, &before, &schema.work, &job).is_err());
        assert_eq!(admission.usage().unwrap(), usage);
    }
    #[test]
    fn resolved_materialization_rejects_stale_absent_and_changed_sequence_without_leaks() {
        let (schema, before, key) = initial();
        let mut update = group("metadata");
        update.work_updates.push(UnclaimedUpdate {
            key: key.clone(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                priority: Some(4),
                ..Default::default()
            }),
        });
        let delta = plan_commit_group(&update, &mut before.clone(), 0, &schema).unwrap();
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let raw = encode_administrative_work_fragment(&before, &delta, &job).unwrap();
        let command = parse_administrative_work_fragment(raw.as_bytes(), 1).unwrap();
        let usage = admission.usage().unwrap();
        let mut stale = before.clone();
        stale.work.get_mut(&key).unwrap().record.version = 2;
        assert!(materialize_administrative_work(command, &stale, &schema.work, &job).is_err());
        stale.work.clear();
        assert!(materialize_administrative_work(command, &stale, &schema.work, &job).is_err());
        let mut changed = command;
        let EffectRef::WorkPut {
            mut metadata,
            fields,
            body,
        } = command.effect
        else {
            panic!("put")
        };
        metadata.insertion_sequence = 2;
        changed.effect = EffectRef::WorkPut {
            metadata,
            fields,
            body,
        };
        assert!(materialize_administrative_work(changed, &before, &schema.work, &job).is_err());
        assert_eq!(admission.usage().unwrap(), usage);
        let mut selective = before.clone();
        let row = selective.work.get_mut(&key).unwrap();
        row.record.work.fields.clear();
        row.field_coverage = FieldCoverage::Selected {
            names: Default::default(),
            prefixes: Default::default(),
        };
        row.record.work.body = Default::default();
        row.body_loaded = false;
        let result =
            materialize_administrative_work(command, &selective, &schema.work, &job).unwrap();
        assert!(!result.delta().work[0].body_loaded);
        assert!(result.delta().work[0].record.work.fields.is_empty());
        assert!(matches!(
            result.delta().work_fields[&key],
            ResolvedFields::Preserve
        ));
        assert!(matches!(
            result.delta().work_bodies[&key],
            ResolvedBody::Preserve
        ));
    }
    #[test]
    fn absent_coverage_certificates_are_admitted_before_copying() {
        let (schema, mut before, key) = initial();
        let mut update = group("metadata");
        update.work_updates.push(UnclaimedUpdate {
            key: key.clone(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                priority: Some(4),
                ..Default::default()
            }),
        });
        let delta = plan_commit_group(&update, &mut before.clone(), 0, &schema).unwrap();
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let raw = encode_administrative_work_fragment(&before, &delta, &job).unwrap();
        let command = parse_administrative_work_fragment(raw.as_bytes(), 1).unwrap();
        let row = before.work.get_mut(&key).unwrap();
        row.record.work.fields.clear();
        row.record.work.body = Default::default();
        row.body_loaded = false;
        row.field_coverage = FieldCoverage::Selected {
            names: BTreeSet::from(["n".repeat(80000)]),
            prefixes: BTreeSet::from([format!("/payload/{}", "p".repeat(80000))]),
        };
        let small = QueueAdmission::new(1, 200000).unwrap();
        let limited = small.reserve(0).unwrap();
        assert!(matches!(
            materialize_administrative_work(command, &before, &schema.work, &limited),
            Err(PublicationError::Admission(QueueError::OutcomeUnknown(_)))
        ));
        assert_eq!(small.usage().unwrap(), (1, 0));
    }
    #[test]
    fn complete_replacement_regenerates_indexes_without_old_component_coverage() {
        let (mut schema, mut before, key) = initial();
        schema
            .work
            .indexes
            .insert("compound".into(), vec!["text".into(), "gate".into()]);
        let mut update = group("replace");
        update.work_updates.push(UnclaimedUpdate {
            key: key.clone(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                priority: Some(4),
                ..Default::default()
            }),
        });
        let mut delta = plan_commit_group(&update, &mut before.clone(), 0, &schema).unwrap();
        let replacement = BTreeMap::from([("gate".into(), FieldValue::Boolean(false))]);
        delta.work[0].record.work.fields = replacement.clone();
        delta.work[0].field_coverage = FieldCoverage::Complete;
        delta
            .work_fields
            .insert(key.clone(), ResolvedFields::Replace(replacement));
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let raw = encode_administrative_work_fragment(&before, &delta, &job).unwrap();
        let command = parse_administrative_work_fragment(raw.as_bytes(), 1).unwrap();
        let row = before.work.get_mut(&key).unwrap();
        row.record.work.fields.clear();
        row.record.work.body = Default::default();
        row.body_loaded = false;
        row.field_coverage = FieldCoverage::Selected {
            names: Default::default(),
            prefixes: Default::default(),
        };
        let result = materialize_administrative_work(command, &before, &schema.work, &job).unwrap();
        assert_eq!(result.delta().work[0].record.work.fields.len(), 1);
        assert_eq!(
            result.delta().work[0].field_coverage,
            FieldCoverage::Complete
        );
        assert!(!result.delta().work[0].body_loaded);
        assert!(matches!(
            result.delta().work_fields[&key],
            ResolvedFields::Replace(_)
        ));
    }
    #[test]
    fn resolved_delete_needs_no_old_fields_or_body_and_capacity_failure_is_unknown() {
        let (schema, mut before, key) = initial();
        let mut update = group("delete");
        update.work_updates.push(UnclaimedUpdate {
            key: key.clone(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Delete,
        });
        let delta = plan_commit_group(&update, &mut before.clone(), 0, &schema).unwrap();
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = admission.reserve(0).unwrap();
        let raw = encode_administrative_work_fragment(&before, &delta, &job).unwrap();
        let command = parse_administrative_work_fragment(raw.as_bytes(), 1).unwrap();
        let row = before.work.get_mut(&key).unwrap();
        row.record.work.fields.clear();
        row.record.work.body = Default::default();
        row.body_loaded = false;
        row.field_coverage = FieldCoverage::Selected {
            names: Default::default(),
            prefixes: Default::default(),
        };
        let result = materialize_administrative_work(command, &before, &schema.work, &job).unwrap();
        assert_eq!(result.delta().deleted_work, vec![key]);
        drop(result);
        let small = QueueAdmission::new(1, 8192).unwrap();
        let limited = small.reserve(0).unwrap();
        assert!(matches!(
            materialize_administrative_work(command, &before, &schema.work, &limited),
            Err(PublicationError::Admission(QueueError::OutcomeUnknown(_)))
        ));
        assert_eq!(small.usage().unwrap(), (1, 0));
    }
}
