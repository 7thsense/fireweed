//! Input-bounded ordinary planning after complete administrative preparation.
//! No uniqueness approval, snapshot merge or publication authority is implied.
use super::stream::{
    add, copy_probe, delta_bound, ordinary_bound, required_tuples, scale, tuple_bound,
    tuple_identity_bound,
};
use super::*;
use crate::queue_admission::{OwnedQueueBufferCharge, QueueReservation};
use crate::queue_fields::UniqueFieldEffects;
use crate::queue_planner::*;
use std::sync::Arc;

/// Prove expected clock-independent authority before any ordinary CAS rejection.
/// Iterating only the loaded set would silently accept omitted required probes.
pub fn validate_ordinary_prospective_authority(
    group: &CommitGroup,
    image: &RecordSnapshot,
    schema: &QueueSchema,
    reservation: &QueueReservation,
) -> QueueResult<()> {
    validate_snapshot(image)?;
    let mut bytes = scale(
        crate::queue_memory::group_expansion_budget(group, image, schema)
            .map_err(|_| unknown("prospective tuple bound exhausted"))?,
        8,
    )?;
    for key in group
        .claimed_updates
        .iter()
        .map(|u| u.key.as_str())
        .chain(group.work_updates.iter().map(|u| u.key.as_str()))
        .chain(group.work_inserts.iter().map(|u| u.key.as_str()))
    {
        bytes = add(
            bytes,
            scale(
                crate::queue_memory::identity_expansion(&schema.work, key, None)
                    .map_err(|_| unknown("prospective identity bound exhausted"))?,
                4,
            )?,
        )?;
    }
    for update in &group.state_updates {
        let definition = schema
            .state
            .get(&update.collection)
            .ok_or_else(|| unknown("missing prospective state schema"))?;
        bytes = add(
            bytes,
            scale(
                crate::queue_memory::identity_expansion(
                    definition,
                    &update.key,
                    Some(&update.collection),
                )
                .map_err(|_| unknown("prospective identity bound exhausted"))?,
                4,
            )?,
        )?;
    }
    let _scratch = reservation.buffer(add(bytes, 8192)?)?;
    for tuple in prospective_unique_probes(group, image, schema)? {
        if !image.unique_probes_loaded.contains(&tuple)
            || image
                .unique_owners
                .get(&tuple)
                .is_some_and(|owner| owner.collection != tuple.collection)
        {
            return Err(unknown(
                "incomplete or invalid prospective unique authority",
            ));
        }
    }
    Ok(())
}
/// Owned, already resolved ordinary effects. The containing operation still
/// checks the complete administrative/ordinary evidence union and size/receipt
/// limits before atomically applying these commands. Copies need new admission.
pub struct ProvisionalOrdinaryCandidate {
    before: RecordSnapshot,
    delta: CommitDelta,
    effects: UniqueFieldEffects,
    _charge: OwnedQueueBufferCharge,
}
impl ProvisionalOrdinaryCandidate {
    pub fn initial_authority(&self) -> &RecordSnapshot {
        &self.before
    }
    pub fn delta(&self) -> &CommitDelta {
        &self.delta
    }
    pub fn unique_effects(&self) -> &UniqueFieldEffects {
        &self.effects
    }
}
/// Called only after both administrative streams and prospective observations
/// have been completely validated. The only operational clock sample follows
/// snapshot validation. Initial-owner uniqueness is deliberately not checked:
/// an administrative release may make an ordinary claim valid in this group.
pub fn propose_ordinary_after_administration(
    group: &CommitGroup,
    image: &RecordSnapshot,
    ownership_now: impl FnOnce() -> QueueResult<i64>,
    schema: &QueueSchema,
    reservation: &Arc<QueueReservation>,
) -> QueueResult<Result<ProvisionalOrdinaryCandidate, Rejection>> {
    let planning = reservation.buffer(ordinary_bound(group, image)?)?;
    let mut candidate = addressed_snapshot(group, image);
    validate_group_snapshot(&candidate, group)?;
    let now = ownership_now()
        .map_err(|_| unknown("operational clock failed during ordinary finalization"))?;
    let mut delta = match apply_group(group, &mut candidate, now, schema) {
        Ok(delta) => delta,
        Err(reason) => return Ok(Err(reason)),
    };
    resolve_group_writes(group, image, &mut delta, schema)?;
    let before = addressed_snapshot(group, image);
    let mut bytes = add(delta_bound(&delta)?, tuple_identity_bound(&delta, schema)?)?;
    for row in before.work.values().chain(delta.work.iter()) {
        bytes = add(
            bytes,
            crate::queue_memory::fields_expansion(
                row.record
                    .work
                    .fields
                    .iter()
                    .map(|(name, value)| (name.as_str(), Some(value))),
                &schema.work,
            )
            .map_err(|_| unknown("ordinary tuple bound exhausted"))?,
        )?;
    }
    for row in before.state.values().chain(delta.state.iter()) {
        bytes = add(
            bytes,
            crate::queue_memory::fields_expansion(
                row.fields
                    .iter()
                    .map(|(name, value)| (name.as_str(), Some(value))),
                schema
                    .state
                    .get(&row.collection)
                    .ok_or_else(|| unknown("missing ordinary state schema"))?,
            )
            .map_err(|_| unknown("ordinary state tuple bound exhausted"))?,
        )?;
    }
    let tuple_charge = reservation.buffer(scale(add(bytes, 8192)?, 4)?)?;
    let effects = unique_effects(&delta, &before, schema)?;
    // Initial owners may have identifiers much larger than the ordinary targets.
    // Charge those exact external owner copies before materializing authority.
    let mut probe_bytes = 8192;
    for tuple in required_tuples(&effects) {
        probe_bytes = add(
            probe_bytes,
            tuple_bound(tuple, image.unique_owners.get(tuple))?,
        )?;
    }
    let probe_charge = reservation.buffer(probe_bytes)?;
    let mut candidate = ProvisionalOrdinaryCandidate {
        before,
        delta,
        effects,
        _charge: planning
            .merge(tuple_charge)?
            .merge(probe_charge)?
            .into_owned(Arc::clone(reservation))?,
    };
    for tuple in required_tuples(&candidate.effects) {
        copy_probe(&mut candidate.before, image, tuple)?;
    }
    for (tuple, owner) in &candidate.effects.releases {
        if candidate.before.unique_owners.get(tuple) != Some(owner) {
            return Err(unknown("ordinary release lacks exact initial owner"));
        }
    }
    Ok(Ok(candidate))
}

#[cfg(test)]
mod tests {
    use super::super::tests::{ordinary, row, schema};
    use super::*;
    use crate::queue_admission::QueueAdmission;
    use std::cell::Cell;
    use std::collections::{BTreeMap, BTreeSet};
    fn unique_schema() -> QueueSchema {
        let mut schema = schema();
        schema
            .work
            .fields
            .insert("/payload/u".into(), FieldType::Text);
        schema
            .work
            .indexes
            .insert("u".into(), vec!["/payload/u".into()]);
        schema.work.unique_indexes.insert("u".into());
        schema
    }
    fn tuple() -> UniqueIdentity {
        UniqueIdentity {
            collection: None,
            index: "u".into(),
            full_sort_key: crate::queue_index::ordered_key(&[Some(&FieldValue::Text("x".into()))])
                .unwrap(),
        }
    }
    fn insert() -> CommitGroup {
        let mut group = ordinary();
        group.work_inserts.push(NewWork {
            key: "next".into(),
            priority: 0,
            available_at: 0,
            fields: BTreeMap::from([("/payload/u".into(), FieldValue::Text("x".into()))]),
            body: Default::default(),
        });
        group
    }
    #[test]
    fn prospective_completeness_is_checked_without_clock_or_business_planning() {
        let admission = QueueAdmission::new(1, 8 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let mut image = RecordSnapshot {
            next_sequence: 1,
            ..Default::default()
        };
        assert!(matches!(
            validate_ordinary_prospective_authority(&insert(), &image, &unique_schema(), &job),
            Err(QueueError::OutcomeUnknown(_))
        ));
        image.unique_probes_loaded.insert(tuple());
        validate_ordinary_prospective_authority(&insert(), &image, &unique_schema(), &job).unwrap();
        assert_eq!(admission.usage().unwrap(), (1, 0));
    }
    #[test]
    fn ordinary_foreign_initial_owner_is_provisional_until_administrative_release_union() {
        let admission = QueueAdmission::new(1, 8 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let image = RecordSnapshot {
            next_sequence: 1,
            unique_probes_loaded: BTreeSet::from([tuple()]),
            unique_owners: BTreeMap::from([(
                tuple(),
                RecordIdentity {
                    collection: None,
                    key: "administrative-owner".into(),
                },
            )]),
            ..Default::default()
        };
        let group = insert();
        let schema = unique_schema();
        validate_ordinary_prospective_authority(&group, &image, &schema, &job).unwrap();
        let clocks = Cell::new(0);
        let candidate = propose_ordinary_after_administration(
            &group,
            &image,
            || {
                clocks.set(clocks.get() + 1);
                Ok(0)
            },
            &schema,
            &job,
        )
        .unwrap()
        .unwrap();
        assert_eq!(clocks.get(), 1);
        assert_eq!(candidate.delta().next_sequence, 2);
        assert_eq!(candidate.delta().versions[0].version, 1);
        assert_eq!(candidate.unique_effects().claims[0].1.key, "next");
        assert_eq!(
            candidate.initial_authority().unique_owners[&tuple()].key,
            "administrative-owner"
        );
        let mut ordinary_only = image.clone();
        assert_eq!(
            plan_commit_group(&group, &mut ordinary_only, 0, &schema)
                .unwrap()
                .receipt
                .outcome,
            GroupOutcome::Rejected(Rejection::UniqueConflict)
        );
        drop(candidate);
        assert_eq!(admission.usage().unwrap(), (1, 0));
    }
    #[test]
    fn ordinary_snapshot_is_validated_before_clock_and_fresh_expiry_is_rejected_once() {
        let admission = QueueAdmission::new(1, 8 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let mut stored = row();
        stored.record.lifecycle = Lifecycle::Leased;
        stored.record.lease_expires_at = Some(10);
        stored.lease_hash = Some(lease_hash(b"token"));
        let id = RecordIdentity {
            collection: None,
            key: "a".into(),
        };
        let mut image = RecordSnapshot {
            work: BTreeMap::from([("a".into(), stored)]),
            versions: BTreeMap::from([(id.clone(), 2)]),
            next_sequence: 1,
            ..Default::default()
        };
        let mut group = ordinary();
        group.claimed_updates.push(ClaimedUpdate {
            key: "a".into(),
            lease: LeaseCredential {
                token: b"token".to_vec(),
            },
            expected_version: 1,
            patch: Default::default(),
            disposition: Disposition::Complete,
        });
        let clocks = Cell::new(0);
        assert!(matches!(
            propose_ordinary_after_administration(
                &group,
                &image,
                || {
                    clocks.set(1);
                    Ok(10)
                },
                &schema(),
                &job
            ),
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(clocks.get(), 0);
        image.versions.insert(id, 1);
        assert!(matches!(
            propose_ordinary_after_administration(
                &group,
                &image,
                || {
                    clocks.set(clocks.get() + 1);
                    Ok(10)
                },
                &schema(),
                &job
            )
            .unwrap(),
            Err(Rejection::StaleLease)
        ));
        assert_eq!(clocks.get(), 1);
        assert_eq!(admission.usage().unwrap(), (1, 0));
    }
}
