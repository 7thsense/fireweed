//! Shared selective field authority and exact resolved edits. This internal
//! vocabulary is not yet an activated/versioned durable command envelope.
use crate::queue_index::ordered_key;
use fireweed_core::queue_contract::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
pub type FieldEdits = BTreeMap<String, Option<FieldValue>>;
fn unknown(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(message.into())
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldCoverage {
    Complete,
    Selected {
        names: BTreeSet<String>,
        prefixes: BTreeSet<String>,
    },
}
/// Values prove presence; coverage additionally certifies missing names. A
/// certified prefix enumerates ALL matching fields under the initial fence.
pub struct CheckedFields<'a> {
    fields: &'a Fields,
    coverage: &'a FieldCoverage,
}
impl<'a> CheckedFields<'a> {
    pub fn new(fields: &'a Fields, coverage: &'a FieldCoverage) -> QueueResult<Self> {
        if let FieldCoverage::Selected { prefixes, .. } = coverage
            && prefixes.iter().any(|p| p.trim().is_empty())
        {
            return Err(unknown("empty field-coverage prefix"));
        }
        Ok(Self { fields, coverage })
    }
    pub fn get(&self, name: &str) -> QueueResult<Option<&'a FieldValue>> {
        if let Some(value) = self.fields.get(name) {
            return Ok(Some(value));
        }
        let covered = match self.coverage {
            FieldCoverage::Complete => true,
            FieldCoverage::Selected { names, prefixes } => {
                names.contains(name) || prefixes.iter().any(|prefix| name.starts_with(prefix))
            }
        };
        if covered {
            Ok(None)
        } else {
            Err(unknown("field absence outside loaded authority"))
        }
    }
    pub fn prefix<'p>(
        &'p self,
        prefix: &'p str,
    ) -> QueueResult<impl Iterator<Item = (&'a String, &'a FieldValue)> + 'p> {
        if prefix.trim().is_empty() {
            return Err(unknown("empty field-enumeration prefix"));
        }
        let covered = match self.coverage {
            FieldCoverage::Complete => true,
            FieldCoverage::Selected { prefixes, .. } => {
                prefixes.iter().any(|parent| prefix.starts_with(parent))
            }
        };
        if !covered {
            return Err(unknown("prefix enumeration outside loaded authority"));
        }
        Ok(self
            .fields
            .iter()
            .filter(move |(name, _)| name.starts_with(prefix)))
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ResolvedFields {
    Preserve,
    Replace(Fields),
    /// Exact names only; prefix enumeration is already frozen. Never insert a
    /// missing record using this partial representation.
    Edit(FieldEdits),
}
/// One shared edit application for borrowed request patches and owned resolved
/// commands. Callers choose whether values must be cloned or can be moved.
pub(crate) fn apply_field_edits(
    fields: &mut Fields,
    edits: impl IntoIterator<Item = (String, Option<FieldValue>)>,
) {
    for (name, value) in edits {
        if let Some(value) = value {
            fields.insert(name, value);
        } else {
            fields.remove(&name);
        }
    }
}
impl ResolvedFields {
    pub fn apply(self, fields: &mut Fields) {
        match self {
            Self::Preserve => {}
            Self::Replace(replacement) => *fields = replacement,
            Self::Edit(edits) => apply_field_edits(fields, edits),
        }
    }
    /// Ordered composition of field intents, never last partial-record wins.
    pub fn then(self, next: Self) -> Self {
        match (self, next) {
            (old, Self::Preserve) => old,
            (_, Self::Replace(fields)) => Self::Replace(fields),
            (Self::Preserve, next) => next,
            (Self::Replace(mut fields), Self::Edit(edits)) => {
                Self::Edit(edits).apply(&mut fields);
                Self::Replace(fields)
            }
            (Self::Edit(mut old), Self::Edit(next)) => {
                old.extend(next);
                Self::Edit(old)
            }
        }
    }
    fn after<'a>(
        &'a self,
        before: &CheckedFields<'a>,
        name: &str,
    ) -> QueueResult<Option<&'a FieldValue>> {
        match self {
            Self::Preserve => before.get(name),
            Self::Replace(fields) => Ok(fields.get(name)),
            Self::Edit(edits) => match edits.get(name) {
                Some(value) => Ok(value.as_ref()),
                None => before.get(name),
            },
        }
    }
}
/// Freeze prefix removals before explicit edits; additions therefore win. Only
/// actual differences survive, so metadata-only/no-op edits do not copy a bag.
pub fn resolve_edits(
    before: &CheckedFields<'_>,
    explicit: &FieldEdits,
    remove_prefixes: &BTreeSet<String>,
    definition: &CollectionDefinition,
) -> QueueResult<ResolvedFields> {
    let mut edits = FieldEdits::new();
    for prefix in remove_prefixes {
        if prefix.trim().is_empty()
            || !definition
                .additional_field_prefixes
                .iter()
                .any(|namespace| prefix.starts_with(namespace))
        {
            return Err(QueueError::Invalid(
                "prefix removal outside provisioned namespace".into(),
            ));
        }
        for (name, _) in before.prefix(prefix)? {
            edits.insert(name.clone(), None);
        }
    }
    for (name, value) in explicit {
        if name.trim().is_empty() || !definition.admits_field(name) {
            return Err(QueueError::Invalid("undeclared field edit".into()));
        }
        if let Some(value) = value {
            definition.validate_value(name, value)?;
        }
        // Every explicit edit needs its old presence/value to determine no-op.
        before.get(name)?;
        edits.insert(name.clone(), value.clone());
    }
    let mut actual = FieldEdits::new();
    for (name, value) in edits {
        if before.get(&name)? != value.as_ref() {
            actual.insert(name, value);
        }
    }
    Ok(if actual.is_empty() {
        ResolvedFields::Preserve
    } else {
        ResolvedFields::Edit(actual)
    })
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexEffect {
    pub index: String,
    pub unique: bool,
    pub before_unique: bool,
    pub after_unique: bool,
    pub before: Option<Vec<u8>>,
    pub after: Option<Vec<u8>>,
}
/// Components of EVERY affected declared index, including nonunique query
/// indexes. Unaffected index rows/ownership are preserved by physical apply.
pub fn affected_index_fields(
    change: &ResolvedFields,
    definition: &CollectionDefinition,
) -> BTreeSet<String> {
    definition
        .indexes
        .values()
        .filter(|columns| match change {
            ResolvedFields::Preserve => false,
            ResolvedFields::Replace(_) => true,
            ResolvedFields::Edit(edits) => columns.iter().any(|name| edits.contains_key(name)),
        })
        .flat_map(|columns| columns.iter().cloned())
        .collect()
}
/// Existing records only: sparse fields still have physical query-index rows.
/// Enclosing full create/delete generation handles record existence changes.
pub fn resolve_index_effects(
    before: &CheckedFields<'_>,
    change: &ResolvedFields,
    definition: &CollectionDefinition,
) -> QueueResult<Vec<IndexEffect>> {
    let mut effects = Vec::new();
    for (index, columns) in &definition.indexes {
        let affected = match change {
            ResolvedFields::Preserve => false,
            ResolvedFields::Replace(_) => true,
            ResolvedFields::Edit(edits) => columns.iter().any(|name| edits.contains_key(name)),
        };
        if !affected {
            continue;
        }
        let old = columns
            .iter()
            .map(|name| before.get(name))
            .collect::<QueueResult<Vec<_>>>()?;
        let new = columns
            .iter()
            .map(|name| change.after(before, name))
            .collect::<QueueResult<Vec<_>>>()?;
        for (name, value) in columns.iter().zip(&old).chain(columns.iter().zip(&new)) {
            if let Some(value) = value {
                definition
                    .validate_value(name, value)
                    .map_err(|_| unknown("invalid resolved index field authority"))?;
            }
        }
        let unique = definition.unique_indexes.contains(index);
        let before_unique = unique && old.iter().all(Option::is_some);
        let after_unique = unique && new.iter().all(Option::is_some);
        let old = Some(ordered_key(&old)?);
        let new = Some(ordered_key(&new)?);
        if old != new {
            effects.push(IndexEffect {
                index: index.clone(),
                unique,
                before_unique,
                after_unique,
                before: old,
                after: new,
            });
        }
    }
    Ok(effects)
}
#[derive(Debug, Clone, PartialEq)]
pub struct FieldRecord {
    pub version: u64,
    pub fields: Fields,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedFieldCommand {
    pub predecessor: Option<u64>,
    pub prior_highwater: u64,
    pub version: u64,
    pub change: ResolvedFields,
}
/// Internal replay invariant check, not reevaluation of user CAS/selection/time.
/// Inserts require full Replace. The caller atomically stores returned highwater,
/// metadata, receipts and apply cursor with this field result.
pub fn apply_command(
    current: Option<FieldRecord>,
    highwater: u64,
    command: ResolvedFieldCommand,
) -> QueueResult<(FieldRecord, u64)> {
    if command.prior_highwater != highwater
        || command.predecessor == Some(0)
        || current.as_ref().is_some_and(|r| r.version == 0)
        || command.version <= highwater
        || current.as_ref().map(|r| r.version) != command.predecessor
        || current.as_ref().is_some_and(|r| r.version != highwater)
    {
        return Err(unknown("resolved field predecessor/highwater mismatch"));
    }
    let mut fields = if let Some(current) = current {
        current.fields
    } else {
        if !matches!(command.change, ResolvedFields::Replace(_)) {
            return Err(unknown("partial field command cannot insert a record"));
        }
        Fields::new()
    };
    command.change.apply(&mut fields);
    Ok((
        FieldRecord {
            version: command.version,
            fields,
        },
        command.version,
    ))
}

/// Apply to a speculative selective view without turning an unloaded field into
/// known absent. Explicit edited names become certified; Replace becomes complete.
pub fn apply_to_view(fields: &mut Fields, coverage: &mut FieldCoverage, change: ResolvedFields) {
    match &change {
        ResolvedFields::Replace(_) => *coverage = FieldCoverage::Complete,
        ResolvedFields::Edit(edits) => {
            if let FieldCoverage::Selected { names, .. } = coverage {
                names.extend(edits.keys().cloned());
            }
        }
        ResolvedFields::Preserve => {}
    }
    change.apply(fields);
}
#[derive(Debug, Clone, Default)]
pub struct UniqueFieldEffects {
    pub releases:
        BTreeMap<crate::queue_planner::UniqueIdentity, crate::queue_planner::RecordIdentity>,
    pub claims: Vec<(
        crate::queue_planner::UniqueIdentity,
        crate::queue_planner::RecordIdentity,
    )>,
}
impl UniqueFieldEffects {
    pub fn extend(
        &mut self,
        owner: &crate::queue_planner::RecordIdentity,
        effects: &[IndexEffect],
    ) -> QueueResult<()> {
        for effect in effects {
            if (effect.before_unique && (!effect.unique || effect.before.is_none()))
                || (effect.after_unique && (!effect.unique || effect.after.is_none()))
            {
                return Err(unknown("invalid unique index effect eligibility"));
            }
            if !effect.unique
                || (effect.before == effect.after && effect.before_unique == effect.after_unique)
            {
                continue;
            }
            let identity = |key: &Vec<u8>| crate::queue_planner::UniqueIdentity {
                collection: owner.collection.clone(),
                index: effect.index.clone(),
                full_sort_key: key.clone(),
            };
            if effect.before_unique
                && let Some(before) = &effect.before
            {
                let tuple = identity(before);
                if self.releases.get(&tuple).is_some_and(|old| old != owner) {
                    return Err(unknown("contradictory released tuple ownership"));
                }
                self.releases.insert(tuple, owner.clone());
            }
            if effect.after_unique
                && let Some(after) = &effect.after
            {
                self.claims.push((identity(after), owner.clone()));
            }
        }
        Ok(())
    }
    /// Check exact initial authority and final claims. Metadata/version changes
    /// are not releases. An absent probe is never proof of nonownership.
    pub fn validate(
        &self,
        initial: &crate::queue_planner::RecordSnapshot,
    ) -> QueueResult<Result<(), Rejection>> {
        for (tuple, owner) in &self.releases {
            if !initial.unique_probes_loaded.contains(tuple)
                || initial.unique_owners.get(tuple) != Some(owner)
            {
                return Err(unknown(
                    "released unique tuple lacks initial owner authority",
                ));
            }
        }
        let mut claims = BTreeMap::new();
        for (tuple, owner) in &self.claims {
            if !initial.unique_probes_loaded.contains(tuple) {
                return Err(unknown("new unique tuple authority not loaded"));
            }
            if claims.insert(tuple, owner).is_some_and(|old| old != owner) {
                return Ok(Err(Rejection::UniqueConflict));
            }
            if let Some(old) = initial.unique_owners.get(tuple)
                && old != owner
                && self.releases.get(tuple) != Some(old)
            {
                return Ok(Err(Rejection::UniqueConflict));
            }
        }
        Ok(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn definition() -> CollectionDefinition {
        CollectionDefinition {
            fields: BTreeMap::from([
                ("a".into(), FieldType::Integer),
                ("b".into(), FieldType::Text),
                ("u".into(), FieldType::Integer),
            ]),
            indexes: BTreeMap::from([
                ("by_ab".into(), vec!["a".into(), "b".into()]),
                ("by_u".into(), vec!["u".into()]),
            ]),
            unique_indexes: BTreeSet::from(["by_u".into()]),
            additional_field_prefixes: BTreeSet::from(["/gates/".into(), "/payload/".into()]),
        }
    }
    fn selected(names: &[&str], prefixes: &[&str]) -> FieldCoverage {
        FieldCoverage::Selected {
            names: names.iter().map(|n| (*n).into()).collect(),
            prefixes: prefixes.iter().map(|n| (*n).into()).collect(),
        }
    }
    #[test]
    fn absence_requires_coverage_and_prefix_enumeration_is_certified() {
        let fields = Fields::from([("/payload/null".into(), FieldValue::Bytes(vec![0]))]);
        let coverage = selected(&["known"], &["/gates/"]);
        let view = CheckedFields::new(&fields, &coverage).unwrap();
        assert_eq!(view.get("known").unwrap(), None);
        assert_eq!(view.get("/gates/absent").unwrap(), None);
        assert_eq!(
            view.get("/payload/null").unwrap(),
            Some(&FieldValue::Bytes(vec![0]))
        );
        assert!(matches!(
            view.get("unloaded"),
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(view.prefix("/gates/child/").unwrap().count(), 0);
        assert!(matches!(
            view.prefix("/payload/"),
            Err(QueueError::OutcomeUnknown(_))
        ));
        let bad = selected(&[], &[""]);
        assert!(CheckedFields::new(&fields, &bad).is_err());
    }
    #[test]
    fn metadata_edit_delta_never_materializes_unrelated_megabyte_payload() {
        let long = format!("/payload/{}", "x\0λ".repeat(8192));
        let original = Fields::from([
            (long.clone(), FieldValue::Bytes(vec![7; 1 << 20])),
            ("/gates/paused".into(), FieldValue::Boolean(false)),
        ]);
        let partial = Fields::from([("/gates/paused".into(), FieldValue::Boolean(false))]);
        let coverage = selected(&["/gates/paused"], &[]);
        let view = CheckedFields::new(&partial, &coverage).unwrap();
        let explicit =
            FieldEdits::from([("/gates/paused".into(), Some(FieldValue::Boolean(true)))]);
        let change = resolve_edits(&view, &explicit, &BTreeSet::new(), &definition()).unwrap();
        assert!(encoded_len(&change).unwrap() < 128);
        assert!(affected_index_fields(&change, &definition()).is_empty());
        assert!(
            resolve_index_effects(&view, &change, &definition())
                .unwrap()
                .is_empty()
        );
        let mut result = original.clone();
        change.apply(&mut result);
        let mut oracle = original;
        oracle.insert("/gates/paused".into(), FieldValue::Boolean(true));
        assert_eq!(result, oracle);
        assert_eq!(result[&long], FieldValue::Bytes(vec![7; 1 << 20]));
        let empty = Fields::new();
        let coverage = selected(&[], &[]);
        let view = CheckedFields::new(&empty, &coverage).unwrap();
        assert_eq!(
            resolve_edits(&view, &FieldEdits::new(), &BTreeSet::new(), &definition()).unwrap(),
            ResolvedFields::Preserve
        );
    }
    #[test]
    fn nonunique_composite_dependencies_are_loaded_without_touching_other_indexes() {
        let fields = Fields::from([
            ("a".into(), FieldValue::Integer(1)),
            ("b".into(), FieldValue::Text("b\0λ".into())),
        ]);
        let coverage = selected(&["a", "b"], &[]);
        let view = CheckedFields::new(&fields, &coverage).unwrap();
        let edit = resolve_edits(
            &view,
            &FieldEdits::from([("a".into(), Some(FieldValue::Integer(2)))]),
            &BTreeSet::new(),
            &definition(),
        )
        .unwrap();
        assert_eq!(
            affected_index_fields(&edit, &definition()),
            BTreeSet::from(["a".into(), "b".into()])
        );
        let effects = resolve_index_effects(&view, &edit, &definition()).unwrap();
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].index, "by_ab");
        assert!(!effects[0].unique);
        assert_eq!(
            effects[0].after,
            Some(ordered_key(&[Some(&FieldValue::Integer(2)), fields.get("b")]).unwrap())
        );
        let only_a = Fields::from([("a".into(), FieldValue::Integer(1))]);
        let coverage = selected(&["a"], &[]);
        let incomplete = CheckedFields::new(&only_a, &coverage).unwrap();
        assert!(matches!(
            resolve_index_effects(&incomplete, &edit, &definition()),
            Err(QueueError::OutcomeUnknown(_))
        ));
    }
    #[test]
    fn frozen_prefix_removal_preserves_explicit_addition_precedence_and_coverage() {
        let fields = Fields::from([
            ("/gates/a".into(), FieldValue::Boolean(true)),
            ("/gates/b".into(), FieldValue::Boolean(true)),
        ]);
        let mut coverage = selected(&[], &["/gates/"]);
        let view = CheckedFields::new(&fields, &coverage).unwrap();
        let explicit = FieldEdits::from([
            ("/gates/a".into(), Some(FieldValue::Boolean(true))),
            ("/gates/c".into(), Some(FieldValue::Boolean(true))),
        ]);
        let change = resolve_edits(
            &view,
            &explicit,
            &BTreeSet::from(["/gates/".into()]),
            &definition(),
        )
        .unwrap();
        assert_eq!(
            change,
            ResolvedFields::Edit(FieldEdits::from([
                ("/gates/b".into(), None),
                ("/gates/c".into(), Some(FieldValue::Boolean(true)))
            ]))
        );
        let mut fields = fields;
        apply_to_view(&mut fields, &mut coverage, change);
        let view = CheckedFields::new(&fields, &coverage).unwrap();
        assert_eq!(view.get("/gates/b").unwrap(), None);
        assert_eq!(
            view.get("/gates/a").unwrap(),
            Some(&FieldValue::Boolean(true))
        );
        assert_eq!(
            view.get("/gates/c").unwrap(),
            Some(&FieldValue::Boolean(true))
        );
        assert!(view.get("/payload/unloaded").is_err());
    }
    #[test]
    fn packed_composition_matches_ordered_full_image_oracle() {
        let edit = |name: &str, value: Option<i64>| {
            ResolvedFields::Edit(FieldEdits::from([(
                name.into(),
                value.map(FieldValue::Integer),
            )]))
        };
        let operations = vec![
            ResolvedFields::Preserve,
            edit("a", Some(2)),
            edit("a", None),
            edit("b", Some(3)),
            ResolvedFields::Replace(Fields::new()),
            ResolvedFields::Replace(Fields::from([("r".into(), FieldValue::Integer(4))])),
        ];
        for a in &operations {
            for b in &operations {
                for c in &operations {
                    let initial = Fields::from([("a".into(), FieldValue::Integer(1))]);
                    let mut oracle = initial.clone();
                    a.clone().apply(&mut oracle);
                    b.clone().apply(&mut oracle);
                    c.clone().apply(&mut oracle);
                    let mut combined = initial;
                    a.clone()
                        .then(b.clone())
                        .then(c.clone())
                        .apply(&mut combined);
                    assert_eq!(combined, oracle);
                }
            }
        }
        let mut fields = Fields::new();
        let mut coverage = selected(&[], &[]);
        apply_to_view(&mut fields, &mut coverage, edit("a", None));
        assert_eq!(
            CheckedFields::new(&fields, &coverage)
                .unwrap()
                .get("a")
                .unwrap(),
            None
        );
        apply_to_view(
            &mut fields,
            &mut coverage,
            ResolvedFields::Replace(Fields::new()),
        );
        assert_eq!(coverage, FieldCoverage::Complete);
    }
    #[test]
    fn replay_requires_predecessor_and_partial_edits_cannot_create_or_reset_highwater() {
        let edit = ResolvedFields::Edit(FieldEdits::from([(
            "a".into(),
            Some(FieldValue::Integer(2)),
        )]));
        assert!(
            apply_command(
                None,
                0,
                ResolvedFieldCommand {
                    predecessor: None,
                    prior_highwater: 0,
                    version: 1,
                    change: edit.clone()
                }
            )
            .is_err()
        );
        let initial = FieldRecord {
            version: 4,
            fields: Fields::from([("a".into(), FieldValue::Integer(1))]),
        };
        assert!(
            apply_command(
                Some(initial.clone()),
                4,
                ResolvedFieldCommand {
                    predecessor: Some(3),
                    prior_highwater: 4,
                    version: 5,
                    change: edit.clone()
                }
            )
            .is_err()
        );
        let (edited, highwater) = apply_command(
            Some(initial),
            4,
            ResolvedFieldCommand {
                predecessor: Some(4),
                prior_highwater: 4,
                version: 6,
                change: edit,
            },
        )
        .unwrap();
        assert_eq!(edited.fields["a"], FieldValue::Integer(2));
        assert_eq!(highwater, 6);
        // Deletion is owned by the enclosing record command and leaves hwm=7.
        assert!(
            apply_command(
                None,
                7,
                ResolvedFieldCommand {
                    predecessor: None,
                    prior_highwater: 7,
                    version: 1,
                    change: ResolvedFields::Replace(Fields::new())
                }
            )
            .is_err()
        );
        let (reinserted, highwater) = apply_command(
            None,
            7,
            ResolvedFieldCommand {
                predecessor: None,
                prior_highwater: 7,
                version: 8,
                change: ResolvedFields::Replace(Fields::new()),
            },
        )
        .unwrap();
        assert!(reinserted.fields.is_empty());
        assert_eq!(highwater, 8);
    }
    #[test]
    fn sparse_unique_indexes_keep_physical_keys_separate_from_ownership() {
        use crate::queue_planner::RecordIdentity;
        let mut def = definition();
        def.unique_indexes.insert("by_ab".into());
        for (old_b, new_b) in [(None, None), (Some("b"), None), (None, Some("b"))] {
            let mut fields = Fields::from([("a".into(), FieldValue::Integer(1))]);
            if let Some(b) = old_b {
                fields.insert("b".into(), FieldValue::Text(b.into()));
            }
            let view = CheckedFields::new(&fields, &FieldCoverage::Complete).unwrap();
            let change = ResolvedFields::Edit(FieldEdits::from([
                ("a".into(), Some(FieldValue::Integer(2))),
                ("b".into(), new_b.map(|b| FieldValue::Text(b.into()))),
            ]));
            let effects = resolve_index_effects(&view, &change, &def).unwrap();
            assert_eq!(effects.len(), 1);
            let effect = &effects[0];
            assert_eq!(
                effect.before,
                Some(ordered_key(&[fields.get("a"), fields.get("b")]).unwrap())
            );
            let mut after = fields.clone();
            change.apply(&mut after);
            assert_eq!(
                effect.after,
                Some(ordered_key(&[after.get("a"), after.get("b")]).unwrap())
            );
            assert_eq!(effect.before_unique, old_b.is_some());
            assert_eq!(effect.after_unique, new_b.is_some());
            let mut ownership = UniqueFieldEffects::default();
            ownership
                .extend(
                    &RecordIdentity {
                        collection: None,
                        key: "row".into(),
                    },
                    &effects,
                )
                .unwrap();
            assert_eq!(ownership.releases.len(), usize::from(old_b.is_some()));
            assert_eq!(ownership.claims.len(), usize::from(new_b.is_some()));
        }
    }
    #[test]
    fn replay_rejects_zero_versions_and_wrong_absent_highwater() {
        let command = |predecessor, prior_highwater| ResolvedFieldCommand {
            predecessor,
            prior_highwater,
            version: 8,
            change: ResolvedFields::Replace(Fields::new()),
        };
        assert!(
            apply_command(
                Some(FieldRecord {
                    version: 0,
                    fields: Fields::new()
                }),
                0,
                command(Some(0), 0)
            )
            .is_err()
        );
        assert!(apply_command(None, 7, command(None, 4)).is_err());
        assert!(apply_command(None, 4, command(None, 4)).is_ok());
    }
    #[test]
    fn unchanged_unique_owner_is_not_released_and_exact_swaps_remain_atomic() {
        use crate::queue_planner::*;
        let owner = |name: &str| RecordIdentity {
            collection: None,
            key: name.into(),
        };
        let tuple = |value: i64| UniqueIdentity {
            collection: None,
            index: "by_u".into(),
            full_sort_key: ordered_key(&[Some(&FieldValue::Integer(value))]).unwrap(),
        };
        let effect = |old: i64, new: i64| IndexEffect {
            index: "by_u".into(),
            unique: true,
            before_unique: true,
            after_unique: true,
            before: Some(tuple(old).full_sort_key),
            after: Some(tuple(new).full_sort_key),
        };
        let mut initial = RecordSnapshot::default();
        for n in 1..=3 {
            initial.unique_probes_loaded.insert(tuple(n));
        }
        initial.unique_owners.insert(tuple(1), owner("a"));
        initial.unique_owners.insert(tuple(2), owner("b"));
        let mut metadata_and_b = UniqueFieldEffects::default();
        metadata_and_b.extend(&owner("a"), &[]).unwrap();
        metadata_and_b.extend(&owner("b"), &[effect(2, 1)]).unwrap();
        assert_eq!(
            metadata_and_b.validate(&initial).unwrap(),
            Err(Rejection::UniqueConflict)
        );
        let mut swap = UniqueFieldEffects::default();
        swap.extend(&owner("a"), &[effect(1, 2)]).unwrap();
        swap.extend(&owner("b"), &[effect(2, 1)]).unwrap();
        assert_eq!(swap.validate(&initial).unwrap(), Ok(()));
        let mut collision = UniqueFieldEffects::default();
        collision.extend(&owner("a"), &[effect(1, 3)]).unwrap();
        collision.extend(&owner("b"), &[effect(2, 3)]).unwrap();
        assert_eq!(
            collision.validate(&initial).unwrap(),
            Err(Rejection::UniqueConflict)
        );
        initial.unique_probes_loaded.remove(&tuple(2));
        assert!(matches!(
            swap.validate(&initial),
            Err(QueueError::OutcomeUnknown(_))
        ));
    }
}
