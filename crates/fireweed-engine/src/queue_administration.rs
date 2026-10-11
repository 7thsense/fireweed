//! Pure, per-target administrative proposals. These are not durable outcomes:
//! a fenced operation still validates uniqueness/overlap/state CAS and publication.
use crate::queue_planner::{StoredWork, apply_administrative_update};
use fireweed_core::{queue_administration::*, queue_contract::*};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq)]
pub struct AdministrativeProposal {
    pub outcome: AdministrativeOutcome,
    pub before: Option<AdministrativeBefore>,
    /// Candidate replacement; absent for a rejection, no-op or deletion.
    pub replacement: Option<StoredWork>,
}
fn unknown(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(message.into())
}
#[derive(Clone, Copy)]
struct TargetRequest<'a> {
    key: &'a str,
    expected_version: Option<u64>,
    precondition: &'a WorkSelector,
    action: &'a AdministrativeAction,
}
impl<'a> From<&'a AdministrativeEntry> for TargetRequest<'a> {
    fn from(entry: &'a AdministrativeEntry) -> Self {
        Self {
            key: &entry.key,
            expected_version: entry.expected_version,
            precondition: &entry.precondition,
            action: &entry.action,
        }
    }
}
/// The caller supplies complete fenced raw authority and its durable version high
/// water. Do not normalize expired leases before passing the administrative time.
/// Duplicate addressed inputs must be reported positionally as Invalid by the
/// operation planner before this per-target helper is called.
pub fn propose_target(
    entry: &AdministrativeEntry,
    target: Option<&StoredWork>,
    highwater: Option<u64>,
    evaluated_at: i64,
    before_fields: Option<&BTreeSet<String>>,
    schema: &QueueSchema,
    limits: &QueueLimits,
) -> QueueResult<AdministrativeProposal> {
    propose_target_view(
        entry.into(),
        target,
        highwater,
        evaluated_at,
        before_fields,
        schema,
        limits,
    )
}
fn propose_target_view(
    entry: TargetRequest<'_>,
    target: Option<&StoredWork>,
    highwater: Option<u64>,
    evaluated_at: i64,
    before_fields: Option<&BTreeSet<String>>,
    schema: &QueueSchema,
    limits: &QueueLimits,
) -> QueueResult<AdministrativeProposal> {
    entry.precondition.validate(&schema.work)?;
    entry.action.validate(schema, limits)?;
    if let Some(names) = before_fields {
        for name in names {
            if !schema.work.admits_field(name) {
                return Err(QueueError::Invalid(
                    "undeclared administrative before field".into(),
                ));
            }
        }
    }
    let Some(target) = target else {
        return Ok(AdministrativeProposal {
            outcome: AdministrativeOutcome::NotFound,
            before: None,
            replacement: None,
        });
    };
    if !matches!(
        target.field_coverage,
        crate::queue_fields::FieldCoverage::Complete
    ) || target.record.work.key != entry.key
        || target.record.version == 0
        || highwater != Some(target.record.version)
        || target.record.insertion_sequence == 0
    {
        return Err(unknown("incomplete administrative target authority"));
    }
    let before = before_fields.map(|names| AdministrativeBefore {
        version: target.record.version,
        lifecycle: target.record.lifecycle,
        available_at: target.record.work.available_at,
        eligible_since: target.record.eligible_since,
        attempts: target.record.attempts,
        lease_expires_at: target.record.lease_expires_at,
        fields: names
            .iter()
            .filter_map(|name| {
                target
                    .record
                    .work
                    .fields
                    .get(name)
                    .map(|value| (name.clone(), value.clone()))
            })
            .collect(),
    });
    macro_rules! rejected {
        ($outcome:expr) => {
            AdministrativeProposal {
                outcome: $outcome,
                before,
                replacement: None,
            }
        };
    }
    if entry
        .expected_version
        .is_some_and(|version| version != target.record.version)
    {
        return Ok(rejected!(AdministrativeOutcome::Conflict {
            actual_version: target.record.version,
        }));
    }
    if !entry.precondition.matches(&target.record, evaluated_at) {
        return Ok(rejected!(AdministrativeOutcome::PreconditionFailed));
    }
    let (changed, mut replacement) = match &entry.action {
        AdministrativeAction::Delete { lease_guard } => {
            let active = lease_active(&target.record, evaluated_at);
            match lease_guard {
                AdministrativeLeaseGuard::RejectActive if active => {
                    return Ok(rejected!(AdministrativeOutcome::Leased));
                }
                AdministrativeLeaseGuard::RequireActive if !active => {
                    return Ok(rejected!(AdministrativeOutcome::LeaseRequired));
                }
                _ => {}
            }
            let version = target
                .record
                .version
                .checked_add(1)
                .ok_or_else(|| unknown("administrative version exhausted"))?;
            return Ok(rejected!(AdministrativeOutcome::Deleted { version }));
        }
        AdministrativeAction::Update {
            patch,
            lifecycle,
            lease_guard,
            remove_field_prefixes,
        } => {
            if patch.body.is_some() && !target.body_loaded {
                return Err(unknown("administrative body authority not loaded"));
            }
            let mut resolved = patch.clone();
            // Prefix removal precedes explicit field edits, matching gate removal
            // then add precedence. Opaque work/state bodies are never interpreted.
            for name in target.record.work.fields.keys() {
                if remove_field_prefixes
                    .iter()
                    .any(|prefix| name.starts_with(prefix))
                    && !patch.fields.contains_key(name)
                {
                    resolved.fields.insert(name.clone(), None);
                }
            }
            let mut replacement = target.clone();
            match apply_administrative_update(
                &mut replacement,
                &resolved,
                lifecycle,
                lease_guard,
                evaluated_at,
            ) {
                Ok(changed) => (changed, replacement),
                Err(reason) => {
                    let outcome = match reason {
                        Rejection::Leased => AdministrativeOutcome::Leased,
                        Rejection::StaleLease => AdministrativeOutcome::LeaseRequired,
                        Rejection::Terminal => AdministrativeOutcome::Terminal,
                        Rejection::Invalid(_) => AdministrativeOutcome::Invalid,
                        _ => return Err(unknown("unexpected administrative transition rejection")),
                    };
                    return Ok(rejected!(outcome));
                }
            }
        }
    };
    if !changed {
        return Ok(rejected!(AdministrativeOutcome::NoChange));
    }
    replacement.record.version = target
        .record
        .version
        .checked_add(1)
        .ok_or_else(|| unknown("administrative version exhausted"))?;
    Ok(AdministrativeProposal {
        outcome: AdministrativeOutcome::Changed {
            version: replacement.record.version,
        },
        before,
        replacement: Some(replacement),
    })
}

/// Native selection ownership, resolved under the containing queue fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedAdministrativeTarget {
    pub key: String,
    pub clause: usize,
}
#[derive(Debug, Clone, PartialEq)]
pub struct AdministrativeGroupProposal {
    pub delta: crate::queue_planner::CommitDelta,
    /// None on outer rejection. Never acknowledge prior speculative successes.
    pub administration: Option<AdministrativeResult>,
}
/// Pure combined planning only. The caller validates the ordinary wire envelope,
/// admits all planning/publication buffers, and supplies complete native selection
/// plus initial owner/nonowner probes under one queue fence. Neither this proposal
/// nor its legacy ordinary receipt is an administrative durable execution receipt.
pub fn propose_group(
    ordinary: &CommitGroup,
    intent: &AdministrativeIntent,
    selected: &[SelectedAdministrativeTarget],
    snapshot: &crate::queue_planner::RecordSnapshot,
    ownership_now: i64,
    schema: &QueueSchema,
    limits: &QueueLimits,
) -> QueueResult<AdministrativeGroupProposal> {
    use crate::queue_planner::*;
    use std::collections::BTreeMap;
    intent.validate(schema, limits)?;
    validate_snapshot(snapshot)?;
    if snapshot.work.values().any(|row| {
        !matches!(
            row.field_coverage,
            crate::queue_fields::FieldCoverage::Complete
        )
    }) {
        return Err(unknown(
            "selective administrative target planning not activated",
        ));
    }
    let rejected = |reason| AdministrativeGroupProposal {
        delta: CommitDelta {
            work: vec![],
            work_fields: BTreeMap::new(),
            work_bodies: BTreeMap::new(),
            deleted_work: vec![],
            state: vec![],
            deleted_state: vec![],
            versions: vec![],
            next_sequence: snapshot.next_sequence,
            receipt: CommitReceipt {
                request_id: ordinary.request_id.clone(),
                outcome: GroupOutcome::Rejected(reason),
            },
        },
        administration: None,
    };
    let mut result = AdministrativeResult {
        histogram: AdministrativeHistogram::default(),
        items: vec![],
        clauses: vec![],
    };
    match &intent.operation {
        AdministrativeOperation::Addressed(_) => {
            if !selected.is_empty() {
                return Err(unknown("addressed administration has selector targets"));
            }
        }
        AdministrativeOperation::SelectFirst(clauses) => {
            result.clauses = clauses
                .iter()
                .map(|clause| AdministrativeClauseResult {
                    id: clause.id.clone(),
                    histogram: AdministrativeHistogram::default(),
                })
                .collect();
            let mut seen = BTreeSet::new();
            for target in selected {
                let row = snapshot.work.get(&target.key).ok_or_else(|| {
                    unknown("native administrative target missing from initial image")
                })?;
                if !seen.insert(&target.key)
                    || target.clause >= clauses.len()
                    || intent.first_clause(&row.record) != Some(target.clause)
                {
                    return Err(unknown("native administrative target ownership mismatch"));
                }
            }
        }
    }
    let owned: BTreeSet<&str> = match &intent.operation {
        AdministrativeOperation::Addressed(entries) => {
            entries.iter().map(|entry| entry.key.as_str()).collect()
        }
        AdministrativeOperation::SelectFirst(_) => {
            selected.iter().map(|target| target.key.as_str()).collect()
        }
    };
    if ordinary
        .claimed_updates
        .iter()
        .map(|u| u.key.as_str())
        .chain(ordinary.work_updates.iter().map(|u| u.key.as_str()))
        .chain(ordinary.work_inserts.iter().map(|u| u.key.as_str()))
        .any(|key| owned.contains(key))
    {
        return Ok(rejected(Rejection::Invalid(
            "administrative target overlaps explicit group work".into(),
        )));
    }
    let targets: Box<dyn Iterator<Item = (TargetRequest<'_>, Option<usize>)> + '_> = match &intent
        .operation
    {
        AdministrativeOperation::Addressed(entries) => {
            Box::new(entries.iter().map(|entry| (entry.into(), None)))
        }
        AdministrativeOperation::SelectFirst(clauses) => Box::new(selected.iter().map(|target| {
            let clause = &clauses[target.clause];
            (
                TargetRequest {
                    key: &target.key,
                    expected_version: None,
                    precondition: &clause.precondition,
                    action: &clause.action,
                },
                Some(target.clause),
            )
        })),
    };
    let mut seen = BTreeSet::new();
    let mut deltas = Vec::new();
    let mut accepted_unique = BTreeMap::new();
    for (entry, clause) in targets {
        let target = snapshot.work.get(entry.key);
        let id = RecordIdentity {
            collection: None,
            key: entry.key.to_owned(),
        };
        let mut proposal = if seen.insert(entry.key) {
            propose_target_view(
                entry,
                target,
                snapshot.versions.get(&id).copied(),
                intent.evaluated_at,
                intent.before_fields.as_ref(),
                schema,
                limits,
            )?
        } else {
            let before = target.and_then(|target| {
                intent
                    .before_fields
                    .as_ref()
                    .map(|names| AdministrativeBefore {
                        version: target.record.version,
                        lifecycle: target.record.lifecycle,
                        available_at: target.record.work.available_at,
                        eligible_since: target.record.eligible_since,
                        attempts: target.record.attempts,
                        lease_expires_at: target.record.lease_expires_at,
                        fields: names
                            .iter()
                            .filter_map(|name| {
                                target
                                    .record
                                    .work
                                    .fields
                                    .get(name)
                                    .map(|value| (name.clone(), value.clone()))
                            })
                            .collect(),
                    })
            });
            AdministrativeProposal {
                outcome: AdministrativeOutcome::Invalid,
                before,
                replacement: None,
            }
        };
        if matches!(
            proposal.outcome,
            AdministrativeOutcome::Changed { .. } | AdministrativeOutcome::Deleted { .. }
        ) {
            let version = match proposal.outcome {
                AdministrativeOutcome::Changed { version }
                | AdministrativeOutcome::Deleted { version } => version,
                _ => unreachable!(),
            };
            let work_fields = proposal
                .replacement
                .as_ref()
                .map(|row| {
                    (
                        row.record.work.key.clone(),
                        crate::queue_fields::ResolvedFields::Replace(
                            row.record.work.fields.clone(),
                        ),
                    )
                })
                .into_iter()
                .collect();
            let work_bodies = proposal
                .replacement
                .as_ref()
                .map(|row| {
                    let change =
                        if target.is_some_and(|old| old.record.work.body != row.record.work.body) {
                            crate::queue_body::ResolvedBody::Replace(row.record.work.body.clone())
                        } else {
                            crate::queue_body::ResolvedBody::Preserve
                        };
                    (row.record.work.key.clone(), change)
                })
                .into_iter()
                .collect();
            let delta = CommitDelta {
                work_fields,
                work_bodies,
                work: proposal.replacement.take().into_iter().collect(),
                deleted_work: if matches!(proposal.outcome, AdministrativeOutcome::Deleted { .. }) {
                    vec![entry.key.to_owned()]
                } else {
                    vec![]
                },
                state: vec![],
                deleted_state: vec![],
                versions: vec![RecordVersion {
                    collection: None,
                    key: entry.key.to_owned(),
                    version,
                }],
                next_sequence: snapshot.next_sequence,
                receipt: CommitReceipt {
                    request_id: ordinary.request_id.clone(),
                    outcome: GroupOutcome::Committed { versions: vec![] },
                },
            };
            match validate_uniques(&delta, snapshot, schema)? {
                Err(Rejection::UniqueConflict) => {
                    proposal.outcome = AdministrativeOutcome::UniqueConflict
                }
                Err(_) => return Err(unknown("unexpected administrative unique rejection")),
                Ok(additions) => {
                    for (tuple, owner) in additions {
                        if accepted_unique
                            .insert(tuple, owner.clone())
                            .is_some_and(|other| other != owner)
                        {
                            return Ok(rejected(Rejection::UniqueConflict));
                        }
                    }
                    deltas.push(delta);
                }
            }
        }
        result.histogram.record(&proposal.outcome)?;
        if let Some(clause) = clause {
            result.clauses[clause].histogram.record(&proposal.outcome)?;
        } else {
            result.items.push(AdministrativeItemResult {
                key: entry.key.to_owned(),
                outcome: proposal.outcome,
                before: proposal.before,
            });
        }
    }
    // Explicit addressed records are disjoint, so their state/version predicates
    // still resolve against the initial image. Final uniqueness sees all effects.
    let mut candidate = addressed_snapshot(ordinary, snapshot);
    validate_group_snapshot(&candidate, ordinary)?;
    let mut normal = match apply_group(ordinary, &mut candidate, ownership_now, schema) {
        Ok(delta) => delta,
        Err(reason) => return Ok(rejected(reason)),
    };
    resolve_group_writes(ordinary, snapshot, &mut normal, schema)?;
    let ordinary_receipt = normal.receipt.clone();
    deltas.push(normal);
    let mut combined = combine_deltas(&deltas, snapshot.next_sequence)?;
    if let Err(reason) = validate_uniques(&combined, snapshot, schema)? {
        return Ok(rejected(reason));
    }
    combined.receipt = ordinary_receipt;
    Ok(AdministrativeGroupProposal {
        delta: combined,
        administration: Some(result),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    fn schema() -> QueueSchema {
        QueueSchema {
            work: CollectionDefinition {
                fields: BTreeMap::new(),
                indexes: BTreeMap::new(),
                unique_indexes: BTreeSet::new(),
                additional_field_prefixes: BTreeSet::from(["/gates/".into(), "/payload/".into()]),
            },
            state: BTreeMap::new(),
            progress_bound_nanos: 1_000_000_000,
            max_attempts: 3,
        }
    }
    fn row() -> StoredWork {
        StoredWork {
            record: WorkRecord {
                work: NewWork {
                    key: "a".into(),
                    priority: 0,
                    available_at: -1,
                    fields: BTreeMap::from([
                        ("/gates/a".into(), FieldValue::Boolean(true)),
                        ("/gates/b".into(), FieldValue::Boolean(true)),
                        ("/payload/null".into(), FieldValue::Bytes(vec![0])),
                    ]),
                    body: Default::default(),
                },
                version: 1,
                insertion_sequence: 1,
                lifecycle: Lifecycle::Pending,
                eligible_since: -1,
                attempts: 0,
                lease_expires_at: None,
            },
            lease_hash: None,
            field_coverage: crate::queue_fields::FieldCoverage::Complete,
            body_loaded: false,
        }
    }
    fn update() -> AdministrativeAction {
        AdministrativeAction::Update {
            patch: WorkPatch::default(),
            lifecycle: AdministrativeLifecycle::Keep,
            lease_guard: AdministrativeLeaseGuard::RejectActive,
            remove_field_prefixes: BTreeSet::new(),
        }
    }
    fn entry(action: AdministrativeAction) -> AdministrativeEntry {
        AdministrativeEntry {
            key: "a".into(),
            expected_version: None,
            precondition: WorkSelector::All(vec![]),
            action,
        }
    }
    fn proposal(entry: &AdministrativeEntry, row: &StoredWork, at: i64) -> AdministrativeProposal {
        propose_target(
            entry,
            Some(row),
            Some(row.record.version),
            at,
            None,
            &schema(),
            &QueueLimits::default(),
        )
        .unwrap()
    }
    #[test]
    fn raw_lifecycle_presence_and_exact_operator_time_are_distinct() {
        let mut record = row().record;
        record.lifecycle = Lifecycle::Leased;
        record.lease_expires_at = Some(10);
        assert!(WorkSelector::All(vec![]).matches(&record, 10));
        assert!(!WorkSelector::Any(vec![]).matches(&record, 10));
        assert!(WorkSelector::StoredLifecycleIn(vec![Lifecycle::Leased]).matches(&record, 10));
        assert!(WorkSelector::EffectiveLifecycleIn(vec![Lifecycle::Pending]).matches(&record, 10));
        assert!(WorkSelector::LeaseActive(true).matches(&record, 9));
        assert!(!WorkSelector::LeaseActive(true).matches(&record, 10));
        assert!(
            WorkSelector::All(vec![
                WorkSelector::Present {
                    name: "/payload/null".into(),
                    present: true
                },
                WorkSelector::Present {
                    name: "/payload/absent".into(),
                    present: false
                },
                WorkSelector::Field {
                    name: "/payload/null".into(),
                    predicate: Predicate::Equal(FieldValue::Bytes(vec![0]))
                },
                WorkSelector::AvailableAt {
                    comparison: Comparison::Equal,
                    value: -1
                },
                WorkSelector::Attempts {
                    comparison: Comparison::Equal,
                    value: 0
                }
            ])
            .matches(&record, 10)
        );
        assert!(
            !WorkSelector::Field {
                name: "/payload/null".into(),
                predicate: Predicate::Equal(FieldValue::Integer(0))
            }
            .matches(&record, 10)
        );
    }
    #[test]
    fn first_clause_keeps_ownership_before_failed_precondition_above_4096_targets() {
        let intent = AdministrativeIntent {
            evaluated_at: 0,
            before_fields: None,
            operation: AdministrativeOperation::SelectFirst(vec![
                AdministrativeClause {
                    id: "first".into(),
                    selector: WorkSelector::All(vec![]),
                    precondition: WorkSelector::Any(vec![]),
                    action: update(),
                },
                AdministrativeClause {
                    id: "second".into(),
                    selector: WorkSelector::All(vec![]),
                    precondition: WorkSelector::All(vec![]),
                    action: update(),
                },
            ]),
        };
        intent.validate(&schema(), &QueueLimits::default()).unwrap();
        let AdministrativeOperation::SelectFirst(clauses) = &intent.operation else {
            unreachable!()
        };
        let mut rejected = 0;
        for i in 0..5001 {
            let mut row = row();
            row.record.work.key = format!("row-{i}");
            let owned = intent.first_clause(&row.record).unwrap();
            assert_eq!(owned, 0);
            let entry = AdministrativeEntry {
                key: row.record.work.key.clone(),
                expected_version: None,
                precondition: clauses[owned].precondition.clone(),
                action: clauses[owned].action.clone(),
            };
            assert_eq!(
                proposal(&entry, &row, 0).outcome,
                AdministrativeOutcome::PreconditionFailed
            );
            rejected += 1;
        }
        assert_eq!(rejected, 5001);
    }
    #[test]
    fn prefix_removal_before_addition_and_compact_before_preserve_unloaded_body() {
        let row = row();
        let action = AdministrativeAction::Update {
            patch: WorkPatch {
                fields: BTreeMap::from([("/gates/a".into(), Some(FieldValue::Boolean(false)))]),
                ..WorkPatch::default()
            },
            lifecycle: AdministrativeLifecycle::Keep,
            lease_guard: AdministrativeLeaseGuard::RejectActive,
            remove_field_prefixes: BTreeSet::from(["/gates/".into()]),
        };
        let fields = BTreeSet::from(["/gates/a".into(), "/gates/absent".into()]);
        let result = propose_target(
            &entry(action),
            Some(&row),
            Some(1),
            0,
            Some(&fields),
            &schema(),
            &QueueLimits::default(),
        )
        .unwrap();
        assert_eq!(
            result.outcome,
            AdministrativeOutcome::Changed { version: 2 }
        );
        assert_eq!(
            result.before.unwrap().fields,
            BTreeMap::from([("/gates/a".into(), FieldValue::Boolean(true))])
        );
        let replacement = result.replacement.unwrap();
        assert_eq!(
            replacement.record.work.fields["/gates/a"],
            FieldValue::Boolean(false)
        );
        assert!(!replacement.record.work.fields.contains_key("/gates/b"));
        assert!(replacement.record.work.fields.contains_key("/payload/null"));
        assert!(!replacement.body_loaded);
        assert!(row.record.work.fields.contains_key("/gates/b"));
    }
    #[test]
    fn noop_conflicts_missing_and_materialization_fail_closed() {
        let mut row = row();
        assert_eq!(
            proposal(&entry(update()), &row, 0).outcome,
            AdministrativeOutcome::NoChange
        );
        let mut stale = entry(update());
        stale.expected_version = Some(0);
        assert_eq!(
            proposal(&stale, &row, 0).outcome,
            AdministrativeOutcome::Conflict { actual_version: 1 }
        );
        assert_eq!(
            propose_target(
                &stale,
                None,
                Some(4),
                0,
                None,
                &schema(),
                &QueueLimits::default()
            )
            .unwrap()
            .outcome,
            AdministrativeOutcome::NotFound
        );
        assert!(matches!(
            propose_target(
                &entry(update()),
                Some(&row),
                Some(2),
                0,
                None,
                &schema(),
                &QueueLimits::default()
            ),
            Err(QueueError::OutcomeUnknown(_))
        ));
        let body = entry(AdministrativeAction::Update {
            patch: WorkPatch {
                body: Some(Default::default()),
                ..WorkPatch::default()
            },
            lifecycle: AdministrativeLifecycle::Keep,
            lease_guard: AdministrativeLeaseGuard::RejectActive,
            remove_field_prefixes: BTreeSet::new(),
        });
        assert!(matches!(
            propose_target(
                &body,
                Some(&row),
                Some(1),
                0,
                None,
                &schema(),
                &QueueLimits::default()
            ),
            Err(QueueError::OutcomeUnknown(_))
        ));
        row.body_loaded = true;
        assert_eq!(
            proposal(&body, &row, 0).outcome,
            AdministrativeOutcome::NoChange
        );
        row.field_coverage = crate::queue_fields::FieldCoverage::Selected {
            names: Default::default(),
            prefixes: Default::default(),
        };
        assert!(matches!(
            propose_target(
                &entry(update()),
                Some(&row),
                Some(1),
                0,
                None,
                &schema(),
                &QueueLimits::default()
            ),
            Err(QueueError::OutcomeUnknown(_))
        ));
    }
    #[test]
    fn delete_guard_uses_operator_time_and_never_creates_claim_credentials() {
        let mut row = row();
        row.record.lifecycle = Lifecycle::Leased;
        row.record.lease_expires_at = Some(10);
        row.lease_hash = Some([1; 32]);
        let reject = entry(AdministrativeAction::Delete {
            lease_guard: AdministrativeLeaseGuard::RejectActive,
        });
        assert_eq!(
            proposal(&reject, &row, 9).outcome,
            AdministrativeOutcome::Leased
        );
        assert_eq!(
            proposal(&reject, &row, 10).outcome,
            AdministrativeOutcome::Deleted { version: 2 }
        );
        let invalidate = entry(AdministrativeAction::Delete {
            lease_guard: AdministrativeLeaseGuard::InvalidateActive,
        });
        assert_eq!(
            proposal(&invalidate, &row, 9).outcome,
            AdministrativeOutcome::Deleted { version: 2 }
        );
        let require = entry(AdministrativeAction::Delete {
            lease_guard: AdministrativeLeaseGuard::RequireActive,
        });
        assert_eq!(
            proposal(&require, &row, 10).outcome,
            AdministrativeOutcome::LeaseRequired
        );
        assert_eq!(row.record.version, 1);
    }
    #[test]
    fn selector_depth_validation_and_wide_traversal_are_iterative() {
        let mut selector = WorkSelector::All(vec![]);
        for _ in 1..MAX_SELECTOR_DEPTH {
            selector = WorkSelector::Not(Box::new(selector));
        }
        selector.validate(&schema().work).unwrap();
        let too_deep = WorkSelector::Not(Box::new(selector));
        assert!(matches!(
            too_deep.validate(&schema().work),
            Err(QueueError::Invalid(_))
        ));
        let wide = WorkSelector::All((0..10_000).map(|_| WorkSelector::All(vec![])).collect());
        wide.validate(&schema().work).unwrap();
        assert!(wide.matches(&row().record, 0));
        let duplicate = AdministrativeIntent {
            evaluated_at: 0,
            before_fields: Some(BTreeSet::new()),
            operation: AdministrativeOperation::Addressed(vec![entry(update()), entry(update())]),
        };
        duplicate
            .validate(&schema(), &QueueLimits::default())
            .unwrap();
    }
    #[test]
    fn addressed_admin_uses_common_count_limit_not_a_backend_only_1000_cap() {
        let entries = (0..1001)
            .map(|i| AdministrativeEntry {
                key: format!("row-{i}"),
                ..entry(update())
            })
            .collect();
        let intent = AdministrativeIntent {
            evaluated_at: 0,
            before_fields: None,
            operation: AdministrativeOperation::Addressed(entries),
        };
        intent.validate(&schema(), &QueueLimits::default()).unwrap();
        let limits = QueueLimits {
            max_records: 1000,
            ..QueueLimits::default()
        };
        assert!(matches!(
            intent.validate(&schema(), &limits),
            Err(QueueError::Invalid(_))
        ));
    }
    fn ordinary() -> CommitGroup {
        CommitGroup {
            request_id: "atomic-admin".into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![],
        }
    }
    fn source() -> crate::queue_planner::RecordSnapshot {
        let mut source = crate::queue_planner::RecordSnapshot {
            next_sequence: 1,
            ..Default::default()
        };
        source.work.insert("a".into(), row());
        source.versions.insert(
            crate::queue_planner::RecordIdentity {
                collection: None,
                key: "a".into(),
            },
            1,
        );
        source
    }
    fn addressed(entries: Vec<AdministrativeEntry>) -> AdministrativeIntent {
        AdministrativeIntent {
            evaluated_at: 0,
            operation: AdministrativeOperation::Addressed(entries),
            before_fields: Some(BTreeSet::new()),
        }
    }
    fn change_priority() -> AdministrativeEntry {
        entry(AdministrativeAction::Update {
            patch: WorkPatch {
                priority: Some(7),
                ..WorkPatch::default()
            },
            lifecycle: AdministrativeLifecycle::Keep,
            lease_guard: AdministrativeLeaseGuard::RejectActive,
            remove_field_prefixes: BTreeSet::new(),
        })
    }
    fn assert_outer_rejected(plan: &AdministrativeGroupProposal) {
        assert!(matches!(
            plan.delta.receipt.outcome,
            GroupOutcome::Rejected(_)
        ));
        assert!(plan.administration.is_none());
        assert!(
            plan.delta.work.is_empty()
                && plan.delta.deleted_work.is_empty()
                && plan.delta.state.is_empty()
                && plan.delta.versions.is_empty()
        );
    }
    #[test]
    fn complete_group_admin_state_and_new_work_are_speculative_and_atomic() {
        let mut schema = schema();
        schema.state.insert(
            "instances".into(),
            CollectionDefinition {
                fields: BTreeMap::new(),
                indexes: BTreeMap::new(),
                unique_indexes: BTreeSet::new(),
                additional_field_prefixes: BTreeSet::new(),
            },
        );
        let source = source();
        let original = source.work.clone();
        let mut group = ordinary();
        group.state_updates.push(StateUpdate {
            collection: "instances".into(),
            key: "run".into(),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: BTreeMap::new(),
                value: vec![9].into(),
            },
        });
        let mut next = row().record.work;
        next.key = "next".into();
        group.work_inserts.push(next);
        let plan = propose_group(
            &group,
            &addressed(vec![change_priority()]),
            &[],
            &source,
            100,
            &schema,
            &QueueLimits::default(),
        )
        .unwrap();
        assert_eq!(
            plan.administration.as_ref().unwrap().items[0].outcome,
            AdministrativeOutcome::Changed { version: 2 }
        );
        assert_eq!(plan.delta.work.len(), 2);
        assert_eq!(plan.delta.state[0].value.as_ref(), &[9]);
        assert_eq!(plan.delta.next_sequence, 2);
        assert_eq!(source.work, original);
        group.state_updates[0].expect = StateExpect::Version(7);
        let rejected = propose_group(
            &group,
            &addressed(vec![change_priority()]),
            &[],
            &source,
            100,
            &schema,
            &QueueLimits::default(),
        )
        .unwrap();
        assert_outer_rejected(&rejected);
        assert_eq!(source.work, original);
    }
    #[test]
    fn addressed_duplicates_missing_and_initial_before_are_positional() {
        let source = source();
        let mut missing = change_priority();
        missing.key = "missing".into();
        let intent = addressed(vec![
            change_priority(),
            change_priority(),
            missing.clone(),
            missing,
        ]);
        let plan = propose_group(
            &ordinary(),
            &intent,
            &[],
            &source,
            0,
            &schema(),
            &QueueLimits::default(),
        )
        .unwrap();
        let result = plan.administration.unwrap();
        assert_eq!(result.histogram.matched, 4);
        assert_eq!(
            result
                .items
                .iter()
                .map(|item| item.outcome.clone())
                .collect::<Vec<_>>(),
            vec![
                AdministrativeOutcome::Changed { version: 2 },
                AdministrativeOutcome::Invalid,
                AdministrativeOutcome::NotFound,
                AdministrativeOutcome::Invalid
            ]
        );
        assert_eq!(result.items[1].before.as_ref().unwrap().version, 1);
        assert!(result.items[3].before.is_none());
        assert_eq!(plan.delta.work.len(), 1);
    }
    #[test]
    fn missing_and_skipped_targets_own_overlap_and_zero_match_is_valid() {
        let source = source();
        let mut group = ordinary();
        let mut inserted = row().record.work;
        inserted.key = "missing".into();
        group.work_inserts.push(inserted);
        let mut missing = change_priority();
        missing.key = "missing".into();
        assert_outer_rejected(
            &propose_group(
                &group,
                &addressed(vec![missing]),
                &[],
                &source,
                0,
                &schema(),
                &QueueLimits::default(),
            )
            .unwrap(),
        );
        let mut skipped = change_priority();
        skipped.precondition = WorkSelector::Any(vec![]);
        let mut group = ordinary();
        group.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch::default()),
        });
        assert_outer_rejected(
            &propose_group(
                &group,
                &addressed(vec![skipped]),
                &[],
                &source,
                0,
                &schema(),
                &QueueLimits::default(),
            )
            .unwrap(),
        );
        let empty = AdministrativeIntent {
            evaluated_at: 0,
            before_fields: None,
            operation: AdministrativeOperation::SelectFirst(vec![]),
        };
        let plan = propose_group(
            &ordinary(),
            &empty,
            &[],
            &source,
            0,
            &schema(),
            &QueueLimits::default(),
        )
        .unwrap();
        assert_eq!(plan.administration.unwrap().histogram.matched, 0);
        assert!(plan.delta.work.is_empty());
        assert!(matches!(
            plan.delta.receipt.outcome,
            GroupOutcome::Committed { .. }
        ));
    }
    #[test]
    fn native_first_ownership_corruption_fails_closed() {
        let source = source();
        let clause = |id: &str| AdministrativeClause {
            id: id.into(),
            selector: WorkSelector::All(vec![]),
            precondition: WorkSelector::Any(vec![]),
            action: update(),
        };
        let intent = AdministrativeIntent {
            evaluated_at: 0,
            before_fields: None,
            operation: AdministrativeOperation::SelectFirst(vec![
                clause("first"),
                clause("second"),
            ]),
        };
        for selected in [
            vec![SelectedAdministrativeTarget {
                key: "missing".into(),
                clause: 0,
            }],
            vec![SelectedAdministrativeTarget {
                key: "a".into(),
                clause: 1,
            }],
            vec![SelectedAdministrativeTarget {
                key: "a".into(),
                clause: 99,
            }],
            vec![
                SelectedAdministrativeTarget {
                    key: "a".into(),
                    clause: 0
                };
                2
            ],
        ] {
            assert!(matches!(
                propose_group(
                    &ordinary(),
                    &intent,
                    &selected,
                    &source,
                    0,
                    &schema(),
                    &QueueLimits::default()
                ),
                Err(QueueError::OutcomeUnknown(_))
            ));
        }
        let plan = propose_group(
            &ordinary(),
            &intent,
            &[SelectedAdministrativeTarget {
                key: "a".into(),
                clause: 0,
            }],
            &source,
            0,
            &schema(),
            &QueueLimits::default(),
        )
        .unwrap();
        let result = plan.administration.unwrap();
        assert!(result.items.is_empty());
        assert_eq!(result.clauses[0].histogram.matched, 1);
        assert_eq!(result.clauses[1].histogram.matched, 0);
        assert_eq!(
            result.histogram.outcomes[&AdministrativeOutcomeKind::PreconditionFailed],
            1
        );
    }
    fn unique_source() -> (QueueSchema, crate::queue_planner::RecordSnapshot) {
        use crate::queue_planner::*;
        let mut schema = schema();
        schema.work.fields.insert("u".into(), FieldType::Text);
        schema.work.indexes.insert("by_u".into(), vec!["u".into()]);
        schema.work.unique_indexes.insert("by_u".into());
        let mut snapshot = source();
        snapshot.next_sequence = 2;
        snapshot
            .work
            .get_mut("a")
            .unwrap()
            .record
            .work
            .fields
            .insert("u".into(), FieldValue::Text("x".into()));
        let mut b = row();
        b.record.work.key = "b".into();
        b.record.insertion_sequence = 2;
        b.record
            .work
            .fields
            .insert("u".into(), FieldValue::Text("y".into()));
        snapshot.work.insert("b".into(), b);
        snapshot.versions.insert(
            RecordIdentity {
                collection: None,
                key: "b".into(),
            },
            1,
        );
        for key in ["x", "y", "z"] {
            for tuple in record_unique_keys(
                None,
                &BTreeMap::from([("u".into(), FieldValue::Text(key.into()))]),
                &schema.work,
            )
            .unwrap()
            {
                snapshot.unique_probes_loaded.insert(tuple);
            }
        }
        for (key, row) in &snapshot.work {
            for tuple in record_unique_keys(None, &row.record.work.fields, &schema.work).unwrap() {
                snapshot.unique_owners.insert(
                    tuple,
                    RecordIdentity {
                        collection: None,
                        key: key.clone(),
                    },
                );
            }
        }
        (schema, snapshot)
    }
    fn rekey(key: &str, value: &str) -> AdministrativeEntry {
        AdministrativeEntry {
            key: key.into(),
            action: AdministrativeAction::Update {
                patch: WorkPatch {
                    fields: BTreeMap::from([("u".into(), Some(FieldValue::Text(value.into())))]),
                    ..WorkPatch::default()
                },
                lifecycle: AdministrativeLifecycle::Keep,
                lease_guard: AdministrativeLeaseGuard::RejectActive,
                remove_field_prefixes: BTreeSet::new(),
            },
            ..entry(update())
        }
    }
    #[test]
    fn administration_unique_checks_initial_owners_and_rejects_free_tuple_sibling_collision() {
        let (schema, source) = unique_source();
        let original = source.work.clone();
        let plan = propose_group(
            &ordinary(),
            &addressed(vec![rekey("a", "z"), rekey("b", "x")]),
            &[],
            &source,
            0,
            &schema,
            &QueueLimits::default(),
        )
        .unwrap();
        assert_eq!(
            plan.administration.unwrap().items[1].outcome,
            AdministrativeOutcome::UniqueConflict
        );
        assert_eq!(plan.delta.work.len(), 1);
        let collision = propose_group(
            &ordinary(),
            &addressed(vec![rekey("a", "z"), rekey("b", "z")]),
            &[],
            &source,
            0,
            &schema,
            &QueueLimits::default(),
        )
        .unwrap();
        assert_outer_rejected(&collision);
        assert_eq!(
            collision.delta.receipt.outcome,
            GroupOutcome::Rejected(Rejection::UniqueConflict)
        );
        assert_eq!(source.work, original);
    }
    #[test]
    fn final_uniqueness_includes_disjoint_ordinary_work_without_admin_first_wins() {
        let (schema, source) = unique_source();
        let mut group = ordinary();
        group.work_updates.push(UnclaimedUpdate {
            key: "b".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                fields: BTreeMap::from([("u".into(), Some(FieldValue::Text("x".into())))]),
                ..WorkPatch::default()
            }),
        });
        let plan = propose_group(
            &group,
            &addressed(vec![rekey("a", "z")]),
            &[],
            &source,
            0,
            &schema,
            &QueueLimits::default(),
        )
        .unwrap();
        assert!(plan.administration.is_some());
        assert_eq!(plan.delta.work.len(), 2);
        let UnclaimedChange::Patch(patch) = &mut group.work_updates[0].change else {
            unreachable!()
        };
        patch
            .fields
            .insert("u".into(), Some(FieldValue::Text("z".into())));
        assert_outer_rejected(
            &propose_group(
                &group,
                &addressed(vec![rekey("a", "z")]),
                &[],
                &source,
                0,
                &schema,
                &QueueLimits::default(),
            )
            .unwrap(),
        );
    }
    #[test]
    fn historical_admin_time_cannot_make_an_expired_claim_valid() {
        let (schema, mut source) = unique_source();
        let token = b"held-token";
        let b = source.work.get_mut("b").unwrap();
        b.record.lifecycle = Lifecycle::Leased;
        b.record.lease_expires_at = Some(10);
        b.lease_hash = Some(crate::queue_planner::lease_hash(token));
        let mut group = ordinary();
        group.claimed_updates.push(ClaimedUpdate {
            key: "b".into(),
            lease: LeaseCredential {
                token: token.to_vec(),
            },
            expected_version: 1,
            patch: WorkPatch::default(),
            disposition: Disposition::Complete,
        });
        let intent = addressed(vec![change_priority()]);
        let live = propose_group(
            &group,
            &intent,
            &[],
            &source,
            9,
            &schema,
            &QueueLimits::default(),
        )
        .unwrap();
        assert!(live.administration.is_some());
        let expired = propose_group(
            &group,
            &intent,
            &[],
            &source,
            10,
            &schema,
            &QueueLimits::default(),
        )
        .unwrap();
        assert_outer_rejected(&expired);
        assert_eq!(
            expired.delta.receipt.outcome,
            GroupOutcome::Rejected(Rejection::StaleLease)
        );
    }
    #[test]
    fn complete_pure_selector_planner_accepts_5001_noop_targets_with_compact_results() {
        use crate::queue_planner::*;
        let mut source = RecordSnapshot {
            next_sequence: 5001,
            ..Default::default()
        };
        let mut selected = Vec::new();
        for i in 0..5001 {
            let key = format!("row-{i}");
            let mut row = row();
            row.record.work.key = key.clone();
            row.record.insertion_sequence = i + 1;
            source.work.insert(key.clone(), row);
            source.versions.insert(
                RecordIdentity {
                    collection: None,
                    key: key.clone(),
                },
                1,
            );
            selected.push(SelectedAdministrativeTarget { key, clause: 0 });
        }
        let intent = AdministrativeIntent {
            evaluated_at: 0,
            before_fields: None,
            operation: AdministrativeOperation::SelectFirst(vec![AdministrativeClause {
                id: "all".into(),
                selector: WorkSelector::All(vec![]),
                precondition: WorkSelector::All(vec![]),
                action: update(),
            }]),
        };
        let plan = propose_group(
            &ordinary(),
            &intent,
            &selected,
            &source,
            0,
            &schema(),
            &QueueLimits::default(),
        )
        .unwrap();
        let result = plan.administration.unwrap();
        assert_eq!(result.histogram.matched, 5001);
        assert_eq!(
            result.histogram.outcomes[&AdministrativeOutcomeKind::NoChange],
            5001
        );
        assert!(result.items.is_empty());
        assert_eq!(result.clauses.len(), 1);
        assert!(plan.delta.work.is_empty() && plan.delta.versions.is_empty());
    }
}
