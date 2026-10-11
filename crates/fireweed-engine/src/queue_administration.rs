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
    if !target.fields_loaded
        || target.record.work.key != entry.key
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
            fields_loaded: true,
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
        row.fields_loaded = false;
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
}
