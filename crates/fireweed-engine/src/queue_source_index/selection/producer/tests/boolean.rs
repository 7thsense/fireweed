use super::*;
use fireweed_core::queue_administration::{
    AdministrativeAction, AdministrativeClause, WorkSelector,
};
pub(super) fn intent(selectors: Vec<WorkSelector>) -> AdministrativeIntent {
    AdministrativeIntent {
        evaluated_at: 0,
        before_fields: None,
        operation: AdministrativeOperation::SelectFirst(
            selectors
                .into_iter()
                .enumerate()
                .map(|(index, selector)| AdministrativeClause {
                    id: format!("clause-{index}"),
                    selector,
                    precondition: WorkSelector::all(vec![]),
                    action: AdministrativeAction::Delete {
                        lease_guard: AdministrativeLeaseGuard::RejectActive,
                    },
                })
                .collect(),
        ),
    }
}
macro_rules! start_boolean {
    ($f:ident, $intent:ident) => {
        QueueSourceCandidateProducer::begin(
            $f.reader.take().unwrap(),
            $f.scratch.take().unwrap(),
            &$f.definition,
            QueueSourceSelectionRequest::Administration {
                queue: &$f.queue,
                intent: &$intent,
            },
            0,
            &QueueLimits::default(),
            $f.job.clone(),
            $f.budget.clone(),
        )
        .await
        .unwrap()
    };
}
fn eq(field: &str, n: i64) -> WorkSelector {
    WorkSelector::leaf(SelectorNode::Field {
        name: field.into(),
        predicate: Predicate::Equal(FieldValue::Integer(n)),
    })
}
fn present(field: &str, value: bool) -> WorkSelector {
    WorkSelector::leaf(SelectorNode::Present {
        name: field.into(),
        present: value,
    })
}
pub(super) fn rows<'a>(
    proof: &'a QueueSourceBooleanClause<'_, Completion, Set>,
) -> &'a BTreeSet<(Option<String>, String)> {
    let set = match &proof.result {
        CandidateOperand::Value(index) => &proof.values[*index].set,
        CandidateOperand::Presence(index) => &proof.presence[*index].set,
        CandidateOperand::Keys(index) => &proof.keys[*index].set,
        CandidateOperand::Universe => &proof.universe.as_ref().unwrap().set,
        CandidateOperand::Empty => proof.empty.as_ref().unwrap(),
        CandidateOperand::Owned(set) => set,
    };
    &set.0.rows
}
#[test]
fn actual_boolean_programs_distinguish_missing_nonmatching_and_syntactic_empty() {
    futures::executor::block_on(async {
        for (selector, expected) in [
            (WorkSelector::all(vec![]), vec!["a", "b", "c", "d"]),
            (WorkSelector::any(vec![]), vec![]),
            (WorkSelector::negate(eq("n", 1)), vec!["b", "c"]),
            (present("n", false), vec!["b"]),
            (WorkSelector::negate(present("n", true)), vec!["b"]),
            (
                WorkSelector::all(vec![present("n", true), WorkSelector::negate(eq("n", 1))]),
                vec!["c"],
            ),
            (
                WorkSelector::any(vec![present("n", false), eq("n", 1)]),
                vec!["a", "b", "d"],
            ),
            (WorkSelector::negate(eq("meta/any", 1)), vec!["b", "c", "d"]),
            (present("meta/any", false), vec!["b"]),
        ] {
            let mut f =
                fixture_contents(Signals::default(), false, FieldType::Integer, false, true);
            let intent = intent(vec![selector]);
            let (producer, proof) = start_boolean!(f, intent).boolean_clause(0).await.unwrap();
            let expected = expected
                .into_iter()
                .map(|key| (None, key.to_owned()))
                .collect::<BTreeSet<_>>();
            assert_eq!(rows(&proof), &expected);
            assert_eq!(
                producer.boolean_clause_len(&proof, 0).unwrap(),
                expected.len() as u64
            );
            drop(proof);
            drop(producer);
            assert!(f.reader_alive.upgrade().is_none());
            assert!(f.scratch_alive.upgrade().is_none());
        }
    });
}
#[test]
fn boolean_operands_are_bound_to_actual_occurrences_and_all_manifests_are_retained() {
    futures::executor::block_on(async {
        let mut f = fixture_contents(Signals::default(), false, FieldType::Integer, false, true);
        // Two Field occurrences, a Presence occurrence, and operators make the
        // second clause's Field offset differ from both its clause index and
        // the preceding program's total node count.
        let intent = intent(vec![
            WorkSelector::all(vec![
                eq("n", 1),
                WorkSelector::any(vec![eq("n", 1), present("n", true)]),
            ]),
            eq("n", 2),
        ]);
        let (producer, mut first) = start_boolean!(f, intent).boolean_clause(0).await.unwrap();
        let (producer, mut second) = producer.boolean_clause(1).await.unwrap();
        assert_eq!(
            rows(&first),
            &BTreeSet::from([(None, "a".into()), (None, "d".into())])
        );
        assert_eq!(rows(&second), &BTreeSet::from([(None, "c".into())]));
        let before = f.budget.remaining(&f.job).unwrap();
        std::mem::swap(&mut first.values[0], &mut second.values[0]);
        assert!(producer.validate_boolean_clause(&first, 0).is_err());
        assert!(producer.validate_boolean_clause(&second, 1).is_err());
        assert_eq!(f.budget.remaining(&f.job).unwrap(), before);
        std::mem::swap(&mut first.values[0], &mut second.values[0]);
        assert_eq!(producer.boolean_clause_len(&first, 0).unwrap(), 2);
        assert!(producer.validate_boolean_clause(&first, 1).is_err());
    });
}
#[test]
fn unsupported_nodes_fail_whole_program_preflight_even_in_decisive_branches() {
    futures::executor::block_on(async {
        for selector in [
            WorkSelector::all(vec![
                WorkSelector::any(vec![]),
                WorkSelector::leaf(SelectorNode::Attempts {
                    comparison: fireweed_core::queue_administration::Comparison::Equal,
                    value: 1,
                }),
            ]),
            WorkSelector::any(vec![
                WorkSelector::all(vec![]),
                WorkSelector::leaf(SelectorNode::LeaseActive(false)),
            ]),
        ] {
            let mut f =
                fixture_contents(Signals::default(), false, FieldType::Integer, false, true);
            let intent = intent(vec![selector]);
            let producer = start_boolean!(f, intent);
            let before = f.budget.remaining(&f.job).unwrap();
            assert!(
                matches!(producer.boolean_clause(0).await, Err(QueueError::OutcomeUnknown(reason)) if reason == "source candidate producer: Boolean clause contains an unsupported leaf")
            );
            assert_eq!(f.signals.opens.load(Ordering::SeqCst), 0);
            assert_eq!(f.signals.writes.load(Ordering::SeqCst), 0);
            assert_eq!(f.signals.finishes.load(Ordering::SeqCst), 0);
            assert_eq!(f.budget.remaining(&f.job).unwrap(), before);
            assert!(f.reader_alive.upgrade().is_none());
            assert!(f.scratch_alive.upgrade().is_none());
        }
    });
}
#[test]
fn decisive_empty_result_cannot_hide_an_unresolved_required_leaf() {
    futures::executor::block_on(async {
        let mut f = fixture_contents(
            Signals {
                foreign_completion: true,
                ..Default::default()
            },
            false,
            FieldType::Integer,
            false,
            true,
        );
        let intent = intent(vec![WorkSelector::all(vec![
            WorkSelector::any(vec![]),
            eq("n", 1),
        ])]);
        assert!(start_boolean!(f, intent).boolean_clause(0).await.is_err());
        assert_eq!(f.signals.writes.load(Ordering::SeqCst), 2);
        assert_eq!(f.signals.finishes.load(Ordering::SeqCst), 1);
        assert!(f.reader_alive.upgrade().is_none());
        assert!(f.scratch_alive.upgrade().is_none());
    });
}

#[test]
fn identical_empty_programs_still_bind_the_outer_clause() {
    futures::executor::block_on(async {
        let mut f = fixture_contents(Signals::default(), false, FieldType::Integer, false, true);
        let intent = intent(vec![WorkSelector::any(vec![]), WorkSelector::any(vec![])]);
        let (producer, first) = start_boolean!(f, intent).boolean_clause(0).await.unwrap();
        let (producer, second) = producer.boolean_clause(1).await.unwrap();
        let before = f.budget.remaining(&f.job).unwrap();
        assert_eq!(producer.boolean_clause_len(&first, 0).unwrap(), 0);
        assert_eq!(producer.boolean_clause_len(&second, 1).unwrap(), 0);
        assert!(producer.validate_boolean_clause(&first, 1).is_err());
        assert!(producer.validate_boolean_clause(&second, 0).is_err());
        assert_eq!(f.budget.remaining(&f.job).unwrap(), before);
    });
}

#[test]
fn administrative_time_is_bound_before_any_source_or_selection_work() {
    futures::executor::block_on(async {
        let mut f = fixture_contents(Signals::default(), false, FieldType::Integer, false, true);
        let mut intent = intent(vec![eq("n", 1)]);
        intent.evaluated_at = 1;
        let before = f.budget.remaining(&f.job).unwrap();
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
        assert!(matches!(result, Err(QueueError::OutcomeUnknown(reason))
            if reason == "source candidate producer: evaluation time differs from actual administrative intent"));
        assert_eq!(f.budget.remaining(&f.job).unwrap(), before);
        assert_eq!(f.signals.lookups.load(Ordering::SeqCst), 0);
        assert_eq!(f.signals.opens.load(Ordering::SeqCst), 0);
        assert_eq!(f.signals.writes.load(Ordering::SeqCst), 0);
        assert!(f.reader_alive.upgrade().is_none());
        assert!(f.scratch_alive.upgrade().is_none());
    });
}
