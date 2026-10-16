use super::*;
use fireweed_core::queue_administration::{
    AdministrativeAction, AdministrativeClause, WorkSelector,
};
fn intent(field: &str) -> AdministrativeIntent {
    let present = || {
        WorkSelector::leaf(SelectorNode::Present {
            name: field.into(),
            present: true,
        })
    };
    AdministrativeIntent {
        evaluated_at: 0,
        before_fields: None,
        operation: AdministrativeOperation::SelectFirst(
            [
                (
                    "two-identical",
                    WorkSelector::all(vec![present(), present()]),
                ),
                ("same-in-another-clause", present()),
                (
                    "missing",
                    WorkSelector::leaf(SelectorNode::Present {
                        name: field.into(),
                        present: false,
                    }),
                ),
            ]
            .into_iter()
            .map(|(id, selector)| AdministrativeClause {
                id: id.into(),
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
macro_rules! start_presence {
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
#[test]
fn presence_covers_every_permitted_type_and_the_actual_full_field_name() {
    futures::executor::block_on(async {
        for kind in [
            FieldType::Integer,
            FieldType::Float,
            FieldType::Boolean,
            FieldType::Text,
            FieldType::Bytes,
        ] {
            for field in ["n", "meta/any"] {
                let mut f = fixture_contents(Signals::default(), false, kind, false, true);
                let intent = intent(field);
                let (producer, leaf) = start_presence!(f, intent).presence(0, 0).await.unwrap();
                assert_eq!(producer.presence_len(&leaf, 0, 0).unwrap(), 3);
                assert_eq!(
                    leaf.set.0.rows,
                    BTreeSet::from([(None, "a".into()), (None, "c".into()), (None, "d".into())])
                );
                assert_eq!(
                    f.signals.finishes.load(Ordering::SeqCst),
                    if field == "n" { 1 } else { 5 }
                );
                let before = f.budget.remaining(&f.job).unwrap();
                assert!(producer.validate_presence(&leaf, 1, 0).is_err());
                assert!(producer.validate_presence(&leaf, 0, 1).is_err());
                assert_eq!(f.budget.remaining(&f.job).unwrap(), before);
                let (producer, missing_node_presence) = producer.presence(2, 0).await.unwrap();
                assert_eq!(
                    producer.presence_len(&missing_node_presence, 2, 0).unwrap(),
                    3
                );
                assert_eq!(missing_node_presence.set.0.rows, leaf.set.0.rows);
                drop(missing_node_presence);
                drop(leaf);
                drop(producer);
                assert!(f.reader_alive.upgrade().is_none());
                assert!(f.scratch_alive.upgrade().is_none());
            }
        }
    });
}
#[test]
fn presence_requires_every_typed_range_proof_even_when_that_type_has_no_rows() {
    futures::executor::block_on(async {
        let mut f = fixture_contents(Signals::default(), false, FieldType::Integer, false, true);
        let intent = intent("meta/any");
        let (producer, mut leaf) = start_presence!(f, intent).presence(0, 0).await.unwrap();
        assert_eq!(leaf.completions.len(), 5);
        let before = f.budget.remaining(&f.job).unwrap();
        let final_empty_type = leaf.completions.pop().unwrap();
        assert!(producer.validate_presence(&leaf, 0, 0).is_err());
        assert_eq!(f.budget.remaining(&f.job).unwrap(), before);
        leaf.completions.push(final_empty_type);
        leaf.completions.swap(0, 4);
        assert!(producer.validate_presence(&leaf, 0, 0).is_err());
        leaf.completions.swap(0, 4);
        assert_eq!(producer.presence_len(&leaf, 0, 0).unwrap(), 3);
    });
}
#[test]
fn late_presence_write_failure_cannot_turn_completed_earlier_types_into_a_leaf() {
    futures::executor::block_on(async {
        let mut f = fixture_contents(
            Signals {
                fail_write: 3,
                ..Default::default()
            },
            false,
            FieldType::Integer,
            false,
            true,
        );
        let intent = intent("meta/any");
        let producer = start_presence!(f, intent);
        assert!(producer.presence(0, 0).await.is_err());
        assert_eq!(f.signals.writes.load(Ordering::SeqCst), 3);
        assert_eq!(f.signals.finishes.load(Ordering::SeqCst), 3);
        assert!(f.reader_alive.upgrade().is_none());
        assert!(f.scratch_alive.upgrade().is_none());
    });
}
