use super::*;
use crate::{
    queue_admission::QueueAdmission,
    queue_log_record::encode_checked_genesis,
    queue_schema_codec::{AdmittedQueueDefinition, encode_definition},
};
fn queue() -> QueueAddress {
    QueueAddress {
        tenant: "work-source".into(),
        queue: "proof".into(),
    }
}
fn group(id: &str, key: &str) -> CommitGroup {
    CommitGroup {
        request_id: id.into(),
        administration: None,
        claimed_updates: vec![],
        state_updates: vec![],
        work_inserts: vec![],
        work_updates: vec![UnclaimedUpdate {
            key: key.into(),
            expect: StateExpect::Absent,
            change: UnclaimedChange::Patch(WorkPatch {
                fields: [("meta/value".into(), Some(FieldValue::Text("new".into())))].into(),
                ..Default::default()
            }),
        }],
    }
}
fn schema() -> QueueSchema {
    QueueSchema {
        work: CollectionDefinition {
            fields: Default::default(),
            indexes: Default::default(),
            unique_indexes: Default::default(),
            additional_field_prefixes: ["meta/".into()].into(),
        },
        state: Default::default(),
        progress_bound_nanos: 1,
        max_attempts: 3,
    }
}
#[test]
fn initialization_errors_are_sticky_and_never_seal_a_repaired_tracker() {
    for case in ["wrong-job", "wrong-history", "double-init"] {
        let admission = QueueAdmission::new(2, 8 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let foreign = Arc::new(admission.reserve(0).unwrap());
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema()).unwrap(), &job)
                .unwrap();
        let genesis =
            encode_checked_genesis(1, &queue(), &definition, &[31; 32], 1 << 20, &job).unwrap();
        let checked = genesis.checked().unwrap();
        let batch = Arc::new(CommitBatch {
            queue: queue(),
            groups: vec![group("one", "a")],
        });
        let mut tracker =
            ColdGroupStateTracker::with_work(&queue(), GroupDeclarations::Batch(batch), &job)
                .unwrap();
        let mut history = ColdQueueHistory::new(&queue(), &job).unwrap();
        if case != "wrong-history" {
            history.observe_record(0, &checked).unwrap();
        }
        let failure = match case {
            "wrong-job" => tracker.initialize_work(&history, &checked, &foreign),
            "double-init" => {
                tracker.initialize_work(&history, &checked, &job).unwrap();
                tracker.initialize_work(&history, &checked, &job)
            }
            _ => tracker.initialize_work(&history, &checked, &job),
        };
        assert!(
            matches!(failure, Err(QueueError::OutcomeUnknown(_))),
            "{case}"
        );
        if case == "wrong-history" {
            history.observe_record(0, &checked).unwrap();
        }
        assert!(
            tracker.initialize_work(&history, &checked, &job).is_err(),
            "{case} cannot repair initialization"
        );
        assert!(
            tracker.observe_record(0, &checked).is_err(),
            "{case} cannot observe"
        );
        assert!(
            tracker.finish(&history, &genesis, (1, 0, 1)).is_err(),
            "{case} cannot seal"
        );
        drop(history);
        drop(genesis);
        drop(definition);
        drop(foreign);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
}
#[test]
fn initialized_empty_source_checks_group_key_membership_and_continuation_owner() {
    let admission = QueueAdmission::new(2, 8 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let definition =
        AdmittedQueueDefinition::from_raw(&encode_definition(&schema()).unwrap(), &job).unwrap();
    let genesis =
        encode_checked_genesis(1, &queue(), &definition, &[31; 32], 1 << 20, &job).unwrap();
    let checked = genesis.checked().unwrap();
    let mut sibling = group("two", "a");
    sibling.work_updates[0].change = UnclaimedChange::Patch(WorkPatch {
        fields: [("meta/other".into(), None)].into(),
        ..Default::default()
    });
    let batch = Arc::new(CommitBatch {
        queue: queue(),
        groups: vec![group("one", "a"), sibling, group("three", "b")],
    });
    let mut tracker =
        ColdGroupStateTracker::with_work(&queue(), GroupDeclarations::Batch(batch.clone()), &job)
            .unwrap();
    let mut history = ColdQueueHistory::new(&queue(), &job).unwrap();
    history.observe_record(0, &checked).unwrap();
    tracker.initialize_work(&history, &checked, &job).unwrap();
    tracker.observe_record(0, &checked).unwrap();
    let proof = Arc::new(tracker.finish(&history, &genesis, (1, 0, 1)).unwrap());
    proof
        .validate_batch(&batch, &genesis, (1, 0, 1), &job)
        .unwrap();
    let first = proof.group(0).unwrap();
    let descriptor = first.work_descriptor("a").unwrap();
    assert_eq!(descriptor.highwater, 0);
    assert!(descriptor.metadata.is_none());
    assert_eq!(descriptor.fields.get("meta/value"), Some(&None));
    assert!(first.work_descriptor("b").is_err());
    assert_eq!(descriptor.fields.get("meta/other"), Some(&None));
    assert_eq!(
        proof.group(1).unwrap().work_descriptor("a").unwrap(),
        descriptor
    );
    assert!(proof.group(2).unwrap().work_descriptor("a").is_err());
    assert!(
        proof
            .validate_batch(&batch, &genesis, (2, 0, 1), &job)
            .is_err()
    );
    let foreign = Arc::new(admission.reserve(0).unwrap());
    assert!(ColdGroupStateTracker::resume(&proof, &batch, &genesis, (1, 0, 1), &foreign).is_err());
    let resumed = ColdGroupStateTracker::resume(&proof, &batch, &genesis, (1, 0, 1), &job).unwrap();
    let continued = Arc::new(resumed.finish(&history, &genesis, (1, 0, 1)).unwrap());
    assert_eq!(
        continued.group(0).unwrap().work_descriptor("a").unwrap(),
        descriptor
    );
    drop(first);
    drop(continued);
    drop(proof);
    drop(history);
    drop(batch);
    drop(genesis);
    drop(definition);
    drop(foreign);
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

#[test]
fn work_initialization_rejects_foreign_address_and_replaced_genesis_token() {
    for foreign_address in [false, true] {
        let admission = QueueAdmission::new(1, 8 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema()).unwrap(), &job)
                .unwrap();
        let genesis =
            encode_checked_genesis(1, &queue(), &definition, &[31; 32], 1 << 20, &job).unwrap();
        let mut alternate_queue = queue();
        if foreign_address {
            alternate_queue.queue = "foreign".into();
        }
        let alternate =
            encode_checked_genesis(1, &alternate_queue, &definition, &[32; 32], 1 << 20, &job)
                .unwrap();
        let checked = genesis.checked().unwrap();
        let alternate_checked = alternate.checked().unwrap();
        let mut history = ColdQueueHistory::new(&queue(), &job).unwrap();
        history.observe_record(0, &checked).unwrap();
        let mut alternate_history = ColdQueueHistory::new(&alternate_queue, &job).unwrap();
        alternate_history
            .observe_record(0, &alternate_checked)
            .unwrap();
        let batch = Arc::new(CommitBatch {
            queue: queue(),
            groups: vec![group("one", "a")],
        });
        let mut tracker =
            ColdGroupStateTracker::with_work(&queue(), GroupDeclarations::Batch(batch), &job)
                .unwrap();
        if foreign_address {
            assert!(
                tracker
                    .initialize_work(&alternate_history, &alternate_checked, &job)
                    .is_err()
            );
            assert!(tracker.initialize_work(&history, &checked, &job).is_err());
        } else {
            tracker.initialize_work(&history, &checked, &job).unwrap();
            assert!(
                matches!(tracker.observe_record(0,&alternate_checked),Err(QueueError::OutcomeUnknown(reason))
                if reason.contains("different genesis token"))
            );
        }
        assert!(tracker.observe_record(0, &checked).is_err());
        assert!(tracker.finish(&history, &genesis, (1, 0, 1)).is_err());
        drop(alternate_history);
        drop(history);
        drop(alternate);
        drop(genesis);
        drop(definition);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
}
