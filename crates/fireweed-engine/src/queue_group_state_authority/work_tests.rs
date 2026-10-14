use super::*;
use crate::{
    queue_admission::QueueAdmission,
    queue_log_record::{
        OwnedQueueLogRecord, encode_checked_genesis, encode_checked_publication_record,
    },
    queue_planner::{RecordSnapshot, combine_deltas, plan_commit_group, plan_insert_vector},
    queue_publication::{
        PublicationReceipt, WorkMetadataRef, encode_publication_pinned, view_publication,
    },
    queue_schema_codec::{AdmittedQueueDefinition, encode_definition},
};
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
fn group(id: &str, change: UnclaimedChange) -> CommitGroup {
    CommitGroup {
        request_id: id.into(),
        administration: None,
        claimed_updates: vec![],
        state_updates: vec![],
        work_inserts: vec![],
        work_updates: vec![UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change,
        }],
    }
}
fn apply_group(
    set: &mut WorkSet,
    snapshot: &mut RecordSnapshot,
    group: &CommitGroup,
    definition: &AdmittedQueueDefinition,
    job: &Arc<QueueReservation>,
    mut shadow: Option<&mut WorkSet>,
) -> OwnedQueueLogRecord {
    let before = snapshot.clone();
    let delta = plan_commit_group(group, snapshot, 0, &schema()).unwrap();
    assert!(matches!(
        delta.receipt.outcome,
        GroupOutcome::Committed { .. }
    ));
    let queue = QueueAddress {
        tenant: "t".into(),
        queue: "q".into(),
    };
    let receipt = PublicationReceipt::commit(&queue, group, &delta.receipt).unwrap();
    let frame = encode_publication_pinned(
        &queue,
        definition.pin(),
        &before,
        &delta,
        &[receipt],
        1 << 20,
        job,
    )
    .unwrap();
    for command in view_publication(frame.as_bytes()).unwrap().commands() {
        let command = command.unwrap();
        set.observe(command).unwrap();
        if let Some(shadow) = &mut shadow {
            shadow.observe(command).unwrap();
        }
    }
    encode_checked_publication_record(1, frame.as_bytes(), 1 << 20, job).unwrap()
}
#[test]
fn selected_fold_handles_replace_edit_preserve_delete_recreate_and_lease_metadata() {
    let admission = QueueAdmission::new(1, 8 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let definition =
        AdmittedQueueDefinition::from_raw(&encode_definition(&schema()).unwrap(), &job).unwrap();
    let selected: BTreeSet<String> = ["meta/x".into(), "meta/y".into()].into();
    let declaration = group(
        "scope",
        UnclaimedChange::Administrative {
            patch: WorkPatch {
                fields: [("meta/x".into(), None), ("meta/y".into(), None)].into(),
                body: Some(bytes::Bytes::new()),
                ..Default::default()
            },
            lifecycle: AdministrativeLifecycle::Completed,
            lease_guard: AdministrativeLeaseGuard::RejectActive,
        },
    );
    let mut set = WorkSet::new(std::slice::from_ref(&declaration), &schema(), &job).unwrap();
    let queue = QueueAddress {
        tenant: "t".into(),
        queue: "q".into(),
    };
    let request = InsertRequest {
        queue: queue.clone(),
        request_id: "insert".into(),
        mode: InsertMode::InsertOnly,
        items: vec![NewWork {
            key: "a".into(),
            priority: 5,
            available_at: 0,
            fields: [
                ("meta/x".into(), FieldValue::Integer(7)),
                ("meta/y".into(), FieldValue::Float(-0.0)),
            ]
            .into(),
            body: bytes::Bytes::from_static(b"abc"),
        }],
    };
    let genesis = encode_checked_genesis(1, &queue, &definition, &[31; 32], 1 << 20, &job).unwrap();
    let mut records = vec![genesis];
    let mut snapshot = RecordSnapshot::default();
    let before = snapshot.clone();
    let (deltas, outcomes) =
        plan_insert_vector(&request, &mut snapshot, &schema(), || Ok(0)).unwrap();
    let delta = combine_deltas(&deltas, 0).unwrap();
    let receipt = PublicationReceipt::insert(&request, &outcomes).unwrap();
    let frame = encode_publication_pinned(
        &queue,
        definition.pin(),
        &before,
        &delta,
        &[receipt],
        1 << 20,
        &job,
    )
    .unwrap();
    for command in view_publication(frame.as_bytes()).unwrap().commands() {
        set.observe(command.unwrap()).unwrap();
    }
    records.push(encode_checked_publication_record(1, frame.as_bytes(), 1 << 20, &job).unwrap());
    let source = set.descriptor(0, "a").unwrap();
    source
        .verify_fields(&request.items[0].fields, &selected)
        .unwrap();
    source.verify_body(b"abc").unwrap();
    assert!(source.verify_body(b"abd").is_err());
    assert_eq!(source.metadata.unwrap().priority, 5);
    let edit = group(
        "edit",
        UnclaimedChange::Patch(WorkPatch {
            fields: [("meta/x".into(), None)].into(),
            ..Default::default()
        }),
    );
    records.push(apply_group(
        &mut set,
        &mut snapshot,
        &edit,
        &definition,
        &job,
        None,
    ));
    let mut resumed = set.copy_admitted(&job).unwrap();
    let expected: Fields = [("meta/y".into(), FieldValue::Float(-0.0))].into();
    set.descriptor(0, "a")
        .unwrap()
        .verify_fields(&expected, &selected)
        .unwrap();
    let mut preserve = group(
        "preserve",
        UnclaimedChange::Patch(WorkPatch {
            priority: Some(9),
            ..Default::default()
        }),
    );
    preserve.work_updates[0].expect = StateExpect::Version(2);
    records.push(apply_group(
        &mut set,
        &mut snapshot,
        &preserve,
        &definition,
        &job,
        Some(&mut resumed),
    ));
    assert_eq!(set.entries, resumed.entries);
    assert_eq!(
        set.descriptor(0, "a").unwrap().metadata.unwrap().priority,
        9
    );
    set.descriptor(0, "a")
        .unwrap()
        .verify_fields(&expected, &selected)
        .unwrap();
    set.descriptor(0, "a").unwrap().verify_body(b"abc").unwrap();
    let mut replace = group(
        "replace",
        UnclaimedChange::Replace(NewWork {
            key: "a".into(),
            priority: 11,
            available_at: 0,
            fields: [("meta/x".into(), FieldValue::Bytes(vec![0, 255]))].into(),
            body: bytes::Bytes::from_static(b"xyz"),
        }),
    );
    replace.work_updates[0].expect = StateExpect::Version(3);
    records.push(apply_group(
        &mut set,
        &mut snapshot,
        &replace,
        &definition,
        &job,
        Some(&mut resumed),
    ));
    assert_eq!(set.entries, resumed.entries);
    let expected: Fields = [("meta/x".into(), FieldValue::Bytes(vec![0, 255]))].into();
    set.descriptor(0, "a")
        .unwrap()
        .verify_fields(&expected, &selected)
        .unwrap();
    assert_eq!(
        set.descriptor(0, "a").unwrap().fields.get("meta/y"),
        Some(&None)
    );
    set.descriptor(0, "a").unwrap().verify_body(b"xyz").unwrap();
    let batch = Arc::new(CommitBatch {
        queue: queue.clone(),
        groups: vec![declaration],
    });
    let mut history = crate::queue_recovery::ColdQueueHistory::new(&queue, &job).unwrap();
    let mut cold =
        ColdGroupStateTracker::with_work(&queue, GroupDeclarations::Batch(batch.clone()), &job)
            .unwrap();
    let mut prefix =
        ColdGroupStateTracker::with_work(&queue, GroupDeclarations::Batch(batch.clone()), &job)
            .unwrap();
    for (offset, record) in records[..3].iter().enumerate() {
        let checked = record.checked().unwrap();
        history.observe_record(offset as u64, &checked).unwrap();
        if offset == 0 {
            cold.initialize_work(&history, &checked, &job).unwrap();
            prefix.initialize_work(&history, &checked, &job).unwrap();
        }
        cold.observe_record(offset as u64, &checked).unwrap();
        prefix.observe_record(offset as u64, &checked).unwrap();
    }
    let point = (
        history.next_offset(),
        history.insertion_sequence(),
        history.historical_epoch(),
    );
    let old = Arc::new(prefix.finish(&history, &records[0], point).unwrap());
    let mut continued =
        ColdGroupStateTracker::resume(&old, &batch, &records[0], point, &job).unwrap();
    for (offset, record) in records.iter().enumerate().skip(3) {
        let checked = record.checked().unwrap();
        history.observe_record(offset as u64, &checked).unwrap();
        cold.observe_record(offset as u64, &checked).unwrap();
        continued.observe_record(offset as u64, &checked).unwrap();
    }
    let point = (
        history.next_offset(),
        history.insertion_sequence(),
        history.historical_epoch(),
    );
    let cold = Arc::new(cold.finish(&history, &records[0], point).unwrap());
    let continued = Arc::new(continued.finish(&history, &records[0], point).unwrap());
    cold.validate_batch(&batch, &records[0], point, &job)
        .unwrap();
    continued
        .validate_batch(&batch, &records[0], point, &job)
        .unwrap();
    assert_eq!(
        cold.group(0).unwrap().work_descriptor("a").unwrap(),
        continued.group(0).unwrap().work_descriptor("a").unwrap()
    );
    assert_eq!(
        cold.group(0).unwrap().work_descriptor("a").unwrap(),
        set.descriptor(0, "a").unwrap()
    );
    drop(cold);
    drop(continued);
    drop(old);
    drop(history);
    drop(batch);
    drop(records);
    // Claim publications use these same physical resolved commands. This checks
    // folding metadata and preserve knowledge, not a second plan_claim execution.
    set.observe(CommandRef {
        identity: crate::queue_publication::IdentityRef {
            collection: None,
            key: "a",
        },
        expected_version: Some(4),
        prior_highwater: 4,
        output_highwater: 5,
        effect: EffectRef::WorkPut {
            metadata: WorkMetadataRef {
                priority: 11,
                available_at: 0,
                insertion_sequence: 1,
                lifecycle: Lifecycle::Leased,
                eligible_since: 0,
                attempts: 1,
                lease_expires_at: Some(100),
                lease_hash: Some(&[7; 32]),
            },
            fields: FieldsCommandRef::Preserve,
            body: BodyCommandRef::Preserve,
        },
    })
    .unwrap();
    let source = set.descriptor(0, "a").unwrap();
    assert_eq!(source.metadata.unwrap().lease_hash, Some([7; 32]));
    assert_eq!(source.metadata.unwrap().lease_expires_at, Some(100));
    source.verify_fields(&expected, &selected).unwrap();
    source.verify_body(b"xyz").unwrap();
    set.observe(CommandRef {
        identity: crate::queue_publication::IdentityRef {
            collection: None,
            key: "a",
        },
        expected_version: Some(5),
        prior_highwater: 5,
        output_highwater: 9,
        effect: EffectRef::Delete,
    })
    .unwrap();
    assert_eq!(set.descriptor(0, "a").unwrap().highwater, 9);
    assert!(set.descriptor(0, "a").unwrap().metadata.is_none());
    assert!(set.descriptor(0, "a").unwrap().body.is_none());
    assert!(
        set.descriptor(0, "a")
            .unwrap()
            .fields
            .values()
            .all(Option::is_none)
    );
    // Recreate from the real complete replacement command, with retained HWM.
    let replacement = view_publication(frame.as_bytes())
        .unwrap()
        .commands()
        .next()
        .unwrap()
        .unwrap();
    set.observe(CommandRef {
        prior_highwater: 9,
        expected_version: None,
        output_highwater: 12,
        ..replacement
    })
    .unwrap();
    assert_eq!(set.descriptor(0, "a").unwrap().highwater, 12);
    set.descriptor(0, "a")
        .unwrap()
        .verify_fields(&request.items[0].fields, &selected)
        .unwrap();
    set.descriptor(0, "a").unwrap().verify_body(b"abc").unwrap();
    drop(resumed);
    drop(set);
    drop(frame);
    drop(delta);
    drop(deltas);
    drop(snapshot);
    drop(definition);
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
}
