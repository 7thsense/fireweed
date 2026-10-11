//! Complete-image reference only. Production must use bounded physical replay
//! inside its owned writer transaction with the external apply cursor.
use super::tests::{group, queue, schema, seed};
use super::*;
use crate::{
    queue_admission::QueueAdmission,
    queue_fields::FieldCoverage,
    queue_planner::{combine_deltas, lease_hash, plan_commit_group},
    queue_schema_codec::encode_definition,
};
use std::collections::BTreeMap;
#[derive(Clone, Default)]
struct Image {
    snapshot: RecordSnapshot,
    receipts: BTreeMap<(u8, String), ([u8; 32], Vec<u8>)>,
}
fn own(v: ValueRef<'_>) -> FieldValue {
    match v {
        ValueRef::Integer(n) => FieldValue::Integer(n),
        ValueRef::Float(n) => FieldValue::Float(n),
        ValueRef::Boolean(b) => FieldValue::Boolean(b),
        ValueRef::Text(t) => FieldValue::Text(t.into()),
        ValueRef::Bytes(b) => FieldValue::Bytes(b.into()),
    }
}
fn id(identity: IdentityRef<'_>) -> RecordIdentity {
    RecordIdentity {
        collection: identity.collection.map(str::to_owned),
        key: identity.key.into(),
    }
}
fn checked_current(
    s: &RecordSnapshot,
    i: IdentityRef<'_>,
) -> PublicationResult<(Option<u64>, u64)> {
    let key = id(i);
    let highwater = s.versions.get(&key).copied().unwrap_or(0);
    let version = if let Some(collection) = i.collection {
        s.state
            .get(&(collection.into(), i.key.into()))
            .map(|r| {
                if r.collection != collection || r.key != i.key {
                    return invalid("corrupt initial state identity");
                }
                Ok(r.version)
            })
            .transpose()?
    } else {
        s.work
            .get(i.key)
            .map(|r| {
                if r.record.work.key != i.key
                    || r.record.insertion_sequence == 0
                    || r.record.insertion_sequence > s.next_sequence
                {
                    return invalid("corrupt initial work identity/sequence");
                }
                Ok(r.record.version)
            })
            .transpose()?
    };
    if version.is_some_and(|v| v == 0 || v != highwater) {
        return invalid("corrupt initial highwater");
    }
    Ok((version, highwater))
}
fn admit_fields(fields: FieldsRef<'_>, def: &CollectionDefinition) -> PublicationResult<()> {
    for entry in fields.iter() {
        let (name, value) = entry?;
        if !def.admits_field(name)
            || value.is_some_and(|value| {
                def.fields
                    .get(name)
                    .is_some_and(|ty| *ty != value.field_type())
            })
        {
            return invalid("replay field schema mismatch");
        }
    }
    Ok(())
}
fn apply_reference(
    image: &mut Image,
    raw: &[u8],
    q: &QueueAddress,
    definition: &[u8],
    schema: &QueueSchema,
) -> PublicationResult<()> {
    let view = view_publication(raw)?;
    if view.format() != PublicationFormat::V1 {
        return invalid("FWQP1 reference does not apply FWQP2");
    }
    let summary = view.summary();
    if summary.queue != (q.tenant.as_str(), q.queue.as_str())
        || summary.definition_pin != DefinitionPin::from_persisted_bytes(definition).as_bytes()
        || summary.initial_sequence != image.snapshot.next_sequence
    {
        return invalid("replay queue/schema/sequence authority mismatch");
    }
    let mut bounds = BTreeMap::new();
    for command in view.commands() {
        let c = command?;
        let identity = id(c.identity);
        let (actual, hwm) = checked_current(&image.snapshot, c.identity)?;
        if actual != c.expected_version || hwm != c.prior_highwater {
            return invalid("replay predecessor authority mismatch");
        }
        bounds.insert(identity, (hwm, c.output_highwater));
        let definition = if let Some(collection) = c.identity.collection {
            schema
                .state
                .get(collection)
                .ok_or(PublicationError::Invalid("unknown replay state collection"))?
        } else {
            &schema.work
        };
        match c.effect {
            EffectRef::WorkPut {
                metadata, fields, ..
            } => {
                if let Some(old) = image.snapshot.work.get(c.identity.key) {
                    if metadata.insertion_sequence != old.record.insertion_sequence
                        && metadata.insertion_sequence <= summary.initial_sequence
                    {
                        return invalid("replay work identity/sequence mismatch");
                    }
                }
                if let FieldsCommandRef::Replace(f) | FieldsCommandRef::Edit(f) = fields {
                    admit_fields(f, definition)?;
                }
            }
            EffectRef::StatePut { fields, .. } => {
                let collection = c.identity.collection.unwrap();
                if let Some(old) = image
                    .snapshot
                    .state
                    .get(&(collection.into(), c.identity.key.into()))
                {
                    if old.collection != collection || old.key != c.identity.key {
                        return invalid("replay state identity mismatch");
                    }
                }
                admit_fields(fields, definition)?;
            }
            EffectRef::Delete => {}
        }
    }
    let check_success = |i: IdentityRef<'_>, version: u64| -> PublicationResult<()> {
        let key = id(i);
        let (prior, final_version) = if let Some(bounds) = bounds.get(&key) {
            *bounds
        } else {
            if let Some(collection) = i.collection {
                if !schema.state.contains_key(collection) {
                    return invalid("unknown omitted state collection");
                }
            }
            let (current, hwm) = checked_current(&image.snapshot, i)?;
            if current.is_none() {
                return invalid("missing omitted no-op authority");
            }
            (hwm, hwm)
        };
        if version == 0 || version < prior || version > final_version {
            return invalid("replay receipt version mismatch");
        }
        Ok(())
    };
    for receipt in view.receipts() {
        let r = receipt?;
        if image
            .receipts
            .contains_key(&(r.domain, r.request_id.into()))
        {
            return invalid("replay receipt not fresh");
        }
        match r.outcome {
            ReceiptOutcomeRef::Commit(CommitOutcomeRef::Committed(versions)) => {
                for version in versions.iter() {
                    let v = version?;
                    check_success(v.identity, v.version)?;
                }
            }
            ReceiptOutcomeRef::Insert(outcomes) => {
                for outcome in outcomes.iter() {
                    match outcome? {
                        InsertOutcomeRef::Inserted { key, version }
                        | InsertOutcomeRef::Replaced { key, version } => check_success(
                            IdentityRef {
                                collection: None,
                                key,
                            },
                            version,
                        )?,
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    // Test-only full image clone: all validation precedes all effects and receipts.
    let mut next = image.clone();
    for command in view.commands() {
        let c = command?;
        let identity = id(c.identity);
        match c.effect {
            EffectRef::Delete => {
                if let Some(collection) = c.identity.collection {
                    next.snapshot
                        .state
                        .remove(&(collection.into(), c.identity.key.into()));
                } else {
                    next.snapshot.work.remove(c.identity.key);
                }
            }
            EffectRef::StatePut { fields, value } => {
                let collection = c.identity.collection.unwrap();
                let mut owned = Fields::new();
                for entry in fields.iter() {
                    let (name, value) = entry?;
                    owned.insert(name.into(), own(value.unwrap()));
                }
                next.snapshot.state.insert(
                    (collection.into(), c.identity.key.into()),
                    StateRecord {
                        collection: collection.into(),
                        key: c.identity.key.into(),
                        version: c.output_highwater,
                        fields: owned,
                        value: value.to_vec().into(),
                    },
                );
            }
            EffectRef::WorkPut {
                metadata: m,
                fields,
                body,
            } => {
                let row = next
                    .snapshot
                    .work
                    .entry(c.identity.key.into())
                    .or_insert_with(|| StoredWork {
                        record: WorkRecord {
                            work: NewWork {
                                key: c.identity.key.into(),
                                priority: 0,
                                available_at: 0,
                                fields: Fields::new(),
                                body: Default::default(),
                            },
                            version: 0,
                            insertion_sequence: 0,
                            lifecycle: Lifecycle::Pending,
                            eligible_since: 0,
                            attempts: 0,
                            lease_expires_at: None,
                        },
                        lease_hash: None,
                        field_coverage: FieldCoverage::Complete,
                        body_loaded: true,
                    });
                row.record.version = c.output_highwater;
                row.record.work.priority = m.priority;
                row.record.work.available_at = m.available_at;
                row.record.insertion_sequence = m.insertion_sequence;
                row.record.lifecycle = m.lifecycle;
                row.record.eligible_since = m.eligible_since;
                row.record.attempts = m.attempts;
                row.record.lease_expires_at = m.lease_expires_at;
                row.lease_hash = m.lease_hash.copied();
                match fields {
                    FieldsCommandRef::Preserve => {}
                    FieldsCommandRef::Replace(f) => {
                        row.record.work.fields.clear();
                        for entry in f.iter() {
                            let (name, value) = entry?;
                            row.record
                                .work
                                .fields
                                .insert(name.into(), own(value.unwrap()));
                        }
                    }
                    FieldsCommandRef::Edit(f) => {
                        for entry in f.iter() {
                            let (name, value) = entry?;
                            if let Some(v) = value {
                                row.record.work.fields.insert(name.into(), own(v));
                            } else {
                                row.record.work.fields.remove(name);
                            }
                        }
                    }
                }
                if let BodyCommandRef::Replace(b) = body {
                    row.record.work.body = b.to_vec().into();
                }
            }
        }
        next.snapshot.versions.insert(identity, c.output_highwater);
    }
    next.snapshot.next_sequence = summary.final_sequence;
    for receipt in view.receipts() {
        let r = receipt?;
        next.receipts.insert(
            (r.domain, r.request_id.into()),
            (*r.fingerprint, r.encoded_outcome().to_vec()),
        );
    }
    *image = next;
    Ok(())
}
fn same_snapshot(a: &RecordSnapshot, b: &RecordSnapshot) {
    assert_eq!(a.work, b.work);
    assert_eq!(a.state, b.state);
    assert_eq!(a.versions, b.versions);
    assert_eq!(a.next_sequence, b.next_sequence);
}
fn unchanged(a: &Image, b: &Image) {
    same_snapshot(&a.snapshot, &b.snapshot);
    assert_eq!(a.receipts, b.receipts);
}
#[test]
fn borrowed_views_replay_complete_planner_images_and_restart_iterators() {
    let q = queue();
    let schema = schema();
    let raw = encode_definition(&schema).unwrap();
    let before = RecordSnapshot::default();
    let mut expected = before.clone();
    let create = seed();
    let delta = plan_commit_group(&create, &mut expected, 0, &schema).unwrap();
    let packed = combine_deltas(std::slice::from_ref(&delta), 0).unwrap();
    let a = QueueAdmission::new(1, 65536).unwrap();
    let job = a.reserve(0).unwrap();
    let frame = encode_publication(
        &q,
        &raw,
        &before,
        &packed,
        &[PublicationReceipt::commit(&q, &create, &delta.receipt).unwrap()],
        65536,
        &job,
    )
    .unwrap();
    let view = view_publication(frame.as_bytes()).unwrap();
    assert_eq!(view.commands().count(), 2);
    assert_eq!(view.commands().count(), 2);
    let command = view.commands().next().unwrap().unwrap();
    let EffectRef::WorkPut {
        fields: FieldsCommandRef::Replace(fields),
        body: BodyCommandRef::Replace(body),
        ..
    } = command.effect
    else {
        panic!("wrong decoded command")
    };
    assert_eq!(fields.iter().count(), 5);
    assert_eq!(fields.iter().count(), 5);
    assert_eq!(body, &[0, 255]);
    assert!(
        body.as_ptr() >= frame.as_bytes().as_ptr() && body.as_ptr() < end_address(frame.as_bytes())
    );
    let mut image = Image::default();
    apply_reference(&mut image, frame.as_bytes(), &q, &raw, &schema).unwrap();
    same_snapshot(&image.snapshot, &expected);
    let input_before = image.snapshot.clone();
    let key = create.work_inserts[0].key.clone();
    let mut patch = group("b-edit");
    patch.work_updates.push(UnclaimedUpdate {
        key: key.clone(),
        expect: StateExpect::Version(1),
        change: UnclaimedChange::Patch(WorkPatch {
            fields: BTreeMap::from([
                ("gate".into(), Some(FieldValue::Boolean(false))),
                ("text".into(), None),
            ]),
            body: Some(Default::default()),
            ..WorkPatch::default()
        }),
    });
    patch.state_updates.push(StateUpdate {
        collection: "instances".into(),
        key: "state\0/β".into(),
        expect: StateExpect::Version(1),
        change: StateChange::Put {
            fields: Fields::new(),
            value: vec![9].into(),
        },
    });
    patch.work_inserts.push(NewWork {
        key: "new\0α".into(),
        priority: 1,
        available_at: 0,
        fields: Fields::new(),
        body: vec![4].into(),
    });
    let mut after = input_before.clone();
    let planned = plan_commit_group(&patch, &mut after, 0, &schema).unwrap();
    let packed =
        combine_deltas(std::slice::from_ref(&planned), input_before.next_sequence).unwrap();
    let frame = encode_publication(
        &q,
        &raw,
        &input_before,
        &packed,
        &[PublicationReceipt::commit(&q, &patch, &planned.receipt).unwrap()],
        65536,
        &job,
    )
    .unwrap();
    apply_reference(&mut image, frame.as_bytes(), &q, &raw, &schema).unwrap();
    same_snapshot(&image.snapshot, &after);
    let rejected_before = image.clone();
    assert!(apply_reference(&mut image, frame.as_bytes(), &q, &raw, &schema).is_err());
    unchanged(&image, &rejected_before);
}
// Integer address comparison avoids creating another pointer outside the slice.
fn end_address(bytes: &[u8]) -> *const u8 {
    ((bytes.as_ptr() as usize) + bytes.len()) as *const u8
}
#[test]
fn late_authority_failures_leave_data_and_receipts_unchanged() {
    let q = queue();
    let schema = schema();
    let raw = encode_definition(&schema).unwrap();
    let create = seed();
    let before = RecordSnapshot::default();
    let mut after = before.clone();
    let delta = plan_commit_group(&create, &mut after, 0, &schema).unwrap();
    let packed = combine_deltas(std::slice::from_ref(&delta), 0).unwrap();
    let a = QueueAdmission::new(1, 65536).unwrap();
    let job = a.reserve(0).unwrap();
    let frame = encode_publication(
        &q,
        &raw,
        &before,
        &packed,
        &[PublicationReceipt::commit(&q, &create, &delta.receipt).unwrap()],
        65536,
        &job,
    )
    .unwrap();
    let mut corrupt = Image::default();
    corrupt.snapshot.versions.insert(
        RecordIdentity {
            collection: Some("instances".into()),
            key: "state\0/β".into(),
        },
        9,
    );
    let original = corrupt.clone();
    assert!(apply_reference(&mut corrupt, frame.as_bytes(), &q, &raw, &schema).is_err());
    unchanged(&corrupt, &original);
    corrupt = Image::default();
    corrupt
        .receipts
        .insert((2, create.request_id.clone()), ([0; 32], vec![]));
    let original = corrupt.clone();
    assert!(apply_reference(&mut corrupt, frame.as_bytes(), &q, &raw, &schema).is_err());
    unchanged(&corrupt, &original);
    corrupt = Image::default();
    let original = corrupt.clone();
    let mut wrong = q.clone();
    wrong.tenant.push('x');
    assert!(apply_reference(&mut corrupt, frame.as_bytes(), &wrong, &raw, &schema).is_err());
    unchanged(&corrupt, &original);
    assert!(
        apply_reference(
            &mut corrupt,
            frame.as_bytes(),
            &q,
            b"wrong-definition",
            &schema
        )
        .is_err()
    );
    unchanged(&corrupt, &original);
    let receipt = view_publication(frame.as_bytes())
        .unwrap()
        .receipts()
        .next()
        .unwrap()
        .unwrap();
    corrupt = Image::default();
    corrupt.receipts.insert(
        (receipt.domain, receipt.request_id.into()),
        (*receipt.fingerprint, receipt.encoded_outcome().to_vec()),
    );
    let matching = corrupt.clone();
    assert!(apply_reference(&mut corrupt, frame.as_bytes(), &q, &raw, &schema).is_err());
    unchanged(&corrupt, &matching);
    // Structurally valid bytes with the wrong declared leaf type must fail
    // authority validation, even though the publisher-side representation agrees.
    let mut malformed_delta = packed.clone();
    malformed_delta.work[0]
        .record
        .work
        .fields
        .insert("gate".into(), FieldValue::Integer(3));
    let ResolvedFields::Replace(values) = malformed_delta
        .work_fields
        .get_mut(&create.work_inserts[0].key)
        .unwrap()
    else {
        panic!("full seed expected")
    };
    values.insert("gate".into(), FieldValue::Integer(3));
    let bad_type =
        encode_publication(&q, &raw, &before, &malformed_delta, &[], 65536, &job).unwrap();
    corrupt = Image::default();
    let empty = corrupt.clone();
    assert!(apply_reference(&mut corrupt, bad_type.as_bytes(), &q, &raw, &schema).is_err());
    unchanged(&corrupt, &empty);
    corrupt = Image::default();
    let mut malformed = frame.as_bytes().to_vec();
    malformed.push(0);
    assert!(view_publication(&malformed).is_err());
    assert!(apply_reference(&mut corrupt, &malformed, &q, &raw, &schema).is_err());
    unchanged(&corrupt, &original);
}
#[test]
fn leased_retention_delete_reinsert_and_omitted_noop_replay() {
    let q = queue();
    let schema = schema();
    let raw = encode_definition(&schema).unwrap();
    let create = seed();
    let mut before = RecordSnapshot::default();
    plan_commit_group(&create, &mut before, 0, &schema).unwrap();
    let key = create.work_inserts[0].key.clone();
    let old = before.work.get_mut(&key).unwrap();
    old.record.version = 2;
    old.record.lifecycle = Lifecycle::Leased;
    old.record.lease_expires_at = Some(100);
    old.lease_hash = Some(lease_hash(&[4]));
    before.versions.insert(
        RecordIdentity {
            collection: None,
            key: key.clone(),
        },
        2,
    );
    let mut retain = group("a-retain");
    retain.claimed_updates.push(ClaimedUpdate {
        key: key.clone(),
        lease: LeaseCredential { token: vec![4] },
        expected_version: 2,
        patch: WorkPatch::default(),
        disposition: Disposition::Retain { renew_nanos: None },
    });
    let mut after = before.clone();
    let first = plan_commit_group(&retain, &mut after, 0, &schema).unwrap();
    let mut delete = group("b-delete");
    delete.claimed_updates.push(ClaimedUpdate {
        key: key.clone(),
        lease: LeaseCredential { token: vec![4] },
        expected_version: 3,
        patch: WorkPatch::default(),
        disposition: Disposition::Delete,
    });
    let second = plan_commit_group(&delete, &mut after, 0, &schema).unwrap();
    let mut reinsert = group("c-reinsert");
    reinsert.work_inserts = create.work_inserts.clone();
    let third = plan_commit_group(&reinsert, &mut after, 0, &schema).unwrap();
    let packed = combine_deltas(
        &[first.clone(), second.clone(), third.clone()],
        before.next_sequence,
    )
    .unwrap();
    let a = QueueAdmission::new(1, 65536).unwrap();
    let job = a.reserve(0).unwrap();
    let frame = encode_publication(
        &q,
        &raw,
        &before,
        &packed,
        &[
            PublicationReceipt::commit(&q, &retain, &first.receipt).unwrap(),
            PublicationReceipt::commit(&q, &delete, &second.receipt).unwrap(),
            PublicationReceipt::commit(&q, &reinsert, &third.receipt).unwrap(),
        ],
        65536,
        &job,
    )
    .unwrap();
    let mut image = Image {
        snapshot: before,
        receipts: Default::default(),
    };
    apply_reference(&mut image, frame.as_bytes(), &q, &raw, &schema).unwrap();
    same_snapshot(&image.snapshot, &after);
    let noop_before = after.clone();
    let mut noop = group("noop");
    noop.work_updates.push(UnclaimedUpdate {
        key,
        expect: StateExpect::Version(5),
        change: UnclaimedChange::Administrative {
            patch: Default::default(),
            lifecycle: AdministrativeLifecycle::Keep,
            lease_guard: AdministrativeLeaseGuard::RejectActive,
        },
    });
    let planned = plan_commit_group(&noop, &mut after, 0, &schema).unwrap();
    let packed = combine_deltas(std::slice::from_ref(&planned), noop_before.next_sequence).unwrap();
    let frame = encode_publication(
        &q,
        &raw,
        &noop_before,
        &packed,
        &[PublicationReceipt::commit(&q, &noop, &planned.receipt).unwrap()],
        65536,
        &job,
    )
    .unwrap();
    assert_eq!(
        view_publication(frame.as_bytes())
            .unwrap()
            .summary()
            .commands,
        0
    );
    apply_reference(&mut image, frame.as_bytes(), &q, &raw, &schema).unwrap();
    same_snapshot(&image.snapshot, &after);
    assert!(image.receipts.contains_key(&(2, "noop".into())));
}

fn replay_group(
    image: &mut Image,
    g: &CommitGroup,
    q: &QueueAddress,
    raw: &[u8],
    schema: &QueueSchema,
) -> CommitDelta {
    let before = image.snapshot.clone();
    let mut expected = before.clone();
    let planned = plan_commit_group(g, &mut expected, 0, schema).unwrap();
    let packed = combine_deltas(std::slice::from_ref(&planned), before.next_sequence).unwrap();
    let a = QueueAdmission::new(1, 65536).unwrap();
    let job = a.reserve(0).unwrap();
    let frame = encode_publication(
        q,
        raw,
        &before,
        &packed,
        &[PublicationReceipt::commit(q, g, &planned.receipt).unwrap()],
        65536,
        &job,
    )
    .unwrap();
    let view = view_publication(frame.as_bytes()).unwrap();
    if planned.receipt.outcome == GroupOutcome::Rejected(Rejection::NotFound) {
        let receipt = view.receipts().next().unwrap().unwrap();
        assert!(matches!(
            receipt.outcome,
            ReceiptOutcomeRef::Commit(CommitOutcomeRef::Rejected(RejectionRef::NotFound))
        ));
    }
    for command in view_publication(frame.as_bytes()).unwrap().commands() {
        let command = command.unwrap();
        match command.effect {
            EffectRef::WorkPut {
                metadata,
                fields,
                body,
            } => {
                let row = packed
                    .work
                    .iter()
                    .find(|r| r.record.work.key == command.identity.key)
                    .unwrap();
                assert_eq!(metadata.lifecycle, row.record.lifecycle);
                assert_eq!(metadata.lease_expires_at, row.record.lease_expires_at);
                assert_eq!(metadata.lease_hash.copied(), row.lease_hash);
                match (&packed.work_fields[command.identity.key], fields) {
                    (ResolvedFields::Preserve, FieldsCommandRef::Preserve) => {}
                    (ResolvedFields::Replace(_), FieldsCommandRef::Replace(_)) => {}
                    (ResolvedFields::Edit(_), FieldsCommandRef::Edit(_)) => {}
                    _ => panic!("wrong field command"),
                };
                match (&packed.work_bodies[command.identity.key], body) {
                    (ResolvedBody::Preserve, BodyCommandRef::Preserve) => {}
                    (ResolvedBody::Replace(expected), BodyCommandRef::Replace(actual)) => {
                        assert_eq!(expected.as_ref(), actual)
                    }
                    _ => panic!("wrong body command"),
                };
            }
            EffectRef::Delete => assert!(
                packed
                    .deleted_work
                    .iter()
                    .any(|k| k == command.identity.key)
                    || packed
                        .deleted_state
                        .iter()
                        .any(|(c, k)| Some(c.as_str()) == command.identity.collection
                            && k == command.identity.key)
            ),
            EffectRef::StatePut { .. } => assert!(command.identity.collection.is_some()),
        }
    }
    apply_reference(image, frame.as_bytes(), q, raw, schema).unwrap();
    same_snapshot(&image.snapshot, &expected);
    planned
}
#[test]
fn uncoalesced_leased_preserve_work_and_state_tombstones_replay() {
    let q = queue();
    let schema = schema();
    let raw = encode_definition(&schema).unwrap();
    let create = seed();
    let mut image = Image::default();
    replay_group(&mut image, &create, &q, &raw, &schema);
    let key = create.work_inserts[0].key.clone();
    let row = image.snapshot.work.get_mut(&key).unwrap();
    row.record.version = 2;
    row.record.lifecycle = Lifecycle::Leased;
    row.record.lease_expires_at = Some(100);
    row.lease_hash = Some(lease_hash(&[4]));
    image.snapshot.versions.insert(
        RecordIdentity {
            collection: None,
            key: key.clone(),
        },
        2,
    );
    let mut retain = group("retain-visible");
    retain.claimed_updates.push(ClaimedUpdate {
        key: key.clone(),
        lease: LeaseCredential { token: vec![4] },
        expected_version: 2,
        patch: WorkPatch::default(),
        disposition: Disposition::Retain { renew_nanos: None },
    });
    replay_group(&mut image, &retain, &q, &raw, &schema);
    assert_eq!(
        image.snapshot.work[&key].record.lifecycle,
        Lifecycle::Leased
    );
    assert_eq!(
        image.snapshot.work[&key].record.work.body.as_ref(),
        &[0, 255]
    );
    let mut delete = group("delete-work-visible");
    delete.claimed_updates = retain.claimed_updates.clone();
    delete.claimed_updates[0].expected_version = 3;
    delete.claimed_updates[0].disposition = Disposition::Delete;
    replay_group(&mut image, &delete, &q, &raw, &schema);
    assert!(!image.snapshot.work.contains_key(&key));
    assert_eq!(
        image.snapshot.versions[&RecordIdentity {
            collection: None,
            key: key.clone()
        }],
        4
    );
    let mut state_delete = group("delete-state-visible");
    state_delete.state_updates.push(StateUpdate {
        collection: "instances".into(),
        key: "state\0/β".into(),
        expect: StateExpect::Version(1),
        change: StateChange::Delete,
    });
    replay_group(&mut image, &state_delete, &q, &raw, &schema);
    assert!(image.snapshot.state.is_empty());
    assert_eq!(
        image.snapshot.versions[&RecordIdentity {
            collection: Some("instances".into()),
            key: "state\0/β".into()
        }],
        2
    );
    let mut reinsert = create;
    reinsert.request_id = "reinsert-visible".into();
    replay_group(&mut image, &reinsert, &q, &raw, &schema);
    assert_eq!(image.snapshot.work[&key].record.version, 5);
    assert_eq!(
        image.snapshot.state[&("instances".into(), "state\0/β".into())].version,
        3
    );
}
#[test]
fn decoded_insert_and_rejected_receipts_replay() {
    let q = queue();
    let schema = schema();
    let raw = encode_definition(&schema).unwrap();
    let items = seed().work_inserts;
    let mut image = Image::default();
    for (request_id, mode) in [
        ("insert", InsertMode::InsertOnly),
        ("upsert", InsertMode::Upsert),
        ("already-exists", InsertMode::InsertOnly),
    ] {
        let request = InsertRequest {
            queue: q.clone(),
            request_id: request_id.into(),
            mode,
            items: items.clone(),
        };
        let before = image.snapshot.clone();
        let mut expected = before.clone();
        let (deltas, outcomes) =
            crate::queue_planner::plan_insert_vector(&request, &mut expected, &schema, || Ok(0))
                .unwrap();
        let packed = combine_deltas(&deltas, before.next_sequence).unwrap();
        let a = QueueAdmission::new(1, 65536).unwrap();
        let job = a.reserve(0).unwrap();
        let frame = encode_publication(
            &q,
            &raw,
            &before,
            &packed,
            &[PublicationReceipt::insert(&request, &outcomes).unwrap()],
            65536,
            &job,
        )
        .unwrap();
        let view = view_publication(frame.as_bytes()).unwrap();
        let receipt = view.receipts().next().unwrap().unwrap();
        let ReceiptOutcomeRef::Insert(results) = receipt.outcome else {
            panic!("insert receipt expected")
        };
        assert_eq!(results.len(), 1);
        let result = results.iter().next().unwrap().unwrap();
        match (request_id, result) {
            ("insert", InsertOutcomeRef::Inserted { key, version }) => {
                assert_eq!(key, items[0].key);
                assert_eq!(version, 1);
            }
            ("upsert", InsertOutcomeRef::Replaced { key, version }) => {
                assert_eq!(key, items[0].key);
                assert_eq!(version, 2);
            }
            (
                "already-exists",
                InsertOutcomeRef::Rejected {
                    key,
                    reason: RejectionRef::AlreadyExists,
                },
            ) => assert_eq!(key, items[0].key),
            _ => panic!("wrong decoded insert outcome"),
        };
        apply_reference(&mut image, frame.as_bytes(), &q, &raw, &schema).unwrap();
        same_snapshot(&image.snapshot, &expected);
    }
    let mut rejected = group("rejected-version");
    rejected.state_updates.push(StateUpdate {
        collection: "instances".into(),
        key: "missing".into(),
        expect: StateExpect::Version(999),
        change: StateChange::Delete,
    });
    let planned = replay_group(&mut image, &rejected, &q, &raw, &schema);
    assert_eq!(
        planned.receipt.outcome,
        GroupOutcome::Rejected(Rejection::NotFound)
    );
}
#[test]
fn corrupt_initial_identity_rejected_for_delete_and_omitted_noop() {
    let q = queue();
    let schema = schema();
    let raw = encode_definition(&schema).unwrap();
    let create = seed();
    let mut before = RecordSnapshot::default();
    plan_commit_group(&create, &mut before, 0, &schema).unwrap();
    let key = create.work_inserts[0].key.clone();
    for delete in [false, true] {
        let mut g = group(if delete { "delete" } else { "noop" });
        g.work_updates.push(UnclaimedUpdate {
            key: key.clone(),
            expect: StateExpect::Version(1),
            change: if delete {
                UnclaimedChange::Delete
            } else {
                UnclaimedChange::Administrative {
                    patch: Default::default(),
                    lifecycle: AdministrativeLifecycle::Keep,
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                }
            },
        });
        let mut expected = before.clone();
        let planned = plan_commit_group(&g, &mut expected, 0, &schema).unwrap();
        let packed = combine_deltas(std::slice::from_ref(&planned), before.next_sequence).unwrap();
        let a = QueueAdmission::new(1, 65536).unwrap();
        let job = a.reserve(0).unwrap();
        let frame = encode_publication(
            &q,
            &raw,
            &before,
            &packed,
            &[PublicationReceipt::commit(&q, &g, &planned.receipt).unwrap()],
            65536,
            &job,
        )
        .unwrap();
        for corrupt_sequence in [false, true] {
            let mut image = Image {
                snapshot: before.clone(),
                receipts: Default::default(),
            };
            let record = image.snapshot.work.get_mut(&key).unwrap();
            if corrupt_sequence {
                record.record.insertion_sequence = 0;
            } else {
                record.record.work.key = "wrong".into();
            }
            let original = image.clone();
            assert!(apply_reference(&mut image, frame.as_bytes(), &q, &raw, &schema).is_err());
            unchanged(&image, &original);
        }
    }
}
