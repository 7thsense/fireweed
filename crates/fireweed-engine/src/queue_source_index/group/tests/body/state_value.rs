//! State values require exact full identity, complete span and current HWM.
use super::*;
use crate::queue_source_index::{QueueSourceStateValueEvidence, verify_source_state_values};
fn state_key(collection: &str, key: &str) -> Vec<u8> {
    QueueSourceKey::Record {
        collection: Some(collection),
        key,
    }
    .encode()
    .unwrap()
}
fn state_evidence<'a>(
    f: &'a Fixture,
    collection: &str,
    key: &str,
) -> Option<QueueSourceStateValueEvidence<'a>> {
    let encoded = state_key(collection, key);
    let cut = f.reader.cut().unwrap();
    QueueSourceStateValueEvidence::from_source(
        cut,
        collection,
        key,
        cut.point_from_source(&encoded, f.reader.facts.get(&encoded).map(Vec::as_slice)),
        &f.job,
    )
    .unwrap()
}
fn append_state(f: &mut Fixture, value: Vec<u8>) {
    let mut change = group("next-state");
    change.state_updates.push(StateUpdate {
        collection: "s".into(),
        key: "state".into(),
        expect: StateExpect::Version(f.snapshot.state[&("s".into(), "state".into())].version),
        change: StateChange::Put {
            fields: work("", "state").fields,
            value: value.into(),
        },
    });
    let (publication, snapshot) = record(
        &f.queue,
        f.definition.schema(),
        &f.snapshot,
        &change,
        &f.job,
    );
    f.history.push(publication);
    f.snapshot = snapshot;
    f.reader.through = f.history.len() as u64;
    f.reader.facts = oracle::fold(&f.history, &f.snapshot, f.definition.schema());
}
#[test]
fn source_state_values_preserve_aliases_empty_and_actual_frame_ownership() {
    let mut f = fixture();
    for (expected, offset) in [(vec![5], 1), (vec![], 2)] {
        if offset == 2 {
            append_state(&mut f, vec![]);
        }
        let baseline = f.admission.usage().unwrap();
        assert!(state_evidence(&f, "s", "missing").is_none());
        let member = state_evidence(&f, "s", "state").unwrap();
        assert_eq!(member.version(), offset);
        assert_eq!(member.collection(), "s");
        assert_eq!(member.key(), "state");
        let checked = f.history[offset as usize].checked().unwrap();
        let values = verify_source_state_values(
            &[&member, &member],
            &checked,
            offset,
            f.definition.pin(),
            &f.job,
        )
        .unwrap();
        assert_eq!(values.value(0).unwrap(), expected);
        assert_eq!(values.value(1).unwrap(), expected);
        assert!(values.value(2).is_err());
        let frame = f.history[offset as usize].bytes();
        let raw = values.value(0).unwrap();
        assert!(raw.as_ptr().addr() >= frame.as_ptr().addr());
        assert!(raw.as_ptr().addr() + raw.len() <= frame.as_ptr().addr() + frame.len());
        drop(member);
        assert_eq!(values.value(0).unwrap(), expected);
        drop(values);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    }
}
#[test]
fn source_state_values_reject_equal_bytes_old_version_and_literal_aliases() {
    let mut f = fixture();
    let key = state_key("s", "state");
    let old = f.reader.facts[&key].clone();
    append_state(&mut f, vec![5]);
    let current = state_evidence(&f, "s", "state").unwrap();
    let checked = f.history[2].checked().unwrap();
    let values =
        verify_source_state_values(&[&current], &checked, 2, f.definition.pin(), &f.job).unwrap();
    assert_eq!(values.value(0).unwrap(), [5]);
    drop(values);
    drop(current);
    let mut fact: QueueSourceRecord = decode(&old).unwrap();
    fact.highwater = 2;
    f.reader.facts.insert(key.clone(), encode(&fact).unwrap());
    let stale = state_evidence(&f, "s", "state").unwrap();
    assert!(
        verify_source_state_values(
            &[&stale],
            &f.history[1].checked().unwrap(),
            1,
            f.definition.pin(),
            &f.job
        )
        .is_err()
    );
    drop(stale);
    // Eliminate the version mismatch before testing full literal identity.
    fact = decode(&old).unwrap();
    f.reader.facts.insert(key.clone(), encode(&fact).unwrap());
    let genuine = state_evidence(&f, "s", "state").unwrap();
    assert!(
        verify_source_state_values(
            &[&genuine],
            &f.history[1].checked().unwrap(),
            1,
            f.definition.pin(),
            &f.job
        )
        .is_ok()
    );
    drop(genuine);
    // Copy a valid descriptor under a different full literal identity. Equal
    // bytes and an existing span never establish that identity's membership.
    for (collection, name) in [("s", "other"), ("other", "state")] {
        f.reader
            .facts
            .insert(state_key(collection, name), encode(&fact).unwrap());
        let member = state_evidence(&f, collection, name).unwrap();
        assert!(
            verify_source_state_values(
                &[&member],
                &f.history[1].checked().unwrap(),
                1,
                f.definition.pin(),
                &f.job
            )
            .is_err()
        );
    }
}
#[test]
fn source_state_values_reject_subspans_work_domains_and_foreign_authority() {
    for variant in 0..3 {
        let mut f = fixture();
        let checked = f.history[1].checked().unwrap();
        let QueueLogPayload::Publication(publication) = checked.view().payload() else {
            panic!()
        };
        let raw = if variant == 2 {
            publication
                .receipts()
                .next()
                .unwrap()
                .unwrap()
                .encoded_outcome()
        } else {
            publication
                .commands()
                .map(Result::unwrap)
                .find_map(|c| match c.effect {
                    EffectRef::StatePut { value, .. } if variant == 0 => Some(&value[..0]),
                    EffectRef::WorkPut {
                        body: BodyCommandRef::Replace(value),
                        ..
                    } if variant == 1 => Some(value),
                    _ => None,
                })
                .unwrap()
        };
        let payload = QueueSourcePayload::from_checked(&checked, 1, raw).unwrap();
        let key = state_key("s", "state");
        let mut fact: QueueSourceRecord = decode(&f.reader.facts[&key]).unwrap();
        fact.live = Some(QueueSourceLive::State { value: payload });
        f.reader.facts.insert(key, encode(&fact).unwrap());
        let member = state_evidence(&f, "s", "state").unwrap();
        let baseline = f.admission.usage().unwrap();
        assert!(
            verify_source_state_values(&[&member], &checked, 1, f.definition.pin(), &f.job)
                .is_err()
        );
        assert_eq!(f.admission.usage().unwrap(), baseline);
    }
    let f = fixture();
    let foreign = fixture();
    let member = state_evidence(&f, "s", "state").unwrap();
    let other = state_evidence(&foreign, "s", "state").unwrap();
    let checked = f.history[1].checked().unwrap();
    let baseline = f.admission.usage().unwrap();
    assert!(
        verify_source_state_values(&[&member, &other], &checked, 1, f.definition.pin(), &f.job)
            .is_err()
    );
    assert!(
        verify_source_state_values(
            &[&member],
            &foreign.history[1].checked().unwrap(),
            1,
            f.definition.pin(),
            &f.job
        )
        .is_err()
    );
    assert!(
        verify_source_state_values(&[&member], &checked, 0, f.definition.pin(), &f.job).is_err()
    );
    assert!(
        verify_source_state_values(&[&member], &checked, 1, f.definition.pin(), &foreign.job)
            .is_err()
    );
    let mut schema = f.definition.schema().clone();
    schema.max_attempts += 1;
    let definition =
        AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &f.job).unwrap();
    assert!(verify_source_state_values(&[&member], &checked, 1, definition.pin(), &f.job).is_err());
    drop(definition);
    assert_eq!(f.admission.usage().unwrap(), baseline);
}

#[test]
fn source_state_values_mixed_equal_numbered_same_job_cuts_are_foreign() {
    let f = fixture();
    let first = state_evidence(&f, "s", "state").unwrap();
    let original = f.reader.cut().unwrap();
    let identity = Arc::new(f.job.owned_buffer(128).unwrap());
    let foreign = QueueSourceCut::from_captured(
        &identity,
        original.address(),
        original.epoch(),
        original.through(),
        &f.job,
    )
    .unwrap();
    let key = state_key("s", "state");
    let second = QueueSourceStateValueEvidence::from_source(
        foreign,
        "s",
        "state",
        foreign.point_from_source(&key, Some(&f.reader.facts[&key])),
        &f.job,
    )
    .unwrap()
    .unwrap();
    let checked = f.history[1].checked().unwrap();
    assert!(verify_source_state_values(&[&first], &checked, 1, f.definition.pin(), &f.job).is_ok());
    assert!(
        verify_source_state_values(&[&second], &checked, 1, f.definition.pin(), &f.job).is_ok()
    );
    let baseline = f.admission.usage().unwrap();
    assert!(
        verify_source_state_values(&[&first, &second], &checked, 1, f.definition.pin(), &f.job)
            .is_err()
    );
    assert_eq!(f.admission.usage().unwrap(), baseline);
}

#[test]
fn source_state_values_validate_second_alias_and_reject_zero_offset_at_entry() {
    let f = fixture();
    let first = state_evidence(&f, "s", "state").unwrap();
    let checked = f.history[1].checked().unwrap();
    let QueueLogPayload::Publication(view) = checked.view().payload() else {
        panic!()
    };
    let raw = view
        .commands()
        .map(Result::unwrap)
        .find_map(|command| match command.effect {
            EffectRef::WorkPut {
                body: BodyCommandRef::Replace(bytes),
                ..
            } => Some(bytes),
            _ => None,
        })
        .unwrap();
    let key = state_key("s", "state");
    let mut fact: QueueSourceRecord = decode(&f.reader.facts[&key]).unwrap();
    fact.live = Some(QueueSourceLive::State {
        value: QueueSourcePayload::from_checked(&checked, 1, raw).unwrap(),
    });
    let encoded = encode(&fact).unwrap();
    let cut = f.reader.cut().unwrap();
    let alias = QueueSourceStateValueEvidence::from_source(
        cut,
        "s",
        "state",
        cut.point_from_source(&key, Some(&encoded)),
        &f.job,
    )
    .unwrap()
    .unwrap();
    assert!(
        verify_source_state_values(&[&first, &first], &checked, 1, f.definition.pin(), &f.job)
            .is_ok()
    );
    let baseline = f.admission.usage().unwrap();
    assert!(
        verify_source_state_values(&[&first, &alias], &checked, 1, f.definition.pin(), &f.job)
            .is_err()
    );
    assert_eq!(f.admission.usage().unwrap(), baseline);
    fact.live = Some(QueueSourceLive::State {
        value: QueueSourcePayload::from_checked(&checked, 0, raw).unwrap(),
    });
    let zero = encode(&fact).unwrap();
    assert!(
        QueueSourceStateValueEvidence::from_source(
            cut,
            "s",
            "state",
            cut.point_from_source(&key, Some(&zero)),
            &f.job
        )
        .is_err()
    );
    assert_eq!(f.admission.usage().unwrap(), baseline);
}
