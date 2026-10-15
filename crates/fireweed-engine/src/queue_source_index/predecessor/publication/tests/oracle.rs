//! Test-only full history/snapshot oracle. It never calls the compiler, a
//! transition visitor, a provisional reducer, or intrinsic posting derivation.
use super::*;
use crate::{
    queue_group_state_authority::WorkMetadata,
    queue_planner::{RecordIdentity, RecordSnapshot},
    queue_publication::{BodyCommandRef, EffectRef},
    queue_source_index::{QueueSourceOriginal, QueueSourcePayload, QueueSourceSpan},
};
use std::collections::BTreeMap;

pub(super) fn fold(
    history: &[OwnedQueueLogRecord],
    snapshot: &RecordSnapshot,
    schema: &QueueSchema,
) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut output = BTreeMap::new();
    let mut payloads = BTreeMap::new();
    for (offset, record) in history.iter().enumerate() {
        let checked = record.checked().unwrap();
        let publication = match checked.view().payload() {
            QueueLogPayload::Genesis(genesis) => {
                assert_eq!(offset, 0);
                output.insert(
                    QueueSourceKey::Genesis.encode().unwrap(),
                    queue_storage_layout::encode(&QueueSourceGenesis {
                        source: QueueSourceSpan::from_checked(
                            &checked,
                            0,
                            &record.bytes()[checked.payload_interval().unwrap()],
                        )
                        .unwrap(),
                        definition_pin: genesis.definition_pin(),
                    })
                    .unwrap(),
                );
                continue;
            }
            QueueLogPayload::Publication(publication) => {
                for receipt in publication.receipts() {
                    let receipt = receipt.unwrap();
                    output.insert(
                        QueueSourceKey::Original {
                            domain: receipt.domain,
                            request_id: receipt.request_id,
                        }
                        .encode()
                        .unwrap(),
                        queue_storage_layout::encode(&QueueSourceOriginal {
                            fingerprint_format: receipt.fingerprint_format,
                            fingerprint: *receipt.fingerprint,
                            outcome: QueueSourcePayload::from_checked(
                                &checked,
                                offset as u64,
                                receipt.encoded_outcome(),
                            )
                            .unwrap(),
                        })
                        .unwrap(),
                    );
                }
                publication
            }
            QueueLogPayload::Claim(claim) => {
                output.insert(
                    QueueSourceKey::Original {
                        domain: 1,
                        request_id: claim.request_id(),
                    }
                    .encode()
                    .unwrap(),
                    queue_storage_layout::encode(&QueueSourceOriginal {
                        fingerprint_format: claim.fingerprint_format(),
                        fingerprint: *claim.fingerprint(),
                        outcome: QueueSourcePayload::from_checked(
                            &checked,
                            offset as u64,
                            claim.receipt_bytes(),
                        )
                        .unwrap(),
                    })
                    .unwrap(),
                );
                claim.commands()
            }
        };
        for command in publication.commands() {
            let command = command.unwrap();
            let identity = RecordIdentity {
                collection: command.identity.collection.map(str::to_owned),
                key: command.identity.key.into(),
            };
            match command.effect {
                EffectRef::Delete => {
                    payloads.remove(&identity);
                }
                EffectRef::WorkPut {
                    body: BodyCommandRef::Replace(bytes),
                    ..
                }
                | EffectRef::StatePut { value: bytes, .. } => {
                    payloads.insert(
                        identity,
                        QueueSourcePayload::from_checked(&checked, offset as u64, bytes).unwrap(),
                    );
                }
                EffectRef::WorkPut {
                    body: BodyCommandRef::Preserve,
                    ..
                } => {
                    assert!(payloads.contains_key(&identity));
                }
            }
        }
    }
    output.insert(
        QueueSourceKey::Sequence.encode().unwrap(),
        queue_storage_layout::encode(&snapshot.next_sequence).unwrap(),
    );
    for lifecycle in [
        Lifecycle::Pending,
        Lifecycle::Leased,
        Lifecycle::Completed,
        Lifecycle::Failed,
    ] {
        let count = snapshot
            .work
            .values()
            .filter(|row| row.record.lifecycle == lifecycle)
            .count() as u64;
        output.insert(
            QueueSourceKey::LifecycleCount(lifecycle).encode().unwrap(),
            queue_storage_layout::encode(&count).unwrap(),
        );
    }
    for (identity, highwater) in &snapshot.versions {
        let row = match &identity.collection {
            None => snapshot.work.get(&identity.key).map(|stored| {
                let row = &stored.record;
                (
                    QueueSourceLive::Work {
                        metadata: WorkMetadata {
                            version: row.version,
                            priority: row.work.priority,
                            available_at: row.work.available_at,
                            insertion_sequence: row.insertion_sequence,
                            lifecycle: row.lifecycle,
                            eligible_since: row.eligible_since,
                            attempts: row.attempts,
                            lease_expires_at: row.lease_expires_at,
                            lease_hash: stored.lease_hash,
                        },
                        body: payloads[identity],
                    },
                    &row.work.fields,
                    &schema.work,
                )
            }),
            Some(collection) => snapshot
                .state
                .get(&(collection.clone(), identity.key.clone()))
                .map(|row| {
                    (
                        QueueSourceLive::State {
                            value: payloads[identity],
                        },
                        &row.fields,
                        &schema.state[collection],
                    )
                }),
        };
        let fact = QueueSourceRecord {
            highwater: *highwater,
            field_count: row.as_ref().map_or(0, |(_, fields, _)| fields.len() as u64),
            live: row.as_ref().map(|(live, _, _)| *live),
        };
        output.insert(
            QueueSourceKey::Record {
                collection: identity.collection.as_deref(),
                key: &identity.key,
            }
            .encode()
            .unwrap(),
            queue_storage_layout::encode(&fact).unwrap(),
        );
        let Some((_, fields, definition)) = row else {
            continue;
        };
        for (field, value) in fields {
            output.insert(
                QueueSourceKey::Field {
                    collection: identity.collection.as_deref(),
                    key: &identity.key,
                    field,
                }
                .encode()
                .unwrap(),
                queue_storage_layout::encode(value).unwrap(),
            );
            let sort = crate::queue_index::ordered_key(&[Some(value)]).unwrap();
            output.insert(
                QueueSourceKey::ScalarPosting {
                    collection: identity.collection.as_deref(),
                    key: &identity.key,
                    field,
                    sort: &sort,
                }
                .encode()
                .unwrap(),
                Vec::new(),
            );
        }
        for (index, columns) in &definition.indexes {
            let values: Vec<_> = columns.iter().map(|name| fields.get(name)).collect();
            let sort = crate::queue_index::ordered_key(&values).unwrap();
            output.insert(
                QueueSourceKey::NamedPosting {
                    collection: identity.collection.as_deref(),
                    key: &identity.key,
                    index,
                    sort: &sort,
                }
                .encode()
                .unwrap(),
                Vec::new(),
            );
            if definition.unique_indexes.contains(index) && values.iter().all(Option::is_some) {
                assert!(
                    output
                        .insert(
                            QueueSourceKey::UniqueOwner {
                                collection: identity.collection.as_deref(),
                                index,
                                sort: &sort
                            }
                            .encode()
                            .unwrap(),
                            queue_storage_layout::encode(identity).unwrap()
                        )
                        .is_none()
                );
            }
        }
    }
    for (key, value) in super::super::super::intrinsics::tests::inventory(snapshot) {
        assert!(output.insert(key, value).is_none());
    }
    output
}
