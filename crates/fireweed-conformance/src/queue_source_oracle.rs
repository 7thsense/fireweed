//! Independent full-history source-map oracle for backend conformance tests.
//! Uses cold snapshots and raw history, never compiler reducers or posting helpers.
/// Instantiate the cold oracle with the caller's domain crate identity.
/// Domain unit tests and backend tests therefore share code without type conversion.
#[macro_export]
macro_rules! define_queue_source_oracle {
    ($engine:ident) => {
        use fireweed_core::queue_contract::*;
        use std::collections::BTreeMap;
        use $engine::{
            queue_group_state_authority::WorkMetadata,
            queue_log_record::{OwnedQueueLogRecord, QueueLogPayload},
            queue_planner::{RecordIdentity, RecordSnapshot},
            queue_publication::{BodyCommandRef, EffectRef},
            queue_source_index::{
                QueueSourceGenesis, QueueSourceKey, QueueSourceLive, QueueSourceOriginal,
                QueueSourcePayload, QueueSourceRecord, QueueSourceSpan,
            },
            queue_storage_layout,
        };

        pub fn fold(
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
                                QueueSourcePayload::from_checked(&checked, offset as u64, bytes)
                                    .unwrap(),
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
                    let sort = $engine::queue_index::ordered_key(&[Some(value)]).unwrap();
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
                    let sort = $engine::queue_index::ordered_key(&values).unwrap();
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
                    if definition.unique_indexes.contains(index)
                        && values.iter().all(Option::is_some)
                    {
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
            for (key, value) in inventory(snapshot) {
                assert!(output.insert(key, value).is_none());
            }
            output
        }

        pub fn inventory(snapshot: &RecordSnapshot) -> BTreeMap<Vec<u8>, Vec<u8>> {
            fn escaped(out: &mut Vec<u8>, value: &str) {
                for byte in value.bytes() {
                    if byte == 0 {
                        out.extend_from_slice(&[0, 255]);
                    } else {
                        out.push(byte);
                    }
                }
                out.extend_from_slice(&[0, 0]);
            }
            fn signed(n: i64) -> [u8; 8] {
                ((n as u64) ^ (1u64 << 63)).to_be_bytes()
            }
            let mut out = BTreeMap::new();
            for (literal, stored) in &snapshot.work {
                let row = &stored.record;
                let mut key = vec![1, 10, 0];
                escaped(&mut key, literal);
                out.insert(key, vec![]);
                let lifecycle = match row.lifecycle {
                    fireweed_core::queue_contract::Lifecycle::Pending => 0,
                    fireweed_core::queue_contract::Lifecycle::Leased => 1,
                    fireweed_core::queue_contract::Lifecycle::Completed => 2,
                    fireweed_core::queue_contract::Lifecycle::Failed => 3,
                };
                let mut tuples = vec![
                    (0, vec![lifecycle]),
                    (1, row.attempts.to_be_bytes().to_vec()),
                ];
                if let Some(expiry) = row.lease_expires_at {
                    tuples.push((2, signed(expiry).to_vec()));
                }
                let candidate = lifecycle <= 1;
                for (kind, overdue, descending, scheduled) in [
                    (3, false, false, false),
                    (4, false, true, false),
                    (5, false, false, true),
                    (6, true, false, false),
                    (7, true, true, false),
                    (8, true, false, true),
                ] {
                    if kind != 5 && !candidate {
                        continue;
                    }
                    let mut tuple = Vec::new();
                    if overdue {
                        tuple.extend_from_slice(&signed(row.eligible_since));
                    }
                    let mut primary = signed(if scheduled {
                        row.work.available_at
                    } else {
                        row.work.priority
                    });
                    if descending {
                        for byte in &mut primary {
                            *byte = !*byte;
                        }
                    }
                    tuple.extend_from_slice(&primary);
                    tuple.extend_from_slice(&row.insertion_sequence.to_be_bytes());
                    tuples.push((kind, tuple));
                }
                for (kind, tuple) in tuples {
                    let mut key = vec![1, 11, kind];
                    key.extend_from_slice(&tuple);
                    escaped(&mut key, literal);
                    assert!(out.insert(key, vec![]).is_none());
                }
            }
            for (collection, literal) in snapshot.state.keys() {
                let mut key = vec![1, 10, 1];
                escaped(&mut key, collection);
                escaped(&mut key, literal);
                assert!(out.insert(key, vec![]).is_none());
            }
            out
        }
    };
}
crate::define_queue_source_oracle!(fireweed_engine);
