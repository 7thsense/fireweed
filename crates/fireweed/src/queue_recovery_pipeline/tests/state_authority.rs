//! Cached state must agree with the complete checked source, not only its cursor.
use super::*;
use fireweed_engine::{
    queue_contract::QueueStore,
    queue_storage_layout::{discovery_digest, encode},
};
fn state_schema() -> QueueSchema {
    let mut schema = schema();
    schema.state.insert(
        "workflow".into(),
        CollectionDefinition {
            fields: [
                ("phase".into(), FieldType::Text),
                ("score".into(), FieldType::Float),
            ]
            .into(),
            indexes: Default::default(),
            unique_indexes: Default::default(),
            additional_field_prefixes: ["meta/".into()].into(),
        },
    );
    schema
}
fn fields() -> Fields {
    [
        ("phase".into(), FieldValue::Text("running".into())),
        ("score".into(), FieldValue::Float(-0.0)),
        ("meta/\0雪".into(), FieldValue::Bytes(vec![0, 255])),
    ]
    .into()
}
fn state_group(id: &str, updates: Vec<StateUpdate>) -> CommitGroup {
    CommitGroup {
        request_id: id.into(),
        administration: None,
        claimed_updates: vec![],
        work_updates: vec![],
        work_inserts: vec![],
        state_updates: updates,
    }
}
fn update(key: &str, expect: StateExpect, change: StateChange) -> StateUpdate {
    StateUpdate {
        collection: "workflow".into(),
        key: key.into(),
        expect,
        change,
    }
}
fn read(address: &QueueAddress, keys: &[&str], max_bytes: u64) -> StateReadRequest {
    StateReadRequest {
        queue: address.clone(),
        collection: "workflow".into(),
        keys: keys.iter().map(|key| (*key).into()).collect(),
        max_bytes,
    }
}
async fn seed_states(queue: &NativeQueueStore, address: &QueueAddress) -> Vec<Option<StateRecord>> {
    queue
        .provision(ProvisionRequest {
            queue: address.clone(),
            schema: state_schema(),
        })
        .await
        .unwrap();
    let groups = vec![
        state_group(
            "create",
            vec![
                update(
                    "a",
                    StateExpect::Absent,
                    StateChange::Put {
                        fields: fields(),
                        value: bytes::Bytes::from_static(b"exact"),
                    },
                ),
                update(
                    "empty",
                    StateExpect::Absent,
                    StateChange::Put {
                        fields: Fields::new(),
                        value: bytes::Bytes::new(),
                    },
                ),
            ],
        ),
        state_group(
            "tombstone",
            vec![update("tomb", StateExpect::Absent, StateChange::Delete)],
        ),
    ];
    let receipts = queue
        .commit(CommitBatch {
            queue: address.clone(),
            groups,
        })
        .await
        .unwrap();
    assert!(
        receipts
            .iter()
            .all(|receipt| matches!(receipt.outcome, GroupOutcome::Committed { .. }))
    );
    vec![
        Some(StateRecord {
            collection: "workflow".into(),
            key: "a".into(),
            version: 1,
            fields: fields(),
            value: bytes::Bytes::from_static(b"exact"),
        }),
        None,
        Some(StateRecord {
            collection: "workflow".into(),
            key: "empty".into(),
            version: 1,
            fields: Fields::new(),
            value: bytes::Bytes::new(),
        }),
        Some(StateRecord {
            collection: "workflow".into(),
            key: "a".into(),
            version: 1,
            fields: fields(),
            value: bytes::Bytes::from_static(b"exact"),
        }),
        None,
    ]
}
async fn state_read_vector_contract(s3: bool) {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("fireweed-state-vector-{s3}-{nonce}"));
    std::fs::create_dir_all(&root).unwrap();
    let prefix = s3.then(|| format!("api007-state-vector-{nonce}"));
    let clock = Arc::new(fireweed_conformance::queue_contract::TestClock::default());
    let address = QueueAddress {
        tenant: "state-vector".into(),
        queue: "queue".into(),
    };
    let queue = native_port_fixture(&root, "primary.db", prefix.as_deref(), clock.clone()).await;
    let expected = seed_states(&queue, &address).await;
    let keys = ["a", "missing", "empty", "a", "tomb"];
    let exact = fireweed_core::queue_contract::encoded_len(&expected).unwrap();
    assert_eq!(
        queue
            .read_state(read(&address, &keys, exact))
            .await
            .unwrap(),
        expected
    );
    assert_eq!(
        queue.read_state(read(&address, &keys, exact - 1)).await,
        Err(QueueError::TooLarge)
    );
    assert!(
        queue
            .read_state(read(&address, &[], 1))
            .await
            .unwrap()
            .is_empty()
    );
    let groups = vec![
        state_group(
            "delete",
            vec![update("a", StateExpect::Version(1), StateChange::Delete)],
        ),
        state_group(
            "delete-again",
            vec![update("a", StateExpect::Absent, StateChange::Delete)],
        ),
        state_group(
            "recreate",
            vec![update(
                "a",
                StateExpect::Absent,
                StateChange::Put {
                    fields: fields(),
                    value: bytes::Bytes::from_static(b"final"),
                },
            )],
        ),
        state_group(
            "check",
            vec![update("a", StateExpect::Version(4), StateChange::Check)],
        ),
    ];
    assert!(
        queue
            .commit(CommitBatch {
                queue: address.clone(),
                groups
            })
            .await
            .unwrap()
            .iter()
            .all(|receipt| matches!(receipt.outcome, GroupOutcome::Committed { .. }))
    );
    let mut expected = expected;
    for record in expected
        .iter_mut()
        .flatten()
        .filter(|record| record.key == "a")
    {
        record.version = 4;
        record.value = bytes::Bytes::from_static(b"final");
    }
    let result = queue
        .read_state(read(&address, &keys, 65536))
        .await
        .unwrap();
    assert_eq!(result, expected);
    let FieldValue::Float(score) = &result[0].as_ref().unwrap().fields["score"] else {
        panic!()
    };
    assert_eq!(score.to_bits(), (-0.0f64).to_bits());
    queue.shutdown().await.unwrap();
    drop(queue);
    let rebuilt = native_port_fixture(&root, "rebuilt.db", prefix.as_deref(), clock).await;
    assert_eq!(
        rebuilt
            .read_state(read(&address, &keys, 65536))
            .await
            .unwrap(),
        expected
    );
    rebuilt.shutdown().await.unwrap();
    drop(rebuilt);
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn native_state_authority_aliases_delete_recreate_and_budget_local() {
    state_read_vector_contract(false).await;
}
#[tokio::test]
async fn native_state_authority_aliases_delete_recreate_and_budget_real_s3() {
    state_read_vector_contract(true).await;
}

async fn corrupt_state_cache_contract(s3: bool) {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    for case in [
        "missing-live",
        "phantom-live",
        "version",
        "body-same-length",
        "body-oversized",
        "field-same-length",
        "field-oversized",
        "field-missing",
        "float-sign",
        "tomb-version",
        "tomb-missing",
        "absent-tomb",
    ] {
        let root =
            std::env::temp_dir().join(format!("fireweed-state-authority-{s3}-{case}-{nonce}"));
        std::fs::create_dir_all(&root).unwrap();
        let prefix = s3.then(|| format!("api007-state-authority-{case}-{nonce}"));
        let clock = Arc::new(fireweed_conformance::queue_contract::TestClock::default());
        let address = QueueAddress {
            tenant: "state-authority".into(),
            queue: case.into(),
        };
        let queue =
            native_port_fixture(&root, "primary.db", prefix.as_deref(), clock.clone()).await;
        let expected = seed_states(&queue, &address).await;
        queue.shutdown().await.unwrap();
        drop(queue);
        let db = turso::Builder::new_local(root.join("primary.db").to_str().unwrap())
            .build()
            .await
            .unwrap();
        let connection = db.connect().unwrap();
        let key = |key: &[u8]| turso::Value::Blob(key.to_vec());
        let mut request_keys = vec!["empty", "a", "a"];
        match case {
            "missing-live" => {
                connection
                    .execute(
                        "DELETE FROM q7_records WHERE collection=1 AND key=?1",
                        vec![key(b"a")],
                    )
                    .await
                    .unwrap();
            }
            "phantom-live" | "absent-tomb" => {
                let source = if case == "phantom-live" {
                    b"empty".as_slice()
                } else {
                    b"tomb".as_slice()
                };
                connection.execute("INSERT INTO q7_records(queue_id,collection,key_hash,key,version,active,lifecycle,priority,available_at,eligible_since,attempts,lease_until,lease_hash,insertion_sequence,last_change_offset,prior_highwater) SELECT queue_id,collection,?2,?3,version,active,lifecycle,priority,available_at,eligible_since,attempts,lease_until,lease_hash,insertion_sequence,last_change_offset,prior_highwater FROM q7_records WHERE collection=1 AND key=?1",vec![key(source),key(&discovery_digest(b"phantom")),key(b"phantom")]).await.unwrap();
                if case == "phantom-live" {
                    connection.execute("INSERT INTO q7_bodies(record_id,value) SELECT id,?2 FROM q7_records WHERE collection=1 AND key=?1",vec![key(b"phantom"),key(b"")]).await.unwrap();
                }
                request_keys = vec!["empty", "phantom", "a"];
            }
            "version" | "tomb-version" => {
                let target = if case == "version" {
                    b"a".as_slice()
                } else {
                    b"tomb".as_slice()
                };
                connection
                    .execute(
                        "UPDATE q7_records SET version=?2 WHERE collection=1 AND key=?1",
                        vec![key(target), key(&2u64.to_be_bytes())],
                    )
                    .await
                    .unwrap();
                if case == "tomb-version" {
                    request_keys = vec!["empty", "tomb", "a"];
                }
            }
            "tomb-missing" => {
                connection
                    .execute(
                        "DELETE FROM q7_records WHERE collection=1 AND key=?1",
                        vec![key(b"tomb")],
                    )
                    .await
                    .unwrap();
                request_keys = vec!["empty", "tomb", "a"];
            }
            "body-same-length" => {
                connection.execute("UPDATE q7_bodies SET value=?2 WHERE record_id IN (SELECT id FROM q7_records WHERE collection=1 AND key=?1)",vec![key(b"a"),key(b"wrong")]).await.unwrap();
            }
            "body-oversized" => {
                connection.execute("UPDATE q7_bodies SET value=zeroblob(8388608) WHERE record_id IN (SELECT id FROM q7_records WHERE collection=1 AND key=?1)",vec![key(b"a")]).await.unwrap();
            }
            "field-same-length" | "float-sign" => {
                let (field, value) = if case == "float-sign" {
                    (1, FieldValue::Float(0.0))
                } else {
                    (0, FieldValue::Text("waiting".into()))
                };
                connection.execute("UPDATE q7_fields SET value=?2 WHERE field=?3 AND record_id IN (SELECT id FROM q7_records WHERE collection=1 AND key=?1)",vec![key(b"a"),key(&encode(&value).unwrap()),turso::Value::Integer(field)]).await.unwrap();
            }
            "field-oversized" => {
                connection.execute("UPDATE q7_fields SET value=zeroblob(8388608) WHERE field=0 AND record_id IN (SELECT id FROM q7_records WHERE collection=1 AND key=?1)",vec![key(b"a")]).await.unwrap();
            }
            "field-missing" => {
                connection.execute("DELETE FROM q7_fields WHERE field=0 AND record_id IN (SELECT id FROM q7_records WHERE collection=1 AND key=?1)",vec![key(b"a")]).await.unwrap();
            }
            _ => unreachable!(),
        }
        drop(connection);
        drop(db);
        if case == "phantom-live" {
            prove_unchecked_phantom_readable(&root.join("primary.db"), &address).await;
        }
        let reopened =
            native_port_fixture(&root, "primary.db", prefix.as_deref(), clock.clone()).await;
        assert!(
            matches!(
                reopened
                    .read_state(read(&address, &request_keys, 65536))
                    .await,
                Err(QueueError::OutcomeUnknown(_))
            ),
            "{s3} {case}: no partial results or cache-driven known classification"
        );
        reopened.shutdown().await.unwrap();
        drop(reopened);
        let rebuilt = native_port_fixture(&root, "rebuilt.db", prefix.as_deref(), clock).await;
        assert_eq!(
            rebuilt
                .read_state(read(
                    &address,
                    &["a", "missing", "empty", "a", "tomb"],
                    65536
                ))
                .await
                .unwrap(),
            expected,
            "{s3} {case}: exact source rebuild"
        );
        assert_eq!(
            rebuilt
                .read_state(read(&address, &["phantom"], 65536))
                .await
                .unwrap(),
            vec![None]
        );
        rebuilt.shutdown().await.unwrap();
        drop(rebuilt);
        let db = turso::Builder::new_local(root.join("rebuilt.db").to_str().unwrap())
            .build()
            .await
            .unwrap();
        let connection = db.connect().unwrap();
        let mut rows = connection
            .query("SELECT applied_next_offset FROM q7_queues", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get_value(0).unwrap(),
            turso::Value::Integer(3),
            "integrity failure must not append work"
        );
        drop(rows);
        drop(connection);
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }
}
#[tokio::test]
async fn native_state_authority_rejects_corrupt_cache_local() {
    corrupt_state_cache_contract(false).await;
}
#[tokio::test]
async fn native_state_authority_rejects_corrupt_cache_real_s3() {
    corrupt_state_cache_contract(true).await;
}

/// Characterize the old unproved native path: this phantom has valid metadata,
/// an empty body and no fields, so source absence is the only new rejection.
async fn prove_unchecked_phantom_readable(path: &std::path::Path, address: &QueueAddress) {
    let db = turso::Builder::new_local(path.to_str().unwrap())
        .build()
        .await
        .unwrap();
    let connection = db.connect().unwrap();
    let mut rows=connection.query("SELECT definition,cursor_secret,applied_next_offset,next_sequence,last_record_epoch FROM q7_queues",()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let blob = |index| match row.get_value(index).unwrap() {
        turso::Value::Blob(value) => value,
        _ => panic!("invalid fixture metadata"),
    };
    let raw = blob(0);
    let secret: [u8; 32] = blob(1).try_into().unwrap();
    let head = match row.get_value(2).unwrap() {
        turso::Value::Integer(value) => value as u64,
        _ => panic!(),
    };
    let sequence = u64::from_be_bytes(blob(3).try_into().unwrap());
    let epoch = u64::from_be_bytes(blob(4).try_into().unwrap());
    drop(row);
    drop(rows);
    drop(connection);
    drop(db);
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let job = Arc::new(
        QueueAdmission::new(1, 64 << 20)
            .unwrap()
            .reserve(0)
            .unwrap(),
    );
    let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
    let genesis = encode_checked_genesis(1, address, &definition, &secret, 1 << 20, &job).unwrap();
    let projection = QueueProjection::open(path, &job, &workers).await.unwrap();
    let result = projection
        .read_state_records(
            fireweed_turso::queue_projection::StateReadInput {
                authority: None,
                genesis,
                request: read(address, &["phantom"], 65536),
                limits: QueueLimits::default(),
                required_next_offset: head,
                required_insertion_sequence: sequence,
                required_last_record_epoch: epoch,
            },
            &job,
            &workers,
        )
        .await
        .unwrap();
    assert_eq!(
        result.records(),
        &[Some(StateRecord {
            collection: "workflow".into(),
            key: "phantom".into(),
            version: 1,
            fields: Fields::new(),
            value: bytes::Bytes::new()
        })]
    );
    drop(result);
    drop(projection);
    drop(definition);
    drop(job);
    workers.close_and_drain().await.unwrap();
}

async fn state_preparation_cache_contract(s3: bool) {
    for (case, change, succeeds) in [
        ("check-unused", StateChange::Check, true),
        (
            "put-unused-body",
            StateChange::Put {
                fields: fields(),
                value: bytes::Bytes::from_static(b"next"),
            },
            true,
        ),
        ("delete-unused-body", StateChange::Delete, true),
        ("check-missing", StateChange::Check, false),
        ("check-hwm", StateChange::Check, false),
        (
            "put-field",
            StateChange::Put {
                fields: fields(),
                value: bytes::Bytes::new(),
            },
            false,
        ),
        ("delete-field", StateChange::Delete, false),
    ] {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("fireweed-state-preparation-{s3}-{case}-{nonce}"));
        std::fs::create_dir_all(&root).unwrap();
        let prefix = s3.then(|| format!("api007-state-preparation-{case}-{nonce}"));
        let address = QueueAddress {
            tenant: "state-preparation".into(),
            queue: case.into(),
        };
        let clock = Arc::new(fireweed_conformance::queue_contract::TestClock::default());
        let queue =
            native_port_fixture(&root, "primary.db", prefix.as_deref(), clock.clone()).await;
        seed_states(&queue, &address).await;
        queue.shutdown().await.unwrap();
        drop(queue);
        let db = turso::Builder::new_local(root.join("primary.db").to_str().unwrap())
            .build()
            .await
            .unwrap();
        let connection = db.connect().unwrap();
        let key = turso::Value::Blob(b"a".to_vec());
        let sql = match case {
            "check-unused" | "put-unused-body" | "delete-unused-body" => {
                "DELETE FROM q7_bodies WHERE record_id IN (SELECT id FROM q7_records WHERE collection=1 AND key=?1)"
            }
            "check-missing" => "DELETE FROM q7_records WHERE collection=1 AND key=?1",
            "check-hwm" => "UPDATE q7_records SET version=?2 WHERE collection=1 AND key=?1",
            "put-field" => {
                "UPDATE q7_fields SET value=?2 WHERE field=0 AND record_id IN (SELECT id FROM q7_records WHERE collection=1 AND key=?1)"
            }
            "delete-field" => {
                "UPDATE q7_fields SET value=zeroblob(8388608) WHERE field=0 AND record_id IN (SELECT id FROM q7_records WHERE collection=1 AND key=?1)"
            }
            _ => unreachable!(),
        };
        let mut params = vec![key.clone()];
        if case == "check-hwm" {
            params.push(turso::Value::Blob(2u64.to_be_bytes().to_vec()));
        }
        if case == "put-field" {
            params.push(turso::Value::Blob(
                encode(&FieldValue::Text("waiting".into())).unwrap(),
            ));
        }
        connection.execute(sql, params).await.unwrap();
        if case == "check-unused" {
            connection.execute("UPDATE q7_fields SET value=zeroblob(8388608) WHERE record_id IN (SELECT id FROM q7_records WHERE collection=1 AND key=?1)", vec![key]).await.unwrap();
        }
        drop(connection);
        drop(db);
        let queue =
            native_port_fixture(&root, "primary.db", prefix.as_deref(), clock.clone()).await;
        // Immutable originals remain replayable even when current state is corrupt.
        let original = state_group(
            "create",
            vec![
                update(
                    "a",
                    StateExpect::Absent,
                    StateChange::Put {
                        fields: fields(),
                        value: bytes::Bytes::from_static(b"exact"),
                    },
                ),
                update(
                    "empty",
                    StateExpect::Absent,
                    StateChange::Put {
                        fields: Fields::new(),
                        value: bytes::Bytes::new(),
                    },
                ),
            ],
        );
        let replay = queue
            .commit(CommitBatch {
                queue: address.clone(),
                groups: vec![original],
            })
            .await
            .unwrap();
        assert!(
            matches!(replay[0].outcome, GroupOutcome::Committed { .. }),
            "{s3} {case}: immutable original replay"
        );
        let group = state_group("fresh", vec![update("a", StateExpect::Version(1), change)]);
        let preview = queue
            .preview(PreviewRequest {
                queue: address.clone(),
                group: group.clone(),
            })
            .await;
        let committed = queue
            .commit(CommitBatch {
                queue: address.clone(),
                groups: vec![group],
            })
            .await;
        if succeeds {
            assert!(
                matches!(
                    preview,
                    Ok(PreviewResult {
                        outcome: PreviewOutcome::Committed { .. }
                    })
                ),
                "{s3} {case}: {preview:?}"
            );
            assert!(
                matches!(&committed, Ok(receipts) if matches!(receipts[0].outcome, GroupOutcome::Committed { .. })),
                "{s3} {case}: {committed:?}"
            );
        } else {
            assert!(
                matches!(preview, Err(QueueError::OutcomeUnknown(_))),
                "{s3} {case}: {preview:?}"
            );
            assert!(
                matches!(committed, Err(QueueError::OutcomeUnknown(_))),
                "{s3} {case}: {committed:?}"
            );
        }
        queue.shutdown().await.unwrap();
        drop(queue);
        let rebuilt = native_port_fixture(&root, "rebuilt.db", prefix.as_deref(), clock).await;
        let restored = rebuilt
            .read_state(read(&address, &["a"], 65536))
            .await
            .unwrap();
        assert_eq!(
            restored[0].as_ref().map(|record| record.version),
            if case == "delete-unused-body" {
                None
            } else if case == "put-unused-body" {
                Some(2)
            } else {
                Some(1)
            }
        );
        rebuilt.shutdown().await.unwrap();
        drop(rebuilt);
        let db = turso::Builder::new_local(root.join("rebuilt.db").to_str().unwrap())
            .build()
            .await
            .unwrap();
        let connection = db.connect().unwrap();
        let mut rows = connection
            .query("SELECT applied_next_offset FROM q7_queues", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get_value(0).unwrap(),
            turso::Value::Integer(if succeeds { 4 } else { 3 }),
            "preview and rejected preparation cannot append"
        );
        drop(rows);
        drop(connection);
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }
}
#[tokio::test]
async fn native_state_preparation_source_cache_local() {
    state_preparation_cache_contract(false).await;
}
#[tokio::test]
async fn native_state_preparation_source_cache_real_s3() {
    state_preparation_cache_contract(true).await;
}

async fn state_preparation_administration_contract(s3: bool) {
    use fireweed_core::queue_administration::{
        AdministrativeAction, AdministrativeEntry, AdministrativeIntent, AdministrativeOperation,
        WorkSelector,
    };
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("fireweed-state-admin-proof-{s3}-{nonce}"));
    std::fs::create_dir_all(&root).unwrap();
    let prefix = s3.then(|| format!("api007-state-admin-proof-{nonce}"));
    let clock = Arc::new(fireweed_conformance::queue_contract::TestClock::default());
    let address = QueueAddress {
        tenant: "state-admin-proof".into(),
        queue: "queue".into(),
    };
    let queue = native_port_fixture(&root, "primary.db", prefix.as_deref(), clock.clone()).await;
    queue
        .provision(ProvisionRequest {
            queue: address.clone(),
            schema: state_schema(),
        })
        .await
        .unwrap();
    let administrative = |id: &str, expect| {
        let mut group = state_group(
            id,
            vec![update(
                "a",
                expect,
                StateChange::Put {
                    fields: fields(),
                    value: bytes::Bytes::from_static(b"exact"),
                },
            )],
        );
        group.administration = Some(AdministrativeCommit {
            max_result_bytes: 65536,
            intent: AdministrativeIntent {
                evaluated_at: 0,
                before_fields: None,
                operation: AdministrativeOperation::Addressed(vec![AdministrativeEntry {
                    key: "missing".into(),
                    expected_version: None,
                    precondition: WorkSelector::all(vec![]),
                    action: AdministrativeAction::Delete {
                        lease_guard: AdministrativeLeaseGuard::RejectActive,
                    },
                }]),
            },
        });
        group
    };
    let original = administrative("original-fp4", StateExpect::Absent);
    let recorded = queue
        .commit(CommitBatch {
            queue: address.clone(),
            groups: vec![original.clone()],
        })
        .await
        .unwrap();
    assert!(
        matches!(&recorded[0].outcome, GroupOutcome::AdministrativeCommitted { versions, .. } if versions == &vec![RecordVersion { collection: Some("workflow".into()), key: "a".into(), version: 1 }])
    );
    queue.shutdown().await.unwrap();
    drop(queue);
    let db = turso::Builder::new_local(root.join("primary.db").to_str().unwrap())
        .build()
        .await
        .unwrap();
    let connection = db.connect().unwrap();
    connection.execute("UPDATE q7_fields SET value=?1 WHERE field=0 AND record_id IN (SELECT id FROM q7_records WHERE collection=1)", vec![turso::Value::Blob(encode(&FieldValue::Text("waiting".into())).unwrap())]).await.unwrap();
    drop(connection);
    drop(db);
    let queue = native_port_fixture(&root, "primary.db", prefix.as_deref(), clock.clone()).await;
    assert_eq!(
        queue
            .commit(CommitBatch {
                queue: address.clone(),
                groups: vec![original]
            })
            .await
            .unwrap(),
        recorded,
        "FP4 original replay precedes current-state cache checks"
    );
    let fresh = administrative("fresh-fp4", StateExpect::Version(1));
    assert!(
        matches!(
            queue
                .preview(PreviewRequest {
                    queue: address.clone(),
                    group: fresh.clone()
                })
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ),
        "FP4 preview must consume proved old fields"
    );
    assert!(
        matches!(
            queue
                .commit(CommitBatch {
                    queue: address.clone(),
                    groups: vec![fresh]
                })
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ),
        "FP4 publication must consume proved old fields"
    );
    queue.shutdown().await.unwrap();
    drop(queue);
    let rebuilt = native_port_fixture(&root, "rebuilt.db", prefix.as_deref(), clock).await;
    assert_eq!(
        rebuilt
            .read_state(read(&address, &["a"], 65536))
            .await
            .unwrap()[0]
            .as_ref()
            .unwrap()
            .version,
        1
    );
    rebuilt.shutdown().await.unwrap();
    drop(rebuilt);
    let db = turso::Builder::new_local(root.join("rebuilt.db").to_str().unwrap())
        .build()
        .await
        .unwrap();
    let connection = db.connect().unwrap();
    let mut rows = connection
        .query("SELECT applied_next_offset FROM q7_queues", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get_value(0).unwrap(),
        turso::Value::Integer(2)
    );
    drop(rows);
    drop(connection);
    drop(db);
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn native_state_preparation_administration_local() {
    state_preparation_administration_contract(false).await;
}
#[tokio::test]
async fn native_state_preparation_administration_real_s3() {
    state_preparation_administration_contract(true).await;
}
