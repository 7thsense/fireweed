use fireweed_core::queue_contract::*;
use fireweed_engine::{Clock, queue_contract::QueueStore};
use fireweed_postgres::PostgresQueueStore;
use std::sync::Arc;
struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> fireweed_core::UtcTimestamp {
        timestamp_from_nanos(0)
    }
}
#[tokio::test]
#[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
async fn condition_checks_use_metadata_admission_and_preserve_unique_owners() {
    use std::collections::{BTreeMap, BTreeSet};
    let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
    let queue = QueueAddress {
        tenant: "api007-check-admission".into(),
        queue: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .to_string(),
    };
    let mut schema = fireweed_conformance::queue_contract::schema();
    schema.work.fields.insert("opaque".into(), FieldType::Bytes);
    let state = schema.state.get_mut("instances").unwrap();
    state.fields.insert("opaque".into(), FieldType::Bytes);
    state.unique_indexes.insert("by_run".into());
    let seed = PostgresQueueStore::connect(&url, Arc::new(FixedClock), QueueLimits::default(), 1)
        .await
        .unwrap();
    seed.provision(ProvisionRequest {
        queue: queue.clone(),
        schema,
    })
    .await
    .unwrap();
    let mut group = CommitGroup {
        request_id: "wide-seed".into(),
        claimed_updates: vec![],
        state_updates: vec![StateUpdate {
            collection: "instances".into(),
            key: "owner".into(),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: BTreeMap::from([
                    ("run".into(), FieldValue::Text("owner".into())),
                    ("opaque".into(), FieldValue::Bytes(vec![7; 1 << 20])),
                ]),
                value: vec![8; 1 << 20].into(),
            },
        }],
        work_inserts: vec![NewWork {
            key: "owner".into(),
            priority: 0,
            available_at: 0,
            fields: BTreeMap::from([
                ("target".into(), FieldValue::Text("owner".into())),
                ("opaque".into(), FieldValue::Bytes(vec![9; 1 << 20])),
            ]),
            body: vec![10; 1 << 20].into(),
        }],
        work_updates: vec![],
        administration: None,
    };
    assert!(matches!(
        seed.commit(CommitBatch {
            queue: queue.clone(),
            groups: vec![group.clone()]
        })
        .await
        .unwrap()[0]
            .outcome,
        GroupOutcome::Committed { .. }
    ));
    let claimed = seed
        .claim(ClaimRequest {
            queue: queue.clone(),
            request_id: "lease".into(),
            filter: Filter::default(),
            order: ClaimOrder::PriorityAscending,
            eligible_at: 0,
            lease_nanos: 1_000_000,
            max_records: 1,
            max_bytes: 65536,
            fields: Some(BTreeSet::new()),
            include_body: false,
        })
        .await
        .unwrap()
        .remove(0);
    seed.shutdown().await.unwrap();
    let tight = PostgresQueueStore::connect_with_runtime(
        &url,
        Arc::new(FixedClock),
        QueueLimits {
            max_response_bytes: 65536,
            ..Default::default()
        },
        1,
        fireweed_core::queue_runtime::QueueRuntimeOptions {
            retained_bytes: Some(512 << 10),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    group.request_id = "metadata-only-checks".into();
    group.work_inserts.clear();
    group.claimed_updates.push(ClaimedUpdate {
        key: "owner".into(),
        lease: claimed.lease.clone(),
        expected_version: claimed.record.version,
        patch: WorkPatch::default(),
        disposition: Disposition::Check,
    });
    group.state_updates[0].expect = StateExpect::Version(1);
    group.state_updates[0].change = StateChange::Check;
    let original = tight
        .commit(CommitBatch {
            queue: queue.clone(),
            groups: vec![group.clone()],
        })
        .await
        .unwrap();
    assert_eq!(
        original[0].outcome,
        GroupOutcome::Committed { versions: vec![] }
    );
    let observer =
        PostgresQueueStore::connect(&url, Arc::new(FixedClock), QueueLimits::default(), 1)
            .await
            .unwrap();
    let state = observer
        .read_state(StateReadRequest {
            queue: queue.clone(),
            collection: "instances".into(),
            keys: vec!["owner".into()],
            max_bytes: 8 << 20,
        })
        .await
        .unwrap()
        .remove(0)
        .unwrap();
    assert_eq!(state.version, 1);
    assert_eq!(state.fields["opaque"], FieldValue::Bytes(vec![7; 1 << 20]));
    assert_eq!(state.value.as_ref(), vec![8; 1 << 20]);
    let page = observer
        .query(QueryRequest {
            queue: queue.clone(),
            source: RecordSource::Work,
            index: "$key".into(),
            filter: Filter::default(),
            descending: false,
            cursor: None,
            max_records: 1,
            max_bytes: 8 << 20,
        })
        .await
        .unwrap();
    let QueryRecord::Work(work) = &page.records[0] else {
        panic!("expected seeded work")
    };
    assert_eq!(work.version, claimed.record.version);
    assert_eq!(
        work.work.fields["opaque"],
        FieldValue::Bytes(vec![9; 1 << 20])
    );
    assert_eq!(work.work.body.as_ref(), vec![10; 1 << 20]);
    observer.shutdown().await.unwrap();
    let mut work_conflict = group.clone();
    work_conflict.request_id = "checked-work-owner-still-unique".into();
    work_conflict.work_inserts.push(NewWork {
        key: "duplicate".into(),
        priority: 0,
        available_at: 0,
        fields: BTreeMap::from([("target".into(), FieldValue::Text("owner".into()))]),
        body: vec![1].into(),
    });
    assert_eq!(
        tight
            .commit(CommitBatch {
                queue: queue.clone(),
                groups: vec![work_conflict]
            })
            .await
            .unwrap()[0]
            .outcome,
        GroupOutcome::Rejected(Rejection::UniqueConflict)
    );
    let mut conflict = group.clone();
    conflict.request_id = "checked-state-owner-still-unique".into();
    conflict.state_updates.push(StateUpdate {
        collection: "instances".into(),
        key: "duplicate".into(),
        expect: StateExpect::Absent,
        change: StateChange::Put {
            fields: BTreeMap::from([("run".into(), FieldValue::Text("owner".into()))]),
            value: vec![1].into(),
        },
    });
    assert_eq!(
        tight
            .commit(CommitBatch {
                queue: queue.clone(),
                groups: vec![conflict]
            })
            .await
            .unwrap()[0]
            .outcome,
        GroupOutcome::Rejected(Rejection::UniqueConflict)
    );
    let mut complete = group.clone();
    complete.request_id = "same-credential-still-valid".into();
    complete.claimed_updates[0].disposition = Disposition::Complete;
    let result = tight
        .commit(CommitBatch {
            queue: queue.clone(),
            groups: vec![complete],
        })
        .await
        .unwrap();
    assert_eq!(
        result[0].outcome,
        GroupOutcome::Committed {
            versions: vec![RecordVersion {
                collection: None,
                key: "owner".into(),
                version: claimed.record.version + 1,
            }]
        }
    );
    assert_eq!(
        tight
            .commit(CommitBatch {
                queue,
                groups: vec![group]
            })
            .await
            .unwrap(),
        original
    );
    tight.shutdown().await.unwrap();
}
#[tokio::test]
#[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
async fn mandatory_shared_queue_contract_and_reopen() {
    let url =
        std::env::var("FIREWEED_API007_PG_TEST_URL").expect("isolated PostgreSQL fixture URL");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let queue = QueueAddress {
        tenant: "api007-conformance".into(),
        queue: nonce.to_string(),
    };
    let store = PostgresQueueStore::connect(&url, Arc::new(FixedClock), QueueLimits::default(), 2)
        .await
        .unwrap();
    fireweed_conformance::queue_contract::run(&store, queue.clone()).await;
    store.shutdown().await.unwrap();
    let reopened =
        PostgresQueueStore::connect(&url, Arc::new(FixedClock), QueueLimits::default(), 2)
            .await
            .unwrap();
    let state = reopened
        .read_state(StateReadRequest {
            queue: queue.clone(),
            collection: "instances".into(),
            keys: vec!["instance".into()],
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    assert_eq!(state[0].as_ref().unwrap().value.as_ref(), &[7, 8]);
    let receipt = reopened
        .read_commit(ReceiptReadRequest {
            queue,
            request_ids: vec!["transition".into()],
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    assert!(matches!(receipt[0], ReceiptLookup::Found(_)));
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
async fn mandatory_competition_expiry_and_long_ordering() {
    use fireweed_conformance::queue_contract::{self as suite, TestClock};
    let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
    let clock = Arc::new(TestClock::default());
    let a = PostgresQueueStore::connect(&url, clock.clone(), QueueLimits::default(), 2)
        .await
        .unwrap();
    let b = PostgresQueueStore::connect(&url, clock.clone(), QueueLimits::default(), 2)
        .await
        .unwrap();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let q = |name: &str| QueueAddress {
        tenant: "api007-conformance".into(),
        queue: format!("{nonce}-{name}"),
    };
    suite::competing_claims(&a, &b, q("competition")).await;
    suite::expiry_and_administration(&a, &clock, q("expiry")).await;
    suite::ordered_long_values_and_cursor_scope(&a, q("long")).await;
    suite::predicate_and_order_oracles(&a, q("predicate-oracles")).await;
    suite::ordered_groups_sparse_stats_and_replay_precedence(&a, &clock, q("groups-stats-replay"))
        .await;
    suite::indexed_additional_attributes(&a, q("additional-attributes")).await;
    suite::numeric_bucket_stats(&a, &clock, q("numeric-buckets")).await;
    suite::ordered_insert_uniqueness(&a, q("insert-uniqueness")).await;
    suite::shrinking_upsert_index_amplification(&a, q("shrinking-index-upsert")).await;
    suite::unrestricted_names_obey_response_bounds(&a, q("names-bounds")).await;
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires isolated FIREWEED_API007_PG_TEST_URL"]
async fn legacy_api007_definition_reopens_work_state_and_receipt() {
    use std::collections::{BTreeMap, BTreeSet};
    #[derive(serde::Serialize)]
    struct OldDefinition<'a> {
        fields: &'a BTreeMap<String, FieldType>,
        indexes: &'a BTreeMap<String, Vec<String>>,
        unique_indexes: &'a BTreeSet<String>,
    }
    #[derive(serde::Serialize)]
    struct OldSchema<'a> {
        work: OldDefinition<'a>,
        state: BTreeMap<&'a String, OldDefinition<'a>>,
        progress_bound_nanos: u64,
        max_attempts: u32,
    }
    let schema = fireweed_conformance::queue_contract::schema();
    fn old_def(def: &CollectionDefinition) -> OldDefinition<'_> {
        OldDefinition {
            fields: &def.fields,
            indexes: &def.indexes,
            unique_indexes: &def.unique_indexes,
        }
    }
    let old = OldSchema {
        work: old_def(&schema.work),
        state: schema
            .state
            .iter()
            .map(|(name, def)| (name, old_def(def)))
            .collect(),
        progress_bound_nanos: schema.progress_bound_nanos,
        max_attempts: schema.max_attempts,
    };
    let raw = postcard::to_allocvec(&old).unwrap();
    let url = std::env::var("FIREWEED_API007_PG_TEST_URL").unwrap();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let queue = QueueAddress {
        tenant: "api007-legacy-definition".into(),
        queue: nonce.to_string(),
    };
    let store = PostgresQueueStore::connect(&url, Arc::new(FixedClock), QueueLimits::default(), 2)
        .await
        .unwrap();
    fireweed_conformance::queue_contract::run(&store, queue.clone()).await;
    let expected = store
        .read_commit(ReceiptReadRequest {
            queue: queue.clone(),
            request_ids: vec!["transition".into()],
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    store.shutdown().await.unwrap();
    let (client, driver) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let driver = tokio::spawn(driver);
    assert_eq!(
        client
            .execute(
                "UPDATE fireweed_api007.queues SET definition=$1 WHERE tenant=$2 AND name=$3",
                &[&raw, &queue.tenant.as_bytes(), &queue.queue.as_bytes()]
            )
            .await
            .unwrap(),
        1
    );
    drop(client);
    driver.await.unwrap().unwrap();
    let reopened =
        PostgresQueueStore::connect(&url, Arc::new(FixedClock), QueueLimits::default(), 1)
            .await
            .unwrap();
    reopened
        .provision(ProvisionRequest {
            queue: queue.clone(),
            schema,
        })
        .await
        .unwrap();
    let state = reopened
        .read_state(StateReadRequest {
            queue: queue.clone(),
            collection: "instances".into(),
            keys: vec!["instance".into()],
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    assert_eq!(state[0].as_ref().unwrap().value.as_ref(), &[7, 8]);
    assert_eq!(
        reopened
            .read_commit(ReceiptReadRequest {
                queue: queue.clone(),
                request_ids: vec!["transition".into()],
                max_bytes: 1 << 20
            })
            .await
            .unwrap(),
        expected
    );
    let work = reopened
        .query(QueryRequest {
            queue,
            source: RecordSource::Work,
            index: "$key".into(),
            filter: Filter::default(),
            descending: false,
            cursor: None,
            max_records: 20,
            max_bytes: 1 << 20,
        })
        .await
        .unwrap();
    assert!(work.records.iter().any(|record|matches!(record,QueryRecord::Work(work) if work.work.key=="low" && work.lifecycle==Lifecycle::Completed)));
    reopened.shutdown().await.unwrap();
}
