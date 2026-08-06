//! Focused live-Postgres proofs for API-001 `BatchUpdate`.
//!
//! Run with:
//! `FIREWEED_PG_TEST_URL=postgres://postgres:fireweed@127.0.0.1:5433/postgres cargo test -p fireweed-postgres --test api001_batch_update`

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use fireweed_core::{
    ClientItemKey, GateKeyPolicy, ItemId, Metadata, MetadataValue, PriorityValue, RequestId,
    WorkerId,
};
use fireweed_engine::{
    BatchUpdateEntry, BatchUpdateItemRef, BatchUpdateOutcome, BatchUpdatePort, BatchUpdateRequest,
    BatchUpdateValue, ClaimCompatibility, ClaimPort, ClaimRequest, ControlPlaneStore, EngineError,
    FinalizeKind, FinalizeOutcome, FinalizePort, ProjectionRead, PushPort, PushSpec, QueueKey,
};
use fireweed_postgres::PostgresRelationalBackend;

fn fresh_schema() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "fireweed_api001_update_{}_{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

fn pg_url(test: &str) -> String {
    let _ = test;
    std::env::var("FIREWEED_PG_TEST_URL")
        .expect("FIREWEED_PG_TEST_URL required (fail-closed live postgres; no LOUD skip)")
}

fn key(index: usize) -> ClientItemKey {
    ClientItemKey::new(format!("update-{index:04}")).unwrap()
}

fn fields(value: impl Into<Bytes>) -> BTreeMap<String, Bytes> {
    BTreeMap::from([("value".to_string(), value.into())])
}

fn update(item_ref: BatchUpdateItemRef, value: impl Into<Bytes>) -> BatchUpdateEntry {
    BatchUpdateEntry {
        item_ref,
        expected_item_version: None,
        priority: BatchUpdateValue::Keep,
        not_before: BatchUpdateValue::Keep,
        payload: BatchUpdateValue::Keep,
        metadata: BatchUpdateValue::Keep,
        gate_keys: BatchUpdateValue::Keep,
        fields: BatchUpdateValue::Replace(fields(value)),
    }
}

fn qdef(max_batch: usize) -> fireweed_core::QueueDefinition {
    let mut definition = fireweed_conformance::qdef();
    definition.max_push_batch_size = max_batch as u64;
    definition.max_claim_batch_size = max_batch as u64;
    definition.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
    definition.eligibility_policy.max_gate_keys_per_item = Some(2);
    definition
}

fn pushes(count: usize) -> Vec<PushSpec> {
    (0..count)
        .map(|index| PushSpec {
            client_item_key: Some(key(index)),
            priority: Some(PriorityValue::Int64(index as i64)),
            fields: fields(Bytes::from_static(b"before")),
            ..Default::default()
        })
        .collect()
}

fn claim_request(shard: &QueueKey, token: &str, now: i64) -> ClaimRequest {
    ClaimRequest {
        eligibility_time: None,
        shard: shard.clone(),
        worker_id: WorkerId::new("api001-worker").unwrap(),
        max_items: 1,
        lease_token: fireweed_core::LeaseToken::new(token).unwrap(),
        lease_expires_at: fireweed_conformance::ts(now + 100),
        now: fireweed_conformance::ts(now),
        compatibility: ClaimCompatibility::default(),
        expected_epoch: None,
    }
}

#[test]
fn batch_update_is_set_based_at_sizes_1_100_and_1000() {
    let url = pg_url("batch_update_is_set_based_at_sizes_1_100_and_1000");
    for size in [1_usize, 100, 1_000] {
        let schema = fresh_schema();
        futures::executor::block_on(async {
            let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
            let shard = fireweed_conformance::shard();
            backend.create_queue(qdef(1_000)).await.unwrap();
            let ids = backend
                .push(&shard, pushes(size), fireweed_conformance::ts(0), None)
                .await
                .unwrap();
            let request_id = RequestId::new(format!("batch-size-{size}")).unwrap();
            let response = backend
                .batch_update(
                    &shard,
                    BatchUpdateRequest {
                        request_id: request_id.clone(),
                        updates: ids
                            .iter()
                            .enumerate()
                            .map(|(index, id)| {
                                update(
                                    BatchUpdateItemRef::ItemId(*id),
                                    Bytes::from(format!("after-{index}")),
                                )
                            })
                            .collect(),
                    },
                    fireweed_conformance::ts(1),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(response.request_id, request_id);
            assert_eq!(response.results.len(), size);
            for (index, result) in response.results.iter().enumerate() {
                assert_eq!(
                    result,
                    &BatchUpdateOutcome::Updated {
                        item_id: ids[index],
                        client_item_key: key(index),
                        item_version: 2,
                    }
                );
            }
            let sample_indexes = [0, size / 2, size - 1];
            let sample_keys: Vec<_> = sample_indexes.iter().map(|index| key(*index)).collect();
            let views = backend.live_items(&shard, &sample_keys).await.unwrap();
            for (view, index) in views.into_iter().zip(sample_indexes) {
                assert_eq!(
                    view.unwrap().fields,
                    fields(Bytes::from(format!("after-{index}")))
                );
            }
        });
    }
}

#[test]
fn batch_update_preserves_order_and_idempotency_across_mixed_outcomes() {
    let url = pg_url("batch_update_preserves_order_and_idempotency_across_mixed_outcomes");
    let schema = fresh_schema();
    futures::executor::block_on(async {
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        let shard = fireweed_conformance::shard();
        backend.create_queue(qdef(100)).await.unwrap();
        let ids = backend
            .push(&shard, pushes(10), fireweed_conformance::ts(0), None)
            .await
            .unwrap();

        let terminal = backend
            .claim(claim_request(&shard, "terminal-lease", 1))
            .await
            .unwrap()
            .items[0]
            .item_id;
        assert_eq!(terminal, ids[0]);
        backend
            .finalize(
                &shard,
                vec![FinalizeOutcome::new(terminal, FinalizeKind::Complete)],
                fireweed_conformance::ts(2),
                None,
            )
            .await
            .unwrap();
        let leased = backend
            .claim(claim_request(&shard, "active-lease", 3))
            .await
            .unwrap()
            .items[0]
            .item_id;
        assert_eq!(leased, ids[1]);

        let mut metadata = Metadata::new();
        metadata.insert("phase", MetadataValue::String("updated".into()));
        let mut first = update(BatchUpdateItemRef::ItemId(ids[2]), "by-id");
        first.expected_item_version = Some(1);
        first.priority = BatchUpdateValue::Replace(PriorityValue::Int64(99));
        first.not_before = BatchUpdateValue::Replace(Some(fireweed_conformance::ts(50)));
        first.payload = BatchUpdateValue::Replace(Some(Bytes::from_static(b"payload-v2")));
        first.metadata = BatchUpdateValue::Replace(metadata);
        first.gate_keys =
            BatchUpdateValue::Replace(vec!["gate-b".into(), "gate-a".into(), "gate-a".into()]);
        let request = BatchUpdateRequest {
            request_id: RequestId::new("mixed-update").unwrap(),
            updates: vec![
                first,
                update(BatchUpdateItemRef::ClientItemKey(key(3)), "by-key"),
                update(
                    BatchUpdateItemRef::Both {
                        item_id: ids[4],
                        client_item_key: key(4),
                    },
                    "by-both",
                ),
                update(BatchUpdateItemRef::ItemId(leased), "leased"),
                BatchUpdateEntry {
                    expected_item_version: Some(99),
                    ..update(BatchUpdateItemRef::ItemId(ids[5]), "bad-cas")
                },
                update(BatchUpdateItemRef::ItemId(terminal), "terminal"),
                update(
                    BatchUpdateItemRef::ItemId(ItemId::from_u64(u64::MAX)),
                    "missing",
                ),
                update(
                    BatchUpdateItemRef::Both {
                        item_id: ids[6],
                        client_item_key: key(7),
                    },
                    "mismatched-dual-ref",
                ),
                BatchUpdateEntry {
                    fields: BatchUpdateValue::Replace(BTreeMap::from([(
                        "payload".into(),
                        Bytes::from_static(b"reserved"),
                    )])),
                    ..update(BatchUpdateItemRef::ItemId(ids[6]), "reserved-field")
                },
                BatchUpdateEntry {
                    priority: BatchUpdateValue::Replace(PriorityValue::Timestamp(
                        fireweed_conformance::ts(99),
                    )),
                    ..update(BatchUpdateItemRef::ItemId(ids[7]), "wrong-priority-kind")
                },
                BatchUpdateEntry {
                    gate_keys: BatchUpdateValue::Replace(vec!["not a gate key".into()]),
                    ..update(BatchUpdateItemRef::ItemId(ids[8]), "malformed-gate")
                },
                BatchUpdateEntry {
                    gate_keys: BatchUpdateValue::Replace(vec![
                        "gate-a".into(),
                        "gate-b".into(),
                        "gate-c".into(),
                    ]),
                    ..update(BatchUpdateItemRef::ItemId(ids[9]), "too-many-gates")
                },
            ],
        };
        let response = backend
            .batch_update(&shard, request.clone(), fireweed_conformance::ts(4), None)
            .await
            .unwrap();
        assert_eq!(
            response.results,
            vec![
                BatchUpdateOutcome::Updated {
                    item_id: ids[2],
                    client_item_key: key(2),
                    item_version: 2,
                },
                BatchUpdateOutcome::Updated {
                    item_id: ids[3],
                    client_item_key: key(3),
                    item_version: 2,
                },
                BatchUpdateOutcome::Updated {
                    item_id: ids[4],
                    client_item_key: key(4),
                    item_version: 2,
                },
                BatchUpdateOutcome::Conflict,
                BatchUpdateOutcome::Conflict,
                BatchUpdateOutcome::Terminal,
                BatchUpdateOutcome::NotFound,
                BatchUpdateOutcome::Invalid,
                BatchUpdateOutcome::Invalid,
                BatchUpdateOutcome::Invalid,
                BatchUpdateOutcome::Invalid,
                BatchUpdateOutcome::Invalid,
            ]
        );
        let replay = backend
            .batch_update(&shard, request.clone(), fireweed_conformance::ts(5), None)
            .await
            .unwrap();
        assert_eq!(replay, response);
        let mut changed = request;
        changed.updates[0].fields = BatchUpdateValue::Replace(fields("different"));
        assert_eq!(
            backend
                .batch_update(&shard, changed, fireweed_conformance::ts(5), None)
                .await,
            Err(EngineError::RequestIdConflict)
        );

        let view = backend.live_items(&shard, &[key(2)]).await.unwrap()[0]
            .clone()
            .unwrap();
        assert_eq!(view.priority, Some(PriorityValue::Int64(99)));
        assert_eq!(view.not_before, Some(fireweed_conformance::ts(50)));
        assert_eq!(view.payload, Some(Bytes::from_static(b"payload-v2")));
        assert_eq!(view.fields, fields("by-id"));
        for view in backend
            .live_items(&shard, &[key(6), key(7), key(8), key(9)])
            .await
            .unwrap()
        {
            let view = view.expect("invalid entry leaves its pending item live");
            assert_eq!(view.item_version, 1);
            assert_eq!(view.fields, fields(Bytes::from_static(b"before")));
        }
        let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
        client
            .batch_execute(&format!("SET search_path TO {schema}"))
            .unwrap();
        let row = client
            .query_one(
                "SELECT metadata,(SELECT array_agg(gate_key ORDER BY gate_key) FROM fireweed_item_gates g \
                 WHERE g.tenant_id=i.tenant_id AND g.queue_id=i.queue_id AND g.item_id=i.item_id) \
                 FROM fireweed_items i WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
                &[&shard.tenant_id.as_str(), &shard.queue_id.as_str(), &ids[2].to_string()],
            )
            .unwrap();
        assert!(row.get::<_, String>(0).contains("updated"));
        assert_eq!(row.get::<_, Vec<String>>(1), vec!["gate-a", "gate-b"]);
    });
}

#[test]
fn disabled_gate_update_is_invalid_without_aborting_valid_sibling() {
    let url = pg_url("disabled_gate_update_is_invalid_without_aborting_valid_sibling");
    let schema = fresh_schema();
    futures::executor::block_on(async {
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        let shard = fireweed_conformance::shard();
        backend
            .create_queue(fireweed_conformance::qdef())
            .await
            .unwrap();
        let ids = backend
            .push(&shard, pushes(2), fireweed_conformance::ts(0), None)
            .await
            .unwrap();
        let response = backend
            .batch_update(
                &shard,
                BatchUpdateRequest {
                    request_id: RequestId::new("disabled-gate-mixed").unwrap(),
                    updates: vec![
                        update(BatchUpdateItemRef::ItemId(ids[0]), "valid-sibling"),
                        BatchUpdateEntry {
                            gate_keys: BatchUpdateValue::Replace(vec!["disabled".into()]),
                            ..update(BatchUpdateItemRef::ItemId(ids[1]), "invalid-gate")
                        },
                    ],
                },
                fireweed_conformance::ts(1),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            response.results,
            vec![
                BatchUpdateOutcome::Updated {
                    item_id: ids[0],
                    client_item_key: key(0),
                    item_version: 2,
                },
                BatchUpdateOutcome::Invalid,
            ]
        );
        let views = backend.live_items(&shard, &[key(0), key(1)]).await.unwrap();
        assert_eq!(views[0].as_ref().unwrap().fields, fields("valid-sibling"));
        assert_eq!(views[1].as_ref().unwrap().fields, fields("before"));
        assert_eq!(views[1].as_ref().unwrap().item_version, 1);
    });
}

#[test]
fn stale_epoch_and_snapshot_tail_rebuild_preserve_batch_update_replay() {
    let url = pg_url("stale_epoch_and_snapshot_tail_rebuild_preserve_batch_update_replay");
    let schema = fresh_schema();
    let shard = fireweed_conformance::shard();
    let successful = BatchUpdateRequest {
        request_id: RequestId::new("rebuild-success").unwrap(),
        updates: vec![update(BatchUpdateItemRef::ClientItemKey(key(0)), "durable")],
    };
    let rejected = BatchUpdateRequest {
        request_id: RequestId::new("rebuild-all-rejected").unwrap(),
        updates: vec![update(
            BatchUpdateItemRef::ItemId(ItemId::from_u64(u64::MAX)),
            "missing",
        )],
    };
    let (successful_response, rejected_response) = futures::executor::block_on(async {
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        backend.create_queue(qdef(100)).await.unwrap();
        let ids = backend
            .push(&shard, pushes(1), fireweed_conformance::ts(0), None)
            .await
            .unwrap();
        let stale_epoch = backend.acquire_epoch(&shard).await.unwrap();
        let current_epoch = backend.acquire_epoch(&shard).await.unwrap();
        assert!(current_epoch > stale_epoch);

        let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
        client
            .batch_execute(&format!("SET search_path TO {schema}"))
            .unwrap();
        let before: (i64, i64, i64, String) = {
            let row = client
                .query_one(
                    "SELECT c.next_seq,(SELECT COUNT(*) FROM fireweed_commands p WHERE p.tenant=c.tenant AND p.queue=c.queue), \
                            i.item_version,i.fields FROM relational_cursor c JOIN fireweed_items i \
                            ON i.tenant_id=c.tenant AND i.queue_id=c.queue \
                      WHERE c.tenant=$1 AND c.queue=$2 AND i.item_id=$3",
                    &[&shard.tenant_id.as_str(), &shard.queue_id.as_str(), &ids[0].to_string()],
                )
                .unwrap();
            (row.get(0), row.get(1), row.get(2), row.get(3))
        };
        assert_eq!(
            backend
                .batch_update(
                    &shard,
                    BatchUpdateRequest {
                        request_id: RequestId::new("stale-batch").unwrap(),
                        updates: vec![update(BatchUpdateItemRef::ItemId(ids[0]), "stale")],
                    },
                    fireweed_conformance::ts(1),
                    Some(stale_epoch),
                )
                .await,
            Err(EngineError::EpochFenced)
        );
        let row = client
            .query_one(
                "SELECT c.next_seq,(SELECT COUNT(*) FROM fireweed_commands p WHERE p.tenant=c.tenant AND p.queue=c.queue), \
                        i.item_version,i.fields FROM relational_cursor c JOIN fireweed_items i \
                        ON i.tenant_id=c.tenant AND i.queue_id=c.queue \
                  WHERE c.tenant=$1 AND c.queue=$2 AND i.item_id=$3",
                &[&shard.tenant_id.as_str(), &shard.queue_id.as_str(), &ids[0].to_string()],
            )
            .unwrap();
        let after = (row.get(0), row.get(1), row.get(2), row.get(3));
        assert_eq!(
            after, before,
            "stale epoch changed cursor, log, or projection"
        );

        let success = backend
            .batch_update(
                &shard,
                successful.clone(),
                fireweed_conformance::ts(2),
                Some(current_epoch),
            )
            .await
            .unwrap();
        let rejection = backend
            .batch_update(
                &shard,
                rejected.clone(),
                fireweed_conformance::ts(3),
                Some(current_epoch),
            )
            .await
            .unwrap();
        assert_eq!(rejection.results, vec![BatchUpdateOutcome::NotFound]);
        (success, rejection)
    });

    let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
    backend.rebuild_from_command_baseline(&shard).unwrap();
    drop(backend);
    futures::executor::block_on(async {
        let reopened = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        assert_eq!(
            reopened
                .batch_update(&shard, successful, fireweed_conformance::ts(4), None)
                .await
                .unwrap(),
            successful_response
        );
        assert_eq!(
            reopened
                .batch_update(&shard, rejected, fireweed_conformance::ts(4), None)
                .await
                .unwrap(),
            rejected_response
        );
        assert_eq!(
            reopened.live_items(&shard, &[key(0)]).await.unwrap()[0]
                .as_ref()
                .unwrap()
                .fields,
            fields("durable")
        );
    });
}
