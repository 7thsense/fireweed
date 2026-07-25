//! Live PostgreSQL proofs for the backend-erased atomic item-mutation contract.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use fireweed_core::{
    ClientItemKey, GateKeyPolicy, ItemState, LeaseToken, Metadata, MetadataValue, PriorityValue,
    RequestId, WorkerId,
};
use fireweed_engine::{
    AddressedMutation, ClaimCompatibility, ClaimPort, ClaimRequest, ControlPlaneStore, EngineError,
    EntityPredicateValue, GateChange, GateKeyDelta, ItemMutationOperation, ItemMutationOutcome,
    ItemMutationPort, ItemMutationRequest, ItemMutationReturning, ItemPatch, ItemPredicate,
    ItemSelector, ItemSelectorScope, LeaseGuard, LifecyclePatch, ProjectionRead, PushPort,
    PushSpec, SelectedMutation, SetGatesCommand, SetGatesPort,
};
use fireweed_postgres::PostgresRelationalBackend;

fn pg_url(test: &str) -> Option<String> {
    match std::env::var("FIREWEED_PG_TEST_URL") {
        Ok(url) => Some(url),
        Err(_) => {
            eprintln!("POSTGRES ITEM MUTATION SKIPPED ({test}) — FIREWEED_PG_TEST_URL is unset");
            None
        }
    }
}

fn fresh_schema() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "fireweed_item_mutation_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    )
}

fn definition() -> fireweed_core::QueueDefinition {
    let mut definition = fireweed_conformance::qdef();
    definition.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
    definition.eligibility_policy.max_gate_keys_per_item = Some(8);
    definition.eligibility_policy.max_gates_per_request = Some(8);
    definition
}

fn item(entity_id: &str) -> PushSpec {
    PushSpec {
        client_item_key: Some(ClientItemKey::new(entity_id).unwrap()),
        fields: BTreeMap::from([("owner".into(), Bytes::from_static(b"before"))]),
        entity: Some(serde_json::json!({
            "workflow": {"id": entity_id, "revision": 1}
        })),
        ..Default::default()
    }
}

fn selector_request(request_id: &str, evaluated_at: i64) -> ItemMutationRequest {
    ItemMutationRequest {
        request_id: RequestId::new(request_id).unwrap(),
        evaluated_at: fireweed_conformance::ts(evaluated_at),
        dry_run: false,
        returning: ItemMutationReturning::BeforeSnapshot,
        gate_changes: vec![GateChange {
            gate_keys: vec!["operator-hold".into()],
            blocked: true,
        }],
        operation: ItemMutationOperation::SelectFirst {
            clauses: vec![SelectedMutation {
                selector_id: "unindexed-workflow".into(),
                selector: ItemSelector {
                    scope: ItemSelectorScope::Live,
                    predicates: vec![ItemPredicate::EntityEq {
                        pointer: "/workflow/id".into(),
                        value: EntityPredicateValue::Value(serde_json::json!("workflow-a")),
                    }],
                },
                predicates: vec![],
                lease_guard: LeaseGuard::RejectActive,
                patch: ItemPatch {
                    priority: fireweed_engine::BatchUpdateValue::Replace(Some(
                        PriorityValue::Int64(7),
                    )),
                    not_before: fireweed_engine::BatchUpdateValue::Replace(Some(
                        fireweed_conformance::ts(20),
                    )),
                    payload: fireweed_engine::BatchUpdateValue::Replace(Some(Bytes::from_static(
                        b"mutation-payload",
                    ))),
                    metadata: fireweed_engine::BatchUpdateValue::Replace(Metadata::from_entries(
                        BTreeMap::from([(
                            "source".into(),
                            MetadataValue::String("operator".into()),
                        )]),
                    )),
                    gate_keys: GateKeyDelta {
                        add: vec!["operator-hold".into()],
                        remove: vec![],
                        remove_prefixes: vec![],
                    },
                    field_edits: BTreeMap::from([(
                        "owner".into(),
                        Some(Bytes::from_static(b"after")),
                    )]),
                    ..Default::default()
                },
            }],
        },
    }
}

#[test]
fn exact_replay_survives_reopen_and_unindexed_selector_is_authoritative() {
    let Some(url) = pg_url("exact_replay_survives_reopen_and_unindexed_selector_is_authoritative")
    else {
        return;
    };
    let schema = fresh_schema();
    let shard = fireweed_conformance::shard();
    let request = selector_request("pg-mutation-replay", 10);
    let committed = futures::executor::block_on(async {
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        backend.create_queue(definition()).await.unwrap();
        backend
            .push(
                &shard,
                vec![item("workflow-a"), item("workflow-b")],
                fireweed_conformance::ts(0),
                None,
            )
            .await
            .unwrap();
        let response = backend
            .mutate_items(&shard, request.clone(), None)
            .await
            .unwrap();
        assert_eq!(response.summary.changed, 1);
        assert_eq!(response.selectors[0].matched, 1);
        assert!(matches!(
            response.results[0].outcome,
            ItemMutationOutcome::Updated {
                item_version: 2,
                state: ItemState::Pending
            }
        ));
        let views = backend
            .live_items(&shard, &[ClientItemKey::new("workflow-a").unwrap()])
            .await
            .unwrap();
        assert_eq!(
            views[0].as_ref().unwrap().fields["owner"].as_ref(),
            b"after"
        );
        let view = views[0].as_ref().unwrap();
        assert_eq!(view.priority, Some(PriorityValue::Int64(7)));
        assert_eq!(view.not_before, Some(fireweed_conformance::ts(20)));
        assert_eq!(view.payload.as_deref(), Some(&b"mutation-payload"[..]));
        backend
            .set_gates(
                &shard,
                SetGatesCommand {
                    gate_keys: vec!["operator-hold".into()],
                    blocked: false,
                },
                fireweed_conformance::ts(21),
                None,
            )
            .await
            .unwrap();
        let claimed = backend
            .claim(ClaimRequest {
                eligibility_time: None,
                shard: shard.clone(),
                worker_id: WorkerId::new("replay-proof-worker").unwrap(),
                max_items: 1,
                lease_token: LeaseToken::new("replay-proof-lease").unwrap(),
                lease_expires_at: fireweed_conformance::ts(100),
                now: fireweed_conformance::ts(30),
                compatibility: ClaimCompatibility::default(),
                expected_epoch: None,
            })
            .await
            .unwrap();
        assert_eq!(
            claimed.items[0].metadata.get("source"),
            Some(&MetadataValue::String("operator".into()))
        );
        backend.rebuild_from_command_baseline(&shard).unwrap();
        assert_eq!(
            backend
                .mutate_items(&shard, request.clone(), None)
                .await
                .unwrap(),
            response,
            "snapshot-tail rebuild must restore the retained response without selector evaluation"
        );
        response
    });

    futures::executor::block_on(async {
        let reopened = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        let replayed = reopened
            .mutate_items(&shard, request.clone(), None)
            .await
            .unwrap();
        assert_eq!(replayed, committed);

        let mut changed_body = request;
        changed_body.gate_changes.clear();
        assert_eq!(
            reopened
                .mutate_items(&shard, changed_body, None)
                .await
                .unwrap_err(),
            EngineError::RequestIdConflict
        );
    });
}

#[test]
fn invalid_request_rolls_back_items_gates_and_idempotency_and_dry_run_writes_nothing() {
    let Some(url) =
        pg_url("invalid_request_rolls_back_items_gates_and_idempotency_and_dry_run_writes_nothing")
    else {
        return;
    };
    let schema = fresh_schema();
    futures::executor::block_on(async {
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        let shard = fireweed_conformance::shard();
        backend.create_queue(definition()).await.unwrap();
        backend
            .push(
                &shard,
                vec![item("workflow-a")],
                fireweed_conformance::ts(0),
                None,
            )
            .await
            .unwrap();

        let mut invalid = selector_request("pg-mutation-invalid", 10);
        invalid.gate_changes[0].gate_keys = vec!["not a valid gate".into()];
        assert!(matches!(
            backend.mutate_items(&shard, invalid, None).await,
            Err(EngineError::Invalid(_))
        ));
        assert_eq!(
            backend
                .live_items(&shard, &[ClientItemKey::new("workflow-a").unwrap()])
                .await
                .unwrap()[0]
                .as_ref()
                .unwrap()
                .item_version,
            1
        );

        let mut preview = selector_request("pg-mutation-preview", 11);
        preview.dry_run = true;
        let response = backend.mutate_items(&shard, preview, None).await.unwrap();
        assert!(response.position.is_none());
        assert!(matches!(
            response.results[0].outcome,
            ItemMutationOutcome::WouldUpdate { .. }
        ));
        assert_eq!(
            backend
                .live_items(&shard, &[ClientItemKey::new("workflow-a").unwrap()])
                .await
                .unwrap()[0]
                .as_ref()
                .unwrap()
                .item_version,
            1
        );

        // The preview request id was not retained, so a different committed body may reuse it.
        let mut committed = selector_request("pg-mutation-preview", 12);
        committed.returning = ItemMutationReturning::Identity;
        assert_eq!(
            backend
                .mutate_items(&shard, committed, None)
                .await
                .unwrap()
                .summary
                .changed,
            1
        );
    });
}

#[test]
fn lease_invalidation_clears_durable_and_live_lease_state_once() {
    let Some(url) = pg_url("lease_invalidation_clears_durable_and_live_lease_state_once") else {
        return;
    };
    let schema = fresh_schema();
    futures::executor::block_on(async {
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        let shard = fireweed_conformance::shard();
        backend.create_queue(definition()).await.unwrap();
        let item_id = backend
            .push(
                &shard,
                vec![item("workflow-a")],
                fireweed_conformance::ts(0),
                None,
            )
            .await
            .unwrap()[0];
        let lease = LeaseToken::new("mutation-lease").unwrap();
        backend
            .claim(ClaimRequest {
                eligibility_time: None,
                shard: shard.clone(),
                worker_id: WorkerId::new("mutation-worker").unwrap(),
                max_items: 1,
                lease_token: lease.clone(),
                lease_expires_at: fireweed_conformance::ts(100),
                now: fireweed_conformance::ts(1),
                compatibility: ClaimCompatibility::default(),
                expected_epoch: None,
            })
            .await
            .unwrap();

        let response = backend
            .mutate_items(
                &shard,
                ItemMutationRequest {
                    request_id: RequestId::new("pg-mutation-lease").unwrap(),
                    evaluated_at: fireweed_conformance::ts(2),
                    dry_run: false,
                    returning: ItemMutationReturning::Identity,
                    gate_changes: vec![],
                    operation: ItemMutationOperation::Addressed {
                        entries: vec![AddressedMutation {
                            item_id,
                            expected_item_version: Some(2),
                            predicates: vec![ItemPredicate::StateIn(vec![ItemState::Leased])],
                            lease_guard: LeaseGuard::RequireActive,
                            patch: ItemPatch {
                                lifecycle: LifecyclePatch::SetComplete,
                                ..Default::default()
                            },
                        }],
                    },
                },
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            response.results[0].outcome,
            ItemMutationOutcome::Updated {
                item_version: 3,
                state: ItemState::Complete
            }
        ));
        assert!(backend.pending(&shard).await.unwrap().is_empty());
        assert!(
            backend
                .live_items(&shard, &[ClientItemKey::new("workflow-a").unwrap()])
                .await
                .unwrap()[0]
                .is_none()
        );
        assert_eq!(backend.metrics(&shard).await.unwrap().complete, 1);
    });
}
