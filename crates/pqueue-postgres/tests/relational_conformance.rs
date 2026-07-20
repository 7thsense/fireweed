//! BQ-12 — the CORE conformance class + the relational-reconnect class run against the postgres
//! **relational** backend (`PostgresRelationalBackend`: rebuildable `pqueue_items` cache, real
//! `FOR UPDATE SKIP LOCKED` claim), **env-gated** on a live database via `PQUEUE_PG_TEST_URL`.
//!
//! Each scenario gets a process-unique schema (`connect_in_schema`); a scenario's repeated `make()` calls
//! reopen the SAME schema, so the relational-reconnect scenarios exercise rebuildable-cache recovery.
//! The TOCTOU prerequisite bead `pqueue-b59f4897` stays explicit in the migration path for the later
//! multi-node rollout.
//!
//! If `PQUEUE_PG_TEST_URL` is ABSENT, every scenario prints a LOUD skip — a green run is then VISIBLY
//! partial (postgres unverified against a live DB), never a hidden pass. Compiling this file already proves
//! `PostgresRelationalBackend` satisfies `ConformanceCore` (the scenarios' generic bound). To run live:
//!   docker run -d --name pq-pg -p 5433:5432 -e POSTGRES_PASSWORD=pq postgres:16
//!   PQUEUE_PG_TEST_URL=postgres://postgres:pq@127.0.0.1:5433/postgres cargo test -p pqueue-postgres
//!
//! NOTE: only `ConformanceCore`-bounded scenarios appear here — the relational backend is log-optional
//! (no `LogRead`/`SnapshotStore`), so the log-class scenarios (high_water/snapshots/log-replay) do not
//! apply. The `FOR UPDATE SKIP LOCKED` contended-writer evidence is likewise live-DB-gated and pending,
//! and the multi-node prerequisite remains `pqueue-b59f4897`.

use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_core::{EntitySchemaDocument, RequestId};
use pqueue_engine::{
    ClaimPort, CommandPosition, ControlPlaneStore, EngineError, ProjectionRead, ProjectionStore,
    PushPort, RecoveryReadPort,
};
use pqueue_postgres::{PostgresRelationalBackend, composed_postgres_relational_in_schema};
use serde_json::json;

fn fresh_schema() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "pq_rel_{}_{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

/// One `#[test]` per scenario: env-gated + schema-isolated. A single schema is computed per scenario and
/// reused across that scenario's `make()` calls (so the reconnect scenarios reopen the same DB). Driven by
/// `futures::executor::block_on` (the sync postgres client panics under an ambient tokio runtime).
macro_rules! pg_relational {
    ($($name:ident),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                match std::env::var("PQUEUE_PG_TEST_URL") {
                    Ok(url) => {
                        let schema = fresh_schema();
                        futures::executor::block_on(pqueue_conformance::scenarios::$name(move || {
                            PostgresRelationalBackend::connect_in_schema(&url, &schema)
                                .expect("connect postgres (is PQUEUE_PG_TEST_URL a live DB?)")
                        }));
                    }
                    Err(_) => {
                        eprintln!(
                            "POSTGRES RELATIONAL SKIPPED ({}) — set PQUEUE_PG_TEST_URL to a live DB",
                            stringify!($name)
                        );
                    }
                }
            }
        )+
    };
}

// The full CORE class (the `core_suite!(@atomic)` scenario list) + the relational-reconnect class.
pg_relational!(
    push_then_select_eligible_in_priority_order,
    claim_then_complete_lifecycle,
    claim_returns_priority_ordered_rich_items,
    claim_empty_when_nothing_eligible,
    claimed_item_shape_includes_payload_fields_and_gate_keys,
    claimed_item_shape_reflects_update_fields_after_reclaim,
    claimed_item_shape_omits_empty_conditionals,
    structured_live_items_are_ordered_and_only_live,
    tick_reclaims_expired_lease_with_no_client_traffic,
    tick_lease_boundary_is_half_open,
    paused_queue_yields_no_claims,
    fenced_lease_finalize_is_stale,
    claimed_view_renders_leased_items,
    retry_beyond_max_attempts_goes_terminal,
    retry_with_backoff_defers_eligibility,
    peek_is_priority_ordered_and_nondestructive,
    pending_lists_leased_items,
    renew_extends_lease_and_rejects,
    reassign_swaps_token_and_charges_attempt,
    purge_removes_present_items_and_gates_leased,
    finalize_of_nonleased_item_is_rejected_without_appending,
    replace_pending_supersedes_old,
    upsert_inserts_then_replaces_pending,
    upsert_rejects_claimed_and_terminal,
    upsert_preserves_group_delay_and_payload_in_claim_shape,
    update_fields_merges_and_cas,
    reclaim_expired_sweeps_per_queue,
    successful_push_is_visible_before_response_returns,
    rejected_finalize_leaves_visible_state_unchanged,
    request_id_push_replays_once_and_conflicts_on_body_change,
    reconnect_after_crash_preserves_committed_state,
    reconnect_preserves_terminal_and_pending_state,
    reconnect_preserves_leased_item_state,
    claimed_item_shape_whole_cohort_omits_per_item_lease_token,
    claim_compatibility_is_resolved_and_gated,
    stale_epoch_append_is_fenced,
    epoch_fence_closes_pre_segment_window,
    adr011_schema_validation_rejects_before_visible_state,
    adr011_typed_scalar_and_compound_indexes_work,
    adr011_typed_missing_fields_remain_sparse,
    adr011_typed_unique_conflicts_are_atomic,
    adr011_typed_update_fields_and_replace_rekey,
    adr011_typed_purge_frees_unique_key,
    adr011_typed_upsert_insert_unique_conflict_is_atomic,
    adr011_typed_schema_less_queue_unaffected,
    filtered_lifecycle_metrics_are_exact_and_read_only,
);

fn typed_qdef() -> pqueue_core::QueueDefinition {
    let mut def = pqueue_conformance::qdef();
    def.entity_schema = Some(
        serde_json::from_value::<EntitySchemaDocument>(json!({
            "entity_schema": {
                "type": "object",
                "required": ["name"],
                "properties": {
                    "name": {"type": "string"}
                }
            }
        }))
        .unwrap(),
    );
    def
}

fn typed_item(valid: bool) -> pqueue_engine::PushSpec {
    pqueue_engine::PushSpec {
        entity: Some(if valid {
            json!({"name": "ok"})
        } else {
            json!({"count": 1})
        }),
        ..Default::default()
    }
}

async fn schema_validation_backend<B>(backend: &B)
where
    B: ControlPlaneStore + PushPort + ProjectionRead,
{
    let shard = pqueue_conformance::shard();
    backend.create_queue(typed_qdef()).await.unwrap();

    let err = backend
        .push(
            &shard,
            vec![typed_item(false)],
            pqueue_conformance::ts(0),
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::EntitySchemaViolation(_)));
    assert_eq!(backend.metrics(&shard).await.unwrap().pending, 0);

    let rid = RequestId::new("req-1").unwrap();
    let err = backend
        .push_with_request_id(
            &shard,
            rid.clone(),
            vec![typed_item(false)],
            pqueue_conformance::ts(1),
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::EntitySchemaViolation(_)));
    assert_eq!(backend.metrics(&shard).await.unwrap().pending, 0);

    let first = backend
        .push_with_request_id(
            &shard,
            rid.clone(),
            vec![typed_item(true)],
            pqueue_conformance::ts(2),
            None,
        )
        .await
        .unwrap();
    let replay = backend
        .push_with_request_id(
            &shard,
            rid,
            vec![typed_item(true)],
            pqueue_conformance::ts(3),
            None,
        )
        .await
        .unwrap();
    assert_eq!(first, replay);
    assert_eq!(backend.metrics(&shard).await.unwrap().pending, 1);
}

#[test]
fn schema_validation_rejects_before_append_and_idempotency_on_postgres_relational() {
    match std::env::var("PQUEUE_PG_TEST_URL") {
        Ok(url) => {
            let schema = fresh_schema();
            futures::executor::block_on(async {
                let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema)
                    .expect("connect postgres (is PQUEUE_PG_TEST_URL a live DB?)");
                schema_validation_backend(&backend).await;
            });
        }
        Err(_) => {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (schema_validation_rejects_before_append_and_idempotency_on_postgres_relational) — set PQUEUE_PG_TEST_URL to a live DB"
            );
        }
    }
}

#[test]
fn commit_transition_shared_scenario_runs_against_postgres_relational() {
    use pqueue_conformance::scenarios::commit_transition_writes_side_records_enqueues_lifecycle_finalizes_and_survives_reopen;

    match std::env::var("PQUEUE_PG_TEST_URL") {
        Ok(url) => {
            let schema = fresh_schema();
            futures::executor::block_on(async {
                let make = || {
                    PostgresRelationalBackend::connect_in_schema(&url, &schema)
                        .expect("connect postgres (is PQUEUE_PG_TEST_URL a live DB?)")
                };
                commit_transition_writes_side_records_enqueues_lifecycle_finalizes_and_survives_reopen(
                    make,
                )
                .await;
            });
        }
        Err(_) => {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (commit_transition_shared_scenario_runs_against_postgres_relational) — set PQUEUE_PG_TEST_URL to a live DB"
            );
        }
    }
}

#[test]
fn commit_transition_explain_commit_shared_scenario_runs_against_postgres_relational() {
    use pqueue_conformance::scenarios::commit_transition_explain_commit_recovers_transition_and_survives_reopen;

    match std::env::var("PQUEUE_PG_TEST_URL") {
        Ok(url) => {
            let schema = fresh_schema();
            futures::executor::block_on(async {
                let make = || {
                    PostgresRelationalBackend::connect_in_schema(&url, &schema)
                        .expect("connect postgres (is PQUEUE_PG_TEST_URL a live DB?)")
                };
                commit_transition_explain_commit_recovers_transition_and_survives_reopen(make)
                    .await;

                let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema)
                    .expect("reconnect postgres (is PQUEUE_PG_TEST_URL a live DB?)");
                assert_eq!(
                    backend
                        .side_record(&pqueue_conformance::qkey(), b"missing/audit-key")
                        .await
                        .unwrap(),
                    None
                );
            });
        }
        Err(_) => {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (commit_transition_explain_commit_shared_scenario_runs_against_postgres_relational) — set PQUEUE_PG_TEST_URL to a live DB"
            );
        }
    }
}

#[test]
fn postgres_relational_recovery_high_water() {
    match std::env::var("PQUEUE_PG_TEST_URL") {
        Ok(url) => {
            let schema = fresh_schema();
            futures::executor::block_on(async {
                let backend = composed_postgres_relational_in_schema(&url, &schema)
                    .expect("connect postgres (is PQUEUE_PG_TEST_URL a live DB?)");
                backend
                    .create_queue(pqueue_conformance::qdef())
                    .await
                    .unwrap();
                backend
                    .push_with_request_id(
                        &pqueue_conformance::shard(),
                        RequestId::new("relational-high-water").unwrap(),
                        vec![
                            pqueue_engine::PushSpec::default(),
                            pqueue_engine::PushSpec::default(),
                        ],
                        pqueue_conformance::ts(0),
                        None,
                    )
                    .await
                    .unwrap();
                backend
                    .claim(pqueue_conformance::claim_req(1, 500, 10))
                    .await
                    .unwrap();
                assert_eq!(
                    backend.with_projection(|projection| {
                        projection
                            .recovery_high_water(&pqueue_conformance::shard())
                            .unwrap()
                    }),
                    // push(2 items) is ONE command (seq 0); claim is seq 1. `create_queue` is
                    // control-plane, not a sequenced command, so recovery_high_water (last-applied =
                    // next_seq-1) is seq 1 here — not 2 (which miscounted create_queue as seq 0).
                    Some(CommandPosition::new(pqueue_conformance::shard(), 0, 1))
                );
            });

            let reopened = composed_postgres_relational_in_schema(&url, &schema)
                .expect("reconnect postgres (is PQUEUE_PG_TEST_URL a live DB?)");
            assert_eq!(
                reopened.with_projection(|projection| {
                    projection
                        .recovery_high_water(&pqueue_conformance::shard())
                        .unwrap()
                }),
                Some(CommandPosition::new(pqueue_conformance::shard(), 0, 1)),
                "the relational projection must reopen at the last applied position"
            );
        }
        Err(_) => {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (TestPostgresRelationalRecoveryHighWater) — set PQUEUE_PG_TEST_URL to a live DB"
            );
        }
    }
}

#[test]
fn postgres_relational_truncate_then_recover_exact_state() {
    match std::env::var("PQUEUE_PG_TEST_URL") {
        Ok(url) => {
            let schema = fresh_schema();
            let request_id = RequestId::new("replay-1").unwrap();
            let body = vec![
                pqueue_engine::PushSpec {
                    priority: Some(pqueue_core::PriorityValue::Int64(10)),
                    ..Default::default()
                },
                pqueue_engine::PushSpec {
                    priority: Some(pqueue_core::PriorityValue::Int64(20)),
                    ..Default::default()
                },
            ];
            let (original_ids, fence_epoch) = futures::executor::block_on(async {
                let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema)
                    .expect("connect postgres (is PQUEUE_PG_TEST_URL a live DB?)");
                backend
                    .create_queue(pqueue_conformance::qdef())
                    .await
                    .unwrap();
                let original_ids = backend
                    .push_with_request_id(
                        &pqueue_conformance::shard(),
                        request_id.clone(),
                        body.clone(),
                        pqueue_conformance::ts(0),
                        None,
                    )
                    .await
                    .unwrap();

                let claimed = backend
                    .claim(pqueue_conformance::claim_req(1, 500, 10))
                    .await
                    .unwrap();
                assert_eq!(claimed.items.len(), 1, "one leased item before reopen");
                let fence_epoch = backend
                    .acquire_epoch(&pqueue_conformance::shard())
                    .await
                    .unwrap();

                let metrics = backend.metrics(&pqueue_conformance::shard()).await.unwrap();
                assert_eq!((metrics.pending, metrics.leased), (1, 1));
                assert_eq!(
                    backend
                        .current_epoch(&pqueue_conformance::shard())
                        .await
                        .unwrap(),
                    fence_epoch
                );
                (original_ids, fence_epoch)
            });

            let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema)
                .expect("reconnect postgres (is PQUEUE_PG_TEST_URL a live DB?)");
            futures::executor::block_on(async {
                assert_eq!(
                    backend
                        .current_epoch(&pqueue_conformance::shard())
                        .await
                        .unwrap(),
                    fence_epoch,
                    "the durable fence epoch survives reopen"
                );
                assert_eq!(
                    backend.metrics(&pqueue_conformance::shard()).await.unwrap(),
                    pqueue_engine::QueueMetrics {
                        pending: 1,
                        leased: 1,
                        complete: 0,
                        failed: 0,
                        resident_terminal_count: 0,
                    },
                    "the item lifecycle counts survive reopen"
                );
                assert!(
                    backend
                        .pending(&pqueue_conformance::shard())
                        .await
                        .unwrap()
                        .is_empty(),
                    "the leased item's live token is dropped on reopen"
                );

                let replayed = backend
                    .push_with_request_id(
                        &pqueue_conformance::shard(),
                        request_id,
                        body,
                        pqueue_conformance::ts(1),
                        None,
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    replayed, original_ids,
                    "request-id replay returns the original item ids"
                );

                let fresh = backend
                    .push(
                        &pqueue_conformance::shard(),
                        vec![pqueue_engine::PushSpec::default()],
                        pqueue_conformance::ts(2),
                        None,
                    )
                    .await
                    .unwrap();
                assert_eq!(fresh.len(), 1);
                assert!(
                    !original_ids.contains(&fresh[0]),
                    "counter recovery must mint a fresh item id after reopen"
                );
            });
        }
        Err(_) => {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (TestPostgresRelationalTruncateThenRecoverExactState) — set PQUEUE_PG_TEST_URL to a live DB"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// ADR-011 typed secondary index conformance — postgres relational backend
// These tests mirror pqueue-sqlite/tests/relational_conformance.rs §9 and are
// env-gated on PQUEUE_PG_TEST_URL exactly like the rest of this file.
// ---------------------------------------------------------------------------

use axon_esf::IndexDef;
use pqueue_core::{ClientItemKey, IndexDeclaration, IndexType, QueueIndex};
use pqueue_engine::{IndexQueryPort, PayloadUpdate, PurgePort, UpdateFieldsPort, UpsertPort};
use std::collections::BTreeMap;

fn pg_qdef_unique_str_index(index_name: &str, field: &str) -> pqueue_core::QueueDefinition {
    pqueue_core::QueueDefinition {
        typed_indexes: vec![QueueIndex {
            name: index_name.to_string(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: field.to_string(),
                index_type: IndexType::String,
                unique: true,
            }),
        }],
        ..pqueue_conformance::qdef()
    }
}

fn pg_qdef_nonunique_str_index(index_name: &str, field: &str) -> pqueue_core::QueueDefinition {
    pqueue_core::QueueDefinition {
        typed_indexes: vec![QueueIndex {
            name: index_name.to_string(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: field.to_string(),
                index_type: IndexType::String,
                unique: false,
            }),
        }],
        ..pqueue_conformance::qdef()
    }
}

fn pg_entity(field: &str, value: &str) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    m.insert(
        field.to_string(),
        serde_json::Value::String(value.to_string()),
    );
    serde_json::Value::Object(m)
}

fn pg_typed_connect(url: &str, schema: &str) -> PostgresRelationalBackend {
    PostgresRelationalBackend::connect_in_schema(url, schema)
        .expect("connect postgres (is PQUEUE_PG_TEST_URL a live DB?)")
}

#[test]
fn pg_rel_typed_index_push_then_get_unique() {
    match std::env::var("PQUEUE_PG_TEST_URL") {
        Ok(url) => {
            let schema = fresh_schema();
            futures::executor::block_on(async {
                let b = pg_typed_connect(&url, &schema);
                let shard = pqueue_conformance::shard();
                b.create_queue(pg_qdef_unique_str_index("by_email", "email"))
                    .await
                    .unwrap();
                let ids = b
                    .push(
                        &shard,
                        vec![pqueue_engine::PushSpec {
                            entity: Some(pg_entity("email", "alice@example.com")),
                            ..Default::default()
                        }],
                        pqueue_conformance::ts(0),
                        None,
                    )
                    .await
                    .unwrap();
                let hit = b
                    .index_get_unique(&shard, "by_email", &[b"alice@example.com".to_vec()])
                    .await
                    .unwrap();
                assert!(hit.is_some(), "indexed item must be findable");
                assert_eq!(hit.unwrap().item_id, ids[0]);
            });
        }
        Err(_) => eprintln!(
            "POSTGRES RELATIONAL SKIPPED (pg_rel_typed_index_push_then_get_unique) — set PQUEUE_PG_TEST_URL"
        ),
    }
}

#[test]
fn pg_rel_typed_index_push_then_lookup_nonunique() {
    match std::env::var("PQUEUE_PG_TEST_URL") {
        Ok(url) => {
            let schema = fresh_schema();
            futures::executor::block_on(async {
                let b = pg_typed_connect(&url, &schema);
                let shard = pqueue_conformance::shard();
                b.create_queue(pg_qdef_nonunique_str_index("by_tag", "tag"))
                    .await
                    .unwrap();
                let ids = b
                    .push(
                        &shard,
                        vec![
                            pqueue_engine::PushSpec {
                                entity: Some(pg_entity("tag", "red")),
                                ..Default::default()
                            },
                            pqueue_engine::PushSpec {
                                entity: Some(pg_entity("tag", "red")),
                                ..Default::default()
                            },
                        ],
                        pqueue_conformance::ts(0),
                        None,
                    )
                    .await
                    .unwrap();
                let hits = b
                    .index_lookup(&shard, "by_tag", &[b"red".to_vec()])
                    .await
                    .unwrap();
                assert_eq!(hits.len(), 2);
                let mut found: Vec<_> = hits.iter().map(|h| h.item_id).collect();
                found.sort();
                let mut expected = ids.to_vec();
                expected.sort();
                assert_eq!(found, expected);
            });
        }
        Err(_) => eprintln!(
            "POSTGRES RELATIONAL SKIPPED (pg_rel_typed_index_push_then_lookup_nonunique) — set PQUEUE_PG_TEST_URL"
        ),
    }
}

#[test]
fn pg_rel_typed_unique_index_conflict_is_rejected() {
    match std::env::var("PQUEUE_PG_TEST_URL") {
        Ok(url) => {
            let schema = fresh_schema();
            futures::executor::block_on(async {
                let b = pg_typed_connect(&url, &schema);
                let shard = pqueue_conformance::shard();
                b.create_queue(pg_qdef_unique_str_index("by_email", "email"))
                    .await
                    .unwrap();
                b.push(
                    &shard,
                    vec![pqueue_engine::PushSpec {
                        entity: Some(pg_entity("email", "alice@example.com")),
                        ..Default::default()
                    }],
                    pqueue_conformance::ts(0),
                    None,
                )
                .await
                .unwrap();
                let result = b
                    .push(
                        &shard,
                        vec![pqueue_engine::PushSpec {
                            entity: Some(pg_entity("email", "alice@example.com")),
                            ..Default::default()
                        }],
                        pqueue_conformance::ts(1),
                        None,
                    )
                    .await;
                assert!(
                    matches!(result, Err(EngineError::Conflict)),
                    "duplicate unique key must be rejected: {:?}",
                    result
                );
                assert_eq!(b.metrics(&shard).await.unwrap().pending, 1);
            });
        }
        Err(_) => eprintln!(
            "POSTGRES RELATIONAL SKIPPED (pg_rel_typed_unique_index_conflict_is_rejected) — set PQUEUE_PG_TEST_URL"
        ),
    }
}

#[test]
fn pg_rel_typed_unique_index_within_batch_conflict_rejected() {
    match std::env::var("PQUEUE_PG_TEST_URL") {
        Ok(url) => {
            let schema = fresh_schema();
            futures::executor::block_on(async {
                let b = pg_typed_connect(&url, &schema);
                let shard = pqueue_conformance::shard();
                b.create_queue(pg_qdef_unique_str_index("by_email", "email"))
                    .await
                    .unwrap();
                let result = b
                    .push(
                        &shard,
                        vec![
                            pqueue_engine::PushSpec {
                                entity: Some(pg_entity("email", "shared@example.com")),
                                ..Default::default()
                            },
                            pqueue_engine::PushSpec {
                                entity: Some(pg_entity("email", "shared@example.com")),
                                ..Default::default()
                            },
                        ],
                        pqueue_conformance::ts(0),
                        None,
                    )
                    .await;
                assert!(
                    matches!(result, Err(EngineError::Conflict)),
                    "within-batch duplicate must be rejected"
                );
                assert_eq!(
                    b.metrics(&shard).await.unwrap().pending,
                    0,
                    "nothing inserted"
                );
            });
        }
        Err(_) => eprintln!(
            "POSTGRES RELATIONAL SKIPPED (pg_rel_typed_unique_index_within_batch_conflict_rejected) — set PQUEUE_PG_TEST_URL"
        ),
    }
}

#[test]
fn pg_rel_typed_index_purge_frees_unique_key() {
    match std::env::var("PQUEUE_PG_TEST_URL") {
        Ok(url) => {
            let schema = fresh_schema();
            futures::executor::block_on(async {
                let b = pg_typed_connect(&url, &schema);
                let shard = pqueue_conformance::shard();
                b.create_queue(pg_qdef_unique_str_index("by_email", "email"))
                    .await
                    .unwrap();
                let ids = b
                    .push(
                        &shard,
                        vec![pqueue_engine::PushSpec {
                            entity: Some(pg_entity("email", "purge_me@example.com")),
                            ..Default::default()
                        }],
                        pqueue_conformance::ts(0),
                        None,
                    )
                    .await
                    .unwrap();
                b.purge(&shard, vec![ids[0]], false, pqueue_conformance::ts(1), None)
                    .await
                    .unwrap();
                let hit = b
                    .index_get_unique(&shard, "by_email", &[b"purge_me@example.com".to_vec()])
                    .await
                    .unwrap();
                assert!(hit.is_none(), "purged item must not appear in index");
                let new_ids = b
                    .push(
                        &shard,
                        vec![pqueue_engine::PushSpec {
                            entity: Some(pg_entity("email", "purge_me@example.com")),
                            ..Default::default()
                        }],
                        pqueue_conformance::ts(2),
                        None,
                    )
                    .await
                    .unwrap();
                assert_eq!(new_ids.len(), 1);
            });
        }
        Err(_) => eprintln!(
            "POSTGRES RELATIONAL SKIPPED (pg_rel_typed_index_purge_frees_unique_key) — set PQUEUE_PG_TEST_URL"
        ),
    }
}

#[test]
fn pg_rel_typed_index_replace_pending_updates_index() {
    match std::env::var("PQUEUE_PG_TEST_URL") {
        Ok(url) => {
            let schema = fresh_schema();
            futures::executor::block_on(async {
                let b = pg_typed_connect(&url, &schema);
                let shard = pqueue_conformance::shard();
                b.create_queue(pg_qdef_unique_str_index("by_email", "email"))
                    .await
                    .unwrap();
                let key = ClientItemKey::new("ck1").unwrap();
                b.replace_if_pending(
                    &shard,
                    &key,
                    None,
                    None,
                    None,
                    None,
                    BTreeMap::new(),
                    Default::default(),
                    Some(pg_entity("email", "orig@example.com")),
                    pqueue_conformance::ts(0),
                    None,
                )
                .await
                .unwrap();
                let result = b
                    .replace_if_pending(
                        &shard,
                        &key,
                        None,
                        None,
                        None,
                        None,
                        BTreeMap::new(),
                        Default::default(),
                        Some(pg_entity("email", "orig@example.com")),
                        pqueue_conformance::ts(1),
                        None,
                    )
                    .await;
                assert!(
                    result.is_ok(),
                    "replacing with same unique key must not conflict: {:?}",
                    result
                );
                assert_eq!(b.metrics(&shard).await.unwrap().pending, 1);
                let hit = b
                    .index_get_unique(&shard, "by_email", &[b"orig@example.com".to_vec()])
                    .await
                    .unwrap();
                assert!(hit.is_some(), "replacement must be in the index");
            });
        }
        Err(_) => eprintln!(
            "POSTGRES RELATIONAL SKIPPED (pg_rel_typed_index_replace_pending_updates_index) — set PQUEUE_PG_TEST_URL"
        ),
    }
}

#[test]
fn pg_rel_typed_index_update_fields_moves_index_key() {
    match std::env::var("PQUEUE_PG_TEST_URL") {
        Ok(url) => {
            let schema = fresh_schema();
            futures::executor::block_on(async {
                let b = pg_typed_connect(&url, &schema);
                let shard = pqueue_conformance::shard();
                b.create_queue(pg_qdef_unique_str_index("by_email", "email"))
                    .await
                    .unwrap();
                let ids = b
                    .push(
                        &shard,
                        vec![pqueue_engine::PushSpec {
                            entity: Some(pg_entity("email", "old@example.com")),
                            ..Default::default()
                        }],
                        pqueue_conformance::ts(0),
                        None,
                    )
                    .await
                    .unwrap();
                b.update_fields(
                    &shard,
                    ids[0],
                    BTreeMap::new(),
                    PayloadUpdate::Keep,
                    Some(pg_entity("email", "new@example.com")),
                    None,
                    pqueue_conformance::ts(1),
                    None,
                )
                .await
                .unwrap();
                let old = b
                    .index_get_unique(&shard, "by_email", &[b"old@example.com".to_vec()])
                    .await
                    .unwrap();
                assert!(old.is_none(), "old typed-index key must be removed");
                let new = b
                    .index_get_unique(&shard, "by_email", &[b"new@example.com".to_vec()])
                    .await
                    .unwrap();
                assert_eq!(
                    new.expect("new typed-index key must find item").item_id,
                    ids[0]
                );
            });
        }
        Err(_) => eprintln!(
            "POSTGRES RELATIONAL SKIPPED (pg_rel_typed_index_update_fields_moves_index_key) — set PQUEUE_PG_TEST_URL"
        ),
    }
}

#[test]
fn pg_rel_typed_index_update_fields_unique_conflict_is_atomic() {
    match std::env::var("PQUEUE_PG_TEST_URL") {
        Ok(url) => {
            let schema = fresh_schema();
            futures::executor::block_on(async {
                let b = pg_typed_connect(&url, &schema);
                let shard = pqueue_conformance::shard();
                b.create_queue(pg_qdef_unique_str_index("by_email", "email"))
                    .await
                    .unwrap();
                let ids = b
                    .push(
                        &shard,
                        vec![
                            pqueue_engine::PushSpec {
                                entity: Some(pg_entity("email", "a@example.com")),
                                ..Default::default()
                            },
                            pqueue_engine::PushSpec {
                                entity: Some(pg_entity("email", "b@example.com")),
                                ..Default::default()
                            },
                        ],
                        pqueue_conformance::ts(0),
                        None,
                    )
                    .await
                    .unwrap();
                let result = b
                    .update_fields(
                        &shard,
                        ids[1],
                        BTreeMap::new(),
                        PayloadUpdate::Keep,
                        Some(pg_entity("email", "a@example.com")),
                        None,
                        pqueue_conformance::ts(1),
                        None,
                    )
                    .await;
                assert!(
                    matches!(result, Err(EngineError::Conflict)),
                    "moving into an occupied unique typed-index key must conflict"
                );
                let hit = b
                    .index_get_unique(&shard, "by_email", &[b"b@example.com".to_vec()])
                    .await
                    .unwrap();
                assert_eq!(
                    hit.expect("failed update must keep old typed-index row")
                        .item_id,
                    ids[1]
                );
                assert_eq!(b.metrics(&shard).await.unwrap().pending, 2);
            });
        }
        Err(_) => eprintln!(
            "POSTGRES RELATIONAL SKIPPED (pg_rel_typed_index_update_fields_unique_conflict_is_atomic) — set PQUEUE_PG_TEST_URL"
        ),
    }
}

#[test]
fn pg_rel_typed_index_schema_less_queue_unaffected() {
    match std::env::var("PQUEUE_PG_TEST_URL") {
        Ok(url) => {
            let schema = fresh_schema();
            futures::executor::block_on(async {
                let b = pg_typed_connect(&url, &schema);
                let shard = pqueue_conformance::shard();
                b.create_queue(pqueue_conformance::qdef()).await.unwrap();
                let ids = b
                    .push(
                        &shard,
                        vec![pqueue_engine::PushSpec::default()],
                        pqueue_conformance::ts(0),
                        None,
                    )
                    .await
                    .unwrap();
                assert_eq!(ids.len(), 1);
                assert_eq!(b.metrics(&shard).await.unwrap().pending, 1);
                let res = b
                    .index_get_unique(&shard, "nonexistent", &[b"x".to_vec()])
                    .await;
                assert!(res.is_err(), "unknown index name must error");
            });
        }
        Err(_) => eprintln!(
            "POSTGRES RELATIONAL SKIPPED (pg_rel_typed_index_schema_less_queue_unaffected) — set PQUEUE_PG_TEST_URL"
        ),
    }
}
