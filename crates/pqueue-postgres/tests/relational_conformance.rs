//! BQ-12 — the CORE conformance class + the relational-reconnect class run against the postgres
//! **relational** backend (`PostgresRelationalBackend`: DB-authoritative `pqueue_items`, real
//! `FOR UPDATE SKIP LOCKED` claim), **env-gated** on a live database via `PQUEUE_PG_TEST_URL`.
//!
//! Each scenario gets a process-unique schema (`connect_in_schema`); a scenario's repeated `make()` calls
//! reopen the SAME schema, so the relational-reconnect scenarios exercise real DB-authoritative recovery.
//!
//! If `PQUEUE_PG_TEST_URL` is ABSENT, every scenario prints a LOUD skip — a green run is then VISIBLY
//! partial (postgres unverified against a live DB), never a hidden pass. Compiling this file already proves
//! `PostgresRelationalBackend` satisfies `ConformanceCore` (the scenarios' generic bound). To run live:
//!   docker run -d --name pq-pg -p 5433:5432 -e POSTGRES_PASSWORD=pq postgres:16
//!   PQUEUE_PG_TEST_URL=postgres://postgres:pq@127.0.0.1:5433/postgres cargo test -p pqueue-postgres
//!
//! NOTE: only `ConformanceCore`-bounded scenarios appear here — the relational backend is log-optional
//! (no `LogRead`/`SnapshotStore`), so the log-class scenarios (high_water/snapshots/log-replay) do not
//! apply. The `FOR UPDATE SKIP LOCKED` contended-writer evidence is likewise live-DB-gated and pending.

use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_core::{EntitySchemaDocument, RequestId};
use pqueue_engine::{ControlPlaneStore, EngineError, ProjectionRead, PushPort};
use pqueue_postgres::PostgresRelationalBackend;
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
                "POSTGRES RELATIONAL SKIPPED ({}) — set PQUEUE_PG_TEST_URL to a live DB",
                "schema_validation_rejects_before_append_and_idempotency_on_postgres_relational"
            );
        }
    }
}
