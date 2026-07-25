//! The shared backend-conformance suite run against the postgres backend, **env-gated** on a live
//! database via `FIREWEED_PG_TEST_URL` (e.g. `postgres://postgres:fireweed@127.0.0.1:5433/postgres`). Each
//! scenario gets its OWN connection isolated in a UNIQUE schema (`connect_in_schema` → `CREATE SCHEMA …;
//! SET search_path`), so cargo's concurrent tests never race on a shared `search_path` or shared rows.
//!
//! If `FIREWEED_PG_TEST_URL` is ABSENT, every scenario prints a LOUD skip and returns — a green run is then
//! VISIBLY partial (postgres unverified), never a hidden pass. To run it:
//!   docker run -d --name fireweed-pg -p 5433:5432 -e POSTGRES_PASSWORD=fireweed postgres:16
//!   FIREWEED_PG_TEST_URL=postgres://postgres:fireweed@127.0.0.1:5433/postgres cargo test -p fireweed-postgres

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use fireweed_core::{EntitySchemaDocument, RequestId};
use fireweed_engine::{
    CommandPosition, ControlPlaneStore, EngineError, LogRead, ProjectionRead, PushPort,
    SnapshotStore,
};
use fireweed_postgres::PostgresBackend;
use serde_json::json;

/// A process-unique schema name per backend instance (pid + monotonic counter), so concurrent scenarios
/// and repeated `make()` calls within a scenario never collide.
fn fresh_schema() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "fireweed_test_{}_{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

fn pg_url() -> Option<String> {
    std::env::var("FIREWEED_PG_TEST_URL").ok()
}

/// Generate one `#[test]` per conformance scenario, each env-gated + schema-isolated. Driven by a
/// NON-tokio executor (see the dev-dependency note: the sync postgres client panics under a tokio
/// runtime). Mirrors the atomic `conformance_suite!` scenario list (postgres is an atomic-class backend).
macro_rules! pg_conformance {
    ($($name:ident),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                match std::env::var("FIREWEED_PG_TEST_URL") {
                    Ok(url) => {
                        futures::executor::block_on(fireweed_conformance::scenarios::$name(|| {
                            PostgresBackend::connect_in_schema(&url, &fresh_schema())
                                .expect("connect postgres (is FIREWEED_PG_TEST_URL a live DB?)")
                        }));
                    }
                    Err(_) => {
                        eprintln!(
                            "POSTGRES CONFORMANCE SKIPPED ({}) — set FIREWEED_PG_TEST_URL to a live DB",
                            stringify!($name)
                        );
                    }
                }
            }
        )+
    };
}

pg_conformance!(
    push_then_select_eligible_in_priority_order,
    claim_then_complete_lifecycle,
    replace_pending_supersedes_old,
    high_water_is_monotonic,
    claim_returns_priority_ordered_rich_items,
    claim_empty_when_nothing_eligible,
    claimed_item_shape_includes_payload_fields_and_gate_keys_if_supported,
    claimed_item_shape_reflects_update_fields_after_reclaim,
    claimed_item_shape_omits_empty_conditionals,
    structured_live_items_are_ordered_and_only_live,
    upsert_inserts_then_replaces_pending,
    upsert_rejects_claimed_and_terminal,
    upsert_preserves_group_delay_and_payload_in_claim_shape,
    update_fields_merges_and_cas,
    reclaim_expired_sweeps_per_queue,
    tick_reclaims_expired_lease_with_no_client_traffic,
    tick_lease_boundary_is_half_open,
    paused_queue_yields_no_claims,
    fenced_lease_finalize_is_stale,
    renew_extends_lease_and_rejects,
    reassign_swaps_token_and_charges_attempt,
    claimed_view_renders_leased_items,
    purge_removes_present_items_and_gates_leased,
    retry_beyond_max_attempts_goes_terminal,
    retry_with_backoff_defers_eligibility,
    finalize_of_nonleased_item_is_rejected_without_appending,
    successful_push_is_visible_before_response_returns,
    rejected_finalize_leaves_visible_state_unchanged,
    request_id_push_replays_once_and_conflicts_on_body_change,
    pause_and_fence_reconstruct_from_log,
    high_water_advances_on_each_commit,
    peek_is_priority_ordered_and_nondestructive,
    pending_lists_leased_items,
    snapshots_write_read_latest,
    claim_compatibility_is_resolved_and_gated,
    stale_epoch_append_is_fenced,
    epoch_fence_closes_pre_segment_window,
);

fn typed_qdef() -> fireweed_core::QueueDefinition {
    let mut def = fireweed_conformance::qdef();
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

fn typed_item(valid: bool) -> fireweed_engine::PushSpec {
    fireweed_engine::PushSpec {
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
    let shard = fireweed_conformance::shard();
    backend.create_queue(typed_qdef()).await.unwrap();

    let err = backend
        .push(
            &shard,
            vec![typed_item(false)],
            fireweed_conformance::ts(0),
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
            fireweed_conformance::ts(1),
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
            fireweed_conformance::ts(2),
            None,
        )
        .await
        .unwrap();
    let replay = backend
        .push_with_request_id(
            &shard,
            rid,
            vec![typed_item(true)],
            fireweed_conformance::ts(3),
            None,
        )
        .await
        .unwrap();
    assert_eq!(first, replay);
    assert_eq!(backend.metrics(&shard).await.unwrap().pending, 1);
}

#[test]
fn schema_validation_rejects_before_append_and_idempotency_on_postgres_log() {
    match std::env::var("FIREWEED_PG_TEST_URL") {
        Ok(url) => {
            let schema = fresh_schema();
            futures::executor::block_on(async {
                let backend = PostgresBackend::connect_in_schema(&url, &schema)
                    .expect("connect postgres (is FIREWEED_PG_TEST_URL a live DB?)");
                schema_validation_backend(&backend).await;
            });
        }
        Err(_) => {
            eprintln!(
                "POSTGRES CONFORMANCE SKIPPED (schema_validation_rejects_before_append_and_idempotency_on_postgres_log) — set FIREWEED_PG_TEST_URL to a live DB"
            );
        }
    }
}

#[test]
fn commit_transition_shared_scenario_runs_against_postgres_log_replay() {
    use fireweed_conformance::scenarios::commit_transition_writes_side_records_enqueues_lifecycle_finalizes_and_survives_reopen;

    match std::env::var("FIREWEED_PG_TEST_URL") {
        Ok(url) => {
            let schema = fresh_schema();
            futures::executor::block_on(async {
                let make = || {
                    PostgresBackend::connect_in_schema(&url, &schema)
                        .expect("connect postgres (is FIREWEED_PG_TEST_URL a live DB?)")
                };
                commit_transition_writes_side_records_enqueues_lifecycle_finalizes_and_survives_reopen(
                    make,
                )
                .await;
            });
        }
        Err(_) => {
            eprintln!(
                "POSTGRES CONFORMANCE SKIPPED (commit_transition_shared_scenario_runs_against_postgres_log_replay) — set FIREWEED_PG_TEST_URL to a live DB"
            );
        }
    }
}

#[test]
fn postgres_high_water_concurrent_monotonic() {
    let Some(url) = pg_url() else {
        eprintln!(
            "POSTGRES CONFORMANCE SKIPPED (postgres_high_water_concurrent_monotonic) — set FIREWEED_PG_TEST_URL to a live DB"
        );
        return;
    };
    let schema = fresh_schema();
    futures::executor::block_on(async {
        let backend = PostgresBackend::connect_in_schema(&url, &schema)
            .expect("connect postgres (is FIREWEED_PG_TEST_URL a live DB?)");
        backend
            .create_queue(fireweed_conformance::qdef())
            .await
            .unwrap();
        let shard = fireweed_conformance::shard();
        let base = CommandPosition::new(shard.clone(), 0, 0);
        backend.set_high_water(&shard, base.clone()).await.unwrap();
    });

    let shard = fireweed_conformance::shard();
    for seq in 0..8u64 {
        let current = CommandPosition::new(shard.clone(), 0, seq);
        let next = CommandPosition::new(shard.clone(), 0, seq + 1);
        let barrier = Arc::new(Barrier::new(2));
        let handles = [
            {
                let barrier = barrier.clone();
                let url = url.clone();
                let schema = schema.clone();
                let shard = shard.clone();
                let current = current.clone();
                thread::spawn(move || {
                    let backend = PostgresBackend::connect_in_schema(&url, &schema)
                        .expect("connect postgres");
                    let observed = futures::executor::block_on(backend.high_water(&shard))
                        .expect("read high_water")
                        .expect("seeded high_water");
                    assert_eq!(
                        observed, current,
                        "both writers must race from the same prior value"
                    );
                    barrier.wait();
                    futures::executor::block_on(backend.set_high_water(&shard, current))
                })
            },
            {
                let barrier = barrier.clone();
                let url = url.clone();
                let schema = schema.clone();
                let shard = shard.clone();
                let current = current.clone();
                let next = next.clone();
                thread::spawn(move || {
                    let backend = PostgresBackend::connect_in_schema(&url, &schema)
                        .expect("connect postgres");
                    let observed = futures::executor::block_on(backend.high_water(&shard))
                        .expect("read high_water")
                        .expect("seeded high_water");
                    assert_eq!(
                        observed, current,
                        "both writers must race from the same prior value"
                    );
                    barrier.wait();
                    futures::executor::block_on(backend.set_high_water(&shard, next))
                })
            },
        ];

        let results = handles
            .into_iter()
            .map(|handle| handle.join().expect("writer thread panicked"))
            .collect::<Vec<_>>();
        assert!(
            results.iter().any(|r| r.is_ok()),
            "at least one writer should advance the high-water"
        );
        for result in results {
            match result {
                Ok(()) => {}
                Err(EngineError::Invalid("high-water regression")) => {}
                Err(err) => panic!("unexpected high-water result: {err:?}"),
            }
        }

        let verifier = PostgresBackend::connect_in_schema(&url, &schema).expect("connect postgres");
        let stored = futures::executor::block_on(verifier.high_water(&shard))
            .expect("read final high_water")
            .expect("high_water must exist");
        assert_eq!(stored, next, "the stored high-water must never regress");
    }
}

#[test]
fn postgres_append_concurrent_sequence_no_gap_no_dup() {
    let Some(url) = pg_url() else {
        eprintln!(
            "POSTGRES CONFORMANCE SKIPPED (postgres_append_concurrent_sequence_no_gap_no_dup) — set FIREWEED_PG_TEST_URL to a live DB"
        );
        return;
    };
    let schema = fresh_schema();
    futures::executor::block_on(async {
        let backend = PostgresBackend::connect_in_schema(&url, &schema)
            .expect("connect postgres (is FIREWEED_PG_TEST_URL a live DB?)");
        backend
            .create_queue(fireweed_conformance::qdef())
            .await
            .unwrap();
    });

    let shard = fireweed_conformance::shard();
    for round in 0..8u64 {
        let barrier = Arc::new(Barrier::new(2));
        let handles = [
            {
                let barrier = barrier.clone();
                let url = url.clone();
                let schema = schema.clone();
                let shard = shard.clone();
                thread::spawn(move || {
                    let backend = PostgresBackend::connect_in_schema(&url, &schema)
                        .expect("connect postgres")
                        .with_node_id(1);
                    barrier.wait();
                    futures::executor::block_on(backend.push(
                        &shard,
                        vec![typed_item(true)],
                        fireweed_conformance::ts(round as i64),
                        None,
                    ))
                })
            },
            {
                let barrier = barrier.clone();
                let url = url.clone();
                let schema = schema.clone();
                let shard = shard.clone();
                thread::spawn(move || {
                    let backend = PostgresBackend::connect_in_schema(&url, &schema)
                        .expect("connect postgres")
                        .with_node_id(2);
                    barrier.wait();
                    futures::executor::block_on(backend.push(
                        &shard,
                        vec![typed_item(true)],
                        fireweed_conformance::ts(round as i64 + 1),
                        None,
                    ))
                })
            },
        ];

        for handle in handles {
            handle
                .join()
                .expect("append thread panicked")
                .expect("concurrent append must succeed");
        }
    }

    let verifier = PostgresBackend::connect_in_schema(&url, &schema).expect("connect postgres");
    let page = futures::executor::block_on(verifier.read_from(&shard, None, 64)).expect("read log");
    assert_eq!(page.entries.len(), 16, "two successful appends per round");
    for (expected_seq, (position, _)) in page.entries.iter().enumerate() {
        assert_eq!(
            position.sequence, expected_seq as u64,
            "log sequences must be contiguous"
        );
    }
    assert!(page.next.is_none(), "the read should consume the full log");
}
