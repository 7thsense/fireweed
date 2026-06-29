//! The shared backend-conformance suite run against the postgres backend, **env-gated** on a live
//! database via `PQUEUE_PG_TEST_URL` (e.g. `postgres://postgres:pq@127.0.0.1:5433/postgres`). Each
//! scenario gets its OWN connection isolated in a UNIQUE schema (`connect_in_schema` → `CREATE SCHEMA …;
//! SET search_path`), so cargo's concurrent tests never race on a shared `search_path` or shared rows.
//!
//! If `PQUEUE_PG_TEST_URL` is ABSENT, every scenario prints a LOUD skip and returns — a green run is then
//! VISIBLY partial (postgres unverified), never a hidden pass. To run it:
//!   docker run -d --name pq-pg -p 5433:5432 -e POSTGRES_PASSWORD=pq postgres:16
//!   PQUEUE_PG_TEST_URL=postgres://postgres:pq@127.0.0.1:5433/postgres cargo test -p pqueue-postgres

use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_postgres::PostgresBackend;

/// A process-unique schema name per backend instance (pid + monotonic counter), so concurrent scenarios
/// and repeated `make()` calls within a scenario never collide.
fn fresh_schema() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "pq_test_{}_{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

/// Generate one `#[test]` per conformance scenario, each env-gated + schema-isolated. Driven by a
/// NON-tokio executor (see the dev-dependency note: the sync postgres client panics under a tokio
/// runtime). Mirrors the atomic `conformance_suite!` scenario list (postgres is an atomic-class backend).
macro_rules! pg_conformance {
    ($($name:ident),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                match std::env::var("PQUEUE_PG_TEST_URL") {
                    Ok(url) => {
                        futures::executor::block_on(pqueue_conformance::scenarios::$name(|| {
                            PostgresBackend::connect_in_schema(&url, &fresh_schema())
                                .expect("connect postgres (is PQUEUE_PG_TEST_URL a live DB?)")
                        }));
                    }
                    Err(_) => {
                        eprintln!(
                            "POSTGRES CONFORMANCE SKIPPED ({}) — set PQUEUE_PG_TEST_URL to a live DB",
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
    claimed_item_shape_includes_payload_fields_and_gate_keys,
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
    pause_and_fence_reconstruct_from_log,
    high_water_advances_on_each_commit,
    peek_is_priority_ordered_and_nondestructive,
    pending_lists_leased_items,
    snapshots_write_read_latest,
    claim_compatibility_is_resolved_and_gated,
    stale_epoch_append_is_fenced,
    epoch_fence_closes_pre_segment_window,
);
