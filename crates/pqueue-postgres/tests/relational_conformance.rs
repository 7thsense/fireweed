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

use pqueue_postgres::PostgresRelationalBackend;

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
    tick_reclaims_expired_lease_with_no_client_traffic,
    tick_lease_boundary_is_half_open,
    paused_queue_yields_no_claims,
    fenced_lease_finalize_is_stale,
    claimed_view_renders_leased_items,
    retry_beyond_max_attempts_goes_terminal,
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
    reconnect_after_crash_preserves_committed_state,
    reconnect_preserves_terminal_and_pending_state,
    reconnect_preserves_leased_item_state,
    claim_compatibility_is_resolved_and_gated,
    stale_epoch_append_is_fenced,
    epoch_fence_closes_pre_segment_window,
);
