//! ADR-012 P1b-ii: the CORE conformance class against the UNIFIED postgres-relational COMPOSITION
//! (`ComposedBackend<PostgresRelational, PostgresRelational, InProcessControlPlane>` — one relational store
//! on both the log and projection axes, so the generic `commit_locked` drives append+apply into ONE durable
//! postgres transaction). **Env-gated** on a live database via `FIREWEED_PG_TEST_URL`.
//!
//! Each `make()` call gets a FRESH process-unique schema (the postgres analogue of a fresh `:memory:`
//! sqlite store), so every backend the scenario builds is independent — matching how the sqlite unified
//! composition runs `core_suite!(@atomic)` over fresh `:memory:` stores. The relational-reconnect class is
//! NOT included here (it reopens shared durable state, which the in-process control plane does not retain);
//! the monolithic `PostgresRelationalBackend` carries that class in `relational_conformance.rs`.
//!
//! If `FIREWEED_PG_TEST_URL` is ABSENT, every scenario prints a LOUD skip — a green run is then VISIBLY
//! partial (the unified postgres composition unverified against a live DB), never a hidden pass. Compiling
//! this file already proves the composition satisfies `ConformanceCore`. To run live:
//!   docker run -d --name fireweed-pg -p 5433:5432 -e POSTGRES_PASSWORD=fireweed postgres:16
//!   FIREWEED_PG_TEST_URL=postgres://postgres:fireweed@127.0.0.1:5433/postgres cargo test -p fireweed-postgres \
//!     --test composed_relational_conformance

use std::sync::atomic::{AtomicU64, Ordering};

use fireweed_postgres::PostgresRelationalBackend;

fn fresh_schema() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "fireweed_crel_{}_{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

/// One `#[test]` per scenario: env-gated, with a FRESH schema per `make()` so each composed backend is
/// independent. Driven by `futures::executor::block_on` (the sync postgres client panics under an ambient
/// tokio runtime).
macro_rules! pg_composed {
    ($($name:ident),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                match std::env::var("FIREWEED_PG_TEST_URL") {
                    Ok(url) => {
                        futures::executor::block_on(fireweed_conformance::scenarios::$name(move || {
                            let schema = fresh_schema();
                            PostgresRelationalBackend::connect_in_schema(&url, &schema)
                                .expect("connect postgres (is FIREWEED_PG_TEST_URL a live DB?)")
                        }));
                    }
                    Err(_) => { panic!(
                            "POSTGRES UNIFIED COMPOSITION SKIPPED ({}) — set FIREWEED_PG_TEST_URL to a live DB",
                            stringify!($name)
                        ); }
                }
            }
        )+
    };
}

// The full CORE class (the `core_suite!(@atomic)` scenario list) against the unified composition.
pg_composed!(
    push_then_select_eligible_in_priority_order,
    claim_then_complete_lifecycle,
    claim_returns_priority_ordered_rich_items,
    claim_empty_when_nothing_eligible,
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
    claim_compatibility_is_resolved_and_gated,
    stale_epoch_append_is_fenced,
    epoch_fence_closes_pre_segment_window,
);

/// The claimed-item-shape gate-key assertion is `_if_supported`: the orthogonal composition reports
/// `supports_gates() == false` (gate state is a relational-only feature the in-process/log-replay family
/// stores, NOT exposed through the generic composition), so this is a no-op skip — exactly as the sqlite
/// unified composition handles it in `core_suite!(@atomic)`.
#[test]
fn claimed_item_shape_includes_payload_fields_and_gate_keys_if_supported() {
    match std::env::var("FIREWEED_PG_TEST_URL") {
        Ok(url) => {
            futures::executor::block_on(
                fireweed_conformance::claimed_item_shape_includes_payload_fields_and_gate_keys_if_supported(
                    move || {
                        let schema = fresh_schema();
                        PostgresRelationalBackend::connect_in_schema(&url, &schema)
                            .expect("connect postgres (is FIREWEED_PG_TEST_URL a live DB?)")
                    },
                ),
            );
        }
        Err(_) => {
            panic!(
                "POSTGRES UNIFIED COMPOSITION SKIPPED (claimed_item_shape_includes_payload_fields_and_gate_keys_if_supported) — set FIREWEED_PG_TEST_URL to a live DB"
            );
        }
    }
}
