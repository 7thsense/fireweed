//! ADR-012 P2 recovery-on-open: the durable-reconnect conformance class against the UNIFIED
//! postgres-relational COMPOSITION (`ComposedBackend<PostgresRelational, PostgresRelational,
//! InProcessControlPlane>`). **Env-gated** on a live database via `PQUEUE_PG_TEST_URL`.
//!
//! Each scenario calls `make()` twice (open → drop the handle → REOPEN the same schema). The composition's
//! `recover()` repopulates the in-process control plane from the durable `queues` catalog and re-seeds the
//! id-mint counters from `pqueue_items` — so the committed/terminal/pending/leased state survives the
//! reopen with NO re-create_queue, mirroring the monolithic `PostgresRelationalBackend`'s
//! `relational_conformance.rs` reconnect class. The schema is process-unique but STABLE per scenario (so the
//! two opens share it, and a fresh process starts from an empty schema — no leftover state).
//!
//! If `PQUEUE_PG_TEST_URL` is ABSENT, every scenario prints a LOUD skip — a green run is then VISIBLY
//! partial, never a hidden pass. Compiling this file already proves the composition satisfies the bound.

use pqueue_postgres::composed_postgres_relational_in_schema;

macro_rules! pg_reconnect {
    ($($name:ident),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                match std::env::var("PQUEUE_PG_TEST_URL") {
                    Ok(url) => {
                        // Process-unique + scenario-stable: the two opens within this scenario share the
                        // schema; a fresh process gets a brand-new (empty) schema, so no manual cleanup.
                        let schema = format!(
                            "pq_crel_recon_{}_{}",
                            std::process::id(),
                            stringify!($name)
                        );
                        futures::executor::block_on(pqueue_conformance::scenarios::$name(move || {
                            composed_postgres_relational_in_schema(&url, &schema)
                                .expect("connect postgres (is PQUEUE_PG_TEST_URL a live DB?)")
                        }));
                    }
                    Err(_) => {
                        eprintln!(
                            "POSTGRES UNIFIED COMPOSITION RECONNECT SKIPPED ({}) — set PQUEUE_PG_TEST_URL to a live DB",
                            stringify!($name)
                        );
                    }
                }
            }
        )+
    };
}

// The durable-reconnect class (the `durable_reconnect_suite!` scenario list) against the unified composition.
pg_reconnect!(
    reconnect_after_crash_preserves_committed_state,
    reconnect_preserves_terminal_and_pending_state,
    reconnect_preserves_leased_item_state,
    reconnect_after_rejected_mutation_has_no_phantom_commit,
);
