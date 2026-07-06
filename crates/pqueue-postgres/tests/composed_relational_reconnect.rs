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

use pqueue_conformance::{claim_req, qdef, shard, ts};
use pqueue_core::{ItemId, PriorityValue};
use pqueue_engine::{
    ClaimPort, CommandPosition, ControlPlaneStore, ProjectionRead, ProjectionStore, PushPort,
    PushSpec,
};
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

fn unique_schema(tag: &str) -> String {
    format!("pq_crel_recon_{}_{}", std::process::id(), tag)
}

fn push(priority: i64) -> PushSpec {
    PushSpec {
        priority: Some(PriorityValue::Int64(priority)),
        ..Default::default()
    }
}

fn open(url: &str, schema: &str) -> pqueue_postgres::ComposedPostgresRelationalBackend {
    composed_postgres_relational_in_schema(url, schema)
        .expect("open composed postgres-relational db")
}

#[test]
fn TestComposedRelationalRecoverReplaysTail() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!(
            "POSTGRES UNIFIED COMPOSITION RECONNECT SKIPPED (TestComposedRelationalRecoverReplaysTail) — set PQUEUE_PG_TEST_URL to a live DB"
        );
        return;
    };
    let schema = unique_schema("tail");

    {
        let backend = open(&url, &schema);
        futures::executor::block_on(backend.create_queue(qdef())).unwrap();
        let first =
            futures::executor::block_on(backend.push(&shard(), vec![push(10)], ts(0), None))
                .unwrap();
        let second =
            futures::executor::block_on(backend.push(&shard(), vec![push(20)], ts(1), None))
                .unwrap();
        assert_eq!(first[0], ItemId::mint(0, 0, 0));
        assert_eq!(second[0], ItemId::mint(0, 0, 1));
        let claimed = futures::executor::block_on(backend.claim(claim_req(1, 500, 2))).unwrap();
        assert_eq!(claimed.items.len(), 1);
        assert_eq!(
            backend.with_projection(|projection| projection.recovery_high_water(&shard()).unwrap()),
            Some(CommandPosition::new(shard(), 0, 2)),
            "the composed reopen cursor should reflect the persisted applied high-water"
        );
    }

    let reopened = open(&url, &schema);
    assert_eq!(
        reopened.with_projection(|projection| projection.recovery_high_water(&shard()).unwrap()),
        Some(CommandPosition::new(shard(), 0, 2)),
        "recovery must resume from the durable relational cursor rather than genesis"
    );
    let metrics = futures::executor::block_on(reopened.metrics(&shard())).unwrap();
    assert_eq!((metrics.pending, metrics.leased), (1, 1));
}

#[test]
fn TestComposedRelationalRecoverySeedsCounters() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!(
            "POSTGRES UNIFIED COMPOSITION RECONNECT SKIPPED (TestComposedRelationalRecoverySeedsCounters) — set PQUEUE_PG_TEST_URL to a live DB"
        );
        return;
    };
    let schema = unique_schema("counters");

    {
        let backend = open(&url, &schema);
        futures::executor::block_on(backend.create_queue(qdef())).unwrap();
        let first =
            futures::executor::block_on(backend.push(&shard(), vec![push(10)], ts(0), None))
                .unwrap();
        let second =
            futures::executor::block_on(backend.push(&shard(), vec![push(20)], ts(1), None))
                .unwrap();
        assert_eq!(first[0], ItemId::mint(0, 0, 0));
        assert_eq!(second[0], ItemId::mint(0, 0, 1));
    }

    let reopened = open(&url, &schema);
    let third =
        futures::executor::block_on(reopened.push(&shard(), vec![push(30)], ts(2), None)).unwrap();
    assert_eq!(
        third[0],
        ItemId::mint(0, 0, 2),
        "item-id counters must resume past the durable projection snapshot"
    );
}
