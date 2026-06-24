//! Postgres-specific durability: the projection is a derived view; the LOG (in postgres tables) is the
//! source of truth. These tests reconnect to the SAME schema and assert the committed state is
//! reconstructed by replaying the durable log — the property the shared conformance suite (a fresh schema
//! per scenario) cannot exercise. Env-gated on `PQUEUE_PG_TEST_URL`; LOUD skip if absent.

use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_conformance::{claim_req, commit, envelope, item, qdef, qkey};
use pqueue_engine::{ClaimPort, ControlPlaneStore, ProjectionRead, PushCommand, QueueCommand};
use pqueue_postgres::PostgresBackend;

fn pg_url() -> Option<String> {
    std::env::var("PQUEUE_PG_TEST_URL").ok()
}

fn fresh_schema(tag: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "pq_dura_{}_{}_{}",
        tag,
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

#[test]
fn projection_rebuilds_from_durable_log_on_reconnect() {
    let Some(url) = pg_url() else {
        eprintln!("POSTGRES DURABILITY SKIPPED (rebuild) — set PQUEUE_PG_TEST_URL to a live DB");
        return;
    };
    futures::executor::block_on(projection_rebuilds_from_durable_log_on_reconnect_inner(url));
}

async fn projection_rebuilds_from_durable_log_on_reconnect_inner(url: String) {
    let schema = fresh_schema("reopen");

    // Session 1: create the queue, push three items, claim the highest-priority one.
    {
        let b = PostgresBackend::connect_in_schema(&url, &schema).expect("connect");
        b.create_queue(qdef()).await.unwrap();
        commit(
            &b,
            envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![item("a", "ka", 30), item("b", "kb", 10), item("c", "kc", 20)],
                }),
                vec![],
            ),
        )
        .await;
        // Claims "b" (priority 10, lowest = highest priority under ascending).
        let claimed = b.claim(claim_req(1, 500, 100)).await.unwrap();
        assert_eq!(claimed.items.len(), 1);
        let m = b.metrics(&qkey()).await.unwrap();
        assert_eq!((m.pending, m.leased), (2, 1));
    } // backend dropped — only the durable postgres rows remain.

    // Session 2: RECONNECT to the same schema. The in-memory projection is gone; it must be rebuilt from
    // the log.
    {
        let b = PostgresBackend::connect_in_schema(&url, &schema).expect("reconnect");
        let m = b.metrics(&qkey()).await.unwrap();
        assert_eq!(
            (m.pending, m.leased),
            (2, 1),
            "reconnected projection must reflect the 3 pushes + 1 claim replayed from the durable log"
        );
        // The still-eligible items are the two unclaimed ones, in priority order (c=20 before a=30).
        let elig = b
            .select_eligible(&pqueue_conformance::shard(), pqueue_conformance::ts(200), 10)
            .await
            .unwrap();
        let ids: Vec<&str> = elig.iter().map(|i| i.as_str()).collect();
        assert_eq!(ids, vec!["c", "a"], "eligibility order survives the rebuild");
    }
}

#[test]
fn orchestration_writes_after_reconnect_do_not_collide() {
    let Some(url) = pg_url() else {
        eprintln!("POSTGRES DURABILITY SKIPPED (recollide) — set PQUEUE_PG_TEST_URL to a live DB");
        return;
    };
    futures::executor::block_on(orchestration_writes_after_reconnect_do_not_collide_inner(url));
}

async fn orchestration_writes_after_reconnect_do_not_collide_inner(url: String) {
    // `cmd_seq` is restored past the highest replayed `pg-N`, so a claim AFTER a reconnect mints a fresh
    // command id and commits durably (a colliding id would fail the PK / corrupt the log).
    let schema = fresh_schema("recollide");
    {
        let b = PostgresBackend::connect_in_schema(&url, &schema).expect("connect");
        b.create_queue(qdef()).await.unwrap();
        commit(
            &b,
            envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![item("a", "ka", 5), item("b", "kb", 9)],
                }),
                vec![],
            ),
        )
        .await;
        // A claim goes through make_envelope -> "pg-0" durably.
        b.claim(claim_req(1, 500, 100)).await.unwrap();
    }
    {
        let b = PostgresBackend::connect_in_schema(&url, &schema).expect("reconnect");
        // Claim again post-reconnect: must succeed (fresh id, no collision) and lease the remaining item.
        let claimed = b.claim(claim_req(1, 500, 100)).await.unwrap();
        assert_eq!(claimed.items.len(), 1, "second item claimable after reconnect");
        let m = b.metrics(&qkey()).await.unwrap();
        assert_eq!(
            (m.pending, m.leased),
            (0, 2),
            "both items leased across the two sessions"
        );
    }
}
