//! Reopen/recovery for the COMPOSED postgres backend (`ComposedBackend<PostgresLog, InMemoryProjection,
//! InProcessControlPlane>`, ADR-012 P2). Proves the generic `ComposedBackend::recover` rebuilds the
//! in-memory projection by replaying the durable postgres log on reconnect — the same property the
//! monolithic `PostgresBackend` durability suite proves, now for the composed path. Env-gated on
//! `PQUEUE_PG_TEST_URL`; LOUD-skips if absent.

use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_conformance::{claim_req, commit, envelope, item, qdef, qkey};
use pqueue_engine::{ClaimPort, ControlPlaneStore, ProjectionRead, PushCommand, QueueCommand};
use pqueue_postgres::composed_postgres_backend_in_schema;

fn pg_url() -> Option<String> {
    std::env::var("PQUEUE_PG_TEST_URL").ok()
}

fn fresh_schema(tag: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "pq_cmp_{}_{}_{}",
        tag,
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

#[test]
fn composed_postgres_projection_rebuilds_from_durable_log_on_reconnect() {
    let Some(url) = pg_url() else {
        eprintln!(
            "COMPOSED POSTGRES RECOVERY SKIPPED (reopen) — set PQUEUE_PG_TEST_URL to a live DB"
        );
        return;
    };
    futures::executor::block_on(reopen_inner(url));
}

async fn reopen_inner(url: String) {
    let schema = fresh_schema("reopen");

    // Session 1: create the queue, push three items, claim the highest-priority one.
    {
        let b = composed_postgres_backend_in_schema(&url, &schema).expect("connect");
        b.create_queue(qdef()).await.unwrap();
        commit(
            &b,
            envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![
                        item("1", "ka", 30),
                        item("2", "kb", 10),
                        item("3", "kc", 20),
                    ],
                }),
                vec![],
            ),
        )
        .await;
        let claimed = b.claim(claim_req(1, 500, 100)).await.unwrap();
        assert_eq!(claimed.items.len(), 1);
        let m = b.metrics(&qkey()).await.unwrap();
        assert_eq!((m.pending, m.leased), (2, 1));
    } // composition dropped — only the durable postgres rows (log + epoch + queue catalog) remain.

    // Session 2: RECONNECT to the same schema. The in-memory projection AND the in-process control plane are
    // gone; `ComposedBackend::recover` must rebuild both from the durable log + queue catalog WITHOUT a
    // re-create_queue.
    {
        let b = composed_postgres_backend_in_schema(&url, &schema).expect("reconnect");
        let m = b.metrics(&qkey()).await.unwrap();
        assert_eq!(
            (m.pending, m.leased),
            (2, 1),
            "composed reopen must reconstruct the projection from the durable log"
        );
        // The recovered control plane knows the queue (recovered from the durable catalog), so a further
        // claim works against the rebuilt projection.
        let claimed = b.claim(claim_req(2, 500, 200)).await.unwrap();
        assert_eq!(
            claimed.items.len(),
            2,
            "the two pending items are claimable after reopen"
        );
    }
}
