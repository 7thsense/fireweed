//! Reopen/recovery for the COMPOSED postgres backend (`ComposedBackend<PostgresLog, InMemoryProjection,
//! InProcessControlPlane>`, ADR-012 P2). Proves the generic `ComposedBackend::recover` rebuilds the
//! in-memory projection by replaying the durable postgres log on reconnect — the same property the
//! monolithic `PostgresBackend` durability suite proves, now for the composed path. Env-gated on
//! `PQUEUE_PG_TEST_URL`; LOUD-skips if absent.

use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_conformance::{claim_req, commit, envelope, item, qdef, qkey};
use pqueue_engine::{
    ClaimPort, ControlPlaneStore, LogStore, ProjectionRead, PushCommand, QueueCommand,
};
use pqueue_postgres::{PostgresLog, composed_postgres_backend_in_schema};

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

#[test]
fn postgres_log_pagination_resumes_after_last_returned_position() {
    let Some(url) = pg_url() else {
        eprintln!(
            "COMPOSED POSTGRES RECOVERY SKIPPED (pagination) — set PQUEUE_PG_TEST_URL to a live DB"
        );
        return;
    };
    let mut log = PostgresLog::connect_in_schema(&url, &fresh_schema("pagination"))
        .expect("connect postgres log");
    let shard = qkey();
    log.ensure_shard(&shard).unwrap();
    let epoch = log.acquire_epoch(&shard).unwrap();
    let commands = [
        ("1", "page-a", 10),
        ("2", "page-b", 20),
        ("3", "page-c", 30),
    ]
    .into_iter()
    .map(|(id, key, priority)| {
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item(id, key, priority)],
            }),
            vec![],
        )
    })
    .collect::<Vec<_>>();
    log.append(&shard, &commands, epoch).unwrap();

    let first = log.read_from(&shard, None, 1).unwrap();
    assert_eq!(first.entries[0].0.sequence, 0);
    assert_eq!(
        first.next.as_ref().map(|position| position.sequence),
        Some(0)
    );
    let second = log.read_from(&shard, first.next, 1).unwrap();
    assert_eq!(second.entries[0].0.sequence, 1);
    assert_eq!(
        second.next.as_ref().map(|position| position.sequence),
        Some(1)
    );
    let third = log.read_from(&shard, second.next, 1).unwrap();
    assert_eq!(third.entries[0].0.sequence, 2);
    assert!(third.next.is_none());
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
