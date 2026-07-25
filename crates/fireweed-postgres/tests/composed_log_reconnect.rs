//! Reopen/recovery for the COMPOSED postgres backend (`ComposedBackend<PostgresLog, InMemoryProjection,
//! InProcessControlPlane>`, ADR-012 P2). Proves the generic `ComposedBackend::recover` rebuilds the
//! in-memory projection by replaying the durable postgres log on reconnect — the same property the
//! monolithic `PostgresBackend` durability suite proves, now for the composed path. Env-gated on
//! `FIREWEED_PG_TEST_URL`; LOUD-skips if absent.

use std::sync::atomic::{AtomicU64, Ordering};

use fireweed_conformance::{claim_req, commit, envelope, item, qdef, qkey};
use fireweed_engine::{
    ClaimPort, ControlPlaneStore, LogStore, ProjectionRead, PushCommand, QueueCommand,
};
use fireweed_postgres::{PostgresLog, composed_postgres_backend_in_schema};

fn pg_url() -> Option<String> {
    std::env::var("FIREWEED_PG_TEST_URL").ok()
}

fn fresh_schema(tag: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "fireweed_cmp_{}_{}_{}",
        tag,
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

#[test]
fn composed_postgres_projection_rebuilds_from_durable_log_on_reconnect() {
    let Some(url) = pg_url() else {
        eprintln!(
            "COMPOSED POSTGRES RECOVERY SKIPPED (reopen) — set FIREWEED_PG_TEST_URL to a live DB"
        );
        return;
    };
    futures::executor::block_on(reopen_inner(url));
}

#[test]
fn postgres_log_pagination_resumes_after_last_returned_position() {
    let Some(url) = pg_url() else {
        eprintln!(
            "COMPOSED POSTGRES RECOVERY SKIPPED (pagination) — set FIREWEED_PG_TEST_URL to a live DB"
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

#[test]
fn postgres_log_batches_sequence_allocation_and_pages_across_insert_chunks() {
    let Some(url) = pg_url() else {
        eprintln!(
            "COMPOSED POSTGRES RECOVERY SKIPPED (batched append) — set FIREWEED_PG_TEST_URL to a live DB"
        );
        return;
    };
    let mut log = PostgresLog::connect_in_schema(&url, &fresh_schema("batched_append"))
        .expect("connect postgres log");
    let shard = qkey();
    log.ensure_shard(&shard).unwrap();
    let epoch = log.acquire_epoch(&shard).unwrap();

    // 1,025 crosses the production insert chunk boundary. The entire append still reserves one sequence
    // range and advances high-water only after every chunk is inserted in the same transaction.
    let commands = (0..1_025)
        .map(|index| {
            let id = (index + 1_000_000).to_string();
            let key = format!("batch-key-{index}");
            envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![item(&id, &key, index as i64)],
                }),
                vec![],
            )
        })
        .collect::<Vec<_>>();
    let positions = log.append(&shard, &commands, epoch).unwrap();
    assert_eq!(positions.len(), commands.len());
    for (expected, position) in positions.iter().enumerate() {
        assert_eq!(position.sequence, expected as u64);
        assert_eq!(position.backend_epoch, epoch);
    }

    let tail = log.append(&shard, &commands[..3], epoch).unwrap();
    assert_eq!(
        tail.iter()
            .map(|position| position.sequence)
            .collect::<Vec<_>>(),
        vec![1_025, 1_026, 1_027]
    );
    assert_eq!(log.high_water(&shard).unwrap().unwrap().sequence, 1_027);

    let first = log.read_from(&shard, None, 512).unwrap();
    assert_eq!(first.entries.len(), 512);
    assert_eq!(first.next.as_ref().unwrap().sequence, 511);
    let second = log.read_from(&shard, first.next, 512).unwrap();
    assert_eq!(second.entries.len(), 512);
    assert_eq!(second.entries[0].0.sequence, 512);
    assert_eq!(second.next.as_ref().unwrap().sequence, 1_023);
    let final_page = log.read_from(&shard, second.next, 512).unwrap();
    assert_eq!(final_page.entries.len(), 4);
    assert_eq!(final_page.entries[0].0.sequence, 1_024);
    assert_eq!(final_page.entries[3].0.sequence, 1_027);
    assert!(final_page.next.is_none());
}

#[test]
fn composed_postgres_log_preserves_request_id_idempotency() {
    let Some(url) = pg_url() else {
        eprintln!(
            "COMPOSED POSTGRES RECOVERY SKIPPED (request id) — set FIREWEED_PG_TEST_URL to a live DB"
        );
        return;
    };
    futures::executor::block_on(
        fireweed_conformance::scenarios::request_id_push_replays_once_and_conflicts_on_body_change(
            || {
                composed_postgres_backend_in_schema(&url, &fresh_schema("request_id"))
                    .expect("connect composed postgres backend")
            },
        ),
    );
}

#[test]
fn postgres_log_cross_chunk_append_is_one_atomic_transaction() {
    let Some(url) = pg_url() else {
        eprintln!(
            "COMPOSED POSTGRES RECOVERY SKIPPED (batched atomicity) — set FIREWEED_PG_TEST_URL to a live DB"
        );
        return;
    };
    let schema = fresh_schema("batched_atomicity");
    let mut log = PostgresLog::connect_in_schema(&url, &schema).expect("connect postgres log");
    let shard = qkey();
    log.ensure_shard(&shard).unwrap();
    let epoch = log.acquire_epoch(&shard).unwrap();

    let commands = (0..1_025)
        .map(|index| {
            let id = (index + 2_000_000).to_string();
            let key = format!("atomic-key-{index}");
            envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![item(&id, &key, index as i64)],
                }),
                vec![],
            )
        })
        .collect::<Vec<_>>();

    let mut admin =
        postgres::Client::connect(&url, postgres::NoTls).expect("connect trigger client");
    admin
        .batch_execute(&format!(
            "SET search_path TO {schema};
             CREATE FUNCTION reject_second_chunk() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN RAISE EXCEPTION 'forced second chunk failure'; END $$;
             CREATE TRIGGER reject_second_chunk BEFORE INSERT ON log_entries
             FOR EACH ROW WHEN (NEW.seq = 1024) EXECUTE FUNCTION reject_second_chunk();"
        ))
        .expect("install failure trigger");

    assert!(
        log.append(&shard, &commands, epoch).is_err(),
        "a failure in the second insert chunk must reject the whole append"
    );
    assert!(log.read_from(&shard, None, 1).unwrap().entries.is_empty());
    assert!(log.high_water(&shard).unwrap().is_none());

    admin
        .batch_execute(
            "DROP TRIGGER reject_second_chunk ON log_entries; DROP FUNCTION reject_second_chunk();",
        )
        .expect("remove failure trigger");
    let retry = log.append(&shard, &commands[..1], epoch).unwrap();
    assert_eq!(
        retry[0].sequence, 0,
        "the failed transaction must roll back its sequence range allocation"
    );
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
