//! Sqlite-specific durability: the projection is a derived view; the LOG is the source of truth. These
//! tests reopen a real file-backed database and assert the committed state is reconstructed by replaying
//! the durable log — the property the shared conformance suite (fresh `:memory:`) cannot exercise.

use pqueue_conformance::{claim_req, commit, envelope, item, qdef, qkey};
use pqueue_engine::{ClaimPort, ControlPlaneStore, ProjectionRead, PushCommand, QueueCommand};
use pqueue_sqlite::{composed_sqlite_backend, composed_sqlite_backend_in_memory};

fn temp_db(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("pqueue-sqlite-{tag}-{}.db", std::process::id()))
}

#[tokio::test]
async fn projection_rebuilds_from_durable_log_on_reopen() {
    let path = temp_db("reopen");
    let _ = std::fs::remove_file(&path);
    let p = path.to_str().unwrap();

    // Session 1: create the queue, push three items, claim the highest-priority one.
    {
        let b = composed_sqlite_backend(p).expect("open");
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
        // Claims "b" (priority 10, lowest = highest priority under ascending).
        let claimed = b.claim(claim_req(1, 500, 100)).await.unwrap();
        assert_eq!(claimed.items.len(), 1);
        let m = b.metrics(&qkey()).await.unwrap();
        assert_eq!((m.pending, m.leased), (2, 1));
    } // backend dropped — only the sqlite file remains.

    // Session 2: REOPEN. The in-memory projection is gone; it must be rebuilt from the log.
    {
        let b = composed_sqlite_backend(p).expect("reopen");
        let m = b.metrics(&qkey()).await.unwrap();
        assert_eq!(
            (m.pending, m.leased),
            (2, 1),
            "reopened projection must reflect the 3 pushes + 1 claim replayed from the durable log"
        );
        // The still-eligible items are the two unclaimed ones, in priority order (c=20 before a=30).
        let elig = b
            .select_eligible(
                &pqueue_conformance::shard(),
                pqueue_conformance::ts(200),
                10,
            )
            .await
            .unwrap();
        let ids: Vec<u64> = elig.iter().map(|i| i.as_u64()).collect();
        assert_eq!(
            ids,
            vec![3, 1],
            "eligibility order survives the rebuild (c=prio20 before a=prio30)"
        );
    }

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn orchestration_writes_after_reopen_do_not_collide() {
    // Regression for B1: `cmd_seq` is restored past the highest replayed `sql-N`, so a claim AFTER a
    // reopen mints a fresh command id and commits durably (a colliding id would fail the PK / corrupt
    // the log). Also proves the reopened projection accepts further commands and replays again.
    let path = temp_db("recollide");
    let _ = std::fs::remove_file(&path);
    let p = path.to_str().unwrap();
    {
        let b = composed_sqlite_backend(p).expect("open");
        b.create_queue(qdef()).await.unwrap();
        commit(
            &b,
            envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![item("1", "ka", 5), item("2", "kb", 9)],
                }),
                vec![],
            ),
        )
        .await;
        // A claim goes through make_envelope -> "sql-0" durably.
        b.claim(claim_req(1, 500, 100)).await.unwrap();
    }
    {
        let b = composed_sqlite_backend(p).expect("reopen");
        // Claim again post-reopen: must succeed (fresh id, no collision) and lease the remaining item.
        let claimed = b.claim(claim_req(1, 500, 100)).await.unwrap();
        assert_eq!(claimed.items.len(), 1, "second item claimable after reopen");
        let m = b.metrics(&qkey()).await.unwrap();
        assert_eq!(
            (m.pending, m.leased),
            (0, 2),
            "both items leased across the two sessions"
        );
        // A third reopen replays the post-reopen claim too (log stayed consistent).
        drop(b);
        let b = composed_sqlite_backend(p).expect("reopen 2");
        let m = b.metrics(&qkey()).await.unwrap();
        assert_eq!((m.pending, m.leased), (0, 2));
    }
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn snapshots_round_trip_and_latest_is_most_recent() {
    use pqueue_engine::{ProjectionSnapshot, SnapshotStore};
    let b = composed_sqlite_backend_in_memory().expect("open");
    b.create_queue(qdef()).await.unwrap();
    let sk = pqueue_conformance::shard();
    let pos = pqueue_engine::CommandPosition::new(sk.clone(), 0, 0);
    let r1 = b
        .write_snapshot(
            &sk,
            pos.clone(),
            ProjectionSnapshot {
                payload: vec![1, 2, 3],
            },
        )
        .await
        .unwrap();
    let r2 = b
        .write_snapshot(
            &sk,
            pos,
            ProjectionSnapshot {
                payload: vec![4, 5, 6],
            },
        )
        .await
        .unwrap();
    assert_ne!(r1.ref_id, r2.ref_id, "each snapshot gets a distinct ref");
    // latest is the second write.
    let latest = b
        .latest_snapshot(&sk)
        .await
        .unwrap()
        .expect("a snapshot exists");
    assert_eq!(latest.ref_id, r2.ref_id);
    // read by ref returns the right payload.
    assert_eq!(b.read_snapshot(&r1).await.unwrap().payload, vec![1, 2, 3]);
    assert_eq!(b.read_snapshot(&r2).await.unwrap().payload, vec![4, 5, 6]);
}

#[tokio::test]
async fn high_water_persists_across_reopen() {
    use pqueue_engine::SnapshotStore;
    let path = temp_db("highwater");
    let _ = std::fs::remove_file(&path);
    let p = path.to_str().unwrap();

    let before = {
        let b = composed_sqlite_backend(p).expect("open");
        b.create_queue(qdef()).await.unwrap();
        commit(
            &b,
            envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![item("1", "ka", 5)],
                }),
                vec![],
            ),
        )
        .await;
        b.high_water(&pqueue_conformance::shard())
            .await
            .unwrap()
            .expect("high-water set after a commit")
    };

    {
        let b = composed_sqlite_backend(p).expect("reopen");
        let after = b
            .high_water(&pqueue_conformance::shard())
            .await
            .unwrap()
            .expect("high-water persisted");
        assert_eq!(
            before, after,
            "persisted high-water survives reopen (TD-007 §4)"
        );
    }

    let _ = std::fs::remove_file(&path);
}
