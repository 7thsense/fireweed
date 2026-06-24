//! Object-log durability + class semantics: the object log is the source of truth (reopen rebuilds the
//! projection), and the backend declares the eventual-apply class (so upsert is refused).

use pqueue_conformance::{claim_req, commit, envelope, item, qdef, qkey, shard};
use pqueue_core::{ClientItemKey, ItemId};
use pqueue_engine::{
    Backend, ClaimPort, ControlPlaneStore, DurabilityClass, EngineError, LogWriter, ProjectionRead,
    ProjectionWriter, PushCommand, QueueCommand, ReplacePendingCommand,
};
use pqueue_objectlog::ObjectLogBackend;

fn tmp_root(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("pqueue-objlog-dur-{tag}-{}", std::process::id()))
}

/// Overwrite the highest-numbered log object under `root`'s single shard with invalid JSON (simulates an
/// append interrupted mid-write).
fn corrupt_last_log_object(root: &std::path::Path) {
    for shard_entry in std::fs::read_dir(root).unwrap() {
        let log_dir = shard_entry.unwrap().path().join("log");
        if !log_dir.exists() {
            continue;
        }
        let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(&log_dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        files.sort();
        std::fs::write(files.last().unwrap(), b"{ truncated not valid json").unwrap();
    }
}

#[tokio::test]
async fn declares_eventual_apply_class() {
    let root = tmp_root("class");
    let _ = std::fs::remove_dir_all(&root);
    let b = ObjectLogBackend::open(&root).expect("open");
    assert_eq!(b.durability_class(), DurabilityClass::EventualApply);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn replace_pending_command_is_refused_at_the_write_path() {
    // I-1: the upsert ban holds at the durable write path, not only at `replace_if_pending`. A raw
    // ReplacePending command driven through `Backend::write` must be refused with Unavailable BEFORE any
    // object is written.
    let root = tmp_root("rp");
    let _ = std::fs::remove_dir_all(&root);
    let b = ObjectLogBackend::open(&root).expect("open");
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("a", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    let env = envelope(
        QueueCommand::ReplacePending(ReplacePendingCommand {
            client_item_key: ClientItemKey::new("ka").unwrap(),
            superseded_item_id: ItemId::new("a").unwrap(),
            replacement: item("new", "ka", 5),
        }),
        vec![],
    );
    let res = b
        .write(
            move |lw: &mut dyn LogWriter, pw: &mut dyn ProjectionWriter| {
                let pos = lw.append(&shard(), std::slice::from_ref(&env))?;
                pw.apply(&pos, std::slice::from_ref(&env))?;
                Ok(())
            },
        )
        .await;
    assert_eq!(
        res,
        Err(EngineError::Unavailable),
        "ReplacePending is refused on the eventual-apply class at the write path"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn torn_trailing_object_is_skipped_on_reopen() {
    // I-2: a partial/torn highest-seq object (an append interrupted by a crash) must be treated as
    // uncommitted — `open()` recovers the prior state rather than bricking.
    let root = tmp_root("torn");
    let _ = std::fs::remove_dir_all(&root);
    {
        let b = ObjectLogBackend::open(&root).expect("open");
        b.create_queue(qdef()).await.unwrap();
        // Two separate commits → two log objects (seq 0, seq 1).
        commit(
            &b,
            envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![item("a", "ka", 5)],
                }),
                vec![],
            ),
        )
        .await;
        commit(
            &b,
            envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![item("b", "kb", 9)],
                }),
                vec![],
            ),
        )
        .await;
        assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 2);
    }
    corrupt_last_log_object(&root); // torn seq-1 object

    let b = ObjectLogBackend::open(&root).expect("open must tolerate a torn trailing object");
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        1,
        "only the intact (seq 0) object replayed; the torn trailing one was skipped"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn projection_rebuilds_from_object_log_on_reopen() {
    let root = tmp_root("reopen");
    let _ = std::fs::remove_dir_all(&root);

    // Session 1: create the queue, push three items, claim the highest-priority one.
    {
        let b = ObjectLogBackend::open(&root).expect("open");
        b.create_queue(qdef()).await.unwrap();
        commit(
            &b,
            envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![
                        item("a", "ka", 30),
                        item("b", "kb", 10),
                        item("c", "kc", 20),
                    ],
                }),
                vec![],
            ),
        )
        .await;
        b.claim(claim_req(1, 500, 100)).await.unwrap(); // claims "b" (priority 10)
        let m = b.metrics(&qkey()).await.unwrap();
        assert_eq!((m.pending, m.leased), (2, 1));
    }

    // Session 2: REOPEN the same object store — projection must be replayed from the objects.
    {
        let b = ObjectLogBackend::open(&root).expect("reopen");
        let m = b.metrics(&qkey()).await.unwrap();
        assert_eq!(
            (m.pending, m.leased),
            (2, 1),
            "reopened projection reflects 3 pushes + 1 claim replayed from the object log"
        );
        // A claim after reopen must succeed (cmd_seq restored → no object-name collision) and lease the
        // next item; a further reopen replays that too.
        b.claim(claim_req(1, 500, 100)).await.unwrap();
        let m = b.metrics(&qkey()).await.unwrap();
        assert_eq!((m.pending, m.leased), (1, 2));
    }
    {
        let b = ObjectLogBackend::open(&root).expect("reopen 2");
        let m = b.metrics(&qkey()).await.unwrap();
        assert_eq!(
            (m.pending, m.leased),
            (1, 2),
            "post-reopen claim survived replay"
        );
    }

    let _ = std::fs::remove_dir_all(&root);
}
