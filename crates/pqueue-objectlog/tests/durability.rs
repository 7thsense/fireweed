//! Object-log durability + class semantics: the object log is the source of truth (reopen rebuilds the
//! projection), and the backend declares the eventual-apply class (so upsert is refused).

use pqueue_conformance::{claim_req, commit, envelope, item, qdef, qkey, shard};
use pqueue_core::{ClientItemKey, EntitySchemaDocument, ItemId, RequestId};
use pqueue_engine::{
    Backend, ClaimPort, ControlPlaneStore, DurabilityClass, EngineError, LogRead, LogWriter,
    ProjectionRead, ProjectionWriter, PushCommand, PushPort, QueueCommand, ReplacePendingCommand,
    SetGatesCommand,
};
use pqueue_objectlog::{LocalObjectLog, ObjectLogBackend, ObjectLogSegmentConfig};
use serde_json::json;

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

fn push_env(id: &str) -> pqueue_engine::CommandEnvelope {
    envelope(
        QueueCommand::Push(PushCommand {
            items: vec![item(id, &format!("k{id}"), 1)],
        }),
        vec![ItemId::new(id).unwrap()],
    )
}

fn typed_qdef() -> pqueue_core::QueueDefinition {
    let mut def = qdef();
    def.entity_schema = Some(
        serde_json::from_value::<EntitySchemaDocument>(json!({
            "entity_schema": {
                "type": "object",
                "required": ["name"],
                "properties": {
                    "name": {"type": "string"}
                }
            }
        }))
        .unwrap(),
    );
    def
}

fn typed_valid_item(id: &str) -> pqueue_engine::PushSpec {
    pqueue_engine::PushSpec {
        entity: Some(json!({"name": id})),
        ..Default::default()
    }
}

fn typed_invalid_item(id: &str) -> pqueue_engine::PushSpec {
    pqueue_engine::PushSpec {
        entity: Some(json!({"count": id.len()})),
        ..Default::default()
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
async fn local_object_log_appends_reads_and_reopens_without_projection() {
    let root = tmp_root("local-store");
    let _ = std::fs::remove_dir_all(&root);
    let store = LocalObjectLog::open(&root).expect("open");
    store.create_queue(qdef()).unwrap();
    let envs = [
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ka", 5)],
            }),
            vec![ItemId::new("1").unwrap()],
        ),
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("2", "kb", 7)],
            }),
            vec![ItemId::new("2").unwrap()],
        ),
    ];

    let positions = store.append(&shard(), &envs, 0).unwrap();
    assert_eq!(
        positions
            .iter()
            .map(|position| position.sequence)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );

    let reopened = LocalObjectLog::open(&root).expect("reopen");
    let page = reopened.read_from(&shard(), None, 10).await.unwrap();
    assert_eq!(page.entries.len(), 2);
    assert_eq!(page.entries[0].0.sequence, 0);
    assert_eq!(page.entries[1].0.sequence, 1);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn local_object_log_rejects_stale_expected_epoch_before_append() {
    let root = tmp_root("local-epoch");
    let _ = std::fs::remove_dir_all(&root);
    let store = LocalObjectLog::open(&root).expect("open");
    store.create_queue(qdef()).unwrap();
    assert_eq!(store.acquire_epoch(&shard()).unwrap(), 1);
    let env = envelope(
        QueueCommand::Push(PushCommand {
            items: vec![item("1", "ka", 5)],
        }),
        vec![ItemId::new("1").unwrap()],
    );

    let err = store.append(&shard(), &[env], 0).unwrap_err();
    assert_eq!(err, EngineError::EpochFenced);
    let page = store.read_from(&shard(), None, 10).await.unwrap();
    assert!(page.entries.is_empty(), "stale append wrote no objects");
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn local_object_log_rejects_replace_pending_before_append() {
    let root = tmp_root("local-rp");
    let _ = std::fs::remove_dir_all(&root);
    let store = LocalObjectLog::open(&root).expect("open");
    store.create_queue(qdef()).unwrap();
    let valid = envelope(
        QueueCommand::Push(PushCommand {
            items: vec![item("1", "ka", 5)],
        }),
        vec![ItemId::new("1").unwrap()],
    );
    let unsupported = envelope(
        QueueCommand::ReplacePending(ReplacePendingCommand {
            client_item_key: ClientItemKey::new("ka").unwrap(),
            superseded_item_id: ItemId::new("1").unwrap(),
            replacement: item("2", "ka", 5),
        }),
        vec![],
    );

    let err = store
        .append(&shard(), &[valid, unsupported], 0)
        .unwrap_err();
    assert_eq!(err, EngineError::Unavailable);
    let page = store.read_from(&shard(), None, 10).await.unwrap();
    assert!(
        page.entries.is_empty(),
        "unsupported command wrote no objects"
    );
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
                items: vec![item("1", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    let env = envelope(
        QueueCommand::ReplacePending(ReplacePendingCommand {
            client_item_key: ClientItemKey::new("ka").unwrap(),
            superseded_item_id: ItemId::new("1").unwrap(),
            replacement: item("6", "ka", 5),
        }),
        vec![],
    );
    let epoch = b.current_epoch(&shard()).await.unwrap();
    let res = b
        .write(
            move |lw: &mut dyn LogWriter, pw: &mut dyn ProjectionWriter| {
                let pos = lw.append(&shard(), std::slice::from_ref(&env), epoch)?;
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
async fn raw_setgates_is_rejected_before_any_object_is_written() {
    // Unsupported gate commands must be rejected before the object log appends anything.
    let root = tmp_root("setgates");
    let _ = std::fs::remove_dir_all(&root);
    let b = ObjectLogBackend::open(&root).expect("open");
    b.create_queue(qdef()).await.unwrap();
    let valid = envelope(
        QueueCommand::Push(PushCommand {
            items: vec![item("1", "ka", 5)],
        }),
        vec![],
    );
    let unsupported = envelope(
        QueueCommand::SetGates(SetGatesCommand {
            gate_keys: vec!["hold".to_string()],
            blocked: true,
        }),
        vec![],
    );
    let epoch = b.current_epoch(&shard()).await.unwrap();
    let res = b
        .write(
            move |lw: &mut dyn LogWriter, pw: &mut dyn ProjectionWriter| {
                let pos = lw.append(&shard(), &[valid.clone(), unsupported.clone()], epoch)?;
                pw.apply(&pos, &[valid, unsupported])?;
                Ok(())
            },
        )
        .await;
    assert_eq!(res, Err(EngineError::Unavailable));
    let page = b.read_from(&shard(), None, 10).await.unwrap();
    assert!(page.entries.is_empty(), "rejected batch wrote no objects");
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
                    items: vec![item("1", "ka", 5)],
                }),
                vec![],
            ),
        )
        .await;
        commit(
            &b,
            envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![item("2", "kb", 9)],
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
async fn objectlog_segment_configuration_is_respected() {
    let root = tmp_root("segment-config");
    let _ = std::fs::remove_dir_all(&root);
    let shard = shard();
    let store = LocalObjectLog::open_with_config(
        &root,
        ObjectLogSegmentConfig {
            segment_max_commands: 10,
            segment_max_bytes: 1,
            segment_max_latency_ms: 5,
        },
    )
    .expect("open");
    store.create_queue(qdef()).unwrap();
    store
        .append(&shard, &[push_env("1"), push_env("2"), push_env("3")], 0)
        .expect("append");

    let backend = ObjectLogBackend::open(&root).expect("reopen");
    let stats = backend.segment_stats(&shard).expect("segment stats");
    assert_eq!(
        stats.segment_objects, 3,
        "segment_max_bytes=1 should cap the batch size at one command per segment"
    );
    assert_eq!(stats.command_objects, 3);

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
                        item("1", "ka", 30),
                        item("2", "kb", 10),
                        item("3", "kc", 20),
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

async fn schema_validation_backend<B>(backend: &B)
where
    B: ControlPlaneStore + PushPort + ProjectionRead,
{
    let q = shard();
    backend.create_queue(typed_qdef()).await.unwrap();

    let err = backend
        .push(
            &q,
            vec![typed_invalid_item("bad")],
            pqueue_conformance::ts(0),
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::EntitySchemaViolation(_)));
    assert_eq!(backend.metrics(&q).await.unwrap().pending, 0);

    let rid = RequestId::new("req-1").unwrap();
    let err = backend
        .push_with_request_id(
            &q,
            rid.clone(),
            vec![typed_invalid_item("bad")],
            pqueue_conformance::ts(1),
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::EntitySchemaViolation(_)));
    assert_eq!(backend.metrics(&q).await.unwrap().pending, 0);

    let first = backend
        .push_with_request_id(
            &q,
            rid.clone(),
            vec![typed_valid_item("ok")],
            pqueue_conformance::ts(2),
            None,
        )
        .await
        .unwrap();
    let replay = backend
        .push_with_request_id(
            &q,
            rid,
            vec![typed_valid_item("ok")],
            pqueue_conformance::ts(3),
            None,
        )
        .await
        .unwrap();
    assert_eq!(first, replay, "valid replay must reuse the committed ids");
    assert_eq!(backend.metrics(&q).await.unwrap().pending, 1);
}

#[tokio::test]
async fn object_log_backend_schema_validation_rejects_before_append_and_idempotency() {
    let root = tmp_root("schema-object-log");
    let _ = std::fs::remove_dir_all(&root);
    let backend = ObjectLogBackend::open(&root).expect("open");
    schema_validation_backend(&backend).await;
    let _ = std::fs::remove_dir_all(&root);
}
