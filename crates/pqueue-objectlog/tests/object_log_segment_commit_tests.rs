use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use pqueue_conformance::{envelope, item};
use pqueue_core::{
    EligibilityPolicy, ItemId, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use pqueue_engine::LogStore;
use pqueue_engine::{ControlPlaneStore, EngineError, ProjectionRead, PushCommand, QueueCommand};
use pqueue_objectlog::{
    FaultCutPoint, FaultHook, LocalObjectLog, ObjectLog, ObjectLogBackend, ObjectLogSegmentConfig,
};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let p = std::env::temp_dir().join(format!(
        "pqueue-objlog-seg-{tag}-{}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn shard() -> pqueue_engine::QueueKey {
    pqueue_engine::QueueKey::new(
        TenantId::new("tenant").unwrap(),
        QueueId::new("queue").unwrap(),
    )
}

fn qdef() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("tenant").unwrap(),
        queue_id: QueueId::new("queue").unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: pqueue_core::OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 10 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

fn branch_qdef() -> QueueDefinition {
    QueueDefinition {
        queue_id: QueueId::new("branch").unwrap(),
        ..qdef()
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

fn log_dir(root: &std::path::Path) -> std::path::PathBuf {
    std::fs::read_dir(root)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path()
        .join("log")
}

fn collect_files(root: &std::path::Path) -> Vec<String> {
    fn walk(base: &std::path::Path, dir: &std::path::Path, out: &mut Vec<String>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(base, &path, out);
            } else {
                out.push(
                    path.strip_prefix(base)
                        .unwrap()
                        .to_string_lossy()
                        .replace('\\', "/"),
                );
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort();
    out
}

fn attempt_segment_files(root: &std::path::Path) -> Vec<String> {
    collect_files(root)
        .into_iter()
        .filter(|p| p.contains("/seg_attempt/") && p.ends_with(".seg"))
        .collect()
}

fn manifest_head_files(root: &std::path::Path) -> Vec<String> {
    collect_files(root)
        .into_iter()
        .filter(|p| p.contains("/manifest_head/") && p.ends_with(".json"))
        .collect()
}

struct CrashAt(FaultCutPoint);

impl FaultHook for CrashAt {
    fn fault_point(&self, cut: FaultCutPoint) -> pqueue_engine::EngineResult<()> {
        if cut == self.0 {
            Err(EngineError::Storage(format!("crash at {cut:?}")))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn segmented_commands_wait_for_manifest_commit() {
    let root = tmp_root("commit");
    let store = LocalObjectLog::open_with_config(
        &root,
        ObjectLogSegmentConfig {
            segment_max_commands: 2,
            segment_max_bytes: 0,
            segment_max_latency_ms: 10,
        },
    )
    .expect("open");
    store.create_queue(qdef()).unwrap();
    let shard = shard();

    let positions = store
        .append(&shard, &[push_env("1"), push_env("2"), push_env("3")], 0)
        .expect("append");
    assert_eq!(
        positions.iter().map(|p| p.sequence).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );

    let files = std::fs::read_dir(log_dir(&root)).unwrap().count();
    assert_eq!(
        files, 2,
        "two durable segment objects for three commands at max=2"
    );

    let reopened = ObjectLogBackend::open(&root).expect("reopen");
    assert_eq!(
        reopened.metrics(&shard).await.unwrap().pending,
        3,
        "reopen must rebuild the committed segment contents"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn segment_manifest_cas_fences_concurrent_writers() {
    let root = tmp_root("fence");
    let store = Arc::new(
        LocalObjectLog::open_with_config(&root, ObjectLogSegmentConfig::default()).expect("open"),
    );
    store.create_queue(qdef()).unwrap();
    let shard = shard();

    assert_eq!(store.acquire_epoch(&shard).unwrap(), 1);

    let barrier = Arc::new(Barrier::new(2));
    let stale_store = Arc::clone(&store);
    let stale_barrier = Arc::clone(&barrier);
    let stale_shard = shard.clone();
    let stale = thread::spawn(move || {
        stale_barrier.wait();
        stale_store
            .append(&stale_shard, &[push_env("1")], 0)
            .map(|_| ())
    });

    let current_store = Arc::clone(&store);
    let current_barrier = Arc::clone(&barrier);
    let current_shard = shard.clone();
    let current = thread::spawn(move || {
        current_barrier.wait();
        current_store
            .append(&current_shard, &[push_env("2")], 1)
            .map(|_| ())
    });

    let stale_res = stale.join().expect("stale writer thread");
    let current_res = current.join().expect("current writer thread");

    assert_eq!(stale_res.unwrap_err(), EngineError::EpochFenced);
    current_res.expect("current writer should commit");

    let files = std::fs::read_dir(log_dir(&root)).unwrap().count();
    assert_eq!(
        files, 1,
        "only the fenced-in writer should commit a segment"
    );

    let reopened = ObjectLogBackend::open(&root).expect("reopen");
    assert_eq!(reopened.current_epoch(&shard).await.unwrap(), 1);
    assert_eq!(
        reopened.metrics(&shard).await.unwrap().pending,
        1,
        "only one committed command should be recovered"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn seal_head_cas_ack_boundary_preserves_replay_semantics() {
    let root = tmp_root("head-cas-boundary");
    let mut log = ObjectLog::open(root.clone()).expect("open");
    let shard = shard();
    log.ensure_shard(&shard).unwrap();

    log.set_fault_hook(Some(Arc::new(CrashAt(FaultCutPoint::BeforeSegmentWrite))));
    assert!(log.append(&shard, &[push_env("10")], 0).is_err());
    assert!(attempt_segment_files(&root).is_empty());
    assert!(manifest_head_files(&root).is_empty());
    assert!(log.read_from(&shard, None, 10).unwrap().entries.is_empty());

    log.set_fault_hook(Some(Arc::new(CrashAt(
        FaultCutPoint::AfterSegmentWriteBeforeManifest,
    ))));
    assert!(log.append(&shard, &[push_env("11")], 0).is_err());
    assert_eq!(
        attempt_segment_files(&root).len(),
        1,
        "segment write before head CAS leaves only an unreachable attempt object"
    );
    assert!(manifest_head_files(&root).is_empty());
    assert!(log.read_from(&shard, None, 10).unwrap().entries.is_empty());

    log.set_fault_hook(None);
    let acked = log
        .append(&shard, &[push_env("12")], 0)
        .expect("retry after pre-head fault commits");
    assert_eq!(
        acked.iter().map(|p| p.sequence).collect::<Vec<_>>(),
        vec![0]
    );
    assert_eq!(manifest_head_files(&root).len(), 1);
    assert_eq!(log.read_from(&shard, None, 10).unwrap().entries.len(), 1);

    log.set_fault_hook(Some(Arc::new(CrashAt(
        FaultCutPoint::AfterManifestBeforeAck,
    ))));
    assert!(
        log.append(&shard, &[push_env("13")], 0).is_err(),
        "fault after head CAS must withhold the ack from the caller"
    );
    drop(log);

    let mut reopened = ObjectLog::open(root.clone()).expect("reopen");
    reopened.ensure_shard(&shard).unwrap();
    let replayed = reopened.read_from(&shard, None, 10).unwrap().entries;
    assert_eq!(
        replayed.len(),
        2,
        "the post-head/pre-ack segment is durable and replays exactly once"
    );
    let acked_after_reopen = reopened
        .append(&shard, &[push_env("14")], 0)
        .expect("append after lost ack");
    assert_eq!(acked_after_reopen[0].sequence, 2);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn unique_attempt_segment_keys_do_not_clobber_live_branch_or_later_segments() {
    let root = tmp_root("attempt-keys");
    let shard = shard();

    let mut stale = ObjectLog::open(root.clone()).expect("open stale owner");
    stale.ensure_shard(&shard).unwrap();

    stale.set_fault_hook(Some(Arc::new(CrashAt(
        FaultCutPoint::AfterSegmentWriteBeforeManifest,
    ))));
    assert!(stale.append(&shard, &[push_env("20")], 0).is_err());
    let orphan_key = attempt_segment_files(&root)
        .into_iter()
        .next()
        .expect("failed attempt segment");
    assert!(orphan_key.contains("/s00000000000000000000-"));

    stale.set_fault_hook(None);
    let mut current = ObjectLog::open(root.clone()).expect("open current owner");
    current.ensure_shard(&shard).unwrap();
    let live = current
        .append(&shard, &[push_env("21")], 0)
        .expect("live retry");
    assert_eq!(live[0].sequence, 0);
    let after_live = attempt_segment_files(&root);
    assert_eq!(after_live.len(), 2);
    assert!(
        after_live.iter().any(|k| k == &orphan_key),
        "successful retry must not delete or overwrite the failed attempt object"
    );
    let live_key = after_live
        .iter()
        .find(|k| *k != &orphan_key)
        .expect("live segment key")
        .clone();
    let live_len = std::fs::metadata(root.join(&live_key))
        .expect("live segment exists")
        .len();
    assert_ne!(
        orphan_key, live_key,
        "failed and live attempts for the same first sequence must use unique keys"
    );

    let branch_def = branch_qdef();
    current
        .branch(&shard, &branch_def, &live[0], 60_000, 0)
        .expect("branch pins source segment");
    assert!(
        collect_files(&root)
            .iter()
            .any(|p| p.contains("/branches/") || p.ends_with("/branch.json")),
        "branch creation must publish branch metadata before the stale peer races"
    );

    let later = current
        .append(&shard, &[push_env("22")], 0)
        .expect("later commit");
    assert_eq!(later[0].sequence, 1);

    let stale_race = stale.append(&shard, &[push_env("23")], 0);
    assert!(
        matches!(
            stale_race,
            Err(EngineError::Conflict | EngineError::EpochFenced)
        ),
        "dormant stale owner must not ack after losing the manifest-head CAS: {stale_race:?}"
    );

    let keys = attempt_segment_files(&root);
    let seq0_keys = keys
        .iter()
        .filter(|k| k.contains("/s00000000000000000000-"))
        .count();
    assert!(
        seq0_keys >= 3,
        "orphan, live, and stale attempts at first_seq=0 keep distinct attempt objects: {keys:?}"
    );
    assert_eq!(
        current.read_from(&shard, None, 10).unwrap().entries.len(),
        2,
        "stale attempt cannot clobber the live or later committed source log"
    );
    assert_eq!(
        std::fs::metadata(root.join(&live_key))
            .map(|m| m.len())
            .ok(),
        Some(live_len),
        "stale attempt cannot delete or overwrite the branch-pinned source segment"
    );

    let _ = std::fs::remove_dir_all(&root);
}
