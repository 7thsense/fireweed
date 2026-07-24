use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use fireweed_conformance::{envelope, item};
use fireweed_core::{
    EligibilityPolicy, ItemId, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use fireweed_engine::LogStore;
use fireweed_engine::{
    ControlPlaneStore, EngineError, MaintenanceStopReason, ProjectionRead, PushCommand,
    QueueCommand,
};
use fireweed_objectlog::{
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

fn shard() -> fireweed_engine::QueueKey {
    fireweed_engine::QueueKey::new(
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
        ordering_mode: fireweed_core::OrderingMode::Strict,
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

fn push_env(id: &str) -> fireweed_engine::CommandEnvelope {
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

fn legacy_manifest_files(root: &std::path::Path) -> Vec<String> {
    collect_files(root)
        .into_iter()
        .filter(|p| {
            p.contains("/manifest/") && !p.contains("/manifest_head/") && p.ends_with(".json")
        })
        .collect()
}

fn delete_legacy_manifest_files(root: &std::path::Path) {
    for rel in legacy_manifest_files(root) {
        std::fs::remove_file(root.join(rel)).expect("delete legacy manifest");
    }
}

struct CrashAt(FaultCutPoint);

impl FaultHook for CrashAt {
    fn fault_point(&self, cut: FaultCutPoint) -> fireweed_engine::EngineResult<()> {
        if cut == self.0 {
            Err(EngineError::Storage(format!("crash at {cut:?}")))
        } else {
            Ok(())
        }
    }
}

struct PauseAt {
    cut: FaultCutPoint,
    entered: Arc<Barrier>,
    resume: Arc<Barrier>,
}

impl FaultHook for PauseAt {
    fn fault_point(&self, cut: FaultCutPoint) -> fireweed_engine::EngineResult<()> {
        if cut == self.cut {
            self.entered.wait();
            self.resume.wait();
        }
        Ok(())
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

/// `TestReplayAcrossRestartWithHeadAndDeletion`: restart after object-log crashes around the manifest head
/// and segment deletion must replay committed commands exactly once and keep orphan/stale attempts rejected.
#[test]
fn replay_across_restart_with_head_and_deletion() {
    // Crash after the segment object is written but before the manifest head CAS. The orphan must stay
    // unreachable across restart, and a clean retry must not be confused by it.
    {
        let root = tmp_root("restart-orphan");
        let shard = shard();
        let mut log = ObjectLog::open(root.clone()).expect("open");
        log.ensure_shard(&shard).unwrap();
        log.set_fault_hook(Some(Arc::new(CrashAt(
            FaultCutPoint::AfterSegmentWriteBeforeManifest,
        ))));
        assert!(
            log.append(&shard, &[push_env("20")], 0).is_err(),
            "the orphaned segment must not ack"
        );
        assert!(
            log.read_from(&shard, None, 10).unwrap().entries.is_empty(),
            "the orphan segment must stay invisible before restart"
        );
        drop(log);

        let mut reopened = ObjectLog::open(root.clone()).expect("reopen");
        reopened.ensure_shard(&shard).unwrap();
        assert!(
            reopened
                .read_from(&shard, None, 10)
                .unwrap()
                .entries
                .is_empty(),
            "restart must not surface the orphan segment"
        );
        reopened
            .append(&shard, &[push_env("21")], 0)
            .expect("retry after orphan");
        let entries = reopened.read_from(&shard, None, 10).unwrap().entries;
        assert_eq!(
            entries.iter().map(|(p, _)| p.sequence).collect::<Vec<_>>(),
            vec![0],
            "the retried command is committed exactly once after restart"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    // Crash after the manifest head CAS but before the caller sees the ack. Restart must replay the
    // committed command exactly once and keep the next append contiguous.
    {
        let root = tmp_root("restart-head-cas");
        let shard = shard();
        let mut log = ObjectLog::open(root.clone()).expect("open");
        log.ensure_shard(&shard).unwrap();
        log.set_fault_hook(Some(Arc::new(CrashAt(
            FaultCutPoint::AfterManifestBeforeAck,
        ))));
        assert!(
            log.append(&shard, &[push_env("30")], 0).is_err(),
            "the post-head/pre-ack fault must withhold the ack"
        );
        drop(log);

        let mut reopened = ObjectLog::open(root.clone()).expect("reopen");
        reopened.ensure_shard(&shard).unwrap();
        let replayed = reopened.read_from(&shard, None, 10).unwrap().entries;
        assert_eq!(
            replayed.len(),
            1,
            "the committed command must replay exactly once after restart"
        );
        assert_eq!(replayed[0].0.sequence, 0);
        let next = reopened
            .append(&shard, &[push_env("31")], 0)
            .expect("append after replay");
        assert_eq!(next[0].sequence, 1);
        let _ = std::fs::remove_dir_all(&root);
    }

    // Crash during owner reassignment. The fence survives restart and stale-epoch writes remain rejected.
    {
        let root = tmp_root("restart-owner-reassignment");
        let shard = shard();
        let mut log = ObjectLog::open(root.clone()).expect("open");
        log.ensure_shard(&shard).unwrap();
        log.set_fault_hook(Some(Arc::new(CrashAt(
            FaultCutPoint::DuringOwnerReassignment,
        ))));
        assert!(
            log.acquire_epoch(&shard).is_err(),
            "the owner-reassignment fault must abort acquire_epoch"
        );
        drop(log);

        let mut reopened = ObjectLog::open(root.clone()).expect("reopen");
        reopened.ensure_shard(&shard).unwrap();
        assert_eq!(reopened.current_epoch(&shard).unwrap(), 1);
        let stale = reopened.append(&shard, &[push_env("40")], 0);
        assert!(
            matches!(
                stale,
                Err(EngineError::EpochFenced) | Err(EngineError::Conflict)
            ),
            "a stale writer must stay fenced after restart; got {stale:?}"
        );
        assert!(
            reopened.append(&shard, &[push_env("41")], 1).is_ok(),
            "the current owner must still be able to append after restart"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    // Crash during snapshot write. Restart must preserve the log and not publish a snapshot ref.
    {
        let root = tmp_root("restart-snapshot-write");
        let shard = shard();
        let mut log = ObjectLog::open(root.clone()).expect("open");
        log.ensure_shard(&shard).unwrap();
        let positions = log
            .append(&shard, &[push_env("50")], 0)
            .expect("append before snapshot");
        log.set_fault_hook(Some(Arc::new(CrashAt(FaultCutPoint::DuringSnapshotWrite))));
        assert!(
            log.write_snapshot(
                &shard,
                positions[0].clone(),
                fireweed_engine::ProjectionSnapshot {
                    payload: vec![1, 2, 3],
                },
            )
            .is_err(),
            "the snapshot write fault must abort the snapshot"
        );
        drop(log);

        let mut reopened = ObjectLog::open(root.clone()).expect("reopen");
        reopened.ensure_shard(&shard).unwrap();
        assert!(
            reopened.latest_snapshot(&shard).unwrap().is_none(),
            "a failed snapshot write must not leave a committed snapshot ref"
        );
        assert_eq!(
            reopened.read_from(&shard, None, 10).unwrap().entries.len(),
            1,
            "the command log must remain intact after a lost snapshot write"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    // Crash during segment reclamation. Restart must still replay the committed tail exactly once, and a
    // second trim must finish the interrupted deletion.
    {
        let root = tmp_root("restart-deletion");
        let shard = shard();
        let mut log = ObjectLog::open(root.clone()).expect("open");
        log.ensure_shard(&shard).unwrap();
        for i in 0..4 {
            log.append(&shard, &[push_env(&format!("60{i}"))], 0)
                .expect("seed old segment");
        }
        let _tail = log
            .append(&shard, &[push_env("64")], 0)
            .expect("append live tail");
        let owner_epoch = log.acquire_epoch(&shard).expect("acquire trim owner");
        log.advance_retention_floor(
            &shard,
            fireweed_engine::CommandPosition::new(shard.clone(), owner_epoch, 3),
            owner_epoch,
        )
        .expect("advance floor");
        log.set_fault_hook(Some(Arc::new(CrashAt(FaultCutPoint::DuringSegmentExpiry))));
        let interrupted = log
            .expire_segments_through_bounded(&shard, 3, 1_000)
            .expect("bounded trim returns partial evidence");
        assert_eq!(interrupted.permanent_failures, 1);
        assert_eq!(
            interrupted.stopped_by,
            Some(MaintenanceStopReason::PermanentFailure),
            "the deletion fault must stop the trim with typed partial evidence"
        );
        drop(log);

        let mut reopened = ObjectLog::open(root.clone()).expect("reopen");
        reopened.ensure_shard(&shard).unwrap();
        let floor = reopened.retention_floor(&shard).unwrap().unwrap();
        assert_eq!(floor.sequence, 3);
        let tail_entries = reopened
            .read_from(&shard, Some(floor.clone()), 10)
            .unwrap()
            .entries;
        assert_eq!(
            tail_entries
                .iter()
                .map(|(p, _)| p.sequence)
                .collect::<Vec<_>>(),
            vec![4],
            "restart must replay the committed tail exactly once after interrupted deletion"
        );
        let deletes_before = reopened.counters().delete_count;
        reopened
            .acquire_epoch(&shard)
            .expect("reacquire trim owner after restart");
        reopened
            .expire_segments_through_bounded(&shard, 3, 1_000)
            .expect("retry trim after restart");
        assert!(
            reopened.counters().delete_count > deletes_before,
            "the retry must finish the interrupted deletion"
        );
        assert_eq!(
            reopened
                .read_from(&shard, Some(floor), 10)
                .unwrap()
                .entries
                .len(),
            1,
            "the tail remains readable after the compaction deletion retry"
        );
        let _ = std::fs::remove_dir_all(&root);
    }
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

    delete_legacy_manifest_files(&root);
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

#[test]
fn stale_writer_cannot_false_ack_after_deleted_index() {
    let root = tmp_root("deleted-index");
    let shard = shard();

    let mut owner_a = ObjectLog::open(root.clone()).expect("open owner a");
    owner_a.ensure_shard(&shard).unwrap();
    let first = owner_a.append(&shard, &[push_env("30")], 0).unwrap();
    assert_eq!(first[0].sequence, 0);

    let entered = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let mut frozen = ObjectLog::open(root.clone()).expect("open frozen owner");
    frozen.ensure_shard(&shard).unwrap();
    frozen.set_fault_hook(Some(Arc::new(PauseAt {
        cut: FaultCutPoint::AfterSegmentWriteBeforeManifest,
        entered: Arc::clone(&entered),
        resume: Arc::clone(&resume),
    })));
    let shard_for_thread = shard.clone();
    let frozen_handle = thread::spawn(move || {
        let mut frozen = frozen;
        frozen.append(&shard_for_thread, &[push_env("31")], 0)
    });

    entered.wait();

    let mut owner_b = ObjectLog::open(root.clone()).expect("open owner b");
    owner_b.ensure_shard(&shard).unwrap();
    assert_eq!(
        owner_b.acquire_epoch(&shard).unwrap(),
        1,
        "owner B advances the permanent head"
    );
    delete_legacy_manifest_files(&root);

    resume.wait();
    let frozen_res = frozen_handle.join().expect("frozen owner thread");
    assert!(
        matches!(
            frozen_res,
            Err(EngineError::EpochFenced | EngineError::Conflict)
        ),
        "a stale owner resuming after the head advanced must be fenced, got {frozen_res:?}"
    );

    let page = owner_b.read_from(&shard, None, 10).unwrap();
    assert_eq!(
        page.entries
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![0],
        "the stale attempt did not create a phantom acknowledgement"
    );
    assert_eq!(owner_b.current_epoch(&shard).unwrap(), 1);
    assert!(
        legacy_manifest_files(&root).is_empty(),
        "the old manifest index objects were deleted"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn stale_writer_attempt_garbage_unpublished() {
    let root = tmp_root("attempt-garbage");
    let shard = shard();

    let mut stale = ObjectLog::open(root.clone()).expect("open stale owner");
    stale.ensure_shard(&shard).unwrap();
    stale.set_fault_hook(Some(Arc::new(CrashAt(
        FaultCutPoint::AfterSegmentWriteBeforeManifest,
    ))));
    assert!(
        stale.append(&shard, &[push_env("40")], 0).is_err(),
        "the initial stale attempt should crash after the segment write"
    );
    let orphan_key = attempt_segment_files(&root)
        .into_iter()
        .find(|k| k.contains("/s00000000000000000000-"))
        .expect("orphan segment object");

    let mut current = ObjectLog::open(root.clone()).expect("open current owner");
    current.ensure_shard(&shard).unwrap();
    let live = current.append(&shard, &[push_env("41")], 0).unwrap();
    assert_eq!(live[0].sequence, 0);
    let live_key = attempt_segment_files(&root)
        .into_iter()
        .find(|k| k != &orphan_key)
        .expect("live segment object");
    let live_len = std::fs::metadata(root.join(&live_key))
        .expect("live segment exists")
        .len();

    let branch_def = branch_qdef();
    current
        .branch(&shard, &branch_def, &live[0], 60_000, 0)
        .expect("branch pins the live source segment");
    assert!(
        collect_files(&root)
            .iter()
            .any(|p| p.contains("/branches/") || p.ends_with("/branch.json")),
        "branch metadata must be published before the stale peer races"
    );

    let later = current.append(&shard, &[push_env("42")], 0).unwrap();
    assert_eq!(later[0].sequence, 1);

    delete_legacy_manifest_files(&root);
    stale.set_fault_hook(None);
    let stale_race = stale.append(&shard, &[push_env("43")], 0);
    assert!(
        matches!(
            stale_race,
            Err(EngineError::Conflict | EngineError::EpochFenced)
        ),
        "a stale owner must not ack after the head advances: {stale_race:?}"
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
        "the stale attempt stays unreachable from the manifest head"
    );
    assert_eq!(
        std::fs::metadata(root.join(&live_key))
            .map(|m| m.len())
            .ok(),
        Some(live_len),
        "the stale attempt must not overwrite the live segment"
    );

    let _ = std::fs::remove_dir_all(&root);
}
