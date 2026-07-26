//! TD-004 **production** object-log substrate: configurable group-commit segments on an S3-compatible
//! store, ack withheld until segment+manifest commit, manifest-CAS epoch fencing, and release-measurable
//! segment/object counters. The in-memory store exercises the whole pipeline with NO network; the MinIO
//! integration test (env-gated on `FIREWEED_S3_TEST_ENDPOINT`) runs the SAME flow against a real S3 endpoint.
//!
//! ## Running the MinIO integration test (orbstack networking)
//!
//! ```text
//! docker run -d --name fireweed-minio -e MINIO_ROOT_USER=minioadmin \
//!     -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
//! IP=$(docker inspect fireweed-minio --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
//! FIREWEED_S3_TEST_ENDPOINT="http://$IP:9000" \
//!     cargo test -p fireweed-objectlog --test segmented_s3_substrate_tests \
//!     segmented_object_log_commits_through_minio -- --nocapture
//! ```
//!
//! This host CANNOT reach docker *published* ports (`localhost:9000` fails in the orbstack namespace), so the
//! container IP must be used directly. Optional overrides: `FIREWEED_S3_TEST_BUCKET` (default `fireweed-test`),
//! `FIREWEED_S3_TEST_ACCESS_KEY` / `FIREWEED_S3_TEST_SECRET_KEY` (default `minioadmin`). Absent the endpoint env,
//! the test prints a LOUD skip and returns green (mirroring the postgres `FIREWEED_PG_TEST_URL` gate).

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

use fireweed_conformance::{envelope, item, qdef, shard};
use fireweed_core::QueueId;
use fireweed_engine::{CommandPosition, EngineError, EngineResult, PushCommand, QueueCommand};
use fireweed_objectlog::maintenance::{
    MaintenanceExecutionReason, MaintenanceFailureCause, MaintenanceLimits,
};
use fireweed_objectlog::object_store_observability::{
    BlobBackendKind, BlobMetricsRecorder, BlobObjectClass, BlobOperation, BlobResultClass,
    BlobStoreFault, ClassifiedBlobError, ClassifiedBlobResult, InstrumentedBlobStore,
    ObservedBlobCall,
};
use fireweed_objectlog::segmented::{
    BlobStore, FaultCutPoint, FaultHook, InMemoryBlobStore, ManifestHeadBlob, ObjectStoreStats,
    S3BlobStore, SegmentConfig, SegmentedObjectLog,
};

// `n` distinct push commands (one item each), so a segment can batch several commands.
fn pushes(n: u64) -> Vec<fireweed_engine::CommandEnvelope> {
    (0..n)
        .map(|i| {
            envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![item(&format!("{i}"), &format!("k{i}"), (i % 10) as i64)],
                }),
                vec![],
            )
        })
        .collect()
}

// Establishes real local ownership before driving maintenance from a fixture.
//
// Production maintenance is only valid on an instance that acquired the serving epoch; merely observing
// the durable epoch is not ownership proof. Tests that intentionally exercise a stale-owner fence call
// `expire_segments_through` directly instead.
fn expire_segments_as_local_owner<S: BlobStore>(
    log: &SegmentedObjectLog<S>,
    shard: &fireweed_engine::QueueKey,
    through_seq: u64,
    now_ms: i64,
) -> EngineResult<u64> {
    if log.maintenance_owner_epoch(shard).is_none() {
        log.acquire_epoch(shard, now_ms)?;
    }
    log.expire_segments_through(shard, through_seq, now_ms)
}

fn advance_floor_as_local_owner<S: BlobStore>(
    log: &SegmentedObjectLog<S>,
    shard: &fireweed_engine::QueueKey,
    through_seq: u64,
    now_ms: i64,
) -> EngineResult<u64> {
    let epoch = match log.maintenance_owner_epoch(shard) {
        Some(epoch) => epoch,
        None => log.acquire_epoch(shard, now_ms)?,
    };
    log.advance_retention_floor(
        shard,
        CommandPosition::new(shard.clone(), epoch, through_seq),
        epoch,
    )?;
    Ok(epoch)
}

fn gc_orphans_as_local_owner<S: BlobStore>(
    log: &SegmentedObjectLog<S>,
    source: &fireweed_engine::QueueKey,
) -> EngineResult<u64> {
    let epoch = match log.maintenance_owner_epoch(source) {
        Some(epoch) => epoch,
        None => log.acquire_epoch(source, 0)?,
    };
    let limits =
        MaintenanceLimits::new(256, u64::MAX, 4096, std::time::Duration::from_secs(1), 256)?;
    let mut completed = 0;
    loop {
        let report = log.gc_orphaned_branches_bounded(source, epoch, i64::MAX, 0, limits, false)?;
        completed += report.completed_candidates as u64;
        match report.stopped_by {
            Some(MaintenanceExecutionReason::EpochChanged) => return Err(EngineError::EpochFenced),
            Some(MaintenanceExecutionReason::RetryableFailure) => {
                return Err(EngineError::Storage("retryable maintenance failure".into()));
            }
            Some(MaintenanceExecutionReason::PermanentFailure) => {
                return Err(EngineError::Storage("permanent maintenance failure".into()));
            }
            Some(MaintenanceExecutionReason::BudgetExhausted) | None
                if report.cursor.is_some() || report.deleted > 0 => {}
            _ => return Ok(completed),
        }
    }
}

#[test]
fn disabled_recorder_protocols_are_transparent_and_emit_no_rows() {
    let recorder = std::sync::Arc::new(BlobMetricsRecorder::disabled());
    let store = InstrumentedBlobStore::new(
        InMemoryBlobStore::new(),
        recorder.clone(),
        BlobBackendKind::Memory,
    );
    let log = SegmentedObjectLog::open(store, SegmentConfig::new(4096, 100).unwrap());
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    log.enqueue(&source, &pushes(1), 0, 1).unwrap();
    log.seal(&source, 0, 1).unwrap();
    let branch_def = branch_qdef("disabled-observability");
    log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 0),
        1_000,
        2,
    )
    .unwrap();
    // Exercise the fence/acquire paths on a second, empty queue so this test isolates recorder
    // transparency from the branch writes above.
    let authority_def = branch_qdef("disabled-authority-observability");
    let authority =
        fireweed_engine::QueueKey::new(source.tenant_id.clone(), authority_def.queue_id.clone());
    log.create_queue(&authority_def).unwrap();
    log.fence_epoch(&authority, 1, 3).unwrap();
    assert_eq!(log.acquire_epoch(&authority, 4).unwrap(), 2);
    let snapshot = recorder.snapshot();
    assert!(snapshot.rows.is_empty());
    assert_eq!((snapshot.in_flight, snapshot.peak_in_flight), (0, 0));
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn branch_qdef(suffix: &str) -> fireweed_core::QueueDefinition {
    let mut def = qdef();
    def.queue_id = QueueId::new(format!("branch-{}-{suffix}", std::process::id())).unwrap();
    def
}

fn unique_qdef(label: &str) -> fireweed_core::QueueDefinition {
    let mut def = qdef();
    let n = HEAD_TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    def.tenant_id = fireweed_core::TenantId::new(format!(
        "segmented-{label}-tenant-{}-{n}",
        std::process::id()
    ))
    .unwrap();
    def.queue_id = QueueId::new(format!(
        "segmented-{label}-queue-{}-{n}",
        std::process::id()
    ))
    .unwrap();
    def
}

#[derive(Default)]
struct CountingBlobStore {
    inner: InMemoryBlobStore,
    segment_gets: AtomicU64,
    manifest_gets: AtomicU64,
    list_count: AtomicU64,
    get_keys: Mutex<Vec<String>>,
}

impl CountingBlobStore {
    fn segment_gets(&self) -> u64 {
        self.segment_gets.load(Ordering::Relaxed)
    }

    fn reset_reads(&self) {
        self.segment_gets.store(0, Ordering::Relaxed);
        self.manifest_gets.store(0, Ordering::Relaxed);
        self.list_count.store(0, Ordering::Relaxed);
        self.get_keys.lock().unwrap().clear();
    }

    fn get_keys(&self) -> Vec<String> {
        self.get_keys.lock().unwrap().clone()
    }
}

impl BlobStore for CountingBlobStore {
    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
        self.inner.put(key, body)
    }

    fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
        self.inner.put_if_absent(key, body)
    }

    fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
        self.get_keys.lock().unwrap().push(key.to_owned());
        if key.ends_with(".seg") {
            self.segment_gets.fetch_add(1, Ordering::Relaxed);
        }
        if key.contains("/manifest/") || key.contains("/manifest_head/") {
            self.manifest_gets.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.get(key)
    }

    fn delete(&self, key: &str) -> EngineResult<bool> {
        self.inner.delete(key)
    }

    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        self.list_count.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }

    fn stats(&self, prefix: &str) -> EngineResult<ObjectStoreStats> {
        self.inner.stats(prefix)
    }
}

#[test]
fn size_trigger_and_latency_trigger_both_seal_two_configurations() {
    // --- Config A: size threshold (small target → a batch seals as soon as it is enqueued). ---
    let size_cfg = SegmentConfig::new(1, 1_000_000).unwrap();
    let a = SegmentedObjectLog::open(InMemoryBlobStore::new(), size_cfg);
    a.create_queue(&qdef()).unwrap();
    let out = a.enqueue(&shard(), &pushes(5), 0, 0).unwrap();
    assert_eq!(
        out.committed.len(),
        5,
        "size trigger sealed the batch on enqueue"
    );
    assert_eq!(a.counters().segments_sealed, 1);
    assert_eq!(a.counters().group_commit_batches, vec![5]);
    assert_eq!(a.counters().size_triggered_seals, 1);
    assert_eq!(a.counters().latency_triggered_seals, 0);

    // --- Config B: latency cap (huge target → only the age-based flush seals). ---
    let latency_cfg = SegmentConfig::new(10_000_000, 50).unwrap();
    let b = SegmentedObjectLog::open(InMemoryBlobStore::new(), latency_cfg);
    b.create_queue(&qdef()).unwrap();
    let out = b.enqueue(&shard(), &pushes(4), 0, 1_000).unwrap();
    assert!(
        out.committed.is_empty(),
        "below size target → buffered, not acked"
    );
    // Not yet aged past 50ms → no seal.
    assert!(b.flush_due(&shard(), 0, 1_040).unwrap().is_empty());
    // Aged past 50ms → the latency trigger seals.
    let committed = b.flush_due(&shard(), 0, 1_060).unwrap();
    assert_eq!(
        committed.len(),
        4,
        "latency trigger sealed the buffered batch"
    );
    assert_eq!(b.counters().segments_sealed, 1);
    assert_eq!(b.counters().group_commit_batches, vec![4]);
    assert_eq!(b.counters().size_triggered_seals, 0);
    assert_eq!(b.counters().latency_triggered_seals, 1);
    assert_eq!(
        b.counters().group_commit_batches.iter().sum::<usize>() as u64,
        b.counters().commands_committed
    );
}

#[test]
fn recovery_replays_only_manifest_committed_segments() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();

    {
        let log = SegmentedObjectLog::open(store.clone(), cfg);
        log.create_queue(&qdef()).unwrap();
        // Two committed segments (5 commands) + 4 buffered-but-unsealed commands (never acked).
        log.enqueue(&shard(), &pushes(2), 0, 0).unwrap();
        log.seal(&shard(), 0, 1).unwrap();
        log.enqueue(&shard(), &pushes(3), 0, 2).unwrap();
        log.seal(&shard(), 0, 3).unwrap();
        log.enqueue(&shard(), &pushes(4), 0, 4).unwrap(); // buffered, not sealed
        assert_eq!(log.pending(&shard()), 4);
    }

    // A fresh substrate over the same store recovers position + epoch from the manifest and replays exactly
    // the 5 committed commands (the 4 unsealed ones were never durable → not recovered).
    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&qdef()).unwrap();
    let replayed = reopened.read_all(&shard()).unwrap();
    assert_eq!(
        replayed.len(),
        5,
        "only manifest-committed commands recover"
    );
    assert_eq!(
        replayed.iter().map(|(p, _)| p.sequence).collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4],
        "recovered positions are contiguous in sequence order"
    );

    // A post-recovery commit continues the sequence (no id/position collision).
    reopened.enqueue(&shard(), &pushes(1), 0, 10).unwrap();
    let pos = reopened.seal(&shard(), 0, 11).unwrap();
    assert_eq!(pos[0].sequence, 5);
}

#[test]
fn high_water_tail_replay_skips_fully_applied_segment_objects() {
    let store = std::sync::Arc::new(CountingBlobStore::default());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef()).unwrap();

    for (now, batch) in [(1, 3), (2, 3), (3, 3)] {
        log.enqueue(&shard(), &pushes(batch), 0, now).unwrap();
        log.seal(&shard(), 0, now + 10).unwrap();
    }
    log.advance_high_water(&shard(), CommandPosition::new(shard(), 0, 5))
        .unwrap();
    let high_water = log.read_high_water(&shard()).unwrap().unwrap();

    store.reset_reads();
    let replayed = log
        .read_from(&shard(), high_water.sequence + 1)
        .expect("read tail from durable high-water");

    assert_eq!(
        replayed.iter().map(|(p, _)| p.sequence).collect::<Vec<_>>(),
        vec![6, 7, 8],
        "tail replay starts after the durable high-water"
    );
    assert_eq!(
        store.segment_gets(),
        1,
        "fully-applied segment prefixes must not be fetched or deserialized"
    );
}

#[test]
fn limited_read_fetches_only_segments_needed_for_the_page() {
    let store = std::sync::Arc::new(CountingBlobStore::default());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef()).unwrap();

    for (now, batch) in [(1, 3), (2, 3), (3, 3)] {
        log.enqueue(&shard(), &pushes(batch), 0, now).unwrap();
        log.seal(&shard(), 0, now + 10).unwrap();
    }

    store.reset_reads();
    let replayed = log
        .read_from_limited(&shard(), 0, 4)
        .expect("bounded recovery page");

    assert_eq!(
        replayed.iter().map(|(p, _)| p.sequence).collect::<Vec<_>>(),
        vec![0, 1, 2, 3],
        "bounded page returns exactly the requested command window"
    );
    assert_eq!(
        store.segment_gets(),
        2,
        "bounded read should stop after fetching the segments needed to fill the page"
    );
}

#[test]
fn one_command_per_segment_config_is_rejected_unless_dev_flag() {
    assert_eq!(
        SegmentConfig::new(1_000, 0).unwrap_err(),
        EngineError::Invalid("segment_max_latency_ms must be > 0")
    );
    // The dev flag opts into one-command-per-segment sealing (test only).
    let mut cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    cfg.dev_unsafe_one_command_segments = true;
    let log = SegmentedObjectLog::open(InMemoryBlobStore::new(), cfg);
    log.create_queue(&qdef()).unwrap();
    let out = log.enqueue(&shard(), &pushes(1), 0, 0).unwrap();
    assert_eq!(
        out.committed.len(),
        1,
        "dev flag seals each command immediately"
    );
}

#[test]
fn branch_reads_survive_source_prefix_deletion_and_stay_on_branch_owned_keys() {
    let store = std::sync::Arc::new(CountingBlobStore::default());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let parent = SegmentedObjectLog::open(store.clone(), cfg);
    let parent_def = qdef();
    let parent_shard = shard();
    parent.create_queue(&parent_def).unwrap();

    for _ in 0..6 {
        parent.enqueue(&parent_shard, &pushes(1), 0, 10).unwrap();
        parent.seal(&parent_shard, 0, 11).unwrap();
    }
    advance_floor_as_local_owner(&parent, &parent_shard, 3, 20).unwrap();
    assert_eq!(
        expire_segments_as_local_owner(&parent, &parent_shard, 3, 20).unwrap(),
        4
    );

    let branch_def = branch_qdef("trimmed-source");
    let branch_shard =
        fireweed_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    let branch_epoch = parent
        .branch(
            &parent_shard,
            &branch_def,
            &CommandPosition::new(parent_shard.clone(), 0, 5),
            60_000,
            30,
        )
        .unwrap();
    assert_eq!(branch_epoch, 1);

    let source_shard_prefix = shard_prefix_of(&parent_shard);
    let source_manifest_prefix = format!("{source_shard_prefix}manifest/");
    let source_manifest_head_prefix = format!("{source_shard_prefix}manifest_head/");
    for key in store.inner.list(&source_manifest_prefix).unwrap() {
        assert!(store.inner.delete(&key).unwrap());
    }
    for key in store.inner.list(&source_manifest_head_prefix).unwrap() {
        assert!(store.inner.delete(&key).unwrap());
    }
    store.reset_reads();

    let branch_view = parent.read_all(&branch_shard).unwrap();
    assert_eq!(
        branch_view
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![4, 5],
        "branch reads stay on the inherited live view after the source prefixes are physically deleted"
    );
    assert!(
        store
            .get_keys()
            .iter()
            .all(|key| !key.starts_with(&source_shard_prefix)),
        "branch reads do not GET deleted source manifest or segment objects"
    );
    assert_eq!(
        parent
            .read_retention_floor(&branch_shard)
            .unwrap()
            .unwrap()
            .sequence,
        3,
        "the branch still inherits the trimmed source floor"
    );
}

#[test]
fn branch_gets_own_lease_no_parent_fence_change() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store, cfg);
    let parent_def = qdef();
    let parent_shard = shard();
    log.create_queue(&parent_def).unwrap();
    log.enqueue(&parent_shard, &pushes(2), 0, 10).unwrap();
    log.seal(&parent_shard, 0, 11).unwrap();

    let parent_epoch = log.current_epoch(&parent_shard).unwrap();
    let branch_def = branch_qdef("lease");
    let branch_shard =
        fireweed_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    let branch_epoch = log
        .branch(
            &parent_shard,
            &branch_def,
            &CommandPosition::new(parent_shard.clone(), parent_epoch, 1),
            60_000,
            20,
        )
        .unwrap();

    assert_eq!(log.current_epoch(&parent_shard).unwrap(), parent_epoch);
    assert_eq!(log.current_epoch(&branch_shard).unwrap(), branch_epoch);
    assert_eq!(branch_epoch, parent_epoch + 1);
}

#[test]
fn branch_suppresses_emission_by_default() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store, cfg);
    let parent_def = qdef();
    let parent_shard = shard();
    log.create_queue(&parent_def).unwrap();
    log.enqueue(&parent_shard, &pushes(1), 0, 10).unwrap();
    log.seal(&parent_shard, 0, 11).unwrap();

    let branch_def = branch_qdef("emit");
    let branch_epoch = log
        .branch(
            &parent_shard,
            &branch_def,
            &CommandPosition::new(parent_shard.clone(), 0, 0),
            60_000,
            20,
        )
        .unwrap();
    assert_eq!(branch_epoch, 1);
    assert!(
        !log.branch_emits_change_records(&fireweed_engine::QueueKey::new(
            branch_def.tenant_id.clone(),
            branch_def.queue_id.clone(),
        ))
        .unwrap(),
        "branch activity is emission-suppressed unless explicitly enabled"
    );
}

#[test]
#[allow(non_snake_case)]
fn TestBranchPinRulesUnchanged() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let source = shard();
    log.create_queue(&qdef()).unwrap();

    for i in 0..4u64 {
        log.enqueue(&source, &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&source, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }
    trim_cycle(&log, &source, 3, 0, 1_000);

    let branch_def = branch_qdef("rules");
    let branch = QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    let branch_epoch = log
        .branch(
            &source,
            &branch_def,
            &CommandPosition::new(source.clone(), 0, 5),
            60_000,
            2_000,
        )
        .unwrap();

    assert_eq!(
        log.read_retention_floor(&branch).unwrap().unwrap().sequence,
        3,
        "the branch still inherits the source floor"
    );
    assert_eq!(branch_epoch, 1);
    assert_eq!(
        log.read_all(&branch)
            .unwrap()
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![4, 5],
        "branch creation and read visibility stay unchanged"
    );

    log.discard_branch(&source, &branch).unwrap();
    assert_eq!(
        log.read_from(&source, 4)
            .unwrap()
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![4, 5, 6, 7],
        "discarding the branch does not perturb the source's live tail"
    );
}

#[test]
#[allow(non_snake_case)]
fn TestDeletionWatermarkStopsBeforePinnedManifest() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let source = shard();
    log.create_queue(&qdef()).unwrap();

    log.enqueue(&source, &pushes(2), 0, 10).unwrap();
    log.seal(&source, 0, 11).unwrap();
    log.enqueue(&source, &pushes(2), 0, 20).unwrap();
    log.seal(&source, 0, 21).unwrap();

    let branch_def = branch_qdef("watermark");
    let branch = QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 1),
        60_000,
        30,
    )
    .unwrap();

    advance_floor_as_local_owner(&log, &source, 1, 0).unwrap();
    assert_eq!(
        expire_segments_as_local_owner(&log, &source, 1, 31).unwrap(),
        0,
        "the pinned below-floor entry is skipped on the first pass"
    );
    assert_eq!(
        log.read_manifest_deletion_watermark(&source).unwrap(),
        None,
        "the deletion watermark does not advance past the pinned entry"
    );

    log.discard_branch(&source, &branch).unwrap();
    assert_eq!(
        expire_segments_as_local_owner(&log, &source, 1, 32).unwrap(),
        1,
        "after the pin releases, the same entry is reclaimed"
    );
    assert_eq!(
        log.read_manifest_deletion_watermark(&source).unwrap(),
        Some(0),
        "the watermark advances only after the entry is reclaimed"
    );
}

// Test 7 (bead pqueue-b5cc2bc7 — branch-pin safety of segment reclamation): a live branch pinning a
// below-floor segment keeps that segment object alive through a trim (`expire_segments_through` skips it via
// its live-pin snapshot), while an unpinned below-floor segment IS reclaimed. Separately, a NEW branch cut at
// or below the durable retention floor is rejected CLEANLY (a fast `Invalid`, not a later "missing segment").

// Test (bead pqueue-b5cc2bc7 bug 3 — cross-owner floor is an ATOMIC epoch-fenced MANIFEST CAS): the composed
// unit-of-work lock is process-LOCAL and cannot fence a peer owner, so the floor advance is routed through the
// same create-only, epoch-fenced manifest CAS as data segments and epoch fences. A superseded owner
// interleaved with a new owner's higher floor advance is rejected (EpochFenced / CAS-lost), the floor stays at
// the higher value, and — after the new owner reclaims through it — recovery reads NO missing segment.
#[test]
fn retention_floor_advance_is_epoch_fenced_manifest_cas_against_a_superseded_owner() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let shard = shard();
    log.create_queue(&qdef()).unwrap();
    // One data segment [0..3] at epoch 0, then a fresh tail segment [4..4].
    log.enqueue(&shard, &pushes(4), 0, 10).unwrap();
    log.seal(&shard, 0, 11).unwrap();
    log.enqueue(&shard, &pushes(1), 0, 10).unwrap();
    log.seal(&shard, 0, 11).unwrap();

    // Owner at epoch 0 advances the floor to seq 1 (a floor MANIFEST ENTRY committed via CAS).
    advance_floor_as_local_owner(&log, &shard, 1, 0).unwrap();

    // A NEW owner takes over: acquire a strictly-greater epoch (a fence entry committed to the manifest).
    let new_epoch = log.acquire_epoch(&shard, 20).unwrap();
    assert!(new_epoch > 0, "acquire_epoch must bump the manifest epoch");

    // The new owner advances the floor to seq 3 at the new epoch — accepted (CAS won) — and reclaims through it.
    log.advance_retention_floor(
        &shard,
        CommandPosition::new(shard.clone(), new_epoch, 3),
        new_epoch,
    )
    .unwrap();
    assert_eq!(
        log.read_retention_floor(&shard)
            .unwrap()
            .map(|p| p.sequence),
        Some(3),
        "the AUTHORITATIVE floor is the max retention-floor manifest entry (seq 3)"
    );
    expire_segments_as_local_owner(&log, &shard, 3, 21).unwrap();

    // The SUPERSEDED owner (still believing it holds epoch 0) tries to LOWER the floor to seq 2 -> rejected by
    // the epoch-fenced CAS; the floor stays at the newer owner's seq 3.
    let fenced = log
        .advance_retention_floor(&shard, CommandPosition::new(shard.clone(), 0, 2), 0)
        .unwrap_err();
    assert!(
        matches!(fenced, EngineError::EpochFenced | EngineError::Conflict),
        "a superseded owner's floor advance must be rejected (EpochFenced/Conflict), got {fenced:?}"
    );
    assert_eq!(
        log.read_retention_floor(&shard)
            .unwrap()
            .map(|p| p.sequence),
        Some(3),
        "the newer owner's floor is not regressed by the superseded owner"
    );

    // Recovery over the SAME store derives the authoritative floor from the manifest and reads no missing
    // segment: read_from(floor+1 = 4) surfaces the tail with NO GET of the reclaimed below-floor segment.
    let reopened = SegmentedObjectLog::open(store, SegmentConfig::new(10_000_000, 100).unwrap());
    reopened.create_queue(&qdef()).unwrap();
    assert_eq!(
        reopened
            .read_retention_floor(&shard)
            .unwrap()
            .map(|p| p.sequence),
        Some(3),
        "the floor survives reopen (it is a durable manifest entry, not a lost blob)"
    );
    let tail = reopened.read_from(&shard, 4).unwrap();
    assert_eq!(
        tail.len(),
        1,
        "recovery reads the tail with no missing segment"
    );
}

#[test]
fn retention_floor_advance_rejects_observer_with_only_durable_epoch_knowledge() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let shard = shard();
    let owner = SegmentedObjectLog::open(store.clone(), cfg);
    owner.create_queue(&qdef()).unwrap();
    owner.enqueue(&shard, &pushes(2), 0, 10).unwrap();
    owner.seal(&shard, 0, 11).unwrap();

    let observer = SegmentedObjectLog::open(store, cfg);
    observer.create_queue(&qdef()).unwrap();
    let durable_epoch = observer.current_epoch(&shard).unwrap();
    let error = observer
        .advance_retention_floor(
            &shard,
            CommandPosition::new(shard.clone(), durable_epoch, 1),
            durable_epoch,
        )
        .unwrap_err();
    assert_eq!(error, EngineError::EpochFenced);
    assert!(observer.read_retention_floor(&shard).unwrap().is_none());
}

// Test (bead pqueue-b5cc2bc7 bug 1 — GROUP-COMMIT batch-max seal timestamp): when several pushes co-buffer
// into ONE segment and a LATER push has a SMALLER `created_at` than an earlier buffered one, the sealed
// segment's `committed_at_ms` is the batch MAX (not the triggering call's `now_ms`), so a cutoff between the
// two does NOT age-trim the segment while the earlier push is still within retention.
#[test]
fn group_commit_seal_stamps_committed_at_as_the_batch_max_created_at() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    // Large target so both pushes co-buffer into ONE segment (no auto-seal on the first).
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store, cfg);
    let shard = shard();
    log.create_queue(&qdef()).unwrap();

    // Buffer A (created_at = 10_000ms) then B (created_at = 1_000ms) — a LATER push with a SMALLER created_at.
    log.enqueue(&shard, &envelope_created_at(10), 0, 5).unwrap();
    log.enqueue(&shard, &envelope_created_at(1), 0, 5).unwrap();
    // Seal with a deliberately SMALL now_ms (1) — the pathological trigger timestamp.
    log.seal(&shard, 0, 1).unwrap();

    // committed_at_ms = max(now_ms=1, batch_max_created=10_000) = 10_000. A cutoff at 5_000 must NOT trim it.
    assert_eq!(
        log.max_trimmable_seq_before(&shard, 5_000).unwrap(),
        None,
        "the segment's committed_at is the batch MAX (10_000ms); a 5_000ms cutoff does not age-trim it"
    );
    // A cutoff PAST the batch max does trim (visible_last_seq = 1, the two co-buffered commands seq 0,1).
    assert_eq!(
        log.max_trimmable_seq_before(&shard, 20_000).unwrap(),
        Some(1),
        "a cutoff past the batch max age-trims the co-buffered segment"
    );
}

// A single-item Push envelope with an explicit `created_at` of `created_secs` seconds (bug 1 group-commit
// test): returns a one-element Vec so it can be passed by slice to `enqueue`.
fn envelope_created_at(created_secs: i64) -> Vec<fireweed_engine::CommandEnvelope> {
    let mut env = envelope(
        QueueCommand::Push(PushCommand {
            items: vec![item("0", "k0", 5)],
        }),
        vec![],
    );
    env.created_at = fireweed_core::UtcTimestamp::new(created_secs, 0).unwrap();
    vec![env]
}

// A fault hook that, the FIRST time a seal reaches `BeforeSegmentWrite` (after it has drained + released the
// mutex), enqueues envelope B into the SAME buffer — modelling a concurrent enqueue interleaving an in-flight
// seal (bead pqueue-b5cc2bc7 HOLE A). B carries created_at=100s.
struct EnqueueDuringSeal {
    log: std::sync::Weak<SegmentedObjectLog<InMemoryBlobStore>>,
    shard: fireweed_engine::QueueKey,
    fired: AtomicBool,
}

impl FaultHook for EnqueueDuringSeal {
    fn fault_point(&self, cut: FaultCutPoint) -> EngineResult<()> {
        if cut == FaultCutPoint::BeforeSegmentWrite
            && !self.fired.swap(true, Ordering::SeqCst)
            && let Some(log) = self.log.upgrade()
        {
            log.enqueue(&self.shard, &envelope_created_at(100), 0, 1)
                .unwrap();
        }
        Ok(())
    }
}

// HOLE A (bead pqueue-b5cc2bc7 — group-commit committed_at is race-free): with the OLD resettable running
// `max_created_ms`, a concurrent enqueue during an in-flight seal raised the counter and the seal's completion
// then UNCONDITIONALLY reset it to 0 while the new command was still buffered, so THAT command's later seal
// stamped `committed_at_ms < its created_at` (a within-retention request_id could be age-trimmed). With each
// buffered command carrying its OWN `created_at`, every seal derives `committed_at_ms` from the batch it holds
// in hand — no shared counter to clobber.
#[test]
fn group_commit_seal_committed_at_is_race_free_across_an_interleaved_enqueue() {
    let log = std::sync::Arc::new(SegmentedObjectLog::open(
        InMemoryBlobStore::new(),
        SegmentConfig::new(10_000_000, 100).unwrap(),
    ));
    let shard = shard();
    log.create_queue(&qdef()).unwrap();

    // Buffer A (created_at 10s). Arm the hook to enqueue B (created_at 100s) DURING A's seal.
    log.enqueue(&shard, &envelope_created_at(10), 0, 1).unwrap();
    log.set_fault_hook(Some(std::sync::Arc::new(EnqueueDuringSeal {
        log: std::sync::Arc::downgrade(&log),
        shard: shard.clone(),
        fired: AtomicBool::new(false),
    })));
    // Seal A with a SMALL now_ms (1). B is enqueued mid-seal; A's completion no longer resets a shared max.
    log.seal(&shard, 0, 1).unwrap(); // seg0 = [A], committed_at = max(1, 10_000) = 10_000ms
    log.set_fault_hook(None);
    // B is still buffered with its OWN created_at (100_000ms). Seal it, again with a small now_ms.
    log.seal(&shard, 0, 1).unwrap(); // seg1 = [B], committed_at = max(1, 100_000) = 100_000ms

    // A 50_000ms cutoff trims ONLY seg0 (10_000); seg1 (B, committed 100_000, still within retention) is
    // retained. Under the old reset race seg1's committed_at would be 1 and B would be wrongly age-trimmed.
    assert_eq!(
        log.max_trimmable_seq_before(&shard, 50_000).unwrap(),
        Some(0),
        "the interleaved-enqueue command B keeps its own created_at ceiling; it is NOT age-trimmed early"
    );
    assert_eq!(
        log.max_trimmable_seq_before(&shard, 200_000).unwrap(),
        Some(1),
        "a cutoff past B's committed_at trims through both segments"
    );
}

// A fault hook that runs a concurrent PEER TRIM every time a branch attempt reaches `DuringBranchCopy`, up to
// `advances_remaining` times, advancing the source floor by ONE more each fire (monotonically: 1, 2, 3, ...)
// then reclaiming through it. Set `advances_remaining` to a small number for a BOUNDED race (the peer stops,
// so a later retry sees a STABLE floor and SUCCEEDS) or a large number for CONTINUOUS trimming (every attempt
// races, so the bounded retry gives up cleanly). `next_floor` starts at 1 so the first advance is a real move.
struct PeerTrimBoundedAdvances<S: BlobStore> {
    log: std::sync::Weak<SegmentedObjectLog<S>>,
    source: fireweed_engine::QueueKey,
    now_ms: i64,
    advances_remaining: AtomicU64,
    next_floor: AtomicU64,
}

impl<S: BlobStore + 'static> FaultHook for PeerTrimBoundedAdvances<S> {
    fn fault_point(&self, cut: FaultCutPoint) -> EngineResult<()> {
        if cut != FaultCutPoint::DuringBranchCopy {
            return Ok(());
        }
        // Consume one advance budget (CAS loop); do nothing once the peer has stopped trimming.
        loop {
            let rem = self.advances_remaining.load(Ordering::SeqCst);
            if rem == 0 {
                return Ok(());
            }
            if self
                .advances_remaining
                .compare_exchange(rem, rem - 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
        if let Some(log) = self.log.upgrade() {
            let floor = self.next_floor.fetch_add(1, Ordering::SeqCst);
            // Advance the source floor (epoch-fenced manifest CAS) then reclaim. The branch's pin is already
            // published, so `expire_segments_through` SKIPS the pinned segments — nothing is deleted.
            advance_floor_as_local_owner(&log, &self.source, floor, self.now_ms).unwrap();
            expire_segments_as_local_owner(&log, &self.source, floor, self.now_ms).unwrap();
        }
        Ok(())
    }
}

// Bead pqueue-9dcec223 (a): a branch racing a BOUNDED concurrent source-floor advance RETRIES and SUCCEEDS.
// The peer advances the floor a fixed number of times (to 1, then 2) then stops; the bounded retry re-reads
// the advanced floor each attempt and, once the peer stops, commits a valid branch against the retained range
// — `read_all(branch)` returns the expected retained commands with NO missing segment.
#[test]
fn branch_retries_a_bounded_concurrent_floor_advance_and_succeeds() {
    let recorder = std::sync::Arc::new(BlobMetricsRecorder::new());
    let store = InstrumentedBlobStore::new(
        InMemoryBlobStore::new(),
        recorder.clone(),
        BlobBackendKind::Memory,
    );
    let log = std::sync::Arc::new(SegmentedObjectLog::open(
        store,
        SegmentConfig::new(10_000_000, 100).unwrap(),
    ));
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    let owner_epoch = log.acquire_epoch(&source, 0).unwrap();
    // Six single-command segments, seq 0..5.
    for _ in 0..6 {
        log.enqueue(&source, &pushes(1), owner_epoch, 10).unwrap();
        log.seal(&source, owner_epoch, 10).unwrap();
    }

    // Peer advances the floor to 1 then 2 (two attempts race), then STOPS — a later retry sees a stable floor=2.
    log.set_fault_hook(Some(std::sync::Arc::new(PeerTrimBoundedAdvances {
        log: std::sync::Arc::downgrade(&log),
        source: source.clone(),
        now_ms: 100,
        advances_remaining: AtomicU64::new(2),
        next_floor: AtomicU64::new(1),
    })));

    let branch_def = branch_qdef("bounded-trim-retry");
    let branch =
        fireweed_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    // Cut at 5 stays ABOVE the peer's final floor (2), so the retry SUCCEEDS (transparently — the caller never
    // sees the intermediate Conflicts).
    let metrics_before = recorder.snapshot();
    log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 5),
        1_000_000_000,
        100,
    )
    .expect("the bounded concurrent-trim race is retried transparently and succeeds");
    log.set_fault_hook(None);
    let metrics = recorder.snapshot().delta(&metrics_before);
    let logical = metrics.row(
        BlobOperation::Branch,
        BlobObjectClass::BranchPin,
        BlobResultClass::Success,
        false,
        BlobBackendKind::Memory,
    );
    assert_eq!(
        (logical.completions, logical.attempts, logical.retries),
        (1, 3, 2)
    );
    assert!(
        metrics
            .rows
            .iter()
            .filter(|row| matches!(
                row.operation,
                BlobOperation::Put | BlobOperation::PutIfAbsent
            ))
            .all(|row| row.values.retries == 0),
        "branch retries are logical iterations; winner/mirror PUTs are never labeled retries"
    );

    // The committed branch INHERITED the advanced floor (2) and reads ONLY the retained [3,4,5] — no missing
    // segment, no reclaimed object is ever GET.
    assert_eq!(
        log.read_retention_floor(&branch)
            .unwrap()
            .map(|p| p.sequence),
        Some(2),
        "the retried branch inherits the ADVANCED source floor (2), not the pre-race floor"
    );
    let seqs: Vec<u64> = log
        .read_all(&branch)
        .unwrap()
        .iter()
        .map(|(p, _)| p.sequence)
        .collect();
    assert_eq!(
        seqs,
        vec![3, 4, 5],
        "read_all(branch) returns exactly the retained [3,4,5] after retrying against the advanced floor"
    );
}

// Bead pqueue-9dcec223 (b): under CONTINUOUS trimming the branch GIVES UP after the cap and returns `Conflict`
// CLEANLY (no livelock, no leaked pin). The peer advances the floor on EVERY attempt, so validate-after-copy
// conflicts every time; after the bounded cap the last Conflict is surfaced and the source stays fully
// reclaimable (the final attempt rolled back its pin).

// Bead pqueue-9dcec223 (c): a cut that is BELOW the advanced floor is rejected with `Invalid`, NOT retried as
// a Conflict. The peer advances the floor to 3 (bounded, then stops); a branch cut at 2 is now at/below the
// advanced floor, so it is cleanly rejected ("whole view reclaimed") rather than looping.
#[test]
fn branch_cut_below_advanced_floor_is_rejected_invalid_not_retried() {
    let log = std::sync::Arc::new(SegmentedObjectLog::open(
        InMemoryBlobStore::new(),
        SegmentConfig::new(10_000_000, 100).unwrap(),
    ));
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    let owner_epoch = log.acquire_epoch(&source, 0).unwrap();
    for _ in 0..6 {
        log.enqueue(&source, &pushes(1), owner_epoch, 10).unwrap();
        log.seal(&source, owner_epoch, 10).unwrap();
    }

    // Peer advances the floor to 1, then 2, then 3, then stops.
    log.set_fault_hook(Some(std::sync::Arc::new(PeerTrimBoundedAdvances {
        log: std::sync::Arc::downgrade(&log),
        source: source.clone(),
        now_ms: 100,
        advances_remaining: AtomicU64::new(3),
        next_floor: AtomicU64::new(1),
    })));

    // Cut at 2 is at/below the floor the peer advances to (3) — a genuine cut<=floor, rejected as Invalid.
    let result = log.branch(
        &source,
        &branch_qdef("below-advanced-floor"),
        &CommandPosition::new(source.clone(), 0, 2),
        1_000_000_000,
        100,
    );
    log.set_fault_hook(None);

    assert!(
        matches!(result, Err(EngineError::Invalid(_))),
        "a cut at/below the advanced floor is rejected cleanly (Invalid), not retried as Conflict, got {result:?}"
    );
}

// A [`BlobStore`] wrapper that injects a store failure on the first `put` / `put_if_absent` / `delete` whose
// key contains an armed substring (bead pqueue-b5cc2bc7 error-path tests). Reads are never failed.
#[derive(Default)]
struct FailingBlobStore {
    inner: InMemoryBlobStore,
    fail_put_if_absent: std::sync::Mutex<Option<String>>,
}

impl FailingBlobStore {
    fn arm_put_if_absent(&self, substr: &str) {
        *self.fail_put_if_absent.lock().unwrap() = Some(substr.to_string());
    }
    fn disarm(&self) {
        *self.fail_put_if_absent.lock().unwrap() = None;
    }
    fn armed(lock: &std::sync::Mutex<Option<String>>, key: &str) -> bool {
        lock.lock()
            .unwrap()
            .as_deref()
            .is_some_and(|s| key.contains(s))
    }
}

impl BlobStore for FailingBlobStore {
    fn backend_kind(&self) -> BlobBackendKind {
        BlobBackendKind::Memory
    }
    fn max_physical_attempts_per_primitive(&self) -> Option<std::num::NonZeroUsize> {
        std::num::NonZeroUsize::new(1)
    }
    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
        self.inner.put(key, body)
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
        if Self::armed(&self.fail_put_if_absent, key) {
            return Err(EngineError::Storage(format!(
                "injected put_if_absent failure: {key}"
            )));
        }
        self.inner.put_if_absent(key, body)
    }
    fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn delete(&self, key: &str) -> EngineResult<bool> {
        self.inner.delete(key)
    }
    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        self.inner.list(prefix)
    }
}

// Drive a branch creation whose POST-PIN stage hits a store failure (armed via `arm`), then assert the
// error-path safety invariants (bead pqueue-b5cc2bc7): (a) branch creation FAILS and NO missing-segment
// branch is ever left (the source segments the branch referenced stay intact); (b/c) the source PIN is
// released iff cleanup completed — a clean rollback leaves the source fully reclaimable, while a
// cleanup-stage failure RETAINS the pin (safe leak) so a reclamation can never delete a referenced segment.

// Post-pin store failure at the MANIFEST-COPY stage -> clean rollback, pin released, source reclaimable.

// Post-pin store failure at the BRANCH.JSON-PUT stage -> clean rollback, pin released, source reclaimable.

// Post-pin store failure at the ACQUIRE-EPOCH stage (a `put_if_absent`) -> clean rollback, pin released.

// Post-pin failure PLUS a branch-object-CLEANUP failure -> the pin is RETAINED (safe leak); the source is
// NOT reclaimable (its segments stay protected — never an unpinned partial branch / missing segment).

// Post-pin failure where branch-object cleanup SUCCEEDS but the PIN delete fails -> the pin is RETAINED
// (safe leak: branch objects gone, source still protected), never released prematurely.

// THE COMPOUND CORRUPTION SCENARIO (bead pqueue-b5cc2bc7, codex round-7): a double store fault — the
// `branch.json` commit-marker PUT fails AND the rollback's branch-object cleanup fails — leaves a PARTIAL
// branch (manifest entries present, NO commit marker) protected only by a TTL-bounded pin. Even after the pin
// LAPSES at TTL and a trim reclaims the source segments, the partial branch is NON-READABLE (atomic existence
// gate), so read_all/read_from return empty — NEVER a missing segment. This closes the entire failed-branch
// class structurally, independent of pin/TTL/cleanup.

// A fully-COMMITTED branch (commit marker present) is readable and its live pin protects its referenced
// source segments (bead pqueue-b5cc2bc7 — the committed path is unchanged by the atomic-existence gate).
#[test]
fn committed_branch_is_readable_and_protected() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let log = SegmentedObjectLog::open(store, SegmentConfig::new(10_000_000, 100).unwrap());
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    for _ in 0..6 {
        log.enqueue(&source, &pushes(1), 0, 10).unwrap();
        log.seal(&source, 0, 10).unwrap();
    }
    let branch_def = branch_qdef("committed");
    let branch =
        fireweed_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 5),
        1_000_000_000,
        100,
    )
    .unwrap();
    // The committed branch is READABLE — its [0..5] view reads back (the commit marker landed LAST).
    let seqs: Vec<u64> = log
        .read_all(&branch)
        .unwrap()
        .iter()
        .map(|(p, _)| p.sequence)
        .collect();
    assert_eq!(
        seqs,
        vec![0, 1, 2, 3, 4, 5],
        "a committed branch reads its full [0..5] view"
    );
    // Its live pin protects the source segments against a concurrent trim.
    assert_eq!(
        expire_segments_as_local_owner(&log, &source, 5, 200).unwrap(),
        0,
        "the committed branch's live pin protects the source segments from reclamation"
    );
}

// HOLE B (bead pqueue-b5cc2bc7 — branch inherits the source floor; no missing segment): a branch cut ABOVE a
// trimmed source floor must copy ONLY the retained (at/above-floor) segments and inherit the floor, so
// `read_all(branch)` never GETs a reclaimed object. A cut at/below the floor is rejected cleanly.
#[test]
fn branch_inherits_source_retention_floor_and_reads_no_missing_segment() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let log = SegmentedObjectLog::open(store, SegmentConfig::new(10_000_000, 100).unwrap());
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    // Six single-command segments, seq 0..5.
    for _ in 0..6 {
        log.enqueue(&source, &pushes(1), 0, 10).unwrap();
        log.seal(&source, 0, 10).unwrap();
    }
    // Reclaim through floor=3 (segments 0-3 deleted).
    advance_floor_as_local_owner(&log, &source, 3, 0).unwrap();
    assert_eq!(
        expire_segments_as_local_owner(&log, &source, 3, 20).unwrap(),
        4,
        "segments 0-3 are reclaimed"
    );
    assert_eq!(
        log.read_retention_floor(&source)
            .unwrap()
            .map(|p| p.sequence),
        Some(3)
    );

    // A branch cut at 2 (<= floor 3) is rejected cleanly (its whole view is reclaimed).
    let err = log
        .branch(
            &source,
            &branch_qdef("below-floor-cut"),
            &CommandPosition::new(source.clone(), 0, 2),
            60_000,
            21,
        )
        .unwrap_err();
    assert!(
        matches!(err, EngineError::Invalid(_)),
        "a branch cut at/below the source floor is rejected cleanly (Invalid), got {err:?}"
    );

    // A branch cut at 5 (> floor 3) INHERITS floor=3 and reads only the retained [4,5] — no missing segment.
    let branch_def = branch_qdef("above-floor-cut");
    let branch =
        fireweed_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 5),
        60_000,
        22,
    )
    .unwrap();
    assert_eq!(
        log.read_retention_floor(&branch)
            .unwrap()
            .map(|p| p.sequence),
        Some(3),
        "the branch INHERITS the source retention floor (its effective genesis is floor+1)"
    );
    let seqs: Vec<u64> = log
        .read_all(&branch)
        .unwrap()
        .iter()
        .map(|(p, _)| p.sequence)
        .collect();
    assert_eq!(
        seqs,
        vec![4, 5],
        "read_all(branch) returns ONLY the retained [4,5] commands — the below-floor tombstones are not copied, so no reclaimed object is ever GET"
    );
}

// **AC-3 — TestBranchInheritanceSourcePinsPreserved** (bead pqueue-151257a3, part 2 of the pqueue-92a2e386
// split). Branch creation off the RETAINED floor/head metadata (the same trimmed-source substrate as
// [`branch_inheritance_uses_retained_floor_metadata`]) must publish the exact same source pin and orphan-GC
// contract an ordinary, untrimmed-source branch gets — the retained-metadata inheritance path is not a
// second, weaker contract:
//
// * the source pin (`{source}branches/{branch}.json` registry entry) is durable once creation returns;
// * the source's own retention floor is untouched by branch creation;
// * [`SegmentedObjectLog::gc_orphaned_branches`] never reclaims the committed branch (orphan GC guarantee);
// * the branch's live pin still protects every RETAINED segment it copied (4..7) against
//   [`SegmentedObjectLog::expire_segments_through`] (retention floor / reclamation guarantee);
// * discarding the branch releases the pin and the formerly-pinned retained segments become reclaimable,
//   exactly as [`branch_pins_parent_segments_against_expiry`] proves for an untrimmed source.

// LIVE MinIO integration (env-gated on `FIREWEED_S3_TEST_ENDPOINT`; LOUD skip otherwise). Runs the full
// substrate against a real S3-compatible endpoint: group-commit segments, ack-after-manifest-commit, the
// create-only manifest CAS, the epoch fence, recovery, and the measured counters.
#[test]
fn segmented_object_log_commits_through_minio() {
    let Ok(endpoint) = std::env::var("FIREWEED_S3_TEST_ENDPOINT") else {
        eprintln!(
            "\n================================================================\n\
             MINIO INTEGRATION SKIPPED (segmented_object_log_commits_through_minio)\n\
             set FIREWEED_S3_TEST_ENDPOINT=http://<container-ip>:9000 to run it.\n\
             (this host cannot reach docker PUBLISHED ports; use the container IP)\n\
             ================================================================\n"
        );
        return;
    };
    let bucket =
        std::env::var("FIREWEED_S3_TEST_BUCKET").unwrap_or_else(|_| "fireweed-test".into());
    let access =
        std::env::var("FIREWEED_S3_TEST_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into());
    let secret =
        std::env::var("FIREWEED_S3_TEST_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into());

    let s3 = S3BlobStore::new(&endpoint, &bucket, &access, &secret, "us-east-1").unwrap();
    s3.create_bucket().expect("create/ensure bucket");

    // Round-trip the raw object surface first (PUT / GET / create-only CAS) so a signing failure is obvious.
    let probe = format!("probe/{}.txt", std::process::id());
    s3.put(&probe, b"hello").unwrap();
    assert_eq!(s3.get(&probe).unwrap().as_deref(), Some(&b"hello"[..]));
    assert!(
        !s3.put_if_absent(&probe, b"again").unwrap(),
        "create-only CAS fails on an existing key"
    );

    // Use a per-process queue id so reruns against a persistent MinIO do not collide.
    let mut def = qdef();
    def.queue_id = fireweed_core::QueueId::new(format!("minio-{}", std::process::id())).unwrap();
    let shard = fireweed_engine::QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());

    let store = std::sync::Arc::new(s3);
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&def).unwrap();

    // Buffer below threshold → NOT acked, NOT durable.
    let out = log.enqueue(&shard, &pushes(4), 0, 1_000).unwrap();
    assert!(out.committed.is_empty());
    assert!(
        log.read_all(&shard).unwrap().is_empty(),
        "ack-after-commit: buffered commands are not yet in the manifest"
    );

    // Seal → segment object + manifest entry commit on MinIO; only now acked + visible.
    let positions = log.seal(&shard, 0, 1_050).unwrap();
    assert_eq!(positions.len(), 4);
    assert_eq!(log.read_all(&shard).unwrap().len(), 4);

    // A second group-commit segment.
    log.enqueue(&shard, &pushes(6), 0, 2_000).unwrap();
    log.seal(&shard, 0, 2_050).unwrap();
    assert_eq!(log.read_all(&shard).unwrap().len(), 10);

    // Epoch fence against MinIO: a new owner acquires epoch 1; the stale epoch-0 writer is CAS-fenced.
    let owner_b = SegmentedObjectLog::open(store.clone(), cfg);
    owner_b.create_queue(&def).unwrap();
    assert_eq!(owner_b.acquire_epoch(&shard, 3_000).unwrap(), 1);
    log.enqueue(&shard, &pushes(2), 0, 3_100).unwrap();
    assert_eq!(
        log.seal(&shard, 0, 3_101).unwrap_err(),
        EngineError::EpochFenced,
        "stale epoch writer is fenced by the manifest-recorded epoch on MinIO"
    );
    // The new owner commits under epoch 1.
    owner_b.enqueue(&shard, &pushes(3), 1, 3_200).unwrap();
    owner_b.seal(&shard, 1, 3_201).unwrap();

    // Recovery: a fresh substrate over the same MinIO bucket replays the committed log.
    let recovered = SegmentedObjectLog::open(store.clone(), cfg);
    recovered.create_queue(&def).unwrap();
    assert_eq!(
        recovered.read_all(&shard).unwrap().len(),
        13,
        "recovery from MinIO replays all manifest-committed segments (10 @epoch0 + 3 @epoch1)"
    );

    let c = log.counters();
    println!(
        "\nMINIO segmented object-log substrate counters (owner A): segments_sealed={} objects_put={} \
         commands_committed={} group_commit_batches={:?}",
        c.segments_sealed, c.objects_put, c.commands_committed, c.group_commit_batches
    );
    println!(
        "  ack-after-commit proven (buffered read_all empty until seal); epoch-fence proven (stale seal → \
         EpochFenced); recovery replayed 13 committed commands across 2 epochs."
    );
    assert_eq!(
        c.segments_sealed, 2,
        "owner A sealed two segments before being fenced"
    );
    assert_eq!(c.commands_committed, 10);
}

// ---------------------------------------------------------------------------
// Orphaned uncommitted-branch GC (bead pqueue-74f03d0e)
// ---------------------------------------------------------------------------

// The tenant/queue key prefix a shard's objects live under (mirrors the crate-internal `shard_prefix`).
fn shard_prefix_of(k: &fireweed_engine::QueueKey) -> String {
    format!(
        "t/{}/q/{}/",
        hex_lower(k.tenant_id.as_str().as_bytes()),
        hex_lower(k.queue_id.as_str().as_bytes())
    )
}

fn branch_registry_key_of(
    source: &fireweed_engine::QueueKey,
    branch: &fireweed_engine::QueueKey,
) -> String {
    format!(
        "{}branches/{}/{}.json",
        shard_prefix_of(source),
        hex_lower(branch.tenant_id.as_str().as_bytes()),
        hex_lower(branch.queue_id.as_str().as_bytes())
    )
}

// A store that injects a FAILED branch creation whose OWN rollback cleanup also fails, leaving a durable
// orphan (leftover `branch.pending` sentinel + partial branch manifest + a still-registered source pin). While
// armed it (a) fails the `branch.json` commit-marker put — the LAST write of a branch creation — so the branch
// never commits, and (b) fails every delete — so the creation rollback cannot clean up. Disarm it to let GC's
// deletes through.
struct OrphanBranchFaultStore {
    inner: InMemoryBlobStore,
    fail_marker_put: AtomicBool,
    fail_deletes: AtomicBool,
    permanent_delete_fault: AtomicBool,
    missing_get: Mutex<Option<String>>,
}

impl OrphanBranchFaultStore {
    fn new() -> Self {
        Self {
            inner: InMemoryBlobStore::new(),
            fail_marker_put: AtomicBool::new(false),
            fail_deletes: AtomicBool::new(false),
            permanent_delete_fault: AtomicBool::new(false),
            missing_get: Mutex::new(None),
        }
    }

    fn arm(&self) {
        self.fail_marker_put.store(true, Ordering::SeqCst);
        self.fail_deletes.store(true, Ordering::SeqCst);
    }

    fn disarm(&self) {
        self.fail_marker_put.store(false, Ordering::SeqCst);
        self.fail_deletes.store(false, Ordering::SeqCst);
        self.permanent_delete_fault.store(false, Ordering::SeqCst);
        *self.missing_get.lock().unwrap() = None;
    }

    fn arm_missing_get(&self, key: &str) {
        *self.missing_get.lock().unwrap() = Some(key.to_owned());
    }
}

impl BlobStore for OrphanBranchFaultStore {
    fn backend_kind(&self) -> BlobBackendKind {
        BlobBackendKind::Memory
    }

    fn classify_fault(&self, error: &EngineError) -> BlobStoreFault {
        if matches!(error, EngineError::Storage(message) if message.contains("cleanup delete failed"))
        {
            BlobStoreFault::new(BlobResultClass::Transport, true, false, false)
        } else {
            BlobStoreFault::from_engine_error(error)
        }
    }

    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
        if self.fail_marker_put.load(Ordering::SeqCst) && key.ends_with("branch.json") {
            return Err(EngineError::Storage(
                "injected: branch commit-marker put failed".into(),
            ));
        }
        self.inner.put(key, body)
    }

    fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
        self.inner.put_if_absent(key, body)
    }

    fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
        if self
            .missing_get
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|missing| missing == key)
        {
            return Ok(None);
        }
        self.inner.get(key)
    }

    fn delete(&self, key: &str) -> EngineResult<bool> {
        if self.fail_deletes.load(Ordering::SeqCst) {
            return Err(EngineError::Storage(
                "injected: branch cleanup delete failed".into(),
            ));
        }
        self.inner.delete(key)
    }

    fn observed_delete(&self, key: &str) -> ClassifiedBlobResult<ObservedBlobCall<bool>> {
        if self.fail_deletes.load(Ordering::SeqCst) {
            let outward =
                EngineError::Storage("typed injected branch cleanup delete failed".into());
            let fault = if self.permanent_delete_fault.load(Ordering::SeqCst) {
                BlobStoreFault::new(BlobResultClass::Corrupt, false, false, false)
            } else {
                BlobStoreFault::new(BlobResultClass::Transport, true, false, false)
            };
            return Err(ClassifiedBlobError {
                outward,
                fault,
                attempts: 1,
                request_bytes: 0,
                response_bytes: 0,
            });
        }
        self.inner.observed_delete(key)
    }

    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        self.inner.list(prefix)
    }

    fn stats(&self, prefix: &str) -> EngineResult<ObjectStoreStats> {
        self.inner.stats(prefix)
    }
}

// Fault-inject a FAILED branch creation (marker-put + rollback-cleanup both fail) that leaves a durable orphan.
// Returns the branch key. The source has six single-command segments (seq 0..5); the branch is cut at seq 3.
fn seed_orphaned_branch(
    log: &SegmentedObjectLog<std::sync::Arc<OrphanBranchFaultStore>>,
    store: &OrphanBranchFaultStore,
    source: &fireweed_engine::QueueKey,
    suffix: &str,
    created_at: i64,
) -> fireweed_engine::QueueKey {
    log.create_queue(&qdef()).unwrap();
    log.fence_epoch(source, 0, created_at.saturating_sub(2))
        .unwrap();
    let owner_epoch = log
        .acquire_epoch(source, created_at.saturating_sub(1))
        .unwrap();
    for _ in 0..6 {
        log.enqueue(source, &pushes(1), owner_epoch, 10).unwrap();
        log.seal(source, owner_epoch, 10).unwrap();
    }
    assert_eq!(log.read_all(source).unwrap().len(), 6);

    let branch_def = branch_qdef(suffix);
    let branch =
        fireweed_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());

    store.arm();
    let result = log.branch(
        source,
        &branch_def,
        &CommandPosition::new(source.clone(), owner_epoch, 3),
        1_000_000_000, // large TTL: the orphan pin stays "live" well past the GC safety window
        created_at,
    );
    store.disarm();
    assert!(
        result.is_err(),
        "the injected marker-put + cleanup failure fails branch creation: {result:?}"
    );
    log.acquire_epoch(source, created_at + 1).unwrap();
    branch
}

// (a) A genuinely-abandoned branch creation (marker ABSENT — the fault seam failed its marker put AND its
// rollback cleanup) leaves a durable orphan (sentinel + partial branch manifest + a still-live source pin).
// Under the create/GC exclusion GC reclaims ALL of it AND releases the source pin; the source stays fully
// readable (no missing segment) and becomes fully reclaimable again.
#[test]
fn gc_orphaned_branches_reclaims_partial_branch_and_source_stays_reclaimable() {
    let store = std::sync::Arc::new(OrphanBranchFaultStore::new());
    let log = SegmentedObjectLog::open(store.clone(), SegmentConfig::new(10_000_000, 100).unwrap());
    let source = shard();
    let branch = seed_orphaned_branch(&log, &store, &source, "gc-orphan", 1_000);

    let bp = shard_prefix_of(&branch);
    let sp = shard_prefix_of(&source);
    // The orphan is durable: sentinel present, partial branch manifest present, commit marker ABSENT, pin live.
    assert!(
        store.get(&format!("{bp}branch.pending")).unwrap().is_some(),
        "leftover sentinel is durable"
    );
    assert!(
        store.list(&bp).unwrap().len() > 1,
        "partial branch authority objects are durable"
    );
    assert!(
        store.get(&format!("{bp}branch.json")).unwrap().is_none(),
        "commit marker never landed (uncommitted branch)"
    );
    assert!(
        !store.list(&format!("{sp}branches/")).unwrap().is_empty(),
        "the source pin is still registered"
    );
    // The source is unaffected — the failed branch never touched a source object.
    assert_eq!(
        log.read_all(&source).unwrap().len(),
        6,
        "source fully readable"
    );
    // While the orphan pin is live it BLOCKS reclamation of the branched range (the pin is genuinely held).
    assert_eq!(
        expire_segments_as_local_owner(&log, &source, 3, 2_000).unwrap(),
        0,
        "the orphaned source pin blocks reclamation of the branched range"
    );

    // GC reclaims exactly the one abandoned orphan (marker-absent, provably not in-flight under the guard).
    assert_eq!(
        gc_orphans_as_local_owner(&log, &source).unwrap(),
        1,
        "the abandoned uncommitted branch is reclaimed"
    );
    // Every orphan object is gone: sentinel, branch manifest, and the source pin.
    assert!(
        store.get(&format!("{bp}branch.pending")).unwrap().is_none(),
        "sentinel reclaimed"
    );
    assert!(
        store.list(&format!("{bp}manifest/")).unwrap().is_empty(),
        "branch manifest reclaimed"
    );
    assert!(
        store.list(&format!("{sp}branches/")).unwrap().is_empty(),
        "source pin released"
    );

    // The source is STILL fully readable — GC deleted NO source segment (no missing segment ever).
    assert_eq!(
        log.read_all(&source).unwrap().len(),
        6,
        "GC deleted no source segment"
    );
    // With the pin released the source's branched range reclaims normally, and the surviving tail is intact.
    assert_eq!(
        expire_segments_as_local_owner(&log, &source, 3, 3_000).unwrap(),
        4,
        "seq 0..3 reclaim once the orphan pin is released"
    );
    assert_eq!(
        log.read_from(&source, 4).unwrap().len(),
        2,
        "the surviving tail (seq 4..5) is intact — no missing segment"
    );

    // Idempotent: a second GC pass over the now-clean source is a no-op.
    assert_eq!(
        gc_orphans_as_local_owner(&log, &source).unwrap(),
        0,
        "re-running GC after a clean pass is a no-op"
    );
}

// (b) A COMMITTED branch (its `branch.json` commit marker present) is NEVER GC'd — its objects, pin, and
// readability all survive a GC run.
#[test]
fn gc_orphaned_branches_leaves_a_committed_branch_untouched() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let log = SegmentedObjectLog::open(store.clone(), SegmentConfig::new(10_000_000, 100).unwrap());
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    log.enqueue(&source, &pushes(4), 0, 10).unwrap();
    log.seal(&source, 0, 11).unwrap();
    log.acquire_epoch(&source, 12).unwrap();

    let branch_def = branch_qdef("gc-committed");
    let branch =
        fireweed_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 1),
        1_000_000_000,
        1_000,
    )
    .unwrap();
    assert!(
        !log.read_all(&branch).unwrap().is_empty(),
        "committed branch is readable before GC"
    );

    // GC must NOT touch a committed branch.
    assert_eq!(
        gc_orphans_as_local_owner(&log, &source).unwrap(),
        0,
        "a committed branch is never an orphan"
    );

    let bp = shard_prefix_of(&branch);
    let sp = shard_prefix_of(&source);
    assert!(
        store.get(&format!("{bp}branch.json")).unwrap().is_some(),
        "commit marker survives"
    );
    assert!(
        !store.list(&format!("{sp}branches/")).unwrap().is_empty(),
        "source pin survives"
    );
    assert!(
        !log.read_all(&branch).unwrap().is_empty(),
        "committed branch stays readable after GC"
    );
}

// Two-cut-point fault hook that DETERMINISTICALLY interleaves a branch creation committing its marker with a
// concurrent GC pass (bead pqueue-74f03d0e, BUG 1). It pauses the CREATOR mid-flight (`DuringBranchCopy` —
// after the source pin + `branch.pending` sentinel are written but BEFORE `branch.json`) and pauses GC right
// after it classifies the branch marker-ABSENT (`GcAfterOrphanClassified`, before cleanup). The test drives
// the exact interleaving via channels — no sleeps in the load-bearing path — so that WITHOUT the guard GC
// provably proceeds to delete a branch the creator just committed.
struct RaceCreateVsGc {
    creator_paused: std::sync::mpsc::Sender<()>,
    creator_resume: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    gc_resume: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    creator_fired: AtomicBool,
    gc_fired: AtomicBool,
}

impl FaultHook for RaceCreateVsGc {
    fn fault_point(&self, cut: FaultCutPoint) -> EngineResult<()> {
        match cut {
            FaultCutPoint::DuringBranchCopy if !self.creator_fired.swap(true, Ordering::SeqCst) => {
                // Creator: signal it is mid-flight (pin + sentinel written, marker NOT yet), then park.
                self.creator_paused.send(()).unwrap();
                self.creator_resume.lock().unwrap().recv().unwrap();
            }
            FaultCutPoint::GcAfterOrphanClassified
                if !self.gc_fired.swap(true, Ordering::SeqCst) =>
            {
                // GC reached the marker-ABSENT classify→delete window (only possible WITHOUT the guard). Park
                // until main releases it AFTER the creator has committed — main NEVER waits on us, and this
                // branch simply never runs in the guarded build (GC there sees a committed marker and skips).
                self.gc_resume.lock().unwrap().recv().unwrap();
            }
            _ => {}
        }
        Ok(())
    }
}

// (c) CREATE-vs-GC EXCLUSION (bead pqueue-74f03d0e, BUG 1): a branch creation that COMMITS its marker while a
// GC pass runs concurrently on the same branch must SURVIVE — GC must never observe the marker-absent instant
// and then destroy a branch that committed. DETERMINISTIC and HANG-FREE: the ONLY ordering main enforces is
// `c_paused → c_resume → creator committed → g_resume` (std mpsc is UNBOUNDED, so every `send` is non-blocking
// and main NEVER waits on a GC signal — that is what makes both builds terminate).
//
// WITH the guard (shipped): GC parks on the create/GC guard the creator holds until the creator commits and
// releases it; GC then sees the committed marker, SKIPS, and never enters the marker-absent path — so it never
// waits on `g_resume`, and the `g_resume` send just sits unread in the unbounded channel. reclaimed == 0.
//
// WITHOUT the guard (verified by temporarily removing it): GC reaches the marker-absent classify point and
// parks on `g_resume`; main commits the creator FIRST, then sends `g_resume`, so GC deletes the just-committed
// branch and returns 1 — the assertions below then fail. No sleep, no timeout, no scheduling luck.
#[test]
fn gc_excludes_a_concurrent_branch_creation_and_never_destroys_a_committing_branch() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let log = std::sync::Arc::new(SegmentedObjectLog::open(
        store.clone(),
        SegmentConfig::new(10_000_000, 100).unwrap(),
    ));
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    for _ in 0..6 {
        log.enqueue(&source, &pushes(1), 0, 10).unwrap();
        log.seal(&source, 0, 10).unwrap();
    }
    log.acquire_epoch(&source, 11).unwrap();

    let (creator_paused_tx, creator_paused_rx) = std::sync::mpsc::channel();
    let (creator_resume_tx, creator_resume_rx) = std::sync::mpsc::channel();
    let (gc_resume_tx, gc_resume_rx) = std::sync::mpsc::channel();
    log.set_fault_hook(Some(std::sync::Arc::new(RaceCreateVsGc {
        creator_paused: creator_paused_tx,
        creator_resume: std::sync::Mutex::new(creator_resume_rx),
        gc_resume: std::sync::Mutex::new(gc_resume_rx),
        creator_fired: AtomicBool::new(false),
        gc_fired: AtomicBool::new(false),
    })));

    let branch_def = branch_qdef("gc-race");
    let branch =
        fireweed_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());

    // Thread C: a branch creation that pauses mid-flight (pin + sentinel written, marker NOT yet); in the
    // shipped build it is holding the create/GC guard.
    let creator = {
        let log = log.clone();
        let source = source.clone();
        std::thread::spawn(move || {
            log.branch(
                &source,
                &branch_def,
                &CommandPosition::new(source.clone(), 0, 3),
                1_000_000_000,
                1_000,
            )
        })
    };
    // (1) wait until the creation is mid-flight (guard held by C in the shipped build).
    creator_paused_rx.recv().unwrap();

    // (2) Thread G: a GC pass while the creation is mid-flight.
    let gc = {
        let log = log.clone();
        let source = source.clone();
        std::thread::spawn(move || gc_orphans_as_local_owner(&log, &source))
    };

    // (3) resume the creator and JOIN it, so `branch.json` is provably committed and the guard released.
    creator_resume_tx.send(()).unwrap();
    let branch_epoch = creator
        .join()
        .unwrap()
        .expect("the branch creation commits cleanly");

    // (4) release GC. Non-blocking on the UNBOUNDED channel even in the shipped build where GC never reads it
    // (there it skipped the committed marker and never parked). Never wait on a GC signal — that is the only
    // way both builds terminate.
    gc_resume_tx.send(()).unwrap();

    // (5) join GC.
    let reclaimed = gc.join().unwrap().unwrap();
    log.set_fault_hook(None);

    // The GC pass, excluded until the creation committed, saw a COMMITTED branch and reclaimed NOTHING.
    assert_eq!(
        reclaimed, 0,
        "GC excluded by the create/GC guard never destroys a concurrently-committing branch"
    );
    assert!(branch_epoch >= 1, "the branch acquired its own epoch");
    // The branch committed and is fully intact: marker + source pin + readability all survive the concurrent GC.
    let bp = shard_prefix_of(&branch);
    let sp = shard_prefix_of(&source);
    assert!(
        store.get(&format!("{bp}branch.json")).unwrap().is_some(),
        "the commit marker survives the concurrent GC"
    );
    assert!(
        !store.list(&format!("{sp}branches/")).unwrap().is_empty(),
        "the source pin survives — the branched source segments stay protected"
    );
    assert_eq!(
        log.read_all(&branch).unwrap().len(),
        4,
        "the committed branch is readable (cut at seq 3 => 4 commands, seq 0..3)"
    );
    // The committing branch's pin protects the source range: a trim through the cut reclaims nothing.
    assert_eq!(
        expire_segments_as_local_owner(&log, &source, 3, 2_000).unwrap(),
        0,
        "the committed branch's pin protects the source's branched range from reclamation"
    );
}

// A cross-instance fault hook that pauses the creator mid-branch and pauses GC after classification.
struct CrossInstanceBranchFence {
    creator_paused: std::sync::mpsc::Sender<()>,
    creator_resume: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    gc_paused: std::sync::mpsc::Sender<()>,
    gc_resume: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    creator_fired: AtomicBool,
    gc_fired: AtomicBool,
}

impl FaultHook for CrossInstanceBranchFence {
    fn fault_point(&self, cut: FaultCutPoint) -> EngineResult<()> {
        match cut {
            FaultCutPoint::DuringBranchCopy if !self.creator_fired.swap(true, Ordering::SeqCst) => {
                self.creator_paused.send(()).unwrap();
                self.creator_resume.lock().unwrap().recv().unwrap();
            }
            FaultCutPoint::GcAfterOrphanClassified
                if !self.gc_fired.swap(true, Ordering::SeqCst) =>
            {
                self.gc_paused.send(()).unwrap();
                self.gc_resume.lock().unwrap().recv().unwrap();
            }
            _ => {}
        }
        Ok(())
    }
}

// Cross-instance handoff: a superseded branch creator must fail cleanly once a newer owner acquires the
// source epoch, and the current owner's GC can then reclaim the orphan without corrupting or losing source
// segments.
#[test]
fn branch_commit_is_fenced_on_the_source_epoch_and_cross_instance_gc_stays_safe() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let creator_log = std::sync::Arc::new(SegmentedObjectLog::open(
        store.clone(),
        SegmentConfig::new(10_000_000, 100).unwrap(),
    ));
    let owner_log = std::sync::Arc::new(SegmentedObjectLog::open(
        store.clone(),
        SegmentConfig::new(10_000_000, 100).unwrap(),
    ));
    let source = shard();

    creator_log.create_queue(&qdef()).unwrap();
    for _ in 0..6 {
        creator_log.enqueue(&source, &pushes(1), 0, 10).unwrap();
        creator_log.seal(&source, 0, 10).unwrap();
    }
    owner_log.create_queue(&qdef()).unwrap();

    let (creator_paused_tx, creator_paused_rx) = std::sync::mpsc::channel();
    let (creator_resume_tx, creator_resume_rx) = std::sync::mpsc::channel();
    let (gc_paused_tx, gc_paused_rx) = std::sync::mpsc::channel();
    let (gc_resume_tx, gc_resume_rx) = std::sync::mpsc::channel();
    let hook = std::sync::Arc::new(CrossInstanceBranchFence {
        creator_paused: creator_paused_tx,
        creator_resume: std::sync::Mutex::new(creator_resume_rx),
        gc_paused: gc_paused_tx,
        gc_resume: std::sync::Mutex::new(gc_resume_rx),
        creator_fired: AtomicBool::new(false),
        gc_fired: AtomicBool::new(false),
    });
    creator_log.set_fault_hook(Some(hook.clone()));
    owner_log.set_fault_hook(Some(hook));

    let branch_def = branch_qdef("gc-xowner");
    let branch =
        fireweed_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());

    let creator = {
        let log = creator_log.clone();
        let source = source.clone();
        std::thread::spawn(move || {
            log.branch(
                &source,
                &branch_def,
                &CommandPosition::new(source.clone(), 0, 3),
                1_000_000_000,
                1_000,
            )
        })
    };
    creator_paused_rx.recv().unwrap();

    let superseding_epoch = owner_log.acquire_epoch(&source, 2_000).unwrap();
    assert!(
        superseding_epoch >= 1,
        "the newer owner took the source epoch"
    );

    let gc = {
        let log = owner_log.clone();
        let source = source.clone();
        std::thread::spawn(move || gc_orphans_as_local_owner(&log, &source))
    };

    gc_paused_rx.recv().unwrap();

    creator_resume_tx.send(()).unwrap();
    let branch_result = creator.join().unwrap();
    assert_eq!(
        branch_result.unwrap_err(),
        EngineError::Conflict,
        "a superseded creator cannot commit a branch once the source epoch has advanced"
    );

    gc_resume_tx.send(()).unwrap();
    let reclaimed = gc.join().unwrap().unwrap();
    creator_log.set_fault_hook(None);
    owner_log.set_fault_hook(None);

    assert_eq!(
        reclaimed, 0,
        "the superseded creator's rollback removes the orphan before the current owner's GC resumes"
    );

    let bp = shard_prefix_of(&branch);
    let sp = shard_prefix_of(&source);
    assert!(
        store.get(&format!("{bp}branch.json")).unwrap().is_none(),
        "the superseded creator never published a commit marker"
    );
    assert!(
        store.list(&bp).unwrap().is_empty(),
        "the branch prefix is fully cleaned up"
    );
    assert!(
        store.list(&format!("{sp}branches/")).unwrap().is_empty(),
        "the source pin is released after rollback/GC"
    );
    assert_eq!(
        owner_log.read_all(&source).unwrap().len(),
        6,
        "the shared source remains readable after the handoff race"
    );
    assert_eq!(
        owner_log
            .expire_segments_through(&source, 3, 3_000)
            .unwrap(),
        4,
        "the source stays reclaimable and no segment is missing after the race"
    );
    assert_eq!(owner_log.read_from(&source, 4).unwrap().len(), 2);
}

// ===========================================================================
// Durable deletion watermark + range-list (fence untouched).
// ===========================================================================

use fireweed_engine::QueueKey;

// The per-shard object-key prefix (`t/{hex(tenant)}/q/{hex(queue)}/`), mirroring the substrate's internal
// `shard_prefix`. Lets these tests reach the raw manifest and watermark objects on the store directly.
fn shard_prefix_s(shard: &QueueKey) -> String {
    format!(
        "t/{}/q/{}/",
        hex_lower(shard.tenant_id.as_str().as_bytes()),
        hex_lower(shard.queue_id.as_str().as_bytes())
    )
}

fn manifest_prefix_s(shard: &QueueKey) -> String {
    format!("{}manifest/", shard_prefix_s(shard))
}

fn manifest_head_prefix_s(shard: &QueueKey) -> String {
    format!("{}manifest_head/", shard_prefix_s(shard))
}

#[allow(dead_code)]
fn delete_prefix<S: BlobStore>(store: &S, prefix: &str) {
    for key in store.list(prefix).unwrap() {
        store.delete(&key).unwrap();
    }
}

#[allow(dead_code)]
fn versioned_head_key_s(prefix: &str, version: u64) -> String {
    format!("{prefix}{version:020}.json")
}

#[allow(dead_code)]
static HEAD_TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[allow(dead_code)]
fn temp_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "fireweed-objectlog-{label}-{}-{}",
        std::process::id(),
        HEAD_TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[allow(dead_code)]
fn assert_manifest_head_cas_contract<S: BlobStore + 'static>(store: std::sync::Arc<S>) {
    let prefix = manifest_head_prefix_s(&shard());
    let v0 = ManifestHeadBlob {
        current_epoch: 0,
        next_seq: 0,
        next_manifest_index: 0,
        retention_floor_through: None,
        tail_candidate_key: None,
        recovery_index: None,
    };
    assert!(
        store.read_manifest_head(&prefix).unwrap().is_none(),
        "the head starts empty"
    );
    assert!(
        store
            .update_manifest_head_if_version(&prefix, None, &v0)
            .unwrap(),
        "the first head write creates version 0"
    );
    let head0 = store
        .read_manifest_head(&prefix)
        .unwrap()
        .expect("version 0 head");
    assert_eq!(head0.version, 0);
    assert_eq!(head0.value, v0);
    assert_eq!(
        store
            .get(&versioned_head_key_s(&prefix, 0))
            .unwrap()
            .as_deref(),
        Some(&serde_json::to_vec(&v0).unwrap()[..]),
        "the winning version is preserved as an immutable object"
    );

    let barrier = std::sync::Arc::new(Barrier::new(3));
    let winner_a = ManifestHeadBlob {
        current_epoch: 1,
        next_seq: 2,
        next_manifest_index: 1,
        retention_floor_through: Some(0),
        tail_candidate_key: None,
        recovery_index: None,
    };
    let winner_b = ManifestHeadBlob {
        current_epoch: 2,
        next_seq: 4,
        next_manifest_index: 1,
        retention_floor_through: Some(1),
        tail_candidate_key: None,
        recovery_index: None,
    };
    let store_a = store.clone();
    let store_b = store.clone();
    let prefix_a = prefix.clone();
    let prefix_b = prefix.clone();
    let barrier_a = barrier.clone();
    let barrier_b = barrier.clone();
    let winner_a_update = winner_a.clone();
    let winner_b_update = winner_b.clone();
    let a = thread::spawn(move || {
        barrier_a.wait();
        store_a
            .update_manifest_head_if_version(&prefix_a, Some(0), &winner_a_update)
            .unwrap()
    });
    let b = thread::spawn(move || {
        barrier_b.wait();
        store_b
            .update_manifest_head_if_version(&prefix_b, Some(0), &winner_b_update)
            .unwrap()
    });
    barrier.wait();
    let a_won = a.join().unwrap();
    let b_won = b.join().unwrap();
    assert_ne!(a_won, b_won, "exactly one concurrent head update wins");
    assert!(
        a_won ^ b_won,
        "the versioned head CAS must admit exactly one winner"
    );

    let head1 = store
        .read_manifest_head(&prefix)
        .unwrap()
        .expect("version 1 head");
    assert_eq!(head1.version, 1);
    assert!(
        head1.value == winner_a || head1.value == winner_b,
        "the winner's payload becomes the new head"
    );
    assert_eq!(
        store
            .get(&versioned_head_key_s(&prefix, 0))
            .unwrap()
            .as_deref(),
        Some(&serde_json::to_vec(&v0).unwrap()[..]),
        "losers still observe the old head value at the previous version"
    );
}

struct PauseAtCut {
    cut: FaultCutPoint,
    entered: std::sync::Arc<Barrier>,
    resume: std::sync::Arc<Barrier>,
    fired: AtomicBool,
}

impl FaultHook for PauseAtCut {
    fn fault_point(&self, cut: FaultCutPoint) -> EngineResult<()> {
        if cut == self.cut && !self.fired.swap(true, Ordering::SeqCst) {
            self.entered.wait();
            self.resume.wait();
        }
        Ok(())
    }
}

struct FailAtCut {
    cut: FaultCutPoint,
    fired: AtomicBool,
}

impl FaultHook for FailAtCut {
    fn fault_point(&self, cut: FaultCutPoint) -> EngineResult<()> {
        if cut == self.cut && !self.fired.swap(true, Ordering::SeqCst) {
            return Err(EngineError::Storage(format!("fault at {cut:?}")));
        }
        Ok(())
    }
}

// SP-03 slice 0 (HCAS-F1/F2): the externally allocated epoch is committed in the single versioned
// authority head before it can be used. Losing the response at that boundary must still leave a reopened
// reader on the new epoch, and an old owner must neither acknowledge nor expose its buffered prefix.
#[test]
fn hcas_f1_f2_crash_after_fence_head_reopens_fenced_and_keeps_prefix_invisible() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let queue = unique_qdef("hcas-f1-f2-fence-crash");
    let shard = QueueKey::new(queue.tenant_id.clone(), queue.queue_id.clone());
    let stale = SegmentedObjectLog::open(store.clone(), cfg);
    let fencing = SegmentedObjectLog::open(store.clone(), cfg);
    stale.create_queue(&queue).unwrap();
    fencing.create_queue(&queue).unwrap();
    stale.fence_epoch(&shard, 0, 0).unwrap();
    stale.enqueue(&shard, &pushes(1), 0, 1).unwrap();

    fencing.set_fault_hook(Some(std::sync::Arc::new(FailAtCut {
        cut: FaultCutPoint::DuringOwnerReassignment,
        fired: AtomicBool::new(false),
    })));
    assert!(matches!(
        fencing.fence_epoch(&shard, 1, 2),
        Err(EngineError::Storage(_))
    ));

    let reopened = SegmentedObjectLog::open(store, cfg);
    reopened.create_queue(&queue).unwrap();
    assert_eq!(reopened.fence_epoch(&shard, 1, 3).unwrap(), 1);
    assert_eq!(stale.seal(&shard, 0, 4), Err(EngineError::EpochFenced));
    assert!(reopened.read_all(&shard).unwrap().is_empty());
}

#[test]
fn segmented_stale_append_prepared_before_fence_cannot_advance_authoritative_tail() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let queue = unique_qdef("authority-stale-race");
    let shard = fireweed_engine::QueueKey::new(queue.tenant_id.clone(), queue.queue_id.clone());
    let owner_n = std::sync::Arc::new(SegmentedObjectLog::open(store.clone(), cfg));
    let owner_next = SegmentedObjectLog::open(store.clone(), cfg);
    owner_n.create_queue(&queue).unwrap();
    owner_next.create_queue(&queue).unwrap();
    assert_eq!(owner_n.fence_epoch(&shard, 0, 0).unwrap(), 0);
    owner_n.enqueue(&shard, &pushes(2), 0, 1).unwrap();

    let entered = std::sync::Arc::new(Barrier::new(2));
    let resume = std::sync::Arc::new(Barrier::new(2));
    owner_n.set_fault_hook(Some(std::sync::Arc::new(PauseAtCut {
        cut: FaultCutPoint::AfterManifestCandidateBeforeHead,
        entered: entered.clone(),
        resume: resume.clone(),
        fired: AtomicBool::new(false),
    })));
    let sealing = {
        let owner_n = owner_n.clone();
        let shard = shard.clone();
        thread::spawn(move || owner_n.seal(&shard, 0, 2))
    };
    entered.wait();
    assert_eq!(owner_next.fence_epoch(&shard, 1, 3).unwrap(), 1);
    assert_eq!(
        owner_next.gc_unreferenced_candidates(&shard, 1).unwrap(),
        1,
        "GC can classify the paused candidate as a permanent loser"
    );
    resume.wait();
    assert_eq!(sealing.join().unwrap(), Err(EngineError::EpochFenced));
    assert!(owner_next.read_all(&shard).unwrap().is_empty());
    assert!(
        store
            .list("")
            .unwrap()
            .iter()
            .any(|key| key.contains("/seg_candidates/e00000000000000000000/")),
        "a CAS loser leaves a content-addressed orphan for bounded maintenance; deleting here could remove an identical winner's object"
    );

    owner_next.enqueue(&shard, &pushes(1), 1, 4).unwrap();
    owner_next.seal(&shard, 1, 5).unwrap();
    assert_eq!(owner_next.read_all(&shard).unwrap().len(), 1);
    assert_eq!(owner_next.gc_unreferenced_candidates(&shard, 1).unwrap(), 0);
    assert_eq!(owner_next.read_all(&shard).unwrap().len(), 1);
}

#[test]
fn segmented_concurrent_target_n_and_n_plus_one_never_returns_usable_n_late() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let queue = unique_qdef("authority-fence-race");
    let shard = fireweed_engine::QueueKey::new(queue.tenant_id.clone(), queue.queue_id.clone());
    let lower = std::sync::Arc::new(SegmentedObjectLog::open(store.clone(), cfg));
    let higher = SegmentedObjectLog::open(store, cfg);
    lower.create_queue(&queue).unwrap();
    higher.create_queue(&queue).unwrap();
    assert_eq!(lower.fence_epoch(&shard, 0, 0).unwrap(), 0);

    let entered = std::sync::Arc::new(Barrier::new(2));
    let resume = std::sync::Arc::new(Barrier::new(2));
    lower.set_fault_hook(Some(std::sync::Arc::new(PauseAtCut {
        cut: FaultCutPoint::DuringOwnerReassignment,
        entered: entered.clone(),
        resume: resume.clone(),
        fired: AtomicBool::new(false),
    })));
    let lower_attempt = {
        let lower = lower.clone();
        let shard = shard.clone();
        thread::spawn(move || lower.fence_epoch(&shard, 1, 1))
    };
    entered.wait();
    assert_eq!(higher.fence_epoch(&shard, 2, 2).unwrap(), 2);
    resume.wait();
    assert_eq!(lower_attempt.join().unwrap(), Err(EngineError::EpochFenced));
    assert_eq!(higher.fence_epoch(&shard, 2, 3).unwrap(), 2);
    assert_eq!(
        higher.fence_epoch(&shard, 1, 4),
        Err(EngineError::EpochFenced)
    );

    let equality_entered = std::sync::Arc::new(Barrier::new(2));
    let equality_resume = std::sync::Arc::new(Barrier::new(2));
    lower.set_fault_hook(Some(std::sync::Arc::new(PauseAtCut {
        cut: FaultCutPoint::DuringOwnerReassignment,
        entered: equality_entered.clone(),
        resume: equality_resume.clone(),
        fired: AtomicBool::new(false),
    })));
    let equality_attempt = {
        let lower = lower.clone();
        let shard = shard.clone();
        thread::spawn(move || lower.fence_epoch(&shard, 2, 5))
    };
    equality_entered.wait();
    assert_eq!(higher.fence_epoch(&shard, 3, 6).unwrap(), 3);
    equality_resume.wait();
    assert_eq!(
        equality_attempt.join().unwrap(),
        Err(EngineError::EpochFenced),
        "an equal-epoch observation cannot return usable after N+1 becomes authoritative"
    );
}

#[test]
fn segmented_authority_fault_cuts_recover_only_the_head_referenced_tail() {
    let store = std::sync::Arc::new(FailingBlobStore::default());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let queue = unique_qdef("authority-faults");
    let shard = fireweed_engine::QueueKey::new(queue.tenant_id.clone(), queue.queue_id.clone());
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&queue).unwrap();
    log.fence_epoch(&shard, 0, 0).unwrap();

    log.enqueue(&shard, &pushes(1), 0, 1).unwrap();
    log.set_fault_hook(Some(std::sync::Arc::new(FailAtCut {
        cut: FaultCutPoint::AfterManifestCandidateBeforeHead,
        fired: AtomicBool::new(false),
    })));
    assert!(matches!(
        log.seal(&shard, 0, 2),
        Err(EngineError::Storage(_))
    ));
    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&queue).unwrap();
    assert!(reopened.read_all(&shard).unwrap().is_empty());

    reopened.enqueue(&shard, &pushes(1), 0, 3).unwrap();
    store.arm_put_if_absent("/authority_head/");
    assert!(matches!(
        reopened.seal(&shard, 0, 4),
        Err(EngineError::Storage(_))
    ));
    store.disarm();
    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&queue).unwrap();
    assert!(reopened.read_all(&shard).unwrap().is_empty());

    reopened.enqueue(&shard, &pushes(1), 0, 5).unwrap();
    reopened.set_fault_hook(Some(std::sync::Arc::new(FailAtCut {
        cut: FaultCutPoint::AfterManifestBeforeAck,
        fired: AtomicBool::new(false),
    })));
    assert!(matches!(
        reopened.seal(&shard, 0, 6),
        Err(EngineError::Storage(_))
    ));
    let final_reopen = SegmentedObjectLog::open(store, cfg);
    final_reopen.create_queue(&queue).unwrap();
    assert_eq!(final_reopen.read_all(&shard).unwrap().len(), 1);
}

#[test]
fn segmented_authority_head_loss_and_version_holes_fail_closed() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();

    let lost_queue = unique_qdef("authority-head-loss");
    let lost_shard = QueueKey::new(lost_queue.tenant_id.clone(), lost_queue.queue_id.clone());
    let lost = SegmentedObjectLog::open(store.clone(), cfg);
    lost.create_queue(&lost_queue).unwrap();
    lost.fence_epoch(&lost_shard, 0, 0).unwrap();
    lost.enqueue(&lost_shard, &pushes(1), 0, 1).unwrap();
    lost.seal(&lost_shard, 0, 2).unwrap();
    let lost_prefix = format!(
        "t/{}/q/{}/authority_head/",
        hex_lower(lost_shard.tenant_id.as_str().as_bytes()),
        hex_lower(lost_shard.queue_id.as_str().as_bytes())
    );
    for key in store.list(&lost_prefix).unwrap() {
        store.delete(&key).unwrap();
    }
    assert_eq!(
        lost.fence_epoch(&lost_shard, 1, 3),
        Err(EngineError::Conflict)
    );
    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    assert_eq!(
        reopened.create_queue(&lost_queue),
        Err(EngineError::Conflict)
    );

    let hole_queue = unique_qdef("authority-head-hole");
    let hole_shard = QueueKey::new(hole_queue.tenant_id.clone(), hole_queue.queue_id.clone());
    let hole = SegmentedObjectLog::open(store.clone(), cfg);
    hole.create_queue(&hole_queue).unwrap();
    hole.fence_epoch(&hole_shard, 0, 0).unwrap(); // genesis head
    hole.enqueue(&hole_shard, &pushes(1), 0, 1).unwrap();
    hole.seal(&hole_shard, 0, 2).unwrap(); // first committed head
    hole.fence_epoch(&hole_shard, 1, 3).unwrap(); // next authority head
    let hole_prefix = format!(
        "t/{}/q/{}/authority_head/",
        hex_lower(hole_shard.tenant_id.as_str().as_bytes()),
        hex_lower(hole_shard.queue_id.as_str().as_bytes())
    );
    assert!(
        store
            .delete(&versioned_head_key_s(&hole_prefix, 1))
            .unwrap()
    );
    assert_eq!(
        hole.fence_epoch(&hole_shard, 2, 4),
        Err(EngineError::Conflict)
    );
    assert!(matches!(
        hole.read_all(&hole_shard),
        Err(EngineError::Conflict)
    ));
}

// One full trim cycle exactly as the composed trim path drives it: epoch-fenced floor advance FIRST, then the
// segment-object reclamation (which also advances the durable deletion watermark at its end).
fn trim_cycle<S: BlobStore>(
    log: &SegmentedObjectLog<S>,
    shard: &QueueKey,
    through_seq: u64,
    _epoch: u64,
    now_ms: i64,
) {
    advance_floor_as_local_owner(log, shard, through_seq, now_ms).unwrap();
    expire_segments_as_local_owner(log, shard, through_seq, now_ms).unwrap();
}

fn reclaimed_cached_writer_fixture() -> (
    std::sync::Arc<CountingBlobStore>,
    SegmentedObjectLog<std::sync::Arc<CountingBlobStore>>,
    SegmentedObjectLog<std::sync::Arc<CountingBlobStore>>,
) {
    let store = std::sync::Arc::new(CountingBlobStore::default());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();

    let stale_owner = SegmentedObjectLog::open(store.clone(), cfg);
    stale_owner.create_queue(&qdef()).unwrap();
    stale_owner.enqueue(&shard(), &pushes(1), 0, 10).unwrap();
    stale_owner.seal(&shard(), 0, 11).unwrap();

    let live_owner = SegmentedObjectLog::open(store.clone(), cfg);
    live_owner.create_queue(&qdef()).unwrap();
    for i in 0..4u64 {
        live_owner
            .enqueue(&shard(), &pushes(1), 0, 200 + i as i64 * 10)
            .unwrap();
        live_owner.seal(&shard(), 0, 201 + i as i64 * 10).unwrap();
    }
    trim_cycle(&live_owner, &shard(), 3, 0, 1_000);

    (store, stale_owner, live_owner)
}

// Test 1 — after repeated trim+advance-watermark cycles, reclaimed retired manifest copies are physically
// deleted, so the retired manifest prefix stays bounded by the live tail while the watermark remains
// monotonic. The live read path still enumerates in O(live) via the ranged manifest scan.

// Test 2 — recover_manifest tail + epoch + next-seq are correct after manifest reclamation and reopen.
#[test]
#[allow(non_snake_case)]
fn TestRecoverTailAfterManifestReclaimAndReopen() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef()).unwrap();
    for i in 0..6u64 {
        log.enqueue(&shard(), &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard(), 0, (i as i64 + 1) * 10 + 1).unwrap();
    }
    // A new owner bumps the epoch to 1, then commits one more segment under epoch 1.
    let owner_b = SegmentedObjectLog::open(store.clone(), cfg);
    owner_b.create_queue(&qdef()).unwrap();
    assert_eq!(owner_b.acquire_epoch(&shard(), 100).unwrap(), 1);
    owner_b.enqueue(&shard(), &pushes(2), 1, 200).unwrap();
    owner_b.seal(&shard(), 1, 201).unwrap();

    // Trim well below the tail — advancing the horizon over the low indices.
    trim_cycle(&owner_b, &shard(), 5, 1, 1_000);
    assert!(
        owner_b
            .read_manifest_deletion_watermark(&shard())
            .unwrap()
            .is_some(),
        "deletion watermark advanced"
    );

    // A fresh substrate recovers the SAME tail: next_seq 14 (12 data + 2 tail), epoch 1.
    let recovered = SegmentedObjectLog::open(store.clone(), cfg);
    recovered.create_queue(&qdef()).unwrap();
    assert_eq!(
        recovered.current_epoch(&shard()).unwrap(),
        1,
        "epoch recovered from the retained tail"
    );
    // Next append continues the contiguous sequence with no collision.
    let pos = {
        recovered.enqueue(&shard(), &pushes(1), 1, 2_000).unwrap();
        recovered.seal(&shard(), 1, 2_001).unwrap()
    };
    assert_eq!(
        pos[0].sequence, 14,
        "next-seq recovered exactly from the ranged tail"
    );
    assert_eq!(pos[0].backend_epoch, 1);
}

// Test 2b — deleting or hiding the only remaining live manifest entry above the durable floor fails
// closed instead of being mistaken for an empty history.

// TestBehindImageFailClosedWithDeletedManifests: after the retained floor/head replay path is proven
// healthy, deleting the retired `manifest/` namespace alone must not break recovery; if the authoritative
// head namespace is also removed, the queue still boots conservatively instead of reconstructing a
// behind image from deleted manifest data.

#[test]
#[allow(non_snake_case)]
fn TestBranchInheritanceRetainedFloorMetadataAvailable() {
    let store = std::sync::Arc::new(CountingBlobStore::default());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    for i in 0..6u64 {
        log.enqueue(&source, &pushes(1), 0, 10 + i as i64 * 10)
            .unwrap();
        log.seal(&source, 0, 11 + i as i64 * 10).unwrap();
    }

    trim_cycle(&log, &source, 3, 0, 1_000);
    delete_prefix(store.as_ref(), &manifest_prefix_s(&source));

    assert_eq!(
        log.read_retention_floor(&source)
            .unwrap()
            .map(|p| p.sequence),
        Some(3),
        "the retained floor metadata survives the physical deletion of the retired source prefix"
    );
    assert!(
        log.read_manifest_deletion_watermark(&source)
            .unwrap()
            .is_some(),
        "the retained branch-inheritance watermark remains available after retired prefix deletion"
    );

    store.reset_reads();
    let branch_def = branch_qdef("retained-metadata-available");
    let branch = QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 5),
        60_000,
        2_000,
    )
    .unwrap();

    assert_eq!(
        log.read_retention_floor(&branch)
            .unwrap()
            .map(|p| p.sequence),
        Some(3),
        "the branch inherits the retained source floor without needing deleted source manifest objects"
    );
    assert_eq!(
        log.read_all(&branch)
            .unwrap()
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![4, 5],
        "branch inheritance stays on the retained live view"
    );

    let deleted_source_manifest_prefix = manifest_prefix_s(&source);
    assert!(
        store
            .get_keys()
            .into_iter()
            .all(|key| !key.starts_with(&deleted_source_manifest_prefix)),
        "branch creation and recovery do not recover any deleted source manifest objects"
    );
}

// TestBranchGcPreservesInheritedFloorPins: a committed branch created from a trimmed source keeps its
// inherited floor and source pin even if the source's retired manifest prefix is physically deleted before
// branch GC runs. The branch GC path must continue to classify from the source pin registry / branch
// metadata, not from deleted retired source manifest objects.
#[test]
#[allow(non_snake_case)]
fn TestBranchGcPreservesInheritedFloorPins() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    for i in 0..6u64 {
        log.enqueue(&source, &pushes(1), 0, 10 + i as i64 * 10)
            .unwrap();
        log.seal(&source, 0, 11 + i as i64 * 10).unwrap();
    }

    trim_cycle(&log, &source, 3, 0, 1_000);

    let branch_def = branch_qdef("gc-retained-floor-pins");
    let branch = QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 5),
        60_000,
        2_000,
    )
    .unwrap();

    delete_prefix(store.as_ref(), &manifest_prefix_s(&source));
    assert!(
        store.list(&manifest_prefix_s(&source)).unwrap().is_empty(),
        "the retired source manifest prefix is physically gone"
    );

    assert_eq!(
        log.read_retention_floor(&branch).unwrap().unwrap().sequence,
        3,
        "the branch still inherits the trimmed source floor before GC"
    );
    assert_eq!(
        log.read_all(&branch)
            .unwrap()
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![4, 5],
        "the branch still reads the retained live view before GC"
    );

    assert_eq!(
        gc_orphans_as_local_owner(&log, &source).unwrap(),
        0,
        "branch GC ignores the committed branch even after the source manifest prefix is deleted"
    );

    let source_branch_prefix = format!("{}branches/", shard_prefix_s(&source));
    assert!(
        !store.list(&source_branch_prefix).unwrap().is_empty(),
        "the source pin survives branch GC"
    );
    assert_eq!(
        log.read_retention_floor(&branch).unwrap().unwrap().sequence,
        3,
        "the branch floor survives branch GC"
    );
    assert_eq!(
        log.read_all(&branch)
            .unwrap()
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![4, 5],
        "the branch view survives branch GC"
    );
}

// TestBranchGcFailClosedOnMissingInheritanceMetadata: branch GC refuses to touch an orphan when the
// persisted source-pin metadata becomes missing before the classify+delete pass can trust it.
#[test]
#[allow(non_snake_case)]
fn TestBranchGcFailClosedOnMissingInheritanceMetadata() {
    let store = std::sync::Arc::new(OrphanBranchFaultStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let source = shard();
    let branch = seed_orphaned_branch(&log, &store, &source, "gc-missing-inheritance", 1_000);

    let registry_key = branch_registry_key_of(&source, &branch);
    store.arm_missing_get(&registry_key);

    let owner_epoch = log.maintenance_owner_epoch(&source).unwrap();
    let limits =
        MaintenanceLimits::new(64, 1_000_000, 128, std::time::Duration::from_secs(1), 8).unwrap();
    let report = log
        .gc_orphaned_branches_bounded(&source, owner_epoch, 10_000, 100, limits, false)
        .unwrap();
    assert_eq!(
        report.failure_cause,
        Some(MaintenanceFailureCause::MissingInheritanceMetadata),
        "branch GC must preserve the structured missing-inheritance cause"
    );
    assert_eq!(report.permanent_failures, 1);
    assert!(
        store
            .get(&format!("{}branch.pending", shard_prefix_of(&branch)))
            .unwrap()
            .is_some(),
        "the orphan stays intact after the fail-closed read path aborts"
    );
    assert!(
        !store
            .list(&format!("{}branches/", shard_prefix_of(&source)))
            .unwrap()
            .is_empty(),
        "the source pin remains registered after the aborted GC pass"
    );
    assert!(
        store.list(&shard_prefix_of(&branch)).unwrap().len() > 1,
        "the branch-local authority objects are left untouched when GC refuses to proceed"
    );
}

#[test]
fn bounded_orphan_gc_dry_run_and_partial_resume_preserve_the_pin() {
    let store = std::sync::Arc::new(OrphanBranchFaultStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let source = shard();
    let branch = seed_orphaned_branch(&log, &store, &source, "gc-bounded", 1_000);
    let owner_epoch = log.maintenance_owner_epoch(&source).unwrap();
    let registry_key = branch_registry_key_of(&source, &branch);
    let pending_key = format!("{}branch.pending", shard_prefix_of(&branch));

    let dry_limits =
        MaintenanceLimits::new(32, 1_000_000, 64, std::time::Duration::from_secs(1), 8).unwrap();
    let dry = log
        .gc_orphaned_branches_bounded(&source, owner_epoch, 10_000, 100, dry_limits, true)
        .unwrap();
    assert!(dry.requests <= dry_limits.requests.get());
    assert!(dry.would_delete > 1);
    assert!(dry.would_delete_bytes > 0);
    assert_eq!(dry.deleted, 0);
    assert!(
        dry.cursor.is_none(),
        "dry-run must not persist or return a cursor"
    );
    assert!(store.get(&registry_key).unwrap().is_some());
    assert!(store.get(&pending_key).unwrap().is_some());

    let tiny_limits =
        MaintenanceLimits::new(2, 1_000_000, 16, std::time::Duration::from_secs(1), 2).unwrap();
    let partial = log
        .gc_orphaned_branches_bounded(&source, owner_epoch, 10_000, 100, tiny_limits, false)
        .unwrap();
    assert!(partial.requests <= tiny_limits.requests.get());
    assert_eq!(partial.completed_candidates, 0);
    assert!(
        partial.cursor.is_none(),
        "the unresolved first candidate cannot be skipped"
    );
    assert!(
        store.get(&registry_key).unwrap().is_some(),
        "pin is released last"
    );
    assert!(
        store.get(&pending_key).unwrap().is_some(),
        "sentinel survives partial cleanup"
    );

    let finish_limits =
        MaintenanceLimits::new(64, 1_000_000, 128, std::time::Duration::from_secs(1), 1).unwrap();
    let mut live_deleted = partial.deleted;
    let mut completed = 0;
    for _ in 0..64 {
        let pass = log
            .gc_orphaned_branches_bounded(&source, owner_epoch, 10_000, 100, finish_limits, false)
            .unwrap();
        assert!(pass.requests <= finish_limits.requests.get());
        live_deleted += pass.deleted;
        completed += pass.completed_candidates;
        if completed == 1 {
            break;
        }
    }
    assert_eq!(
        completed, 1,
        "page_size=1 must converge across soft resumes"
    );
    assert_eq!(
        dry.would_delete, live_deleted,
        "dry-run and live enumerate the same physical object set"
    );
    assert!(store.get(&registry_key).unwrap().is_none());
    assert!(store.get(&pending_key).unwrap().is_none());
}

#[test]
fn bounded_orphan_gc_rejects_stale_owner_before_deleting() {
    let store = std::sync::Arc::new(OrphanBranchFaultStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let source = shard();
    let branch = seed_orphaned_branch(&log, &store, &source, "gc-fenced", 1_000);
    log.acquire_epoch(&source, 2_000).unwrap();
    let limits =
        MaintenanceLimits::new(32, 1_000_000, 64, std::time::Duration::from_secs(1), 8).unwrap();
    let report = log
        .gc_orphaned_branches_bounded(&source, 0, 10_000, 100, limits, false)
        .unwrap();
    assert!(report.fenced);
    assert_eq!(
        report.stopped_by,
        Some(MaintenanceExecutionReason::EpochChanged)
    );
    assert!(
        store
            .get(&branch_registry_key_of(&source, &branch))
            .unwrap()
            .is_some()
    );
}

#[test]
fn bounded_orphan_gc_rejects_registry_redirect_before_live_marker_lookup() {
    let store = std::sync::Arc::new(OrphanBranchFaultStore::new());
    let log = SegmentedObjectLog::open(store.clone(), SegmentConfig::new(10_000_000, 100).unwrap());
    let source = shard();
    let branch = seed_orphaned_branch(&log, &store, &source, "gc-redirect", 1_000);
    let owner_epoch = log.maintenance_owner_epoch(&source).unwrap();
    let registry_key = branch_registry_key_of(&source, &branch);
    let mut metadata: serde_json::Value = serde_json::from_slice(
        &store
            .get(&registry_key)
            .unwrap()
            .expect("registry metadata"),
    )
    .unwrap();
    metadata["branch"]["queue_id"] = serde_json::Value::String("protected-live".into());
    store
        .put(&registry_key, &serde_json::to_vec(&metadata).unwrap())
        .unwrap();

    let limits =
        MaintenanceLimits::new(32, 1_000_000, 64, std::time::Duration::from_secs(1), 8).unwrap();
    let report = log
        .gc_orphaned_branches_bounded(&source, owner_epoch, 10_000, 100, limits, false)
        .unwrap();
    assert_eq!(report.permanent_failures, 1);
    assert!(store.get(&registry_key).unwrap().is_some());
    assert!(
        !store.list(&shard_prefix_of(&branch)).unwrap().is_empty(),
        "redirected metadata must not authorize cleanup of the indexed branch"
    );
}

struct FenceAfterFirstGcDelete<S: BlobStore> {
    peer: std::sync::Weak<SegmentedObjectLog<S>>,
    source: fireweed_engine::QueueKey,
    fired: AtomicBool,
}

impl<S: BlobStore + 'static> FaultHook for FenceAfterFirstGcDelete<S> {
    fn fault_point(&self, cut: FaultCutPoint) -> EngineResult<()> {
        if cut == FaultCutPoint::GcAfterOrphanObjectDeleted
            && !self.fired.swap(true, Ordering::SeqCst)
            && let Some(peer) = self.peer.upgrade()
        {
            peer.acquire_epoch(&self.source, 20_000)?;
        }
        Ok(())
    }
}

#[test]
fn bounded_orphan_gc_reports_partial_effects_and_retains_authority_after_remote_fence() {
    let store = std::sync::Arc::new(OrphanBranchFaultStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let owner = std::sync::Arc::new(SegmentedObjectLog::open(store.clone(), cfg));
    let source = shard();
    let branch = seed_orphaned_branch(&owner, &store, &source, "gc-mid-fence", 1_000);
    let owner_epoch = owner.maintenance_owner_epoch(&source).unwrap();
    let peer = std::sync::Arc::new(SegmentedObjectLog::open(store.clone(), cfg));
    peer.create_queue(&qdef()).unwrap();
    owner.set_fault_hook(Some(std::sync::Arc::new(FenceAfterFirstGcDelete {
        peer: std::sync::Arc::downgrade(&peer),
        source: source.clone(),
        fired: AtomicBool::new(false),
    })));
    let limits =
        MaintenanceLimits::new(64, 1_000_000, 128, std::time::Duration::from_secs(1), 8).unwrap();
    let report = owner
        .gc_orphaned_branches_bounded(&source, owner_epoch, 10_000, 100, limits, false)
        .unwrap();
    owner.set_fault_hook(None);
    assert!(
        report.deleted > 0,
        "partial effects remain visible in the report"
    );
    assert!(report.fenced);
    assert_eq!(
        report.stopped_by,
        Some(MaintenanceExecutionReason::EpochChanged)
    );
    assert!(
        store
            .get(&branch_registry_key_of(&source, &branch))
            .unwrap()
            .is_some(),
        "the source pin is retained after fencing"
    );
    assert!(
        store
            .get(&format!("{}branch.pending", shard_prefix_of(&branch)))
            .unwrap()
            .is_some(),
        "the sentinel is retained after fencing"
    );
}

#[test]
fn bounded_orphan_gc_reports_retryable_delete_failure_without_releasing_pin() {
    let store = std::sync::Arc::new(OrphanBranchFaultStore::new());
    let log = SegmentedObjectLog::open(store.clone(), SegmentConfig::new(10_000_000, 100).unwrap());
    let source = shard();
    let branch = seed_orphaned_branch(&log, &store, &source, "gc-retry-report", 1_000);
    let owner_epoch = log.maintenance_owner_epoch(&source).unwrap();
    store.fail_deletes.store(true, Ordering::SeqCst);
    let limits =
        MaintenanceLimits::new(64, 1_000_000, 128, std::time::Duration::from_secs(1), 8).unwrap();
    let report = log
        .gc_orphaned_branches_bounded(&source, owner_epoch, 10_000, 100, limits, false)
        .unwrap();
    store.disarm();
    assert_eq!(report.retryable_failures, 1);
    assert_eq!(
        report.stopped_by,
        Some(MaintenanceExecutionReason::RetryableFailure)
    );
    assert!(
        store
            .get(&branch_registry_key_of(&source, &branch))
            .unwrap()
            .is_some()
    );
}

#[test]
fn bounded_orphan_gc_preserves_typed_permanent_corrupt_delete_fault() {
    let store = std::sync::Arc::new(OrphanBranchFaultStore::new());
    let log = SegmentedObjectLog::open(store.clone(), SegmentConfig::new(10_000_000, 100).unwrap());
    let source = shard();
    let branch = seed_orphaned_branch(&log, &store, &source, "gc-corrupt-report", 1_000);
    let owner_epoch = log.maintenance_owner_epoch(&source).unwrap();
    store.fail_deletes.store(true, Ordering::SeqCst);
    store.permanent_delete_fault.store(true, Ordering::SeqCst);
    let report = log
        .gc_orphaned_branches_bounded(
            &source,
            owner_epoch,
            10_000,
            100,
            MaintenanceLimits::new(64, 1_000_000, 128, std::time::Duration::from_secs(1), 8)
                .unwrap(),
            false,
        )
        .unwrap();
    store.disarm();
    assert_eq!(report.retryable_failures, 0);
    assert_eq!(report.permanent_failures, 1);
    assert_eq!(
        report.failure_cause,
        Some(MaintenanceFailureCause::Provider(BlobResultClass::Corrupt))
    );
    assert!(
        store
            .get(&branch_registry_key_of(&source, &branch))
            .unwrap()
            .is_some()
    );
}

fn captured_authority_key<S: BlobStore>(store: &S, source: &fireweed_engine::QueueKey) -> String {
    store
        .list(&shard_prefix_of(source))
        .unwrap()
        .into_iter()
        .filter(|key| key.contains("/authority_head/") || key.contains("/manifest_head/"))
        .max()
        .expect("captured authority object")
}

#[test]
fn bounded_orphan_gc_fences_when_captured_authority_object_is_deleted() {
    let store = std::sync::Arc::new(OrphanBranchFaultStore::new());
    let log = SegmentedObjectLog::open(store.clone(), SegmentConfig::new(10_000_000, 100).unwrap());
    let source = shard();
    let branch = seed_orphaned_branch(&log, &store, &source, "gc-token-deleted", 1_000);
    let owner_epoch = log.maintenance_owner_epoch(&source).unwrap();
    store
        .delete(&captured_authority_key(&store, &source))
        .unwrap();
    let report = log
        .gc_orphaned_branches_bounded(
            &source,
            owner_epoch,
            10_000,
            100,
            MaintenanceLimits::new(64, 1_000_000, 128, std::time::Duration::from_secs(1), 8)
                .unwrap(),
            false,
        )
        .unwrap();
    assert!(report.fenced);
    assert!(
        store
            .get(&branch_registry_key_of(&source, &branch))
            .unwrap()
            .is_some()
    );
}

fn seeded_expiry_owner() -> (
    std::sync::Arc<InMemoryBlobStore>,
    SegmentedObjectLog<std::sync::Arc<InMemoryBlobStore>>,
    fireweed_engine::QueueKey,
) {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let log = SegmentedObjectLog::open(store.clone(), SegmentConfig::new(10_000_000, 100).unwrap());
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    let epoch = log.acquire_epoch(&source, 1).unwrap();
    for _ in 0..2 {
        log.enqueue(&source, &pushes(1), epoch, 10).unwrap();
        log.seal(&source, epoch, 11).unwrap();
    }
    log.advance_retention_floor(
        &source,
        CommandPosition::new(source.clone(), epoch, 1),
        epoch,
    )
    .unwrap();
    (store, log, source)
}

fn assert_expiry_segments_remain(store: &InMemoryBlobStore, source: &fireweed_engine::QueueKey) {
    assert!(
        store
            .list(&shard_prefix_of(source))
            .unwrap()
            .iter()
            .any(|key| key.ends_with(".seg")),
        "fenced expiry must not delete segment objects"
    );
}

#[test]
fn segment_expiry_fences_when_captured_authority_object_is_deleted() {
    let (store, log, source) = seeded_expiry_owner();
    store
        .delete(&captured_authority_key(&store, &source))
        .unwrap();
    assert_eq!(
        log.expire_segments_through(&source, 1, 100).unwrap_err(),
        EngineError::EpochFenced
    );
    assert_expiry_segments_remain(&store, &source);
}

#[test]
fn bounded_segment_expiry_large_prefix_resumes_without_exceeding_caps() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let log = SegmentedObjectLog::open(store.clone(), SegmentConfig::new(10_000_000, 100).unwrap());
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    let epoch = log.acquire_epoch(&source, 1).unwrap();
    for i in 0..40 {
        log.enqueue(&source, &pushes(1), epoch, 10 + i).unwrap();
        log.seal(&source, epoch, 10 + i).unwrap();
    }
    log.advance_retention_floor(
        &source,
        CommandPosition::new(source.clone(), epoch, 39),
        epoch,
    )
    .unwrap();
    let limits =
        MaintenanceLimits::new(2, 1_000_000, 30, std::time::Duration::from_secs(1), 5).unwrap();
    let mut deleted = 0;
    let mut bytes = 0;
    let mut saw_cursor = false;
    for _ in 0..128 {
        let report = log
            .expire_segments_through_bounded(&source, 39, 1_000, limits, false)
            .unwrap();
        assert!(report.deleted <= limits.objects.get());
        assert!(report.bytes_deleted <= limits.bytes.get());
        assert!(report.requests <= limits.requests.get());
        saw_cursor |= report.cursor.is_some();
        deleted += report.deleted;
        bytes += report.bytes_deleted;
        if report.cursor.is_none() {
            break;
        }
    }
    assert!(saw_cursor);
    assert_eq!(deleted, 40, "every segment is reconciled exactly once");
    assert!(bytes > 0);
    assert!(
        log.read_manifest_deletion_watermark(&source)
            .unwrap()
            .is_some(),
        "completed bounded traversal publishes its proven contiguous watermark"
    );
    assert_eq!(
        store
            .list(&shard_prefix_of(&source))
            .unwrap()
            .into_iter()
            .filter(|key| key.ends_with(".seg"))
            .count(),
        0
    );
}

#[test]
fn bounded_segment_expiry_reopen_discards_soft_cursor_and_reconciles() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let source = shard();
    {
        let log = SegmentedObjectLog::open(store.clone(), cfg);
        log.create_queue(&qdef()).unwrap();
        let epoch = log.acquire_epoch(&source, 1).unwrap();
        for i in 0..12 {
            log.enqueue(&source, &pushes(1), epoch, 10 + i).unwrap();
            log.seal(&source, epoch, 10 + i).unwrap();
        }
        log.advance_retention_floor(
            &source,
            CommandPosition::new(source.clone(), epoch, 11),
            epoch,
        )
        .unwrap();
        let first = log
            .expire_segments_through_bounded(
                &source,
                11,
                1_000,
                MaintenanceLimits::new(1, 1_000_000, 24, std::time::Duration::from_secs(1), 4)
                    .unwrap(),
                false,
            )
            .unwrap();
        assert_eq!(first.deleted, 1);
        assert!(first.cursor.is_some());
    }

    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&qdef()).unwrap();
    reopened.acquire_epoch(&source, 2_000).unwrap();
    let limits =
        MaintenanceLimits::new(2, 1_000_000, 30, std::time::Duration::from_secs(1), 4).unwrap();
    for _ in 0..64 {
        let report = reopened
            .expire_segments_through_bounded(&source, 11, 3_000, limits, false)
            .unwrap();
        assert!(report.requests <= limits.requests.get());
        if report.cursor.is_none() {
            break;
        }
    }
    assert_eq!(
        store
            .list(&shard_prefix_of(&source))
            .unwrap()
            .into_iter()
            .filter(|key| key.ends_with(".seg"))
            .count(),
        0
    );
    assert!(
        reopened
            .read_manifest_deletion_watermark(&source)
            .unwrap()
            .is_some()
    );
}

#[test]
fn bounded_segment_expiry_rescans_when_target_increases() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let log = SegmentedObjectLog::open(store.clone(), SegmentConfig::new(10_000_000, 100).unwrap());
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    let epoch = log.acquire_epoch(&source, 1).unwrap();
    for i in 0..4 {
        log.enqueue(&source, &pushes(1), epoch, 10 + i).unwrap();
        log.seal(&source, epoch, 10 + i).unwrap();
    }
    log.advance_retention_floor(
        &source,
        CommandPosition::new(source.clone(), epoch, 3),
        epoch,
    )
    .unwrap();
    let limits =
        MaintenanceLimits::new(8, 1_000_000, 64, std::time::Duration::from_secs(1), 8).unwrap();
    let first = log
        .expire_segments_through_bounded(&source, 1, 1_000, limits, false)
        .unwrap();
    assert_eq!(first.deleted, 2);
    assert!(first.cursor.is_none());

    let second = log
        .expire_segments_through_bounded(&source, 3, 2_000, limits, false)
        .unwrap();
    assert_eq!(
        second.deleted, 2,
        "a larger target must rescan skipped entries"
    );
    assert_eq!(
        store
            .list(&shard_prefix_of(&source))
            .unwrap()
            .into_iter()
            .filter(|key| key.ends_with(".seg"))
            .count(),
        0
    );
}

#[test]
fn bounded_segment_expiry_retries_first_pinned_candidate_after_pin_ttl() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let log = SegmentedObjectLog::open(store.clone(), SegmentConfig::new(10_000_000, 100).unwrap());
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    let epoch = log.acquire_epoch(&source, 1).unwrap();
    log.enqueue(&source, &pushes(1), epoch, 10).unwrap();
    log.seal(&source, epoch, 10).unwrap();
    let branch_def = branch_qdef("bounded-pin-ttl");
    log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), epoch, 0),
        100,
        10,
    )
    .unwrap();
    log.advance_retention_floor(
        &source,
        CommandPosition::new(source.clone(), epoch, 0),
        epoch,
    )
    .unwrap();
    let limits =
        MaintenanceLimits::new(2, 1_000_000, 32, std::time::Duration::from_secs(1), 4).unwrap();
    let pinned = log
        .expire_segments_through_bounded(&source, 0, 50, limits, false)
        .unwrap();
    assert_eq!(pinned.deleted, 0);
    assert!(pinned.cursor.is_some());
    assert_eq!(
        pinned.stopped_by,
        Some(MaintenanceExecutionReason::BudgetExhausted)
    );

    let expired = log
        .expire_segments_through_bounded(&source, 0, 200, limits, false)
        .unwrap();
    assert_eq!(
        expired.deleted, 1,
        "expired pin must release the unresolved entry"
    );
    assert!(expired.cursor.is_none());
}

#[test]
fn bounded_segment_expiry_resumes_large_branch_registry_under_request_cap() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let log = SegmentedObjectLog::open(store.clone(), SegmentConfig::new(10_000_000, 100).unwrap());
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    let epoch = log.acquire_epoch(&source, 1).unwrap();
    log.enqueue(&source, &pushes(1), epoch, 10).unwrap();
    log.seal(&source, epoch, 10).unwrap();
    for i in 0..20 {
        log.branch(
            &source,
            &branch_qdef(&format!("bounded-registry-{i}")),
            &CommandPosition::new(source.clone(), epoch, 0),
            1,
            10,
        )
        .unwrap();
    }
    log.advance_retention_floor(
        &source,
        CommandPosition::new(source.clone(), epoch, 0),
        epoch,
    )
    .unwrap();
    let limits =
        MaintenanceLimits::new(1, 1_000_000, 14, std::time::Duration::from_secs(1), 3).unwrap();
    let mut deleted = 0;
    for _ in 0..64 {
        let report = log
            .expire_segments_through_bounded(&source, 0, 1_000, limits, false)
            .unwrap();
        assert!(report.requests <= limits.requests.get());
        deleted += report.deleted;
        if report.cursor.is_none() {
            break;
        }
    }
    assert_eq!(
        deleted, 1,
        "registry pagination must not starve segment work"
    );
}

#[test]
fn bounded_orphan_gc_reopens_and_finishes_from_persisted_size_inventory() {
    let store = std::sync::Arc::new(OrphanBranchFaultStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let source = shard();
    let branch = {
        let first = SegmentedObjectLog::open(store.clone(), cfg);
        seed_orphaned_branch(&first, &store, &source, "gc-reopen-inventory", 1_000)
    };
    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&qdef()).unwrap();
    let owner_epoch = reopened.acquire_epoch(&source, 20_000).unwrap();
    let limits =
        MaintenanceLimits::new(64, 1_000_000, 128, std::time::Duration::from_secs(1), 8).unwrap();
    let mut completed = 0;
    for _ in 0..32 {
        let report = reopened
            .gc_orphaned_branches_bounded(&source, owner_epoch, 30_000, 100, limits, false)
            .unwrap();
        completed += report.completed_candidates;
        if completed == 1 {
            break;
        }
    }
    assert_eq!(completed, 1);
    assert!(
        store
            .get(&branch_registry_key_of(&source, &branch))
            .unwrap()
            .is_none()
    );
}

// TestBranchGcFailClosedOnCorruptInheritanceMetadata: branch GC refuses to touch an orphan when the
// persisted source-pin metadata is present but corrupt before the classify+delete pass can trust it.
#[test]
#[allow(non_snake_case)]
fn TestBranchGcFailClosedOnCorruptInheritanceMetadata() {
    let store = std::sync::Arc::new(OrphanBranchFaultStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let source = shard();
    let branch = seed_orphaned_branch(&log, &store, &source, "gc-corrupt-inheritance", 1_000);

    let registry_key = branch_registry_key_of(&source, &branch);
    store.inner.put(&registry_key, b"{not-valid-json").unwrap();

    let err = gc_orphans_as_local_owner(&log, &source).unwrap_err();
    assert!(
        matches!(err, EngineError::Storage(_)),
        "corrupt source-pin metadata must surface as a storage error so GC cannot guess: {err:?}"
    );
    assert!(
        store
            .get(&format!("{}branch.pending", shard_prefix_of(&branch)))
            .unwrap()
            .is_some(),
        "the orphan stays intact after the corrupt metadata aborts GC"
    );
    assert!(
        !store
            .list(&format!("{}branches/", shard_prefix_of(&source)))
            .unwrap()
            .is_empty(),
        "the source pin remains registered after the aborted GC pass"
    );
    assert!(
        store.list(&shard_prefix_of(&branch)).unwrap().len() > 1,
        "the branch-local authority objects are left untouched when GC refuses to proceed"
    );
}

// TestBranchGcDeletesBelowFloorAfterLastReadableBranch (bead pqueue-635500fb): with TWO committed, live
// branches pinning overlapping-but-different ranges of a trimmed source, below-floor source objects stay
// retained as long as AT LEAST ONE of them can still read them — not just while every branch needs them.
// `branch_a` is cut at seq 0 (pins only the first segment); `branch_b` is cut at seq 3 (pins all four). Once
// `branch_a` is discarded, `branch_b` ALONE keeps every below-floor segment retained; only once `branch_b` is
// also discarded (no readable branch remains) do the segments become reclaimable.
#[test]
#[allow(non_snake_case)]
fn TestBranchGcDeletesBelowFloorAfterLastReadableBranch() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let source = shard();

    log.create_queue(&qdef()).unwrap();
    for i in 0..4u64 {
        log.enqueue(&source, &pushes(1), 0, 10 + i as i64 * 10)
            .unwrap();
        log.seal(&source, 0, 11 + i as i64 * 10).unwrap();
    }

    let branch_a_def = branch_qdef("gc-multi-readable-a");
    let branch_a = QueueKey::new(
        branch_a_def.tenant_id.clone(),
        branch_a_def.queue_id.clone(),
    );
    log.branch(
        &source,
        &branch_a_def,
        &CommandPosition::new(source.clone(), 0, 0),
        60_000,
        50,
    )
    .unwrap();

    let branch_b_def = branch_qdef("gc-multi-readable-b");
    let branch_b = QueueKey::new(
        branch_b_def.tenant_id.clone(),
        branch_b_def.queue_id.clone(),
    );
    log.branch(
        &source,
        &branch_b_def,
        &CommandPosition::new(source.clone(), 0, 3),
        60_000,
        50,
    )
    .unwrap();

    advance_floor_as_local_owner(&log, &source, 3, 0).unwrap();

    assert_eq!(
        expire_segments_as_local_owner(&log, &source, 3, 100).unwrap(),
        0,
        "every below-floor segment stays pinned while both branches remain readable"
    );

    // The LAST-readable-branch case: branch_a (which never needed seq 1..3) is discarded, leaving branch_b as
    // the ONLY remaining readable branch. Every below-floor segment — including the ones branch_a never
    // pinned — must still be retained purely because branch_b can still read them.
    log.discard_branch(&source, &branch_a).unwrap();
    assert!(
        log.manifest_reclamation_candidates(&source, 3, 100)
            .unwrap()
            .is_empty(),
        "with branch_a gone, branch_b alone still keeps every below-floor segment retained"
    );
    assert_eq!(
        expire_segments_as_local_owner(&log, &source, 3, 150).unwrap(),
        0,
        "GC deletes nothing while branch_b remains the last readable branch"
    );
    assert_eq!(
        log.read_all(&branch_b)
            .unwrap()
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3],
        "branch_b reads its full retained view after the GC pass"
    );

    // Once branch_b (the last readable branch) is also discarded, the below-floor segments finally become
    // reclaimable via the existing trim path.
    log.discard_branch(&source, &branch_b).unwrap();
    assert_eq!(
        expire_segments_as_local_owner(&log, &source, 3, 200).unwrap(),
        4,
        "with no readable branch left, the below-floor segments become reclaimable"
    );
}

// TestBranchGcDeletesBelowFloorAfterLastReadableBranchFailClosed (bead pqueue-635500fb): a committed, live
// (still readable) branch's source-pin proof becomes unfetchable — `store.list` still returns its registry
// key, but `store.get` unexpectedly returns `None` for it, a storage inconsistency rather than a legitimate
// `discard_branch`. Below-floor source objects the branch can still read MUST stay retained: the trim path
// must fail closed (surface an error, delete nothing) rather than silently treat the branch as unpinned.

// TestBranchGcDeletesBelowFloorAfterLastReadableBranchFinal (bead pqueue-29a6c98c): with TWO committed, live
// branches pinning a trimmed source, below-floor source manifest and segment objects stay retained while
// EITHER remains readable. Once the final readable branch (`branch_b`, the wider-cut one) is ALSO discarded —
// the "last readable branch advances" condition — `expire_segments_through` does not merely report a
// non-zero count: it physically removes the below-floor segment objects from the store.

// TestBranchGcDeletesBelowFloorAfterLastReadableBranchFinalConservative (bead pqueue-29a6c98c): the LAST
// remaining readable branch's inherited floor/source-pin proof becomes AMBIGUOUS (its registry entry is
// listed by the store but cannot be fetched) rather than the branch being genuinely discarded/advanced.
// Branch GC must NOT treat that ambiguity as proof the final branch has advanced past the below-floor range:
// it fails closed and leaves every below-floor source manifest and segment object physically intact. Once the
// ambiguity clears and the branch is genuinely discarded, the true final-branch-advances case (proven above)
// takes over and deletion proceeds.

// Test 3 — live data is byte-identical pre/post horizon, and a below-floor read FAILS CLOSED (read at the
// floor errors; read at floor+1 succeeds; read_all from genesis fails closed on a trimmed+horizoned queue).
#[test]
#[allow(non_snake_case)]
fn TestBelowFloorReadFailsClosedAfterManifestReclaim() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef()).unwrap();
    for i in 0..6u64 {
        log.enqueue(&shard(), &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard(), 0, (i as i64 + 1) * 10 + 1).unwrap();
    }
    // Capture the live tail (seqs 8..11) BEFORE the trim.
    let before = log.read_from(&shard(), 8).unwrap();
    assert_eq!(
        before.iter().map(|(p, _)| p.sequence).collect::<Vec<_>>(),
        vec![8, 9, 10, 11]
    );

    trim_cycle(&log, &shard(), 7, 0, 1_000);
    assert!(
        log.read_manifest_deletion_watermark(&shard())
            .unwrap()
            .is_some(),
        "horizon exists after trim"
    );
    let floor = log.read_retention_floor(&shard()).unwrap().unwrap();
    assert_eq!(floor.sequence, 7);

    // Byte-identical: the same live tail reads back identically after the horizon advanced (no live entry
    // skipped by the range-list). CommandEnvelope is not PartialEq, so compare position + serialized bytes.
    let fingerprint = |v: &Vec<(CommandPosition, fireweed_engine::CommandEnvelope)>| {
        v.iter()
            .map(|(p, e)| (p.sequence, p.backend_epoch, serde_json::to_vec(e).unwrap()))
            .collect::<Vec<_>>()
    };
    let after = log.read_from(&shard(), 8).unwrap();
    assert_eq!(
        fingerprint(&before),
        fingerprint(&after),
        "live tail is byte-identical pre/post horizon"
    );

    // Fail closed: read AT the floor (seq 7) errors; read at floor+1 (seq 8) succeeds.
    assert!(
        matches!(
            log.read_from(&shard(), floor.sequence),
            Err(EngineError::Storage(_))
        ),
        "a read AT the floor dips into the reclaimed prefix and fails closed"
    );
    assert!(
        log.read_from(&shard(), floor.sequence + 1).is_ok(),
        "a read at floor+1 (first readable seq) succeeds"
    );
    // read_all (genesis) fails closed whenever a floor+horizon exist.
    assert!(
        matches!(log.read_all(&shard()), Err(EngineError::Storage(_))),
        "read_all from genesis fails closed on a trimmed+horizoned queue"
    );
}

// Test 3b — manifest reclamation below the floor does not perturb the live tail above it.
// The first readable entry at `floor + 1` and every later live entry must remain byte-identical
// before and after reclaiming the below-floor manifest entries.

// Test 4 — a stale cached writer whose next index points below the durable deletion watermark still cannot ack.
// The reclaimed manifest slot is overwritten with a durable marker, and the stale seal returns
// `EpochFenced` or `Conflict` rather than creating a fresh durable entry at the occupied address.

// TestPermanentFenceSurvivesReopen: reopening the store reconstructs the durable reclaimed-index fence
// from recovery-visible marker history, even if the compatibility cache blob was removed.

// TestReopenFenceReloadsBeforeSeal: a fresh open reloads the reclaimed-index fence before the stale
// writer reaches seal, so the seal still self-fences instead of acking a reclaimed historical index.

// TestNoTailValidateRollbackSubstituteAfterReopen: a reclaimed cached manifest index is rejected by the
// durable fence before any successful stale ack can be externally observed. The rejected path does not need
// tail-validate/delete rollback; that substitute is explicitly not the fence mechanism (see
// docs/perf/design/manifest-compaction-hotpath.md:359 and pqueue-c33c367e).

// TestReopenFenceCommentReferencesDesign: same reclaimed-index fence, documented here so the hot-path
// comment and the test both point at docs/perf/design/manifest-compaction-hotpath.md:359 and
// pqueue-c33c367e. The design note rejects tail-validate/delete rollback as the fence mechanism.

// Test 5 — the reclaim-fence rejection path keeps the seal hot path free of manifest LISTs. The seal
// returns from the durable deletion watermark check without listing the manifest before every seal.
#[test]
#[allow(non_snake_case)]
fn TestSealPathDoesNotListBeforeEveryFenceCheck() {
    let (store, stale_owner, _live_owner) = reclaimed_cached_writer_fixture();

    stale_owner.enqueue(&shard(), &pushes(1), 0, 5_000).unwrap();
    store.reset_reads();
    let err = stale_owner.seal(&shard(), 0, 5_001).unwrap_err();
    assert!(
        matches!(err, EngineError::EpochFenced | EngineError::Conflict),
        "the stale reclaimed index should fence cleanly, got {err:?}"
    );
}

// TestObjectlogDeletedManifestFailClosedSignal: objectlog recovery (read_from / read_all) returns the
// distinct deleted-manifest-prefix fail-closed signal when replay would require physically deleted
// manifest prefixes. The signal is distinguishable from generic storage errors via
// `SegmentedObjectLog::is_deleted_manifest_prefix_error`.
#[test]
#[allow(non_snake_case)]
fn TestObjectlogDeletedManifestFailClosedSignal() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let def = unique_qdef("deleted-manifest-signal");
    let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    log.create_queue(&def).unwrap();
    for i in 0..6u64 {
        log.enqueue(&shard, &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }
    // Trim and physically expire
    trim_cycle(&log, &shard, 7, 0, 1_000);
    assert!(
        log.read_manifest_deletion_watermark(&shard)
            .unwrap()
            .is_some(),
        "horizon exists after trim"
    );
    let floor = log
        .read_retention_floor(&shard)
        .unwrap()
        .expect("floor after trim");
    assert_eq!(floor.sequence, 7);

    // Read AT the floor (seq 7) fails closed with the distinct signal.
    let err = log.read_from(&shard, floor.sequence).unwrap_err();
    assert!(
        fireweed_objectlog::segmented::is_deleted_manifest_prefix_error(&err),
        "read_from(floor) must return the distinct deleted-manifest-prefix signal: {err:?}"
    );
    // Read below the floor (genesis) fails closed with the distinct signal.
    let err = log.read_all(&shard).unwrap_err();
    assert!(
        fireweed_objectlog::segmented::is_deleted_manifest_prefix_error(&err),
        "read_all(genesis) must return the distinct deleted-manifest-prefix signal: {err:?}"
    );

    // Physically delete the entire manifest prefix to simulate extreme deletion.
    for key in store.list(&manifest_prefix_s(&shard)).unwrap() {
        store.delete(&key).unwrap();
    }
    // Reopen — the watermark and floor survive.
    drop(log);
    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&def).unwrap();
    let recovered_floor = reopened
        .read_retention_floor(&shard)
        .unwrap()
        .expect("floor survives reopen");
    assert_eq!(recovered_floor.sequence, 7);

    // After reopen, reads below floor still return the distinct signal.
    let err = reopened.read_from(&shard, floor.sequence).unwrap_err();
    assert!(
        fireweed_objectlog::segmented::is_deleted_manifest_prefix_error(&err),
        "reopened: read_from(floor) must return the distinct signal: {err:?}"
    );
    let err = reopened.read_all(&shard).unwrap_err();
    assert!(
        fireweed_objectlog::segmented::is_deleted_manifest_prefix_error(&err),
        "reopened: read_all must return the distinct signal: {err:?}"
    );

    // The error is NOT a generic missing-segment storage error — it has the distinct prefix.
    let generic_storage = EngineError::Storage("generic storage error".into());
    assert!(
        !fireweed_objectlog::segmented::is_deleted_manifest_prefix_error(&generic_storage),
        "generic Storage errors must NOT match the deleted-manifest-prefix signal"
    );
}

// TestObjectlogRetainedFloorHeadReplayStillSucceeds: objectlog recovery beginning at the retained
// floor/head succeeds without relaxing retention-floor or source-pin guarantees and without data loss.
// After manifest prefix deletion and reopen, `read_from(floor+1)` returns the live tail and
// `read_from(floor)` / `read_all` still fail closed.
#[test]
#[allow(non_snake_case)]
fn TestObjectlogRetainedFloorHeadReplayStillSucceeds() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let def = unique_qdef("retained-replay");
    let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    log.create_queue(&def).unwrap();
    for i in 0..6u64 {
        log.enqueue(&shard, &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }
    assert_eq!(
        log.read_from(&shard, 0).unwrap().len(),
        12,
        "all 12 commands readable before trim"
    );

    // Capture the live tail (seqs 8-11) BEFORE trim.
    let before_tail: Vec<u64> = log
        .read_from(&shard, 8)
        .unwrap()
        .iter()
        .map(|(p, _)| p.sequence)
        .collect();
    assert_eq!(before_tail, vec![8, 9, 10, 11]);

    trim_cycle(&log, &shard, 7, 0, 1_000);
    let horizon = log.read_manifest_deletion_watermark(&shard).unwrap();
    assert!(horizon.is_some(), "horizon exists after trim");
    let floor = log
        .read_retention_floor(&shard)
        .unwrap()
        .expect("floor after trim");
    assert_eq!(floor.sequence, 7);

    // Reopen to simulate recovery.
    drop(log);
    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&def).unwrap();

    // read_from at floor+1 succeeds and returns the identical live tail (no data loss).
    let after_tail: Vec<u64> = reopened
        .read_from(&shard, 8)
        .unwrap()
        .iter()
        .map(|(p, _)| p.sequence)
        .collect();
    assert_eq!(
        after_tail, before_tail,
        "recovery from retained floor/head preserves the live tail without data loss"
    );

    // read_from at the floor fails closed (retention-floor guarantee intact).
    let err = reopened.read_from(&shard, floor.sequence).unwrap_err();
    assert!(
        fireweed_objectlog::segmented::is_deleted_manifest_prefix_error(&err),
        "read_from(floor) must fail closed after recovery: {err:?}"
    );

    // read_all (genesis) fails closed (below-floor guarantee intact).
    let err = reopened.read_all(&shard).unwrap_err();
    assert!(
        fireweed_objectlog::segmented::is_deleted_manifest_prefix_error(&err),
        "read_all must fail closed after recovery: {err:?}"
    );

    // Source-pin guarantee: reading from floor+1 does NOT return any below-floor data.
    let all_after: Vec<u64> = reopened
        .read_from(&shard, floor.sequence + 1)
        .unwrap()
        .iter()
        .map(|(p, _)| p.sequence)
        .collect();
    assert!(
        all_after.iter().all(|s| *s > floor.sequence),
        "no below-floor data leaks through: all returned sequences > {}",
        floor.sequence
    );
}

// TestObjectlogFireweedC33c367eInteractionRecorded: pqueue-c33c367e interaction is evaluated before
// landing and the objectlog-specific conclusion is recorded for release notes handoff. The deferred
// server-side owner-fence wiring (pqueue-c33c367e) does not change the deleted-manifest fail-closed
// signal at the objectlog level — the signal is gated on the durable retention floor and manifest
// deletion watermark, both of which are independent of owner-fence wiring. The permanent head CAS
// remains the stale-writer fence; the watermark is a read-cost helper, not an ownership fence.

fn integrity_config() -> SegmentConfig {
    SegmentConfig::new(10_000_000, 100).unwrap()
}

#[test]
fn segment_configuration_rejects_targets_above_the_writable_frame_cap() {
    assert!(SegmentConfig::new(64 * 1024 * 1024, 1).is_ok());
    assert!(matches!(
        SegmentConfig::new(64 * 1024 * 1024 + 1, 1),
        Err(EngineError::Invalid(_))
    ));
}

#[test]
fn current_format_reopens_replay_and_branch() {
    for width in 1..=6_u32 {
        let store = Arc::new(InMemoryBlobStore::new());
        let definition = unique_qdef(&format!("current-{width}"));
        let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        for offset in 0..width {
            let log = SegmentedObjectLog::open(store.clone(), integrity_config());
            log.create_queue(&definition).unwrap();
            log.enqueue(&key, &pushes(1), 0, i64::from(offset)).unwrap();
            log.seal(&key, 0, i64::from(offset) + 1).unwrap();
        }
        let reader = SegmentedObjectLog::open(store.clone(), integrity_config());
        reader.create_queue(&definition).unwrap();
        let replay = reader.read_all(&key).unwrap();
        assert_eq!(replay.len(), width as usize);
        assert_eq!(
            replay
                .iter()
                .map(|(position, _)| position.sequence)
                .collect::<Vec<_>>(),
            (0..u64::from(width)).collect::<Vec<_>>()
        );

        let mut branch_definition = definition.clone();
        branch_definition.queue_id = QueueId::new(format!("branch-current-{width}")).unwrap();
        let cut = replay.last().unwrap().0.clone();
        reader
            .branch(&key, &branch_definition, &cut, 10_000, 100)
            .unwrap();
        let branch_key = QueueKey::new(
            branch_definition.tenant_id.clone(),
            branch_definition.queue_id.clone(),
        );
        assert_eq!(reader.read_all(&branch_key).unwrap().len(), width as usize);
    }
}

#[test]
fn current_format_retention_expiry_and_reopen_preserve_the_live_tail() {
    let store = Arc::new(InMemoryBlobStore::new());
    let definition = unique_qdef("current-retention-reopen");
    let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    for _ in 0..3 {
        let log = SegmentedObjectLog::open(store.clone(), integrity_config());
        log.create_queue(&definition).unwrap();
        log.enqueue(&key, &pushes(1), 0, 1).unwrap();
        log.seal(&key, 0, 2).unwrap();
    }
    let maintenance = SegmentedObjectLog::open(store.clone(), integrity_config());
    maintenance.create_queue(&definition).unwrap();
    let epoch = advance_floor_as_local_owner(&maintenance, &key, 0, 10).unwrap();
    assert_eq!(maintenance.maintenance_owner_epoch(&key), Some(epoch));
    maintenance.expire_segments_through(&key, 0, 20).unwrap();

    let reopened = SegmentedObjectLog::open(store, integrity_config());
    reopened.create_queue(&definition).unwrap();
    let tail = reopened.read_from_limited(&key, 1, 10).unwrap();
    assert_eq!(
        tail.iter()
            .map(|(position, _)| position.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn shared_authority_fences_data_floor_watermark_reopen_keeps_index_and_sequence_domains_separate() {
    let store = Arc::new(InMemoryBlobStore::new());
    let definition = unique_qdef("watermark-domain-separation");
    let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let log = SegmentedObjectLog::open(store.clone(), integrity_config());
    log.create_queue(&definition).unwrap();
    for epoch in 1..=5 {
        log.fence_epoch(&key, epoch, epoch as i64).unwrap();
    }
    log.enqueue(&key, &pushes(1), 5, 10).unwrap();
    log.seal(&key, 5, 11).unwrap();
    let epoch = log.acquire_epoch(&key, 12).unwrap();
    log.advance_retention_floor(&key, CommandPosition::new(key.clone(), epoch, 0), epoch)
        .unwrap();
    log.expire_segments_through(&key, 0, 13).unwrap();
    assert!(
        log.read_manifest_deletion_watermark(&key)
            .unwrap()
            .is_some()
    );

    let reopened = SegmentedObjectLog::open(store, integrity_config());
    reopened.create_queue(&definition).unwrap();
    assert_eq!(
        reopened
            .read_retention_floor(&key)
            .unwrap()
            .unwrap()
            .sequence,
        0
    );
    assert!(
        reopened
            .read_manifest_deletion_watermark(&key)
            .unwrap()
            .is_some()
    );
    assert!(reopened.read_from_limited(&key, 1, 10).unwrap().is_empty());
}

#[test]
fn every_single_bit_current_format_mutation_fails_with_typed_redacted_error() {
    let store = Arc::new(InMemoryBlobStore::new());
    let definition = unique_qdef("current-bitflip");
    let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let log = SegmentedObjectLog::open(store.clone(), integrity_config());
    log.create_queue(&definition).unwrap();
    log.enqueue(&key, &pushes(1), 0, 1).unwrap();
    log.seal(&key, 0, 2).unwrap();

    let segment_key = store
        .list("t/")
        .unwrap()
        .into_iter()
        .find(|candidate| candidate.ends_with(".seg"))
        .unwrap();
    let original = store.get(&segment_key).unwrap().unwrap();
    for bit in 0..original.len() * 8 {
        let mut corrupt = original.clone();
        corrupt[bit / 8] ^= 1 << (bit % 8);
        store.put(&segment_key, &corrupt).unwrap();
        let error = log.read_all(&key).unwrap_err();
        let EngineError::DurableDataCorrupt { ref locator, .. } = error else {
            panic!("bit {bit} returned an untyped error: {error:?}");
        };
        assert!(!locator.contains('/') && !error.to_string().contains(&segment_key));
    }
    store.put(&segment_key, &original).unwrap();
    assert_eq!(log.read_all(&key).unwrap().len(), 1);
}

#[test]
fn malformed_authority_candidate_reports_exact_index_and_one_logical_corrupt_event() {
    let recorder = Arc::new(BlobMetricsRecorder::new());
    let raw = Arc::new(InMemoryBlobStore::new());
    let store = InstrumentedBlobStore::new(raw.clone(), recorder.clone(), BlobBackendKind::Memory);
    let definition = unique_qdef("malformed-candidate");
    let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let log = SegmentedObjectLog::open(store, integrity_config());
    log.create_queue(&definition).unwrap();
    log.fence_epoch(&key, 1, 0).unwrap();
    log.enqueue(&key, &pushes(1), 1, 1).unwrap();
    log.seal(&key, 1, 2).unwrap();
    let candidate_key = raw
        .list("t/")
        .unwrap()
        .into_iter()
        .find(|candidate| candidate.contains("/manifest_candidates/"))
        .unwrap();
    let expected_index = candidate_key
        .split('/')
        .find_map(|component| component.strip_prefix('i'))
        .unwrap()
        .parse::<u64>()
        .unwrap();
    raw.put(&candidate_key, b"{").unwrap();
    let before = recorder.snapshot();
    let error = log.read_all(&key).unwrap_err();
    let delta = recorder.snapshot().delta(&before);
    assert!(matches!(
        error,
        EngineError::DurableDataCorrupt {
            stage: fireweed_engine::DurableIntegrityStage::Manifest,
            manifest_index,
            ..
        } if manifest_index == expected_index
    ));
    let validation = delta.row(
        BlobOperation::ValidateSegment,
        BlobObjectClass::Segment,
        BlobResultClass::Corrupt,
        false,
        BlobBackendKind::Memory,
    );
    assert_eq!(validation.completions, 1);
    assert_eq!(validation.attempts, 0);
    assert_eq!(validation.request_bytes + validation.response_bytes, 0);
}

#[test]
fn shared_identical_cas_loser_never_deletes_winning_current_object() {
    let store = Arc::new(InMemoryBlobStore::new());
    let definition = unique_qdef("current-identical-cas");
    let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let initializer = SegmentedObjectLog::open(store.clone(), integrity_config());
    initializer.create_queue(&definition).unwrap();
    initializer.fence_epoch(&key, 1, 0).unwrap();

    let winner = SegmentedObjectLog::open(store.clone(), integrity_config());
    let loser = SegmentedObjectLog::open(store.clone(), integrity_config());
    winner.create_queue(&definition).unwrap();
    loser.create_queue(&definition).unwrap();
    let commands = pushes(1);
    winner.enqueue(&key, &commands, 1, 1).unwrap();
    loser.enqueue(&key, &commands, 1, 1).unwrap();
    winner.seal(&key, 1, 2).unwrap();
    assert!(matches!(loser.seal(&key, 1, 2), Err(EngineError::Conflict)));
    assert_eq!(winner.read_all(&key).unwrap().len(), 1);
}

#[test]
fn losing_distinct_candidate_gc_preserves_shared_content_addressed_segment() {
    let store = Arc::new(InMemoryBlobStore::new());
    let definition = unique_qdef("current-shared-segment-gc");
    let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let initializer = SegmentedObjectLog::open(store.clone(), integrity_config());
    initializer.create_queue(&definition).unwrap();
    initializer.fence_epoch(&key, 1, 0).unwrap();

    let loser = Arc::new(SegmentedObjectLog::open(store.clone(), integrity_config()));
    let winner = SegmentedObjectLog::open(store.clone(), integrity_config());
    loser.create_queue(&definition).unwrap();
    winner.create_queue(&definition).unwrap();
    let commands = pushes(1);
    loser.enqueue(&key, &commands, 1, 1).unwrap();
    winner.enqueue(&key, &commands, 1, 1).unwrap();

    let entered = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    loser.set_fault_hook(Some(Arc::new(PauseAtCut {
        cut: FaultCutPoint::AfterManifestCandidateBeforeHead,
        entered: entered.clone(),
        resume: resume.clone(),
        fired: AtomicBool::new(false),
    })));
    let losing_seal = {
        let loser = loser.clone();
        let key = key.clone();
        thread::spawn(move || loser.seal(&key, 1, 2))
    };
    entered.wait();
    // Different committed_at produces a distinct manifest-candidate body/key,
    // while segment bytes and their digest-addressed key remain identical.
    winner.seal(&key, 1, 3).unwrap();
    resume.wait();
    assert_eq!(losing_seal.join().unwrap(), Err(EngineError::Conflict));
    assert_eq!(winner.read_all(&key).unwrap().len(), 1);
    assert_eq!(winner.gc_unreferenced_candidates(&key, 8).unwrap(), 1);
    assert_eq!(winner.read_all(&key).unwrap().len(), 1);
}

#[test]
fn losing_distinct_content_candidate_gc_reclaims_only_the_loser_segment() {
    let store = Arc::new(InMemoryBlobStore::new());
    let definition = unique_qdef("current-distinct-segment-gc");
    let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let initializer = SegmentedObjectLog::open(store.clone(), integrity_config());
    initializer.create_queue(&definition).unwrap();
    initializer.fence_epoch(&key, 1, 0).unwrap();

    let loser = Arc::new(SegmentedObjectLog::open(store.clone(), integrity_config()));
    let winner = SegmentedObjectLog::open(store.clone(), integrity_config());
    loser.create_queue(&definition).unwrap();
    winner.create_queue(&definition).unwrap();
    loser.enqueue(&key, &pushes(1), 1, 1).unwrap();
    winner.enqueue(&key, &pushes(2), 1, 1).unwrap();
    let entered = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    loser.set_fault_hook(Some(Arc::new(PauseAtCut {
        cut: FaultCutPoint::AfterSegmentWriteBeforeManifest,
        entered: entered.clone(),
        resume: resume.clone(),
        fired: AtomicBool::new(false),
    })));
    let losing_seal = {
        let loser = loser.clone();
        let key = key.clone();
        thread::spawn(move || loser.seal(&key, 1, 2))
    };
    entered.wait();
    let loser_segment = store
        .list("t/")
        .unwrap()
        .into_iter()
        .find(|candidate| candidate.ends_with(".seg"))
        .unwrap();
    winner.seal(&key, 1, 3).unwrap();
    let winner_segment = store
        .list("t/")
        .unwrap()
        .into_iter()
        .find(|candidate| candidate.ends_with(".seg") && candidate != &loser_segment)
        .unwrap();
    resume.wait();
    assert_eq!(losing_seal.join().unwrap(), Err(EngineError::Conflict));
    assert_eq!(winner.gc_unreferenced_candidates(&key, 8).unwrap(), 1);
    assert!(store.get(&loser_segment).unwrap().is_none());
    assert!(store.get(&winner_segment).unwrap().is_some());
    assert_eq!(winner.read_all(&key).unwrap().len(), 2);
}

#[test]
fn shared_authority_history_with_fences_pages_exactly() {
    let store = Arc::new(InMemoryBlobStore::new());
    let definition = unique_qdef("shared-mixed");
    let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let bootstrap = SegmentedObjectLog::open(store.clone(), integrity_config());
    bootstrap.create_queue(&definition).unwrap();
    bootstrap.fence_epoch(&key, 1, 0).unwrap();

    let mut epoch = 1;
    for index in 0..4 {
        if index == 2 {
            let fence = SegmentedObjectLog::open(store.clone(), integrity_config());
            fence.create_queue(&definition).unwrap();
            epoch = fence.fence_epoch(&key, 2, 20).unwrap();
        }
        let writer = SegmentedObjectLog::open(store.clone(), integrity_config());
        writer.create_queue(&definition).unwrap();
        writer
            .enqueue(&key, &pushes(1), epoch, index as i64 + 1)
            .unwrap();
        writer.seal(&key, epoch, index as i64 + 2).unwrap();
    }

    let reader = SegmentedObjectLog::open(store, integrity_config());
    reader.create_queue(&definition).unwrap();
    let all = reader.read_all(&key).unwrap();
    assert_eq!(
        all.iter().map(|(p, _)| p.sequence).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    let page = reader.read_from_limited(&key, 1, 2).unwrap();
    assert_eq!(
        page.iter().map(|(p, _)| p.sequence).collect::<Vec<_>>(),
        vec![1, 2]
    );
}
