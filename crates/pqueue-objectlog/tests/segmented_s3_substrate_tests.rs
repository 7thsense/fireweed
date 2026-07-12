//! TD-004 **production** object-log substrate: configurable group-commit segments on an S3-compatible
//! store, ack withheld until segment+manifest commit, manifest-CAS epoch fencing, and release-measurable
//! segment/object counters. The in-memory store exercises the whole pipeline with NO network; the MinIO
//! integration test (env-gated on `PQUEUE_S3_TEST_ENDPOINT`) runs the SAME flow against a real S3 endpoint.
//!
//! ## Running the MinIO integration test (orbstack networking)
//!
//! ```text
//! docker run -d --name pqueue-minio -e MINIO_ROOT_USER=minioadmin \
//!     -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
//! IP=$(docker inspect pqueue-minio --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
//! PQUEUE_S3_TEST_ENDPOINT="http://$IP:9000" \
//!     cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests \
//!     segmented_object_log_commits_through_minio -- --nocapture
//! ```
//!
//! This host CANNOT reach docker *published* ports (`localhost:9000` fails in the orbstack namespace), so the
//! container IP must be used directly. Optional overrides: `PQUEUE_S3_TEST_BUCKET` (default `pqueue-test`),
//! `PQUEUE_S3_TEST_ACCESS_KEY` / `PQUEUE_S3_TEST_SECRET_KEY` (default `minioadmin`). Absent the endpoint env,
//! the test prints a LOUD skip and returns green (mirroring the postgres `PQUEUE_PG_TEST_URL` gate).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use pqueue_conformance::{envelope, item, qdef, shard};
use pqueue_core::QueueId;
use pqueue_engine::{CommandPosition, EngineError, EngineResult, PushCommand, QueueCommand};
use pqueue_objectlog::segmented::{
    BlobStore, FaultCutPoint, FaultHook, InMemoryBlobStore, ObjectStoreStats, S3BlobStore,
    SegmentConfig, SegmentedObjectLog,
};

/// `n` distinct push commands (one item each), so a segment can batch several commands.
fn pushes(n: u64) -> Vec<pqueue_engine::CommandEnvelope> {
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

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn branch_qdef(suffix: &str) -> pqueue_core::QueueDefinition {
    let mut def = qdef();
    def.queue_id = QueueId::new(format!("branch-{}-{suffix}", std::process::id())).unwrap();
    def
}

#[derive(Default)]
struct CountingBlobStore {
    inner: InMemoryBlobStore,
    segment_gets: AtomicU64,
    manifest_gets: AtomicU64,
    list_count: AtomicU64,
}

impl CountingBlobStore {
    fn segment_gets(&self) -> u64 {
        self.segment_gets.load(Ordering::Relaxed)
    }

    fn reset_reads(&self) {
        self.segment_gets.store(0, Ordering::Relaxed);
        self.manifest_gets.store(0, Ordering::Relaxed);
        self.list_count.store(0, Ordering::Relaxed);
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
        if key.contains("/seg/") {
            self.segment_gets.fetch_add(1, Ordering::Relaxed);
        }
        if key.contains("/manifest/") {
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

struct PaginatedListBlobStore {
    inner: InMemoryBlobStore,
    billable_list_requests: u64,
}

impl PaginatedListBlobStore {
    fn new(billable_list_requests: u64) -> Self {
        Self {
            inner: InMemoryBlobStore::new(),
            billable_list_requests,
        }
    }
}

impl BlobStore for PaginatedListBlobStore {
    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
        self.inner.put(key, body)
    }

    fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
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

    fn list_with_request_count(&self, prefix: &str) -> EngineResult<(Vec<String>, u64)> {
        self.inner
            .list(prefix)
            .map(|keys| (keys, self.billable_list_requests))
    }

    fn stats(&self, prefix: &str) -> EngineResult<ObjectStoreStats> {
        self.inner.stats(prefix)
    }
}

#[test]
fn ack_is_withheld_until_segment_and_manifest_commit() {
    // Latency-dominant config: a big byte target so enqueue NEVER size-seals; only an explicit seal commits.
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(InMemoryBlobStore::new(), cfg);
    log.create_queue(&qdef()).unwrap();

    // Buffer three commands below the size threshold: NOT acked, NOT visible to a reader.
    let out = log.enqueue(&shard(), &pushes(3), 0, 1_000).unwrap();
    assert!(out.committed.is_empty(), "buffered commands are not acked");
    assert_eq!(out.pending, 3);
    assert!(
        log.read_all(&shard()).unwrap().is_empty(),
        "an un-sealed command is invisible to the read/recovery path (no manifest entry)"
    );
    assert_eq!(log.counters().segments_sealed, 0);

    // Seal: the segment object + manifest entry commit, and ONLY now are the positions acked + visible.
    let positions = log.seal(&shard(), 0, 1_050).unwrap();
    assert_eq!(positions.len(), 3);
    assert_eq!(
        positions.iter().map(|p| p.sequence).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(log.read_all(&shard()).unwrap().len(), 3, "now visible");
    assert_eq!(log.pending(&shard()), 0);

    let c = log.counters();
    assert_eq!(c.segments_sealed, 1);
    assert_eq!(c.commands_committed, 3);
    assert_eq!(c.group_commit_batches, vec![3]);
    // One segment object + one manifest object PUT.
    assert_eq!(c.objects_put, 2);
}

#[test]
fn list_counter_records_billable_list_requests_not_logical_calls() {
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(PaginatedListBlobStore::new(3), cfg);

    log.create_queue(&qdef()).unwrap();

    assert_eq!(
        log.counters().list_count,
        3,
        "one logical manifest recovery list that spans three object-store pages must count as three billable LIST requests"
    );
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
}

#[test]
fn stale_epoch_writer_is_cas_fenced_with_no_torn_segment() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();

    // Owner A (epoch 0) commits a segment.
    let a = SegmentedObjectLog::open(store.clone(), cfg);
    a.create_queue(&qdef()).unwrap();
    a.enqueue(&shard(), &pushes(2), 0, 0).unwrap();
    a.seal(&shard(), 0, 1).unwrap();
    assert_eq!(a.read_all(&shard()).unwrap().len(), 2);
    let committed_before = a.read_all(&shard()).unwrap().len();
    let objects_before = store.object_count();

    // A new owner B acquires the queue at epoch 1, publishing a fence entry to the manifest.
    let b = SegmentedObjectLog::open(store.clone(), cfg);
    b.create_queue(&qdef()).unwrap();
    let new_epoch = b.acquire_epoch(&shard(), 100).unwrap();
    assert_eq!(new_epoch, 1);
    assert_eq!(b.current_epoch(&shard()).unwrap(), 1);

    // Owner A — still holding the stale epoch 0 — tries to seal another segment. The manifest-CAS epoch
    // fence rejects it: EpochFenced, and NO torn/committed segment is added to the durable log.
    a.enqueue(&shard(), &pushes(3), 0, 200).unwrap();
    let err = a.seal(&shard(), 0, 201).unwrap_err();
    assert_eq!(err, EngineError::EpochFenced, "stale epoch is CAS-fenced");
    assert_eq!(
        a.read_all(&shard()).unwrap().len(),
        committed_before,
        "the fenced seal committed nothing new (no torn segment)"
    );
    // The stale writer may leave one orphan segment object before losing the manifest CAS. Readers never
    // observe it because only manifest-named segments are committed; avoiding a manifest LIST before every
    // seal keeps the single-writer hot path bounded.
    assert_eq!(store.object_count(), objects_before + 2);

    // The new owner B commits under epoch 1 successfully and the log extends.
    b.enqueue(&shard(), &pushes(2), 1, 300).unwrap();
    let pos = b.seal(&shard(), 1, 301).unwrap();
    assert_eq!(pos.len(), 2);
    assert_eq!(pos[0].backend_epoch, 1);
    assert_eq!(
        b.read_all(&shard()).unwrap().len(),
        committed_before + 2,
        "new-epoch owner's commit is durable and contiguous after the stale writer was fenced"
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
fn branch_shares_segments_and_diverges() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let parent = SegmentedObjectLog::open(store.clone(), cfg);
    let parent_def = qdef();
    let parent_shard = shard();
    parent.create_queue(&parent_def).unwrap();

    parent.enqueue(&parent_shard, &pushes(4), 0, 10).unwrap();
    parent.seal(&parent_shard, 0, 11).unwrap();

    let cut = CommandPosition::new(parent_shard.clone(), 0, 1);
    let branch_def = branch_qdef("shares");
    let branch_shard =
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    let branch_epoch = parent
        .branch(&parent_shard, &branch_def, &cut, 60_000, 20)
        .unwrap();
    assert_eq!(branch_epoch, 1);

    let parent_prefix = parent.read_as_of(&parent_shard, &cut).unwrap();
    let branch_prefix = parent.read_as_of(&branch_shard, &cut).unwrap();
    assert_eq!(
        parent_prefix
            .iter()
            .map(|(_, env)| format!("{:?}", env.command))
            .collect::<Vec<_>>(),
        branch_prefix
            .iter()
            .map(|(_, env)| format!("{:?}", env.command))
            .collect::<Vec<_>>(),
        "branch view matches the parent at the cut position"
    );

    let source_seg_key = format!(
        "t/{}/q/{}/seg/00000000000000000000.seg",
        hex_lower(parent_shard.tenant_id.as_str().as_bytes()),
        hex_lower(parent_shard.queue_id.as_str().as_bytes())
    );
    assert!(
        store.get(&source_seg_key).unwrap().is_some(),
        "parent segment remains stored"
    );
    assert!(
        store
            .list(&format!(
                "t/{}/q/{}/seg/",
                hex_lower(branch_shard.tenant_id.as_str().as_bytes()),
                hex_lower(branch_shard.queue_id.as_str().as_bytes())
            ))
            .unwrap()
            .is_empty(),
        "branch creation does not duplicate parent segment objects"
    );

    parent.enqueue(&parent_shard, &pushes(1), 0, 30).unwrap();
    parent.seal(&parent_shard, 0, 31).unwrap();
    parent
        .enqueue(&branch_shard, &pushes(1), branch_epoch, 32)
        .unwrap();
    parent.seal(&branch_shard, branch_epoch, 33).unwrap();

    let parent_tail = parent.read_all(&parent_shard).unwrap();
    let branch_tail = parent.read_all(&branch_shard).unwrap();
    assert_eq!(parent_tail.len(), 5);
    assert_eq!(branch_tail.len(), 3);
    assert_ne!(
        parent_tail
            .iter()
            .map(|(_, env)| format!("{:?}", env.command))
            .collect::<Vec<_>>(),
        branch_tail
            .iter()
            .map(|(_, env)| format!("{:?}", env.command))
            .collect::<Vec<_>>(),
        "post-cut writes diverge independently"
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
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
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
        !log.branch_emits_change_records(&pqueue_engine::QueueKey::new(
            branch_def.tenant_id.clone(),
            branch_def.queue_id.clone(),
        ))
        .unwrap(),
        "branch activity is emission-suppressed unless explicitly enabled"
    );
}

#[test]
fn branch_pins_parent_segments_against_expiry() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let parent_def = qdef();
    let parent_shard = shard();
    log.create_queue(&parent_def).unwrap();
    log.enqueue(&parent_shard, &pushes(4), 0, 10).unwrap();
    log.seal(&parent_shard, 0, 11).unwrap();

    let branch_def = branch_qdef("pins");
    let branch_shard =
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    log.branch(
        &parent_shard,
        &branch_def,
        &CommandPosition::new(parent_shard.clone(), 0, 1),
        60_000,
        20,
    )
    .unwrap();

    let deleted = log.expire_segments_through(&parent_shard, 3, 21).unwrap();
    assert_eq!(
        deleted, 0,
        "live branch pins parent segments against expiry"
    );

    let parent_seg_key = format!(
        "t/{}/q/{}/seg/00000000000000000000.seg",
        hex_lower(parent_shard.tenant_id.as_str().as_bytes()),
        hex_lower(parent_shard.queue_id.as_str().as_bytes())
    );
    assert!(
        store.get(&parent_seg_key).unwrap().is_some(),
        "pinned segment remains present while the branch is live"
    );

    log.discard_branch(&parent_shard, &branch_shard).unwrap();
    let deleted_after = log.expire_segments_through(&parent_shard, 3, 22).unwrap();
    assert_eq!(deleted_after, 1, "discarding the branch releases the pin");
    assert!(
        store.get(&parent_seg_key).unwrap().is_none(),
        "expired parent segment is removed once no branch references it"
    );
}

/// Test 7 (bead pqueue-b5cc2bc7 — branch-pin safety of segment reclamation): a live branch pinning a
/// below-floor segment keeps that segment object alive through a trim (`expire_segments_through` skips it via
/// `branch_pins_segment`), while an unpinned below-floor segment IS reclaimed. Separately, a NEW branch cut at
/// or below the durable retention floor is rejected CLEANLY (a fast `Invalid`, not a later "missing segment").
#[test]
fn retention_floor_trim_respects_branch_pins_and_rejects_below_floor_cuts() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let parent = shard();
    log.create_queue(&qdef()).unwrap();

    // Three segments at distinct commit times: seg0[0..1]@10ms, seg1[2..3]@20ms (both OLD), seg2[4..5]@1000ms
    // (fresh). One enqueue+seal each.
    log.enqueue(&parent, &pushes(2), 0, 10).unwrap();
    log.seal(&parent, 0, 10).unwrap();
    log.enqueue(&parent, &pushes(2), 0, 20).unwrap();
    log.seal(&parent, 0, 20).unwrap();
    log.enqueue(&parent, &pushes(2), 0, 1000).unwrap();
    log.seal(&parent, 0, 1000).unwrap();

    // A live branch cut inside seg0 (position seq 1) pins seg0 (first_seq 0 <= cut 1).
    let branch_def = branch_qdef("floor-pin");
    let branch_shard =
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    log.branch(
        &parent,
        &branch_def,
        &CommandPosition::new(parent.clone(), 0, 1),
        60_000,
        5,
    )
    .unwrap();

    // Retention horizon at cutoff=500ms: seg0(10) + seg1(20) are expired (max visible_last_seq = 3); seg2(1000)
    // is fresh and stops the prefix scan.
    let horizon = log.max_trimmable_seq_before(&parent, 500).unwrap();
    assert_eq!(horizon, Some(3), "the expired-segment prefix ends at seq 3");

    // Crash-safe order: advance the floor FIRST (epoch-fenced at the current epoch 0), then delete.
    log.advance_retention_floor(&parent, CommandPosition::new(parent.clone(), 0, 3), 0)
        .unwrap();
    let deleted = log.expire_segments_through(&parent, 3, 30).unwrap();
    assert_eq!(
        deleted, 1,
        "only the UNPINNED below-floor segment (seg1) is reclaimed; the branch-pinned seg0 is skipped"
    );
    // The pinned seg0 is reported so the composed trim caller holds its completed-deletion watermark BELOW it
    // (a released pin must be re-scanned, not skipped forever — bug 2b).
    assert_eq!(
        log.lowest_branch_pinned_below(&parent, 3, 30).unwrap(),
        Some(0),
        "the branch-pinned below-floor segment (first_seq 0) is reported while the pin is live"
    );

    let seg_key = |first: u64| {
        format!(
            "t/{}/q/{}/seg/{first:020}.seg",
            hex_lower(parent.tenant_id.as_str().as_bytes()),
            hex_lower(parent.queue_id.as_str().as_bytes())
        )
    };
    assert!(
        store.get(&seg_key(0)).unwrap().is_some(),
        "the branch-pinned below-floor segment survives the trim"
    );
    assert!(
        store.get(&seg_key(2)).unwrap().is_none(),
        "the unpinned below-floor segment is reclaimed"
    );
    assert!(
        store.get(&seg_key(4)).unwrap().is_some(),
        "the fresh (above-floor) tail segment survives"
    );

    // Reading from the floor (seq 3 -> from_seq 4) is contiguous with NO missing-segment error.
    let tail = log.read_from(&parent, 4).unwrap();
    assert_eq!(tail.len(), 2, "the tail segment's two commands read back");
    assert!(
        tail.iter().all(|(pos, _)| pos.sequence >= 4),
        "no below-floor position is surfaced"
    );

    // A NEW branch cut at or below the floor (seq 3) is rejected cleanly.
    let below = branch_qdef("below-floor");
    let err = log
        .branch(
            &parent,
            &below,
            &CommandPosition::new(parent.clone(), 0, 3),
            60_000,
            40,
        )
        .unwrap_err();
    assert!(
        matches!(err, EngineError::Invalid(_)),
        "a branch cut at/below the retention floor is rejected cleanly (Invalid), got {err:?}"
    );

    // The original pin is still honored (branch is live); discarding it releases seg0 for the next trim.
    log.discard_branch(&parent, &branch_shard).unwrap();
    assert_eq!(
        log.lowest_branch_pinned_below(&parent, 3, 41).unwrap(),
        None,
        "after the pin releases nothing below the floor is pinned (the watermark can settle at the floor)"
    );
    let deleted_after = log.expire_segments_through(&parent, 3, 41).unwrap();
    assert_eq!(
        deleted_after, 1,
        "discarding the branch releases seg0 to be reclaimed"
    );
    assert!(
        store.get(&seg_key(0)).unwrap().is_none(),
        "seg0 is reclaimed once the pin is gone"
    );
}

/// Test (bead pqueue-b5cc2bc7 bug 3 — cross-owner floor is an ATOMIC epoch-fenced MANIFEST CAS): the composed
/// unit-of-work lock is process-LOCAL and cannot fence a peer owner, so the floor advance is routed through the
/// same create-only, epoch-fenced manifest CAS as data segments and epoch fences. A superseded owner
/// interleaved with a new owner's higher floor advance is rejected (EpochFenced / CAS-lost), the floor stays at
/// the higher value, and — after the new owner reclaims through it — recovery reads NO missing segment.
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
    log.advance_retention_floor(&shard, CommandPosition::new(shard.clone(), 0, 1), 0)
        .unwrap();

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
    log.expire_segments_through(&shard, 3, 21).unwrap();

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

/// Test (bead pqueue-b5cc2bc7 bug 1 — GROUP-COMMIT batch-max seal timestamp): when several pushes co-buffer
/// into ONE segment and a LATER push has a SMALLER `created_at` than an earlier buffered one, the sealed
/// segment's `committed_at_ms` is the batch MAX (not the triggering call's `now_ms`), so a cutoff between the
/// two does NOT age-trim the segment while the earlier push is still within retention.
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

/// A single-item Push envelope with an explicit `created_at` of `created_secs` seconds (bug 1 group-commit
/// test): returns a one-element Vec so it can be passed by slice to `enqueue`.
fn envelope_created_at(created_secs: i64) -> Vec<pqueue_engine::CommandEnvelope> {
    let mut env = envelope(
        QueueCommand::Push(PushCommand {
            items: vec![item("0", "k0", 5)],
        }),
        vec![],
    );
    env.created_at = pqueue_core::UtcTimestamp::new(created_secs, 0).unwrap();
    vec![env]
}

/// A fault hook that, the FIRST time a seal reaches `BeforeSegmentWrite` (after it has drained + released the
/// mutex), enqueues envelope B into the SAME buffer — modelling a concurrent enqueue interleaving an in-flight
/// seal (bead pqueue-b5cc2bc7 HOLE A). B carries created_at=100s.
struct EnqueueDuringSeal {
    log: std::sync::Weak<SegmentedObjectLog<InMemoryBlobStore>>,
    shard: pqueue_engine::QueueKey,
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

/// HOLE A (bead pqueue-b5cc2bc7 — group-commit committed_at is race-free): with the OLD resettable running
/// `max_created_ms`, a concurrent enqueue during an in-flight seal raised the counter and the seal's completion
/// then UNCONDITIONALLY reset it to 0 while the new command was still buffered, so THAT command's later seal
/// stamped `committed_at_ms < its created_at` (a within-retention request_id could be age-trimmed). With each
/// buffered command carrying its OWN `created_at`, every seal derives `committed_at_ms` from the batch it holds
/// in hand — no shared counter to clobber.
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

/// A fault hook that runs a concurrent PEER TRIM (advance the source floor + reclaim) exactly once, when a
/// branch reaches `DuringBranchCopy` — after it has published its source pin + read the source floor but
/// before it copies (bead pqueue-b5cc2bc7 HOLE B, branch-vs-concurrent-trim).
struct PeerTrimDuringBranch {
    log: std::sync::Weak<SegmentedObjectLog<InMemoryBlobStore>>,
    source: pqueue_engine::QueueKey,
    new_floor: u64,
    now_ms: i64,
    fired: AtomicBool,
}

impl FaultHook for PeerTrimDuringBranch {
    fn fault_point(&self, cut: FaultCutPoint) -> EngineResult<()> {
        if cut == FaultCutPoint::DuringBranchCopy
            && !self.fired.swap(true, Ordering::SeqCst)
            && let Some(log) = self.log.upgrade()
        {
            // Peer trim: advance the source floor (epoch-fenced manifest CAS) then reclaim. The branch's pin is
            // already published, so `expire_segments_through` SKIPS the pinned segments — nothing is deleted.
            log.advance_retention_floor(
                &self.source,
                CommandPosition::new(self.source.clone(), 0, self.new_floor),
                0,
            )
            .unwrap();
            log.expire_segments_through(&self.source, self.new_floor, self.now_ms)
                .unwrap();
        }
        Ok(())
    }
}

/// HOLE B cross-owner (bead pqueue-b5cc2bc7 — branch vs CONCURRENT trim): a peer that advances the source floor
/// and reclaims WHILE a branch is being created must never yield a corrupt/missing-segment branch. Pin-first
/// makes the peer's `expire` SKIP the branched range (no data deleted); validate-after-copy detects the floor
/// movement and FAILS the attempt cleanly with a full rollback. The peer here advances the floor to 5 — EQUAL
/// to the cut (5) — so the bounded transparent retry (bead pqueue-9dcec223) re-reads the advanced floor and
/// cleanly REJECTS the now cut<=floor view as `Invalid` ("whole view reclaimed"), NOT a bare Conflict. The
/// safety invariants are unchanged: no data deleted during the race, source fully reclaimable, no leaked pin.
#[test]
fn branch_over_a_concurrently_trimming_source_fails_cleanly_with_no_missing_segment() {
    let log = std::sync::Arc::new(SegmentedObjectLog::open(
        InMemoryBlobStore::new(),
        SegmentConfig::new(10_000_000, 100).unwrap(),
    ));
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    for _ in 0..6 {
        log.enqueue(&source, &pushes(1), 0, 10).unwrap();
        log.seal(&source, 0, 10).unwrap();
    }

    // Arm the concurrent peer trim (advance source floor to 5 + reclaim) to fire DURING branch creation.
    log.set_fault_hook(Some(std::sync::Arc::new(PeerTrimDuringBranch {
        log: std::sync::Arc::downgrade(&log),
        source: source.clone(),
        new_floor: 5,
        now_ms: 100,
        fired: AtomicBool::new(false),
    })));

    let branch_def = branch_qdef("concurrent-trim");
    let branch =
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    let result = log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 5),
        1_000_000_000,
        100,
    );
    log.set_fault_hook(None);

    // The branch creation FAILS CLEANLY — NEVER a corrupt/missing-segment branch. With transparent retry the
    // peer's floor advance to 5 (== cut) is re-read and the now cut<=floor view is cleanly rejected as Invalid.
    assert!(
        matches!(result, Err(EngineError::Invalid(_))),
        "a branch whose cut becomes <= the advanced floor is rejected cleanly (Invalid), got {result:?}"
    );
    // NO data loss: the branch pin protected the segments during the peer's expire, so all six source segment
    // objects are intact — read_all(source) GETs them with NO missing segment.
    assert_eq!(
        log.read_all(&source).unwrap().len(),
        6,
        "the pin protected every branched segment during the concurrent expire — no object was deleted"
    );
    // The partial branch is fully rolled back, leaving NO lingering source pin: a subsequent trim (the floor is
    // now 5) reclaims all six segments, proving the rollback released the pin.
    assert_eq!(
        log.expire_segments_through(&source, 5, 200).unwrap(),
        6,
        "the rolled-back branch left no lingering pin — the source is fully reclaimable afterward"
    );
    assert!(
        log.read_retention_floor(&branch).unwrap().is_none(),
        "the failed branch was rolled back (no branch floor / manifest remains)"
    );
}

/// A fault hook that runs a concurrent PEER TRIM every time a branch attempt reaches `DuringBranchCopy`, up to
/// `advances_remaining` times, advancing the source floor by ONE more each fire (monotonically: 1, 2, 3, ...)
/// then reclaiming through it. Set `advances_remaining` to a small number for a BOUNDED race (the peer stops,
/// so a later retry sees a STABLE floor and SUCCEEDS) or a large number for CONTINUOUS trimming (every attempt
/// races, so the bounded retry gives up cleanly). `next_floor` starts at 1 so the first advance is a real move.
struct PeerTrimBoundedAdvances {
    log: std::sync::Weak<SegmentedObjectLog<InMemoryBlobStore>>,
    source: pqueue_engine::QueueKey,
    now_ms: i64,
    advances_remaining: AtomicU64,
    next_floor: AtomicU64,
}

impl FaultHook for PeerTrimBoundedAdvances {
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
            log.advance_retention_floor(
                &self.source,
                CommandPosition::new(self.source.clone(), 0, floor),
                0,
            )
            .unwrap();
            log.expire_segments_through(&self.source, floor, self.now_ms)
                .unwrap();
        }
        Ok(())
    }
}

/// Bead pqueue-9dcec223 (a): a branch racing a BOUNDED concurrent source-floor advance RETRIES and SUCCEEDS.
/// The peer advances the floor a fixed number of times (to 1, then 2) then stops; the bounded retry re-reads
/// the advanced floor each attempt and, once the peer stops, commits a valid branch against the retained range
/// — `read_all(branch)` returns the expected retained commands with NO missing segment.
#[test]
fn branch_retries_a_bounded_concurrent_floor_advance_and_succeeds() {
    let log = std::sync::Arc::new(SegmentedObjectLog::open(
        InMemoryBlobStore::new(),
        SegmentConfig::new(10_000_000, 100).unwrap(),
    ));
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    // Six single-command segments, seq 0..5.
    for _ in 0..6 {
        log.enqueue(&source, &pushes(1), 0, 10).unwrap();
        log.seal(&source, 0, 10).unwrap();
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
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    // Cut at 5 stays ABOVE the peer's final floor (2), so the retry SUCCEEDS (transparently — the caller never
    // sees the intermediate Conflicts).
    log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 5),
        1_000_000_000,
        100,
    )
    .expect("the bounded concurrent-trim race is retried transparently and succeeds");
    log.set_fault_hook(None);

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

/// Bead pqueue-9dcec223 (b): under CONTINUOUS trimming the branch GIVES UP after the cap and returns `Conflict`
/// CLEANLY (no livelock, no leaked pin). The peer advances the floor on EVERY attempt, so validate-after-copy
/// conflicts every time; after the bounded cap the last Conflict is surfaced and the source stays fully
/// reclaimable (the final attempt rolled back its pin).
#[test]
fn branch_gives_up_cleanly_under_continuous_trimming() {
    let log = std::sync::Arc::new(SegmentedObjectLog::open(
        InMemoryBlobStore::new(),
        SegmentConfig::new(10_000_000, 100).unwrap(),
    ));
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    // Eight single-command segments, seq 0..7 — headroom so the cut stays above the floor for every attempt.
    for _ in 0..8 {
        log.enqueue(&source, &pushes(1), 0, 10).unwrap();
        log.seal(&source, 0, 10).unwrap();
    }

    // Peer advances the floor on EVERY attempt (budget far exceeds the retry cap) — continuous trimming.
    log.set_fault_hook(Some(std::sync::Arc::new(PeerTrimBoundedAdvances {
        log: std::sync::Arc::downgrade(&log),
        source: source.clone(),
        now_ms: 100,
        advances_remaining: AtomicU64::new(1_000),
        next_floor: AtomicU64::new(1),
    })));

    let branch_def = branch_qdef("continuous-trim-giveup");
    let branch =
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    // Cut at 7 stays above the floor the peer reaches within the cap, so the give-up is a Conflict (not Invalid).
    let result = log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 7),
        1_000_000_000,
        100,
    );
    log.set_fault_hook(None);

    assert!(
        matches!(result, Err(EngineError::Conflict)),
        "continuous trimming makes the bounded retry give up cleanly with Conflict (no livelock), got {result:?}"
    );
    // No leaked pin: the final attempt rolled back, so the source is FULLY reclaimable afterward.
    assert_eq!(
        log.expire_segments_through(&source, 7, 200).unwrap(),
        8,
        "the given-up branch left no lingering pin — all eight source segments are reclaimable"
    );
    assert!(
        log.read_retention_floor(&branch).unwrap().is_none(),
        "no partial branch remains after the bounded give-up"
    );
}

/// Bead pqueue-9dcec223 (c): a cut that is BELOW the advanced floor is rejected with `Invalid`, NOT retried as
/// a Conflict. The peer advances the floor to 3 (bounded, then stops); a branch cut at 2 is now at/below the
/// advanced floor, so it is cleanly rejected ("whole view reclaimed") rather than looping.
#[test]
fn branch_cut_below_advanced_floor_is_rejected_invalid_not_retried() {
    let log = std::sync::Arc::new(SegmentedObjectLog::open(
        InMemoryBlobStore::new(),
        SegmentConfig::new(10_000_000, 100).unwrap(),
    ));
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    for _ in 0..6 {
        log.enqueue(&source, &pushes(1), 0, 10).unwrap();
        log.seal(&source, 0, 10).unwrap();
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

/// A [`BlobStore`] wrapper that injects a store failure on the first `put` / `put_if_absent` / `delete` whose
/// key contains an armed substring (bead pqueue-b5cc2bc7 error-path tests). Reads are never failed.
#[derive(Default)]
struct FailingBlobStore {
    inner: InMemoryBlobStore,
    fail_put: std::sync::Mutex<Option<String>>,
    fail_put_if_absent: std::sync::Mutex<Option<String>>,
    fail_delete: std::sync::Mutex<Option<String>>,
}

impl FailingBlobStore {
    fn arm_put(&self, substr: &str) {
        *self.fail_put.lock().unwrap() = Some(substr.to_string());
    }
    fn arm_put_if_absent(&self, substr: &str) {
        *self.fail_put_if_absent.lock().unwrap() = Some(substr.to_string());
    }
    fn arm_delete(&self, substr: &str) {
        *self.fail_delete.lock().unwrap() = Some(substr.to_string());
    }
    fn disarm(&self) {
        *self.fail_put.lock().unwrap() = None;
        *self.fail_put_if_absent.lock().unwrap() = None;
        *self.fail_delete.lock().unwrap() = None;
    }
    fn armed(lock: &std::sync::Mutex<Option<String>>, key: &str) -> bool {
        lock.lock()
            .unwrap()
            .as_deref()
            .is_some_and(|s| key.contains(s))
    }
}

impl BlobStore for FailingBlobStore {
    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
        if Self::armed(&self.fail_put, key) {
            return Err(EngineError::Storage(format!("injected put failure: {key}")));
        }
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
        if Self::armed(&self.fail_delete, key) {
            return Err(EngineError::Storage(format!(
                "injected delete failure: {key}"
            )));
        }
        self.inner.delete(key)
    }
    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        self.inner.list(prefix)
    }
}

/// Drive a branch creation whose POST-PIN stage hits a store failure (armed via `arm`), then assert the
/// error-path safety invariants (bead pqueue-b5cc2bc7): (a) branch creation FAILS and NO missing-segment
/// branch is ever left (the source segments the branch referenced stay intact); (b/c) the source PIN is
/// released iff cleanup completed — a clean rollback leaves the source fully reclaimable, while a
/// cleanup-stage failure RETAINS the pin (safe leak) so a reclamation can never delete a referenced segment.
fn check_post_pin_store_failure(
    arm: impl Fn(&FailingBlobStore),
    expect_source_reclaimable_after: bool,
) {
    let store = std::sync::Arc::new(FailingBlobStore::default());
    let log = SegmentedObjectLog::open(
        std::sync::Arc::clone(&store),
        SegmentConfig::new(10_000_000, 100).unwrap(),
    );
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    for _ in 0..6 {
        log.enqueue(&source, &pushes(1), 0, 10).unwrap();
        log.seal(&source, 0, 10).unwrap();
    }

    arm(&store);
    let branch_def = branch_qdef("post-pin-fail");
    let branch =
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    let result = log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 5),
        1_000_000_000,
        100,
    );
    store.disarm();

    // (a) branch creation FAILED under the store fault.
    assert!(
        result.is_err(),
        "a post-pin store failure must fail branch creation, got {result:?}"
    );
    // (a) ATOMIC EXISTENCE: the failed branch never wrote its `branch.json` commit marker, so it is
    // NON-READABLE — read_all/read_from return EMPTY (never a missing-segment GET), regardless of what partial
    // objects survived cleanup.
    assert!(
        log.read_all(&branch).unwrap().is_empty(),
        "a partial (uncommitted) branch is non-readable — read_all returns empty, never a missing segment"
    );
    assert!(
        log.read_from(&branch, 0).unwrap().is_empty(),
        "a partial branch read_from is empty (non-existent)"
    );
    // (a) NO missing-segment branch: every source segment the branch referenced is intact + readable.
    assert_eq!(
        log.read_all(&source).unwrap().len(),
        6,
        "all six source segments remain readable — no missing segment under any post-pin failure"
    );
    // (b)/(c) the pin is released iff the rollback's branch-object cleanup completed.
    let deleted = log.expire_segments_through(&source, 5, 200).unwrap();
    if expect_source_reclaimable_after {
        assert_eq!(
            deleted, 6,
            "a clean rollback released the source pin — the source is fully reclaimable afterward"
        );
    } else {
        assert_eq!(
            deleted, 0,
            "cleanup failed -> the source pin is RETAINED (a safe, TTL-bounded leak); the source segments stay protected, never deleted out from under the branch"
        );
    }
}

/// Post-pin store failure at the MANIFEST-COPY stage -> clean rollback, pin released, source reclaimable.
#[test]
fn branch_manifest_copy_store_failure_rolls_back_and_releases_the_pin() {
    check_post_pin_store_failure(|s| s.arm_put("/manifest/"), true);
}

/// Post-pin store failure at the BRANCH.JSON-PUT stage -> clean rollback, pin released, source reclaimable.
#[test]
fn branch_metadata_put_store_failure_rolls_back_and_releases_the_pin() {
    check_post_pin_store_failure(|s| s.arm_put("branch.json"), true);
}

/// Post-pin store failure at the ACQUIRE-EPOCH stage (a `put_if_absent`) -> clean rollback, pin released.
#[test]
fn branch_acquire_epoch_store_failure_rolls_back_and_releases_the_pin() {
    check_post_pin_store_failure(|s| s.arm_put_if_absent("/manifest/"), true);
}

/// Post-pin failure PLUS a branch-object-CLEANUP failure -> the pin is RETAINED (safe leak); the source is
/// NOT reclaimable (its segments stay protected — never an unpinned partial branch / missing segment).
#[test]
fn branch_object_cleanup_store_failure_retains_the_pin() {
    check_post_pin_store_failure(
        |s| {
            s.arm_put("branch.json"); // trigger the post-pin failure
            s.arm_delete("/manifest/"); // and fail the rollback's branch-object cleanup
        },
        false,
    );
}

/// Post-pin failure where branch-object cleanup SUCCEEDS but the PIN delete fails -> the pin is RETAINED
/// (safe leak: branch objects gone, source still protected), never released prematurely.
#[test]
fn branch_pin_delete_store_failure_retains_the_pin() {
    check_post_pin_store_failure(
        |s| {
            s.arm_put("branch.json"); // trigger the post-pin failure
            s.arm_delete("branches/"); // branch objects delete fine; the PIN delete (registry key) fails LAST
        },
        false,
    );
}

/// THE COMPOUND CORRUPTION SCENARIO (bead pqueue-b5cc2bc7, codex round-7): a double store fault — the
/// `branch.json` commit-marker PUT fails AND the rollback's branch-object cleanup fails — leaves a PARTIAL
/// branch (manifest entries present, NO commit marker) protected only by a TTL-bounded pin. Even after the pin
/// LAPSES at TTL and a trim reclaims the source segments, the partial branch is NON-READABLE (atomic existence
/// gate), so read_all/read_from return empty — NEVER a missing segment. This closes the entire failed-branch
/// class structurally, independent of pin/TTL/cleanup.
#[test]
fn compound_marker_and_cleanup_failure_then_ttl_and_trim_never_yields_a_missing_segment() {
    let store = std::sync::Arc::new(FailingBlobStore::default());
    let log = SegmentedObjectLog::open(
        std::sync::Arc::clone(&store),
        SegmentConfig::new(10_000_000, 100).unwrap(),
    );
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    for _ in 0..6 {
        log.enqueue(&source, &pushes(1), 0, 10).unwrap();
        log.seal(&source, 0, 10).unwrap();
    }

    // Double fault: the commit-marker PUT fails, AND the rollback's branch-object cleanup fails.
    store.arm_put("branch.json");
    store.arm_delete("/manifest/");
    let branch_def = branch_qdef("compound");
    let branch =
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    let ttl_ms: u64 = 1_000;
    let created_now = 100i64;
    let result = log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 5),
        ttl_ms,
        created_now,
    );
    store.disarm();
    assert!(result.is_err(), "the double-fault branch creation fails");

    // The partial branch (marker absent) is NON-READABLE right now, even though its manifest entries + pin
    // survived the failed cleanup.
    assert!(
        log.read_all(&branch).unwrap().is_empty(),
        "the partial branch is non-readable immediately after the compound failure"
    );

    // TTL passes: the pin (expires_at = created_now + ttl) lapses, so a trim on the source now reclaims the
    // segments the partial branch's manifest still references.
    let now_after_ttl = created_now + ttl_ms as i64 + 1;
    log.advance_retention_floor(&source, CommandPosition::new(source.clone(), 0, 5), 0)
        .unwrap();
    assert_eq!(
        log.expire_segments_through(&source, 5, now_after_ttl)
            .unwrap(),
        6,
        "after the TTL lapses the pin no longer protects the source — all six segments are reclaimed"
    );

    // THE CRUX: the partial branch is STILL non-readable, so it never GETs the now-deleted source segments —
    // NO missing segment EVER, regardless of pin/TTL/cleanup outcome.
    assert!(
        log.read_all(&branch).unwrap().is_empty(),
        "the uncommitted branch stays non-readable after TTL + trim — NO missing segment is ever surfaced"
    );
    assert!(
        log.read_from(&branch, 0).unwrap().is_empty(),
        "read_from(uncommitted branch) is empty after TTL + trim"
    );
}

/// A fully-COMMITTED branch (commit marker present) is readable and its live pin protects its referenced
/// source segments (bead pqueue-b5cc2bc7 — the committed path is unchanged by the atomic-existence gate).
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
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
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
        log.expire_segments_through(&source, 5, 200).unwrap(),
        0,
        "the committed branch's live pin protects the source segments from reclamation"
    );
}

/// HOLE B (bead pqueue-b5cc2bc7 — branch inherits the source floor; no missing segment): a branch cut ABOVE a
/// trimmed source floor must copy ONLY the retained (at/above-floor) segments and inherit the floor, so
/// `read_all(branch)` never GETs a reclaimed object. A cut at/below the floor is rejected cleanly.
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
    log.advance_retention_floor(&source, CommandPosition::new(source.clone(), 0, 3), 0)
        .unwrap();
    assert_eq!(
        log.expire_segments_through(&source, 3, 20).unwrap(),
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
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
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

#[test]
fn counters_surface_emits_a_release_ledger_row() {
    // Drive several group-commit segments, then emit the measured segment/object counts to the release
    // ledger harness (the same JSONL surface the E3 object-log evidence row uses).
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(InMemoryBlobStore::new(), cfg);
    log.create_queue(&qdef()).unwrap();
    for batch in [10u64, 25, 7, 18] {
        log.enqueue(&shard(), &pushes(batch), 0, 0).unwrap();
        log.seal(&shard(), 0, 1).unwrap();
    }
    let c = log.counters();
    assert_eq!(c.segments_sealed, 4);
    assert_eq!(c.commands_committed, 60);
    assert_eq!(c.group_commit_batches, vec![10, 25, 7, 18]);
    // objects_put = 4 segment objects + 4 manifest objects.
    assert_eq!(c.objects_put, 8);

    let row = pqueue_release::LedgerRow {
        suite: "segmented_s3_substrate_tests".into(),
        command: "cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests".into(),
        backend_profile: "object_log_sqlite_projection".into(),
        scale: "in-process-smoke".into(),
        seed: 0,
        environment:
            "in-memory BlobStore substrate (no network); group-commit segments, manifest-CAS epoch fence; \
             the live MinIO run is gated on PQUEUE_S3_TEST_ENDPOINT"
                .into(),
        exit_status: 0,
        ac_ids: vec![],
        inv_ids: vec![],
        pass_bar: "ack only after segment+manifest commit; durable-commit cost scales with segments not commands"
            .into(),
        evidence_tier: "smoke".into(),
        measurements: pqueue_release::Measurements {
            tp002_evidence_ids: vec!["E3".into()],
            values: std::collections::BTreeMap::from([
                ("segments_sealed".into(), serde_json::json!(c.segments_sealed)),
                ("objects_put".into(), serde_json::json!(c.objects_put)),
                ("commands_committed".into(), serde_json::json!(c.commands_committed)),
                ("mean_commands_per_segment".into(), serde_json::json!(c.mean_batch_size())),
                ("max_group_commit_batch".into(), serde_json::json!(c.max_batch_size())),
            ]),
        },
    };
    let path =
        pqueue_release::ledger_path(env!("CARGO_MANIFEST_DIR"), "segmented_s3_substrate_tests");
    let _ = std::fs::remove_file(&path);
    pqueue_release::append_row(&path, &row).expect("emit segment-counter ledger row");
    let summary = pqueue_release::verify_ledger(&path, true).expect("emitted row validates strict");
    assert!(summary.smoke_evidence_ids.contains("E3"));
}

/// LIVE MinIO integration (env-gated on `PQUEUE_S3_TEST_ENDPOINT`; LOUD skip otherwise). Runs the full
/// substrate against a real S3-compatible endpoint: group-commit segments, ack-after-manifest-commit, the
/// create-only manifest CAS, the epoch fence, recovery, and the measured counters.
#[test]
fn segmented_object_log_commits_through_minio() {
    let Ok(endpoint) = std::env::var("PQUEUE_S3_TEST_ENDPOINT") else {
        eprintln!(
            "\n================================================================\n\
             MINIO INTEGRATION SKIPPED (segmented_object_log_commits_through_minio)\n\
             set PQUEUE_S3_TEST_ENDPOINT=http://<container-ip>:9000 to run it.\n\
             (this host cannot reach docker PUBLISHED ports; use the container IP)\n\
             ================================================================\n"
        );
        return;
    };
    let bucket = std::env::var("PQUEUE_S3_TEST_BUCKET").unwrap_or_else(|_| "pqueue-test".into());
    let access = std::env::var("PQUEUE_S3_TEST_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into());
    let secret = std::env::var("PQUEUE_S3_TEST_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into());

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
    def.queue_id = pqueue_core::QueueId::new(format!("minio-{}", std::process::id())).unwrap();
    let shard = pqueue_engine::QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());

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
