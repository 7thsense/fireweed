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

use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_conformance::{envelope, item, qdef, shard};
use pqueue_core::QueueId;
use pqueue_engine::{CommandPosition, EngineError, EngineResult, PushCommand, QueueCommand};
use pqueue_objectlog::segmented::{
    BlobStore, InMemoryBlobStore, ObjectStoreStats, S3BlobStore, SegmentConfig, SegmentedObjectLog,
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

    // Crash-safe order: advance the floor FIRST, then delete the segment objects.
    log.advance_retention_floor(&parent, CommandPosition::new(parent.clone(), 0, 3))
        .unwrap();
    let deleted = log.expire_segments_through(&parent, 3, 30).unwrap();
    assert_eq!(
        deleted, 1,
        "only the UNPINNED below-floor segment (seg1) is reclaimed; the branch-pinned seg0 is skipped"
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
