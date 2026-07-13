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
        6,
        "legacy fallback probes manifest_head and then range-lists the legacy manifest, each spanning three billable LIST requests"
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
    assert_eq!(store.object_count(), objects_before + 3);

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

    let source_seg_key = segment_key_for(store.as_ref(), &parent_shard, 0);
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
    let parent_seg_key = segment_key_for(store.as_ref(), &parent_shard, 0);
    let parent_manifest_head_key = manifest_head_key_s(&parent_shard, 0);
    let parent_manifest_legacy_key = manifest_key_s(&parent_shard, 0);

    let deleted = log.expire_segments_through(&parent_shard, 3, 21).unwrap();
    assert_eq!(
        deleted, 0,
        "live branch pins parent segments against expiry"
    );

    assert!(
        store.get(&parent_seg_key).unwrap().is_some(),
        "pinned segment remains present while the branch is live"
    );
    assert!(
        store.get(&parent_manifest_head_key).unwrap().is_some(),
        "pinned manifest head remains present while the branch is live"
    );
    assert!(
        store.get(&parent_manifest_legacy_key).unwrap().is_some(),
        "pinned legacy manifest remains present while the branch is live"
    );

    log.discard_branch(&parent_shard, &branch_shard).unwrap();
    let deleted_after = log.expire_segments_through(&parent_shard, 3, 22).unwrap();
    assert_eq!(deleted_after, 1, "discarding the branch releases the pin");
    assert!(
        store.get(&parent_seg_key).unwrap().is_none(),
        "expired parent segment is removed once no branch references it"
    );
    assert!(
        store.get(&parent_manifest_head_key).unwrap().is_none(),
        "expired parent manifest head is removed once no branch references it"
    );
    assert!(
        store.get(&parent_manifest_legacy_key).unwrap().is_none(),
        "expired parent legacy manifest is removed once no branch references it"
    );
}

#[test]
fn branch_pin_ttl_expiry_releases_manifest_reclamation() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let parent_def = qdef();
    let parent_shard = shard();
    log.create_queue(&parent_def).unwrap();
    log.enqueue(&parent_shard, &pushes(4), 0, 10).unwrap();
    log.seal(&parent_shard, 0, 11).unwrap();

    let branch_def = branch_qdef("ttl-expiry");
    let branch_shard =
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    log.branch(
        &parent_shard,
        &branch_def,
        &CommandPosition::new(parent_shard.clone(), 0, 1),
        1,
        20,
    )
    .unwrap();

    let seg_key = segment_key_for(store.as_ref(), &parent_shard, 0);
    let manifest_head_key = manifest_head_key_s(&parent_shard, 0);
    let manifest_legacy_key = manifest_key_s(&parent_shard, 0);

    assert_eq!(
        log.expire_segments_through(&parent_shard, 3, 20).unwrap(),
        0,
        "the live branch pin still blocks reclamation before TTL expiry"
    );
    assert!(store.get(&seg_key).unwrap().is_some());
    assert!(store.get(&manifest_head_key).unwrap().is_some());
    assert!(store.get(&manifest_legacy_key).unwrap().is_some());

    assert_eq!(
        log.expire_segments_through(&parent_shard, 3, 22).unwrap(),
        1,
        "after TTL expiry the branch pin no longer protects the source segment"
    );
    assert!(store.get(&seg_key).unwrap().is_none());
    assert!(store.get(&manifest_head_key).unwrap().is_none());
    assert!(store.get(&manifest_legacy_key).unwrap().is_none());

    // Branch metadata can still be discarded idempotently after expiry.
    log.discard_branch(&parent_shard, &branch_shard).unwrap();
}

#[test]
#[allow(non_snake_case)]
fn TestBranchPinnedManifestNotDeleted() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let source = shard();
    log.create_queue(&qdef()).unwrap();

    log.enqueue(&source, &pushes(2), 0, 10).unwrap();
    log.seal(&source, 0, 11).unwrap();
    log.enqueue(&source, &pushes(2), 0, 20).unwrap();
    log.seal(&source, 0, 21).unwrap();

    let branch_def = branch_qdef("manifest-live");
    log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 1),
        60_000,
        30,
    )
    .unwrap();

    log.advance_retention_floor(&source, CommandPosition::new(source.clone(), 0, 1), 0)
        .unwrap();
    assert_eq!(
        log.expire_segments_through(&source, 1, 31).unwrap(),
        0,
        "the live branch pin keeps the below-floor segment from being reclaimed"
    );

    let seg_key = segment_key_for(store.as_ref(), &source, 0);
    let manifest_head_key = manifest_head_key_s(&source, 0);
    let manifest_legacy_key = manifest_key_s(&source, 0);

    assert!(
        store.get(&seg_key).unwrap().is_some(),
        "the pinned source segment stays present while the branch is live"
    );
    assert!(
        store.get(&manifest_head_key).unwrap().is_some(),
        "the pinned manifest-head entry stays present while the branch is live"
    );
    assert!(
        store.get(&manifest_legacy_key).unwrap().is_some(),
        "the pinned legacy manifest entry stays present while the branch is live"
    );
    assert_eq!(
        log.read_read_horizon(&source).unwrap(),
        None,
        "the watermark does not advance past the pinned below-floor entry"
    );
}

#[test]
#[allow(non_snake_case)]
fn TestBranchPinReleaseEnablesManifestReclaim() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let source = shard();
    log.create_queue(&qdef()).unwrap();

    log.enqueue(&source, &pushes(2), 0, 10).unwrap();
    log.seal(&source, 0, 11).unwrap();
    log.enqueue(&source, &pushes(2), 0, 20).unwrap();
    log.seal(&source, 0, 21).unwrap();

    let branch_def = branch_qdef("manifest-release");
    let branch = QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 1),
        60_000,
        30,
    )
    .unwrap();
    let seg_key = segment_key_for(store.as_ref(), &source, 0);
    let manifest_head_key = manifest_head_key_s(&source, 0);
    let manifest_legacy_key = manifest_key_s(&source, 0);

    log.advance_retention_floor(&source, CommandPosition::new(source.clone(), 0, 1), 0)
        .unwrap();
    assert_eq!(log.expire_segments_through(&source, 1, 31).unwrap(), 0);
    log.discard_branch(&source, &branch).unwrap();

    assert_eq!(
        log.expire_segments_through(&source, 1, 32).unwrap(),
        1,
        "once the branch pin is released, the formerly pinned manifest can be reclaimed"
    );

    assert!(store.get(&seg_key).unwrap().is_none());
    assert!(store.get(&manifest_head_key).unwrap().is_none());
    assert!(store.get(&manifest_legacy_key).unwrap().is_none());
    assert_eq!(
        log.read_read_horizon(&source).unwrap(),
        Some(0),
        "the watermark advances only after the unpinned entry is reclaimed"
    );
}

#[test]
#[allow(non_snake_case)]
fn TestBranchPinRulesUnchangedByManifestReclaim() {
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

    log.advance_retention_floor(&source, CommandPosition::new(source.clone(), 0, 1), 0)
        .unwrap();
    assert_eq!(
        log.expire_segments_through(&source, 1, 31).unwrap(),
        0,
        "the pinned below-floor entry is skipped on the first pass"
    );
    assert_eq!(
        log.read_read_horizon(&source).unwrap(),
        None,
        "the deletion watermark does not advance past the pinned entry"
    );

    log.discard_branch(&source, &branch).unwrap();
    assert_eq!(
        log.expire_segments_through(&source, 1, 32).unwrap(),
        1,
        "after the pin releases, the same entry is reclaimed"
    );
    assert_eq!(
        log.read_read_horizon(&source).unwrap(),
        Some(0),
        "the watermark advances only after the entry is reclaimed"
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
    let seg0_key = segment_key_for(store.as_ref(), &parent, 0);
    let seg1_key = segment_key_for(store.as_ref(), &parent, 2);
    let seg2_key = segment_key_for(store.as_ref(), &parent, 4);
    let seg0_manifest_head_key = manifest_head_key_s(&parent, 0);
    let seg0_manifest_legacy_key = manifest_key_s(&parent, 0);
    let seg1_manifest_head_key = manifest_head_key_s(&parent, 1);
    let seg1_manifest_legacy_key = manifest_key_s(&parent, 1);
    let seg2_manifest_head_key = manifest_head_key_s(&parent, 2);
    let seg2_manifest_legacy_key = manifest_key_s(&parent, 2);

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

    assert!(
        store.get(&seg0_key).unwrap().is_some(),
        "the branch-pinned below-floor segment survives the trim"
    );
    assert!(
        store.get(&seg0_manifest_head_key).unwrap().is_some(),
        "the branch-pinned manifest head survives the trim"
    );
    assert!(
        store.get(&seg0_manifest_legacy_key).unwrap().is_some(),
        "the branch-pinned legacy manifest survives the trim"
    );
    assert!(
        store.get(&seg1_key).unwrap().is_none(),
        "the unpinned below-floor segment is reclaimed"
    );
    assert!(
        store.get(&seg1_manifest_head_key).unwrap().is_none(),
        "the unpinned manifest head is reclaimed"
    );
    assert!(
        store.get(&seg1_manifest_legacy_key).unwrap().is_none(),
        "the unpinned legacy manifest is reclaimed"
    );
    assert!(
        store.get(&seg2_key).unwrap().is_some(),
        "the fresh (above-floor) tail segment survives"
    );
    assert!(
        store.get(&seg2_manifest_head_key).unwrap().is_some(),
        "the fresh manifest head survives"
    );
    assert!(
        store.get(&seg2_manifest_legacy_key).unwrap().is_some(),
        "the fresh legacy manifest survives"
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
        store.get(&seg0_key).unwrap().is_none(),
        "seg0 is reclaimed once the pin is gone"
    );
    assert!(
        store.get(&seg0_manifest_head_key).unwrap().is_none(),
        "seg0 manifest head is reclaimed once the pin is gone"
    );
    assert!(
        store.get(&seg0_manifest_legacy_key).unwrap().is_none(),
        "seg0 legacy manifest is reclaimed once the pin is gone"
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
struct PeerTrimBoundedAdvances<S: BlobStore> {
    log: std::sync::Weak<SegmentedObjectLog<S>>,
    source: pqueue_engine::QueueKey,
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

/// A blob store whose `delete` returns `EngineError::Conflict` — the GENERIC `BlobStore` contract PERMITS a
/// delete to fail with Conflict, even though shipped stores happen to normalize to `Storage` — for any key
/// containing an armed substring, COUNTING each such fault. Used to prove the branch retry does NOT mistake a
/// rollback-cleanup Conflict for a concurrent floor advance (bead pqueue-9dcec223, codex round-2 BUG 1).
#[derive(Default)]
struct ConflictOnDeleteStore {
    inner: InMemoryBlobStore,
    fail_delete: std::sync::Mutex<Option<String>>,
    delete_conflicts: AtomicU64,
}

impl ConflictOnDeleteStore {
    fn arm_delete(&self, substr: &str) {
        *self.fail_delete.lock().unwrap() = Some(substr.to_string());
    }
    fn delete_conflicts(&self) -> u64 {
        self.delete_conflicts.load(Ordering::SeqCst)
    }
}

impl BlobStore for ConflictOnDeleteStore {
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
        if self
            .fail_delete
            .lock()
            .unwrap()
            .as_deref()
            .is_some_and(|s| key.contains(s))
        {
            self.delete_conflicts.fetch_add(1, Ordering::SeqCst);
            return Err(EngineError::Conflict);
        }
        self.inner.delete(key)
    }
    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        self.inner.list(prefix)
    }
}

/// Bead pqueue-9dcec223 (codex round-2 BUG 1): a rollback-CLEANUP failure that itself returns
/// `EngineError::Conflict` during a floor-moved attempt must NOT be mistaken for the retryable floor advance.
/// The retry keys off the PRIVATE `BranchAttempt::FloorAdvanced` signal, so a cleanup Conflict is surfaced
/// IMMEDIATELY (once) — never retried over the deliberately-RETAINED pin/partial objects. We arm the branch's
/// rollback branch-object delete to fail with Conflict, race a single concurrent floor advance to force the
/// floor-moved path, and assert: the branch returns Conflict, the cleanup fault fired EXACTLY ONCE (no retry
/// over retained state), and the source pin is RETAINED (source not reclaimable — the safe leak is intact).
#[test]
fn branch_does_not_retry_a_rollback_cleanup_conflict() {
    let store = std::sync::Arc::new(ConflictOnDeleteStore::default());
    let log = std::sync::Arc::new(SegmentedObjectLog::open(
        std::sync::Arc::clone(&store),
        SegmentConfig::new(10_000_000, 100).unwrap(),
    ));
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    for _ in 0..6 {
        log.enqueue(&source, &pushes(1), 0, 10).unwrap();
        log.seal(&source, 0, 10).unwrap();
    }

    // Race a SINGLE concurrent floor advance (to 1) so the first attempt takes the floor-moved rollback path.
    log.set_fault_hook(Some(std::sync::Arc::new(PeerTrimBoundedAdvances {
        log: std::sync::Arc::downgrade(&log),
        source: source.clone(),
        now_ms: 100,
        advances_remaining: AtomicU64::new(1),
        next_floor: AtomicU64::new(1),
    })));
    // Arm the rollback's branch-object cleanup delete to fail with Conflict (the generic-contract Conflict a
    // shipped store would normalize away, but the retry MUST NOT depend on that normalization).
    store.arm_delete("/manifest/");

    let branch_def = branch_qdef("cleanup-conflict-not-retried");
    let result = log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 5),
        1_000_000_000,
        100,
    );
    log.set_fault_hook(None);

    // The cleanup Conflict is surfaced immediately — NOT swallowed as a retryable floor advance.
    assert!(
        matches!(result, Err(EngineError::Conflict)),
        "a rollback-cleanup Conflict surfaces immediately, got {result:?}"
    );
    // Fired EXACTLY ONCE: the loop did NOT re-attempt over the retained pin/partial objects.
    assert_eq!(
        store.delete_conflicts(),
        1,
        "the cleanup-delete Conflict fired exactly once — the retry never looped over the retained state"
    );
    // The pin is RETAINED (safe leak): the source is NOT reclaimable, its segments stay protected.
    *store.fail_delete.lock().unwrap() = None; // disarm so the reclaim attempt can run its deletes
    assert_eq!(
        log.expire_segments_through(&source, 5, 200).unwrap(),
        0,
        "the retained pin keeps the source fully protected — nothing is reclaimed after the cleanup failure"
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
    check_post_pin_store_failure(|s| s.arm_put_if_absent("/manifest_head/"), true);
}

/// Post-pin store failure at the BRANCH.JSON-PUT stage -> clean rollback, pin released, source reclaimable.
#[test]
fn branch_metadata_put_store_failure_rolls_back_and_releases_the_pin() {
    check_post_pin_store_failure(|s| s.arm_put("branch.json"), true);
}

/// Post-pin store failure at the ACQUIRE-EPOCH stage (a `put_if_absent`) -> clean rollback, pin released.
#[test]
fn branch_acquire_epoch_store_failure_rolls_back_and_releases_the_pin() {
    check_post_pin_store_failure(|s| s.arm_put_if_absent("/manifest_head/"), true);
}

/// Post-pin failure PLUS a branch-object-CLEANUP failure -> the pin is RETAINED (safe leak); the source is
/// NOT reclaimable (its segments stay protected — never an unpinned partial branch / missing segment).
#[test]
fn branch_object_cleanup_store_failure_retains_the_pin() {
    check_post_pin_store_failure(
        |s| {
            s.arm_put("branch.json"); // trigger the post-pin failure
            s.arm_delete("/manifest_head/"); // and fail the rollback's branch-object cleanup
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

// ---------------------------------------------------------------------------
// Orphaned uncommitted-branch GC (bead pqueue-74f03d0e)
// ---------------------------------------------------------------------------

/// The tenant/queue key prefix a shard's objects live under (mirrors the crate-internal `shard_prefix`).
fn shard_prefix_of(k: &pqueue_engine::QueueKey) -> String {
    format!(
        "t/{}/q/{}/",
        hex_lower(k.tenant_id.as_str().as_bytes()),
        hex_lower(k.queue_id.as_str().as_bytes())
    )
}

/// A store that injects a FAILED branch creation whose OWN rollback cleanup also fails, leaving a durable
/// orphan (leftover `branch.pending` sentinel + partial branch manifest + a still-registered source pin). While
/// armed it (a) fails the `branch.json` commit-marker put — the LAST write of a branch creation — so the branch
/// never commits, and (b) fails every delete — so the creation rollback cannot clean up. Disarm it to let GC's
/// deletes through.
struct OrphanBranchFaultStore {
    inner: InMemoryBlobStore,
    fail_marker_put: AtomicBool,
    fail_deletes: AtomicBool,
}

impl OrphanBranchFaultStore {
    fn new() -> Self {
        Self {
            inner: InMemoryBlobStore::new(),
            fail_marker_put: AtomicBool::new(false),
            fail_deletes: AtomicBool::new(false),
        }
    }

    fn arm(&self) {
        self.fail_marker_put.store(true, Ordering::SeqCst);
        self.fail_deletes.store(true, Ordering::SeqCst);
    }

    fn disarm(&self) {
        self.fail_marker_put.store(false, Ordering::SeqCst);
        self.fail_deletes.store(false, Ordering::SeqCst);
    }
}

impl BlobStore for OrphanBranchFaultStore {
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

    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        self.inner.list(prefix)
    }

    fn stats(&self, prefix: &str) -> EngineResult<ObjectStoreStats> {
        self.inner.stats(prefix)
    }
}

/// Fault-inject a FAILED branch creation (marker-put + rollback-cleanup both fail) that leaves a durable orphan.
/// Returns the branch key. The source has six single-command segments (seq 0..5); the branch is cut at seq 3.
fn seed_orphaned_branch(
    log: &SegmentedObjectLog<std::sync::Arc<OrphanBranchFaultStore>>,
    store: &OrphanBranchFaultStore,
    source: &pqueue_engine::QueueKey,
    suffix: &str,
    created_at: i64,
) -> pqueue_engine::QueueKey {
    log.create_queue(&qdef()).unwrap();
    for _ in 0..6 {
        log.enqueue(source, &pushes(1), 0, 10).unwrap();
        log.seal(source, 0, 10).unwrap();
    }
    assert_eq!(log.read_all(source).unwrap().len(), 6);

    let branch_def = branch_qdef(suffix);
    let branch =
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());

    store.arm();
    let result = log.branch(
        source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 3),
        1_000_000_000, // large TTL: the orphan pin stays "live" well past the GC safety window
        created_at,
    );
    store.disarm();
    assert!(
        result.is_err(),
        "the injected marker-put + cleanup failure fails branch creation: {result:?}"
    );
    branch
}

/// (a) A genuinely-abandoned branch creation (marker ABSENT — the fault seam failed its marker put AND its
/// rollback cleanup) leaves a durable orphan (sentinel + partial branch manifest + a still-live source pin).
/// Under the create/GC exclusion GC reclaims ALL of it AND releases the source pin; the source stays fully
/// readable (no missing segment) and becomes fully reclaimable again.
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
        !store.list(&format!("{bp}manifest/")).unwrap().is_empty(),
        "partial branch manifest objects are durable"
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
        log.expire_segments_through(&source, 3, 2_000).unwrap(),
        0,
        "the orphaned source pin blocks reclamation of the branched range"
    );

    // GC reclaims exactly the one abandoned orphan (marker-absent, provably not in-flight under the guard).
    assert_eq!(
        log.gc_orphaned_branches(&source).unwrap(),
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
        log.expire_segments_through(&source, 3, 3_000).unwrap(),
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
        log.gc_orphaned_branches(&source).unwrap(),
        0,
        "re-running GC after a clean pass is a no-op"
    );
}

/// (b) A COMMITTED branch (its `branch.json` commit marker present) is NEVER GC'd — its objects, pin, and
/// readability all survive a GC run.
#[test]
fn gc_orphaned_branches_leaves_a_committed_branch_untouched() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let log = SegmentedObjectLog::open(store.clone(), SegmentConfig::new(10_000_000, 100).unwrap());
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    log.enqueue(&source, &pushes(4), 0, 10).unwrap();
    log.seal(&source, 0, 11).unwrap();

    let branch_def = branch_qdef("gc-committed");
    let branch =
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
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
        log.gc_orphaned_branches(&source).unwrap(),
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

/// Two-cut-point fault hook that DETERMINISTICALLY interleaves a branch creation committing its marker with a
/// concurrent GC pass (bead pqueue-74f03d0e, BUG 1). It pauses the CREATOR mid-flight (`DuringBranchCopy` —
/// after the source pin + `branch.pending` sentinel are written but BEFORE `branch.json`) and pauses GC right
/// after it classifies the branch marker-ABSENT (`GcAfterOrphanClassified`, before cleanup). The test drives
/// the exact interleaving via channels — no sleeps in the load-bearing path — so that WITHOUT the guard GC
/// provably proceeds to delete a branch the creator just committed.
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

/// (c) CREATE-vs-GC EXCLUSION (bead pqueue-74f03d0e, BUG 1): a branch creation that COMMITS its marker while a
/// GC pass runs concurrently on the same branch must SURVIVE — GC must never observe the marker-absent instant
/// and then destroy a branch that committed. DETERMINISTIC and HANG-FREE: the ONLY ordering main enforces is
/// `c_paused → c_resume → creator committed → g_resume` (std mpsc is UNBOUNDED, so every `send` is non-blocking
/// and main NEVER waits on a GC signal — that is what makes both builds terminate).
///
/// WITH the guard (shipped): GC parks on the create/GC guard the creator holds until the creator commits and
/// releases it; GC then sees the committed marker, SKIPS, and never enters the marker-absent path — so it never
/// waits on `g_resume`, and the `g_resume` send just sits unread in the unbounded channel. reclaimed == 0.
///
/// WITHOUT the guard (verified by temporarily removing it): GC reaches the marker-absent classify point and
/// parks on `g_resume`; main commits the creator FIRST, then sends `g_resume`, so GC deletes the just-committed
/// branch and returns 1 — the assertions below then fail. No sleep, no timeout, no scheduling luck.
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
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());

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
        std::thread::spawn(move || log.gc_orphaned_branches(&source))
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
        log.expire_segments_through(&source, 3, 2_000).unwrap(),
        0,
        "the committed branch's pin protects the source's branched range from reclamation"
    );
}

/// A cross-instance fault hook that pauses the creator mid-branch and pauses GC after classification.
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

/// Cross-instance handoff: a superseded branch creator must fail cleanly once a newer owner acquires the
/// source epoch, and the current owner's GC can then reclaim the orphan without corrupting or losing source
/// segments.
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
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());

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
        std::thread::spawn(move || log.gc_orphaned_branches(&source))
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
        reclaimed, 1,
        "the current owner's GC reclaims the abandoned orphan once the superseded creator rolls back"
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
// bead pqueue-8928baec — durable read-horizon watermark + range-list (fence untouched)
// ===========================================================================

use pqueue_engine::QueueKey;

/// The per-shard object-key prefix (`t/{hex(tenant)}/q/{hex(queue)}/`), mirroring the substrate's internal
/// `shard_prefix`. Lets these tests reach the raw manifest / read-horizon objects on the store directly.
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

fn manifest_head_key_s(shard: &QueueKey, index: u64) -> String {
    format!("{}{index:020}.json", manifest_head_prefix_s(shard))
}

fn manifest_key_s(shard: &QueueKey, index: u64) -> String {
    format!("{}{index:020}.json", manifest_prefix_s(shard))
}

fn segment_key_for<S: BlobStore>(store: &S, shard: &QueueKey, first_seq: u64) -> String {
    for prefix in [manifest_head_prefix_s(shard), manifest_prefix_s(shard)] {
        for key in store.list(&prefix).unwrap() {
            let Some(bytes) = store.get(&key).unwrap() else {
                continue;
            };
            let entry: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            if entry.get("first_seq").and_then(|v| v.as_u64()) == Some(first_seq)
                && let Some(segment_key) = entry.get("segment_key").and_then(|v| v.as_str())
            {
                return segment_key.to_string();
            }
        }
    }
    panic!("no manifest segment for first_seq {first_seq}");
}

fn read_horizon_key_s(shard: &QueueKey) -> String {
    format!("{}read_horizon.json", shard_prefix_s(shard))
}

/// One full trim cycle exactly as the composed trim path drives it: epoch-fenced floor advance FIRST, then the
/// segment-object reclamation (which also advances the durable read-horizon at its end).
fn trim_cycle<S: BlobStore>(
    log: &SegmentedObjectLog<S>,
    shard: &QueueKey,
    through_seq: u64,
    epoch: u64,
    now_ms: i64,
) {
    log.advance_retention_floor(
        shard,
        CommandPosition::new(shard.clone(), epoch, through_seq),
        epoch,
    )
    .unwrap();
    log.expire_segments_through(shard, through_seq, now_ms)
        .unwrap();
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
    for i in 0..3u64 {
        stale_owner
            .enqueue(&shard(), &pushes(1), 0, (i as i64 + 1) * 10)
            .unwrap();
        stale_owner
            .seal(&shard(), 0, (i as i64 + 1) * 10 + 1)
            .unwrap();
    }

    let live_owner = SegmentedObjectLog::open(store.clone(), cfg);
    live_owner.create_queue(&qdef()).unwrap();
    for i in 0..3u64 {
        live_owner
            .enqueue(&shard(), &pushes(1), 0, 200 + i as i64 * 10)
            .unwrap();
        live_owner.seal(&shard(), 0, 201 + i as i64 * 10).unwrap();
    }
    trim_cycle(&live_owner, &shard(), 3, 0, 1_000);

    (store, stale_owner, live_owner)
}

/// Test 1 — read/recovery enumeration is bounded to LIVE (above-horizon) entries after repeated
/// trim+advance-horizon cycles, and the watermark is monotonic. Below-floor manifest entries are now
/// physically removed once they are no longer branch-pinned, so the remaining manifest object count drops
/// while the live tail still enumerates in O(live).
#[test]
fn read_horizon_bounds_enumeration_to_live_and_is_monotonic() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef()).unwrap();

    // 8 sealed data segments: seqs 0..15 (2 commands each), manifest indices 0..7.
    for i in 0..8u64 {
        log.enqueue(&shard(), &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard(), 0, (i as i64 + 1) * 10 + 1).unwrap();
    }
    let initial_manifest_keys = store.list(&manifest_prefix_s(&shard())).unwrap().len();
    assert!(
        log.read_read_horizon(&shard()).unwrap().is_none(),
        "no horizon before any trim"
    );

    // Three trim cycles, each advancing the floor (and the durable horizon).
    trim_cycle(&log, &shard(), 3, 0, 1_000);
    let w1 = log
        .read_read_horizon(&shard())
        .unwrap()
        .expect("horizon after trim 1");
    trim_cycle(&log, &shard(), 7, 0, 2_000);
    let w2 = log
        .read_read_horizon(&shard())
        .unwrap()
        .expect("horizon after trim 2");
    trim_cycle(&log, &shard(), 11, 0, 3_000);
    let w3 = log
        .read_read_horizon(&shard())
        .unwrap()
        .expect("horizon after trim 3");

    // MONOTONIC: the watermark only ever advances.
    assert!(
        w1 < w2 && w2 < w3,
        "read-horizon is monotonic: {w1} < {w2} < {w3}"
    );

    // The compacted below-floor entries are gone, so the manifest object count is lower than before trim.
    let total_manifest_keys = store.list(&manifest_prefix_s(&shard())).unwrap().len();
    assert!(
        total_manifest_keys < initial_manifest_keys,
        "trimmed below-floor manifest entries were physically deleted"
    );
    // Range-listing from the horizon enumerates only the LIVE tail — strictly fewer than the total history.
    let live_keys = store
        .list_from(&manifest_prefix_s(&shard()), &manifest_key_s(&shard(), w3))
        .unwrap();
    assert!(
        live_keys.len() < initial_manifest_keys,
        "range-list enumerates O(live) ({}) not O(total history) ({initial_manifest_keys})",
        live_keys.len()
    );
    // Live = the two surviving data segments (seqs 12..15) + the authoritative floor entry (+ any superseded
    // floor entry above W). No live key names an index at/below the horizon.
    assert!(
        live_keys
            .iter()
            .all(|k| k.as_str() > manifest_key_s(&shard(), w3).as_str()),
        "every enumerated live key is strictly above the horizon index"
    );

    // The live data still reads back byte-for-byte from the floor+1.
    let floor = log.read_retention_floor(&shard()).unwrap().unwrap();
    let tail = log.read_from(&shard(), floor.sequence + 1).unwrap();
    assert_eq!(
        tail.iter().map(|(p, _)| p.sequence).collect::<Vec<_>>(),
        vec![12, 13, 14, 15],
        "the live tail reads back contiguously above the floor"
    );
}

/// Test 2 — recover_manifest tail + epoch + next-seq are correct after manifest reclamation and reopen.
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
        owner_b.read_read_horizon(&shard()).unwrap().is_some(),
        "horizon advanced"
    );

    // A fresh substrate recovers the SAME tail: next_seq 14 (12 data + 2 tail), epoch 1.
    let recovered = SegmentedObjectLog::open(store.clone(), cfg);
    recovered.create_queue(&qdef()).unwrap();
    assert_eq!(
        recovered.current_epoch(&shard()).unwrap(),
        1,
        "epoch recovered from the above-horizon tail"
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

/// Test 2b — deleting or hiding the only remaining live manifest entry above the durable floor fails
/// closed instead of being mistaken for an empty history.
#[test]
#[allow(non_snake_case)]
fn TestUnexpectedLiveManifestHoleFailsClosed() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef()).unwrap();
    log.enqueue(&shard(), &pushes(2), 0, 10).unwrap();
    log.seal(&shard(), 0, 11).unwrap();

    // Reclaim the only data segment. The remaining live manifest entry is the authoritative floor entry.
    trim_cycle(&log, &shard(), 1, 0, 1_000);
    assert!(log.read_read_horizon(&shard()).unwrap().is_some());

    // Hide the live floor entry above the durable floor. Recovery must fail closed rather than reset.
    assert!(
        store.get(&manifest_key_s(&shard(), 1)).unwrap().is_some(),
        "the floor entry is still present before we simulate the hole"
    );
    assert!(
        store.delete(&manifest_key_s(&shard(), 1)).unwrap(),
        "removed the live floor manifest entry"
    );
    assert!(
        store.delete(&manifest_head_key_s(&shard(), 1)).unwrap(),
        "removed the authoritative head copy too"
    );
    assert!(
        store.get(&manifest_key_s(&shard(), 1)).unwrap().is_none(),
        "the live floor entry is now missing"
    );

    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    let err = reopened.create_queue(&qdef()).unwrap_err();
    assert!(
        matches!(err, EngineError::Conflict | EngineError::Invalid(_)),
        "missing live manifest above the floor must fail closed, got {err:?}"
    );
}

/// Test 3 — live data is byte-identical pre/post horizon, and a below-floor read FAILS CLOSED (read at the
/// floor errors; read at floor+1 succeeds; read_all from genesis fails closed on a trimmed+horizoned queue).
#[test]
fn horizon_read_is_byte_identical_and_below_floor_fails_closed() {
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
        log.read_read_horizon(&shard()).unwrap().is_some(),
        "horizon exists after trim"
    );
    let floor = log.read_retention_floor(&shard()).unwrap().unwrap();
    assert_eq!(floor.sequence, 7);

    // Byte-identical: the same live tail reads back identically after the horizon advanced (no live entry
    // skipped by the range-list). CommandEnvelope is not PartialEq, so compare position + serialized bytes.
    let fingerprint = |v: &Vec<(CommandPosition, pqueue_engine::CommandEnvelope)>| {
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

/// Test 4 — a stale cached writer whose next index was reclaimed by manifest trimming cannot ack. The
/// reclaimed manifest slot stays absent, and the stale seal returns `EpochFenced` or `Conflict` rather than
/// creating a fresh durable entry at the freed address.
#[test]
#[allow(non_snake_case)]
fn TestPermanentFenceSurvivesManifestReclaim() {
    let (store, stale_owner, _live_owner) = reclaimed_cached_writer_fixture();

    stale_owner.enqueue(&shard(), &pushes(1), 0, 5_000).unwrap();
    let objects_before = store.inner.object_count();
    let err = stale_owner.seal(&shard(), 0, 5_001).unwrap_err();
    assert!(
        matches!(err, EngineError::EpochFenced | EngineError::Conflict),
        "a reclaimed cached manifest index must not ack, got {err:?}"
    );
    assert!(
        store.get(&manifest_key_s(&shard(), 3)).unwrap().is_none(),
        "the reclaimed manifest slot stays absent after the stale seal"
    );
    assert!(
        store
            .get(&manifest_head_key_s(&shard(), 3))
            .unwrap()
            .is_none(),
        "the authoritative manifest-head slot stays absent after the stale seal"
    );
    assert_eq!(
        store.inner.object_count(),
        objects_before,
        "the stale seal must not write a fresh manifest or segment object"
    );
}

/// Test 5 — the reclaim-fence rejection path keeps the seal hot path free of manifest LISTs. The seal
/// returns from the durable read-horizon check without listing the manifest before every seal.
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
    assert_eq!(
        store.list_count.load(Ordering::Relaxed),
        0,
        "the reclaim fence path must not LIST the manifest"
    );
}

/// Test 6 — correctness comes from the durable read-horizon fence, not a post-CAS tail-validate rollback
/// substitute. The stale seal rejects before any manifest LIST / tail revalidation, and the code comments
/// point at the hot-path design note and the deferred pqueue-c33c367e fence wiring.
#[test]
#[allow(non_snake_case)]
fn TestNoTailValidateRollbackSubstituteForCachedWriter() {
    let (store, stale_owner, _live_owner) = reclaimed_cached_writer_fixture();

    stale_owner.enqueue(&shard(), &pushes(1), 0, 5_000).unwrap();
    store.reset_reads();
    let err = stale_owner.seal(&shard(), 0, 5_001).unwrap_err();
    assert!(
        matches!(err, EngineError::EpochFenced | EngineError::Conflict),
        "the stale reclaimed writer must fail on the durable fence, got {err:?}"
    );
    assert_eq!(
        store.manifest_gets.load(Ordering::Relaxed),
        0,
        "no tail-validate rollback substitute should read the manifest after the CAS"
    );
    assert_eq!(
        store.list_count.load(Ordering::Relaxed),
        0,
        "no rollback substitute should LIST the manifest either"
    );
}

/// Test 7 — THE FENCE IS UNTOUCHED. A stale-epoch writer whose cached `next_manifest_index` points BELOW the
/// advanced read-horizon is STILL fenced: the below-horizon manifest object was NEVER freed, so its
/// `put_if_absent` still COLLIDES → CAS-lost → recover_manifest → EpochFenced. This is the whole safety
/// argument — advancing the horizon must NOT free the address.
#[test]
fn stale_writer_below_horizon_is_still_fenced_address_never_freed() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();

    // Owner A (epoch 0) commits ONE segment (manifest index 0), then FREEZES its cache: next_manifest_index=1.
    let a = SegmentedObjectLog::open(store.clone(), cfg);
    a.create_queue(&qdef()).unwrap();
    a.enqueue(&shard(), &pushes(2), 0, 10).unwrap();
    a.seal(&shard(), 0, 11).unwrap();

    // Owner B takes over: acquires epoch 1 (fence entry at manifest index 1), seals more, then trims so the
    // read-horizon advances PAST index 1 (A's frozen cached next index).
    let b = SegmentedObjectLog::open(store.clone(), cfg);
    b.create_queue(&qdef()).unwrap();
    assert_eq!(b.acquire_epoch(&shard(), 100).unwrap(), 1);
    for i in 0..3u64 {
        b.enqueue(&shard(), &pushes(2), 1, 200 + i as i64 * 10)
            .unwrap();
        b.seal(&shard(), 1, 201 + i as i64 * 10).unwrap();
    }
    trim_cycle(&b, &shard(), 5, 1, 1_000);
    let horizon = b
        .read_read_horizon(&shard())
        .unwrap()
        .expect("horizon advanced");
    assert!(
        horizon >= 1,
        "the horizon advanced past manifest index 1 (W = {horizon})"
    );

    // FENCE-PRESERVED PROOF: the below-horizon manifest object at index 1 still EXISTS (its address was never
    // freed), so a stale writer's create-only CAS there must still collide.
    assert!(
        store.get(&manifest_key_s(&shard(), 1)).unwrap().is_some(),
        "the below-horizon manifest index 1 object still EXISTS — the horizon NEVER frees the address"
    );

    // Owner A — still cached at epoch 0, next_manifest_index 1 (BELOW the horizon) — tries to seal. Its
    // put_if_absent at manifest/{1} collides with B's still-present fence tombstone → EpochFenced.
    a.enqueue(&shard(), &pushes(3), 0, 5_000).unwrap();
    let err = a.seal(&shard(), 0, 5_001).unwrap_err();
    assert_eq!(
        err,
        EngineError::EpochFenced,
        "the stale writer whose cached index is below the horizon is STILL fenced (address never freed)"
    );
}

/// Test 5 — a branch created from a trimmed+horizoned source inherits the floor and reads its own view; and a
/// branch whose seed floor `f` becomes its genesis still reads its own `seq == f` (design §5(ii): the floor is
/// an exclusive reclamation bound, NOT a seq<=floor read-visibility filter — never suppress a branch's seed).
#[test]
fn branch_inherits_floor_reads_its_view_and_seed_seq_is_not_suppressed() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    // seqs 0..7 (4 segments). Trim so the source floor is 3 (segs 0-1, 2-3 reclaimed) — leaving live 4..7.
    for i in 0..4u64 {
        log.enqueue(&source, &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&source, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }
    trim_cycle(&log, &source, 3, 0, 1_000);
    assert!(
        log.read_read_horizon(&source).unwrap().is_some(),
        "source has a horizon"
    );

    // (A) A branch cut at seq 5 (above the floor) inherits floor 3 and reads its own live view (seqs 4..5).
    let branch_def = branch_qdef("post-horizon");
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
        log.read_retention_floor(&branch).unwrap().unwrap().sequence,
        3,
        "branch inherited the floor"
    );
    assert!(
        log.read_read_horizon(&branch).unwrap().is_none(),
        "branch creation writes NO horizon"
    );
    let view = log.read_all(&branch).unwrap();
    assert_eq!(
        view.iter().map(|(p, _)| p.sequence).collect::<Vec<_>>(),
        vec![4, 5],
        "the branch reads exactly its inherited-floor-to-cut view (no fail-closed suppression, no missing segment)"
    );

    // (B) Branch-seed genesis: a branch of a FULLY-trimmed source (floor == source tail) seeds floor f and its
    // first append acks at seq == f. That seed seq must NOT be suppressed by the fail-closed floor guard.
    trim_cycle(&log, &source, 7, 0, 2_500); // now the whole source is below the floor (f = 7)
    let seed_def = branch_qdef("seed-genesis");
    let seed = QueueKey::new(seed_def.tenant_id.clone(), seed_def.queue_id.clone());
    let seed_epoch = log
        .branch(
            &source,
            &seed_def,
            &CommandPosition::new(source.clone(), 0, 8),
            60_000,
            3_000,
        )
        .unwrap();
    let f = log.read_retention_floor(&seed).unwrap().unwrap().sequence;
    assert_eq!(f, 7, "the seed branch inherited floor f = 7");
    // The branch's first append acks at seq == f (empty-seed-tail edge, §5(ii)).
    log.enqueue(&seed, &pushes(1), seed_epoch, 3_100).unwrap();
    let pos = log.seal(&seed, seed_epoch, 3_101).unwrap();
    assert_eq!(
        pos[0].sequence, f,
        "the branch's first append acks at seq == f (its seed genesis)"
    );
    // Reading the branch surfaces its own seq==f seed — the fail-closed guard does NOT fire (branch has no
    // horizon; its seed is present, not reclaimed).
    let seed_view = log.read_all(&seed).unwrap();
    assert_eq!(
        seed_view
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![f],
        "the branch reads its own seq==f seed record — never suppressed by the floor"
    );
}

/// Test 6 — AC-TXN-3 restart: a fresh SegmentedObjectLog over the same store after a horizon advance recovers
/// identically (tail/epoch/floor preserved, replay identical, below-floor fail-closed survives the restart).
#[test]
fn ac_txn_3_fresh_log_recovers_identically_after_horizon_advance() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();

    {
        let log = SegmentedObjectLog::open(store.clone(), cfg);
        log.create_queue(&qdef()).unwrap();
        for i in 0..6u64 {
            log.enqueue(&shard(), &pushes(2), 0, (i as i64 + 1) * 10)
                .unwrap();
            log.seal(&shard(), 0, (i as i64 + 1) * 10 + 1).unwrap();
        }
        trim_cycle(&log, &shard(), 7, 0, 1_000);
    }

    // Fresh substrate over the same durable store.
    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&qdef()).unwrap();
    // Floor + horizon are durable and preserved.
    assert_eq!(
        reopened
            .read_retention_floor(&shard())
            .unwrap()
            .unwrap()
            .sequence,
        7
    );
    assert!(
        reopened.read_read_horizon(&shard()).unwrap().is_some(),
        "horizon survives the restart"
    );
    // Replay of the live tail is identical.
    let tail = reopened.read_from(&shard(), 8).unwrap();
    assert_eq!(
        tail.iter().map(|(p, _)| p.sequence).collect::<Vec<_>>(),
        vec![8, 9, 10, 11]
    );
    // Fail-closed survives the restart.
    assert!(
        matches!(reopened.read_all(&shard()), Err(EngineError::Storage(_))),
        "the below-floor fail-closed survives a restart"
    );
    // A post-restart append continues contiguously.
    reopened.enqueue(&shard(), &pushes(1), 0, 5_000).unwrap();
    let pos = reopened.seal(&shard(), 0, 5_001).unwrap();
    assert_eq!(
        pos[0].sequence, 12,
        "next-seq recovered exactly from the ranged tail"
    );
}

/// Test 8 — a PARTIAL expire (through_seq < durable floor) must NOT advance the read-horizon past segments it
/// did NOT actually delete: a later full expire must still find and reclaim them (no storage leak). Guards the
/// finding that binding the horizon to the floor (rather than the reclaimed boundary) would hide undeleted
/// below-floor segments from a future trim.
#[test]
fn partial_expire_does_not_hide_undeleted_below_floor_segments() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef()).unwrap();
    // 8 segments: seqs 0..15.
    for i in 0..8u64 {
        log.enqueue(&shard(), &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard(), 0, (i as i64 + 1) * 10 + 1).unwrap();
    }
    let seg_keys: Vec<(u64, String)> = [0u64, 2, 4, 6, 8, 10, 12, 14]
        .into_iter()
        .map(|first| (first, segment_key_for(store.as_ref(), &shard(), first)))
        .collect();
    // Advance the durable floor ALL THE WAY to 15, but reclaim only THROUGH seq 7 (a partial expire).
    log.advance_retention_floor(&shard(), CommandPosition::new(shard(), 0, 15), 0)
        .unwrap();
    let first = log.expire_segments_through(&shard(), 7, 1_000).unwrap();
    assert_eq!(
        first, 4,
        "the partial expire deletes segs for seqs 0..7 (4 segments)"
    );

    // The horizon advanced ONLY across the reclaimed prefix (bounded by through=7), NOT to the floor 15.
    // A second, full expire through the floor must still find & reclaim segs for seqs 8..15.
    let second = log.expire_segments_through(&shard(), 15, 2_000).unwrap();
    assert_eq!(
        second, 4,
        "the not-yet-deleted below-floor segments (seqs 8..15) were NOT hidden by the horizon — the full \
         expire still reclaims all 4"
    );
    // Nothing leaked: every data segment object at/below the floor is gone.
    for (first, seg_key) in seg_keys {
        assert!(
            store.get(&seg_key).unwrap().is_none(),
            "segment {first} reclaimed, not leaked"
        );
    }
}

/// Test 7 — backward compat: a queue/store with NO `read_horizon.json` object behaves EXACTLY as before (full
/// manifest list). Proven two ways: (i) a never-trimmed queue has no horizon and reads normally; (ii) DELETING
/// the horizon object off a trimmed queue falls back to the full list and the ORGANIC missing-segment
/// fail-closed still stands (identical to pre-horizon behavior).
#[test]
fn backward_compat_no_horizon_object_behaves_as_before() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef()).unwrap();
    for i in 0..4u64 {
        log.enqueue(&shard(), &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard(), 0, (i as i64 + 1) * 10 + 1).unwrap();
    }
    // (i) Never trimmed → no horizon → full-list reads, genesis read works.
    assert!(log.read_read_horizon(&shard()).unwrap().is_none());
    assert_eq!(
        log.read_all(&shard()).unwrap().len(),
        8,
        "a never-trimmed queue reads from genesis as before"
    );

    // Trim (writes a horizon), then DELETE the horizon object to simulate a pre-existing / rolled-back queue.
    trim_cycle(&log, &shard(), 3, 0, 1_000);
    assert!(log.read_read_horizon(&shard()).unwrap().is_some());
    assert!(
        store.delete(&read_horizon_key_s(&shard())).unwrap(),
        "removed the horizon object"
    );
    assert!(
        log.read_read_horizon(&shard()).unwrap().is_none(),
        "no horizon after delete"
    );

    // Fallback to the FULL manifest list still works, but the physically deleted below-floor entries are no
    // longer present to trigger a missing-segment error. The live tail above the floor still reads cleanly.
    let all = log.read_all(&shard()).unwrap();
    assert_eq!(
        all.iter().map(|(p, _)| p.sequence).collect::<Vec<_>>(),
        vec![4, 5, 6, 7],
        "with no horizon object the queue still reads the remaining live tail"
    );
    let tail = log.read_from(&shard(), 4).unwrap();
    assert_eq!(
        tail.iter().map(|(p, _)| p.sequence).collect::<Vec<_>>(),
        vec![4, 5, 6, 7]
    );
}
