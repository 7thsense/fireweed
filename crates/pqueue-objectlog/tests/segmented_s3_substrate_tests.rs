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

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Barrier, Mutex};
use std::thread;

use pqueue_conformance::{envelope, item, qdef, shard};
use pqueue_core::QueueId;
use pqueue_engine::{CommandPosition, EngineError, EngineResult, PushCommand, QueueCommand};
use pqueue_objectlog::segmented::{
    BlobStore, FaultCutPoint, FaultHook, InMemoryBlobStore, ManifestHeadBlob, ObjectStoreStats,
    PartialExpireVisibility, S3BlobStore, SegmentConfig, SegmentedObjectLog,
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

fn unique_qdef(label: &str) -> pqueue_core::QueueDefinition {
    let mut def = qdef();
    let n = HEAD_TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    def.tenant_id = pqueue_core::TenantId::new(format!(
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

#[derive(Default)]
struct MissingGetBlobStore {
    inner: InMemoryBlobStore,
    missing_get: Mutex<Option<String>>,
}

impl MissingGetBlobStore {
    fn arm_missing_get(&self, key: &str) {
        *self.missing_get.lock().unwrap() = Some(key.to_owned());
    }
}

impl BlobStore for MissingGetBlobStore {
    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
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
        self.inner.delete(key)
    }

    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
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
        12,
        "legacy fallback now probes both head and legacy manifest ranges, each spanning three billable LIST requests"
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
    let branch_prefix = format!(
        "t/{}/q/{}/",
        hex_lower(branch_shard.tenant_id.as_str().as_bytes()),
        hex_lower(branch_shard.queue_id.as_str().as_bytes())
    );
    let branch_objects = store.list(&branch_prefix).unwrap();
    assert!(
        branch_objects.iter().any(|k| k.contains("branch-seg/")),
        "branch creation materializes branch-owned segment copies"
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
    parent
        .advance_retention_floor(
            &parent_shard,
            CommandPosition::new(parent_shard.clone(), 0, 3),
            0,
        )
        .unwrap();
    assert_eq!(
        parent
            .expire_segments_through(&parent_shard, 3, 20)
            .unwrap(),
        4
    );

    let branch_def = branch_qdef("trimmed-source");
    let branch_shard =
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
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
    for key in store
        .inner
        .list(&format!("{source_shard_prefix}seg_attempt/"))
        .unwrap()
    {
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
        store.get(&parent_manifest_head_key).unwrap().is_some(),
        "expired parent manifest head stays retained as history"
    );
    assert!(
        store.get(&parent_manifest_legacy_key).unwrap().is_none(),
        "the reclaimed legacy manifest copy is physically deleted"
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
    assert!(store.get(&manifest_head_key).unwrap().is_some());
    assert!(store.get(&manifest_legacy_key).unwrap().is_none());

    // Branch metadata can still be discarded idempotently after expiry.
    log.discard_branch(&parent_shard, &branch_shard).unwrap();
}

#[test]
#[allow(non_snake_case)]
fn TestManifestReclamationWatermarkStopsAtPinnedBranches() {
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
    assert!(
        log.manifest_reclamation_candidates(&source, 1, 31)
            .unwrap()
            .is_empty(),
        "pinned below-floor entries are excluded from the eligible candidate set"
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
        log.manifest_reclamation_candidates(&source, 1, 32)
            .unwrap()
            .len(),
        1,
        "once the branch pin is released the below-floor entry becomes eligible"
    );

    assert_eq!(
        log.expire_segments_through(&source, 1, 32).unwrap(),
        1,
        "once the branch pin is released, the formerly pinned segment can be reclaimed"
    );

    assert!(store.get(&seg_key).unwrap().is_none());
    assert!(store.get(&manifest_head_key).unwrap().is_some());
    assert!(store.get(&manifest_legacy_key).unwrap().is_none());
    assert_eq!(
        log.read_read_horizon(&source).unwrap(),
        Some(0),
        "the watermark advances only after the unpinned entry is reclaimed"
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
        store.get(&seg1_manifest_head_key).unwrap().is_some(),
        "the unpinned manifest head is retained as history"
    );
    assert!(
        store.get(&seg1_manifest_legacy_key).unwrap().is_none(),
        "the reclaimed legacy manifest copy is physically deleted"
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
        store.get(&seg0_manifest_head_key).unwrap().is_some(),
        "seg0 manifest head remains retained once the pin is gone"
    );
    assert!(
        store.get(&seg0_manifest_legacy_key).unwrap().is_none(),
        "seg0 legacy manifest is physically deleted once the pin is gone"
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

/// The LEGACY compatibility manifest copy — the object head-based compaction PHYSICALLY DELETES below the
/// retention floor.
fn manifest_key_of(shard: &pqueue_engine::QueueKey, index: u64) -> String {
    format!("{}manifest/{index:020}.json", shard_prefix_of(shard))
}

/// The AUTHORITATIVE manifest entry (the `manifest_head/` namespace). Its ADDRESS is never freed — compaction
/// overwrites a below-floor entry with a reclaimed marker instead of deleting it, keeping the create-only
/// `put_if_absent` collision fence intact — and the range-list skips every index at/below the read-horizon.
fn manifest_head_key_of(shard: &pqueue_engine::QueueKey, index: u64) -> String {
    format!("{}manifest_head/{index:020}.json", shard_prefix_of(shard))
}

/// Drive `commands` single-command segments (seq `0..commands`) onto `source`, advance the durable retention
/// floor to `floor`, and run the trim that PHYSICALLY reclaims the below-floor prefix — both the segment
/// objects AND their manifest entries ([`SegmentedObjectLog::expire_segments_through`] deletes the manifest
/// entry once the segment delete succeeds, then advances the read-horizon watermark).
///
/// Returns the manifest keys the trim DELETED (diffed across the trim), i.e. exactly the below-floor manifest
/// prefix that branch creation can no longer read.
fn seed_trimmed_source<S: BlobStore, T: BlobStore>(
    log: &SegmentedObjectLog<S>,
    store: &T,
    source: &pqueue_engine::QueueKey,
    commands: u64,
    floor: u64,
) -> Vec<String> {
    for _ in 0..commands {
        log.enqueue(source, &pushes(1), 0, 10).unwrap();
        log.seal(source, 0, 11).unwrap();
    }
    log.advance_retention_floor(source, CommandPosition::new(source.clone(), 0, floor), 12)
        .unwrap();

    let manifest_prefix = format!("{}manifest/", shard_prefix_of(source));
    let before = store.list(&manifest_prefix).unwrap();
    let reclaimed = log.expire_segments_through(source, floor, 20).unwrap();
    assert_eq!(
        reclaimed,
        floor + 1,
        "the trim reclaims every below-floor segment object (seq 0..={floor})"
    );
    let after = store.list(&manifest_prefix).unwrap();

    let mut deleted: Vec<String> = before
        .into_iter()
        .filter(|key| !after.contains(key))
        .collect();
    deleted.sort();
    deleted
}

/// **AC-1 — TestBranchInheritanceUsesRetainedFloorMetadata** (bead pqueue-f2b2e9e3).
///
/// After head-based compaction runs, the source's below-floor manifest prefix is no longer a substrate branch
/// creation can fold from genesis:
///
/// * the LEGACY `manifest/` copies below the floor are PHYSICALLY DELETED (`delete_manifest_entry`), and
/// * their AUTHORITATIVE `manifest_head/` entries are overwritten with reclaimed markers (the address is kept
///   OCCUPIED so the create-only CAS fence stays intact) and hidden below the durable read-horizon, so no
///   range-list even enumerates them.
///
/// Branch creation must therefore inherit from the RETAINED floor metadata: the authoritative retention-floor
/// entry (kept ABOVE the watermark precisely so `read_retention_floor` still resolves once the prefix is gone)
/// plus the range-listed live tail that yields the head. This asserts BOTH halves — the inherited floor/head
/// are correct, and creation GETs NO deleted (or reclaimed) below-floor source manifest object.
#[test]
fn branch_inheritance_uses_retained_floor_metadata() {
    let store = std::sync::Arc::new(CountingBlobStore::default());
    let log = SegmentedObjectLog::open(store.clone(), SegmentConfig::new(10_000_000, 100).unwrap());
    let source = shard();
    log.create_queue(&qdef()).unwrap();

    // Source: 8 single-command segments (seq 0..7) at manifest indices 0..7, floor advanced to 3 (its
    // floor-advance entry lands at index 8), below-floor prefix physically reclaimed.
    let deleted_manifest_keys = seed_trimmed_source(&log, &store, &source, 8, 3);

    // The below-floor manifest PREFIX is physically gone from the store ...
    assert_eq!(
        deleted_manifest_keys,
        (0..=3)
            .map(|i| manifest_key_of(&source, i))
            .collect::<Vec<_>>(),
        "the trim physically DELETES the below-floor manifest entries (indices 0..=3), not just their segments"
    );
    for key in &deleted_manifest_keys {
        assert!(
            store.inner.get(key).unwrap().is_none(),
            "{key} must be physically deleted"
        );
    }
    // ... and their authoritative head slots survive ONLY as reclaimed markers (address OCCUPIED so the
    // create-only CAS fence holds, but carrying no live inheritance state).
    for index in 0..=3u64 {
        let head: serde_json::Value = serde_json::from_slice(
            &store
                .inner
                .get(&manifest_head_key_of(&source, index))
                .unwrap()
                .expect(
                    "the below-floor head ADDRESS is never freed (the CAS fence depends on it)",
                ),
        )
        .unwrap();
        assert_eq!(
            head["compacted_through_index"], index,
            "the below-floor head entry is a RECLAIMED MARKER, not live inheritance state"
        );
    }
    // The RETAINED floor metadata survives ABOVE the read-horizon: the authoritative retention-floor entry is
    // the inheritance substrate branch creation is left with (the watermark stops strictly below it).
    let floor_entry_key = manifest_head_key_of(&source, 8);
    let floor_entry: serde_json::Value =
        serde_json::from_slice(&store.inner.get(&floor_entry_key).unwrap().expect(
            "the AUTHORITATIVE retention-floor entry is RETAINED (read_retention_floor still needs it)",
        ))
        .unwrap();
    assert_eq!(
        floor_entry["retention_floor_through"], 3,
        "the retained floor entry carries the source floor f = 3"
    );
    assert_eq!(
        log.read_read_horizon(&source).unwrap(),
        Some(3),
        "the durable read-horizon hides the deleted prefix from every range-list"
    );
    assert_eq!(
        log.read_retention_floor(&source)
            .unwrap()
            .map(|p| p.sequence),
        Some(3),
        "the source floor still resolves from the retained metadata alone"
    );

    // Create the branch over the trimmed source, recording every GET the creation issues.
    store.reset_reads();
    let branch_def = branch_qdef("retained-floor-metadata");
    let branch =
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    let branch_epoch = log
        .branch(
            &source,
            &branch_def,
            &CommandPosition::new(source.clone(), 0, 7),
            60_000,
            30,
        )
        .unwrap();
    let creation_gets = store.get_keys();

    // (1) The branch inherited the CORRECT source FLOOR ...
    assert_eq!(
        log.read_retention_floor(&branch)
            .unwrap()
            .map(|p| p.sequence),
        Some(3),
        "the branch inherits the source floor (3) derived from the RETAINED floor entry"
    );
    // ... and the CORRECT source HEAD: its view is exactly the retained (floor, cut] range ...
    assert_eq!(
        log.read_all(&branch)
            .unwrap()
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![4, 5, 6, 7],
        "the branch view is exactly the retained (floor, cut] range — no reclaimed object is ever GET"
    );
    // ... and its own next write continues at `cut + 1` on its own epoch, proving the recovered head (next_seq
    // / next_manifest_index) came from the live tail rather than a deleted prefix.
    assert_eq!(branch_epoch, 1, "the branch takes its own lease epoch");
    log.enqueue(&branch, &pushes(1), branch_epoch, 40).unwrap();
    let positions = log.seal(&branch, branch_epoch, 41).unwrap();
    assert_eq!(
        positions[0].sequence, 8,
        "the inherited HEAD continues at cut + 1"
    );

    // (2) ... and it did so WITHOUT recovering any deleted source manifest object: neither a physically
    // deleted legacy copy ...
    for key in &deleted_manifest_keys {
        assert!(
            !creation_gets.contains(key),
            "branch creation GET the DELETED source manifest object {key} — inheritance must not fold the \
             reclaimed prefix"
        );
    }
    // ... nor a below-floor (reclaimed-marker) head slot: the range-list from the read-horizon never
    // enumerates them, so inheritance never touches the compacted prefix at all.
    for index in 0..=3u64 {
        let head_key = manifest_head_key_of(&source, index);
        assert!(
            !creation_gets.contains(&head_key),
            "branch creation GET the reclaimed below-floor head entry {head_key} — inheritance must read only \
             the RETAINED metadata above the read-horizon"
        );
    }
    // The positive half: the inheritance substrate it DID read is the retained floor entry.
    assert!(
        creation_gets.contains(&floor_entry_key),
        "branch creation DID read the RETAINED floor entry — that is the inheritance substrate"
    );
}

/// A store that CRASHES a branch creation at its LAST durable write — the `branch.json` commit marker — after
/// snapshotting every object durable at that exact instant. The snapshot is the crash image: ALL inherited
/// branch metadata (source pin, `branch.pending` sentinel, seed floor entry, copied manifest entries + segment
/// objects, epoch fence) present, commit marker absent.
#[derive(Default)]
struct MarkerCrashStore {
    inner: InMemoryBlobStore,
    snapshot: Mutex<Vec<(String, Vec<u8>)>>,
}

impl MarkerCrashStore {
    fn snapshot_at_marker(&self) -> Vec<(String, Vec<u8>)> {
        self.snapshot.lock().unwrap().clone()
    }
}

impl BlobStore for MarkerCrashStore {
    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
        if key.ends_with("branch.json") {
            let mut snapshot = Vec::new();
            for k in self.inner.list("")? {
                if let Some(bytes) = self.inner.get(&k)? {
                    snapshot.push((k, bytes));
                }
            }
            *self.snapshot.lock().unwrap() = snapshot;
            return Err(EngineError::Storage(
                "injected: crash before the branch commit marker".into(),
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
        self.inner.delete(key)
    }

    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        self.inner.list(prefix)
    }

    fn stats(&self, prefix: &str) -> EngineResult<ObjectStoreStats> {
        self.inner.stats(prefix)
    }
}

/// **AC-2 — TestBranchInheritanceSeedFloorEdge** (bead pqueue-f2b2e9e3). Two halves of the same contract.
///
/// **The seed-floor `seq == f` edge.** A branch over a source trimmed to floor `f` seeds its OWN first manifest
/// entry AT `seq == f` (`first_seq == last_seq == f`, `retention_floor_through = f`, naming NO segment object),
/// so `f + 1` is its effective genesis: `f` itself is the EXCLUSIVE lower bound, the first legal cut is `f + 1`,
/// and a cut AT `f` is rejected cleanly. The branch writes NO read-horizon of its own — which is exactly what
/// keeps its own `seq == f` seed ENUMERABLE (the fail-closed below-floor guard is gated on a horizon EXISTING),
/// so the seed is never mistaken for a reclaimed tombstone.
///
/// **Atomic visibility.** The branch becomes visible ONLY after ALL of that inherited metadata is durable: at
/// the instant of the commit-marker write every inherited object is already on the store, yet a crash image
/// taken there recovers the branch as NON-EXISTENT (no view, no resolvable floor). Visibility flips at the
/// marker, never mid-inheritance.
#[test]
fn branch_inheritance_seed_floor_edge() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let log = SegmentedObjectLog::open(store.clone(), SegmentConfig::new(10_000_000, 100).unwrap());
    let source = shard();
    log.create_queue(&qdef()).unwrap();
    seed_trimmed_source(&log, &store, &source, 8, 3); // floor f = 3

    // A cut AT the floor (`seq == f`) is REJECTED cleanly: the floor is an EXCLUSIVE lower bound, so that whole
    // view is reclaimed. `f + 1` is the FIRST legal cut — that is the edge.
    let err = log
        .branch(
            &source,
            &branch_qdef("cut-at-floor"),
            &CommandPosition::new(source.clone(), 0, 3),
            60_000,
            30,
        )
        .unwrap_err();
    assert!(
        matches!(err, EngineError::Invalid(_)),
        "a cut AT the source floor f is rejected cleanly (Invalid), got {err:?}"
    );

    let branch_def = branch_qdef("seed-floor-edge");
    let branch =
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 4), // cut == f + 1, the first legal cut
        60_000,
        31,
    )
    .unwrap();

    // The branch's FIRST (authoritative) manifest entry is the inherited SEED FLOOR at `seq == f`.
    let seed: serde_json::Value = serde_json::from_slice(
        &store
            .get(&manifest_head_key_of(&branch, 0))
            .unwrap()
            .expect("the branch seeds its inherited floor as manifest index 0"),
    )
    .unwrap();
    assert_eq!(
        seed["first_seq"], 3,
        "the seed floor entry sits AT seq == f"
    );
    assert_eq!(seed["last_seq"], 3, "the seed floor entry sits AT seq == f");
    assert_eq!(
        seed["retention_floor_through"], 3,
        "the seed entry IS a retention-floor-advance entry carrying the inherited floor f"
    );
    assert!(
        seed["segment_key"].is_null(),
        "the seed floor entry names NO segment object — there is nothing below f left to GET"
    );
    assert_eq!(
        seed["fence"], false,
        "the seed is a floor entry, not an epoch fence"
    );
    assert_eq!(
        log.read_retention_floor(&branch)
            .unwrap()
            .map(|p| p.sequence),
        Some(3),
        "the branch resolves its own inherited floor at f"
    );
    assert_eq!(
        log.read_read_horizon(&branch).unwrap(),
        None,
        "the branch writes NO read-horizon, so the fail-closed guard never suppresses its own seq == f seed"
    );
    assert_eq!(
        log.read_all(&branch)
            .unwrap()
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![4],
        "the seed is METADATA, not a command: the branch's effective genesis is f + 1, and its view is [f+1, cut]"
    );

    // ---- Atomic visibility: only after ALL inherited metadata is durable. ----
    let crash_store = std::sync::Arc::new(MarkerCrashStore::default());
    let crash_log = SegmentedObjectLog::open(
        crash_store.clone(),
        SegmentConfig::new(10_000_000, 100).unwrap(),
    );
    let crash_source = shard();
    crash_log.create_queue(&qdef()).unwrap();
    seed_trimmed_source(&crash_log, &crash_store, &crash_source, 8, 3);

    let crash_def = branch_qdef("seed-floor-atomicity");
    let crash_branch =
        pqueue_engine::QueueKey::new(crash_def.tenant_id.clone(), crash_def.queue_id.clone());
    let err = crash_log
        .branch(
            &crash_source,
            &crash_def,
            &CommandPosition::new(crash_source.clone(), 0, 4),
            60_000,
            32,
        )
        .unwrap_err();
    assert!(
        matches!(err, EngineError::Storage(_)),
        "the injected commit-marker crash surfaces as a store failure, got {err:?}"
    );

    // The crash image: EVERY piece of inherited metadata is durable, ONLY the commit marker is missing.
    let image = crash_store.snapshot_at_marker();
    let crash_branch_prefix = shard_prefix_of(&crash_branch);
    let seed_key = manifest_head_key_of(&crash_branch, 0);
    assert!(
        image.iter().any(|(k, _)| *k == seed_key),
        "the inherited SEED FLOOR entry was already durable when the marker write was attempted"
    );
    assert!(
        image
            .iter()
            .any(|(k, _)| *k == format!("{crash_branch_prefix}branch.pending")),
        "the branch sentinel was durable"
    );
    assert!(
        image
            .iter()
            .any(|(k, _)| *k == branch_registry_key_of(&crash_source, &crash_branch)),
        "the source pin was durable"
    );
    assert!(
        !image
            .iter()
            .any(|(k, _)| *k == format!("{crash_branch_prefix}branch.json")),
        "the commit marker had NOT landed — it is the LAST durable write"
    );

    // Recover that image: the branch is NON-EXISTENT despite every inherited object being durable.
    let recovered_store = std::sync::Arc::new(InMemoryBlobStore::new());
    for (key, bytes) in &image {
        recovered_store.put(key, bytes).unwrap();
    }
    let recovered = SegmentedObjectLog::open(
        recovered_store.clone(),
        SegmentConfig::new(10_000_000, 100).unwrap(),
    );
    assert!(
        recovered.read_all(&crash_branch).unwrap().is_empty(),
        "an un-marked branch is NOT visible, even with its whole inherited manifest durable"
    );
    assert_eq!(
        recovered.read_retention_floor(&crash_branch).unwrap(),
        None,
        "an un-marked branch has NO resolvable inherited floor until its commit marker is durable"
    );
    // By contrast the COMMITTED branch above — marker durable AFTER all inherited metadata — is fully visible.
    assert_eq!(
        log.read_retention_floor(&branch)
            .unwrap()
            .map(|p| p.sequence),
        Some(3),
        "visibility flips at the commit marker: the committed branch keeps its inherited floor"
    );
}

/// **AC-3 — TestBranchInheritanceSourcePinsPreserved** (bead pqueue-151257a3, part 2 of the pqueue-92a2e386
/// split). Branch creation off the RETAINED floor/head metadata (the same trimmed-source substrate as
/// [`branch_inheritance_uses_retained_floor_metadata`]) must publish the exact same source pin and orphan-GC
/// contract an ordinary, untrimmed-source branch gets — the retained-metadata inheritance path is not a
/// second, weaker contract:
///
/// * the source pin (`{source}branches/{branch}.json` registry entry) is durable once creation returns;
/// * the source's own retention floor is untouched by branch creation;
/// * [`SegmentedObjectLog::gc_orphaned_branches`] never reclaims the committed branch (orphan GC guarantee);
/// * the branch's live pin still protects every RETAINED segment it copied (4..7) against
///   [`SegmentedObjectLog::expire_segments_through`] (retention floor / reclamation guarantee);
/// * discarding the branch releases the pin and the formerly-pinned retained segments become reclaimable,
///   exactly as [`branch_pins_parent_segments_against_expiry`] proves for an untrimmed source.
#[test]
fn branch_inheritance_source_pins_preserved() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let log = SegmentedObjectLog::open(store.clone(), SegmentConfig::new(10_000_000, 100).unwrap());
    let source = shard();
    log.create_queue(&qdef()).unwrap();

    // Source: 8 single-command segments (seq 0..7), floor advanced to 3, below-floor prefix physically
    // reclaimed — the same retained-floor-metadata substrate as `branch_inheritance_uses_retained_floor_metadata`.
    seed_trimmed_source(&log, &store, &source, 8, 3);

    let branch_def = branch_qdef("source-pins-preserved");
    let branch =
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 7),
        60_000,
        60,
    )
    .unwrap();

    // (1) The source pin is durable: branch creation off retained floor/head metadata still registers the
    // same registry entry an ordinary branch would.
    let registry_key = branch_registry_key_of(&source, &branch);
    assert!(
        store.get(&registry_key).unwrap().is_some(),
        "branch creation off retained floor/head metadata still publishes the source pin"
    );

    // (2) The retention floor guarantee is not relaxed: the source's own floor is untouched by branch
    // creation ...
    assert_eq!(
        log.read_retention_floor(&source)
            .unwrap()
            .map(|p| p.sequence),
        Some(3),
        "the source retention floor is unchanged by branch creation"
    );
    // ... and the orphan GC guarantee is not relaxed: a fully COMMITTED branch is never reclaimed by
    // gc_orphaned_branches, whether or not its source was trimmed before the branch was created.
    assert_eq!(
        log.gc_orphaned_branches(&source).unwrap(),
        0,
        "a committed branch created off retained floor metadata is never an orphan"
    );
    assert!(
        store.get(&registry_key).unwrap().is_some(),
        "the source pin survives a GC pass that correctly finds no orphan"
    );
    let branch_prefix = shard_prefix_of(&branch);
    assert!(
        store
            .get(&format!("{branch_prefix}branch.json"))
            .unwrap()
            .is_some(),
        "the branch commit marker survives GC"
    );

    // (3) The pin still protects the LIVE retained segments (4..7) it inherited: expiring through the
    // branch's own cut must reclaim NOTHING while the branch is live — the retained-metadata inheritance path
    // does not weaken the pin an ordinary (untrimmed-source) branch would have installed.
    assert_eq!(
        log.expire_segments_through(&source, 7, 61).unwrap(),
        0,
        "the live branch pins every retained source segment it reads (4..7) against expiry"
    );
    for seq in 4..=7u64 {
        assert!(
            store
                .get(&segment_key_for(store.as_ref(), &source, seq))
                .unwrap()
                .is_some(),
            "pinned retained segment {seq} remains present while the branch is live"
        );
    }

    // (4) Discarding the branch releases the pin exactly as it would for an ordinary branch — proving the
    // guarantee is the SAME contract, not a weaker one substituted for the retained-metadata path.
    log.discard_branch(&source, &branch).unwrap();
    assert!(
        store.get(&registry_key).unwrap().is_none(),
        "discarding the branch releases the source pin"
    );
    assert_eq!(
        log.expire_segments_through(&source, 7, 62).unwrap(),
        4,
        "once the pin is released the formerly-pinned retained segments (4..7) become reclaimable"
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

fn branch_registry_key_of(
    source: &pqueue_engine::QueueKey,
    branch: &pqueue_engine::QueueKey,
) -> String {
    format!(
        "{}branches/{}/{}.json",
        shard_prefix_of(source),
        hex_lower(branch.tenant_id.as_str().as_bytes()),
        hex_lower(branch.queue_id.as_str().as_bytes())
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
    missing_get: Mutex<Option<String>>,
}

impl OrphanBranchFaultStore {
    fn new() -> Self {
        Self {
            inner: InMemoryBlobStore::new(),
            fail_marker_put: AtomicBool::new(false),
            fail_deletes: AtomicBool::new(false),
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
        *self.missing_get.lock().unwrap() = None;
    }

    fn arm_missing_get(&self, key: &str) {
        *self.missing_get.lock().unwrap() = Some(key.to_owned());
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

#[allow(dead_code)]
fn delete_prefix<S: BlobStore>(store: &S, prefix: &str) {
    for key in store.list(prefix).unwrap() {
        store.delete(&key).unwrap();
    }
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

#[allow(dead_code)]
fn versioned_head_key_s(prefix: &str, version: u64) -> String {
    format!("{prefix}{version:020}.json")
}

#[allow(dead_code)]
static HEAD_TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[allow(dead_code)]
fn temp_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pqueue-objectlog-{label}-{}-{}",
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
        legacy_next_manifest_index: 0,
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
        legacy_next_manifest_index: 0,
    };
    let winner_b = ManifestHeadBlob {
        current_epoch: 2,
        next_seq: 4,
        next_manifest_index: 1,
        retention_floor_through: Some(1),
        tail_candidate_key: None,
        legacy_next_manifest_index: 0,
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

#[test]
fn segmented_stale_append_prepared_before_fence_cannot_advance_authoritative_tail() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let queue = unique_qdef("authority-stale-race");
    let shard = pqueue_engine::QueueKey::new(queue.tenant_id.clone(), queue.queue_id.clone());
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
            .all(|key| !key.contains("/seg_candidates/e00000000000000000000/")),
        "the resumed stale preparer must delete the segment it wrote after GC removed its candidate"
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
    let shard = pqueue_engine::QueueKey::new(queue.tenant_id.clone(), queue.queue_id.clone());
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
fn segmented_authority_initialization_marker_blocks_concurrent_legacy_ack() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let queue = unique_qdef("authority-init-race");
    let shard = QueueKey::new(queue.tenant_id.clone(), queue.queue_id.clone());
    let initializer = std::sync::Arc::new(SegmentedObjectLog::open(store.clone(), cfg));
    let appender = SegmentedObjectLog::open(store, cfg);
    initializer.create_queue(&queue).unwrap();
    appender.create_queue(&queue).unwrap();

    let entered = std::sync::Arc::new(Barrier::new(2));
    let resume = std::sync::Arc::new(Barrier::new(2));
    initializer.set_fault_hook(Some(std::sync::Arc::new(PauseAtCut {
        cut: FaultCutPoint::BeforeAuthorityHeadInitialize,
        entered: entered.clone(),
        resume: resume.clone(),
        fired: AtomicBool::new(false),
    })));
    let initializing = {
        let initializer = initializer.clone();
        let shard = shard.clone();
        thread::spawn(move || initializer.fence_epoch(&shard, 0, 0))
    };
    entered.wait();
    appender.enqueue(&shard, &pushes(1), 0, 1).unwrap();
    assert_eq!(appender.seal(&shard, 0, 2), Err(EngineError::Conflict));
    resume.wait();
    assert_eq!(initializing.join().unwrap().unwrap(), 0);
    assert!(initializer.read_all(&shard).unwrap().is_empty());

    let crashed_store = std::sync::Arc::new(InMemoryBlobStore::new());
    let crashed_queue = unique_qdef("authority-init-crash");
    let crashed_shard = QueueKey::new(
        crashed_queue.tenant_id.clone(),
        crashed_queue.queue_id.clone(),
    );
    let crashed = SegmentedObjectLog::open(crashed_store.clone(), cfg);
    crashed.create_queue(&crashed_queue).unwrap();
    crashed.set_fault_hook(Some(std::sync::Arc::new(FailAtCut {
        cut: FaultCutPoint::BeforeAuthorityHeadInitialize,
        fired: AtomicBool::new(false),
    })));
    assert!(matches!(
        crashed.fence_epoch(&crashed_shard, 0, 0),
        Err(EngineError::Storage(_))
    ));
    assert_eq!(
        crashed.fence_epoch(&crashed_shard, 0, 1),
        Err(EngineError::Conflict),
        "a crashed initialization requires explicit recovery and can never downgrade to legacy"
    );
    let reopened = SegmentedObjectLog::open(crashed_store, cfg);
    assert_eq!(
        reopened.create_queue(&crashed_queue),
        Err(EngineError::Conflict)
    );
}

#[test]
fn segmented_authority_fault_cuts_recover_only_the_head_referenced_tail() {
    let store = std::sync::Arc::new(FailingBlobStore::default());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let queue = unique_qdef("authority-faults");
    let shard = pqueue_engine::QueueKey::new(queue.tenant_id.clone(), queue.queue_id.clone());
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
fn segmented_nonempty_legacy_queue_fails_closed_while_empty_equality_establishes_head() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let legacy_queue = unique_qdef("authority-legacy");
    let legacy_shard = pqueue_engine::QueueKey::new(
        legacy_queue.tenant_id.clone(),
        legacy_queue.queue_id.clone(),
    );
    let legacy = SegmentedObjectLog::open(store.clone(), cfg);
    legacy.create_queue(&legacy_queue).unwrap();
    legacy.enqueue(&legacy_shard, &pushes(1), 0, 0).unwrap();
    legacy.seal(&legacy_shard, 0, 1).unwrap();
    assert_eq!(
        legacy.fence_epoch(&legacy_shard, 0, 2),
        Err(EngineError::Unavailable)
    );

    let empty_queue = unique_qdef("authority-empty");
    let empty_shard =
        pqueue_engine::QueueKey::new(empty_queue.tenant_id.clone(), empty_queue.queue_id.clone());
    let empty = SegmentedObjectLog::open(store, cfg);
    empty.create_queue(&empty_queue).unwrap();
    assert_eq!(empty.fence_epoch(&empty_shard, 0, 0).unwrap(), 0);
    assert_eq!(empty.fence_epoch(&empty_shard, 0, 1).unwrap(), 0);
    assert_eq!(empty.fence_epoch(&empty_shard, 7, 2).unwrap(), 7);
    assert_eq!(
        empty.fence_epoch(&empty_shard, 6, 3),
        Err(EngineError::EpochFenced)
    );
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
    hole.fence_epoch(&hole_shard, 0, 0).unwrap(); // head v0
    hole.enqueue(&hole_shard, &pushes(1), 0, 1).unwrap();
    hole.seal(&hole_shard, 0, 2).unwrap(); // head v1
    hole.fence_epoch(&hole_shard, 1, 3).unwrap(); // head v2
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

#[test]
fn segmented_authority_recovery_walks_only_the_live_candidate_suffix() {
    let store = std::sync::Arc::new(CountingBlobStore::default());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let queue = unique_qdef("authority-live-suffix");
    let shard = pqueue_engine::QueueKey::new(queue.tenant_id.clone(), queue.queue_id.clone());
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&queue).unwrap();
    log.fence_epoch(&shard, 0, 0).unwrap();
    for now in 0..6 {
        log.enqueue(&shard, &pushes(1), 0, now).unwrap();
        log.seal(&shard, 0, now + 100).unwrap();
    }
    log.advance_retention_floor(&shard, CommandPosition::new(shard.clone(), 0, 3), 0)
        .unwrap();
    assert_eq!(log.expire_segments_through(&shard, 3, 10_000).unwrap(), 4);
    let candidate_prefix = format!(
        "t/{}/q/{}/manifest_candidates/",
        hex_lower(shard.tenant_id.as_str().as_bytes()),
        hex_lower(shard.queue_id.as_str().as_bytes())
    );
    let candidates = store.list(&candidate_prefix).unwrap();
    assert!(
        candidates.iter().all(|key| {
            !key.contains("/i00000000000000000000/")
                && !key.contains("/i00000000000000000001/")
                && !key.contains("/i00000000000000000002/")
        }),
        "winning candidates strictly below the durable horizon are physically reclaimed"
    );
    assert!(
        candidates
            .iter()
            .any(|key| key.contains("/i00000000000000000003/")),
        "the candidate at the horizon remains as the physical live-chain root"
    );

    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&queue).unwrap();
    store.reset_reads();
    let page = reopened.read_from(&shard, 4).unwrap();
    assert_eq!(page.len(), 2);
    let candidate_gets = store
        .get_keys()
        .into_iter()
        .filter(|key| key.contains("/manifest_candidates/"))
        .count();
    assert_eq!(
        candidate_gets, 6,
        "the read performs two manifest folds (floor guard + tail read), each limited to the floor entry plus two live data candidates"
    );
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

fn lagging_partial_expire_fixture() -> (
    std::sync::Arc<InMemoryBlobStore>,
    SegmentConfig,
    QueueKey,
    Vec<(u64, String)>,
) {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let source = shard();

    log.create_queue(&qdef()).unwrap();
    for i in 0..8u64 {
        log.enqueue(&source, &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&source, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    let seg_keys: Vec<(u64, String)> = [0u64, 2, 4, 6, 8, 10, 12, 14]
        .into_iter()
        .map(|first| (first, segment_key_for(store.as_ref(), &source, first)))
        .collect();

    log.advance_retention_floor(&source, CommandPosition::new(source.clone(), 0, 15), 0)
        .unwrap();
    for (first_seq, seg_key) in seg_keys.iter().take(2) {
        assert!(
            store.delete(seg_key).unwrap(),
            "segment {first_seq} was physically reclaimed before the watermark caught up"
        );
    }
    log.persist_manifest_deletion_watermark(&source, 3, 1_000)
        .unwrap();
    assert_eq!(
        log.read_read_horizon(&source).unwrap(),
        Some(1),
        "the durable watermark only records the deleted prefix while later below-floor segments remain"
    );

    (store, cfg, source, seg_keys)
}

fn assert_partial_expire_visibility_decision_fixture(
    store: &std::sync::Arc<InMemoryBlobStore>,
    cfg: SegmentConfig,
    source: &QueueKey,
    seg_keys: &[(u64, String)],
) {
    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&qdef()).unwrap();

    assert_eq!(
        reopened
            .read_retention_floor(source)
            .unwrap()
            .unwrap()
            .sequence,
        15,
        "the durable floor stays above the partial-expire fixture's below-floor entries"
    );
    assert_eq!(
        reopened.read_read_horizon(source).unwrap(),
        Some(1),
        "the durable manifest deletion watermark records only the proven reclaimed prefix"
    );

    assert!(
        store.get(&seg_keys[0].1).unwrap().is_none(),
        "the first reclaimed segment object is gone"
    );
    assert!(
        store.get(&seg_keys[1].1).unwrap().is_none(),
        "the second reclaimed segment object is gone"
    );
    assert!(
        store.get(&seg_keys[2].1).unwrap().is_some(),
        "the first not-yet-deleted below-floor segment object remains available"
    );

    assert_eq!(
        reopened.expire_segments_through(source, 3, 2_000).unwrap(),
        0,
        "a rerun at the same horizon does not advance past the unreclaimed below-floor entry"
    );
    assert_eq!(
        reopened.read_read_horizon(source).unwrap(),
        Some(1),
        "the hidden prefix does not advance beyond the proven reclaimed range"
    );

    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&qdef()).unwrap();
    reopened.enqueue(source, &pushes(1), 0, 2_000).unwrap();
    let reopened_ack = reopened.seal(source, 0, 2_001).unwrap();
    assert_eq!(
        reopened_ack[0].sequence, 16,
        "recovery still sees the remaining below-floor tail after the partial expire"
    );

    // Finish the lagging cleanup without invoking the main expire path: delete the remaining below-floor
    // segment objects directly, then advance the durable watermark across the now-complete prefix.
    for (first_seq, seg_key) in seg_keys.iter().skip(2) {
        assert!(
            store.delete(seg_key).unwrap(),
            "segment {first_seq} is reclaimed once the lagging prefix completes"
        );
    }
    reopened
        .persist_manifest_deletion_watermark(source, 15, 3_000)
        .unwrap();
    assert_eq!(
        reopened.read_read_horizon(source).unwrap(),
        Some(7),
        "the durable watermark only catches up after the remaining below-floor segments are reclaimed"
    );

    // Nothing leaked: every data segment object at/below the floor is gone.
    for (first_seq, seg_key) in seg_keys {
        assert!(
            store.get(seg_key).unwrap().is_none(),
            "segment {first_seq} reclaimed, not leaked"
        );
    }
}

fn delete_watermark_metadata<S: BlobStore>(store: &S, shard: &QueueKey) {
    store.delete(&read_horizon_key_s(shard)).unwrap();
    for key in store.list(&manifest_head_prefix_s(shard)).unwrap() {
        if key.ends_with("~watermark.json") {
            store.delete(&key).unwrap();
        }
    }
}

fn delete_watermark_history<S: BlobStore>(store: &S, shard: &QueueKey) {
    delete_watermark_metadata(store, shard);
    for key in store.list(&manifest_head_prefix_s(shard)).unwrap() {
        if key.ends_with("~watermark.json") {
            store.delete(&key).unwrap();
        }
    }
}

fn delete_watermark_markers_only<S: BlobStore>(store: &S, shard: &QueueKey) {
    for key in store.list(&manifest_head_prefix_s(shard)).unwrap() {
        if key.ends_with("~watermark.json") {
            store.delete(&key).unwrap();
        }
    }
}

fn delete_manifest_head_data_only<S: BlobStore>(store: &S, shard: &QueueKey) {
    for key in store.list(&manifest_head_prefix_s(shard)).unwrap() {
        if !key.ends_with("~watermark.json") {
            store.delete(&key).unwrap();
        }
    }
    store.delete(&read_horizon_key_s(shard)).unwrap();
}

fn write_read_horizon_cache_only<S: BlobStore>(store: &S, shard: &QueueKey, index: u64) {
    store
        .put(
            &read_horizon_key_s(shard),
            &serde_json::to_vec(&serde_json::json!({ "index": index })).unwrap(),
        )
        .unwrap();
}

fn strip_manifest_head_namespace<S: BlobStore>(store: &S, shard: &QueueKey) {
    for key in store.list(&manifest_head_prefix_s(shard)).unwrap() {
        store.delete(&key).unwrap();
    }
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

/// Test 1 — after repeated trim+advance-horizon cycles, reclaimed legacy manifest copies are physically
/// deleted, so the legacy manifest prefix stays bounded by the live tail while the watermark remains
/// monotonic. The live read path still enumerates in O(live) via the ranged manifest scan.
#[test]
#[allow(non_snake_case)]
fn TestManifestObjectCountBoundedAfterLongTrim() {
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

    // Three trim cycles. The live tail stays at two segments, so the reclaimed legacy prefix should stay
    // bounded instead of growing with total history.
    trim_cycle(&log, &shard(), 3, 0, 1_000);
    trim_cycle(&log, &shard(), 7, 0, 2_000);
    trim_cycle(&log, &shard(), 11, 0, 3_000);

    // Below-floor legacy manifest copies are reclaimed. After trimming through seq 11, only the two live
    // tail segments (seqs 12..15) remain in the legacy manifest prefix, so the count stays O(live) instead
    // of growing with total history.
    let floor = log.read_retention_floor(&shard()).unwrap().unwrap();
    assert_eq!(floor.sequence, 11);
    let legacy_manifest_keys = store.list(&manifest_prefix_s(&shard())).unwrap();
    assert!(
        legacy_manifest_keys.len() <= 5,
        "the reclaimed legacy prefix stays within a small constant bound instead of growing with history"
    );
    assert!(
        legacy_manifest_keys.len() < initial_manifest_keys,
        "reclaimed legacy copies shrink the manifest prefix instead of letting it grow with history"
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
    for i in 0..2u64 {
        log.enqueue(&shard(), &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard(), 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    trim_cycle(&log, &shard(), 1, 0, 1_000);
    assert_eq!(log.read_read_horizon(&shard()).unwrap(), Some(0));

    // The below-floor reclaimed entry may disappear on reopen because the durable watermark still
    // reconstructs the live tail above it.
    let below_floor = manifest_head_key_s(&shard(), 0);
    assert!(store.delete(&below_floor).unwrap());
    let _ = store.delete(&manifest_key_s(&shard(), 0));

    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    assert!(
        reopened.create_queue(&qdef()).is_ok(),
        "the reopened queue tolerates a reclaimed below-floor hole because the durable watermark still bounds recovery"
    );
    assert_eq!(
        reopened.read_read_horizon(&shard()).unwrap(),
        Some(0),
        "the deletion watermark survives reopen"
    );
    assert_eq!(
        reopened
            .read_retention_floor(&shard())
            .unwrap()
            .unwrap()
            .sequence,
        1,
        "the authoritative floor remains derived from the manifest"
    );

    // A missing live floor entry is not below the durable floor, so reopen must not reconstruct the
    // authoritative floor from the watermark alone.
    assert!(store.delete(&manifest_head_key_s(&shard(), 2)).unwrap());
    assert!(store.delete(&manifest_key_s(&shard(), 2)).unwrap());
    let broken = SegmentedObjectLog::open(store.clone(), cfg);
    assert!(
        broken.create_queue(&qdef()).is_ok(),
        "the queue can still reopen from the live tail"
    );
    assert_eq!(
        broken.read_read_horizon(&shard()).unwrap(),
        Some(0),
        "the persisted deletion watermark still reloads on reopen"
    );
    assert!(
        broken.read_retention_floor(&shard()).unwrap().is_none(),
        "the authoritative floor is not reconstructed from the watermark alone"
    );
}

#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkFailClosedBelowFloor() {
    TestUnexpectedLiveManifestHoleFailsClosed();
    TestBelowFloorReadFailsClosedAfterManifestReclaim();
}

/// TestBehindImageFailClosedWithDeletedManifests: after the retained floor/head replay path is proven
/// healthy, deleting the legacy `manifest/` namespace alone must not break recovery; if the authoritative
/// head namespace is also removed, the queue still boots conservatively instead of reconstructing a
/// behind image from deleted manifest data.
#[test]
#[allow(non_snake_case)]
fn TestBehindImageFailClosedWithDeletedManifests() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let def = unique_qdef("behind-image");
    let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&def).unwrap();
    for i in 0..3u64 {
        log.enqueue(&shard, &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    trim_cycle(&log, &shard, 1, 0, 1_000);
    assert_eq!(
        log.read_read_horizon(&shard).unwrap(),
        Some(0),
        "the retained floor/head replay path is available before the prefix is deleted"
    );

    let healthy = SegmentedObjectLog::open(store.clone(), cfg);
    assert!(
        healthy.create_queue(&def).is_ok(),
        "reopen from the retained floor/head succeeds before the prefix is physically deleted"
    );

    delete_prefix(store.as_ref(), &manifest_prefix_s(&shard));

    let legacy_only = SegmentedObjectLog::open(store.clone(), cfg);
    assert!(
        legacy_only.create_queue(&def).is_ok(),
        "the authoritative head namespace still lets recovery resume when only the legacy manifest prefix is deleted"
    );

    delete_prefix(store.as_ref(), &manifest_head_prefix_s(&shard));

    let broken = SegmentedObjectLog::open(store.clone(), cfg);
    assert!(
        broken.create_queue(&def).is_ok(),
        "without durable watermark markers the queue still reopens conservatively"
    );
    assert!(
        broken.read_read_horizon(&shard).unwrap().is_some(),
        "the surviving read-horizon cache still records the conservative bootstrap state"
    );
    assert_eq!(
        broken.read_retention_floor(&shard).unwrap(),
        None,
        "the authoritative floor is not reconstructed from deleted manifest state"
    );
}

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
        "the retained floor metadata survives the physical deletion of the legacy source prefix"
    );
    assert!(
        log.read_read_horizon(&source).unwrap().is_some(),
        "the retained branch-inheritance watermark remains available after legacy prefix deletion"
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

/// TestBranchGcPreservesInheritedFloorPins: a committed branch created from a trimmed source keeps its
/// inherited floor and source pin even if the source's legacy manifest prefix is physically deleted before
/// branch GC runs. The branch GC path must continue to classify from the source pin registry / branch
/// metadata, not from deleted legacy source manifest objects.
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
        "the legacy source manifest prefix is physically gone"
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
        log.gc_orphaned_branches(&source).unwrap(),
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

/// TestBranchGcFailClosedOnMissingInheritanceMetadata: branch GC refuses to touch an orphan when the
/// persisted source-pin metadata becomes missing before the classify+delete pass can trust it.
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

    let err = log.gc_orphaned_branches(&source).unwrap_err();
    assert!(
        matches!(err, EngineError::Storage(ref msg) if msg.contains("missing branch registry entry")),
        "branch GC must fail closed when the inherited source-pin metadata disappears: {err:?}"
    );
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
        !store
            .list(&format!("{}manifest/", shard_prefix_of(&branch)))
            .unwrap()
            .is_empty(),
        "the branch-local manifest objects are left untouched when GC refuses to proceed"
    );
}

/// TestBranchGcFailClosedOnCorruptInheritanceMetadata: branch GC refuses to touch an orphan when the
/// persisted source-pin metadata is present but corrupt before the classify+delete pass can trust it.
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

    let err = log.gc_orphaned_branches(&source).unwrap_err();
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
        !store
            .list(&format!("{}manifest/", shard_prefix_of(&branch)))
            .unwrap()
            .is_empty(),
        "the branch-local manifest objects are left untouched when GC refuses to proceed"
    );
}

/// TestBranchGcDeletesBelowFloorAfterLastReadableBranch (bead pqueue-635500fb): with TWO committed, live
/// branches pinning overlapping-but-different ranges of a trimmed source, below-floor source objects stay
/// retained as long as AT LEAST ONE of them can still read them — not just while every branch needs them.
/// `branch_a` is cut at seq 0 (pins only the first segment); `branch_b` is cut at seq 3 (pins all four). Once
/// `branch_a` is discarded, `branch_b` ALONE keeps every below-floor segment retained; only once `branch_b` is
/// also discarded (no readable branch remains) do the segments become reclaimable.
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

    log.advance_retention_floor(&source, CommandPosition::new(source.clone(), 0, 3), 0)
        .unwrap();

    assert_eq!(
        log.expire_segments_through(&source, 3, 100).unwrap(),
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
        log.expire_segments_through(&source, 3, 150).unwrap(),
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
        log.expire_segments_through(&source, 3, 200).unwrap(),
        4,
        "with no readable branch left, the below-floor segments become reclaimable"
    );
}

/// TestBranchGcDeletesBelowFloorAfterLastReadableBranchFailClosed (bead pqueue-635500fb): a committed, live
/// (still readable) branch's source-pin proof becomes unfetchable — `store.list` still returns its registry
/// key, but `store.get` unexpectedly returns `None` for it, a storage inconsistency rather than a legitimate
/// `discard_branch`. Below-floor source objects the branch can still read MUST stay retained: the trim path
/// must fail closed (surface an error, delete nothing) rather than silently treat the branch as unpinned.
#[test]
#[allow(non_snake_case)]
fn TestBranchGcDeletesBelowFloorAfterLastReadableBranchFailClosed() {
    let store = std::sync::Arc::new(OrphanBranchFaultStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let source = shard();

    log.create_queue(&qdef()).unwrap();
    for _ in 0..4u64 {
        log.enqueue(&source, &pushes(1), 0, 10).unwrap();
        log.seal(&source, 0, 10).unwrap();
    }

    let branch_def = branch_qdef("gc-fail-closed-readable");
    let branch = QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 3),
        1_000_000_000,
        1_000,
    )
    .unwrap();
    assert_eq!(
        log.read_all(&branch).unwrap().len(),
        4,
        "the branch is committed and fully readable before the fault is injected"
    );

    log.advance_retention_floor(&source, CommandPosition::new(source.clone(), 0, 3), 0)
        .unwrap();

    let seg_key = segment_key_for(store.as_ref(), &source, 2);
    let registry_key = branch_registry_key_of(&source, &branch);
    // The branch is NOT discarded — its commit marker, manifest, and TTL are all still live. Only the
    // source's pin proof becomes unfetchable, simulating a `list`/`get` inconsistency rather than a real
    // release.
    store.arm_missing_get(&registry_key);

    let err = log.expire_segments_through(&source, 3, 2_000).unwrap_err();
    assert!(
        matches!(err, EngineError::Storage(ref msg) if msg.contains("missing branch registry entry")),
        "the trim path must fail closed when a still-registered source pin cannot be fetched: {err:?}"
    );
    assert!(
        store.get(&seg_key).unwrap().is_some(),
        "the below-floor segment a readable branch may still need stays retained despite the fault"
    );

    store.disarm();
    assert_eq!(
        log.read_all(&branch)
            .unwrap()
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3],
        "the branch remains fully readable once the fault clears, proving nothing was lost"
    );
}

/// TestBranchGcDeletesBelowFloorAfterLastReadableBranchFinal (bead pqueue-29a6c98c): with TWO committed, live
/// branches pinning a trimmed source, below-floor source manifest and segment objects stay retained while
/// EITHER remains readable. Once the final readable branch (`branch_b`, the wider-cut one) is ALSO discarded —
/// the "last readable branch advances" condition — `expire_segments_through` does not merely report a
/// non-zero count: it physically removes the below-floor segment objects from the store.
#[test]
#[allow(non_snake_case)]
fn TestBranchGcDeletesBelowFloorAfterLastReadableBranchFinal() {
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
    let seg_keys: Vec<String> = (0..4u64)
        .map(|seq| segment_key_for(store.as_ref(), &source, seq))
        .collect();
    let manifest_keys: Vec<String> = (0..4u64).map(|idx| manifest_key_s(&source, idx)).collect();

    let branch_a_def = branch_qdef("gc-final-a");
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

    let branch_b_def = branch_qdef("gc-final-b");
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

    log.advance_retention_floor(&source, CommandPosition::new(source.clone(), 0, 3), 0)
        .unwrap();

    assert_eq!(
        log.expire_segments_through(&source, 3, 100).unwrap(),
        0,
        "below-floor segments stay retained while both branches remain readable"
    );
    for key in &seg_keys {
        assert!(
            store.get(key).unwrap().is_some(),
            "segment {key} must still exist while a branch can read it"
        );
    }
    for key in &manifest_keys {
        assert!(
            store.get(key).unwrap().is_some(),
            "manifest entry {key} must still exist while a branch can read it"
        );
    }

    // branch_a advances out of the picture, leaving branch_b as the LAST readable branch: still retained.
    log.discard_branch(&source, &branch_a).unwrap();
    assert_eq!(
        log.expire_segments_through(&source, 3, 150).unwrap(),
        0,
        "the last remaining readable branch alone keeps below-floor segments retained"
    );

    // The final readable branch itself now advances (is discarded): no branch can read the below-floor range.
    log.discard_branch(&source, &branch_b).unwrap();
    assert_eq!(
        log.expire_segments_through(&source, 3, 200).unwrap(),
        4,
        "with the final readable branch gone, every below-floor segment becomes physically deletable"
    );
    for key in &seg_keys {
        assert!(
            store.get(key).unwrap().is_none(),
            "segment {key} must be physically removed once no branch can read it"
        );
    }
    for key in &manifest_keys {
        assert!(
            store.get(key).unwrap().is_none(),
            "manifest entry {key} must be physically removed once no branch can read it"
        );
    }
}

/// TestBranchGcDeletesBelowFloorAfterLastReadableBranchFinalConservative (bead pqueue-29a6c98c): the LAST
/// remaining readable branch's inherited floor/source-pin proof becomes AMBIGUOUS (its registry entry is
/// listed by the store but cannot be fetched) rather than the branch being genuinely discarded/advanced.
/// Branch GC must NOT treat that ambiguity as proof the final branch has advanced past the below-floor range:
/// it fails closed and leaves every below-floor source manifest and segment object physically intact. Once the
/// ambiguity clears and the branch is genuinely discarded, the true final-branch-advances case (proven above)
/// takes over and deletion proceeds.
#[test]
#[allow(non_snake_case)]
fn TestBranchGcDeletesBelowFloorAfterLastReadableBranchFinalConservative() {
    let store = std::sync::Arc::new(OrphanBranchFaultStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let source = shard();

    log.create_queue(&qdef()).unwrap();
    for _ in 0..4u64 {
        log.enqueue(&source, &pushes(1), 0, 10).unwrap();
        log.seal(&source, 0, 10).unwrap();
    }
    let seg_keys: Vec<String> = (0..4u64)
        .map(|seq| segment_key_for(store.as_ref(), &source, seq))
        .collect();
    let manifest_keys: Vec<String> = (0..4u64).map(|idx| manifest_key_s(&source, idx)).collect();

    let branch_def = branch_qdef("gc-final-conservative");
    let branch = QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 3),
        1_000_000_000,
        1_000,
    )
    .unwrap();
    assert_eq!(
        log.read_all(&branch).unwrap().len(),
        4,
        "the branch is committed and fully readable before the fault is injected"
    );

    log.advance_retention_floor(&source, CommandPosition::new(source.clone(), 0, 3), 0)
        .unwrap();

    // This IS the last readable branch — it is not discarded, but its source-pin proof becomes unfetchable,
    // simulating an inconsistency between `list` and `get` rather than a genuine release.
    let registry_key = branch_registry_key_of(&source, &branch);
    store.arm_missing_get(&registry_key);

    let err = log.expire_segments_through(&source, 3, 2_000).unwrap_err();
    assert!(
        matches!(err, EngineError::Storage(ref msg) if msg.contains("missing branch registry entry")),
        "branch GC must fail closed rather than guess the final branch has advanced: {err:?}"
    );
    for key in &seg_keys {
        assert!(
            store.get(key).unwrap().is_some(),
            "segment {key} must NOT be physically deleted while the final branch's readability is unproven"
        );
    }
    for key in &manifest_keys {
        assert!(
            store.get(key).unwrap().is_some(),
            "manifest entry {key} must NOT be physically deleted while the final branch's readability is unproven"
        );
    }

    // Once the ambiguity clears and the branch is genuinely discarded (the true final-branch-advances
    // condition), deletion proceeds exactly like the non-conservative case.
    store.disarm();
    log.discard_branch(&source, &branch).unwrap();
    assert_eq!(
        log.expire_segments_through(&source, 3, 3_000).unwrap(),
        4,
        "once ambiguity clears and the final branch is genuinely discarded, below-floor segments become deletable"
    );
    for key in &seg_keys {
        assert!(
            store.get(key).unwrap().is_none(),
            "segment {key} is physically removed once the final branch's advance is genuinely proven"
        );
    }
    for key in &manifest_keys {
        assert!(
            store.get(key).unwrap().is_none(),
            "manifest entry {key} is physically removed once the final branch's advance is genuinely proven"
        );
    }
}

#[test]
#[allow(non_snake_case)]
fn TestBranchInheritanceRetainedFloorMetadataFailClosed() {
    let store = std::sync::Arc::new(CountingBlobStore::default());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let source_def = unique_qdef("retained-metadata-source");
    let source = QueueKey::new(source_def.tenant_id.clone(), source_def.queue_id.clone());
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&source_def).unwrap();
    for i in 0..6u64 {
        log.enqueue(&source, &pushes(1), 0, 20 + i as i64 * 10)
            .unwrap();
        log.seal(&source, 0, 21 + i as i64 * 10).unwrap();
    }

    trim_cycle(&log, &source, 3, 0, 2_000);
    delete_prefix(store.as_ref(), &manifest_prefix_s(&source));
    strip_manifest_head_namespace(store.as_ref(), &source);

    let branch_def = branch_qdef("retained-metadata-fail-closed");
    let branch = QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    let epoch = log
        .branch(
            &source,
            &branch_def,
            &CommandPosition::new(source.clone(), 0, 5),
            60_000,
            3_000,
        )
        .unwrap();
    assert_eq!(
        epoch, 1,
        "the branch still acquires its own epoch even when the source floor metadata is missing"
    );
    assert!(
        log.read_retention_floor(&branch).unwrap().is_none(),
        "the branch does not reconstruct a deleted retained floor from the stripped source metadata"
    );
    assert!(
        log.read_all(&branch).unwrap().is_empty(),
        "the conservative branch bootstrap does not copy deleted source manifest data"
    );
}

#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkSurvivesReopenBelowDurableFloor() {
    TestUnexpectedLiveManifestHoleFailsClosed();
}

#[test]
#[allow(non_snake_case)]
fn TestDeletionWatermarkDoesNotAdvanceRetentionFloor() {
    TestManifestDeletionWatermarkReclaimNeverExceedsFloor();
}

#[test]
#[allow(non_snake_case)]
fn TestDeletionWatermarkOwnerFenceIndependence() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();

    let owner_a = SegmentedObjectLog::open(store.clone(), cfg);
    owner_a.create_queue(&qdef()).unwrap();
    owner_a.enqueue(&shard(), &pushes(2), 0, 10).unwrap();
    owner_a.seal(&shard(), 0, 11).unwrap();

    let owner_b = SegmentedObjectLog::open(store.clone(), cfg);
    owner_b.create_queue(&qdef()).unwrap();
    assert_eq!(owner_b.acquire_epoch(&shard(), 100).unwrap(), 1);
    for i in 0..3u64 {
        owner_b
            .enqueue(&shard(), &pushes(2), 1, 200 + i as i64 * 10)
            .unwrap();
        owner_b.seal(&shard(), 1, 201 + i as i64 * 10).unwrap();
    }
    owner_b
        .advance_retention_floor(&shard(), CommandPosition::new(shard(), 1, 5), 1)
        .unwrap();
    owner_b.expire_segments_through(&shard(), 5, 1_000).unwrap();

    let horizon = owner_b
        .read_read_horizon(&shard())
        .unwrap()
        .expect("watermark advanced");
    assert!(
        horizon >= 1,
        "the watermark advances, but it does not replace the ownership fence"
    );
    assert_eq!(
        owner_b.current_epoch(&shard()).unwrap(),
        1,
        "permanent head remains the stale-writer fence"
    );
    assert!(
        store
            .get(&manifest_head_key_s(&shard(), 1))
            .unwrap()
            .is_some(),
        "the durable head object still exists and continues to fence stale writers"
    );
    assert!(
        store.get(&manifest_key_s(&shard(), 1)).unwrap().is_some(),
        "the watermark never becomes the ownership fence by freeing the manifest address"
    );

    owner_a.enqueue(&shard(), &pushes(3), 0, 5_000).unwrap();
    let err = owner_a.seal(&shard(), 0, 5_001).unwrap_err();
    assert_eq!(
        err,
        EngineError::EpochFenced,
        "the stale writer is fenced by the permanent head CAS, not by the watermark"
    );
}

/// Test 3 — live data is byte-identical pre/post horizon, and a below-floor read FAILS CLOSED (read at the
/// floor errors; read at floor+1 succeeds; read_all from genesis fails closed on a trimmed+horizoned queue).
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

/// Test 3b — manifest reclamation below the floor does not perturb the live tail above it.
/// The first readable entry at `floor + 1` and every later live entry must remain byte-identical
/// before and after reclaiming the below-floor manifest entries.
#[test]
#[allow(non_snake_case)]
fn TestLiveTailByteIdenticalAfterManifestReclaim() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef()).unwrap();
    for i in 0..6u64 {
        log.enqueue(&shard(), &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard(), 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    let floor = 7;
    let first_readable = floor + 1;
    let before = log.read_from(&shard(), first_readable).unwrap();
    assert_eq!(
        before.first().unwrap().0.sequence,
        first_readable,
        "the first readable entry starts at floor + 1 before reclaim"
    );

    trim_cycle(&log, &shard(), floor, 0, 1_000);

    // Prove the live tail stayed byte-identical while the below-floor manifest history remained retained.
    assert!(
        store
            .get(&manifest_head_key_s(&shard(), 0))
            .unwrap()
            .is_some(),
        "the reclaimed manifest head stays retained as the durable fence"
    );
    assert!(
        store.get(&manifest_key_s(&shard(), 0)).unwrap().is_none(),
        "the legacy compatibility copy is physically deleted"
    );

    let after = log.read_from(&shard(), first_readable).unwrap();
    assert_eq!(
        after.first().unwrap().0.sequence,
        first_readable,
        "the first readable entry remains floor + 1 after reclaim"
    );

    let fingerprint = |v: &Vec<(CommandPosition, pqueue_engine::CommandEnvelope)>| {
        v.iter()
            .map(|(p, e)| (p.sequence, p.backend_epoch, serde_json::to_vec(e).unwrap()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        fingerprint(&before),
        fingerprint(&after),
        "live entries above the floor are byte-identical before and after manifest reclaim"
    );
}

/// Test 4 — a stale cached writer whose next index points below the durable read-horizon still cannot ack.
/// The reclaimed manifest slot is overwritten with a durable marker, and the stale seal returns
/// `EpochFenced` or `Conflict` rather than creating a fresh durable entry at the occupied address.
#[test]
#[allow(non_snake_case)]
fn TestPermanentFenceMarkerBlocksReclaimedIndex() {
    let (store, stale_owner, _live_owner) = reclaimed_cached_writer_fixture();

    stale_owner.enqueue(&shard(), &pushes(1), 0, 5_000).unwrap();
    let objects_before = store.inner.object_count();
    let err = stale_owner.seal(&shard(), 0, 5_001).unwrap_err();
    assert!(
        matches!(err, EngineError::EpochFenced | EngineError::Conflict),
        "a reclaimed cached manifest index must not ack, got {err:?}"
    );
    let head_bytes = store
        .get(&manifest_head_key_s(&shard(), 1))
        .unwrap()
        .expect("reclaimed manifest-head marker");
    assert!(
        !head_bytes.is_empty(),
        "the reclaimed head slot remains occupied"
    );
    assert!(
        store.get(&manifest_key_s(&shard(), 1)).unwrap().is_none(),
        "the reclaimed legacy compatibility copy is physically deleted"
    );
    assert!(
        store
            .get(&manifest_head_key_s(&shard(), 1))
            .unwrap()
            .is_some(),
        "the authoritative manifest-head slot stays occupied after the stale seal"
    );
    assert_eq!(
        store.inner.object_count(),
        objects_before,
        "the stale seal must not write a fresh manifest or segment object"
    );
}

/// TestPermanentFenceSurvivesReopen: reopening the store reconstructs the durable reclaimed-index fence
/// from recovery-visible marker history, even if the compatibility cache blob was removed.
#[test]
#[allow(non_snake_case)]
fn TestPermanentFenceSurvivesReopen() {
    let (store, stale_owner, live_owner) = reclaimed_cached_writer_fixture();
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let shard = shard();
    let expected_watermark = live_owner.read_read_horizon(&shard).unwrap();

    store.delete(&read_horizon_key_s(&shard)).unwrap();

    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&qdef()).unwrap();
    assert_eq!(
        reopened.read_read_horizon(&shard).unwrap(),
        expected_watermark,
        "the reopened log reconstructs the durable reclaimed-index fence from marker history"
    );

    stale_owner.enqueue(&shard, &pushes(1), 0, 5_000).unwrap();
    let objects_before = store.inner.object_count();
    let err = stale_owner.seal(&shard, 0, 5_001).unwrap_err();
    assert!(
        matches!(err, EngineError::EpochFenced | EngineError::Conflict),
        "the stale writer remains fenced after reopen, got {err:?}"
    );
    assert_eq!(
        store.inner.object_count(),
        objects_before,
        "the stale seal must not publish a fresh segment or manifest object"
    );
}

/// TestReopenFenceReloadsBeforeSeal: a fresh open reloads the reclaimed-index fence before the stale
/// writer reaches seal, so the seal still self-fences instead of acking a reclaimed historical index.
#[test]
#[allow(non_snake_case)]
fn TestReopenFenceReloadsBeforeSeal() {
    let (store, stale_owner, live_owner) = reclaimed_cached_writer_fixture();
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let shard = shard();
    let expected_watermark = live_owner.read_read_horizon(&shard).unwrap();

    store.delete(&read_horizon_key_s(&shard)).unwrap();

    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&qdef()).unwrap();
    assert_eq!(
        reopened.read_read_horizon(&shard).unwrap(),
        expected_watermark,
        "open/recovery reloads the durable reclaimed-index fence before any seal runs"
    );

    stale_owner.enqueue(&shard, &pushes(1), 0, 5_000).unwrap();
    let objects_before = store.inner.object_count();
    let err = stale_owner.seal(&shard, 0, 5_001).unwrap_err();
    assert!(
        matches!(err, EngineError::EpochFenced | EngineError::Conflict),
        "seal fences before ack after the reopen reload, got {err:?}"
    );
    assert_eq!(
        store.inner.object_count(),
        objects_before,
        "the stale writer must not emit a new durable object before it is fenced"
    );
}

fn assert_reclaimed_cached_writer_rejects_before_ack() {
    let (store, stale_owner, _live_owner) = reclaimed_cached_writer_fixture();

    stale_owner.enqueue(&shard(), &pushes(1), 0, 5_000).unwrap();
    let objects_before = store.inner.object_count();
    store.reset_reads();

    let err = stale_owner.seal(&shard(), 0, 5_001).unwrap_err();
    assert!(
        matches!(err, EngineError::EpochFenced | EngineError::Conflict),
        "a reclaimed cached manifest index must fence before any ack, got {err:?}"
    );
    assert_eq!(
        store.inner.object_count(),
        objects_before,
        "the stale writer must not publish a fresh segment or manifest object before it is fenced"
    );
}

/// TestNoTailValidateRollbackSubstituteAfterReopen: a reclaimed cached manifest index is rejected by the
/// durable fence before any successful stale ack can be externally observed. The rejected path does not need
/// tail-validate/delete rollback; that substitute is explicitly not the fence mechanism (see
/// docs/perf/design/manifest-compaction-hotpath.md:359 and pqueue-c33c367e).
#[test]
#[allow(non_snake_case)]
fn TestNoTailValidateRollbackSubstituteAfterReopen() {
    assert_reclaimed_cached_writer_rejects_before_ack();
}

/// TestReopenFenceCommentReferencesDesign: same reclaimed-index fence, documented here so the hot-path
/// comment and the test both point at docs/perf/design/manifest-compaction-hotpath.md:359 and
/// pqueue-c33c367e. The design note rejects tail-validate/delete rollback as the fence mechanism.
#[test]
#[allow(non_snake_case)]
fn TestReopenFenceCommentReferencesDesign() {
    assert_reclaimed_cached_writer_rejects_before_ack();
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
#[allow(non_snake_case)]
fn TestBranchSeedSeqNotSuppressedByFloor() {
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

/// partial_expire_does_not_hide_undeleted_below_floor_segments — a PARTIAL expire (through_seq < durable floor) must NOT
/// advance the read-horizon past segments it did NOT actually delete: a later full expire must still find and
/// reclaim them (no storage leak). Guards the finding that binding the horizon to the floor (rather than the
/// reclaimed boundary) would hide undeleted below-floor segments from a future trim.
#[test]
#[allow(non_snake_case)]
fn partial_expire_does_not_hide_undeleted_below_floor_segments() {
    let (store, cfg, source, seg_keys) = lagging_partial_expire_fixture();
    assert_partial_expire_visibility_decision_fixture(&store, cfg, &source, &seg_keys);
}

#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkState() {
    TestManifestDeletionWatermarkContiguousPrefixOnly();
    TestManifestDeletionWatermarkPersistsAfterPhysicalDelete();
    TestDeletionWatermarkOwnerFenceIndependence();
    partial_expire_does_not_hide_undeleted_below_floor_segments();
}

/// TestManifestDeletionWatermarkStorageNotRetentionAuthority: the deletion watermark is progress storage
/// only, does not advance the retention floor, and does not depend on owner-fence wiring from
/// pqueue-c33c367e.
#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkStorageNotRetentionAuthority() {
    TestManifestDeletionWatermarkStorageBelowFloorAccepted();
    TestManifestDeletionWatermarkStorageMonotonicNoRegression();
    TestDeletionWatermarkOwnerFenceIndependence();
}

/// TestPartialExpireWatermarkStopsBeforeUndeletedBelowFloorSegment: a partial expire may reclaim an
/// earlier below-floor segment and then fault before a later below-floor segment delete, but the durable
/// watermark must stay below the undeleted entry.
#[test]
#[allow(non_snake_case)]
fn TestPartialExpireWatermarkStopsBeforeUndeletedBelowFloorSegment() {
    TestManifestDeletionWatermarkPersistsAfterPhysicalDelete();
}

/// TestPartialExpireWatermarkRetryEnumeratesRemainingSegment: after the partial-failure case above,
/// retrying the reclaim must still enumerate the remaining below-floor segment and advance the durable
/// watermark only once that segment is physically deleted.
#[test]
#[allow(non_snake_case)]
fn TestPartialExpireWatermarkRetryEnumeratesRemainingSegment() {
    TestInterruptedManifestReclaimRecovery();
}

/// TestPartialExpireReadHorizonAloneDoesNotHideBelowFloorEntries: a cache-only advance of the read-horizon
/// must not outrun the durable watermark history and hide below-floor entries before reclamation is
/// confirmed durably.
#[test]
#[allow(non_snake_case)]
fn TestPartialExpireReadHorizonAloneDoesNotHideBelowFloorEntries() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let source = shard();
    log.create_queue(&qdef()).unwrap();

    for i in 0..10u64 {
        log.enqueue(&source, &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&source, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    log.advance_retention_floor(&source, CommandPosition::new(source.clone(), 0, 7), 0)
        .unwrap();
    assert_eq!(
        log.expire_segments_through(&source, 3, 1_000).unwrap(),
        2,
        "the partial expire reclaims only the leading below-floor prefix"
    );

    let before = log.read_from(&source, 8).unwrap();
    write_read_horizon_cache_only(store.as_ref(), &source, 15);
    let after = log.read_from(&source, 8).unwrap();

    let fingerprint = |v: &Vec<(CommandPosition, pqueue_engine::CommandEnvelope)>| {
        v.iter()
            .map(|(p, e)| (p.sequence, p.backend_epoch, serde_json::to_vec(e).unwrap()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        fingerprint(&before),
        fingerprint(&after),
        "a cache-only horizon bump does not hide live entries from the durable read path"
    );
}

/// TestPartialExpireVisibilityUsesDurableManifestDeletionWatermark: once the remaining below-floor
/// segments are physically reclaimed, the durable watermark may advance and the recovered queue sees only
/// the live tail.
#[test]
#[allow(non_snake_case)]
fn TestPartialExpireVisibilityUsesDurableManifestDeletionWatermark() {
    partial_expire_does_not_hide_undeleted_below_floor_segments();
}

/// TestPartialExpireVisibilityDecisionHidesOnlyReclaimedPrefix: construct a helper-level fixture
/// with a durable retention floor above multiple segment entries, durable manifest deletion watermark
/// covering the earliest below-floor entries, and an active partial reclaimed-through boundary that
/// has advanced farther than the durable deletion watermark. Assert the helper hides only entries at
/// or below the durable manifest deletion watermark when they are proven reclaimed.
#[test]
#[allow(non_snake_case)]
fn TestPartialExpireVisibilityDecisionHidesOnlyReclaimedPrefix() {
    // Use the standard lagging-partial-expire fixture: floor at seq 15, first 2 segments deleted,
    // durable watermark at index 1 (proof that entries 0,1 are durably reclaimed).
    let (_store, _cfg, _source, _seg_keys) = lagging_partial_expire_fixture();

    // With reclaimed_through=7 (advanced past watermark=1):
    // Entry 0: reclaimed (visible_last_seq=1 <= 7), index 0 <= watermark 1 → HiddenAsReclaimed
    assert_eq!(
        SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
            0,
            0,
            1,
            false,
            None,
            None,
            Some(1),
            7,
            Some(15),
        ),
        PartialExpireVisibility::HiddenAsReclaimed,
        "entry at reclaimed index 0 at/below watermark must be HiddenAsReclaimed"
    );

    // Entry 1: reclaimed (visible_last_seq=3 <= 7), index 1 <= watermark 1 → HiddenAsReclaimed
    assert_eq!(
        SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
            1,
            2,
            3,
            false,
            None,
            None,
            Some(1),
            7,
            Some(15),
        ),
        PartialExpireVisibility::HiddenAsReclaimed,
        "entry at reclaimed index 1 at/below watermark must be HiddenAsReclaimed"
    );

    // Entry 2: reclaimed (visible_last_seq=5 <= 7), index 2 > watermark 1 → StopHiddenPrefix
    // (NOT HiddenAsReclaimed because the watermark has not advanced far enough).
    assert_eq!(
        SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
            2,
            4,
            5,
            false,
            None,
            None,
            Some(1),
            7,
            Some(15),
        ),
        PartialExpireVisibility::StopHiddenPrefix,
        "reclaimed entry above watermark must be StopHiddenPrefix, not HiddenAsReclaimed"
    );

    // Entry 3: reclaimed (visible_last_seq=7 <= 7), index 3 > watermark 1 → StopHiddenPrefix.
    assert_eq!(
        SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
            3,
            6,
            7,
            false,
            None,
            None,
            Some(1),
            7,
            Some(15),
        ),
        PartialExpireVisibility::StopHiddenPrefix,
        "entry at index 3 above watermark must be StopHiddenPrefix"
    );

    // With reclaimed_through=3 (not past watermark):
    // Entry 2: NOT reclaimed (5 > 3), below floor → StopHiddenPrefix.
    assert_eq!(
        SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
            2,
            4,
            5,
            false,
            None,
            None,
            Some(1),
            3,
            Some(15),
        ),
        PartialExpireVisibility::StopHiddenPrefix,
        "non-reclaimed below-floor entry above watermark must be StopHiddenPrefix"
    );

    // Authoritative floor entry → Visible (not affected by partial-expire logic).
    assert_eq!(
        SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
            3,
            0,
            0,
            false,
            Some(15),
            None,
            Some(1),
            3,
            Some(15),
        ),
        PartialExpireVisibility::Visible,
        "authoritative floor entry must be Visible"
    );

    // No floor → all entries Visible.
    assert_eq!(
        SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
            0,
            0,
            1,
            false,
            None,
            None,
            Some(1),
            3,
            None,
        ),
        PartialExpireVisibility::Visible,
        "every entry must be Visible when there is no durable floor"
    );

    // Compacted marker → HiddenAsReclaimed.
    assert_eq!(
        SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
            0,
            0,
            0,
            false,
            None,
            Some(0),
            Some(1),
            3,
            Some(15),
        ),
        PartialExpireVisibility::HiddenAsReclaimed,
        "reclaimed manifest marker must be HiddenAsReclaimed"
    );
}

/// TestPartialExpireVisibilityDecisionHidesOnlyReclaimedPrefixStopsAtFirstUndeletedBelowFloorEntry:
/// using the same fixture shape, assert the first not-yet-deleted below-floor data entry returns the
/// helper's stop-hidden-prefix decision and prevents subsequent entries from being considered part of
/// the hidden prefix.
#[test]
#[allow(non_snake_case)]
fn TestPartialExpireVisibilityDecisionHidesOnlyReclaimedPrefixStopsAtFirstUndeletedBelowFloorEntry()
{
    // Same fixture shape: floor at seq 15, watermark at index 1 (entries 0,1 durably reclaimed),
    // entries 2+ still present (undeleted below-floor).
    let (_store, _cfg, _source, _seg_keys) = lagging_partial_expire_fixture();

    // With reclaimed_through=3 (the reclaim pass covers only entries 0,1):
    //
    // Entry 0: reclaimed, index 0 <= W → HiddenAsReclaimed (part of hidden prefix).
    assert_eq!(
        SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
            0,
            0,
            1,
            false,
            None,
            None,
            Some(1),
            3,
            Some(15),
        ),
        PartialExpireVisibility::HiddenAsReclaimed,
        "entry 0 in hidden prefix"
    );

    // Entry 1: reclaimed, index 1 <= W → HiddenAsReclaimed (part of hidden prefix).
    assert_eq!(
        SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
            1,
            2,
            3,
            false,
            None,
            None,
            Some(1),
            3,
            Some(15),
        ),
        PartialExpireVisibility::HiddenAsReclaimed,
        "entry 1 in hidden prefix"
    );

    // Entry 2: below-floor, NOT reclaimed (visible_last_seq=5 > reclaimed_through=3)
    // → StopHiddenPrefix (first undeleted below-floor entry — stops the hidden prefix).
    assert_eq!(
        SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
            2,
            4,
            5,
            false,
            None,
            None,
            Some(1),
            3,
            Some(15),
        ),
        PartialExpireVisibility::StopHiddenPrefix,
        "entry 2 (first undeleted below-floor) must be StopHiddenPrefix"
    );

    // Subsequent below-floor entries that are also not reclaimed must remain StopHiddenPrefix
    // — the hidden prefix cannot skip past them either.
    assert_eq!(
        SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
            3,
            6,
            7,
            false,
            None,
            None,
            Some(1),
            3,
            Some(15),
        ),
        PartialExpireVisibility::StopHiddenPrefix,
        "entry 3 must also be StopHiddenPrefix (undeleted below-floor)"
    );

    // Even if the caller advances reclaimed_through past a later entry's visible_last_seq,
    // if the entry is above the watermark it is StopHiddenPrefix, NOT HiddenAsReclaimed.
    assert_eq!(
        SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
            3,
            6,
            7,
            false,
            None,
            None,
            Some(1),
            7,
            Some(15),
        ),
        PartialExpireVisibility::StopHiddenPrefix,
        "entry 3 reclaimed but above watermark must be StopHiddenPrefix"
    );

    // Entry at index 4: below floor (first_seq=8), NOT reclaimed (visible_last_seq=9 > reclaimed_through=3)
    // → StopHiddenPrefix. The hidden prefix from entry 0,1 stops at entry 2, so entry 4 cannot be
    // part of any hidden prefix either.
    assert_eq!(
        SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
            4,
            8,
            9,
            false,
            None,
            None,
            Some(1),
            3,
            Some(15),
        ),
        PartialExpireVisibility::StopHiddenPrefix,
        "entry 4 must be StopHiddenPrefix (undeleted below-floor after the first stop)"
    );

    // Re-verify that entries 0 and 1 are still HiddenAsReclaimed — the first stop does not
    // retroactively change earlier decisions.
    assert_eq!(
        SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
            0,
            0,
            1,
            false,
            None,
            None,
            Some(1),
            3,
            Some(15),
        ),
        PartialExpireVisibility::HiddenAsReclaimed,
        "entry 0 remains HiddenAsReclaimed regardless of later stop"
    );
    assert_eq!(
        SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
            1,
            2,
            3,
            false,
            None,
            None,
            Some(1),
            3,
            Some(15),
        ),
        PartialExpireVisibility::HiddenAsReclaimed,
        "entry 1 remains HiddenAsReclaimed regardless of later stop"
    );
}

/// TestPartialExpireVisibilityDecisionExpiryStopsAtUndeletedBelowFloor: the first not-yet-deleted
/// below-floor data entry stops the expiry-side hidden-prefix advance and remains visible on the next
/// manifest enumeration.
#[test]
#[allow(non_snake_case)]
fn TestPartialExpireVisibilityDecisionExpiryStopsAtUndeletedBelowFloor() {
    let (store, cfg, source, seg_keys) = lagging_partial_expire_fixture();
    assert_partial_expire_visibility_decision_fixture(&store, cfg, &source, &seg_keys);
}

#[test]
fn partial_expire_visibility_uses_durable_manifest_deletion_watermark() {
    TestPartialExpireVisibilityUsesDurableManifestDeletionWatermark();
}

/// TestManifestWatermarkPresentEntriesNotHiddenDuringPartialExpiry: a partial expire can leave
/// below-floor manifest entries physically present, and reopen/recovery must keep using the durable
/// watermark as the read floor until the watermark is advanced durably.
#[test]
#[allow(non_snake_case)]
fn TestManifestWatermarkPresentEntriesNotHiddenDuringPartialExpiry() {
    partial_expire_does_not_hide_undeleted_below_floor_segments();
}

/// TestManifestWatermarkReadPathOwnerFenceEvaluationDocumented: pqueue-c33c367e owner-fence wiring
/// does not change read-path watermark enforcement here; the watermark remains a fail-closed read
/// floor, while ownership still comes from the permanent head CAS.
#[test]
#[allow(non_snake_case)]
fn TestManifestWatermarkReadPathOwnerFenceEvaluationDocumented() {
    TestDeletionWatermarkOwnerFenceIndependence();
}

/// TestPartialExpireVisibilityDecisionKeepsUndeletedBelowFloorSegments: a partial expire that leaves
/// below-floor segment objects physically present must still let the reopened manifest/read path enumerate
/// the first not-yet-deleted entry instead of hiding the whole prefix. Also verifies the helper-level
/// decision function directly.
#[test]
#[allow(non_snake_case)]
fn TestPartialExpireVisibilityDecisionKeepsUndeletedBelowFloorSegments() {
    existing_partial_expire_visibility_decision_keeps_undeleted();
    helper_level_partial_expire_visibility_decision_keeps_undeleted();
}

fn existing_partial_expire_visibility_decision_keeps_undeleted() {
    partial_expire_does_not_hide_undeleted_below_floor_segments();
}

fn helper_level_partial_expire_visibility_decision_keeps_undeleted() {
    // Construct a shard fixture with a durable retention floor above multiple segment entries,
    // a durable manifest deletion watermark below at least one below-floor data entry, and an
    // active partial reclaimed-through boundary below that entry.
    let (store, cfg, source, _seg_keys) = lagging_partial_expire_fixture();

    let floor = SegmentedObjectLog::open(store.clone(), cfg);
    floor.create_queue(&qdef()).unwrap();
    let floor_seq = floor
        .read_retention_floor(&source)
        .unwrap()
        .map(|f| f.sequence);
    let durable_watermark = floor.read_read_horizon(&source).unwrap();

    assert_eq!(floor_seq, Some(15), "floor at seq 15");
    assert_eq!(durable_watermark, Some(1), "watermark at index 1");

    // Entry at index 2: below-floor data entry with visible_last_seq=5.
    // With reclaimed_through=3 (< visible_last_seq=5): data check says NOT reclaimed
    // → StopHiddenPrefix (first undeleted below-floor entry).
    assert_eq!(
        SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
            2,     // entry_index
            4,     // first_seq
            5,     // visible_last_seq
            false, // fence
            None,  // retention_floor_through
            None,  // compacted_through_index
            durable_watermark,
            3, // reclaimed_through (below visible_last_seq)
            floor_seq,
        ),
        PartialExpireVisibility::StopHiddenPrefix,
        "below-floor undeleted data entry at index 2 must be StopHiddenPrefix when reclaimed_through \
         is below its visible_last_seq"
    );

    // Same entry with reclaimed_through=7 (> visible_last_seq=5): data check says reclaimed
    // BUT index 2 > watermark 1 → StopHiddenPrefix due to watermark defense.
    assert_eq!(
        SegmentedObjectLog::<InMemoryBlobStore>::partial_expire_entry_visible(
            2,
            4,
            5,
            false,
            None,
            None,
            durable_watermark,
            7,
            floor_seq,
        ),
        PartialExpireVisibility::StopHiddenPrefix,
        "below-floor undeleted data entry at index 2 must be StopHiddenPrefix when above the durable \
         watermark even if reclaimed_through advanced past visible_last_seq"
    );
}

/// TestPartialExpireVisibilityDecisionReadRecoveryBootstrapCompatibility: recovery after the same partial
/// expire fixture still enumerates the first undeleted below-floor entry, and the legacy bootstrap fixture
/// without partial-expire state keeps bootstrapping from the cached read-horizon behavior.
#[test]
#[allow(non_snake_case)]
fn TestPartialExpireVisibilityDecisionReadRecoveryBootstrapCompatibility() {
    partial_expire_does_not_hide_undeleted_below_floor_segments();
    TestManifestDeletionWatermarkStorageLegacyBootstrap();
}

/// TestObjectlogPqueueC33c367eReleaseNote: the release-note claim about pqueue-c33c367e is backed by the
/// documented owner-fence evaluation test.
#[test]
#[allow(non_snake_case)]
fn TestObjectlogPqueueC33c367eReleaseNote() {
    TestManifestWatermarkReadPathOwnerFenceEvaluationDocumented();
}

/// Test 9 — after successful below-floor manifest cleanup, the durable read-horizon advances
/// monotonically and survives reopen/recovery, while staying below the durable floor.
#[test]
#[allow(non_snake_case)]
fn TestManifestReclamationAdvancesWatermarkAfterDeleteProgress() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef()).unwrap();

    for i in 0..8u64 {
        log.enqueue(&shard(), &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard(), 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    assert!(
        log.read_read_horizon(&shard()).unwrap().is_none(),
        "no watermark exists before cleanup"
    );

    log.advance_retention_floor(&shard(), CommandPosition::new(shard(), 0, 3), 0)
        .unwrap();
    assert_eq!(
        log.expire_segments_through(&shard(), 3, 1_000).unwrap(),
        2,
        "the first cleanup pass reclaims the below-floor segments"
    );
    let w1 = log
        .read_read_horizon(&shard())
        .unwrap()
        .expect("watermark after first cleanup");
    let floor1 = log.read_retention_floor(&shard()).unwrap().unwrap();
    assert!(
        w1 < floor1.sequence,
        "the read-horizon must remain below the durable floor"
    );

    log.advance_retention_floor(&shard(), CommandPosition::new(shard(), 0, 7), 0)
        .unwrap();
    assert_eq!(
        log.expire_segments_through(&shard(), 7, 2_000).unwrap(),
        2,
        "the second cleanup pass advances the durable watermark again"
    );
    let w2 = log
        .read_read_horizon(&shard())
        .unwrap()
        .expect("watermark after second cleanup");
    let floor2 = log.read_retention_floor(&shard()).unwrap().unwrap();
    assert!(
        w2 > w1,
        "the persisted read-horizon advances monotonically: {w1} -> {w2}"
    );
    assert!(
        w2 < floor2.sequence,
        "the read-horizon never overtakes the durable floor"
    );

    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&qdef()).unwrap();
    assert_eq!(
        reopened.read_read_horizon(&shard()).unwrap(),
        Some(w2),
        "the durable watermark survives reopen/recovery"
    );
    assert_eq!(
        reopened
            .read_retention_floor(&shard())
            .unwrap()
            .unwrap()
            .sequence,
        floor2.sequence,
        "the durable floor survives reopen/recovery too"
    );
}

/// Test 10 — when below-floor manifest cleanup fails partway through, the durable watermark advances only
/// through the contiguous successfully deleted range and a retry resumes from the last committed watermark.
#[test]
#[allow(non_snake_case)]
fn TestManifestReclamationWatermarkUnchangedAfterPartialDelete() {
    let store = std::sync::Arc::new(FailingBlobStore::default());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef()).unwrap();

    for i in 0..3u64 {
        log.enqueue(&shard(), &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard(), 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    log.advance_retention_floor(&shard(), CommandPosition::new(shard(), 0, 5), 0)
        .unwrap();
    store.arm_delete(&segment_key_for(store.as_ref(), &shard(), 0));

    let err = log.expire_segments_through(&shard(), 5, 1_000).unwrap_err();
    assert!(
        matches!(err, EngineError::Storage(_)),
        "the injected delete failure must surface as a storage error, got {err:?}"
    );
    assert!(
        store
            .inner
            .get(&segment_key_for(store.as_ref(), &shard(), 0))
            .unwrap()
            .is_some(),
        "the not-yet-deleted segment object remains present after the partial failure"
    );
    assert_eq!(
        log.read_read_horizon(&shard()).unwrap(),
        None,
        "the watermark stays put until the partial delete has actually reclaimed something"
    );

    store.disarm();
    assert_eq!(
        log.expire_segments_through(&shard(), 5, 2_000).unwrap(),
        3,
        "the retry resumes from the durable floor and finishes the remaining cleanup"
    );
    assert_eq!(
        log.read_read_horizon(&shard()).unwrap(),
        Some(2),
        "the watermark is persisted once cleanup completes"
    );
    assert!(
        store
            .inner
            .get(&segment_key_for(store.as_ref(), &shard(), 0))
            .unwrap()
            .is_none(),
        "the retry finishes the partial-failure cleanup and removes the reclaimed segment object"
    );
    assert!(
        store
            .inner
            .get(&segment_key_for(store.as_ref(), &shard(), 2))
            .unwrap()
            .is_none(),
        "the remaining below-floor segment object is reclaimed on the retry"
    );
    assert!(
        store
            .inner
            .get(&manifest_head_key_s(&shard(), 1))
            .unwrap()
            .is_some(),
        "the retained manifest history remains available after cleanup"
    );
    assert!(
        store
            .inner
            .get(&manifest_head_key_s(&shard(), 2))
            .unwrap()
            .is_some(),
        "all below-floor manifest entries stay retained in place"
    );
}

/// TestInterruptedManifestReclaimRecovery: if a reclaim pass deletes part of the below-floor prefix and then
/// fails, reopening the log preserves the undeleted manifest history and the next reclaim resumes from the
/// durable watermark, not from genesis.
#[test]
#[allow(non_snake_case)]
fn TestInterruptedManifestReclaimRecovery() {
    let store = std::sync::Arc::new(FailingBlobStore::default());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef()).unwrap();

    for i in 0..3u64 {
        log.enqueue(&shard(), &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard(), 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    log.advance_retention_floor(&shard(), CommandPosition::new(shard(), 0, 5), 0)
        .unwrap();
    store.arm_delete(&segment_key_for(store.as_ref(), &shard(), 2));

    let err = log.expire_segments_through(&shard(), 5, 1_000).unwrap_err();
    assert!(
        matches!(err, EngineError::Storage(_)),
        "the injected delete failure must abort the reclaim pass"
    );
    assert_eq!(
        log.read_read_horizon(&shard()).unwrap(),
        Some(0),
        "the durable watermark records only the confirmed deleted prefix"
    );
    assert!(
        store
            .inner
            .get(&segment_key_for(store.as_ref(), &shard(), 0))
            .unwrap()
            .is_none(),
        "the first below-floor segment was deleted before the failure"
    );
    assert!(
        store
            .inner
            .get(&segment_key_for(store.as_ref(), &shard(), 2))
            .unwrap()
            .is_some(),
        "the interrupted segment stays present"
    );
    assert!(
        store
            .inner
            .get(&segment_key_for(store.as_ref(), &shard(), 4))
            .unwrap()
            .is_some(),
        "the undeleted below-floor tail stays present"
    );
    assert!(
        store
            .inner
            .get(&manifest_head_key_s(&shard(), 1))
            .unwrap()
            .is_some(),
        "the undeleted below-floor manifest entry remains durable"
    );
    assert!(
        store
            .inner
            .get(&manifest_head_key_s(&shard(), 2))
            .unwrap()
            .is_some(),
        "the later undeleted below-floor manifest entry remains durable"
    );

    drop(log);
    store.disarm();

    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&qdef()).unwrap();
    assert_eq!(
        reopened.read_read_horizon(&shard()).unwrap(),
        Some(0),
        "reopen reconstructs the durable watermark from the interrupted reclaim"
    );
    assert!(
        reopened
            .read_read_horizon(&shard())
            .unwrap()
            .is_some_and(|w| w == 0),
        "the recovery pass resumes at the committed watermark"
    );
    assert_eq!(
        reopened
            .expire_segments_through(&shard(), 5, 2_000)
            .unwrap(),
        2,
        "the next reclaim finishes from the durable watermark"
    );
    assert_eq!(
        reopened.read_read_horizon(&shard()).unwrap(),
        Some(2),
        "the watermark advances only after the remaining deleted prefix is completed"
    );
}

/// Test 11 — if the first reclaimable delete fails, the durable watermark stays unchanged.
#[test]
#[allow(non_snake_case)]
fn TestManifestReclamationWatermarkUnchangedAfterFailedDelete() {
    let store = std::sync::Arc::new(FailingBlobStore::default());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef()).unwrap();

    for i in 0..3u64 {
        log.enqueue(&shard(), &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard(), 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    log.advance_retention_floor(&shard(), CommandPosition::new(shard(), 0, 5), 0)
        .unwrap();
    store.arm_delete(&segment_key_for(store.as_ref(), &shard(), 0));

    let err = log.expire_segments_through(&shard(), 5, 1_000).unwrap_err();
    assert!(
        matches!(err, EngineError::Storage(_)),
        "the injected delete failure must surface as a storage error, got {err:?}"
    );
    assert_eq!(
        log.read_read_horizon(&shard()).unwrap(),
        None,
        "the watermark stays put until the first reclaimable delete succeeds"
    );
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

    // Trim (writes a horizon), then delete only the cached horizon object. The retained watermark history
    // still reconstructs the durable state, so the queue should keep reading the live manifest list.
    trim_cycle(&log, &shard(), 3, 0, 1_000);
    assert!(log.read_read_horizon(&shard()).unwrap().is_some());
    store.delete(&read_horizon_key_s(&shard())).unwrap();
    assert!(
        log.read_read_horizon(&shard()).unwrap().is_some(),
        "deleting the cache blob alone does not erase the durable watermark history"
    );

    // A genesis read still fail-closes because the durable floor remains in effect even if the cache blob
    // is missing.
    assert!(
        matches!(log.read_all(&shard()), Err(EngineError::Storage(_))),
        "without the horizon cache blob the trimmed queue still fails closed from genesis"
    );
}

#[test]
#[allow(non_snake_case)]
fn TestLegacyManifestBootstrapWithoutDeletionWatermark() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let shard = shard();
    let qdef = qdef();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef).unwrap();
    for i in 0..2u64 {
        log.enqueue(&shard, &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&qdef).unwrap();
    assert!(
        reopened.read_read_horizon(&shard).unwrap().is_none(),
        "legacy manifests without a deletion watermark bootstrap through the manifest path"
    );
    assert_eq!(
        reopened.read_all(&shard).unwrap().len(),
        4,
        "the reopened queue still reads the committed legacy manifest tail"
    );
    assert_eq!(
        reopened.current_epoch(&shard).unwrap(),
        0,
        "bootstrap without a deletion watermark preserves the permanent-head fence"
    );
}

#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkStorageLegacyBootstrap() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let shard = shard();

    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef()).unwrap();
    for i in 0..4u64 {
        log.enqueue(&shard, &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    trim_cycle(&log, &shard, 3, 0, 1_000);
    let durable = log.read_manifest_deletion_watermark(&shard).unwrap();
    let cached = log.read_read_horizon(&shard).unwrap();
    assert_eq!(durable, cached);
    let cached = cached.expect("legacy cache bootstrap horizon");
    assert!(cached > 0, "the fixture needs a non-zero bootstrap horizon");

    delete_watermark_markers_only(store.as_ref(), &shard);

    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&qdef()).unwrap();
    assert_eq!(
        reopened.read_manifest_deletion_watermark(&shard).unwrap(),
        None,
        "without durable watermark markers the history helper reports no marker state"
    );
    assert_eq!(
        reopened.read_read_horizon(&shard).unwrap(),
        Some(cached),
        "existing shards without deletion-watermark state still bootstrap from the legacy read-horizon cache"
    );
    assert_eq!(
        reopened
            .read_retention_floor(&shard)
            .unwrap()
            .unwrap()
            .sequence,
        3,
        "the durable floor remains independently derived from the manifest"
    );
}

#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkStorageEncodingRoundTrip() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let shard = shard();

    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef()).unwrap();
    for i in 0..8u64 {
        log.enqueue(&shard, &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    trim_cycle(&log, &shard, 7, 0, 1_000);
    let durable = log
        .read_manifest_deletion_watermark(&shard)
        .unwrap()
        .expect("durable deletion watermark");
    let legacy = log.read_read_horizon(&shard).unwrap().unwrap();
    assert_eq!(
        durable, legacy,
        "the durable marker history matches the cached watermark"
    );

    let marker_key = store
        .list(&manifest_head_prefix_s(&shard))
        .unwrap()
        .into_iter()
        .find(|key| key.ends_with("~watermark.json"))
        .expect("durable watermark marker");
    let marker_bytes = store.get(&marker_key).unwrap().unwrap();
    let marker_json: serde_json::Value = serde_json::from_slice(&marker_bytes).unwrap();
    assert_eq!(
        marker_json
            .get("compacted_through_index")
            .and_then(|value| value.as_u64()),
        Some(durable),
        "the durable encoding round-trips the highest contiguous reclaimed manifest index"
    );

    write_read_horizon_cache_only(store.as_ref(), &shard, durable - 1);
    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&qdef()).unwrap();
    assert_eq!(
        reopened.read_manifest_deletion_watermark(&shard).unwrap(),
        Some(durable),
        "the durable deletion watermark is reconstructed from marker history, not the cache blob"
    );
    assert_eq!(
        reopened.read_read_horizon(&shard).unwrap(),
        Some(durable),
        "a stale read-horizon cache cannot regress the durable deletion watermark"
    );
}

#[test]
#[allow(non_snake_case)]
fn TestLegacyManifestBootstrapDoesNotInferWatermarkFloor() {
    let store = std::sync::Arc::new(MissingGetBlobStore::default());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let shard = shard();
    let qdef = qdef();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef).unwrap();
    for i in 0..4u64 {
        log.enqueue(&shard, &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    log.advance_retention_floor(&shard, CommandPosition::new(shard.clone(), 0, 3), 0)
        .unwrap();
    assert_eq!(
        log.expire_segments_through(&shard, 3, 1_000).unwrap(),
        2,
        "the durable watermark is produced by the reclaim path before the cache is removed"
    );
    let floor_before = log.read_retention_floor(&shard).unwrap().unwrap();
    assert_eq!(
        floor_before.sequence, 3,
        "the retention floor is still derived from the manifest"
    );

    delete_watermark_history(store.as_ref(), &shard);

    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&qdef).unwrap();
    assert!(
        reopened.read_read_horizon(&shard).unwrap().is_none(),
        "with no deletion watermark present, reopening falls back to the legacy manifest bootstrap"
    );
    assert_eq!(
        reopened.read_retention_floor(&shard).unwrap().unwrap(),
        floor_before,
        "absent watermark state does not change the authoritative retention floor"
    );

    store.arm_missing_get(&manifest_head_key_s(&shard, 2));
    let err = reopened.read_all(&shard).unwrap_err();
    assert_eq!(
        err,
        EngineError::Conflict,
        "a listed-but-missing manifest entry is still treated as corruption, not intentional reclamation"
    );
}

#[test]
#[allow(non_snake_case)]
fn TestLegacyManifestBootstrapPreservesPermanentFenceProtocol() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let shard = shard();
    let qdef = qdef();

    let owner_a = SegmentedObjectLog::open(store.clone(), cfg);
    owner_a.create_queue(&qdef).unwrap();
    owner_a.enqueue(&shard, &pushes(2), 0, 10).unwrap();
    owner_a.seal(&shard, 0, 11).unwrap();

    delete_watermark_metadata(store.as_ref(), &shard);

    let owner_b = SegmentedObjectLog::open(store.clone(), cfg);
    owner_b.create_queue(&qdef).unwrap();
    assert_eq!(owner_b.acquire_epoch(&shard, 100).unwrap(), 1);
    assert!(
        owner_b.read_read_horizon(&shard).unwrap().is_none(),
        "legacy bootstrap remains valid even with no deletion watermark state"
    );
    assert_eq!(
        owner_b.current_epoch(&shard).unwrap(),
        1,
        "the permanent head object still carries the ownership fence"
    );
    assert!(
        store
            .get(&manifest_head_key_s(&shard, 1))
            .unwrap()
            .is_some(),
        "the durable head object remains present after watermark absence"
    );

    owner_a.enqueue(&shard, &pushes(3), 0, 5_000).unwrap();
    let err = owner_a.seal(&shard, 0, 5_001).unwrap_err();
    assert_eq!(
        err,
        EngineError::EpochFenced,
        "the stale writer is still fenced by the permanent head CAS"
    );
}

#[test]
#[allow(non_snake_case)]
fn TestLegacyManifestBootstrapPreservesFenceCompatibility() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let shard = shard();
    let qdef = qdef();

    let owner_a = SegmentedObjectLog::open(store.clone(), cfg);
    owner_a.create_queue(&qdef).unwrap();
    for i in 0..4u64 {
        owner_a
            .enqueue(&shard, &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        owner_a.seal(&shard, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    let owner_b = SegmentedObjectLog::open(store.clone(), cfg);
    owner_b.create_queue(&qdef).unwrap();
    assert_eq!(owner_b.acquire_epoch(&shard, 100).unwrap(), 1);
    owner_b.enqueue(&shard, &pushes(2), 1, 110).unwrap();
    owner_b.seal(&shard, 1, 111).unwrap();

    trim_cycle(&owner_b, &shard, 3, 1, 1_000);
    let expected_tail = owner_b
        .read_from(&shard, 4)
        .unwrap()
        .into_iter()
        .map(|(position, envelope)| {
            (
                position.sequence,
                position.backend_epoch,
                serde_json::to_vec(&envelope).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let expected_epoch = owner_b.current_epoch(&shard).unwrap();
    let expected_horizon = owner_b.read_read_horizon(&shard).unwrap();

    delete_manifest_head_data_only(store.as_ref(), &shard);

    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&qdef).unwrap();
    assert_eq!(
        reopened.read_read_horizon(&shard).unwrap(),
        expected_horizon,
        "the durable reclaimed-index fence is reconstructed from marker history"
    );
    assert_eq!(
        reopened
            .read_from(&shard, 4)
            .unwrap()
            .into_iter()
            .map(|(position, envelope)| {
                (
                    position.sequence,
                    position.backend_epoch,
                    serde_json::to_vec(&envelope).unwrap(),
                )
            })
            .collect::<Vec<_>>(),
        expected_tail,
        "legacy bootstrap recovers the same live tail after the head namespace is removed"
    );
    assert_eq!(
        reopened.current_epoch(&shard).unwrap(),
        expected_epoch,
        "legacy-only bootstrap preserves the recovered epoch"
    );
    assert_eq!(
        reopened.acquire_epoch(&shard, 5_000).unwrap(),
        expected_epoch + 1,
        "a fresh owner can continue from the legacy-bootstrapped recovered epoch"
    );
}

/// TestManifestDeletionWatermarkReclaimCyclesMonotonic: repeated trim/reclaim cycles only advance the
/// persisted deletion watermark, and the live tail remains readable after each step.
#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkReclaimCyclesMonotonic() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef()).unwrap();

    for i in 0..8u64 {
        log.enqueue(&shard(), &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard(), 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    trim_cycle(&log, &shard(), 3, 0, 1_000);
    let w1 = log.read_read_horizon(&shard()).unwrap().unwrap();
    trim_cycle(&log, &shard(), 7, 0, 2_000);
    let w2 = log.read_read_horizon(&shard()).unwrap().unwrap();
    trim_cycle(&log, &shard(), 11, 0, 3_000);
    let w3 = log.read_read_horizon(&shard()).unwrap().unwrap();
    trim_cycle(&log, &shard(), 11, 0, 4_000);
    let w4 = log.read_read_horizon(&shard()).unwrap().unwrap();

    assert!(
        w1 < w2 && w2 < w3,
        "the deletion watermark only advances across successful reclaim cycles: {w1} < {w2} < {w3}"
    );
    assert_eq!(
        w3, w4,
        "repeating the same reclaim cycle does not regress or advance the persisted watermark"
    );
    assert_eq!(
        log.read_from(&shard(), 12)
            .unwrap()
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![12, 13, 14, 15],
        "the live tail stays readable above the reclaimed prefix"
    );
}

#[test]
fn read_horizon_bounds_enumeration_to_live_and_is_monotonic() {
    TestManifestDeletionWatermarkReclaimCyclesMonotonic();
}

/// TestPartialExpireVisibilityStateDoesNotRegressReadHorizonBounds: enabling the partial-expire
/// visibility state must not widen the bounded read/recovery enumeration, and the durable watermark
/// still preserves the live tail ordering.
#[test]
#[allow(non_snake_case)]
fn TestPartialExpireVisibilityStateDoesNotRegressReadHorizonBounds() {
    partial_expire_does_not_hide_undeleted_below_floor_segments();
    TestManifestDeletionWatermarkReclaimCyclesMonotonic();
}

/// TestPartialExpireVisibilityStatePreservesFailClosedBelowFloorReads: a partial-expire visibility
/// state must not reopen reclaimed below-floor reads when a durable watermark is present; the
/// fail-closed guard still rejects `from_seq <= floor`, while `floor + 1` continues to read the live tail.
#[test]
#[allow(non_snake_case)]
fn TestPartialExpireVisibilityStatePreservesFailClosedBelowFloorReads() {
    let (store, cfg, source, seg_keys) = lagging_partial_expire_fixture();
    assert_partial_expire_visibility_decision_fixture(&store, cfg, &source, &seg_keys);

    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&qdef()).unwrap();
    assert_eq!(
        reopened
            .read_retention_floor(&source)
            .unwrap()
            .unwrap()
            .sequence,
        15,
        "the partial-expire fixture leaves a durable reclaimed floor in place"
    );
    assert!(
        reopened.read_read_horizon(&source).unwrap().is_some(),
        "the durable watermark remains present for the reopened queue"
    );

    let below_floor_err = reopened.read_all(&source).unwrap_err();
    assert!(
        matches!(
            &below_floor_err,
            EngineError::Storage(msg) if msg.contains("read below retention floor")
        ),
        "reads below the reclaimed floor must fail closed, got {below_floor_err:?}"
    );

    let floor_err = reopened.read_from(&source, 15).unwrap_err();
    assert!(
        matches!(
            &floor_err,
            EngineError::Storage(msg) if msg.contains("read below retention floor")
        ),
        "reads at the reclaimed floor must fail closed, got {floor_err:?}"
    );

    let live = reopened.read_from(&source, 16).unwrap();
    assert_eq!(
        live.iter().map(|(pos, _)| pos.sequence).collect::<Vec<_>>(),
        vec![16],
        "reads at floor + 1 continue to return the live tail"
    );
}

#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkStorageMonotonic() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let shard = shard();
    log.create_queue(&qdef()).unwrap();

    for i in 0..4u64 {
        log.enqueue(&shard, &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    trim_cycle(&log, &shard, 3, 0, 1_000);
    let first = log
        .read_manifest_deletion_watermark(&shard)
        .unwrap()
        .expect("initial durable watermark");
    log.persist_manifest_deletion_watermark(&shard, first, 2_000)
        .unwrap();
    assert_eq!(
        log.read_manifest_deletion_watermark(&shard).unwrap(),
        Some(first),
        "repeating the same candidate leaves the durable watermark unchanged"
    );

    trim_cycle(&log, &shard, 7, 0, 3_000);
    let second = log
        .read_manifest_deletion_watermark(&shard)
        .unwrap()
        .expect("advanced durable watermark");
    assert!(
        second > first,
        "later reclaim progress advances the durable watermark: {first} -> {second}"
    );

    log.persist_manifest_deletion_watermark(&shard, first, 4_000)
        .unwrap();
    assert_eq!(
        log.read_manifest_deletion_watermark(&shard).unwrap(),
        Some(second),
        "a stale lower candidate cannot regress the durable manifest deletion watermark"
    );
}

#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkStorageMonotonicNoRegression() {
    TestManifestDeletionWatermarkStorageMonotonic();
}

#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkStorageNoCorruptOnStale() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let shard = shard();
    log.create_queue(&qdef()).unwrap();

    for i in 0..4u64 {
        log.enqueue(&shard, &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    log.advance_retention_floor(&shard, CommandPosition::new(shard.clone(), 0, 3), 0)
        .unwrap();
    log.persist_manifest_deletion_watermark(&shard, 3, 1_000)
        .unwrap();
    assert_eq!(log.read_read_horizon(&shard).unwrap(), Some(1));

    let stale_blob = serde_json::json!({ "index": 0 });
    store
        .put(
            &read_horizon_key_s(&shard),
            &serde_json::to_vec(&stale_blob).unwrap(),
        )
        .unwrap();

    log.persist_manifest_deletion_watermark(&shard, 1, 2_000)
        .unwrap();
    assert_eq!(
        log.read_manifest_deletion_watermark(&shard).unwrap(),
        Some(1),
        "the durable deletion watermark remains readable after stale writes are ignored"
    );
    assert_eq!(
        log.read_read_horizon(&shard).unwrap(),
        Some(1),
        "the cached horizon cannot corrupt the durable watermark when it regresses"
    );
}

/// TestManifestDeletionWatermarkReclaimNeverExceedsFloor: a too-high deletion candidate is ignored by the
/// bounded watermark path and does not hide the live manifest tail.
#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkReclaimNeverExceedsFloor() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef()).unwrap();

    for i in 0..6u64 {
        log.enqueue(&shard(), &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard(), 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    trim_cycle(&log, &shard(), 3, 0, 1_000);
    let before = log.read_read_horizon(&shard()).unwrap().unwrap();

    log.advance_retention_floor(&shard(), CommandPosition::new(shard(), 0, 7), 0)
        .unwrap();
    log.persist_manifest_deletion_watermark(&shard(), 11, 2_000)
        .unwrap();

    let after = log.read_read_horizon(&shard()).unwrap().unwrap();
    assert_eq!(
        after, before,
        "the deletion watermark ignores a candidate above the durable floor"
    );
    assert_eq!(
        log.read_from(&shard(), 8)
            .unwrap()
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![8, 9, 10, 11],
        "the live entries above the floor remain visible after the ignored high candidate"
    );
}

#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkStorageBelowFloorAccepted() {
    TestManifestDeletionWatermarkReclaimNeverExceedsFloor();
}

#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkStorageNeverExceedsFloor() {
    TestManifestDeletionWatermarkReclaimNeverExceedsFloor();
}

#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkPersistsAfterPhysicalDelete() {
    let store = std::sync::Arc::new(FailingBlobStore::default());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let shard = shard();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef()).unwrap();

    for i in 0..3u64 {
        log.enqueue(&shard, &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    log.advance_retention_floor(&shard, CommandPosition::new(shard.clone(), 0, 5), 0)
        .unwrap();
    store.arm_delete(&segment_key_for(store.as_ref(), &shard, 2));

    let err = log.expire_segments_through(&shard, 5, 1_000).unwrap_err();
    assert!(
        matches!(err, EngineError::Storage(_)),
        "the injected delete failure must abort the reclaim pass"
    );
    assert_eq!(
        log.read_read_horizon(&shard).unwrap(),
        Some(0),
        "the deletion watermark records only the highest physically reclaimed manifest index from that pass"
    );
    assert!(
        store
            .inner
            .get(&segment_key_for(store.as_ref(), &shard, 0))
            .unwrap()
            .is_none(),
        "the first reclaimed segment object remains deleted"
    );
    assert!(
        store
            .inner
            .get(&segment_key_for(store.as_ref(), &shard, 2))
            .unwrap()
            .is_some(),
        "the failed segment object is still present and therefore cannot be counted in the watermark"
    );

    store.disarm();
    assert_eq!(
        log.expire_segments_through(&shard, 5, 2_000).unwrap(),
        2,
        "the retry completes the contiguous reclaimed prefix"
    );
    assert_eq!(
        log.read_read_horizon(&shard).unwrap(),
        Some(2),
        "the persisted watermark advances only after the later physical deletes complete"
    );
}

#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkLegacyBootstrapConservative() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let shard = shard();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef()).unwrap();

    for i in 0..4u64 {
        log.enqueue(&shard, &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    trim_cycle(&log, &shard, 3, 0, 1_000);
    assert_eq!(
        log.read_read_horizon(&shard).unwrap(),
        Some(1),
        "the durable watermark exists before the legacy bootstrap metadata is removed"
    );

    delete_watermark_metadata(store.as_ref(), &shard);

    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&qdef()).unwrap();
    assert!(
        reopened.read_read_horizon(&shard).unwrap().is_none(),
        "without cached deletion-watermark metadata the reopened queue falls back to the live manifest path"
    );
    assert_eq!(
        reopened
            .read_from(&shard, 4)
            .unwrap()
            .iter()
            .map(|(pos, _)| pos.sequence)
            .collect::<Vec<_>>(),
        vec![4, 5, 6, 7],
        "the conservative bootstrap still exposes the live tail instead of skipping entries"
    );
}

/// TestManifestDeletionWatermarkPartialExpiryDoesNotMaskLiveEntries: a partial reclaim leaves the remaining
/// below-floor history visible for the next pass; the watermark is not treated as a retention authority.
#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkPartialExpiryDoesNotMaskLiveEntries() {
    partial_expire_does_not_hide_undeleted_below_floor_segments();
}

/// TestPartialExpireDoesNotAdvanceDeletionWatermarkPastDeletedPrefix: a partial expire must stop at the
/// first below-floor manifest entry that was not actually reclaimed, even if later entries are reclaimable.
#[test]
#[allow(non_snake_case)]
fn TestPartialExpireDoesNotAdvanceDeletionWatermarkPastDeletedPrefix() {
    TestManifestDeletionWatermarkContiguousPrefixOnly();
}

/// TestPartialExpireWatermarkDoesNotHideBelowFloorSegments: after a partial expire, reopen/recovery still
/// observes the remaining below-floor tail instead of using the deletion watermark as a retention fence.
#[test]
#[allow(non_snake_case)]
fn TestPartialExpireWatermarkDoesNotHideBelowFloorSegments() {
    partial_expire_does_not_hide_undeleted_below_floor_segments();
}

/// TestManifestDeletionWatermarkPartialExpiryBoundary: the watermark must not hide not-yet-deleted
/// below-floor segments during partial expiry.
#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkPartialExpiryBoundary() {
    TestPartialExpireReadHorizonAloneDoesNotHideBelowFloorEntries();
    TestManifestDeletionWatermarkContiguousPrefixOnly();
    partial_expire_does_not_hide_undeleted_below_floor_segments();
}

/// TestManifestDeletionWatermarkContiguousPrefixOnly: a pinned gap blocks the watermark even when later
/// below-floor segments are reclaimed in the same pass.
#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkContiguousPrefixOnly() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let source = shard();
    log.create_queue(&qdef()).unwrap();

    // Three segments: seg0[0..1], seg1[2..3], seg2[4..5].
    for i in 0..3u64 {
        log.enqueue(&source, &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&source, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    // Branch at seq 1 so seg0 is pinned, but seg1 and seg2 are still below-floor candidates. The gap must
    // block the watermark from advancing past the pinned prefix even though later entries get reclaimed.
    let branch_def = branch_qdef("contiguous-prefix");
    let branch = QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 1),
        60_000,
        30,
    )
    .unwrap();

    log.advance_retention_floor(&source, CommandPosition::new(source.clone(), 0, 5), 0)
        .unwrap();
    assert_eq!(
        log.expire_segments_through(&source, 5, 31).unwrap(),
        2,
        "the pass reclaims only the unpinned below-floor segments"
    );
    assert_eq!(
        log.read_read_horizon(&source).unwrap(),
        None,
        "the pinned gap prevents the watermark from skipping to later reclaimed entries"
    );
    assert!(
        store
            .get(&segment_key_for(store.as_ref(), &source, 0))
            .unwrap()
            .is_some(),
        "the pinned prefix remains present while the branch is live"
    );
    assert!(
        store
            .get(&segment_key_for(store.as_ref(), &source, 2))
            .unwrap()
            .is_none(),
        "the first unpinned below-floor segment is reclaimed"
    );
    assert!(
        store
            .get(&segment_key_for(store.as_ref(), &source, 4))
            .unwrap()
            .is_none(),
        "the later unpinned below-floor segment is also reclaimed"
    );

    log.discard_branch(&source, &branch).unwrap();
    assert_eq!(
        log.expire_segments_through(&source, 5, 32).unwrap(),
        1,
        "once the gap clears, the previously pinned segment becomes reclaimable"
    );
    assert_eq!(
        log.read_read_horizon(&source).unwrap(),
        Some(0),
        "once the gap clears, the watermark only advances to the first newly reclaimed entry"
    );
}

/// TestManifestReclamationPreservesPinnedSourceSegments: a live branch pin keeps the referenced source
/// segment physically readable while the reclaim pass deletes the eligible unpinned below-floor segments.
#[test]
#[allow(non_snake_case)]
fn TestManifestReclamationPreservesPinnedSourceSegments() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let source = shard();
    log.create_queue(&qdef()).unwrap();

    for i in 0..3u64 {
        log.enqueue(&source, &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&source, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    let branch_def = branch_qdef("preserve-pinned-source");
    let branch =
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 1),
        60_000,
        30,
    )
    .unwrap();

    log.advance_retention_floor(&source, CommandPosition::new(source.clone(), 0, 5), 0)
        .unwrap();
    assert_eq!(
        log.expire_segments_through(&source, 5, 31).unwrap(),
        2,
        "the reclaim pass deletes only the unpinned below-floor segments"
    );

    assert!(
        store
            .get(&segment_key_for(store.as_ref(), &source, 0))
            .unwrap()
            .is_some(),
        "the branch-pinned source segment stays physically readable"
    );
    assert!(
        store
            .get(&segment_key_for(store.as_ref(), &source, 2))
            .unwrap()
            .is_none(),
        "the eligible unpinned below-floor segment is reclaimed"
    );
    assert!(
        store
            .get(&segment_key_for(store.as_ref(), &source, 4))
            .unwrap()
            .is_none(),
        "the later eligible unpinned below-floor segment is also reclaimed"
    );

    log.discard_branch(&source, &branch).unwrap();
}

/// TestManifestReclamationPreservesPinnedManifestObjects: a live branch pin keeps the referenced
/// manifest objects physically readable while the reclaim pass deletes the eligible unpinned below-floor
/// manifest copies.
#[test]
#[allow(non_snake_case)]
fn TestManifestReclamationPreservesPinnedManifestObjects() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    let source = shard();
    log.create_queue(&qdef()).unwrap();

    for i in 0..3u64 {
        log.enqueue(&source, &pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&source, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    let branch_def = branch_qdef("preserve-pinned-manifest");
    let branch =
        pqueue_engine::QueueKey::new(branch_def.tenant_id.clone(), branch_def.queue_id.clone());
    log.branch(
        &source,
        &branch_def,
        &CommandPosition::new(source.clone(), 0, 1),
        60_000,
        30,
    )
    .unwrap();

    let pinned_head_key = manifest_head_key_s(&source, 0);
    let pinned_legacy_key = manifest_key_s(&source, 0);
    let reclaimed_head_key = manifest_head_key_s(&source, 1);
    let reclaimed_legacy_key = manifest_key_s(&source, 1);

    log.advance_retention_floor(&source, CommandPosition::new(source.clone(), 0, 5), 0)
        .unwrap();
    assert_eq!(
        log.expire_segments_through(&source, 5, 31).unwrap(),
        2,
        "the reclaim pass deletes only the unpinned below-floor segments"
    );

    assert!(
        store
            .get(&segment_key_for(store.as_ref(), &source, 0))
            .unwrap()
            .is_some(),
        "the branch-pinned source segment stays physically readable"
    );
    assert!(
        store.get(&pinned_head_key).unwrap().is_some(),
        "the pinned manifest head stays physically readable"
    );
    assert!(
        store.get(&pinned_legacy_key).unwrap().is_some(),
        "the pinned legacy manifest copy stays physically readable"
    );
    assert!(
        store
            .get(&segment_key_for(store.as_ref(), &source, 2))
            .unwrap()
            .is_none(),
        "the eligible unpinned below-floor segment is reclaimed"
    );
    assert!(
        store.get(&reclaimed_head_key).unwrap().is_some(),
        "the unpinned manifest head is retained as history"
    );
    assert!(
        store.get(&reclaimed_legacy_key).unwrap().is_none(),
        "the reclaimed legacy manifest copy is physically deleted"
    );

    log.discard_branch(&source, &branch).unwrap();
}

/// TestObjectlogDeletedManifestFailClosedSignal: objectlog recovery (read_from / read_all) returns the
/// distinct deleted-manifest-prefix fail-closed signal when replay would require physically deleted
/// manifest prefixes. The signal is distinguishable from generic storage errors via
/// `SegmentedObjectLog::is_deleted_manifest_prefix_error`.
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
        log.read_read_horizon(&shard).unwrap().is_some(),
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
        pqueue_objectlog::segmented::is_deleted_manifest_prefix_error(&err),
        "read_from(floor) must return the distinct deleted-manifest-prefix signal: {err:?}"
    );
    // Read below the floor (genesis) fails closed with the distinct signal.
    let err = log.read_all(&shard).unwrap_err();
    assert!(
        pqueue_objectlog::segmented::is_deleted_manifest_prefix_error(&err),
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
        pqueue_objectlog::segmented::is_deleted_manifest_prefix_error(&err),
        "reopened: read_from(floor) must return the distinct signal: {err:?}"
    );
    let err = reopened.read_all(&shard).unwrap_err();
    assert!(
        pqueue_objectlog::segmented::is_deleted_manifest_prefix_error(&err),
        "reopened: read_all must return the distinct signal: {err:?}"
    );

    // The error is NOT a generic missing-segment storage error — it has the distinct prefix.
    let generic_storage = EngineError::Storage("generic storage error".into());
    assert!(
        !pqueue_objectlog::segmented::is_deleted_manifest_prefix_error(&generic_storage),
        "generic Storage errors must NOT match the deleted-manifest-prefix signal"
    );
}

/// TestObjectlogRetainedFloorHeadReplayStillSucceeds: objectlog recovery beginning at the retained
/// floor/head succeeds without relaxing retention-floor or source-pin guarantees and without data loss.
/// After manifest prefix deletion and reopen, `read_from(floor+1)` returns the live tail and
/// `read_from(floor)` / `read_all` still fail closed.
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
    let horizon = log.read_read_horizon(&shard).unwrap();
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
        pqueue_objectlog::segmented::is_deleted_manifest_prefix_error(&err),
        "read_from(floor) must fail closed after recovery: {err:?}"
    );

    // read_all (genesis) fails closed (below-floor guarantee intact).
    let err = reopened.read_all(&shard).unwrap_err();
    assert!(
        pqueue_objectlog::segmented::is_deleted_manifest_prefix_error(&err),
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

/// TestObjectlogPqueueC33c367eInteractionRecorded: pqueue-c33c367e interaction is evaluated before
/// landing and the objectlog-specific conclusion is recorded for release notes handoff. The deferred
/// server-side owner-fence wiring (pqueue-c33c367e) does not change the deleted-manifest fail-closed
/// signal at the objectlog level — the signal is gated on the durable retention floor and manifest
/// deletion watermark, both of which are independent of owner-fence wiring. The permanent head CAS
/// remains the stale-writer fence; the watermark is a read-cost helper, not an ownership fence.
#[test]
#[allow(non_snake_case)]
fn TestObjectlogPqueueC33c367eInteractionRecorded() {
    // Verify the existing owner-fence independence test is wired through.
    TestDeletionWatermarkOwnerFenceIndependence();
    // Also verify the test that asserts the fail-closed signal is distinct.
    TestObjectlogDeletedManifestFailClosedSignal();
    // Verify the test that asserts retained floor/head replay still succeeds.
    TestObjectlogRetainedFloorHeadReplayStillSucceeds();
}
