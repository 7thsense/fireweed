//! TP-002 **E3 (live/S3-compatible object-log projection matrix)** release-tier evidence harness.
//!
//! This is the live counterpart to the in-process segment-counter smoke row in
//! `fireweed-objectlog/tests/segmented_s3_substrate_tests.rs::counters_surface_emits_a_release_ledger_row`.
//! It drives the REAL production segmented object-log backends over a real S3-compatible endpoint (MinIO)
//! by injecting an `S3BlobStore` through `open_with_blob_store`, and measures the E3 bars:
//!
//!   1. **>=4 commit-latency bounds** — each profile runs at `1ms`, `5ms`, `20ms`, and `100ms`
//!      `SegmentConfig`s; per bound it reports the measured group-commit counters (segments sealed,
//!      objects PUT, mean/max commands per sealed segment) plus throughput.
//!   2. **Group-commit ack behavior at each configured bound** — concurrent pushes co-buffer; each push's
//!      wall-clock ack latency (returns only after seal+projection-apply) is recorded as declared-topology
//!      capacity evidence. Portable bars require a valid distribution and exact logical equivalence between
//!      interleaved enabled- and disabled-recorder arms, never an absolute machine-speed threshold.
//!   3. **Projection-appropriate recovery within the recovery-window budget** — both variants load a resident
//!      backlog (10,000,000 items in the release shape), reopen, and recover a streaming digest of every
//!      identity, client key, version, lifecycle state, payload, and field with zero missing/duplicate items.
//!      SQLite
//!      MUST resume from its durable snapshot high-water and replay a bounded tail; the intentionally
//!      ephemeral in-memory projection MUST report an exact bounded genesis replay (`start_seq=0`,
//!      `tail_replayed=total_commands`, `snapshot_used=false`).
//!   4. **Measured request-cost linkage** — every bound and recovery records the actual PUT, GET, LIST, and
//!      DELETE requests issued through the live `BlobStore` seam; release cost rows consume these counters.
//!
//! ## ENV-GATING (mirrors the postgres E0/E1 baseline + the MinIO substrate test)
//!
//! Gated on `FIREWEED_S3_TEST_ENDPOINT`; absent it, a LOUD skip prints and the test returns green (the E3
//! evidence is DEFERRED, never a hidden/fabricated pass). The two perf lanes:
//!   - SMOKE (default, any reachable MinIO): MEASURES + reports + emits SMOKE-tier rows. Bars are NOT
//!     hard-failed (a small resident over a casual endpoint is not a valid release perf environment).
//!   - PERF (`FIREWEED_PERF_ENV=1` AND the release resident shape `FIREWEED_E3_RESIDENT=10000000`): hard-asserts
//!     the bars and emits RELEASE-tier rows only when they are met.
//!
//! ## Running it (orbstack networking — this host cannot reach docker PUBLISHED ports; use the container IP)
//!
//! ```text
//! docker run -d --name fireweed-e3-minio -e MINIO_ROOT_USER=minioadmin \
//!     -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
//! IP=$(docker inspect fireweed-e3-minio --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
//! # routine live smoke (small resident, fast):
//! FIREWEED_S3_TEST_ENDPOINT="http://$IP:9000" \
//!     cargo test -p fireweed-server --release --test performance_object_log_e3_live_tests -- --nocapture
//! # the full TP-002 E3 RELEASE shape (10M-item snapshot-tail recovery; hard-fails the bars):
//! FIREWEED_PERF_ENV=1 FIREWEED_E3_RESIDENT=10000000 FIREWEED_S3_TEST_ENDPOINT="http://$IP:9000" \
//!     cargo test -p fireweed-server --release --test performance_object_log_e3_live_tests -- --nocapture
//! ```
//!
//! Optional overrides: `FIREWEED_S3_TEST_BUCKET` (default `fireweed-test`), `FIREWEED_S3_TEST_ACCESS_KEY` /
//! `FIREWEED_S3_TEST_SECRET_KEY` (default `minioadmin`), `FIREWEED_E3_LOAD_BATCH` (items per push command during
//! the recovery-load phase, default 1000), `FIREWEED_E3_ACK_PUSHES` (pushes per ack-latency config, default
//! 100000), `FIREWEED_E3_ACK_CONCURRENCY` (concurrent push tasks, default 384), `FIREWEED_E3_LOAD_CONCURRENCY`
//! (concurrent recovery-load tasks, default 8).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use fireweed_core::{
    ClientItemKey, EligibilityPolicy, ItemState, Metadata, OrderingMode, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use fireweed_engine::{
    CommandChecksum, CommandEnvelope, CommandId, ControlPlaneStore, EngineError, ProjectionRead,
    PushCommand, PushPort, PushSpec, QueueCommand, QueueKey, build_push_items,
};
use fireweed_objectlog::object_store_observability::{
    BlobBackendKind, BlobMetricsRecorder, BlobPhysicalTotals, InstrumentedBlobStore,
};
use fireweed_objectlog::prepare_serialized_commands;
use fireweed_objectlog::segmented::{
    BlobStore, FaultCutPoint, PointerFencedBlobStore, S3_LIST_PAGE_MAX_KEYS, S3BlobStore,
    SegmentConfig, SegmentCounters, SegmentedObjectLog,
};
use fireweed_server::{
    RecoveryStats, SegmentedObjectLogInMemoryBackend, SegmentedObjectLogSqliteBackend,
};

/// The release resident shape: the full TP-002 E3 10M-item snapshot-tail recovery measurement.
const RELEASE_RESIDENT: u64 = 10_000_000;
/// SP-04 slice 6: production recorder overhead versus an interleaved,
/// byte-identical disabled-recorder control must stay within two percent.
const MAX_RECORDER_OVERHEAD_RATIO: f64 = 1.02;
const RECORDER_CONTROL_BLOCKS: usize = 5;
const RELEASE_ACK_PUSHES: u64 = 100_000;
const RELEASE_ACK_CONCURRENCY: u64 = 384;
const RELEASE_LOAD_BATCH: u64 = 1_000;
const RELEASE_LOAD_CONCURRENCY: u64 = 8;
const RELEASE_LOAD_SEGMENT_TARGET_BYTES: usize = 917_504;
const RELEASE_LOAD_SIZE_SEAL_COMMANDS: usize = 4;
const RELEASE_QUEUE_WAITING_BYTES: usize = 16 * 1024 * 1024;
const STORE_OBJECT_PAGE_LIMIT: u64 = S3_LIST_PAGE_MAX_KEYS as u64;

struct RejectManifestHeadObjectWritesStore {
    inner: Arc<dyn BlobStore>,
    manifest_head_object_write_attempts: AtomicU64,
}

impl BlobStore for RejectManifestHeadObjectWritesStore {
    fn put(&self, key: &str, body: &[u8]) -> fireweed_engine::EngineResult<()> {
        if key.contains("/authority_head/") {
            self.manifest_head_object_write_attempts
                .fetch_add(1, Ordering::SeqCst);
            return Err(EngineError::Storage(
                "manifest-head authority must not be written to object storage".into(),
            ));
        }
        self.inner.put(key, body)
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> fireweed_engine::EngineResult<bool> {
        if key.contains("/authority_head/") {
            self.manifest_head_object_write_attempts
                .fetch_add(1, Ordering::SeqCst);
            return Err(EngineError::Storage(
                "manifest-head authority must not be written to object storage".into(),
            ));
        }
        self.inner.put_if_absent(key, body)
    }
    fn get(&self, key: &str) -> fireweed_engine::EngineResult<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn delete(&self, key: &str) -> fireweed_engine::EngineResult<bool> {
        self.inner.delete(key)
    }
    fn list(&self, prefix: &str) -> fireweed_engine::EngineResult<Vec<String>> {
        self.inner.list(prefix)
    }
    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> fireweed_engine::EngineResult<Vec<String>> {
        self.inner.list_page(prefix, start_after, limit)
    }
    fn backend_kind(&self) -> BlobBackendKind {
        self.inner.backend_kind()
    }
}

#[test]
fn pointer_authority_guard_rejects_manifest_head_object_writes() {
    let store = RejectManifestHeadObjectWritesStore {
        inner: Arc::new(fireweed_objectlog::segmented::InMemoryBlobStore::new()),
        manifest_head_object_write_attempts: AtomicU64::new(0),
    };
    let result = store.put(
        "t/tenant/q/queue/authority_head/00000000000000000001.json",
        b"head",
    );
    assert!(matches!(result, Err(EngineError::Storage(_))));
    assert_eq!(
        store
            .manifest_head_object_write_attempts
            .load(Ordering::SeqCst),
        1
    );
}

fn prove_postgres_pointer_fence(s3: &S3Env, source_revision: &str, output: &std::path::Path) {
    use fireweed_conformance::{envelope, item};
    use fireweed_engine::{PushCommand, QueueCommand};

    let postgres_url = std::env::var("FIREWEED_E3_POSTGRES_POINTER_DATABASE_URL")
        .expect("governed no-CAS proof requires FIREWEED_E3_POSTGRES_POINTER_DATABASE_URL");
    let raw_objects: Arc<dyn BlobStore> = Arc::new(
        S3BlobStore::new(
            &s3.endpoint,
            &s3.bucket,
            &s3.access,
            &s3.secret,
            "us-east-1",
        )
        .expect("build no-CAS S3 object store"),
    );
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let push = |id: &str, suffix: &str| {
        vec![envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item(id, &format!("e3-fence-{suffix}"), 0)],
            }),
            vec![],
        )]
    };

    // Execute the release store's native create-only-CAS fence path over MinIO. These observations are
    // independent of the Postgres pointer fallback below; neither path may lend booleans to the other.
    let native_def = qdef("e3", &format!("e3-native-fence-{}", std::process::id()));
    let native_shard = QueueKey::new(native_def.tenant_id.clone(), native_def.queue_id.clone());
    let native_a = SegmentedObjectLog::open(raw_objects.clone(), cfg);
    native_a.create_queue(&native_def).unwrap();
    assert_eq!(native_a.fence_epoch(&native_shard, 0, 1).unwrap(), 0);
    native_a
        .enqueue(&native_shard, &push("1", "native-a"), 0, 2)
        .unwrap();
    native_a.seal(&native_shard, 0, 3).unwrap();
    let native_b = SegmentedObjectLog::open(raw_objects.clone(), cfg);
    native_b.create_queue(&native_def).unwrap();
    assert_eq!(native_b.acquire_epoch(&native_shard, 4).unwrap(), 1);
    native_b
        .enqueue(&native_shard, &push("2", "native-b"), 1, 5)
        .unwrap();
    native_b.seal(&native_shard, 1, 6).unwrap();
    let native_current_epoch_committed = native_b.read_all(&native_shard).unwrap().len() == 2;
    native_a
        .enqueue(&native_shard, &push("3", "native-stale"), 0, 7)
        .unwrap();
    let native_stale_epoch_rejected =
        native_a.seal(&native_shard, 0, 8) == Err(EngineError::EpochFenced);
    assert!(native_current_epoch_committed);
    assert!(native_stale_epoch_rejected);

    let objects = Arc::new(RejectManifestHeadObjectWritesStore {
        inner: raw_objects,
        manifest_head_object_write_attempts: AtomicU64::new(0),
    });
    // Independent clients are essential: a process-local mutex is not the fence under test.
    let pointer_a = Arc::new(
        fireweed_postgres::PostgresManifestPointer::open(&postgres_url)
            .expect("open owner-A Postgres pointer"),
    );
    let pointer_b = Arc::new(
        fireweed_postgres::PostgresManifestPointer::open(&postgres_url)
            .expect("open owner-B Postgres pointer"),
    );
    let no_cas_def = qdef("e3", &format!("e3-pg-fence-{}", std::process::id()));
    let shard = QueueKey::new(no_cas_def.tenant_id.clone(), no_cas_def.queue_id.clone());
    let adapter_a = Arc::new(PointerFencedBlobStore::new(objects.clone(), pointer_a));
    let owner_a = SegmentedObjectLog::open(adapter_a, cfg);
    owner_a.create_queue(&no_cas_def).unwrap();
    assert_eq!(owner_a.fence_epoch(&shard, 0, 1).unwrap(), 0);
    owner_a.enqueue(&shard, &push("1", "a"), 0, 2).unwrap();
    owner_a.seal(&shard, 0, 3).unwrap();

    let adapter_b = Arc::new(PointerFencedBlobStore::new(objects.clone(), pointer_b));
    let owner_b = SegmentedObjectLog::open(adapter_b, cfg);
    owner_b.create_queue(&no_cas_def).unwrap();
    assert_eq!(owner_b.acquire_epoch(&shard, 4).unwrap(), 1);
    owner_b.enqueue(&shard, &push("2", "b"), 1, 5).unwrap();
    owner_b.seal(&shard, 1, 6).unwrap();
    assert_eq!(
        objects
            .manifest_head_object_write_attempts
            .load(Ordering::SeqCst),
        0
    );
    owner_a.enqueue(&shard, &push("3", "stale"), 0, 7).unwrap();
    assert_eq!(owner_a.seal(&shard, 0, 8), Err(EngineError::EpochFenced));
    assert_eq!(owner_b.read_all(&shard).unwrap().len(), 2);

    // Model a process restart with a fresh Postgres client. The authoritative head must be readable
    // directly from Postgres; there is no object-store manifest-head copy to reconstruct or repair.
    let pointer_restart = Arc::new(
        fireweed_postgres::PostgresManifestPointer::open(&postgres_url)
            .expect("open fresh restart Postgres pointer"),
    );
    let restarted = PointerFencedBlobStore::new(objects.clone(), pointer_restart);
    let pointer_prefix =
        SegmentedObjectLog::<Arc<PointerFencedBlobStore>>::authoritative_manifest_pointer_prefix(
            &shard,
        );
    let restarted_head = restarted
        .read_manifest_head(&pointer_prefix)
        .expect("read restart pointer authority")
        .expect("restart pointer head exists");
    assert_eq!(restarted_head.value.current_epoch, 1);
    let manifest_head_object_write_attempts = objects
        .manifest_head_object_write_attempts
        .load(Ordering::SeqCst);
    assert_eq!(manifest_head_object_write_attempts, 0);

    let row = fireweed_release::e3_contract::build_e3_fence_evidence(
        fireweed_release::e3_contract::E3FenceObservation {
            source_revision: source_revision.to_owned(),
            stale_epoch_rejected: native_stale_epoch_rejected,
            current_epoch_committed: native_current_epoch_committed,
            no_cas_stale_epoch_rejected: true,
            no_cas_current_epoch_committed: true,
            no_cas_pointer_and_epoch_atomic: true,
            no_cas_object_store_manifest_head_write_attempts: manifest_head_object_write_attempts,
            no_cas_restart_fresh_postgres_client: true,
            no_cas_restart_read_authoritative_pointer: true,
        },
    )
    .expect("build executed Postgres-pointer fence evidence");
    fireweed_release::e3_contract::write_e3_fence_evidence(output, &row)
        .expect("write executed Postgres-pointer fence evidence");
}

const E3_BOUND_CONFIGS: [BoundConfig; 4] = [
    BoundConfig {
        label: "1ms",
        target_bytes: 8_388_608,
        max_latency_ms: 1,
    },
    BoundConfig {
        label: "5ms",
        target_bytes: 8_388_608,
        max_latency_ms: 5,
    },
    BoundConfig {
        label: "20ms",
        target_bytes: 8_388_608,
        max_latency_ms: 20,
    },
    BoundConfig {
        label: "100ms",
        target_bytes: 8_388_608,
        max_latency_ms: 100,
    },
];

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn qdef(tenant: &str, queue: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new(tenant).unwrap(),
        queue_id: QueueId::new(queue).unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 600_000,
        client_item_key_retention_ms: 600_000,
        terminal_retention_ms: 600_000,
        max_lease_duration_ms: 3_600_000,
        retry_policy: RetryPolicy {
            max_attempts: 1_000_000,
        },
        max_push_batch_size: 10_000_000,
        max_claim_batch_size: 10_000_000,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

fn keyed_spec(payload: &str, client_item_key: Option<ClientItemKey>) -> PushSpec {
    PushSpec {
        client_item_key,
        priority: None,
        not_before: None,
        group_key: None,
        payload: Some(Bytes::from(payload.to_string())),
        fields: BTreeMap::new(),
        metadata: Metadata::default(),
        cohort_size: None,
        gate_keys: Vec::new(),
        entity: None,
    }
}

/// Deterministic, non-trivial state used by both recorder-control arms.
fn ack_spec(worker: u64, id: u64) -> PushSpec {
    let key = format!("ack-{worker}-{id}");
    let mut fields = BTreeMap::new();
    fields.insert("ordinal".into(), Bytes::from(id.to_string()));
    fields.insert("worker".into(), Bytes::from(worker.to_string()));
    PushSpec {
        client_item_key: Some(ClientItemKey::new(key.clone()).unwrap()),
        priority: Some(PriorityValue::Int64((id % 97) as i64)),
        not_before: Some(UtcTimestamp::new(1_700_000_000 + (id % 17) as i64, 0).unwrap()),
        payload: Some(Bytes::from(format!("payload:{key}"))),
        fields,
        ..PushSpec::default()
    }
}

fn ts() -> UtcTimestamp {
    UtcTimestamp::new(1_700_000_000, 0).unwrap()
}

fn wall_ts() -> UtcTimestamp {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after Unix epoch");
    UtcTimestamp::new(now.as_secs() as i64, now.subsec_nanos()).unwrap()
}

fn pct(latencies_ms: &mut [f64], p: f64) -> f64 {
    if latencies_ms.is_empty() {
        return 0.0;
    }
    latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = (((latencies_ms.len() as f64) * p).ceil() as usize)
        .saturating_sub(1)
        .min(latencies_ms.len() - 1);
    latencies_ms[idx]
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

#[derive(Clone, Copy)]
struct BoundConfig {
    label: &'static str,
    target_bytes: usize,
    max_latency_ms: u64,
}

#[derive(Clone)]
struct S3Env {
    endpoint: String,
    bucket: String,
    access: String,
    secret: String,
}

impl S3Env {
    fn instrumented_store(
        &self,
        recorder_enabled: bool,
    ) -> (Arc<dyn BlobStore>, Arc<BlobMetricsRecorder>) {
        let s3 = S3BlobStore::new(
            &self.endpoint,
            &self.bucket,
            &self.access,
            &self.secret,
            "us-east-1",
        )
        .expect("build S3 client");
        let recorder = Arc::new(if recorder_enabled {
            BlobMetricsRecorder::new()
        } else {
            BlobMetricsRecorder::disabled()
        });
        (
            Arc::new(InstrumentedBlobStore::new(
                s3,
                Arc::clone(&recorder),
                BlobBackendKind::S3,
            )),
            recorder,
        )
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct StoreOperations {
    puts: u64,
    gets: u64,
    lists: u64,
    deletes: u64,
    request_bytes: u64,
    response_bytes: u64,
}

impl From<BlobPhysicalTotals> for StoreOperations {
    fn from(value: BlobPhysicalTotals) -> Self {
        Self {
            puts: value.puts,
            gets: value.gets,
            lists: value.lists,
            deletes: value.deletes,
            request_bytes: value.request_bytes,
            response_bytes: value.response_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ResourceBounds {
    configured_global_bytes: u64,
    current_bytes: u64,
    peak_bytes: u64,
    waiters: u64,
    recorder_in_flight: u64,
    recorder_peak_in_flight: u64,
    task_count: u64,
    task_limit: u64,
    store_in_flight_limit: u64,
    object_page_limit: u64,
}

trait E3Flusher {
    fn spawn_background_flusher(self: &Arc<Self>) -> tokio::task::JoinHandle<()>;
}

impl E3Flusher for SegmentedObjectLogSqliteBackend {
    fn spawn_background_flusher(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        SegmentedObjectLogSqliteBackend::spawn_flusher(self)
    }
}

impl E3Flusher for SegmentedObjectLogInMemoryBackend {
    fn spawn_background_flusher(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        SegmentedObjectLogInMemoryBackend::spawn_flusher(self)
    }
}

struct E3ProfileSpec {
    backend_profile: &'static str,
    requires_snapshot: bool,
}

const E3_PROFILE_SPECS: [E3ProfileSpec; 2] = [
    E3ProfileSpec {
        backend_profile: "object_log_inmemory_projection",
        requires_snapshot: false,
    },
    E3ProfileSpec {
        backend_profile: "object_log_sqlite_projection",
        requires_snapshot: true,
    },
];

/// A unique scratch SQLite projection path under the system temp dir (removed at the end of the run).
fn projection_path(label: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir()
        .join(format!(
            "fireweed-e3-{label}-{}-{n}-{nanos}.db",
            std::process::id()
        ))
        .to_str()
        .expect("utf8 temp path")
        .to_string()
}

fn copy_sqlite_projection(source: &str, destination: &str) {
    let connection =
        rusqlite::Connection::open(source).expect("open SQLite projection control source");
    connection
        .execute("VACUUM INTO ?1", [destination])
        .expect("create transactionally consistent SQLite projection control");
}

/// One bound measurement inside one backend-profile run.
struct AckResult {
    label: &'static str,
    target_bytes: usize,
    max_latency_ms: u64,
    segments_sealed: u64,
    objects_put: u64,
    store_operations: StoreOperations,
    resource_bounds: ResourceBounds,
    commands_committed: u64,
    mean_batch: f64,
    max_batch: usize,
    throughput_per_s: f64,
    disabled_control_throughput_per_s: f64,
    recorder_overhead_ratio: f64,
    recorder_overhead_ratio_samples: Vec<f64>,
    recorder_control_order_seed: u64,
    recorder_control_schedule: &'static str,
    recorder_control_fingerprint_algorithm: &'static str,
    recorder_enabled_state_fingerprint: String,
    recorder_disabled_state_fingerprint: String,
    recorder_control_verified_items: u64,
    recorder_control_logical_match: bool,
    throughput_progress_met: bool,
    ack_p50_ms: f64,
    ack_p95_ms: f64,
    ack_p99_ms: f64,
    configured_window_ms: f64,
    latency_distribution_met: bool,
    load_shape_met: bool,
    bar_met: bool,
}

struct AckArm {
    counters: SegmentCounters,
    store_operations: StoreOperations,
    resource_bounds: ResourceBounds,
    wall_s: f64,
    latencies: Vec<f64>,
    pending: u64,
    state_fingerprint: StateFingerprint,
}

struct ProfileRun {
    backend_profile: &'static str,
    projection_label: &'static str,
    ack_results: Vec<AckResult>,
    recovery: Option<RecoveryResult>,
    wall_ms: f64,
    bars_met: bool,
}

trait E3Backend:
    ControlPlaneStore + PushPort + ProjectionRead + E3Flusher + Send + Sync + 'static
{
    fn snapshot_segment_counters(&self) -> SegmentCounters;
    fn resource_bounds(&self) -> ResourceBounds;
    fn set_recovery_tail_fault(
        &self,
        hook: Option<Arc<dyn fireweed_objectlog::segmented::FaultHook>>,
    );
}

impl E3Backend for SegmentedObjectLogSqliteBackend {
    fn snapshot_segment_counters(&self) -> SegmentCounters {
        SegmentedObjectLogSqliteBackend::segment_counters(self)
    }

    fn resource_bounds(&self) -> ResourceBounds {
        let snapshot = self.byte_admission_snapshot();
        ResourceBounds {
            configured_global_bytes: snapshot.configured_global_bytes as u64,
            current_bytes: snapshot.current_bytes as u64,
            peak_bytes: snapshot.peak_bytes as u64,
            waiters: snapshot.waiters as u64,
            ..ResourceBounds::default()
        }
    }

    fn set_recovery_tail_fault(
        &self,
        hook: Option<Arc<dyn fireweed_objectlog::segmented::FaultHook>>,
    ) {
        self.set_object_log_fault_hook(hook);
    }
}

impl E3Backend for SegmentedObjectLogInMemoryBackend {
    fn snapshot_segment_counters(&self) -> SegmentCounters {
        SegmentedObjectLogInMemoryBackend::segment_counters(self)
    }

    fn resource_bounds(&self) -> ResourceBounds {
        let snapshot = self.byte_admission_snapshot();
        ResourceBounds {
            configured_global_bytes: snapshot.configured_global_bytes as u64,
            current_bytes: snapshot.current_bytes as u64,
            peak_bytes: snapshot.peak_bytes as u64,
            waiters: snapshot.waiters as u64,
            ..ResourceBounds::default()
        }
    }

    fn set_recovery_tail_fault(
        &self,
        hook: Option<Arc<dyn fireweed_objectlog::segmented::FaultHook>>,
    ) {
        self.set_object_log_fault_hook(hook);
    }
}

struct CommitBeforeApplyCrash {
    struck: std::sync::atomic::AtomicBool,
}

impl fireweed_objectlog::segmented::FaultHook for CommitBeforeApplyCrash {
    fn fault_point(&self, cut: FaultCutPoint) -> fireweed_engine::EngineResult<()> {
        if cut == FaultCutPoint::AfterManifestBeforeAck && !self.struck.swap(true, Ordering::SeqCst)
        {
            return Err(EngineError::Storage(
                "E3 deterministic crash after manifest commit".into(),
            ));
        }
        Ok(())
    }
}

trait E3RecoveryProbe {
    fn recovery_probe(&self, shard: &QueueKey) -> Option<RecoveryStats>;
}

impl E3RecoveryProbe for SegmentedObjectLogSqliteBackend {
    fn recovery_probe(&self, shard: &QueueKey) -> Option<RecoveryStats> {
        self.recovery_stats(shard)
    }
}

impl E3RecoveryProbe for SegmentedObjectLogInMemoryBackend {
    fn recovery_probe(&self, shard: &QueueKey) -> Option<RecoveryStats> {
        self.recovery_stats(shard)
    }
}

trait E3OrderProbe {
    fn recovery_order_page(
        &self,
        shard: &QueueKey,
        after: Option<fireweed_core::ItemId>,
        limit: usize,
    ) -> fireweed_engine::EngineResult<Vec<fireweed_engine::ItemView>>;
}

impl E3OrderProbe for SegmentedObjectLogSqliteBackend {
    fn recovery_order_page(
        &self,
        shard: &QueueKey,
        after: Option<fireweed_core::ItemId>,
        limit: usize,
    ) -> fireweed_engine::EngineResult<Vec<fireweed_engine::ItemView>> {
        SegmentedObjectLogSqliteBackend::recovery_order_page(self, shard, after, limit)
    }
}

impl E3OrderProbe for SegmentedObjectLogInMemoryBackend {
    fn recovery_order_page(
        &self,
        shard: &QueueKey,
        after: Option<fireweed_core::ItemId>,
        limit: usize,
    ) -> fireweed_engine::EngineResult<Vec<fireweed_engine::ItemView>> {
        SegmentedObjectLogInMemoryBackend::recovery_order_page(self, shard, after, limit)
    }
}

/// Drive `pushes` single-item pushes through one backend/profile over MinIO at `concurrency`, with the
/// flusher running, recording each push's ack latency and end-to-end throughput.
struct AckArmConfig {
    profile: &'static str,
    bound: BoundConfig,
    pushes: u64,
    concurrency: u64,
    recorder_enabled: bool,
    block: usize,
}

async fn run_ack_arm<B, F>(s3: &S3Env, config: AckArmConfig, open: F) -> AckArm
where
    B: E3Backend,
    F: Fn(Arc<dyn BlobStore>, &str, SegmentConfig) -> fireweed_engine::EngineResult<B>,
{
    let arm = if config.recorder_enabled {
        "enabled"
    } else {
        "disabled"
    };
    let qid = format!(
        "e3ack-{}-{}-{arm}-block{}-{}",
        config.profile,
        config.bound.label,
        config.block,
        std::process::id()
    );
    let def = qdef("e3", &qid);
    let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    let proj = projection_path(&format!(
        "ack-{}-{}-{arm}",
        config.profile, config.bound.label
    ));
    let cfg = SegmentConfig::new(config.bound.target_bytes, config.bound.max_latency_ms).unwrap();

    let (store, recorder) = s3.instrumented_store(config.recorder_enabled);
    let backend = Arc::new(open(store, &proj, cfg).expect("open segmented backend over S3"));
    backend.create_queue(def).await.expect("create queue");
    let flusher = backend.spawn_background_flusher();
    let store_baseline = recorder.snapshot();
    let started = Instant::now();

    let mut handles = Vec::new();
    for t in 0..config.concurrency {
        let start_index = t * config.pushes / config.concurrency;
        let end_index = (t + 1) * config.pushes / config.concurrency;
        let backend = backend.clone();
        let shard = shard.clone();
        handles.push(tokio::spawn(async move {
            let mut lat = Vec::with_capacity((end_index - start_index) as usize);
            for i in start_index..end_index {
                let start = Instant::now();
                backend
                    .push(&shard, vec![ack_spec(t, i)], ts(), None)
                    .await
                    .expect("push acked after seal");
                lat.push(start.elapsed().as_secs_f64() * 1000.0);
            }
            lat
        }));
    }
    let task_count = handles.len() as u64;
    let mut latencies = Vec::with_capacity(config.pushes as usize);
    for h in handles {
        latencies.extend(h.await.expect("ack task joined"));
    }
    flusher.abort();
    let wall_s = started.elapsed().as_secs_f64();
    let c = backend.snapshot_segment_counters();
    let pending = backend.metrics(&shard).await.unwrap().pending;
    let state_fingerprint =
        fingerprint_ack_state(backend.as_ref(), &shard, config.pushes, config.concurrency).await;
    let snapshot = recorder.snapshot();
    let mut resource_bounds = backend.resource_bounds();
    resource_bounds.recorder_in_flight = snapshot.in_flight;
    resource_bounds.recorder_peak_in_flight = snapshot.peak_in_flight;
    resource_bounds.task_count = task_count;
    resource_bounds.task_limit = RELEASE_ACK_CONCURRENCY;
    resource_bounds.store_in_flight_limit = RELEASE_ACK_CONCURRENCY;
    resource_bounds.object_page_limit = STORE_OBJECT_PAGE_LIMIT;
    let store_operations = snapshot.delta(&store_baseline).physical_totals().into();
    let _ = std::fs::remove_file(&proj);
    AckArm {
        counters: c,
        store_operations,
        resource_bounds,
        wall_s,
        latencies,
        pending,
        state_fingerprint,
    }
}

fn aggregate_ack_arms(arms: Vec<AckArm>) -> AckArm {
    assert!(!arms.is_empty());
    let mut counters = SegmentCounters::default();
    let mut store_operations = StoreOperations::default();
    let mut resource_bounds = ResourceBounds::default();
    let mut wall_s = 0.0;
    let mut latencies = Vec::new();
    let mut pending = 0;
    let mut digest = StreamingDigest::new();
    let mut fingerprint = StateFingerprint {
        digest: String::new(),
        verified: 0,
        missing: 0,
        duplicates: 0,
        invalid: 0,
    };
    for arm in arms {
        counters.segments_sealed += arm.counters.segments_sealed;
        counters.objects_put += arm.counters.objects_put;
        counters.commands_committed += arm.counters.commands_committed;
        counters
            .group_commit_batches
            .extend(arm.counters.group_commit_batches);
        counters.size_triggered_seals += arm.counters.size_triggered_seals;
        counters.latency_triggered_seals += arm.counters.latency_triggered_seals;
        counters.forced_seals += arm.counters.forced_seals;
        counters.rollover_seals += arm.counters.rollover_seals;
        counters.object_count += arm.counters.object_count;
        counters.total_bytes += arm.counters.total_bytes;
        counters.segment_bytes += arm.counters.segment_bytes;
        counters.max_object_bytes = counters.max_object_bytes.max(arm.counters.max_object_bytes);
        counters.put_count += arm.counters.put_count;
        counters.get_count += arm.counters.get_count;
        counters.list_count += arm.counters.list_count;
        counters.delete_count += arm.counters.delete_count;
        counters.request_bytes += arm.counters.request_bytes;
        counters.response_bytes += arm.counters.response_bytes;
        store_operations.puts += arm.store_operations.puts;
        store_operations.gets += arm.store_operations.gets;
        store_operations.lists += arm.store_operations.lists;
        store_operations.deletes += arm.store_operations.deletes;
        store_operations.request_bytes += arm.store_operations.request_bytes;
        store_operations.response_bytes += arm.store_operations.response_bytes;
        resource_bounds.configured_global_bytes = resource_bounds
            .configured_global_bytes
            .max(arm.resource_bounds.configured_global_bytes);
        resource_bounds.current_bytes += arm.resource_bounds.current_bytes;
        resource_bounds.peak_bytes = resource_bounds
            .peak_bytes
            .max(arm.resource_bounds.peak_bytes);
        resource_bounds.waiters += arm.resource_bounds.waiters;
        resource_bounds.recorder_in_flight += arm.resource_bounds.recorder_in_flight;
        resource_bounds.recorder_peak_in_flight = resource_bounds
            .recorder_peak_in_flight
            .max(arm.resource_bounds.recorder_peak_in_flight);
        resource_bounds.task_count = resource_bounds
            .task_count
            .max(arm.resource_bounds.task_count);
        resource_bounds.task_limit = resource_bounds
            .task_limit
            .max(arm.resource_bounds.task_limit);
        resource_bounds.store_in_flight_limit = resource_bounds
            .store_in_flight_limit
            .max(arm.resource_bounds.store_in_flight_limit);
        resource_bounds.object_page_limit = resource_bounds
            .object_page_limit
            .max(arm.resource_bounds.object_page_limit);
        wall_s += arm.wall_s;
        latencies.extend(arm.latencies);
        pending += arm.pending;
        digest.update(arm.state_fingerprint.digest.as_bytes());
        fingerprint.verified += arm.state_fingerprint.verified;
        fingerprint.missing += arm.state_fingerprint.missing;
        fingerprint.duplicates += arm.state_fingerprint.duplicates;
        fingerprint.invalid += arm.state_fingerprint.invalid;
    }
    fingerprint.digest = digest.finish();
    AckArm {
        counters,
        store_operations,
        resource_bounds,
        wall_s,
        latencies,
        pending,
        state_fingerprint: fingerprint,
    }
}

fn recorder_control_order_seed(profile: &str, bound: BoundConfig) -> u64 {
    let mut seed = 0xcbf2_9ce4_8422_2325u64 ^ bound.max_latency_ms;
    for byte in profile.bytes() {
        seed ^= u64::from(byte);
        seed = seed.wrapping_mul(0x100_0000_01b3);
    }
    seed
}

/// Compare independent, bounded enabled/disabled blocks over byte-identical seeded work. The seeded first
/// arm alternates on every block so neither recorder mode always benefits from warm caches or run order.
async fn run_ack_config<B, F>(
    s3: &S3Env,
    profile: &'static str,
    bound: BoundConfig,
    pushes: u64,
    concurrency: u64,
    open: F,
) -> AckResult
where
    B: E3Backend,
    F: Copy + Fn(Arc<dyn BlobStore>, &str, SegmentConfig) -> fireweed_engine::EngineResult<B>,
{
    let order_seed = recorder_control_order_seed(profile, bound);
    let first_block_enabled_first = order_seed & 1 == 0;
    let mut enabled_blocks = Vec::with_capacity(RECORDER_CONTROL_BLOCKS);
    let mut disabled_blocks = Vec::with_capacity(RECORDER_CONTROL_BLOCKS);
    let mut overhead_samples = Vec::with_capacity(RECORDER_CONTROL_BLOCKS);
    for block in 0..RECORDER_CONTROL_BLOCKS {
        let block_start = block as u64 * pushes / RECORDER_CONTROL_BLOCKS as u64;
        let block_end = (block as u64 + 1) * pushes / RECORDER_CONTROL_BLOCKS as u64;
        let block_pushes = block_end - block_start;
        let run = |recorder_enabled| {
            run_ack_arm::<B, _>(
                s3,
                AckArmConfig {
                    profile,
                    bound,
                    pushes: block_pushes,
                    concurrency,
                    recorder_enabled,
                    block,
                },
                open,
            )
        };
        let enabled_first = if block % 2 == 0 {
            first_block_enabled_first
        } else {
            !first_block_enabled_first
        };
        let (enabled, disabled) = if enabled_first {
            let enabled = run(true).await;
            let disabled = run(false).await;
            (enabled, disabled)
        } else {
            let disabled = run(false).await;
            let enabled = run(true).await;
            (enabled, disabled)
        };
        overhead_samples.push(enabled.wall_s / disabled.wall_s.max(f64::MIN_POSITIVE));
        enabled_blocks.push(enabled);
        disabled_blocks.push(disabled);
    }
    let mut enabled = aggregate_ack_arms(enabled_blocks);
    let disabled = aggregate_ack_arms(disabled_blocks);

    let c = enabled.counters;
    let throughput_per_s = pushes as f64 / enabled.wall_s.max(f64::MIN_POSITIVE);
    let disabled_control_throughput_per_s = pushes as f64 / disabled.wall_s.max(f64::MIN_POSITIVE);
    let mut ordered_overhead_samples = overhead_samples.clone();
    let recorder_overhead_ratio = pct(&mut ordered_overhead_samples, 0.50);
    let recorder_degradation_met = recorder_overhead_ratio.is_finite()
        && recorder_overhead_ratio <= MAX_RECORDER_OVERHEAD_RATIO;
    let ack_p50 = pct(&mut enabled.latencies, 0.50);
    let ack_p95 = pct(&mut enabled.latencies, 0.95);
    let ack_p99 = pct(&mut enabled.latencies, 0.99);
    let configured_window_ms = bound.max_latency_ms as f64 + (bound.max_latency_ms as f64 / 4.0);
    let throughput_progress_met = throughput_per_s.is_finite() && throughput_per_s > 0.0;
    let latency_distribution_met = ack_p50.is_finite()
        && ack_p95.is_finite()
        && ack_p99.is_finite()
        && ack_p50 <= ack_p95
        && ack_p95 <= ack_p99;
    let recorder_control_logical_match = c.commands_committed
        == disabled.counters.commands_committed
        && enabled.pending == pushes
        && disabled.pending == pushes
        && enabled.state_fingerprint.digest == disabled.state_fingerprint.digest
        && enabled.state_fingerprint.verified == pushes
        && disabled.state_fingerprint.verified == pushes
        && enabled.state_fingerprint.missing == 0
        && disabled.state_fingerprint.missing == 0
        && enabled.state_fingerprint.duplicates == 0
        && disabled.state_fingerprint.duplicates == 0
        && enabled.state_fingerprint.invalid == 0
        && disabled.state_fingerprint.invalid == 0;
    let resources_met = enabled.resource_bounds.current_bytes == 0
        && enabled.resource_bounds.waiters == 0
        && enabled.resource_bounds.peak_bytes <= enabled.resource_bounds.configured_global_bytes
        && enabled.resource_bounds.recorder_in_flight == 0
        && enabled.resource_bounds.recorder_peak_in_flight
            <= enabled.resource_bounds.store_in_flight_limit
        && enabled.resource_bounds.task_count <= enabled.resource_bounds.task_limit
        && enabled.resource_bounds.object_page_limit == STORE_OBJECT_PAGE_LIMIT;
    let load_shape_met = c.commands_committed >= pushes
        && c.segments_sealed > 0
        && c.objects_put > 0
        && c.max_batch_size() > 1
        && resources_met;
    let bar_met = throughput_progress_met
        && latency_distribution_met
        && load_shape_met
        && recorder_degradation_met
        && recorder_control_logical_match;

    AckResult {
        label: bound.label,
        target_bytes: bound.target_bytes,
        max_latency_ms: bound.max_latency_ms,
        segments_sealed: c.segments_sealed,
        objects_put: c.objects_put,
        store_operations: enabled.store_operations,
        resource_bounds: enabled.resource_bounds,
        commands_committed: c.commands_committed,
        mean_batch: round3(c.mean_batch_size()),
        max_batch: c.max_batch_size(),
        throughput_per_s: round3(throughput_per_s),
        disabled_control_throughput_per_s: round3(disabled_control_throughput_per_s),
        recorder_overhead_ratio: round3(recorder_overhead_ratio),
        recorder_overhead_ratio_samples: overhead_samples.into_iter().map(round3).collect(),
        recorder_control_order_seed: order_seed,
        recorder_control_schedule: "independent-bounded-blocks-seeded-alternating-order-v1",
        recorder_control_fingerprint_algorithm: "fnv1a128+disk-unique-id-index+canonical-live-state-v1",
        recorder_enabled_state_fingerprint: enabled.state_fingerprint.digest,
        recorder_disabled_state_fingerprint: disabled.state_fingerprint.digest,
        recorder_control_verified_items: enabled.state_fingerprint.verified,
        recorder_control_logical_match,
        throughput_progress_met,
        ack_p50_ms: round3(ack_p50),
        ack_p95_ms: round3(ack_p95),
        ack_p99_ms: round3(ack_p99),
        configured_window_ms: round3(configured_window_ms),
        latency_distribution_met,
        load_shape_met,
        bar_met,
    }
}

struct RecoveryResult {
    resident: u64,
    load_batch: u64,
    load_task_count: u64,
    load_command_count: u64,
    load_segments_sealed: u64,
    load_size_triggered_seals: u64,
    load_latency_triggered_seals: u64,
    load_forced_seals: u64,
    load_rollover_seals: u64,
    load_segment_bytes: u64,
    load_mean_commands_per_segment: f64,
    load_max_commands_per_segment: usize,
    load_group_commit_batch_sum: u64,
    command_count: u64,
    total_commands: u64,
    start_seq: u64,
    tail_replayed: u64,
    snapshot_used: bool,
    recovery_max_tail: u64,
    recovery_wall_ms: f64,
    pending_after: u64,
    state_digest_before: String,
    state_digest_after: String,
    verified_items: u64,
    missing_items: u64,
    duplicate_items: u64,
    invalid_items: u64,
    replay_progress_samples: Vec<u64>,
    replay_command_page_limit: u64,
    peak_replay_commands_buffered: u64,
    peak_manifest_objects_buffered: u64,
    recovery_index_node_visits: u64,
    recovery_index_entries_visited: u64,
    recovery_index_height: u64,
    recovery_index_nodes_written_last_append: u64,
    recovery_segment_gets: u64,
    recovery_segment_bytes_fetched: u64,
    recovery_peak_segment_bytes_buffered: u64,
    recovery_peak_index_node_bytes_buffered: u64,
    recovery_peak_cursor_bytes_buffered: u64,
    bounded_authority_index: bool,
    verification_chunk_items: u64,
    queue_count: u64,
    resource_bounds: ResourceBounds,
    store_operations: StoreOperations,
    checksum_validation_passed: bool,
    bar_met: bool,
}

#[derive(Clone, Copy)]
struct StreamingDigest {
    left: u64,
    right: u64,
}

impl StreamingDigest {
    fn new() -> Self {
        Self {
            left: 0xcbf2_9ce4_8422_2325,
            right: 0x8422_2325_cbf2_9ce4,
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.left ^= u64::from(*byte);
            self.left = self.left.wrapping_mul(0x100_0000_01b3);
            self.right ^= u64::from(*byte).rotate_left(1);
            self.right = self.right.wrapping_mul(0x100_0000_01b3 ^ 0x9e37_79b9);
        }
        self.left ^= 0xff;
        self.right ^= 0x7f;
    }

    fn finish(self) -> String {
        format!("fnv1a128:{:016x}{:016x}", self.left, self.right)
    }
}

struct StateFingerprint {
    digest: String,
    verified: u64,
    missing: u64,
    duplicates: u64,
    invalid: u64,
}

/// Canonically fingerprint the complete ack-control state in bounded pages. Caller keys provide the
/// stable cross-arm identity/order; the disk index proves server-assigned item ids are also unique without
/// retaining a resident-sized set in memory.
async fn fingerprint_ack_state<B: ProjectionRead>(
    backend: &B,
    shard: &QueueKey,
    pushes: u64,
    concurrency: u64,
) -> StateFingerprint {
    const PAGE: u64 = 512;
    let identity_path = projection_path("ack-control-identity-index");
    let mut identity_db = rusqlite::Connection::open(&identity_path).unwrap();
    identity_db
        .execute_batch("PRAGMA journal_mode=OFF; CREATE TABLE seen_ids (id INTEGER PRIMARY KEY)")
        .unwrap();
    let mut digest = StreamingDigest::new();
    let mut verified = 0u64;
    let mut missing = 0u64;
    let mut duplicates = 0u64;
    let mut invalid = 0u64;
    let mut id_xor = 0u64;
    let mut id_sum = 0u64;

    for worker in 0..concurrency {
        let worker_start = worker * pushes / concurrency;
        let worker_end = (worker + 1) * pushes / concurrency;
        let mut start = worker_start;
        while start < worker_end {
            let end = (start + PAGE).min(worker_end);
            let keys = (start..end)
                .map(|id| ClientItemKey::new(format!("ack-{worker}-{id}")).unwrap())
                .collect::<Vec<_>>();
            let views = backend.live_items(shard, &keys).await.unwrap();
            assert_eq!(
                views.len(),
                keys.len(),
                "live_items preserves control shape"
            );
            let tx = identity_db.transaction().unwrap();
            for (offset, (key, view)) in keys.iter().zip(views).enumerate() {
                let id = start + offset as u64;
                let Some(view) = view else {
                    missing += 1;
                    continue;
                };
                verified += 1;
                let expected_payload = format!("payload:{}", key.as_str());
                let expected_priority = Some(PriorityValue::Int64((id % 97) as i64));
                let expected_not_before =
                    Some(UtcTimestamp::new(1_700_000_000 + (id % 17) as i64, 0).unwrap());
                let expected_fields = BTreeMap::from([
                    ("ordinal".to_string(), Bytes::from(id.to_string())),
                    ("worker".to_string(), Bytes::from(worker.to_string())),
                ]);
                if view.client_item_key != *key
                    || view.item_version != 1
                    || view.lifecycle_state != ItemState::Pending
                    || view.priority != expected_priority
                    || view.group_key.is_some()
                    || view.not_before != expected_not_before
                    || view.attempt_count != 0
                    || view.payload.as_deref() != Some(expected_payload.as_bytes())
                    || view.fields != expected_fields
                {
                    invalid += 1;
                }
                if tx
                    .execute(
                        "INSERT OR IGNORE INTO seen_ids(id) VALUES (?1)",
                        [view.item_id.as_u64() as i64],
                    )
                    .unwrap()
                    != 1
                {
                    duplicates += 1;
                }
                id_xor ^= view.item_id.as_u64();
                id_sum = id_sum.wrapping_add(view.item_id.as_u64());
                // Canonical logical order is (worker, id), independent of executor wake order.
                digest.update(&worker.to_le_bytes());
                digest.update(&id.to_le_bytes());
                digest.update(view.client_item_key.as_str().as_bytes());
                digest.update(&view.item_version.to_le_bytes());
                digest.update(format!("{:?}", view.lifecycle_state).as_bytes());
                digest.update(&serde_json::to_vec(&view.priority).unwrap());
                digest.update(&serde_json::to_vec(&view.group_key).unwrap());
                digest.update(&serde_json::to_vec(&view.not_before).unwrap());
                digest.update(&view.attempt_count.to_le_bytes());
                digest.update(view.payload.as_deref().unwrap_or_default());
                for (name, value) in &view.fields {
                    digest.update(name.as_bytes());
                    digest.update(value);
                }
            }
            tx.commit().unwrap();
            start = end;
        }
    }
    digest.update(&id_xor.to_le_bytes());
    digest.update(&id_sum.to_le_bytes());
    let _ = std::fs::remove_file(identity_path);
    StateFingerprint {
        digest: digest.finish(),
        verified,
        missing,
        duplicates,
        invalid,
    }
}

/// Verify the complete resident state without materializing another resident-sized collection. The
/// deterministic client-key stream is queried in bounded chunks; every full live view contributes to a
/// stable digest, while XOR/sum identity accumulators detect duplicate identities independently of count.
async fn fingerprint_state<B: ProjectionRead + E3OrderProbe>(
    backend: &B,
    shard: &QueueKey,
    resident: u64,
    chunk_items: u64,
) -> StateFingerprint {
    let mut digest = StreamingDigest::new();
    let mut verified = 0u64;
    let mut missing = 0u64;
    let mut id_xor = 0u64;
    let mut id_sum = 0u64;
    let identity_path = projection_path("recovery-identity-index");
    let mut identity_db = rusqlite::Connection::open(&identity_path).unwrap();
    identity_db
        .execute_batch(
            "PRAGMA journal_mode=OFF; \
             CREATE TABLE seen_ids (id INTEGER PRIMARY KEY, client_key TEXT NOT NULL UNIQUE); \
             CREATE TABLE seen_order_ids (id INTEGER PRIMARY KEY)",
        )
        .unwrap();
    let mut identity_domain = None;
    let mut duplicates = 0u64;
    let mut invalid = 0u64;
    let mut start = 0u64;
    while start < resident {
        let end = (start + chunk_items).min(resident);
        let keys = (start..end)
            .map(|id| ClientItemKey::new(format!("i{id}")).unwrap())
            .collect::<Vec<_>>();
        let views = backend.live_items(shard, &keys).await.unwrap();
        assert_eq!(views.len(), keys.len(), "live_items preserves input shape");
        let tx = identity_db.transaction().unwrap();
        for (index, (key, view)) in keys.iter().zip(views).enumerate() {
            let ordinal = start + index as u64;
            let Some(view) = view else {
                missing += 1;
                continue;
            };
            verified += 1;
            let expected_payload = format!("i{ordinal}");
            if view.client_item_key != *key
                || view.item_version != 1
                || view.lifecycle_state != ItemState::Pending
                || view.priority.is_some()
                || view.group_key.is_some()
                || view.not_before.is_some()
                || view.attempt_count != 0
                || view.payload.as_deref() != Some(expected_payload.as_bytes())
                || !view.fields.is_empty()
            {
                invalid += 1;
            }
            id_xor ^= view.item_id.as_u64();
            id_sum = id_sum.wrapping_add(view.item_id.as_u64());
            let domain = (view.item_id.epoch(), view.item_id.node());
            if identity_domain.get_or_insert(domain) != &domain {
                duplicates += 1;
            } else {
                let inserted = tx
                    .execute(
                        "INSERT OR IGNORE INTO seen_ids(id, client_key) VALUES (?1, ?2)",
                        rusqlite::params![
                            view.item_id.as_u64() as i64,
                            view.client_item_key.as_str()
                        ],
                    )
                    .unwrap();
                if inserted != 1 {
                    duplicates += 1;
                }
            }
            digest.update(&ordinal.to_le_bytes());
            digest.update(key.as_str().as_bytes());
            digest.update(&view.item_id.as_u64().to_le_bytes());
            digest.update(&view.item_version.to_le_bytes());
            digest.update(format!("{:?}", view.lifecycle_state).as_bytes());
            digest.update(&serde_json::to_vec(&view.priority).unwrap());
            digest.update(&serde_json::to_vec(&view.group_key).unwrap());
            digest.update(&serde_json::to_vec(&view.not_before).unwrap());
            digest.update(&view.attempt_count.to_le_bytes());
            digest.update(view.client_item_key.as_str().as_bytes());
            digest.update(view.payload.as_deref().unwrap_or_default());
            for (field, value) in view.fields {
                digest.update(field.as_bytes());
                digest.update(&value);
            }
        }
        tx.commit().unwrap();
        start = end;
    }
    let unique_ids: u64 = identity_db
        .query_row("SELECT COUNT(*) FROM seen_ids", [], |row| row.get(0))
        .unwrap();
    if unique_ids != verified {
        duplicates = duplicates.saturating_add(verified.abs_diff(unique_ids));
    }
    digest.update(&id_xor.to_le_bytes());
    digest.update(&id_sum.to_le_bytes());
    let mut order_offset = 0usize;
    let mut order_cursor = None;
    while order_offset < resident as usize {
        let page = backend
            .recovery_order_page(shard, order_cursor, chunk_items as usize)
            .unwrap();
        if page.is_empty() {
            invalid = invalid.saturating_add((resident as usize - order_offset) as u64);
            break;
        }
        for (index, item) in page.iter().enumerate() {
            let ordinal = order_offset + index;
            let belongs_to_verified_state = identity_db
                .query_row(
                    "SELECT 1 FROM seen_ids WHERE id=?1 AND client_key=?2",
                    rusqlite::params![item.item_id.as_u64() as i64, item.client_item_key.as_str()],
                    |_| Ok(()),
                )
                .is_ok();
            let first_in_order = identity_db
                .execute(
                    "INSERT OR IGNORE INTO seen_order_ids(id) VALUES (?1)",
                    [item.item_id.as_u64() as i64],
                )
                .unwrap()
                == 1;
            if !belongs_to_verified_state || !first_in_order {
                invalid += 1;
            }
            digest.update(b"authoritative-order");
            digest.update(&(ordinal as u64).to_le_bytes());
            digest.update(&item.item_id.as_u64().to_le_bytes());
            digest.update(item.client_item_key.as_str().as_bytes());
            digest.update(&serde_json::to_vec(&item.priority).unwrap());
            digest.update(&item.item_version.to_le_bytes());
        }
        order_cursor = page.last().map(|item| item.item_id);
        order_offset += page.len();
    }
    let ordered_unique: u64 = identity_db
        .query_row("SELECT COUNT(*) FROM seen_order_ids", [], |row| row.get(0))
        .unwrap();
    if ordered_unique != resident {
        invalid = invalid.saturating_add(resident.abs_diff(ordered_unique));
    }
    drop(identity_db);
    let _ = std::fs::remove_file(identity_path);
    StateFingerprint {
        digest: digest.finish(),
        verified,
        missing,
        duplicates,
        invalid,
    }
}

/// Push with a bounded retry on the substrate's documented same-epoch manifest-CAS `Conflict` (the seal doc
/// says such a transient race is "surfaced as a conflict so it is not mistaken for an ack" and the caller
/// retries). After the S3 `list` pagination fix this is rare, but a bounded retry keeps a long load robust.
async fn push_with_retry<B: PushPort>(backend: &B, shard: &QueueKey, items: Vec<PushSpec>) {
    let mut attempt = 0u64;
    loop {
        match backend.push(shard, items.clone(), wall_ts(), None).await {
            Ok(_) => return,
            Err(EngineError::Conflict) if attempt < 16 => {
                attempt += 1;
                tokio::time::sleep(std::time::Duration::from_millis(20 * attempt)).await;
            }
            Err(e) => panic!("push failed after {attempt} retries: {e:?}"),
        }
    }
}

fn concurrent_load_command_count(resident: u64, load_batch: u64, concurrency: u64) -> u64 {
    let concurrency = concurrency.max(1);
    let share = resident.div_ceil(concurrency);
    (0..concurrency)
        .map(|worker| {
            let start = worker * share;
            let end = (start + share).min(resident);
            end.saturating_sub(start).div_ceil(load_batch)
        })
        .sum()
}

#[derive(Debug)]
struct ReleaseLoadPreflight {
    raw_bytes: Vec<usize>,
    charged_bytes: Vec<usize>,
}

impl ReleaseLoadPreflight {
    fn smallest_subset_raw_bytes(&self, count: usize) -> usize {
        let mut raw = self.raw_bytes.clone();
        raw.sort_unstable();
        raw.into_iter().take(count).sum()
    }

    fn full_wave_charged_bytes(&self) -> usize {
        self.charged_bytes.iter().sum()
    }
}

/// Serialize the exact first wave emitted by the governed loader. This preflight makes the relationship
/// between command shape, size sealing, and byte admission a release invariant instead of a host-speed
/// assumption.
fn release_load_preflight(
    resident: u64,
    load_batch: u64,
    load_concurrency: u64,
) -> ReleaseLoadPreflight {
    let share = resident.div_ceil(load_concurrency);
    let mut raw_bytes = Vec::new();
    let mut charged_bytes = Vec::new();
    for worker in 0..load_concurrency {
        let start = worker * share;
        let end = (start + load_batch).min(resident);
        let specs = (start..end)
            .map(|id| {
                let key = format!("i{id}");
                keyed_spec(&key, Some(ClientItemKey::new(key.clone()).unwrap()))
            })
            .collect();
        let (items, ids) = build_push_items(
            specs,
            0,
            0,
            u32::try_from(worker * load_batch).expect("release counter fits u32"),
            1_000_000,
        );
        let envelope = CommandEnvelope {
            command_id: CommandId::new(format!("seginmem-0-{worker}")),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: ids,
            command: QueueCommand::Push(PushCommand { items }),
            checksum: CommandChecksum(0),
            created_at: ts(),
        };
        let (serialized, charged) =
            prepare_serialized_commands(vec![envelope], RELEASE_QUEUE_WAITING_BYTES)
                .expect("serialize release load command");
        raw_bytes.push(serialized[0].record_len());
        charged_bytes.push(charged);
    }
    ReleaseLoadPreflight {
        raw_bytes,
        charged_bytes,
    }
}

fn assert_release_load_preflight() -> ReleaseLoadPreflight {
    let preflight = release_load_preflight(
        RELEASE_RESIDENT,
        RELEASE_LOAD_BATCH,
        RELEASE_LOAD_CONCURRENCY,
    );
    let smallest_size_seal_raw =
        preflight.smallest_subset_raw_bytes(RELEASE_LOAD_SIZE_SEAL_COMMANDS);
    let smaller_subset_raw =
        preflight.smallest_subset_raw_bytes(RELEASE_LOAD_SIZE_SEAL_COMMANDS - 1);
    assert!(
        smallest_size_seal_raw * 100 >= RELEASE_LOAD_SEGMENT_TARGET_BYTES * 110,
        "four smallest first-wave commands must exceed the target by at least ten percent"
    );
    assert!(
        smaller_subset_raw < RELEASE_LOAD_SEGMENT_TARGET_BYTES,
        "three smallest first-wave commands must stay below the target"
    );
    assert!(
        preflight
            .raw_bytes
            .iter()
            .all(|bytes| *bytes < RELEASE_LOAD_SEGMENT_TARGET_BYTES),
        "one command must not size-seal alone"
    );
    assert!(
        preflight.full_wave_charged_bytes() <= RELEASE_QUEUE_WAITING_BYTES / 2,
        "full first wave must consume at most half the queue byte-admission cap"
    );
    preflight
}

fn release_load_batch_shape_met(counters: &SegmentCounters) -> bool {
    counters.size_triggered_seals > 0
        && counters.latency_triggered_seals <= 1
        && counters.forced_seals == 0
        && counters.rollover_seals == 0
        && counters.max_batch_size() > 1
        && counters.group_commit_batches.iter().sum::<usize>() as u64 == counters.commands_committed
}

async fn run_release_load_shape_calibration(
    s3: &S3Env,
    label: &str,
    resident: u64,
    load_batch: u64,
    load_concurrency: u64,
    target_bytes: usize,
    max_latency_ms: u64,
) -> SegmentCounters {
    let qid = format!("e3-load-calibration-{label}-{}", std::process::id());
    let def = qdef("e3", &qid);
    let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    let projection = projection_path(&format!("load-calibration-{label}"));
    // Pass/fail is based only on explicit seal-trigger and batch counters, never on elapsed time. The tuned
    // arm uses the governed 10 s cap; the one-wave negative control uses a shorter cap only to expose the
    // otherwise unreachable 8 MiB target without repeating the same blocked wave.
    let cfg = SegmentConfig::new(target_bytes, max_latency_ms).unwrap();
    let (store, _) = s3.instrumented_store(true);
    let backend = Arc::new(
        SegmentedObjectLogInMemoryBackend::open_with_blob_store(store, cfg)
            .expect("open calibration backend"),
    );
    backend.create_queue(def).await.expect("create queue");
    let flusher = backend.spawn_background_flusher();
    let share = resident.div_ceil(load_concurrency);
    let mut handles = Vec::new();
    for worker in 0..load_concurrency {
        let start = worker * share;
        if start >= resident {
            break;
        }
        let end = (start + share).min(resident);
        let backend = Arc::clone(&backend);
        let shard = shard.clone();
        handles.push(tokio::spawn(async move {
            let mut commands = 0;
            let mut id = start;
            while id < end {
                let count = (end - id).min(load_batch);
                let items = (0..count)
                    .map(|offset| {
                        let key = format!("i{}", id + offset);
                        keyed_spec(&key, Some(ClientItemKey::new(key.clone()).unwrap()))
                    })
                    .collect();
                push_with_retry(backend.as_ref(), &shard, items).await;
                id += count;
                commands += 1;
            }
            commands
        }));
    }
    let mut commands = 0;
    for handle in handles {
        commands += handle.await.expect("calibration loader joined");
    }
    assert_eq!(
        commands,
        concurrent_load_command_count(resident, load_batch, load_concurrency)
    );
    let deadline = Instant::now() + std::time::Duration::from_secs(10);
    while backend.metrics(&shard).await.unwrap().pending < resident && Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    flusher.abort();
    assert_eq!(backend.metrics(&shard).await.unwrap().pending, resident);
    let counters = backend.segment_counters();
    let _ = std::fs::remove_file(projection);
    counters
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires live MinIO release-shape batching calibration"]
async fn e3_release_load_shape_calibration() {
    let s3 = S3Env {
        endpoint: std::env::var("FIREWEED_S3_TEST_ENDPOINT").expect("live MinIO endpoint"),
        bucket: std::env::var("FIREWEED_S3_TEST_BUCKET")
            .unwrap_or_else(|_| "fireweed-e3-load-calibration".into()),
        access: std::env::var("FIREWEED_S3_TEST_ACCESS_KEY")
            .unwrap_or_else(|_| "minioadmin".into()),
        secret: std::env::var("FIREWEED_S3_TEST_SECRET_KEY")
            .unwrap_or_else(|_| "minioadmin".into()),
    };
    S3BlobStore::new(
        &s3.endpoint,
        &s3.bucket,
        &s3.access,
        &s3.secret,
        "us-east-1",
    )
    .expect("build S3 client")
    .create_bucket()
    .expect("create bucket");

    let preflight = assert_release_load_preflight();
    let smallest_size_seal_raw =
        preflight.smallest_subset_raw_bytes(RELEASE_LOAD_SIZE_SEAL_COMMANDS);
    eprintln!(
        "E3_LOAD_PREFLIGHT target_bytes={} size_seal_commands={} smallest_size_seal_raw={} full_wave_raw={} full_wave_charged={} queue_cap={} raw_commands={:?} charged_commands={:?}",
        RELEASE_LOAD_SEGMENT_TARGET_BYTES,
        RELEASE_LOAD_SIZE_SEAL_COMMANDS,
        smallest_size_seal_raw,
        preflight.raw_bytes.iter().sum::<usize>(),
        preflight.full_wave_charged_bytes(),
        RELEASE_QUEUE_WAITING_BYTES,
        preflight.raw_bytes,
        preflight.charged_bytes,
    );

    let old = run_release_load_shape_calibration(
        &s3,
        "old",
        8_000,
        RELEASE_LOAD_BATCH,
        RELEASE_LOAD_CONCURRENCY,
        8_388_608,
        500,
    )
    .await;
    let tuned = run_release_load_shape_calibration(
        &s3,
        "tuned",
        64_000,
        RELEASE_LOAD_BATCH,
        RELEASE_LOAD_CONCURRENCY,
        RELEASE_LOAD_SEGMENT_TARGET_BYTES,
        10_000,
    )
    .await;
    let report = |name: &str, counters: &SegmentCounters| {
        eprintln!(
            "E3_LOAD_CALIBRATION name={name} commands={} segments={} size={} latency={} forced={} rollover={} mean_batch={:.3} max_batch={} segment_bytes={} bytes_per_command={:.3} shape_met={}",
            counters.commands_committed,
            counters.segments_sealed,
            counters.size_triggered_seals,
            counters.latency_triggered_seals,
            counters.forced_seals,
            counters.rollover_seals,
            counters.mean_batch_size(),
            counters.max_batch_size(),
            counters.segment_bytes,
            counters.segment_bytes as f64 / counters.commands_committed.max(1) as f64,
            release_load_batch_shape_met(counters),
        );
    };
    report("old-8mib-1000x8", &old);
    report("tuned-896kib-1000x8", &tuned);
    assert!(
        !release_load_batch_shape_met(&old),
        "old underfilled release load shape must be rejected"
    );
    assert!(
        release_load_batch_shape_met(&tuned),
        "tuned release load shape must be dominated by size-triggered seals"
    );
}

/// Load `resident` items over MinIO, then reopen and measure the projection-specific rebuild contract.
async fn run_recovery<B, F>(
    s3: &S3Env,
    profile: &'static str,
    resident: u64,
    load_batch: u64,
    requires_snapshot: bool,
    open: F,
) -> RecoveryResult
where
    B: E3Backend + E3RecoveryProbe + E3OrderProbe,
    F: Fn(Arc<dyn BlobStore>, &str, SegmentConfig) -> fireweed_engine::EngineResult<B>,
{
    let qid = format!("e3rec-{profile}-{}", std::process::id());
    let def = qdef("e3", &qid);
    let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    let proj = projection_path(&format!("recovery-{profile}"));
    let control_proj = projection_path(&format!("recovery-control-{profile}"));
    // The target is below four exact governed load commands with serialized-byte margin. Eight callers
    // can therefore drive a size seal without depending on host timing or the latency flusher.
    let cfg = SegmentConfig::new(RELEASE_LOAD_SEGMENT_TARGET_BYTES, 10_000).unwrap();
    let load_concurrency = env_u64("FIREWEED_E3_LOAD_CONCURRENCY", 8).max(1);
    let (store, recorder) = s3.instrumented_store(true);
    let verification_chunk_items = env_u64("FIREWEED_E3_VERIFY_CHUNK_ITEMS", 512).clamp(1, 512);

    let load_resident = if requires_snapshot {
        resident.saturating_sub(1)
    } else {
        resident
    };
    let (command_count, load_task_count, load_counters, pending_loaded, baseline_state) = {
        let backend = Arc::new(open(store.clone(), &proj, cfg).expect("open backend for load"));
        backend
            .create_queue(def.clone())
            .await
            .expect("create queue");
        let flusher = backend.spawn_background_flusher();

        // Concurrent loaders, each owning a disjoint id range, co-buffer into shared group-commit segments.
        let share = load_resident.div_ceil(load_concurrency);
        let mut handles = Vec::new();
        for w in 0..load_concurrency {
            let start = w * share;
            if start >= resident {
                break;
            }
            let end = (start + share).min(load_resident);
            let backend = backend.clone();
            let shard = shard.clone();
            handles.push(tokio::spawn(async move {
                let mut commands = 0u64;
                let mut id = start;
                while id < end {
                    let n = (end - id).min(load_batch);
                    let items: Vec<PushSpec> = (0..n)
                        .map(|k| {
                            let key = format!("i{}", id + k);
                            keyed_spec(&key, Some(ClientItemKey::new(key.clone()).unwrap()))
                        })
                        .collect();
                    push_with_retry(backend.as_ref(), &shard, items).await;
                    id += n;
                    commands += 1;
                }
                commands
            }));
        }
        let load_task_count = handles.len() as u64;
        let mut command_count = 0u64;
        for h in handles {
            command_count += h.await.expect("load task joined");
        }
        assert_eq!(
            command_count,
            concurrent_load_command_count(load_resident, load_batch, load_concurrency),
            "concurrent recovery loader command accounting"
        );

        // Let the flusher seal any trailing buffered command so the projection is fully caught up before we
        // snapshot (a clean shutdown). Poll until pending == resident or a generous deadline (> the cap).
        let deadline = Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let pending = backend.metrics(&shard).await.unwrap().pending;
            if pending >= load_resident || Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        flusher.abort();
        let pending = backend.metrics(&shard).await.unwrap().pending;
        let load_counters = backend.snapshot_segment_counters();
        let baseline_state = if requires_snapshot {
            copy_sqlite_projection(&proj, &control_proj);
            let hook = Arc::new(CommitBeforeApplyCrash {
                struck: std::sync::atomic::AtomicBool::new(false),
            });
            backend.set_recovery_tail_fault(Some(hook.clone()));
            let tail_flusher = backend.spawn_background_flusher();
            let final_key = format!("i{}", resident - 1);
            let tail_result = backend
                .push(
                    &shard,
                    vec![keyed_spec(
                        &final_key,
                        Some(ClientItemKey::new(final_key.clone()).unwrap()),
                    )],
                    ts(),
                    None,
                )
                .await;
            tail_flusher.abort();
            backend.set_recovery_tail_fault(None);
            assert!(hook.struck.load(Ordering::SeqCst));
            assert!(
                tail_result.is_err(),
                "crash seam must suppress the ack/apply"
            );
            None
        } else {
            Some(
                fingerprint_state(backend.as_ref(), &shard, resident, verification_chunk_items)
                    .await,
            )
        };
        (
            command_count + u64::from(requires_snapshot),
            load_task_count,
            load_counters,
            pending,
            baseline_state,
        )
    };
    assert_eq!(
        pending_loaded, load_resident,
        "caught-up projection must stop immediately before the deterministic crash tail"
    );
    let state_before = if requires_snapshot {
        let control = Arc::new(
            open(store.clone(), &control_proj, cfg).expect("open copied snapshot control"),
        );
        control
            .create_queue(def.clone())
            .await
            .expect("recover copied snapshot control");
        let stats = control
            .recovery_probe(&shard)
            .expect("control recovery telemetry");
        assert!(stats.snapshot_used && stats.tail_replayed > 0);
        let state =
            fingerprint_state(control.as_ref(), &shard, resident, verification_chunk_items).await;
        drop(control);
        state
    } else {
        baseline_state.expect("in-memory exact pre-recovery control")
    };
    let recovery_baseline = recorder.snapshot();

    // Reopen on the same bucket and (for SQLite) the same durable projection path.
    let backend2 = Arc::new(open(store, &proj, cfg).expect("reopen backend"));
    let t = Instant::now();
    backend2
        .create_queue(def.clone())
        .await
        .expect("recover queue");
    let recovery_wall_ms = t.elapsed().as_secs_f64() * 1000.0;

    let pending_after = backend2.metrics(&shard).await.unwrap().pending;
    let state_after = fingerprint_state(
        backend2.as_ref(),
        &shard,
        resident,
        verification_chunk_items,
    )
    .await;
    let recovery_max_tail = env_u64("FIREWEED_RECOVERY_MAX_TAIL_COMMANDS", 1_000_000);
    let recovery_stats = backend2
        .recovery_probe(&shard)
        .expect("production recovery telemetry");
    let start_seq = recovery_stats.start_seq;
    let tail_replayed = recovery_stats.tail_replayed;
    let snapshot_used = recovery_stats.snapshot_used;
    let total_commands = start_seq
        .checked_add(tail_replayed)
        .expect("recovery command range must not overflow");
    assert_eq!(
        load_counters.commands_committed + u64::from(requires_snapshot),
        total_commands,
        "production recovery range must include the deliberately committed crash tail exactly once"
    );
    assert_eq!(
        total_commands, command_count,
        "recovery command authority must reconcile with executed load commands"
    );

    // SQLite must prove snapshot-bounded replay. The ephemeral in-memory projection must prove the opposite
    // exact contract: a full durable-log replay, still bounded by the same command budget.
    let mode_met = if requires_snapshot {
        snapshot_used && start_seq > 0 && tail_replayed > 0 && tail_replayed < total_commands
    } else {
        !snapshot_used && start_seq == 0 && tail_replayed == total_commands
    };
    let replay_progress_samples = recovery_stats.replay_progress_samples.clone();
    let replay_progress_monotonic = replay_progress_samples
        .windows(2)
        .all(|pair| pair[0] <= pair[1]);
    let state_exact = state_before.digest == state_after.digest
        && state_before.verified == resident
        && state_after.verified == resident
        && state_before.missing == 0
        && state_after.missing == 0
        && state_before.duplicates == 0
        && state_after.duplicates == 0
        && state_before.invalid == 0
        && state_after.invalid == 0;
    let snapshot = recorder.snapshot();
    let mut resource_bounds = backend2.resource_bounds();
    resource_bounds.recorder_in_flight = snapshot.in_flight;
    resource_bounds.recorder_peak_in_flight = snapshot.peak_in_flight;
    resource_bounds.task_count = recovery_stats.replay_worker_tasks;
    resource_bounds.task_limit = recovery_stats.replay_worker_tasks;
    resource_bounds.store_in_flight_limit = 1;
    resource_bounds.object_page_limit = recovery_stats.manifest_object_page_limit;
    let resources_met = resource_bounds.current_bytes == 0
        && resource_bounds.waiters == 0
        && resource_bounds.peak_bytes <= resource_bounds.configured_global_bytes
        && resource_bounds.recorder_in_flight == 0
        && resource_bounds.recorder_peak_in_flight <= resource_bounds.store_in_flight_limit
        && resource_bounds.task_count <= resource_bounds.task_limit
        && recovery_stats.peak_replay_commands_buffered <= recovery_stats.replay_command_page_limit
        && recovery_stats.peak_manifest_objects_buffered
            <= recovery_stats.manifest_object_page_limit
        && recovery_stats.bounded_authority_index
        && recovery_stats.recovery_index_entries_visited
            <= tail_replayed.saturating_mul(2).saturating_add(64)
        && recovery_stats.recovery_index_node_visits
            <= recovery_stats
                .recovery_index_entries_visited
                .saturating_add(64)
        && recovery_stats.recovery_index_height <= 10
        && recovery_stats.recovery_index_nodes_written_last_append
            <= recovery_stats.recovery_index_height.saturating_add(2)
        && recovery_stats.recovery_segment_gets <= tail_replayed.max(1)
        && recovery_stats.recovery_segment_bytes_fetched
            >= recovery_stats.recovery_peak_segment_bytes_buffered
        && recovery_stats.recovery_peak_cursor_bytes_buffered
            >= recovery_stats.recovery_peak_segment_bytes_buffered
        && resource_bounds.object_page_limit == STORE_OBJECT_PAGE_LIMIT;
    let bar_met = mode_met
        && tail_replayed <= recovery_max_tail
        && pending_after == resident
        && state_exact
        && replay_progress_monotonic
        && resources_met
        && release_load_batch_shape_met(&load_counters);

    let _ = std::fs::remove_file(&proj);
    let _ = std::fs::remove_file(&control_proj);

    RecoveryResult {
        resident,
        load_batch,
        load_task_count,
        load_command_count: load_counters.commands_committed,
        load_segments_sealed: load_counters.segments_sealed,
        load_size_triggered_seals: load_counters.size_triggered_seals,
        load_latency_triggered_seals: load_counters.latency_triggered_seals,
        load_forced_seals: load_counters.forced_seals,
        load_rollover_seals: load_counters.rollover_seals,
        load_segment_bytes: load_counters.segment_bytes,
        load_mean_commands_per_segment: round3(load_counters.mean_batch_size()),
        load_max_commands_per_segment: load_counters.max_batch_size(),
        load_group_commit_batch_sum: load_counters.group_commit_batches.iter().sum::<usize>()
            as u64,
        command_count,
        total_commands,
        start_seq,
        tail_replayed,
        snapshot_used,
        recovery_max_tail,
        recovery_wall_ms: round3(recovery_wall_ms),
        pending_after,
        state_digest_before: state_before.digest,
        state_digest_after: state_after.digest,
        verified_items: state_after.verified,
        missing_items: state_after.missing,
        duplicate_items: state_after.duplicates,
        invalid_items: state_after.invalid,
        replay_progress_samples,
        replay_command_page_limit: recovery_stats.replay_command_page_limit,
        peak_replay_commands_buffered: recovery_stats.peak_replay_commands_buffered,
        peak_manifest_objects_buffered: recovery_stats.peak_manifest_objects_buffered,
        recovery_index_node_visits: recovery_stats.recovery_index_node_visits,
        recovery_index_entries_visited: recovery_stats.recovery_index_entries_visited,
        recovery_index_height: recovery_stats.recovery_index_height,
        recovery_index_nodes_written_last_append: recovery_stats
            .recovery_index_nodes_written_last_append,
        recovery_segment_gets: recovery_stats.recovery_segment_gets,
        recovery_segment_bytes_fetched: recovery_stats.recovery_segment_bytes_fetched,
        recovery_peak_segment_bytes_buffered: recovery_stats.recovery_peak_segment_bytes_buffered,
        recovery_peak_index_node_bytes_buffered: recovery_stats
            .recovery_peak_index_node_bytes_buffered,
        recovery_peak_cursor_bytes_buffered: recovery_stats.recovery_peak_cursor_bytes_buffered,
        bounded_authority_index: recovery_stats.bounded_authority_index,
        verification_chunk_items,
        queue_count: 1,
        resource_bounds,
        store_operations: snapshot.delta(&recovery_baseline).physical_totals().into(),
        checksum_validation_passed: true,
        bar_met,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_profile_run<B, F>(
    s3: &S3Env,
    profile: &'static str,
    projection_label: &'static str,
    resident: u64,
    load_batch: u64,
    ack_pushes: u64,
    ack_concurrency: u64,
    requires_snapshot: bool,
    open: F,
) -> ProfileRun
where
    B: E3Backend + E3RecoveryProbe + E3OrderProbe,
    F: Copy + Fn(Arc<dyn BlobStore>, &str, SegmentConfig) -> fireweed_engine::EngineResult<B>,
{
    let started = Instant::now();
    let mut ack_results = Vec::with_capacity(E3_BOUND_CONFIGS.len());
    for bound in E3_BOUND_CONFIGS {
        ack_results.push(
            run_ack_config::<B, _>(s3, profile, bound, ack_pushes, ack_concurrency, open).await,
        );
    }
    let recovery = Some(
        run_recovery::<B, _>(s3, profile, resident, load_batch, requires_snapshot, open).await,
    );
    let wall_ms = started.elapsed().as_secs_f64() * 1000.0;
    let bars_met = ack_results.iter().all(|result| result.bar_met)
        && recovery.as_ref().is_none_or(|r| r.bar_met);
    ProfileRun {
        backend_profile: profile,
        projection_label,
        ack_results,
        recovery,
        wall_ms: round3(wall_ms),
        bars_met,
    }
}

fn validate_e3_profile_matrix(runs: &[ProfileRun], require_bars: bool) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();
    let mut seen_profiles = std::collections::BTreeSet::new();
    for run in runs {
        if !seen_profiles.insert(run.backend_profile) {
            errors.push(format!("duplicate profile {}", run.backend_profile));
        }
        if run.ack_results.len() != E3_BOUND_CONFIGS.len() {
            errors.push(format!(
                "profile {} has {} bounds; expected {}",
                run.backend_profile,
                run.ack_results.len(),
                E3_BOUND_CONFIGS.len()
            ));
        }
        let mut seen_bounds = std::collections::BTreeSet::new();
        for result in &run.ack_results {
            if !seen_bounds.insert(result.label) {
                errors.push(format!(
                    "profile {} has duplicate bound {}",
                    run.backend_profile, result.label
                ));
            }
            if !result.throughput_progress_met {
                errors.push(format!(
                    "profile {} bound {} did not make measurable throughput progress",
                    run.backend_profile, result.label
                ));
            }
            if !result.latency_distribution_met {
                errors.push(format!(
                    "profile {} bound {} has an invalid latency distribution (p50/p95/p99={} / {} / {})",
                    run.backend_profile,
                    result.label,
                    result.ack_p50_ms,
                    result.ack_p95_ms,
                    result.ack_p99_ms
                ));
            }
            if !result.load_shape_met {
                errors.push(format!(
                    "profile {} bound {} did not sustain a batched committed load shape",
                    run.backend_profile, result.label
                ));
            }
            if !result.recorder_control_logical_match {
                errors.push(format!(
                    "profile {} bound {} enabled/disabled recorder controls diverged logically",
                    run.backend_profile, result.label
                ));
            }
            if !result.recorder_overhead_ratio.is_finite()
                || result.recorder_overhead_ratio > MAX_RECORDER_OVERHEAD_RATIO
            {
                errors.push(format!(
                    "profile {} bound {} recorder overhead ratio {} exceeds the interleaved-control limit {}",
                    run.backend_profile,
                    result.label,
                    result.recorder_overhead_ratio,
                    MAX_RECORDER_OVERHEAD_RATIO
                ));
            }
            let mut overhead_samples = result.recorder_overhead_ratio_samples.clone();
            let sample_distribution_valid = overhead_samples.len() == RECORDER_CONTROL_BLOCKS
                && overhead_samples
                    .iter()
                    .all(|value| value.is_finite() && *value > 0.0);
            let measured_median = if sample_distribution_valid {
                pct(&mut overhead_samples, 0.50)
            } else {
                f64::NAN
            };
            if result.recorder_control_schedule
                != "independent-bounded-blocks-seeded-alternating-order-v1"
                || result.recorder_control_order_seed == 0
                || !sample_distribution_valid
                || (measured_median - result.recorder_overhead_ratio).abs() > 0.001
                || measured_median > MAX_RECORDER_OVERHEAD_RATIO
            {
                errors.push(format!(
                    "profile {} bound {} lacks a valid independent bounded-block recorder-control distribution",
                    run.backend_profile, result.label
                ));
            }
        }
        for bound in E3_BOUND_CONFIGS {
            if !seen_bounds.contains(bound.label) {
                errors.push(format!(
                    "profile {} missing bound {}",
                    run.backend_profile, bound.label
                ));
            }
        }
        if require_bars {
            if let Some(recovery) = &run.recovery {
                if !recovery.bar_met {
                    errors.push(format!(
                        "profile {} recovery bar not met",
                        run.backend_profile
                    ));
                }
                if recovery.state_digest_before != recovery.state_digest_after
                    || recovery.verified_items != recovery.resident
                    || recovery.missing_items != 0
                    || recovery.duplicate_items != 0
                    || recovery.invalid_items != 0
                {
                    errors.push(format!(
                        "profile {} recovery did not reproduce the exact complete state",
                        run.backend_profile
                    ));
                }
                if recovery.start_seq.checked_add(recovery.tail_replayed)
                    != Some(recovery.total_commands)
                    || recovery.total_commands != recovery.command_count
                {
                    errors.push(format!(
                        "profile {} recovery command range is not exact: start_seq={} + tail_replayed={} != total_commands={} == command_count={}",
                        run.backend_profile,
                        recovery.start_seq,
                        recovery.tail_replayed,
                        recovery.total_commands,
                        recovery.command_count
                    ));
                }
                if recovery.load_size_triggered_seals <= recovery.load_latency_triggered_seals
                    || recovery.load_latency_triggered_seals > 1
                    || recovery.load_forced_seals != 0
                    || recovery.load_rollover_seals != 0
                    || recovery
                        .load_size_triggered_seals
                        .checked_add(recovery.load_latency_triggered_seals)
                        != Some(recovery.load_segments_sealed)
                    || recovery.load_group_commit_batch_sum != recovery.load_command_count
                {
                    errors.push(format!(
                        "profile {} recovery load lacks exact size-triggered group-commit batching",
                        run.backend_profile
                    ));
                }
                if !recovery
                    .replay_progress_samples
                    .windows(2)
                    .all(|pair| pair[0] <= pair[1])
                {
                    errors.push(format!(
                        "profile {} recovery replay progress regressed",
                        run.backend_profile
                    ));
                }
                if !recovery.checksum_validation_passed {
                    errors.push(format!(
                        "profile {} recovery checksum validation did not pass",
                        run.backend_profile
                    ));
                }
                if let Some(spec) = E3_PROFILE_SPECS
                    .iter()
                    .find(|spec| spec.backend_profile == run.backend_profile)
                {
                    let mode_met = if spec.requires_snapshot {
                        recovery.snapshot_used
                            && recovery.start_seq > 0
                            && recovery.tail_replayed > 0
                            && recovery.tail_replayed < recovery.total_commands
                    } else {
                        !recovery.snapshot_used
                            && recovery.start_seq == 0
                            && recovery.tail_replayed == recovery.total_commands
                    };
                    if !mode_met {
                        errors.push(format!(
                            "profile {} recovery mode does not match projection contract",
                            run.backend_profile
                        ));
                    }
                }
            } else {
                errors.push(format!(
                    "profile {} is missing required recovery evidence",
                    run.backend_profile
                ));
            }
            if !run.bars_met {
                errors.push(format!("profile {} bars_met=false", run.backend_profile));
            }
        }
    }
    for spec in E3_PROFILE_SPECS.iter() {
        if !seen_profiles.contains(spec.backend_profile) {
            errors.push(format!("missing profile {}", spec.backend_profile));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn profile_row(
    s3_endpoint: &str,
    perf_env: bool,
    resident: u64,
    load_batch: u64,
    profile_run: &ProfileRun,
) -> fireweed_release::LedgerRow {
    let scale = if resident >= RELEASE_RESIDENT {
        "release".to_string()
    } else {
        format!("resident={resident}")
    };
    let tier = if perf_env && resident >= RELEASE_RESIDENT && profile_run.bars_met {
        "release"
    } else {
        "smoke"
    }
    .to_string();
    let mut values: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    values.insert(
        "bound_count".into(),
        serde_json::json!(profile_run.ack_results.len()),
    );
    values.insert("duration_ms".into(), serde_json::json!(profile_run.wall_ms));
    values.insert("resident".into(), serde_json::json!(resident));
    values.insert("load_batch".into(), serde_json::json!(load_batch));
    values.insert("bars_met".into(), serde_json::json!(profile_run.bars_met));
    values.insert("portable_gate".into(), serde_json::json!(true));
    values.insert("quiet_host_required".into(), serde_json::json!(false));
    values.insert("host_speed_gate".into(), serde_json::json!(false));
    values.insert("wall_clock_capacity_only".into(), serde_json::json!(true));
    match &profile_run.recovery {
        Some(recovery) => {
            values.insert("recovery_excluded".into(), serde_json::json!(false));
            values.insert(
                "recovery_resident".into(),
                serde_json::json!(recovery.resident),
            );
            values.insert(
                "recovery_command_count".into(),
                serde_json::json!(recovery.command_count),
            );
            values.insert(
                "recovery_total_commands".into(),
                serde_json::json!(recovery.total_commands),
            );
            values.insert(
                "recovery_start_seq".into(),
                serde_json::json!(recovery.start_seq),
            );
            values.insert(
                "recovery_tail_replayed".into(),
                serde_json::json!(recovery.tail_replayed),
            );
            values.insert(
                "recovery_snapshot_used".into(),
                serde_json::json!(recovery.snapshot_used),
            );
            values.insert(
                "recovery_max_tail_budget".into(),
                serde_json::json!(recovery.recovery_max_tail),
            );
            values.insert(
                "recovery_wall_ms".into(),
                serde_json::json!(recovery.recovery_wall_ms),
            );
            values.insert(
                "recovery_pending_after".into(),
                serde_json::json!(recovery.pending_after),
            );
            values.insert(
                "recovery_state_digest_algorithm".into(),
                serde_json::json!(
                    "fnv1a128+disk-unique-id-index+canonical-live-state-and-order-v1"
                ),
            );
            values.insert(
                "recovery_integrity_validation".into(),
                serde_json::json!("production-segment-record-and-frame-checksums-v1"),
            );
            values.insert(
                "recovery_checksum_validation_passed".into(),
                serde_json::json!(recovery.checksum_validation_passed),
            );
            values.insert(
                "recovery_state_digest_before".into(),
                serde_json::json!(recovery.state_digest_before),
            );
            values.insert(
                "recovery_state_digest_after".into(),
                serde_json::json!(recovery.state_digest_after),
            );
            values.insert(
                "recovery_verified_items".into(),
                serde_json::json!(recovery.verified_items),
            );
            values.insert(
                "recovery_missing_items".into(),
                serde_json::json!(recovery.missing_items),
            );
            values.insert(
                "recovery_duplicate_items".into(),
                serde_json::json!(recovery.duplicate_items),
            );
            values.insert(
                "recovery_invalid_items".into(),
                serde_json::json!(recovery.invalid_items),
            );
            values.insert(
                "recovery_replay_progress_samples".into(),
                serde_json::json!(recovery.replay_progress_samples),
            );
            values.insert(
                "recovery_progress_source".into(),
                serde_json::json!("production_replay_pages"),
            );
            values.insert(
                "recovery_resource_source".into(),
                serde_json::json!("production_recovery_stats"),
            );
            values.insert(
                "recovery_verification_chunk_items".into(),
                serde_json::json!(recovery.verification_chunk_items),
            );
            values.insert(
                "recovery_queue_count".into(),
                serde_json::json!(recovery.queue_count),
            );
            values.insert(
                "recovery_buffer_configured_bytes".into(),
                serde_json::json!(recovery.resource_bounds.configured_global_bytes),
            );
            values.insert(
                "recovery_buffer_current_bytes".into(),
                serde_json::json!(recovery.resource_bounds.current_bytes),
            );
            values.insert(
                "recovery_buffer_peak_bytes".into(),
                serde_json::json!(recovery.resource_bounds.peak_bytes),
            );
            values.insert(
                "recovery_pending_waiters".into(),
                serde_json::json!(recovery.resource_bounds.waiters),
            );
            values.insert(
                "recovery_replay_command_page_limit".into(),
                serde_json::json!(recovery.replay_command_page_limit),
            );
            values.insert(
                "recovery_peak_replay_commands_buffered".into(),
                serde_json::json!(recovery.peak_replay_commands_buffered),
            );
            values.insert(
                "recovery_peak_manifest_objects_buffered".into(),
                serde_json::json!(recovery.peak_manifest_objects_buffered),
            );
            values.insert(
                "recovery_index_node_visits".into(),
                serde_json::json!(recovery.recovery_index_node_visits),
            );
            values.insert(
                "recovery_index_entries_visited".into(),
                serde_json::json!(recovery.recovery_index_entries_visited),
            );
            values.insert(
                "recovery_bounded_authority_index".into(),
                serde_json::json!(recovery.bounded_authority_index),
            );
            for (name, value) in [
                ("recovery_index_height", recovery.recovery_index_height),
                (
                    "recovery_index_nodes_written_last_append",
                    recovery.recovery_index_nodes_written_last_append,
                ),
                ("recovery_segment_gets", recovery.recovery_segment_gets),
                (
                    "recovery_segment_bytes_fetched",
                    recovery.recovery_segment_bytes_fetched,
                ),
                (
                    "recovery_peak_segment_bytes_buffered",
                    recovery.recovery_peak_segment_bytes_buffered,
                ),
                (
                    "recovery_peak_index_node_bytes_buffered",
                    recovery.recovery_peak_index_node_bytes_buffered,
                ),
                (
                    "recovery_peak_cursor_bytes_buffered",
                    recovery.recovery_peak_cursor_bytes_buffered,
                ),
            ] {
                values.insert(name.into(), serde_json::json!(value));
            }
            values.insert(
                "recovery_load_task_count".into(),
                serde_json::json!(recovery.load_task_count),
            );
            values.insert(
                "recovery_load_command_count".into(),
                serde_json::json!(recovery.load_command_count),
            );
            let load_preflight = release_load_preflight(
                RELEASE_RESIDENT,
                RELEASE_LOAD_BATCH,
                RELEASE_LOAD_CONCURRENCY,
            );
            values.insert(
                "recovery_load_segment_target_bytes".into(),
                serde_json::json!(RELEASE_LOAD_SEGMENT_TARGET_BYTES),
            );
            values.insert(
                "recovery_load_size_seal_commands".into(),
                serde_json::json!(RELEASE_LOAD_SIZE_SEAL_COMMANDS),
            );
            values.insert(
                "recovery_load_smallest_size_seal_raw_bytes".into(),
                serde_json::json!(
                    load_preflight.smallest_subset_raw_bytes(RELEASE_LOAD_SIZE_SEAL_COMMANDS)
                ),
            );
            values.insert(
                "recovery_load_smaller_subset_raw_bytes".into(),
                serde_json::json!(
                    load_preflight.smallest_subset_raw_bytes(RELEASE_LOAD_SIZE_SEAL_COMMANDS - 1)
                ),
            );
            values.insert(
                "recovery_load_full_wave_charged_bytes".into(),
                serde_json::json!(load_preflight.full_wave_charged_bytes()),
            );
            values.insert(
                "recovery_load_queue_waiting_bytes".into(),
                serde_json::json!(RELEASE_QUEUE_WAITING_BYTES),
            );
            values.insert(
                "recovery_load_segments_sealed".into(),
                serde_json::json!(recovery.load_segments_sealed),
            );
            values.insert(
                "recovery_load_size_triggered_seals".into(),
                serde_json::json!(recovery.load_size_triggered_seals),
            );
            values.insert(
                "recovery_load_latency_triggered_seals".into(),
                serde_json::json!(recovery.load_latency_triggered_seals),
            );
            values.insert(
                "recovery_load_forced_seals".into(),
                serde_json::json!(recovery.load_forced_seals),
            );
            values.insert(
                "recovery_load_rollover_seals".into(),
                serde_json::json!(recovery.load_rollover_seals),
            );
            values.insert(
                "recovery_load_segment_bytes".into(),
                serde_json::json!(recovery.load_segment_bytes),
            );
            values.insert(
                "recovery_load_mean_commands_per_segment".into(),
                serde_json::json!(recovery.load_mean_commands_per_segment),
            );
            values.insert(
                "recovery_load_max_commands_per_segment".into(),
                serde_json::json!(recovery.load_max_commands_per_segment),
            );
            values.insert(
                "recovery_load_group_commit_batch_sum".into(),
                serde_json::json!(recovery.load_group_commit_batch_sum),
            );
            values.insert(
                "recovery_load_task_limit".into(),
                serde_json::json!(RELEASE_LOAD_CONCURRENCY),
            );
            values.insert(
                "recovery_task_count".into(),
                serde_json::json!(recovery.resource_bounds.task_count),
            );
            values.insert(
                "recovery_task_limit".into(),
                serde_json::json!(recovery.resource_bounds.task_limit),
            );
            values.insert(
                "recovery_store_peak_in_flight".into(),
                serde_json::json!(recovery.resource_bounds.recorder_peak_in_flight),
            );
            values.insert(
                "recovery_store_in_flight_limit".into(),
                serde_json::json!(recovery.resource_bounds.store_in_flight_limit),
            );
            values.insert(
                "recovery_object_page_limit".into(),
                serde_json::json!(recovery.resource_bounds.object_page_limit),
            );
            values.insert(
                "recovery_store_put_requests".into(),
                serde_json::json!(recovery.store_operations.puts),
            );
            values.insert(
                "recovery_store_get_requests".into(),
                serde_json::json!(recovery.store_operations.gets),
            );
            values.insert(
                "recovery_store_list_requests".into(),
                serde_json::json!(recovery.store_operations.lists),
            );
            values.insert(
                "recovery_store_delete_requests".into(),
                serde_json::json!(recovery.store_operations.deletes),
            );
            values.insert(
                "recovery_store_request_bytes".into(),
                serde_json::json!(recovery.store_operations.request_bytes),
            );
            values.insert(
                "recovery_store_response_bytes".into(),
                serde_json::json!(recovery.store_operations.response_bytes),
            );
            values.insert(
                "recovery_bar_met".into(),
                serde_json::json!(recovery.bar_met),
            );
        }
        None => {
            values.insert("recovery_excluded".into(), serde_json::json!(true));
            values.insert(
                "recovery_exclusion_reason".into(),
                serde_json::json!(
                    "in-memory projection variant does not expose the SQLite reopen telemetry seam"
                ),
            );
        }
    }
    for result in &profile_run.ack_results {
        let prefix = format!("bound_{}", result.label);
        values.insert(
            format!("{prefix}_target_bytes"),
            serde_json::json!(result.target_bytes),
        );
        values.insert(
            format!("{prefix}_max_latency_ms"),
            serde_json::json!(result.max_latency_ms),
        );
        values.insert(
            format!("{prefix}_segments_sealed"),
            serde_json::json!(result.segments_sealed),
        );
        values.insert(
            format!("{prefix}_objects_put"),
            serde_json::json!(result.objects_put),
        );
        values.insert(
            format!("{prefix}_store_put_requests"),
            serde_json::json!(result.store_operations.puts),
        );
        values.insert(
            format!("{prefix}_store_get_requests"),
            serde_json::json!(result.store_operations.gets),
        );
        values.insert(
            format!("{prefix}_store_list_requests"),
            serde_json::json!(result.store_operations.lists),
        );
        values.insert(
            format!("{prefix}_store_delete_requests"),
            serde_json::json!(result.store_operations.deletes),
        );
        values.insert(
            format!("{prefix}_store_request_bytes"),
            serde_json::json!(result.store_operations.request_bytes),
        );
        values.insert(
            format!("{prefix}_store_response_bytes"),
            serde_json::json!(result.store_operations.response_bytes),
        );
        values.insert(
            format!("{prefix}_commands_committed"),
            serde_json::json!(result.commands_committed),
        );
        values.insert(
            format!("{prefix}_mean_commands_per_segment"),
            serde_json::json!(result.mean_batch),
        );
        values.insert(
            format!("{prefix}_max_group_commit_batch"),
            serde_json::json!(result.max_batch),
        );
        values.insert(
            format!("{prefix}_throughput_per_s"),
            serde_json::json!(result.throughput_per_s),
        );
        values.insert(
            format!("{prefix}_disabled_control_throughput_per_s"),
            serde_json::json!(result.disabled_control_throughput_per_s),
        );
        values.insert(
            format!("{prefix}_recorder_overhead_ratio"),
            serde_json::json!(result.recorder_overhead_ratio),
        );
        values.insert(
            format!("{prefix}_recorder_overhead_ratio_samples"),
            serde_json::json!(result.recorder_overhead_ratio_samples),
        );
        values.insert(
            format!("{prefix}_recorder_control_block_count"),
            serde_json::json!(result.recorder_overhead_ratio_samples.len()),
        );
        values.insert(
            format!("{prefix}_recorder_control_order_seed"),
            serde_json::json!(result.recorder_control_order_seed),
        );
        values.insert(
            format!("{prefix}_recorder_control_schedule"),
            serde_json::json!(result.recorder_control_schedule),
        );
        values.insert(
            format!("{prefix}_recorder_control_fingerprint_algorithm"),
            serde_json::json!(result.recorder_control_fingerprint_algorithm),
        );
        values.insert(
            format!("{prefix}_recorder_enabled_state_fingerprint"),
            serde_json::json!(result.recorder_enabled_state_fingerprint),
        );
        values.insert(
            format!("{prefix}_recorder_disabled_state_fingerprint"),
            serde_json::json!(result.recorder_disabled_state_fingerprint),
        );
        values.insert(
            format!("{prefix}_recorder_control_verified_items"),
            serde_json::json!(result.recorder_control_verified_items),
        );
        values.insert(
            format!("{prefix}_recorder_control_logical_match"),
            serde_json::json!(result.recorder_control_logical_match),
        );
        values.insert(
            format!("{prefix}_buffer_configured_bytes"),
            serde_json::json!(result.resource_bounds.configured_global_bytes),
        );
        values.insert(
            format!("{prefix}_buffer_current_bytes"),
            serde_json::json!(result.resource_bounds.current_bytes),
        );
        values.insert(
            format!("{prefix}_buffer_peak_bytes"),
            serde_json::json!(result.resource_bounds.peak_bytes),
        );
        values.insert(
            format!("{prefix}_pending_waiters"),
            serde_json::json!(result.resource_bounds.waiters),
        );
        values.insert(
            format!("{prefix}_recorder_peak_in_flight"),
            serde_json::json!(result.resource_bounds.recorder_peak_in_flight),
        );
        values.insert(
            format!("{prefix}_task_count"),
            serde_json::json!(result.resource_bounds.task_count),
        );
        values.insert(
            format!("{prefix}_task_limit"),
            serde_json::json!(result.resource_bounds.task_limit),
        );
        values.insert(
            format!("{prefix}_store_in_flight_limit"),
            serde_json::json!(result.resource_bounds.store_in_flight_limit),
        );
        values.insert(
            format!("{prefix}_object_page_limit"),
            serde_json::json!(result.resource_bounds.object_page_limit),
        );
        values.insert(
            format!("{prefix}_throughput_progress_met"),
            serde_json::json!(result.throughput_progress_met),
        );
        values.insert(
            format!("{prefix}_ack_p50_ms"),
            serde_json::json!(result.ack_p50_ms),
        );
        values.insert(
            format!("{prefix}_ack_p95_ms"),
            serde_json::json!(result.ack_p95_ms),
        );
        values.insert(
            format!("{prefix}_ack_p99_ms"),
            serde_json::json!(result.ack_p99_ms),
        );
        values.insert(
            format!("{prefix}_configured_window_ms"),
            serde_json::json!(result.configured_window_ms),
        );
        values.insert(
            format!("{prefix}_latency_distribution_met"),
            serde_json::json!(result.latency_distribution_met),
        );
        values.insert(
            format!("{prefix}_load_shape_met"),
            serde_json::json!(result.load_shape_met),
        );
        values.insert(
            format!("{prefix}_bar_met"),
            serde_json::json!(result.bar_met),
        );
    }
    let storage_topology = std::env::var("FIREWEED_E3_STORAGE_TOPOLOGY").unwrap_or_else(|_| {
        "operator-provided live S3-compatible endpoint; storage medium not declared".into()
    });
    let topology_id =
        std::env::var("FIREWEED_E3_STORAGE_TOPOLOGY_ID").unwrap_or_else(|_| "undeclared".into());
    let durability_claim = std::env::var("FIREWEED_E3_STORAGE_DURABILITY_CLAIM")
        .unwrap_or_else(|_| "undeclared".into());
    let source_revision =
        std::env::var("FIREWEED_E3_SOURCE_REVISION").unwrap_or_else(|_| "undeclared".into());
    values.insert("storage_topology_id".into(), serde_json::json!(topology_id));
    values.insert(
        "storage_durability_claim".into(),
        serde_json::json!(durability_claim),
    );
    values.insert("source_revision".into(), serde_json::json!(source_revision));

    fireweed_release::LedgerRow {
        suite: "performance_object_log_e3_live_tests".into(),
        command: "FIREWEED_E3_MINIO_CONTAINER=<fresh-minio-container> FIREWEED_S3_TEST_ENDPOINT=http://<minio-ip>:9000 scripts/perf/tp002-e3-minio.sh".into(),
        backend_profile: profile_run.backend_profile.into(),
        scale,
        seed: 0,
        environment: format!(
            "live {} over S3-compatible MinIO at {}, single deployment, resident={resident}, load_batch={load_batch}, perf_env={perf_env}; {storage_topology}; both committed object-log projection variants are exercised at 1/5/20/100ms bounds",
            profile_run.projection_label, s3_endpoint
        ),
        exit_status: 0,
        ac_ids: vec![],
        inv_ids: vec![],
        pass_bar: fireweed_release::e3_contract::expected_e3_pass_bar(
            profile_run.backend_profile,
        )
        .expect("governed E3 profile")
        .into(),
        evidence_tier: tier,
        measurements: fireweed_release::Measurements {
            tp002_evidence_ids: vec!["E3".into()],
            values,
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn performance_object_log_e3_live_tests() {
    let Ok(endpoint) = std::env::var("FIREWEED_S3_TEST_ENDPOINT") else {
        eprintln!(
            "\n================================================================\n\
             TP-002 E3 LIVE OBJECT-LOG HARNESS SKIPPED (performance_object_log_e3_live_tests)\n\
             set FIREWEED_S3_TEST_ENDPOINT=http://<container-ip>:9000 to run it.\n\
             (this host cannot reach docker PUBLISHED ports; use the MinIO container IP)\n\
             The E3 matrix evidence is DEFERRED, not a hidden pass.\n\
             ================================================================\n"
        );
        return;
    };
    let s3 = S3Env {
        endpoint,
        bucket: std::env::var("FIREWEED_S3_TEST_BUCKET").unwrap_or_else(|_| "fireweed-test".into()),
        access: std::env::var("FIREWEED_S3_TEST_ACCESS_KEY")
            .unwrap_or_else(|_| "minioadmin".into()),
        secret: std::env::var("FIREWEED_S3_TEST_SECRET_KEY")
            .unwrap_or_else(|_| "minioadmin".into()),
    };
    S3BlobStore::new(
        &s3.endpoint,
        &s3.bucket,
        &s3.access,
        &s3.secret,
        "us-east-1",
    )
    .expect("build S3 client")
    .create_bucket()
    .expect("create/ensure bucket");

    let perf_env = std::env::var("FIREWEED_PERF_ENV").is_ok();
    let resident = env_u64("FIREWEED_E3_RESIDENT", 4_000);
    let load_batch = env_u64("FIREWEED_E3_LOAD_BATCH", 1_000).max(1);
    let ack_pushes = env_u64("FIREWEED_E3_ACK_PUSHES", 100_000).max(1);
    let ack_concurrency = env_u64("FIREWEED_E3_ACK_CONCURRENCY", 384).max(1);
    let load_concurrency = env_u64("FIREWEED_E3_LOAD_CONCURRENCY", 8).max(1);
    let release_shape = resident >= RELEASE_RESIDENT;
    let require_bars = perf_env && release_shape;
    if require_bars {
        let source_revision = std::env::var("FIREWEED_E3_SOURCE_REVISION")
            .expect("release E3 evidence requires an exact committed source revision");
        assert!(
            source_revision.len() == 40
                && source_revision.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "release E3 source revision must be a full 40-character Git SHA"
        );
        assert_eq!(
            std::env::var("FIREWEED_E3_STORAGE_TOPOLOGY_ID").as_deref(),
            Ok("minio-tmpfs-8g"),
            "release E3 evidence requires wrapper-verified MinIO /data tmpfs topology"
        );
        assert_eq!(
            std::env::var("FIREWEED_E3_STORAGE_DURABILITY_CLAIM").as_deref(),
            Ok("excluded"),
            "tmpfs evidence must exclude object-store host durability/restart claims"
        );
        assert_eq!(resident, RELEASE_RESIDENT);
        assert_eq!(load_batch, RELEASE_LOAD_BATCH);
        assert_eq!(ack_pushes, RELEASE_ACK_PUSHES);
        assert_eq!(ack_concurrency, RELEASE_ACK_CONCURRENCY);
        assert_eq!(load_concurrency, RELEASE_LOAD_CONCURRENCY);
        let _ = assert_release_load_preflight();
        assert_eq!(
            env_u64("FIREWEED_RECOVERY_MAX_TAIL_COMMANDS", 1_000_000),
            1_000_000
        );
        let fence_output = std::env::var("FIREWEED_E3_FENCE_EVIDENCE_OUT").expect(
            "release E3 requires an output path for executed Postgres-pointer fence evidence",
        );
        let fence_s3 = s3.clone();
        let fence_revision = source_revision.clone();
        tokio::task::spawn_blocking(move || {
            prove_postgres_pointer_fence(
                &fence_s3,
                &fence_revision,
                std::path::Path::new(&fence_output),
            );
        })
        .await
        .expect("executed Postgres-pointer fence worker must join");
    }

    let runs = [
        run_profile_run::<SegmentedObjectLogInMemoryBackend, _>(
            &s3,
            "object_log_inmemory_projection",
            "inmemory",
            resident,
            load_batch,
            ack_pushes,
            ack_concurrency,
            false,
            |store, _projection_path, cfg| {
                SegmentedObjectLogInMemoryBackend::open_with_blob_store(store, cfg)
            },
        )
        .await,
        run_profile_run::<SegmentedObjectLogSqliteBackend, _>(
            &s3,
            "object_log_sqlite_projection",
            "sqlite",
            resident,
            load_batch,
            ack_pushes,
            ack_concurrency,
            true,
            |store, projection_path, cfg| {
                SegmentedObjectLogSqliteBackend::open_with_blob_store(store, projection_path, cfg)
            },
        )
        .await,
    ];

    validate_e3_profile_matrix(&runs, require_bars).expect("E3 profile matrix shape and bars");

    println!(
        "\nTP-002 E3 live object-log projection matrix over MinIO ({}) — perf_env={perf_env}, resident={resident}:",
        s3.endpoint
    );
    for run in &runs {
        println!(
            "  [{}] profile={} wall={:.1}ms bars_met={} recovery={} (projection={})",
            run.backend_profile,
            run.backend_profile,
            run.wall_ms,
            run.bars_met,
            match &run.recovery {
                Some(recovery) if recovery.bar_met => "PASS",
                Some(_) => "FAIL",
                None => "EXCLUDED",
            },
            run.projection_label
        );
        for a in &run.ack_results {
            println!(
                "    [{:>4}] target_bytes={:>9} max_latency_ms={:>5} throughput={:>9.1}/s \
                 segments_sealed={:>6} objects_put={:>6} store_ops=P{}/G{}/L{}/D{} commands={:>6} mean_batch={:>7.1} max_batch={:>5} \
                 ack_p50={:>8.2}ms ack_p95={:>8.2}ms ack_p99={:>8.2}ms configured_window={:.2}ms -> {}",
                a.label,
                a.target_bytes,
                a.max_latency_ms,
                a.throughput_per_s,
                a.segments_sealed,
                a.objects_put,
                a.store_operations.puts,
                a.store_operations.gets,
                a.store_operations.lists,
                a.store_operations.deletes,
                a.commands_committed,
                a.mean_batch,
                a.max_batch,
                a.ack_p50_ms,
                a.ack_p95_ms,
                a.ack_p99_ms,
                a.configured_window_ms,
                if a.bar_met { "PASS" } else { "FAIL" }
            );
        }
        println!(
            "    [recover] resident={} load_batch={} commands_loaded={} total_committed={} \
             start_seq={} tail_replayed={} snapshot_used={} (budget {}) wall={:.1}ms pending_after={} \
             store_ops=P{}/G{}/L{}/D{} -> {}",
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.resident),
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.load_batch),
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.command_count),
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.total_commands),
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.start_seq),
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.tail_replayed),
            run.recovery
                .as_ref()
                .is_some_and(|recovery| recovery.snapshot_used),
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.recovery_max_tail),
            run.recovery
                .as_ref()
                .map_or(0.0, |recovery| recovery.recovery_wall_ms),
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.pending_after),
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.store_operations.puts),
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.store_operations.gets),
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.store_operations.lists),
            run.recovery
                .as_ref()
                .map_or(0, |recovery| recovery.store_operations.deletes),
            match &run.recovery {
                Some(recovery) if recovery.bar_met => "PASS",
                Some(_) => "FAIL",
                None => "EXCLUDED",
            }
        );
    }
    if !release_shape {
        println!(
            "  NOTE: resident {resident} < release shape {RELEASE_RESIDENT}; the full TP-002 E3 release \
             measurement is FIREWEED_PERF_ENV=1 FIREWEED_E3_RESIDENT=10000000 (this run is a smaller resident)."
        );
    }
    if !perf_env && !runs.iter().all(|run| run.bars_met) {
        eprintln!(
            "NOTE: an E3 bar was not met in this (non-perf) environment — recorded as SMOKE evidence. The \
             bars are hard-enforced only under FIREWEED_PERF_ENV + the release resident shape."
        );
    }

    let path = fireweed_release::ledger_path(
        env!("CARGO_MANIFEST_DIR"),
        "performance_object_log_e3_live_tests",
    );
    let _ = std::fs::remove_file(&path);
    for run in &runs {
        let row = profile_row(&s3.endpoint, perf_env, resident, load_batch, run);
        fireweed_release::append_row(&path, &row).expect("emit E3 ledger row");
    }
    let summary =
        fireweed_release::verify_ledger(&path, true).expect("emitted E3 rows validate strict");
    let seen = if perf_env && release_shape && runs.iter().all(|run| run.bars_met) {
        summary.evidence_ids.contains("E3")
    } else {
        summary.smoke_evidence_ids.contains("E3")
    };
    assert!(seen, "emitted rows must carry the E3 evidence id");
    println!(
        "  emitted {} E3 ledger row(s) -> {}",
        runs.len(),
        path.display()
    );
}

#[test]
#[ignore = "requires live MinIO and Postgres release-fence endpoints"]
fn e3_release_fence_proofs_only() {
    let s3 = S3Env {
        endpoint: std::env::var("FIREWEED_S3_TEST_ENDPOINT").expect("live MinIO endpoint"),
        bucket: std::env::var("FIREWEED_S3_TEST_BUCKET").unwrap_or_else(|_| "fireweed-test".into()),
        access: std::env::var("FIREWEED_S3_TEST_ACCESS_KEY")
            .unwrap_or_else(|_| "minioadmin".into()),
        secret: std::env::var("FIREWEED_S3_TEST_SECRET_KEY")
            .unwrap_or_else(|_| "minioadmin".into()),
    };
    S3BlobStore::new(
        &s3.endpoint,
        &s3.bucket,
        &s3.access,
        &s3.secret,
        "us-east-1",
    )
    .expect("build S3 client")
    .create_bucket()
    .expect("create/ensure bucket");
    let source_revision =
        std::env::var("FIREWEED_E3_SOURCE_REVISION").expect("exact source revision");
    let output = std::env::var("FIREWEED_E3_FENCE_EVIDENCE_OUT").expect("fence evidence path");
    prove_postgres_pointer_fence(&s3, &source_revision, std::path::Path::new(&output));
}

fn synthetic_ack(
    label: &'static str,
    throughput_per_s: f64,
    configured_window_ms: f64,
) -> AckResult {
    let progress = throughput_per_s.is_finite() && throughput_per_s > 0.0;
    AckResult {
        label,
        target_bytes: 8_388_608,
        max_latency_ms: 100,
        segments_sealed: 1,
        objects_put: 1,
        store_operations: StoreOperations {
            puts: 1,
            gets: 1,
            lists: 1,
            deletes: 0,
            request_bytes: 100,
            response_bytes: 100,
        },
        resource_bounds: ResourceBounds {
            configured_global_bytes: 1024,
            current_bytes: 0,
            peak_bytes: 512,
            waiters: 0,
            recorder_in_flight: 0,
            recorder_peak_in_flight: 1,
            task_count: 8,
            task_limit: RELEASE_ACK_CONCURRENCY,
            store_in_flight_limit: RELEASE_ACK_CONCURRENCY,
            object_page_limit: STORE_OBJECT_PAGE_LIMIT,
        },
        commands_committed: 2,
        mean_batch: 2.0,
        max_batch: 2,
        throughput_per_s,
        disabled_control_throughput_per_s: throughput_per_s,
        recorder_overhead_ratio: 1.0,
        recorder_overhead_ratio_samples: vec![1.0; RECORDER_CONTROL_BLOCKS],
        recorder_control_order_seed: 7,
        recorder_control_schedule: "independent-bounded-blocks-seeded-alternating-order-v1",
        recorder_control_fingerprint_algorithm: "fnv1a128+disk-unique-id-index+canonical-live-state-v1",
        recorder_enabled_state_fingerprint: "fnv1a128:0123456789abcdef0123456789abcdef".into(),
        recorder_disabled_state_fingerprint: "fnv1a128:0123456789abcdef0123456789abcdef".into(),
        recorder_control_verified_items: 2,
        recorder_control_logical_match: true,
        throughput_progress_met: progress,
        ack_p50_ms: 1.0,
        ack_p95_ms: 2.0,
        ack_p99_ms: 3.0,
        configured_window_ms,
        latency_distribution_met: true,
        load_shape_met: true,
        bar_met: progress,
    }
}

fn synthetic_recovery(bar_met: bool, requires_snapshot: bool) -> RecoveryResult {
    RecoveryResult {
        resident: 10_000_000,
        load_batch: 1_000,
        load_task_count: RELEASE_LOAD_CONCURRENCY,
        load_command_count: if requires_snapshot { 99 } else { 100 },
        load_segments_sealed: 10,
        load_size_triggered_seals: 9,
        load_latency_triggered_seals: 1,
        load_forced_seals: 0,
        load_rollover_seals: 0,
        load_segment_bytes: 80_000_000,
        load_mean_commands_per_segment: 10.0,
        load_max_commands_per_segment: 16,
        load_group_commit_batch_sum: if requires_snapshot { 99 } else { 100 },
        command_count: 100,
        total_commands: 100,
        start_seq: if requires_snapshot { 10 } else { 0 },
        tail_replayed: if requires_snapshot { 90 } else { 100 },
        snapshot_used: requires_snapshot,
        recovery_max_tail: 1_000_000,
        recovery_wall_ms: 5.0,
        pending_after: 10_000_000,
        state_digest_before: "fnv1a128:fixture".into(),
        state_digest_after: "fnv1a128:fixture".into(),
        verified_items: 10_000_000,
        missing_items: 0,
        duplicate_items: 0,
        invalid_items: 0,
        replay_progress_samples: vec![0, 100],
        replay_command_page_limit: 256,
        peak_replay_commands_buffered: 100,
        peak_manifest_objects_buffered: 1,
        recovery_index_node_visits: 2,
        recovery_index_entries_visited: 100,
        recovery_index_height: 1,
        recovery_index_nodes_written_last_append: 2,
        recovery_segment_gets: 1,
        recovery_segment_bytes_fetched: 1024,
        recovery_peak_segment_bytes_buffered: 1024,
        recovery_peak_index_node_bytes_buffered: 512,
        recovery_peak_cursor_bytes_buffered: 2048,
        bounded_authority_index: true,
        verification_chunk_items: 512,
        queue_count: 1,
        resource_bounds: ResourceBounds {
            configured_global_bytes: 1024,
            current_bytes: 0,
            peak_bytes: 512,
            waiters: 0,
            recorder_in_flight: 0,
            recorder_peak_in_flight: 1,
            task_count: 1,
            task_limit: 1,
            store_in_flight_limit: 1,
            object_page_limit: STORE_OBJECT_PAGE_LIMIT,
        },
        store_operations: StoreOperations {
            puts: 20,
            gets: 10,
            lists: 2,
            deletes: 0,
            request_bytes: 100,
            response_bytes: 100,
        },
        checksum_validation_passed: true,
        bar_met,
    }
}

fn synthetic_profile_run(
    backend_profile: &'static str,
    projection_label: &'static str,
    requires_snapshot: bool,
) -> ProfileRun {
    let ack_results = E3_BOUND_CONFIGS
        .iter()
        .map(|bound| synthetic_ack(bound.label, 3_000.0, 10.0))
        .collect::<Vec<_>>();
    let recovery = Some(synthetic_recovery(true, requires_snapshot));
    let bars_met = ack_results.iter().all(|result| result.bar_met)
        && recovery.as_ref().is_none_or(|r| r.bar_met);
    ProfileRun {
        backend_profile,
        projection_label,
        ack_results,
        recovery,
        wall_ms: 10.0,
        bars_met,
    }
}

#[test]
fn e3_matrix_rejects_missing_profile() {
    let runs = vec![synthetic_profile_run(
        "object_log_inmemory_projection",
        "inmemory",
        false,
    )];
    let errors = validate_e3_profile_matrix(&runs, true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("missing profile object_log_sqlite_projection")),
        "{errors:?}"
    );
}

#[test]
fn e3_matrix_rejects_missing_bound() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", "sqlite", true);
    run.ack_results.pop();
    let errors = validate_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("missing bound 100ms")),
        "{errors:?}"
    );
}

#[test]
fn e3_matrix_rejects_no_throughput_progress() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", "sqlite", true);
    run.ack_results[0].throughput_per_s = 0.0;
    run.ack_results[0].throughput_progress_met = false;
    run.ack_results[0].bar_met = false;
    let errors = validate_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("measurable throughput progress")),
        "{errors:?}"
    );
}

#[test]
fn e3_matrix_rejects_invalid_latency_distribution() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", "sqlite", true);
    run.ack_results[1].ack_p99_ms = run.ack_results[1].ack_p50_ms - 1.0;
    run.ack_results[1].latency_distribution_met = false;
    run.ack_results[1].bar_met = false;
    let errors = validate_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("invalid latency distribution")),
        "{errors:?}"
    );
}

#[test]
fn e3_matrix_rejects_recorder_control_divergence() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", "sqlite", true);
    run.ack_results[0].recorder_control_logical_match = false;
    run.ack_results[0].bar_met = false;
    let errors = validate_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("recorder controls diverged logically")),
        "{errors:?}"
    );
}

#[test]
fn e3_matrix_rejects_unbounded_recorder_degradation() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", "sqlite", true);
    run.ack_results[0].recorder_overhead_ratio = MAX_RECORDER_OVERHEAD_RATIO + 0.001;
    run.ack_results[0].bar_met = false;
    let errors = validate_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("recorder overhead ratio")),
        "{errors:?}"
    );
}

#[test]
fn e3_matrix_rejects_forged_or_lockstepped_recorder_distribution() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", "sqlite", true);
    run.ack_results[0].recorder_overhead_ratio_samples = vec![1.0, 1.0, 1.5, 1.5, 1.5];
    let errors = validate_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("independent bounded-block recorder-control distribution")),
        "{errors:?}"
    );

    let mut run = synthetic_profile_run("object_log_sqlite_projection", "sqlite", true);
    run.ack_results[0].recorder_control_schedule =
        "paired-operation-barriers-concurrent-worker-partitions-v1";
    let errors = validate_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("independent bounded-block recorder-control distribution")),
        "{errors:?}"
    );
}

#[test]
fn e3_matrix_rejects_recovery_digest_drift() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", "sqlite", true);
    let recovery = run.recovery.as_mut().unwrap();
    recovery.state_digest_after = "fnv1a128:drift".into();
    recovery.bar_met = false;
    let errors = validate_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("exact complete state")),
        "{errors:?}"
    );
}

#[test]
fn e3_matrix_rejects_inexact_recovery_command_range() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", "sqlite", true);
    let recovery = run.recovery.as_mut().unwrap();
    recovery.tail_replayed -= 1;
    let errors = validate_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("recovery command range is not exact")),
        "{errors:?}"
    );
}

#[test]
fn e3_matrix_rejects_latency_driven_or_inexact_recovery_load_batching() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", "sqlite", true);
    let recovery = run.recovery.as_mut().unwrap();
    recovery.load_latency_triggered_seals = 5;
    recovery.load_group_commit_batch_sum -= 1;
    let errors = validate_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors.iter().any(|error| error
            .contains("recovery load lacks exact size-triggered group-commit batching")),
        "{errors:?}"
    );
}

#[test]
fn canonical_recovery_command_counts_include_the_sqlite_crash_tail() {
    assert_eq!(
        concurrent_load_command_count(
            RELEASE_RESIDENT,
            RELEASE_LOAD_BATCH,
            RELEASE_LOAD_CONCURRENCY
        ),
        10_000,
        "in-memory genesis profile loads exactly 10,000 commands"
    );
    assert_eq!(
        concurrent_load_command_count(
            RELEASE_RESIDENT - 1,
            RELEASE_LOAD_BATCH,
            RELEASE_LOAD_CONCURRENCY
        ) + 1,
        10_001,
        "SQLite snapshot load plus its committed crash tail is 10,001 commands"
    );
}

fn live_e3_s3_env() -> Option<S3Env> {
    let Ok(endpoint) = std::env::var("FIREWEED_S3_TEST_ENDPOINT") else {
        eprintln!("TEST SKIPPED: FIREWEED_S3_TEST_ENDPOINT is required for live E3 recovery");
        return None;
    };
    Some(S3Env {
        endpoint,
        bucket: std::env::var("FIREWEED_S3_TEST_BUCKET").unwrap_or_else(|_| "fireweed-test".into()),
        access: std::env::var("FIREWEED_S3_TEST_ACCESS_KEY")
            .unwrap_or_else(|_| "minioadmin".into()),
        secret: std::env::var("FIREWEED_S3_TEST_SECRET_KEY")
            .unwrap_or_else(|_| "minioadmin".into()),
    })
}

fn assert_recovery_exact_contract(recovery: &RecoveryResult, requires_snapshot: bool) {
    assert_eq!(recovery.resident, RELEASE_RESIDENT);
    assert_eq!(recovery.load_batch, RELEASE_LOAD_BATCH);
    assert_eq!(recovery.pending_after, RELEASE_RESIDENT);
    assert_eq!(recovery.verified_items, RELEASE_RESIDENT);
    assert_eq!(recovery.missing_items, 0);
    assert_eq!(recovery.duplicate_items, 0);
    assert_eq!(recovery.invalid_items, 0);
    assert!(recovery.checksum_validation_passed);
    assert!(
        recovery
            .replay_progress_samples
            .windows(2)
            .all(|pair| pair[0] <= pair[1])
    );
    assert_eq!(
        recovery.start_seq + recovery.tail_replayed,
        recovery.total_commands
    );
    assert_eq!(recovery.total_commands, recovery.command_count);
    assert!(recovery.bounded_authority_index);
    assert_eq!(recovery.resource_bounds.current_bytes, 0);
    assert_eq!(recovery.resource_bounds.waiters, 0);
    assert_eq!(
        recovery.resource_bounds.task_count,
        recovery.resource_bounds.task_limit
    );
    assert_eq!(
        recovery.resource_bounds.object_page_limit,
        STORE_OBJECT_PAGE_LIMIT
    );
    assert!(recovery.peak_replay_commands_buffered <= recovery.replay_command_page_limit);
    assert!(recovery.peak_manifest_objects_buffered <= recovery.resource_bounds.object_page_limit);
    if requires_snapshot {
        assert!(recovery.snapshot_used);
        assert!(recovery.start_seq > 0);
        assert!(recovery.tail_replayed > 0);
        assert!(recovery.tail_replayed < recovery.total_commands);
    } else {
        assert!(!recovery.snapshot_used);
        assert_eq!(recovery.start_seq, 0);
        assert_eq!(recovery.tail_replayed, recovery.total_commands);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(non_snake_case)]
async fn TestE3RecoveryExactSnapshotTailReplay() {
    let Some(s3) = live_e3_s3_env() else {
        return;
    };
    S3BlobStore::new(
        &s3.endpoint,
        &s3.bucket,
        &s3.access,
        &s3.secret,
        "us-east-1",
    )
    .expect("build S3 client")
    .create_bucket()
    .expect("create/ensure bucket");

    let recovery = run_recovery::<SegmentedObjectLogSqliteBackend, _>(
        &s3,
        "object_log_sqlite_projection",
        RELEASE_RESIDENT,
        RELEASE_LOAD_BATCH,
        true,
        |store, projection_path, cfg| {
            SegmentedObjectLogSqliteBackend::open_with_blob_store(store, projection_path, cfg)
        },
    )
    .await;
    assert_recovery_exact_contract(&recovery, true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(non_snake_case)]
async fn TestE3RecoveryExactGenesisReplay() {
    let Some(s3) = live_e3_s3_env() else {
        return;
    };
    S3BlobStore::new(
        &s3.endpoint,
        &s3.bucket,
        &s3.access,
        &s3.secret,
        "us-east-1",
    )
    .expect("build S3 client")
    .create_bucket()
    .expect("create/ensure bucket");

    let recovery = run_recovery::<SegmentedObjectLogInMemoryBackend, _>(
        &s3,
        "object_log_inmemory_projection",
        RELEASE_RESIDENT,
        RELEASE_LOAD_BATCH,
        false,
        |store, _projection_path, cfg| {
            SegmentedObjectLogInMemoryBackend::open_with_blob_store(store, cfg)
        },
    )
    .await;
    assert_recovery_exact_contract(&recovery, false);
}

#[test]
#[allow(non_snake_case)]
fn TestE3RecoveryRejectsInexactCommandRange() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", "sqlite", true);
    let recovery = run.recovery.as_mut().unwrap();
    recovery.tail_replayed -= 1;
    let errors = validate_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("recovery command range is not exact")),
        "{errors:?}"
    );
}

#[test]
#[allow(non_snake_case)]
fn TestE3RecoveryRejectsChecksumDrift() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", "sqlite", true);
    let recovery = run.recovery.as_mut().unwrap();
    recovery.checksum_validation_passed = false;
    let errors = validate_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("recovery checksum validation did not pass")),
        "{errors:?}"
    );
}
