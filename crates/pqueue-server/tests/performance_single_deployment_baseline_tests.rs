//! TP-002 **E0 portable per-queue progress/capacity contract + E1 single-deployment envelope** evidence on `postgres_native`
//! (TD-002, the DB-authoritative `PostgresRelationalBackend`).
//!
//! ENV-GATED on `PQUEUE_PG_TEST_URL` (a live database). Without it the test prints a LOUD skip and returns —
//! a green run is then VISIBLY partial (the E0/E1 evidence is DEFERRED, never a hidden/fabricated pass), and
//! no release claim is produced for the skipped live-DB run.
//!
//! To run live:
//!   docker run -d --name pq-pg -p 5433:5432 -e POSTGRES_PASSWORD=pq postgres:16
//!   PQUEUE_PG_TEST_URL=postgres://postgres:pq@127.0.0.1:5433/postgres \
//!     cargo test -p pqueue-server --features postgres --test performance_single_deployment_baseline_tests -- --nocapture
//!   # the full E1 resident shape (10M retained terminal rows) is the heavier release configuration:
//!   PQUEUE_E1_RESIDENT=10000000 PQUEUE_PG_TEST_URL=... cargo test ... --release
//!
//! WHAT THIS MEASURES (when a DB is present): on ONE postgres deployment, one queue —
//!   - E0: exact concurrent push/claim/finalize progress to a 10M retained-terminal checkpoint.
//!   - E1: production-facade push/update-window/claim/finalize at batch sizes 1/100(/1000) with the
//!     retained resident set present. Rates and percentiles are capacity observations, never host gates.
//!
//! TWO LANES (honest perf-environment gating):
//!   - SMOKE (default, any DB): MEASURES + reports + emits SMOKE-tier ledger rows (recorded + gate-visible,
//!     but never satisfy a release E0/E1 requirement). Exact outcomes, configured progress, and bounded-resource
//!     invariants are asserted. Capacity rates and percentiles are reported without becoming host-speed gates.
//!   - PERF (`PQUEUE_PERF_ENV=1`, a provisioned instance): emits RELEASE-tier rows only when exact outcomes,
//!     the explicitly declared and persisted progress bound, declared topology, full shape, and bounded resources
//!     are all measured and met.
//!
//! A row's `exit_status` is always 0 (the measurement run completed; the strict verifier requires it) and so
//! carries NO pass/fail signal — pass/fail lives in `measurements.bars_met` and `evidence_tier`.
//!
//! Defaults are small (`PQUEUE_E1_RESIDENT`, default 1000) so a routine run is short. The relational backend
//! drives configured batches through set-based downstream primitives; the full release shape
//! (`PQUEUE_E1_RESIDENT=10000000 PQUEUE_E1_FULL=1`) is the provisioned perf-env run. Wall-clock capacity
//! remains visible without becoming a host-speed gate.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use pqueue::{NewItem, PayloadUpdate, Pqueue, SystemClock};
use pqueue_core::{
    ClientItemKey, EligibilityPolicy, GroupKey, ItemId, OrderingMode, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId,
};
use pqueue_engine::{ControlPlaneStore, DiscoveryGranularity, EngineError, QueueKey};
use pqueue_postgres::{PostgresConnectConfig, PostgresRelationalBackend};
use pqueue_server::{PostgresWholeOperationAdapter, fixed_postgres_relational_pool};

const CONFIGURED_MAX_BATCH_SIZE: u64 = 1_000;
const CONFIGURED_CONCURRENCY: u64 = 2;
const SMOKE_PROGRESS_BOUND_MS: u64 = 60_000;
const GROUP_CARDINALITY: u64 = 64;

#[derive(Clone, Copy)]
struct RunCaps {
    threads: u64,
    rss_bytes: u64,
    connections: u64,
    workers: u64,
    pending_items: u64,
    in_flight_operations: u64,
    explicitly_declared: bool,
}

impl RunCaps {
    fn from_env(release: bool) -> Self {
        let read = |key: &str, default: u64| match std::env::var(key) {
            Ok(value) => value
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0)
                .unwrap_or_else(|| panic!("{key} must be a positive integer")),
            Err(_) if release => panic!("release evidence requires explicit {key}"),
            Err(_) => default,
        };
        let keys = [
            "PQUEUE_E0E1_THREAD_CAP",
            "PQUEUE_E0E1_RSS_CAP_BYTES",
            "PQUEUE_E0E1_CONNECTION_CAP",
            "PQUEUE_E0E1_WORKER_CAP",
            "PQUEUE_E0E1_PENDING_ITEM_CAP",
            "PQUEUE_E0E1_IN_FLIGHT_CAP",
        ];
        Self {
            threads: read(keys[0], 64),
            rss_bytes: read(keys[1], 2 * 1024 * 1024 * 1024),
            connections: read(keys[2], 2),
            workers: read(keys[3], 2),
            pending_items: read(keys[4], 2_000),
            in_flight_operations: read(keys[5], 2),
            explicitly_declared: keys.iter().all(|key| std::env::var(key).is_ok()),
        }
    }
}

#[derive(Default)]
struct OperationProbe {
    in_flight: AtomicU64,
    max_in_flight: AtomicU64,
}

struct OperationGuard<'a>(&'a OperationProbe);

impl OperationProbe {
    fn begin(&self) -> OperationGuard<'_> {
        let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(current, Ordering::SeqCst);
        OperationGuard(self)
    }

    fn max_observed(&self) -> u64 {
        self.max_in_flight.load(Ordering::SeqCst)
    }
}

impl Drop for OperationGuard<'_> {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct WorkerProbe {
    active: AtomicU64,
    peak: AtomicU64,
    started: AtomicU64,
    completed: AtomicU64,
}

struct WorkerGuard<'a>(&'a WorkerProbe);

impl WorkerProbe {
    fn start(&self) -> WorkerGuard<'_> {
        self.started.fetch_add(1, Ordering::SeqCst);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        WorkerGuard(self)
    }
}

impl Drop for WorkerGuard<'_> {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        self.0.completed.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct ResourceObservations {
    samples: u64,
    max_threads: u64,
    max_connections: u64,
    max_rss_bytes: u64,
    postgres_server_version: String,
    postgres_max_connections: u64,
    postgres_shared_buffers_bytes: u64,
    postgres_database_size_bytes: u64,
    postgres_temp_bytes: u64,
    postgres_blocks_read: u64,
    postgres_blocks_hit: u64,
}

impl ResourceObservations {
    fn sample(&mut self, observer_url: &str, application_name_prefix: &str) {
        let status = std::fs::read_to_string("/proc/self/status")
            .expect("release evidence requires Linux /proc process accounting");
        let value = |name: &str| -> u64 {
            status
                .lines()
                .find_map(|line| line.strip_prefix(name))
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse().ok())
                .unwrap_or_else(|| panic!("/proc/self/status is missing {name}"))
        };
        let threads = value("Threads:");
        let rss_bytes = value("VmHWM:") * 1024;
        let mut observer = postgres::Client::connect(observer_url, postgres::NoTls)
            .expect("connect Postgres resource observer");
        let connections: i64 = observer
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE $1",
                &[&format!("{application_name_prefix}%")],
            )
            .expect("sample producer Postgres connections")
            .get(0);
        let server = observer
            .query_one(
                "SELECT current_setting('server_version'), current_setting('max_connections')::bigint, pg_size_bytes(current_setting('shared_buffers'))::bigint, pg_database_size(current_database())::bigint",
                &[],
            )
            .expect("sample Postgres topology");
        let stats = observer
            .query_one(
                "SELECT temp_bytes::bigint, blks_read::bigint, blks_hit::bigint FROM pg_stat_database WHERE datname=current_database()",
                &[],
            )
            .expect("sample Postgres database stats");
        self.samples += 1;
        self.max_threads = self.max_threads.max(threads);
        self.max_connections = self.max_connections.max(connections as u64);
        self.max_rss_bytes = self.max_rss_bytes.max(rss_bytes);
        self.postgres_server_version = server.get(0);
        self.postgres_max_connections = server.get::<_, i64>(1) as u64;
        self.postgres_shared_buffers_bytes = server.get::<_, i64>(2) as u64;
        self.postgres_database_size_bytes = server.get::<_, i64>(3) as u64;
        self.postgres_temp_bytes = stats.get::<_, i64>(0) as u64;
        self.postgres_blocks_read = stats.get::<_, i64>(1) as u64;
        self.postgres_blocks_hit = stats.get::<_, i64>(2) as u64;
    }

    fn within_bounds(&self, max_in_flight: u64, pending_items_peak: u64, caps: RunCaps) -> bool {
        self.samples >= 3
            && self.max_threads > 0
            && self.max_threads <= caps.threads
            && self.max_connections > 0
            && self.max_connections <= caps.connections
            && self.max_rss_bytes > 0
            && self.max_rss_bytes <= caps.rss_bytes
            && max_in_flight > 0
            && max_in_flight <= caps.in_flight_operations
            && pending_items_peak > 0
            && pending_items_peak <= caps.pending_items
    }
}

struct MeasuredContract<'a> {
    exact_outcomes: bool,
    monotonic_progress: bool,
    bounded_resources: bool,
    finalized_samples: &'a [u64],
    cursor_samples: &'a [u64],
    oldest_eligible_age_samples_ms: &'a [u64],
    progress_identity_sample_count: u64,
    progress_latency_lower_max_ms: u64,
    progress_latency_upper_max_ms: u64,
    progress_latency_over_60000_ms_count: u64,
    progress_bound_ms: u64,
    persisted_progress_bound_ms: u64,
    progress_bound_explicit: bool,
    progress_bound_violations: u64,
    progress_latency_upper_buckets: &'a BTreeMap<String, u64>,
    lifecycle_snapshots: &'a [serde_json::Value],
    discovery_query_count: u64,
    discovery_nonempty_count: u64,
    resources: &'a ResourceObservations,
    max_in_flight: u64,
    pending_items_peak: u64,
    caps: RunCaps,
    workers: &'a WorkerProbe,
}

fn insert_measured_contract(
    values: &mut BTreeMap<String, serde_json::Value>,
    measured: MeasuredContract<'_>,
) {
    let MeasuredContract {
        exact_outcomes,
        monotonic_progress,
        bounded_resources,
        finalized_samples,
        cursor_samples,
        oldest_eligible_age_samples_ms,
        progress_identity_sample_count,
        progress_latency_lower_max_ms,
        progress_latency_upper_max_ms,
        progress_latency_over_60000_ms_count,
        progress_bound_ms,
        persisted_progress_bound_ms,
        progress_bound_explicit,
        progress_bound_violations,
        progress_latency_upper_buckets,
        lifecycle_snapshots,
        discovery_query_count,
        discovery_nonempty_count,
        resources,
        max_in_flight,
        pending_items_peak,
        caps,
        workers,
    } = measured;
    values.extend([
        ("exact_outcomes".into(), serde_json::json!(exact_outcomes)),
        (
            "monotonic_progress".into(),
            serde_json::json!(monotonic_progress),
        ),
        (
            "bounded_resources".into(),
            serde_json::json!(bounded_resources),
        ),
        (
            "progress_samples_finalized".into(),
            serde_json::json!(finalized_samples),
        ),
        (
            "progress_sample_count".into(),
            serde_json::json!(finalized_samples.len()),
        ),
        ("cursor_samples".into(), serde_json::json!(cursor_samples)),
        (
            "oldest_eligible_age_samples_ms".into(),
            serde_json::json!(oldest_eligible_age_samples_ms),
        ),
        ("discovery_query_count".into(), serde_json::json!(discovery_query_count)),
        ("discovery_nonempty_count".into(), serde_json::json!(discovery_nonempty_count)),
        (
            "progress_identity_sample_count".into(),
            serde_json::json!(progress_identity_sample_count),
        ),
        (
            "progress_latency_lower_max_ms".into(),
            serde_json::json!(progress_latency_lower_max_ms),
        ),
        (
            "progress_latency_upper_max_ms".into(),
            serde_json::json!(progress_latency_upper_max_ms),
        ),
        (
            "progress_bound_ms".into(),
            serde_json::json!(progress_bound_ms),
        ),
        (
            "progress_bound_explicit".into(),
            serde_json::json!(progress_bound_explicit),
        ),
        (
            "persisted_progress_bound_ms".into(),
            serde_json::json!(persisted_progress_bound_ms),
        ),
        (
            "progress_bound_buckets".into(),
            serde_json::json!({
                "within_declared_bound": progress_identity_sample_count - progress_bound_violations,
                "over_declared_bound": progress_bound_violations,
            }),
        ),
        (
            "progress_bound_violations".into(),
            serde_json::json!(progress_bound_violations),
        ),
        (
            "progress_latency_over_60000_ms_count".into(),
            serde_json::json!(progress_latency_over_60000_ms_count),
        ),
        (
            "fixed_latency_buckets_capacity_only".into(),
            serde_json::json!(true),
        ),
        (
            "progress_measurement".into(),
            serde_json::json!("per-item accepted and claimed timestamp intervals"),
        ),
        (
            "progress_latency_upper_buckets".into(),
            serde_json::json!(progress_latency_upper_buckets),
        ),
        (
            "resource_sample_count".into(),
            serde_json::json!(resources.samples),
        ),
        (
            "max_threads_observed".into(),
            serde_json::json!(resources.max_threads),
        ),
        ("thread_limit".into(), serde_json::json!(caps.threads)),
        (
            "max_connections_observed".into(),
            serde_json::json!(resources.max_connections),
        ),
        (
            "connection_limit".into(),
            serde_json::json!(caps.connections),
        ),
        (
            "max_rss_bytes_observed".into(),
            serde_json::json!(resources.max_rss_bytes),
        ),
        (
            "rss_limit_bytes".into(),
            serde_json::json!(caps.rss_bytes),
        ),
        (
            "max_in_flight_operations_observed".into(),
            serde_json::json!(max_in_flight),
        ),
        (
            "configured_concurrency".into(),
            serde_json::json!(CONFIGURED_CONCURRENCY),
        ),
        (
            "shared_workers_peak".into(),
            serde_json::json!(workers.peak.load(Ordering::SeqCst)),
        ),
        ("workers_started".into(), serde_json::json!(workers.started.load(Ordering::SeqCst))),
        ("workers_completed".into(), serde_json::json!(workers.completed.load(Ordering::SeqCst))),
        (
            "shared_workers_limit".into(),
            serde_json::json!(caps.workers),
        ),
        (
            "connections_peak".into(),
            serde_json::json!(resources.max_connections),
        ),
        (
            "connections_limit".into(),
            serde_json::json!(caps.connections),
        ),
        (
            "pending_work_items_peak".into(),
            serde_json::json!(pending_items_peak),
        ),
        (
            "pending_work_items_limit".into(),
            serde_json::json!(caps.pending_items),
        ),
        (
            "memory_peak_bytes".into(),
            serde_json::json!(resources.max_rss_bytes),
        ),
        (
            "memory_limit_bytes".into(),
            serde_json::json!(caps.rss_bytes),
        ),
        (
            "postgres_server_version".into(),
            serde_json::json!(resources.postgres_server_version),
        ),
        (
            "postgres_max_connections".into(),
            serde_json::json!(resources.postgres_max_connections),
        ),
        (
            "postgres_shared_buffers_bytes".into(),
            serde_json::json!(resources.postgres_shared_buffers_bytes),
        ),
        (
            "postgres_database_size_bytes".into(),
            serde_json::json!(resources.postgres_database_size_bytes),
        ),
        (
            "postgres_temp_bytes".into(),
            serde_json::json!(resources.postgres_temp_bytes),
        ),
        (
            "postgres_blocks_read".into(),
            serde_json::json!(resources.postgres_blocks_read),
        ),
        (
            "postgres_blocks_hit".into(),
            serde_json::json!(resources.postgres_blocks_hit),
        ),
        (
            "postgres_instance_class".into(),
            serde_json::json!(
                std::env::var("PQUEUE_PG_INSTANCE_CLASS").unwrap_or_else(|_| "local-smoke".into())
            ),
        ),
        (
            "host_cpu_count".into(),
            serde_json::json!(
                std::thread::available_parallelism()
                    .map(|n| n.get() as u64)
                    .unwrap_or(1)
            ),
        ),
        (
            "host_memory_bytes".into(),
            serde_json::json!(
                std::fs::read_to_string("/proc/meminfo")
                    .ok()
                    .and_then(|body| body.lines().find_map(|line| line
                        .strip_prefix("MemTotal:")
                        .and_then(|value| value.split_whitespace().next())
                        .and_then(|value| value.parse::<u64>().ok())))
                    .unwrap_or(0)
                    * 1024
            ),
        ),
        (
            "postgres_cpu_limit".into(),
            serde_json::json!(
                std::env::var("PQUEUE_PG_CPU_LIMIT")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(1)
            ),
        ),
        (
            "postgres_memory_limit_bytes".into(),
            serde_json::json!(
                std::env::var("PQUEUE_PG_MEMORY_LIMIT_BYTES")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(caps.rss_bytes)
            ),
        ),
        (
            "postgres_iops_profile".into(),
            serde_json::json!(
                std::env::var("PQUEUE_PG_IOPS_PROFILE")
                    .unwrap_or_else(|_| "local-unspecified".into())
            ),
        ),
        (
            "postgres_storage_class".into(),
            serde_json::json!(
                std::env::var("PQUEUE_PG_STORAGE_CLASS")
                    .unwrap_or_else(|_| "local-filesystem".into())
            ),
        ),
        (
            "postgres_pool_limit".into(),
            serde_json::json!(caps.connections),
        ),
        (
            "topology".into(),
            serde_json::json!("single-process+single-postgres+fixed-2-member-affinity-pool"),
        ),
        (
            "topology_declared".into(),
            serde_json::json!(
                caps.explicitly_declared && [
                    "PQUEUE_PG_INSTANCE_CLASS",
                    "PQUEUE_PG_CPU_LIMIT",
                    "PQUEUE_PG_MEMORY_LIMIT_BYTES",
                    "PQUEUE_PG_IOPS_PROFILE",
                    "PQUEUE_PG_STORAGE_CLASS",
                ]
                .iter()
                .all(|key| std::env::var(key).is_ok())
            ),
        ),
        (
            "telemetry_surface".into(),
            serde_json::json!("Pqueue::metrics+current_position+discover_active_scopes"),
        ),
        (
            "telemetry_sample_count".into(),
            serde_json::json!(lifecycle_snapshots.len()),
        ),
        (
            "lifecycle_snapshots".into(),
            serde_json::json!(lifecycle_snapshots),
        ),
        (
            "in_flight_operation_limit".into(),
            serde_json::json!(caps.in_flight_operations),
        ),
        (
            "resource_measurement_source".into(),
            serde_json::json!(
                "linux_procfs+declared_workload_caps+postgres_pg_stat_activity+natural_operation_counter"
            ),
        ),
    ]);
}

fn fresh_schema() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "pq_e0e1_{}_{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

fn checkout_provenance() -> (String, String, bool, String, bool) {
    let explicit_root = std::env::var_os("PQUEUE_SOURCE_ROOT").map(std::path::PathBuf::from);
    let probe_dir = explicit_root
        .clone()
        .unwrap_or_else(|| std::env::current_dir().expect("resolve current directory"));
    let probe_dir = probe_dir
        .canonicalize()
        .expect("canonicalize PQUEUE_SOURCE_ROOT");
    let run = |directory: &std::path::Path, args: &[&str]| {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(directory)
            .args(args)
            .output()
            .expect("run git provenance command");
        assert!(
            output.status.success(),
            "git provenance command must succeed"
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    };
    let root = run(&probe_dir, &["rev-parse", "--show-toplevel"]);
    let root_path = std::path::Path::new(&root).canonicalize().unwrap();
    if explicit_root.is_some() {
        assert_eq!(
            root_path, probe_dir,
            "PQUEUE_SOURCE_ROOT must be the producing repository root"
        );
    }
    let revision = run(&root_path, &["rev-parse", "HEAD"]);
    let status = run(
        &root_path,
        &["status", "--porcelain=v1", "--untracked-files=normal"],
    );
    let compile_manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .expect("canonicalize compile-time CARGO_MANIFEST_DIR");
    let compile_source_root = run(&compile_manifest_dir, &["rev-parse", "--show-toplevel"]);
    let compile_source_root = std::path::Path::new(&compile_source_root)
        .canonicalize()
        .expect("canonicalize compile-time source root");
    let compile_source_root_bound = compile_source_root == root_path;
    (
        root_path.to_string_lossy().into_owned(),
        revision,
        status.is_empty(),
        compile_source_root.to_string_lossy().into_owned(),
        compile_source_root_bound,
    )
}

fn sk(tenant: &str, queue: &str) -> QueueKey {
    QueueKey::new(TenantId::new(tenant).unwrap(), QueueId::new(queue).unwrap())
}

fn big_qdef(tenant: &str, queue: &str, progress_bound_ms: u64) -> QueueDefinition {
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
        progress_bound_ms,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 600_000,
        client_item_key_retention_ms: 600_000,
        terminal_retention_ms: 7 * 24 * 60 * 60 * 1000,
        max_lease_duration_ms: 3_600_000,
        retry_policy: RetryPolicy {
            max_attempts: 1_000_000,
        },
        max_push_batch_size: CONFIGURED_MAX_BATCH_SIZE,
        max_claim_batch_size: CONFIGURED_MAX_BATCH_SIZE,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

fn representative_item(sequence: u64, sentinel: bool) -> NewItem {
    let mut fields = BTreeMap::new();
    fields.insert("shape".into(), Bytes::from_static(b"hot_record_v1"));
    fields.insert("sequence".into(), Bytes::from(sequence.to_string()));
    NewItem {
        client_item_key: Some(ClientItemKey::new(format!("e0e1-{sequence}")).unwrap()),
        priority: Some(PriorityValue::Int64(if sentinel {
            i64::MIN
        } else if sequence.is_multiple_of(10) {
            -100
        } else {
            (sequence % 100) as i64
        })),
        group_key: Some(GroupKey::new(format!("group-{}", sequence % GROUP_CARDINALITY)).unwrap()),
        payload: Some(Bytes::from(vec![
            b'x';
            [512, 1024, 2048][sequence as usize % 3]
        ])),
        fields,
        ..Default::default()
    }
}

async fn push_batch<B: pqueue::LibBackend>(
    pq: &Pqueue<B>,
    shard: &QueueKey,
    base: u64,
    n: u64,
    operations: &OperationProbe,
) -> Vec<ItemId> {
    let _operation = operations.begin();
    let items = (0..n)
        .map(|k| representative_item(base + k, k + 1 == n))
        .collect();
    pq.push_batch(shard, items)
        .await
        .expect("facade push_batch")
}

/// Claim up to `n` eligible items from `shard`, returning their ids.
async fn claim<B: pqueue::LibBackend>(
    pq: &Pqueue<B>,
    shard: &QueueKey,
    n: usize,
    operations: &OperationProbe,
) -> Vec<ItemId> {
    let _operation = operations.begin();
    pq.claim(shard, n, 3_600_000)
        .await
        .expect("facade claim")
        .into_iter()
        .map(|c| c.item_id)
        .collect()
}

/// Finalize-complete the given ids on `shard`.
async fn finalize<B: pqueue::LibBackend>(
    pq: &Pqueue<B>,
    shard: &QueueKey,
    ids: &[ItemId],
    operations: &OperationProbe,
) {
    let _operation = operations.begin();
    pq.ack(shard, ids.iter().copied())
        .await
        .expect("facade ack");
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

struct ProducerResult {
    accepted: u64,
    duplicate_ids: u64,
    seen_ids: Vec<bool>,
    identity_prefix: Option<u64>,
    counter_min: u64,
    counter_max: u64,
    payload_size_counts: BTreeMap<u64, u64>,
    group_counts: Vec<u64>,
    priority_counts: BTreeMap<String, u64>,
    push_batches: u64,
    push_operation_ns: u64,
}

struct ConsumerResult {
    claimed: u64,
    finalized: u64,
    duplicate_claims: u64,
    seen_ids: Vec<bool>,
    identity_prefix: Option<u64>,
    counter_min: u64,
    counter_max: u64,
    progress_identity_sample_count: u64,
    progress_latency_lower_max_ms: u64,
    progress_latency_upper_max_ms: u64,
    progress_latency_over_60000_ms_count: u64,
    progress_bound_violations: u64,
    progress_latency_upper_buckets: BTreeMap<String, u64>,
    finalized_samples: Vec<u64>,
    cursor_samples: Vec<u64>,
    oldest_eligible_age_samples_ms: Vec<u64>,
    lifecycle_snapshots: Vec<serde_json::Value>,
    discovery_query_count: u64,
    discovery_nonempty_count: u64,
    claim_batches: u64,
    finalize_batches: u64,
    claim_finalize_operation_ns: u64,
}

fn single_deployment_evidence_row(
    tag: &str,
    full_shape: bool,
    passed: bool,
    environment: String,
    pass_bar: &str,
    mut values: BTreeMap<String, serde_json::Value>,
) -> pqueue_release::LedgerRow {
    let evidence_id = match tag {
        "e0" => "E0",
        "e1" => "E1",
        other => panic!("unsupported single-deployment evidence tag {other}"),
    };
    values.insert("bars_met".into(), serde_json::json!(passed));
    pqueue_release::LedgerRow {
        suite: "performance_single_deployment_baseline_tests".into(),
        command: "PQUEUE_PERF_ENV=1 PQUEUE_E1_RESIDENT=10000000 PQUEUE_E0E1_PROGRESS_BOUND_MS=<declared> PQUEUE_PG_TEST_URL=… cargo test -p pqueue-postgres --test performance_single_deployment_baseline_tests".into(),
        backend_profile: "postgres_native".into(),
        scale: if full_shape { "release".into() } else { "baseline".into() },
        seed: 0,
        environment,
        exit_status: 0,
        ac_ids: vec![],
        inv_ids: vec![],
        pass_bar: pass_bar.into(),
        evidence_tier: if passed { "release".into() } else { "smoke".into() },
        measurements: pqueue_release::Measurements {
            tp002_evidence_ids: vec![evidence_id.into()],
            values,
        },
    }
}

#[test]
fn passing_e0_row_is_exact_governed_release_evidence() {
    let row = single_deployment_evidence_row(
        "e0",
        true,
        true,
        "declared topology under ordinary load".into(),
        "portable contract",
        BTreeMap::new(),
    );
    assert_eq!(row.scale, "release");
    assert_eq!(row.evidence_tier, "release");
    assert_eq!(row.measurements.tp002_evidence_ids, ["E0"]);
    assert_eq!(
        row.measurements.values.get("bars_met"),
        Some(&serde_json::json!(true))
    );
}

#[test]
fn nonpassing_e0_row_remains_identified_smoke_evidence() {
    let row = single_deployment_evidence_row(
        "e0",
        false,
        false,
        "local smoke".into(),
        "portable contract",
        BTreeMap::new(),
    );
    assert_eq!(row.scale, "baseline");
    assert_eq!(row.evidence_tier, "smoke");
    assert_eq!(row.measurements.tp002_evidence_ids, ["E0"]);
    assert_eq!(
        row.measurements.values.get("bars_met"),
        Some(&serde_json::json!(false))
    );
}

#[test]
fn production_wrapper_batches_10k_through_native_ports() {
    let Ok(observer_url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!("POSTGRES WRAPPER 10K BATCH PROOF SKIPPED — set PQUEUE_PG_TEST_URL");
        return;
    };
    let schema = fresh_schema();
    let application_name = format!("pqueue_e0_batch_proof_{}", std::process::id());
    let separator = if observer_url.contains('?') { '&' } else { '?' };
    let pool_url = format!("{observer_url}{separator}application_name={application_name}_pool");
    let backend =
        fixed_postgres_relational_pool(PostgresConnectConfig::new(pool_url), Some(&schema), 2, 0)
            .expect("connect production pool");
    let pq = Pqueue::new(backend, Arc::new(SystemClock));
    let shard = sk("batch-proof", "hot");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build batch-proof runtime");
    runtime.block_on(async {
        pq.create_queue(big_qdef("batch-proof", "hot", 60_000))
            .await
            .unwrap();
        let operations = OperationProbe::default();
        let mut push_sizes = Vec::new();
        for base in (0..10_000).step_by(1_000) {
            let ids = push_batch(&pq, &shard, base, 1_000, &operations).await;
            push_sizes.push(ids.len());
        }
        let mut claim_sizes = Vec::new();
        let mut finalize_sizes = Vec::new();
        while claim_sizes.iter().sum::<usize>() < 10_000 {
            let ids = claim(&pq, &shard, 1_000, &operations).await;
            assert!(!ids.is_empty(), "10K batch proof must keep making progress");
            claim_sizes.push(ids.len());
            finalize(&pq, &shard, &ids, &operations).await;
            finalize_sizes.push(ids.len());
        }
        assert_eq!(push_sizes, vec![1_000; 10]);
        assert_eq!(claim_sizes, vec![1_000; 10]);
        assert_eq!(finalize_sizes, vec![1_000; 10]);
        let metrics = pq.metrics(&shard).await.unwrap();
        assert_eq!(
            (metrics.pending, metrics.leased, metrics.complete),
            (0, 0, 10_000)
        );
    });
}

#[test]
fn performance_single_deployment_baseline_tests() {
    let Ok(observer_url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!(
            "POSTGRES E0/E1 SINGLE-DEPLOYMENT BASELINE SKIPPED — set PQUEUE_PG_TEST_URL to a live DB. \
             Portable correctness, configured-progress, bounded-resource, and capacity evidence is \
             DEFERRED (not measured), not a hidden pass."
        );
        return;
    };
    // A designated PERF environment may emit RELEASE-tier evidence. Without it, this is a SMOKE lane that
    // measures the same invariants but never satisfies a release gate. Host speed never decides either lane.
    let perf_env = std::env::var("PQUEUE_PERF_ENV").is_ok();
    let progress_bound_value = std::env::var("PQUEUE_E0E1_PROGRESS_BOUND_MS");
    let progress_bound_explicit = progress_bound_value.is_ok();
    let progress_bound_ms = match progress_bound_value {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
            .expect("PQUEUE_E0E1_PROGRESS_BOUND_MS must be a positive integer"),
        Err(_) if perf_env => {
            panic!("release evidence requires explicit PQUEUE_E0E1_PROGRESS_BOUND_MS")
        }
        Err(_) => SMOKE_PROGRESS_BOUND_MS,
    };
    // Small fast defaults (the relational backend issues per-item INSERT round-trips, so large batches over a
    // network bridge are slow); the real release shape is env-scaled. Default resident keeps a routine run short.
    let resident: u64 = std::env::var("PQUEUE_E1_RESIDENT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000);
    let load_batch = CONFIGURED_MAX_BATCH_SIZE;
    // Latency probe batch sizes: [1, 100] by default; the full release shape (+1000) needs PQUEUE_E1_FULL.
    let full = perf_env || std::env::var("PQUEUE_E1_FULL").is_ok();
    let batch_sizes: &[u64] = if full { &[1, 100, 1000] } else { &[1, 100] };

    let schema = fresh_schema();
    let shard = sk("e0e1", "hot");
    let application_name = format!("pqueue_e0e1_evidence_{}", std::process::id());
    let separator = if observer_url.contains('?') { '&' } else { '?' };
    let pool_url = format!("{observer_url}{separator}application_name={application_name}_pool");
    let b: Arc<PostgresWholeOperationAdapter<PostgresRelationalBackend>> =
        fixed_postgres_relational_pool(PostgresConnectConfig::new(pool_url), Some(&schema), 2, 0)
            .expect("connect fixed postgres production pool");
    let pq = Arc::new(Pqueue::new(Arc::clone(&b), Arc::new(SystemClock)));
    let operations = Arc::new(OperationProbe::default());
    let workers = Arc::new(WorkerProbe::default());
    let caps = RunCaps::from_env(perf_env);
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("build benchmark runtime"),
    );

    runtime.clone().block_on(async {
        pq.create_queue(big_qdef("e0e1", "hot", progress_bound_ms))
            .await
            .unwrap();
        let persisted = b.queue_definition(&shard).await.unwrap();
        assert_eq!(persisted.max_push_batch_size, CONFIGURED_MAX_BATCH_SIZE);
        assert_eq!(persisted.max_claim_batch_size, CONFIGURED_MAX_BATCH_SIZE);
        assert_eq!(persisted.progress_bound_ms, progress_bound_ms);
        let consumer_pq = Arc::clone(&pq);

        let hot_pool_partition = pqueue_engine::queue_worker_partition(&shard, 2);
        let (canary_queue_id, canary_shard, canary_pool_partition) = (0..100)
            .map(|index| format!("canary-{index}"))
            .map(|queue_id| {
                let key = sk("e0e1", &queue_id);
                let partition = pqueue_engine::queue_worker_partition(&key, 2);
                (queue_id, key, partition)
            })
            .find(|(_, _, partition)| *partition != hot_pool_partition)
            .expect("two queue keys must cover both production pool members");
        pq.create_queue(big_qdef("e0e1", &canary_queue_id, progress_bound_ms))
            .await
            .unwrap();

        // The first hot insert cannot finish until the affinity-routed canary reaches the other pool
        // member and removes this row. This makes canary progress causal, not a host-speed comparison.
        let gate_url = observer_url.clone();
        let gate_schema = schema.clone();
        let gate_canary = canary_queue_id.clone();
        std::thread::spawn(move || {
            let mut gate_admin = postgres::Client::connect(&gate_url, postgres::NoTls)
                .expect("connect causal-gate observer");
            gate_admin
                .batch_execute(&format!(
                "SET search_path TO {schema};
                 CREATE TABLE e0_pool_hold(queue_id TEXT PRIMARY KEY);
                 INSERT INTO e0_pool_hold(queue_id) VALUES('hot');
                 CREATE FUNCTION e0_pool_causal_gate() RETURNS trigger LANGUAGE plpgsql AS $$
                 BEGIN
                   IF NEW.queue_id = 'hot' THEN
                     WHILE EXISTS (SELECT 1 FROM e0_pool_hold WHERE queue_id = 'hot') LOOP
                       PERFORM pg_sleep(0.01);
                     END LOOP;
                   ELSIF NEW.queue_id = '{gate_canary}' THEN
                     DELETE FROM e0_pool_hold WHERE queue_id = 'hot';
                   END IF;
                   RETURN NEW;
                 END $$;
                 CREATE TRIGGER e0_pool_causal_gate BEFORE INSERT ON pqueue_items
                   FOR EACH ROW EXECUTE FUNCTION e0_pool_causal_gate();",
                schema = gate_schema,
            ))
                .expect("install causal production-pool gate");
        })
        .join()
        .expect("causal-gate setup thread");

        // E0: one fixed two-member production pool. Stable affinity serializes the hot queue on one member;
        // the causally gated canary proves an unrelated queue progresses on the other member under load.
        let pending = Arc::new(Mutex::new(HashMap::<ItemId, (Instant, Instant)>::new()));
        let pending_peak = Arc::new(AtomicU64::new(0));
        let producer_done = Arc::new(AtomicBool::new(false));
        let consumer_done = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let (signal_tx, signal_rx) = std::sync::mpsc::sync_channel::<()>(2);

        let (producer_result, consumer_result, canary_causal_progress, mut resources) =
            std::thread::scope(|scope| {
                let producer = scope.spawn({
                    let pq = Arc::clone(&pq);
                    let shard = shard.clone();
                    let operations = Arc::clone(&operations);
                    let workers = Arc::clone(&workers);
                    let pending = Arc::clone(&pending);
                    let pending_peak = Arc::clone(&pending_peak);
                let producer_done = Arc::clone(&producer_done);
                let barrier = Arc::clone(&barrier);
                let runtime = Arc::clone(&runtime);
                move || {
                    let _runtime = runtime.enter();
                    let _worker = workers.start();
                        barrier.wait();
                        let mut result = ProducerResult {
                            accepted: 0,
                            duplicate_ids: 0,
                            seen_ids: vec![false; resident as usize + 2],
                            identity_prefix: None,
                            counter_min: u64::MAX,
                            counter_max: 0,
                            payload_size_counts: BTreeMap::new(),
                            group_counts: vec![0; GROUP_CARDINALITY as usize],
                            priority_counts: BTreeMap::new(),
                            push_batches: 0,
                            push_operation_ns: 0,
                        };
                        while result.accepted < resident {
                            let n = (resident - result.accepted).min(load_batch);
                            let accept_started = Instant::now();
                            let ids = futures::executor::block_on(push_batch(
                                &pq,
                                &shard,
                                result.accepted,
                                n,
                                &operations,
                            ));
                            let accept_completed = Instant::now();
                            result.push_operation_ns +=
                                accept_completed.duration_since(accept_started).as_nanos() as u64;
                            {
                                let mut map = pending.lock().unwrap();
                                for id in &ids {
                                    map.insert(*id, (accept_started, accept_completed));
                                }
                                pending_peak.fetch_max(map.len() as u64, Ordering::SeqCst);
                            }
                            for offset in 0..n {
                                let sequence = result.accepted + offset;
                                *result
                                    .payload_size_counts
                                    .entry([512, 1024, 2048][sequence as usize % 3] as u64)
                                    .or_default() += 1;
                                result.group_counts
                                    [sequence as usize % GROUP_CARDINALITY as usize] += 1;
                                let class = if offset + 1 == n {
                                    "sentinel"
                                } else if sequence.is_multiple_of(10) {
                                    "high"
                                } else {
                                    "regular"
                                };
                                *result.priority_counts.entry(class.into()).or_default() += 1;
                            }
                            for id in ids {
                                let raw = id.as_u64();
                                let prefix = raw >> 32;
                                if result
                                    .identity_prefix
                                    .replace(prefix)
                                    .is_some_and(|seen| seen != prefix)
                                {
                                    result.duplicate_ids += 1;
                                }
                                let counter = (id.as_u64() & u32::MAX as u64) as usize;
                                result.counter_min = result.counter_min.min(counter as u64);
                                result.counter_max = result.counter_max.max(counter as u64);
                                if counter >= result.seen_ids.len() || result.seen_ids[counter] {
                                    result.duplicate_ids += 1;
                                } else {
                                    result.seen_ids[counter] = true;
                                }
                            }
                            result.accepted += n;
                            result.push_batches += 1;
                            signal_tx.send(()).expect("claimant remains live");
                        }
                        producer_done.store(true, Ordering::SeqCst);
                        result
                    }
                });

                let consumer = scope.spawn({
                let pq = Arc::clone(&consumer_pq);
                let shard = shard.clone();
                let operations = Arc::clone(&operations);
                let workers = Arc::clone(&workers);
                let pending = Arc::clone(&pending);
                let producer_done = Arc::clone(&producer_done);
                let consumer_done = Arc::clone(&consumer_done);
                let barrier = Arc::clone(&barrier);
                let runtime = Arc::clone(&runtime);
                move || {
                    let _runtime = runtime.enter();
                    let _worker = workers.start();
                    barrier.wait();
                    let mut result = ConsumerResult {
                        claimed: 0,
                        finalized: 0,
                        duplicate_claims: 0,
                        seen_ids: vec![false; resident as usize + 2],
                        identity_prefix: None,
                        counter_min: u64::MAX,
                        counter_max: 0,
                        progress_identity_sample_count: 0,
                        progress_latency_lower_max_ms: 0,
                        progress_latency_upper_max_ms: 0,
                        progress_latency_over_60000_ms_count: 0,
                        progress_bound_violations: 0,
                        progress_latency_upper_buckets: BTreeMap::from([
                            ("le_1000".into(), 0),
                            ("le_10000".into(), 0),
                            ("le_60000".into(), 0),
                            ("gt_60000".into(), 0),
                        ]),
                        finalized_samples: vec![0],
                        cursor_samples: vec![0],
                        oldest_eligible_age_samples_ms: Vec::new(),
                        lifecycle_snapshots: Vec::new(),
                        discovery_query_count: 0,
                        discovery_nonempty_count: 0,
                        claim_batches: 0,
                        finalize_batches: 0,
                        claim_finalize_operation_ns: 0,
                    };
                    let sample_stride = resident.clamp(1, 100_000);
                    let mut next_scope_sample = 0;
                    loop {
                        let _ = signal_rx.try_recv();
                        if result.claimed >= next_scope_sample
                            && !pending.lock().unwrap().is_empty()
                        {
                            let scopes = futures::executor::block_on(
                                pq.discover_active_scopes(&shard, DiscoveryGranularity::Group),
                            )
                            .unwrap();
                            result.discovery_query_count += 1;
                            result.discovery_nonempty_count += u64::from(!scopes.is_empty());
                            result.oldest_eligible_age_samples_ms.extend(
                                scopes.iter().map(|scope| scope.oldest_eligible_age_ms),
                            );
                            next_scope_sample += sample_stride;
                        }
                        let claim_started = Instant::now();
                        let ids = futures::executor::block_on(claim(
                            &pq,
                            &shard,
                            load_batch as usize,
                            &operations,
                        ));
                        let claim_completed = Instant::now();
                        if ids.is_empty() {
                            if producer_done.load(Ordering::SeqCst)
                                && pending.lock().unwrap().is_empty()
                            {
                                break;
                            }
                            let _ = signal_rx.recv_timeout(std::time::Duration::from_millis(10));
                            continue;
                        }
                        result.claim_finalize_operation_ns +=
                            claim_completed.duration_since(claim_started).as_nanos() as u64;
                        result.claim_batches += 1;
                        for id in &ids {
                            let identity_deadline = Instant::now()
                                + std::time::Duration::from_millis(progress_bound_ms);
                            let accepted = loop {
                                if let Some(interval) = pending.lock().unwrap().remove(id) {
                                    break interval;
                                }
                                assert!(
                                    Instant::now() < identity_deadline,
                                    "claimed phantom or duplicate identity {id} without a matching accepted interval"
                                );
                                std::thread::yield_now();
                            };
                            let lower = claim_started
                                .checked_duration_since(accepted.1)
                                .unwrap_or_default()
                                .as_millis() as u64;
                            let upper = claim_completed
                                .checked_duration_since(accepted.0)
                                .unwrap_or_default()
                                .as_millis() as u64;
                            result.progress_identity_sample_count += 1;
                            result.progress_latency_lower_max_ms =
                                result.progress_latency_lower_max_ms.max(lower);
                            result.progress_latency_upper_max_ms =
                                result.progress_latency_upper_max_ms.max(upper);
                            result.progress_latency_over_60000_ms_count +=
                                u64::from(upper > 60_000);
                            result.progress_bound_violations +=
                                u64::from(upper > progress_bound_ms);
                            let bucket = if upper <= 1_000 {
                                "le_1000"
                            } else if upper <= 10_000 {
                                "le_10000"
                            } else if upper <= 60_000 {
                                "le_60000"
                            } else {
                                "gt_60000"
                            };
                            *result
                                .progress_latency_upper_buckets
                                .entry(bucket.into())
                                .or_default() += 1;
                            let counter = (id.as_u64() & u32::MAX as u64) as usize;
                            let prefix = id.as_u64() >> 32;
                            if result.identity_prefix.replace(prefix).is_some_and(|seen| seen != prefix) {
                                result.duplicate_claims += 1;
                            }
                            result.counter_min = result.counter_min.min(counter as u64);
                            result.counter_max = result.counter_max.max(counter as u64);
                            if counter >= result.seen_ids.len() || result.seen_ids[counter] {
                                result.duplicate_claims += 1;
                            } else {
                                result.seen_ids[counter] = true;
                            }
                        }
                        result.claimed += ids.len() as u64;
                        let finalize_started = Instant::now();
                        futures::executor::block_on(finalize(&pq, &shard, &ids, &operations));
                        result.claim_finalize_operation_ns +=
                            finalize_started.elapsed().as_nanos() as u64;
                        result.finalized += ids.len() as u64;
                        result.finalize_batches += 1;
                        if result.finalized.is_multiple_of(100_000) || result.finalized == resident
                        {
                            let metrics = futures::executor::block_on(pq.metrics(&shard)).unwrap();
                            let cursor = futures::executor::block_on(pq.current_position(&shard))
                                .unwrap()
                                .sequence;
                            let scopes = futures::executor::block_on(
                                pq.discover_active_scopes(&shard, DiscoveryGranularity::Group),
                            )
                            .unwrap();
                            result.discovery_query_count += 1;
                            result.discovery_nonempty_count += u64::from(!scopes.is_empty());
                            result.oldest_eligible_age_samples_ms.extend(
                                scopes.iter().map(|scope| scope.oldest_eligible_age_ms),
                            );
                            result.finalized_samples.push(result.finalized);
                            result.cursor_samples.push(cursor);
                            result.lifecycle_snapshots.push(serde_json::json!({
                                "pending": metrics.pending,
                                "leased": metrics.leased,
                                "complete": metrics.complete,
                                "failed": metrics.failed,
                                "resident_terminal_count": metrics.resident_terminal_count,
                                "cursor": cursor,
                            }));
                        }
                    }
                    consumer_done.store(true, Ordering::SeqCst);
                    result
                }
            });

                let canary = scope.spawn({
                let pq = Arc::clone(&pq);
                let canary_shard = canary_shard.clone();
                let producer_done = Arc::clone(&producer_done);
                let barrier = Arc::clone(&barrier);
                let observer_url = observer_url.clone();
                let application_name = application_name.clone();
                let runtime = Arc::clone(&runtime);
                move || {
                    let _runtime = runtime.enter();
                    barrier.wait();
                    let mut observer = postgres::Client::connect(&observer_url, postgres::NoTls)
                        .expect("connect canary causal observer");
                    let observed_hot_sleep = loop {
                        let sleeping: bool = observer
                            .query_one(
                                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name LIKE $1 AND wait_event='PgSleep')",
                                &[&format!("{application_name}%")],
                            )
                            .expect("observe hot production pool member")
                            .get(0);
                        if sleeping {
                            break true;
                        }
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    };
                    assert!(observed_hot_sleep, "hot queue never reached causal pg_sleep gate");
                    let canary_ops = OperationProbe::default();
                    let ids = futures::executor::block_on(push_batch(
                        &pq,
                        &canary_shard,
                        resident + 10_000,
                        1,
                        &canary_ops,
                    ));
                    let claimed = futures::executor::block_on(claim(
                        &pq,
                        &canary_shard,
                        1,
                        &canary_ops,
                    ));
                    futures::executor::block_on(finalize(
                        &pq,
                        &canary_shard,
                        &claimed,
                        &canary_ops,
                    ));
                    observed_hot_sleep
                        && ids == claimed
                        && ids.len() == 1
                        && !producer_done.load(Ordering::SeqCst)
                }
            });

                let sampler = scope.spawn({
                    let consumer_done = Arc::clone(&consumer_done);
                    let observer_url = observer_url.clone();
                    let application_name = application_name.clone();
                    move || {
                        let mut observations = ResourceObservations::default();
                        while !consumer_done.load(Ordering::SeqCst) || observations.samples < 3 {
                            observations.sample(&observer_url, &application_name);
                            std::thread::sleep(std::time::Duration::from_millis(25));
                        }
                        observations
                    }
                });
                (
                    producer.join().unwrap(),
                    consumer.join().unwrap(),
                    canary.join().unwrap(),
                    sampler.join().unwrap(),
                )
            });
        let accepted = producer_result.accepted;
        let claimed = consumer_result.claimed;
        let finalized = consumer_result.finalized;
        let duplicate_claims = consumer_result.duplicate_claims;
        let mut finalized_samples = consumer_result.finalized_samples;
        let mut cursor_samples = consumer_result.cursor_samples;
        let oldest_eligible_age_samples_ms = consumer_result.oldest_eligible_age_samples_ms;
        let producer_ingest_completion_per_s =
            accepted as f64 / (producer_result.push_operation_ns as f64 / 1_000_000_000.0);
        let claimant_finalize_completion_per_s = finalized as f64
            / (consumer_result.claim_finalize_operation_ns as f64 / 1_000_000_000.0);
        let producer_completion_ms = producer_result.push_operation_ns as f64 / 1_000_000.0;
        let claimant_completion_ms =
            consumer_result.claim_finalize_operation_ns as f64 / 1_000_000.0;
        let checkpoint = pq.metrics(&shard).await.unwrap();
        finalized_samples.push(finalized);
        cursor_samples.push(pq.current_position(&shard).await.unwrap().sequence);
        let sample_url = observer_url.clone();
        let sample_application = application_name.clone();
        resources = std::thread::spawn(move || {
            resources.sample(&sample_url, &sample_application);
            resources
        })
        .join()
        .expect("resource sample thread");
        assert_eq!(
            (
                checkpoint.pending,
                checkpoint.leased,
                checkpoint.complete,
                checkpoint.failed
            ),
            (0, 0, resident, 0)
        );
        assert_eq!(checkpoint.resident_terminal_count, resident);
        assert_eq!(
            (accepted, claimed, finalized, duplicate_claims),
            (resident, resident, resident, 0)
        );
        assert_eq!(producer_result.duplicate_ids, 0);
        assert_eq!(producer_result.seen_ids, consumer_result.seen_ids);
        assert!(pending.lock().unwrap().is_empty());

        // E1 representative control/load/control probes against the retained ten-million-row state.
        let base_cycles: usize = std::env::var("PQUEUE_E1_CYCLES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(if perf_env { 500 } else { 20 });
        let mut lat: BTreeMap<String, Vec<f64>> = BTreeMap::new();
        let mut samples: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        let mut next_id = resident;
        let mut probe_accepted = 0u64;
        let mut probe_claimed = 0u64;
        let mut probe_finalized = 0u64;
        let mut probe_push_batches = 0u64;
        let mut probe_update_item_calls = 0u64;
        let mut probe_claim_batches = 0u64;
        let mut probe_finalize_batches = 0u64;
        let mut actual_push_sizes = BTreeSet::new();
        let mut actual_update_window_sizes = BTreeSet::new();
        let mut actual_claim_sizes = BTreeSet::new();
        let mut actual_finalize_sizes = BTreeSet::new();
        let mut probe_accepted_ids = BTreeSet::new();
        let mut probe_claimed_ids = BTreeSet::new();
        let mut probe_finalized_ids = BTreeSet::new();

        // At the exact 10M resident checkpoint, submit a max-batch write and claim/finalize concurrently
        // to the same hot queue. Caller intervals overlap, while stable affinity and the queue gate serialize
        // their database transactions on the hot member. The separate canary proves cross-queue progress.
        let control_seed = push_batch(&pq, &shard, next_id, 1_000, &operations).await;
        next_id += 1_000;
        probe_push_batches += 1;
        probe_accepted += 1_000;
        probe_accepted_ids.extend(control_seed.iter().copied());
        for id in &control_seed {
            let mut ops = BTreeMap::new();
            ops.insert("probe".into(), Some(Bytes::from_static(b"updated")));
            pq.update_fields(&shard, *id, ops, PayloadUpdate::Keep, None, None)
                .await
                .expect("post-10M update_fields");
            probe_update_item_calls += 1;
        }
        let active_pending_before = pq.metrics(&shard).await.unwrap().pending;
        let control_barrier = Arc::new(std::sync::Barrier::new(2));
        let control_operations = Arc::new(OperationProbe::default());
        let (
            (write_started, write_completed, control_write),
            (claim_started, claim_completed, first_claim),
        ) = std::thread::scope(|scope| {
            let barrier = Arc::clone(&control_barrier);
            let producer_operations = Arc::clone(&control_operations);
            let producer_pq = Arc::clone(&pq);
            let producer_shard = shard.clone();
            let producer_runtime = Arc::clone(&runtime);
            let producer = scope.spawn(move || {
                let _runtime = producer_runtime.enter();
                barrier.wait();
                let started = Instant::now();
                let ids = futures::executor::block_on(push_batch(
                    &producer_pq,
                    &producer_shard,
                    next_id,
                    1_000,
                    &producer_operations,
                ));
                (started, Instant::now(), ids)
            });
            let barrier = Arc::clone(&control_barrier);
            let claimant_operations = Arc::clone(&control_operations);
            let claimant_pq = Arc::clone(&consumer_pq);
            let claimant_shard = shard.clone();
            let claimant_runtime = Arc::clone(&runtime);
            let claimant = scope.spawn(move || {
                let _runtime = claimant_runtime.enter();
                barrier.wait();
                let started = Instant::now();
                let ids = futures::executor::block_on(claim(
                    &claimant_pq,
                    &claimant_shard,
                    1_000,
                    &claimant_operations,
                ));
                futures::executor::block_on(finalize(
                    &claimant_pq,
                    &claimant_shard,
                    &ids,
                    &claimant_operations,
                ));
                (started, Instant::now(), ids)
            });
            (producer.join().unwrap(), claimant.join().unwrap())
        });
        next_id += 1_000;
        probe_push_batches += 1;
        probe_claim_batches += 1;
        probe_finalize_batches += 1;
        probe_accepted += control_write.len() as u64;
        probe_claimed += first_claim.len() as u64;
        probe_finalized += first_claim.len() as u64;
        probe_accepted_ids.extend(control_write.iter().copied());
        probe_claimed_ids.extend(first_claim.iter().copied());
        probe_finalized_ids.extend(first_claim.iter().copied());
        let second_claim = claim(&consumer_pq, &shard, 1_000, &operations).await;
        finalize(&consumer_pq, &shard, &second_claim, &operations).await;
        probe_claim_batches += 1;
        probe_finalize_batches += 1;
        probe_claimed += second_claim.len() as u64;
        probe_finalized += second_claim.len() as u64;
        probe_claimed_ids.extend(second_claim.iter().copied());
        probe_finalized_ids.extend(second_claim.iter().copied());
        let post10m_affinity_serialization_probe = active_pending_before == 1_000
            && control_write.len() == 1_000
            && first_claim.len() == 1_000
            && second_claim.len() == 1_000
            && write_started < claim_completed
            && claim_started < write_completed
            && control_operations.max_observed() >= 2;
        let post10m_caller_interval_overlap_observed =
            write_started < claim_completed && claim_started < write_completed;
        actual_push_sizes.insert(1_000);
        actual_update_window_sizes.insert(1_000);
        actual_claim_sizes.insert(1_000);
        actual_finalize_sizes.insert(1_000);

        for &bsz in batch_sizes {
            let cycles = if bsz == 1 {
                base_cycles
            } else {
                (base_cycles / 4).max(8)
            };
            samples.insert(format!("samples_per_op_b{bsz}"), serde_json::json!(cycles));
            for _ in 0..cycles {
                probe_push_batches += 1;
                let t = Instant::now();
                let pushed_ids = push_batch(&pq, &shard, next_id, bsz, &operations).await;
                lat.entry(format!("push_b{bsz}_ms"))
                    .or_default()
                    .push(ms(t));
                next_id += bsz;
                probe_accepted += pushed_ids.len() as u64;
                probe_accepted_ids.extend(pushed_ids.iter().copied());
                actual_push_sizes.insert(pushed_ids.len() as u64);

                let t = Instant::now();
                for id in &pushed_ids {
                    let mut ops = BTreeMap::new();
                    ops.insert("probe".into(), Some(Bytes::from_static(b"updated")));
                    pq.update_fields(&shard, *id, ops, PayloadUpdate::Keep, None, None)
                        .await
                        .expect("facade update_fields");
                    probe_update_item_calls += 1;
                }
                lat.entry(format!("update_window_b{bsz}_ms"))
                    .or_default()
                    .push(ms(t));
                actual_update_window_sizes.insert(pushed_ids.len() as u64);

                let t = Instant::now();
                let ids = claim(&pq, &shard, bsz as usize, &operations).await;
                lat.entry(format!("claim_b{bsz}_ms"))
                    .or_default()
                    .push(ms(t));
                assert_eq!(
                    ids.len() as u64,
                    bsz,
                    "claim must return the requested batch"
                );
                probe_claimed += ids.len() as u64;
                probe_claim_batches += 1;
                probe_claimed_ids.extend(ids.iter().copied());
                actual_claim_sizes.insert(ids.len() as u64);

                let t = Instant::now();
                finalize(&pq, &shard, &ids, &operations).await;
                lat.entry(format!("finalize_b{bsz}_ms"))
                    .or_default()
                    .push(ms(t));
                probe_finalized += ids.len() as u64;
                probe_finalize_batches += 1;
                probe_finalized_ids.extend(ids.iter().copied());
                actual_finalize_sizes.insert(ids.len() as u64);
            }
            let metrics = pq.metrics(&shard).await.unwrap();
            assert_eq!(metrics.pending, 0, "probe cycles must fully finalize");
            let sample_url = observer_url.clone();
            let sample_application = application_name.clone();
            resources = std::thread::spawn(move || {
                resources.sample(&sample_url, &sample_application);
                resources
            })
            .join()
            .expect("resource sample thread");
        }
        let oversize_push_rejected = matches!(
            pq.push_batch(
                &shard,
                (0..=CONFIGURED_MAX_BATCH_SIZE)
                    .map(|n| representative_item(next_id + n, false))
                    .collect(),
            )
            .await,
            Err(EngineError::BatchTooLarge)
        );
        let oversize_claim_rejected = matches!(
            pq.claim(&shard, CONFIGURED_MAX_BATCH_SIZE as usize + 1, 1_000)
                .await,
            Err(EngineError::BatchTooLarge)
        );
        let post_probe = pq.metrics(&shard).await.unwrap();
        let probe_identity_exact = probe_accepted_ids.len() as u64 == probe_accepted
            && probe_accepted_ids == probe_claimed_ids
            && probe_claimed_ids == probe_finalized_ids;

        let monotonic_progress = finalized_samples.first() == Some(&0)
            && finalized_samples.last() == Some(&resident)
            && finalized_samples
                .windows(2)
                .all(|window| window[1] >= window[0])
            && finalized_samples
                .windows(2)
                .any(|window| window[1] > window[0])
            && cursor_samples.windows(2).all(|w| w[1] >= w[0])
            && cursor_samples.windows(2).any(|w| w[1] > w[0]);
        let progress_evidence_complete = consumer_result.progress_identity_sample_count == resident
            && consumer_result
                .progress_latency_upper_buckets
                .values()
                .sum::<u64>()
                == resident
            && consumer_result.discovery_query_count > 0
            && consumer_result.discovery_nonempty_count > 0
            && !oldest_eligible_age_samples_ms.is_empty()
            && consumer_result.progress_bound_violations == 0
            && consumer_result.progress_latency_upper_max_ms <= progress_bound_ms
            && oldest_eligible_age_samples_ms
                .iter()
                .all(|value| *value <= progress_bound_ms);
        let max_in_flight = operations.max_observed();
        let pending_items_peak = pending_peak.load(Ordering::SeqCst);
        let bounded_resources = resources.within_bounds(max_in_flight, pending_items_peak, caps)
            && workers.peak.load(Ordering::SeqCst) <= caps.workers
            && workers.started.load(Ordering::SeqCst) == CONFIGURED_CONCURRENCY
            && workers.completed.load(Ordering::SeqCst) == CONFIGURED_CONCURRENCY;
        assert!(
            monotonic_progress,
            "measured finalized-count and command-position samples must advance monotonically"
        );
        assert!(
            progress_evidence_complete,
            "identity/discovery evidence failed declared {}ms progress bound: identities={} violations={} upper_max={} oldest_samples={:?}",
            progress_bound_ms,
            consumer_result.progress_identity_sample_count,
            consumer_result.progress_bound_violations,
            consumer_result.progress_latency_upper_max_ms,
            oldest_eligible_age_samples_ms,
        );
        assert!(
            bounded_resources,
            "measured process/database resources exceeded declared bounds"
        );

        // ----- Percentiles -----
        let mut p50 = std::collections::BTreeMap::new();
        let mut p95 = std::collections::BTreeMap::new();
        let mut p99 = std::collections::BTreeMap::new();
        let mut worst_p99 = 0.0f64;
        for (k, v) in lat.iter_mut() {
            worst_p99 = worst_p99.max(pct(v, 0.99));
            p50.insert(
                k.replace("_ms", "_p50_ms"),
                (pct(v, 0.50) * 1000.0).round() / 1000.0,
            );
            p95.insert(
                k.replace("_ms", "_p95_ms"),
                (pct(v, 0.95) * 1000.0).round() / 1000.0,
            );
            p99.insert(
                k.replace("_ms", "_p99_ms"),
                (pct(v, 0.99) * 1000.0).round() / 1000.0,
            );
        }

        // Wall-clock results are topology-bound capacity observations. Release
        // eligibility is the portable correctness/progress/resource contract.
        let full_shape = resident == 10_000_000
            && batch_sizes == [1, 100, CONFIGURED_MAX_BATCH_SIZE]
            && batch_sizes.last() == Some(&CONFIGURED_MAX_BATCH_SIZE);
        let source_revision = std::env::var("PQUEUE_SOURCE_REVISION").unwrap_or_default();
        let (
            checkout_root,
            checkout_revision,
            checkout_clean,
            compile_source_root,
            compile_source_root_bound,
        ) = checkout_provenance();
        let source_root_explicit = std::env::var_os("PQUEUE_SOURCE_ROOT").is_some();
        let revision_bound = source_revision == checkout_revision
            && checkout_clean
            && source_root_explicit
            && compile_source_root_bound;
        let topology_declared = caps.explicitly_declared
            && [
                "PQUEUE_PG_INSTANCE_CLASS",
                "PQUEUE_PG_CPU_LIMIT",
                "PQUEUE_PG_MEMORY_LIMIT_BYTES",
                "PQUEUE_PG_IOPS_PROFILE",
                "PQUEUE_PG_STORAGE_CLASS",
            ]
            .iter()
            .all(|key| std::env::var(key).is_ok());
        let exact_outcomes = accepted == resident
            && claimed == resident
            && finalized == resident
            && duplicate_claims == 0
            && checkpoint.pending == 0
            && checkpoint.leased == 0
            && checkpoint.complete == resident
            && checkpoint.failed == 0
            && producer_result.duplicate_ids == 0
            && producer_result.seen_ids == consumer_result.seen_ids
            && producer_result.identity_prefix == consumer_result.identity_prefix
            && producer_result.counter_min == consumer_result.counter_min
            && producer_result.counter_max == consumer_result.counter_max
            && producer_result.counter_max - producer_result.counter_min + 1 == resident;
        let e0_pass = full_shape
            && revision_bound
            && topology_declared
            && exact_outcomes
            && monotonic_progress
            && progress_evidence_complete
            && bounded_resources
            && canary_causal_progress
            && resources.max_connections == 2;
        let e1_pass = e0_pass
            && probe_accepted == probe_claimed
            && probe_claimed == probe_finalized
            && probe_identity_exact
            && post_probe.pending == 0
            && post_probe.leased == 0
            && post_probe.complete == resident + probe_finalized
            && post_probe.failed == 0
            && actual_push_sizes.iter().copied().collect::<Vec<_>>() == [1, 100, 1000]
            && actual_update_window_sizes
                .iter()
                .copied()
                .collect::<Vec<_>>()
                == [1, 100, 1000]
            && actual_claim_sizes.iter().copied().collect::<Vec<_>>() == [1, 100, 1000]
            && actual_finalize_sizes.iter().copied().collect::<Vec<_>>() == [1, 100, 1000]
            && oversize_push_rejected
            && oversize_claim_rejected
            && post10m_affinity_serialization_probe;

        println!(
            "\nTP-002 E0/E1 postgres_native single-deployment baseline (resident={resident}, perf_env={perf_env}):"
        );
        println!("  producer ingest capacity     : {producer_ingest_completion_per_s:.0} items/s");
        println!(
            "  claimant/finalize capacity   : {claimant_finalize_completion_per_s:.0} items/s"
        );
        println!("  worst operation p99 capacity : {worst_p99:.1} ms");
        println!("  release evidence contract    : {e1_pass}");
        if full_shape {
            assert!(
                revision_bound,
                "full E0/E1 producer requires PQUEUE_SOURCE_REVISION=<exact HEAD>"
            );
        }

        // ----- Emit E0 and E1 ledger rows from the REAL measured values -----
        // The fixed two-member pool is a declared shared-resource bound. The hot queue uses one affinity
        // member; the causal cross-queue canary proves the other member remains live under hot load.
        // RELEASE-tier only when a perf env actually met the bar; otherwise SMOKE (recorded, gate-visible, but
        // never satisfies a release E0/E1 requirement). A failing/non-perf run is honest evidence, not fake.
        let env_note = format!(
            "live postgres_native (TD-002 PostgresRelationalBackend), single deployment, resident={resident}, perf_env={perf_env}; the full TP-002 E1 shape is a provisioned instance with PQUEUE_E1_RESIDENT=10000000 + PQUEUE_PERF_ENV=1"
        );
        let lost_items = producer_result
            .seen_ids
            .iter()
            .zip(&consumer_result.seen_ids)
            .filter(|(accepted, claimed)| **accepted && !**claimed)
            .count() as u64;
        let workload_operation_mix = serde_json::json!({
            "push_batches": producer_result.push_batches,
            "claim_batches": consumer_result.claim_batches,
            "finalize_batches": consumer_result.finalize_batches,
        });

        let mut e0_vals = std::collections::BTreeMap::from([
            (
                "producer_ingest_completion_per_s".to_string(),
                serde_json::json!(producer_ingest_completion_per_s),
            ),
            (
                "claimant_finalize_completion_per_s".to_string(),
                serde_json::json!(claimant_finalize_completion_per_s),
            ),
            (
                "producer_completion_ms".to_string(),
                serde_json::json!(producer_completion_ms),
            ),
            (
                "claimant_completion_ms".to_string(),
                serde_json::json!(claimant_completion_ms),
            ),
            (
                "producer_completion_timing".to_string(),
                serde_json::json!("sum of successful push operation durations"),
            ),
            (
                "claimant_completion_timing".to_string(),
                serde_json::json!("sum of successful claim and finalize operation durations"),
            ),
            (
                "resident_set_items".to_string(),
                serde_json::json!(resident),
            ),
            (
                "retained_terminal_items".to_string(),
                serde_json::json!(checkpoint.resident_terminal_count),
            ),
            (
                "one_instance_production_wrapper".to_string(),
                serde_json::json!(true),
            ),
            ("production_pool_size".to_string(), serde_json::json!(2)),
            (
                "production_pool_connections_observed".to_string(),
                serde_json::json!(resources.max_connections),
            ),
            (
                "hot_queue_pool_partition".to_string(),
                serde_json::json!(hot_pool_partition),
            ),
            (
                "canary_queue_pool_partition".to_string(),
                serde_json::json!(canary_pool_partition),
            ),
            (
                "canary_observed_hot_pg_sleep".to_string(),
                serde_json::json!(canary_causal_progress),
            ),
            (
                "canary_exact_outcomes".to_string(),
                serde_json::json!(canary_causal_progress),
            ),
            (
                "canary_completed_before_hot".to_string(),
                serde_json::json!(canary_causal_progress),
            ),
            (
                "canary_causal_progress".to_string(),
                serde_json::json!(canary_causal_progress),
            ),
            ("portable_gate".to_string(), serde_json::json!(true)),
            ("quiet_host_required".to_string(), serde_json::json!(false)),
            ("host_speed_gate".to_string(), serde_json::json!(false)),
            (
                "wall_clock_capacity_only".to_string(),
                serde_json::json!(true),
            ),
            (
                "source_revision".to_string(),
                serde_json::json!(source_revision),
            ),
            (
                "checkout_revision".to_string(),
                serde_json::json!(checkout_revision),
            ),
            (
                "checkout_root".to_string(),
                serde_json::json!(checkout_root),
            ),
            (
                "checkout_clean".to_string(),
                serde_json::json!(checkout_clean),
            ),
            (
                "compile_source_root".to_string(),
                serde_json::json!(compile_source_root),
            ),
            (
                "compile_source_root_bound".to_string(),
                serde_json::json!(compile_source_root_bound),
            ),
            (
                "source_root_explicit".to_string(),
                serde_json::json!(source_root_explicit),
            ),
            ("accepted_items".to_string(), serde_json::json!(accepted)),
            ("claimed_items".to_string(), serde_json::json!(claimed)),
            ("finalized_items".to_string(), serde_json::json!(finalized)),
            (
                "checkpoint_pending".to_string(),
                serde_json::json!(checkpoint.pending),
            ),
            (
                "checkpoint_leased".to_string(),
                serde_json::json!(checkpoint.leased),
            ),
            (
                "checkpoint_complete".to_string(),
                serde_json::json!(checkpoint.complete),
            ),
            (
                "checkpoint_failed".to_string(),
                serde_json::json!(checkpoint.failed),
            ),
            ("lost_items".to_string(), serde_json::json!(lost_items)),
            (
                "identity_epoch_node_prefix".to_string(),
                serde_json::json!(producer_result.identity_prefix),
            ),
            (
                "identity_counter_min".to_string(),
                serde_json::json!(producer_result.counter_min),
            ),
            (
                "identity_counter_max".to_string(),
                serde_json::json!(producer_result.counter_max),
            ),
            (
                "identity_bijection".to_string(),
                serde_json::json!(exact_outcomes),
            ),
            (
                "duplicate_claims".to_string(),
                serde_json::json!(duplicate_claims),
            ),
            (
                "payload_size_counts".to_string(),
                serde_json::json!(producer_result.payload_size_counts),
            ),
            (
                "group_item_counts".to_string(),
                serde_json::json!(producer_result.group_counts),
            ),
            (
                "priority_class_counts".to_string(),
                serde_json::json!(producer_result.priority_counts),
            ),
            (
                "workload_operation_mix".to_string(),
                workload_operation_mix.clone(),
            ),
        ]);
        insert_measured_contract(
            &mut e0_vals,
            MeasuredContract {
                exact_outcomes,
                monotonic_progress,
                bounded_resources,
                finalized_samples: &finalized_samples,
                cursor_samples: &cursor_samples,
                oldest_eligible_age_samples_ms: &oldest_eligible_age_samples_ms,
                progress_identity_sample_count: consumer_result.progress_identity_sample_count,
                progress_latency_lower_max_ms: consumer_result.progress_latency_lower_max_ms,
                progress_latency_upper_max_ms: consumer_result.progress_latency_upper_max_ms,
                progress_latency_over_60000_ms_count: consumer_result
                    .progress_latency_over_60000_ms_count,
                progress_bound_ms,
                persisted_progress_bound_ms: persisted.progress_bound_ms,
                progress_bound_explicit,
                progress_bound_violations: consumer_result.progress_bound_violations,
                progress_latency_upper_buckets: &consumer_result.progress_latency_upper_buckets,
                lifecycle_snapshots: &consumer_result.lifecycle_snapshots,
                discovery_query_count: consumer_result.discovery_query_count,
                discovery_nonempty_count: consumer_result.discovery_nonempty_count,
                resources: &resources,
                max_in_flight,
                pending_items_peak,
                caps,
                workers: &workers,
            },
        );
        emit(
            "e0",
            single_deployment_evidence_row(
                "e0",
                full_shape,
                e0_pass,
                env_note.clone(),
                "E0: exact accepted, claimed, and finalized outcomes; monotonic configured-bound progress; bounded shared resources; timings are capacity observations only",
                e0_vals,
            ),
        );

        let mut e1_vals: std::collections::BTreeMap<String, serde_json::Value> =
            std::collections::BTreeMap::new();
        for (k, v) in p50.iter().chain(p95.iter()).chain(p99.iter()) {
            e1_vals.insert(k.clone(), serde_json::json!(v));
        }
        e1_vals.insert(
            "producer_ingest_completion_per_s".into(),
            serde_json::json!(producer_ingest_completion_per_s),
        );
        e1_vals.insert(
            "claimant_finalize_completion_per_s".into(),
            serde_json::json!(claimant_finalize_completion_per_s),
        );
        e1_vals.insert(
            "producer_completion_ms".into(),
            serde_json::json!(producer_completion_ms),
        );
        e1_vals.insert(
            "claimant_completion_ms".into(),
            serde_json::json!(claimant_completion_ms),
        );
        e1_vals.insert(
            "producer_completion_timing".into(),
            serde_json::json!("sum of successful push operation durations"),
        );
        e1_vals.insert(
            "claimant_completion_timing".into(),
            serde_json::json!("sum of successful claim and finalize operation durations"),
        );
        e1_vals.insert("resident_set_items".into(), serde_json::json!(resident));
        e1_vals.insert(
            "retained_terminal_items".into(),
            serde_json::json!(checkpoint.resident_terminal_count),
        );
        e1_vals.insert(
            "checkpoint_pending".into(),
            serde_json::json!(checkpoint.pending),
        );
        e1_vals.insert(
            "checkpoint_leased".into(),
            serde_json::json!(checkpoint.leased),
        );
        e1_vals.insert(
            "checkpoint_complete".into(),
            serde_json::json!(checkpoint.complete),
        );
        e1_vals.insert(
            "checkpoint_failed".into(),
            serde_json::json!(checkpoint.failed),
        );
        e1_vals.insert(
            "payload_size_counts".into(),
            serde_json::json!(producer_result.payload_size_counts),
        );
        e1_vals.insert(
            "group_item_counts".into(),
            serde_json::json!(producer_result.group_counts),
        );
        e1_vals.insert(
            "priority_class_counts".into(),
            serde_json::json!(producer_result.priority_counts),
        );
        e1_vals.insert("workload_operation_mix".into(), workload_operation_mix);
        e1_vals.insert(
            "worst_op_p99_ms".into(),
            serde_json::json!((worst_p99 * 1000.0).round() / 1000.0),
        );
        e1_vals.insert("portable_gate".into(), serde_json::json!(true));
        e1_vals.insert("quiet_host_required".into(), serde_json::json!(false));
        e1_vals.insert("host_speed_gate".into(), serde_json::json!(false));
        e1_vals.insert("wall_clock_capacity_only".into(), serde_json::json!(true));
        e1_vals.insert("source_revision".into(), serde_json::json!(source_revision));
        e1_vals.insert(
            "checkout_revision".into(),
            serde_json::json!(checkout_revision),
        );
        e1_vals.insert("checkout_root".into(), serde_json::json!(checkout_root));
        e1_vals.insert("checkout_clean".into(), serde_json::json!(checkout_clean));
        e1_vals.insert(
            "compile_source_root".into(),
            serde_json::json!(compile_source_root),
        );
        e1_vals.insert(
            "compile_source_root_bound".into(),
            serde_json::json!(compile_source_root_bound),
        );
        e1_vals.insert(
            "source_root_explicit".into(),
            serde_json::json!(source_root_explicit),
        );
        e1_vals.insert(
            "push_batch_sizes".into(),
            serde_json::json!(actual_push_sizes),
        );
        e1_vals.insert(
            "update_window_sizes".into(),
            serde_json::json!(actual_update_window_sizes),
        );
        e1_vals.insert(
            "claim_batch_sizes".into(),
            serde_json::json!(actual_claim_sizes),
        );
        e1_vals.insert(
            "finalize_batch_sizes".into(),
            serde_json::json!(actual_finalize_sizes),
        );
        e1_vals.insert(
            "configured_max_batch_size".into(),
            serde_json::json!(CONFIGURED_MAX_BATCH_SIZE),
        );
        e1_vals.insert(
            "persisted_max_push_batch_size".into(),
            serde_json::json!(persisted.max_push_batch_size),
        );
        e1_vals.insert(
            "persisted_max_claim_batch_size".into(),
            serde_json::json!(persisted.max_claim_batch_size),
        );
        e1_vals.insert(
            "oversize_push_rejected".into(),
            serde_json::json!(oversize_push_rejected),
        );
        e1_vals.insert(
            "oversize_claim_rejected".into(),
            serde_json::json!(oversize_claim_rejected),
        );
        e1_vals.insert(
            "probe_accepted_items".into(),
            serde_json::json!(probe_accepted),
        );
        e1_vals.insert(
            "probe_claimed_items".into(),
            serde_json::json!(probe_claimed),
        );
        e1_vals.insert(
            "probe_finalized_items".into(),
            serde_json::json!(probe_finalized),
        );
        e1_vals.insert(
            "probe_unique_accepted_ids".into(),
            serde_json::json!(probe_accepted_ids.len()),
        );
        e1_vals.insert(
            "probe_unique_claimed_ids".into(),
            serde_json::json!(probe_claimed_ids.len()),
        );
        e1_vals.insert(
            "probe_unique_finalized_ids".into(),
            serde_json::json!(probe_finalized_ids.len()),
        );
        e1_vals.insert(
            "probe_identity_exact".into(),
            serde_json::json!(probe_identity_exact),
        );
        e1_vals.insert(
            "post_probe_pending".into(),
            serde_json::json!(post_probe.pending),
        );
        e1_vals.insert(
            "post_probe_leased".into(),
            serde_json::json!(post_probe.leased),
        );
        e1_vals.insert(
            "post_probe_complete".into(),
            serde_json::json!(post_probe.complete),
        );
        e1_vals.insert(
            "post_probe_failed".into(),
            serde_json::json!(post_probe.failed),
        );
        e1_vals.insert(
            "post_probe_resident_terminal_count".into(),
            serde_json::json!(post_probe.resident_terminal_count),
        );
        e1_vals.insert(
            "probe_operation_mix".into(),
            serde_json::json!({
                "push_items": probe_accepted,
                "push_batches": probe_push_batches,
                "update_item_calls": probe_update_item_calls,
                "claim_items": probe_claimed,
                "claim_batches": probe_claim_batches,
                "finalize_items": probe_finalized,
                "finalize_batches": probe_finalize_batches,
            }),
        );
        e1_vals.insert(
            "post10m_affinity_serialization_probe".into(),
            serde_json::json!(post10m_affinity_serialization_probe),
        );
        e1_vals.insert(
            "post10m_caller_interval_overlap_observed".into(),
            serde_json::json!(post10m_caller_interval_overlap_observed),
        );
        e1_vals.insert(
            "post10m_caller_in_flight_observed".into(),
            serde_json::json!(control_operations.max_observed()),
        );
        e1_vals.insert(
            "post10m_active_pending_before".into(),
            serde_json::json!(active_pending_before),
        );
        e1_vals.insert(
            "total_accepted_items".into(),
            serde_json::json!(accepted + probe_accepted),
        );
        e1_vals.insert(
            "total_claimed_items".into(),
            serde_json::json!(claimed + probe_claimed),
        );
        e1_vals.insert(
            "total_finalized_items".into(),
            serde_json::json!(finalized + probe_finalized),
        );
        e1_vals.insert("lost_items".into(), serde_json::json!(lost_items));
        e1_vals.insert(
            "duplicate_claims".into(),
            serde_json::json!(duplicate_claims),
        );
        e1_vals.insert(
            "identity_epoch_node_prefix".into(),
            serde_json::json!(producer_result.identity_prefix),
        );
        e1_vals.insert(
            "identity_counter_min".into(),
            serde_json::json!(producer_result.counter_min),
        );
        e1_vals.insert(
            "identity_counter_max".into(),
            serde_json::json!(producer_result.counter_max),
        );
        e1_vals.insert(
            "identity_bijection".into(),
            serde_json::json!(exact_outcomes),
        );
        e1_vals.extend(samples); // samples_per_op_b<sz> — percentile fidelity is visible in the row
        insert_measured_contract(
            &mut e1_vals,
            MeasuredContract {
                exact_outcomes,
                monotonic_progress,
                bounded_resources,
                finalized_samples: &finalized_samples,
                cursor_samples: &cursor_samples,
                oldest_eligible_age_samples_ms: &oldest_eligible_age_samples_ms,
                progress_identity_sample_count: consumer_result.progress_identity_sample_count,
                progress_latency_lower_max_ms: consumer_result.progress_latency_lower_max_ms,
                progress_latency_upper_max_ms: consumer_result.progress_latency_upper_max_ms,
                progress_latency_over_60000_ms_count: consumer_result
                    .progress_latency_over_60000_ms_count,
                progress_bound_ms,
                persisted_progress_bound_ms: persisted.progress_bound_ms,
                progress_bound_explicit,
                progress_bound_violations: consumer_result.progress_bound_violations,
                progress_latency_upper_buckets: &consumer_result.progress_latency_upper_buckets,
                lifecycle_snapshots: &consumer_result.lifecycle_snapshots,
                discovery_query_count: consumer_result.discovery_query_count,
                discovery_nonempty_count: consumer_result.discovery_nonempty_count,
                resources: &resources,
                max_in_flight,
                pending_items_peak,
                caps,
                workers: &workers,
            },
        );
        emit(
            "e1",
            single_deployment_evidence_row(
                "e1",
                full_shape,
                e1_pass,
                env_note,
                "E1: exact batch outcomes, monotonic progress, and bounded resources at the full resident shape; latency percentiles are capacity observations only",
                e1_vals,
            ),
        );
    });
}

fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1000.0
}

/// Write a single-row ledger file `<suite-tag>.jsonl` and assert it round-trips strict validation as a
/// release-tier row carrying its evidence id. (E0 and E1 each get their own file so both are gate-visible.)
fn emit(tag: &str, row: pqueue_release::LedgerRow) {
    let expected_id = match tag {
        "e0" => "E0",
        "e1" => "E1",
        other => panic!("unsupported single-deployment evidence tag {other}"),
    };
    assert_eq!(
        row.measurements.tp002_evidence_ids.as_slice(),
        [expected_id],
        "single-deployment evidence must carry exactly its governed identity"
    );
    let suite = format!("performance_single_deployment_baseline_tests_{tag}");
    let path = pqueue_release::ledger_path(env!("CARGO_MANIFEST_DIR"), &suite);
    let _ = std::fs::remove_file(&path);
    pqueue_release::append_row(&path, &row).expect("emit ledger row");
    let summary = pqueue_release::verify_ledger(&path, true).expect("emitted row validates strict");
    // Release-tier ids count as headline evidence; smoke-tier ids are tracked separately.
    let seen = if row.evidence_tier == "smoke" {
        summary.smoke_evidence_ids.contains(expected_id)
    } else {
        summary.evidence_ids.contains(expected_id)
    };
    assert!(seen, "emitted row must carry the {expected_id} evidence id");
}
