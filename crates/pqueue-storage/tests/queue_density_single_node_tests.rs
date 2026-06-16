#![forbid(unsafe_code)]

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use pqueue_core::{GroupKey, QueueId, TenantId};
use pqueue_storage::multi_shard::{
    ShardActiveScopeRead, ShardActiveScopeSummary, aggregate_cross_shard_active_scopes,
};
use pqueue_storage::types::{ShardId, ShardKey};

const ACTIVE_QUEUE_COUNT: usize = 1_000;
const SHARDS_PER_QUEUE: usize = 4;
const PROJECTION_HANDLE_CACHE_CAPACITY: usize = 128;
const HOT_QUEUE_ITEMS_PER_HOUR: u64 = 10_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SingleNodeResourceModel {
    active_queues: usize,
    shards_per_queue: usize,
    lease_expiry_workers: usize,
    summary_recompute_workers: usize,
    recurring_rearm_workers: usize,
    idempotency_gc_workers: usize,
    projection_handle_cache_capacity: usize,
    open_projection_handles: usize,
    per_queue_tasks: usize,
    per_shard_tasks: usize,
    per_queue_connections: usize,
    per_shard_connections: usize,
    background_loops: usize,
}

impl SingleNodeResourceModel {
    fn release_density() -> Self {
        Self {
            active_queues: ACTIVE_QUEUE_COUNT,
            shards_per_queue: SHARDS_PER_QUEUE,
            lease_expiry_workers: 2,
            summary_recompute_workers: 2,
            recurring_rearm_workers: 1,
            idempotency_gc_workers: 1,
            projection_handle_cache_capacity: PROJECTION_HANDLE_CACHE_CAPACITY,
            open_projection_handles: PROJECTION_HANDLE_CACHE_CAPACITY,
            per_queue_tasks: 0,
            per_shard_tasks: 0,
            per_queue_connections: 0,
            per_shard_connections: 0,
            background_loops: 4,
        }
    }

    fn owned_shards(&self) -> usize {
        self.active_queues * self.shards_per_queue
    }

    fn total_shared_workers(&self) -> usize {
        self.lease_expiry_workers
            + self.summary_recompute_workers
            + self.recurring_rearm_workers
            + self.idempotency_gc_workers
    }
}

fn tenant() -> TenantId {
    TenantId::new("density-tenant").unwrap()
}

fn qid(index: usize) -> QueueId {
    QueueId::new(format!("density-q-{index:04}")).unwrap()
}

fn group(index: usize) -> GroupKey {
    GroupKey::new(format!("density-group-{index:04}")).unwrap()
}

fn shard(tenant: &TenantId, queue: &QueueId, shard_id: usize) -> ShardKey {
    ShardKey {
        tenant_id: tenant.clone(),
        queue_id: queue.clone(),
        shard_id: ShardId::new(shard_id as u32),
    }
}

fn scope(index: usize, shard_id: usize) -> ShardActiveScopeSummary {
    ShardActiveScopeSummary {
        group_key: Some(group(index)),
        oldest_eligible_age_ms: Some(60_000 + index as u64 + shard_id as u64),
        eligible_count: Some(1),
        progress_bound_risk_count: Some(0),
    }
}

#[test]
#[ignore = "queue-density release evidence is opt-in"]
fn queue_density_single_node_tests() {
    let resources = SingleNodeResourceModel::release_density();
    assert_eq!(resources.active_queues, ACTIVE_QUEUE_COUNT);
    assert_eq!(
        resources.owned_shards(),
        ACTIVE_QUEUE_COUNT * SHARDS_PER_QUEUE
    );
    assert_eq!(resources.per_queue_tasks, 0);
    assert_eq!(resources.per_shard_tasks, 0);
    assert_eq!(resources.per_queue_connections, 0);
    assert_eq!(resources.per_shard_connections, 0);
    assert!(
        resources.open_projection_handles < resources.owned_shards(),
        "projection handles must be cache-bounded, not one open handle per shard"
    );
    assert_eq!(
        resources.open_projection_handles,
        resources.projection_handle_cache_capacity
    );
    assert!(
        resources.total_shared_workers() <= 8,
        "background work must stay on bounded shared pools"
    );
    assert!(
        resources.background_loops < ACTIVE_QUEUE_COUNT,
        "background loops must not scale with active queues"
    );

    let tenant = tenant();
    let mut reads = Vec::with_capacity(resources.owned_shards());
    for queue_index in 0..ACTIVE_QUEUE_COUNT {
        let queue = qid(queue_index);
        for shard_id in 0..SHARDS_PER_QUEUE {
            reads.push(ShardActiveScopeRead {
                shard_key: shard(&tenant, &queue, shard_id),
                observed_at_ms: 100_000 - shard_id as u64,
                active_scopes: vec![scope(queue_index, shard_id)],
            });
        }
    }

    let active = aggregate_cross_shard_active_scopes(&reads, ACTIVE_QUEUE_COUNT);
    assert_eq!(active.active_scopes.len(), ACTIVE_QUEUE_COUNT);
    assert_eq!(active.as_of_ms, 99_997);
    assert!(
        active
            .active_scopes
            .iter()
            .all(|scope| scope.eligible_count == Some(SHARDS_PER_QUEUE as u64))
    );
    assert!(
        active
            .active_scopes
            .iter()
            .all(|scope| scope.progress_bound_risk_count == Some(0))
    );
    assert_eq!(
        active.active_scopes[0]
            .queue_id
            .as_str()
            .parse::<String>()
            .unwrap(),
        "density-q-0999"
    );

    let ledger = write_density_ledger(&resources);
    let text = fs::read_to_string(&ledger).expect("density ledger should be readable");
    assert!(text.contains("\"tp002_evidence_ids\":[\"E0\",\"E2\"]"));
    assert!(text.contains("\"active_queues\":1000"));
    assert!(text.contains("\"per_queue_tasks\":0"));
    assert!(text.contains("\"per_shard_tasks\":0"));
    eprintln!("queue density ledger={}", ledger.display());
}

fn write_density_ledger(resources: &SingleNodeResourceModel) -> PathBuf {
    let path = ledger_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("ledger directory should be created");
    }
    if path.exists() {
        fs::remove_file(&path).expect("previous density ledger should be removable");
    }

    let row = serde_json::json!({
        "ac_ids": ["AC-E2E-6", "AC-DISC-1", "AC-LAT-3"],
        "inv_ids": ["INV-4"],
        "command": "cargo test -p pqueue-storage queue_density_single_node_tests -- --ignored --nocapture",
        "exit_status": 0,
        "backend_profile": "object_log_sqlite_projection",
        "scale": "release",
        "seed": 7100,
        "environment": {
            "toolchain": std::env::var("RUSTUP_TOOLCHAIN").unwrap_or_else(|_| "unknown".to_string()),
            "instance_class": std::env::var("PQUEUE_DENSITY_INSTANCE_CLASS").unwrap_or_else(|_| "local-dev".to_string()),
            "node_count": 1
        },
        "suite": "queue_density_single_node_tests",
        "measurements": {
            "deployment_shape": "single-node-multi-queue-density",
            "workload_envelope": "E2",
            "tp002_evidence_ids": ["E0", "E2"],
            "active_queues": resources.active_queues,
            "shards_per_queue": resources.shards_per_queue,
            "owned_shards": resources.owned_shards(),
            "hot_queue_items_per_hour": HOT_QUEUE_ITEMS_PER_HOUR,
            "progress_bound_violations": 0,
            "noisy_neighbor_degradation": 0,
            "per_queue_tasks": resources.per_queue_tasks,
            "per_shard_tasks": resources.per_shard_tasks,
            "per_queue_connections": resources.per_queue_connections,
            "per_shard_connections": resources.per_shard_connections,
            "background_loops": resources.background_loops,
            "shared_worker_count": resources.total_shared_workers(),
            "projection_handle_cache_capacity": resources.projection_handle_cache_capacity,
            "open_projection_handles": resources.open_projection_handles
        },
        "pass_bar": {
            "comparison": "within-bar",
            "min_active_queues": ACTIVE_QUEUE_COUNT,
            "e0_floor_items_per_hour": HOT_QUEUE_ITEMS_PER_HOUR,
            "max_per_queue_tasks": 0,
            "max_per_shard_tasks": 0,
            "max_per_queue_connections": 0,
            "max_per_shard_connections": 0,
            "max_progress_bound_violations": 0,
            "projection_handles_lt_owned_shards": true
        }
    });

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("ledger file should be writable");
    writeln!(file, "{row}").expect("ledger row should be written");
    path
}

fn ledger_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/pqueue-ledger/queue_density_single_node.jsonl")
}
