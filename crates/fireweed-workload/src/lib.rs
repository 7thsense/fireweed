//! Application-shaped workload. Only the public `fireweed` crate is a dependency:
//! workers discover every stage from the queue, never from a harness work channel.
pub mod primitives;
pub mod retention;

use fireweed::*;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    Bulk,
    Mutable,
    Snorri,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub items: usize,
    pub cycles: usize,
    pub batch: usize,
    /// Independent physical projection/log pairs, not merely queue labels.
    pub shards: usize,
    pub workers: usize,
    pub profile: Profile,
    pub memory: bool,
    pub faults: bool,
    pub payload_bytes: usize,
    pub deadline: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            items: 120,
            cycles: 3,
            batch: 30,
            shards: 1,
            workers: 1,
            profile: Profile::Mutable,
            memory: false,
            faults: true,
            payload_bytes: 1024,
            deadline: Duration::from_secs(120),
        }
    }
}

/// Public injected clock: eligibility can advance without sleeping through a campaign.
/// Capacity measurements always use Instant wall time, never this virtual clock.
pub struct TestClock(pub AtomicU64);
impl TestClock {
    pub fn at(seconds: u64) -> Arc<Self> {
        Arc::new(Self(AtomicU64::new(seconds)))
    }
    pub fn set(&self, seconds: u64) {
        self.0.store(seconds, Ordering::SeqCst);
    }
}
impl Clock for TestClock {
    fn now(&self) -> UtcTimestamp {
        ts(self.0.load(Ordering::SeqCst) as i64)
    }
}
pub fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

pub fn open_store(root: &Path, memory: bool, clock: Arc<dyn Clock>) -> Result<Fireweed> {
    if memory {
        return Ok(open_memory(clock));
    }
    std::fs::create_dir_all(root)?;
    Ok(open(
        StorageConfig {
            log: LogConfig::Filesystem {
                root: root.join("log"),
            },
            projection: ProjectionStoreConfig::Turso {
                path: root.join("projection.db"),
            },
            control_plane: None,
            authority: None,
            response_barrier: ResponseBarrier::AsyncProjection,
            async_projection: Some(AsyncProjectionSpec::default()),
            sqlite_projection_deferred_flush_chunk: None,
            segments: SegmentConfig {
                target_bytes: 256 * 1024,
                max_latency_ms: 5,
            },
            namespace: "workflow-workload".into(),
            recovery: RecoveryPolicy::default(),
        },
        clock,
    )?)
}

pub fn definition(name: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("workflow").unwrap(),
        queue_id: QueueId::new(name).unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy {
            gate_keys: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: Some(8),
            ..Default::default()
        },
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 3_600_000,
        client_item_key_retention_ms: 0,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 3_600_000,
        retry_policy: RetryPolicy { max_attempts: 5 },
        max_push_batch_size: 1000,
        max_claim_batch_size: 1000,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}
pub async fn create_queue(fw: &Fireweed, name: &str) -> Result<QueueKey> {
    let d = definition(name);
    let q = QueueKey::new(d.tenant_id.clone(), d.queue_id.clone());
    fw.create_queue(d).await?;
    Ok(q)
}
pub fn metadata(stage: usize, id: usize) -> Metadata {
    Metadata::from_entries(BTreeMap::from([
        ("stage".into(), MetadataValue::String(stage.to_string())),
        ("recipient".into(), MetadataValue::String(id.to_string())),
    ]))
}
pub fn compatibility(stage: usize) -> ClaimCompatibility {
    let mut c = ClaimCompatibility::default();
    c.metadata_equals
        .insert("stage".into(), MetadataValue::String(stage.to_string()));
    c
}
pub fn due(id: usize) -> i64 {
    100 + ((id * 37) % 17) as i64
}
pub fn body(id: usize, stage: usize, size: usize) -> Bytes {
    let mut bytes = format!("recipient={id};stage={stage};color={};", id % 7).into_bytes();
    bytes.resize(size.max(bytes.len()), b'a' + (id % 26) as u8);
    Bytes::from(bytes)
}
pub fn item(id: usize, stage: usize, size: usize) -> NewItem {
    NewItem {
        client_item_key: Some(ClientItemKey::new(format!("r-{id:09}-s{stage}")).unwrap()),
        priority: Some(PriorityValue::Int64(if stage == 2 {
            due(id)
        } else {
            id as i64
        })),
        not_before: Some(ts(if stage == 2 { due(id) } else { 1 })),
        metadata: metadata(stage, id),
        payload: Some(body(id, stage, size)),
        ..Default::default()
    }
}
pub fn claim_ref(item: &ClaimedItem) -> ClaimRef {
    ClaimRef {
        item_id: item.item_id,
        lease_token: item.lease_token.clone().expect("claim must carry lease"),
        lease_expires_at: item.lease_expires_at,
        item_version: item.item_version,
    }
}
pub fn recipient(item: &ClaimedItem) -> usize {
    match item.metadata.get("recipient").unwrap() {
        MetadataValue::String(s) => s.parse().unwrap(),
        _ => panic!("invalid recipient metadata"),
    }
}

pub async fn retry<T, F, Fut>(deadline: Instant, mut call: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = EngineResult<T>>,
{
    loop {
        match call().await {
            Ok(value) => return Ok(value),
            Err(EngineError::Backpressure { .. }) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(2)).await
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[derive(Default)]
struct Observations {
    stage_locks: [tokio::sync::Mutex<()>; 2],
    prepared: [AtomicUsize; 2],
    delivered: AtomicUsize,
    failed: AtomicUsize,
    retries: AtomicUsize,
    claims: AtomicUsize,
    /// Independent acceptance oracle; not consulted for work discovery.
    receipts: std::sync::Mutex<BTreeMap<usize, bool>>,
    transient_failures: std::sync::Mutex<std::collections::BTreeSet<usize>>,
}

// Snorri dispatches claimed transition inputs in one shared worker pool.
const SHARED_DISPATCH: usize = 3;

async fn worker(
    fw: Arc<Fireweed>,
    q: QueueKey,
    stage: usize,
    worker: usize,
    cfg: Config,
    observations: Arc<Observations>,
    expected: usize,
    deadline: Instant,
) -> Result<()> {
    let debug = std::env::var_os("FIREWEED_WORKLOAD_DEBUG").is_some();
    let mut generation = 0;
    loop {
        let completed = if stage < 2 {
            observations.prepared[stage].load(Ordering::SeqCst)
        } else {
            observations.delivered.load(Ordering::SeqCst)
                + observations.failed.load(Ordering::SeqCst)
        };
        if completed == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("stage {stage} timed out: {completed}/{expected}").into());
        }
        // Seventh Sense serializes each scheduler job using a SKIP LOCKED job row.
        // Model that application-level ownership while batch_update requires Pending.
        let _stage_guard = if stage < 2 && cfg.profile == Profile::Mutable {
            Some(observations.stage_locks[stage].lock().await)
        } else {
            None
        };
        if debug && generation == 0 {
            eprintln!("stage={stage} worker={worker} requesting claim");
        }
        if stage == SHARED_DISPATCH {
            retry(deadline, || fw.reclaim_expired_at(&q, None, ts(200)))
                .await
                .map_err(|e| format!("reclaim worker {worker} generation {generation}: {e}"))?;
        }
        let claimed = retry(deadline, || async {
            if stage == SHARED_DISPATCH {
                fw.claim(&q, cfg.batch, 3_600_000).await
            } else {
                fw.claim_with(&q, cfg.batch, 3_600_000, compatibility(stage))
                    .await
            }
        })
        .await
        .map_err(|e| format!("claim stage {stage} worker {worker} generation {generation}: {e}"))?;
        if claimed.is_empty() {
            tokio::time::sleep(Duration::from_millis(2)).await;
            continue;
        }
        if debug {
            eprintln!(
                "stage={stage} worker={worker} generation={generation} claimed={}",
                claimed.len()
            );
            eprintln!(
                "claimed_ids={:?}",
                claimed.iter().map(|item| item.item_id).collect::<Vec<_>>()
            );
        }
        observations
            .claims
            .fetch_add(claimed.len(), Ordering::SeqCst);
        // Deterministic handler scheduling variation; no network or unbounded sleep.
        if cfg.faults && generation % 3 == 0 {
            tokio::task::yield_now().await;
        }
        let mut patches = vec![];
        let mut commits = vec![];
        let mut successes = vec![];
        let mut failures = vec![];
        let mut retry_ids = vec![];
        let mut prepared = [0usize; 2];
        for claimed in &claimed {
            let stage = if stage == SHARED_DISPATCH {
                match claimed.metadata.get("stage") {
                    Some(MetadataValue::String(value)) => value.parse::<usize>()?,
                    _ => return Err("missing transition stage".into()),
                }
            } else {
                stage
            };
            if stage > 2 {
                return Err("invalid transition stage".into());
            }
            let id = recipient(claimed);
            if claimed.payload.as_ref() != Some(&body(id, stage, cfg.payload_bytes)) {
                return Err(format!("payload mismatch recipient {id} stage {stage}").into());
            }
            if stage < 2 {
                prepared[stage] += 1;
                if cfg.profile == Profile::Snorri {
                    commits.push(CommitEntry {
                        claim_ref: claim_ref(claimed),
                        finalize: FinalizeKind::Complete,
                        side_records: vec![SideRecord {
                            key: format!("instance/{id}").into_bytes(),
                            payload: body(id, stage + 1, cfg.payload_bytes),
                        }],
                        lifecycle_items: vec![item(id, stage + 1, cfg.payload_bytes)],
                        instance_fence: Some(InstanceFence {
                            instance_key: format!("instance/{id}").into_bytes(),
                            expected: stage as u64,
                            next: stage as u64 + 1,
                        }),
                    });
                } else {
                    patches.push(BatchUpdateEntry {
                        item_ref: BatchUpdateItemRef::ClientItemKey(
                            claimed.client_item_key.clone(),
                        ),
                        expected_item_version: None,
                        payload: BatchUpdateValue::Replace(Some(body(
                            id,
                            stage + 1,
                            cfg.payload_bytes,
                        ))),
                        metadata: BatchUpdateValue::Replace(metadata(stage + 1, id)),
                        priority: BatchUpdateValue::Replace(PriorityValue::Int64(if stage == 1 {
                            due(id)
                        } else {
                            id as i64
                        })),
                        not_before: BatchUpdateValue::Replace(Some(ts(if stage == 1 {
                            due(id)
                        } else {
                            1
                        }))),
                        fields: BatchUpdateValue::Keep,
                        gate_keys: BatchUpdateValue::Keep,
                    });
                }
            } else {
                if cfg.faults
                    && id % 19 == 0
                    && observations.transient_failures.lock().unwrap().insert(id)
                {
                    retry_ids.push(claimed.item_id);
                    continue;
                }
                let failed = cfg.faults && id % 31 == 0;
                commits.push(CommitEntry {
                    claim_ref: claim_ref(claimed),
                    finalize: if failed {
                        FinalizeKind::Fail
                    } else {
                        FinalizeKind::Complete
                    },
                    side_records: vec![SideRecord {
                        key: format!("receipt/{id}").into_bytes(),
                        payload: Bytes::from(format!(
                            "{}:{}",
                            if failed { "failed" } else { "accepted" },
                            claimed.attempt_count
                        )),
                    }],
                    lifecycle_items: vec![],
                    instance_fence: None,
                });
                if failed {
                    failures.push(id);
                } else {
                    successes.push(id);
                }
            }
        }
        // Persist a bounded batch of equal retry outcomes, as delivery workers do.
        if !retry_ids.is_empty() {
            retry(deadline, || {
                fw.retry(&q, retry_ids.iter().copied(), Some(ts(100)))
            })
            .await
            .map_err(|e| {
                format!("retry stage {stage} worker {worker} generation {generation}: {e}")
            })?;
            observations
                .retries
                .fetch_add(retry_ids.len(), Ordering::SeqCst);
        }
        let request_id = RequestId::new(format!("s{stage}-w{worker}-g{generation}")).unwrap();
        generation += 1;
        if !patches.is_empty() {
            retry(deadline, || {
                fw.release(&q, claimed.iter().map(|c| c.item_id))
            })
            .await?;
            let request = BatchUpdateRequest {
                request_id: request_id.clone(),
                updates: patches,
            };
            if debug {
                eprintln!("stage={stage} released; updating");
            }
            let response = retry(deadline, || fw.batch_update(&q, request.clone())).await?;
            if debug {
                eprintln!("stage={stage} updated");
            }
            for result in response.results {
                if !matches!(result, BatchUpdateOutcome::Updated { .. }) {
                    return Err(format!("stage update rejected: {result:?}").into());
                }
            }
        }
        if !commits.is_empty() {
            let request = CommitRequest {
                request_id: Some(request_id),
                entries: commits,
            };
            let response = retry(deadline, || fw.commit(&q, request.clone()))
                .await
                .map_err(|e| {
                    format!("commit stage {stage} worker {worker} generation {generation}: {e}")
                })?;
            if response
                .iter()
                .any(|r| !matches!(r, EntryOutcome::Committed { .. }))
            {
                return Err(format!("transition rejected: {response:?}").into());
            }
        }
        for (stage, count) in prepared.into_iter().enumerate() {
            observations.prepared[stage].fetch_add(count, Ordering::SeqCst);
        }
        {
            let mut receipts = observations.receipts.lock().unwrap();
            for id in successes.iter().chain(&failures) {
                if receipts.insert(*id, failures.contains(id)).is_some() {
                    return Err(format!("duplicate terminal recipient {id}").into());
                }
            }
            observations
                .delivered
                .fetch_add(successes.len(), Ordering::SeqCst);
            observations
                .failed
                .fetch_add(failures.len(), Ordering::SeqCst);
        }
    }
}

pub async fn run(cfg: Config, root: &Path) -> Result<serde_json::Value> {
    tokio::time::timeout(cfg.deadline, run_inner(cfg, root))
        .await
        .map_err(|_| "workload exceeded wall-clock deadline (including API waits)")?
}

async fn run_inner(cfg: Config, root: &Path) -> Result<serde_json::Value> {
    if cfg.items == 0 || cfg.shards == 0 || cfg.workers == 0 || !(1..=1000).contains(&cfg.batch) {
        return Err("items/shards/workers must be positive and batch in 1..=1000".into());
    }
    let start = Instant::now();
    let deadline = start + cfg.deadline;
    let mut handles = tokio::task::JoinSet::new();
    for shard in 0..cfg.shards {
        let cfg = cfg.clone();
        let root = root.join(format!("shard-{shard}"));
        handles.spawn(async move {
            let fw = Arc::new(open_store(&root, cfg.memory, TestClock::at(200))?);
            let q = create_queue(&fw, "workflow").await?;
            let ids: Vec<_> = (shard..cfg.items).step_by(cfg.shards).collect(); let expected = ids.len();
            let observations = Arc::new(Observations::default());
            let load = async {
                for chunk in ids.chunks(cfg.batch) {
                    let items: Vec<_> = chunk.iter().map(|id| item(*id, if cfg.profile == Profile::Bulk { 2 } else { 0 }, cfg.payload_bytes)).collect();
                    retry(deadline, || fw.push_batch(&q, items.clone())).await
                        .map_err(|e| format!("load shard {shard} starting recipient {}: {e}", chunk[0]))?;
                    if std::env::var_os("FIREWEED_WORKLOAD_DEBUG").is_some() { eprintln!("loaded {}", chunk.len()); }
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            };
            let consume = async {
                let mut workers = vec![];
                let stages = match cfg.profile {
                    Profile::Bulk => 2..3,
                    Profile::Mutable => 0..3,
                    Profile::Snorri => SHARED_DISPATCH..SHARED_DISPATCH + 1,
                };
                for stage in stages {
                    for w in 0..cfg.workers {
                        workers.push(worker(fw.clone(), q.clone(), stage, w, cfg.clone(), observations.clone(), expected, deadline));
                    }
                }
                futures::future::try_join_all(workers).await.map(|_| ())
            };
            // Bulk scheduled backlog is loaded before delivery. Incremental profiles overlap.
            if cfg.profile == Profile::Bulk { load.await?; consume.await?; }
            else { tokio::try_join!(load, consume)?; }
            let metrics = retry(deadline, || fw.metrics(&q)).await?;
            let terminal = expected * if cfg.profile == Profile::Snorri { 3 } else { 1 };
            if metrics.pending != 0 || metrics.leased != 0 || metrics.complete + metrics.failed != terminal as u64 {
                return Err(format!("unexpected final metrics: {metrics:?}").into());
            }
            if observations.receipts.lock().unwrap().len() != expected { return Err("missing terminal receipts".into()); }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(serde_json::json!({"shard": shard, "items": expected,
                "delivered": observations.delivered.load(Ordering::SeqCst), "failed": observations.failed.load(Ordering::SeqCst),
                "retries": observations.retries.load(Ordering::SeqCst), "claims": observations.claims.load(Ordering::SeqCst),
                "complete": metrics.complete, "pending": metrics.pending, "leased": metrics.leased }))
        });
    }
    let mut reports = vec![];
    while let Some(result) = handles.join_next().await {
        reports.push(result??);
    }
    reports.sort_by_key(|r| r["shard"].as_u64());
    Ok(
        serde_json::json!({ "schema": "workflow-capacity/v2", "profile": format!("{:?}", cfg.profile),
        "cell": if cfg.memory { "memory--memory" } else { "filesystem--turso" },
        "items": cfg.items, "physical_shards": cfg.shards, "workers_per_pool": cfg.workers,
        "worker_pools_per_shard": if cfg.profile == Profile::Mutable { 3 } else { 1 },
        "dispatch": if cfg.profile == Profile::Snorri { "shared-normal-claim" } else { "stage-filtered" },
        "batch": cfg.batch, "payload_bytes": cfg.payload_bytes, "faults": cfg.faults,
        "settled_wall_s": start.elapsed().as_secs_f64(),
        "completed_lifecycles_per_s": cfg.items as f64 / start.elapsed().as_secs_f64(), "shards": reports }),
    )
}
