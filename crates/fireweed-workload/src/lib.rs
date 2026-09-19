//! Application-shaped workload. Only the public `fireweed` crate is a dependency:
//! workers discover every stage from the queue, never from a harness work channel.
pub mod campaign;
pub mod primitives;
pub mod retention;

use fireweed::*;
use futures::{StreamExt, TryStreamExt};
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
    pub recycle: bool,
    pub batch: usize,
    /// Independent physical projection/log pairs, not merely queue labels.
    pub shards: usize,
    pub workers: usize,
    /// Bounded concurrent public push_batch calls per shard.
    pub load_workers: usize,
    /// Optional retention batch independent of handler batch size.
    pub purge_batch: Option<usize>,
    /// Campaign-only public async policy override; None preserves library defaults.
    pub apply_debt_bytes: Option<u64>,
    pub profile: Profile,
    pub memory: bool,
    pub projection_root: Option<std::path::PathBuf>,
    pub faults: bool,
    pub payload_bytes: usize,
    /// Primitive component control using the campaign's varied JSON bodies.
    pub primitive_varied_payload: bool,
    /// Persist enrichment attributes in row metadata, retaining the original payload.
    pub campaign_metadata_only: bool,
    /// Use Snorri-style timestamp priorities; false retains mixed-unit stress.
    pub campaign_timestamp_priority: bool,
    pub deadline: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            items: 120,
            cycles: 3,
            recycle: false,
            batch: 30,
            shards: 1,
            workers: 1,
            load_workers: 1,
            purge_batch: None,
            apply_debt_bytes: None,
            profile: Profile::Mutable,
            memory: false,
            projection_root: None,
            faults: true,
            payload_bytes: 1024,
            primitive_varied_payload: false,
            campaign_metadata_only: false,
            campaign_timestamp_priority: false,
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
pub(crate) fn process_rss_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find(|line| line.starts_with("VmRSS:"))
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|value| value.parse().ok())
        })
}

pub fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

pub fn open_store(root: &Path, memory: bool, clock: Arc<dyn Clock>) -> Result<Fireweed> {
    open_store_with_projection_root(root, memory, clock, root)
}

pub fn open_store_with_projection_root(
    root: &Path,
    memory: bool,
    clock: Arc<dyn Clock>,
    projection_root: &Path,
) -> Result<Fireweed> {
    open_store_with_async_policy(
        root,
        memory,
        clock,
        projection_root,
        AsyncProjectionSpec::default(),
    )
}

fn open_store_with_async_policy(
    root: &Path,
    memory: bool,
    clock: Arc<dyn Clock>,
    projection_root: &Path,
    async_projection: AsyncProjectionSpec,
) -> Result<Fireweed> {
    if memory {
        return Err(fireweed::RETIRED_STORAGE_CELL.into());
    }
    std::fs::create_dir_all(root)?;
    std::fs::create_dir_all(projection_root)?;
    let s3 = fireweed_objectlog::shared_s3_test_env();
    let mut config = StorageConfig::s3_turso(
        s3.endpoint.clone(),
        s3.bucket.clone(),
        s3.region.clone(),
        s3.access_key.clone(),
        s3.secret_key.clone(),
        s3.allow_insecure_http(),
        projection_root.join("projection.db"),
    );
    config.response_barrier = ResponseBarrier::AsyncProjection;
    config.async_projection = Some(async_projection);
    config.segments = SegmentConfig {
        target_bytes: 256 * 1024,
        max_latency_ms: 5,
    };
    config.namespace = format!("workflow-workload-{}", root.display());
    Ok(open(config, clock)?)
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
    item_with_payload(id, stage, body(id, stage, size))
}

pub(crate) fn item_with_payload(id: usize, stage: usize, payload: Bytes) -> NewItem {
    NewItem {
        client_item_key: Some(ClientItemKey::new(format!("r-{id:09}-s{stage}")).unwrap()),
        priority: Some(PriorityValue::Int64(if stage == 2 {
            due(id)
        } else {
            id as i64
        })),
        not_before: Some(ts(if stage == 2 { due(id) } else { 1 })),
        metadata: metadata(stage, id),
        payload: Some(payload),
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

#[track_caller]
pub fn retry<T, F, Fut>(
    deadline: Instant,
    mut call: F,
) -> impl std::future::Future<Output = Result<T>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = EngineResult<T>>,
{
    let site = std::panic::Location::caller();
    async move {
        let started = Instant::now();
        let result = loop {
            match call().await {
                Ok(value) => break Ok(value),
                Err(EngineError::Backpressure { resource }) if Instant::now() < deadline => {
                    if std::env::var_os("FIREWEED_WORKLOAD_TIMING").is_some() {
                        eprintln!(
                            "api_retry resource={resource} at={}:{}",
                            site.file(),
                            site.line()
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await
                }
                Err(error) => break Err(error.into()),
            }
        };
        if std::env::var_os("FIREWEED_WORKLOAD_TIMING").is_some() {
            eprintln!(
                "api_us={} at={}:{}",
                started.elapsed().as_micros(),
                site.file(),
                site.line()
            );
        }
        result
    }
}

#[derive(Default)]
struct Observations {
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

#[allow(
    clippy::too_many_arguments,
    reason = "Each worker owns its queue, stage identity, observations, population bound, and clock inputs"
)]
async fn worker(
    fw: Arc<Fireweed>,
    q: QueueKey,
    stage: usize,
    worker: usize,
    cfg: Config,
    observations: Arc<Observations>,
    expected: usize,
    deadline: Instant,
    now: UtcTimestamp,
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
        if debug && generation == 0 {
            eprintln!("stage={stage} worker={worker} requesting claim");
        }
        if stage == SHARED_DISPATCH && cfg.profile == Profile::Snorri {
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
        let mut mutations = vec![];
        let mut commits = vec![];
        let mut successes = vec![];
        let mut failures = vec![];
        let mut retry_ids = vec![];
        let mut complete_ids = vec![];
        let mut failed_ids = vec![];
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
                    mutations.push(AddressedMutation {
                        item_id: claimed.item_id,
                        expected_item_version: Some(claimed.item_version),
                        predicates: vec![],
                        lease_guard: LeaseGuard::Match(
                            claimed
                                .lease_token
                                .clone()
                                .ok_or("missing enrichment lease")?,
                        ),
                        patch: ItemPatch {
                            lifecycle: LifecyclePatch::SetPending,
                            payload: BatchUpdateValue::Replace(Some(body(
                                id,
                                stage + 1,
                                cfg.payload_bytes,
                            ))),
                            metadata: BatchUpdateValue::Replace(metadata(stage + 1, id)),
                            priority: BatchUpdateValue::Replace(Some(PriorityValue::Int64(
                                if stage == 1 { due(id) } else { id as i64 },
                            ))),
                            not_before: BatchUpdateValue::Replace(Some(ts(if stage == 1 {
                                due(id)
                            } else {
                                1
                            }))),
                            ..Default::default()
                        },
                    });
                }
            } else {
                if cfg.faults
                    && id.is_multiple_of(19)
                    && observations.transient_failures.lock().unwrap().insert(id)
                {
                    if cfg.profile == Profile::Mutable {
                        if claimed.attempt_count >= claimed.max_attempts {
                            return Err(
                                "retry exhausted before the deterministic transient failure".into(),
                            );
                        }
                        mutations.push(AddressedMutation {
                            item_id: claimed.item_id,
                            expected_item_version: Some(claimed.item_version),
                            predicates: vec![],
                            lease_guard: LeaseGuard::Match(
                                claimed.lease_token.clone().ok_or("missing retry lease")?,
                            ),
                            patch: ItemPatch {
                                lifecycle: LifecyclePatch::SetPending,
                                not_before: BatchUpdateValue::Replace(Some(ts(100))),
                                ..Default::default()
                            },
                        });
                        observations.retries.fetch_add(1, Ordering::SeqCst);
                    } else {
                        retry_ids.push(claimed.item_id);
                    }
                    continue;
                }
                let failed = cfg.faults && id.is_multiple_of(31);
                if cfg.profile == Profile::Snorri {
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
                } else if cfg.profile == Profile::Mutable {
                    let mut tracking = claimed.metadata.clone();
                    tracking.insert(
                        "outcome",
                        MetadataValue::String(if failed { "failed" } else { "accepted" }.into()),
                    );
                    mutations.push(AddressedMutation {
                        item_id: claimed.item_id,
                        expected_item_version: Some(claimed.item_version),
                        predicates: vec![],
                        lease_guard: LeaseGuard::Match(
                            claimed
                                .lease_token
                                .clone()
                                .ok_or("missing delivery lease")?,
                        ),
                        patch: ItemPatch {
                            lifecycle: if failed {
                                LifecyclePatch::SetFailed
                            } else {
                                LifecyclePatch::SetComplete
                            },
                            metadata: BatchUpdateValue::Replace(tracking),
                            ..Default::default()
                        },
                    });
                } else if failed {
                    failed_ids.push(claimed.item_id);
                } else {
                    complete_ids.push(claimed.item_id);
                }
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
        if !mutations.is_empty() {
            let request = ItemMutationRequest {
                request_id: request_id.clone(),
                evaluated_at: now,
                dry_run: false,
                returning: ItemMutationReturning::Identity,
                gate_changes: vec![],
                operation: ItemMutationOperation::Addressed { entries: mutations },
            };
            let response = retry(deadline, || fw.mutate_items(&q, request.clone())).await?;
            for result in response.results {
                if !matches!(result.outcome, ItemMutationOutcome::Updated { .. }) {
                    return Err(format!("stage mutation rejected: {result:?}").into());
                }
            }
        }
        if !complete_ids.is_empty() {
            retry(deadline, || fw.complete(&q, complete_ids.iter().copied())).await?;
        }
        if !failed_ids.is_empty() {
            retry(deadline, || fw.fail(&q, failed_ids.iter().copied())).await?;
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
    if cfg.items == 0
        || cfg.cycles == 0
        || cfg.shards == 0
        || cfg.workers == 0
        || cfg.load_workers == 0
        || !(1..=1000).contains(&cfg.batch)
        || cfg.purge_batch.is_some_and(|n| !(1..=8192).contains(&n))
    {
        return Err(
            "items/shards/workers/load-workers must be positive, batch in 1..=1000, purge-batch in 1..=8192".into(),
        );
    }
    if cfg.recycle && cfg.profile == Profile::Snorri {
        return Err("recycling currently supports original-row mutable and bulk profiles".into());
    }
    let start = Instant::now();
    let deadline = start + cfg.deadline;
    let mut handles = tokio::task::JoinSet::new();
    for shard in 0..cfg.shards {
        let cfg = cfg.clone();
        let root = root.join(format!("shard-{shard}"));
        handles.spawn(async move {
            let clock = TestClock::at(200);
            let projection_root = cfg.projection_root.as_ref().map(|path| path.join(format!("shard-{shard}"))).unwrap_or_else(|| root.clone());
            let fw = Arc::new(open_store_with_projection_root(&root, cfg.memory, clock.clone(), &projection_root)?);
            let mut d = definition("workflow");
            if cfg.recycle { d.request_id_retention_ms = 1000; }
            let q = QueueKey::new(d.tenant_id.clone(), d.queue_id.clone());
            fw.create_queue(d).await?;
            let ids: Vec<_> = (shard..cfg.items).step_by(cfg.shards).collect(); let expected = ids.len();
            let mut cycles = Vec::new();
            for cycle in 0..if cfg.recycle { cfg.cycles } else { 1 } {
            clock.set(200 + cycle as u64 * 7200);
            let cycle_started = Instant::now();
            let loaded_ids = std::sync::Mutex::new(Vec::with_capacity(expected));
            let observations = Arc::new(Observations::default());
            let load = async {
                futures::stream::iter(0..ids.len().div_ceil(cfg.batch)).map(|batch_index| {
                    let ids = &ids; let cfg = &cfg; let fw = &fw; let q = &q; let loaded_ids = &loaded_ids;
                    async move {
                    let chunk = &ids[batch_index * cfg.batch..((batch_index + 1) * cfg.batch).min(ids.len())];
                    let items: Vec<_> = chunk.iter().map(|id| item(*id, if cfg.profile == Profile::Bulk { 2 } else { 0 }, cfg.payload_bytes)).collect();
                    let accepted = retry(deadline, || fw.push_batch(q, items.clone())).await
                        .map_err(|e| format!("load shard {shard} starting recipient {}: {e}", chunk[0]))?;
                    loaded_ids.lock().unwrap().extend(accepted);
                    if std::env::var_os("FIREWEED_WORKLOAD_DEBUG").is_some() { eprintln!("loaded {}", chunk.len()); }
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
                }}).buffer_unordered(cfg.load_workers).try_collect::<Vec<_>>().await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            };
            let consume = async {
                let mut workers = vec![];
                let stages = match cfg.profile {
                    Profile::Bulk => 2..3,
                    Profile::Mutable | Profile::Snorri => SHARED_DISPATCH..SHARED_DISPATCH + 1,
                };
                for stage in stages {
                    for w in 0..cfg.workers {
                        workers.push(worker(fw.clone(), q.clone(), stage, w, cfg.clone(), observations.clone(), expected, deadline, clock.now()));
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
            let processing_wall_s = cycle_started.elapsed().as_secs_f64();
            let purge_started = Instant::now();
            if cfg.recycle {
                clock.set(270 + cycle as u64 * 7200);
                let retained_ids = loaded_ids.into_inner().unwrap();
                for chunk in retained_ids.chunks(cfg.purge_batch.unwrap_or(cfg.batch)) {
                    let removed = retry(deadline, || fw.purge(&q, chunk.iter().copied(), false)).await?;
                    if removed != chunk.len() as u64 { return Err("recycling purge count mismatch".into()); }
                }
                if !cfg.memory && !retry(deadline, || fw.retained_items(&q,None,1)).await?.is_empty() {
                    return Err("recycling left projected queue rows".into());
                }
                let retained = retry(deadline, || fw.metrics(&q)).await?;
                if retained.pending + retained.leased + retained.complete + retained.failed != 0 {
                    return Err("recycling left retained queue rows".into());
                }
            }
            let report = serde_json::json!({"shard": shard, "cycle": cycle, "items": expected,
                "delivered": observations.delivered.load(Ordering::SeqCst), "failed": observations.failed.load(Ordering::SeqCst),
                "retries": observations.retries.load(Ordering::SeqCst), "claims": observations.claims.load(Ordering::SeqCst),
                "complete": metrics.complete, "pending": metrics.pending, "leased": metrics.leased,
                "processing_wall_s": processing_wall_s,
                "purge_wall_s": if cfg.recycle { purge_started.elapsed().as_secs_f64() } else { 0.0 },
                "wall_s": cycle_started.elapsed().as_secs_f64(), "process_rss_kib": process_rss_kib(),
                "projection_bytes": std::fs::metadata(projection_root.join("projection.db")).ok().map(|m| m.len()),
                "projection_wal_bytes": std::fs::metadata(projection_root.join("projection.db-wal")).ok().map(|m| m.len()) });
            if cfg.recycle { eprintln!("workflow_cycle_complete {report}"); }
            cycles.push(report);
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(if cfg.recycle {
                serde_json::json!({"shard": shard, "cycles": cycles})
            } else { cycles.pop().unwrap() })
        });
    }
    let mut reports = vec![];
    while let Some(result) = handles.join_next().await {
        reports.push(result??);
    }
    reports.sort_by_key(|r| r["shard"].as_u64());
    Ok(
        serde_json::json!({ "schema": "workflow-capacity/v6", "profile": format!("{:?}", cfg.profile),
        "cell": if cfg.memory { "memory--memory" } else { "s3--turso" },
        "items": cfg.items, "cycles": if cfg.recycle { cfg.cycles } else { 1 }, "includes_purge": cfg.recycle,
        "physical_shards": cfg.shards, "projection_root": cfg.projection_root, "workers_per_pool": cfg.workers, "load_workers_per_shard": cfg.load_workers,
        "purge_batch": cfg.purge_batch.unwrap_or(cfg.batch),
        "worker_pools_per_shard": 1,
        "dispatch": if cfg.profile != Profile::Bulk { "shared-normal-claim" } else { "stage-filtered" },
        "atomic_original_row_mutation": cfg.profile == Profile::Mutable,
        "batch": cfg.batch, "payload_bytes": cfg.payload_bytes, "faults": cfg.faults,
        "settled_wall_s": start.elapsed().as_secs_f64(),
        "completed_lifecycles_per_s": (cfg.items * if cfg.recycle { cfg.cycles } else { 1 }) as f64 / start.elapsed().as_secs_f64(), "shards": reports }),
    )
}
