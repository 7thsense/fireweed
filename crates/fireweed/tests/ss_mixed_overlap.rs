//! Settlement-aware Seventh Sense mixed-overlap and admission baseline.
//!
//! The default run is the release evidence lane (N=10,000):
//!
//! ```text
//! cargo test -p fireweed --test ss_mixed_overlap --release \
//!   ss_mixed_overlap_baseline -- --exact --nocapture
//! ```
//!
//! `SS_MIXED_N` may be lowered for local calibration. Set
//! `SS_EVIDENCE_WRITE=0` to avoid writing a non-authoritative calibration run.
//!
//! S3s shadow calibration (inert S3c composition; serving is unchanged):
//!
//! ```text
//! cargo test -p fireweed --test ss_mixed_overlap -- --exact --nocapture shadow_
//! cargo test -p fireweed --test ss_mixed_overlap -- --ignored --exact --nocapture \
//!   shadow_mutation_generation_calibration
//! ```
//!
//! S3m Claim-turn/slot and exact fence-bound calibration after S3c and packed
//! Claim apply. Production selection-fence dispositions stay inert:
//!
//! ```text
//! cargo test -p fireweed --test ss_mixed_overlap -- --exact --nocapture shadow_claim_
//! cargo test -p fireweed --test ss_mixed_overlap -- --ignored --exact --nocapture \
//!   shadow_claim_drain_calibration_uses_exact_high_water
//! ```

#![cfg(all(feature = "objectlog", feature = "turso"))]

use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use fireweed::turso_compose::open_turso_projection_async;
use fireweed::*;
use fireweed_core::{Metadata, MetadataValue};
use fireweed_engine::{
    AsyncLogStore, AsyncProjectionStore, CLAIM_DRIVER_INGRESS_RESOURCE,
    CLAIM_DRIVER_SLOTS_RESOURCE, CLAIM_GENERATION_MAX_REQUESTS, CLAIM_MAX_DRIVERS,
    CLAIM_QUEUE_TURN_RESOURCE, CLAIM_TURN_DEFAULT_MAX_WAIT, ClaimCoordinator,
    ClaimDriverReadAdmission, ClaimQueueTurn, CoordinationError, DEFAULT_KEYED_QUEUE_MAX_PER_KEY,
    DRIVER_SLOT_DEFAULT_MAX_WAIT, GENERATION_MAX_ITEMS, KeyedQueueGate,
    MUTATION_MAX_REQUESTS_PER_QUEUE, MUTATION_SEQUENCER_DEFAULT_MAX_WAIT,
    MUTATION_SEQUENCER_RESOURCE, MUTATION_SEQUENCER_WAIT_RESOURCE, MutationGenerationKind,
    MutationIngress, MutationSequencer, OUTCOME_READ_SLOTS_RESOURCE, OUTCOME_SLOT_DEFAULT_MAX_WAIT,
    OutcomeReadAdmission, QueueGateError, S3M_DERIVED_CLAIM_SLOT_WAIT,
    S3M_DERIVED_COVERAGE_OR_WORK_WAIT, S3M_DERIVED_FENCE_ACQUIRE_WAIT, S3M_DERIVED_TURN_WAIT,
    S3M_DRIVER_POOL_BORROW_CAP, S3M_WAIT_FLOOR, S3S_COVERAGE_OR_WORK_CAP,
    S3S_DERIVED_COVERAGE_OR_WORK_WAIT, S3S_DERIVED_DRIVER_SLOT_WAIT, S3S_DERIVED_OUTCOME_SLOT_WAIT,
    S3S_DERIVED_TURN_WAIT, S3S_FENCE_ACQUIRE_CARRIED_CAP, S3S_WAIT_FLOOR,
    SHARED_DRIVER_SLOTS_RESOURCE, SelectionFence, SelectionFenceAdmission,
    SharedDriverReadAdmission, abort_unplanned_generation_on_deadline, derive_structural_wait,
};
use fireweed_objectlog::{ObjectLogEngineStore, flush_config_from_segment};
use fireweed_turso::{
    COMMITTED_DRIVER_POOL_SIZE, COMMITTED_OUTCOME_POOL_SIZE, TursoConfig, TursoRelational,
};
use serde_json::{Value, json};

const DEFAULT_N: usize = 10_000;
const BATCH: usize = 100;
const CLAIM_BATCH: usize = 100;
const RETRY_CADENCE: Duration = Duration::from_millis(25);
const OBSERVATION_SAMPLES: usize = 16;
const PACK_LINGER_MS: u64 = 20;
const KEYED_QUEUE_PER_KEY_WAITERS: &str = "keyed queue per-key waiters";
const SEVENTEEN_READER_DEADLINE: Duration = Duration::from_millis(31_050);
const S3M_SOAK_N: usize = 16;
const S3M_CALIBRATION_N: usize = 100_000;
const T2_ITEMS_PER_SECOND: f64 = 4_000.0;

type MixedRuntime = Fireweed;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn now() -> UtcTimestamp {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    UtcTimestamp::new(elapsed.as_secs() as i64, elapsed.subsec_nanos()).unwrap()
}

fn unique_root() -> PathBuf {
    std::env::temp_dir().join(format!(
        "fireweed-ss-mixed-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ))
}

fn queue_key(name: &str) -> QueueKey {
    QueueKey::new(
        TenantId::new("t-ss-mixed").unwrap(),
        QueueId::new(name).unwrap(),
    )
}

fn qdef(name: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t-ss-mixed").unwrap(),
        queue_id: QueueId::new(name).unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Timestamp,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 3_600_000,
        eligibility_policy: EligibilityPolicy {
            metadata_blockers: BTreeMap::new(),
            gate_keys: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: Some(4),
            max_gates_per_request: Some(4),
        },
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 86_400_000,
        client_item_key_retention_ms: 86_400_000,
        terminal_retention_ms: 86_400_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 1_000,
        max_claim_batch_size: 1_000,
        max_eligible_group_size: Some(100),
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: false,
    }
}

fn metadata(lane: &str, ordinal: usize) -> Metadata {
    let mut value = Metadata::new();
    value.insert("lane", MetadataValue::String(lane.to_owned()));
    value.insert("ordinal", MetadataValue::Integer(ordinal as i64));
    value
}

fn realistic_item(prefix: &str, ordinal: usize, due: UtcTimestamp) -> NewItem {
    let mut fields = BTreeMap::new();
    fields.insert("workflow".into(), Bytes::from(vec![b'w'; 96]));
    fields.insert("profile".into(), Bytes::from(vec![b'p'; 256]));
    NewItem {
        client_item_key: Some(ClientItemKey::new(format!("{prefix}-{ordinal:08}")).unwrap()),
        priority: Some(PriorityValue::Timestamp(due)),
        group_key: Some(GroupKey::new(format!("{prefix}-job-{}", ordinal % 100)).unwrap()),
        not_before: Some(due),
        payload: Some(Bytes::from(vec![b'x'; 1_024])),
        fields,
        metadata: metadata(prefix, ordinal),
        gate_keys: vec!["serving-open".into()],
        ..Default::default()
    }
}

#[derive(Clone, Debug, Default)]
struct Latency {
    samples: Vec<Duration>,
}

impl Latency {
    fn record(&mut self, value: Duration) {
        self.samples.push(value);
    }

    fn percentile_ms(&self, percentile: f64) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let mut values = self.samples.clone();
        values.sort_unstable();
        let index = ((percentile / 100.0) * (values.len() as f64 - 1.0)).round() as usize;
        values[index.min(values.len() - 1)].as_secs_f64() * 1_000.0
    }

    fn evidence(&self) -> Value {
        json!({
            "samples": self.samples.len(),
            "p50_ms": self.percentile_ms(50.0),
            "p95_ms": self.percentile_ms(95.0),
            "p99_ms": self.percentile_ms(99.0),
        })
    }
}

#[derive(Debug)]
struct RequestTiming {
    request_id: String,
    retries: usize,
    admitted_service: Duration,
    original_age: Duration,
}

impl RequestTiming {
    fn evidence(&self) -> Value {
        json!({
            "request_id": self.request_id,
            "retry_count": self.retries,
            "admitted_service_ms": self.admitted_service.as_secs_f64() * 1_000.0,
            "original_request_to_success_age_ms": self.original_age.as_secs_f64() * 1_000.0,
        })
    }
}

fn retryable_admission(error: &EngineError) -> bool {
    match error {
        EngineError::Backpressure { .. } => true,
        EngineError::Storage(message)
            if message.contains("PerKeyFull") || message.contains(KEYED_QUEUE_PER_KEY_WAITERS) =>
        {
            true
        }
        _ => false,
    }
}

async fn retry_25ms<T, F, Fut>(
    request_id: String,
    mut operation: F,
) -> EngineResult<(T, RequestTiming)>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = EngineResult<T>>,
{
    let original = Instant::now();
    let mut retries = 0usize;
    loop {
        let admitted = Instant::now();
        match operation().await {
            Ok(value) => {
                return Ok((
                    value,
                    RequestTiming {
                        request_id,
                        retries,
                        admitted_service: admitted.elapsed(),
                        original_age: original.elapsed(),
                    },
                ));
            }
            Err(error) if retryable_admission(&error) => {
                retries += 1;
                assert!(retries < 100_000, "fixed-cadence retry failed to converge");
                tokio::time::sleep(RETRY_CADENCE).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn cohort_evidence(
    name: &str,
    timings: &[RequestTiming],
    completed_units: usize,
    settled_wall: Duration,
) -> Value {
    let mut service = Latency::default();
    let mut age = Latency::default();
    for timing in timings {
        service.record(timing.admitted_service);
        age.record(timing.original_age);
    }
    json!({
        "name": name,
        "fixed_retry_cadence_ms": RETRY_CADENCE.as_millis(),
        "original_request_count": timings.len(),
        "completed_original_request_count": timings.len(),
        "completed_units": completed_units,
        "capacity_rejections": timings.iter().map(|timing| timing.retries).sum::<usize>(),
        "admitted_service": service.evidence(),
        "original_request_to_success_age": age.evidence(),
        "settled_wall_s": settled_wall.as_secs_f64(),
        "settled_throughput_per_s": completed_units as f64 / settled_wall.as_secs_f64().max(1e-9),
        "requests": timings.iter().map(RequestTiming::evidence).collect::<Vec<_>>(),
    })
}

async fn settle(fireweed: &MixedRuntime, queue: &QueueKey) -> EngineResult<QueueMetrics> {
    // A retained read waits for the durable frontier captured on entry. Metrics
    // alone may fold an unapplied log tail and cannot establish physical drain.
    fireweed.retained_items(queue, None, 1).await?;
    fireweed.metrics(queue).await
}

async fn push_in_batches(
    fireweed: &MixedRuntime,
    queue: &QueueKey,
    items: Vec<NewItem>,
) -> EngineResult<Vec<ItemId>> {
    let mut ids = Vec::with_capacity(items.len());
    for batch in items.chunks(BATCH) {
        ids.extend(fireweed.push_batch(queue, batch.to_vec()).await?);
    }
    Ok(ids)
}

fn response_bytes(items: &[ClaimedItem]) -> usize {
    items
        .iter()
        .map(|item| {
            item.payload.as_ref().map_or(0, Bytes::len)
                + item
                    .fields
                    .iter()
                    .map(|(key, value)| key.len() + value.len())
                    .sum::<usize>()
                + serde_json::to_vec(&item.metadata).map_or(0, |bytes| bytes.len())
                + item.gate_keys.iter().map(String::len).sum::<usize>()
                + item
                    .entity
                    .as_ref()
                    .and_then(|value| serde_json::to_vec(value).ok())
                    .map_or(0, |bytes| bytes.len())
        })
        .sum()
}

fn wal_bytes(path: &Path) -> u64 {
    std::fs::metadata(format!("{}-wal", path.display()))
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn source_sha() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".into())
}

fn host_name() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|value| value.trim().to_owned())
        })
        .unwrap_or_else(|| "unknown".into())
}

fn pack_wait_evidence(compatible_mutations: &Value) -> Value {
    json!({
        "configured_linger_ms": PACK_LINGER_MS,
        "measurement": "compatible BatchUpdate admitted service includes current pack wait",
        "direct_wait_metric": "not exposed through the backend-erased public Fireweed handle at S0",
        "compatible_mutation_admitted_service": compatible_mutations["admitted_service"].clone(),
    })
}

fn open_mixed_product(root: &Path) -> EngineResult<Fireweed> {
    open(
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
                target_bytes: 256 * 1_024,
                max_latency_ms: 50,
            },
            namespace: "ss-mixed".to_owned(),
            recovery: RecoveryPolicy::default(),
        },
        Arc::new(SystemClock),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objectlog_turso_non_default_claim_registers_lease_before_render() -> EngineResult<()> {
    let root = unique_root();
    let fireweed = open_mixed_product(&root)?;
    let queue = queue_key("q-legacy-claim-regression");
    fireweed
        .create_queue(qdef("q-legacy-claim-regression"))
        .await?;
    let due = now();
    let item = realistic_item("legacy-claim", 0, due);
    let expected_group = item.group_key.clone().expect("realistic group");
    let original = fireweed.push_batch(&queue, vec![item]).await?;
    settle(&fireweed, &queue).await?;

    let claimed = fireweed
        .claim_with(
            &queue,
            1,
            30_000,
            ClaimCompatibility {
                group_key: Some(expected_group.clone()),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].item_id, original[0]);
    assert_eq!(claimed[0].group_key.as_ref(), Some(&expected_group));
    assert_eq!(claimed[0].payload.as_ref().map(Bytes::len), Some(1_024));
    assert_eq!(claimed[0].fields.get("profile").map(Bytes::len), Some(256));
    assert_eq!(
        claimed[0].metadata.get("lane"),
        Some(&MetadataValue::String("legacy-claim".into()))
    );
    assert_eq!(claimed[0].gate_keys, vec!["serving-open"]);
    fireweed.complete(&queue, original).await?;
    let metrics = settle(&fireweed, &queue).await?;
    assert_eq!(metrics.complete, 1);
    assert_eq!(metrics.leased, 0);

    drop(fireweed);
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objectlog_turso_eventual_lifecycle_accepts_durable_append() -> EngineResult<()> {
    let root = unique_root();
    let fireweed = open_mixed_product(&root)?;
    let queue = queue_key("q-eventual-lifecycle-regression");
    fireweed
        .create_queue(qdef("q-eventual-lifecycle-regression"))
        .await?;
    let due = now();
    let original = fireweed
        .push_batch(
            &queue,
            vec![
                realistic_item("eventual-lifecycle", 0, due),
                realistic_item("eventual-lifecycle", 1, due),
            ],
        )
        .await?;
    settle(&fireweed, &queue).await?;
    let claimed = fireweed.claim(&queue, 2, 60_000).await?;
    assert_eq!(claimed.len(), 2);

    fireweed.renew(&queue, [original[0]], 45_000).await?;
    assert_eq!(fireweed.purge(&queue, [original[1]], true).await?, 1);
    let intermediate = settle(&fireweed, &queue).await?;
    assert_eq!(intermediate.leased, 1);
    assert_eq!(intermediate.pending, 0);

    fireweed.complete(&queue, [original[0]]).await?;
    let settled = settle(&fireweed, &queue).await?;
    assert_eq!(settled.complete, 1);
    assert_eq!(settled.leased, 0);
    assert_eq!(settled.pending, 0);

    drop(fireweed);
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

async fn observation_cohort(
    fireweed: &MixedRuntime,
    projection: &TursoRelational,
    queue: &QueueKey,
    keys: &[ClientItemKey],
) -> EngineResult<Value> {
    let mut peek = Latency::default();
    let mut pending = Latency::default();
    let mut page = Latency::default();
    let mut range = Latency::default();
    let mut live = Latency::default();
    let mut metrics = Latency::default();
    let mut counts = Vec::new();

    // This separate white-box reader must observe applied leases before timing.
    // Public metrics can report durable outcomes without waiting for projection.
    settle(fireweed, queue).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if projection.server_pending(queue).await?.len() == 32 {
                return EngineResult::Ok(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| EngineError::Storage("observation projection did not catch up".into()))??;
    for _ in 0..OBSERVATION_SAMPLES {
        let started = Instant::now();
        let peeked = projection.server_peek(queue, 16).await?;
        peek.record(started.elapsed());

        let started = Instant::now();
        let pending_rows = projection.server_pending(queue).await?;
        pending.record(started.elapsed());

        let started = Instant::now();
        let pending_page = projection.server_pending_page(queue, None, 16).await?;
        page.record(started.elapsed());

        let started = Instant::now();
        let pending_range = projection
            .server_pending_range(queue, None, None, None, 16)
            .await?;
        range.record(started.elapsed());

        let started = Instant::now();
        let live_rows = projection
            .server_live_items(queue, &keys[..keys.len().min(16)])
            .await?;
        live.record(started.elapsed());

        let started = Instant::now();
        let queue_metrics = projection.server_metrics(queue).await?;
        metrics.record(started.elapsed());

        let present = live_rows.iter().filter(|row| row.is_some()).count();
        counts.push(json!({
            "server_peek": peeked.len(),
            "server_pending": pending_rows.len(),
            "server_pending_page": pending_page.entries.len(),
            "server_pending_range": pending_range.len(),
            "server_live_items": present,
            "server_metrics_total": queue_metrics.pending + queue_metrics.leased + queue_metrics.complete + queue_metrics.failed,
        }));
        assert_eq!(peeked.len(), 16);
        assert_eq!(pending_rows.len(), 32);
        assert_eq!(pending_page.entries.len(), 16);
        assert_eq!(pending_range.len(), 16);
        assert_eq!(present, 16);
        assert_eq!(queue_metrics.pending + queue_metrics.leased, 64);
    }

    let rate = |latency: &Latency| {
        latency.samples.len() as f64
            / latency
                .samples
                .iter()
                .copied()
                .sum::<Duration>()
                .as_secs_f64()
                .max(1e-9)
    };
    Ok(json!({
        "samples_per_operation": OBSERVATION_SAMPLES,
        "operations": {
            "server_peek": { "rate_per_s": rate(&peek), "latency": peek.evidence() },
            "server_pending": { "rate_per_s": rate(&pending), "latency": pending.evidence() },
            "server_pending_page": { "rate_per_s": rate(&page), "latency": page.evidence() },
            "server_pending_range": { "rate_per_s": rate(&range), "latency": range.evidence() },
            "server_live_items": { "rate_per_s": rate(&live), "latency": live.evidence() },
            "server_metrics": { "rate_per_s": rate(&metrics), "latency": metrics.evidence() },
        },
        "exact_response_counts": counts,
    }))
}

async fn compatible_mutation_cohort(
    fireweed: Arc<MixedRuntime>,
    queue: QueueKey,
    due: UtcTimestamp,
) -> EngineResult<Value> {
    fireweed.create_queue(qdef(queue.queue_id.as_str())).await?;
    let keys: Vec<_> = (0..64)
        .map(|index| ClientItemKey::new(format!("mutation-{index:03}")).unwrap())
        .collect();
    let items = keys
        .iter()
        .enumerate()
        .map(|(index, key)| NewItem {
            client_item_key: Some(key.clone()),
            not_before: Some(due),
            priority: Some(PriorityValue::Timestamp(due)),
            group_key: Some(GroupKey::new(format!("mutation-group-{index}")).unwrap()),
            payload: Some(Bytes::from(vec![b'm'; 512])),
            metadata: metadata("mutation", index),
            ..Default::default()
        })
        .collect();
    push_in_batches(fireweed.as_ref(), &queue, items).await?;
    settle(fireweed.as_ref(), &queue).await?;

    let started = Instant::now();
    let futures = (0..32).map(|request_index| {
        let fireweed = Arc::clone(&fireweed);
        let queue = queue.clone();
        let pair = [
            keys[request_index * 2].clone(),
            keys[request_index * 2 + 1].clone(),
        ];
        async move {
            let evidence_id = format!("compatible-mutation-{request_index:02}");
            retry_25ms(evidence_id.clone(), || {
                let updates = pair
                    .iter()
                    .map(|key| BatchUpdateEntry {
                        item_ref: BatchUpdateItemRef::ClientItemKey(key.clone()),
                        expected_item_version: None,
                        priority: BatchUpdateValue::Keep,
                        not_before: BatchUpdateValue::Keep,
                        payload: BatchUpdateValue::Keep,
                        metadata: BatchUpdateValue::Replace(metadata(
                            "mutation-updated",
                            request_index,
                        )),
                        gate_keys: BatchUpdateValue::Keep,
                        fields: BatchUpdateValue::Keep,
                    })
                    .collect();
                fireweed.batch_update(
                    &queue,
                    BatchUpdateRequest {
                        request_id: RequestId::new(evidence_id.clone()).unwrap(),
                        updates,
                    },
                )
            })
            .await
        }
    });
    let mut timings = Vec::new();
    for result in futures::future::join_all(futures).await {
        let (response, timing) = result?;
        assert_eq!(
            response
                .results
                .iter()
                .filter(|outcome| matches!(outcome, BatchUpdateOutcome::Updated { .. }))
                .count(),
            2
        );
        timings.push(timing);
    }
    let metrics = settle(fireweed.as_ref(), &queue).await?;
    assert_eq!(metrics.pending, 64);
    Ok(cohort_evidence(
        "32_compatible_mutations",
        &timings,
        64,
        started.elapsed(),
    ))
}

async fn incompatible_claim_cohort(
    fireweed: Arc<MixedRuntime>,
    queue: QueueKey,
    due: UtcTimestamp,
) -> EngineResult<Value> {
    fireweed.create_queue(qdef(queue.queue_id.as_str())).await?;
    let mut items = Vec::new();
    for group in 0..4 {
        let mut item = realistic_item("claim-key", group, due);
        item.group_key = Some(GroupKey::new(format!("claim-group-{group}")).unwrap());
        items.push(item);
    }
    let original_ids = push_in_batches(fireweed.as_ref(), &queue, items).await?;
    settle(fireweed.as_ref(), &queue).await?;

    let started = Instant::now();
    let mut timings = Vec::new();
    let mut claimed_ids = HashSet::new();
    let mut batches = Vec::new();
    // These compatibility keys are intentionally incompatible and therefore
    // cannot share a driver generation. Run their admitted services in FIFO
    // order while preserving one closed, same-queue request cohort.
    for group in 0..4 {
        let request_id = format!("claim-compatibility-key-{group}");
        let compatibility = ClaimCompatibility {
            group_key: Some(GroupKey::new(format!("claim-group-{group}")).unwrap()),
            ..Default::default()
        };
        let (items, timing) = retry_25ms(request_id, || {
            fireweed.claim_with(&queue, 1, 30_000, compatibility.clone())
        })
        .await?;
        assert_eq!(items.len(), 1);
        let ids: Vec<_> = items.iter().map(|item| item.item_id).collect();
        claimed_ids.extend(ids.iter().copied());
        batches.push(ids);
        timings.push(timing);
    }
    for result in futures::future::join_all(
        batches
            .into_iter()
            .map(|ids| fireweed.complete(&queue, ids)),
    )
    .await
    {
        result?;
    }
    let metrics = settle(fireweed.as_ref(), &queue).await?;
    assert_eq!(metrics.complete, 4);
    assert_eq!(claimed_ids.len(), original_ids.len());
    assert!(original_ids.iter().all(|id| claimed_ids.contains(id)));
    let mut evidence = cohort_evidence(
        "four_incompatible_legal_claim_keys",
        &timings,
        4,
        started.elapsed(),
    );
    evidence["submission"] = json!("FIFO closed cohort on one queue");
    evidence["compatibility_keys"] = json!(
        (0..4)
            .map(|group| format!("claim-group-{group}"))
            .collect::<Vec<_>>()
    );
    evidence["original_item_ids"] = json!(
        original_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    );
    evidence["claimed_original_item_ids"] = json!(
        original_ids
            .iter()
            .filter(|id| claimed_ids.contains(id))
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    );
    evidence["every_original_item_completed"] = json!(true);
    Ok(evidence)
}

async fn mixed_gate_command(
    fireweed: &MixedRuntime,
    queue: &QueueKey,
    index: usize,
    item_id: ItemId,
) -> EngineResult<()> {
    match index % 4 {
        0 => fireweed.renew(queue, [item_id], 30_000).await,
        1 => fireweed.reassign(queue, [item_id], 30_000).await,
        2 => {
            let purged = fireweed.purge(queue, [item_id], true).await?;
            assert_eq!(purged, 1);
            Ok(())
        }
        _ => fireweed.renew(queue, [item_id], 45_000).await,
    }
}

async fn same_keyed_gate_cohort(
    fireweed: Arc<MixedRuntime>,
    queue: QueueKey,
    due: UtcTimestamp,
) -> EngineResult<Value> {
    fireweed.create_queue(qdef(queue.queue_id.as_str())).await?;
    let seeds = (0..32)
        .map(|ordinal| realistic_item("gate-lifecycle", ordinal, due))
        .collect();
    let original_ids = Arc::new(push_in_batches(fireweed.as_ref(), &queue, seeds).await?);
    settle(fireweed.as_ref(), &queue).await?;
    let leased = fireweed.claim(&queue, 32, 60_000).await?;
    assert_eq!(leased.len(), 32);
    assert!(
        original_ids
            .iter()
            .all(|id| leased.iter().any(|item| item.item_id == *id))
    );

    let started = Instant::now();
    let futures = (0..32).map(|index| {
        let fireweed = Arc::clone(&fireweed);
        let queue = queue.clone();
        let original_ids = Arc::clone(&original_ids);
        async move {
            retry_25ms(format!("same-key-command-{index:02}"), || {
                mixed_gate_command(fireweed.as_ref(), &queue, index, original_ids[index])
            })
            .await
        }
    });
    let mut timings = Vec::new();
    for result in futures::future::join_all(futures).await {
        let (_, timing) = result?;
        timings.push(timing);
    }
    let intermediate = settle(fireweed.as_ref(), &queue).await?;
    assert_eq!(intermediate.complete, 0);
    assert_eq!(intermediate.pending, 0);
    assert_eq!(intermediate.leased, 24);

    let retained_ids = original_ids
        .iter()
        .enumerate()
        .filter(|(index, _)| index % 4 != 2)
        .map(|(_, id)| *id)
        .collect::<Vec<_>>();
    fireweed.complete(&queue, retained_ids).await?;
    let settled = settle(fireweed.as_ref(), &queue).await?;
    assert_eq!(settled.complete, 24);
    assert_eq!(settled.pending, 0);
    assert_eq!(settled.leased, 0);

    let mut evidence = cohort_evidence(
        "32_mixed_commands_one_keyed_queue_gate_key",
        &timings,
        32,
        started.elapsed(),
    );
    evidence["original_item_ids"] = json!(
        original_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    );
    evidence["declared_item_outcomes"] = json!(
        original_ids
            .iter()
            .enumerate()
            .map(|(index, item_id)| json!({
                "item_id": item_id.to_string(),
                "outcome": match index % 4 {
                    0 => "renewed_30s_then_completed",
                    1 => "reassigned_30s_then_completed",
                    2 => "force_purged",
                    _ => "renewed_45s_then_completed",
                },
            }))
            .collect::<Vec<_>>()
    );
    evidence["every_original_item_reached_declared_outcome"] = json!(true);
    Ok(evidence)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn ss_mixed_overlap_baseline() -> EngineResult<()> {
    let process_started = Instant::now();
    let n = env_usize("SS_MIXED_N", DEFAULT_N);
    assert!(n > 0 && n.is_multiple_of(CLAIM_BATCH));

    let root = unique_root();
    let log_root = root.join("log");
    let projection_path = root.join("projection.db");
    std::fs::create_dir_all(&log_root).expect("mixed evidence root");
    let log =
        ObjectLogEngineStore::open_local(&log_root, flush_config_from_segment(256 * 1_024, 50))
            .await?;

    // Measure the durable metadata operation on a dedicated, never-served key
    // before moving the log into the product composition.
    let epoch_probe_queue = queue_key("q-epoch-probe");
    let epoch_started = Instant::now();
    let acquired_epoch = AsyncLogStore::acquire_epoch(&log, epoch_probe_queue.clone()).await?;
    let epoch_acquire = epoch_started.elapsed();
    assert_eq!(acquired_epoch, 1);

    drop(log);
    let fireweed = Arc::new(open_mixed_product(&root)?);
    let observation_reader = open_turso_projection_async(&projection_path).await?;
    let wal_before = wal_bytes(&projection_path);

    let main_queue = queue_key("q-main");
    let cross_queue = queue_key("q-cross");
    fireweed.create_queue(qdef("q-main")).await?;
    fireweed.create_queue(qdef("q-cross")).await?;
    let due = now();
    let future = UtcTimestamp::new(due.seconds.saturating_add(86_400), due.nanoseconds).unwrap();
    let ready_items: Vec<_> = (0..n)
        .map(|ordinal| realistic_item("original", ordinal, due))
        .collect();
    let original_ids = push_in_batches(fireweed.as_ref(), &main_queue, ready_items).await?;
    let seeded = settle(fireweed.as_ref(), &main_queue).await?;
    assert_eq!(seeded.pending, n as u64);

    let mixed_started = Instant::now();
    let producer = {
        let fireweed = Arc::clone(&fireweed);
        let main_queue = main_queue.clone();
        let cross_queue = cross_queue.clone();
        async move {
            let mut append = Latency::default();
            let mut cross = Latency::default();
            for chunk in (0..n).step_by(BATCH) {
                let end = (chunk + BATCH).min(n);
                let main_items = (chunk..end)
                    .map(|ordinal| realistic_item("far-future", ordinal, future))
                    .collect();
                let cross_items = (chunk..end)
                    .map(|ordinal| realistic_item("cross-future", ordinal, future))
                    .collect();
                let main = async {
                    let started = Instant::now();
                    let ids = fireweed.push_batch(&main_queue, main_items).await?;
                    EngineResult::Ok((started.elapsed(), ids.len()))
                };
                let other = async {
                    let started = Instant::now();
                    let ids = fireweed.push_batch(&cross_queue, cross_items).await?;
                    EngineResult::Ok((started.elapsed(), ids.len()))
                };
                let (main_result, cross_result) = tokio::join!(main, other);
                let (elapsed, count) = main_result?;
                append.record(elapsed);
                assert_eq!(count, end - chunk);
                let (elapsed, count) = cross_result?;
                cross.record(elapsed);
                assert_eq!(count, end - chunk);
            }
            EngineResult::Ok((append, cross))
        }
    };
    let worker = {
        let fireweed = Arc::clone(&fireweed);
        let main_queue = main_queue.clone();
        async move {
            let mut claim = Latency::default();
            let mut complete = Latency::default();
            let mut fill = Vec::new();
            let mut bytes = Vec::new();
            let mut seen = HashSet::with_capacity(n);
            while seen.len() < n {
                let started = Instant::now();
                let items = fireweed.claim(&main_queue, CLAIM_BATCH, 30_000).await?;
                claim.record(started.elapsed());
                if items.is_empty() {
                    tokio::time::sleep(RETRY_CADENCE).await;
                    continue;
                }
                fill.push(items.len());
                bytes.push(response_bytes(&items));
                let ids: Vec<_> = items.iter().map(|item| item.item_id).collect();
                seen.extend(ids.iter().copied());
                let started = Instant::now();
                fireweed.complete(&main_queue, ids).await?;
                complete.record(started.elapsed());
            }
            EngineResult::Ok((claim, complete, fill, bytes, seen))
        }
    };
    let (producer_result, worker_result) = tokio::join!(producer, worker);
    let (append_latency, cross_queue_append_latency) = producer_result?;
    let (claim_latency, complete_latency, fill, response_sizes, seen) = worker_result?;
    let ack_wall = mixed_started.elapsed();
    let main_metrics = settle(fireweed.as_ref(), &main_queue).await?;
    let cross_metrics = settle(fireweed.as_ref(), &cross_queue).await?;
    let settled_wall = mixed_started.elapsed();
    let wal_after_mixed_settle = wal_bytes(&projection_path);
    assert_eq!(seen.len(), original_ids.len());
    assert!(original_ids.iter().all(|id| seen.contains(id)));
    assert_eq!(main_metrics.complete, n as u64);
    assert_eq!(main_metrics.pending, n as u64);
    assert_eq!(main_metrics.leased, 0);
    assert_eq!(cross_metrics.pending, n as u64);

    let _emission_position = fireweed.current_position(&main_queue).await?;
    let emission_started = Instant::now();
    let emission = observation_reader
        .server_terminal_emission_metrics(&main_queue)
        .await?;
    let emission_cursor_wait = emission_started.elapsed();

    let observation_queue = queue_key("q-observation");
    fireweed.create_queue(qdef("q-observation")).await?;
    let observation_keys: Vec<_> = (0..64)
        .map(|index| ClientItemKey::new(format!("observe-{index:03}")).unwrap())
        .collect();
    let observation_items = observation_keys
        .iter()
        .enumerate()
        .map(|(index, key)| {
            let mut item = realistic_item("observe", index, due);
            item.client_item_key = Some(key.clone());
            item
        })
        .collect();
    push_in_batches(fireweed.as_ref(), &observation_queue, observation_items).await?;
    settle(fireweed.as_ref(), &observation_queue).await?;
    let held = fireweed.claim(&observation_queue, 32, 60_000).await?;
    assert_eq!(held.len(), 32);
    let held_token = held[0]
        .lease_token
        .clone()
        .expect("observation claim lease token");
    assert!(
        held.iter()
            .all(|item| item.lease_token.as_ref() == Some(&held_token))
    );
    observation_reader
        .remember_leases(
            &observation_queue,
            &held.iter().map(|item| item.item_id).collect::<Vec<_>>(),
            held_token,
        )
        .await;
    let observation = observation_cohort(
        fireweed.as_ref(),
        &observation_reader,
        &observation_queue,
        &observation_keys,
    )
    .await?;

    let compatible = compatible_mutation_cohort(
        Arc::clone(&fireweed),
        queue_key("q-compatible-mutations"),
        due,
    )
    .await?;
    let incompatible_claims = incompatible_claim_cohort(
        Arc::clone(&fireweed),
        queue_key("q-incompatible-claims"),
        due,
    )
    .await?;
    let keyed_gate =
        same_keyed_gate_cohort(Arc::clone(&fireweed), queue_key("q-keyed-gate"), due).await?;

    let wal_final = wal_bytes(&projection_path);
    let numeric_percentiles = |values: &[usize]| {
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        let at = |p: f64| {
            let index = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
            sorted[index.min(sorted.len() - 1)]
        };
        json!({ "p50": at(50.0), "p95": at(95.0), "p99": at(99.0) })
    };
    let evidence = json!({
        "schema": "ss-mixed-overlap/v1",
        "source_sha": source_sha(),
        "host": host_name(),
        "command": format!(
            "SS_MIXED_N={n} cargo test -p fireweed --test ss_mixed_overlap --release ss_mixed_overlap_baseline -- --exact --nocapture"
        ),
        "n": n,
        "realistic_response_shape": {
            "payload_bytes": 1_024,
            "field_bytes": 352,
            "metadata": true,
            "gate_keys": true,
            "group_key": true,
            "not_before_and_priority": true,
        },
        "mixed_lifecycle": {
            "original_ids": original_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "completed_original_ids": original_ids
                .iter()
                .filter(|id| seen.contains(id))
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            "every_original_id_completed": original_ids.iter().all(|id| seen.contains(id)),
            "far_future_push_count": n,
            "cross_queue_push_count": n,
            "claim_complete_count": n,
            "ack_wall_s": ack_wall.as_secs_f64(),
            "settled_wall_s": settled_wall.as_secs_f64(),
            "settlement_lag_s": settled_wall.saturating_sub(ack_wall).as_secs_f64(),
            "settled_claim_complete_items_per_s": n as f64 / settled_wall.as_secs_f64().max(1e-9),
            "settled_far_future_push_items_per_s": n as f64 / settled_wall.as_secs_f64().max(1e-9),
            "append_service_ms": append_latency.evidence(),
            "cross_queue_append_wait_ms": cross_queue_append_latency.evidence(),
            "claim_admitted_service_ms": claim_latency.evidence(),
            "complete_admitted_service_ms": complete_latency.evidence(),
            "achieved_fill": numeric_percentiles(&fill),
            "response_bytes": numeric_percentiles(&response_sizes),
            "residual": {
                "main": main_metrics,
                "cross_queue": cross_metrics,
            },
        },
        "waits_and_storage": {
            "append_wait_measurement": "public append invocation-to-ack service",
            "cross_queue_wait_measurement": "concurrent other-queue append invocation-to-ack service",
            "epoch_acquire_ms": epoch_acquire.as_secs_f64() * 1_000.0,
            "emission_cursor_wait_ms": emission_cursor_wait.as_secs_f64() * 1_000.0,
            "emission_metrics": emission,
            "pack_wait": pack_wait_evidence(&compatible),
            "wal_bytes": {
                "before": wal_before,
                "after_mixed_settle": wal_after_mixed_settle,
                "final": wal_final,
            },
        },
        "observation_cohort": observation,
        "fixed_retry_cohorts": [compatible, incompatible_claims, keyed_gate],
        "process_wall_s": process_started.elapsed().as_secs_f64(),
    });

    eprintln!(
        "mixed settled={:.1} items/s fill_p50={} response_p50={}B append_p95={:.1}ms claim_p95={:.1}ms complete_p95={:.1}ms",
        n as f64 / settled_wall.as_secs_f64().max(1e-9),
        numeric_percentiles(&fill)["p50"],
        numeric_percentiles(&response_sizes)["p50"],
        append_latency.percentile_ms(95.0),
        claim_latency.percentile_ms(95.0),
        complete_latency.percentile_ms(95.0),
    );

    if std::env::var("SS_EVIDENCE_WRITE").as_deref() != Ok("0") {
        let utc = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .to_string();
        let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/ss-phased")
            .join(utc);
        std::fs::create_dir_all(&directory).expect("mixed evidence directory");
        let path = directory.join("mixed-summary.json");
        std::fs::write(
            &path,
            serde_json::to_vec_pretty(&evidence).expect("serialize mixed evidence"),
        )
        .expect("write mixed evidence");
        eprintln!("wrote {}", path.display());
    }

    drop(fireweed);
    drop(observation_reader);
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

// ---------------------------------------------------------------------------
// S3s shadow calibration: reconstruct the S3c composition without switching serving.
//
// Remaining calibration not landed in the default lane (too expensive here):
// - N=100k mixed soak versus S0 >=90% settled-rate / <=125% p95/p99 non-regression
// - Full closed-cohort publication budgets 2,021.075 s / 17,246.775 s
// - Pool-cache repartition inside the 224 MiB post-S3c envelope (S3r predicted 352 MiB)
// ---------------------------------------------------------------------------

fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    Pin::new(future).poll(&mut Context::from_waker(Waker::noop()))
}

#[derive(Debug, Default)]
struct ShadowCounters {
    fill: Vec<usize>,
    capacity: BTreeMap<&'static str, usize>,
    deadline: BTreeMap<&'static str, usize>,
    retries: usize,
    first_third_generation_index: Option<usize>,
}

impl ShadowCounters {
    fn capacity(&mut self, resource: &'static str) {
        *self.capacity.entry(resource).or_insert(0) += 1;
    }

    fn deadline(&mut self, resource: &'static str) {
        *self.deadline.entry(resource).or_insert(0) += 1;
    }

    fn evidence(&self) -> Value {
        json!({
            "fill": self.fill,
            "ingress_capacity_rejections": self.capacity,
            "deadline_expiries": self.deadline,
            "retry_count": self.retries,
            "first_third_generation_index": self.first_third_generation_index,
            "fixed_retry_cadence_ms": RETRY_CADENCE.as_millis(),
        })
    }
}

/// Reconstructs inert S3c admissions. Serving still uses the live Fireweed path.
struct ShadowS3cComposition {
    sequencer: MutationSequencer<String, MutationGenerationKind, usize>,
    claim_turns: ClaimQueueTurn<String>,
    claim_coordinator: ClaimCoordinator<String, usize>,
    claim_slots: ClaimDriverReadAdmission,
    shared_slots: SharedDriverReadAdmission,
    outcome_slots: OutcomeReadAdmission,
    keyed_gate: KeyedQueueGate<String>,
}

impl ShadowS3cComposition {
    fn new() -> Self {
        Self {
            sequencer: MutationSequencer::new(),
            claim_turns: ClaimQueueTurn::default(),
            claim_coordinator: ClaimCoordinator::default(),
            claim_slots: ClaimDriverReadAdmission::default(),
            shared_slots: SharedDriverReadAdmission::default(),
            outcome_slots: OutcomeReadAdmission::default(),
            keyed_gate: KeyedQueueGate::new_with_per_key_limit(
                1_024,
                DEFAULT_KEYED_QUEUE_MAX_PER_KEY,
            ),
        }
    }
}

fn request_17_compatible_mutations(counters: &mut ShadowCounters) {
    let sequencer = MutationSequencer::<&'static str, MutationGenerationKind, u8>::new();
    let payloads: Vec<_> = (0..32).map(|index| Arc::new(index as u8)).collect();
    let mut tickets = Vec::new();
    let mut first_rejected = None;
    for (index, payload) in payloads.iter().enumerate() {
        match sequencer.admit(
            "q",
            MutationGenerationKind::Push,
            if index % 2 == 0 {
                MutationIngress::Direct
            } else {
                MutationIngress::KeyedPermitLive
            },
            Arc::clone(payload),
            1,
            1,
        ) {
            Ok(ticket) => {
                assert!(Arc::ptr_eq(ticket.request(), payload));
                tickets.push(ticket);
            }
            Err(CoordinationError::Capacity { resource }) => {
                assert_eq!(resource, MUTATION_SEQUENCER_RESOURCE);
                counters.capacity(resource);
                if first_rejected.is_none() {
                    first_rejected = Some(index + 1);
                }
            }
            Err(error) => panic!("compatible mutation {index} failed: {error:?}"),
        }
    }
    assert_eq!(first_rejected, Some(17));
    assert_eq!(
        sequencer.request_count(&"q"),
        MUTATION_MAX_REQUESTS_PER_QUEUE
    );
    assert_eq!(sequencer.generation_count(&"q"), 2);
    let first = sequencer
        .start_generation(&"q")
        .expect("active compatible generation");
    counters.fill.push(first.requests().len());
    assert_eq!(first.requests().len(), CLAIM_GENERATION_MAX_REQUESTS);
    assert!(
        first
            .requests()
            .iter()
            .zip(&payloads)
            .all(|(retained, original)| Arc::ptr_eq(retained, original))
    );
    assert_eq!(tickets.len(), MUTATION_MAX_REQUESTS_PER_QUEUE);
    drop(first);
    drop(tickets);
    assert_eq!(sequencer.request_count(&"q"), 0);
}

fn four_incompatible_claim_keys(counters: &mut ShadowCounters) {
    let turns = ClaimQueueTurn::<&'static str>::default();
    let coordinator = ClaimCoordinator::<u8, usize>::default();
    let mut callers = Vec::new();
    for key in 0..4u8 {
        callers.push(
            coordinator
                .join(key, Arc::new(key as usize), 1, 8)
                .expect("four Claim keys fit the eight-driver budget"),
        );
    }
    let mut first = turns.acquire("q");
    let active = match poll_once(&mut first) {
        Poll::Ready(Ok(permit)) => permit,
        _ => panic!("first Claim turn: unexpected"),
    };
    let mut second = turns.acquire("q");
    assert!(
        matches!(poll_once(&mut second), Poll::Pending),
        "second incompatible Claim key must queue on the one-queue turn"
    );
    for key in 2..4 {
        let mut extra = turns.acquire("q");
        match poll_once(&mut extra) {
            Poll::Ready(Err(CoordinationError::Capacity { resource })) => {
                assert_eq!(resource, CLAIM_QUEUE_TURN_RESOURCE);
                counters.capacity(resource);
            }
            _ => panic!("Claim key {key} must be turn-capacity, got unexpected"),
        }
    }
    drop(second);
    drop(active);
    drop(callers);
    assert_eq!(turns.queued(), 0);
    assert_eq!(turns.entry_count(), 0);
}

fn request_17_keyed_queue_gate(counters: &mut ShadowCounters) {
    let gate = KeyedQueueGate::new_with_per_key_limit(1_024, DEFAULT_KEYED_QUEUE_MAX_PER_KEY);
    let mut first = gate.acquire("q");
    let active = match poll_once(&mut first) {
        Poll::Ready(Ok(permit)) => permit,
        _ => panic!("first same-key command: unexpected"),
    };
    let mut waiters = Vec::new();
    for _ in 0..15 {
        let mut waiter = gate.acquire("q");
        assert!(
            matches!(poll_once(&mut waiter), Poll::Pending),
            "same-key commands 2-16 must queue"
        );
        waiters.push(waiter);
    }
    let mut first_rejected = None;
    for index in 17..=32 {
        let mut extra = gate.acquire("q");
        match poll_once(&mut extra) {
            Poll::Ready(Err(QueueGateError::PerKeyFull)) => {
                counters.capacity(KEYED_QUEUE_PER_KEY_WAITERS);
                if first_rejected.is_none() {
                    first_rejected = Some(index);
                }
            }
            _ => panic!("same-key command {index}: unexpected"),
        }
    }
    assert_eq!(first_rejected, Some(17));
    assert_eq!(gate.queued(), 15);
    drop(active);
    drop(waiters);
    assert_eq!(gate.queued(), 0);
}

fn claim_queue_9(counters: &mut ShadowCounters) {
    let coordinator = ClaimCoordinator::<u8, usize>::default();
    let turns = ClaimQueueTurn::<u8>::default();
    let slots = ClaimDriverReadAdmission::default();
    let mut callers = Vec::new();
    let mut held_turns = Vec::new();
    for queue in 0..CLAIM_MAX_DRIVERS as u8 {
        callers.push(
            coordinator
                .join(queue, Arc::new(queue as usize), 1, 8)
                .expect("Claim queue within eight-driver budget"),
        );
        let mut turn = turns.acquire(queue);
        held_turns.push(match poll_once(&mut turn) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("Claim queue {queue} turn: unexpected"),
        });
    }
    match coordinator.join(CLAIM_MAX_DRIVERS as u8, Arc::new(9), 1, 8) {
        Err(CoordinationError::Capacity { resource }) => {
            assert_eq!(resource, CLAIM_DRIVER_INGRESS_RESOURCE);
            counters.capacity(resource);
        }
        _ => panic!("Claim queue 9 must miss driver ingress, got unexpected"),
    }
    assert_eq!(turns.entry_count(), CLAIM_MAX_DRIVERS);

    let mut slot_active = Vec::new();
    let mut slot_queued = Vec::new();
    for _ in 0..4 {
        let mut acquire = slots.acquire();
        slot_active.push(match poll_once(&mut acquire) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("Claim slot: unexpected"),
        });
    }
    for _ in 0..4 {
        let mut acquire = slots.acquire();
        assert!(matches!(poll_once(&mut acquire), Poll::Pending));
        slot_queued.push(acquire);
    }
    let mut ninth_slot = slots.acquire();
    match poll_once(&mut ninth_slot) {
        Poll::Ready(Err(CoordinationError::Capacity { resource })) => {
            assert_eq!(resource, CLAIM_DRIVER_SLOTS_RESOURCE);
            counters.capacity(resource);
        }
        _ => panic!("ninth Claim slot: unexpected"),
    }
    drop(slot_queued);
    drop(slot_active);
    drop(held_turns);
    drop(callers);
}

fn shared_generation_queue_25(counters: &mut ShadowCounters) {
    let sequencer = MutationSequencer::<u8, u8, u8>::new();
    let slots = SharedDriverReadAdmission::default();
    let mut tickets = Vec::new();
    let mut active = Vec::new();
    let mut queued = Vec::new();
    for queue in 0..24u8 {
        assert!(
            slots.active() + slots.queued() < 24,
            "shared slot cap reached before queue {}",
            queue + 1
        );
        tickets.push(
            sequencer
                .admit(queue, 1, MutationIngress::Direct, Arc::new(queue), 1, 1)
                .expect("shared generation within 24-queue budget"),
        );
        let mut acquire = slots.acquire();
        match poll_once(&mut acquire) {
            Poll::Ready(Ok(permit)) => active.push(permit),
            Poll::Pending => queued.push(acquire),
            _ => panic!("shared slot queue {}: unexpected", queue + 1),
        }
    }
    assert_eq!(active.len(), 12);
    assert_eq!(queued.len(), 12);
    let mut queue_25 = slots.acquire();
    match poll_once(&mut queue_25) {
        Poll::Ready(Err(CoordinationError::Capacity { resource })) => {
            assert_eq!(resource, SHARED_DRIVER_SLOTS_RESOURCE);
            counters.capacity(resource);
        }
        _ => panic!("shared queue 25: unexpected"),
    }
    assert_eq!(sequencer.request_count(&24), 0);
    drop(queued);
    drop(active);
    drop(tickets);
}

fn realistic_first_third_generation_index(counters: &mut ShadowCounters) {
    let sequencer = MutationSequencer::<&'static str, MutationGenerationKind, u8>::new();
    let items = GENERATION_MAX_ITEMS / 2;
    let response_bytes = items.saturating_mul(1_400);
    let mut tickets = Vec::new();
    for index in 0..32u8 {
        match sequencer.admit(
            "q",
            MutationGenerationKind::Update,
            MutationIngress::Direct,
            Arc::new(index),
            items,
            response_bytes,
        ) {
            Ok(ticket) => tickets.push(ticket),
            Err(CoordinationError::Capacity { resource }) => {
                assert_eq!(resource, MUTATION_SEQUENCER_RESOURCE);
                counters.capacity(resource);
                if counters.first_third_generation_index.is_none() {
                    counters.first_third_generation_index = Some(index as usize + 1);
                }
            }
            Err(error) => panic!("realistic mutation {index}: {error:?}"),
        }
    }
    assert_eq!(
        sequencer.generation_count(&"q"),
        fireweed_engine::MUTATION_MAX_GENERATIONS_PER_QUEUE
    );
    assert!(
        counters
            .first_third_generation_index
            .is_some_and(|index| index != 17),
        "realistic payloads must report the observed third-generation index, not assume request 17; got {:?}",
        counters.first_third_generation_index
    );
    drop(tickets);
}

fn combined_soak_one_below_every_cap(counters: &mut ShadowCounters) {
    let shadow = ShadowS3cComposition::new();
    let mut mutation_tickets = Vec::new();
    for index in 0..15 {
        mutation_tickets.push(
            shadow
                .sequencer
                .admit(
                    "q-mut".to_owned(),
                    MutationGenerationKind::Push,
                    MutationIngress::Direct,
                    Arc::new(index),
                    1,
                    1,
                )
                .expect("combined soak stays below the 16-request sequencer cap"),
        );
    }
    let active_generation = shadow.sequencer.start_generation(&"q-mut".to_owned());
    if let Some(batch) = &active_generation {
        counters.fill.push(batch.requests().len());
    }

    let mut claim_turn = shadow.claim_turns.acquire("q-claim".to_owned());
    let claim_turn = match poll_once(&mut claim_turn) {
        Poll::Ready(Ok(permit)) => permit,
        _ => panic!("combined Claim turn: unexpected"),
    };

    let mut claim_slots = Vec::new();
    let mut claim_queued = Vec::new();
    for _ in 0..4 {
        let mut acquire = shadow.claim_slots.acquire();
        claim_slots.push(match poll_once(&mut acquire) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("combined Claim slot: unexpected"),
        });
    }
    for _ in 0..3 {
        let mut acquire = shadow.claim_slots.acquire();
        assert!(matches!(poll_once(&mut acquire), Poll::Pending));
        claim_queued.push(acquire);
    }

    let mut shared_slots = Vec::new();
    let mut shared_queued = Vec::new();
    for _ in 0..12 {
        let mut acquire = shadow.shared_slots.acquire();
        shared_slots.push(match poll_once(&mut acquire) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("combined shared slot: unexpected"),
        });
    }
    for _ in 0..11 {
        let mut acquire = shadow.shared_slots.acquire();
        assert!(matches!(poll_once(&mut acquire), Poll::Pending));
        shared_queued.push(acquire);
    }

    let mut outcome_slots = Vec::new();
    let mut outcome_queued = Vec::new();
    for _ in 0..8 {
        let mut acquire = shadow.outcome_slots.acquire();
        outcome_slots.push(match poll_once(&mut acquire) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("combined outcome slot: unexpected"),
        });
    }
    for _ in 0..7 {
        let mut acquire = shadow.outcome_slots.acquire();
        assert!(matches!(poll_once(&mut acquire), Poll::Pending));
        outcome_queued.push(acquire);
    }

    let mut gate = shadow.keyed_gate.acquire("q-gate".to_owned());
    let gate_active = match poll_once(&mut gate) {
        Poll::Ready(Ok(permit)) => permit,
        _ => panic!("combined keyed gate: unexpected"),
    };
    let mut gate_waiters = Vec::new();
    for _ in 0..14 {
        let mut waiter = shadow.keyed_gate.acquire("q-gate".to_owned());
        assert!(matches!(poll_once(&mut waiter), Poll::Pending));
        gate_waiters.push(waiter);
    }

    assert!(shadow.claim_coordinator.driver_count() <= CLAIM_MAX_DRIVERS);
    assert_eq!(shadow.sequencer.request_count(&"q-mut".to_owned()), 15);
    assert_eq!(shadow.claim_slots.active() + shadow.claim_slots.queued(), 7);
    assert_eq!(
        shadow.shared_slots.active() + shadow.shared_slots.queued(),
        23
    );
    assert_eq!(
        shadow.outcome_slots.active() + shadow.outcome_slots.queued(),
        15
    );
    assert_eq!(shadow.keyed_gate.queued(), 14);
    drop(gate_waiters);
    drop(gate_active);
    drop(outcome_queued);
    drop(outcome_slots);
    drop(shared_queued);
    drop(shared_slots);
    drop(claim_queued);
    drop(claim_slots);
    drop(claim_turn);
    drop(active_generation);
    drop(mutation_tickets);
}

fn deadline_expiry_distinct_from_capacity(counters: &mut ShadowCounters) {
    let admission = ClaimDriverReadAdmission::new(Duration::ZERO);
    let mut active = Vec::new();
    for _ in 0..4 {
        let mut acquire = admission.acquire();
        active.push(match poll_once(&mut acquire) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("deadline-probe slot: unexpected"),
        });
    }
    let mut queued = admission.acquire();
    match poll_once(&mut queued) {
        Poll::Ready(Err(CoordinationError::Deadline { resource })) => {
            assert_eq!(resource, CLAIM_DRIVER_SLOTS_RESOURCE);
            counters.deadline(resource);
        }
        Poll::Pending => match poll_once(&mut queued) {
            Poll::Ready(Err(CoordinationError::Deadline { resource })) => {
                assert_eq!(resource, CLAIM_DRIVER_SLOTS_RESOURCE);
                counters.deadline(resource);
            }
            _ => panic!("queued slot deadline: unexpected"),
        },
        Poll::Ready(Err(CoordinationError::Capacity { resource })) => {
            panic!("deadline probe must not report ingress capacity {resource}")
        }
        _ => panic!("deadline probe: unexpected"),
    }
    drop(active);

    let expired = abort_unplanned_generation_on_deadline(
        Vec::<fireweed_engine::MutationTicket<&str, u8, u8>>::new(),
        Instant::now() - Duration::from_secs(1),
        Duration::ZERO,
    );
    match expired {
        Err(EngineError::Backpressure { resource }) => {
            assert_eq!(resource, MUTATION_SEQUENCER_WAIT_RESOURCE);
            counters.deadline(resource);
        }
        _ => panic!("sequencer wait expiry: unexpected"),
    }
}

#[test]
fn shadow_compatible_mutation_request_17_is_capacity_rejected() {
    let mut counters = ShadowCounters::default();
    request_17_compatible_mutations(&mut counters);
    assert_eq!(
        counters.capacity.get(MUTATION_SEQUENCER_RESOURCE).copied(),
        Some(16)
    );
    assert!(counters.deadline.is_empty());
}

#[test]
fn shadow_four_incompatible_claim_keys_reject_third_turn() {
    let mut counters = ShadowCounters::default();
    four_incompatible_claim_keys(&mut counters);
    assert_eq!(
        counters.capacity.get(CLAIM_QUEUE_TURN_RESOURCE).copied(),
        Some(2)
    );
}

#[test]
fn shadow_same_keyed_queue_gate_request_17_is_capacity_rejected() {
    let mut counters = ShadowCounters::default();
    request_17_keyed_queue_gate(&mut counters);
    assert_eq!(
        counters.capacity.get(KEYED_QUEUE_PER_KEY_WAITERS).copied(),
        Some(16)
    );
}

#[test]
fn shadow_claim_queue_9_is_capacity_rejected() {
    let mut counters = ShadowCounters::default();
    claim_queue_9(&mut counters);
    assert_eq!(
        counters
            .capacity
            .get(CLAIM_DRIVER_INGRESS_RESOURCE)
            .copied(),
        Some(1)
    );
    assert_eq!(
        counters.capacity.get(CLAIM_DRIVER_SLOTS_RESOURCE).copied(),
        Some(1)
    );
}

#[test]
fn shadow_shared_generation_queue_25_is_capacity_rejected() {
    let mut counters = ShadowCounters::default();
    shared_generation_queue_25(&mut counters);
    assert_eq!(
        counters.capacity.get(SHARED_DRIVER_SLOTS_RESOURCE).copied(),
        Some(1)
    );
}

#[test]
fn shadow_realistic_payloads_report_first_third_generation_index() {
    let mut counters = ShadowCounters::default();
    realistic_first_third_generation_index(&mut counters);
    eprintln!(
        "s3s realistic first_third_generation_index={:?}",
        counters.first_third_generation_index
    );
}

#[test]
fn shadow_combined_soak_stays_one_below_every_cap() {
    let mut counters = ShadowCounters::default();
    combined_soak_one_below_every_cap(&mut counters);
    assert!(counters.capacity.is_empty());
    assert!(counters.deadline.is_empty());
}

#[test]
fn shadow_deadline_expiry_is_distinct_from_ingress_capacity() {
    let mut counters = ShadowCounters::default();
    deadline_expiry_distinct_from_capacity(&mut counters);
    assert!(counters.capacity.is_empty());
    assert_eq!(
        counters.deadline.get(CLAIM_DRIVER_SLOTS_RESOURCE).copied(),
        Some(1)
    );
    assert_eq!(
        counters
            .deadline
            .get(MUTATION_SEQUENCER_WAIT_RESOURCE)
            .copied(),
        Some(1)
    );
}

#[test]
fn shadow_s3s_structural_bounds_match_reviewed_caps() {
    assert_eq!(S3S_WAIT_FLOOR, Duration::from_millis(500));
    assert_eq!(S3S_DERIVED_TURN_WAIT, CLAIM_TURN_DEFAULT_MAX_WAIT);
    assert_eq!(S3S_DERIVED_DRIVER_SLOT_WAIT, DRIVER_SLOT_DEFAULT_MAX_WAIT);
    assert_eq!(S3S_DERIVED_OUTCOME_SLOT_WAIT, OUTCOME_SLOT_DEFAULT_MAX_WAIT);
    assert_eq!(S3S_DERIVED_COVERAGE_OR_WORK_WAIT, S3S_COVERAGE_OR_WORK_CAP);
    assert_eq!(MUTATION_SEQUENCER_DEFAULT_MAX_WAIT, S3S_DERIVED_TURN_WAIT);
    assert_eq!(S3S_FENCE_ACQUIRE_CARRIED_CAP, Duration::from_secs(75));
    assert_eq!(
        derive_structural_wait(
            Duration::ZERO,
            Duration::from_secs(250),
            S3S_WAIT_FLOOR,
            CLAIM_TURN_DEFAULT_MAX_WAIT,
        ),
        Some(S3S_DERIVED_TURN_WAIT)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shadow_outcome_reader_17_is_capacity_rejected() -> EngineResult<()> {
    let mut counters = ShadowCounters::default();
    let root = unique_root();
    std::fs::create_dir_all(&root).expect("s3s outcome root");
    let path = root.join("s3s-outcome.db");
    let store = TursoRelational::open(TursoConfig::local(&path))
        .await
        .map_err(|error| EngineError::Storage(error.to_string()))?;
    let pools = store.committed_pools().expect("file-backed pools").clone();
    assert_eq!(pools.driver_size(), COMMITTED_DRIVER_POOL_SIZE);
    assert_eq!(pools.outcome_size(), COMMITTED_OUTCOME_POOL_SIZE);
    let admission = OutcomeReadAdmission::default();
    let started = Instant::now();

    let mut first_wave = Vec::new();
    for _ in 0..8 {
        let mut acquire = admission.acquire();
        first_wave.push(match poll_once(&mut acquire) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("outcome first wave: unexpected"),
        });
    }
    let mut second_wave = Vec::new();
    for _ in 0..8 {
        let mut acquire = admission.acquire();
        assert!(matches!(poll_once(&mut acquire), Poll::Pending));
        second_wave.push(acquire);
    }
    let mut reader_17 = admission.acquire();
    match poll_once(&mut reader_17) {
        Poll::Ready(Err(CoordinationError::Capacity { resource })) => {
            assert_eq!(resource, OUTCOME_READ_SLOTS_RESOURCE);
            counters.capacity(resource);
        }
        _ => panic!("reader 17: unexpected"),
    }

    let mut first_guards = Vec::new();
    for permit in first_wave {
        first_guards.push((permit, pools.borrow_outcome().await?));
    }
    drop(first_guards);

    let mut second_permits = Vec::new();
    for mut queued in second_wave {
        second_permits.push(match poll_once(&mut queued) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("outcome second wave: unexpected"),
        });
    }
    drop(second_permits);

    let mut completed_17 = false;
    while started.elapsed() < SEVENTEEN_READER_DEADLINE {
        let mut acquire = admission.acquire();
        match poll_once(&mut acquire) {
            Poll::Ready(Ok(permit)) => {
                let guard = pools.borrow_outcome().await?;
                drop(guard);
                drop(permit);
                completed_17 = true;
                break;
            }
            Poll::Ready(Err(CoordinationError::Capacity { resource })) => {
                assert_eq!(resource, OUTCOME_READ_SLOTS_RESOURCE);
                counters.capacity(resource);
                counters.retries += 1;
                tokio::time::sleep(RETRY_CADENCE).await;
            }
            Poll::Pending => {
                let permit = acquire.await.expect("reader 17 queued then admitted");
                let guard = pools.borrow_outcome().await?;
                drop(guard);
                drop(permit);
                completed_17 = true;
                break;
            }
            Poll::Ready(Err(error)) => panic!("reader 17 retry: {error:?}"),
        }
    }
    assert!(completed_17, "reader 17 did not complete");
    assert!(started.elapsed() <= SEVENTEEN_READER_DEADLINE);
    drop(store);
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

/// Ignored/opt-in S3s harness. Reconstructs the S3c composition without switching serving.
///
/// Remaining work documented at the S3s section header: N=100k soak, full publication
/// budgets, and 224 MiB pool-cache repartition.
#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn shadow_mutation_generation_calibration() -> EngineResult<()> {
    let mut counters = ShadowCounters::default();
    request_17_compatible_mutations(&mut counters);
    four_incompatible_claim_keys(&mut counters);
    request_17_keyed_queue_gate(&mut counters);
    claim_queue_9(&mut counters);
    shared_generation_queue_25(&mut counters);
    realistic_first_third_generation_index(&mut counters);
    deadline_expiry_distinct_from_capacity(&mut counters);
    combined_soak_one_below_every_cap(&mut counters);

    let root = unique_root();
    let fireweed = Arc::new(open_mixed_product(&root)?);
    let due = now();
    let compatible =
        compatible_mutation_cohort(Arc::clone(&fireweed), queue_key("q-s3s-compatible"), due)
            .await?;
    let incompatible =
        incompatible_claim_cohort(Arc::clone(&fireweed), queue_key("q-s3s-claim-keys"), due)
            .await?;
    let keyed =
        same_keyed_gate_cohort(Arc::clone(&fireweed), queue_key("q-s3s-keyed"), due).await?;

    let reclaim_queue = queue_key("q-s3s-reclaim");
    fireweed
        .create_queue(qdef(reclaim_queue.queue_id.as_str()))
        .await?;
    let reclaim_ids = fireweed
        .push_batch(
            &reclaim_queue,
            vec![
                realistic_item("s3s-reclaim", 0, due),
                realistic_item("s3s-reclaim", 1, due),
            ],
        )
        .await?;
    settle(fireweed.as_ref(), &reclaim_queue).await?;
    let claimed = fireweed.claim(&reclaim_queue, 2, 1).await?;
    assert_eq!(claimed.len(), 2);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let reclaim_now = now();
    let reclaimed = retry_25ms("s3s-reclaim-gate".to_owned(), || {
        fireweed.reclaim_expired_at(&reclaim_queue, Some(8), reclaim_now)
    })
    .await?;
    assert_eq!(reclaimed.0.len(), 2);
    counters.retries += reclaimed.1.retries;
    let after_reclaim = settle(fireweed.as_ref(), &reclaim_queue).await?;
    assert_eq!(after_reclaim.pending, 2);
    assert_eq!(after_reclaim.leased, 0);

    let before_reject = settle(fireweed.as_ref(), &reclaim_queue).await?;
    let sequencer = MutationSequencer::<&'static str, MutationGenerationKind, u8>::new();
    let mut held = Vec::new();
    for index in 0..MUTATION_MAX_REQUESTS_PER_QUEUE {
        held.push(
            sequencer
                .admit(
                    "shadow",
                    MutationGenerationKind::Push,
                    MutationIngress::Direct,
                    Arc::new(index as u8),
                    1,
                    1,
                )
                .expect("held generation"),
        );
    }
    assert!(matches!(
        sequencer.admit(
            "shadow",
            MutationGenerationKind::Push,
            MutationIngress::Direct,
            Arc::new(99),
            1,
            1,
        ),
        Err(CoordinationError::Capacity {
            resource: MUTATION_SEQUENCER_RESOURCE,
        })
    ));
    let after_reject = settle(fireweed.as_ref(), &reclaim_queue).await?;
    assert_eq!(after_reject.pending, before_reject.pending);
    assert_eq!(after_reject.leased, before_reject.leased);
    assert_eq!(after_reject.complete, before_reject.complete);
    drop(held);

    let evidence = json!({
        "schema": "ss-s3s-shadow-calibration/v1",
        "serving_switched": false,
        "predicted_interim_ceiling_mib": 352,
        "committed_driver_pool_size": COMMITTED_DRIVER_POOL_SIZE,
        "committed_outcome_pool_size": COMMITTED_OUTCOME_POOL_SIZE,
        "derived_bounds": {
            "floor_ms": S3S_WAIT_FLOOR.as_millis(),
            "turn_s": S3S_DERIVED_TURN_WAIT.as_secs(),
            "driver_slot_s": S3S_DERIVED_DRIVER_SLOT_WAIT.as_secs(),
            "outcome_slot_s": S3S_DERIVED_OUTCOME_SLOT_WAIT.as_secs(),
            "coverage_or_work_s": S3S_DERIVED_COVERAGE_OR_WORK_WAIT.as_secs(),
            "fence_acquire_carried_s": S3S_FENCE_ACQUIRE_CARRIED_CAP.as_secs(),
        },
        "shadow": counters.evidence(),
        "public_cohorts": [compatible, incompatible, keyed],
        "reclaim_original_ids": reclaim_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "rejected_pre_position_had_no_durable_effect": true,
    });
    eprintln!(
        "{}",
        serde_json::to_string_pretty(&evidence).expect("s3s evidence")
    );

    drop(fireweed);
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

// ---------------------------------------------------------------------------
// S3m Claim-turn/slot and exact fence-bound calibration.
//
// Production selection-fence dispositions stay inert through S3c. Isolated
// non-serving shadow queues take the real SelectionFence so acquire/starvation
// can be timed without activating S5.
//
// Remaining calibration not landed in the default lane (too expensive here):
// - N=100k exact-high-water drain soak versus T2
//   `mean_claim_cycle_ms <= 1000 × achieved_items_per_claim_vector / 4000`
//   (200 ms at fill 800). The harness exists as
//   `shadow_claim_drain_calibration_uses_exact_high_water`.
// - Full closed-cohort publication budgets 2,021.075 s / 4,040 s / 4,075 s
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct S3mTimings {
    claim_turn: Latency,
    pre_fence_coverage: Latency,
    claim_slot: Latency,
    driver_pool: Latency,
    fence_acquire: Latency,
    fence_drain: Latency,
    delta_coverage: Latency,
    select_reserve_encode: Latency,
    claim_cycle: Latency,
    fills: Vec<usize>,
    shared_fence_starvation: usize,
    driver_pool_expiries: usize,
    reservation_split_rounds: usize,
}

impl S3mTimings {
    fn mean_fill(&self) -> f64 {
        if self.fills.is_empty() {
            return 0.0;
        }
        self.fills.iter().sum::<usize>() as f64 / self.fills.len() as f64
    }

    fn mean_claim_cycle_ms(&self) -> f64 {
        if self.claim_cycle.samples.is_empty() {
            return 0.0;
        }
        self.claim_cycle
            .samples
            .iter()
            .copied()
            .sum::<Duration>()
            .as_secs_f64()
            * 1_000.0
            / self.claim_cycle.samples.len() as f64
    }

    fn t2_budget_ms(&self) -> f64 {
        1_000.0 * self.mean_fill() / T2_ITEMS_PER_SECOND
    }

    fn t2_diagnostic(&self) -> Value {
        let mean_cycle = self.mean_claim_cycle_ms();
        let fill = self.mean_fill();
        let budget = self.t2_budget_ms();
        json!({
            "harness": "mean_claim_cycle_ms <= 1000 × achieved_items_per_claim_vector / 4000",
            "mean_claim_cycle_ms": mean_cycle,
            "achieved_items_per_claim_vector": fill,
            "t2_budget_ms": budget,
            "t2_budget_at_fill_800_ms": 1_000.0 * 800.0 / T2_ITEMS_PER_SECOND,
            "t2_diagnostic_holds": fill > 0.0 && mean_cycle <= budget,
            "samples": self.claim_cycle.samples.len(),
            "note": "S3m records the diagnostic; a short T2 does not fail this slice. S5 re-derives on the activated fence path.",
        })
    }

    fn evidence(&self) -> Value {
        json!({
            "claim_turn_ms": self.claim_turn.evidence(),
            "pre_fence_coverage_ms": self.pre_fence_coverage.evidence(),
            "claim_slot_ms": self.claim_slot.evidence(),
            "driver_pool_ms": self.driver_pool.evidence(),
            "fence_acquire_ms": self.fence_acquire.evidence(),
            "fence_drain_ms": self.fence_drain.evidence(),
            "delta_coverage_ms": self.delta_coverage.evidence(),
            "select_reserve_encode_ms": self.select_reserve_encode.evidence(),
            "claim_publication_plus_apply_cycle_ms": self.claim_cycle.evidence(),
            "achieved_fill": self.fills,
            "achieved_concurrency": {
                "claim_slot_active_plus_queued_cap": 8,
                "claim_turn_per_queue_cap": 2,
                "claim_driver_ingress_cap": CLAIM_MAX_DRIVERS,
            },
            "shared_fence_starvation": self.shared_fence_starvation,
            "driver_pool_expiries": self.driver_pool_expiries,
            "reservation_split_rounds": self.reservation_split_rounds,
            "t2": self.t2_diagnostic(),
        })
    }
}

struct ShadowS3mComposition {
    claim_turns: ClaimQueueTurn<String>,
    claim_coordinator: ClaimCoordinator<String, usize>,
    claim_slots: ClaimDriverReadAdmission,
    fence: SelectionFence<String>,
    fence_admission: SelectionFenceAdmission,
    shared_slots: SharedDriverReadAdmission,
    outcome_slots: OutcomeReadAdmission,
    sequencer: MutationSequencer<String, MutationGenerationKind, usize>,
}

impl ShadowS3mComposition {
    fn new() -> Self {
        Self {
            claim_turns: ClaimQueueTurn::default(),
            claim_coordinator: ClaimCoordinator::default(),
            claim_slots: ClaimDriverReadAdmission::default(),
            fence: SelectionFence::default(),
            fence_admission: SelectionFenceAdmission::new(1_024),
            shared_slots: SharedDriverReadAdmission::default(),
            outcome_slots: OutcomeReadAdmission::default(),
            sequencer: MutationSequencer::new(),
        }
    }
}

fn s3m_derived_bounds_evidence() -> Value {
    json!({
        "floor_ms": S3M_WAIT_FLOOR.as_millis(),
        "claim_turn_s": S3M_DERIVED_TURN_WAIT.as_secs(),
        "claim_slot_s": S3M_DERIVED_CLAIM_SLOT_WAIT.as_secs(),
        "fence_acquire_s": S3M_DERIVED_FENCE_ACQUIRE_WAIT.as_secs(),
        "coverage_or_work_s": S3M_DERIVED_COVERAGE_OR_WORK_WAIT.as_secs(),
        "driver_pool_borrow_cap_ms": S3M_DRIVER_POOL_BORROW_CAP.as_millis(),
        "production_fence_activated": false,
        "derived": {
            "claim_turn": derive_structural_wait(
                Duration::ZERO,
                Duration::from_secs(250),
                S3M_WAIT_FLOOR,
                CLAIM_TURN_DEFAULT_MAX_WAIT,
            )
            .map(|wait| wait.as_secs()),
            "claim_slot": derive_structural_wait(
                Duration::ZERO,
                Duration::from_secs(90),
                S3M_WAIT_FLOOR,
                DRIVER_SLOT_DEFAULT_MAX_WAIT,
            )
            .map(|wait| wait.as_secs()),
            "fence_acquire": derive_structural_wait(
                Duration::ZERO,
                Duration::from_secs(70),
                S3M_WAIT_FLOOR,
                S3S_FENCE_ACQUIRE_CARRIED_CAP,
            )
            .map(|wait| wait.as_secs()),
            "coverage_or_work": derive_structural_wait(
                Duration::ZERO,
                Duration::ZERO,
                S3M_WAIT_FLOOR,
                S3S_COVERAGE_OR_WORK_CAP,
            )
            .map(|wait| wait.as_secs()),
        },
    })
}

fn shadow_exclusive_fence_on_isolated_queue(timings: &mut S3mTimings) {
    let fence = SelectionFence::<&'static str>::default();
    let admission = SelectionFenceAdmission::new(1_024);
    let waiter = admission
        .admit_waiter()
        .expect("isolated shadow fence waiter");
    let mut shared = fence.acquire_shared("q-s3m-shadow");
    let shared = match poll_once(&mut shared) {
        Poll::Ready(Ok(permit)) => permit,
        _ => panic!("shared fence: unexpected"),
    };
    let started = Instant::now();
    let mut exclusive = fence.acquire_exclusive("q-s3m-shadow");
    assert!(
        matches!(poll_once(&mut exclusive), Poll::Pending),
        "exclusive Claim fence must wait for shared holders"
    );
    let mut later_shared = fence.acquire_shared("q-s3m-shadow");
    assert!(
        matches!(poll_once(&mut later_shared), Poll::Pending),
        "later shared holder must not pass a queued exclusive waiter"
    );
    timings.shared_fence_starvation += 1;
    drop(shared);
    let exclusive = match poll_once(&mut exclusive) {
        Poll::Ready(Ok(permit)) => permit,
        _ => panic!("exclusive fence: unexpected"),
    };
    timings.fence_acquire.record(started.elapsed());
    assert!(matches!(poll_once(&mut later_shared), Poll::Pending));
    drop(exclusive);
    let later_shared = match poll_once(&mut later_shared) {
        Poll::Ready(Ok(permit)) => permit,
        _ => panic!("later shared fence: unexpected"),
    };
    drop(later_shared);
    drop(waiter);
    assert_eq!(fence.entry_count(), 0);
    assert_eq!(admission.waiter_count(), 0);
}

fn s3m_combined_soak_one_below_every_cap(counters: &mut ShadowCounters, timings: &mut S3mTimings) {
    let shadow = ShadowS3mComposition::new();
    let mut mutation_tickets = Vec::new();
    for index in 0..15 {
        mutation_tickets.push(
            shadow
                .sequencer
                .admit(
                    "q-mut".to_owned(),
                    MutationGenerationKind::Push,
                    MutationIngress::Direct,
                    Arc::new(index),
                    1,
                    1,
                )
                .expect("combined soak stays below the 16-request sequencer cap"),
        );
    }

    let turn_started = Instant::now();
    let mut claim_turn = shadow.claim_turns.acquire("q-claim".to_owned());
    let claim_turn = match poll_once(&mut claim_turn) {
        Poll::Ready(Ok(permit)) => permit,
        _ => panic!("combined Claim turn: unexpected"),
    };
    timings.claim_turn.record(turn_started.elapsed());

    let mut claim_slots = Vec::new();
    let mut claim_queued = Vec::new();
    for _ in 0..4 {
        let slot_started = Instant::now();
        let mut acquire = shadow.claim_slots.acquire();
        claim_slots.push(match poll_once(&mut acquire) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("combined Claim slot: unexpected"),
        });
        timings.claim_slot.record(slot_started.elapsed());
    }
    for _ in 0..3 {
        let mut acquire = shadow.claim_slots.acquire();
        assert!(matches!(poll_once(&mut acquire), Poll::Pending));
        claim_queued.push(acquire);
    }

    let waiter = shadow
        .fence_admission
        .admit_waiter()
        .expect("combined soak fence waiter");
    let mut shared = shadow.fence.acquire_shared("q-claim".to_owned());
    let shared = match poll_once(&mut shared) {
        Poll::Ready(Ok(permit)) => permit,
        _ => panic!("combined shared fence: unexpected"),
    };
    let fence_started = Instant::now();
    let mut exclusive = shadow.fence.acquire_exclusive("q-claim".to_owned());
    assert!(matches!(poll_once(&mut exclusive), Poll::Pending));
    drop(shared);
    let exclusive = match poll_once(&mut exclusive) {
        Poll::Ready(Ok(permit)) => permit,
        _ => panic!("combined exclusive fence: unexpected"),
    };
    timings.fence_acquire.record(fence_started.elapsed());

    let mut shared_slots = Vec::new();
    let mut shared_queued = Vec::new();
    for _ in 0..12 {
        let mut acquire = shadow.shared_slots.acquire();
        shared_slots.push(match poll_once(&mut acquire) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("combined shared slot: unexpected"),
        });
    }
    for _ in 0..11 {
        let mut acquire = shadow.shared_slots.acquire();
        assert!(matches!(poll_once(&mut acquire), Poll::Pending));
        shared_queued.push(acquire);
    }

    let mut outcome_slots = Vec::new();
    let mut outcome_queued = Vec::new();
    for _ in 0..8 {
        let mut acquire = shadow.outcome_slots.acquire();
        outcome_slots.push(match poll_once(&mut acquire) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("combined outcome slot: unexpected"),
        });
    }
    for _ in 0..7 {
        let mut acquire = shadow.outcome_slots.acquire();
        assert!(matches!(poll_once(&mut acquire), Poll::Pending));
        outcome_queued.push(acquire);
    }

    assert!(shadow.claim_coordinator.driver_count() <= CLAIM_MAX_DRIVERS);
    assert_eq!(shadow.sequencer.request_count(&"q-mut".to_owned()), 15);
    assert_eq!(shadow.claim_slots.active() + shadow.claim_slots.queued(), 7);
    assert_eq!(
        shadow.shared_slots.active() + shadow.shared_slots.queued(),
        23
    );
    assert_eq!(
        shadow.outcome_slots.active() + shadow.outcome_slots.queued(),
        15
    );
    drop(exclusive);
    drop(waiter);
    drop(outcome_queued);
    drop(outcome_slots);
    drop(shared_queued);
    drop(shared_slots);
    drop(claim_queued);
    drop(claim_slots);
    drop(claim_turn);
    drop(mutation_tickets);
    assert!(counters.capacity.is_empty());
}

async fn measure_driver_borrow_after_slot(
    pools: &fireweed_turso::CommittedReaderPools,
    timings: &mut S3mTimings,
) -> EngineResult<()> {
    let slots = ClaimDriverReadAdmission::default();
    let mut acquire = slots.acquire();
    let permit = match poll_once(&mut acquire) {
        Poll::Ready(Ok(permit)) => permit,
        _ => panic!("driver-borrow slot: unexpected"),
    };
    let started = Instant::now();
    match pools.borrow_driver().await {
        Ok(guard) => {
            timings.driver_pool.record(started.elapsed());
            drop(guard);
        }
        Err(EngineError::Backpressure { resource }) => {
            timings.driver_pool_expiries += 1;
            panic!("driver borrow after slot expired: {resource}");
        }
        Err(error) => return Err(error),
    }
    drop(permit);
    let p99 = timings.driver_pool.percentile_ms(99.0);
    assert!(
        p99 <= S3M_DRIVER_POOL_BORROW_CAP.as_secs_f64() * 1_000.0,
        "driver borrow after slot p99 {p99} ms exceeds {:?}",
        S3M_DRIVER_POOL_BORROW_CAP
    );
    assert_eq!(timings.driver_pool_expiries, 0);
    Ok(())
}

async fn claim_publication_plus_apply_cycle(
    fireweed: &MixedRuntime,
    queue: &QueueKey,
    max_items: usize,
    timings: &mut S3mTimings,
) -> EngineResult<Vec<ItemId>> {
    let coverage_started = Instant::now();
    settle(fireweed, queue).await?;
    timings
        .pre_fence_coverage
        .record(coverage_started.elapsed());

    let started = Instant::now();
    let items = fireweed.claim(queue, max_items, 30_000).await?;
    timings.select_reserve_encode.record(started.elapsed());
    timings.fills.push(items.len());
    if items.is_empty() {
        timings.claim_cycle.record(started.elapsed());
        return Ok(Vec::new());
    }
    let ids: Vec<_> = items.iter().map(|item| item.item_id).collect();
    fireweed.complete(queue, ids.clone()).await?;
    let drain_started = Instant::now();
    settle(fireweed, queue).await?;
    let drain = drain_started.elapsed();
    timings.fence_drain.record(drain);
    timings.delta_coverage.record(drain);
    timings.claim_cycle.record(started.elapsed());
    if items.len() >= GENERATION_MAX_ITEMS {
        timings.reservation_split_rounds = timings.reservation_split_rounds.max(1);
    }
    Ok(ids)
}

async fn live_nine_pending_claim_queues(
    fireweed: Arc<MixedRuntime>,
    due: UtcTimestamp,
    counters: &mut ShadowCounters,
) -> EngineResult<Value> {
    let queues: Vec<_> = (0..9)
        .map(|index| queue_key(&format!("q-s3m-pending-{index}")))
        .collect();
    for (index, queue) in queues.iter().enumerate() {
        fireweed.create_queue(qdef(queue.queue_id.as_str())).await?;
        let mut item = realistic_item("s3m-pending", index, due);
        item.group_key = Some(GroupKey::new(format!("s3m-pending-{index}")).unwrap());
        fireweed.push_batch(queue, vec![item]).await?;
        settle(fireweed.as_ref(), queue).await?;
    }

    let barrier = Arc::new(tokio::sync::Barrier::new(9));
    let started = Instant::now();
    let futures = queues.iter().enumerate().map(|(index, queue)| {
        let fireweed = Arc::clone(&fireweed);
        let queue = queue.clone();
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            let compatibility = ClaimCompatibility {
                group_key: Some(GroupKey::new(format!("s3m-pending-{index}")).unwrap()),
                ..Default::default()
            };
            match fireweed.claim_with(&queue, 1, 30_000, compatibility).await {
                Ok(items) => EngineResult::Ok((index, items, None)),
                Err(EngineError::Backpressure { resource }) => {
                    Ok((index, Vec::new(), Some(resource)))
                }
                Err(error) => Err(error),
            }
        }
    });
    let mut admitted = Vec::new();
    let mut rejected = Vec::new();
    for result in futures::future::join_all(futures).await {
        let (index, items, resource) = result?;
        if let Some(resource) = resource {
            counters.capacity(resource);
            rejected.push(index);
        } else {
            assert_eq!(
                items.len(),
                1,
                "admitted Claim queue {index} must consume Pending"
            );
            admitted.push((index, items[0].item_id));
        }
    }
    assert_eq!(
        admitted.len() + rejected.len(),
        9,
        "nine Pending-consuming Claim queues must all report"
    );
    // A scheduling race may release a driver before the ninth admission.
    // Deterministic saturation is covered by claim_queue_9; the live contract
    // is bounded progress with no lost or duplicate original rows.
    for (index, item_id) in &admitted {
        fireweed.complete(&queues[*index], [*item_id]).await?;
    }
    let mut retry_count = 0;
    for &rejected_index in &rejected {
        let (retry_items, timing) =
            retry_25ms(format!("s3m-pending-retry-{rejected_index}"), || {
                let compatibility = ClaimCompatibility {
                    group_key: Some(
                        GroupKey::new(format!("s3m-pending-{rejected_index}")).unwrap(),
                    ),
                    ..Default::default()
                };
                fireweed.claim_with(&queues[rejected_index], 1, 30_000, compatibility)
            })
            .await?;
        assert_eq!(retry_items.len(), 1);
        counters.retries += timing.retries;
        retry_count += timing.retries;
        fireweed
            .complete(&queues[rejected_index], [retry_items[0].item_id])
            .await?;
    }
    for queue in &queues {
        let metrics = settle(fireweed.as_ref(), queue).await?;
        assert_eq!(metrics.complete, 1);
        assert_eq!(metrics.pending, 0);
        assert_eq!(metrics.leased, 0);
    }
    Ok(json!({
        "name": "nine_pending_consuming_claim_queues",
        "admitted_first_wave": admitted.len(),
        "capacity_rejected_first_wave": rejected.len(),
        "rejected_indexes": rejected,
        "retry_count": retry_count,
        "settled_wall_s": started.elapsed().as_secs_f64(),
        "every_original_item_consumed": true,
    }))
}

async fn live_four_incompatible_pending_claim_keys(
    fireweed: Arc<MixedRuntime>,
    due: UtcTimestamp,
    counters: &mut ShadowCounters,
) -> EngineResult<Value> {
    let queue = queue_key("q-s3m-incompatible-pending");
    fireweed.create_queue(qdef(queue.queue_id.as_str())).await?;
    let mut items = Vec::new();
    for group in 0..4 {
        let mut item = realistic_item("s3m-claim-key", group, due);
        item.group_key = Some(GroupKey::new(format!("s3m-claim-group-{group}")).unwrap());
        items.push(item);
    }
    let original_ids = push_in_batches(fireweed.as_ref(), &queue, items).await?;
    settle(fireweed.as_ref(), &queue).await?;

    let barrier = Arc::new(tokio::sync::Barrier::new(4));
    let started = Instant::now();
    let futures = (0..4).map(|group| {
        let fireweed = Arc::clone(&fireweed);
        let queue = queue.clone();
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            let compatibility = ClaimCompatibility {
                group_key: Some(GroupKey::new(format!("s3m-claim-group-{group}")).unwrap()),
                ..Default::default()
            };
            match fireweed.claim_with(&queue, 1, 30_000, compatibility).await {
                Ok(items) => EngineResult::Ok((group, items, None)),
                Err(EngineError::Backpressure { resource }) => {
                    Ok((group, Vec::new(), Some(resource)))
                }
                Err(error) => Err(error),
            }
        }
    });
    let mut admitted = Vec::new();
    let mut rejected_groups = Vec::new();
    for result in futures::future::join_all(futures).await {
        let (group, items, resource) = result?;
        if let Some(resource) = resource {
            counters.capacity(resource);
            rejected_groups.push(group);
        } else {
            assert_eq!(items.len(), 1);
            admitted.push(items[0].item_id);
        }
    }
    assert_eq!(
        counters.capacity.get(CLAIM_QUEUE_TURN_RESOURCE).copied(),
        Some(2),
        "incompatible keys 3 and 4 must miss the one-queue turn; admitted={} rejected={rejected_groups:?}",
        admitted.len()
    );
    assert_eq!(admitted.len(), 2);
    fireweed.complete(&queue, admitted.clone()).await?;

    for group in &rejected_groups {
        let (items, timing) = retry_25ms(format!("s3m-incompatible-retry-{group}"), || {
            let compatibility = ClaimCompatibility {
                group_key: Some(GroupKey::new(format!("s3m-claim-group-{group}")).unwrap()),
                ..Default::default()
            };
            fireweed.claim_with(&queue, 1, 30_000, compatibility)
        })
        .await?;
        assert_eq!(items.len(), 1);
        counters.retries += timing.retries;
        fireweed.complete(&queue, [items[0].item_id]).await?;
    }
    let metrics = settle(fireweed.as_ref(), &queue).await?;
    assert_eq!(metrics.complete, 4);
    assert_eq!(metrics.pending, 0);
    assert_eq!(metrics.leased, 0);
    Ok(json!({
        "name": "four_incompatible_same_queue_claim_keys",
        "admitted_first_wave": admitted.len(),
        "capacity_rejected_first_wave": rejected_groups.len(),
        "rejected_groups": rejected_groups,
        "original_item_ids": original_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "every_original_item_consumed": true,
        "settled_wall_s": started.elapsed().as_secs_f64(),
    }))
}

async fn s3m_live_below_cap_apply_traffic(
    fireweed: Arc<MixedRuntime>,
    due: UtcTimestamp,
    timings: &mut S3mTimings,
) -> EngineResult<Value> {
    // Smaller than the S0 mixed cohorts: 32 concurrent BatchUpdates plus 16
    // observation samples do not finish in a few minutes in this debug lane.
    // Live mutations stay at 8 (below the 16-request sequencer cap); observations
    // are a handful of committed peeks overlapping apply.
    let queue = queue_key("q-s3m-soak");
    fireweed.create_queue(qdef(queue.queue_id.as_str())).await?;
    let items: Vec<_> = (0..S3M_SOAK_N)
        .map(|ordinal| realistic_item("s3m-soak", ordinal, due))
        .collect();
    let original_ids = push_in_batches(fireweed.as_ref(), &queue, items).await?;
    settle(fireweed.as_ref(), &queue).await?;

    let mutation_queue = queue_key("q-s3m-soak-mutations");
    fireweed
        .create_queue(qdef(mutation_queue.queue_id.as_str()))
        .await?;
    let mutation_keys: Vec<_> = (0..8)
        .map(|index| ClientItemKey::new(format!("s3m-mut-{index:03}")).unwrap())
        .collect();
    let mutation_items = mutation_keys
        .iter()
        .enumerate()
        .map(|(index, key)| NewItem {
            client_item_key: Some(key.clone()),
            not_before: Some(due),
            priority: Some(PriorityValue::Timestamp(due)),
            group_key: Some(GroupKey::new(format!("s3m-mut-group-{index}")).unwrap()),
            payload: Some(Bytes::from(vec![b'm'; 64])),
            metadata: metadata("s3m-mut", index),
            ..Default::default()
        })
        .collect();
    push_in_batches(fireweed.as_ref(), &mutation_queue, mutation_items).await?;
    settle(fireweed.as_ref(), &mutation_queue).await?;
    let mutation_started = Instant::now();
    let mut mutation_timings = Vec::new();
    for (index, key) in mutation_keys.iter().enumerate() {
        let evidence_id = format!("s3m-soak-mutation-{index:02}");
        let (response, timing) = retry_25ms(evidence_id.clone(), || {
            fireweed.batch_update(
                &mutation_queue,
                BatchUpdateRequest {
                    request_id: RequestId::new(evidence_id.clone()).unwrap(),
                    updates: vec![BatchUpdateEntry {
                        item_ref: BatchUpdateItemRef::ClientItemKey(key.clone()),
                        expected_item_version: None,
                        priority: BatchUpdateValue::Keep,
                        not_before: BatchUpdateValue::Keep,
                        payload: BatchUpdateValue::Keep,
                        metadata: BatchUpdateValue::Replace(metadata("s3m-mut-updated", index)),
                        gate_keys: BatchUpdateValue::Keep,
                        fields: BatchUpdateValue::Keep,
                    }],
                },
            )
        })
        .await?;
        assert!(matches!(
            response.results.first(),
            Some(BatchUpdateOutcome::Updated { .. })
        ));
        mutation_timings.push(timing);
    }
    let mutation_metrics = settle(fireweed.as_ref(), &mutation_queue).await?;
    assert_eq!(mutation_metrics.pending, 8);

    let peek_started = Instant::now();
    let peeked = fireweed.peek(&queue, 8).await?;
    let peek_ms = peek_started.elapsed().as_secs_f64() * 1_000.0;
    assert!(!peeked.is_empty());

    let mut seen = HashSet::new();
    while seen.len() < S3M_SOAK_N {
        let ids =
            claim_publication_plus_apply_cycle(fireweed.as_ref(), &queue, CLAIM_BATCH, timings)
                .await?;
        if ids.is_empty() {
            tokio::time::sleep(RETRY_CADENCE).await;
            continue;
        }
        seen.extend(ids);
    }
    let metrics = settle(fireweed.as_ref(), &queue).await?;
    assert_eq!(seen.len(), original_ids.len());
    assert!(original_ids.iter().all(|id| seen.contains(id)));
    assert_eq!(metrics.complete, S3M_SOAK_N as u64);
    assert_eq!(metrics.leased, 0);
    Ok(json!({
        "soak_n": S3M_SOAK_N,
        "completed_original_ids": seen.len(),
        "live_mutations": cohort_evidence(
            "eight_compatible_mutations_below_sequencer_cap",
            &mutation_timings,
            8,
            mutation_started.elapsed(),
        ),
        "oversubscribed_observations": {
            "reconstructed_outcome_slots_held": 15,
            "live_server_peek": peeked.len(),
            "live_server_peek_ms": peek_ms,
        },
        "every_original_item_consumed": true,
        "note": "S0 32-mutation / 16-sample observation cohorts remain on the ignored N=100k harness; this unignored soak stays small.",
    }))
}

#[test]
fn shadow_s3m_structural_bounds_match_reviewed_caps() {
    assert_eq!(S3M_WAIT_FLOOR, Duration::from_millis(500));
    assert_eq!(S3M_DERIVED_TURN_WAIT, CLAIM_TURN_DEFAULT_MAX_WAIT);
    assert_eq!(S3M_DERIVED_CLAIM_SLOT_WAIT, DRIVER_SLOT_DEFAULT_MAX_WAIT);
    assert_eq!(
        S3M_DERIVED_FENCE_ACQUIRE_WAIT,
        S3S_FENCE_ACQUIRE_CARRIED_CAP
    );
    assert_eq!(S3M_DERIVED_COVERAGE_OR_WORK_WAIT, S3S_COVERAGE_OR_WORK_CAP);
    assert_eq!(S3M_DRIVER_POOL_BORROW_CAP, Duration::from_millis(100));
    assert_eq!(
        derive_structural_wait(
            Duration::ZERO,
            Duration::from_secs(250),
            S3M_WAIT_FLOOR,
            CLAIM_TURN_DEFAULT_MAX_WAIT,
        ),
        Some(S3M_DERIVED_TURN_WAIT)
    );
    assert_eq!(
        derive_structural_wait(
            Duration::ZERO,
            Duration::from_secs(90),
            S3M_WAIT_FLOOR,
            DRIVER_SLOT_DEFAULT_MAX_WAIT,
        ),
        Some(S3M_DERIVED_CLAIM_SLOT_WAIT)
    );
    assert_eq!(
        derive_structural_wait(
            Duration::ZERO,
            Duration::from_secs(70),
            S3M_WAIT_FLOOR,
            S3S_FENCE_ACQUIRE_CARRIED_CAP,
        ),
        Some(S3M_DERIVED_FENCE_ACQUIRE_WAIT)
    );
    assert_eq!(
        derive_structural_wait(
            Duration::ZERO,
            Duration::ZERO,
            S3M_WAIT_FLOOR,
            S3S_COVERAGE_OR_WORK_CAP,
        ),
        Some(S3M_DERIVED_COVERAGE_OR_WORK_WAIT)
    );
}

#[test]
fn shadow_claim_nine_pending_queues_reconstructed_cliff() {
    let mut counters = ShadowCounters::default();
    claim_queue_9(&mut counters);
    assert_eq!(
        counters
            .capacity
            .get(CLAIM_DRIVER_INGRESS_RESOURCE)
            .copied(),
        Some(1)
    );
    assert_eq!(
        counters.capacity.get(CLAIM_DRIVER_SLOTS_RESOURCE).copied(),
        Some(1)
    );
}

#[test]
fn shadow_claim_four_incompatible_pending_keys_reconstructed_cliff() {
    let mut counters = ShadowCounters::default();
    four_incompatible_claim_keys(&mut counters);
    assert_eq!(
        counters.capacity.get(CLAIM_QUEUE_TURN_RESOURCE).copied(),
        Some(2)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shadow_claim_driver_borrow_after_slot_stays_within_100ms() -> EngineResult<()> {
    let mut timings = S3mTimings::default();
    let root = unique_root();
    std::fs::create_dir_all(&root).expect("s3m driver root");
    let path = root.join("s3m-driver.db");
    let store = TursoRelational::open(TursoConfig::local(&path))
        .await
        .map_err(|error| EngineError::Storage(error.to_string()))?;
    let pools = store.committed_pools().expect("file-backed pools").clone();
    assert_eq!(pools.driver_size(), COMMITTED_DRIVER_POOL_SIZE);
    measure_driver_borrow_after_slot(&pools, &mut timings).await?;
    eprintln!(
        "s3m driver_borrow_after_slot_p99_ms={:.3} expiries={}",
        timings.driver_pool.percentile_ms(99.0),
        timings.driver_pool_expiries
    );
    drop(store);
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn claim_nine_pending_queues_eventually_complete() -> EngineResult<()> {
    let mut counters = ShadowCounters::default();
    claim_queue_9(&mut counters);
    assert_eq!(
        counters.capacity.get(CLAIM_DRIVER_SLOTS_RESOURCE).copied(),
        Some(1)
    );
    counters = ShadowCounters::default();

    let root = unique_root();
    let fireweed = Arc::new(open_mixed_product(&root)?);
    let live = live_nine_pending_claim_queues(Arc::clone(&fireweed), now(), &mut counters).await?;
    eprintln!(
        "{}",
        serde_json::to_string_pretty(&live).expect("s3m nine-queue evidence")
    );
    drop(fireweed);
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shadow_claim_four_incompatible_pending_keys_reject_third_turn() -> EngineResult<()> {
    let mut counters = ShadowCounters::default();
    four_incompatible_claim_keys(&mut counters);
    assert_eq!(
        counters.capacity.get(CLAIM_QUEUE_TURN_RESOURCE).copied(),
        Some(2)
    );
    counters = ShadowCounters::default();

    let root = unique_root();
    let fireweed = Arc::new(open_mixed_product(&root)?);
    let live =
        live_four_incompatible_pending_claim_keys(Arc::clone(&fireweed), now(), &mut counters)
            .await?;
    eprintln!(
        "{}",
        serde_json::to_string_pretty(&live).expect("s3m four-key evidence")
    );
    drop(fireweed);
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn shadow_claim_combined_soak_stays_one_below_every_cap() -> EngineResult<()> {
    let mut counters = ShadowCounters::default();
    let mut timings = S3mTimings::default();
    s3m_combined_soak_one_below_every_cap(&mut counters, &mut timings);
    shadow_exclusive_fence_on_isolated_queue(&mut timings);
    assert!(counters.capacity.is_empty());
    assert!(counters.deadline.is_empty());

    let root = unique_root();
    let projection_path = root.join("projection.db");
    let driver_path = root.join("s3m-driver.db");
    let fireweed = Arc::new(open_mixed_product(&root)?);
    let driver_store = TursoRelational::open(TursoConfig::local(&driver_path))
        .await
        .map_err(|error| EngineError::Storage(error.to_string()))?;
    let pools = driver_store
        .committed_pools()
        .expect("file-backed pools")
        .clone();
    measure_driver_borrow_after_slot(&pools, &mut timings).await?;
    let wal_before = wal_bytes(&projection_path);
    let live = s3m_live_below_cap_apply_traffic(Arc::clone(&fireweed), now(), &mut timings).await?;
    let wal_after = wal_bytes(&projection_path);
    let t2 = timings.t2_diagnostic();
    assert!(
        !timings.claim_cycle.samples.is_empty(),
        "S3m T2 measurement harness is missing claim-cycle samples"
    );
    let evidence = json!({
        "schema": "ss-s3m-shadow-calibration/v1",
        "serving_switched": true,
        "production_fence_activated": false,
        "derived_bounds": s3m_derived_bounds_evidence(),
        "shadow": counters.evidence(),
        "timings": timings.evidence(),
        "live": live,
        "wal_bytes": { "before": wal_before, "after": wal_after },
        "t2": t2,
    });
    eprintln!(
        "{}",
        serde_json::to_string_pretty(&evidence).expect("s3m soak evidence")
    );
    drop(fireweed);
    drop(driver_store);
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

/// Ignored/opt-in S3m harness. Reconstructs Claim-turn/slot and the real
/// selection fence on isolated shadow queues without activating production
/// fence dispositions. Uses public retained reads for drain waits and verifies
/// the physical projection high-water against the final durable log position.
///
/// Default N=100k is too expensive for the default lane; override with
/// `SS_CLAIM_CALIBRATION_N`.
#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn shadow_claim_drain_calibration_uses_exact_high_water() -> EngineResult<()> {
    let n = env_usize("SS_CLAIM_CALIBRATION_N", S3M_CALIBRATION_N);
    assert!(n > 0 && n.is_multiple_of(CLAIM_BATCH));

    let mut counters = ShadowCounters::default();
    let mut timings = S3mTimings::default();
    claim_queue_9(&mut counters);
    four_incompatible_claim_keys(&mut counters);
    s3m_combined_soak_one_below_every_cap(&mut ShadowCounters::default(), &mut timings);
    shadow_exclusive_fence_on_isolated_queue(&mut timings);

    let root = unique_root();
    let projection_path = root.join("projection.db");
    let driver_path = root.join("s3m-driver.db");
    let fireweed = Arc::new(open_mixed_product(&root)?);
    let driver_store = TursoRelational::open(TursoConfig::local(&driver_path))
        .await
        .map_err(|error| EngineError::Storage(error.to_string()))?;
    let pools = driver_store
        .committed_pools()
        .expect("file-backed pools")
        .clone();
    measure_driver_borrow_after_slot(&pools, &mut timings).await?;
    let wal_before = wal_bytes(&projection_path);

    let nine = live_nine_pending_claim_queues(
        Arc::clone(&fireweed),
        now(),
        &mut ShadowCounters::default(),
    )
    .await?;
    let four = live_four_incompatible_pending_claim_keys(
        Arc::clone(&fireweed),
        now(),
        &mut ShadowCounters::default(),
    )
    .await?;

    let queue = queue_key("q-s3m-drain");
    fireweed.create_queue(qdef(queue.queue_id.as_str())).await?;
    let due = now();
    let items: Vec<_> = (0..n)
        .map(|ordinal| {
            let mut item = realistic_item("s3m-drain", ordinal, due);
            // Keep each group within the queue's 100-item limit at N=100k.
            item.group_key =
                Some(GroupKey::new(format!("s3m-drain-group-{}", ordinal / CLAIM_BATCH)).unwrap());
            item
        })
        .collect();
    let original_ids = push_in_batches(fireweed.as_ref(), &queue, items).await?;
    settle(fireweed.as_ref(), &queue).await?;

    let mut seen = HashSet::with_capacity(n);
    while seen.len() < n {
        let ids = claim_publication_plus_apply_cycle(
            fireweed.as_ref(),
            &queue,
            CLAIM_BATCH,
            &mut timings,
        )
        .await?;
        if ids.is_empty() {
            tokio::time::sleep(RETRY_CADENCE).await;
            continue;
        }
        seen.extend(ids);
    }
    let metrics = settle(fireweed.as_ref(), &queue).await?;
    assert_eq!(seen.len(), original_ids.len());
    assert!(original_ids.iter().all(|id| seen.contains(id)));
    assert_eq!(metrics.complete, n as u64);
    assert_eq!(metrics.leased, 0);
    let durable_position = fireweed.current_position(&queue).await?;
    let projection_reader = open_turso_projection_async(&projection_path).await?;
    let applied_position =
        AsyncProjectionStore::recovery_high_water(&projection_reader, queue.clone()).await?;
    assert_eq!(applied_position.as_ref(), Some(&durable_position));
    drop(projection_reader);
    assert!(
        !timings.claim_cycle.samples.is_empty(),
        "S3m T2 measurement harness is missing claim-cycle samples"
    );

    let wal_after = wal_bytes(&projection_path);
    let t2 = timings.t2_diagnostic();
    let evidence = json!({
        "schema": "ss-s3m-shadow-calibration/v1",
        "n": n,
        "serving_switched": true,
        "production_fence_activated": false,
        "exact_high_water_drain": true,
        "durable_position": durable_position,
        "applied_position": applied_position,
        "derived_bounds": s3m_derived_bounds_evidence(),
        "shadow": counters.evidence(),
        "timings": timings.evidence(),
        "capacity_subtests": [nine, four],
        "wal_bytes": { "before": wal_before, "after": wal_after },
        "t2": t2,
        "t2_note": "Recorded only; a short T2 does not fail S3m. Breach still blocks S5.",
    });
    eprintln!(
        "{}",
        serde_json::to_string_pretty(&evidence).expect("s3m drain evidence")
    );

    drop(fireweed);
    drop(driver_store);
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}
