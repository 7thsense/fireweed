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

#![cfg(all(feature = "objectlog", feature = "turso"))]

use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use fireweed::turso_compose::open_turso_projection_async;
use fireweed::*;
use fireweed_core::{Metadata, MetadataValue};
use fireweed_engine::AsyncLogStore;
use fireweed_objectlog::{ObjectLogEngineStore, flush_config_from_segment};
use fireweed_turso::TursoRelational;
use serde_json::{Value, json};

const DEFAULT_N: usize = 10_000;
const BATCH: usize = 100;
const CLAIM_BATCH: usize = 100;
const RETRY_CADENCE: Duration = Duration::from_millis(25);
const OBSERVATION_SAMPLES: usize = 16;
const PACK_LINGER_MS: u64 = 20;

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
            Err(EngineError::Backpressure { .. }) => {
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
    // The derived Turso implementation catches up durable projection debt before
    // returning metrics. Every rate below ends at this barrier, not at log ack.
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

    // Settle once at cohort entry. The six calls below hit the committed serving
    // reader directly and therefore measure observation, not apply catch-up.
    settle(fireweed, queue).await?;
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
        let pair = vec![
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
            .join("../../docs/perf/evidence/ss-phased")
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
