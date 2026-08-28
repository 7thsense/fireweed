//! Seventh Sense continuous bounded streaming lane.
//!
//! Replaces `join_all` wave barriers **in this evidence lane** with bounded
//! replenishment: each stage forms public batches of 100, depth defaults to 8,
//! and completion of any in-flight future immediately admits the next batch.
//! Queue capacity is the backpressure mechanism. Exact N — not an empty
//! suffix — terminates every stage; the run settles with zero residual debt.
//!
//! The phased harness in `ss_phased_capacity.rs` keeps its `join_all` phase
//! barriers for per-phase attribution.
//!
//! Default N is 800 so the lane is cheap enough for focused CI. Override with
//! `SS_N` (must be divisible by the public batch). N=10_000 is the smoke size
//! when the cell is fast enough:
//!
//! ```text
//! cargo test -p fireweed --features objectlog --test ss_streaming \
//!   ss_streaming_continuously_replenishes_to_exact_n -- --exact --nocapture
//!
//! SS_N=10000 cargo test -p fireweed --features objectlog --test ss_streaming \
//!   ss_streaming_continuously_replenishes_to_exact_n -- --exact --nocapture
//! ```
//!
//! Optional: `SS_CELL`, `SS_PUSH_BATCH`, `SS_CLAIM_BATCH`, `SS_INFLIGHT`,
//! `SS_EVIDENCE_WRITE=1`.

#![cfg(feature = "objectlog")]

#[path = "support/ss_capacity.rs"]
mod ss_capacity;
use ss_capacity::*;

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use fireweed::*;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;

const STREAMING_DEFAULT_N: usize = 800;
const PUBLIC_BATCH: usize = 100;
const RETRY_CADENCE: Duration = Duration::from_millis(25);
const CLAIM_BATCH_DEADLINE: Duration = Duration::from_secs(120);
const SCHEMA: &str = "ss-streaming-summary/v1";

#[derive(Clone, Copy, Debug)]
struct WorkEvent {
    index: usize,
    admitted_at: Instant,
    completed_at: Instant,
}

struct ReplenishOutcome<R> {
    results: Vec<R>,
    events: Vec<WorkEvent>,
    peak_inflight: usize,
}

struct StageReport {
    name: &'static str,
    items: usize,
    queue: CallStats,
    service: CallStats,
    peak_inflight: usize,
    peak_queue_depth: usize,
    empty_suffix_ignored: usize,
    events: Vec<WorkEvent>,
}

impl StageReport {
    fn evidence(&self) -> Value {
        json!({
            "name": self.name,
            "items": self.items,
            "terminated_by": "exact_n",
            "queue_latency": self.queue.evidence("queue"),
            "service_latency": self.service.evidence("service"),
            "peak_inflight": self.peak_inflight,
            "peak_queue_depth": self.peak_queue_depth,
            "empty_suffix_ignored": self.empty_suffix_ignored,
        })
    }
}

/// Continuous bounded replenishment: completing any future immediately admits
/// the next item. This is the streaming admission mechanism; `join_all` is not.
fn track<R, Fut>(
    index: usize,
    admitted_at: Instant,
    fut: Fut,
) -> impl Future<Output = (usize, Instant, Instant, R)>
where
    Fut: Future<Output = R>,
{
    async move {
        let result = fut.await;
        (index, admitted_at, Instant::now(), result)
    }
}

async fn replenish_items<T, R, F, Fut>(
    depth: usize,
    items: impl IntoIterator<Item = T>,
    mut work: F,
) -> ReplenishOutcome<R>
where
    F: FnMut(usize, T) -> Fut,
    Fut: Future<Output = R>,
{
    let depth = depth.max(1);
    let mut items = items.into_iter();
    let mut inflight = FuturesUnordered::new();
    let mut next_index = 0usize;
    let mut results = Vec::new();
    let mut events = Vec::new();
    let mut peak_inflight = 0usize;

    loop {
        while inflight.len() < depth {
            let Some(item) = items.next() else { break };
            let index = next_index;
            next_index += 1;
            inflight.push(track(index, Instant::now(), work(index, item)));
            peak_inflight = peak_inflight.max(inflight.len());
        }
        let Some((index, admitted_at, completed_at, result)) = inflight.next().await else {
            break;
        };
        events.push(WorkEvent {
            index,
            admitted_at,
            completed_at,
        });
        results.push(result);
        // Loop immediately fills the freed slot. No wave barrier.
    }

    ReplenishOutcome {
        results,
        events,
        peak_inflight,
    }
}

async fn replenish_rx<T, R, F, Fut>(
    depth: usize,
    mut rx: mpsc::Receiver<T>,
    mut work: F,
) -> ReplenishOutcome<R>
where
    F: FnMut(usize, T) -> Fut,
    Fut: Future<Output = R>,
{
    let depth = depth.max(1);
    let mut inflight = FuturesUnordered::new();
    let mut next_index = 0usize;
    let mut results = Vec::new();
    let mut events = Vec::new();
    let mut peak_inflight = 0usize;
    let mut closed = false;

    loop {
        while inflight.len() < depth && !closed {
            match rx.try_recv() {
                Ok(item) => {
                    let index = next_index;
                    next_index += 1;
                    inflight.push(track(index, Instant::now(), work(index, item)));
                    peak_inflight = peak_inflight.max(inflight.len());
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    closed = true;
                    break;
                }
            }
        }

        if inflight.is_empty() {
            if closed {
                break;
            }
            match rx.recv().await {
                Some(item) => {
                    let index = next_index;
                    next_index += 1;
                    inflight.push(track(index, Instant::now(), work(index, item)));
                    peak_inflight = peak_inflight.max(inflight.len());
                }
                None => break,
            }
            continue;
        }

        let done = if closed || inflight.len() >= depth {
            inflight.next().await
        } else {
            tokio::select! {
                done = inflight.next() => done,
                item = rx.recv() => {
                    match item {
                        Some(item) => {
                            let index = next_index;
                            next_index += 1;
                            inflight.push(track(index, Instant::now(), work(index, item)));
                            peak_inflight = peak_inflight.max(inflight.len());
                            continue;
                        }
                        None => {
                            closed = true;
                            continue;
                        }
                    }
                }
            }
        };
        let Some((index, admitted_at, completed_at, result)) = done else {
            break;
        };
        events.push(WorkEvent {
            index,
            admitted_at,
            completed_at,
        });
        results.push(result);
    }

    ReplenishOutcome {
        results,
        events,
        peak_inflight,
    }
}

async fn retry_call<T, F, Fut>(label: &str, mut make: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = EngineResult<T>>,
{
    let mut retries = 0u32;
    loop {
        match make().await {
            Ok(value) => return value,
            Err(EngineError::Backpressure { .. }) => {
                retries += 1;
                assert!(
                    retries < 100_000,
                    "{label} backpressure did not converge after {retries} retries"
                );
                tokio::time::sleep(RETRY_CADENCE).await;
            }
            Err(error) => panic!("{label}: {error}"),
        }
    }
}

fn used_queue_depth<T>(tx: &mpsc::Sender<T>) -> usize {
    tx.max_capacity().saturating_sub(tx.capacity())
}

async fn forward<T: Send>(tx: &mpsc::Sender<T>, value: T) -> (Duration, usize) {
    let started = Instant::now();
    tx.send(value).await.expect("streaming stage still open");
    (started.elapsed(), used_queue_depth(tx))
}

fn batches(n: usize, batch: usize) -> Vec<(usize, usize)> {
    (0..n)
        .step_by(batch)
        .map(|start| (start, (start + batch).min(n)))
        .collect()
}

fn now_ts() -> UtcTimestamp {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    UtcTimestamp::new(elapsed.as_secs() as i64, 0).unwrap()
}

fn streaming_evidence_v1(
    mut run: Value,
    stages: &[StageReport],
    n: usize,
    settled_wall: Duration,
    process_wall: Duration,
    residual: &ResidualSnapshot,
) -> Value {
    let object = run.as_object_mut().expect("run evidence object");
    object.insert("schema".into(), json!(SCHEMA));
    object.insert("n".into(), json!(n));
    object.insert("admission".into(), json!("bounded-replenish"));
    object.insert("process_wall_s".into(), json!(process_wall.as_secs_f64()));
    object.insert("settled_wall_s".into(), json!(settled_wall.as_secs_f64()));
    object.insert(
        "settled_items_per_s".into(),
        json!(n as f64 / settled_wall.as_secs_f64().max(1e-9)),
    );
    object.insert("final_residual".into(), residual.evidence());
    object.insert(
        "stages".into(),
        Value::Array(stages.iter().map(StageReport::evidence).collect()),
    );
    run
}

async fn prove_replenish_frees_slot_immediately() {
    let depth = 8;
    let n = 24;
    let outcome = replenish_items(depth, 0..n, |index, _item| async move {
        let delay = if index % depth == 0 {
            Duration::from_millis(20)
        } else {
            Duration::from_millis(250)
        };
        tokio::time::sleep(delay).await;
        index
    })
    .await;

    assert_eq!(outcome.results.len(), n, "exact N, not an empty suffix");
    assert!(
        outcome.peak_inflight <= depth,
        "stage queue must stay bounded: peak {} depth {depth}",
        outcome.peak_inflight
    );
    assert_eq!(outcome.peak_inflight, depth);

    let by_index: HashMap<_, _> = outcome
        .events
        .iter()
        .map(|event| (event.index, *event))
        .collect();
    let ninth = by_index
        .get(&depth)
        .expect("ninth batch must be admitted after a slot frees");
    let first_slow = by_index.get(&1).expect("slow first-wave member");
    assert!(
        ninth.admitted_at < first_slow.completed_at,
        "completion of one future must admit the next batch before the rest of a join_all wave would finish; ninth admitted {:?} vs first-slow completed {:?}",
        ninth.admitted_at,
        first_slow.completed_at
    );
}

struct PipelineConfig {
    fw: Arc<Fireweed>,
    queue: QueueKey,
    n: usize,
    batch: usize,
    claim_batch: usize,
    depth: usize,
    stub: Bytes,
    profile: Bytes,
    far: UtcTimestamp,
    due: UtcTimestamp,
}

async fn run_streaming_pipeline(
    cfg: PipelineConfig,
) -> (Vec<StageReport>, ResidualSnapshot, Duration) {
    let started = Instant::now();
    let depth = cfg.depth;
    let (enrich_tx, enrich_rx) = mpsc::channel::<Vec<ClientItemKey>>(depth);
    let (schedule_tx, schedule_rx) = mpsc::channel::<Vec<ClientItemKey>>(depth);
    let (claim_tx, claim_rx) = mpsc::channel::<Vec<ClientItemKey>>(depth);
    let (complete_tx, complete_rx) = mpsc::channel::<Vec<ItemId>>(depth);

    let ingest = ingest_stage(&cfg, enrich_tx);
    let enrich = enrich_stage(&cfg, enrich_rx, schedule_tx);
    let schedule = schedule_stage(&cfg, schedule_rx, claim_tx);
    let claim = claim_stage(&cfg, claim_rx, complete_tx);
    let complete = complete_stage(&cfg, complete_rx);

    let (ingest, enrich, schedule, claim, complete) =
        tokio::join!(ingest, enrich, schedule, claim, complete);

    assert_eq!(ingest.items, cfg.n, "ingest exact N");
    assert_eq!(enrich.items, cfg.n, "enrich exact N");
    assert_eq!(schedule.items, cfg.n, "schedule exact N");
    assert_eq!(claim.items, cfg.n, "claim exact N, not an empty suffix");
    assert_eq!(complete.items, cfg.n, "complete exact N");
    let expected_batches = batches(cfg.n, cfg.batch).len();
    assert_eq!(ingest.events.len(), expected_batches);
    assert_eq!(enrich.events.len(), expected_batches);
    assert_eq!(schedule.events.len(), expected_batches);
    assert_eq!(claim.events.len(), expected_batches);
    assert_eq!(complete.events.len(), expected_batches);
    for stage in [&ingest, &enrich, &schedule, &claim, &complete] {
        assert!(
            stage.peak_inflight <= depth,
            "{} peak inflight {} exceeds depth {depth}",
            stage.name,
            stage.peak_inflight
        );
        assert!(
            stage.peak_queue_depth <= depth,
            "{} peak queue {} exceeds depth {depth}",
            stage.name,
            stage.peak_queue_depth
        );
    }

    let (settled_wall, residual) =
        settle_phase(cfg.fw.as_ref(), &cfg.queue, started, cfg.n + 1).await;
    let settle = StageReport {
        name: "settle",
        items: residual.complete as usize,
        queue: CallStats::new(),
        service: {
            let mut stats = CallStats::new();
            stats.record(settled_wall);
            stats
        },
        peak_inflight: 1,
        peak_queue_depth: 0,
        empty_suffix_ignored: 0,
        events: Vec::new(),
    };

    (
        vec![ingest, enrich, schedule, claim, complete, settle],
        residual,
        settled_wall,
    )
}

async fn ingest_stage(cfg: &PipelineConfig, tx: mpsc::Sender<Vec<ClientItemKey>>) -> StageReport {
    let fw = Arc::clone(&cfg.fw);
    let queue = cfg.queue.clone();
    let stub = cfg.stub.clone();
    let far = cfg.far;
    let n = cfg.n;
    let mut queue_stats = CallStats::new();
    let mut service = CallStats::new();
    let mut peak_queue_depth = 0usize;
    let outcome = replenish_items(
        cfg.depth,
        batches(cfg.n, cfg.batch),
        |index, (start, end)| {
            let fw = Arc::clone(&fw);
            let queue = queue.clone();
            let stub = stub.clone();
            let tx = tx.clone();
            async move {
                let keys: Vec<ClientItemKey> = (start..end).map(key).collect();
                let items: Vec<NewItem> = (start..end)
                    .map(|i| NewItem {
                        client_item_key: Some(key(i)),
                        group_key: Some(job_key(i, n)),
                        payload: Some(stub.clone()),
                        metadata: phase_meta("needs_profile"),
                        not_before: Some(far),
                        priority: Some(PriorityValue::Timestamp(far)),
                        ..Default::default()
                    })
                    .collect();
                let service_started = Instant::now();
                let ids = retry_call("ingest push", || fw.push_batch(&queue, items.clone())).await;
                assert_eq!(ids.len(), end - start, "ingest batch {index}");
                let service_elapsed = service_started.elapsed();
                let (queue_elapsed, queued) = forward(&tx, keys.clone()).await;
                (keys.len(), service_elapsed, queue_elapsed, queued)
            }
        },
    )
    .await;
    drop(tx);
    let mut items = 0usize;
    for (count, service_elapsed, queue_elapsed, queued) in outcome.results {
        items += count;
        service.record(service_elapsed);
        queue_stats.record(queue_elapsed);
        peak_queue_depth = peak_queue_depth.max(queued);
    }
    StageReport {
        name: "ingest",
        items,
        queue: queue_stats,
        service,
        peak_inflight: outcome.peak_inflight,
        peak_queue_depth,
        empty_suffix_ignored: 0,
        events: outcome.events,
    }
}

async fn enrich_stage(
    cfg: &PipelineConfig,
    rx: mpsc::Receiver<Vec<ClientItemKey>>,
    tx: mpsc::Sender<Vec<ClientItemKey>>,
) -> StageReport {
    let fw = Arc::clone(&cfg.fw);
    let queue = cfg.queue.clone();
    let profile = cfg.profile.clone();
    let mut queue_stats = CallStats::new();
    let mut service = CallStats::new();
    let mut peak_queue_depth = 0usize;
    let outcome = replenish_rx(cfg.depth, rx, |index, keys: Vec<ClientItemKey>| {
        let fw = Arc::clone(&fw);
        let queue = queue.clone();
        let profile = profile.clone();
        let tx = tx.clone();
        async move {
            let updates: Vec<BatchUpdateEntry> = keys
                .iter()
                .map(|item_key| BatchUpdateEntry {
                    item_ref: BatchUpdateItemRef::ClientItemKey(item_key.clone()),
                    expected_item_version: None,
                    priority: BatchUpdateValue::Keep,
                    not_before: BatchUpdateValue::Keep,
                    payload: BatchUpdateValue::Replace(Some(profile.clone())),
                    metadata: BatchUpdateValue::Replace(phase_meta("needs_schedule")),
                    gate_keys: BatchUpdateValue::Keep,
                    fields: BatchUpdateValue::Keep,
                })
                .collect();
            let expect = keys.len();
            let req = BatchUpdateRequest {
                request_id: RequestId::new(format!("enrich-{index:08}")).unwrap(),
                updates,
            };
            let service_started = Instant::now();
            let resp = retry_call("enrich", || fw.batch_update(&queue, req.clone())).await;
            let ok = resp
                .results
                .iter()
                .filter(|r| matches!(r, BatchUpdateOutcome::Updated { .. }))
                .count();
            assert_eq!(ok, expect, "enrich every entry Updated");
            let service_elapsed = service_started.elapsed();
            let (queue_elapsed, queued) = forward(&tx, keys).await;
            (ok, service_elapsed, queue_elapsed, queued)
        }
    })
    .await;
    drop(tx);
    let mut items = 0usize;
    for (count, service_elapsed, queue_elapsed, queued) in outcome.results {
        items += count;
        service.record(service_elapsed);
        queue_stats.record(queue_elapsed);
        peak_queue_depth = peak_queue_depth.max(queued);
    }
    StageReport {
        name: "enrich",
        items,
        queue: queue_stats,
        service,
        peak_inflight: outcome.peak_inflight,
        peak_queue_depth,
        empty_suffix_ignored: 0,
        events: outcome.events,
    }
}

async fn schedule_stage(
    cfg: &PipelineConfig,
    rx: mpsc::Receiver<Vec<ClientItemKey>>,
    tx: mpsc::Sender<Vec<ClientItemKey>>,
) -> StageReport {
    let fw = Arc::clone(&cfg.fw);
    let queue = cfg.queue.clone();
    let due = cfg.due;
    let mut queue_stats = CallStats::new();
    let mut service = CallStats::new();
    let mut peak_queue_depth = 0usize;
    let outcome = replenish_rx(cfg.depth, rx, |index, keys: Vec<ClientItemKey>| {
        let fw = Arc::clone(&fw);
        let queue = queue.clone();
        let tx = tx.clone();
        async move {
            let updates: Vec<BatchUpdateEntry> = keys
                .iter()
                .map(|item_key| BatchUpdateEntry {
                    item_ref: BatchUpdateItemRef::ClientItemKey(item_key.clone()),
                    expected_item_version: None,
                    priority: BatchUpdateValue::Replace(PriorityValue::Timestamp(due)),
                    not_before: BatchUpdateValue::Replace(Some(due)),
                    payload: BatchUpdateValue::Keep,
                    metadata: BatchUpdateValue::Replace(phase_meta("ready")),
                    gate_keys: BatchUpdateValue::Keep,
                    fields: BatchUpdateValue::Keep,
                })
                .collect();
            let expect = keys.len();
            let req = BatchUpdateRequest {
                request_id: RequestId::new(format!("schedule-{index:08}")).unwrap(),
                updates,
            };
            let service_started = Instant::now();
            let resp = retry_call("schedule", || fw.batch_update(&queue, req.clone())).await;
            let ok = resp
                .results
                .iter()
                .filter(|r| matches!(r, BatchUpdateOutcome::Updated { .. }))
                .count();
            assert_eq!(ok, expect, "schedule every entry Updated");
            let service_elapsed = service_started.elapsed();
            let (queue_elapsed, queued) = forward(&tx, keys).await;
            (ok, service_elapsed, queue_elapsed, queued)
        }
    })
    .await;
    drop(tx);
    let mut items = 0usize;
    for (count, service_elapsed, queue_elapsed, queued) in outcome.results {
        items += count;
        service.record(service_elapsed);
        queue_stats.record(queue_elapsed);
        peak_queue_depth = peak_queue_depth.max(queued);
    }
    StageReport {
        name: "schedule",
        items,
        queue: queue_stats,
        service,
        peak_inflight: outcome.peak_inflight,
        peak_queue_depth,
        empty_suffix_ignored: 0,
        events: outcome.events,
    }
}

async fn claim_stage(
    cfg: &PipelineConfig,
    rx: mpsc::Receiver<Vec<ClientItemKey>>,
    tx: mpsc::Sender<Vec<ItemId>>,
) -> StageReport {
    let fw = Arc::clone(&cfg.fw);
    let queue = cfg.queue.clone();
    let claim_batch = cfg.claim_batch;
    let mut queue_stats = CallStats::new();
    let mut service = CallStats::new();
    let mut peak_queue_depth = 0usize;
    let mut empty_suffix_ignored = 0usize;
    let outcome = replenish_rx(cfg.depth, rx, |_index, keys: Vec<ClientItemKey>| {
        let fw = Arc::clone(&fw);
        let queue = queue.clone();
        let tx = tx.clone();
        async move {
            let want = keys.len().min(claim_batch).max(1);
            let deadline = Instant::now() + CLAIM_BATCH_DEADLINE;
            let mut got = Vec::with_capacity(want);
            let mut empty = 0usize;
            let service_started = Instant::now();
            while got.len() < want {
                assert!(
                    Instant::now() < deadline,
                    "claim did not observe scheduled items before deadline (empty suffix is not termination)"
                );
                let take = want - got.len();
                let batch = retry_call("claim", || fw.claim(&queue, take, 30_000)).await;
                if batch.is_empty() {
                    empty += 1;
                    tokio::time::sleep(RETRY_CADENCE).await;
                    continue;
                }
                got.extend(batch.into_iter().map(|item| item.item_id));
            }
            let service_elapsed = service_started.elapsed();
            let count = got.len();
            let (queue_elapsed, queued) = forward(&tx, got).await;
            (count, empty, service_elapsed, queue_elapsed, queued)
        }
    })
    .await;
    drop(tx);
    let mut items = 0usize;
    for (count, empty, service_elapsed, queue_elapsed, queued) in outcome.results {
        items += count;
        empty_suffix_ignored += empty;
        service.record(service_elapsed);
        queue_stats.record(queue_elapsed);
        peak_queue_depth = peak_queue_depth.max(queued);
    }
    StageReport {
        name: "claim",
        items,
        queue: queue_stats,
        service,
        peak_inflight: outcome.peak_inflight,
        peak_queue_depth,
        empty_suffix_ignored,
        events: outcome.events,
    }
}

async fn complete_stage(cfg: &PipelineConfig, rx: mpsc::Receiver<Vec<ItemId>>) -> StageReport {
    let fw = Arc::clone(&cfg.fw);
    let queue = cfg.queue.clone();
    let mut service = CallStats::new();
    let outcome = replenish_rx(cfg.depth, rx, |_index, ids: Vec<ItemId>| {
        let fw = Arc::clone(&fw);
        let queue = queue.clone();
        async move {
            let count = ids.len();
            let service_started = Instant::now();
            retry_call("complete", || fw.complete(&queue, ids.clone())).await;
            (count, service_started.elapsed())
        }
    })
    .await;
    let mut items = 0usize;
    for (count, service_elapsed) in outcome.results {
        items += count;
        service.record(service_elapsed);
    }
    StageReport {
        name: "complete",
        items,
        queue: CallStats::new(),
        service,
        peak_inflight: outcome.peak_inflight,
        peak_queue_depth: 0,
        empty_suffix_ignored: 0,
        events: outcome.events,
    }
}

fn write_streaming_evidence(
    cell: &Cell,
    stages: &[StageReport],
    n: usize,
    batch: usize,
    claim_batch: usize,
    depth: usize,
    residual: &ResidualSnapshot,
    settled_wall: Duration,
    process_wall: Duration,
) {
    if std::env::var("SS_EVIDENCE_WRITE").as_deref() != Ok("1") {
        return;
    }
    let utc = chrono_like_utc();
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/perf/evidence/ss-phased")
        .join(&utc);
    let _ = std::fs::create_dir_all(&dir);
    let command = format!(
        "SS_CELL={} SS_N={n} SS_PUSH_BATCH={batch} SS_CLAIM_BATCH={claim_batch} SS_INFLIGHT={depth} cargo test -p fireweed --features objectlog --test ss_streaming ss_streaming_continuously_replenishes_to_exact_n -- --exact --nocapture",
        cell.cell_name()
    );
    let evidence = streaming_evidence_v1(
        json!({
            "utc": utc,
            "source_sha": source_sha(),
            "host": host_name(),
            "command": command,
            "cell": cell.cell_name(),
            "log_axis": cell.log_axis(),
            "projection_axis": cell.projection_axis(),
            "depth": depth,
            "push_batch": batch,
            "claim_batch": claim_batch,
        }),
        stages,
        n,
        settled_wall,
        process_wall,
        residual,
    );
    let path = dir.join("streaming-summary.json");
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&evidence).expect("serialize streaming evidence"),
    )
    .expect("write streaming evidence");
    eprintln!("wrote {}", path.display());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn ss_streaming_continuously_replenishes_to_exact_n() {
    let process_started = Instant::now();
    prove_replenish_frees_slot_immediately().await;

    let n = env_usize("SS_N", STREAMING_DEFAULT_N);
    let batch = env_usize("SS_PUSH_BATCH", PUBLIC_BATCH);
    let claim_batch = env_usize("SS_CLAIM_BATCH", PUBLIC_BATCH);
    assert!(
        n > 0 && n.is_multiple_of(batch) && n.is_multiple_of(claim_batch),
        "SS_N must be >0 and divisible by SS_PUSH_BATCH and SS_CLAIM_BATCH"
    );

    let cell = Cell::parse_for("streaming");
    let depth = cell.inflight();
    eprintln!("{} depth={depth} N={n} batch={batch}", cell.describe());
    let fw = Arc::new(cell.open(Arc::new(SystemClock)));
    let def = qdef("t-ss-stream", "q-ss", batch, claim_batch);
    fw.create_queue(def).await.expect("create streaming queue");
    let queue = QueueKey::new(
        TenantId::new("t-ss-stream").unwrap(),
        QueueId::new("q-ss").unwrap(),
    );
    let now = now_ts();
    let cfg = PipelineConfig {
        fw: Arc::clone(&fw),
        queue: queue.clone(),
        n,
        batch,
        claim_batch,
        depth,
        stub: Bytes::from(vec![b's'; STUB_BYTES]),
        profile: Bytes::from(vec![b'p'; PROFILE_BYTES]),
        far: UtcTimestamp::new(now.seconds.saturating_add(86_400), 0).unwrap(),
        due: UtcTimestamp::new(now.seconds.saturating_sub(1), 0).unwrap(),
    };

    let (stages, residual, settled_wall) = run_streaming_pipeline(cfg).await;
    assert_eq!(residual.pending, 0, "residual pending");
    assert_eq!(residual.leased, 0, "residual leased");
    assert_eq!(residual.complete, n as u64, "complete count");
    assert_eq!(residual.failed, 0, "residual failed");
    assert_eq!(residual.eligible, 0, "residual eligible");

    eprintln!(
        "=== ss_streaming cell={} depth={depth} N={n} settled_wall_s={:.3} settled_items_per_s={:.0} ===",
        cell.cell_name(),
        settled_wall.as_secs_f64(),
        n as f64 / settled_wall.as_secs_f64().max(1e-9)
    );
    for stage in &stages {
        eprintln!(
            "  {}\titems={}\tpeak_inflight={}\tpeak_queue={}\tempty_ignored={}\tservice_p50={:.2}ms\tqueue_p50={:.2}ms",
            stage.name,
            stage.items,
            stage.peak_inflight,
            stage.peak_queue_depth,
            stage.empty_suffix_ignored,
            stage.service.percentile_ms(50.0),
            stage.queue.percentile_ms(50.0)
        );
    }

    write_streaming_evidence(
        &cell,
        &stages,
        n,
        batch,
        claim_batch,
        depth,
        &residual,
        settled_wall,
        process_started.elapsed(),
    );
    drop(fw);
    cell.cleanup();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn ss_streaming_alternating_produce_claim_complete_reaches_tail() {
    let n = env_usize("SS_N", STREAMING_DEFAULT_N);
    let batch = env_usize("SS_PUSH_BATCH", PUBLIC_BATCH);
    assert!(
        n > 0 && n.is_multiple_of(batch),
        "SS_N must be >0 and divisible by SS_PUSH_BATCH"
    );
    let cell = Cell::parse_for("streaming-alt");
    let fw = Arc::new(cell.open(Arc::new(SystemClock)));
    fw.create_queue(qdef("t-ss-stream", "q-ss-alt", batch, batch))
        .await
        .expect("create alt queue");
    let queue = QueueKey::new(
        TenantId::new("t-ss-stream").unwrap(),
        QueueId::new("q-ss-alt").unwrap(),
    );
    let due = now_ts();
    let stub = Bytes::from(vec![b'a'; STUB_BYTES]);
    let mut produced = 0usize;
    let mut completed = 0usize;
    let mut produce_counter = Vec::new();
    let mut complete_counter = Vec::new();

    for (start, end) in batches(n, batch) {
        let items: Vec<NewItem> = (start..end)
            .map(|i| NewItem {
                client_item_key: Some(key(i)),
                group_key: Some(job_key(i, n)),
                payload: Some(stub.clone()),
                metadata: phase_meta("ready"),
                not_before: Some(due),
                priority: Some(PriorityValue::Timestamp(due)),
                ..Default::default()
            })
            .collect();
        let ids = retry_call("alt produce", || fw.push_batch(&queue, items.clone())).await;
        produced += ids.len();
        produce_counter.push(produced);

        let want = end - start;
        let deadline = Instant::now() + CLAIM_BATCH_DEADLINE;
        let mut claimed = Vec::new();
        while claimed.len() < want {
            assert!(
                Instant::now() < deadline,
                "alternating claim starved before tail; produce={produced} complete={completed}"
            );
            let batch_items = retry_call("alt claim", || {
                fw.claim(&queue, want - claimed.len(), 30_000)
            })
            .await;
            if batch_items.is_empty() {
                tokio::time::sleep(RETRY_CADENCE).await;
                continue;
            }
            claimed.extend(batch_items.into_iter().map(|item| item.item_id));
        }
        retry_call("alt complete", || fw.complete(&queue, claimed.clone())).await;
        completed += claimed.len();
        complete_counter.push(completed);
        assert_eq!(
            produced, completed,
            "alternating path must advance both counters together"
        );
    }

    assert_eq!(produced, n);
    assert_eq!(completed, n);
    assert_eq!(*produce_counter.last().unwrap(), n);
    assert_eq!(*complete_counter.last().unwrap(), n);
    let (settled_wall, residual) = settle_phase(fw.as_ref(), &queue, Instant::now(), n + 1).await;
    let _ = settled_wall;
    assert_eq!(residual.pending, 0);
    assert_eq!(residual.leased, 0);
    assert_eq!(residual.complete, n as u64);
    assert_eq!(residual.eligible, 0);
    drop(fw);
    cell.cleanup();
}

#[test]
fn ss_streaming_evidence_schema_reports_settled_rate_and_stage_latency() {
    let mk = |name, items, empty| {
        let mut service = CallStats::new();
        service.record(Duration::from_millis(10));
        let mut queue = CallStats::new();
        queue.record(Duration::from_millis(2));
        StageReport {
            name,
            items,
            queue,
            service,
            peak_inflight: 8,
            peak_queue_depth: 4,
            empty_suffix_ignored: empty,
            events: Vec::new(),
        }
    };
    let stages = [
        mk("ingest", 32, 0),
        mk("enrich", 32, 0),
        mk("schedule", 32, 0),
        mk("claim", 32, 3),
        mk("complete", 32, 0),
        mk("settle", 32, 0),
    ];
    let residual = ResidualSnapshot {
        pending: 0,
        leased: 0,
        complete: 32,
        failed: 0,
        eligible: 0,
    };
    let evidence = streaming_evidence_v1(
        json!({
            "source_sha": "fixture",
            "host": "fixture",
            "command": "fixture",
            "depth": 8,
            "push_batch": 100,
        }),
        &stages,
        32,
        Duration::from_millis(40),
        Duration::from_millis(50),
        &residual,
    );

    assert_eq!(evidence["schema"], SCHEMA);
    assert_eq!(evidence["admission"], "bounded-replenish");
    assert_eq!(evidence["n"], 32);
    assert!((evidence["settled_items_per_s"].as_f64().unwrap() - 800.0).abs() < 1e-6);
    assert_eq!(evidence["settled_wall_s"], 0.04);
    assert_eq!(evidence["process_wall_s"], 0.05);
    assert_eq!(evidence["final_residual"]["complete"], 32);
    assert_eq!(evidence["final_residual"]["pending"], 0);
    let names: Vec<&str> = evidence["stages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "ingest", "enrich", "schedule", "claim", "complete", "settle"
        ]
    );
    for row in evidence["stages"].as_array().unwrap() {
        assert_eq!(row["terminated_by"], "exact_n");
        assert!(row["queue_latency"]["p50_ms"].is_number());
        assert!(row["service_latency"]["p50_ms"].is_number());
        assert!(row["peak_inflight"].as_u64().unwrap() <= 8);
        assert!(row["peak_queue_depth"].as_u64().unwrap() <= 8);
    }
}

#[test]
fn ss_streaming_admission_is_not_join_all() {
    let src = include_str!("ss_streaming.rs");
    let forbidden = ["futures", "future", "join_all"].join("::");
    assert!(
        !src.contains(&forbidden),
        "streaming admission must not use join_all wave barriers"
    );
    assert!(
        src.contains("FuturesUnordered"),
        "streaming admission must replenish via FuturesUnordered"
    );
    assert!(
        src.contains("immediately admits"),
        "replenish helper must document slot-free admission"
    );
}
