//! fireweed-451a6b23: does the shared store-actor funnel (one mutex behind
//! `InProcessProjectionStore::run_with_store` / `run_with_store_mut`) serialize point reads behind
//! concurrent commits at the snorri shape, hiding wait that a pure-commit probe cannot see?
//!
//! `sqlite_commit_section_contention_probe` (fireweed-77ae7a87) drives ONLY claim+commit and found
//! the projection-axis mutex flat (~0.14 ms/entry, w=1..8) — but snorri's real w=8 worker ladder pins
//! caller-side commit-span concurrency at 5.2-5.3/8 instead of 8/8, with commit latency rising from
//! 0.18 ms/entry (w=1) to 0.75-0.79 ms/entry (w=8). HYPOTHESIS: real workloads interleave commits with
//! point reads of instance state (snorri's `instance_state_read`, ~36 ms/call at commit time); those
//! reads and the concurrent workers' commits share the SAME `InMemoryProjection` mutex on `open_sqlite`
//! (log=sqlite, projection=memory — `offload_projection=false`, so every op funnels through one
//! `Mutex<InMemoryProjection>` inline). This probe adds a point-read op class per finalized entry,
//! interleaved with claim+commit exactly as a real worker would, and decomposes wall time per op class
//! so the funnel is visible even though the pure-commit probe missed it.
//!
//! Evidence: docs/perf/evidence/tp005/mixed-op-funnel-probe-20260811.md

#![cfg(feature = "sqlite")]
#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axon_esf::IndexDef;
use fireweed::*;
use fireweed_core::{IndexDeclaration, IndexType, QueueIndex};
use fireweed_memory::ManualClock;
use serde_json::{Value as JsonValue, json};

const N_INDEXES: usize = 19;
const PAYLOAD_BYTES: usize = 2300;
/// Entries committed per worker weight — same shape as the pure-commit probe so the two are
/// directly comparable ms/entry.
const TOTAL_ENTRIES: usize = 8000;
const CLAIM_BATCH: usize = 500;
/// Stable pool of never-claimed items (pinned to the back of priority order) that every worker reads
/// by unique key once per finalized entry, modeling a caller checking related instance state at
/// commit time (snorri's `instance_state_read`).
const SENTINEL_COUNT: usize = 2000;

fn qdef_snorri_shaped() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t-mixed-op-funnel").unwrap(),
        queue_id: QueueId::new("q-mixed-op-funnel").unwrap(),
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
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 10_000,
        max_claim_batch_size: 10_000,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: (0..N_INDEXES)
            .map(|i| QueueIndex {
                name: format!("by_f{i}"),
                declaration: IndexDeclaration::Single(IndexDef {
                    field: format!("f{i}"),
                    index_type: IndexType::String,
                    unique: i == 0,
                }),
            })
            .collect(),
        emit_change_records: false,
    }
}

fn tmp_sqlite(tag: &str) -> String {
    let path = std::env::temp_dir().join(format!(
        "fireweed-mixed-op-funnel-{tag}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    path.to_string_lossy().into_owned()
}

fn entity_with_key(f0: String) -> JsonValue {
    let mut entity = serde_json::Map::new();
    entity.insert("f0".into(), json!(f0));
    for i in 1..N_INDEXES {
        entity.insert(format!("f{i}"), json!(format!("v{i}-{f0}")));
    }
    JsonValue::Object(entity)
}

fn entity_for(k: u64) -> JsonValue {
    entity_with_key(format!("k-{k}"))
}

fn sentinel_key(j: usize) -> String {
    format!("sentinel-{j}")
}

/// Cumulative call count / wall time for one op class, accumulated across every concurrent worker.
#[derive(Default)]
struct OpClassCounters {
    calls: AtomicU64,
    nanos: AtomicU64,
}

impl OpClassCounters {
    fn record(&self, elapsed: Duration) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.nanos
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
    }

    fn ms_per_entry(&self, total_entries: usize) -> f64 {
        (self.nanos.load(Ordering::Relaxed) as f64 / 1_000_000.0) / total_entries as f64
    }

    fn ms_per_call(&self) -> f64 {
        let calls = self.calls.load(Ordering::Relaxed);
        if calls == 0 {
            return 0.0;
        }
        (self.nanos.load(Ordering::Relaxed) as f64 / 1_000_000.0) / calls as f64
    }
}

#[derive(Default)]
struct MixedOpMetrics {
    claim: OpClassCounters,
    read: OpClassCounters,
    commit: OpClassCounters,
}

/// One worker's slice: claim `iterations` batches of `CLAIM_BATCH`, point-read one sentinel key per
/// claimed item (interleaved before the batch commits, as a real finalize-time state check would),
/// then finalize+lifecycle-push the batch — using a key namespace disjoint from every other
/// worker/run (unique `f0` index).
async fn worker_loop_mixed(
    fw: Arc<Fireweed>,
    queue: QueueKey,
    payload: bytes::Bytes,
    iterations: usize,
    key_base: u64,
    metrics: Arc<MixedOpMetrics>,
) -> usize {
    let mut committed = 0usize;
    let mut next_key = key_base;
    let mut read_cursor = key_base as usize % SENTINEL_COUNT;
    for _ in 0..iterations {
        let claim_start = Instant::now();
        let claimed = fw.claim(&queue, CLAIM_BATCH, 30_000).await.expect("claim");
        metrics.claim.record(claim_start.elapsed());
        assert_eq!(claimed.len(), CLAIM_BATCH, "claim batch size");

        for _ in 0..CLAIM_BATCH {
            let key = sentinel_key(read_cursor % SENTINEL_COUNT);
            read_cursor += 1;
            let read_start = Instant::now();
            let _ = fw
                .query_index_unique_typed(&queue, "by_f0", &[json!(key)])
                .await
                .expect("point read");
            metrics.read.record(read_start.elapsed());
        }

        let entries: Vec<CommitEntry> = claimed
            .into_iter()
            .map(|item| {
                let k = next_key;
                next_key += 1;
                CommitEntry {
                    claim_ref: ClaimRef {
                        item_id: item.item_id,
                        lease_token: item.lease_token.expect("lease"),
                        lease_expires_at: item.lease_expires_at,
                        item_version: item.item_version,
                    },
                    finalize: FinalizeKind::Complete,
                    side_records: vec![],
                    lifecycle_items: vec![NewItem {
                        entity: Some(entity_for(k)),
                        payload: Some(payload.clone()),
                        ..Default::default()
                    }],
                    instance_fence: None,
                }
            })
            .collect();

        let commit_start = Instant::now();
        let outcomes = fw
            .commit(
                &queue,
                CommitRequest {
                    request_id: None,
                    entries,
                },
            )
            .await
            .expect("commit");
        metrics.commit.record(commit_start.elapsed());
        for o in outcomes {
            if let EntryOutcome::Rejected(e) = o {
                panic!("rejected: {e}");
            }
        }
        committed += CLAIM_BATCH;
    }
    committed
}

/// Wall time, per-op-class decomposition, and log/projection lock-phase stats for one worker-count
/// weight.
struct WeightResult {
    workers: usize,
    wall: Duration,
    metrics: Arc<MixedOpMetrics>,
    log_stats: fireweed_engine::LockPhaseSnapshot,
    projection_stats: fireweed_engine::LockPhaseSnapshot,
}

impl WeightResult {
    fn ms_per_entry(&self) -> f64 {
        self.wall.as_secs_f64() * 1000.0 / TOTAL_ENTRIES as f64
    }
    fn projection_wait_ms_per_entry(&self) -> f64 {
        self.projection_stats.wait.as_secs_f64() * 1000.0 / TOTAL_ENTRIES as f64
    }
    fn projection_hold_ms_per_entry(&self) -> f64 {
        self.projection_stats.hold.as_secs_f64() * 1000.0 / TOTAL_ENTRIES as f64
    }
}

async fn measure_weight(workers: usize, run_tag: &str) -> WeightResult {
    assert!(TOTAL_ENTRIES.is_multiple_of(CLAIM_BATCH));
    let iterations = TOTAL_ENTRIES / CLAIM_BATCH;
    assert!(
        iterations.is_multiple_of(workers),
        "iterations={iterations} must divide evenly across workers={workers}"
    );
    let iterations_per_worker = iterations / workers;

    let path = tmp_sqlite(&format!("{run_tag}-w{workers}"));
    let clock = Arc::new(ManualClock::at(0));
    let (fw, backend) =
        open_sqlite_with_lock_stats_handle(&path, clock).expect("open sqlite×memory");
    let fw = Arc::new(fw);
    let def = qdef_snorri_shaped();
    let queue = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    fw.create_queue(def).await.expect("create queue");

    let payload = bytes::Bytes::from(vec![b'x'; PAYLOAD_BYTES]);

    // Seed the stable sentinel read pool first, pinned to the back of priority order so claim never
    // reaches it — it exists purely as a target for point reads throughout the run.
    let sentinels: Vec<NewItem> = (0..SENTINEL_COUNT)
        .map(|j| NewItem {
            entity: Some(entity_with_key(sentinel_key(j))),
            payload: Some(payload.clone()),
            priority: Some(PriorityValue::Int64(i64::MAX)),
            ..Default::default()
        })
        .collect();
    for chunk in sentinels.chunks(500) {
        fw.push_batch(&queue, chunk.to_vec())
            .await
            .expect("seed sentinel pool");
    }

    // Seed one full round of claimable churn items per worker up front; each commit's
    // lifecycle_items replenishes one new item per finalized item, so supply never runs dry mid-run
    // and never reaches the sentinel pool (default priority 0 always sorts before i64::MAX).
    let seed_total = CLAIM_BATCH * workers;
    let seed: Vec<NewItem> = (0..seed_total)
        .map(|i| NewItem {
            entity: Some(entity_for(u64::MAX - i as u64)),
            payload: Some(payload.clone()),
            ..Default::default()
        })
        .collect();
    for chunk in seed.chunks(500) {
        fw.push_batch(&queue, chunk.to_vec())
            .await
            .expect("seed churn pool");
    }

    backend.reset_lock_phase_stats();
    let metrics = Arc::new(MixedOpMetrics::default());
    let start = Instant::now();
    let mut tasks = Vec::with_capacity(workers);
    for w in 0..workers {
        let fw = Arc::clone(&fw);
        let queue = queue.clone();
        let payload = payload.clone();
        let key_base = (w as u64) * 10_000_000;
        tasks.push(tokio::spawn(worker_loop_mixed(
            fw,
            queue,
            payload,
            iterations_per_worker,
            key_base,
            Arc::clone(&metrics),
        )));
    }
    let mut total_committed = 0usize;
    for t in tasks {
        total_committed += t.await.expect("worker task panicked");
    }
    let wall = start.elapsed();
    assert_eq!(total_committed, TOTAL_ENTRIES);

    let result = WeightResult {
        workers,
        wall,
        metrics,
        log_stats: backend.log_lock_phase_stats(),
        projection_stats: backend.projection_lock_phase_stats(),
    };
    let _ = std::fs::remove_file(&path);
    result
}

/// fireweed-451a6b23: print the per-op-class (claim / point-read / commit) ms/entry and ms/call
/// decomposition across w=1/4/8 on the snorri-shaped `open_sqlite` product cell, with commits, point
/// reads, and claims interleaved exactly as a real worker would. Evidence-gathering probe, not a
/// regression gate — absolute contention numbers are host core-count dependent. The only asserted
/// invariant is functional (every commit accepted, entry counts add up); `WeightResult` /
/// `MixedOpMetrics` print the full breakdown for
/// `docs/perf/evidence/tp005/mixed-op-funnel-probe-20260811.md`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sqlite_mixed_op_funnel_ladder_probe() {
    eprintln!("=== mixed-op funnel probe (open_sqlite, 19 indexes, ~2.3KB payload) ===");
    eprintln!(
        "workers\twall_ms/entry\tclaim_ms/entry\tclaim_ms/call\tread_ms/entry\tread_ms/call\tcommit_ms/entry\tcommit_ms/call\tproj_wait_ms/entry\tproj_hold_ms/entry"
    );
    let mut results = Vec::new();
    for workers in [1usize, 4, 8] {
        let r = measure_weight(workers, "mixed-op-funnel").await;
        eprintln!(
            "{}\t{:.4}\t{:.4}\t{:.4}\t{:.4}\t{:.4}\t{:.4}\t{:.4}\t{:.4}\t{:.4}",
            r.workers,
            r.ms_per_entry(),
            r.metrics.claim.ms_per_entry(TOTAL_ENTRIES),
            r.metrics.claim.ms_per_call(),
            r.metrics.read.ms_per_entry(TOTAL_ENTRIES),
            r.metrics.read.ms_per_call(),
            r.metrics.commit.ms_per_entry(TOTAL_ENTRIES),
            r.metrics.commit.ms_per_call(),
            r.projection_wait_ms_per_entry(),
            r.projection_hold_ms_per_entry(),
        );
        results.push(r);
    }
    let w1 = &results[0];
    let w8 = &results[2];
    eprintln!(
        "w=1 commit_ms/entry={:.4} w=8 commit_ms/entry={:.4} (w=8 commit grew {:.2}x over w=1; w=1 wall_ms/entry={:.4} w=8 wall_ms/entry={:.4}, {:.2}x)",
        w1.metrics.commit.ms_per_entry(TOTAL_ENTRIES),
        w8.metrics.commit.ms_per_entry(TOTAL_ENTRIES),
        w8.metrics.commit.ms_per_entry(TOTAL_ENTRIES)
            / w1.metrics.commit.ms_per_entry(TOTAL_ENTRIES).max(1e-9),
        w1.ms_per_entry(),
        w8.ms_per_entry(),
        w8.ms_per_entry() / w1.ms_per_entry().max(1e-9),
    );
}
