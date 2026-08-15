//! fireweed-77ae7a87: decompose the w=8 commit-section span into hold-time (executing while
//! holding the log/projection store mutex), wait-time (queueing to acquire it), and off-lock work
//! (queue-definition lookup, command validation, response assembly) on a snorri-shaped workload.
//!
//! Same shape as `sqlite_commit_snorri_shaped_ladder_probe` (19 typed indexes, ~2.3 KB payload,
//! finalize + lifecycle-push), but driven by concurrent workers committing against ONE shared
//! queue instead of one sequential loop, so the store mutex actually contends the way snorri's
//! real w=1/4/8 worker ladder does. `open_sqlite` (log=sqlite × projection=memory) only — the
//! product cell snorri's 10k-tps campaign measures.
//!
//! Evidence: docs/perf/evidence/tp005/commit-section-contention-20260811.md

#![cfg(feature = "sqlite")]
#![allow(dead_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use axon_esf::IndexDef;
use fireweed::*;
use fireweed_core::{IndexDeclaration, IndexType, QueueIndex};
use fireweed_memory::ManualClock;
use serde_json::{Value as JsonValue, json};

const N_INDEXES: usize = 19;
const PAYLOAD_BYTES: usize = 2300;
/// Entries committed per worker weight (matches snorri's claim-batch=500 rung shape). Divisible by
/// every worker count below and by `CLAIM_BATCH` so each weight commits the identical total.
const TOTAL_ENTRIES: usize = 8000;
const CLAIM_BATCH: usize = 500;

fn qdef_snorri_shaped() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t-commit-contention").unwrap(),
        queue_id: QueueId::new("q-commit-contention").unwrap(),
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
        "fireweed-commit-contention-{tag}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    path.to_string_lossy().into_owned()
}

fn entity_for(k: u64) -> JsonValue {
    let mut entity = serde_json::Map::new();
    entity.insert("f0".into(), json!(format!("k-{k}")));
    for i in 1..N_INDEXES {
        entity.insert(format!("f{i}"), json!(format!("v{i}-{k}")));
    }
    JsonValue::Object(entity)
}

/// One worker's slice: claim `iterations` batches of `CLAIM_BATCH` and finalize+lifecycle-push
/// each, using a key namespace disjoint from every other worker/run (unique `f0` index).
async fn worker_loop(
    fw: Arc<Fireweed>,
    queue: QueueKey,
    payload: bytes::Bytes,
    iterations: usize,
    key_base: u64,
) -> usize {
    let mut committed = 0usize;
    let mut next_key = key_base;
    for _ in 0..iterations {
        let claimed = fw.claim(&queue, CLAIM_BATCH, 30_000).await.expect("claim");
        assert_eq!(claimed.len(), CLAIM_BATCH, "claim batch size");
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
        for o in outcomes {
            if let EntryOutcome::Rejected(e) = o {
                panic!("rejected: {e}");
            }
        }
        committed += CLAIM_BATCH;
    }
    committed
}

/// Wall time, log-axis lock phase, and projection-axis lock phase for one worker-count weight.
struct WeightResult {
    workers: usize,
    wall: Duration,
    log_stats: fireweed_engine::LockPhaseSnapshot,
    projection_stats: fireweed_engine::LockPhaseSnapshot,
}

impl WeightResult {
    fn ms_per_entry(&self) -> f64 {
        self.wall.as_secs_f64() * 1000.0 / TOTAL_ENTRIES as f64
    }
    fn log_hold_ms_per_entry(&self) -> f64 {
        self.log_stats.hold.as_secs_f64() * 1000.0 / TOTAL_ENTRIES as f64
    }
    fn log_wait_ms_per_entry(&self) -> f64 {
        self.log_stats.wait.as_secs_f64() * 1000.0 / TOTAL_ENTRIES as f64
    }
    fn projection_hold_ms_per_entry(&self) -> f64 {
        self.projection_stats.hold.as_secs_f64() * 1000.0 / TOTAL_ENTRIES as f64
    }
    fn projection_wait_ms_per_entry(&self) -> f64 {
        self.projection_stats.wait.as_secs_f64() * 1000.0 / TOTAL_ENTRIES as f64
    }
    fn off_lock_ms_per_entry(&self) -> f64 {
        (self.ms_per_entry()
            - self.log_hold_ms_per_entry()
            - self.log_wait_ms_per_entry()
            - self.projection_hold_ms_per_entry()
            - self.projection_wait_ms_per_entry())
        .max(0.0)
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
    // Seed one full round of claimable items per worker up front; each commit's lifecycle_items
    // replenishes one new item per finalized item, so supply never runs dry mid-run.
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
            .expect("seed push");
    }

    backend.reset_lock_phase_stats();
    let start = Instant::now();
    let mut tasks = Vec::with_capacity(workers);
    for w in 0..workers {
        let fw = Arc::clone(&fw);
        let queue = queue.clone();
        let payload = payload.clone();
        let key_base = (w as u64) * 10_000_000;
        tasks.push(tokio::spawn(worker_loop(
            fw,
            queue,
            payload,
            iterations_per_worker,
            key_base,
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
        log_stats: backend.log_lock_phase_stats(),
        projection_stats: backend.projection_lock_phase_stats(),
    };
    let _ = std::fs::remove_file(&path);
    result
}

/// fireweed-77ae7a87: print the hold/wait/off-lock decomposition across w=1/4/8 on the
/// snorri-shaped `open_sqlite` product cell. Evidence-gathering probe, not a regression gate —
/// absolute contention numbers are host core-count dependent. The only asserted invariant is
/// functional (every commit accepted, entry counts add up); `WeightResult` prints the full
/// breakdown for `docs/perf/evidence/tp005/commit-section-contention-20260811.md`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sqlite_commit_section_contention_ladder_probe() {
    eprintln!("=== commit-section contention (open_sqlite, 19 indexes, ~2.3KB payload) ===");
    eprintln!(
        "workers\twall_ms/entry\tlog_hold_ms/entry\tlog_wait_ms/entry\tproj_hold_ms/entry\tproj_wait_ms/entry\toff_lock_ms/entry"
    );
    let mut results = Vec::new();
    for workers in [1usize, 4, 8] {
        let r = measure_weight(workers, "contention").await;
        eprintln!(
            "{}\t{:.4}\t{:.4}\t{:.4}\t{:.4}\t{:.4}\t{:.4}",
            r.workers,
            r.ms_per_entry(),
            r.log_hold_ms_per_entry(),
            r.log_wait_ms_per_entry(),
            r.projection_hold_ms_per_entry(),
            r.projection_wait_ms_per_entry(),
            r.off_lock_ms_per_entry(),
        );
        results.push(r);
    }
    let w1 = &results[0];
    let w8 = &results[2];
    eprintln!(
        "w=1 ms/entry={:.4} w=8 ms/entry={:.4} (w=8 log wait grew {:.2}x over w=1)",
        w1.ms_per_entry(),
        w8.ms_per_entry(),
        w8.log_wait_ms_per_entry() / w1.log_wait_ms_per_entry().max(1e-6),
    );
}
