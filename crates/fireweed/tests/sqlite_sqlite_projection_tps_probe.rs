//! In-tree multi-worker durable tps on **sqlite log × sqlite projection** (snorri-shaped).
//!
//! Same worker shape as `sqlite_multi_worker_tps_probe` (`claim_finalize_push_cycle`, 19
//! typed indexes, ~2.3 KB payload, batch 500) but apply hits the relational
//! [`SqliteProjectionStore`] instead of `InMemoryProjection`.
//!
//! **Goal:** ~13k durable tps (same as sqlite × memory on this shape).
//!
//! ```text
//! cargo test -p fireweed --test sqlite_sqlite_projection_tps_probe --release --features sqlite \
//!   -- --nocapture
//! ```

#![cfg(feature = "sqlite")]
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axon_esf::IndexDef;
use fireweed::*;
use fireweed_core::{IndexDeclaration, IndexType, QueueIndex, TypedValue, UtcTimestamp};
use fireweed_engine::PushSpec;
use fireweed_memory::ManualClock;

const N_INDEXES: usize = 19;
const PAYLOAD_BYTES: usize = 2300;
const TOTAL_ENTRIES: usize = 8000;
const CLAIM_BATCH: usize = 500;

fn qdef_snorri_shaped() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t-sqlite-proj-tps").unwrap(),
        queue_id: QueueId::new("q-sqlite-proj-tps").unwrap(),
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

fn tmp_pair(tag: &str) -> (String, String) {
    let stamp = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let log = std::env::temp_dir().join(format!("fireweed-sqlite-proj-tps-{tag}-{stamp}-log.db"));
    let proj = std::env::temp_dir().join(format!("fireweed-sqlite-proj-tps-{tag}-{stamp}-proj.db"));
    let _ = std::fs::remove_file(&log);
    let _ = std::fs::remove_file(&proj);
    (
        log.to_string_lossy().into_owned(),
        proj.to_string_lossy().into_owned(),
    )
}

fn index_fields_for(k: u64) -> BTreeMap<String, TypedValue> {
    let mut fields = BTreeMap::new();
    fields.insert("f0".into(), TypedValue::String(format!("k-{k}")));
    for i in 1..N_INDEXES {
        fields.insert(format!("f{i}"), TypedValue::String(format!("v{i}-{k}")));
    }
    fields
}

async fn worker_loop(
    backend: Arc<
        fireweed_engine::AsyncLogReplayBackend<
            fireweed_sqlite::SqliteLog,
            fireweed_sqlite::SqliteProjectionStore,
        >,
    >,
    queue: QueueKey,
    payload: bytes::Bytes,
    iterations: usize,
    key_base: u64,
) -> usize {
    let mut committed = 0usize;
    let mut next_key = key_base;
    let now = UtcTimestamp::new(0, 0).unwrap();
    for _ in 0..iterations {
        let lifecycle: Vec<PushSpec> = (0..CLAIM_BATCH)
            .map(|_| {
                let k = next_key;
                next_key += 1;
                PushSpec {
                    index_fields: index_fields_for(k),
                    payload: Some(payload.clone()),
                    ..Default::default()
                }
            })
            .collect();
        let n = backend
            .claim_finalize_push_cycle(queue.clone(), 30_000, now, None, lifecycle)
            .await
            .expect("claim_finalize_push_cycle");
        assert_eq!(n, CLAIM_BATCH, "cycle batch size");
        committed += n;
    }
    committed
}

struct WeightResult {
    workers: usize,
    wall: Duration,
    committed: usize,
}

impl WeightResult {
    fn ms_per_entry(&self) -> f64 {
        self.wall.as_secs_f64() * 1000.0 / self.committed as f64
    }
    fn durable_tps(&self) -> f64 {
        self.committed as f64 / self.wall.as_secs_f64().max(1e-9)
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

    let (log, proj) = tmp_pair(&format!("{run_tag}-w{workers}"));
    let clock = Arc::new(ManualClock::at(0));
    let (fw, backend) =
        open_sqlite_sqlite_projection_with_handle(&log, &proj, clock).expect("open sqlite×sqlite");
    let def = qdef_snorri_shaped();
    let queue = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    fw.create_queue(def).await.expect("create queue");

    let payload = bytes::Bytes::from(vec![b'x'; PAYLOAD_BYTES]);
    let seed_total = CLAIM_BATCH * workers;
    let seed: Vec<NewItem> = (0..seed_total)
        .map(|i| NewItem {
            index_fields: index_fields_for(u64::MAX - i as u64),
            payload: Some(payload.clone()),
            ..Default::default()
        })
        .collect();
    for chunk in seed.chunks(500) {
        fw.push_batch(&queue, chunk.to_vec())
            .await
            .expect("seed push");
    }

    let start = Instant::now();
    let mut tasks = Vec::with_capacity(workers);
    for w in 0..workers {
        let backend = Arc::clone(&backend);
        let queue = queue.clone();
        let payload = payload.clone();
        let key_base = (w as u64) * 10_000_000;
        tasks.push(tokio::spawn(worker_loop(
            backend,
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

    let _ = std::fs::remove_file(&log);
    let _ = std::fs::remove_file(&proj);
    WeightResult {
        workers,
        wall,
        committed: total_committed,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sqlite_sqlite_projection_durable_tps_ladder() {
    eprintln!(
        "=== multi-worker durable tps (open_sqlite_sqlite_projection, snorri-shaped, 19 idx, ~2.3KB) ==="
    );
    eprintln!("workers\tcommitted\twall_s\tms/entry\tdurable_tps");
    let mut results = Vec::new();
    for workers in [1usize, 4, 8] {
        let mut best: Option<WeightResult> = None;
        for run in 0..2 {
            let r = measure_weight(workers, &format!("tps-r{run}")).await;
            best = Some(match best {
                None => r,
                Some(prev) if r.durable_tps() > prev.durable_tps() => r,
                Some(prev) => prev,
            });
        }
        let r = best.expect("measured");
        eprintln!(
            "{}\t{}\t{:.3}\t{:.4}\t{:.0}",
            r.workers,
            r.committed,
            r.wall.as_secs_f64(),
            r.ms_per_entry(),
            r.durable_tps(),
        );
        results.push(r);
    }
    let w1 = &results[0];
    eprintln!(
        "SCOREBOARD sqlite×sqlite w1_tps={:.0} w4_tps={:.0} w8_tps={:.0}",
        w1.durable_tps(),
        results[1].durable_tps(),
        results[2].durable_tps(),
    );
    assert!(w1.durable_tps() > 50.0, "w=1 should clear 50 tps");
    assert!(results[2].committed == TOTAL_ENTRIES);
}
