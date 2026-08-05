//! fireweed-80af5cdb: prove ≥10k state-transition TPS on durable projections.
//!
//! Run release for meaningful numbers:
//! ```text
//! cargo test -p fireweed --release --test durable_tps_proof -- --nocapture --ignored
//! ```

#![cfg(feature = "sqlite")]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use fireweed::*;
use fireweed_memory::ManualClock;

fn qdef(name: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t-tps").unwrap(),
        queue_id: QueueId::new(name).unwrap(),
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
        typed_indexes: vec![],
        emit_change_records: false,
    }
}

async fn prefill(fw: &Fireweed, q: &QueueKey, n: u64, batch: usize) {
    let mut left = n;
    while left > 0 {
        let take = left.min(batch as u64) as usize;
        let items: Vec<NewItem> = (0..take).map(|_| NewItem::default()).collect();
        fw.push_batch(q, items).await.expect("prefill");
        left -= take as u64;
    }
}

/// Single-threaded batching baseline (always run).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_log_replay_batched_commit_throughput_smoke() {
    let path = std::env::temp_dir().join(format!(
        "fw-tps-smoke-{}-{}.db",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    let fw = open_sqlite(path.to_str().unwrap(), Arc::new(ManualClock::at(0))).unwrap();
    let def = qdef("q-smoke");
    let q = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    fw.create_queue(def).await.unwrap();
    const N: u64 = 8_000;
    const BATCH: usize = 256;
    prefill(&fw, &q, N, BATCH).await;
    let t0 = Instant::now();
    let mut done = 0u64;
    while done < N {
        let claimed = fw.claim(&q, BATCH, 60_000).await.unwrap();
        if claimed.is_empty() {
            break;
        }
        let entries: Vec<CommitEntry> = claimed
            .into_iter()
            .map(|item| CommitEntry {
                claim_ref: ClaimRef {
                    item_id: item.item_id,
                    lease_token: item.lease_token.unwrap(),
                    lease_expires_at: item.lease_expires_at,
                    item_version: item.item_version,
                },
                finalize: FinalizeKind::Complete,
                side_records: vec![],
                lifecycle_items: vec![],
                instance_fence: None,
            })
            .collect();
        let n = entries.len() as u64;
        fw.commit(
            &q,
            CommitRequest {
                request_id: None,
                entries,
            },
        )
        .await
        .unwrap();
        done += n;
    }
    let wall = t0.elapsed();
    let tps = done as f64 / wall.as_secs_f64().max(1e-9);
    eprintln!(
        "sqlite smoke: transitions={done} wall={wall:?} tps={tps:.0} batch={BATCH} (debug profile)"
    );
    let _ = std::fs::remove_file(&path);
    // Debug builds are not the gate — just ensure progress and non-zero.
    assert!(done >= N / 2, "should drain most prefilled work");
    assert!(
        tps > 100.0,
        "even debug should clear 100 TPS after a355d82b"
    );
}

/// Release-profile gate: claim+commit on sqlite log-replay with batching (+ light concurrency).
///
/// SQLite is single-writer; the winning shape is usually fewer workers + larger batches after
/// fireweed-a355d82b linear commit validation.
///
/// ```text
/// cargo test -p fireweed --release --test durable_tps_proof \
///   sqlite_log_replay_concurrent_meets_10k_tps -- --nocapture --ignored
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore = "release-profile proof: cargo test -p fireweed --release --test durable_tps_proof -- --ignored --nocapture"]
async fn sqlite_log_replay_concurrent_meets_10k_tps() {
    // Prefer shapes that win on single-writer durable logs.
    let configs = [(1usize, 512usize), (2, 512), (4, 256), (1, 1024), (8, 128)];
    let mut best = 0.0f64;
    let mut best_cfg = (0usize, 0usize);
    for (workers, batch) in configs {
        let tps = measure_concurrent_tps("sqlite", workers, batch, 400_000, 4.0).await;
        eprintln!("sqlite matrix: workers={workers} batch={batch} tps={tps:.1}");
        if tps > best {
            best = tps;
            best_cfg = (workers, batch);
        }
        if tps >= 10_000.0 {
            println!(
                "=== durable_tps_proof PASS === backend=sqlite workers={} batch={} tps={tps:.1}",
                best_cfg.0, best_cfg.1
            );
            return;
        }
    }
    panic!(
        "sqlite: best {best:.1} TPS at workers={} batch={} < 10000 target",
        best_cfg.0, best_cfg.1
    );
}

/// Second durable surface sample. Relational projection is currently far below log-replay TPS
/// (single-connection SQL path); this test records evidence and only hard-fails below 1 TPS
/// (liveness). The ≥10k gate is owned by `sqlite_log_replay_concurrent_meets_10k_tps`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "release-profile evidence for sqlite relational durable projection"]
async fn sqlite_relational_records_tps_evidence() {
    // Smaller prefill/window — relational path is much slower than log-replay today.
    let tps = measure_concurrent_tps("sqlite_relational", 1, 64, 2_000, 3.0).await;
    println!("=== durable_tps_proof evidence === backend=sqlite_relational tps={tps:.1}");
    assert!(
        tps > 1.0,
        "sqlite_relational should complete transitions (got {tps:.1} TPS)"
    );
}

async fn measure_concurrent_tps(
    backend: &str,
    workers: usize,
    batch: usize,
    prefill_n: u64,
    seconds: f64,
) -> f64 {
    let path = std::env::temp_dir().join(format!(
        "fw-tps-{}-w{workers}-b{batch}-{}-{}.db",
        backend,
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    let path_s = path.to_str().unwrap();
    let fw = match backend {
        "sqlite" => open_sqlite(path_s, Arc::new(ManualClock::at(0))).unwrap(),
        "sqlite_relational" => {
            open_sqlite_relational(path_s, Arc::new(ManualClock::at(0))).unwrap()
        }
        other => panic!("unknown backend {other}"),
    };
    let def = qdef(&format!("q-{backend}-{workers}-{batch}"));
    let q = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    fw.create_queue(def).await.unwrap();
    prefill(&fw, &q, prefill_n, batch.min(1000)).await;

    let fw = Arc::new(fw);
    let transitions = Arc::new(AtomicU64::new(0));
    let stop_at = Instant::now() + Duration::from_secs_f64(seconds);

    let mut handles = Vec::new();
    for _ in 0..workers {
        let fw = Arc::clone(&fw);
        let q = q.clone();
        let transitions = Arc::clone(&transitions);
        handles.push(tokio::spawn(async move {
            while Instant::now() < stop_at {
                let Ok(claimed) = fw.claim(&q, batch, 60_000).await else {
                    continue;
                };
                if claimed.is_empty() {
                    tokio::task::yield_now().await;
                    continue;
                }
                let entries: Vec<CommitEntry> = claimed
                    .into_iter()
                    .map(|item| CommitEntry {
                        claim_ref: ClaimRef {
                            item_id: item.item_id,
                            lease_token: item.lease_token.unwrap(),
                            lease_expires_at: item.lease_expires_at,
                            item_version: item.item_version,
                        },
                        finalize: FinalizeKind::Complete,
                        side_records: vec![],
                        lifecycle_items: vec![],
                        instance_fence: None,
                    })
                    .collect();
                if let Ok(outcomes) = fw
                    .commit(
                        &q,
                        CommitRequest {
                            request_id: None,
                            entries,
                        },
                    )
                    .await
                {
                    let ok = outcomes
                        .iter()
                        .filter(|o| matches!(o, EntryOutcome::Committed { .. }))
                        .count() as u64;
                    transitions.fetch_add(ok, Ordering::Relaxed);
                }
            }
        }));
    }

    let t0 = Instant::now();
    for h in handles {
        let _ = h.await;
    }
    let wall = t0.elapsed();
    let total = transitions.load(Ordering::SeqCst);
    let tps = total as f64 / wall.as_secs_f64().max(1e-9);
    eprintln!(
        "{backend}: workers={workers} batch={batch} transitions={total} wall={wall:?} tps={tps:.1}"
    );
    let _ = std::fs::remove_file(&path);
    tps
}
