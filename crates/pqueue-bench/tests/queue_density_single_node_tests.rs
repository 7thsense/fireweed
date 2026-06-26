//! TP-002 **E2 — queue density** evidence (ADR-008: the queue is the unit of sharding; a single node hosts
//! MANY queues, and a hot queue reaches its floor while the rest stay active on the SAME node).
//!
//! Two phases on ONE `MemoryBackend` node — both measured, never hard-coded:
//!
//! PHASE 1 — RESIDENCY LADDER (single-threaded). Stand up a growing population of co-resident queues
//! (0 → 100 → 1000+), each seeded with a pending set that is NEVER drained, and at each level drive a fresh
//! HOT queue through push + claim + ack and measure its throughput. This proves the bars observable WITHOUT
//! concurrency:
//!   (1) >=1000 queues concurrently RESIDENT on one node — verified via per-queue `metrics()`, every cold
//!       queue still holding its full pending set both before AND after the hot run;
//!   (2) the hot-path per-operation cost does NOT grow with the resident queue count — the hot throughput is
//!       flat from 0 to 1000 co-resident queues (per-queue ownership: the hot path is keyed to ONE queue, an
//!       O(1) lookup; a design with an O(total_queues) per-op scan would visibly fall off here even
//!       single-threaded). This is an ALGORITHMIC-cost check, NOT a concurrency/noisy-neighbor claim;
//!   (3) CORRECTNESS isolation — the hot workload corrupts/disturbs no neighbor's data (every cold queue's
//!       pending set is intact and unleased afterwards);
//!   (4) the hot queue clears the per-queue E0 floor (10M items/hr == 2777.78 items/s) with the population
//!       resident.
//!
//! PHASE 2 — CONCURRENT NOISY-NEIGHBOR ISOLATION (real threads, FR-43). A BOUNDED worker pool concurrently
//! drives the cold queues (continuous push+claim+ack) on the SAME shared node WHILE the hot queue runs, so
//! the node's shared state (the single `Mutex<State>`) is genuinely CONTENDED. We assert the hot queue STILL
//! clears the E0 floor under that concurrent load — the real "any single queue reaches the floor while the
//! others stay active" bar (TP-002 §E2). A bounded pool (not one thread per queue) is itself the faithful
//! shape: a real node serves 1000 queues with a bounded pool, never 1000 loops.
//!
//! WHAT THIS DOES NOT MEASURE (honestly deferred — NOT claimed here):
//!   - The in-memory backend serializes all queues behind ONE global `Mutex<State>`; it is used here for an
//!     in-process measurement, but it is NOT the production density substrate. Bar (d) — per-queue background
//!     work (lease-expiry sweeps, progress-bound aggregation, summary recompute, recurring rearm, retention
//!     GC) multiplexed onto BOUNDED shared per-node pools, never one loop/connection per queue — is a
//!     pqueue-SERVER RUNTIME property (the library facade runs NO background loops at all). It and the
//!     DURABLE-backend (object_log_sqlite_projection / postgres) density point and "every active queue meeting
//!     its progress bound under a live sweeper" are the server-runtime + live-cluster run's job (deferred to
//!     bead pqueue-c33c367e and the live run). Aggregate single-node throughput is REPORTED, not required to
//!     be 1000x the per-queue floor (TP-002 §E2: multi-node provides aggregate headroom).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use pqueue::{NewItem, Pqueue};
use pqueue_core::{
    EligibilityPolicy, ItemId, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy,
    TenantId, UtcTimestamp,
};
use pqueue_engine::{Clock, QueueKey};
use pqueue_memory::MemoryBackend;

/// The E0 per-queue throughput floor (TP-002): 10,000,000 accepted items/hr == 2,777.78 items/s.
const FLOOR_ITEMS_PER_SEC: f64 = 10_000_000.0 / 3600.0;

/// Items a Phase-2 noisy worker pushes+drains per cycle.
const NOISY_BATCH: u64 = 200;

struct SysClock;
impl Clock for SysClock {
    fn now(&self) -> UtcTimestamp {
        let d = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        UtcTimestamp::new(d.as_secs() as i64, d.subsec_nanos()).expect("valid unix ts")
    }
}

fn qdef(tenant: &str, queue: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new(tenant).unwrap(),
        queue_id: QueueId::new(queue).unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        max_lease_duration_ms: 3_600_000,
        retry_policy: RetryPolicy {
            max_attempts: 1_000_000,
        },
        max_push_batch_size: 10_000_000,
        max_claim_batch_size: 10_000_000,
        max_eligible_group_size: None,
    }
}

fn qk(tenant: &str, queue: &str) -> QueueKey {
    QueueKey::new(TenantId::new(tenant).unwrap(), QueueId::new(queue).unwrap())
}

async fn seed<B: pqueue::LibBackend>(pq: &Pqueue<B>, key: &QueueKey, items: u64, batch: usize) {
    let mut pushed = 0u64;
    while pushed < items {
        let n = (items - pushed).min(batch as u64) as usize;
        let batch_items: Vec<NewItem> = (0..n)
            .map(|k| NewItem {
                priority: Some(PriorityValue::Int64(((pushed + k as u64) % 1000) as i64)),
                ..Default::default()
            })
            .collect();
        pq.push_batch(key, batch_items).await.unwrap();
        pushed += n as u64;
    }
}

/// Drive one queue through a full push + claim + ack of `items`, returning (push_rate, claim_rate) in items/s.
async fn run_hot<B: pqueue::LibBackend>(
    pq: &Pqueue<B>,
    key: &QueueKey,
    items: u64,
    batch: usize,
) -> (f64, f64) {
    let t_push = Instant::now();
    seed(pq, key, items, batch).await;
    let push_rate = items as f64 / t_push.elapsed().as_secs_f64();

    let t_claim = Instant::now();
    let mut drained = 0u64;
    while drained < items {
        let claimed = pq.claim(key, batch, 3_600_000).await.unwrap();
        if claimed.is_empty() {
            break;
        }
        let ids: Vec<ItemId> = claimed.iter().map(|c| c.item_id.clone()).collect();
        drained += ids.len() as u64;
        pq.ack(key, ids).await.unwrap();
    }
    assert_eq!(drained, items, "the hot queue must fully drain");
    let claim_rate = items as f64 / t_claim.elapsed().as_secs_f64();
    (push_rate, claim_rate)
}

// ---------------------------------------------------------------------------
// PHASE 1 — residency ladder (single-threaded)
// ---------------------------------------------------------------------------

struct ResidencyPoint {
    density: usize,
    hot_push_rate: f64,
    hot_claim_rate: f64,
    cold_resident_after: usize,
}

/// On a FRESH single node: create `density` cold queues each seeded with `cold_each` pending items (left
/// resident, never drained), then drive one hot queue and measure it. Verify every cold queue is still fully
/// resident afterwards (undisturbed by the hot workload — a correctness-isolation check).
fn measure_residency(
    density: usize,
    cold_each: u64,
    hot_items: u64,
    batch: usize,
) -> ResidencyPoint {
    let pq = Pqueue::new(Arc::new(MemoryBackend::new()), Arc::new(SysClock));
    futures::executor::block_on(async {
        for i in 0..density {
            let key = qk("density", &format!("cold{i}"));
            pq.create_queue(qdef("density", &format!("cold{i}")))
                .await
                .unwrap();
            seed(&pq, &key, cold_each, batch).await;
        }
        let hot = qk("density", "hot");
        pq.create_queue(qdef("density", "hot")).await.unwrap();
        let (hot_push_rate, hot_claim_rate) = run_hot(&pq, &hot, hot_items, batch).await;

        let mut cold_resident_after = 0usize;
        for i in 0..density {
            let m = pq
                .metrics(&qk("density", &format!("cold{i}")))
                .await
                .unwrap();
            if m.pending == cold_each && m.leased == 0 {
                cold_resident_after += 1;
            }
        }
        ResidencyPoint {
            density,
            hot_push_rate,
            hot_claim_rate,
            cold_resident_after,
        }
    })
}

// ---------------------------------------------------------------------------
// PHASE 2 — concurrent noisy-neighbor isolation (real threads, shared node)
// ---------------------------------------------------------------------------

/// Stand up `cold` queues on ONE shared node, start a BOUNDED pool of `workers` threads continuously driving
/// those cold queues (real concurrent push+claim+ack — contending the node's shared `Mutex<State>`), then
/// measure the hot queue's throughput UNDER that concurrent load. Returns (hot_push_rate, hot_claim_rate,
/// total_noisy_ops).
fn measure_hot_under_concurrent_load(
    cold: usize,
    workers: usize,
    hot_items: u64,
    batch: usize,
) -> (f64, f64, u64) {
    let pq = Arc::new(Pqueue::new(
        Arc::new(MemoryBackend::new()),
        Arc::new(SysClock),
    ));
    futures::executor::block_on(async {
        for i in 0..cold {
            pq.create_queue(qdef("noisy", &format!("c{i}")))
                .await
                .unwrap();
        }
        pq.create_queue(qdef("noisy", "hot")).await.unwrap();
    });

    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(workers + 1));
    let handles: Vec<_> = (0..workers)
        .map(|w| {
            let pq = Arc::clone(&pq);
            let stop = Arc::clone(&stop);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let my_qs: Vec<QueueKey> = (w..cold)
                    .step_by(workers)
                    .map(|i| qk("noisy", &format!("c{i}")))
                    .collect();
                let mut ops = 0u64;
                futures::executor::block_on(async {
                    barrier.wait(); // all noisy workers + the hot driver start together
                    let mut idx = 0usize;
                    while !stop.load(Ordering::Relaxed) {
                        let q = &my_qs[idx % my_qs.len()];
                        idx += 1;
                        // Push a small batch then fully drain it: keeps the cold queue genuinely active
                        // (real concurrent work) and its resident set bounded while contending the shared node.
                        seed(&pq, q, NOISY_BATCH, NOISY_BATCH as usize).await;
                        let mut d = 0u64;
                        while d < NOISY_BATCH {
                            let c = pq.claim(q, NOISY_BATCH as usize, 3_600_000).await.unwrap();
                            if c.is_empty() {
                                break;
                            }
                            let ids: Vec<ItemId> = c.iter().map(|x| x.item_id.clone()).collect();
                            d += ids.len() as u64;
                            pq.ack(q, ids).await.unwrap();
                        }
                        ops += NOISY_BATCH;
                    }
                });
                ops
            })
        })
        .collect();

    // Hot driver: wait for the noisy pool to be running, then measure the hot queue under concurrent load.
    barrier.wait();
    let hot = qk("noisy", "hot");
    let (push_rate, claim_rate) = futures::executor::block_on(run_hot(&pq, &hot, hot_items, batch));
    stop.store(true, Ordering::Relaxed);
    let noisy_ops: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    (push_rate, claim_rate, noisy_ops)
}

#[test]
fn queue_density_single_node_tests() {
    let cold_each = 50u64;
    let hot_items = 60_000u64;
    let batch = 10_000usize;
    let densities = [0usize, 100, 1000];

    // ---- PHASE 1: residency ladder ----
    println!("\nTP-002 E2 queue density — PHASE 1 residency ladder (single node, in-memory):");
    println!("  co-resident queues | hot push items/s | hot claim items/s | cold still resident");
    let mut points = Vec::new();
    for &d in &densities {
        let p = measure_residency(d, cold_each, hot_items, batch);
        println!(
            "  {:>18} | {:>16.0} | {:>17.0} | {:>6} / {}",
            p.density, p.hot_push_rate, p.hot_claim_rate, p.cold_resident_after, p.density
        );
        points.push(p);
    }
    let at = |d: usize| points.iter().find(|p| p.density == d).unwrap();
    let top = at(1000);

    // (1) >=1000 queues concurrently RESIDENT on one node, every one verified intact after the hot run.
    assert!(
        top.density >= 1000,
        "ran {} co-resident queues",
        top.density
    );
    assert_eq!(
        top.cold_resident_after, top.density,
        "all {} cold queues must remain fully resident and undisturbed (correctness isolation); only {} were",
        top.density, top.cold_resident_after
    );

    // (4) hot queue clears the E0 floor at every density level with the population resident.
    for p in &points {
        assert!(
            p.hot_push_rate >= FLOOR_ITEMS_PER_SEC && p.hot_claim_rate >= FLOOR_ITEMS_PER_SEC,
            "hot queue must hold the E0 floor (>= {FLOOR_ITEMS_PER_SEC:.0}/s) at {} co-resident queues: push={:.0}/s claim={:.0}/s",
            p.density,
            p.hot_push_rate,
            p.hot_claim_rate
        );
    }

    // (2) the hot-path per-OPERATION cost does not grow with the resident queue count (rules out an
    // O(total_queues) per-op scan): hot throughput at 1000 co-resident queues retains >=70% of the
    // 0-neighbour rate. NOTE: this is an algorithmic-cost check (neighbours are idle here), NOT a
    // concurrency/noisy-neighbour claim — that is Phase 2.
    let base = at(0);
    let push_keep = top.hot_push_rate / base.hot_push_rate;
    let claim_keep = top.hot_claim_rate / base.hot_claim_rate;
    println!(
        "  per-op cost flat across density: push retains {:.0}%, claim retains {:.0}% of the 0-neighbour rate at {} resident queues",
        push_keep * 100.0,
        claim_keep * 100.0,
        top.density
    );
    assert!(
        push_keep >= 0.70 && claim_keep >= 0.70,
        "hot-path per-op cost appears to scale with resident queue count: push {:.0}% claim {:.0}% of baseline at {} queues",
        push_keep * 100.0,
        claim_keep * 100.0,
        top.density
    );

    // ---- PHASE 2: concurrent noisy-neighbor isolation ----
    let cores = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let workers = (cores.saturating_sub(1)).clamp(1, 8);
    let (hot_push_load, hot_claim_load, noisy_ops) =
        measure_hot_under_concurrent_load(1000, workers, hot_items, batch);
    println!(
        "\nTP-002 E2 queue density — PHASE 2 concurrent noisy-neighbor ({workers} worker threads cycling 1000 queues on the SAME node):"
    );
    println!(
        "  hot UNDER LOAD: push {hot_push_load:.0}/s, claim {hot_claim_load:.0}/s  ({} noisy ops driven concurrently)",
        noisy_ops
    );
    println!(
        "  vs unloaded baseline push {:.0}/s claim {:.0}/s  -> hot retained {:.0}%/{:.0}% under concurrent contention",
        base.hot_push_rate,
        base.hot_claim_rate,
        hot_push_load / base.hot_push_rate * 100.0,
        hot_claim_load / base.hot_claim_rate * 100.0
    );

    // Liveness: every worker must have driven at least one full cycle, so the contention was real and the
    // floor result below is genuinely "under load" (a stalled/no-op pool cannot make this pass vacuously).
    assert!(
        noisy_ops >= workers as u64 * NOISY_BATCH,
        "the noisy-neighbor pool must have driven real concurrent work: only {noisy_ops} ops across {workers} workers"
    );

    // The genuine FR-43 bar: under REAL concurrent noisy-neighbor load contending the shared node, the hot
    // queue STILL clears the per-queue E0 floor.
    assert!(
        hot_push_load >= FLOOR_ITEMS_PER_SEC && hot_claim_load >= FLOOR_ITEMS_PER_SEC,
        "under concurrent noisy-neighbor load the hot queue must STILL hold the E0 floor (>= {FLOOR_ITEMS_PER_SEC:.0}/s): push={hot_push_load:.0}/s claim={hot_claim_load:.0}/s"
    );

    // Relative tripwire (complements the absolute floor): contention must not collapse the hot queue's
    // throughput. Real lock contention here retains ~50-80% of the unloaded baseline; a hot-path pathology
    // that scales with the queue population (an O(total_queues) scan, a fully-serializing global section)
    // would crater this far below 20%. The 20% bar is conservative — generous to normal contention/hardware
    // variance, but a genuine density regression would still breach it.
    let push_keep_load = hot_push_load / base.hot_push_rate;
    let claim_keep_load = hot_claim_load / base.hot_claim_rate;
    assert!(
        push_keep_load >= 0.20 && claim_keep_load >= 0.20,
        "concurrent contention collapsed the hot queue (possible per-population hot-path cost): push retained {:.0}%, claim retained {:.0}% of baseline under load",
        push_keep_load * 100.0,
        claim_keep_load * 100.0
    );
}
