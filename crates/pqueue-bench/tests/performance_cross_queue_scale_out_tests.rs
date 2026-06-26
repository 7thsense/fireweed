//! TP-002 **E2 — cross-queue scale-out** evidence (ADR-008: the queue is the unit of sharding; horizontal
//! scale comes from distributing queues across INDEPENDENT owner nodes, NOT from intra-queue sharding).
//!
//! WHAT THIS MEASURES (real, in-process): each "owner node" is an INDEPENDENT backend instance owning a
//! disjoint set of queues (no shared lock / no shared state — exactly the ADR-008 ownership model). We run
//! a fixed-per-owner push+claim+ack workload concurrently across a growing number of owners (1/2/4/8) on
//! real OS threads and MEASURE the aggregate throughput (items / wall-clock) plus the worst single queue's
//! throughput. Because owners share nothing, adding owners adds throughput up to the machine's core count.
//! From the measured numbers the test asserts the ADR-008 owner-independence property in three load-bearing
//! parts: (1) NO cross-owner contention — aggregate does not regress as owners grow; (2) genuine PARALLEL
//! scale-out — at the largest owner count that does not oversubscribe cores, the aggregate is >=60% of the
//! ideal multiple of the 2-owner baseline (the SHAPE of the spec's "8-owner >= 3.5x 2-owner, ~70%" bar,
//! scaled to the available cores and made conservative for single-node noise); (3) the per-queue E0 floor
//! held by the WORST single queue (not an average). Every number here is measured, never hard-coded.
//!
//! WHAT THIS DOES NOT MEASURE (honestly deferred — this is NOT the E2 headline evidence): TP-002 §E2's
//! HEADLINE requires the `object_log_sqlite_projection` backend (TD-004) across REAL multi-NODE
//! network-distributed owners, with the published >=3.5x-at-8-owners multiple at ~70% cross-node efficiency.
//! That needs a live multi-node cluster on the durable object-log backend and is NOT run here. This test
//! uses the in-memory backend on ONE node, so it substantiates only the ARCHITECTURAL property (owner
//! independence -> no cross-owner contention -> scaling); the cross-node network-efficiency multiple is the
//! live-cluster release-evidence run's job (tracked separately — see the BQ-40 follow-ups on BQ-42/BQ-43).
//! The in-memory single-node 8/2 aggregate ratio is PRINTED for context but deliberately NOT asserted as the
//! >=3.5x headline, and must not be cited as cross-node E2 evidence.

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

/// Run ONE owner node's full workload (push then claim+ack `items_per_queue` across `queues_per_owner`
/// queues) on a fresh INDEPENDENT in-memory backend. Returns the per-queue throughput (items/s) of EACH
/// queue this owner drove, timed INDIVIDUALLY (each queue's own wall) so a single starved queue is visible
/// — not hidden behind an owner-level average. No shared state with any other owner.
fn run_owner(
    owner_idx: usize,
    queues_per_owner: usize,
    items_per_queue: u64,
    batch: usize,
) -> Vec<f64> {
    let pq = Pqueue::new(Arc::new(MemoryBackend::new()), Arc::new(SysClock));
    futures::executor::block_on(async {
        let mut per_queue_rates = Vec::with_capacity(queues_per_owner);
        for qi in 0..queues_per_owner {
            let tenant = format!("o{owner_idx}");
            let qname = format!("q{qi}");
            let qk = QueueKey::new(
                TenantId::new(&tenant).unwrap(),
                QueueId::new(&qname).unwrap(),
            );
            pq.create_queue(qdef(&tenant, &qname)).await.unwrap();
            let q_start = Instant::now();
            // Push.
            let mut pushed = 0u64;
            while pushed < items_per_queue {
                let n = (items_per_queue - pushed).min(batch as u64) as usize;
                let items: Vec<NewItem> = (0..n)
                    .map(|k| NewItem {
                        priority: Some(PriorityValue::Int64(((pushed + k as u64) % 1000) as i64)),
                        ..Default::default()
                    })
                    .collect();
                pq.push_batch(&qk, items).await.unwrap();
                pushed += n as u64;
            }
            // Claim + ack (drain).
            let mut drained = 0u64;
            while drained < items_per_queue {
                let claimed = pq.claim(&qk, batch, 3_600_000).await.unwrap();
                if claimed.is_empty() {
                    break;
                }
                let ids: Vec<ItemId> = claimed.iter().map(|c| c.item_id.clone()).collect();
                drained += ids.len() as u64;
                pq.ack(&qk, ids).await.unwrap();
            }
            assert_eq!(drained, items_per_queue, "every pushed item must drain");
            per_queue_rates.push(items_per_queue as f64 / q_start.elapsed().as_secs_f64());
        }
        per_queue_rates
    })
}

/// One scale point: the aggregate throughput (items/s) of `owner_count` INDEPENDENT owners running
/// concurrently, and the MINIMUM single-queue throughput observed across every queue of every owner (the
/// worst-case queue — what the per-queue floor must actually clear). A barrier releases all owner threads
/// together so the wall-clock reflects genuine parallel execution.
struct ScalePoint {
    owners: usize,
    aggregate: f64,
    min_per_queue: f64,
}

fn measure(
    owner_count: usize,
    queues_per_owner: usize,
    items_per_queue: u64,
    batch: usize,
) -> ScalePoint {
    let barrier = Arc::new(Barrier::new(owner_count + 1));
    let handles: Vec<_> = (0..owner_count)
        .map(|i| {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait(); // all owners start together
                run_owner(i, queues_per_owner, items_per_queue, batch)
            })
        })
        .collect();
    barrier.wait();
    let start = Instant::now();
    let per_queue_rates: Vec<f64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    let wall = start.elapsed().as_secs_f64();
    let total_items = (owner_count * queues_per_owner) as f64 * items_per_queue as f64;
    let min_per_queue = per_queue_rates
        .iter()
        .copied()
        .fold(f64::INFINITY, f64::min);
    ScalePoint {
        owners: owner_count,
        aggregate: total_items / wall,
        min_per_queue,
    }
}

#[test]
fn performance_cross_queue_scale_out_tests() {
    let cores = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let queues_per_owner = 2;
    // ~120k items/owner: a long-enough window (~1s+ per scale point on an in-memory backend) that the
    // aggregate is not dominated by start-up/scheduling jitter — so the monotonic tolerance below can be
    // tight rather than papering over a noisy short run.
    let items_per_queue = 60_000u64;
    let batch = 10_000usize;

    // Measure at the TP-002 §E2 owner-node counts (2/4/8) plus 1 as the single-owner baseline.
    let counts = [1usize, 2, 4, 8];
    let mut points = Vec::new();
    println!(
        "\nTP-002 E2 cross-queue scale-out (in-process owner independence; {cores} cores available)"
    );
    println!("  owners | aggregate items/s | min per-queue items/s");
    for &n in &counts {
        let p = measure(n, queues_per_owner, items_per_queue, batch);
        println!(
            "  {:>6} | {:>17.0} | {:>21.0}",
            p.owners, p.aggregate, p.min_per_queue
        );
        points.push(p);
    }
    let at = |n: usize| points.iter().find(|p| p.owners == n).unwrap();

    // (1) NO CROSS-OWNER CONTENTION: adding independent owners never MATERIALLY reduces aggregate
    // throughput. Owners share nothing, so each added owner contributes its own work; a contended/shared-lock
    // design would visibly degrade here as owners pile up. (NOT a claim of strict monotonic increase — the
    // spec's strict-increase headline is the multi-node run below; here we only require "does not collapse",
    // a >=0.90 step, which a 10% jitter band absorbs but a real regression would not.)
    for w in counts.windows(2) {
        let (a, b) = (at(w[0]).aggregate, at(w[1]).aggregate);
        assert!(
            b >= a * 0.90,
            "aggregate must not regress as owners grow (no cross-owner contention): {} owners={:.0}/s then {} owners={:.0}/s",
            w[0],
            a,
            w[1],
            b
        );
    }

    // (2) GENUINE PARALLEL SCALE-OUT, in the SHAPE of the spec bar (aggregate vs the 2-owner baseline,
    // efficiency-scaled). The spec headline is 8-owner >= 3.5x the 2-owner aggregate (~70% of the ideal 4x).
    // In-process on one node we can only observe scaling up to the core count, so we assert at the largest
    // owner count that does NOT oversubscribe cores, and require >=60% efficiency (conservative vs the spec's
    // 70%, to absorb single-node scheduling noise). On 1 core there is nothing to scale onto — LOUD-skip.
    let max_unsub = *counts.iter().filter(|&&n| n <= cores).max().unwrap();
    if max_unsub >= 4 {
        let ideal = max_unsub as f64 / 2.0; // ideal multiple of the 2-owner aggregate
        let observed = at(max_unsub).aggregate / at(2).aggregate;
        let bar = ideal * 0.60;
        assert!(
            observed >= bar,
            "independent owners must scale out: {max_unsub} owners = {observed:.2}x the 2-owner aggregate, below the {bar:.2}x bar (60% of ideal {ideal:.1}x; cores={cores})"
        );
        println!(
            "  scale-out: {max_unsub} owners = {observed:.2}x the 2-owner aggregate (>= {bar:.2}x = 60% of ideal {ideal:.1}x; cores={cores})"
        );
    } else if max_unsub == 2 {
        let observed = at(2).aggregate / at(1).aggregate;
        assert!(
            observed >= 1.5,
            "independent owners must scale out: 2 owners = {observed:.2}x the 1-owner aggregate (cores={cores})"
        );
        println!("  scale-out: 2 owners = {observed:.2}x the 1-owner aggregate (cores={cores})");
    } else {
        eprintln!(
            "E2 SCALE-OUT NOT MEASURED — only {cores} core available; parallel owner scaling cannot be observed"
        );
    }

    // (3) PER-QUEUE FLOOR HELD — and held by the WORST queue, not an average. Across every queue of every
    // owner at all owner counts (including 8 owners, where a contended design's noisy-neighbor starvation
    // would surface), the slowest single queue still clears the E0 floor (10M items/hr == 2777.78/s). On the
    // in-memory backend this holds with large headroom; the floor under the DURABLE backends is part of the
    // deferred live run (see the module doc). Using the MIN gives the check teeth a single starved queue
    // would trip.
    let worst = points
        .iter()
        .map(|p| p.min_per_queue)
        .fold(f64::INFINITY, f64::min);
    assert!(
        worst >= FLOOR_ITEMS_PER_SEC,
        "the worst single queue must hold the E0 floor (>= {FLOOR_ITEMS_PER_SEC:.0}/s): measured {worst:.0}/s"
    );

    // (4) A SINGLE QUEUE DOES NOT EXCEED ONE OWNER (TP-002 E2 bar) holds BY CONSTRUCTION: every queue is
    // driven by exactly one owner thread on one backend and is never split, so no queue's throughput can
    // exceed a single owner's. Asserted structurally — the worst (and best) per-queue rate is, trivially, a
    // single owner's single-queue rate.

    // The headline cross-NODE multiple (default bar: 8-owner aggregate >= 3.5x the 2-owner aggregate, ~70%
    // efficiency) is the OBJECT-LOG-BACKEND, REAL-MULTI-NODE live cluster's evidence (TP-002 §E2), NOT this
    // in-process in-memory single-node run. The number below is in-memory/single-node and proves only the
    // architectural property (owner independence); it is NOT the E2 headline and must not be cited as it.
    println!(
        "  in-memory single-node 8/2 aggregate ratio = {:.2}x  (NOT the cross-node E2 headline; that >=3.5x is the deferred live object-log multi-node run)",
        at(8).aggregate / at(2).aggregate
    );
}
