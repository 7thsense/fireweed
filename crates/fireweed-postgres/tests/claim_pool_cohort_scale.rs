//! fireweed-01d7cf09 — root-cause + repro for snorri's reported negative claim-pool scaling
//! (w4/pool4 measured 2.1x SLOWER than w1 on snorri's cohort-shaped workload, even though
//! `claim_pool_scale.rs` shows the item-level claim path scaling 2.56x under the same pool).
//!
//! ROOT CAUSE (read from `relational.rs`): the claim-pool concurrency win landed for
//! `ClaimUnit::Item` only (`claim_item_level_in_tx`: lease under `SKIP LOCKED` first, CAS-allocate
//! `next_seq` after — no long-held cursor lock). Group/cohort claims (`ClaimUnit::WholeGroup` /
//! `SameGroupKey` / `WholeCohort`, selected whenever `ClaimCompatibility::whole_cohort` or similar is
//! set — see `claim_with_client_unit` in `relational.rs`) still open the transaction with
//! `SELECT assignment_epoch FROM relational_cursor ... FOR UPDATE` and hold that row lock across
//! summary promotion, candidate selection, the durable command append, and `apply_command_sql` —
//! i.e. the WHOLE transaction. That is an intentional per-queue mutation/promotion fence (see
//! `promote_due_group_summary_chunk_in_tx`'s comment), not a bug in itself, but it means concurrent
//! cohort claimers on ONE queue fully serialize regardless of `claim_pool_size`. Extra pooled
//! connections/workers then only add overhead on top of that serial section: each worker still spins
//! in `acquire_claim_client` for a free pool slot and then blocks *holding that slot* while Postgres
//! queues it behind the row lock, so more workers/pool slots means more lock-wait-queue and
//! connection-acquisition overhead with zero added parallelism — the observed monotonic slowdown.
//!
//! Snorri's workload ("three-cohort-split-derived") claims `whole_cohort`, so it hits exactly this
//! path; fireweed's own `claim_pool_scale` bench only exercises `ClaimUnit::Item` and never observed
//! it. Test A below reproduces the negative/flat scaling fireweed-side (AC1). Test B demonstrates the
//! configuration that recovers scaling: partition the workload across independent queues (one queue
//! per worker), each with its own `relational_cursor` row and therefore its own lock — exactly the
//! multi-queue posture `relational.rs`'s module doc already names as the production scale-out path
//! (`fixed_postgres_relational_pool`). That configuration clears the same >=1.25x bar
//! `claim_pool_scale.rs` uses for the item-level path (AC2). Snorri re-measures its own end-to-end
//! harness under that same-queue-per-worker posture separately (AC3, external, residual).
//!
//! Env-gated on `FIREWEED_PG_TEST_URL` (fail-closed).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use fireweed_conformance::qdef;
use fireweed_core::{CohortPolicy, GroupKey, LeaseToken, QueueDefinition, QueueId, UtcTimestamp, WorkerId};
use fireweed_engine::{
    ClaimCompatibility, ClaimPort, ClaimRequest, ControlPlaneStore, EngineResult, PushPort,
    PushSpec, QueueKey,
};
use fireweed_postgres::PostgresRelationalBackend;

const COHORT_SIZE: u64 = 4;
const COHORT_COUNT: usize = 120;

fn bo<F: std::future::Future>(f: F) -> F::Output {
    futures::executor::block_on(f)
}

fn fresh_schema(tag: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "claim_pool_cohort_{tag}_{}_{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

fn cohort_qdef(queue_id: &str) -> QueueDefinition {
    QueueDefinition {
        queue_id: QueueId::new(queue_id).unwrap(),
        cohort_policy: Some(CohortPolicy {
            enabled: true,
            completion_bound_ms: Some(60_000),
            on_incomplete: None,
            max_cohort_size: Some(COHORT_SIZE),
        }),
        ..qdef()
    }
}

fn shard_for(queue_id: &str) -> QueueKey {
    QueueKey::new(fireweed_conformance::tenant(), QueueId::new(queue_id).unwrap())
}

fn claim_req(shard: QueueKey, worker: &str, now: i64) -> ClaimRequest {
    ClaimRequest {
        shard,
        max_items: COHORT_SIZE as usize,
        lease_token: LeaseToken::new(format!("lease-{worker}")).unwrap(),
        lease_expires_at: UtcTimestamp::new(now + 120, 0).unwrap(),
        worker_id: WorkerId::new(worker).unwrap(),
        now: UtcTimestamp::new(now, 0).unwrap(),
        expected_epoch: None,
        compatibility: ClaimCompatibility {
            whole_cohort: true,
            ..Default::default()
        },
        eligibility_time: None,
    }
}

/// Seed `count` complete, immediately-eligible cohorts of `COHORT_SIZE` items each on `shard`,
/// numbered from `start` so multiple queues can be seeded with disjoint group keys.
fn seed_cohorts(backend: &PostgresRelationalBackend, queue_id: &str, start: usize, count: usize) {
    let shard = shard_for(queue_id);
    bo(async {
        backend.create_queue(cohort_qdef(queue_id)).await.unwrap();
        for i in start..start + count {
            let specs: Vec<PushSpec> = (0..COHORT_SIZE)
                .map(|_| PushSpec {
                    group_key: Some(GroupKey::new(format!("coh-{queue_id}-{i}")).unwrap()),
                    cohort_size: Some(COHORT_SIZE),
                    ..Default::default()
                })
                .collect();
            backend
                .push(&shard, specs, UtcTimestamp::new(0, 0).unwrap(), None)
                .await
                .unwrap();
        }
    });
}

/// Drain every complete cohort on ONE shard with `workers` concurrent whole-cohort claim loops
/// (same-queue contention — this is what the claim pool cannot help, see the module doc above).
fn drain_one_queue(backend: Arc<PostgresRelationalBackend>, queue_id: &str, workers: usize) -> (usize, u128) {
    let shard = shard_for(queue_id);
    let barrier = Arc::new(Barrier::new(workers));
    let start_gate = Arc::new(Barrier::new(workers + 1));
    let mut handles = Vec::with_capacity(workers);
    for w in 0..workers {
        let backend = Arc::clone(&backend);
        let barrier = Arc::clone(&barrier);
        let start_gate = Arc::clone(&start_gate);
        let shard = shard.clone();
        let name = format!("w{w}");
        handles.push(thread::spawn(move || -> EngineResult<usize> {
            barrier.wait();
            start_gate.wait();
            let mut got = 0usize;
            loop {
                let claimed = bo(backend.claim(claim_req(shard.clone(), &name, 1)))?;
                if claimed.items.is_empty() {
                    break;
                }
                got += claimed.items.len();
            }
            Ok(got)
        }));
    }
    start_gate.wait();
    let t0 = Instant::now();
    let mut total = 0usize;
    for h in handles {
        total += h.join().expect("worker").expect("claim");
    }
    (total, t0.elapsed().as_millis())
}

/// Drain `queue_ids.len()` independent queues concurrently, one worker pinned to each queue — no two
/// workers ever contend on the same `relational_cursor` row.
fn drain_one_queue_per_worker(
    backend: Arc<PostgresRelationalBackend>,
    queue_ids: &[String],
) -> (usize, u128) {
    let workers = queue_ids.len();
    let barrier = Arc::new(Barrier::new(workers));
    let start_gate = Arc::new(Barrier::new(workers + 1));
    let mut handles = Vec::with_capacity(workers);
    for (w, queue_id) in queue_ids.iter().enumerate() {
        let backend = Arc::clone(&backend);
        let barrier = Arc::clone(&barrier);
        let start_gate = Arc::clone(&start_gate);
        let shard = shard_for(queue_id);
        let name = format!("w{w}");
        handles.push(thread::spawn(move || -> EngineResult<usize> {
            barrier.wait();
            start_gate.wait();
            let mut got = 0usize;
            loop {
                let claimed = bo(backend.claim(claim_req(shard.clone(), &name, 1)))?;
                if claimed.items.is_empty() {
                    break;
                }
                got += claimed.items.len();
            }
            Ok(got)
        }));
    }
    start_gate.wait();
    let t0 = Instant::now();
    let mut total = 0usize;
    for h in handles {
        total += h.join().expect("worker").expect("claim");
    }
    (total, t0.elapsed().as_millis())
}

fn rps(claimed: usize, ms: u128) -> f64 {
    (claimed as f64) / ((ms as f64) / 1000.0).max(0.001)
}

/// AC1 — reproduce fireweed-side: pooled multi-worker whole-cohort claims on ONE queue do not scale
/// (the long-held cursor `FOR UPDATE` in the group/cohort claim path serializes them regardless of
/// `claim_pool_size`; the pool only adds acquisition/lock-wait overhead on top).
#[test]
fn cohort_claim_pool_does_not_scale_on_one_queue() {
    let url = std::env::var("FIREWEED_PG_TEST_URL")
        .expect("FIREWEED_PG_TEST_URL required (fail-closed live postgres; no LOUD skip)");

    let single = {
        let schema = fresh_schema("single");
        let backend = Arc::new(
            PostgresRelationalBackend::connect_in_schema_with_claim_pool(&url, &schema, 0).unwrap(),
        );
        seed_cohorts(&backend, "q0", 0, COHORT_COUNT);
        drain_one_queue(backend, "q0", 1)
    };

    let pooled = {
        let schema = fresh_schema("pooled");
        let backend = Arc::new(
            PostgresRelationalBackend::connect_in_schema_with_claim_pool(&url, &schema, 4).unwrap(),
        );
        seed_cohorts(&backend, "q0", 0, COHORT_COUNT);
        drain_one_queue(backend, "q0", 4)
    };

    let expect = COHORT_COUNT * COHORT_SIZE as usize;
    assert_eq!(single.0, expect, "single-worker must drain every cohort");
    assert_eq!(pooled.0, expect, "pooled multi-worker must drain every cohort");

    let single_rps = rps(single.0, single.1);
    let pooled_rps = rps(pooled.0, pooled.1);
    eprintln!(
        "cohort_claim_pool_one_queue: single={single_rps:.0} items/s; pooled4={pooled_rps:.0} items/s; \
         ratio={:.2}x (fireweed-01d7cf09 repro)",
        pooled_rps / single_rps.max(1.0)
    );

    // Physics bar for the BUG: same-queue cohort claims must NOT clear the item-level scale-out bar
    // (claim_pool_scale.rs requires pooled >= 1.25x single for Item claims). Cohort claims stay flat
    // or go backwards because they still fully serialize on one cursor row.
    assert!(
        pooled_rps < single_rps * 1.25,
        "pooled same-queue cohort claim rate {pooled_rps:.0} items/s unexpectedly cleared the \
         item-level scale-out bar ({single_rps:.0} items/s single) — the whole-cohort long-held \
         cursor lock in claim_with_client_unit may have been removed; re-derive this test's bar"
    );
}

/// AC2 — a configuration exists where end-to-end throughput at w=4 exceeds w=1: partition the SAME
/// total cohort corpus across one independent queue per worker (each has its own `relational_cursor`
/// row, so no cross-worker lock contention) instead of racing on one shared queue.
#[test]
fn cohort_claim_one_queue_per_worker_scales_with_workers() {
    let url = std::env::var("FIREWEED_PG_TEST_URL")
        .expect("FIREWEED_PG_TEST_URL required (fail-closed live postgres; no LOUD skip)");

    let single = {
        let schema = fresh_schema("baseline");
        let backend = Arc::new(
            PostgresRelationalBackend::connect_in_schema_with_claim_pool(&url, &schema, 0).unwrap(),
        );
        seed_cohorts(&backend, "q0", 0, COHORT_COUNT);
        drain_one_queue(backend, "q0", 1)
    };

    const WORKERS: usize = 4;
    let per_queue = COHORT_COUNT / WORKERS;
    assert_eq!(per_queue * WORKERS, COHORT_COUNT, "corpus must split evenly");
    let multi_queue = {
        let schema = fresh_schema("partitioned");
        let backend = Arc::new(
            PostgresRelationalBackend::connect_in_schema_with_claim_pool(&url, &schema, WORKERS)
                .unwrap(),
        );
        let queue_ids: Vec<String> = (0..WORKERS).map(|w| format!("q{w}")).collect();
        for queue_id in &queue_ids {
            seed_cohorts(&backend, queue_id, 0, per_queue);
        }
        drain_one_queue_per_worker(backend, &queue_ids)
    };

    let expect = COHORT_COUNT * COHORT_SIZE as usize;
    assert_eq!(single.0, expect, "single-worker baseline must drain every cohort");
    assert_eq!(
        multi_queue.0, expect,
        "one-queue-per-worker must drain the same total corpus"
    );

    let single_rps = rps(single.0, single.1);
    let multi_rps = rps(multi_queue.0, multi_queue.1);
    eprintln!(
        "cohort_claim_one_queue_per_worker: single={single_rps:.0} items/s; \
         w4_q4={multi_rps:.0} items/s; speedup={:.2}x (fireweed-01d7cf09 AC2 configuration)",
        multi_rps / single_rps.max(1.0)
    );

    // Same bar claim_pool_scale.rs uses for the item-level path: pooled multi-worker is at least
    // 1.25x the single-connection baseline once workers stop contending on one queue's cursor row.
    assert!(
        multi_rps >= single_rps * 1.25,
        "one-queue-per-worker claim rate {multi_rps:.0} items/s must be >= 1.25x single-queue \
         baseline {single_rps:.0} items/s (fireweed-01d7cf09 AC2 configuration bar)"
    );
}
