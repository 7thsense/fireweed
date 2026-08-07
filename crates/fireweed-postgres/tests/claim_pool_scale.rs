//! fireweed-66d64e91 — claim-pool throughput should scale with concurrent workers.
//!
//! Env-gated on `FIREWEED_PG_TEST_URL` (fail-closed). Compares wall-clock claim drain of a
//! fixed corpus under:
//! - single connection (`claim_pool_size=0`) with 1 worker
//! - pooled connections (`claim_pool_size=4`) with 4 workers
//!
//! Acceptance bar: pooled multi-worker drain is strictly faster than the single-connection
//! baseline (host-local physics; not a portable SLA).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use fireweed_conformance::qdef;
use fireweed_core::{LeaseToken, UtcTimestamp, WorkerId};
use fireweed_engine::{
    ClaimPort, ClaimRequest, ControlPlaneStore, EngineResult, PushPort, PushSpec,
};
use fireweed_postgres::PostgresRelationalBackend;

const CORPUS: usize = 4_000;
const BATCH: usize = 32;

fn bo<F: std::future::Future>(f: F) -> F::Output {
    futures::executor::block_on(f)
}

fn fresh_schema(tag: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "claim_pool_scale_{tag}_{}_{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

fn claim_req(max: usize, worker: &str, now: i64) -> ClaimRequest {
    ClaimRequest {
        shard: fireweed_conformance::shard(),
        max_items: max,
        lease_token: LeaseToken::new(format!("lease-{worker}")).unwrap(),
        lease_expires_at: UtcTimestamp::new(now + 120, 0).unwrap(),
        worker_id: WorkerId::new(worker).unwrap(),
        now: UtcTimestamp::new(now, 0).unwrap(),
        expected_epoch: None,
        compatibility: Default::default(),
        eligibility_time: None,
    }
}

fn seed(backend: &PostgresRelationalBackend) {
    let shard = fireweed_conformance::shard();
    bo(async {
        backend.create_queue(qdef()).await.unwrap();
        let mut i = 0usize;
        while i < CORPUS {
            let end = (i + 500).min(CORPUS);
            let specs: Vec<PushSpec> = (i..end)
                .map(|j| PushSpec {
                    client_item_key: Some(
                        fireweed_core::ClientItemKey::new(format!("scale-{j}")).unwrap(),
                    ),
                    ..Default::default()
                })
                .collect();
            backend
                .push(&shard, specs, UtcTimestamp::new(0, 0).unwrap(), None)
                .await
                .unwrap();
            i = end;
        }
    });
}

/// Drain every pending item with `workers` concurrent claim loops. Returns (claimed, elapsed_ms).
fn drain(backend: Arc<PostgresRelationalBackend>, workers: usize) -> (usize, u128) {
    let barrier = Arc::new(Barrier::new(workers));
    let start_gate = Arc::new(Barrier::new(workers + 1));
    let mut handles = Vec::with_capacity(workers);
    for w in 0..workers {
        let backend = Arc::clone(&backend);
        let barrier = Arc::clone(&barrier);
        let start_gate = Arc::clone(&start_gate);
        let name = format!("scale-w{w}");
        handles.push(thread::spawn(move || -> EngineResult<usize> {
            barrier.wait();
            start_gate.wait();
            let mut got = 0usize;
            loop {
                let claimed = bo(backend.claim(claim_req(BATCH, &name, 1)))?;
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
    let ms = t0.elapsed().as_millis();
    (total, ms)
}

#[test]
fn claim_pool_throughput_scales_with_workers() {
    let url = std::env::var("FIREWEED_PG_TEST_URL")
        .expect("FIREWEED_PG_TEST_URL required (fail-closed live postgres; no LOUD skip)");

    // Baseline: one connection, one worker (legacy Mutex serialization posture).
    let single = {
        let schema = fresh_schema("single");
        let backend = Arc::new(
            PostgresRelationalBackend::connect_in_schema_with_claim_pool(&url, &schema, 0).unwrap(),
        );
        assert_eq!(backend.claim_pool_size(), 0);
        seed(&backend);
        drain(backend, 1)
    };

    // Pooled: 4 claim connections, 4 workers racing via FOR UPDATE SKIP LOCKED.
    let pooled = {
        let schema = fresh_schema("pooled");
        let backend = Arc::new(
            PostgresRelationalBackend::connect_in_schema_with_claim_pool(&url, &schema, 4).unwrap(),
        );
        assert_eq!(backend.claim_pool_size(), 4);
        seed(&backend);
        drain(backend, 4)
    };

    assert_eq!(single.0, CORPUS, "single-writer must drain full corpus");
    assert_eq!(
        pooled.0, CORPUS,
        "pooled multi-writer must drain full corpus"
    );

    let single_rps = (single.0 as f64) / ((single.1 as f64) / 1000.0).max(0.001);
    let pooled_rps = (pooled.0 as f64) / ((pooled.1 as f64) / 1000.0).max(0.001);
    eprintln!(
        "claim_pool_scale: single={} claimed in {} ms ({:.0} items/s); \
         pooled4={} claimed in {} ms ({:.0} items/s); speedup={:.2}x",
        single.0,
        single.1,
        single_rps,
        pooled.0,
        pooled.1,
        pooled_rps,
        pooled_rps / single_rps.max(1.0)
    );

    // Physics bar: pooled multi-worker is at least 1.25× the single-connection claim rate.
    // (Below linear scaling is fine; flat/negative scaling fails the bead.)
    assert!(
        pooled_rps >= single_rps * 1.25,
        "pooled claim rate {pooled_rps:.0} items/s must be >= 1.25× single {single_rps:.0} items/s \
         (fireweed-66d64e91 scale-out bar)"
    );
}
