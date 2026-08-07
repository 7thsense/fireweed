//! fireweed-66d64e91 — multi-connection claim pool exercises `FOR UPDATE SKIP LOCKED` concurrently.
//!
//! Env-gated on `FIREWEED_PG_TEST_URL` (fail-closed: no LOUD skip). Two claim-pool workers race over
//! one schema; every pushed item is claimed exactly once.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use fireweed_conformance::qdef;
use fireweed_core::{ItemId, LeaseToken, UtcTimestamp, WorkerId};
use fireweed_engine::{
    ClaimPort, ClaimRequest, ControlPlaneStore, EngineResult, ProjectionRead, PushPort, PushSpec,
};
use fireweed_postgres::PostgresRelationalBackend;

fn bo<F: std::future::Future>(f: F) -> F::Output {
    futures::executor::block_on(f)
}

fn fresh_schema() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "claim_pool_{}_{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

fn claim_req(max: usize, worker: &str, now: i64) -> ClaimRequest {
    ClaimRequest {
        shard: fireweed_conformance::shard(),
        max_items: max,
        lease_token: LeaseToken::new(format!("lease-{worker}")).unwrap(),
        lease_expires_at: UtcTimestamp::new(now + 60, 0).unwrap(),
        worker_id: WorkerId::new(worker).unwrap(),
        now: UtcTimestamp::new(now, 0).unwrap(),
        expected_epoch: None,
        compatibility: Default::default(),
        eligibility_time: None,
    }
}

#[test]
fn claim_pool_concurrent_workers_partition_items_via_skip_locked() {
    let url = std::env::var("FIREWEED_PG_TEST_URL")
        .expect("FIREWEED_PG_TEST_URL required (fail-closed live postgres; no LOUD skip)");
    let schema = fresh_schema();
    // 4 claim connections so two workers never share one SYNC client Mutex for claim.
    let backend = Arc::new(
        PostgresRelationalBackend::connect_in_schema_with_claim_pool(&url, &schema, 4).unwrap(),
    );
    assert_eq!(backend.claim_pool_size(), 4);

    let shard = fireweed_conformance::shard();
    bo(async {
        backend.create_queue(qdef()).await.unwrap();
        let specs: Vec<PushSpec> = (0..200)
            .map(|i| PushSpec {
                client_item_key: Some(
                    fireweed_core::ClientItemKey::new(format!("item-{i}")).unwrap(),
                ),
                ..Default::default()
            })
            .collect();
        backend
            .push(&shard, specs, UtcTimestamp::new(0, 0).unwrap(), None)
            .await
            .unwrap();
    });

    let barrier = Arc::new(Barrier::new(2));
    let make_worker = |name: &'static str| {
        let backend = Arc::clone(&backend);
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || -> EngineResult<Vec<ItemId>> {
            barrier.wait();
            let mut got = Vec::new();
            loop {
                let claimed = bo(backend.claim(claim_req(16, name, 1)))?;
                if claimed.items.is_empty() {
                    break;
                }
                got.extend(claimed.items.into_iter().map(|item| item.item_id));
            }
            Ok(got)
        })
    };

    let a = make_worker("w-a");
    let b = make_worker("w-b");
    let ids_a = a.join().expect("worker a").expect("claim a");
    let ids_b = b.join().expect("worker b").expect("claim b");

    let mut all = HashSet::new();
    for id in ids_a.iter().chain(ids_b.iter()) {
        assert!(all.insert(*id), "duplicate claim of {id}");
    }
    assert_eq!(
        all.len(),
        200,
        "every pushed item must be claimed exactly once"
    );
    assert!(
        !ids_a.is_empty() && !ids_b.is_empty(),
        "both claim-pool workers should observe work (SKIP LOCKED multi-connection)"
    );

    // `pending()` is the leased-with-token view (not lifecycle Pending). After a full drain every
    // claim should appear there with a live token from the claim-pool path.
    let leased = bo(backend.pending(&shard)).unwrap();
    assert_eq!(
        leased.len(),
        200,
        "every claim must install a live token under the process state lock"
    );
}
