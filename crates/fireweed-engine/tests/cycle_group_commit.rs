//! Concurrent `claim_finalize_push_cycle` callers on one shard share a seal.

use std::sync::{Arc, Barrier};

use fireweed_conformance::{qdef, shard, ts};
use fireweed_engine::{ControlPlaneStore, PushPort, PushSpec, assemble_async_log_replay};
use fireweed_projection::{InMemoryProjection, MemoryLog};

#[test]
fn concurrent_cycles_coalesce_into_fewer_seals() {
    let backend = Arc::new(
        assemble_async_log_replay(MemoryLog::new(), InMemoryProjection::new(), 1)
            .expect("assemble"),
    );
    futures::executor::block_on(async {
        backend.create_queue(qdef()).await.expect("create queue");
        let items: Vec<PushSpec> = (0..80).map(|_| PushSpec::default()).collect();
        backend
            .push(&shard(), items, ts(0), None)
            .await
            .expect("seed");
    });

    let start = Arc::new(Barrier::new(8));
    let mut joins = Vec::with_capacity(8);
    for _ in 0..8 {
        let backend = Arc::clone(&backend);
        let start = Arc::clone(&start);
        joins.push(std::thread::spawn(move || {
            start.wait();
            futures::executor::block_on(async {
                let lifecycle: Vec<PushSpec> = (0..10).map(|_| PushSpec::default()).collect();
                backend
                    .claim_finalize_push_cycle(shard(), 30_000, ts(0), None, lifecycle)
                    .await
                    .expect("cycle")
            })
        }));
    }
    let total: usize = joins.into_iter().map(|j| j.join().expect("thread")).sum();
    assert_eq!(total, 80, "every seeded item must cycle once");
    let (seals, cycles) = backend.cycle_group_commit_stats();
    assert_eq!(cycles, 8);
    assert!(
        seals < cycles,
        "concurrent cycles must coalesce: seals={seals} cycles={cycles}"
    );
}
