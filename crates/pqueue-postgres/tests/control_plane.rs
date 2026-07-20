//! BQ-22 — the postgres [`PostgresControlPlane`] lease lifecycle + C4b seam invariants run against a LIVE
//! database, **env-gated** on `PQUEUE_PG_TEST_URL`. Without it every scenario prints a LOUD skip — a green
//! run is then VISIBLY partial (the durable control plane unverified against a real DB), never a hidden
//! pass. Compiling this file already proves `PostgresControlPlane` implements `QueueControlPlane` and shares
//! the engine's pure lease decisions. The single-connection scenarios mirror the in-memory reference's
//! lifecycle + fail-closed tests; `genesis_concurrent_acquire_has_a_single_winner` adds the
//! POSTGRES-SPECIFIC two-connection contention proof (the in-memory reference's mutex makes that race
//! impossible, but two postgres owner-nodes are exactly the topology this backend exists for — it exercises
//! the B1 genesis-row fix and would FAIL without it). Live-DB execution is deferred where no DB is present.
//!
//! To run live:
//!   docker run -d --name pq-pg -p 5433:5432 -e POSTGRES_PASSWORD=pq postgres:16
//!   PQUEUE_PG_TEST_URL=postgres://postgres:pq@127.0.0.1:5433/postgres cargo test -p pqueue-postgres

use std::sync::atomic::{AtomicU64, Ordering};

use postgres::{Client, NoTls};
use pqueue_core::{OwnerId, QueueId, TenantId, UtcTimestamp};
use pqueue_engine::{
    AcquireOutcome, ControlPlaneConfig, EngineError, LeaseRenewal, LeaseRenewalOutcome, LeaseState,
    QueueControlPlane, QueueKey,
};
use pqueue_postgres::PostgresControlPlane;

fn fresh_schema() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "pq_cp_{}_{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

#[test]
fn batch_renewal_preserves_order_and_independent_outcomes() {
    with_cp("batch_renewal", |cp| {
        let a = owner("a");
        let b = owner("b");
        cp.register_owner(&a, ts(0)).unwrap();
        cp.register_owner(&b, ts(0)).unwrap();
        let assigned = qk("assigned");
        let draining = qk("draining");
        let missing = qk("missing");
        let stale = qk("stale");
        for queue in [&assigned, &draining, &stale] {
            cp.acquire_queue_lease(queue, &a, ts(0)).unwrap();
            cp.confirm_queue_lease_fence(queue, &a, 1, ts(0)).unwrap();
        }
        cp.begin_drain(&draining, 1, &b, ts(1)).unwrap();

        let outcomes = cp
            .renew_queue_leases(
                &[
                    LeaseRenewal {
                        queue: assigned,
                        owner: a.clone(),
                        expected_epoch: 1,
                    },
                    LeaseRenewal {
                        queue: draining,
                        owner: a.clone(),
                        expected_epoch: 1,
                    },
                    LeaseRenewal {
                        queue: missing,
                        owner: a.clone(),
                        expected_epoch: 1,
                    },
                    LeaseRenewal {
                        queue: stale,
                        owner: a.clone(),
                        expected_epoch: 99,
                    },
                    LeaseRenewal {
                        queue: qk("assigned"),
                        owner: a,
                        expected_epoch: 99,
                    },
                ],
                ts(2),
            )
            .unwrap();

        assert!(matches!(
            &outcomes[0],
            LeaseRenewalOutcome::Renewed(lease) if lease.state == LeaseState::Assigned
        ));
        assert!(matches!(
            &outcomes[1],
            LeaseRenewalOutcome::Renewed(lease) if lease.state == LeaseState::Draining
        ));
        assert_eq!(outcomes[2], LeaseRenewalOutcome::Missing);
        assert_eq!(outcomes[3], LeaseRenewalOutcome::Fenced);
        assert_eq!(outcomes[4], LeaseRenewalOutcome::Fenced);
    });
}

#[test]
fn batch_renewal_handles_1000_queues_in_one_call() {
    with_cp("batch_renewal_1000", |cp| {
        let owner = owner("density-node");
        cp.register_owner(&owner, ts(0)).unwrap();
        let mut renewals = Vec::with_capacity(1_000);
        for index in 0..1_000 {
            let queue = qk(&format!("q{index:04}"));
            cp.acquire_queue_lease(&queue, &owner, ts(0)).unwrap();
            cp.confirm_queue_lease_fence(&queue, &owner, 1, ts(0))
                .unwrap();
            renewals.push(LeaseRenewal {
                queue,
                owner: owner.clone(),
                expected_epoch: 1,
            });
        }

        let outcomes = cp.renew_queue_leases(&renewals, ts(1)).unwrap();
        assert_eq!(outcomes.len(), 1_000);
        assert!(outcomes.iter().all(|outcome| matches!(
            outcome,
            LeaseRenewalOutcome::Renewed(lease)
                if lease.assignment_epoch == 1 && lease.lease_expires_at == Some(ts(16))
        )));
    });
}

#[test]
fn concurrent_reverse_order_batches_do_not_deadlock_or_shorten_leases() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!(
            "POSTGRES CONTROL-PLANE SKIPPED (concurrent_reverse_order_batches) — set PQUEUE_PG_TEST_URL"
        );
        return;
    };
    let schema = fresh_schema();
    let mut client = Client::connect(&url, NoTls).expect("connect");
    client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .expect("drop schema");
    drop(client);

    let node = owner("batch-node");
    let cp_a = PostgresControlPlane::connect_in_schema(&url, &schema, cfg()).expect("connect a");
    cp_a.register_owner(&node, ts(0)).unwrap();
    let mut forward = Vec::with_capacity(100);
    for index in 0..100 {
        let queue = qk(&format!("race-{index:03}"));
        cp_a.acquire_queue_lease(&queue, &node, ts(0)).unwrap();
        cp_a.confirm_queue_lease_fence(&queue, &node, 1, ts(0))
            .unwrap();
        forward.push(LeaseRenewal {
            queue,
            owner: node.clone(),
            expected_epoch: 1,
        });
    }
    let mut reverse = forward.clone();
    reverse.reverse();
    let cp_b = PostgresControlPlane::connect_in_schema(&url, &schema, cfg()).expect("connect b");
    let first = std::thread::spawn(move || cp_a.renew_queue_leases(&forward, ts(2)).unwrap());
    let second = std::thread::spawn(move || cp_b.renew_queue_leases(&reverse, ts(1)).unwrap());
    let first = first.join().expect("forward batch completed");
    let second = second.join().expect("reverse batch completed");
    assert_eq!(first.len(), 100);
    assert_eq!(second.len(), 100);
    assert!(first.iter().chain(&second).all(|outcome| matches!(
        outcome,
        LeaseRenewalOutcome::Renewed(lease) if lease.assignment_epoch == 1
    )));
    let verify = PostgresControlPlane::connect_in_schema(&url, &schema, cfg()).expect("verify");
    for index in 0..100 {
        assert_eq!(
            verify
                .lease(&qk(&format!("race-{index:03}")))
                .unwrap()
                .lease_expires_at,
            Some(ts(17)),
            "an older concurrent sample must not shorten a newer renewal"
        );
    }
}

#[test]
fn expired_batch_renewal_racing_takeover_is_fenced_at_epoch_two() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!(
            "POSTGRES CONTROL-PLANE SKIPPED (expired_batch_renewal_racing_takeover) — set PQUEUE_PG_TEST_URL"
        );
        return;
    };
    let schema = fresh_schema();
    let mut client = Client::connect(&url, NoTls).expect("connect");
    client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .expect("drop schema");
    drop(client);

    let (a, b, queue) = (owner("expired-a"), owner("takeover-b"), qk("takeover"));
    let cp_a = PostgresControlPlane::connect_in_schema(&url, &schema, cfg()).expect("connect a");
    cp_a.register_owner(&a, ts(0)).unwrap();
    cp_a.acquire_queue_lease(&queue, &a, ts(0)).unwrap();
    cp_a.confirm_queue_lease_fence(&queue, &a, 1, ts(0))
        .unwrap();
    let cp_b = PostgresControlPlane::connect_in_schema(&url, &schema, cfg()).expect("connect b");
    cp_b.register_owner(&b, ts(20)).unwrap();

    let renewal_queue = queue.clone();
    let renewal_owner = a.clone();
    let acquire_queue = queue.clone();
    let acquire_owner = b.clone();
    let renewal = std::thread::spawn(move || {
        cp_a.renew_queue_leases(
            &[LeaseRenewal {
                queue: renewal_queue,
                owner: renewal_owner,
                expected_epoch: 1,
            }],
            ts(20),
        )
        .unwrap()
    });
    let acquire = std::thread::spawn(move || {
        cp_b.acquire_queue_lease(&acquire_queue, &acquire_owner, ts(20))
            .unwrap()
    });
    assert_eq!(renewal.join().unwrap(), vec![LeaseRenewalOutcome::Fenced]);
    let AcquireOutcome::Acquired(lease) = acquire.join().unwrap() else {
        panic!("expired lease must be reclaimable");
    };
    assert_eq!(lease.assignment_epoch, 2);
    assert_eq!(lease.active_owner_id.as_ref(), Some(&b));
}

fn cfg() -> ControlPlaneConfig {
    ControlPlaneConfig {
        heartbeat_ttl_ms: 5_000,
        lease_ttl_ms: 15_000,
    }
}
fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}
fn owner(s: &str) -> OwnerId {
    OwnerId::new(s).unwrap()
}
fn qk(q: &str) -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new(q).unwrap())
}

/// Run `body` against a fresh schema, or LOUD-skip when no live DB is configured.
fn with_cp(name: &str, body: impl FnOnce(PostgresControlPlane)) {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!("POSTGRES CONTROL-PLANE SKIPPED ({name}) — set PQUEUE_PG_TEST_URL to a live DB");
        return;
    };
    let schema = fresh_schema();
    let mut c = Client::connect(&url, NoTls).expect("connect");
    c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .expect("drop schema");
    drop(c);
    let cp = PostgresControlPlane::connect_in_schema(&url, &schema, cfg()).expect("connect cp");
    body(cp);
}

#[test]
fn full_lifecycle_acquire_renew_drain_release_reacquire() {
    with_cp("full_lifecycle", |cp| {
        let (a, b, q) = (owner("a"), owner("b"), qk("q1"));
        cp.register_owner(&a, ts(0)).unwrap();

        let AcquireOutcome::Acquired(l1) = cp.acquire_queue_lease(&q, &a, ts(0)).unwrap() else {
            panic!("expected Acquired");
        };
        assert_eq!(l1.assignment_epoch, 1);
        assert_eq!(l1.state, LeaseState::PendingFence);
        assert_eq!(l1.active_owner_id.as_ref(), Some(&a));
        assert_eq!(l1.lease_expires_at, Some(ts(15)));
        let l1 = cp.confirm_queue_lease_fence(&q, &a, 1, ts(0)).unwrap();
        assert_eq!(l1.state, LeaseState::Assigned);

        let l2 = cp.renew_queue_lease(&q, &a, 1, ts(10)).unwrap();
        assert_eq!(l2.assignment_epoch, 1, "renew never changes the epoch");
        assert_eq!(l2.lease_expires_at, Some(ts(25)));

        cp.register_owner(&b, ts(10)).unwrap();
        let l3 = cp.begin_drain(&q, 1, &b, ts(11)).unwrap();
        assert_eq!(l3.state, LeaseState::Draining);
        assert_eq!(l3.target_owner_id.as_ref(), Some(&b));
        assert_eq!(l3.active_owner_id.as_ref(), Some(&a));

        cp.release_queue_lease(&q, &a, 1, ts(12)).unwrap();
        let rel = cp.lease(&q).unwrap();
        assert_eq!(rel.state, LeaseState::Unassigned);
        assert_eq!(rel.active_owner_id, None);
        assert_eq!(
            rel.assignment_epoch, 1,
            "epoch retained across release (durable)"
        );

        let AcquireOutcome::Acquired(l4) = cp.acquire_queue_lease(&q, &b, ts(13)).unwrap() else {
            panic!("expected Acquired");
        };
        assert_eq!(l4.assignment_epoch, 2, "strictly-greater after release");
    });
}

#[test]
fn a_different_owners_live_lease_blocks_acquire() {
    with_cp("single_lease", |cp| {
        let (a, b, q) = (owner("a"), owner("b"), qk("q1"));
        cp.register_owner(&a, ts(0)).unwrap();
        cp.register_owner(&b, ts(0)).unwrap();
        cp.acquire_queue_lease(&q, &a, ts(0)).unwrap();
        cp.heartbeat(&b, ts(4)).unwrap();
        let AcquireOutcome::Rejected(held) = cp.acquire_queue_lease(&q, &b, ts(4)).unwrap() else {
            panic!("expected Rejected");
        };
        assert_eq!(held.active_owner_id.as_ref(), Some(&a));
        assert_eq!(held.assignment_epoch, 1);
        assert_eq!(
            cp.lease(&q).unwrap().assignment_epoch,
            1,
            "a rejected acquire bumps nothing"
        );
    });
}

#[test]
fn dead_or_unregistered_owner_cannot_acquire() {
    with_cp("fail_closed_acquire", |cp| {
        let (a, q) = (owner("a"), qk("q1"));
        assert!(matches!(
            cp.acquire_queue_lease(&q, &a, ts(0)),
            Err(EngineError::Forbidden(_))
        ));
        cp.register_owner(&a, ts(0)).unwrap();
        // heartbeat 5s TTL, acquire at 10s → dead.
        assert!(matches!(
            cp.acquire_queue_lease(&q, &a, ts(10)),
            Err(EngineError::Forbidden(_))
        ));
    });
}

#[test]
fn renew_fails_closed_on_stale_epoch_wrong_owner_or_expiry() {
    with_cp("fail_closed_renew", |cp| {
        let (a, b, q) = (owner("a"), owner("b"), qk("q1"));
        cp.register_owner(&a, ts(0)).unwrap();
        cp.acquire_queue_lease(&q, &a, ts(0)).unwrap();
        assert_eq!(
            cp.renew_queue_lease(&q, &a, 99, ts(1)),
            Err(EngineError::EpochFenced)
        );
        assert_eq!(
            cp.renew_queue_lease(&q, &b, 1, ts(1)),
            Err(EngineError::EpochFenced)
        );
        assert_eq!(
            cp.renew_queue_lease(&q, &a, 1, ts(100)),
            Err(EngineError::EpochFenced)
        );
    });
}

#[test]
fn expired_lease_is_reclaimable_at_a_strictly_greater_epoch() {
    with_cp("expired_reclaim", |cp| {
        let (a, b, q) = (owner("a"), owner("b"), qk("q1"));
        cp.register_owner(&a, ts(0)).unwrap();
        cp.acquire_queue_lease(&q, &a, ts(0)).unwrap(); // epoch 1, expires ts(15)
        cp.register_owner(&b, ts(20)).unwrap();
        let AcquireOutcome::Acquired(l2) = cp.acquire_queue_lease(&q, &b, ts(20)).unwrap() else {
            panic!("expected Acquired (a's lease expired)");
        };
        assert_eq!(l2.assignment_epoch, 2);
        assert_eq!(l2.active_owner_id.as_ref(), Some(&b));
        // The superseded owner a is fenced on renew (queue-epoch-stale).
        assert_eq!(
            cp.renew_queue_lease(&q, &a, 1, ts(21)),
            Err(EngineError::EpochFenced)
        );
    });
}

#[test]
fn resolve_reports_deterministic_target_and_durable_epoch() {
    with_cp("resolve", |cp| {
        let q = qk("q1");
        // No live owner → fail-closed.
        assert_eq!(
            cp.resolve_queue_owner(&q, ts(0)).unwrap().target_owner,
            None
        );
        for o in ["a", "b", "c"] {
            cp.register_owner(&owner(o), ts(0)).unwrap();
        }
        let r1 = cp.resolve_queue_owner(&q, ts(0)).unwrap();
        let r2 = cp.resolve_queue_owner(&q, ts(1)).unwrap();
        assert!(r1.target_owner.is_some());
        assert_eq!(r1.target_owner, r2.target_owner, "HRW is deterministic");
        assert_eq!(
            r1.assignment_epoch, None,
            "genesis epoch is None, not Some(0)"
        );

        // After an acquire by the target, resolve reports the durable epoch + active owner.
        let target = r1.target_owner.clone().unwrap();
        cp.acquire_queue_lease(&q, &target, ts(0)).unwrap();
        let r3 = cp.resolve_queue_owner(&q, ts(0)).unwrap();
        assert_eq!(r3.assignment_epoch, Some(1));
        assert_eq!(r3.active_owner.as_ref(), Some(&target));
        assert_eq!(r3.state, LeaseState::PendingFence);
        cp.confirm_queue_lease_fence(&q, &target, 1, ts(0)).unwrap();
        assert_eq!(
            cp.resolve_queue_owner(&q, ts(0)).unwrap().state,
            LeaseState::Assigned
        );
    });
}

/// B1 (BQ-22 fresh-eyes BLOCKING regression): two owner NODES (separate connections to the same schema)
/// concurrently first-acquire the SAME genesis queue. Exactly ONE must win at epoch 1 — `FOR UPDATE` on a
/// missing row locks nothing, so without the genesis-materialization fix both would `Acquired` epoch 1
/// (two live writers at one epoch). Env-gated; LOUD-skips without a DB.
#[test]
fn genesis_concurrent_acquire_has_a_single_winner() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!(
            "POSTGRES CONTROL-PLANE SKIPPED (genesis_concurrent_acquire_has_a_single_winner) — set PQUEUE_PG_TEST_URL"
        );
        return;
    };
    let schema = fresh_schema();
    let mut c = Client::connect(&url, NoTls).expect("connect");
    c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .expect("drop schema");
    drop(c);

    let (a, b, q) = (owner("a"), owner("b"), qk("q1"));
    // Two independent owner nodes against the SAME durable schema.
    let cp_a = PostgresControlPlane::connect_in_schema(&url, &schema, cfg()).expect("connect a");
    cp_a.register_owner(&a, ts(0)).unwrap();
    cp_a.register_owner(&b, ts(0)).unwrap();
    let cp_b = PostgresControlPlane::connect_in_schema(&url, &schema, cfg()).expect("connect b");

    // Race two first-acquires of the genesis queue (no authority row exists yet).
    let (qa, qb) = (q.clone(), q.clone());
    let h1 = std::thread::spawn(move || cp_a.acquire_queue_lease(&qa, &a, ts(0)).unwrap());
    let h2 = std::thread::spawn(move || cp_b.acquire_queue_lease(&qb, &b, ts(0)).unwrap());
    let r1 = h1.join().unwrap();
    let r2 = h2.join().unwrap();

    let acquired = [&r1, &r2]
        .iter()
        .filter(|r| matches!(r, AcquireOutcome::Acquired(_)))
        .count();
    assert_eq!(
        acquired, 1,
        "exactly one owner wins the genesis acquire — never two writers at epoch 1"
    );
    // The durable epoch advanced exactly once.
    let verify =
        PostgresControlPlane::connect_in_schema(&url, &schema, cfg()).expect("verify conn");
    assert_eq!(
        verify.lease(&q).unwrap().assignment_epoch,
        1,
        "the genesis acquire advanced the durable epoch exactly once"
    );
}
