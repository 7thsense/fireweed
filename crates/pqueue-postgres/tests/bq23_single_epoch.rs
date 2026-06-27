//! BQ-23 (ADR-009 / TD-003): the control-plane lease epoch and the storage append-fence epoch are ONE
//! durable value, advanced ATOMICALLY by the acquire transaction.
//!
//! Env-gated on `PQUEUE_PG_TEST_URL` (LOUD-skips without a DB). The control plane + the storage backend
//! share one schema (one DB), so the acquire transaction binds `queues.assignment_epoch` to the lease
//! epoch. A NON-tokio executor (`futures::executor::block_on`) drives the sync postgres client.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};

use postgres::{Client, NoTls};
use pqueue_conformance::qdef;
use pqueue_core::{OwnerId, QueueId, TenantId, UtcTimestamp};
use pqueue_engine::{
    AcquireOutcome, ControlPlaneConfig, ControlPlaneStore, OwnershipOutcome, QueueControlPlane,
    QueueKey, acquire_and_fence,
};
use pqueue_postgres::{PostgresBackend, PostgresControlPlane};

fn bo<F: Future>(f: F) -> F::Output {
    futures::executor::block_on(f)
}
fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}
fn qk() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}

static SEQ: AtomicU64 = AtomicU64::new(0);
fn fresh_schema() -> String {
    format!(
        "bq23_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    )
}

/// (backend, cp) sharing one fresh schema, or `None` when no DB is configured (LOUD skip at the call site).
fn pair(name: &str) -> Option<(PostgresBackend, PostgresControlPlane)> {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!("BQ-23 SKIPPED ({name}) — set PQUEUE_PG_TEST_URL to a live DB");
        return None;
    };
    let schema = fresh_schema();
    let mut c = Client::connect(&url, NoTls).expect("connect");
    c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .expect("drop");
    drop(c);
    let backend = PostgresBackend::connect_in_schema(&url, &schema).expect("backend");
    let cp = PostgresControlPlane::connect_in_schema(&url, &schema, ControlPlaneConfig::default())
        .expect("cp");
    Some((backend, cp))
}

/// The CP acquire transaction ALONE advances the storage fence epoch (BQ-23): in the pre-BQ-23 code the
/// control-plane acquire did not touch `queues`, so the storage epoch would still read 0 here.
#[test]
fn cp_acquire_binds_storage_fence_epoch() {
    let Some((backend, cp)) = pair("cp_acquire_binds") else {
        return;
    };
    bo(backend.create_queue(qdef())).unwrap();
    assert_eq!(
        bo(backend.current_epoch(&qk())).unwrap(),
        0,
        "genesis storage epoch is 0"
    );

    let owner = OwnerId::new("owner-A").unwrap();
    cp.register_owner(&owner, ts(0)).unwrap();
    let AcquireOutcome::Acquired(lease) = cp.acquire_queue_lease(&qk(), &owner, ts(0)).unwrap()
    else {
        panic!("expected Acquired");
    };
    assert_eq!(lease.assignment_epoch, 1, "first acquire is lease epoch 1");

    // BQ-23: the acquire transaction advanced the STORAGE fence epoch to the lease epoch atomically — with
    // NO separate `acquire_epoch`. Pre-BQ-23 this read 0 (the two counters were independent).
    assert_eq!(
        bo(backend.current_epoch(&qk())).unwrap(),
        1,
        "the CP acquire alone advanced the storage fence epoch (single durable value)"
    );
    assert!(cp.binds_storage_epoch(), "postgres CP binds the storage epoch");
}

/// `acquire_and_fence` over a binding CP yields a session whose fence epoch IS the lease epoch (one value),
/// matching the storage backend's current epoch — with no separate, non-atomic storage bump.
#[test]
fn acquire_and_fence_uses_one_bound_epoch() {
    let Some((backend, cp)) = pair("acquire_and_fence_bound") else {
        return;
    };
    bo(backend.create_queue(qdef())).unwrap();
    let owner = OwnerId::new("owner-A").unwrap();
    cp.register_owner(&owner, ts(0)).unwrap();

    let OwnershipOutcome::Owned(session) =
        bo(acquire_and_fence(&cp, &backend, &qk(), &owner, ts(0))).unwrap()
    else {
        panic!("expected Owned");
    };
    assert_eq!(
        session.fence_epoch, session.lease_epoch,
        "BQ-23: the fence epoch IS the lease epoch (bound), not a separate counter"
    );
    assert_eq!(
        bo(backend.current_epoch(&qk())).unwrap(),
        session.fence_epoch,
        "the storage backend's durable epoch matches the bound session epoch"
    );
}
