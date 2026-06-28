//! ADR-009 boundary (formerly BQ-23): the control plane records ownership in its OWN authority table and
//! NEVER writes the storage backend's tables; the storage backend's `acquire_epoch` is the single
//! authority for the append-fence epoch. The control-plane lease epoch and the storage fence epoch are
//! advanced one-per-acquire by `acquire_and_fence`, so a session's `fence_epoch == lease_epoch` even though
//! they are two independently-owned counters in two tables (no cross-table write, no pg_attribute sniffing).
//!
//! Env-gated on `PQUEUE_PG_TEST_URL` (LOUD-skips without a DB). A NON-tokio executor
//! (`futures::executor::block_on`) drives the sync postgres client.

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
        "bnd1_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    )
}

/// (backend, cp) sharing one fresh schema, or `None` when no DB is configured (LOUD skip at the call site).
fn pair(name: &str) -> Option<(PostgresBackend, PostgresControlPlane)> {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!("BND-1 SKIPPED ({name}) — set PQUEUE_PG_TEST_URL to a live DB");
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

/// BOUNDARY: the control-plane acquire records ownership in its own authority table ONLY and does NOT touch
/// the storage backend's append-fence epoch. (In the pre-cleanup BQ-23 code this advanced `queues`/
/// `relational_cursor.assignment_epoch` via a cross-table write — that write is gone.)
#[test]
fn cp_acquire_does_not_write_storage_epoch() {
    let Some((backend, cp)) = pair("cp_acquire_no_cross_write") else {
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

    // The control-plane acquire alone leaves the storage fence epoch untouched — the CP never writes the
    // backend's tables. The storage fence is advanced separately by `acquire_epoch` (the authority).
    assert_eq!(
        bo(backend.current_epoch(&qk())).unwrap(),
        0,
        "the control plane does NOT write the storage backend's fence epoch (boundary respected)"
    );
}

/// `acquire_and_fence` advances the STORAGE fence epoch (the single authority) and yields a session whose
/// `fence_epoch` matches the backend's durable epoch; lease and fence epochs advance in lock-step, so they
/// are equal — two independently-owned counters, not one cross-written value.
#[test]
fn acquire_and_fence_storage_owns_the_fence() {
    let Some((backend, cp)) = pair("acquire_and_fence_storage_owns") else {
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
        bo(backend.current_epoch(&qk())).unwrap(),
        session.fence_epoch,
        "the storage backend's durable epoch is the session's fence epoch (storage owns the fence)"
    );
    assert_eq!(
        session.fence_epoch, session.lease_epoch,
        "lease + fence epochs advance one-per-acquire in lock-step, so they are equal"
    );
    assert!(
        session.fence_epoch >= 1,
        "the first acquire advances the fence past genesis"
    );
}
