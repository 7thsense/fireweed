//! BQ-23 postgres binding: when the postgres control plane and storage backend share one schema,
//! `acquire_queue_lease` advances the storage append-fence epoch in the same transaction as the owner row.
//! `acquire_and_fence` then reuses that already-bound epoch instead of double-incrementing it.
//!
//! Env-gated on `FIREWEED_PG_TEST_URL` (LOUD-skips without a DB). A NON-tokio executor
//! (`futures::executor::block_on`) drives the sync postgres client.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed_conformance::qdef;
use fireweed_core::{OwnerId, QueueId, TenantId, UtcTimestamp};
use fireweed_engine::{
    AcquireOutcome, ControlPlaneConfig, ControlPlaneStore, OwnershipOutcome, QueueControlPlane,
    QueueKey, acquire_and_fence,
};
use fireweed_postgres::{PostgresBackend, PostgresControlPlane};
use postgres::{Client, NoTls};

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
    let Ok(url) = std::env::var("FIREWEED_PG_TEST_URL") else {
        eprintln!("BND-1 SKIPPED ({name}) — set FIREWEED_PG_TEST_URL to a live DB");
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

/// The postgres acquire transaction binds the owner row and the storage append fence to one epoch.
#[test]
fn cp_acquire_writes_storage_epoch_in_same_schema() {
    let Some((backend, cp)) = pair("cp_acquire_binds_storage_epoch") else {
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

    assert_eq!(
        bo(backend.current_epoch(&qk())).unwrap(),
        lease.assignment_epoch,
        "the control-plane acquire binds the storage append-fence epoch in the same transaction"
    );
}

/// `acquire_and_fence` reuses the storage fence epoch already bound by the postgres acquire transaction.
#[test]
fn acquire_and_fence_reuses_bound_postgres_epoch() {
    let Some((backend, cp)) = pair("acquire_and_fence_reuses_bound") else {
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
        "the storage backend's durable epoch is the session's fence epoch"
    );
    assert_eq!(
        session.fence_epoch, session.lease_epoch,
        "the lease and fence epochs are one bound postgres value"
    );
    assert_eq!(session.fence_epoch, 1, "the first acquire advances once");
}
