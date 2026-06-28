//! OWED-6: the blessed postgres construction paths (require `--features postgres` + a live DB).
//!
//! `open_postgres_coordinated` builds the postgres backend AND the binding control plane internally and
//! returns a coordinated `Pqueue` — the client names neither a backend nor a control plane. Env-gated on
//! `PQUEUE_PG_TEST_URL`; driven by a non-tokio executor (the sync postgres client).
#![cfg(feature = "postgres")]

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use pqueue::NewItem;
use pqueue_core::{
    EligibilityPolicy, OrderingMode, OwnerId, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
    UtcTimestamp,
};
use pqueue_engine::{Clock, ControlPlaneConfig, QueueKey};

fn bo<F: Future>(f: F) -> F::Output {
    futures::executor::block_on(f)
}

struct ManualClock(AtomicI64);
impl Clock for ManualClock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::new(self.0.load(Ordering::SeqCst), 0).unwrap()
    }
}

fn qkey() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}
fn qdef() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t1").unwrap(),
        queue_id: QueueId::new("q1").unwrap(),
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
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
    }
}

/// The coordinated postgres constructor builds backend + binding control plane internally and yields a
/// working multi-instance owner (acquires + fences) without the client naming either.
#[test]
fn open_postgres_coordinated_builds_a_working_owner() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!("OWED-6 SKIPPED — set PQUEUE_PG_TEST_URL to a live DB");
        return;
    };
    // The test runner points PQUEUE_PG_TEST_URL at a fresh database, so the public search_path is clean.
    let clock = Arc::new(ManualClock(AtomicI64::new(0)));
    let pq = pqueue::open_postgres_coordinated(
        &url,
        clock,
        OwnerId::new("inst-1").unwrap(),
        ControlPlaneConfig::default(),
    )
    .expect("postgres binds the storage epoch, so the coordinated constructor succeeds");

    bo(pq.create_queue(qdef())).unwrap();
    bo(pq.push(&qkey(), NewItem::default())).unwrap();
    assert_eq!(bo(pq.metrics(&qkey())).unwrap().pending, 1);
    // It is a real coordinated owner: ownership resolves to Mine at a granted epoch.
    assert!(matches!(
        bo(pq.ownership(&qkey())).unwrap(),
        pqueue::Ownership::Mine { epoch: Some(e) } if e >= 1
    ));
}
