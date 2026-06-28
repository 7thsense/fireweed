//! B5 (ADR-009 / TD-003): durable multi-instance shared-store competition over POSTGRES — the full stack
//! (B1 data-plane fence + B2 coordinated library owner + B4 single durable epoch) end to end.
//!
//! Two `Pqueue` instances, each with its OWN postgres backend + control-plane connection but the SAME
//! schema (one durable store), compete for a queue. The keystone guarantee under test is the **durable
//! epoch fence**: the fence epoch lives in the shared `queues` table (BQ-23), so a superseded instance is
//! rejected `EpochFenced` across connections — proven below. (Cross-instance ITEM visibility additionally
//! needs the DB-authoritative relational backend; the log-replay `PostgresBackend` rebuilds its in-memory
//! projection per connection, so item counts are asserted only against the writer's own view here.)
//!
//! Env-gated on `PQUEUE_PG_TEST_URL` (LOUD skip without a DB). A shared in-test `ManualClock` drives lease
//! expiry deterministically. A NON-tokio executor drives the sync pg client.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use postgres::{Client, NoTls};
use pqueue::{NewItem, Pqueue};
use pqueue_conformance::qdef;
use pqueue_core::{OwnerId, QueueId, TenantId, UtcTimestamp};
use pqueue_engine::{Clock, ControlPlaneConfig, EngineError, QueueControlPlane, QueueKey};
use pqueue_postgres::{PostgresBackend, PostgresControlPlane, PostgresRelationalBackend};

fn bo<F: Future>(f: F) -> F::Output {
    futures::executor::block_on(f)
}
fn qk() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}

/// A test clock with interior mutability so a shared `Arc<ManualClock>` advances time for both instances.
struct ManualClock(AtomicI64);
impl ManualClock {
    fn at(s: i64) -> Self {
        Self(AtomicI64::new(s))
    }
    fn set(&self, s: i64) {
        self.0.store(s, Ordering::SeqCst);
    }
}
impl Clock for ManualClock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::new(self.0.load(Ordering::SeqCst), 0).unwrap()
    }
}

static SEQ: AtomicU64 = AtomicU64::new(0);
fn fresh_schema() -> String {
    format!("b5_{}_{}", std::process::id(), SEQ.fetch_add(1, Ordering::SeqCst))
}

#[test]
fn two_instances_compete_over_shared_postgres() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!("B5 multi-instance SKIPPED — set PQUEUE_PG_TEST_URL to a live DB");
        return;
    };
    let schema = fresh_schema();
    let mut c = Client::connect(&url, NoTls).expect("connect");
    c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .expect("drop");
    drop(c);

    let clock = Arc::new(ManualClock::at(0));
    // Each instance: its OWN backend + control-plane connection, the SAME schema (one durable store).
    let make = |owner: &str, node: u8| -> Pqueue<PostgresBackend> {
        // Each replica carries a DISTINCT node_id (ADR-009) — as the config-driven service would assign —
        // so even a split-brain/handoff window cannot mint a colliding id over the shared store.
        let backend = Arc::new(
            PostgresBackend::connect_in_schema(&url, &schema)
                .expect("backend")
                .with_node_id(node),
        );
        let cp: Arc<dyn QueueControlPlane> = Arc::new(
            PostgresControlPlane::connect_in_schema(&url, &schema, ControlPlaneConfig::default())
                .expect("cp"),
        );
        // Postgres binds the storage epoch (BQ-23), so the durable multi-instance constructor accepts it.
        Pqueue::with_control_plane(backend, clock.clone(), OwnerId::new(owner).unwrap(), cp)
            .expect("postgres control plane presents the atomic acquire->fence capability")
    };

    let a = make("owner-A", 1);
    bo(a.create_queue(qdef())).unwrap();
    // B connects AFTER the queue exists, so its per-connection projection includes the queue.
    let b = make("owner-B", 2);

    // A acquires the queue (epoch 1 — BQ-23 binds the storage fence epoch atomically) and operates.
    bo(a.push(&qk(), NewItem::default())).unwrap();
    assert_eq!(bo(a.metrics(&qk())).unwrap().pending, 1, "A's own write is visible to A");

    // While A holds a live lease, B cannot operate on the queue — owned elsewhere.
    assert!(
        matches!(bo(b.push(&qk(), NewItem::default())), Err(EngineError::Forbidden(_))),
        "a peer cannot operate on a queue a live owner holds"
    );
    assert!(
        matches!(bo(b.ownership(&qk())).unwrap(), pqueue::Ownership::Elsewhere { owner, epoch: Some(1) } if owner.as_str() == "owner-A"),
        "B sees A as the live owner at epoch 1"
    );

    // Advance past A's lease TTL (default 15s) so the queue is reclaimable.
    clock.set(20);
    // B reclaims the queue at a strictly-greater epoch (BQ-23 advances the single durable epoch to 2).
    bo(b.push(&qk(), NewItem::default())).unwrap();

    // A is superseded. Its NEXT op stamps its cached (stale) epoch and is EpochFenced AT COMMIT, durably,
    // over the shared postgres store — the full B1+B2+B4 fence, across connections (not an in-process check).
    assert!(
        matches!(bo(a.push(&qk(), NewItem::default())), Err(EngineError::EpochFenced)),
        "a superseded instance must be durably fenced over the shared postgres store"
    );
    // The fence dropped A's stale session; A re-resolves and sees the queue is owned elsewhere.
    assert!(
        matches!(bo(a.push(&qk(), NewItem::default())), Err(EngineError::Forbidden(_))),
        "a fenced instance re-resolves to owned-elsewhere"
    );
    // B, the current owner, keeps operating.
    bo(b.push(&qk(), NewItem::default())).unwrap();
}

/// OWED-4: over the DB-authoritative RELATIONAL backend (`postgres_native`), multi-instance competition has
/// **full cross-instance item visibility** (both instances read `pqueue_items` from the shared DB) AND the
/// durable epoch fence — the complete production multi-instance guarantee.
#[test]
fn relational_multi_instance_has_item_visibility_and_fence() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!("B5 relational multi-instance SKIPPED — set PQUEUE_PG_TEST_URL to a live DB");
        return;
    };
    let schema = fresh_schema();
    let mut c = Client::connect(&url, NoTls).expect("connect");
    c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .expect("drop");
    drop(c);

    let clock = Arc::new(ManualClock::at(0));
    let make = |owner: &str, node: u8| -> Pqueue<PostgresRelationalBackend> {
        let backend = Arc::new(
            PostgresRelationalBackend::connect_in_schema(&url, &schema)
                .expect("backend")
                .with_node_id(node),
        );
        let cp: Arc<dyn QueueControlPlane> = Arc::new(
            PostgresControlPlane::connect_in_schema(&url, &schema, ControlPlaneConfig::default())
                .expect("cp"),
        );
        Pqueue::with_control_plane(backend, clock.clone(), OwnerId::new(owner).unwrap(), cp)
            .expect("postgres binds the storage epoch")
    };

    let a = make("owner-A", 1);
    bo(a.create_queue(qdef())).unwrap();
    let b = make("owner-B", 2);

    bo(a.push(&qk(), NewItem::default())).unwrap();
    // Cross-instance item visibility: B reads A's write from the shared DB (impossible on the log-replay
    // backend, which holds a per-connection in-memory projection).
    assert_eq!(
        bo(b.metrics(&qk())).unwrap().pending,
        1,
        "the relational backend gives B authoritative visibility of A's write"
    );

    // While A's lease is live, B is owned-elsewhere.
    assert!(matches!(bo(b.push(&qk(), NewItem::default())), Err(EngineError::Forbidden(_))));

    // After A's lease expires, B reclaims the queue (epoch 2) and CLAIMS A's pending item across the
    // instance boundary — proving DB-authoritative cross-instance work handoff (no new item minted, so this
    // sidesteps the per-connection push-id limitation noted below).
    clock.set(20);
    let claimed = bo(b.claim(&qk(), 10, 1_000)).unwrap();
    assert_eq!(claimed.len(), 1, "B claims A's pending item across the instance boundary");

    // A is superseded → its next data-plane op is durably fenced on the relational backend too.
    assert!(
        matches!(bo(a.push(&qk(), NewItem::default())), Err(EngineError::EpochFenced)),
        "a superseded instance is durably fenced on the relational backend"
    );
}

// NOTE (relational concurrent-push id limitation, separate from the fence): the relational backend mints
// item ids from a per-connection sequence prefix, so two SEPARATE connections each pushing a fresh item can
// collide on `pqueue_items_pkey` (both start at 0). Full concurrent multi-writer push needs a DB-sequence-
// based (globally unique) item id — a tracked follow-up. The fence, cross-instance visibility, and
// cross-instance claim handoff (the safety + work-handoff guarantees) are unaffected and proven above.
