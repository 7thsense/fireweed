//! ADR-012 P2 recovery-on-open: the `durable_reconnect_suite!` against the composed UNIFIED sqlite-relational
//! backend (`ComposedBackend<SqliteRelational, SqliteRelational, InProcessControlPlane>`).
//!
//! The DB-authoritative projection needs no log replay (its `apply` wrote durably in the same transaction),
//! so the composition's `recover()` only repopulates the in-process control plane from the durable `queues`
//! catalog and re-seeds the id-mint counters from `pqueue_items`. Mirrors the monolith's
//! `relational_reconnect.rs`. The db path is keyed by the test's thread id.

use pqueue_conformance::{claim_req, qdef, shard, ts};
use pqueue_core::{ItemId, PriorityValue};
use pqueue_engine::{
    ClaimPort, CommandPosition, ControlPlaneStore, ProjectionRead, ProjectionStore, PushPort,
    PushSpec,
};
use pqueue_sqlite::composed_sqlite_relational;
use std::cell::Cell;
use std::future::Future;

thread_local! {
    static CLEANED: Cell<bool> = const { Cell::new(false) };
}

fn db_path() -> String {
    std::env::temp_dir()
        .join(format!(
            "pqueue-composed-relational-reconnect-{:?}.db",
            std::thread::current().id()
        ))
        .to_str()
        .unwrap()
        .to_string()
}

fn make() -> pqueue_sqlite::ComposedSqliteRelationalBackend {
    let p = db_path();
    CLEANED.with(|c| {
        if !c.get() {
            let _ = std::fs::remove_file(&p);
            c.set(true);
        }
    });
    composed_sqlite_relational(&p).expect("open composed unified sqlite-relational reconnect db")
}

pqueue_conformance::durable_reconnect_suite!(make);

fn unique_path(tag: &str) -> String {
    std::env::temp_dir()
        .join(format!(
            "pqueue-composed-relational-reconnect-{}-{}.db",
            std::process::id(),
            tag
        ))
        .to_str()
        .unwrap()
        .to_string()
}

fn push(priority: i64) -> PushSpec {
    PushSpec {
        priority: Some(PriorityValue::Int64(priority)),
        ..Default::default()
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

#[test]
fn TestComposedRelationalRecoverReplaysTail() {
    let path = unique_path("tail");
    let _ = std::fs::remove_file(&path);

    {
        let backend =
            composed_sqlite_relational(&path).expect("open composed sqlite-relational db");
        block_on(backend.create_queue(qdef())).unwrap();
        let first = block_on(backend.push(&shard(), vec![push(10)], ts(0), None)).unwrap();
        let second = block_on(backend.push(&shard(), vec![push(20)], ts(1), None)).unwrap();
        assert_eq!(first[0], ItemId::mint(0, 0, 0));
        assert_eq!(second[0], ItemId::mint(0, 0, 1));
        let claimed = block_on(backend.claim(claim_req(1, 500, 2))).unwrap();
        assert_eq!(claimed.items.len(), 1);
        assert_eq!(
            backend.with_projection(|projection| projection.recovery_high_water(&shard()).unwrap()),
            Some(CommandPosition::new(shard(), 0, 2)),
            "the composed reopen cursor should reflect the persisted applied high-water"
        );
    }

    let reopened = composed_sqlite_relational(&path).expect("reopen composed sqlite-relational db");
    assert_eq!(
        reopened.with_projection(|projection| projection.recovery_high_water(&shard()).unwrap()),
        Some(CommandPosition::new(shard(), 0, 2)),
        "recovery must resume from the durable relational cursor rather than genesis"
    );
    let metrics = block_on(reopened.metrics(&shard())).unwrap();
    assert_eq!((metrics.pending, metrics.leased), (1, 1));
}

#[test]
fn TestComposedRelationalRecoverySeedsCounters() {
    let path = unique_path("counters");
    let _ = std::fs::remove_file(&path);

    {
        let backend =
            composed_sqlite_relational(&path).expect("open composed sqlite-relational db");
        block_on(backend.create_queue(qdef())).unwrap();
        let first = block_on(backend.push(&shard(), vec![push(10)], ts(0), None)).unwrap();
        let second = block_on(backend.push(&shard(), vec![push(20)], ts(1), None)).unwrap();
        assert_eq!(first[0], ItemId::mint(0, 0, 0));
        assert_eq!(second[0], ItemId::mint(0, 0, 1));
    }

    let reopened = composed_sqlite_relational(&path).expect("reopen composed sqlite-relational db");
    let third = block_on(reopened.push(&shard(), vec![push(30)], ts(2), None)).unwrap();
    assert_eq!(
        third[0],
        ItemId::mint(0, 0, 2),
        "item-id counters must resume past the durable projection snapshot"
    );
}
