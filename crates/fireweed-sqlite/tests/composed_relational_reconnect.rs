//! ADR-012 P2 recovery-on-open: the `durable_reconnect_suite!` against the composed UNIFIED sqlite-relational
//! backend (`ComposedBackend<SqliteRelational, SqliteRelational, InProcessControlPlane>`).
//!
//! The DB-authoritative projection needs no log replay (its `apply` wrote durably in the same transaction),
//! so the composition's `recover()` only repopulates the in-process control plane from the durable `queues`
//! catalog and re-seeds the id-mint counters from `pqueue_items`. Mirrors the monolith's
//! `relational_reconnect.rs`. The db path is keyed by the test's thread id.

use fireweed_conformance::{claim_req, qdef, shard, ts};
use fireweed_core::{ItemId, PriorityValue};
use fireweed_engine::{
    ClaimPort, CommandPosition, ControlPlaneStore, FinalizeKind, FinalizeOutcome, FinalizePort,
    LogStore, ProjectionRead, ProjectionStore, PushPort, PushSpec,
};
use fireweed_sqlite::{SqliteRelational, composed_sqlite_relational};
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

fn make() -> fireweed_sqlite::ComposedSqliteRelationalBackend {
    let p = db_path();
    CLEANED.with(|c| {
        if !c.get() {
            let _ = std::fs::remove_file(&p);
            c.set(true);
        }
    });
    composed_sqlite_relational(&p).expect("open composed unified sqlite-relational reconnect db")
}

fireweed_conformance::durable_reconnect_suite!(make);

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
fn composed_relational_recover_replays_tail() {
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
fn composed_relational_recovery_seeds_counters() {
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

/// Terminal-item reaping deletes durable `pqueue_items` rows (the mint-counter authority for this
/// DB-authoritative backend), so recovery must restore the id-mint floor from the durable
/// `pqueue_id_high_water` high-water, NOT only from surviving rows — otherwise reaping ALL rows and reopening
/// on the SAME epoch re-mints a reaped id (ADR-009 id-uniqueness). Regression guard for the unified relational
/// family (bead pqueue-41bf00d7, codex review), the analogue of the hybrid-async guard.
#[test]
fn composed_relational_reap_all_does_not_resurrect_ids() {
    let path = unique_path("reap-no-resurrect");
    let _ = std::fs::remove_file(&path);

    let reaped_ids: Vec<ItemId> = {
        let backend =
            composed_sqlite_relational(&path).expect("open composed sqlite-relational db");
        block_on(backend.create_queue(qdef())).unwrap();
        let mut ids = Vec::new();
        for p in [10i64, 20, 30] {
            ids.extend(block_on(backend.push(&shard(), vec![push(p)], ts(0), None)).unwrap());
        }
        // Claim + finalize all three Complete so they become terminal.
        let claimed = block_on(backend.claim(claim_req(10, 500, 1))).unwrap();
        assert_eq!(claimed.items.len(), 3, "claim must lease all 3 items");
        let outcomes = claimed
            .items
            .iter()
            .map(|it| FinalizeOutcome::new(it.item_id, FinalizeKind::Complete))
            .collect::<Vec<_>>();
        block_on(backend.finalize(&shard(), outcomes, ts(2), None)).unwrap();
        // Reap ALL terminal rows (retention elapsed; opted-out of emission so retention alone reaps).
        let reaped = backend
            .reap_terminal_items(&shard(), ts(10), 1, false)
            .unwrap();
        assert_eq!(reaped, 3, "all 3 terminal rows must reap");
        assert_eq!(
            block_on(backend.metrics(&shard())).unwrap().complete,
            0,
            "no terminal row survives the full reap"
        );
        ids
    };
    let max_reaped = reaped_ids
        .iter()
        .max_by_key(|id| (id.epoch(), id.counter()))
        .copied()
        .expect("3 ids");

    // Reopen on the SAME epoch (no re-acquire) and push. The new id MUST be strictly past every reaped id —
    // the durable mint-counter floor survived the reap of the rows that carried it (no remint/resurrection).
    let reopened = composed_sqlite_relational(&path).expect("reopen composed sqlite-relational db");
    let new_ids = block_on(reopened.push(&shard(), vec![push(40)], ts(3), None)).unwrap();
    let new_id = new_ids[0];
    assert!(
        (new_id.epoch(), new_id.counter()) > (max_reaped.epoch(), max_reaped.counter()),
        "post-reopen mint must be strictly past the greatest reaped id (no resurrection): new={new_id:?} max_reaped={max_reaped:?}"
    );
    for r in &reaped_ids {
        assert_ne!(
            new_id, *r,
            "post-reopen mint reused a reaped id (resurrection)"
        );
    }
}

#[tokio::test]
async fn emission_cursor_persists_across_reopen_sqlite_relational() {
    let path = unique_path("emission-cursor");
    let _ = std::fs::remove_file(&path);

    let mut store = SqliteRelational::open(&path).expect("open sqlite relational store");
    ProjectionStore::ensure_shard(&mut store, &qdef()).unwrap();
    assert_eq!(store.emission_cursor(&shard()).unwrap(), None);
    store
        .set_emission_cursor(&shard(), CommandPosition::new(shard(), 0, 0))
        .unwrap();
    assert_eq!(
        store.emission_cursor(&shard()).unwrap(),
        Some(CommandPosition::new(shard(), 0, 0))
    );

    drop(store);

    let reopened = SqliteRelational::open(&path).expect("reopen sqlite relational store");
    assert_eq!(
        reopened.emission_cursor(&shard()).unwrap(),
        Some(CommandPosition::new(shard(), 0, 0))
    );
    let mut reopened = reopened;
    reopened
        .set_emission_cursor(&shard(), CommandPosition::new(shard(), 0, 1))
        .unwrap();
    assert_eq!(
        reopened.emission_cursor(&shard()).unwrap(),
        Some(CommandPosition::new(shard(), 0, 1))
    );
}
