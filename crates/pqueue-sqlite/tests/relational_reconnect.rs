//! BQ-11d — the relational-reconnect conformance class against the DB-authoritative
//! `SqliteRelationalBackend`: a reopen of the same sqlite file recovers committed state from
//! `pqueue_items` itself, with NO command log to replay.
//!
//! Two layers:
//! 1. The shared `relational_reconnect_suite!` (also run against the log-backed backend in
//!    `reconnect_smoke.rs`): committed pending/terminal/leased state survives a reopen. Lease tokens are
//!    NOT asserted post-reopen — the relational family deliberately loses the cleartext token on reopen
//!    (hash-only at rest), so only the recovered lifecycle *state* is shared across both families.
//! 2. Relational-specific contracts the shared class can't carry (the log family has no equivalent): the
//!    `client_item_key` retention tombstone survives reopen, and a leased item survives as `Leased` but is
//!    omitted from `pending()` after reopen (its live token is gone) yet remains reclaimable.

use std::collections::BTreeMap;

use pqueue_conformance::{qdef, shard};
use pqueue_core::{ClientItemKey, LeaseToken, PriorityValue, RequestId, UtcTimestamp, WorkerId};
use pqueue_engine::{
    ClaimPort, ClaimRequest, CommandPosition, ControlPlaneStore, EngineError, FinalizeKind,
    FinalizeOutcome, FinalizePort, ProjectionRead, PurgePort, PushPort, PushSpec, ReclaimDriver,
    UpsertOutcome, UpsertPort,
};
use pqueue_sqlite::SqliteRelationalBackend;
use rusqlite::Connection;
use std::cell::Cell;

fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}

// --- shared relational-reconnect suite (file-backed; per-thread isolation, see reconnect_smoke.rs) -----

thread_local! {
    static CLEANED: Cell<bool> = const { Cell::new(false) };
}

fn suite_db_path() -> String {
    std::env::temp_dir()
        .join(format!(
            "pqueue-rel-reconnect-suite-{:?}.db",
            std::thread::current().id()
        ))
        .to_str()
        .unwrap()
        .to_string()
}

fn make() -> SqliteRelationalBackend {
    let p = suite_db_path();
    CLEANED.with(|c| {
        if !c.get() {
            let _ = std::fs::remove_file(&p);
            c.set(true);
        }
    });
    SqliteRelationalBackend::open(&p).expect("open relational reconnect db")
}

pqueue_conformance::relational_reconnect_suite!(make);

// --- relational-specific reopen contracts -------------------------------------------------------------

fn unique_path(tag: &str) -> String {
    std::env::temp_dir()
        .join(format!(
            "pqueue-rel-reconnect-{tag}-{}.db",
            std::process::id()
        ))
        .to_str()
        .unwrap()
        .to_string()
}

#[test]
fn open_upgrades_existing_relational_items_table_with_metadata_column() {
    let path = unique_path("metadata-migration");
    let _ = std::fs::remove_file(&path);
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE pqueue_items (
                tenant_id TEXT NOT NULL,
                queue_id TEXT NOT NULL,
                item_id TEXT NOT NULL,
                client_item_key TEXT NOT NULL,
                lifecycle_state TEXT NOT NULL,
                priority TEXT,
                priority_sort BLOB NOT NULL,
                not_before INTEGER,
                eligible_since INTEGER,
                group_key TEXT,
                cohort_size INTEGER,
                recurrence_until INTEGER,
                payload BLOB,
                fields TEXT NOT NULL DEFAULT '{}',
                retry_count INTEGER NOT NULL DEFAULT 0,
                item_version INTEGER NOT NULL,
                lease_token_hash BLOB,
                lease_expires_at INTEGER,
                worker_id TEXT,
                last_command_sequence INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                terminal_at INTEGER,
                fenced INTEGER NOT NULL DEFAULT 0,
                superseded INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL,
                created_seq INTEGER NOT NULL,
                PRIMARY KEY (tenant_id, queue_id, item_id)
             );",
        )
        .unwrap();
    }

    let _backend = SqliteRelationalBackend::open(&path).unwrap();
    let conn = Connection::open(&path).unwrap();
    let mut stmt = conn.prepare("PRAGMA table_info(pqueue_items)").unwrap();
    let columns: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(
        columns.iter().any(|column| column == "metadata"),
        "open() must upgrade old relational pqueue_items tables with metadata"
    );
}

fn claim_req(max: usize, exp: i64, now: i64) -> ClaimRequest {
    ClaimRequest {
        shard: shard(),
        worker_id: WorkerId::new("w1").unwrap(),
        max_items: max,
        lease_token: LeaseToken::new("lease-1").unwrap(),
        lease_expires_at: ts(exp),
        now: ts(now),
        compatibility: pqueue_engine::ClaimCompatibility::default(),
        expected_epoch: None,
    }
}

/// The relational cursor reopens at the last applied position, not at genesis.
#[tokio::test]
async fn recovery_high_water_tracks_applied_position_after_reopen() {
    let path = unique_path("high-water");
    let _ = std::fs::remove_file(&path);
    let rid = RequestId::new("high-water-push").unwrap();

    {
        let a = SqliteRelationalBackend::open(&path).unwrap();
        a.create_queue(qdef()).await.unwrap();
        a.push_with_request_id(
            &shard(),
            rid,
            vec![PushSpec::default(), PushSpec::default()],
            ts(0),
            None,
        )
        .await
        .unwrap();
        a.claim(claim_req(1, 500, 10)).await.unwrap();
    } // crash

    let b = SqliteRelationalBackend::open(&path).unwrap();
    assert_eq!(
        b.recovery_high_water(&shard()).unwrap(),
        Some(CommandPosition::new(shard(), 0, 1)),
        "reopen must resume from the last applied command position"
    );
    let _ = std::fs::remove_file(&path);
}

/// The `pqueue_item_key_retention` tombstone is durable: a terminal item purged before a reopen still
/// blocks re-push of its `client_item_key` (within retention) AFTER the reopen — proving the tombstone
/// recovers from the DB, not from any in-process state.
#[tokio::test]
async fn retention_tombstone_survives_reopen() {
    let path = unique_path("retention");
    let _ = std::fs::remove_file(&path);
    let key = ClientItemKey::new("rk").unwrap();

    {
        let a = SqliteRelationalBackend::open(&path).unwrap();
        a.create_queue(qdef()).await.unwrap();
        let id = match a
            .replace_if_pending(
                &shard(),
                &key,
                Some(PriorityValue::Int64(5)),
                None,
                None,
                None,
                BTreeMap::new(),
                Default::default(),
                None,
                ts(0),
                None,
            )
            .await
            .unwrap()
        {
            UpsertOutcome::Inserted { item_id } => item_id,
            _ => panic!("insert"),
        };
        a.claim(claim_req(1, 500, 1)).await.unwrap();
        a.finalize(
            &shard(),
            vec![FinalizeOutcome::new(id, FinalizeKind::Complete)],
            ts(2),
            None,
        )
        .await
        .unwrap();
        a.purge(&shard(), vec![id], false, ts(3), None)
            .await
            .unwrap(); // terminal purge -> retention tombstone
    } // crash

    let b = SqliteRelationalBackend::open(&path).unwrap();
    // Within retention (now=10 << 3s + 60s): the recovered tombstone still rejects the re-push. Assert the
    // EXACT `Terminal` (a broken def-cache reload would surface as `NotFound`, which this rules out).
    assert_eq!(
        b.replace_if_pending(
            &shard(),
            &key,
            None,
            None,
            None,
            None,
            BTreeMap::new(),
            Default::default(),
            None,
            ts(10),
            None
        )
        .await,
        Err(EngineError::Terminal),
        "the retention tombstone survives reopen and still blocks the duplicate as Terminal"
    );
    let _ = std::fs::remove_file(&path);
}

/// A reopened queue is fully operational: its definition cache + command-sequence counter recover, so a
/// NEW push after reopen mints a non-colliding id and is immediately claimable (DB-authoritative recovery,
/// not just read-only state). Covers `reload()`'s cmd_seq restoration + def-cache reload + a live claim.
#[tokio::test]
async fn reopened_queue_accepts_new_push_and_claim() {
    let path = unique_path("operational");
    let _ = std::fs::remove_file(&path);

    let pre_id;
    {
        let a = SqliteRelationalBackend::open(&path).unwrap();
        a.create_queue(qdef()).await.unwrap();
        let ids = a
            .push(&shard(), vec![PushSpec::default()], ts(0), None)
            .await
            .unwrap();
        pre_id = ids[0];
        // Claim + complete it so the pre-reopen item is terminal (not eligible) after reopen.
        a.claim(claim_req(1, 500, 1)).await.unwrap();
        a.finalize(
            &shard(),
            vec![FinalizeOutcome::new(pre_id, FinalizeKind::Complete)],
            ts(2),
            None,
        )
        .await
        .unwrap();
    } // crash

    let b = SqliteRelationalBackend::open(&path).unwrap();
    // A fresh push: the id must NOT collide with the pre-reopen id (cmd_seq restored from pqueue_items).
    let new_ids = b
        .push(&shard(), vec![PushSpec::default()], ts(10), None)
        .await
        .unwrap();
    assert_eq!(new_ids.len(), 1);
    assert_ne!(
        new_ids[0], pre_id,
        "post-reopen push must mint a fresh id (no PK collision)"
    );
    // And the queue is live: the new item is claimable (def cache reloaded, claim CTE operates).
    let claimed = b.claim(claim_req(1, 500, 11)).await.unwrap();
    assert_eq!(claimed.items.len(), 1, "reopened queue serves new claims");
    assert_eq!(claimed.items[0].item_id, new_ids[0]);
    let _ = std::fs::remove_file(&path);
}

/// The documented token contract across reopen: a leased item survives as `Leased` in `pqueue_items`
/// (metrics), but is OMITTED from `pending()` after reopen because its cleartext token is gone (hash-only
/// at rest). It remains reclaimable by the owner via the reclaim tick.
#[tokio::test]
async fn leased_item_survives_reopen_but_loses_its_live_token() {
    let path = unique_path("leased");
    let _ = std::fs::remove_file(&path);

    {
        let a = SqliteRelationalBackend::open(&path).unwrap();
        a.create_queue(qdef()).await.unwrap();
        a.push(&shard(), vec![PushSpec::default()], ts(0), None)
            .await
            .unwrap();
        let claimed = a.claim(claim_req(1, 500, 10)).await.unwrap();
        assert_eq!(claimed.items.len(), 1, "leased before crash");
        // Pre-crash the live token IS visible.
        assert_eq!(a.pending(&shard()).await.unwrap().len(), 1);
    } // crash

    let b = SqliteRelationalBackend::open(&path).unwrap();
    assert_eq!(
        b.metrics(&shard()).await.unwrap().leased,
        1,
        "leased state survives reopen"
    );
    assert!(
        b.pending(&shard()).await.unwrap().is_empty(),
        "the live token is gone after reopen -> the lease is omitted from pending() (documented contract)"
    );
    // Still reclaimable: the lease deadline survived, so the tick recovers the tokenless in-flight lease.
    b.tick(ts(501)).await.unwrap();
    let m = b.metrics(&shard()).await.unwrap();
    assert_eq!(
        (m.leased, m.pending),
        (0, 1),
        "reclaim tick recovers the tokenless lease after reopen"
    );
    let _ = std::fs::remove_file(&path);
}

/// Truncating the process and reopening the sqlite relational family preserves the durable work state:
/// items, leases, counters, request-id replay, fences, and metrics all come back from sqlite.
#[tokio::test]
async fn truncate_then_recover_exact_state() {
    let path = unique_path("exact-state");
    let _ = std::fs::remove_file(&path);
    let request_id = RequestId::new("replay-1").unwrap();
    let body = vec![
        PushSpec {
            priority: Some(PriorityValue::Int64(10)),
            ..Default::default()
        },
        PushSpec {
            priority: Some(PriorityValue::Int64(20)),
            ..Default::default()
        },
    ];
    let original_ids;
    let fence_epoch;

    {
        let a = SqliteRelationalBackend::open(&path).unwrap();
        a.create_queue(qdef()).await.unwrap();
        original_ids = a
            .push_with_request_id(&shard(), request_id.clone(), body.clone(), ts(0), None)
            .await
            .unwrap();

        let claimed = a.claim(claim_req(1, 500, 10)).await.unwrap();
        assert_eq!(claimed.items.len(), 1, "one leased item before reopen");
        fence_epoch = a.acquire_epoch(&shard()).await.unwrap();

        let metrics = a.metrics(&shard()).await.unwrap();
        assert_eq!((metrics.pending, metrics.leased), (1, 1));
        assert_eq!(a.current_epoch(&shard()).await.unwrap(), fence_epoch);
    } // crash

    let b = SqliteRelationalBackend::open(&path).unwrap();
    assert_eq!(
        b.current_epoch(&shard()).await.unwrap(),
        fence_epoch,
        "the durable fence epoch survives reopen"
    );
    assert_eq!(
        b.metrics(&shard()).await.unwrap(),
        pqueue_engine::QueueMetrics {
            pending: 1,
            leased: 1,
            complete: 0,
            failed: 0,
        },
        "the item lifecycle counts survive reopen"
    );
    assert!(
        b.pending(&shard()).await.unwrap().is_empty(),
        "the leased item's live token is dropped on reopen"
    );

    let replayed = b
        .push_with_request_id(&shard(), request_id, body, ts(1), None)
        .await
        .unwrap();
    assert_eq!(
        replayed, original_ids,
        "request-id replay returns the original item ids"
    );

    let fresh = b
        .push(&shard(), vec![PushSpec::default()], ts(2), None)
        .await
        .unwrap();
    assert_eq!(fresh.len(), 1);
    assert!(
        !original_ids.contains(&fresh[0]),
        "counter recovery must mint a fresh item id after reopen"
    );
    let _ = std::fs::remove_file(&path);
}
