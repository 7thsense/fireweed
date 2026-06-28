//! C9 (epic pqueue-2201fd37) — durable-backend parity for the authoritative vectorized claimed-work commit
//! (Snorri StateStore boundary) on the DB-authoritative `SqliteRelationalBackend`.
//!
//! Proves, on "at least one durable backend" (acceptance #2/#5):
//! - one entry (valid claim_ref + opaque side record + dispatchable lifecycle item + finalize=Complete)
//!   commits atomically; the input is finalized (complete, not claimable); the side record is present and is
//!   NOT claimable/peekable/metrics-as-work; the lifecycle item is ordinary pending claimable work;
//! - the side record + finalized state SURVIVE a reopen (`open` the same file) — recovery / acceptance #5;
//! - a bad lease token -> Rejected(StaleLease); a bad item_version -> Rejected(Conflict); both write nothing;
//! - a request-id replay returns the prior outcomes with NO double-write.

use bytes::Bytes;
use pqueue_conformance::{qdef, shard};
use pqueue_core::{LeaseToken, PriorityValue, RequestId, UtcTimestamp, WorkerId};
use pqueue_engine::{
    ClaimCompatibility, ClaimPort, ClaimRef, ClaimRequest, CommitEntryOutcome, CommitTransition,
    CommitTransitionEntry, CommitTransitionPort, ControlPlaneStore, EngineError, FinalizeKind,
    ProjectionRead, PushPort, PushSpec, QueueKey, SideRecord,
};
use pqueue_sqlite::SqliteRelationalBackend;
use rusqlite::Connection;

fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}

fn unique_path(tag: &str) -> String {
    std::env::temp_dir()
        .join(format!("pqueue-rel-commit-{tag}-{}.db", std::process::id()))
        .to_str()
        .unwrap()
        .to_string()
}

fn claim_req(max: usize, exp: i64, now: i64) -> ClaimRequest {
    ClaimRequest {
        shard: shard(),
        worker_id: WorkerId::new("w1").unwrap(),
        max_items: max,
        lease_token: LeaseToken::new("lease-1").unwrap(),
        lease_expires_at: ts(exp),
        now: ts(now),
        compatibility: ClaimCompatibility::default(),
        expected_epoch: None,
    }
}

fn item(priority: i64) -> PushSpec {
    PushSpec {
        priority: Some(PriorityValue::Int64(priority)),
        ..Default::default()
    }
}

fn side(key: &str, payload: &str) -> SideRecord {
    SideRecord {
        key: key.as_bytes().to_vec(),
        payload: Bytes::copy_from_slice(payload.as_bytes()),
    }
}

/// Push one input item and claim it, returning the `ClaimRef` (id + lease token + version) the commit needs.
async fn push_and_claim(b: &SqliteRelationalBackend, now: i64) -> ClaimRef {
    b.push(&shard(), vec![item(10)], ts(now), None)
        .await
        .unwrap();
    let claimed = b.claim(claim_req(1, now + 600, now)).await.unwrap();
    assert_eq!(claimed.items.len(), 1, "exactly one item claimed");
    let c = &claimed.items[0];
    ClaimRef {
        item_id: c.item_id,
        lease_token: c.lease_token.clone().expect("claimed item carries a token"),
        lease_expires_at: c.lease_expires_at,
        item_version: c.item_version,
    }
}

/// Read a side record's payload straight from the durable table (no log, no in-RAM state) — the recovery
/// read that proves a side record is present after a fresh `open`.
fn read_side_record(path: &str, q: &QueueKey, key: &str) -> Option<Vec<u8>> {
    let conn = Connection::open(path).unwrap();
    conn.query_row(
        "SELECT payload FROM pqueue_side_records WHERE tenant_id=?1 AND queue_id=?2 AND key=?3",
        rusqlite::params![q.tenant_id.as_str(), q.queue_id.as_str(), key.as_bytes()],
        |row| row.get::<_, Vec<u8>>(0),
    )
    .ok()
}

fn count_side_records(path: &str) -> i64 {
    let conn = Connection::open(path).unwrap();
    conn.query_row("SELECT COUNT(*) FROM pqueue_side_records", [], |row| {
        row.get(0)
    })
    .unwrap()
}

/// Acceptance #2/#5: a valid entry commits atomically, the input finalizes, the side record is non-work-safe
/// + survives a reopen, and the lifecycle item is ordinary claimable work.
#[tokio::test]
async fn relational_commit_writes_side_records_enqueues_lifecycle_finalizes_and_survives_reopen() {
    let path = unique_path("survive");
    let _ = std::fs::remove_file(&path);
    let q = shard();
    let input_id;
    let lifecycle_id;

    {
        let b = SqliteRelationalBackend::open(&path).unwrap();
        b.create_queue(qdef()).await.unwrap();
        let claim_ref = push_and_claim(&b, 0).await;
        input_id = claim_ref.item_id;

        let outcomes = b
            .commit_transition(
                &q,
                CommitTransition {
                    request_id: Some(RequestId::new("txn-1").unwrap()),
                    entries: vec![CommitTransitionEntry {
                        claim_ref,
                        finalize: FinalizeKind::Complete,
                        side_records: vec![side("state/run-1", "audit-bytes")],
                        lifecycle_items: vec![item(20)],
                    }],
                },
                ts(1),
                None,
            )
            .await
            .unwrap();

        assert_eq!(outcomes.len(), 1);
        lifecycle_id = match &outcomes[0] {
            CommitEntryOutcome::Committed { lifecycle_item_ids } => {
                assert_eq!(lifecycle_item_ids.len(), 1, "one lifecycle item enqueued");
                lifecycle_item_ids[0]
            }
            other => panic!("expected Committed, got {other:?}"),
        };
        assert_ne!(lifecycle_id, input_id, "lifecycle item is a fresh item");

        // Input finalized to complete; lifecycle item is the only pending claimable work.
        let m = b.metrics(&q).await.unwrap();
        assert_eq!(
            (m.pending, m.leased, m.complete),
            (1, 0, 1),
            "input complete, lifecycle item pending, side record never counted as work"
        );
        let peeked = b.peek(&q, 10).await.unwrap();
        assert_eq!(peeked.len(), 1, "exactly the lifecycle item is peekable");
        assert_eq!(peeked[0].item_id, lifecycle_id);

        // The side record is present in its own table but is NOT claimable: claiming everything claimable
        // returns only the lifecycle item.
        assert_eq!(
            read_side_record(&path, &q, "state/run-1").as_deref(),
            Some(b"audit-bytes".as_slice())
        );
        let claimed = b.claim(claim_req(10, 600, 2)).await.unwrap();
        assert_eq!(
            claimed.items.len(),
            1,
            "only the lifecycle item is claimable"
        );
        assert_eq!(claimed.items[0].item_id, lifecycle_id);
    } // crash

    // Recovery (acceptance #5): reopen the same file. The side record + finalized input state survive from
    // the durable tables (no log replay).
    let b = SqliteRelationalBackend::open(&path).unwrap();
    assert_eq!(
        read_side_record(&path, &q, "state/run-1").as_deref(),
        Some(b"audit-bytes".as_slice()),
        "the opaque side record survives a reopen"
    );
    let m = b.metrics(&q).await.unwrap();
    assert_eq!(m.complete, 1, "the finalized input survives a reopen");
    // The side record is never resurrected as work: it stays out of pending/peek.
    let peeked = b.peek(&q, 10).await.unwrap();
    assert!(
        peeked.iter().all(|v| v.item_id != input_id),
        "the finalized input is not claimable/peekable after reopen"
    );

    let _ = std::fs::remove_file(&path);
}

/// Acceptance #2 (rejection arms): wrong lease token -> Rejected(StaleLease); wrong version ->
/// Rejected(Conflict). Nothing is written (input stays leased, no side record).
#[tokio::test]
async fn relational_commit_rejects_bad_token_and_bad_version_without_writing() {
    // Wrong lease token.
    {
        let path = unique_path("bad-token");
        let _ = std::fs::remove_file(&path);
        let q = shard();
        let b = SqliteRelationalBackend::open(&path).unwrap();
        b.create_queue(qdef()).await.unwrap();
        let mut claim_ref = push_and_claim(&b, 0).await;
        claim_ref.lease_token = LeaseToken::new("not-the-real-token").unwrap();

        let outcomes = b
            .commit_transition(
                &q,
                CommitTransition {
                    request_id: None,
                    entries: vec![CommitTransitionEntry {
                        claim_ref,
                        finalize: FinalizeKind::Complete,
                        side_records: vec![side("state/x", "v")],
                        lifecycle_items: vec![item(20)],
                    }],
                },
                ts(1),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            outcomes,
            vec![CommitEntryOutcome::Rejected(EngineError::StaleLease)]
        );
        let m = b.metrics(&q).await.unwrap();
        assert_eq!(
            (m.pending, m.leased, m.complete),
            (0, 1, 0),
            "bad token: input still leased, nothing enqueued"
        );
        assert_eq!(
            count_side_records(&path),
            0,
            "bad token: no side record written"
        );
        let _ = std::fs::remove_file(&path);
    }

    // Wrong item_version.
    {
        let path = unique_path("bad-version");
        let _ = std::fs::remove_file(&path);
        let q = shard();
        let b = SqliteRelationalBackend::open(&path).unwrap();
        b.create_queue(qdef()).await.unwrap();
        let mut claim_ref = push_and_claim(&b, 0).await;
        claim_ref.item_version += 99;

        let outcomes = b
            .commit_transition(
                &q,
                CommitTransition {
                    request_id: None,
                    entries: vec![CommitTransitionEntry {
                        claim_ref,
                        finalize: FinalizeKind::Complete,
                        side_records: vec![side("state/x", "v")],
                        lifecycle_items: vec![item(20)],
                    }],
                },
                ts(1),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            outcomes,
            vec![CommitEntryOutcome::Rejected(EngineError::Conflict)]
        );
        let m = b.metrics(&q).await.unwrap();
        assert_eq!(
            (m.pending, m.leased, m.complete),
            (0, 1, 0),
            "bad version: input still leased, nothing enqueued"
        );
        assert_eq!(
            count_side_records(&path),
            0,
            "bad version: no side record written"
        );
        let _ = std::fs::remove_file(&path);
    }
}

/// Acceptance #3: a same request body + request_id replays the prior outcomes with NO double-write.
#[tokio::test]
async fn relational_commit_request_id_replays_without_double_write() {
    let path = unique_path("replay");
    let _ = std::fs::remove_file(&path);
    let q = shard();
    let b = SqliteRelationalBackend::open(&path).unwrap();
    b.create_queue(qdef()).await.unwrap();
    let claim_ref = push_and_claim(&b, 0).await;
    let rid = RequestId::new("txn-replay-1").unwrap();
    let body = |cr: ClaimRef| CommitTransition {
        request_id: Some(rid.clone()),
        entries: vec![CommitTransitionEntry {
            claim_ref: cr,
            finalize: FinalizeKind::Complete,
            side_records: vec![side("state/run-1", "v1")],
            lifecycle_items: vec![item(20)],
        }],
    };

    let first = b
        .commit_transition(&q, body(claim_ref.clone()), ts(1), None)
        .await
        .unwrap();
    let lifecycle_id = match &first[0] {
        CommitEntryOutcome::Committed { lifecycle_item_ids } => lifecycle_item_ids[0],
        other => panic!("expected Committed, got {other:?}"),
    };

    // Replay: identical body + request_id returns the SAME outcomes and writes nothing new.
    let replay = b
        .commit_transition(&q, body(claim_ref.clone()), ts(1), None)
        .await
        .unwrap();
    assert_eq!(first, replay, "replay returns the prior per-entry outcomes");
    let m = b.metrics(&q).await.unwrap();
    assert_eq!(
        (m.pending, m.leased, m.complete),
        (1, 0, 1),
        "replay did not enqueue a second lifecycle item or re-finalize"
    );
    assert_eq!(
        count_side_records(&path),
        1,
        "replay did not duplicate the side record"
    );
    let peeked = b.peek(&q, 10).await.unwrap();
    assert_eq!(peeked.len(), 1);
    assert_eq!(peeked[0].item_id, lifecycle_id);

    // Different body, same request_id -> RequestIdConflict (whole call errors, nothing written).
    let conflict = b
        .commit_transition(
            &q,
            CommitTransition {
                request_id: Some(rid.clone()),
                entries: vec![CommitTransitionEntry {
                    claim_ref,
                    finalize: FinalizeKind::Fail, // different body
                    side_records: vec![side("state/run-1", "v1")],
                    lifecycle_items: vec![item(20)],
                }],
            },
            ts(1),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(conflict, EngineError::RequestIdConflict);
    assert_eq!(
        b.metrics(&q).await.unwrap().pending,
        1,
        "the conflicting body wrote nothing"
    );
    assert_eq!(
        count_side_records(&path),
        1,
        "the conflicting body wrote no side record"
    );

    let _ = std::fs::remove_file(&path);
}
