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

use axon_esf::IndexDef;
use bytes::Bytes;
use fireweed_conformance::{qdef, shard};
use fireweed_core::{
    EntitySchemaDocument, IndexDeclaration, IndexType, ItemId, LeaseToken, PriorityValue,
    QueueIndex, RequestId, UtcTimestamp, WorkerId,
};
use fireweed_engine::{
    Backend, ClaimCompatibility, ClaimPort, ClaimRef, ClaimRequest, CommitEntryOutcome,
    CommitEntryStatus, CommitTransition, CommitTransitionEntry, CommitTransitionPort,
    ControlPlaneStore, EngineError, FinalizeKind, InstanceFence, ProjectionRead, PushPort,
    PushSpec, QueueKey, RecoveryReadPort, SideRecord,
};
use fireweed_sqlite::SqliteRelationalBackend;
use rusqlite::Connection;
use serde_json::json;

fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}

fn unique_path(tag: &str) -> String {
    std::env::temp_dir()
        .join(format!(
            "fireweed-rel-commit-{tag}-{}.db",
            std::process::id()
        ))
        .to_str()
        .unwrap()
        .to_string()
}

fn claim_req(max: usize, exp: i64, now: i64) -> ClaimRequest {
    ClaimRequest {
        eligibility_time: None,
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

fn typed_qdef() -> fireweed_core::QueueDefinition {
    let mut def = qdef();
    def.entity_schema = Some(
        serde_json::from_value::<EntitySchemaDocument>(json!({
            "entity_schema": {
                "type": "object",
                "required": ["name"],
                "properties": {
                    "name": {"type": "string"}
                }
            }
        }))
        .unwrap(),
    );
    def
}

fn typed_item(priority: i64, valid: bool) -> PushSpec {
    PushSpec {
        priority: Some(PriorityValue::Int64(priority)),
        entity: Some(if valid {
            json!({"name": "ok"})
        } else {
            json!({"count": 1})
        }),
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
        "SELECT payload FROM fireweed_side_records WHERE tenant_id=?1 AND queue_id=?2 AND key=?3",
        rusqlite::params![q.tenant_id.as_str(), q.queue_id.as_str(), key.as_bytes()],
        |row| row.get::<_, Vec<u8>>(0),
    )
    .ok()
}

fn count_side_records(path: &str) -> i64 {
    let conn = Connection::open(path).unwrap();
    conn.query_row("SELECT COUNT(*) FROM fireweed_side_records", [], |row| {
        row.get(0)
    })
    .unwrap()
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
                        additional_claim_refs: Vec::new(),
                        finalize: FinalizeKind::Complete,
                        side_records: vec![side("state/x", "v")],
                        lifecycle_items: vec![item(20)],
                        instance_fence: None,
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
                        additional_claim_refs: Vec::new(),
                        finalize: FinalizeKind::Complete,
                        side_records: vec![side("state/x", "v")],
                        lifecycle_items: vec![item(20)],
                        instance_fence: None,
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
            additional_claim_refs: Vec::new(),
            finalize: FinalizeKind::Complete,
            side_records: vec![side("state/run-1", "v1")],
            lifecycle_items: vec![item(20)],
            instance_fence: None,
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
                    additional_claim_refs: Vec::new(),
                    finalize: FinalizeKind::Fail, // different body
                    side_records: vec![side("state/run-1", "v1")],
                    lifecycle_items: vec![item(20)],
                    instance_fence: None,
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

fn read_instance_fence(path: &str, q: &QueueKey, key: &[u8]) -> Option<i64> {
    let conn = Connection::open(path).unwrap();
    conn.query_row(
        "SELECT fence FROM fireweed_instance_fences WHERE tenant_id=?1 AND queue_id=?2 AND instance_key=?3",
        rusqlite::params![q.tenant_id.as_str(), q.queue_id.as_str(), key],
        |row| row.get::<_, i64>(0),
    )
    .ok()
}

/// Durable command sequence high-water mark (`relational_cursor.next_seq`) — one tick per
/// `apply_command_sql` call inside `commit_transition`. Reading the delta across a single commit
/// counts how many command groups that commit applied, independent of wall-clock timing.
fn read_next_seq(path: &str, q: &QueueKey) -> i64 {
    let conn = Connection::open(path).unwrap();
    conn.query_row(
        "SELECT next_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
        rusqlite::params![q.tenant_id.as_str(), q.queue_id.as_str()],
        |row| row.get::<_, i64>(0),
    )
    .unwrap()
}

/// C6 (durable): an entry advancing `expected -> next` succeeds and the durable fence is `next`; a stale
/// `expected` -> Rejected(Conflict) (nothing written: fence unchanged, side record absent, input still
/// leased); a non-monotonic `next <= expected` -> Rejected(Invalid).
#[tokio::test]
async fn relational_commit_advances_validates_and_rejects_instance_fence() {
    let path = unique_path("fence");
    let _ = std::fs::remove_file(&path);
    let q = shard();
    let key = b"instance/run-1".to_vec();
    let b = SqliteRelationalBackend::open(&path).unwrap();
    b.create_queue(qdef()).await.unwrap();

    // First transition: stored fence unset (== 0). expected=0 -> next=1 commits and advances.
    let cr1 = push_and_claim(&b, 0).await;
    let outcomes = b
        .commit_transition(
            &q,
            CommitTransition {
                request_id: Some(RequestId::new("fence-1").unwrap()),
                entries: vec![CommitTransitionEntry {
                    claim_ref: cr1,
                    additional_claim_refs: Vec::new(),
                    finalize: FinalizeKind::Complete,
                    side_records: vec![side("state/run-1", "v1")],
                    lifecycle_items: vec![],
                    instance_fence: Some(InstanceFence {
                        instance_key: key.clone(),
                        expected: 0,
                        next: 1,
                    }),
                }],
            },
            ts(1),
            None,
        )
        .await
        .unwrap();
    assert!(matches!(outcomes[0], CommitEntryOutcome::Committed { .. }));
    assert_eq!(
        read_instance_fence(&path, &q, &key),
        Some(1),
        "fence advanced to 1"
    );

    // STALE expected (stored is 1, caller presents 0) -> Conflict, nothing written.
    let cr2 = push_and_claim(&b, 2).await;
    let stale = b
        .commit_transition(
            &q,
            CommitTransition {
                request_id: None,
                entries: vec![CommitTransitionEntry {
                    claim_ref: cr2,
                    additional_claim_refs: Vec::new(),
                    finalize: FinalizeKind::Complete,
                    side_records: vec![side("state/should-not-write", "x")],
                    lifecycle_items: vec![item(20)],
                    instance_fence: Some(InstanceFence {
                        instance_key: key.clone(),
                        expected: 0,
                        next: 2,
                    }),
                }],
            },
            ts(3),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        stale,
        vec![CommitEntryOutcome::Rejected(EngineError::Conflict)]
    );
    assert_eq!(
        read_instance_fence(&path, &q, &key),
        Some(1),
        "stale fence: unchanged"
    );
    assert!(
        read_side_record(&path, &q, "state/should-not-write").is_none(),
        "stale: no side record"
    );
    let m = b.metrics(&q).await.unwrap();
    assert_eq!(
        (m.pending, m.leased),
        (0, 1),
        "stale fence: input still leased, nothing enqueued"
    );

    // NON-MONOTONIC (stored 1; expected=1, next=1) -> Invalid.
    let cr3 = push_and_claim(&b, 4).await;
    let nonmono = b
        .commit_transition(
            &q,
            CommitTransition {
                request_id: None,
                entries: vec![CommitTransitionEntry {
                    claim_ref: cr3,
                    additional_claim_refs: Vec::new(),
                    finalize: FinalizeKind::Complete,
                    side_records: vec![],
                    lifecycle_items: vec![],
                    instance_fence: Some(InstanceFence {
                        instance_key: key.clone(),
                        expected: 1,
                        next: 1,
                    }),
                }],
            },
            ts(5),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        nonmono,
        vec![CommitEntryOutcome::Rejected(EngineError::Invalid(
            "instance fence is not monotonic"
        ))]
    );

    let _ = std::fs::remove_file(&path);
}

/// C7 (durable): the relational backend advertises the full authoritative-commit capability set.
#[tokio::test]
async fn relational_advertises_full_commit_capabilities() {
    let path = unique_path("caps");
    let _ = std::fs::remove_file(&path);
    let b = SqliteRelationalBackend::open(&path).unwrap();
    let caps = b.commit_capabilities();
    assert!(caps.atomic_transition_commit);
    assert!(caps.vectorized_commit);
    assert!(caps.lease_validation);
    assert!(caps.retained_commit_idempotency);
    assert!(caps.non_work_side_records);
    assert!(caps.authoritative_recovery_reads);
    let _ = std::fs::remove_file(&path);
}

/// C8 (durable): after a successful commit, `explain_commit(request_id)` reconstructs the transition and
/// `side_record(key)` returns the bytes — BOTH survive a reopen (recovery from durable tables, acceptance #5).
#[tokio::test]
async fn relational_explain_commit_recovers_transition_and_survives_reopen() {
    let path = unique_path("explain");
    let _ = std::fs::remove_file(&path);
    let q = shard();
    let rid = RequestId::new("recover-1").unwrap();
    let instance_key = b"instance/run-1".to_vec();
    let input_id;
    let lifecycle_id;

    {
        let b = SqliteRelationalBackend::open(&path).unwrap();
        b.create_queue(qdef()).await.unwrap();
        let cr = push_and_claim(&b, 0).await;
        input_id = cr.item_id;
        let outcomes = b
            .commit_transition(
                &q,
                CommitTransition {
                    request_id: Some(rid.clone()),
                    entries: vec![CommitTransitionEntry {
                        claim_ref: cr,
                        additional_claim_refs: Vec::new(),
                        finalize: FinalizeKind::Complete,
                        side_records: vec![side("audit/run-1", "audit-bytes")],
                        lifecycle_items: vec![item(20)],
                        instance_fence: Some(InstanceFence {
                            instance_key: instance_key.clone(),
                            expected: 0,
                            next: 5,
                        }),
                    }],
                },
                ts(1),
                None,
            )
            .await
            .unwrap();
        lifecycle_id = match &outcomes[0] {
            CommitEntryOutcome::Committed { lifecycle_item_ids } => lifecycle_item_ids[0],
            other => panic!("expected Committed, got {other:?}"),
        };
    } // drop the handle

    // Reopen the same file: recovery comes entirely from durable tables.
    let b = SqliteRelationalBackend::open(&path).unwrap();
    let recovery = b
        .explain_commit(&q, rid.clone())
        .await
        .unwrap()
        .expect("record survives reopen");
    assert_eq!(recovery.request_id, rid);
    assert_eq!(recovery.entries.len(), 1);
    let e = &recovery.entries[0];
    assert_eq!(e.consumed_input_id, input_id);
    assert_eq!(e.instance, Some((instance_key.clone(), 5)));
    // fireweed-bf03cbf5: no longer retained in the durable outcome — see
    // `fireweed_engine::EntryRecovery::side_record_keys`. The bytes themselves are still recovered via
    // `side_record(key)` below.
    assert_eq!(e.side_record_keys, Vec::<Vec<u8>>::new());
    assert_eq!(e.lifecycle_item_ids, vec![lifecycle_id]);
    assert_eq!(e.status, CommitEntryStatus::Committed);

    // side_record(key) returns the bytes after reopen.
    assert_eq!(
        b.side_record(&q, b"audit/run-1").await.unwrap().as_deref(),
        Some(&b"audit-bytes"[..])
    );

    // The input is finalized + not re-claimable; the side record is not claimable/peekable work.
    assert_eq!(b.metrics(&q).await.unwrap().complete, 1);
    let claimed = b.claim(claim_req(10, 600, 2)).await.unwrap();
    assert_eq!(
        claimed.items.len(),
        1,
        "only the lifecycle item is claimable"
    );
    assert_eq!(claimed.items[0].item_id, lifecycle_id);

    let _ = std::fs::remove_file(&path);
}

/// Bead fireweed-e47e9287: `side_records_by_prefix` reads back one instance's audit chain in key order,
/// stays isolated from a sibling instance's records under a different prefix, pages via `next_cursor`, and
/// survives a reopen (recovery from the durable `fireweed_side_records` table, same as point-get
/// `side_record`).
#[tokio::test]
async fn relational_side_records_by_prefix_pages_ordered_and_survives_reopen() {
    let path = unique_path("prefix-scan");
    let _ = std::fs::remove_file(&path);
    let q = shard();

    {
        let b = SqliteRelationalBackend::open(&path).unwrap();
        b.create_queue(qdef()).await.unwrap();
        let cr = push_and_claim(&b, 0).await;
        b.commit_transition(
            &q,
            CommitTransition {
                request_id: Some(RequestId::new("prefix-scan-1").unwrap()),
                entries: vec![CommitTransitionEntry {
                    claim_ref: cr,
                    additional_claim_refs: Vec::new(),
                    finalize: FinalizeKind::Complete,
                    side_records: vec![
                        side("audit:instance-1:001", "a1"),
                        side("audit:instance-1:003", "a3"),
                        side("audit:instance-1:002", "a2"),
                        side("audit:instance-2:001", "other-instance"),
                    ],
                    lifecycle_items: vec![],
                    instance_fence: None,
                }],
            },
            ts(1),
            None,
        )
        .await
        .unwrap();
    } // drop the handle

    // Reopen the same file: the scan reads from durable tables, not an in-process cache.
    let b = SqliteRelationalBackend::open(&path).unwrap();

    let first_page = b
        .side_records_by_prefix(&q, b"audit:instance-1:", 2, None)
        .await
        .unwrap();
    assert_eq!(
        first_page.entries,
        vec![
            (b"audit:instance-1:001".to_vec(), Bytes::from_static(b"a1")),
            (b"audit:instance-1:002".to_vec(), Bytes::from_static(b"a2")),
        ]
    );
    let cursor = first_page
        .next_cursor
        .clone()
        .expect("a third matching entry remains");
    assert_eq!(cursor, b"audit:instance-1:003".to_vec());

    let second_page = b
        .side_records_by_prefix(&q, b"audit:instance-1:", 2, Some(cursor))
        .await
        .unwrap();
    assert_eq!(
        second_page.entries,
        vec![(b"audit:instance-1:003".to_vec(), Bytes::from_static(b"a3"))]
    );
    assert_eq!(
        second_page.next_cursor, None,
        "the prefix's key range is exhausted"
    );

    // A sibling instance's records under a different prefix are excluded entirely.
    let other = b
        .side_records_by_prefix(&q, b"audit:instance-2:", 10, None)
        .await
        .unwrap();
    assert_eq!(
        other.entries,
        vec![(
            b"audit:instance-2:001".to_vec(),
            Bytes::from_static(b"other-instance")
        )]
    );

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn relational_multi_claim_commit_replays_and_survives_reopen() {
    let path = unique_path("multi-claim-reopen");
    let _ = std::fs::remove_file(&path);
    let q = shard();
    let rid = RequestId::new("result-await-continuation-sqlite").unwrap();
    let primary_id;
    let additional_id;
    let continuation_id;

    {
        let backend = SqliteRelationalBackend::open(&path).unwrap();
        backend.create_queue(qdef()).await.unwrap();
        backend
            .push(&q, vec![item(10), item(11)], ts(0), None)
            .await
            .unwrap();
        let claimed = backend.claim(claim_req(2, 600, 0)).await.unwrap();
        assert_eq!(claimed.items.len(), 2);
        let to_ref = |item: &fireweed_engine::ClaimedItem| ClaimRef {
            item_id: item.item_id,
            lease_token: item
                .lease_token
                .clone()
                .expect("claimed item carries token"),
            lease_expires_at: item.lease_expires_at,
            item_version: item.item_version,
        };
        let primary = to_ref(&claimed.items[0]);
        let additional = to_ref(&claimed.items[1]);
        primary_id = primary.item_id;
        additional_id = additional.item_id;
        let body = CommitTransition {
            request_id: Some(rid.clone()),
            entries: vec![CommitTransitionEntry {
                claim_ref: primary,
                additional_claim_refs: vec![additional],
                finalize: FinalizeKind::Complete,
                side_records: vec![side("instance/result-await", "revision-2")],
                lifecycle_items: vec![item(20)],
                instance_fence: Some(InstanceFence {
                    instance_key: b"result-await".to_vec(),
                    expected: 0,
                    next: 2,
                }),
            }],
        };
        let first = backend
            .commit_transition(&q, body.clone(), ts(1), None)
            .await
            .unwrap();
        let replay = backend
            .commit_transition(&q, body, ts(2), None)
            .await
            .unwrap();
        assert_eq!(replay, first);
        continuation_id = match &first[0] {
            CommitEntryOutcome::Committed { lifecycle_item_ids } => lifecycle_item_ids[0],
            other => panic!("expected committed multi-claim entry, got {other:?}"),
        };
        assert_eq!(backend.metrics(&q).await.unwrap().complete, 2);
    }

    let reopened = SqliteRelationalBackend::open(&path).unwrap();
    let recovery = reopened
        .explain_commit(&q, rid)
        .await
        .unwrap()
        .expect("multi-claim recovery survives reopen");
    assert_eq!(recovery.entries[0].consumed_input_id, primary_id);
    assert_eq!(
        recovery.entries[0].additional_consumed_input_ids,
        vec![additional_id]
    );
    assert_eq!(
        reopened
            .side_record(&q, b"instance/result-await")
            .await
            .unwrap()
            .as_deref(),
        Some(b"revision-2".as_slice())
    );
    let claimed = reopened.claim(claim_req(10, 600, 3)).await.unwrap();
    assert_eq!(claimed.items.len(), 1);
    assert_eq!(claimed.items[0].item_id, continuation_id);
    assert!(
        reopened
            .claim(claim_req(10, 600, 4))
            .await
            .unwrap()
            .items
            .is_empty()
    );
    let _ = std::fs::remove_file(&path);
}

async fn schema_validation_backend<B>(backend: &B)
where
    B: ControlPlaneStore
        + ClaimPort
        + PushPort
        + ProjectionRead
        + RecoveryReadPort
        + CommitTransitionPort,
{
    let q = shard();
    backend.create_queue(typed_qdef()).await.unwrap();

    let err = backend
        .push(&q, vec![typed_item(1, false)], ts(0), None)
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::EntitySchemaViolation(_)));
    assert_eq!(backend.metrics(&q).await.unwrap().pending, 0);

    let rid = RequestId::new("req-1").unwrap();
    let err = backend
        .push_with_request_id(&q, rid.clone(), vec![typed_item(1, false)], ts(1), None)
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::EntitySchemaViolation(_)));
    assert_eq!(backend.metrics(&q).await.unwrap().pending, 0);

    let first = backend
        .push_with_request_id(&q, rid.clone(), vec![typed_item(1, true)], ts(2), None)
        .await
        .unwrap();
    let replay = backend
        .push_with_request_id(&q, rid, vec![typed_item(1, true)], ts(3), None)
        .await
        .unwrap();
    assert!(first.is_fresh());
    assert!(replay.is_replayed());
    assert_eq!(first.item_ids, replay.item_ids);
    assert_eq!(backend.metrics(&q).await.unwrap().pending, 1);

    backend.push(&q, vec![item(5)], ts(4), None).await.unwrap();
    let claimed = backend.claim(claim_req(1, 600, 4)).await.unwrap();
    let claim_ref = ClaimRef {
        item_id: claimed.items[0].item_id,
        lease_token: claimed.items[0]
            .lease_token
            .clone()
            .expect("claimed item carries a token"),
        lease_expires_at: claimed.items[0].lease_expires_at,
        item_version: claimed.items[0].item_version,
    };
    let outcomes = backend
        .commit_transition(
            &q,
            CommitTransition {
                request_id: None,
                entries: vec![CommitTransitionEntry {
                    claim_ref,
                    additional_claim_refs: Vec::new(),
                    finalize: FinalizeKind::Complete,
                    side_records: vec![side("schema/run-1", "bytes")],
                    lifecycle_items: vec![typed_item(20, false)],
                    instance_fence: None,
                }],
            },
            ts(5),
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        outcomes[0],
        CommitEntryOutcome::Rejected(EngineError::EntitySchemaViolation(_))
    ));
    assert_eq!(backend.metrics(&q).await.unwrap().leased, 1);
    assert!(
        backend
            .side_record(&q, b"schema/run-1")
            .await
            .unwrap()
            .is_none(),
        "invalid lifecycle items must reject before side records are written"
    );
}

#[tokio::test]
async fn schema_validation_rejects_before_append_and_idempotency_on_sqlite_relational() {
    let path = unique_path("schema-relational");
    let _ = std::fs::remove_file(&path);
    let backend = SqliteRelationalBackend::open(&path).unwrap();
    schema_validation_backend(&backend).await;
    let _ = std::fs::remove_file(&path);
}

/// fireweed-4ba1dfd7 (lineage: fireweed-6bfe48ca revert c6c411ff, decision record
/// docs/perf/evidence/tp005/relational-commit-transition-per-entry-apply-decision.md): pin the
/// restored per-entry apply shape on `commit_transition` behaviorally (no timing dependency), so a
/// future re-landing of all-entries bulk-apply coalescing on side records / fences / lifecycle
/// pushes fails this test even though the perf amortization gates only run under `--release`.
///
/// Each entry's side record, instance fence, and lifecycle push are independently attributable
/// (entry i's own key/value/fence, never merged with entry j's), and the durable command-sequence
/// high-water mark (`relational_cursor.next_seq`) advances by exactly `3*N + 1` for N entries that
/// each carry a side record + fence + lifecycle item: 3 per-entry command groups (side records,
/// fence, lifecycle Push stay entry-scoped) plus ONE coalesced `Finalize` envelope for the whole
/// body (the accepted, evidenced-safe optimization from 00f3bd8b). Bulk-coalescing side/fence/push
/// back across entries collapses this count toward O(1) instead of O(N); the assertion catches
/// that regardless of wall-clock noise.
#[tokio::test]
async fn relational_commit_transition_applies_each_entry_independently() {
    const N: usize = 6;
    let path = unique_path("per-entry-independent");
    let _ = std::fs::remove_file(&path);
    let q = shard();
    let b = SqliteRelationalBackend::open(&path).unwrap();
    b.create_queue(qdef()).await.unwrap();

    let mut claim_refs = Vec::with_capacity(N);
    for _ in 0..N {
        claim_refs.push(push_and_claim(&b, 0).await);
    }

    let entries: Vec<CommitTransitionEntry> = claim_refs
        .into_iter()
        .enumerate()
        .map(|(i, claim_ref)| CommitTransitionEntry {
            claim_ref,
            additional_claim_refs: Vec::new(),
            finalize: FinalizeKind::Complete,
            side_records: vec![side(&format!("state/entry-{i}"), &format!("payload-{i}"))],
            lifecycle_items: vec![item(20 + i as i64)],
            instance_fence: Some(InstanceFence {
                instance_key: format!("instance/entry-{i}").into_bytes(),
                expected: 0,
                next: 1,
            }),
        })
        .collect();

    let before_seq = read_next_seq(&path, &q);
    let outcomes = b
        .commit_transition(
            &q,
            CommitTransition {
                request_id: None,
                entries,
            },
            ts(1),
            None,
        )
        .await
        .unwrap();
    let after_seq = read_next_seq(&path, &q);

    assert_eq!(
        outcomes.len(),
        N,
        "outcome cardinality must equal the entry count"
    );
    for (i, outcome) in outcomes.iter().enumerate() {
        match outcome {
            CommitEntryOutcome::Committed { lifecycle_item_ids } => {
                assert_eq!(
                    lifecycle_item_ids.len(),
                    1,
                    "entry {i} must produce exactly its own lifecycle item"
                );
            }
            other => panic!("entry {i} expected Committed, got {other:?}"),
        }
    }

    // Independent attributability: entry i's own side record and fence, never another entry's.
    for i in 0..N {
        assert_eq!(
            read_side_record(&path, &q, &format!("state/entry-{i}")),
            Some(format!("payload-{i}").into_bytes()),
            "entry {i} side record must be independently attributable"
        );
        assert_eq!(
            read_instance_fence(&path, &q, format!("instance/entry-{i}").as_bytes()),
            Some(1),
            "entry {i} fence must be independently advanced"
        );
    }
    assert_eq!(
        count_side_records(&path),
        N as i64,
        "no side records were merged or dropped across entries"
    );

    // Behavioral (non-timing) pin on the applied-command shape: see doc comment above.
    let applied_commands = after_seq - before_seq;
    assert_eq!(
        applied_commands,
        3 * N as i64 + 1,
        "expected 3 per-entry command groups (side records, fence, lifecycle push) per entry \
         plus one coalesced Finalize for the whole body; got {applied_commands} command groups \
         for {N} entries -- cross-entry coalescing (or de-coalescing) of side records, fences, \
         lifecycle pushes, or finalizes changed the applied-command shape restored by c6c411ff"
    );

    let _ = std::fs::remove_file(&path);
}

/// Snorri-shaped queue definition (19 typed indexes, one unique) matching the regressing shape in
/// docs/perf/evidence/tp005/commit-amortization-latest.md (HOLD fireweed-6bfe48ca): 19 typed
/// indexes, entity documents, ~2.3 KB payloads, 500-entry commit batches.
fn snorri_shape_qdef(n_indexes: usize) -> fireweed_core::QueueDefinition {
    let mut def = qdef();
    def.max_push_batch_size = 10_000;
    def.max_claim_batch_size = 10_000;
    def.typed_indexes = (0..n_indexes)
        .map(|i| QueueIndex {
            name: format!("by_f{i}"),
            declaration: IndexDeclaration::Single(IndexDef {
                field: format!("f{i}"),
                index_type: IndexType::String,
                unique: i == 0,
            }),
        })
        .collect();
    def
}

/// fireweed-4ba1dfd7: at the exact batch size that regressed snorri (500 entries/commit, 19 typed
/// indexes, entity documents, ~2.3 KB payloads), `CommitEntryOutcome` order and per-entry content
/// must match input entry order -- proving the restored per-entry path preserves per-entry identity
/// at the regressing shape, not just at small N (see the 6-entry cardinality/attribution test
/// above). Ground truth: each entry's lifecycle item carries a distinct ascending priority equal to
/// its entry index, so re-claiming under `OrderingMode::Strict` returns the newly-pushed items in
/// exactly entry order; if outcomes were reordered or cross-entry-coalesced this element-wise
/// comparison would fail even though the outcome COUNT still matched N.
#[tokio::test]
async fn relational_commit_transition_per_entry_outcomes_are_order_stable() {
    const N: usize = 500;
    const N_INDEXES: usize = 19;
    const PAYLOAD_BYTES: usize = 2300;

    let path = unique_path("order-stable-snorri");
    let _ = std::fs::remove_file(&path);
    let def = snorri_shape_qdef(N_INDEXES);
    let q = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    let b = SqliteRelationalBackend::open(&path).unwrap();
    b.create_queue(def).await.unwrap();

    let payload = Bytes::from(vec![b'x'; PAYLOAD_BYTES]);
    let inputs: Vec<PushSpec> = (0..N)
        .map(|_| PushSpec {
            payload: Some(payload.clone()),
            ..Default::default()
        })
        .collect();
    b.push(&q, inputs, ts(0), None).await.unwrap();

    let claimed = b.claim(claim_req(N, 600, 0)).await.unwrap();
    assert_eq!(claimed.items.len(), N, "claim batch size");

    let entries: Vec<CommitTransitionEntry> = claimed
        .items
        .iter()
        .enumerate()
        .map(|(i, claimed_item)| {
            let mut entity = serde_json::Map::new();
            entity.insert("f0".into(), json!(format!("k-{i}")));
            for f in 1..N_INDEXES {
                entity.insert(format!("f{f}"), json!(format!("v{f}-{i}")));
            }
            CommitTransitionEntry {
                claim_ref: ClaimRef {
                    item_id: claimed_item.item_id,
                    lease_token: claimed_item
                        .lease_token
                        .clone()
                        .expect("claimed item carries a token"),
                    lease_expires_at: claimed_item.lease_expires_at,
                    item_version: claimed_item.item_version,
                },
                additional_claim_refs: Vec::new(),
                finalize: FinalizeKind::Complete,
                side_records: vec![],
                lifecycle_items: vec![PushSpec {
                    priority: Some(PriorityValue::Int64(i as i64)),
                    entity: Some(serde_json::Value::Object(entity)),
                    payload: Some(payload.clone()),
                    ..Default::default()
                }],
                instance_fence: None,
            }
        })
        .collect();

    let outcomes = b
        .commit_transition(
            &q,
            CommitTransition {
                request_id: None,
                entries,
            },
            ts(1),
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        outcomes.len(),
        N,
        "outcome cardinality must equal entry count at the regressing batch size"
    );
    let outcome_ids: Vec<ItemId> = outcomes
        .iter()
        .enumerate()
        .map(|(i, o)| match o {
            CommitEntryOutcome::Committed { lifecycle_item_ids } => {
                assert_eq!(
                    lifecycle_item_ids.len(),
                    1,
                    "entry {i} must produce exactly its own lifecycle item"
                );
                lifecycle_item_ids[0]
            }
            other => panic!("entry {i} expected Committed, got {other:?}"),
        })
        .collect();

    let reclaimed = b.claim(claim_req(N, 605, 5)).await.unwrap();
    assert_eq!(
        reclaimed.items.len(),
        N,
        "all 500 lifecycle items claimable"
    );
    let reclaimed_ids: Vec<ItemId> = reclaimed.items.iter().map(|it| it.item_id).collect();
    assert_eq!(
        outcome_ids, reclaimed_ids,
        "CommitEntryOutcome order must match input entry order at 500 entries/commit \
         (snorri-shaped: 19 typed indexes, entity documents, ~2.3 KB payloads)"
    );

    let _ = std::fs::remove_file(&path);
}
