//! Regression for pqueue-29bef1e4 (P1 durability): a RELATIONAL composed `commit_transition` whose two
//! entries enqueue lifecycle items sharing the SAME unique typed-index key must REJECT the colliding entry
//! at VALIDATION (Conflict) — before the durable log append — so the appended batch is always APPLIABLE and
//! a reopen recovers cleanly (no recovery poison).
//!
//! Pre-fix behavior (the bug): the relational `ProjectionStore::index_validate_push` was a no-op, so the
//! collision was NOT caught at validation; typed-index uniqueness was enforced only at APPLY time. The
//! composed commit path (`commit_locked_batch`) appended the whole envelope batch DURABLY and THEN applied
//! it, so the second colliding Push passed validation, the batch was appended, apply failed, the projection
//! transaction rolled back, but the durable log append stayed — a durable-but-unappliable batch that every
//! reopen re-hits → RECOVERY POISON for that shard. These tests would FAIL pre-fix at the `recover()` call.

use axon_esf::IndexDef;
use fireweed_conformance::{claim_req, qdef, shard, ts};
use fireweed_core::{IndexDeclaration, IndexType, PriorityValue, QueueDefinition, QueueIndex};
use fireweed_engine::{
    ClaimPort, ClaimRef, CommitEntryOutcome, CommitTransition, CommitTransitionEntry,
    CommitTransitionPort, ControlPlaneStore, EngineError, FinalizeKind, ProjectionRead, PushPort,
    PushSpec,
};
use rusqlite::Connection;
use serde_json::json;

fn unique_paths(tag: &str) -> (String, String) {
    let base = std::env::temp_dir().join(format!(
        "fireweed-composed-dupidx-{tag}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    (
        format!("{}.log.db", base.to_str().unwrap()),
        format!("{}.proj.db", base.to_str().unwrap()),
    )
}

/// A queue def carrying a UNIQUE typed index on entity field `email` (mirrors `relational_conformance.rs`).
fn qdef_unique_email() -> QueueDefinition {
    QueueDefinition {
        typed_indexes: vec![QueueIndex {
            name: "by_email".to_string(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: "email".to_string(),
                index_type: IndexType::String,
                unique: true,
            }),
        }],
        ..qdef()
    }
}

/// A plain input push (no entity) — becomes a claimable input the commit consumes.
fn input(priority: i64) -> PushSpec {
    PushSpec {
        priority: Some(PriorityValue::Int64(priority)),
        ..Default::default()
    }
}

/// A lifecycle push carrying the unique-indexed `email` entity field.
fn lifecycle_with_email(priority: i64, email: &str) -> PushSpec {
    PushSpec {
        priority: Some(PriorityValue::Int64(priority)),
        entity: Some(json!({ "email": email })),
        ..Default::default()
    }
}

/// Count durable `fireweed_item_index` rows for the unique index in the projection db.
fn count_index_rows(projection_path: &str) -> i64 {
    let conn = Connection::open(projection_path).unwrap();
    conn.query_row(
        "SELECT COUNT(*) FROM fireweed_item_index WHERE index_name='by_email'",
        [],
        |row| row.get(0),
    )
    .unwrap()
}

/// The shared body: on a durable relational commit backend, a two-entry `commit_transition` whose entries
/// enqueue lifecycle items with the SAME unique `email` rejects the second entry at validation (Conflict),
/// commits the first, and leaves the durable log appliable. Returns nothing; asserts inline.
async fn run_reject<B>(backend: &B)
where
    B: ControlPlaneStore + ClaimPort + PushPort + ProjectionRead + CommitTransitionPort,
{
    let q = shard();
    backend.create_queue(qdef_unique_email()).await.unwrap();

    // Two claimable inputs, both claimed under one lease so we can build two claim_refs.
    backend
        .push(&q, vec![input(10), input(10)], ts(0), None)
        .await
        .unwrap();
    let claimed = backend.claim(claim_req(2, 600, 0)).await.unwrap();
    assert_eq!(claimed.items.len(), 2, "both inputs claimed");
    let claim_ref = |i: usize| ClaimRef {
        item_id: claimed.items[i].item_id,
        lease_token: claimed.items[i]
            .lease_token
            .clone()
            .expect("claimed item carries a token"),
        lease_expires_at: claimed.items[i].lease_expires_at,
        item_version: claimed.items[i].item_version,
    };

    // Two entries whose lifecycle items collide on the SAME unique typed-index key.
    let outcomes = backend
        .commit_transition(
            &q,
            CommitTransition {
                request_id: None,
                entries: vec![
                    CommitTransitionEntry {
                        claim_ref: claim_ref(0),
                        additional_claim_refs: Vec::new(),
                        finalize: FinalizeKind::Complete,
                        side_records: vec![],
                        lifecycle_items: vec![lifecycle_with_email(20, "dup@example.com")],
                        instance_fence: None,
                    },
                    CommitTransitionEntry {
                        claim_ref: claim_ref(1),
                        additional_claim_refs: Vec::new(),
                        finalize: FinalizeKind::Complete,
                        side_records: vec![],
                        lifecycle_items: vec![lifecycle_with_email(20, "dup@example.com")],
                        instance_fence: None,
                    },
                ],
            },
            ts(1),
            None,
        )
        .await
        .unwrap();

    // Acceptance #1: first entry commits, the colliding second entry is Rejected(Conflict) at VALIDATION.
    assert_eq!(outcomes.len(), 2);
    assert!(
        matches!(outcomes[0], CommitEntryOutcome::Committed { .. }),
        "first entry commits: {:?}",
        outcomes[0]
    );
    assert_eq!(
        outcomes[1],
        CommitEntryOutcome::Rejected(EngineError::Conflict),
        "the in-commit duplicate unique key is rejected at validation, not applied"
    );

    // The first entry's lifecycle item is the ONLY holder of the unique key (not two).
    assert_eq!(
        backend.metrics(&q).await.unwrap().pending,
        1,
        "exactly one lifecycle item enqueued"
    );
}

/// Composed sqlite-LOG + sqlite-PROJECTION backend (the `sqlite_log` durable relational family) — the exact
/// path the bead fingers: `SqliteProjectionStore::index_validate_push` used to be a no-op.
#[tokio::test]
async fn composed_sqlite_log_rejects_in_commit_duplicate_unique_key_and_reopens_clean() {
    let (log_path, proj_path) = unique_paths("sqlite-log");
    let _ = std::fs::remove_file(&log_path);
    let _ = std::fs::remove_file(&proj_path);

    {
        let backend =
            fireweed_sqlite::composed_sqlite_log_sqlite_projection(&log_path, &proj_path).unwrap();
        run_reject(&backend).await;
    } // drop the handle

    // Exactly one durable index row survives — the appended batch held only the appliable (first) entry.
    assert_eq!(
        count_index_rows(&proj_path),
        1,
        "only the committed entry's unique key is durable"
    );

    // Acceptance #1: reopen replays the durable log tail — pre-fix this re-hit the poisoned batch and errored.
    let reopened =
        fireweed_sqlite::composed_sqlite_log_sqlite_projection(&log_path, &proj_path).unwrap();
    // Reopen is CLEAN: the projected state is consistent (one lifecycle item, not two).
    assert_eq!(
        reopened.metrics(&shard()).await.unwrap().pending,
        1,
        "reopen recovers exactly one lifecycle item"
    );
    assert_eq!(
        count_index_rows(&proj_path),
        1,
        "still one unique key after reopen"
    );

    let _ = std::fs::remove_file(&log_path);
    let _ = std::fs::remove_file(&proj_path);
}

/// Composed UNIFIED sqlite-relational backend (`SqliteRelational` on both axes) — the other relational
/// composed commit path whose `index_validate_push` was a no-op.
#[tokio::test]
async fn composed_sqlite_relational_rejects_in_commit_duplicate_unique_key_and_reopens_clean() {
    let (path, _unused) = unique_paths("sqlite-rel");
    let _ = std::fs::remove_file(&path);

    {
        let backend = fireweed_sqlite::composed_sqlite_relational(&path).unwrap();
        run_reject(&backend).await;
    } // drop the handle

    // The DB-authoritative projection is its own store: count its durable index rows directly.
    assert_eq!(
        count_index_rows(&path),
        1,
        "only the committed entry's unique key is durable"
    );

    // Reopen (recovery-on-open) is CLEAN — no poisoned batch to replay.
    let reopened = fireweed_sqlite::composed_sqlite_relational(&path).unwrap();
    assert_eq!(
        reopened.metrics(&shard()).await.unwrap().pending,
        1,
        "reopen recovers exactly one lifecycle item"
    );

    let _ = std::fs::remove_file(&path);
}

/// Native `index_fields` (no entity JSON) must populate `fireweed_item_index`.
#[tokio::test]
async fn native_index_fields_write_typed_index_rows() {
    use fireweed_core::TypedValue;
    use std::collections::BTreeMap;

    let (log_path, proj_path) = unique_paths("native-idx");
    let _ = std::fs::remove_file(&log_path);
    let _ = std::fs::remove_file(&proj_path);
    let backend =
        fireweed_sqlite::composed_sqlite_log_sqlite_projection(&log_path, &proj_path).unwrap();
    backend.create_queue(qdef_unique_email()).await.unwrap();
    let ids = backend
        .push(
            &shard(),
            vec![PushSpec {
                index_fields: BTreeMap::from([("email".into(), TypedValue::String("n@x".into()))]),
                ..Default::default()
            }],
            ts(0),
            None,
        )
        .await
        .unwrap();
    assert_eq!(ids.len(), 1);
    assert_eq!(
        count_index_rows(&proj_path),
        1,
        "native index_fields must land in fireweed_item_index"
    );
    let _ = std::fs::remove_file(&log_path);
    let _ = std::fs::remove_file(&proj_path);
}
