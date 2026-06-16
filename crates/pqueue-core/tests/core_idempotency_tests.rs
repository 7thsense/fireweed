// B-012: core idempotency semantics — AC-CORE-3 / INV-5
//
// Verifies the three idempotency rules:
//   1. Replayed request_id with identical body → Replay (no-op).
//   2. Replayed request_id with conflicting body → RequestIdConflict.
//   3. Duplicate client_item_key (non-terminal) → ClientItemKeyDuplicate.

use pqueue_core::{BodyHash, ClientItemKey, IdempotencyOutcome, RequestId, check_idempotency};

fn rid(s: &str) -> RequestId {
    RequestId::new(s).unwrap()
}

fn key(s: &str) -> ClientItemKey {
    ClientItemKey::new(s).unwrap()
}

fn hash(n: u64) -> BodyHash {
    BodyHash(n)
}

// ---------------------------------------------------------------------------
// 1. Novel request — no prior records
// ---------------------------------------------------------------------------

#[test]
fn core_idempotency_tests_new_request_proceeds() {
    let outcome = check_idempotency(&rid("r1"), hash(1), None, None, &key("item-1"));
    assert_eq!(outcome, IdempotencyOutcome::Proceed);
}

// ---------------------------------------------------------------------------
// 2. Replayed request_id with identical body → Replay
// ---------------------------------------------------------------------------

#[test]
fn core_idempotency_tests_replay_identical_body() {
    let outcome = check_idempotency(
        &rid("r1"),
        hash(42),
        Some((rid("r1"), hash(42))),
        None,
        &key("item-1"),
    );
    assert_eq!(outcome, IdempotencyOutcome::Replay);
}

#[test]
fn core_idempotency_tests_prior_request_different_id_does_not_replay() {
    // Prior request has a different request_id; should not trigger replay.
    let outcome = check_idempotency(
        &rid("r2"),
        hash(42),
        Some((rid("r1"), hash(42))),
        None,
        &key("item-1"),
    );
    assert_eq!(outcome, IdempotencyOutcome::Proceed);
}

// ---------------------------------------------------------------------------
// 3. Same request_id, conflicting body → RequestIdConflict
// ---------------------------------------------------------------------------

#[test]
fn core_idempotency_tests_conflicting_body_returns_conflict() {
    let outcome = check_idempotency(
        &rid("r1"),
        hash(99),
        Some((rid("r1"), hash(42))), // same rid, different hash
        None,
        &key("item-1"),
    );
    assert_eq!(outcome, IdempotencyOutcome::RequestIdConflict);
}

// ---------------------------------------------------------------------------
// 4. Duplicate client_item_key → ClientItemKeyDuplicate
// ---------------------------------------------------------------------------

#[test]
fn core_idempotency_tests_duplicate_client_item_key() {
    let outcome = check_idempotency(
        &rid("r2"),
        hash(1),
        None,
        Some(&key("item-1")), // prior item with same key
        &key("item-1"),
    );
    assert_eq!(outcome, IdempotencyOutcome::ClientItemKeyDuplicate);
}

#[test]
fn core_idempotency_tests_different_client_item_key_proceeds() {
    let outcome = check_idempotency(
        &rid("r2"),
        hash(1),
        None,
        Some(&key("item-99")), // different key
        &key("item-1"),
    );
    assert_eq!(outcome, IdempotencyOutcome::Proceed);
}

// ---------------------------------------------------------------------------
// 5. request_id conflict takes precedence over client_item_key duplicate
// ---------------------------------------------------------------------------

#[test]
fn core_idempotency_tests_request_id_conflict_takes_priority_over_key_duplicate() {
    // Both a conflicting request_id and a duplicate key are present.
    // request_id conflict should be returned first.
    let outcome = check_idempotency(
        &rid("r1"),
        hash(99),
        Some((rid("r1"), hash(42))),
        Some(&key("item-1")),
        &key("item-1"),
    );
    assert_eq!(outcome, IdempotencyOutcome::RequestIdConflict);
}

#[test]
fn core_idempotency_tests_replay_takes_priority_over_key_duplicate() {
    // Replay should be returned before client_item_key duplicate.
    let outcome = check_idempotency(
        &rid("r1"),
        hash(42),
        Some((rid("r1"), hash(42))),
        Some(&key("item-1")),
        &key("item-1"),
    );
    assert_eq!(outcome, IdempotencyOutcome::Replay);
}
