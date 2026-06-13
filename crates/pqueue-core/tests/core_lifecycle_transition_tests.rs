// B-012: item lifecycle state machine — AC-CORE-2 / INV-3
//
// Exhaustive state×event matrix: every legal transition accepted, every
// illegal transition rejected with a typed TransitionError.

use pqueue_core::{ItemEvent, ItemState, TransitionError, apply_transition};

const ALL_STATES: &[ItemState] = &[
    ItemState::Pending,
    ItemState::Leased,
    ItemState::Complete,
    ItemState::Failed,
];

const ALL_EVENTS: &[ItemEvent] = &[
    ItemEvent::Claim,
    ItemEvent::FinalizeComplete,
    ItemEvent::FinalizeFail,
    ItemEvent::FinalizeRetry,
    ItemEvent::FinalizeRelease,
    ItemEvent::FinalizeRearm,
    ItemEvent::LeaseExpired,
];

// ---------------------------------------------------------------------------
// Legal transitions
// ---------------------------------------------------------------------------

#[test]
fn core_lifecycle_transition_tests_pending_claim_yields_leased() {
    assert_eq!(
        apply_transition(ItemState::Pending, ItemEvent::Claim),
        Ok(ItemState::Leased)
    );
}

#[test]
fn core_lifecycle_transition_tests_leased_finalize_complete_yields_complete() {
    assert_eq!(
        apply_transition(ItemState::Leased, ItemEvent::FinalizeComplete),
        Ok(ItemState::Complete)
    );
}

#[test]
fn core_lifecycle_transition_tests_leased_finalize_fail_yields_failed() {
    assert_eq!(
        apply_transition(ItemState::Leased, ItemEvent::FinalizeFail),
        Ok(ItemState::Failed)
    );
}

#[test]
fn core_lifecycle_transition_tests_leased_finalize_retry_yields_pending() {
    assert_eq!(
        apply_transition(ItemState::Leased, ItemEvent::FinalizeRetry),
        Ok(ItemState::Pending)
    );
}

#[test]
fn core_lifecycle_transition_tests_leased_finalize_release_yields_pending() {
    assert_eq!(
        apply_transition(ItemState::Leased, ItemEvent::FinalizeRelease),
        Ok(ItemState::Pending)
    );
}

#[test]
fn core_lifecycle_transition_tests_leased_finalize_rearm_yields_pending() {
    assert_eq!(
        apply_transition(ItemState::Leased, ItemEvent::FinalizeRearm),
        Ok(ItemState::Pending)
    );
}

#[test]
fn core_lifecycle_transition_tests_leased_lease_expired_yields_pending() {
    assert_eq!(
        apply_transition(ItemState::Leased, ItemEvent::LeaseExpired),
        Ok(ItemState::Pending)
    );
}

// ---------------------------------------------------------------------------
// INV-3: terminal states reject all events
// ---------------------------------------------------------------------------

#[test]
fn core_lifecycle_transition_tests_complete_rejects_all_events() {
    for &event in ALL_EVENTS {
        let result = apply_transition(ItemState::Complete, event);
        assert!(
            result.is_err(),
            "Complete + {:?} should be rejected, got Ok({:?})",
            event,
            result.unwrap()
        );
        let err = result.unwrap_err();
        assert_eq!(err.state, ItemState::Complete);
        assert_eq!(err.event, event);
    }
}

#[test]
fn core_lifecycle_transition_tests_failed_rejects_all_events() {
    for &event in ALL_EVENTS {
        let result = apply_transition(ItemState::Failed, event);
        assert!(
            result.is_err(),
            "Failed + {:?} should be rejected, got Ok({:?})",
            event,
            result.unwrap()
        );
        let err = result.unwrap_err();
        assert_eq!(err.state, ItemState::Failed);
        assert_eq!(err.event, event);
    }
}

// ---------------------------------------------------------------------------
// Exhaustive matrix: enumerate all (state, event) pairs
// ---------------------------------------------------------------------------

fn is_legal(state: ItemState, event: ItemEvent) -> bool {
    matches!(
        (state, event),
        (ItemState::Pending, ItemEvent::Claim)
            | (ItemState::Leased, ItemEvent::FinalizeComplete)
            | (ItemState::Leased, ItemEvent::FinalizeFail)
            | (ItemState::Leased, ItemEvent::FinalizeRetry)
            | (ItemState::Leased, ItemEvent::FinalizeRelease)
            | (ItemState::Leased, ItemEvent::FinalizeRearm)
            | (ItemState::Leased, ItemEvent::LeaseExpired)
    )
}

#[test]
fn core_lifecycle_transition_tests_exhaustive_matrix() {
    let mut legal_count = 0usize;
    let mut illegal_count = 0usize;

    for &state in ALL_STATES {
        for &event in ALL_EVENTS {
            let result = apply_transition(state, event);
            if is_legal(state, event) {
                assert!(
                    result.is_ok(),
                    "Expected legal: {:?} + {:?} should succeed, got {:?}",
                    state, event, result.unwrap_err()
                );
                legal_count += 1;
            } else {
                assert!(
                    result.is_err(),
                    "Expected illegal: {:?} + {:?} should fail, got Ok({:?})",
                    state, event, result.unwrap()
                );
                let err: TransitionError = result.unwrap_err();
                assert_eq!(err.state, state);
                assert_eq!(err.event, event);
                illegal_count += 1;
            }
        }
    }

    // 4 states × 7 events = 28 total; 7 legal, 21 illegal.
    assert_eq!(legal_count, 7, "expected exactly 7 legal transitions");
    assert_eq!(illegal_count, 21, "expected exactly 21 illegal transitions");
}

// ---------------------------------------------------------------------------
// is_terminal helper
// ---------------------------------------------------------------------------

#[test]
fn core_lifecycle_transition_tests_is_terminal_correct() {
    assert!(!ItemState::Pending.is_terminal());
    assert!(!ItemState::Leased.is_terminal());
    assert!(ItemState::Complete.is_terminal());
    assert!(ItemState::Failed.is_terminal());
}
