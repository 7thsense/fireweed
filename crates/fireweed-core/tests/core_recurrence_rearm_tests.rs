// B-012: retry exhaustion and recurrence rearm — AC-CORE-4
//
// Verifies:
//   - After max_attempts failures, the next failure becomes terminal (FinalizeFail → Failed).
//   - Rearm resets the per-cycle retry counter.
//   - The pure `is_retry_exhausted` and `failure_event` helpers are correct.

use fireweed_core::{ItemEvent, ItemState, apply_transition, failure_event, is_retry_exhausted};

// ---------------------------------------------------------------------------
// is_retry_exhausted
// ---------------------------------------------------------------------------

#[test]
fn core_recurrence_rearm_tests_not_exhausted_before_max() {
    assert!(!is_retry_exhausted(0, 1));
    assert!(!is_retry_exhausted(0, 3));
    assert!(!is_retry_exhausted(2, 3));
    assert!(!is_retry_exhausted(9, 10));
}

#[test]
fn core_recurrence_rearm_tests_exhausted_at_max() {
    assert!(is_retry_exhausted(1, 1));
    assert!(is_retry_exhausted(3, 3));
    assert!(is_retry_exhausted(10, 10));
}

#[test]
fn core_recurrence_rearm_tests_exhausted_beyond_max() {
    assert!(is_retry_exhausted(2, 1));
    assert!(is_retry_exhausted(4, 3));
    assert!(is_retry_exhausted(11, 10));
}

// ---------------------------------------------------------------------------
// failure_event
// ---------------------------------------------------------------------------

#[test]
fn core_recurrence_rearm_tests_failure_event_before_max_is_retry() {
    assert_eq!(failure_event(0, 3), ItemEvent::FinalizeRetry);
    assert_eq!(failure_event(1, 3), ItemEvent::FinalizeRetry);
    assert_eq!(failure_event(2, 3), ItemEvent::FinalizeRetry);
}

#[test]
fn core_recurrence_rearm_tests_failure_event_at_max_is_fail() {
    assert_eq!(failure_event(3, 3), ItemEvent::FinalizeFail);
    assert_eq!(failure_event(4, 3), ItemEvent::FinalizeFail);
}

// ---------------------------------------------------------------------------
// AC-CORE-4: simulate full retry cycle for max_attempts ∈ {1, 3, 10}
//
// Each cycle: Claim → (FinalizeRetry until exhausted) → FinalizeFail → Failed.
// The item must reach Failed exactly once, on attempt max_attempts+1.
// ---------------------------------------------------------------------------

fn simulate_retry_cycle(max_attempts: u32) {
    let mut state = ItemState::Pending;
    let mut attempts = 0u32;
    let mut terminal_fail_count = 0u32;

    loop {
        // Claim the item.
        state = apply_transition(state, ItemEvent::Claim).expect("Claim from Pending must succeed");
        assert_eq!(state, ItemState::Leased);

        // Choose the failure event based on exhaustion.
        let event = failure_event(attempts, max_attempts);
        state = apply_transition(state, event).expect("failure event from Leased must succeed");

        attempts += 1;

        match state {
            ItemState::Pending => {
                // Retry: continue the loop.
                assert!(
                    attempts <= max_attempts,
                    "got Pending after attempt {}, max was {}",
                    attempts,
                    max_attempts
                );
            }
            ItemState::Failed => {
                terminal_fail_count += 1;
                break;
            }
            other => panic!("unexpected state {:?} after failure event", other),
        }
    }

    assert_eq!(
        attempts,
        max_attempts + 1,
        "expected terminal fail on attempt {}, got it on {}",
        max_attempts + 1,
        attempts
    );
    assert_eq!(
        terminal_fail_count, 1,
        "item must reach Failed exactly once"
    );
    // Verify no further transitions are accepted (INV-3).
    assert!(apply_transition(state, ItemEvent::Claim).is_err());
}

#[test]
fn core_recurrence_rearm_tests_retry_cycle_max_1() {
    simulate_retry_cycle(1);
}

#[test]
fn core_recurrence_rearm_tests_retry_cycle_max_3() {
    simulate_retry_cycle(3);
}

#[test]
fn core_recurrence_rearm_tests_retry_cycle_max_10() {
    simulate_retry_cycle(10);
}

// ---------------------------------------------------------------------------
// Rearm: per-cycle retry counter resets; item cycles through Pending→Leased
// ---------------------------------------------------------------------------

#[test]
fn core_recurrence_rearm_tests_rearm_resets_per_cycle_attempts() {
    // Simulate: exhaust max_attempts retries in cycle 1, then rearm,
    // then verify a full second cycle is available.
    let max_attempts = 2u32;

    // Cycle 1: retry → retry → ??? after rearm
    let mut state = ItemState::Pending;
    let mut attempts = 0u32;

    for _ in 0..max_attempts {
        state = apply_transition(state, ItemEvent::Claim).unwrap();
        state = apply_transition(state, failure_event(attempts, max_attempts)).unwrap();
        attempts += 1;
        assert_eq!(state, ItemState::Pending);
    }

    // At this point attempts == max_attempts, so next failure_event is FinalizeFail.
    // But instead, we rearm (recurring queue sends FinalizeRearm).
    state = apply_transition(state, ItemEvent::Claim).unwrap();
    state = apply_transition(state, ItemEvent::FinalizeRearm).unwrap();
    assert_eq!(state, ItemState::Pending, "rearm returns item to Pending");

    // Cycle 2 starts with a fresh per-cycle attempt counter (reset by caller logic).
    // In the pure domain layer, we just re-simulate from attempts=0.
    attempts = 0;
    simulate_retry_cycle_from(state, attempts, max_attempts);
}

fn simulate_retry_cycle_from(initial_state: ItemState, start_attempts: u32, max_attempts: u32) {
    let mut state = initial_state;
    let mut attempts = start_attempts;
    loop {
        state = apply_transition(state, ItemEvent::Claim).unwrap();
        let event = failure_event(attempts, max_attempts);
        state = apply_transition(state, event).unwrap();
        attempts += 1;
        if state == ItemState::Failed {
            break;
        }
    }
    assert_eq!(
        attempts,
        start_attempts + max_attempts + 1,
        "second cycle should also terminate after max_attempts"
    );
}
