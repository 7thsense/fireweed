//! Legacy-compatibility assertions migrated to the provider-neutral AsyncProjection test ledger.
//!
//! Bounded async-apply debt, backpressure, and fail-closed poison for the `objectlog/hybrid-async`
//! profile (bead pqueue-6da52695; TD-004 §"Async apply debt, backpressure, and poison thresholds").
//!
//! These tests exercise the runtime debt controller ([`HybridAsyncMonitor`]) and its configured bounds
//! ([`HybridAsyncThresholds`]): the typed backpressure level with the normative 75%/50% hysteresis, the
//! mutation-admission gate, the fail-closed poison latch on repeated apply failure, the recovery
//! high-water backpressure rule, and the exported observability surface — plus the SQLite WAL-size gauge
//! and the apply-lag-in-commands metric on [`SqliteCheckpointStore`].

use fireweed_conformance::{qdef, shard};
use fireweed_engine::EngineError;
use fireweed_sqlite::{
    BackpressureLevel, HybridAsyncDebt, HybridAsyncMonitor, HybridAsyncThresholds,
    SqliteCheckpointStore,
};

/// Thresholds with a lag hard-limit of 100 commands (soft at 75, clear at 50) and a poison retry
/// threshold of 3, so the level transitions are easy to reason about.
fn thresholds() -> HybridAsyncThresholds {
    HybridAsyncThresholds::new(100, 1_000_000, 100, 60_000, 3).expect("valid thresholds")
}

fn lag(commands: u64) -> HybridAsyncDebt {
    HybridAsyncDebt {
        apply_lag_commands: commands,
        ..Default::default()
    }
}

#[test]
fn async_projection_backpressure_zero_threshold_is_rejected() {
    // A zero bound would leave a queue instantly and permanently backpressured.
    assert!(HybridAsyncThresholds::new(0, 1, 1, 1, 1).is_err());
    assert!(HybridAsyncThresholds::new(1, 0, 1, 1, 1).is_err());
    assert!(HybridAsyncThresholds::new(1, 1, 0, 1, 1).is_err());
    assert!(HybridAsyncThresholds::new(1, 1, 1, 0, 1).is_err());
    assert!(HybridAsyncThresholds::new(1, 1, 1, 1, 0).is_err());
    assert!(HybridAsyncThresholds::new(1, 1, 1, 1, 1).is_ok());
}

#[test]
fn async_projection_backpressure_debt_crosses_soft_then_hard_bands() {
    let mut monitor = HybridAsyncMonitor::new(thresholds());
    assert_eq!(monitor.observe(lag(10), 0), BackpressureLevel::Clear);
    assert_eq!(monitor.observe(lag(80), 1), BackpressureLevel::Soft);
    assert_eq!(monitor.observe(lag(100), 2), BackpressureLevel::Hard);
}

#[test]
fn async_projection_backpressure_any_single_metric_at_its_hard_limit_trips_backpressure() {
    let mut monitor = HybridAsyncMonitor::new(thresholds());
    // Queue depth alone at its hard limit is enough even when every other metric is quiet.
    let debt = HybridAsyncDebt {
        apply_queue_depth: 100,
        ..Default::default()
    };
    assert_eq!(monitor.observe(debt, 0), BackpressureLevel::Hard);
}

#[test]
fn async_projection_backpressure_hard_backpressure_holds_until_debt_clears_below_half_after_a_clean_batch()
 {
    let mut monitor = HybridAsyncMonitor::new(thresholds());
    assert_eq!(monitor.observe(lag(100), 0), BackpressureLevel::Hard);
    // Down to 60 (still >= 50% clear band) — hysteresis holds Hard.
    assert_eq!(monitor.observe(lag(60), 1), BackpressureLevel::Hard);
    // Below the 50% band but no clean ordered batch has applied yet — still Hard.
    assert_eq!(monitor.observe(lag(40), 2), BackpressureLevel::Hard);
    monitor.record_apply_success();
    // Now both release conditions hold: all metrics < 50% AND a clean batch applied.
    assert_eq!(monitor.observe(lag(40), 3), BackpressureLevel::Clear);
}

#[test]
fn async_projection_backpressure_admission_gate_rejects_mutations_only_under_hard_backpressure() {
    let mut monitor = HybridAsyncMonitor::new(thresholds());
    assert!(monitor.admit_mutation().is_ok(), "clear admits mutations");
    monitor.observe(lag(80), 0);
    assert!(
        monitor.admit_mutation().is_ok(),
        "soft warns but still admits mutations"
    );
    monitor.observe(lag(100), 1);
    assert!(
        matches!(monitor.admit_mutation(), Err(EngineError::Unavailable)),
        "hard backpressure rejects new mutations with a retryable error"
    );
}

#[test]
fn async_projection_backpressure_repeated_apply_failure_poisons_and_fails_closed() {
    let mut monitor = HybridAsyncMonitor::new(thresholds());
    assert!(!monitor.record_checkpoint_error("io error"));
    assert!(!monitor.record_checkpoint_error("io error"));
    assert!(!monitor.is_poisoned());
    // Third consecutive failure reaches the poison threshold (3).
    assert!(monitor.record_checkpoint_error("io error"));
    assert!(monitor.is_poisoned());
    // A poisoned queue fails mutations closed with a storage error (NOT the retryable backpressure error).
    match monitor.admit_mutation() {
        Err(EngineError::Storage(msg)) => assert!(msg.contains("poisoned"), "{msg}"),
        other => panic!("expected Storage poison error, got {other:?}"),
    }
}

#[test]
fn async_projection_backpressure_a_clean_batch_resets_the_consecutive_retry_count_before_poison() {
    let mut monitor = HybridAsyncMonitor::new(thresholds());
    monitor.record_checkpoint_error("blip");
    monitor.record_checkpoint_error("blip");
    monitor.record_apply_success();
    assert_eq!(monitor.consecutive_retries(), 0);
    // The counter restarts, so two more failures do not poison (would need three consecutive).
    assert!(!monitor.record_checkpoint_error("blip"));
    assert!(!monitor.record_checkpoint_error("blip"));
    assert!(!monitor.is_poisoned());
}

#[test]
fn async_projection_backpressure_non_contiguous_apply_poisons_immediately() {
    let mut monitor = HybridAsyncMonitor::new(thresholds());
    monitor.poison("non-contiguous apply: expected sequence 42, got 44");
    assert!(monitor.is_poisoned());
    assert!(monitor.poison_reason().unwrap().contains("non-contiguous"));
}

#[test]
fn async_projection_backpressure_recovery_high_water_is_withheld_under_hard_backpressure_and_poison()
 {
    let mut monitor = HybridAsyncMonitor::new(thresholds());
    let hw = fireweed_engine::CommandPosition::new(shard(), 0, 41);
    // Clear: the recorded high-water is a safe replay-skip point.
    assert_eq!(
        monitor.recovery_high_water_safe(Some(hw.clone())),
        Some(hw.clone())
    );
    // Hard: the lagging high-water must not be advertised.
    monitor.observe(lag(100), 0);
    assert_eq!(monitor.recovery_high_water_safe(Some(hw.clone())), None);
    // Poison: likewise withheld (fail closed).
    monitor.poison("corruption");
    assert_eq!(monitor.recovery_high_water_safe(Some(hw)), None);
}

#[test]
fn async_projection_backpressure_retention_advances_only_when_clear_and_healthy() {
    let mut monitor = HybridAsyncMonitor::new(thresholds());
    assert!(monitor.retention_may_advance());
    monitor.observe(lag(80), 0);
    assert!(
        !monitor.retention_may_advance(),
        "soft debt halts retention"
    );
    monitor.observe(lag(10), 1);
    assert!(monitor.retention_may_advance());
    monitor.poison("worker failed");
    assert!(!monitor.retention_may_advance(), "poison halts retention");
}

#[test]
fn async_projection_backpressure_backpressure_count_and_duration_are_tracked() {
    let mut monitor = HybridAsyncMonitor::new(thresholds());
    // Enter Hard at t=100, leave at t=250 (150ms). record_apply_success lets it release below the band.
    monitor.observe(lag(100), 100);
    monitor.record_apply_success();
    monitor.observe(lag(10), 250);
    // Re-enter Hard once more.
    monitor.observe(lag(100), 400);
    let metrics = monitor.metrics();
    assert_eq!(metrics.backpressure_events, 2, "entered Hard twice");
    assert_eq!(
        metrics.backpressure_ms_total, 150,
        "accumulated the first Hard span's duration"
    );
    assert_eq!(metrics.backpressure_level, BackpressureLevel::Hard);
}

#[test]
fn async_projection_backpressure_metrics_snapshot_exposes_the_full_observability_surface() {
    let mut monitor = HybridAsyncMonitor::new(thresholds());
    monitor.set_wal_size_bytes(4096);
    monitor.observe(
        HybridAsyncDebt {
            apply_lag_commands: 30,
            apply_debt_bytes: 2048,
            apply_queue_depth: 5,
            oldest_unapplied_age_ms: 1234,
        },
        10,
    );
    monitor.record_checkpoint_error("blip");
    let m = monitor.metrics();
    assert_eq!(m.apply_lag_commands, 30);
    assert_eq!(m.apply_debt_bytes, 2048);
    assert_eq!(m.apply_queue_depth, 5);
    assert_eq!(m.oldest_unapplied_age_ms, 1234);
    assert_eq!(m.apply_retry_count, 1);
    assert_eq!(m.checkpoint_errors, 1);
    assert_eq!(m.wal_size_bytes, 4096);
    assert_eq!(m.backpressure_level, BackpressureLevel::Clear);
    assert!(!m.poisoned);
}

#[test]
fn async_projection_backpressure_checkpoint_store_reports_wal_size_and_apply_lag() {
    let store = SqliteCheckpointStore::in_memory().expect("open in-memory checkpoint store");
    let sh = shard();
    store
        .create_queue_projection(qdef())
        .expect("create queue projection");
    // An in-memory database has no WAL file, so the gauge is zero.
    assert_eq!(store.wal_size_bytes().expect("wal size"), 0);
    // Nothing checkpointed yet: with a committed log head at sequence 41, all 42 commands are lag.
    assert_eq!(
        store.apply_lag_commands(&sh, Some(41)).expect("apply lag"),
        42
    );
    // An empty log (no committed head) has zero lag.
    assert_eq!(store.apply_lag_commands(&sh, None).expect("apply lag"), 0);
}
