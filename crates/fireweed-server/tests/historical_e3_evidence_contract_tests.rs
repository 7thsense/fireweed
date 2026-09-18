//! Offline contract regression tests for the historical TP-002 E3 schema-v1 matrix.
//!
//! These synthetic fixtures preserve the old memory/SQLite evidence vocabulary; they neither
//! instantiate a retired projection nor measure current Fireweed performance. The retired live
//! producer inferred seal/PUT counters from caller batches and no longer exercised an E3 matrix.
//! Its 10M memory-only recovery route has been removed instead of publishing inferred measurements.
//!
//! Current recovery behavior is exercised through the public API in
//! `fireweed/tests/p5as3_s3_reopen_parity.rs` and `public_interface_external_conformance.rs`, and
//! through exact replay/cursor assertions in `fireweed-turso/tests/recovery.rs`. Those correctness
//! checks do not qualify the historical E3 performance bar. Immutable E3 artifacts remain validated
//! by `fireweed-release/tests/e3_contract_evidence_tests.rs`.

const RECORDER_CONTROL_BLOCKS: usize = 5;
const EXPECTED_RECORDER_CONTROL_SCHEDULE: &str =
    "independent-bounded-blocks-seeded-alternating-order-v1";
const EXPECTED_RECORDER_CONTROL_FINGERPRINT_ALGORITHM: &str =
    "fnv1a128+disk-unique-id-index+canonical-live-state-v1";

const E3_BOUND_CONFIGS: [BoundConfig; 4] = [
    BoundConfig { label: "1ms" },
    BoundConfig { label: "5ms" },
    BoundConfig { label: "20ms" },
    BoundConfig { label: "100ms" },
];

fn pct(latencies_ms: &mut [f64], p: f64) -> f64 {
    if latencies_ms.is_empty() {
        return 0.0;
    }
    latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = (((latencies_ms.len() as f64) * p).ceil() as usize)
        .saturating_sub(1)
        .min(latencies_ms.len() - 1);
    latencies_ms[idx]
}

#[derive(Clone, Copy)]
struct BoundConfig {
    label: &'static str,
}

struct E3ProfileSpec {
    backend_profile: &'static str,
    requires_snapshot: bool,
}

const HISTORICAL_V1_PROFILE_SPECS: [E3ProfileSpec; 2] = [
    E3ProfileSpec {
        backend_profile: "object_log_inmemory_projection",
        requires_snapshot: false,
    },
    E3ProfileSpec {
        backend_profile: "object_log_sqlite_projection",
        requires_snapshot: true,
    },
];

/// Historical evidence inputs; all values in this target are explicitly synthetic fixtures.
struct AckResult {
    label: &'static str,
    disabled_control_throughput_per_s: f64,
    recorder_overhead_ratio: f64,
    recorder_overhead_ratio_samples: Vec<f64>,
    recorder_control_order_seed: u64,
    recorder_control_schedule: &'static str,
    recorder_control_fingerprint_algorithm: &'static str,
    recorder_control_logical_match: bool,
    throughput_progress_met: bool,
    ack_p50_ms: f64,
    ack_p95_ms: f64,
    ack_p99_ms: f64,
    latency_distribution_met: bool,
    load_shape_met: bool,
    bar_met: bool,
}

struct ProfileRun {
    backend_profile: &'static str,
    ack_results: Vec<AckResult>,
    recovery: Option<RecoveryResult>,
    bars_met: bool,
}

struct RecoveryResult {
    resident: u64,
    load_command_count: u64,
    load_segments_sealed: u64,
    load_size_triggered_seals: u64,
    load_latency_triggered_seals: u64,
    load_forced_seals: u64,
    load_rollover_seals: u64,
    load_group_commit_batch_sum: u64,
    command_count: u64,
    total_commands: u64,
    start_seq: u64,
    tail_replayed: u64,
    snapshot_used: bool,
    state_digest_before: String,
    state_digest_after: String,
    verified_items: u64,
    missing_items: u64,
    duplicate_items: u64,
    invalid_items: u64,
    replay_progress_samples: Vec<u64>,
    checksum_validation_passed: bool,
    bar_met: bool,
}

fn validate_historical_e3_profile_matrix(
    runs: &[ProfileRun],
    require_bars: bool,
) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();
    let mut seen_profiles = std::collections::BTreeSet::new();
    for run in runs {
        if !seen_profiles.insert(run.backend_profile) {
            errors.push(format!("duplicate profile {}", run.backend_profile));
        }
        if run.ack_results.len() != E3_BOUND_CONFIGS.len() {
            errors.push(format!(
                "profile {} has {} bounds; expected {}",
                run.backend_profile,
                run.ack_results.len(),
                E3_BOUND_CONFIGS.len()
            ));
        }
        let mut seen_bounds = std::collections::BTreeSet::new();
        for result in &run.ack_results {
            if !seen_bounds.insert(result.label) {
                errors.push(format!(
                    "profile {} has duplicate bound {}",
                    run.backend_profile, result.label
                ));
            }
            if !result.throughput_progress_met {
                errors.push(format!(
                    "profile {} bound {} did not make measurable throughput progress",
                    run.backend_profile, result.label
                ));
            }
            if !result.latency_distribution_met {
                errors.push(format!(
                    "profile {} bound {} has an invalid latency distribution (p50/p95/p99={} / {} / {})",
                    run.backend_profile,
                    result.label,
                    result.ack_p50_ms,
                    result.ack_p95_ms,
                    result.ack_p99_ms
                ));
            }
            if !result.load_shape_met {
                errors.push(format!(
                    "profile {} bound {} did not sustain a batched committed load shape",
                    run.backend_profile, result.label
                ));
            }
            if !result.recorder_control_logical_match {
                errors.push(format!(
                    "profile {} bound {} enabled/disabled recorder controls diverged logically",
                    run.backend_profile, result.label
                ));
            }
            let mut overhead_samples = result.recorder_overhead_ratio_samples.clone();
            let sample_distribution_valid = overhead_samples.len() == RECORDER_CONTROL_BLOCKS
                && overhead_samples
                    .iter()
                    .all(|value| value.is_finite() && *value > 0.0);
            let measured_median = if sample_distribution_valid {
                pct(&mut overhead_samples, 0.50)
            } else {
                f64::NAN
            };
            if result.recorder_control_schedule != EXPECTED_RECORDER_CONTROL_SCHEDULE
                || result.recorder_control_fingerprint_algorithm
                    != EXPECTED_RECORDER_CONTROL_FINGERPRINT_ALGORITHM
                || result.recorder_control_order_seed == 0
                || !sample_distribution_valid
                || !result.disabled_control_throughput_per_s.is_finite()
                || result.disabled_control_throughput_per_s <= 0.0
                || !result.recorder_overhead_ratio.is_finite()
                || result.recorder_overhead_ratio <= 0.0
                || (measured_median - result.recorder_overhead_ratio).abs() > 0.001
            {
                errors.push(format!(
                    "profile {} bound {} lacks a valid independent bounded-block recorder-control distribution",
                    run.backend_profile, result.label
                ));
            }
        }
        for bound in E3_BOUND_CONFIGS {
            if !seen_bounds.contains(bound.label) {
                errors.push(format!(
                    "profile {} missing bound {}",
                    run.backend_profile, bound.label
                ));
            }
        }
        if require_bars {
            if let Some(recovery) = &run.recovery {
                if !recovery.bar_met {
                    errors.push(format!(
                        "profile {} recovery bar not met",
                        run.backend_profile
                    ));
                }
                if recovery.state_digest_before != recovery.state_digest_after
                    || recovery.verified_items != recovery.resident
                    || recovery.missing_items != 0
                    || recovery.duplicate_items != 0
                    || recovery.invalid_items != 0
                {
                    errors.push(format!(
                        "profile {} recovery did not reproduce the exact complete state",
                        run.backend_profile
                    ));
                }
                if recovery.start_seq.checked_add(recovery.tail_replayed)
                    != Some(recovery.total_commands)
                    || recovery.total_commands != recovery.command_count
                {
                    errors.push(format!(
                        "profile {} recovery command range is not exact: start_seq={} + tail_replayed={} != total_commands={} == command_count={}",
                        run.backend_profile,
                        recovery.start_seq,
                        recovery.tail_replayed,
                        recovery.total_commands,
                        recovery.command_count
                    ));
                }
                if recovery.load_size_triggered_seals <= recovery.load_latency_triggered_seals
                    || recovery.load_latency_triggered_seals > 1
                    || recovery.load_forced_seals != 0
                    || recovery.load_rollover_seals != 0
                    || recovery
                        .load_size_triggered_seals
                        .checked_add(recovery.load_latency_triggered_seals)
                        != Some(recovery.load_segments_sealed)
                    || recovery.load_group_commit_batch_sum != recovery.load_command_count
                {
                    errors.push(format!(
                        "profile {} recovery load lacks exact size-triggered group-commit batching",
                        run.backend_profile
                    ));
                }
                if !recovery
                    .replay_progress_samples
                    .windows(2)
                    .all(|pair| pair[0] <= pair[1])
                {
                    errors.push(format!(
                        "profile {} recovery replay progress regressed",
                        run.backend_profile
                    ));
                }
                if !recovery.checksum_validation_passed {
                    errors.push(format!(
                        "profile {} recovery checksum validation did not pass",
                        run.backend_profile
                    ));
                }
                if let Some(spec) = HISTORICAL_V1_PROFILE_SPECS
                    .iter()
                    .find(|spec| spec.backend_profile == run.backend_profile)
                {
                    let mode_met = if spec.requires_snapshot {
                        recovery.snapshot_used
                            && recovery.start_seq > 0
                            && recovery.tail_replayed > 0
                            && recovery.tail_replayed < recovery.total_commands
                    } else {
                        !recovery.snapshot_used
                            && recovery.start_seq == 0
                            && recovery.tail_replayed == recovery.total_commands
                    };
                    if !mode_met {
                        errors.push(format!(
                            "profile {} recovery mode does not match projection contract",
                            run.backend_profile
                        ));
                    }
                }
            } else {
                errors.push(format!(
                    "profile {} is missing required recovery evidence",
                    run.backend_profile
                ));
            }
            if !run.bars_met {
                errors.push(format!("profile {} bars_met=false", run.backend_profile));
            }
        }
    }
    for spec in HISTORICAL_V1_PROFILE_SPECS.iter() {
        if !seen_profiles.contains(spec.backend_profile) {
            errors.push(format!("missing profile {}", spec.backend_profile));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn synthetic_ack(label: &'static str, throughput_per_s: f64) -> AckResult {
    let progress = throughput_per_s.is_finite() && throughput_per_s > 0.0;
    AckResult {
        label,
        disabled_control_throughput_per_s: throughput_per_s,
        recorder_overhead_ratio: 1.0,
        recorder_overhead_ratio_samples: vec![1.0; RECORDER_CONTROL_BLOCKS],
        recorder_control_order_seed: 7,
        recorder_control_schedule: EXPECTED_RECORDER_CONTROL_SCHEDULE,
        recorder_control_fingerprint_algorithm: EXPECTED_RECORDER_CONTROL_FINGERPRINT_ALGORITHM,
        recorder_control_logical_match: true,
        throughput_progress_met: progress,
        ack_p50_ms: 1.0,
        ack_p95_ms: 2.0,
        ack_p99_ms: 3.0,
        latency_distribution_met: true,
        load_shape_met: true,
        bar_met: progress,
    }
}

fn synthetic_recovery(bar_met: bool, requires_snapshot: bool) -> RecoveryResult {
    RecoveryResult {
        resident: 10_000_000,
        load_command_count: if requires_snapshot { 99 } else { 100 },
        load_segments_sealed: 10,
        load_size_triggered_seals: 9,
        load_latency_triggered_seals: 1,
        load_forced_seals: 0,
        load_rollover_seals: 0,
        load_group_commit_batch_sum: if requires_snapshot { 99 } else { 100 },
        command_count: 100,
        total_commands: 100,
        start_seq: if requires_snapshot { 10 } else { 0 },
        tail_replayed: if requires_snapshot { 90 } else { 100 },
        snapshot_used: requires_snapshot,
        state_digest_before: "fnv1a128:fixture".into(),
        state_digest_after: "fnv1a128:fixture".into(),
        verified_items: 10_000_000,
        missing_items: 0,
        duplicate_items: 0,
        invalid_items: 0,
        replay_progress_samples: vec![0, 100],
        checksum_validation_passed: true,
        bar_met,
    }
}

fn synthetic_profile_run(backend_profile: &'static str, requires_snapshot: bool) -> ProfileRun {
    let ack_results = E3_BOUND_CONFIGS
        .iter()
        .map(|bound| synthetic_ack(bound.label, 3_000.0))
        .collect::<Vec<_>>();
    let recovery = Some(synthetic_recovery(true, requires_snapshot));
    let bars_met = ack_results.iter().all(|result| result.bar_met)
        && recovery.as_ref().is_none_or(|r| r.bar_met);
    ProfileRun {
        backend_profile,
        ack_results,
        recovery,
        bars_met,
    }
}

#[test]
fn historical_e3_matrix_rejects_missing_profile() {
    let runs = vec![synthetic_profile_run(
        "object_log_inmemory_projection",
        false,
    )];
    let errors = validate_historical_e3_profile_matrix(&runs, true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("missing profile object_log_sqlite_projection")),
        "{errors:?}"
    );
}

#[test]
fn historical_e3_matrix_rejects_missing_bound() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", true);
    run.ack_results.pop();
    let errors = validate_historical_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("missing bound 100ms")),
        "{errors:?}"
    );
}

#[test]
fn historical_e3_matrix_rejects_no_throughput_progress() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", true);
    run.ack_results[0].throughput_progress_met = false;
    run.ack_results[0].bar_met = false;
    let errors = validate_historical_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("measurable throughput progress")),
        "{errors:?}"
    );
}

#[test]
fn historical_e3_matrix_rejects_invalid_latency_distribution() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", true);
    run.ack_results[1].ack_p99_ms = run.ack_results[1].ack_p50_ms - 1.0;
    run.ack_results[1].latency_distribution_met = false;
    run.ack_results[1].bar_met = false;
    let errors = validate_historical_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("invalid latency distribution")),
        "{errors:?}"
    );
}

#[test]
fn historical_e3_matrix_rejects_recorder_control_divergence() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", true);
    run.ack_results[0].recorder_control_logical_match = false;
    run.ack_results[0].bar_met = false;
    let errors = validate_historical_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("recorder controls diverged logically")),
        "{errors:?}"
    );
}

#[test]
fn historical_e3_matrix_accepts_large_consistent_recorder_ratio_as_diagnostic() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", true);
    run.ack_results[0].recorder_overhead_ratio = 1.5;
    run.ack_results[0].recorder_overhead_ratio_samples = vec![1.5; RECORDER_CONTROL_BLOCKS];
    validate_historical_e3_profile_matrix(
        &[
            synthetic_profile_run("object_log_inmemory_projection", false),
            run,
        ],
        true,
    )
    .unwrap();
}

#[test]
fn historical_e3_matrix_rejects_forged_or_lockstepped_recorder_distribution() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", true);
    run.ack_results[0].recorder_overhead_ratio_samples = vec![1.0, 1.0, 1.5, 1.5, 1.5];
    let errors = validate_historical_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("independent bounded-block recorder-control distribution")),
        "{errors:?}"
    );

    let mut run = synthetic_profile_run("object_log_sqlite_projection", true);
    run.ack_results[0].recorder_control_schedule =
        "paired-operation-barriers-concurrent-worker-partitions-v1";
    let errors = validate_historical_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("independent bounded-block recorder-control distribution")),
        "{errors:?}"
    );
}

#[test]
fn historical_e3_controls_require_finite_positive_median_consistent_ratio() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", true);
    run.ack_results[0].recorder_overhead_ratio = 0.0;
    let errors = validate_historical_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("independent bounded-block recorder-control distribution")),
        "{errors:?}"
    );

    let mut run = synthetic_profile_run("object_log_sqlite_projection", true);
    run.ack_results[0].recorder_overhead_ratio_samples[0] = 0.0;
    let errors = validate_historical_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("independent bounded-block recorder-control distribution")),
        "{errors:?}"
    );
}

#[test]
fn historical_e3_matrix_rejects_recovery_digest_drift() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", true);
    let recovery = run.recovery.as_mut().unwrap();
    recovery.state_digest_after = "fnv1a128:drift".into();
    recovery.bar_met = false;
    let errors = validate_historical_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("exact complete state")),
        "{errors:?}"
    );
}

#[test]
fn historical_e3_matrix_rejects_inexact_recovery_command_range() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", true);
    let recovery = run.recovery.as_mut().unwrap();
    recovery.tail_replayed -= 1;
    let errors = validate_historical_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("recovery command range is not exact")),
        "{errors:?}"
    );
}

#[test]
fn historical_e3_matrix_rejects_latency_driven_or_inexact_recovery_load_batching() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", true);
    let recovery = run.recovery.as_mut().unwrap();
    recovery.load_latency_triggered_seals = 5;
    recovery.load_group_commit_batch_sum -= 1;
    let errors = validate_historical_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors.iter().any(|error| error
            .contains("recovery load lacks exact size-triggered group-commit batching")),
        "{errors:?}"
    );
}

#[test]
fn historical_e3_matrix_rejects_checksum_drift() {
    let mut run = synthetic_profile_run("object_log_sqlite_projection", true);
    let recovery = run.recovery.as_mut().unwrap();
    recovery.checksum_validation_passed = false;
    let errors = validate_historical_e3_profile_matrix(&[run], true).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("recovery checksum validation did not pass")),
        "{errors:?}"
    );
}
