use pqueue_service::verification_ledger::{
    run_from_args, validate_ledger_file, validate_ledger_text,
};
use std::path::PathBuf;

#[test]
fn verification_ledger_tests() {
    let fixtures = fixture_paths();

    let (valid_path, _) = &fixtures[0];
    let ledger = validate_ledger_file(valid_path).expect("valid fixture should pass");
    assert_eq!(ledger.rows.len(), 1);

    let valid_cli_path = valid_path.to_string_lossy().into_owned();
    let cli_rows = run_from_args([
        "pqueue-verify-ledger",
        "--strict",
        "--ledger",
        valid_cli_path.as_str(),
    ])
    .expect("valid fixture should pass through the CLI entrypoint");
    assert_eq!(cli_rows, 1);

    for (path, expected_field) in fixtures.iter().skip(1) {
        let err = validate_ledger_file(path).expect_err("fixture should fail strict validation");
        assert_eq!(err.field.as_deref(), Some(*expected_field));
        assert!(
            err.to_string().contains(expected_field),
            "error should mention the missing field"
        );

        let cli_err = run_from_args([
            "pqueue-verify-ledger",
            "--strict",
            "--ledger",
            path.to_string_lossy().as_ref(),
        ])
        .expect_err("CLI validation should fail for the same missing field");
        assert_eq!(cli_err.field.as_deref(), Some(*expected_field));
    }
}

#[test]
fn verification_ledger_tests_require_performance_scale_fields() {
    let missing = serde_json::json!({
        "ac_ids": ["AC-LAT-1"],
        "inv_ids": ["INV-4"],
        "command": "cargo test -p pqueue-service performance_batch_operation_tests",
        "exit_status": 0,
        "backend_profile": "postgres_native",
        "scale": "smoke",
        "seed": 6200,
        "environment": {
            "instance_class": "local-dev"
        },
        "suite": "performance_batch_operation_tests",
        "measurements": {
            "deployment_shape": "single-deployment",
            "workload_envelope": "E1",
            "query_plan": "synthetic smoke query plan",
            "tp002_evidence_ids": ["E0", "E1"],
            "items_per_hour": 10000000,
            "p95_ms": 125
        },
        "pass_bar": {
            "e0_floor_items_per_hour": 10000000,
            "p95_ms_lt": 250,
            "p99_ms_lt": 1000
        }
    });

    let err = validate_ledger_text(&format!("{missing}\n"))
        .expect_err("performance rows must include p99 measurements");
    assert_eq!(err.field.as_deref(), Some("measurements.p99_ms"));
}

#[test]
fn verification_ledger_tests_require_object_log_e3_fields() {
    let missing = serde_json::json!({
        "ac_ids": ["AC-LAT-1", "AC-LAT-2", "AC-LAT-3", "AC-LAT-4"],
        "inv_ids": ["INV-2", "INV-3", "INV-4", "INV-5", "INV-10"],
        "command": "PQUEUE_OBJECTLOG_E3_SCALE=release cargo test -p pqueue-objectlog object_log_commit_recovery_tests",
        "exit_status": 0,
        "backend_profile": "object_log_sqlite_projection",
        "scale": "release",
        "seed": 8103,
        "environment": {
            "instance_class": "local-dev"
        },
        "suite": "object_log_commit_recovery_tests",
        "measurements": {
            "deployment_shape": "object-log-sqlite-projection",
            "workload_envelope": "E3",
            "tp002_evidence_ids": ["E0", "E3"],
            "items_per_hour": 10000000,
            "p95_ms": 125,
            "p99_ms": 500,
            "segment_size_commands": 1024,
            "segment_max_latency_ms": 100,
            "durable_commit_cost_per_billion_commands_usd": 10,
            "postgres_native_cost_per_billion_commands_usd": 200,
            "recovery_items": 10000000,
            "acked_commands": 1024,
            "manifest_fence_rejections": 1,
            "fallback_fence_rejections": 1
        },
        "pass_bar": {
            "comparison": "within-bar",
            "e0_floor_items_per_hour": 10000000,
            "p95_ms_lt": 250,
            "p99_ms_lt": 1000,
            "recovery_window_budget_ms": 300000
        }
    });

    let err = validate_ledger_text(&format!("{missing}\n"))
        .expect_err("object-log E3 rows must include recovery timing");
    assert_eq!(err.field.as_deref(), Some("measurements.recovery_ms"));
}

fn fixture_paths() -> Vec<(PathBuf, &'static str)> {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    vec![
        (base.join("ledger_valid.jsonl"), "ac_ids"),
        (base.join("ledger_missing_ac.jsonl"), "ac_ids"),
        (base.join("ledger_missing_command.jsonl"), "command"),
        (base.join("ledger_missing_exit_status.jsonl"), "exit_status"),
        (
            base.join("ledger_missing_backend_profile.jsonl"),
            "backend_profile",
        ),
        (base.join("ledger_missing_scale.jsonl"), "scale"),
        (base.join("ledger_missing_seed.jsonl"), "seed"),
        (base.join("ledger_missing_environment.jsonl"), "environment"),
        (base.join("ledger_missing_suite.jsonl"), "suite"),
        (
            base.join("ledger_missing_measurement.jsonl"),
            "measurements",
        ),
        (base.join("ledger_missing_pass_bar.jsonl"), "pass_bar"),
    ]
}
