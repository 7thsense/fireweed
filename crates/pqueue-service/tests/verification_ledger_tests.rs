use pqueue_service::verification_ledger::{run_from_args, validate_ledger_file};
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
