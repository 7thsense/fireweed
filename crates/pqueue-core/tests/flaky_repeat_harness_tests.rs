// Integration tests for the repeat-suite harness (scripts/ci/repeat-suite.sh).
//
// Verifies: script existence, pass/fail propagation, strict flaky-rate gate,
// suite-list mode, and required report fields (TP-003 §5 AC-4: run_count,
// failures, flaky_rate, failing_selectors).
//
// Fixtures in scripts/ci/fixtures/repeat/ provide controlled exit-code
// behaviour without invoking cargo test in a nested subprocess.

use std::path::PathBuf;
use std::process::Command;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn repeat_suite_script() -> PathBuf {
    workspace_root().join("scripts/ci/repeat-suite.sh")
}

fn fixture(name: &str) -> PathBuf {
    workspace_root()
        .join("scripts/ci/fixtures/repeat")
        .join(name)
}

/// Run `repeat-suite.sh <args>` from the workspace root; return (exit_code, combined_output).
fn run_harness(args: &[&str]) -> (i32, String) {
    let script = repeat_suite_script();
    let out = Command::new("bash")
        .arg(&script)
        .args(args)
        .current_dir(workspace_root())
        .output()
        .unwrap_or_else(|e| panic!("failed to run repeat-suite.sh: {e}"));
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    let combined = format!("{stdout}{stderr}");
    let code = out.status.code().unwrap_or(-1);
    (code, combined)
}

#[test]
fn flaky_repeat_harness_tests_repeat_suite_script_exists() {
    let path = repeat_suite_script();
    assert!(path.exists(), "repeat-suite.sh not found at {path:?}");
}

#[test]
fn flaky_repeat_harness_tests_report_script_exists() {
    let path = workspace_root().join("scripts/ci/repeat-suite-report.py");
    assert!(
        path.exists(),
        "repeat-suite-report.py not found at {path:?}"
    );
}

#[test]
fn flaky_repeat_harness_tests_fixtures_exist() {
    for name in &[
        "always-pass.sh",
        "always-fail.sh",
        "repeat-fixture-suites.toml",
    ] {
        let path = fixture(name);
        assert!(path.exists(), "fixture not found: {path:?}");
    }
}

#[test]
fn flaky_repeat_harness_tests_always_pass_exits_zero() {
    let (code, out) = run_harness(&[
        "--count",
        "3",
        "--",
        "bash",
        fixture("always-pass.sh").to_str().unwrap(),
    ]);
    assert_eq!(
        code, 0,
        "Expected exit 0 with always-pass fixture. Output:\n{out}"
    );
}

#[test]
fn flaky_repeat_harness_tests_report_contains_required_fields() {
    let (code, out) = run_harness(&[
        "--count",
        "3",
        "--",
        "bash",
        fixture("always-pass.sh").to_str().unwrap(),
    ]);
    assert_eq!(code, 0, "Harness failed. Output:\n{out}");

    for field in &["run_count", "failures", "flaky_rate", "failing_selectors"] {
        assert!(
            out.contains(field),
            "Report missing field '{field}'. Output:\n{out}"
        );
    }
}

#[test]
fn flaky_repeat_harness_tests_zero_failures_passes_strict_threshold() {
    let (code, out) = run_harness(&[
        "--count",
        "10",
        "--max-flaky-rate",
        "0.000999",
        "--",
        "bash",
        fixture("always-pass.sh").to_str().unwrap(),
    ]);
    assert_eq!(
        code, 0,
        "Expected exit 0: 0/10 failures must satisfy 0.000999 threshold. Output:\n{out}"
    );
}

#[test]
fn flaky_repeat_harness_tests_always_fail_exceeds_strict_threshold() {
    let (code, out) = run_harness(&[
        "--count",
        "3",
        "--max-flaky-rate",
        "0.000999",
        "--",
        "bash",
        fixture("always-fail.sh").to_str().unwrap(),
    ]);
    assert_ne!(
        code, 0,
        "Expected nonzero exit: all runs fail, rate 1.0 > 0.000999. Output:\n{out}"
    );
    assert!(
        out.contains("flaky_rate"),
        "Report missing 'flaky_rate'. Output:\n{out}"
    );
    assert!(
        out.contains("FAILED"),
        "Report should contain FAILED. Output:\n{out}"
    );
}

#[test]
fn flaky_repeat_harness_tests_suite_list_fixture_passes() {
    let suite_list = fixture("repeat-fixture-suites.toml");
    let (code, out) = run_harness(&["--count", "3", "--suite-list", suite_list.to_str().unwrap()]);
    assert_eq!(
        code, 0,
        "Expected exit 0 with fixture suite list. Output:\n{out}"
    );
    assert!(
        out.contains("always-pass fixture"),
        "Report should name the suite. Output:\n{out}"
    );
}

#[test]
fn flaky_repeat_harness_tests_always_fail_default_rate_passes_gate() {
    // Default --max-flaky-rate is 1.0: even 100% failures should not trip the gate.
    let (code, out) = run_harness(&[
        "--count",
        "3",
        "--",
        "bash",
        fixture("always-fail.sh").to_str().unwrap(),
    ]);
    assert_eq!(
        code, 0,
        "Default rate=1.0 should never trip the gate. Output:\n{out}"
    );
    assert!(
        out.contains("PASSED"),
        "Report should say PASSED when rate <= 1.0. Output:\n{out}"
    );
}
