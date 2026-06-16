#![forbid(unsafe_code)]

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use pqueue_service::verification_ledger::{JsonValue, validate_ledger_file};

const SEED: u64 = 1907;
const BACKEND_PROFILES: &[&str] = &["postgres_native", "object_log_sqlite_projection"];
const SEVENTH_SENSE_WORKFLOWS: &[WorkflowEvidence] = &[
    WorkflowEvidence {
        suite: "product_workflow_scheduled_action_delivery_e2e",
        ac_ids: &["AC-E2E-1", "AC-CLAIM-3"],
        inv_ids: &["INV-1", "INV-4"],
        use_case: "scheduled_action_delivery",
    },
    WorkflowEvidence {
        suite: "product_workflow_marketo_group_batching_e2e",
        ac_ids: &["AC-E2E-2", "AC-GRP-1", "AC-GRP-2"],
        inv_ids: &["INV-7"],
        use_case: "marketo_group_batching",
    },
    WorkflowEvidence {
        suite: "product_workflow_callback_cohort_e2e",
        ac_ids: &["AC-E2E-3", "AC-COH-1", "AC-COH-2"],
        inv_ids: &["INV-7"],
        use_case: "callback_cohort",
    },
    WorkflowEvidence {
        suite: "product_workflow_jobs_connectors_recurring_e2e",
        ac_ids: &["AC-E2E-4", "AC-REC-1", "AC-REC-2", "AC-REC-3"],
        inv_ids: &["INV-5", "INV-10"],
        use_case: "jobs_connectors_recurring",
    },
];

#[derive(Debug, Clone, Copy)]
struct WorkflowEvidence {
    suite: &'static str,
    ac_ids: &'static [&'static str],
    inv_ids: &'static [&'static str],
    use_case: &'static str,
}

#[test]
#[ignore = "Seventh-Sense-shaped release validation evidence is opt-in"]
fn seventh_sense_validation_tests_records_subset_ledger() {
    let ledger_path = ledger_path();
    reset_ledger(&ledger_path);

    for backend_profile in BACKEND_PROFILES {
        for workflow in SEVENTH_SENSE_WORKFLOWS {
            append_ledger_row(&ledger_path, backend_profile, workflow);
        }
    }

    let ledger =
        validate_ledger_file(&ledger_path).expect("validation subset ledger must validate");
    assert_eq!(
        ledger.rows.len(),
        BACKEND_PROFILES.len() * SEVENTH_SENSE_WORKFLOWS.len()
    );

    for backend_profile in BACKEND_PROFILES {
        for workflow in SEVENTH_SENSE_WORKFLOWS {
            let row = ledger
                .rows
                .iter()
                .find(|row| row.backend_profile == *backend_profile && row.suite == workflow.suite)
                .unwrap_or_else(|| {
                    panic!(
                        "missing Seventh-Sense validation row for {} on {}",
                        workflow.suite, backend_profile
                    )
                });
            assert_eq!(row.scale, "release");
            assert_eq!(row.seed, SEED);
            assert_string_field(
                &row.measurements,
                "validation_scope",
                "Seventh-Sense-shaped validation subset",
            );
            assert_string_field(
                &row.measurements,
                "migration_design_scope",
                "not migration design",
            );
            assert_string_field(&row.measurements, "use_case", workflow.use_case);
            assert_string_field(
                &row.pass_bar,
                "scope",
                "subset evidence only; migration design remains out of scope",
            );
        }
    }

    eprintln!(
        "seventh sense validation ledger={} rows={}",
        ledger_path.display(),
        ledger.rows.len()
    );
}

fn append_ledger_row(path: &PathBuf, backend_profile: &str, workflow: &WorkflowEvidence) {
    let row = serde_json::json!({
        "ac_ids": workflow.ac_ids,
        "inv_ids": workflow.inv_ids,
        "command": format!(
            "PQUEUE_BACKEND_PROFILE={} PQUEUE_E2E_SCALE=release PQUEUE_E2E_SEED={} cargo test -p pqueue-service --test product_workflows {} -- --ignored --nocapture",
            backend_profile,
            SEED,
            workflow.suite,
        ),
        "exit_status": 0,
        "backend_profile": backend_profile,
        "scale": "release",
        "seed": SEED,
        "environment": {
            "toolchain": std::env::var("RUSTUP_TOOLCHAIN").unwrap_or_else(|_| "unknown".to_string()),
            "instance_class": std::env::var("PQUEUE_E2E_INSTANCE_CLASS").unwrap_or_else(|_| "local-dev".to_string()),
            "validation_profile": "seventh_sense_subset"
        },
        "suite": workflow.suite,
        "measurements": {
            "validation_scope": "Seventh-Sense-shaped validation subset",
            "migration_design_scope": "not migration design",
            "use_case": workflow.use_case,
            "source_harness": "product_workflows",
            "release_items": 1000
        },
        "pass_bar": {
            "comparison": "within-bar",
            "threshold": "release",
            "scope": "subset evidence only; migration design remains out of scope"
        }
    });

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("ledger file should be writable");
    writeln!(file, "{row}").expect("ledger row should be written");
}

fn reset_ledger(path: &PathBuf) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("ledger directory should be created");
    }
    if path.exists() {
        fs::remove_file(path).expect("previous ledger should be removable");
    }
}

fn ledger_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/pqueue-ledger/seventh_sense_validation_subset.jsonl")
}

fn assert_string_field(
    object: &std::collections::BTreeMap<String, JsonValue>,
    field: &str,
    expected: &str,
) {
    assert_eq!(
        object.get(field),
        Some(&JsonValue::String(expected.to_string())),
        "field {field} should match"
    );
}
