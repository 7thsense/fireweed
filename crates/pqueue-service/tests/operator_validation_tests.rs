#![forbid(unsafe_code)]

use pqueue_service::verification_ledger::validate_ledger_text;

#[test]
fn operator_validation_tests_ledger_cites_full_api_002_surface() {
    let row = serde_json::json!({
        "ac_ids": [
            "AC-E2E-7",
            "AC-OP-1",
            "AC-OP-2",
            "AC-OP-3",
            "AC-OP-4",
            "AC-OP-5",
            "AC-OP-6",
            "AC-OP-7",
            "AC-OP-8",
            "AC-OP-9",
            "AC-CLAIM-6"
        ],
        "inv_ids": ["INV-8", "INV-11"],
        "command": "PQUEUE_E2E_SCALE=release PQUEUE_E2E_SEED=1907 cargo test -p pqueue-service --test product_workflows product_workflow_operator_repair_redrive_e2e -- --ignored",
        "exit_status": 0,
        "backend_profile": "postgres_native",
        "scale": "release",
        "seed": 1907,
        "environment": {
            "toolchain": "test",
            "instance_class": "local-dev",
            "service_fault_after_ops": null,
            "worker_fault_after_ops": null
        },
        "suite": "product_workflow_operator_repair_redrive_e2e",
        "measurements": {
            "selected_operator_items": 1000000,
            "concurrency": 64,
            "kill_count": 100,
            "pause_resume_claims_while_paused": 0,
            "repair_fenced_lease_renewals_accepted": 0,
            "purge_dry_run_side_effects": 0,
            "archive_idempotent": true,
            "retention_policy_checked": true,
            "async_operation_replay_attempts": 100,
            "duplicate_operation_ids": 0,
            "terminal_counts_exact": true,
            "cancel_rolled_back_committed_shards": 0,
            "unauthorized_operator_successes": 0,
            "cross_tenant_existence_leaks": 0,
            "plaintext_lease_tokens_returned": 0,
            "cohort_split_violations": 0
        },
        "pass_bar": {
            "comparison": "within-bar",
            "threshold": "release",
            "min_selected_operator_items": 1000000,
            "required_async_replay_attempts": 100,
            "max_duplicate_operation_ids": 0,
            "max_stale_lease_renewals_accepted": 0,
            "max_purge_dry_run_side_effects": 0,
            "max_unauthorized_operator_successes": 0,
            "max_cross_tenant_existence_leaks": 0,
            "max_plaintext_lease_tokens_returned": 0,
            "max_cohort_split_violations": 0,
            "max_cancel_rollbacks": 0
        }
    });
    let text = format!("{row}\n");
    let ledger = validate_ledger_text(&text).expect("operator ledger row validates");
    let row = &ledger.rows[0];

    for required in [
        "AC-E2E-7",
        "AC-OP-1",
        "AC-OP-2",
        "AC-OP-3",
        "AC-OP-4",
        "AC-OP-5",
        "AC-OP-6",
        "AC-OP-7",
        "AC-OP-8",
        "AC-OP-9",
        "AC-CLAIM-6",
    ] {
        assert!(row.ac_ids.iter().any(|id| id == required));
    }
    assert!(row.inv_ids.iter().any(|id| id == "INV-8"));
    assert!(row.inv_ids.iter().any(|id| id == "INV-11"));
    assert_eq!(row.suite, "product_workflow_operator_repair_redrive_e2e");
}
