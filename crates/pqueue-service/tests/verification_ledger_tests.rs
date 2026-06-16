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
fn verification_ledger_tests_require_multi_shard_scale_out_fields() {
    let missing = serde_json::json!({
        "ac_ids": ["AC-E2E-6", "AC-LAT-3"],
        "inv_ids": ["INV-4"],
        "command": "cargo test -p pqueue-service performance_multi_shard_scale_out_tests -- --ignored --nocapture",
        "exit_status": 0,
        "backend_profile": "object_log_sqlite_projection",
        "scale": "release",
        "seed": 7202,
        "environment": {
            "instance_class": "local-dev"
        },
        "suite": "performance_multi_shard_scale_out_tests",
        "measurements": {
            "deployment_shape": "multi-shard-horizontal-object-log",
            "workload_envelope": "E2",
            "query_plan": "release-plan: fan-out claim across independent object-log projections",
            "tp002_evidence_ids": ["E0", "E2"],
            "items_per_hour": 42000000,
            "items_per_hour_by_shard_count": [20000000, 30000000, 42000000],
            "single_deployment_ceiling_items_per_hour": 10000000,
            "scale_out_multiple_at_8_shards_x100": 420,
            "independent_storage_units": true,
            "queue_global_progress_checked": true,
            "progress_bound_violations": 0,
            "p95_ms": 180,
            "p99_ms": 700
        },
        "pass_bar": {
            "comparison": "within-bar",
            "e0_floor_items_per_hour": 10000000,
            "eight_shard_min_items_per_hour": 40000000,
            "minimum_scale_out_multiple_at_8_shards_x100": 400,
            "monotonic_non_decreasing_required": true,
            "p95_ms_lt": 250,
            "p99_ms_lt": 1000
        }
    });

    let err = validate_ledger_text(&format!("{missing}\n"))
        .expect_err("multi-shard E2 rows must include shard counts");
    assert_eq!(err.field.as_deref(), Some("measurements.shard_counts"));
}

#[test]
fn verification_ledger_tests_require_noisy_neighbor_release_traceability() {
    let missing = serde_json::json!({
        "ac_ids": ["AC-E2E-6", "AC-DISC-1", "AC-LAT-3"],
        "inv_ids": ["INV-4"],
        "command": "PQUEUE_BACKEND_PROFILE=object_log_sqlite_projection PQUEUE_E2E_SCALE=release cargo test -p pqueue-service --test product_workflows product_workflow_noisy_neighbor_scale_e2e -- --ignored",
        "exit_status": 0,
        "backend_profile": "object_log_sqlite_projection",
        "scale": "release",
        "seed": 1701,
        "environment": {
            "instance_class": "local-dev"
        },
        "suite": "product_workflow_noisy_neighbor_scale_e2e",
        "measurements": {
            "elapsed_ms": 1,
            "smoke_items": 1000,
            "release_topology": "TP-003-3.10-AC-E2E-6",
            "tp002_evidence_ids": ["E0", "E2"],
            "backend_role": "object_log_headline_multi_shard",
            "active_queues": 1000,
            "hot_queue_resident_items": 10000000,
            "small_eligible_queues": 1,
            "concurrency": 64,
            "discover_active_scopes_used": true,
            "active_scope_routing_checked": true,
            "unauthorized_queues_excluded": true,
            "progress_bound_violations": 0,
            "small_queue_claim_p95_ms": 180,
            "small_queue_claim_p99_ms": 700,
            "object_log_multi_shard_required": true
        },
        "pass_bar": {
            "comparison": "within-bar",
            "threshold": "release",
            "min_active_queues": 1000,
            "hot_queue_min_resident_items": 10000000,
            "p95_ms_lt": 250,
            "p99_ms_lt": 1000,
            "max_progress_bound_violations": 0,
            "tp002_evidence_required": "E2"
        }
    });

    let err = validate_ledger_text(&format!("{missing}\n"))
        .expect_err("AC-E2E-6 release rows must cite AC-DISC-2");
    assert_eq!(err.field.as_deref(), Some("ac_ids"));
}

#[test]
fn verification_ledger_tests_require_zero_invariant_stress_violations() {
    let invalid = serde_json::json!({
        "ac_ids": ["AC-CLAIM-1", "AC-E2E-1"],
        "inv_ids": ["INV-1", "INV-2", "INV-3", "INV-4", "INV-5", "INV-6", "INV-7", "INV-8", "INV-9", "INV-10"],
        "command": "cargo test -p pqueue-service invariant_stress_matrix_tests -- --ignored --nocapture",
        "exit_status": 0,
        "backend_profile": "object_log_sqlite_projection",
        "scale": "release",
        "seed": 7503,
        "environment": {
            "instance_class": "local-dev"
        },
        "suite": "invariant_stress_matrix_tests",
        "measurements": {
            "concurrency": 256,
            "resident_item_sizes": [1000000, 10000000],
            "soak_profile": "TP-003-section-2-release-soak",
            "kill_count": 1000,
            "skewed_priority_distribution": true,
            "skewed_group_distribution": true,
            "inv1_violations": 0,
            "inv2_violations": 0,
            "inv3_violations": 0,
            "inv4_violations": 1,
            "inv5_violations": 0,
            "inv6_violations": 0,
            "inv7_violations": 0,
            "inv8_violations": 0,
            "inv9_violations": 0,
            "inv10_violations": 0
        },
        "pass_bar": {
            "comparison": "within-bar",
            "threshold": "release",
            "max_invariant_violations": 0
        }
    });

    let err = validate_ledger_text(&format!("{invalid}\n"))
        .expect_err("P0/core invariant stress rows must record zero violations");
    assert_eq!(err.field.as_deref(), Some("measurements.inv4_violations"));
}

#[test]
fn verification_ledger_tests_require_product_validation_backend_conformance() {
    let invalid = serde_json::json!({
        "ac_ids": ["AC-E2E-1", "AC-E2E-2", "AC-E2E-3", "AC-E2E-4", "AC-E2E-5", "AC-E2E-6", "AC-E2E-8", "AC-E2E-9"],
        "inv_ids": ["INV-1", "INV-2", "INV-3", "INV-4", "INV-5", "INV-6", "INV-7", "INV-8", "INV-9", "INV-10"],
        "command": "PQUEUE_E2E_SCALE=release cargo test -p pqueue-service product_validation_tests -- --ignored --nocapture",
        "exit_status": 0,
        "backend_profile": "aggregate_committed_backends",
        "scale": "release",
        "seed": 9001,
        "environment": {
            "instance_class": "local-dev"
        },
        "suite": "product_validation_tests",
        "measurements": {
            "build_exit_criteria": ["BUILD-001-P0-core-implementation-closed", "TP-002-E0-E3-pass", "TP-003-P0-release-gates-green"],
            "tp002_evidence_ids": ["E0", "E1", "E2", "E3"],
            "workflow_ac_ids": ["AC-E2E-1", "AC-E2E-2", "AC-E2E-3", "AC-E2E-4", "AC-E2E-5", "AC-E2E-6", "AC-E2E-8", "AC-E2E-9"],
            "committed_backend_profiles": ["postgres_native", "object_log_sqlite_projection"],
            "postgres_native_conformance_pct": 100,
            "object_log_sqlite_projection_conformance_pct": 99,
            "invariant_stress_matrix_violations": 0
        },
        "pass_bar": {
            "comparison": "within-bar",
            "threshold": "release",
            "required_backend_conformance_pct": 100,
            "max_invariant_violations": 0
        }
    });

    let err = validate_ledger_text(&format!("{invalid}\n"))
        .expect_err("product validation requires 100% conformance for committed backends");
    assert_eq!(
        err.field.as_deref(),
        Some("measurements.object_log_sqlite_projection_conformance_pct")
    );
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

#[test]
fn verification_ledger_tests_cover_cli_and_parser_errors() {
    for args in [
        vec!["pqueue-verify-ledger", "--ledger", "missing.jsonl"],
        vec!["pqueue-verify-ledger", "--strict"],
        vec!["pqueue-verify-ledger", "--strict", "--ledger"],
        vec!["pqueue-verify-ledger", "--strict", "--unknown"],
        vec!["pqueue-verify-ledger", "--help"],
    ] {
        assert!(
            run_from_args(args).is_err(),
            "invalid CLI args must be rejected"
        );
    }

    for text in [
        "",
        "[]\n",
        "{\"ac_ids\":[]}\n",
        "{\"ac_ids\":[]} trailing\n",
        "{\"ac_ids\":[\"x\",]}\n",
        "{\"ac_ids\":\"\\u0000\"}\n",
        "{\"ac_ids\":\"\\q\"}\n",
        "{\"seed\":1.25}\n",
        "{\"seed\":1e3}\n",
        "{\"seed\":-}\n",
        "{",
    ] {
        assert!(
            validate_ledger_text(text).is_err(),
            "malformed ledger text should fail: {text:?}"
        );
    }
}

#[test]
fn verification_ledger_tests_cover_required_field_shape_errors() {
    let mut cases = Vec::new();

    let mut row = base_ledger_row();
    row["command"] = serde_json::json!("");
    cases.push((row, "command"));

    let mut row = base_ledger_row();
    row["command"] = serde_json::json!(false);
    cases.push((row, "command"));

    let mut row = base_ledger_row();
    row["ac_ids"] = serde_json::json!([]);
    cases.push((row, "ac_ids"));

    let mut row = base_ledger_row();
    row["ac_ids"] = serde_json::json!([""]);
    cases.push((row, "ac_ids"));

    let mut row = base_ledger_row();
    row["ac_ids"] = serde_json::json!([1]);
    cases.push((row, "ac_ids"));

    let mut row = base_ledger_row();
    row["exit_status"] = serde_json::json!("0");
    cases.push((row, "exit_status"));

    let mut row = base_ledger_row();
    row["seed"] = serde_json::json!(-1);
    cases.push((row, "seed"));

    let mut row = base_ledger_row();
    row["environment"] = serde_json::json!({});
    cases.push((row, "environment"));

    let mut row = base_ledger_row();
    row["environment"] = serde_json::json!("local");
    cases.push((row, "environment"));

    for (row, field) in cases {
        assert_ledger_field_error(row, field);
    }
}

#[test]
fn verification_ledger_tests_cover_product_validation_release_matrix() {
    let mutations: Vec<(&str, Box<dyn Fn(&mut serde_json::Value)>)> = vec![
        (
            "measurements.build_exit_criteria",
            Box::new(|row| {
                row["measurements"]["build_exit_criteria"] =
                    serde_json::json!(["TP-002-E0-E3-pass"])
            }),
        ),
        (
            "measurements.tp002_evidence_ids",
            Box::new(|row| {
                row["measurements"]["tp002_evidence_ids"] = serde_json::json!(["E0", "E1", "E2"])
            }),
        ),
        (
            "measurements.workflow_ac_ids",
            Box::new(|row| {
                row["measurements"]["workflow_ac_ids"] = serde_json::json!(["AC-E2E-1"])
            }),
        ),
        (
            "measurements.postgres_native_conformance_pct",
            Box::new(|row| {
                row["measurements"]["postgres_native_conformance_pct"] = serde_json::json!(99)
            }),
        ),
        (
            "measurements.invariant_stress_matrix_violations",
            Box::new(|row| {
                row["measurements"]["invariant_stress_matrix_violations"] = serde_json::json!(1)
            }),
        ),
        (
            "pass_bar.required_backend_conformance_pct",
            Box::new(|row| {
                row["pass_bar"]
                    .as_object_mut()
                    .unwrap()
                    .remove("required_backend_conformance_pct")
                    .map(drop)
                    .unwrap()
            }),
        ),
        (
            "pass_bar.max_invariant_violations",
            Box::new(|row| {
                row["pass_bar"]
                    .as_object_mut()
                    .unwrap()
                    .remove("max_invariant_violations")
                    .map(drop)
                    .unwrap()
            }),
        ),
    ];

    for (field, mutate) in mutations {
        let mut row = product_validation_release_row();
        mutate(&mut row);
        assert_ledger_field_error(row, field);
    }
}

#[test]
fn verification_ledger_tests_cover_invariant_stress_matrix_errors() {
    let mutations: Vec<(&str, Box<dyn Fn(&mut serde_json::Value)>)> = vec![
        (
            "inv_ids",
            Box::new(|row| row["inv_ids"] = serde_json::json!(["INV-1"])),
        ),
        (
            "inv_ids",
            Box::new(|row| {
                let ids = row["inv_ids"].as_array_mut().unwrap();
                ids.push(serde_json::json!("INV-11"));
            }),
        ),
        (
            "scale",
            Box::new(|row| row["scale"] = serde_json::json!("smoke")),
        ),
        (
            "backend_profile",
            Box::new(|row| row["backend_profile"] = serde_json::json!("memory")),
        ),
        (
            "measurements.concurrency",
            Box::new(|row| row["measurements"]["concurrency"] = serde_json::json!(255)),
        ),
        (
            "measurements.resident_item_sizes",
            Box::new(|row| {
                row["measurements"]["resident_item_sizes"] = serde_json::json!([1000000])
            }),
        ),
        (
            "measurements.kill_count",
            Box::new(|row| row["measurements"]["kill_count"] = serde_json::json!(999)),
        ),
        (
            "measurements.skewed_priority_distribution",
            Box::new(|row| {
                row["measurements"]["skewed_priority_distribution"] = serde_json::json!(false)
            }),
        ),
        (
            "measurements.inv10_violations",
            Box::new(|row| row["measurements"]["inv10_violations"] = serde_json::json!(1)),
        ),
        (
            "pass_bar.max_invariant_violations",
            Box::new(|row| row["pass_bar"] = serde_json::json!({"threshold":"release"})),
        ),
    ];

    for (field, mutate) in mutations {
        let mut row = invariant_stress_row();
        mutate(&mut row);
        assert_ledger_field_error(row, field);
    }
}

#[test]
fn verification_ledger_tests_cover_noisy_neighbor_release_errors() {
    let mutations: Vec<(&str, Box<dyn Fn(&mut serde_json::Value)>)> = vec![
        (
            "ac_ids",
            Box::new(|row| row["ac_ids"] = serde_json::json!(["AC-E2E-6"])),
        ),
        (
            "measurements.tp002_evidence_ids",
            Box::new(|row| row["measurements"]["tp002_evidence_ids"] = serde_json::json!(["E0"])),
        ),
        (
            "measurements.backend_role",
            Box::new(|row| row["measurements"]["backend_role"] = serde_json::json!("wrong")),
        ),
        (
            "measurements.active_queues",
            Box::new(|row| row["measurements"]["active_queues"] = serde_json::json!(999)),
        ),
        (
            "measurements.hot_queue_resident_items",
            Box::new(|row| {
                row["measurements"]["hot_queue_resident_items"] = serde_json::json!(9999999)
            }),
        ),
        (
            "measurements.small_queue_claim_p95_ms",
            Box::new(|row| {
                row["measurements"]["small_queue_claim_p95_ms"] = serde_json::json!(250)
            }),
        ),
        (
            "measurements.small_queue_claim_p99_ms",
            Box::new(|row| {
                row["measurements"]["small_queue_claim_p99_ms"] = serde_json::json!(1000)
            }),
        ),
        (
            "measurements.progress_bound_violations",
            Box::new(|row| row["measurements"]["progress_bound_violations"] = serde_json::json!(1)),
        ),
        (
            "measurements.discover_active_scopes_used",
            Box::new(|row| {
                row["measurements"]["discover_active_scopes_used"] = serde_json::json!(false)
            }),
        ),
    ];

    for (field, mutate) in mutations {
        let mut row = noisy_neighbor_release_row();
        mutate(&mut row);
        assert_ledger_field_error(row, field);
    }
}

#[test]
fn verification_ledger_tests_cover_scale_and_object_log_release_errors() {
    let scale_mutations: Vec<(&str, Box<dyn Fn(&mut serde_json::Value)>)> = vec![
        (
            "backend_profile",
            Box::new(|row| row["backend_profile"] = serde_json::json!("postgres_native")),
        ),
        (
            "measurements.tp002_evidence_ids",
            Box::new(|row| row["measurements"]["tp002_evidence_ids"] = serde_json::json!(["E0"])),
        ),
        (
            "measurements.shard_counts",
            Box::new(|row| row["measurements"]["shard_counts"] = serde_json::json!([2, 8])),
        ),
        (
            "measurements.items_per_hour_by_shard_count",
            Box::new(|row| {
                row["measurements"]["items_per_hour_by_shard_count"] = serde_json::json!([1, 2])
            }),
        ),
        (
            "measurements.items_per_hour_by_shard_count",
            Box::new(|row| {
                row["measurements"]["items_per_hour_by_shard_count"] = serde_json::json!([3, 2, 4])
            }),
        ),
        (
            "pass_bar.eight_shard_min_items_per_hour",
            Box::new(|row| {
                row["pass_bar"]["eight_shard_min_items_per_hour"] = serde_json::json!(39999999)
            }),
        ),
        (
            "measurements.items_per_hour_by_shard_count",
            Box::new(|row| {
                row["measurements"]["items_per_hour_by_shard_count"] =
                    serde_json::json!([20000000, 30000000, 39999999])
            }),
        ),
        (
            "measurements.scale_out_multiple_at_8_shards_x100",
            Box::new(|row| {
                row["measurements"]["scale_out_multiple_at_8_shards_x100"] = serde_json::json!(399)
            }),
        ),
        (
            "measurements.progress_bound_violations",
            Box::new(|row| row["measurements"]["progress_bound_violations"] = serde_json::json!(1)),
        ),
        (
            "measurements.independent_storage_units",
            Box::new(|row| {
                row["measurements"]["independent_storage_units"] = serde_json::json!(false)
            }),
        ),
        (
            "measurements.queue_global_progress_checked",
            Box::new(|row| {
                row["measurements"]["queue_global_progress_checked"] = serde_json::json!(false)
            }),
        ),
        (
            "pass_bar.monotonic_non_decreasing_required",
            Box::new(|row| {
                row["pass_bar"]["monotonic_non_decreasing_required"] = serde_json::json!(false)
            }),
        ),
    ];
    for (field, mutate) in scale_mutations {
        let mut row = multi_shard_scale_out_row();
        mutate(&mut row);
        assert_ledger_field_error(row, field);
    }

    let e3_mutations: Vec<(&str, Box<dyn Fn(&mut serde_json::Value)>)> = vec![
        (
            "backend_profile",
            Box::new(|row| row["backend_profile"] = serde_json::json!("postgres_native")),
        ),
        (
            "measurements.tp002_evidence_ids",
            Box::new(|row| row["measurements"]["tp002_evidence_ids"] = serde_json::json!(["E0"])),
        ),
        (
            "measurements.items_per_hour",
            Box::new(|row| row["measurements"]["items_per_hour"] = serde_json::json!(9999999)),
        ),
        (
            "measurements.p95_ms",
            Box::new(|row| row["measurements"]["p95_ms"] = serde_json::json!(250)),
        ),
        (
            "measurements.p99_ms",
            Box::new(|row| row["measurements"]["p99_ms"] = serde_json::json!(1000)),
        ),
        (
            "measurements.durable_commit_cost_per_billion_commands_usd",
            Box::new(|row| {
                row["measurements"]["durable_commit_cost_per_billion_commands_usd"] =
                    serde_json::json!(200)
            }),
        ),
        (
            "measurements.recovery_items",
            Box::new(|row| row["measurements"]["recovery_items"] = serde_json::json!(9999999)),
        ),
        (
            "measurements.recovery_ms",
            Box::new(|row| row["measurements"]["recovery_ms"] = serde_json::json!(300001)),
        ),
    ];
    for (field, mutate) in e3_mutations {
        let mut row = object_log_e3_row();
        mutate(&mut row);
        assert_ledger_field_error(row, field);
    }
}

fn assert_ledger_field_error(row: serde_json::Value, field: &str) {
    let err = validate_ledger_text(&format!("{row}\n")).expect_err("row should fail validation");
    assert_eq!(err.field.as_deref(), Some(field), "{err}");
}

fn base_ledger_row() -> serde_json::Value {
    serde_json::json!({
        "ac_ids": ["AC-E2E-1"],
        "inv_ids": ["INV-1"],
        "command": "cargo test -p pqueue-service verification_ledger_tests",
        "exit_status": 0,
        "backend_profile": "postgres_native",
        "scale": "smoke",
        "seed": 424242,
        "environment": {"toolchain": "stable-1.92.0"},
        "suite": "custom_smoke_tests",
        "measurements": {"elapsed_ms": 1},
        "pass_bar": {"threshold": "smoke"}
    })
}

fn product_validation_release_row() -> serde_json::Value {
    serde_json::json!({
        "ac_ids": ["AC-E2E-1", "AC-E2E-2", "AC-E2E-3", "AC-E2E-4", "AC-E2E-5", "AC-E2E-6", "AC-E2E-8", "AC-E2E-9"],
        "inv_ids": ["INV-1", "INV-2", "INV-3", "INV-4", "INV-5", "INV-6", "INV-7", "INV-8", "INV-9", "INV-10"],
        "command": "cargo test -p pqueue-service product_validation_tests",
        "exit_status": 0,
        "backend_profile": "aggregate_committed_backends",
        "scale": "release",
        "seed": 9001,
        "environment": {"instance_class": "local-dev"},
        "suite": "product_validation_tests",
        "measurements": {
            "build_exit_criteria": ["BUILD-001-P0-core-implementation-closed", "TP-002-E0-E3-pass", "TP-003-P0-release-gates-green"],
            "tp002_evidence_ids": ["E0", "E1", "E2", "E3"],
            "workflow_ac_ids": ["AC-E2E-1", "AC-E2E-2", "AC-E2E-3", "AC-E2E-4", "AC-E2E-5", "AC-E2E-6", "AC-E2E-8", "AC-E2E-9"],
            "postgres_native_conformance_pct": 100,
            "object_log_sqlite_projection_conformance_pct": 100,
            "invariant_stress_matrix_violations": 0
        },
        "pass_bar": {
            "required_backend_conformance_pct": 100,
            "max_invariant_violations": 0
        }
    })
}

fn invariant_stress_row() -> serde_json::Value {
    serde_json::json!({
        "ac_ids": ["AC-CLAIM-1"],
        "inv_ids": ["INV-1", "INV-2", "INV-3", "INV-4", "INV-5", "INV-6", "INV-7", "INV-8", "INV-9", "INV-10"],
        "command": "cargo test -p pqueue-service invariant_stress_matrix_tests",
        "exit_status": 0,
        "backend_profile": "postgres_native",
        "scale": "release",
        "seed": 7503,
        "environment": {"instance_class": "local-dev"},
        "suite": "invariant_stress_matrix_tests",
        "measurements": {
            "concurrency": 256,
            "resident_item_sizes": [1000000, 10000000],
            "soak_profile": "release",
            "kill_count": 1000,
            "skewed_priority_distribution": true,
            "skewed_group_distribution": true,
            "inv1_violations": 0,
            "inv2_violations": 0,
            "inv3_violations": 0,
            "inv4_violations": 0,
            "inv5_violations": 0,
            "inv6_violations": 0,
            "inv7_violations": 0,
            "inv8_violations": 0,
            "inv9_violations": 0,
            "inv10_violations": 0
        },
        "pass_bar": {"max_invariant_violations": 0}
    })
}

fn noisy_neighbor_release_row() -> serde_json::Value {
    serde_json::json!({
        "ac_ids": ["AC-E2E-6", "AC-DISC-1", "AC-DISC-2", "AC-LAT-3"],
        "inv_ids": ["INV-4"],
        "command": "cargo test -p pqueue-service product_workflows noisy_neighbor",
        "exit_status": 0,
        "backend_profile": "object_log_sqlite_projection",
        "scale": "release",
        "seed": 1701,
        "environment": {"instance_class": "local-dev"},
        "suite": "product_workflow_noisy_neighbor_scale_e2e",
        "measurements": {
            "tp002_evidence_ids": ["E2"],
            "backend_role": "object_log_headline_multi_shard",
            "active_queues": 1000,
            "hot_queue_resident_items": 10000000,
            "small_queue_claim_p95_ms": 180,
            "small_queue_claim_p99_ms": 700,
            "progress_bound_violations": 0,
            "discover_active_scopes_used": true,
            "active_scope_routing_checked": true,
            "unauthorized_queues_excluded": true
        },
        "pass_bar": {
            "min_active_queues": 1000,
            "hot_queue_min_resident_items": 10000000,
            "p95_ms_lt": 250,
            "p99_ms_lt": 1000,
            "max_progress_bound_violations": 0
        }
    })
}

fn multi_shard_scale_out_row() -> serde_json::Value {
    serde_json::json!({
        "ac_ids": ["AC-E2E-6"],
        "inv_ids": ["INV-4"],
        "command": "cargo test -p pqueue-service performance_multi_shard_scale_out_tests",
        "exit_status": 0,
        "backend_profile": "object_log_sqlite_projection",
        "scale": "release",
        "seed": 7202,
        "environment": {"instance_class": "local-dev"},
        "suite": "performance_multi_shard_scale_out_tests",
        "measurements": {
            "deployment_shape": "multi-shard-horizontal-object-log",
            "workload_envelope": "E2",
            "query_plan": "fan-out claim",
            "tp002_evidence_ids": ["E0", "E2"],
            "items_per_hour": 42000000,
            "p95_ms": 180,
            "p99_ms": 700,
            "shard_counts": [2, 4, 8],
            "items_per_hour_by_shard_count": [20000000, 30000000, 42000000],
            "single_deployment_ceiling_items_per_hour": 10000000,
            "scale_out_multiple_at_8_shards_x100": 420,
            "progress_bound_violations": 0,
            "independent_storage_units": true,
            "queue_global_progress_checked": true
        },
        "pass_bar": {
            "e0_floor_items_per_hour": 10000000,
            "p95_ms_lt": 250,
            "p99_ms_lt": 1000,
            "eight_shard_min_items_per_hour": 40000000,
            "minimum_scale_out_multiple_at_8_shards_x100": 400,
            "monotonic_non_decreasing_required": true
        }
    })
}

fn object_log_e3_row() -> serde_json::Value {
    serde_json::json!({
        "ac_ids": ["AC-LAT-1"],
        "inv_ids": ["INV-2"],
        "command": "cargo test -p pqueue-objectlog object_log_commit_recovery_tests",
        "exit_status": 0,
        "backend_profile": "object_log_sqlite_projection",
        "scale": "release",
        "seed": 8103,
        "environment": {"instance_class": "local-dev"},
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
            "recovery_ms": 300000,
            "acked_commands": 1024,
            "manifest_fence_rejections": 1,
            "fallback_fence_rejections": 1
        },
        "pass_bar": {
            "e0_floor_items_per_hour": 10000000,
            "p95_ms_lt": 250,
            "p99_ms_lt": 1000,
            "recovery_window_budget_ms": 300000
        }
    })
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
