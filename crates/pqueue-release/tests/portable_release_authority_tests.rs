use std::collections::BTreeMap;

use pqueue_release::{LedgerRow, Measurements};

const REV: &str = "0123456789abcdef0123456789abcdef01234567";

fn portable(id: &str) -> LedgerRow {
    let mut values = BTreeMap::from([
        ("bars_met".into(), serde_json::json!(true)),
        ("portable_gate".into(), serde_json::json!(true)),
        ("quiet_host_required".into(), serde_json::json!(false)),
        ("host_speed_gate".into(), serde_json::json!(false)),
        ("wall_clock_capacity_only".into(), serde_json::json!(true)),
        (
            "fixed_latency_buckets_capacity_only".into(),
            serde_json::json!(true),
        ),
        ("exact_outcomes".into(), serde_json::json!(true)),
        ("monotonic_progress".into(), serde_json::json!(true)),
        ("bounded_resources".into(), serde_json::json!(true)),
        ("source_revision".into(), serde_json::json!(REV)),
        ("checkout_revision".into(), serde_json::json!(REV)),
        ("checkout_root".into(), serde_json::json!("/src/pqueue")),
        ("checkout_clean".into(), serde_json::json!(true)),
        (
            "compile_source_root".into(),
            serde_json::json!("/src/pqueue"),
        ),
        ("compile_source_root_bound".into(), serde_json::json!(true)),
        ("source_root_explicit".into(), serde_json::json!(true)),
        (
            "producer_ingest_completion_per_s".into(),
            serde_json::json!(1000.0),
        ),
        (
            "claimant_finalize_completion_per_s".into(),
            serde_json::json!(900.0),
        ),
        ("producer_completion_ms".into(), serde_json::json!(10_000)),
        ("claimant_completion_ms".into(), serde_json::json!(11_000)),
        (
            "producer_completion_timing".into(),
            serde_json::json!("sum of successful push operation durations"),
        ),
        (
            "claimant_completion_timing".into(),
            serde_json::json!("sum of successful claim and finalize operation durations"),
        ),
        ("resident_set_items".into(), serde_json::json!(10_000_000)),
        (
            "retained_terminal_items".into(),
            serde_json::json!(10_000_000),
        ),
        ("checkpoint_pending".into(), serde_json::json!(0)),
        ("checkpoint_leased".into(), serde_json::json!(0)),
        ("checkpoint_complete".into(), serde_json::json!(10_000_000)),
        ("checkpoint_failed".into(), serde_json::json!(0)),
        ("lost_items".into(), serde_json::json!(0)),
        ("duplicate_claims".into(), serde_json::json!(0)),
        ("identity_epoch_node_prefix".into(), serde_json::json!(256)),
        ("identity_counter_min".into(), serde_json::json!(1)),
        ("identity_counter_max".into(), serde_json::json!(10_000_000)),
        ("identity_bijection".into(), serde_json::json!(true)),
        (
            "progress_samples_finalized".into(),
            serde_json::json!([0, 9_000_000, 10_000_000]),
        ),
        ("progress_sample_count".into(), serde_json::json!(3)),
        ("cursor_samples".into(), serde_json::json!([1, 2, 3])),
        (
            "oldest_eligible_age_samples_ms".into(),
            serde_json::json!([1, 5, 10]),
        ),
        ("discovery_query_count".into(), serde_json::json!(3)),
        ("discovery_nonempty_count".into(), serde_json::json!(3)),
        (
            "progress_identity_sample_count".into(),
            serde_json::json!(10_000_000),
        ),
        (
            "progress_latency_lower_max_ms".into(),
            serde_json::json!(12),
        ),
        (
            "progress_latency_upper_max_ms".into(),
            serde_json::json!(20),
        ),
        (
            "progress_latency_upper_buckets".into(),
            serde_json::json!({"le_1000": 10_000_000, "le_10000": 0, "le_60000": 0, "gt_60000": 0}),
        ),
        (
            "progress_measurement".into(),
            serde_json::json!("per-item accepted and claimed timestamp intervals"),
        ),
        ("progress_bound_ms".into(), serde_json::json!(300_000)),
        (
            "persisted_progress_bound_ms".into(),
            serde_json::json!(300_000),
        ),
        ("progress_bound_explicit".into(), serde_json::json!(true)),
        (
            "progress_bound_buckets".into(),
            serde_json::json!({"within_declared_bound": 10_000_000, "over_declared_bound": 0}),
        ),
        ("progress_bound_violations".into(), serde_json::json!(0)),
        (
            "progress_latency_over_60000_ms_count".into(),
            serde_json::json!(0),
        ),
        ("resource_sample_count".into(), serde_json::json!(3)),
        ("max_threads_observed".into(), serde_json::json!(2)),
        ("thread_limit".into(), serde_json::json!(64)),
        ("max_connections_observed".into(), serde_json::json!(2)),
        ("connection_limit".into(), serde_json::json!(2)),
        (
            "max_rss_bytes_observed".into(),
            serde_json::json!(64 * 1024 * 1024),
        ),
        (
            "rss_limit_bytes".into(),
            serde_json::json!(2_u64 * 1024 * 1024 * 1024),
        ),
        (
            "max_in_flight_operations_observed".into(),
            serde_json::json!(2),
        ),
        ("in_flight_operation_limit".into(), serde_json::json!(2)),
        ("configured_concurrency".into(), serde_json::json!(2)),
        ("shared_workers_peak".into(), serde_json::json!(2)),
        ("shared_workers_limit".into(), serde_json::json!(2)),
        ("workers_started".into(), serde_json::json!(2)),
        ("workers_completed".into(), serde_json::json!(2)),
        ("connections_peak".into(), serde_json::json!(2)),
        ("connections_limit".into(), serde_json::json!(2)),
        ("pending_work_items_peak".into(), serde_json::json!(1000)),
        ("pending_work_items_limit".into(), serde_json::json!(2000)),
        (
            "memory_peak_bytes".into(),
            serde_json::json!(64 * 1024 * 1024),
        ),
        (
            "memory_limit_bytes".into(),
            serde_json::json!(2_u64 * 1024 * 1024 * 1024),
        ),
        ("postgres_server_version".into(), serde_json::json!("16.4")),
        ("postgres_max_connections".into(), serde_json::json!(100)),
        (
            "postgres_shared_buffers_bytes".into(),
            serde_json::json!(128 * 1024 * 1024),
        ),
        (
            "postgres_database_size_bytes".into(),
            serde_json::json!(256 * 1024 * 1024),
        ),
        ("host_cpu_count".into(), serde_json::json!(8)),
        (
            "host_memory_bytes".into(),
            serde_json::json!(32_u64 * 1024 * 1024 * 1024),
        ),
        ("postgres_cpu_limit".into(), serde_json::json!(4)),
        (
            "postgres_memory_limit_bytes".into(),
            serde_json::json!(16_u64 * 1024 * 1024 * 1024),
        ),
        ("postgres_pool_limit".into(), serde_json::json!(2)),
        (
            "postgres_instance_class".into(),
            serde_json::json!("release-pg"),
        ),
        (
            "postgres_iops_profile".into(),
            serde_json::json!("3000-baseline"),
        ),
        ("postgres_storage_class".into(), serde_json::json!("gp3")),
        (
            "topology".into(),
            serde_json::json!("single-process+single-postgres+fixed-2-member-affinity-pool"),
        ),
        (
            "telemetry_surface".into(),
            serde_json::json!("Pqueue::metrics+current_position+discover_active_scopes"),
        ),
        ("telemetry_sample_count".into(), serde_json::json!(3)),
        (
            "lifecycle_snapshots".into(),
            serde_json::json!([
                {"pending": 1000, "leased": 0, "complete": 0, "failed": 0, "resident_terminal_count": 0, "cursor": 1},
                {"pending": 500, "leased": 0, "complete": 5_000_000, "failed": 0, "resident_terminal_count": 5_000_000, "cursor": 2},
                {"pending": 0, "leased": 0, "complete": 10_000_000, "failed": 0, "resident_terminal_count": 10_000_000, "cursor": 3}
            ]),
        ),
        ("topology_declared".into(), serde_json::json!(true)),
        (
            "payload_size_counts".into(),
            serde_json::json!({"512": 3_333_334, "1024": 3_333_333, "2048": 3_333_333}),
        ),
        (
            "group_item_counts".into(),
            serde_json::json!(vec![156_250_u64; 64]),
        ),
        (
            "priority_class_counts".into(),
            serde_json::json!({"regular": 8_981_000, "high": 999_000, "sentinel": 20_000}),
        ),
        (
            "workload_operation_mix".into(),
            serde_json::json!({"push_batches": 20_000, "claim_batches": 20_000, "finalize_batches": 20_000}),
        ),
        (
            "resource_measurement_source".into(),
            serde_json::json!(
                "linux_procfs+declared_workload_caps+postgres_pg_stat_activity+natural_operation_counter"
            ),
        ),
    ]);
    if id == "E0" {
        values.extend([
            ("accepted_items".into(), serde_json::json!(10_000_000)),
            ("claimed_items".into(), serde_json::json!(10_000_000)),
            ("finalized_items".into(), serde_json::json!(10_000_000)),
            (
                "one_instance_production_wrapper".into(),
                serde_json::json!(true),
            ),
            ("production_pool_size".into(), serde_json::json!(2)),
            (
                "production_pool_connections_observed".into(),
                serde_json::json!(2),
            ),
            ("hot_queue_pool_partition".into(), serde_json::json!(0)),
            ("canary_queue_pool_partition".into(), serde_json::json!(1)),
            (
                "canary_observed_hot_pg_sleep".into(),
                serde_json::json!(true),
            ),
            ("canary_exact_outcomes".into(), serde_json::json!(true)),
            (
                "canary_completed_before_hot".into(),
                serde_json::json!(true),
            ),
            ("canary_causal_progress".into(), serde_json::json!(true)),
        ]);
    } else {
        for operation in ["push", "update_window", "claim", "finalize"] {
            for batch_size in [1, 100, 1_000] {
                for percentile in ["p50", "p95", "p99"] {
                    values.insert(
                        format!("{operation}_b{batch_size}_{percentile}_ms"),
                        serde_json::json!(1.0),
                    );
                }
            }
        }
        for key in [
            "push_batch_sizes",
            "update_window_sizes",
            "claim_batch_sizes",
            "finalize_batch_sizes",
        ] {
            values.insert(key.into(), serde_json::json!([1, 100, 1000]));
        }
        values.insert("configured_max_batch_size".into(), serde_json::json!(1000));
        values.insert(
            "persisted_max_push_batch_size".into(),
            serde_json::json!(1000),
        );
        values.insert(
            "persisted_max_claim_batch_size".into(),
            serde_json::json!(1000),
        );
        values.insert("oversize_push_rejected".into(), serde_json::json!(true));
        values.insert("oversize_claim_rejected".into(), serde_json::json!(true));
        values.insert("probe_accepted_items".into(), serde_json::json!(1101));
        values.insert("probe_claimed_items".into(), serde_json::json!(1101));
        values.insert("probe_finalized_items".into(), serde_json::json!(1101));
        values.insert("probe_unique_accepted_ids".into(), serde_json::json!(1101));
        values.insert("probe_unique_claimed_ids".into(), serde_json::json!(1101));
        values.insert("probe_unique_finalized_ids".into(), serde_json::json!(1101));
        values.insert("probe_identity_exact".into(), serde_json::json!(true));
        values.insert("post_probe_pending".into(), serde_json::json!(0));
        values.insert("post_probe_leased".into(), serde_json::json!(0));
        values.insert("post_probe_complete".into(), serde_json::json!(10_001_101));
        values.insert("post_probe_failed".into(), serde_json::json!(0));
        values.insert(
            "post_probe_resident_terminal_count".into(),
            serde_json::json!(10_001_101),
        );
        values.insert(
            "probe_operation_mix".into(),
            serde_json::json!({"push_items": 1101, "push_batches": 10, "update_item_calls": 1000, "claim_items": 1101, "claim_batches": 10, "finalize_items": 1101, "finalize_batches": 10}),
        );
        values.insert(
            "post10m_affinity_serialization_probe".into(),
            serde_json::json!(true),
        );
        values.insert(
            "post10m_caller_interval_overlap_observed".into(),
            serde_json::json!(true),
        );
        values.insert(
            "post10m_caller_in_flight_observed".into(),
            serde_json::json!(2),
        );
        values.insert(
            "post10m_active_pending_before".into(),
            serde_json::json!(1000),
        );
        values.insert("total_accepted_items".into(), serde_json::json!(10_001_101));
        values.insert("total_claimed_items".into(), serde_json::json!(10_001_101));
        values.insert(
            "total_finalized_items".into(),
            serde_json::json!(10_001_101),
        );
    }
    LedgerRow {
        suite: "performance_single_deployment_baseline_tests".into(),
        command: "exact producer".into(),
        backend_profile: "postgres_native".into(),
        scale: "release".into(),
        seed: 0,
        environment: "declared topology under ordinary load".into(),
        exit_status: 0,
        ac_ids: vec![],
        inv_ids: vec![],
        pass_bar: "exact outcomes progress and bounded resources; timings are capacity only".into(),
        evidence_tier: "release".into(),
        measurements: Measurements {
            tp002_evidence_ids: vec![id.into()],
            values,
        },
    }
}

#[test]
fn portable_e0_e1_accept_semantics_not_machine_speed() {
    for id in ["E0", "E1"] {
        pqueue_release::single_deployment::validate_row(&portable(id), id, REV).unwrap();
    }
}

#[test]
fn portable_e0_e1_require_exact_governed_evidence_identity() {
    for id in ["E0", "E1"] {
        let row = portable(id);
        pqueue_release::single_deployment::validate_row(&row, id, REV).unwrap();

        for evidence_ids in [vec![], vec![id.into(), "E2".into()]] {
            let mut malformed = row.clone();
            malformed.measurements.tp002_evidence_ids = evidence_ids;
            assert!(
                pqueue_release::single_deployment::validate_row(&malformed, id, REV).is_err(),
                "{id} authority must carry exactly one matching governed evidence id"
            );
        }
    }
}

#[test]
fn e0_rejects_legacy_nonwrapper_and_noncausal_rows() {
    let row = portable("E0");
    for key in [
        "one_instance_production_wrapper",
        "production_pool_size",
        "production_pool_connections_observed",
        "canary_observed_hot_pg_sleep",
        "canary_exact_outcomes",
        "canary_completed_before_hot",
        "canary_causal_progress",
    ] {
        let mut legacy = row.clone();
        legacy.measurements.values.remove(key);
        assert!(
            pqueue_release::single_deployment::validate_row(&legacy, "E0", REV).is_err(),
            "legacy E0 row missing {key} must fail closed"
        );
    }

    let mut same_member = row;
    same_member
        .measurements
        .values
        .insert("canary_queue_pool_partition".into(), serde_json::json!(0));
    assert!(pqueue_release::single_deployment::validate_row(&same_member, "E0", REV).is_err());
}

#[test]
fn portable_e0_e1_accept_reconciled_progress_timing_over_bucket_boundary() {
    for id in ["E0", "E1"] {
        let mut row = portable(id);
        row.measurements.values.insert(
            "oldest_eligible_age_samples_ms".into(),
            serde_json::json!([1, 60_001, 263_708]),
        );
        row.measurements.values.insert(
            "progress_latency_upper_max_ms".into(),
            serde_json::json!(263_708),
        );
        row.measurements.values.insert(
            "progress_latency_upper_buckets".into(),
            serde_json::json!({
                "le_1000": 9_000_000,
                "le_10000": 500_000,
                "le_60000": 419_455,
                "gt_60000": 80_545,
            }),
        );
        row.measurements.values.insert(
            "progress_latency_over_60000_ms_count".into(),
            serde_json::json!(80_545),
        );
        pqueue_release::single_deployment::validate_row(&row, id, REV).unwrap();
    }
}

#[test]
fn e0_e1_reject_quiet_host_and_absolute_speed_bars() {
    for text in ["quiet host required", "throughput >= 2778", "p99 < 1000ms"] {
        let mut row = portable("E0");
        row.pass_bar = text.into();
        assert!(pqueue_release::single_deployment::validate_row(&row, "E0", REV).is_err());
    }
}

#[test]
fn e0_e1_reject_unmeasured_progress_and_resource_claims() {
    let mut row = portable("E0");
    for key in [
        "progress_samples_finalized",
        "resource_sample_count",
        "max_threads_observed",
        "resource_measurement_source",
        "progress_latency_lower_max_ms",
        "progress_latency_upper_max_ms",
    ] {
        let saved = row.measurements.values.remove(key).unwrap();
        assert!(
            pqueue_release::single_deployment::validate_row(&row, "E0", REV).is_err(),
            "missing {key} must fail closed"
        );
        row.measurements.values.insert(key.into(), saved);
    }

    row.measurements.values.insert(
        "progress_samples_finalized".into(),
        serde_json::json!([0, 0, 0]),
    );
    assert!(pqueue_release::single_deployment::validate_row(&row, "E0", REV).is_err());
}

#[test]
fn e0_e1_reject_resources_over_bounds_and_non_maximum_batch_probe() {
    let mut e0 = portable("E0");
    e0.measurements
        .values
        .insert("max_threads_observed".into(), serde_json::json!(65));
    assert!(pqueue_release::single_deployment::validate_row(&e0, "E0", REV).is_err());

    let mut e1 = portable("E1");
    e1.measurements
        .values
        .insert("configured_max_batch_size".into(), serde_json::json!(2_000));
    assert!(pqueue_release::single_deployment::validate_row(&e1, "E1", REV).is_err());
}

#[test]
fn e0_e1_fail_closed_on_progress_topology_workload_and_reconciliation_drift() {
    let common_mutations = [
        (
            "progress_identity_sample_count",
            serde_json::json!(9_999_999),
        ),
        (
            "fixed_latency_buckets_capacity_only",
            serde_json::json!(false),
        ),
        ("progress_bound_explicit", serde_json::json!(false)),
        ("persisted_progress_bound_ms", serde_json::json!(60_000)),
        ("progress_bound_ms", serde_json::json!(0)),
        ("oldest_eligible_age_samples_ms", serde_json::json!([])),
        (
            "oldest_eligible_age_samples_ms",
            serde_json::json!([1, 300_001]),
        ),
        ("discovery_nonempty_count", serde_json::json!(0)),
        ("progress_latency_upper_max_ms", serde_json::json!(300_001)),
        (
            "progress_bound_buckets",
            serde_json::json!({"within_declared_bound": 9_999_999, "over_declared_bound": 1}),
        ),
        ("progress_bound_violations", serde_json::json!(1)),
        (
            "progress_latency_upper_buckets",
            serde_json::json!({"le_1000": 9_999_999, "gt_60000": 1}),
        ),
        ("progress_latency_over_60000_ms_count", serde_json::json!(1)),
        ("cursor_samples", serde_json::json!([1, 3, 2])),
        ("checkout_revision", serde_json::json!("wrong")),
        ("checkout_clean", serde_json::json!(false)),
        ("compile_source_root_bound", serde_json::json!(false)),
        (
            "compile_source_root",
            serde_json::json!("/src/another-worktree"),
        ),
        ("source_root_explicit", serde_json::json!(false)),
        ("producer_ingest_completion_per_s", serde_json::json!(0.0)),
        (
            "claimant_finalize_completion_per_s",
            serde_json::json!("fast"),
        ),
        (
            "producer_completion_timing",
            serde_json::json!("whole thread elapsed"),
        ),
        ("identity_bijection", serde_json::json!(false)),
        ("identity_counter_max", serde_json::json!(9_999_999)),
        ("configured_concurrency", serde_json::json!(1)),
        ("workers_started", serde_json::json!(1)),
        ("shared_workers_peak", serde_json::json!(1)),
        ("max_connections_observed", serde_json::json!(1)),
        ("pending_work_items_peak", serde_json::json!(2001)),
        ("telemetry_surface", serde_json::json!("literal")),
        ("lifecycle_snapshots", serde_json::json!([])),
        (
            "payload_size_counts",
            serde_json::json!({"1024": 10_000_000}),
        ),
        ("group_item_counts", serde_json::json!(vec![10_000_000_u64])),
        (
            "priority_class_counts",
            serde_json::json!({"regular": 10_000_000}),
        ),
        (
            "workload_operation_mix",
            serde_json::json!({"push_batches": 1, "claim_batches": 2, "finalize_batches": 1}),
        ),
        ("checkpoint_complete", serde_json::json!(9_999_999)),
        ("postgres_instance_class", serde_json::json!("")),
        ("topology_declared", serde_json::json!(false)),
    ];
    for (key, value) in common_mutations {
        let mut row = portable("E0");
        row.measurements.values.insert(key.into(), value);
        assert!(
            pqueue_release::single_deployment::validate_row(&row, "E0", REV).is_err(),
            "drift in {key} must fail closed"
        );
    }

    for (key, value) in [
        ("push_b1_p50_ms", serde_json::json!(0.0)),
        ("update_window_b100_p95_ms", serde_json::json!("slow")),
        ("finalize_b1000_p99_ms", serde_json::Value::Null),
        ("oversize_push_rejected", serde_json::json!(false)),
        ("update_window_sizes", serde_json::json!([1, 100])),
        ("probe_claimed_items", serde_json::json!(1100)),
        ("probe_unique_claimed_ids", serde_json::json!(1100)),
        ("probe_identity_exact", serde_json::json!(false)),
        ("post_probe_complete", serde_json::json!(10_001_100)),
        (
            "post_probe_resident_terminal_count",
            serde_json::json!(10_001_100),
        ),
        ("post_probe_pending", serde_json::json!(1)),
        (
            "probe_operation_mix",
            serde_json::json!({"push_items": 1101, "push_batches": 10, "update_item_calls": 0, "claim_items": 1101, "claim_batches": 10, "finalize_items": 1101, "finalize_batches": 10}),
        ),
        (
            "post10m_affinity_serialization_probe",
            serde_json::json!(false),
        ),
        (
            "post10m_caller_interval_overlap_observed",
            serde_json::json!(false),
        ),
        ("total_finalized_items", serde_json::json!(10_001_100)),
    ] {
        let mut row = portable("E1");
        row.measurements.values.insert(key.into(), value);
        assert!(
            pqueue_release::single_deployment::validate_row(&row, "E1", REV).is_err(),
            "drift in {key} must fail closed"
        );
    }
}
