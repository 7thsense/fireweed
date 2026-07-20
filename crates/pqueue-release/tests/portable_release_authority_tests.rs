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
        ("exact_outcomes".into(), serde_json::json!(true)),
        ("monotonic_progress".into(), serde_json::json!(true)),
        ("bounded_resources".into(), serde_json::json!(true)),
        ("source_revision".into(), serde_json::json!(REV)),
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
        (
            "sentinel_latency_samples_ms".into(),
            serde_json::json!([2, 7, 12]),
        ),
        ("progress_bound_ms".into(), serde_json::json!(60_000)),
        ("progress_bound_violations".into(), serde_json::json!(0)),
        ("resource_sample_count".into(), serde_json::json!(3)),
        ("max_threads_observed".into(), serde_json::json!(2)),
        ("thread_limit".into(), serde_json::json!(64)),
        ("max_connections_observed".into(), serde_json::json!(1)),
        ("connection_limit".into(), serde_json::json!(1)),
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
        ("connections_peak".into(), serde_json::json!(1)),
        ("connections_limit".into(), serde_json::json!(1)),
        ("pending_tasks_peak".into(), serde_json::json!(2)),
        ("pending_tasks_limit".into(), serde_json::json!(2)),
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
        ("postgres_pool_limit".into(), serde_json::json!(1)),
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
            serde_json::json!("single-process+single-postgres+single-production-connection"),
        ),
        ("telemetry_enabled".into(), serde_json::json!(true)),
        ("telemetry_sample_count".into(), serde_json::json!(3)),
        ("topology_declared".into(), serde_json::json!(true)),
        ("payload_bytes_min".into(), serde_json::json!(1024)),
        ("payload_bytes_max".into(), serde_json::json!(1024)),
        ("group_cardinality".into(), serde_json::json!(64)),
        (
            "priority_profile".into(),
            serde_json::json!("90pct_regular+10pct_high+sentinel_highest"),
        ),
        (
            "resource_measurement_source".into(),
            serde_json::json!(
                "linux_procfs+cgroup_limits+postgres_pg_stat_activity+in_process_operation_counter"
            ),
        ),
    ]);
    if id == "E0" {
        values.extend([
            ("accepted_items".into(), serde_json::json!(10_000_000)),
            ("claimed_items".into(), serde_json::json!(10_000_000)),
            ("finalized_items".into(), serde_json::json!(10_000_000)),
        ]);
    } else {
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
        ("sentinel_latency_samples_ms", serde_json::json!([60_001])),
        ("progress_bound_violations", serde_json::json!(1)),
        ("cursor_samples", serde_json::json!([1, 3, 2])),
        ("configured_concurrency", serde_json::json!(1)),
        ("telemetry_enabled", serde_json::json!(false)),
        ("payload_bytes_min", serde_json::json!(0)),
        ("checkpoint_complete", serde_json::json!(9_999_999)),
        ("postgres_instance_class", serde_json::json!("")),
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
        ("oversize_push_rejected", serde_json::json!(false)),
        ("update_window_sizes", serde_json::json!([1, 100])),
        ("probe_claimed_items", serde_json::json!(1100)),
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
