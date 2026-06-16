#![forbid(unsafe_code)]

mod support;

use support::scale_evidence::{
    BenchConfig, BenchScenario, E0_FLOOR_ITEMS_PER_HOUR, run_bench_scenario,
};

#[test]
#[ignore = "release-scale single-deployment baseline runner is opt-in"]
fn performance_single_deployment_baseline_tests_release_records_e1_baseline() {
    let cfg = BenchConfig::from_env(6211);
    assert_eq!(
        cfg.backend_profile, "postgres_native",
        "E1 baseline is the postgres_native single-deployment profile"
    );
    assert_eq!(
        cfg.scale, "release",
        "single-deployment baseline is a release-scale runner"
    );

    let scenario = BenchScenario {
        suite: "performance_single_deployment_baseline_tests",
        ac_ids: &["AC-LAT-1", "AC-LAT-4"],
        inv_ids: &["INV-4"],
        deployment_shape: "single-deployment",
        workload_envelope: "E1",
        tp002_evidence_ids: &["E0", "E1"],
        operation_mix: "representative-seventh-sense-ingest-claim-finalize",
        batch_size: 1_000,
        resident_items: 10_000_000,
        query_plan: "release-plan-capture-required: explain/analyze attached by runner",
        p95_ms: 200,
        p99_ms: 900,
        items_per_hour: E0_FLOOR_ITEMS_PER_HOUR,
    };

    run_bench_scenario(&cfg, &scenario);
}
