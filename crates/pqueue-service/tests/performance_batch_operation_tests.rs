#![forbid(unsafe_code)]

mod support;

use support::scale_evidence::{
    BenchConfig, BenchScenario, E0_FLOOR_ITEMS_PER_HOUR, run_bench_scenario,
};

#[test]
fn performance_batch_operation_tests_smoke_records_latency_and_e1_fields() {
    let cfg = BenchConfig::from_env(6201);
    cfg.assert_smoke_scale();
    let scenario = BenchScenario {
        suite: "performance_batch_operation_tests",
        ac_ids: &["AC-LAT-1", "AC-LAT-4"],
        inv_ids: &["INV-4"],
        deployment_shape: "single-deployment",
        workload_envelope: "E1",
        tp002_evidence_ids: &["E0", "E1"],
        operation_mix: "representative-seventh-sense-ingest-claim-finalize",
        batch_size: 100,
        resident_items: 10_000,
        query_plan: "smoke-plan: indexed queue_id/not_before/priority lookup; no full scan",
        p95_ms: 125,
        p99_ms: 500,
        items_per_hour: E0_FLOOR_ITEMS_PER_HOUR,
    };

    run_bench_scenario(&cfg, &scenario);
}
