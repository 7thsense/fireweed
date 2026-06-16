#![forbid(unsafe_code)]

mod support;

use support::scale_evidence::{
    BenchConfig, BenchScenario, E0_FLOOR_ITEMS_PER_HOUR, run_bench_scenario,
};

#[test]
fn performance_hot_queue_10m_tests_smoke_records_e0_e2_groundwork() {
    let mut cfg = BenchConfig::from_env(6202);
    cfg.assert_smoke_scale();
    cfg.shard_count = cfg.shard_count.max(2);
    cfg.queue_count = cfg.queue_count.max(1_000);

    let scenario = BenchScenario {
        suite: "performance_hot_queue_10m_tests",
        ac_ids: &["AC-E2E-6", "AC-LAT-3"],
        inv_ids: &["INV-4"],
        deployment_shape: "multi-shard-horizontal",
        workload_envelope: "E2",
        tp002_evidence_ids: &["E0", "E2"],
        operation_mix: "one-hot-queue-plus-999-active-queues",
        batch_size: 1_000,
        resident_items: 10_000,
        query_plan: "smoke-plan: active-scope top-n uses bounded summary rows; no per-queue worker",
        p95_ms: 175,
        p99_ms: 750,
        items_per_hour: E0_FLOOR_ITEMS_PER_HOUR,
    };

    run_bench_scenario(&cfg, &scenario);
}
