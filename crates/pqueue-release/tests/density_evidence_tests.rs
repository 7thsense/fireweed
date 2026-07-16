use pqueue_release::density::{
    DensityMeasurement, DensityMetadata, build_release_row, validate_release_row,
};

fn measurement() -> DensityMeasurement {
    DensityMeasurement {
        total_queues: 1001,
        cold_queues_active: 1000,
        cold_queues_progress_eligible: 1000,
        hot_ingest_per_s: 4_000.0,
        hot_claim_finalize_per_s: 3_500.0,
        progress_bound_violations: 0,
        noisy_neighbor_ingest_retention_pct: 82.5,
        noisy_neighbor_claim_retention_pct: 79.0,
        shared_worker_count: 8,
        shared_worker_limit: 8,
        connection_count: 16,
        connection_limit: 16,
        task_count: 17,
        task_limit: 17,
    }
}

fn metadata() -> DensityMetadata {
    DensityMetadata {
        command: "scripts/perf/tp002-e2-density-kind.sh".into(),
        revision: "0123456789abcdef".into(),
        topology: "live one-node kind deployment; objectlog/sqlite; 1001 generated queues".into(),
        hardware: "8 cores; 32 GiB RAM; kindest/node:v1.31.0".into(),
        seed: 42,
        duration_seconds: 60,
        queue_activity_definition: "cold queue completed claim/finalize and retained one eligible item while the hot queue ran".into(),
    }
}

#[test]
fn semantic_density_validator_accepts_complete_release_row() {
    let row = build_release_row(&measurement(), &metadata());
    assert_eq!(row.evidence_tier, "release");
    validate_release_row(&row).expect("complete density row passes");
}

#[test]
fn semantic_density_validator_rejects_every_required_bar_when_missing() {
    for key in [
        "total_queues",
        "hot_ingest_per_s",
        "hot_claim_finalize_per_s",
        "progress_bound_violations",
        "shared_worker_count",
        "noisy_neighbor_ingest_retention_pct",
    ] {
        let mut row = build_release_row(&measurement(), &metadata());
        row.measurements.values.remove(key);
        assert!(
            validate_release_row(&row).is_err(),
            "missing {key} must be rejected"
        );
    }
}

#[test]
fn density_builder_never_promotes_a_failed_measurement() {
    let mut failed = measurement();
    failed.hot_ingest_per_s = 1.0;
    let row = build_release_row(&failed, &metadata());
    assert_eq!(row.evidence_tier, "smoke");
    assert_eq!(row.scale, "smoke");
    assert!(validate_release_row(&row).is_err());
}
