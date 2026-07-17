use pqueue_release::e2_failover::{FailoverEvidence, validate};

fn valid() -> FailoverEvidence {
    FailoverEvidence {
        schema_version: 1,
        suite: "tp002_e2_objectlog_sqlite_failover_kind".into(),
        command: "bash scripts/perf/tp002-e2-failover-kind.sh --release".into(),
        evidence_id: "E2_FAILOVER".into(),
        evidence_tier: "release".into(),
        scale: "release".into(),
        backend_profile: "object_log_sqlite_projection".into(),
        bars_met: true,
        replicas: 3,
        image: "pqueue:e2-failover".into(),
        image_id: "sha256:abc".into(),
        source_revision: "abc".into(),
        chart_revision: "abc".into(),
        postgres_image: "postgres:16".into(),
        minio_image: "minio/minio:latest".into(),
        old_owner_id: "uid-a".into(),
        new_owner_id: "uid-b".into(),
        old_epoch: 1,
        new_epoch: 2,
        stale_append_rejected_before_mutation: true,
        snapshot_tail_recovered: true,
        visible_items_before: 3,
        visible_items_after: 3,
        lost_work: 0,
        double_leases: 0,
        corrupt_writes: 0,
        moved_count: 1,
        retry_count: 1,
        retry_succeeded: true,
        moved_endpoint: "10.244.0.9:8080".into(),
        topology: "kind; three pqueue pods; MinIO; Postgres".into(),
        hardware: "test host".into(),
        seed: 424242,
        duration_ms: 1000,
        fault_schedule: "delete active owner after durable pushes".into(),
        exclusions: "density and throughput are separate E2 rows".into(),
    }
}

#[test]
fn e2_failover_validator_accepts_release_row() {
    validate(&valid()).unwrap();
}

#[test]
fn e2_failover_validator_rejects_missing_or_failed_assertions() {
    let mutations: Vec<Box<dyn Fn(&mut FailoverEvidence)>> = vec![
        Box::new(|r| r.new_epoch = r.old_epoch),
        Box::new(|r| r.stale_append_rejected_before_mutation = false),
        Box::new(|r| r.snapshot_tail_recovered = false),
        Box::new(|r| r.lost_work = 1),
        Box::new(|r| r.double_leases = 1),
        Box::new(|r| r.corrupt_writes = 1),
        Box::new(|r| r.moved_count = 0),
        Box::new(|r| r.retry_count = 2),
        Box::new(|r| r.retry_succeeded = false),
    ];
    for mutate in mutations {
        let mut row = valid();
        mutate(&mut row);
        assert!(validate(&row).is_err());
    }
}
