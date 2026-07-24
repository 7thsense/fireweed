use fireweed_release::e2_failover::{FailoverEvidence, validate};

fn valid() -> FailoverEvidence {
    FailoverEvidence {
        schema_version: 2,
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
        handoff_object_store_profile: None,
    }
}

#[test]
fn e2_failover_validator_accepts_release_row() {
    validate(&valid()).unwrap();
}

#[test]
fn e2_failover_validator_keeps_historical_v1_readable() {
    let row: FailoverEvidence = serde_json::from_str(include_str!(
        "../../../docs/perf/evidence/tp002-e2-objectlog-sqlite-failover-kind.json"
    ))
    .unwrap();
    assert_eq!(row.schema_version, 1);
    assert!(row.handoff_object_store_profile.is_none());
    validate(&row).unwrap();
}

#[test]
fn e2_failover_validator_accepts_v2_handoff_profile() {
    let mut row = valid();
    row.handoff_object_store_profile =
        Some(fireweed_release::e2_failover::HandoffObjectStoreProfile {
            samples: 200,
            queue_items: 1_000,
            scripted_request_latency_ms: 100,
            unapplied_tail_per_handoff: true,
            physical_requests: 347_500,
            p95_modeled_handoff_latency_ms: 325_900,
            p95_perfect_cache_latency_ms: 287_800,
            immutable_gets: 40_600,
            repeated_immutable_gets: 39_999,
            avoidable_immutable_gets: 40_400,
            manifest_candidate_gets: 40_400,
            repeated_manifest_candidate_gets: 39_999,
            segment_gets: 200,
            immutable_response_bytes: 24_587_712,
            tail_commands_replayed: 200,
            first_local_read_requests: 0,
        });
    validate(&row).unwrap();

    let mutations: Vec<fn(&mut fireweed_release::e2_failover::HandoffObjectStoreProfile)> = vec![
        |p| p.p95_modeled_handoff_latency_ms += 1,
        |p| p.p95_perfect_cache_latency_ms += 1,
        |p| p.p95_perfect_cache_latency_ms = p.p95_modeled_handoff_latency_ms + 100,
        |p| p.immutable_gets = p.physical_requests + 1,
        |p| p.immutable_response_bytes = 0,
        |p| p.repeated_manifest_candidate_gets = p.manifest_candidate_gets + 1,
        |p| p.manifest_candidate_gets = p.immutable_gets,
        |p| p.tail_commands_replayed = 0,
    ];
    for mutate in mutations {
        let mut invalid = row.clone();
        mutate(invalid.handoff_object_store_profile.as_mut().unwrap());
        assert!(validate(&invalid).is_err());
    }
}

#[test]
fn e2_failover_validator_rejects_zero_scripted_latency_without_panicking() {
    let mut row = valid();
    row.handoff_object_store_profile =
        Some(fireweed_release::e2_failover::HandoffObjectStoreProfile {
            samples: 1,
            queue_items: 1,
            scripted_request_latency_ms: 0,
            unapplied_tail_per_handoff: false,
            physical_requests: 1,
            p95_modeled_handoff_latency_ms: 1,
            p95_perfect_cache_latency_ms: 1,
            immutable_gets: 0,
            repeated_immutable_gets: 0,
            avoidable_immutable_gets: 0,
            manifest_candidate_gets: 0,
            repeated_manifest_candidate_gets: 0,
            segment_gets: 0,
            immutable_response_bytes: 0,
            tail_commands_replayed: 0,
            first_local_read_requests: 0,
        });
    assert!(validate(&row).is_err());
}

#[test]
fn e2_failover_validator_rejects_missing_or_failed_assertions() {
    let mutations: Vec<fn(&mut FailoverEvidence)> = vec![
        |r| r.new_epoch = r.old_epoch,
        |r| r.stale_append_rejected_before_mutation = false,
        |r| r.snapshot_tail_recovered = false,
        |r| r.lost_work = 1,
        |r| r.double_leases = 1,
        |r| r.corrupt_writes = 1,
        |r| r.moved_count = 0,
        |r| r.retry_count = 2,
        |r| r.retry_succeeded = false,
        |r| {
            r.handoff_object_store_profile =
                Some(fireweed_release::e2_failover::HandoffObjectStoreProfile {
                    samples: 0,
                    queue_items: 1,
                    scripted_request_latency_ms: 25,
                    unapplied_tail_per_handoff: false,
                    physical_requests: 1,
                    p95_modeled_handoff_latency_ms: 25,
                    p95_perfect_cache_latency_ms: 25,
                    immutable_gets: 0,
                    repeated_immutable_gets: 0,
                    avoidable_immutable_gets: 0,
                    manifest_candidate_gets: 0,
                    repeated_manifest_candidate_gets: 0,
                    segment_gets: 0,
                    immutable_response_bytes: 0,
                    tail_commands_replayed: 0,
                    first_local_read_requests: 0,
                })
        },
    ];
    for mutate in mutations {
        let mut row = valid();
        mutate(&mut row);
        assert!(validate(&row).is_err());
    }
}
