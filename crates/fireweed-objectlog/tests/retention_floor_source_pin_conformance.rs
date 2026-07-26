//! Retention-floor and source-pin conformance: deleted-prefix fail-closed, retained floor/head replay,
//! and branch pinning invariants (governing: docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md:224).
//!
//! AC-MAP:
//!   TestConformanceRetentionFloorSourcePinObjectlogInvariant:
//!     - Assert retention-floor and source-pin guarantees remain intact during deleted-prefix
//!       fail-closed and retained floor/head replay recovery.
//!     - Covers deleted-prefix fail-closed (from_seq <= floor returns Storage error),
//!       retained floor/head replay (above-floor reads succeed, recovery preserves watermark),
//!       and source-pin guarantees (branches prevent reclamation of pinned segments).

use fireweed_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use std::sync::Arc;

use fireweed_engine::{
    CommandEnvelope, CommandId, CommandPosition, EngineError, PushCommand, QueueCommand,
};
use fireweed_objectlog::segmented::{InMemoryBlobStore, SegmentConfig, SegmentedObjectLog};

fn sk(tenant: &str, queue: &str) -> fireweed_engine::QueueKey {
    fireweed_engine::QueueKey::new(TenantId::new(tenant).unwrap(), QueueId::new(queue).unwrap())
}

fn qdef(tenant: &str, queue: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new(tenant).unwrap(),
        queue_id: QueueId::new(queue).unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

fn push_envelopes(n: u64) -> Vec<CommandEnvelope> {
    (0..n)
        .map(|_| CommandEnvelope {
            command_id: CommandId::new("c"),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: vec![],
            command: QueueCommand::Push(PushCommand { items: vec![] }),
            checksum: fireweed_engine::CommandChecksum(0),
            created_at: fireweed_core::UtcTimestamp::new(0, 0).unwrap(),
        })
        .collect()
}

fn trim_through(
    log: &SegmentedObjectLog<Arc<InMemoryBlobStore>>,
    shard: &fireweed_engine::QueueKey,
    through_seq: u64,
    _epoch: u64,
    now_ms: i64,
) {
    let epoch = log
        .acquire_epoch(shard, now_ms)
        .expect("acquire trim owner");
    log.advance_retention_floor(
        shard,
        CommandPosition::new(shard.clone(), epoch, through_seq),
        epoch,
    )
    .expect("advance retention floor");
    log.expire_segments_through(shard, through_seq, now_ms)
        .expect("expire segments");
}

/// Verify deleted-prefix fail-closed, retained floor/head replay, and reopen persistence.
fn retention_floor_fail_closed_and_recovery(store: Arc<InMemoryBlobStore>, cfg: SegmentConfig) {
    let shard = sk("rfsp", "conformance");
    let def = qdef("rfsp", "conformance");

    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&def).expect("create queue");

    for i in 0..4u64 {
        let seal_ms = (i as i64 + 1) * 10;
        log.enqueue(&shard, &push_envelopes(2), 0, seal_ms)
            .expect("enqueue");
        log.seal(&shard, 0, seal_ms + 1).expect("seal");
    }
    let all_seqs: Vec<u64> = log
        .read_from(&shard, 0)
        .expect("read all before trim")
        .iter()
        .map(|(pos, _)| pos.sequence)
        .collect();
    assert_eq!(all_seqs, vec![0, 1, 2, 3, 4, 5, 6, 7]);

    trim_through(&log, &shard, 3, 0, 1_000);

    let floor = log
        .read_retention_floor(&shard)
        .expect("read retention floor")
        .expect("floor exists after advance");
    assert_eq!(floor.sequence, 3);

    // Fail-closed below floor (both read_all and read_from).
    for bad_seq in [0u64, 1, 2, 3] {
        let result = log.read_from(&shard, bad_seq);
        assert!(result.is_err(), "read_from({bad_seq}) should fail closed");
        let err = result.unwrap_err();
        assert!(
            matches!(&err, EngineError::Storage(msg) if msg.contains("read below retention floor")),
            "read_from({bad_seq}): {err:?}"
        );
    }
    let err = log.read_all(&shard).expect_err("read_all below floor");
    assert!(
        matches!(&err, EngineError::Storage(msg) if msg.contains("read below retention floor")),
        "read_all: {err:?}"
    );

    // Above-floor reads succeed (floor+1 is the boundary).
    let tail_seqs: Vec<u64> = log
        .read_from(&shard, 4)
        .expect("read_from(4) above floor")
        .iter()
        .map(|(pos, _)| pos.sequence)
        .collect();
    assert_eq!(tail_seqs, vec![4, 5, 6, 7]);

    // Reopen: watermark and floor survive.
    drop(log);
    let reopened = SegmentedObjectLog::open(store, cfg);
    reopened
        .create_queue(&qdef("rfsp", "conformance"))
        .expect("reopen: create queue");

    let deletion_watermark = reopened
        .read_manifest_deletion_watermark(&shard)
        .expect("deletion watermark after reopen");
    assert!(
        deletion_watermark.is_some(),
        "deletion watermark watermark must persist"
    );

    let recovered_floor = reopened
        .read_retention_floor(&shard)
        .expect("read retention floor after reopen")
        .expect("floor must survive reopen");
    assert_eq!(recovered_floor.sequence, 3);

    let err = reopened
        .read_all(&shard)
        .expect_err("read_all fails closed after reopen");
    assert!(
        matches!(&err, EngineError::Storage(msg) if msg.contains("read below retention floor")),
        "after reopen: {err:?}"
    );

    let tail_after: Vec<u64> = reopened
        .read_from(&shard, 4)
        .expect("read_from(4) above floor after reopen")
        .iter()
        .map(|(pos, _)| pos.sequence)
        .collect();
    assert_eq!(tail_after, vec![4, 5, 6, 7]);
}

/// Source-pin guarantee: a branch prevents reclamation of its pinned source segment,
/// and the pin survives expire passes until released. The watermark only advances
/// (and fail-closed only activates) after the pin is released.
fn source_pin_blocks_reclamation(store: Arc<InMemoryBlobStore>, cfg: SegmentConfig) {
    let shard = sk("rfsp", "sourcepin");
    let def = qdef("rfsp", "sourcepin");

    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&def).expect("create queue");

    for i in 0..3u64 {
        let seal_ms = (i as i64 + 1) * 10;
        log.enqueue(&shard, &push_envelopes(2), 0, seal_ms)
            .expect("enqueue");
        log.seal(&shard, 0, seal_ms + 1).expect("seal");
    }

    // Create a branch pinning at seq 1 (covers segment with commands seq 0-1, index 0).
    let branch_shard = sk("rfsp", "pinbranch");
    let branch_def = qdef("rfsp", "pinbranch");
    let pin_pos = CommandPosition::new(shard.clone(), 0, 1);
    log.branch(&shard, &branch_def, &pin_pos, 600_000, 1_000)
        .expect("branch to create source pin");

    // Advance floor through seq 3 and attempt to expire.
    trim_through(&log, &shard, 3, 0, 1_000);

    // The branch pin must still be registered (expire skipped the pinned segment).
    let pinned_seq = log
        .lowest_branch_pinned_below(&shard, 3, 1_000)
        .expect("lowest branch pinned below")
        .expect("should have a pinned segment below seq 3");
    assert_eq!(pinned_seq, 0, "segment first_seq=0 is pinned");

    // The watermark is NOT persisted because the pinned segment blocks it.
    let watermark_before = log
        .read_manifest_deletion_watermark(&shard)
        .expect("deletion watermark before pin release");
    assert!(watermark_before.is_none(), "watermark blocked by pin");

    // A gap in the reclaimed prefix still fails closed even while the first segment remains pinned.
    // The watermark is progress metadata, not permission to skip missing current-format history.
    let err = log
        .read_from(&shard, 0)
        .expect_err("read_from(0) fails closed across a reclaimed gap");
    assert!(matches!(err, EngineError::Storage(_)));

    // Above-floor reads still succeed.
    let tail: Vec<u64> = log
        .read_from(&shard, 4)
        .expect("read_from(4) above floor")
        .iter()
        .map(|(pos, _)| pos.sequence)
        .collect();
    assert_eq!(tail, vec![4, 5], "above-floor tail readable with pin");

    // Release the pin and verify it's gone.
    log.discard_branch(&shard, &branch_shard)
        .expect("discard branch");
    let pinned_after = log
        .lowest_branch_pinned_below(&shard, 3, 1_000)
        .expect("lowest branch pinned below after discard");
    assert!(pinned_after.is_none(), "no pinned segment after discard");

    // Re-expire now that the pin is released. This also reclaims the previously-pinned
    // segment (index 0) and advances the watermark.
    log.expire_segments_through(&shard, 3, 1_000)
        .expect("expire after pin release");

    // Now the watermark should have advanced, so reads below the floor fail closed.
    let watermark_after = log
        .read_manifest_deletion_watermark(&shard)
        .expect("deletion watermark after pin release + re-expire");
    assert!(
        watermark_after.is_some(),
        "watermark advances after pin release"
    );

    for bad_seq in [0u64, 1, 2, 3] {
        let result = log.read_from(&shard, bad_seq);
        assert!(
            result.is_err(),
            "read_from({bad_seq}) should fail closed after pin release"
        );
    }
}

#[test]
#[allow(non_snake_case)]
fn TestConformanceRetentionFloorSourcePinObjectlogInvariant() {
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();

    retention_floor_fail_closed_and_recovery(Arc::new(InMemoryBlobStore::new()), cfg);
    source_pin_blocks_reclamation(Arc::new(InMemoryBlobStore::new()), cfg);
}
