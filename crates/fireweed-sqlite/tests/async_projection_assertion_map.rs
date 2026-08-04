//! One-to-one assertion migration ledger for the retired `hybrid_async` test nomenclature.

use std::collections::BTreeSet;

const ASSERTION_MAP: &[(&str, &str)] = &[
    (
        "hybrid_async_backpressure_zero_threshold_is_rejected",
        "async_projection_backpressure_zero_threshold_is_rejected",
    ),
    (
        "hybrid_async_backpressure_debt_crosses_soft_then_hard_bands",
        "async_projection_backpressure_debt_crosses_soft_then_hard_bands",
    ),
    (
        "hybrid_async_backpressure_any_single_metric_at_its_hard_limit_trips_backpressure",
        "async_projection_backpressure_any_single_metric_at_its_hard_limit_trips_backpressure",
    ),
    (
        "hybrid_async_backpressure_hard_backpressure_holds_until_debt_clears_below_half_after_a_clean_batch",
        "async_projection_backpressure_hard_backpressure_holds_until_debt_clears_below_half_after_a_clean_batch",
    ),
    (
        "hybrid_async_backpressure_admission_gate_rejects_mutations_only_under_hard_backpressure",
        "async_projection_backpressure_admission_gate_rejects_mutations_only_under_hard_backpressure",
    ),
    (
        "hybrid_async_backpressure_repeated_apply_failure_poisons_and_fails_closed",
        "async_projection_backpressure_repeated_apply_failure_poisons_and_fails_closed",
    ),
    (
        "hybrid_async_backpressure_a_clean_batch_resets_the_consecutive_retry_count_before_poison",
        "async_projection_backpressure_a_clean_batch_resets_the_consecutive_retry_count_before_poison",
    ),
    (
        "hybrid_async_backpressure_non_contiguous_apply_poisons_immediately",
        "async_projection_backpressure_non_contiguous_apply_poisons_immediately",
    ),
    (
        "hybrid_async_backpressure_recovery_high_water_is_withheld_under_hard_backpressure_and_poison",
        "async_projection_backpressure_recovery_high_water_is_withheld_under_hard_backpressure_and_poison",
    ),
    (
        "hybrid_async_backpressure_retention_advances_only_when_clear_and_healthy",
        "async_projection_backpressure_retention_advances_only_when_clear_and_healthy",
    ),
    (
        "hybrid_async_backpressure_backpressure_count_and_duration_are_tracked",
        "async_projection_backpressure_backpressure_count_and_duration_are_tracked",
    ),
    (
        "hybrid_async_backpressure_metrics_snapshot_exposes_the_full_observability_surface",
        "async_projection_backpressure_metrics_snapshot_exposes_the_full_observability_surface",
    ),
    (
        "hybrid_async_backpressure_checkpoint_store_reports_wal_size_and_apply_lag",
        "async_projection_backpressure_checkpoint_store_reports_wal_size_and_apply_lag",
    ),
    (
        "hybrid_async_chaos_crash_after_objectlog_commit_before_apply_replays_full_tail",
        "async_projection_chaos_crash_after_objectlog_commit_before_apply_replays_full_tail",
    ),
    (
        "hybrid_async_chaos_crash_after_memory_apply_before_sqlite_apply_resumes_at_prefix",
        "async_projection_chaos_crash_after_memory_apply_before_sqlite_apply_resumes_at_prefix",
    ),
    (
        "hybrid_async_chaos_crash_during_sqlite_txn_leaves_no_partial_apply_or_orphan_lease",
        "async_projection_chaos_crash_during_sqlite_txn_leaves_no_partial_apply_or_orphan_lease",
    ),
    (
        "hybrid_async_chaos_crash_after_high_water_replays_committed_batch_idempotently",
        "async_projection_chaos_crash_after_high_water_replays_committed_batch_idempotently",
    ),
    (
        "hybrid_async_chaos_crash_before_response_delivery_replays_request_id",
        "async_projection_chaos_crash_before_response_delivery_replays_request_id",
    ),
    (
        "hybrid_async_chaos_disk_loss_resets_high_water_and_rebuilds_from_log",
        "async_projection_chaos_disk_loss_resets_high_water_and_rebuilds_from_log",
    ),
    (
        "hybrid_async_chaos_disk_full_apply_failure_poisons_and_never_advances_past_poison",
        "async_projection_chaos_disk_full_apply_failure_poisons_and_never_advances_past_poison",
    ),
    (
        "hybrid_async_chaos_projection_poison_withholds_advanced_high_water",
        "async_projection_chaos_projection_poison_withholds_advanced_high_water",
    ),
    (
        "hybrid_async_chaos_apply_backlog_gates_mutations_and_withholds_high_water_until_drained",
        "async_projection_chaos_apply_backlog_gates_mutations_and_withholds_high_water_until_drained",
    ),
    (
        "hybrid_async_chaos_rolled_back_finalize_keeps_recoverable_inflight_lease",
        "async_projection_chaos_rolled_back_finalize_keeps_recoverable_inflight_lease",
    ),
    (
        "hybrid_async_checkpoint_applies_ordered_batches_and_advances_logical_high_water",
        "async_projection_checkpoint_applies_ordered_batches_and_advances_logical_high_water",
    ),
    (
        "hybrid_async_checkpoint_skips_already_applied_prefix_idempotently",
        "async_projection_checkpoint_skips_already_applied_prefix_idempotently",
    ),
    (
        "hybrid_async_checkpoint_persists_idempotency_rows_through_high_water",
        "async_projection_checkpoint_persists_idempotency_rows_through_high_water",
    ),
    (
        "hybrid_async_checkpoint_records_object_log_lineage",
        "async_projection_checkpoint_records_object_log_lineage",
    ),
    (
        "hybrid_async_checkpoint_distinguishes_logical_high_water_from_wal_checkpoint",
        "async_projection_checkpoint_distinguishes_logical_high_water_from_wal_checkpoint",
    ),
    (
        "hybrid_async_checkpoint_survives_reopen_and_rehydrates_memory",
        "async_projection_checkpoint_survives_reopen_and_rehydrates_memory",
    ),
    (
        "hybrid_async_checkpoint_wraps_an_existing_projection_store",
        "async_projection_checkpoint_wraps_an_existing_projection_store",
    ),
    (
        "hybrid_async_recovery_hydrates_validates_and_advertises_high_water",
        "async_projection_recovery_hydrates_validates_and_advertises_high_water",
    ),
    (
        "hybrid_async_recovery_fails_closed_on_newer_lineage_epoch",
        "async_projection_recovery_fails_closed_on_newer_lineage_epoch",
    ),
    (
        "hybrid_async_recovery_fails_closed_when_sqlite_ahead_of_log",
        "async_projection_recovery_fails_closed_when_sqlite_ahead_of_log",
    ),
    (
        "hybrid_async_recovery_no_lineage_still_checks_high_water",
        "async_projection_recovery_no_lineage_still_checks_high_water",
    ),
];

#[test]
fn async_projection_assertion_map_binds_every_migrated_assertion_exactly_once() {
    let old: BTreeSet<_> = ASSERTION_MAP.iter().map(|(old, _)| *old).collect();
    let new: BTreeSet<_> = ASSERTION_MAP.iter().map(|(_, new)| *new).collect();
    assert_eq!(
        old.len(),
        ASSERTION_MAP.len(),
        "duplicate legacy assertion id"
    );
    assert_eq!(
        new.len(),
        ASSERTION_MAP.len(),
        "duplicate successor assertion id"
    );
    assert!(old.iter().all(|id| id.starts_with("hybrid_async_")));
    assert!(new.iter().all(|id| id.starts_with("async_projection_")));

    let sources = [
        include_str!("async_projection_backpressure.rs"),
        include_str!("async_projection_chaos.rs"),
        include_str!("async_projection_checkpoint.rs"),
        include_str!("async_projection_recovery.rs"),
    ];
    for successor in &new {
        assert_eq!(
            sources
                .iter()
                .filter(|source| source.contains(successor))
                .count(),
            1,
            "successor assertion must occur in exactly one migrated suite: {successor}"
        );
    }
}

#[test]
fn async_projection_canonical_paths_contain_no_retired_hybrid_nomenclature() {
    for (path, source) in [
        (
            "fireweed-sqlite/src/async_projection.rs",
            include_str!("../src/async_projection.rs"),
        ),
        (
            "fireweed-objectlog/src/async_product_sqlite.rs",
            include_str!("../../fireweed-objectlog/src/async_product_sqlite.rs"),
        ),
    ] {
        assert!(
            !source.contains("Hybrid"),
            "retired Hybrid name escaped its guarded compatibility boundary: {path}"
        );
    }
}
