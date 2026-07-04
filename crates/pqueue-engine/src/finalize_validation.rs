//! Finalize / rearm / purge request validation (Phase 2 §4a; migrated from the HTTP service). Pure
//! structured-error rules; the state-dependent purge-force gate takes the looked-up leased flag.
//!
//! The transitional HTTP service delegates to these functions while it still compiles. A future
//! durable PurgePort must read `item_is_leased` for `validate_purge_force` in the SAME transaction as
//! the purge mutation (mirror the FinalizePort pre-commit fencing discipline), so the corrected force
//! gate can't be defeated by a stale flag. Tracked in build-progress.md decisions.

use pqueue_core::{QueueDefinition, RecurrenceMode, UtcTimestamp};

use crate::error::{EngineError, EngineResult};

/// Which targeting fields a finalize request carries (API-001 Batch Finalize).
#[derive(Debug, Clone, Copy)]
pub struct FinalizeTargeting {
    pub has_item_id: bool,
    pub has_cohort_id: bool,
    pub has_lease_token: bool,
    pub has_cohort_lease_token: bool,
}

/// A finalize must target an item (or cohort) and present a lease (or cohort lease).
pub fn validate_finalize_targeting(t: FinalizeTargeting) -> EngineResult<()> {
    if !t.has_item_id && !t.has_cohort_id {
        return Err(EngineError::Invalid("item_id or cohort_id is required"));
    }
    if !t.has_lease_token && !t.has_cohort_lease_token {
        return Err(EngineError::Invalid(
            "lease_token or cohort_lease_token is required",
        ));
    }
    Ok(())
}

/// Rearm rules (API-001): requires a recurring queue + a `not_before`. A `not_before` past
/// `recurrence.until` makes the item **Terminal** (`-ERR pqueue terminal`) rather than re-arming.
pub fn validate_rearm(
    rearm_not_before: Option<UtcTimestamp>,
    queue: &QueueDefinition,
) -> EngineResult<()> {
    if queue.recurrence.mode != RecurrenceMode::Recurring {
        return Err(EngineError::Invalid(
            "rearm requires recurrence.mode=recurring",
        ));
    }
    let Some(nb) = rearm_not_before else {
        return Err(EngineError::Invalid("rearm.not_before is required"));
    };
    if let Some(until) = queue.recurrence.until
        && nb.seconds > until.seconds
    {
        return Err(EngineError::Terminal);
    }
    Ok(())
}

/// A purge must target an item by id or client_item_key.
pub fn validate_purge_targeting(has_item_id: bool, has_client_item_key: bool) -> EngineResult<()> {
    if !has_item_id && !has_client_item_key {
        return Err(EngineError::Invalid(
            "item_id or client_item_key is required",
        ));
    }
    Ok(())
}

/// Purge-force gate (API-001): a **leased** item requires `force`; non-leased items purge freely.
///
/// NOTE: this corrects the HTTP service's pre-storage validator, which conservatively required `force`
/// for *any* purge because it had no item state at the HTTP layer. The engine knows the leased flag,
/// so it applies the real rule.
pub fn validate_purge_force(item_is_leased: bool, force: bool) -> EngineResult<()> {
    if item_is_leased && !force {
        return Err(EngineError::Conflict);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pqueue_core::{
        EligibilityPolicy, OrderingMode, PriorityModel, QueueId, RecurrencePolicy, RetryPolicy,
        TenantId,
    };

    fn qdef(recurrence: RecurrencePolicy) -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("t").unwrap(),
            queue_id: QueueId::new("q").unwrap(),
            priority_model: PriorityModel::timestamp_ascending(),
            ordering_mode: OrderingMode::Strict,
            max_rank_error: 0,
            progress_bound_ms: 60_000,
            eligibility_policy: EligibilityPolicy::default(),
            cohort_policy: None,
            recurrence,
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
    fn ts(s: i64) -> UtcTimestamp {
        UtcTimestamp::new(s, 0).unwrap()
    }
    fn targeting(item: bool, cohort: bool, lease: bool, cohort_lease: bool) -> FinalizeTargeting {
        FinalizeTargeting {
            has_item_id: item,
            has_cohort_id: cohort,
            has_lease_token: lease,
            has_cohort_lease_token: cohort_lease,
        }
    }

    #[test]
    fn finalize_targeting_requires_target_and_lease() {
        // neither item nor cohort
        assert!(matches!(
            validate_finalize_targeting(targeting(false, false, true, false)),
            Err(EngineError::Invalid(_))
        ));
        // target present, no lease at all
        assert!(matches!(
            validate_finalize_targeting(targeting(true, false, false, false)),
            Err(EngineError::Invalid(_))
        ));
        // item + lease ok; cohort + cohort_lease ok
        assert!(validate_finalize_targeting(targeting(true, false, true, false)).is_ok());
        assert!(validate_finalize_targeting(targeting(false, true, false, true)).is_ok());
    }

    #[test]
    fn rearm_requires_recurring_and_not_before() {
        // oneshot queue → invalid
        assert!(matches!(
            validate_rearm(Some(ts(10)), &qdef(RecurrencePolicy::default())),
            Err(EngineError::Invalid(_))
        ));
        let recurring = RecurrencePolicy {
            mode: RecurrenceMode::Recurring,
            until: None,
        };
        // recurring but no not_before → invalid
        assert!(matches!(
            validate_rearm(None, &qdef(recurring)),
            Err(EngineError::Invalid(_))
        ));
        // recurring + not_before, no until → ok
        assert!(validate_rearm(Some(ts(10)), &qdef(recurring)).is_ok());
    }

    #[test]
    fn rearm_past_until_is_terminal() {
        let recurring_until = RecurrencePolicy {
            mode: RecurrenceMode::Recurring,
            until: Some(ts(100)),
        };
        // before until → ok
        assert!(validate_rearm(Some(ts(50)), &qdef(recurring_until)).is_ok());
        // at until → ok (not strictly past)
        assert!(validate_rearm(Some(ts(100)), &qdef(recurring_until)).is_ok());
        // past until → terminal
        assert_eq!(
            validate_rearm(Some(ts(101)), &qdef(recurring_until)),
            Err(EngineError::Terminal)
        );
    }

    #[test]
    fn purge_targeting_and_force_gate() {
        assert!(matches!(
            validate_purge_targeting(false, false),
            Err(EngineError::Invalid(_))
        ));
        assert!(validate_purge_targeting(true, false).is_ok());
        assert!(validate_purge_targeting(false, true).is_ok());

        // leased + !force → conflict; leased + force → ok; not-leased → ok regardless.
        assert_eq!(
            validate_purge_force(true, false),
            Err(EngineError::Conflict)
        );
        assert!(validate_purge_force(true, true).is_ok());
        assert!(validate_purge_force(false, false).is_ok());
    }

    #[test]
    fn finalize_target_and_lease_families_are_independent() {
        // target via item + lease via cohort-lease; and target via cohort + lease via item-lease.
        assert!(validate_finalize_targeting(targeting(true, false, false, true)).is_ok());
        assert!(validate_finalize_targeting(targeting(false, true, true, false)).is_ok());
    }

    #[test]
    fn rearm_missing_not_before_wins_over_until() {
        let recurring_until = RecurrencePolicy {
            mode: RecurrenceMode::Recurring,
            until: Some(ts(100)),
        };
        // The missing-not_before guard short-circuits before the past-until comparison.
        assert!(matches!(
            validate_rearm(None, &qdef(recurring_until)),
            Err(EngineError::Invalid(_))
        ));
    }
}
