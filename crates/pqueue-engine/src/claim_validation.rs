//! Claim-compatibility validation (Phase 2 §4a; migrated from the HTTP service). Pure domain rules
//! over a claim's compatibility options + the queue definition; returns the resolved `ClaimUnit` or a
//! STRUCTURED `EngineError`. Not durable state.

use std::collections::BTreeMap;

use pqueue_core::{GroupKey, MetadataValue, QueueDefinition};

use crate::error::{EngineError, EngineResult};

/// The claim unit a `BatchClaim` resolves to (API-001 Batch Claim).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimUnit {
    /// Default item-level claim.
    Item,
    /// `same_group_key` filter — all returned items share one server-selected group.
    SameGroupKey,
    /// `group_batching` — whole eligible groups.
    WholeGroup,
    /// `whole_cohort` — one complete cohort, all-or-nothing.
    WholeCohort,
}

/// Whole-group claim options (API-001 `compatibility.group_batching`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupBatching {
    pub max_groups: u32,
}

/// A claim's compatibility options. `metadata_equals` is carried for completeness (a conjunctive
/// filter) but does not affect the claim-unit decision.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ClaimCompatibility {
    pub group_key: Option<GroupKey>,
    pub same_group_key: bool,
    pub metadata_equals: BTreeMap<String, MetadataValue>,
    pub group_batching: Option<GroupBatching>,
    pub whole_cohort: bool,
}

fn group_key_is_valid(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 256
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
}

/// Validate a claim's compatibility options against the queue, resolving the claim unit (API-001).
///
/// Errors are structured: invalid combinations / missing capabilities → `Invalid`; a unit that cannot
/// fit `max_items` → `BatchTooLarge`.
pub fn validate_claim_compatibility(
    compat: &ClaimCompatibility,
    max_items: u64,
    queue: &QueueDefinition,
) -> EngineResult<ClaimUnit> {
    if let Some(gk) = &compat.group_key
        && !group_key_is_valid(gk.as_str())
    {
        return Err(EngineError::Invalid(
            "group_key must match ^[A-Za-z0-9._:-]{1,256}$",
        ));
    }

    if let Some(gb) = &compat.group_batching {
        if compat.same_group_key || compat.group_key.is_some() || compat.whole_cohort {
            return Err(EngineError::Invalid(
                "group_batching cannot be combined with same_group_key, group_key, or whole_cohort",
            ));
        }
        if gb.max_groups == 0 {
            return Err(EngineError::Invalid(
                "group_batching.max_groups must be greater than zero",
            ));
        }
        let Some(max_group_size) = queue.max_eligible_group_size else {
            return Err(EngineError::Invalid(
                "group_batching requires group_co_residency and max_eligible_group_size",
            ));
        };
        if !queue.group_co_residency {
            return Err(EngineError::Invalid(
                "group_batching requires group_co_residency and max_eligible_group_size",
            ));
        }
        if max_group_size > max_items {
            return Err(EngineError::BatchTooLarge);
        }
        return Ok(ClaimUnit::WholeGroup);
    }

    if compat.whole_cohort {
        if compat.same_group_key || compat.group_key.is_some() {
            return Err(EngineError::Invalid(
                "whole_cohort cannot be combined with same_group_key, group_key, or group_batching",
            ));
        }
        let enabled = queue.cohort_policy.map(|c| c.enabled).unwrap_or(false);
        if !enabled {
            return Err(EngineError::Invalid(
                "whole_cohort requires cohort_policy.enabled=true",
            ));
        }
        if !queue.group_co_residency {
            return Err(EngineError::Invalid(
                "whole_cohort requires group_co_residency",
            ));
        }
        let Some(completion_bound_ms) = queue.cohort_policy.and_then(|c| c.completion_bound_ms)
        else {
            return Err(EngineError::Invalid(
                "whole_cohort requires cohort completion_bound_ms",
            ));
        };
        // (The service's extra "requires progress_bound_ms" branch is intentionally omitted:
        // QueueDefinition.progress_bound_ms is non-Option and domain-validated > 0, so it can't be missing.)
        if completion_bound_ms > queue.progress_bound_ms {
            return Err(EngineError::Invalid(
                "cohort completion_bound_ms must be <= progress_bound_ms",
            ));
        }
        return Ok(ClaimUnit::WholeCohort);
    }

    if compat.same_group_key {
        return Ok(ClaimUnit::SameGroupKey);
    }
    Ok(ClaimUnit::Item)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pqueue_core::{
        CohortPolicy, EligibilityPolicy, OrderingMode, PriorityModel, QueueId, RecurrencePolicy,
        RetryPolicy, TenantId,
    };

    /// Queue def with knobs for the group/cohort capabilities the validation reads.
    fn qdef(
        group_co_residency: bool,
        max_eligible_group_size: Option<u64>,
        cohort: Option<CohortPolicy>,
    ) -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("t").unwrap(),
            queue_id: QueueId::new("q").unwrap(),
            priority_model: PriorityModel::timestamp_ascending(),
            ordering_mode: OrderingMode::Strict,
            group_co_residency,
            progress_bound_ms: 60_000,
            eligibility_policy: EligibilityPolicy::default(),
            cohort_policy: cohort,
            recurrence: RecurrencePolicy::default(),
            request_id_retention_ms: 60_000,
            client_item_key_retention_ms: 60_000,
            max_lease_duration_ms: 60_000,
            retry_policy: RetryPolicy { max_attempts: 3 },
            max_push_batch_size: 100,
            max_claim_batch_size: 100,
            max_eligible_group_size,
            shard_count: 1,
        }
    }

    fn cohort(enabled: bool, completion_bound_ms: Option<u64>) -> CohortPolicy {
        CohortPolicy {
            enabled,
            completion_bound_ms,
            on_incomplete: None,
            max_cohort_size: Some(10),
        }
    }

    #[test]
    fn default_is_item_unit() {
        let c = ClaimCompatibility::default();
        assert_eq!(
            validate_claim_compatibility(&c, 10, &qdef(false, None, None)),
            Ok(ClaimUnit::Item)
        );
    }

    #[test]
    fn same_group_key_unit() {
        let c = ClaimCompatibility {
            same_group_key: true,
            ..Default::default()
        };
        assert_eq!(
            validate_claim_compatibility(&c, 10, &qdef(false, None, None)),
            Ok(ClaimUnit::SameGroupKey)
        );
    }

    #[test]
    fn group_batching_requires_co_residency_and_max_group_size() {
        let c = ClaimCompatibility {
            group_batching: Some(GroupBatching { max_groups: 2 }),
            ..Default::default()
        };
        // Valid: co-resident + max_eligible_group_size <= max_items.
        assert_eq!(
            validate_claim_compatibility(&c, 10, &qdef(true, Some(5), None)),
            Ok(ClaimUnit::WholeGroup)
        );
        // Missing max_eligible_group_size.
        assert!(matches!(
            validate_claim_compatibility(&c, 10, &qdef(true, None, None)),
            Err(EngineError::Invalid(_))
        ));
        // Not co-resident.
        assert!(matches!(
            validate_claim_compatibility(&c, 10, &qdef(false, Some(5), None)),
            Err(EngineError::Invalid(_))
        ));
        // max_group_size > max_items → BatchTooLarge.
        assert_eq!(
            validate_claim_compatibility(&c, 3, &qdef(true, Some(5), None)),
            Err(EngineError::BatchTooLarge)
        );
    }

    #[test]
    fn group_batching_rejects_zero_max_groups_and_combinations() {
        let zero = ClaimCompatibility {
            group_batching: Some(GroupBatching { max_groups: 0 }),
            ..Default::default()
        };
        assert!(matches!(
            validate_claim_compatibility(&zero, 10, &qdef(true, Some(5), None)),
            Err(EngineError::Invalid(_))
        ));
        let combined = ClaimCompatibility {
            group_batching: Some(GroupBatching { max_groups: 2 }),
            whole_cohort: true,
            ..Default::default()
        };
        assert!(matches!(
            validate_claim_compatibility(&combined, 10, &qdef(true, Some(5), None)),
            Err(EngineError::Invalid(_))
        ));
    }

    #[test]
    fn whole_cohort_requires_enabled_coresident_and_bound() {
        let c = ClaimCompatibility {
            whole_cohort: true,
            ..Default::default()
        };
        // Valid: cohort enabled, co-resident, completion_bound <= progress_bound.
        assert_eq!(
            validate_claim_compatibility(
                &c,
                10,
                &qdef(true, None, Some(cohort(true, Some(30_000))))
            ),
            Ok(ClaimUnit::WholeCohort)
        );
        // Cohort not enabled.
        assert!(matches!(
            validate_claim_compatibility(
                &c,
                10,
                &qdef(true, None, Some(cohort(false, Some(30_000))))
            ),
            Err(EngineError::Invalid(_))
        ));
        // completion_bound > progress_bound (60_000).
        assert!(matches!(
            validate_claim_compatibility(
                &c,
                10,
                &qdef(true, None, Some(cohort(true, Some(90_000))))
            ),
            Err(EngineError::Invalid(_))
        ));
    }

    #[test]
    fn whole_cohort_extra_rejections_and_valid_group_key() {
        let wc = ClaimCompatibility {
            whole_cohort: true,
            ..Default::default()
        };
        // whole_cohort but not co-resident.
        assert!(matches!(
            validate_claim_compatibility(
                &wc,
                10,
                &qdef(false, None, Some(cohort(true, Some(30_000))))
            ),
            Err(EngineError::Invalid(_))
        ));
        // whole_cohort but missing completion_bound_ms.
        assert!(matches!(
            validate_claim_compatibility(&wc, 10, &qdef(true, None, Some(cohort(true, None)))),
            Err(EngineError::Invalid(_))
        ));
        // whole_cohort combined with an explicit group_key.
        let wc_gk = ClaimCompatibility {
            whole_cohort: true,
            group_key: Some(GroupKey::new("ok.key-1").unwrap()),
            ..Default::default()
        };
        assert!(matches!(
            validate_claim_compatibility(
                &wc_gk,
                10,
                &qdef(true, None, Some(cohort(true, Some(30_000))))
            ),
            Err(EngineError::Invalid(_))
        ));
        // A VALID group_key (good charset) flows through to the Item unit.
        let gk = ClaimCompatibility {
            group_key: Some(GroupKey::new("ok.key-1").unwrap()),
            ..Default::default()
        };
        assert_eq!(
            validate_claim_compatibility(&gk, 10, &qdef(false, None, None)),
            Ok(ClaimUnit::Item)
        );
    }

    #[test]
    fn bad_group_key_charset_rejected() {
        let c = ClaimCompatibility {
            group_key: Some(GroupKey::new("bad key!").unwrap()),
            ..Default::default()
        };
        assert!(matches!(
            validate_claim_compatibility(&c, 10, &qdef(false, None, None)),
            Err(EngineError::Invalid(_))
        ));
    }
}
