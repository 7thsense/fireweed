//! Active-scope discovery rollup (`DiscoverActiveScopes`; Phase 2 §4a, sub B; migrated from the HTTP
//! service). Pure domain logic over a queue's active scopes: the Queue-vs-Group granularity decision,
//! the group→queue rollup arithmetic, and the request shape rules. STRUCTURED errors.
//!
//! The wire/transport concerns stay in the adapter: `tenant_id`/`as_of` stamping, filtering by
//! `queue_id`/`group_key`, the final sort, `max_results` truncation, and the `ProjectionRead`-backed
//! source of the scopes. This module is the math + rules only.

use std::collections::BTreeMap;

use crate::error::{EngineError, EngineResult};

/// One active scope (a queue, or a group within a queue) summarized for operator discovery. Counts are
/// `Option`: `None` means "no signal available" (NOT zero), so rollup must not fabricate a count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveScope {
    pub queue_id: String,
    /// Present only at `Group` granularity; `None` at `Queue` granularity (rolled up).
    pub group_key: Option<String>,
    pub oldest_eligible_age_ms: u64,
    pub eligible_count: Option<u64>,
    pub progress_bound_risk_count: Option<u64>,
}

/// Discovery granularity (`DiscoverActiveScopes.granularity`): per-queue summary or per-group detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryGranularity {
    Queue,
    Group,
}

impl DiscoveryGranularity {
    /// Default granularity when the request omits it: `Group` when a `queue_id` is *present* (drilling
    /// into one queue's groups), else `Queue` (a tenant-wide per-queue summary). Presence here is "is
    /// `Some`" — matching the service's `match body.queue_id` — NOT "is non-empty"; an empty `queue_id`
    /// defaults to `Group` and is then rejected by [`validate_discovery_request`] (parity quirk).
    pub fn default_for(queue_id: Option<&str>) -> Self {
        if queue_id.is_some() {
            DiscoveryGranularity::Group
        } else {
            DiscoveryGranularity::Queue
        }
    }
}

/// Resolve the effective granularity: the explicit request value, else [`DiscoveryGranularity::default_for`].
pub fn resolve_granularity(
    requested: Option<DiscoveryGranularity>,
    queue_id: Option<&str>,
) -> DiscoveryGranularity {
    requested.unwrap_or_else(|| DiscoveryGranularity::default_for(queue_id))
}

/// Validate the discovery request shape: `Group` granularity requires a *non-empty* `queue_id` (the
/// service's `is_none_or(str::is_empty)` test — note this is stricter than the `Some(_)` presence test
/// the default uses, so an empty `queue_id` that defaulted to `Group` is rejected here), and
/// `max_results` (after the adapter applies its default) must be greater than zero.
pub fn validate_discovery_request(
    granularity: DiscoveryGranularity,
    queue_id: Option<&str>,
    max_results: u64,
) -> EngineResult<()> {
    if granularity == DiscoveryGranularity::Group && queue_id.is_none_or(str::is_empty) {
        return Err(EngineError::Invalid("group discovery requires queue_id"));
    }
    if max_results == 0 {
        return Err(EngineError::Invalid(
            "max_results must be greater than zero",
        ));
    }
    Ok(())
}

/// Project source scopes to the requested granularity:
/// - `Group`: keep each scope's `group_key` (per-group detail).
/// - `Queue`: drop `group_key` and roll up to one scope per `queue_id` (see [`roll_up_queue_scopes`]).
pub fn project_scopes(
    scopes: Vec<ActiveScope>,
    granularity: DiscoveryGranularity,
) -> Vec<ActiveScope> {
    match granularity {
        DiscoveryGranularity::Group => scopes,
        DiscoveryGranularity::Queue => roll_up_queue_scopes(scopes),
    }
}

/// Roll group-granularity scopes up to one summary per queue: `oldest_eligible_age_ms` is the MAX
/// across the queue's groups (the worst-aged group drives the queue), counts are summed with
/// [`sum_optional`], and `group_key` is cleared. Deterministic order (keyed `BTreeMap` by `queue_id`).
pub fn roll_up_queue_scopes(scopes: Vec<ActiveScope>) -> Vec<ActiveScope> {
    let mut by_queue: BTreeMap<String, ActiveScope> = BTreeMap::new();
    for scope in scopes {
        by_queue
            .entry(scope.queue_id.clone())
            .and_modify(|existing| {
                existing.oldest_eligible_age_ms = existing
                    .oldest_eligible_age_ms
                    .max(scope.oldest_eligible_age_ms);
                existing.eligible_count =
                    sum_optional(existing.eligible_count, scope.eligible_count);
                existing.progress_bound_risk_count = sum_optional(
                    existing.progress_bound_risk_count,
                    scope.progress_bound_risk_count,
                );
            })
            .or_insert(ActiveScope {
                queue_id: scope.queue_id,
                group_key: None,
                oldest_eligible_age_ms: scope.oldest_eligible_age_ms,
                eligible_count: scope.eligible_count,
                progress_bound_risk_count: scope.progress_bound_risk_count,
            });
    }
    by_queue.into_values().collect()
}

/// Sum two optional counts, treating `None` as "no signal" rather than zero: both present → sum (the
/// sum is `saturating_add` — a count overflowing `u64` is not a real condition, but saturating keeps it
/// total/structured rather than panicking); exactly one present → that value; both absent → absent.
fn sum_optional(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.saturating_add(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(
        queue: &str,
        group: Option<&str>,
        age: u64,
        eligible: Option<u64>,
        risk: Option<u64>,
    ) -> ActiveScope {
        ActiveScope {
            queue_id: queue.to_string(),
            group_key: group.map(str::to_string),
            oldest_eligible_age_ms: age,
            eligible_count: eligible,
            progress_bound_risk_count: risk,
        }
    }

    #[test]
    fn default_granularity_depends_on_queue_presence() {
        assert_eq!(
            DiscoveryGranularity::default_for(Some("q")),
            DiscoveryGranularity::Group
        );
        assert_eq!(
            DiscoveryGranularity::default_for(None),
            DiscoveryGranularity::Queue
        );
        // resolve prefers the explicit value over the default.
        assert_eq!(
            resolve_granularity(Some(DiscoveryGranularity::Queue), Some("q")),
            DiscoveryGranularity::Queue
        );
        assert_eq!(
            resolve_granularity(None, Some("q")),
            DiscoveryGranularity::Group
        );
    }

    #[test]
    fn group_discovery_requires_queue_and_nonzero_max() {
        assert!(matches!(
            validate_discovery_request(DiscoveryGranularity::Group, None, 100),
            Err(EngineError::Invalid(_))
        ));
        // Queue granularity without a queue_id is fine (tenant-wide).
        assert!(validate_discovery_request(DiscoveryGranularity::Queue, None, 100).is_ok());
        // max_results must be > 0 regardless of granularity.
        assert!(matches!(
            validate_discovery_request(DiscoveryGranularity::Group, Some("q"), 0),
            Err(EngineError::Invalid(_))
        ));
        assert!(validate_discovery_request(DiscoveryGranularity::Group, Some("q"), 1).is_ok());
    }

    #[test]
    fn empty_queue_id_defaults_to_group_then_is_rejected() {
        // Parity quirk: the default keys off `Some(_)` (empty string → Group)...
        assert_eq!(
            resolve_granularity(None, Some("")),
            DiscoveryGranularity::Group
        );
        // ...but validation keys off non-empty, so that defaulted Group is rejected. Net: Invalid.
        assert!(matches!(
            validate_discovery_request(DiscoveryGranularity::Group, Some(""), 100),
            Err(EngineError::Invalid(_))
        ));
    }

    #[test]
    fn group_projection_is_identity() {
        let scopes = vec![
            scope("q1", Some("g1"), 10, Some(2), None),
            scope("q1", Some("g2"), 30, Some(5), Some(1)),
        ];
        assert_eq!(
            project_scopes(scopes.clone(), DiscoveryGranularity::Group),
            scopes
        );
    }

    #[test]
    fn queue_rollup_takes_max_age_and_sums_counts() {
        let scopes = vec![
            scope("q1", Some("g1"), 10, Some(2), None),
            scope("q1", Some("g2"), 30, Some(5), Some(1)),
            scope("q2", Some("g3"), 7, None, Some(4)),
        ];
        let rolled = project_scopes(scopes, DiscoveryGranularity::Queue);
        // BTreeMap key order → q1 then q2.
        assert_eq!(
            rolled,
            vec![
                // q1: max(10,30)=30; eligible 2+5=7; risk None+Some(1)=Some(1); group_key cleared.
                scope("q1", None, 30, Some(7), Some(1)),
                // q2: single group; counts pass through; group_key cleared.
                scope("q2", None, 7, None, Some(4)),
            ]
        );
    }

    #[test]
    fn sum_optional_treats_none_as_no_signal_not_zero() {
        assert_eq!(sum_optional(None, None), None);
        assert_eq!(sum_optional(Some(3), None), Some(3));
        assert_eq!(sum_optional(None, Some(4)), Some(4));
        assert_eq!(sum_optional(Some(3), Some(4)), Some(7));
        // Saturating (no panic) at the u64 ceiling.
        assert_eq!(sum_optional(Some(u64::MAX), Some(1)), Some(u64::MAX));
    }

    #[test]
    fn empty_rollup_is_empty() {
        assert!(roll_up_queue_scopes(Vec::new()).is_empty());
    }
}
