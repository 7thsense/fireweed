//! Request time and deleted key retention decisions shared by every backend.
//! The policy itself is declared in `fireweed_core::queue_contract::QueueRetention`.
use fireweed_core::queue_contract::{DeletedKeyPolicy, QueueError, QueueResult, QueueRetention};

/// Largest amount by which a request's `issued_at` may lead the backend clock:
/// 5 minutes, fixed. It is also the margin a `Retired` window must add to the
/// request id window when a schema is validated.
pub const ISSUED_AT_SKEW_NANOS: u64 = 300_000_000_000;

/// Refuses a request issued beyond the skew ahead of `now`. Callers run this
/// before any lookup or side effect, so the same request id may be retried.
pub fn check_issued_at(issued_at: i64, now: i64) -> QueueResult<()> {
    if i128::from(issued_at) > i128::from(now) + i128::from(ISSUED_AT_SKEW_NANOS) {
        return Err(QueueError::Invalid(
            "issued_at is ahead of the queue clock".into(),
        ));
    }
    Ok(())
}

/// True when a key deleted at `deleted_at` is still retired at `now`.
pub fn is_retired(retention: &QueueRetention, deleted_at: i64, now: i64) -> bool {
    match retention.deleted_keys {
        DeletedKeyPolicy::Reusable => false,
        DeletedKeyPolicy::Retired { window_nanos } => {
            i128::from(now) - i128::from(deleted_at) < i128::from(window_nanos)
        }
    }
}

/// A request issued before the retention horizon has no lookup and no effect.
/// Callers run this after `check_issued_at` and before any receipt lookup.
pub fn check_horizon(issued_at: i64, horizon: i64) -> QueueResult<()> {
    if issued_at < horizon {
        return Err(QueueError::RequestExpired);
    }
    Ok(())
}

/// Where one queue's cleanup stands. Both are monotone: a pass never lowers
/// either, whatever its `at`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionState {
    /// Requests, receipts and claim records issued before this are removed or
    /// refused with no lookup. `i64::MIN` until the first pass.
    pub horizon: i64,
    /// Retired deleted keys deleted before this are removable. `i64::MIN` until
    /// the first pass, and always so for `Reusable`.
    pub deleted_before: i64,
}
impl RetentionState {
    pub const INITIAL: Self = Self {
        horizon: i64::MIN,
        deleted_before: i64::MIN,
    };
}

/// The state after a cleanup pass at store time `at`. `H = at - request_id_nanos`,
/// held back to `oldest_live_claim`: the earliest `issued_at` of a claim whose
/// lease, plus the skew, has not ended at `at`. Native replays this from the log
/// and Postgres runs it in the cleanup transaction, so both advance alike.
pub fn plan_retention(
    retention: &QueueRetention,
    current: RetentionState,
    at: i64,
    oldest_live_claim: Option<i64>,
) -> RetentionState {
    let back = |nanos: u64| {
        i64::try_from(i128::from(at) - i128::from(nanos).min(i128::from(u64::MAX)))
            .unwrap_or(if at < 0 { i64::MIN } else { i64::MAX })
    };
    let mut horizon = back(retention.request_id_nanos);
    if let Some(claim) = oldest_live_claim {
        horizon = horizon.min(claim);
    }
    let deleted_before = match retention.deleted_keys {
        DeletedKeyPolicy::Reusable => i64::MIN,
        DeletedKeyPolicy::Retired { window_nanos } => back(window_nanos),
    };
    RetentionState {
        horizon: current.horizon.max(horizon),
        deleted_before: current.deleted_before.max(deleted_before),
    }
}

/// A claim whose leases end after this is live at `at`: the lease end plus the
/// skew has not passed. Only a live claim holds the horizon back.
pub fn live_claim_cutoff(at: i64) -> i64 {
    i64::try_from(i128::from(at) - i128::from(ISSUED_AT_SKEW_NANOS)).unwrap_or(i64::MIN)
}

/// Most rows one cleanup pass removes. A pass that removes this many has more
/// to do, and the next one continues it.
pub const RETENTION_BUDGET_ROWS: u32 = 1000;
/// Store time between the cleanup passes a queue is offered: one minute.
pub const RETENTION_INTERVAL_NANOS: i64 = 60_000_000_000;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issued_at_skew_boundary() {
        let skew = i64::try_from(ISSUED_AT_SKEW_NANOS).unwrap();
        assert!(check_issued_at(1_000 + skew, 1_000).is_ok());
        assert!(matches!(
            check_issued_at(1_001 + skew, 1_000),
            Err(QueueError::Invalid(_))
        ));
        assert!(check_issued_at(i64::MAX, i64::MAX).is_ok());
        assert!(check_issued_at(i64::MIN, i64::MAX).is_ok());
    }
    #[test]
    fn horizon_is_monotone_and_held_by_live_claims() {
        let retention = QueueRetention {
            request_id_nanos: 100,
            deleted_keys: DeletedKeyPolicy::Retired { window_nanos: 500 },
        };
        let first = plan_retention(&retention, RetentionState::INITIAL, 1_000, None);
        assert_eq!((first.horizon, first.deleted_before), (900, 500));
        // An earlier `at` never lowers either.
        let back = plan_retention(&retention, first, 10, None);
        assert_eq!(back, first);
        // A live claim holds the horizon, but never below where it already is.
        let held = plan_retention(&retention, first, 2_000, Some(950));
        assert_eq!(held.horizon, 950);
        let older = plan_retention(&retention, held, 3_000, Some(10));
        assert_eq!(older.horizon, 950);
        let free = plan_retention(&retention, older, 3_000, None);
        assert_eq!(free.horizon, 2_900);
        assert!(check_horizon(2_900, free.horizon).is_ok());
        assert_eq!(
            check_horizon(2_899, free.horizon),
            Err(QueueError::RequestExpired)
        );
        let reusable = QueueRetention {
            request_id_nanos: 100,
            deleted_keys: DeletedKeyPolicy::Reusable,
        };
        assert_eq!(
            plan_retention(&reusable, RetentionState::INITIAL, 1_000, None).deleted_before,
            i64::MIN
        );
        assert_eq!(
            plan_retention(&reusable, RetentionState::INITIAL, i64::MIN, None).horizon,
            i64::MIN
        );
    }
    #[test]
    fn claim_liveness_includes_the_skew() {
        let skew = i64::try_from(ISSUED_AT_SKEW_NANOS).unwrap();
        let live = |live_until: i64, at: i64| live_until > live_claim_cutoff(at);
        assert!(live(100, 100 + skew - 1));
        assert!(!live(100, 100 + skew));
        assert_eq!(live_claim_cutoff(i64::MIN), i64::MIN);
    }
    #[test]
    fn retired_window_boundary() {
        let retired = QueueRetention {
            request_id_nanos: 1,
            deleted_keys: DeletedKeyPolicy::Retired { window_nanos: 100 },
        };
        assert!(is_retired(&retired, 0, 99));
        assert!(!is_retired(&retired, 0, 100));
        assert!(is_retired(&retired, 50, 0));
        let reusable = QueueRetention {
            request_id_nanos: 1,
            deleted_keys: DeletedKeyPolicy::Reusable,
        };
        assert!(!is_retired(&reusable, 0, 0));
    }
    #[test]
    fn skew_is_the_retired_margin_of_schema_validation() {
        let at = |window_nanos| QueueRetention {
            request_id_nanos: 10,
            deleted_keys: DeletedKeyPolicy::Retired { window_nanos },
        };
        let floor = 10 + ISSUED_AT_SKEW_NANOS;
        let schema = |retention| fireweed_core::queue_contract::QueueSchema {
            work: fireweed_core::queue_contract::CollectionDefinition {
                fields: Default::default(),
                indexes: Default::default(),
                unique_indexes: Default::default(),
                additional_field_prefixes: Default::default(),
            },
            state: Default::default(),
            progress_bound_nanos: 1,
            max_attempts: 1,
            retention,
        };
        assert!(schema(at(floor)).validate().is_ok());
        assert!(schema(at(floor - 1)).validate().is_err());
    }
}
