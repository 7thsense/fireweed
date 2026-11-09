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
