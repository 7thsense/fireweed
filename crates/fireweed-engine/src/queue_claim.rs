//! Shared claim transition after native filtered, ordered selection.
//! This module does not select records or broaden filters on the client.
use crate::{
    queue_fields::CheckedFields,
    queue_planner::{StoredWork, lease_hash, validate_raw_work_lease},
};
use fireweed_core::queue_contract::*;
#[derive(Clone, Copy)]
pub struct ClaimContext {
    pub now: i64,
    pub eligible_at: i64,
    pub expires_at: i64,
    pub insertion_head: u64,
}
pub struct ClaimTransition {
    pub work: StoredWork,
    pub lease: LeaseCredential,
}
impl ClaimTransition {
    pub fn into_claimed(self) -> ClaimedWork {
        ClaimedWork {
            record: self.work.record,
            lease: self.lease,
        }
    }
}
/// Exact postcard response-prefix accounting. Count framing is measured once per
/// candidate; already fitted records are never reserialized. Failed inclusion
/// preserves the prior prefix, independently of admission or payload validity.
pub struct ClaimResponseBudget {
    max_bytes: u64,
    element_bytes: u64,
    records: u64,
}
impl ClaimResponseBudget {
    pub fn new(max_bytes: u64) -> QueueResult<Self> {
        if encoded_len(&0u64)? > max_bytes {
            return Err(QueueError::TooLarge);
        }
        Ok(Self {
            max_bytes,
            element_bytes: 0,
            records: 0,
        })
    }
    pub fn try_include(&mut self, record: &ClaimedWork) -> QueueResult<bool> {
        let records = self.records.checked_add(1).ok_or(QueueError::TooLarge)?;
        let elements = self
            .element_bytes
            .checked_add(encoded_len(record)?)
            .ok_or(QueueError::TooLarge)?;
        let total = elements
            .checked_add(encoded_len(&records)?)
            .ok_or(QueueError::TooLarge)?;
        if total > self.max_bytes {
            return Ok(false);
        }
        self.records = records;
        self.element_bytes = elements;
        Ok(true)
    }
    pub fn encoded_bytes(&self) -> QueueResult<u64> {
        self.element_bytes
            .checked_add(encoded_len(&self.records)?)
            .ok_or(QueueError::TooLarge)
    }
}
/// Current lease authority for replay; creation versions intentionally do not
/// enter this check because renewal returns the current record and expiry.
#[derive(Clone, Copy)]
pub struct ClaimOwnership {
    pub lifecycle: Lifecycle,
    pub expires_at: Option<i64>,
    pub token_hash: Option<[u8; 32]>,
}
impl ClaimOwnership {
    pub fn validate_metadata(&self) -> QueueResult<()> {
        let leased = self.lifecycle == Lifecycle::Leased;
        if leased != self.expires_at.is_some() || leased != self.token_hash.is_some() {
            return Err(authority("inconsistent raw replay lease"));
        }
        Ok(())
    }
    pub fn validate(&self, token: &[u8], now: i64) -> QueueResult<()> {
        self.validate_metadata()?;
        let leased = self.lifecycle == Lifecycle::Leased;
        if !leased
            || self.expires_at.is_none_or(|expiry| expiry <= now)
            || self.token_hash != Some(lease_hash(token))
        {
            return Err(QueueError::ClaimExpired);
        }
        Ok(())
    }
}
pub fn checked_lease_hash(raw: Option<&[u8]>) -> QueueResult<Option<[u8; 32]>> {
    raw.map(|bytes| {
        bytes
            .try_into()
            .map_err(|_| authority("invalid stored lease hash length"))
    })
    .transpose()
}
fn authority(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("claim authority: {message}"))
}
pub fn claim_expiry(now: i64, duration: u64) -> QueueResult<i64> {
    let duration = i64::try_from(duration)
        .map_err(|_| QueueError::Invalid("lease duration overflow".into()))?;
    if duration <= 0 {
        return Err(QueueError::Invalid(
            "lease duration must be positive".into(),
        ));
    }
    now.checked_add(duration)
        .ok_or_else(|| QueueError::Invalid("lease expiry overflow".into()))
}
/// Consumes the already-admitted projection, preserving field/body knowledge and
/// all payload allocation ownership. Token generation and native persistence
/// belong to the adapter; metadata transitions are identical on every backend.
pub fn plan_claim(
    mut work: StoredWork,
    token: Vec<u8>,
    context: ClaimContext,
) -> QueueResult<ClaimTransition> {
    validate_raw_work_lease(&work)?;
    CheckedFields::new(&work.record.work.fields, &work.field_coverage)?;
    let record = &work.record;
    let next = plan_claim_metadata(
        crate::queue_group_state_authority::WorkMetadata {
            version: record.version,
            priority: record.work.priority,
            available_at: record.work.available_at,
            insertion_sequence: record.insertion_sequence,
            lifecycle: record.lifecycle,
            eligible_since: record.eligible_since,
            attempts: record.attempts,
            lease_expires_at: record.lease_expires_at,
            lease_hash: work.lease_hash,
        },
        &token,
        context,
    )?;
    work.record.version = next.version;
    work.record.attempts = next.attempts;
    work.record.lifecycle = next.lifecycle;
    work.record.lease_expires_at = next.lease_expires_at;
    work.lease_hash = next.lease_hash;
    Ok(ClaimTransition {
        work,
        lease: LeaseCredential { token },
    })
}
/// The same claim policy without materializing or copying body/field payloads.
/// Source compilation compares this result to the resolved command; relational
/// backends reach it through plan_claim above after their native selection.
pub(crate) fn plan_claim_metadata(
    mut record: crate::queue_group_state_authority::WorkMetadata,
    token: &[u8],
    context: ClaimContext,
) -> QueueResult<crate::queue_group_state_authority::WorkMetadata> {
    ClaimOwnership {
        lifecycle: record.lifecycle,
        expires_at: record.lease_expires_at,
        token_hash: record.lease_hash,
    }
    .validate_metadata()?;
    if record.version == 0
        || record.insertion_sequence == 0
        || record.insertion_sequence > context.insertion_head
    {
        return Err(authority(
            "invalid predecessor version or insertion sequence",
        ));
    }
    let pending = record.lifecycle == Lifecycle::Pending
        || (record.lifecycle == Lifecycle::Leased
            && record
                .lease_expires_at
                .is_some_and(|expiry| expiry <= context.now));
    if !pending || record.available_at > context.eligible_at {
        return Err(authority("native selection returned ineligible work"));
    }
    if token.is_empty() || context.expires_at <= context.now {
        return Err(QueueError::Invalid("invalid claim lease".into()));
    }
    if record.lease_hash == Some(lease_hash(token)) {
        return Err(authority("reused predecessor lease token"));
    }
    let version = record
        .version
        .checked_add(1)
        .ok_or_else(|| authority("version exhausted"))?;
    let attempts = record
        .attempts
        .checked_add(1)
        .ok_or_else(|| authority("attempts exhausted"))?;
    record.version = version;
    record.attempts = attempts;
    record.lifecycle = Lifecycle::Leased;
    record.lease_expires_at = Some(context.expires_at);
    record.lease_hash = Some(lease_hash(token));
    Ok(record)
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue_fields::FieldCoverage;
    fn work() -> StoredWork {
        StoredWork {
            record: WorkRecord {
                work: NewWork {
                    key: "a".into(),
                    priority: -4,
                    available_at: 7,
                    fields: Default::default(),
                    body: bytes::Bytes::from_static(b"body"),
                },
                version: 1,
                insertion_sequence: 1,
                lifecycle: Lifecycle::Pending,
                eligible_since: -9,
                attempts: 2,
                lease_expires_at: None,
            },
            lease_hash: None,
            field_coverage: FieldCoverage::Complete,
            body_loaded: true,
        }
    }
    fn context() -> ClaimContext {
        ClaimContext {
            now: 10,
            eligible_at: 7,
            expires_at: 20,
            insertion_head: 1,
        }
    }
    #[test]
    fn response_prefix_budget_matches_real_vector_bytes_across_count_and_scalar_boundaries() {
        let mut budget = ClaimResponseBudget::new(1 << 20).unwrap();
        let mut records: Vec<ClaimedWork> = Vec::new();
        assert_eq!(
            budget.encoded_bytes().unwrap(),
            postcard::to_allocvec(&records).unwrap().len() as u64
        );
        for ordinal in 0..=256 {
            let mut record = plan_claim(work(), vec![3; 32], context())
                .unwrap()
                .into_claimed();
            record.record.version = [1, 127, 128, u64::MAX][ordinal % 4];
            record.record.work.available_at = [i64::MIN, -1, 0, i64::MAX][ordinal % 4];
            record.lease.token = vec![7; [1, 127, 128, 256][ordinal % 4]];
            assert!(budget.try_include(&record).unwrap());
            records.push(record);
            if [1, 2, 127, 128, 129, 255, 256, 257].contains(&records.len()) {
                assert_eq!(
                    budget.encoded_bytes().unwrap(),
                    postcard::to_allocvec(&records).unwrap().len() as u64
                );
            }
        }
        let exact = postcard::to_allocvec(&records[..1]).unwrap().len() as u64;
        let mut one = ClaimResponseBudget::new(exact).unwrap();
        assert!(one.try_include(&records[0]).unwrap());
        assert!(!one.try_include(&records[1]).unwrap());
        assert_eq!(one.encoded_bytes().unwrap(), exact);
        assert!(ClaimResponseBudget::new(0).is_err());
    }
    #[test]
    fn pending_and_expired_claims_share_exact_transition_without_payload_copy() {
        for expired in [false, true] {
            let mut before = work();
            if expired {
                before.record.lifecycle = Lifecycle::Leased;
                before.record.lease_expires_at = Some(10);
                before.lease_hash = Some([8; 32]);
            }
            let body = before.record.work.body.as_ptr();
            let transition = plan_claim(before, vec![3; 32], context()).unwrap();
            assert_eq!(transition.work.record.work.body.as_ptr(), body);
            assert_eq!(
                (
                    transition.work.record.version,
                    transition.work.record.attempts,
                    transition.work.record.eligible_since
                ),
                (2, 3, -9)
            );
            assert_eq!(transition.work.record.lease_expires_at, Some(20));
            assert_eq!(transition.work.lease_hash, Some(lease_hash(&[3; 32])));
            assert_eq!(transition.work.field_coverage, FieldCoverage::Complete);
            assert!(transition.work.body_loaded);
        }
    }
    #[test]
    fn malformed_stale_and_exhausted_claim_authority_fails_closed() {
        for variant in 0..7 {
            let mut before = work();
            match variant {
                0 => before.record.version = 0,
                1 => before.record.insertion_sequence = 2,
                2 => before.record.lifecycle = Lifecycle::Completed,
                3 => before.record.work.available_at = 8,
                4 => before.record.version = u64::MAX,
                5 => before.record.attempts = u32::MAX,
                _ => before.record.lease_expires_at = Some(20),
            }
            assert!(matches!(
                plan_claim(before, vec![3; 32], context()),
                Err(QueueError::OutcomeUnknown(_))
            ));
        }
        let mut active = work();
        active.record.lifecycle = Lifecycle::Leased;
        active.record.lease_expires_at = Some(11);
        active.lease_hash = Some([8; 32]);
        assert!(matches!(
            plan_claim(active, vec![3; 32], context()),
            Err(QueueError::OutcomeUnknown(_))
        ));
        let mut repeated = work();
        repeated.record.lifecycle = Lifecycle::Leased;
        repeated.record.lease_expires_at = Some(10);
        repeated.lease_hash = Some(lease_hash(&[8; 32]));
        assert!(plan_claim(repeated, vec![8; 32], context()).is_err());
        assert!(matches!(
            claim_expiry(i64::MAX, 1),
            Err(QueueError::Invalid(_))
        ));
        assert!(claim_expiry(0, 0).is_err());
        assert_eq!(claim_expiry(-10, 10), Ok(0));
    }
    #[test]
    fn selected_fields_and_unloaded_body_remain_unknown_after_claim() {
        let mut before = work();
        before
            .record
            .work
            .fields
            .insert("present".into(), FieldValue::Integer(7));
        before.field_coverage = FieldCoverage::Selected {
            names: Default::default(),
            prefixes: Default::default(),
        };
        before.record.work.body = bytes::Bytes::new();
        before.body_loaded = false;
        let planned = plan_claim(before, vec![3; 32], context()).unwrap();
        assert!(!planned.work.body_loaded);
        let fields = CheckedFields::new(
            &planned.work.record.work.fields,
            &planned.work.field_coverage,
        )
        .unwrap();
        assert_eq!(
            fields.get("present").unwrap(),
            Some(&FieldValue::Integer(7))
        );
        assert!(fields.get("omitted").is_err());
    }
    #[test]
    fn live_replay_accepts_renewal_and_expires_the_entire_owned_selection() {
        let token = [3; 32];
        let mut live = ClaimOwnership {
            lifecycle: Lifecycle::Leased,
            expires_at: Some(20),
            token_hash: Some(lease_hash(&token)),
        };
        assert_eq!(live.validate(&token, 19), Ok(()));
        assert_eq!(live.validate(&token, 20), Err(QueueError::ClaimExpired));
        live.expires_at = Some(30);
        assert_eq!(live.validate(&token, 20), Ok(()));
        assert_eq!(live.validate(&[4; 32], 20), Err(QueueError::ClaimExpired));
        let settled = ClaimOwnership {
            lifecycle: Lifecycle::Completed,
            expires_at: None,
            token_hash: None,
        };
        assert!(
            [live, settled]
                .iter()
                .try_for_each(|entry| entry.validate(&token, 20))
                .is_err()
        );
        live.token_hash = None;
        assert!(matches!(
            live.validate(&token, 20),
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(checked_lease_hash(None), Ok(None));
        assert_eq!(checked_lease_hash(Some(&token)), Ok(Some(token)));
        assert!(checked_lease_hash(Some(&[3; 33])).is_err());
    }
}
