//! Storage-independent API-007 commit semantics. Storage loads only addressed
//! records, calls this planner under its atomic boundary, and durably stores the
//! returned delta plus receipt. No SQL or log implementation belongs here.
use fireweed_core::queue_contract::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RecordIdentity {
    pub collection: Option<String>,
    pub key: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredWork {
    pub record: WorkRecord,
    pub lease_hash: Option<[u8; 32]>,
}
#[derive(Debug, Clone, Default)]
pub struct RecordSnapshot {
    pub work: BTreeMap<String, StoredWork>,
    pub state: BTreeMap<(String, String), StateRecord>,
    /// Durable high-water versions include deleted keys, preventing ABA.
    pub versions: BTreeMap<RecordIdentity, u64>,
    pub next_sequence: u64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommitDelta {
    pub work: Vec<StoredWork>,
    pub deleted_work: Vec<String>,
    pub state: Vec<StateRecord>,
    pub deleted_state: Vec<(String, String)>,
    pub versions: Vec<RecordVersion>,
    pub next_sequence: u64,
    pub receipt: CommitReceipt,
}
/// A durable log can encode the same delta a SQL adapter stores atomically.
/// Receipt encoding/storage is a storage responsibility, never a caller cache.
pub fn lease_hash(token: &[u8]) -> [u8; 32] {
    Sha256::digest(token).into()
}
fn token_equal(a: &[u8; 32], b: &[u8; 32]) -> bool {
    a.iter()
        .zip(b)
        .fold(0u8, |difference, (a, b)| difference | (a ^ b))
        == 0
}
fn next_version(
    snapshot: &mut RecordSnapshot,
    collection: Option<String>,
    key: &str,
) -> Result<u64, Rejection> {
    let id = RecordIdentity {
        collection,
        key: key.into(),
    };
    let n = snapshot
        .versions
        .get(&id)
        .copied()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| Rejection::Invalid("record version exhausted".into()))?;
    snapshot.versions.insert(id, n);
    Ok(n)
}
fn expect_state(present: Option<u64>, expected: &StateExpect) -> Result<(), Rejection> {
    match (present, expected) {
        (None, StateExpect::Absent) => Ok(()),
        (Some(_), StateExpect::Absent) => Err(Rejection::AlreadyExists),
        (None, StateExpect::Version(_)) => Err(Rejection::NotFound),
        (Some(actual), StateExpect::Version(expected)) if actual == *expected => Ok(()),
        _ => Err(Rejection::VersionConflict),
    }
}
fn patch_work(record: &mut WorkRecord, patch: &WorkPatch) {
    if let Some(priority) = patch.priority {
        record.work.priority = priority;
    }
    if let Some(time) = patch.available_at {
        record.work.available_at = time;
    }
    if let Some(body) = &patch.body {
        record.work.body.clone_from(body);
    }
    for (key, value) in &patch.fields {
        match value {
            Some(value) => {
                record.work.fields.insert(key.clone(), value.clone());
            }
            None => {
                record.work.fields.remove(key);
            }
        }
    }
    // Existing eligible age is preserved; moving eligibility into the future
    // cannot cause an old eligible item to appear younger once it is due again.
}
fn checked_expiry(now: i64, duration: u64) -> Result<i64, Rejection> {
    now.checked_add(
        i64::try_from(duration)
            .map_err(|_| Rejection::Invalid("lease duration overflow".into()))?,
    )
    .ok_or_else(|| Rejection::Invalid("lease expiry overflow".into()))
}
fn apply_group(
    group: &CommitGroup,
    snapshot: &mut RecordSnapshot,
    now: i64,
    schema: &QueueSchema,
) -> Result<CommitDelta, Rejection> {
    let mut delta = CommitDelta {
        work: vec![],
        deleted_work: vec![],
        state: vec![],
        deleted_state: vec![],
        versions: vec![],
        next_sequence: snapshot.next_sequence,
        receipt: CommitReceipt {
            request_id: group.request_id.clone(),
            outcome: GroupOutcome::Committed { versions: vec![] },
        },
    };
    for update in &group.claimed_updates {
        let mut stored = snapshot
            .work
            .get(&update.key)
            .cloned()
            .ok_or(Rejection::NotFound)?;
        if stored.record.lifecycle != Lifecycle::Leased
            || stored
                .record
                .lease_expires_at
                .is_none_or(|expiry| expiry <= now)
            || stored
                .lease_hash
                .is_none_or(|hash| !token_equal(&hash, &lease_hash(&update.lease.token)))
        {
            return Err(Rejection::StaleLease);
        }
        if stored.record.version != update.expected_version {
            return Err(Rejection::VersionConflict);
        }
        patch_work(&mut stored.record, &update.patch);
        let version = next_version(snapshot, None, &update.key)?;
        stored.record.version = version;
        match update.disposition {
            Disposition::Complete => stored.record.lifecycle = Lifecycle::Completed,
            Disposition::Fail => stored.record.lifecycle = Lifecycle::Failed,
            Disposition::Release => stored.record.lifecycle = Lifecycle::Pending,
            Disposition::RetryAt(time) => {
                if stored.record.attempts >= schema.max_attempts {
                    stored.record.lifecycle = Lifecycle::Failed;
                } else {
                    stored.record.lifecycle = Lifecycle::Pending;
                    stored.record.work.available_at = time;
                    stored.record.eligible_since = time;
                }
            }
            Disposition::Retain {
                renew_micros: Some(duration),
            } => stored.record.lease_expires_at = Some(checked_expiry(now, duration)?),
            Disposition::Retain { renew_micros: None } => {}
            Disposition::Delete => {
                snapshot.work.remove(&update.key);
                delta.deleted_work.push(update.key.clone());
                delta.versions.push(RecordVersion {
                    collection: None,
                    key: update.key.clone(),
                    version,
                });
                continue;
            }
        }
        if stored.record.lifecycle != Lifecycle::Leased {
            stored.record.lease_expires_at = None;
            stored.lease_hash = None;
        }
        delta.versions.push(RecordVersion {
            collection: None,
            key: update.key.clone(),
            version,
        });
        snapshot.work.insert(update.key.clone(), stored.clone());
        delta.work.push(stored);
    }
    for update in &group.state_updates {
        let id = (update.collection.clone(), update.key.clone());
        expect_state(
            snapshot.state.get(&id).map(|record| record.version),
            &update.expect,
        )?;
        let version = next_version(snapshot, Some(update.collection.clone()), &update.key)?;
        match &update.change {
            StateChange::Put { fields, value } => {
                let record = StateRecord {
                    collection: update.collection.clone(),
                    key: update.key.clone(),
                    version,
                    fields: fields.clone(),
                    value: value.clone(),
                };
                snapshot.state.insert(id, record.clone());
                delta.state.push(record);
            }
            StateChange::Delete => {
                snapshot.state.remove(&id);
                delta.deleted_state.push(id);
            }
        }
        delta.versions.push(RecordVersion {
            collection: Some(update.collection.clone()),
            key: update.key.clone(),
            version,
        });
    }
    for update in &group.work_updates {
        let mut stored = snapshot
            .work
            .get(&update.key)
            .cloned()
            .ok_or(Rejection::NotFound)?;
        expect_state(Some(stored.record.version), &update.expect)?;
        if stored.record.lifecycle == Lifecycle::Leased
            && stored
                .record
                .lease_expires_at
                .is_some_and(|expiry| expiry > now)
        {
            return Err(Rejection::Leased);
        }
        let version = next_version(snapshot, None, &update.key)?;
        match &update.patch {
            Some(patch) => {
                if stored.record.lifecycle == Lifecycle::Leased {
                    stored.record.lifecycle = Lifecycle::Pending;
                    stored.record.lease_expires_at = None;
                    stored.lease_hash = None;
                }
                patch_work(&mut stored.record, patch);
                stored.record.version = version;
                snapshot.work.insert(update.key.clone(), stored.clone());
                delta.work.push(stored);
            }
            None => {
                snapshot.work.remove(&update.key);
                delta.deleted_work.push(update.key.clone());
            }
        }
        delta.versions.push(RecordVersion {
            collection: None,
            key: update.key.clone(),
            version,
        });
    }
    for work in &group.work_inserts {
        if snapshot.work.contains_key(&work.key) {
            return Err(Rejection::AlreadyExists);
        }
        let version = next_version(snapshot, None, &work.key)?;
        let sequence = snapshot
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| Rejection::Invalid("sequence exhausted".into()))?;
        snapshot.next_sequence = sequence;
        let record = StoredWork {
            record: WorkRecord {
                work: work.clone(),
                version,
                insertion_sequence: sequence,
                lifecycle: Lifecycle::Pending,
                eligible_since: now.max(work.available_at),
                attempts: 0,
                lease_expires_at: None,
            },
            lease_hash: None,
        };
        snapshot.work.insert(work.key.clone(), record.clone());
        delta.work.push(record);
        delta.versions.push(RecordVersion {
            collection: None,
            key: work.key.clone(),
            version,
        });
    }
    delta.next_sequence = snapshot.next_sequence;
    delta.receipt.outcome = GroupOutcome::Committed {
        versions: delta.versions.clone(),
    };
    Ok(delta)
}
fn validate_snapshot(snapshot: &RecordSnapshot) -> QueueResult<()> {
    for (key, stored) in &snapshot.work {
        let id = RecordIdentity {
            collection: None,
            key: key.clone(),
        };
        if stored.record.work.key != *key
            || stored.record.version == 0
            || snapshot.versions.get(&id) != Some(&stored.record.version)
            || stored.record.insertion_sequence == 0
            || stored.record.insertion_sequence > snapshot.next_sequence
        {
            return Err(QueueError::OutcomeUnknown(
                "inconsistent loaded work version/sequence authority".into(),
            ));
        }
    }
    for ((collection, key), stored) in &snapshot.state {
        let id = RecordIdentity {
            collection: Some(collection.clone()),
            key: key.clone(),
        };
        if stored.collection != *collection
            || stored.key != *key
            || stored.version == 0
            || snapshot.versions.get(&id) != Some(&stored.version)
        {
            return Err(QueueError::OutcomeUnknown(
                "inconsistent loaded state version authority".into(),
            ));
        }
    }
    Ok(())
}
/// The caller first validates the envelope and resolves identity/replay. Loaded
/// snapshot/version high waters must be protected against concurrent writers.
/// Rejection leaves the snapshot unchanged but still returns a durable receipt.
pub fn plan_commit_group(
    group: &CommitGroup,
    snapshot: &mut RecordSnapshot,
    now: i64,
    schema: &QueueSchema,
) -> QueueResult<CommitDelta> {
    validate_snapshot(snapshot)?;
    let mut candidate = snapshot.clone();
    match apply_group(group, &mut candidate, now, schema) {
        Ok(delta) => {
            *snapshot = candidate;
            Ok(delta)
        }
        Err(reason) => Ok(CommitDelta {
            work: vec![],
            deleted_work: vec![],
            state: vec![],
            deleted_state: vec![],
            versions: vec![],
            next_sequence: snapshot.next_sequence,
            receipt: CommitReceipt {
                request_id: group.request_id.clone(),
                outcome: GroupOutcome::Rejected(reason),
            },
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn schema() -> QueueSchema {
        QueueSchema {
            work: CollectionDefinition {
                fields: BTreeMap::new(),
                indexes: BTreeMap::new(),
            },
            state: BTreeMap::new(),
            progress_bound_micros: 1_000_000,
            max_attempts: 3,
        }
    }
    fn group() -> CommitGroup {
        CommitGroup {
            request_id: "transition".into(),
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![],
        }
    }
    fn work(key: &str) -> NewWork {
        NewWork {
            key: key.into(),
            priority: 0,
            available_at: 0,
            fields: BTreeMap::new(),
            body: vec![],
        }
    }
    fn state(key: &str, expect: StateExpect) -> StateUpdate {
        StateUpdate {
            collection: "instance".into(),
            key: key.into(),
            expect,
            change: StateChange::Put {
                fields: BTreeMap::new(),
                value: vec![1],
            },
        }
    }
    fn claimed_snapshot() -> RecordSnapshot {
        let mut snapshot = RecordSnapshot::default();
        snapshot.versions.insert(
            RecordIdentity {
                collection: None,
                key: "input".into(),
            },
            2,
        );
        snapshot.work.insert(
            "input".into(),
            StoredWork {
                record: WorkRecord {
                    work: work("input"),
                    version: 2,
                    insertion_sequence: 1,
                    lifecycle: Lifecycle::Leased,
                    eligible_since: 0,
                    attempts: 1,
                    lease_expires_at: Some(100),
                },
                lease_hash: Some(lease_hash(b"token")),
            },
        );
        snapshot.next_sequence = 1;
        snapshot
    }
    fn update() -> ClaimedUpdate {
        ClaimedUpdate {
            key: "input".into(),
            lease: LeaseCredential {
                token: b"token".to_vec(),
            },
            expected_version: 2,
            patch: WorkPatch::default(),
            disposition: Disposition::Complete,
        }
    }
    #[test]
    fn state_and_emitted_work_commit_with_claim() {
        let mut snapshot = claimed_snapshot();
        let mut g = group();
        g.claimed_updates.push(update());
        g.state_updates.push(state("i", StateExpect::Absent));
        g.work_inserts.push(work("effect"));
        let delta = plan_commit_group(&g, &mut snapshot, 10, &schema()).unwrap();
        assert!(matches!(
            delta.receipt.outcome,
            GroupOutcome::Committed { .. }
        ));
        assert_eq!(
            snapshot.work["input"].record.lifecycle,
            Lifecycle::Completed
        );
        assert_eq!(snapshot.state[&("instance".into(), "i".into())].version, 1);
        assert!(snapshot.work.contains_key("effect"));
    }
    #[test]
    fn late_precondition_failure_rolls_back_every_candidate_change() {
        let mut snapshot = claimed_snapshot();
        let mut g = group();
        g.claimed_updates.push(update());
        g.state_updates
            .push(state("missing", StateExpect::Version(1)));
        g.work_inserts.push(work("effect"));
        let before = snapshot.clone();
        let delta = plan_commit_group(&g, &mut snapshot, 10, &schema()).unwrap();
        assert_eq!(
            delta.receipt.outcome,
            GroupOutcome::Rejected(Rejection::NotFound)
        );
        assert_eq!(snapshot.work, before.work);
        assert_eq!(snapshot.versions, before.versions);
        assert!(delta.work.is_empty());
        assert!(snapshot.state.is_empty());
    }
    #[test]
    fn expiry_rejects_without_waiting_for_reclaim() {
        let mut snapshot = claimed_snapshot();
        let mut g = group();
        g.claimed_updates.push(update());
        assert_eq!(
            plan_commit_group(&g, &mut snapshot, 100, &schema())
                .unwrap()
                .receipt
                .outcome,
            GroupOutcome::Rejected(Rejection::StaleLease)
        );
        assert_eq!(snapshot.work["input"].record.version, 2);
    }
    #[test]
    fn ordered_shared_state_conflicts_are_independent() {
        let mut snapshot = RecordSnapshot::default();
        let mut g = group();
        g.state_updates.push(state("i", StateExpect::Absent));
        assert!(matches!(
            plan_commit_group(&g, &mut snapshot, 0, &schema())
                .unwrap()
                .receipt
                .outcome,
            GroupOutcome::Committed { .. }
        ));
        assert_eq!(
            plan_commit_group(&g, &mut snapshot, 0, &schema())
                .unwrap()
                .receipt
                .outcome,
            GroupOutcome::Rejected(Rejection::AlreadyExists)
        );
        assert_eq!(snapshot.state[&("instance".into(), "i".into())].version, 1);
    }
    #[test]
    fn state_delete_reinsert_never_reuses_version() {
        let mut snapshot = RecordSnapshot::default();
        let mut g = group();
        g.state_updates.push(state("i", StateExpect::Absent));
        plan_commit_group(&g, &mut snapshot, 0, &schema()).unwrap();
        g.state_updates[0].expect = StateExpect::Version(1);
        g.state_updates[0].change = StateChange::Delete;
        plan_commit_group(&g, &mut snapshot, 0, &schema()).unwrap();
        g.state_updates[0] = state("i", StateExpect::Absent);
        plan_commit_group(&g, &mut snapshot, 0, &schema()).unwrap();
        assert_eq!(snapshot.state[&("instance".into(), "i".into())].version, 3);
        g.state_updates[0].expect = StateExpect::Version(1);
        assert_eq!(
            plan_commit_group(&g, &mut snapshot, 0, &schema())
                .unwrap()
                .receipt
                .outcome,
            GroupOutcome::Rejected(Rejection::VersionConflict)
        );
    }
    #[test]
    fn unclaimed_mutation_cannot_steal_a_live_lease() {
        let mut snapshot = claimed_snapshot();
        let mut g = group();
        g.work_updates.push(UnclaimedUpdate {
            key: "input".into(),
            expect: StateExpect::Version(2),
            patch: None,
        });
        assert_eq!(
            plan_commit_group(&g, &mut snapshot, 10, &schema())
                .unwrap()
                .receipt
                .outcome,
            GroupOutcome::Rejected(Rejection::Leased)
        );
    }
    #[test]
    fn retry_preserves_supplied_age_and_attempt_exhaustion() {
        for time in [5, 200] {
            let mut snapshot = claimed_snapshot();
            let mut g = group();
            let mut u = update();
            u.disposition = Disposition::RetryAt(time);
            g.claimed_updates.push(u);
            plan_commit_group(&g, &mut snapshot, 10, &schema()).unwrap();
            assert_eq!(snapshot.work["input"].record.eligible_since, time);
            assert_eq!(snapshot.work["input"].record.work.available_at, time);
            assert_eq!(snapshot.work["input"].record.lifecycle, Lifecycle::Pending);
        }
        let mut snapshot = claimed_snapshot();
        snapshot.work.get_mut("input").unwrap().record.attempts = 3;
        let mut g = group();
        let mut u = update();
        u.disposition = Disposition::RetryAt(200);
        g.claimed_updates.push(u);
        plan_commit_group(&g, &mut snapshot, 10, &schema()).unwrap();
        assert_eq!(snapshot.work["input"].record.lifecycle, Lifecycle::Failed);
        assert!(snapshot.work["input"].record.lease_expires_at.is_none());
    }
    #[test]
    fn inconsistent_version_or_sequence_authority_fails_closed() {
        let mut g = group();
        g.claimed_updates.push(update());
        for corruption in 0..3 {
            let mut snapshot = claimed_snapshot();
            match corruption {
                0 => snapshot.versions.clear(),
                1 => {
                    snapshot.versions.insert(
                        RecordIdentity {
                            collection: None,
                            key: "input".into(),
                        },
                        1,
                    );
                }
                _ => snapshot.next_sequence = 0,
            }
            assert!(matches!(
                plan_commit_group(&g, &mut snapshot, 10, &schema()),
                Err(QueueError::OutcomeUnknown(_))
            ));
            assert_eq!(snapshot.work["input"].record.version, 2);
            assert_eq!(snapshot.work["input"].record.lifecycle, Lifecycle::Leased);
        }
    }
}
