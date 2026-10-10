//! Storage-independent API-007 commit semantics. Storage loads only addressed
//! records, calls this planner under its atomic boundary, and durably stores the
//! returned delta plus receipt. No SQL or log implementation belongs here.
use fireweed_core::queue_contract::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RecordIdentity {
    pub collection: Option<String>,
    pub key: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredWork {
    pub record: WorkRecord,
    pub lease_hash: Option<[u8; 32]>,
    /// Internal materialization flags: false means preserve stored payload, not empty.
    /// An unloaded body is not a complete user-facing WorkRecord. Durable log
    /// envelopes must version this representation and apply Preserve against the
    /// existing record; explicit Some(empty) patches instead materialize an empty body.
    pub fields_loaded: bool,
    pub body_loaded: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct UniqueIdentity {
    pub collection: Option<String>,
    pub index: String,
    pub full_sort_key: Vec<u8>,
}
#[derive(Debug, Clone, Default)]
pub struct RecordSnapshot {
    pub work: BTreeMap<String, StoredWork>,
    pub state: BTreeMap<(String, String), StateRecord>,
    /// Durable high-water versions include deleted keys, preventing ABA.
    pub versions: BTreeMap<RecordIdentity, u64>,
    pub next_sequence: u64,
    /// Loaded existence/nonexistence authority for every prospective unique tuple.
    pub unique_probes_loaded: BTreeSet<UniqueIdentity>,
    pub unique_owners: BTreeMap<UniqueIdentity, RecordIdentity>,
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
fn patch_work(stored: &mut StoredWork, patch: &WorkPatch) {
    let record = &mut stored.record;
    if let Some(priority) = patch.priority {
        record.work.priority = priority;
    }
    if let Some(time) = patch.available_at {
        record.work.available_at = time;
    }
    if let Some(body) = &patch.body {
        record.work.body.clone_from(body);
        stored.body_loaded = true;
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
        patch_work(&mut stored, &update.patch);
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
                renew_nanos: Some(duration),
            } => stored.record.lease_expires_at = Some(checked_expiry(now, duration)?),
            Disposition::Retain { renew_nanos: None } => {}
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
        if !matches!(update.change, UnclaimedChange::Administrative { .. })
            && stored.record.lifecycle == Lifecycle::Leased
            && stored
                .record
                .lease_expires_at
                .is_some_and(|expiry| expiry > now)
        {
            return Err(Rejection::Leased);
        }
        let mut version = if matches!(update.change, UnclaimedChange::Administrative { .. }) {
            stored.record.version
        } else {
            next_version(snapshot, None, &update.key)?
        };
        match &update.change {
            UnclaimedChange::Patch(patch) => {
                if stored.record.lifecycle == Lifecycle::Leased {
                    stored.record.lifecycle = Lifecycle::Pending;
                    stored.record.lease_expires_at = None;
                    stored.lease_hash = None;
                }
                patch_work(&mut stored, patch);
                stored.record.version = version;
                snapshot.work.insert(update.key.clone(), stored.clone());
                delta.work.push(stored);
            }
            UnclaimedChange::Administrative {
                patch,
                lifecycle,
                lease_guard,
            } => {
                let before = stored.clone();
                let active = stored.record.lifecycle == Lifecycle::Leased
                    && stored
                        .record
                        .lease_expires_at
                        .is_some_and(|expiry| expiry > now);
                match lease_guard {
                    AdministrativeLeaseGuard::RejectActive if active => {
                        return Err(Rejection::Leased);
                    }
                    AdministrativeLeaseGuard::RequireActive if !active => {
                        return Err(Rejection::StaleLease);
                    }
                    _ => {}
                }
                if matches!(
                    stored.record.lifecycle,
                    Lifecycle::Completed | Lifecycle::Failed
                ) && matches!(
                    lifecycle,
                    AdministrativeLifecycle::Completed | AdministrativeLifecycle::Failed
                ) {
                    return Err(Rejection::Terminal);
                }
                let desired = match lifecycle {
                    AdministrativeLifecycle::Keep
                        if stored.record.lifecycle == Lifecycle::Leased
                            && *lease_guard == AdministrativeLeaseGuard::InvalidateActive =>
                    {
                        Lifecycle::Pending
                    }
                    AdministrativeLifecycle::Keep => stored.record.lifecycle,
                    AdministrativeLifecycle::Pending => Lifecycle::Pending,
                    AdministrativeLifecycle::Completed => Lifecycle::Completed,
                    AdministrativeLifecycle::Failed => Lifecycle::Failed,
                };
                if active
                    && desired != Lifecycle::Leased
                    && *lease_guard == AdministrativeLeaseGuard::RejectActive
                {
                    return Err(Rejection::Leased);
                }
                patch_work(&mut stored, patch);
                stored.record.lifecycle = desired;
                if desired != Lifecycle::Leased
                    || *lease_guard == AdministrativeLeaseGuard::InvalidateActive
                {
                    stored.record.lease_expires_at = None;
                    stored.lease_hash = None;
                }
                if desired == Lifecycle::Pending
                    && (before.record.lifecycle != Lifecycle::Pending
                        || before.record.work.available_at != stored.record.work.available_at)
                {
                    stored.record.eligible_since = now.max(stored.record.work.available_at);
                }
                if stored != before {
                    version = next_version(snapshot, None, &update.key)?;
                }
                stored.record.version = version;
                snapshot.work.insert(update.key.clone(), stored.clone());
                delta.work.push(stored);
            }
            UnclaimedChange::Replace(work) => {
                match stored.record.lifecycle {
                    Lifecycle::Completed | Lifecycle::Failed => return Err(Rejection::Terminal),
                    Lifecycle::Leased => return Err(Rejection::Leased),
                    Lifecycle::Pending => {}
                }
                if work.key != update.key {
                    return Err(Rejection::Invalid("replacement key mismatch".into()));
                }
                let sequence = snapshot
                    .next_sequence
                    .checked_add(1)
                    .ok_or_else(|| Rejection::Invalid("sequence exhausted".into()))?;
                snapshot.next_sequence = sequence;
                stored = StoredWork {
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
                    fields_loaded: true,
                    body_loaded: true,
                };
                snapshot.work.insert(update.key.clone(), stored.clone());
                delta.work.push(stored);
            }
            UnclaimedChange::Delete => {
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
            fields_loaded: true,
            body_loaded: true,
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
fn record_unique_keys(
    collection: Option<String>,
    fields: &Fields,
    definition: &CollectionDefinition,
) -> QueueResult<Vec<UniqueIdentity>> {
    let mut keys = vec![];
    for index in &definition.unique_indexes {
        let columns = definition
            .indexes
            .get(index)
            .ok_or_else(|| QueueError::OutcomeUnknown("invalid unique schema".into()))?;
        let values: Vec<_> = columns.iter().map(|key| fields.get(key)).collect();
        // Preserve PostgreSQL's existing NULLS DISTINCT constraint semantics.
        if values.iter().any(|value| value.is_none()) {
            continue;
        }
        keys.push(UniqueIdentity {
            collection: collection.clone(),
            index: index.clone(),
            full_sort_key: crate::queue_index::ordered_key(&values)?,
        });
    }
    Ok(keys)
}
/// Complete prospective tuples for an insert vector, independent of per-item
/// acceptance. Load authority once, then apply individual items in request order.
/// Sparse tuples remain NULLS DISTINCT; duplicate tuples are probed once.
pub fn insert_unique_probes(
    items: &[NewWork],
    schema: &QueueSchema,
) -> QueueResult<BTreeSet<UniqueIdentity>> {
    let mut keys = BTreeSet::new();
    for item in items {
        keys.extend(record_unique_keys(None, &item.fields, &schema.work)?);
    }
    Ok(keys)
}
/// Probe these complete tuples while retaining the authoritative storage lock.
/// A hash/prefix lookup is only candidate discovery; compare full_sort_key.
pub fn required_unique_probes(
    group: &CommitGroup,
    snapshot: &RecordSnapshot,
    now: i64,
    schema: &QueueSchema,
) -> QueueResult<BTreeSet<UniqueIdentity>> {
    let mut candidate = addressed_snapshot(group, snapshot);
    validate_group_snapshot(&candidate, group)?;
    let Ok(delta) = apply_group(group, &mut candidate, now, schema) else {
        return Ok(BTreeSet::new());
    };
    let mut keys = BTreeSet::new();
    for work in &delta.work {
        keys.extend(record_unique_keys(
            None,
            &work.record.work.fields,
            &schema.work,
        )?);
    }
    for state in &delta.state {
        let definition = schema
            .state
            .get(&state.collection)
            .ok_or_else(|| QueueError::OutcomeUnknown("missing state schema".into()))?;
        keys.extend(record_unique_keys(
            Some(state.collection.clone()),
            &state.fields,
            definition,
        )?);
    }
    Ok(keys)
}
fn validate_uniques(
    delta: &CommitDelta,
    original: &RecordSnapshot,
    schema: &QueueSchema,
) -> QueueResult<Result<Vec<(UniqueIdentity, RecordIdentity)>, Rejection>> {
    let affected: BTreeSet<_> = delta
        .versions
        .iter()
        .map(|version| RecordIdentity {
            collection: version.collection.clone(),
            key: version.key.clone(),
        })
        .collect();
    let mut additions = BTreeMap::new();
    for work in &delta.work {
        for key in record_unique_keys(None, &work.record.work.fields, &schema.work)? {
            let owner = RecordIdentity {
                collection: None,
                key: work.record.work.key.clone(),
            };
            if additions
                .insert(key, owner.clone())
                .is_some_and(|existing| existing != owner)
            {
                return Ok(Err(Rejection::UniqueConflict));
            }
        }
    }
    for state in &delta.state {
        let definition = schema
            .state
            .get(&state.collection)
            .ok_or_else(|| QueueError::OutcomeUnknown("missing state schema".into()))?;
        for key in record_unique_keys(Some(state.collection.clone()), &state.fields, definition)? {
            let owner = RecordIdentity {
                collection: Some(state.collection.clone()),
                key: state.key.clone(),
            };
            if additions
                .insert(key, owner.clone())
                .is_some_and(|existing| existing != owner)
            {
                return Ok(Err(Rejection::UniqueConflict));
            }
        }
    }
    for (key, owner) in &additions {
        if !original.unique_probes_loaded.contains(key) {
            return Err(QueueError::OutcomeUnknown(
                "unique tuple authority not loaded".into(),
            ));
        }
        if original
            .unique_owners
            .get(key)
            .is_some_and(|existing| existing != owner && !affected.contains(existing))
        {
            return Ok(Err(Rejection::UniqueConflict));
        }
    }
    Ok(Ok(additions.into_iter().collect()))
}
fn addressed_snapshot(group: &CommitGroup, source: &RecordSnapshot) -> RecordSnapshot {
    let mut out = RecordSnapshot {
        next_sequence: source.next_sequence,
        ..RecordSnapshot::default()
    };
    let work: BTreeSet<_> = group
        .claimed_updates
        .iter()
        .map(|u| &u.key)
        .chain(group.work_updates.iter().map(|u| &u.key))
        .chain(group.work_inserts.iter().map(|u| &u.key))
        .collect();
    for key in work {
        if let Some(record) = source.work.get(key) {
            out.work.insert(key.clone(), record.clone());
        }
        let id = RecordIdentity {
            collection: None,
            key: key.clone(),
        };
        if let Some(version) = source.versions.get(&id) {
            out.versions.insert(id, *version);
        }
    }
    for update in &group.state_updates {
        let key = (update.collection.clone(), update.key.clone());
        if let Some(record) = source.state.get(&key) {
            out.state.insert(key, record.clone());
        }
        let id = RecordIdentity {
            collection: Some(update.collection.clone()),
            key: update.key.clone(),
        };
        if let Some(version) = source.versions.get(&id) {
            out.versions.insert(id, *version);
        }
    }
    out
}
fn merge_delta(
    snapshot: &mut RecordSnapshot,
    delta: &CommitDelta,
    additions: Vec<(UniqueIdentity, RecordIdentity)>,
    schema: &QueueSchema,
) -> QueueResult<()> {
    // Determine old unique keys from addressed records, without scanning/copying
    // every other queue member or every owner in a large packed batch.
    let mut removals = vec![];
    for version in &delta.versions {
        let owner = RecordIdentity {
            collection: version.collection.clone(),
            key: version.key.clone(),
        };
        let old = match &version.collection {
            None => snapshot
                .work
                .get(&version.key)
                .map(|record| (&record.record.work.fields, &schema.work)),
            Some(collection) => snapshot
                .state
                .get(&(collection.clone(), version.key.clone()))
                .and_then(|record| {
                    schema
                        .state
                        .get(collection)
                        .map(|definition| (&record.fields, definition))
                }),
        };
        if let Some((fields, definition)) = old {
            for key in record_unique_keys(version.collection.clone(), fields, definition)? {
                if snapshot.unique_owners.get(&key) == Some(&owner) {
                    removals.push(key);
                }
            }
        }
    }
    // All fallible checks/encoding above precede mutation of the original image.
    for key in removals {
        snapshot.unique_owners.remove(&key);
    }
    for (key, owner) in additions {
        snapshot.unique_owners.insert(key, owner);
    }
    for key in &delta.deleted_work {
        snapshot.work.remove(key);
    }
    for key in &delta.deleted_state {
        snapshot.state.remove(key);
    }
    for record in &delta.work {
        snapshot
            .work
            .insert(record.record.work.key.clone(), record.clone());
    }
    for record in &delta.state {
        snapshot.state.insert(
            (record.collection.clone(), record.key.clone()),
            record.clone(),
        );
    }
    for version in &delta.versions {
        snapshot.versions.insert(
            RecordIdentity {
                collection: version.collection.clone(),
                key: version.key.clone(),
            },
            version.version,
        );
    }
    snapshot.next_sequence = delta.next_sequence;
    Ok(())
}
fn validate_snapshot(snapshot: &RecordSnapshot) -> QueueResult<()> {
    for (key, stored) in &snapshot.work {
        let id = RecordIdentity {
            collection: None,
            key: key.clone(),
        };
        if !stored.fields_loaded
            || stored.record.work.key != *key
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
fn validate_group_snapshot(candidate: &RecordSnapshot, group: &CommitGroup) -> QueueResult<()> {
    validate_snapshot(candidate)?;
    for update in &group.work_updates {
        if matches!(&update.change,UnclaimedChange::Administrative {patch,..} if patch.body.is_some())
            && candidate
                .work
                .get(&update.key)
                .is_some_and(|work| !work.body_loaded)
        {
            return Err(QueueError::OutcomeUnknown(
                "administrative body authority not loaded".into(),
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
    let mut candidate = addressed_snapshot(group, snapshot);
    validate_group_snapshot(&candidate, group)?;
    match apply_group(group, &mut candidate, now, schema) {
        Ok(delta) => {
            let additions = match validate_uniques(&delta, snapshot, schema)? {
                Ok(additions) => additions,
                Err(reason) => {
                    return Ok(CommitDelta {
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
                    });
                }
            };
            merge_delta(snapshot, &delta, additions, schema)?;
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
                unique_indexes: Default::default(),
            },
            state: BTreeMap::from([(
                "instance".into(),
                CollectionDefinition {
                    fields: BTreeMap::new(),
                    indexes: BTreeMap::new(),
                    unique_indexes: BTreeSet::new(),
                },
            )]),
            progress_bound_nanos: 1_000_000_000,
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
            body: Default::default(),
        }
    }
    fn state(key: &str, expect: StateExpect) -> StateUpdate {
        StateUpdate {
            collection: "instance".into(),
            key: key.into(),
            expect,
            change: StateChange::Put {
                fields: BTreeMap::new(),
                value: vec![1].into(),
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
                fields_loaded: true,
                body_loaded: true,
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
            change: UnclaimedChange::Delete,
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
    #[test]
    fn load_bearing_uniqueness_conflict_and_rekey_are_atomic() {
        let mut definition = schema();
        definition
            .work
            .fields
            .insert("target".into(), FieldType::Text);
        definition
            .work
            .indexes
            .insert("by_target".into(), vec!["target".into()]);
        definition.work.unique_indexes.insert("by_target".into());
        let mut snapshot = RecordSnapshot::default();
        let mut first = group();
        let mut a = work("a");
        a.fields
            .insert("target".into(), FieldValue::Text("x".repeat(1025)));
        first.work_inserts.push(a);
        snapshot
            .unique_probes_loaded
            .extend(required_unique_probes(&first, &snapshot, 0, &definition).unwrap());
        plan_commit_group(&first, &mut snapshot, 0, &definition).unwrap();
        let mut second = group();
        let mut b = work("b");
        b.fields = snapshot.work["a"].record.work.fields.clone();
        second.work_inserts.push(b);
        snapshot
            .unique_probes_loaded
            .extend(required_unique_probes(&second, &snapshot, 0, &definition).unwrap());
        assert_eq!(
            plan_commit_group(&second, &mut snapshot, 0, &definition)
                .unwrap()
                .receipt
                .outcome,
            GroupOutcome::Rejected(Rejection::UniqueConflict)
        );
        assert!(!snapshot.work.contains_key("b"));
        let mut rekey = group();
        rekey.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                fields: BTreeMap::from([("target".into(), Some(FieldValue::Text("new".into())))]),
                ..WorkPatch::default()
            }),
        });
        snapshot
            .unique_probes_loaded
            .extend(required_unique_probes(&rekey, &snapshot, 0, &definition).unwrap());
        plan_commit_group(&rekey, &mut snapshot, 0, &definition).unwrap();
        assert!(matches!(
            plan_commit_group(&second, &mut snapshot, 0, &definition)
                .unwrap()
                .receipt
                .outcome,
            GroupOutcome::Committed { .. }
        ));
    }
    fn unique_schema() -> QueueSchema {
        let mut s = schema();
        s.work.fields.insert("target".into(), FieldType::Text);
        s.work
            .indexes
            .insert("by_target".into(), vec!["target".into()]);
        s.work.unique_indexes.insert("by_target".into());
        s
    }
    fn unique_work(key: &str, target: &str) -> NewWork {
        let mut item = work(key);
        item.fields
            .insert("target".into(), FieldValue::Text(target.into()));
        item
    }
    fn probe_and_plan(
        group: &CommitGroup,
        snapshot: &mut RecordSnapshot,
        s: &QueueSchema,
    ) -> CommitDelta {
        snapshot
            .unique_probes_loaded
            .extend(required_unique_probes(group, snapshot, 0, s).unwrap());
        plan_commit_group(group, snapshot, 0, s).unwrap()
    }
    #[test]
    fn atomic_unique_swap_is_valid_and_duplicate_insert_rolls_back() {
        let s = unique_schema();
        let mut snapshot = RecordSnapshot::default();
        let mut g = group();
        g.work_inserts = vec![unique_work("a", "x"), unique_work("b", "y")];
        probe_and_plan(&g, &mut snapshot, &s);
        let mut swap = group();
        for (key, value) in [("a", "y"), ("b", "x")] {
            swap.work_updates.push(UnclaimedUpdate {
                key: key.into(),
                expect: StateExpect::Version(1),
                change: UnclaimedChange::Patch(WorkPatch {
                    fields: BTreeMap::from([(
                        "target".into(),
                        Some(FieldValue::Text(value.into())),
                    )]),
                    ..WorkPatch::default()
                }),
            });
        }
        assert!(matches!(
            probe_and_plan(&swap, &mut snapshot, &s).receipt.outcome,
            GroupOutcome::Committed { .. }
        ));
        assert_eq!(
            snapshot.work["a"].record.work.fields["target"],
            FieldValue::Text("y".into())
        );
        let mut fresh = RecordSnapshot::default();
        g.work_inserts = vec![unique_work("a", "x"), unique_work("b", "x")];
        assert_eq!(
            probe_and_plan(&g, &mut fresh, &s).receipt.outcome,
            GroupOutcome::Rejected(Rejection::UniqueConflict)
        );
        assert!(fresh.work.is_empty());
        assert!(fresh.state.is_empty());
        assert!(fresh.versions.is_empty());
        assert_eq!(fresh.next_sequence, 0);
    }
    #[test]
    fn missing_unique_authority_cannot_publish_a_candidate() {
        let s = unique_schema();
        let mut snapshot = RecordSnapshot::default();
        let mut g = group();
        g.work_inserts.push(unique_work("a", "x"));
        assert!(matches!(
            plan_commit_group(&g, &mut snapshot, 0, &s),
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert!(snapshot.work.is_empty());
        assert!(snapshot.versions.is_empty());
        assert_eq!(snapshot.next_sequence, 0);
    }
    #[test]
    fn state_uniqueness_and_missing_components_match_nulls_distinct() {
        let mut s = unique_schema();
        s.state.insert("instance".into(), s.work.clone());
        let mut snapshot = RecordSnapshot::default();
        let mut g = group();
        for key in ["a", "b"] {
            g.state_updates.push(StateUpdate {
                collection: "instance".into(),
                key: key.into(),
                expect: StateExpect::Absent,
                change: StateChange::Put {
                    fields: BTreeMap::from([("target".into(), FieldValue::Text("same".into()))]),
                    value: Default::default(),
                },
            });
        }
        assert_eq!(
            probe_and_plan(&g, &mut snapshot, &s).receipt.outcome,
            GroupOutcome::Rejected(Rejection::UniqueConflict)
        );
        assert!(snapshot.state.is_empty());
        for update in &mut g.state_updates {
            update.change = StateChange::Put {
                fields: BTreeMap::new(),
                value: Default::default(),
            };
        }
        assert!(matches!(
            probe_and_plan(&g, &mut snapshot, &s).receipt.outcome,
            GroupOutcome::Committed { .. }
        ));
        assert_eq!(snapshot.state.len(), 2);
    }
    #[test]
    fn lease_expiry_distinguishes_adjacent_nanoseconds() {
        let mut g = group();
        g.claimed_updates.push(update());
        for (now, committed) in [(299, true), (300, false)] {
            let mut snapshot = claimed_snapshot();
            snapshot
                .work
                .get_mut("input")
                .unwrap()
                .record
                .lease_expires_at = Some(300);
            let delta = plan_commit_group(&g, &mut snapshot, now, &schema()).unwrap();
            assert_eq!(
                matches!(delta.receipt.outcome, GroupOutcome::Committed { .. }),
                committed
            );
        }
    }
    #[test]
    fn metadata_patch_preserves_unloaded_body_and_explicit_empty_replaces_it() {
        for body in [None, Some(Default::default())] {
            let mut snapshot = claimed_snapshot();
            snapshot.work.get_mut("input").unwrap().body_loaded = false;
            let mut g = group();
            let mut u = update();
            u.patch.body = body.clone();
            g.claimed_updates.push(u);
            let delta = plan_commit_group(&g, &mut snapshot, 10, &schema()).unwrap();
            assert_eq!(delta.work[0].body_loaded, body.is_some());
            let encoded = postcard::to_allocvec(&delta).unwrap();
            let decoded: CommitDelta = postcard::from_bytes(&encoded).unwrap();
            assert_eq!(decoded, delta);
            assert_eq!(snapshot.work["input"].body_loaded, body.is_some());
        }
    }
    #[test]
    fn incomplete_fields_fail_before_any_effect() {
        let mut snapshot = claimed_snapshot();
        snapshot.work.get_mut("input").unwrap().fields_loaded = false;
        let original = snapshot.clone();
        let mut g = group();
        g.claimed_updates.push(update());
        g.work_inserts.push(work("next"));
        assert!(matches!(
            plan_commit_group(&g, &mut snapshot, 10, &schema()),
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(snapshot.work, original.work);
        assert_eq!(snapshot.versions, original.versions);
        assert_eq!(snapshot.next_sequence, original.next_sequence);
    }
    #[test]
    fn unrelated_owners_survive_and_delete_releases_tuple_for_next_group() {
        let s = unique_schema();
        let mut snapshot = RecordSnapshot::default();
        let mut initial = group();
        initial.work_inserts = vec![unique_work("a", "one"), unique_work("other", "two")];
        probe_and_plan(&initial, &mut snapshot, &s);
        let untouched = snapshot.work["other"].clone();
        let other_owner = snapshot
            .unique_owners
            .iter()
            .find(|(_, v)| v.key == "other")
            .map(|(k, v)| (k.clone(), v.clone()))
            .unwrap();
        let mut conflict = group();
        conflict.work_inserts.push(unique_work("b", "one"));
        assert_eq!(
            probe_and_plan(&conflict, &mut snapshot, &s).receipt.outcome,
            GroupOutcome::Rejected(Rejection::UniqueConflict)
        );
        assert_eq!(snapshot.work["other"], untouched);
        let mut delete = group();
        delete.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Delete,
        });
        probe_and_plan(&delete, &mut snapshot, &s);
        assert!(matches!(
            probe_and_plan(&conflict, &mut snapshot, &s).receipt.outcome,
            GroupOutcome::Committed { .. }
        ));
        assert_eq!(snapshot.work["other"], untouched);
        assert_eq!(
            snapshot.unique_owners.get(&other_owner.0),
            Some(&other_owner.1)
        );
        assert_eq!(
            snapshot.versions[&RecordIdentity {
                collection: None,
                key: "a".into()
            }],
            2
        );
    }
    #[test]
    fn fresh_replacement_resets_pending_work_but_preserves_version_highwater() {
        let mut snapshot = claimed_snapshot();
        let old = snapshot.work.get_mut("input").unwrap();
        old.record.lifecycle = Lifecycle::Pending;
        old.record.attempts = 3;
        old.lease_hash = None;
        old.record.lease_expires_at = None;
        let mut replacement = work("input");
        replacement.body = vec![9].into();
        replacement.available_at = 40;
        let mut g = group();
        g.work_updates.push(UnclaimedUpdate {
            key: "input".into(),
            expect: StateExpect::Version(2),
            change: UnclaimedChange::Replace(replacement.clone()),
        });
        let delta = plan_commit_group(&g, &mut snapshot, 10, &schema()).unwrap();
        let fresh = &delta.work[0];
        assert_eq!(fresh.record.work, replacement);
        assert_eq!(fresh.record.lifecycle, Lifecycle::Pending);
        assert_eq!(fresh.record.attempts, 0);
        assert_eq!(fresh.record.eligible_since, 40);
        assert_eq!(fresh.record.insertion_sequence, 2);
        assert_eq!(fresh.record.version, 3);
        assert!(fresh.body_loaded);
        assert!(fresh.fields_loaded);
        assert_eq!(fresh.lease_hash, None);
        assert_eq!(
            plan_commit_group(&g, &mut snapshot, 10, &schema())
                .unwrap()
                .receipt
                .outcome,
            GroupOutcome::Rejected(Rejection::VersionConflict)
        );
    }
    #[test]
    fn replace_rejects_live_lease_and_cancel_invalidates_it_atomically() {
        let mut snapshot = claimed_snapshot();
        let mut g = group();
        g.work_updates.push(UnclaimedUpdate {
            key: "input".into(),
            expect: StateExpect::Version(2),
            change: UnclaimedChange::Replace(work("input")),
        });
        assert_eq!(
            plan_commit_group(&g, &mut snapshot, 10, &schema())
                .unwrap()
                .receipt
                .outcome,
            GroupOutcome::Rejected(Rejection::Leased)
        );
        g.work_updates[0].change = UnclaimedChange::Administrative {
            patch: WorkPatch::default(),
            lifecycle: AdministrativeLifecycle::Failed,
            lease_guard: AdministrativeLeaseGuard::InvalidateActive,
        };
        g.state_updates.push(state("i", StateExpect::Version(1)));
        assert_eq!(
            plan_commit_group(&g, &mut snapshot, 10, &schema())
                .unwrap()
                .receipt
                .outcome,
            GroupOutcome::Rejected(Rejection::NotFound)
        );
        assert_eq!(snapshot.work["input"].record.lifecycle, Lifecycle::Leased);
        assert_eq!(
            snapshot.work["input"].lease_hash,
            Some(lease_hash(b"token"))
        );
        g.state_updates[0].expect = StateExpect::Absent;
        let delta = plan_commit_group(&g, &mut snapshot, 10, &schema()).unwrap();
        assert!(matches!(
            delta.receipt.outcome,
            GroupOutcome::Committed { .. }
        ));
        assert_eq!(snapshot.work["input"].record.lifecycle, Lifecycle::Failed);
        assert_eq!(snapshot.work["input"].record.version, 3);
        assert_eq!(snapshot.work["input"].lease_hash, None);
        let mut stale = group();
        stale.claimed_updates.push(update());
        assert_eq!(
            plan_commit_group(&stale, &mut snapshot, 10, &schema())
                .unwrap()
                .receipt
                .outcome,
            GroupOutcome::Rejected(Rejection::StaleLease)
        );
    }
    #[test]
    fn terminal_work_is_not_resurrected_by_upsert_or_cancel() {
        for lifecycle in [Lifecycle::Completed, Lifecycle::Failed] {
            for change in [
                UnclaimedChange::Replace(work("input")),
                UnclaimedChange::Administrative {
                    patch: WorkPatch::default(),
                    lifecycle: AdministrativeLifecycle::Failed,
                    lease_guard: AdministrativeLeaseGuard::InvalidateActive,
                },
            ] {
                let mut snapshot = claimed_snapshot();
                let old = snapshot.work.get_mut("input").unwrap();
                old.record.lifecycle = lifecycle;
                old.record.lease_expires_at = None;
                old.lease_hash = None;
                let before = snapshot.work.clone();
                let mut g = group();
                g.work_updates.push(UnclaimedUpdate {
                    key: "input".into(),
                    expect: StateExpect::Version(2),
                    change,
                });
                assert_eq!(
                    plan_commit_group(&g, &mut snapshot, 10, &schema())
                        .unwrap()
                        .receipt
                        .outcome,
                    GroupOutcome::Rejected(Rejection::Terminal)
                );
                assert_eq!(snapshot.work, before);
            }
        }
    }
    #[test]
    fn administrative_rearm_preserves_attempts_sequence_and_resets_eligible_age() {
        for lifecycle in [
            AdministrativeLifecycle::Pending,
            AdministrativeLifecycle::Keep,
        ] {
            let mut snapshot = claimed_snapshot();
            let mut g = group();
            g.work_updates.push(UnclaimedUpdate {
                key: "input".into(),
                expect: StateExpect::Version(2),
                change: UnclaimedChange::Administrative {
                    patch: WorkPatch::default(),
                    lifecycle,
                    lease_guard: AdministrativeLeaseGuard::InvalidateActive,
                },
            });
            plan_commit_group(&g, &mut snapshot, 10, &schema()).unwrap();
            let stored = &snapshot.work["input"];
            assert_eq!(stored.record.lifecycle, Lifecycle::Pending);
            assert_eq!(stored.record.attempts, 1);
            assert_eq!(stored.record.insertion_sequence, 1);
            assert_eq!(stored.record.eligible_since, 10);
            assert_eq!(stored.lease_hash, None);
            assert_eq!(stored.record.version, 3);
        }
    }
    #[test]
    fn administrative_completion_requires_live_lease_and_noop_keeps_version() {
        for now in [10, 100] {
            let mut snapshot = claimed_snapshot();
            let mut g = group();
            g.work_updates.push(UnclaimedUpdate {
                key: "input".into(),
                expect: StateExpect::Version(2),
                change: UnclaimedChange::Administrative {
                    patch: WorkPatch::default(),
                    lifecycle: AdministrativeLifecycle::Completed,
                    lease_guard: AdministrativeLeaseGuard::RequireActive,
                },
            });
            let outcome = plan_commit_group(&g, &mut snapshot, now, &schema())
                .unwrap()
                .receipt
                .outcome;
            if now == 10 {
                assert!(matches!(outcome, GroupOutcome::Committed { .. }));
                assert_eq!(
                    snapshot.work["input"].record.lifecycle,
                    Lifecycle::Completed
                );
            } else {
                assert_eq!(outcome, GroupOutcome::Rejected(Rejection::StaleLease));
            }
        }
        let mut snapshot = claimed_snapshot();
        let stored = snapshot.work.get_mut("input").unwrap();
        stored.record.lifecycle = Lifecycle::Pending;
        stored.record.lease_expires_at = None;
        stored.lease_hash = None;
        let before = stored.clone();
        let mut g = group();
        g.work_updates.push(UnclaimedUpdate {
            key: "input".into(),
            expect: StateExpect::Version(2),
            change: UnclaimedChange::Administrative {
                patch: WorkPatch::default(),
                lifecycle: AdministrativeLifecycle::Pending,
                lease_guard: AdministrativeLeaseGuard::InvalidateActive,
            },
        });
        let delta = plan_commit_group(&g, &mut snapshot, 10, &schema()).unwrap();
        assert_eq!(snapshot.work["input"], before);
        assert_eq!(delta.versions[0].version, 2);
    }
    #[test]
    fn administrative_identical_body_noop_requires_materialized_authority() {
        for body in [vec![], vec![1, 2, 3]] {
            for loaded in [true, false] {
                let mut snapshot = claimed_snapshot();
                let old = snapshot.work.get_mut("input").unwrap();
                old.record.lifecycle = Lifecycle::Pending;
                old.lease_hash = None;
                old.record.lease_expires_at = None;
                old.record.work.body = if loaded {
                    body.clone().into()
                } else {
                    Default::default()
                };
                old.body_loaded = loaded;
                let original = snapshot.work.clone();
                let mut g = group();
                g.work_updates.push(UnclaimedUpdate {
                    key: "input".into(),
                    expect: StateExpect::Version(2),
                    change: UnclaimedChange::Administrative {
                        patch: WorkPatch {
                            body: Some(body.clone().into()),
                            ..WorkPatch::default()
                        },
                        lifecycle: AdministrativeLifecycle::Keep,
                        lease_guard: AdministrativeLeaseGuard::RejectActive,
                    },
                });
                let probes = required_unique_probes(&g, &snapshot, 10, &schema());
                if loaded {
                    assert!(probes.is_ok());
                } else {
                    assert!(matches!(probes, Err(QueueError::OutcomeUnknown(_))));
                }
                let result = plan_commit_group(&g, &mut snapshot, 10, &schema());
                if loaded {
                    assert_eq!(result.unwrap().versions[0].version, 2);
                } else {
                    assert!(matches!(result, Err(QueueError::OutcomeUnknown(_))));
                }
                assert_eq!(snapshot.work, original);
            }
        }
    }
    #[test]
    fn fresh_replacement_rejects_stored_lease_even_after_expiry() {
        let mut snapshot = claimed_snapshot();
        let mut g = group();
        g.work_updates.push(UnclaimedUpdate {
            key: "input".into(),
            expect: StateExpect::Version(2),
            change: UnclaimedChange::Replace(work("input")),
        });
        assert_eq!(
            plan_commit_group(&g, &mut snapshot, 100, &schema())
                .unwrap()
                .receipt
                .outcome,
            GroupOutcome::Rejected(Rejection::Leased)
        );
    }
}
