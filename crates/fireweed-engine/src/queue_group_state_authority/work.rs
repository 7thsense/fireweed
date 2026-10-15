//! Selected ordinary work knowledge composed with the enclosing group cut.
//! No queue-wide state or opaque payload is retained here.
use super::*;
use crate::queue_publication::{
    BodyCommandRef, CommandRef, EffectRef, FieldsCommandRef, FieldsFingerprint,
    borrowed_value_fingerprint, owned_value_fingerprint,
};
use sha2::Digest;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkMetadata {
    pub version: u64,
    pub priority: i64,
    pub available_at: i64,
    pub insertion_sequence: u64,
    pub lifecycle: Lifecycle,
    pub eligible_since: i64,
    pub attempts: u32,
    pub lease_expires_at: Option<i64>,
    pub lease_hash: Option<[u8; 32]>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValueDescriptor {
    pub field_type: FieldType,
    pub fingerprint: FieldsFingerprint,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BodyDescriptor {
    pub bytes: u64,
    pub sha256: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkDescriptor {
    pub highwater: u64,
    pub metadata: Option<WorkMetadata>,
    pub fields: BTreeMap<String, Option<ValueDescriptor>>,
    pub body: Option<BodyDescriptor>,
    body_needed: bool,
}
impl WorkDescriptor {
    pub fn verify_fields(&self, actual: &Fields, names: &BTreeSet<String>) -> QueueResult<()> {
        for name in names {
            let expected = self
                .fields
                .get(name)
                .ok_or_else(|| unknown("field outside selected work scope"))?;
            let observed = actual
                .get(name)
                .map(|value| {
                    Ok(ValueDescriptor {
                        field_type: value.field_type(),
                        fingerprint: owned_value_fingerprint(value)
                            .map_err(|_| unknown("invalid cached work field encoding"))?,
                    })
                })
                .transpose()?;
            if observed != *expected {
                return Err(unknown("selected cached work field differs from source"));
            }
        }
        Ok(())
    }
    pub fn verify_body(&self, actual: &[u8]) -> QueueResult<()> {
        let expected = self
            .body
            .ok_or_else(|| unknown("work body knowledge absent"))?;
        if actual.len() as u64 != expected.bytes
            || <[u8; 32]>::from(sha2::Sha256::digest(actual)) != expected.sha256
        {
            return Err(unknown("cached work body differs from source"));
        }
        Ok(())
    }
}
pub(super) struct WorkSet {
    capacity: u64,
    entries: BTreeMap<String, WorkDescriptor>,
    membership: Vec<BTreeSet<String>>,
    _charge: OwnedQueueBufferCharge,
}
impl WorkSet {
    pub(super) fn copy_admitted(&self, job: &Arc<QueueReservation>) -> QueueResult<Self> {
        if !std::ptr::eq(self._charge.reservation(), job.as_ref()) {
            return Err(unknown("work continuation owner mismatch"));
        }
        let charge = job.owned_buffer(self.capacity)?;
        Ok(Self {
            entries: self.entries.clone(),
            membership: self.membership.clone(),
            capacity: self.capacity,
            _charge: charge,
        })
    }

    pub(super) fn new(
        groups: &[CommitGroup],
        schema: &QueueSchema,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Self> {
        // Bound borrowed schema and patch names before building required-field
        // sets; the same schema closure may be owned by every declared key.
        let schema_bytes = schema
            .work
            .indexes
            .iter()
            .try_fold(1024u64, |n, (name, columns)| {
                columns.iter().try_fold(
                    n.checked_add(name.len() as u64)
                        .ok_or(QueueError::TooLarge)?,
                    |n, column| {
                        n.checked_add(column.len() as u64)
                            .and_then(|n| n.checked_add(256))
                            .ok_or(QueueError::TooLarge)
                    },
                )
            })?;
        let group_headers = (groups.len() as u64)
            .checked_mul((std::mem::size_of::<BTreeSet<String>>() as u64) * 4)
            .and_then(|n| n.checked_add(4096))
            .ok_or(QueueError::TooLarge)?;
        let capacity = groups
            .iter()
            .try_fold(group_headers, |total, group| {
                let names = group
                    .claimed_updates
                    .iter()
                    .map(|u| &u.patch)
                    .chain(group.work_updates.iter().filter_map(|u| match &u.change {
                        UnclaimedChange::Patch(p)
                        | UnclaimedChange::Administrative { patch: p, .. } => Some(p),
                        _ => None,
                    }))
                    .try_fold(schema_bytes, |n, p| {
                        p.fields.keys().try_fold(n, |n, name| {
                            n.checked_add(name.len() as u64)
                                .and_then(|n| n.checked_add(256))
                                .ok_or(QueueError::TooLarge)
                        })
                    })?;
                declared_keys(group).try_fold(total, |n, key| {
                    n.checked_add(names)
                        .and_then(|n| n.checked_add(key.len() as u64))
                        .and_then(|n| n.checked_add(2048))
                        .ok_or(QueueError::TooLarge)
                })
            })?
            .checked_mul(16)
            .ok_or(QueueError::TooLarge)?;
        let charge = job.owned_buffer(capacity)?;
        let mut entries: BTreeMap<String, WorkDescriptor> = BTreeMap::new();
        let mut membership = Vec::with_capacity(groups.len());
        for group in groups {
            if declared_keys(group).next().is_none() {
                membership.push(BTreeSet::new());
                continue;
            }
            let names = crate::queue_planner::required_work_fields(group, schema)?;
            let mut members = BTreeSet::new();
            for key in declared_keys(group) {
                members.insert(key.to_owned());
                entries
                    .entry(key.to_owned())
                    .or_insert_with(|| WorkDescriptor {
                        highwater: 0,
                        metadata: None,
                        fields: BTreeMap::new(),
                        body: None,
                        body_needed: false,
                    });
            }
            for update in &group.claimed_updates {
                if update.disposition != Disposition::Check {
                    add_names(entries.get_mut(&update.key).unwrap(), &names);
                }
            }
            for update in &group.work_updates {
                let entry = entries.get_mut(&update.key).unwrap();
                add_names(entry, &names);
                entry.body_needed |= matches!(&update.change, UnclaimedChange::Administrative {patch,..} if patch.body.is_some());
            }
            for insert in &group.work_inserts {
                add_names(entries.get_mut(&insert.key).unwrap(), &names);
            }
            membership.push(members);
        }
        Ok(Self {
            entries,
            membership,
            capacity,
            _charge: charge,
        })
    }
    pub(super) fn observe(&mut self, command: CommandRef<'_>) -> QueueResult<()> {
        if command.identity.collection.is_some() {
            return Ok(());
        }
        let Some(entry) = self.entries.get_mut(command.identity.key) else {
            return Ok(());
        };
        let expected_version = entry.metadata.map(|m| m.version);
        if command.prior_highwater != entry.highwater
            || command.expected_version != expected_version
            || command.output_highwater <= entry.highwater
        {
            return Err(unknown("source work predecessor disagreement"));
        }
        match command.effect {
            EffectRef::Delete => {
                entry.metadata = None;
                for value in entry.fields.values_mut() {
                    *value = None;
                }
                entry.body = None;
            }
            EffectRef::StatePut { .. } => return Err(unknown("state mutation in work collection")),
            EffectRef::WorkPut {
                metadata,
                fields,
                body,
            } => {
                if entry.metadata.is_none()
                    && (matches!(
                        fields,
                        FieldsCommandRef::Preserve | FieldsCommandRef::Edit(_)
                    ) || matches!(body, BodyCommandRef::Preserve))
                {
                    return Err(unknown("new work lacks complete source payload"));
                }
                if let FieldsCommandRef::Replace(_) = fields {
                    for value in entry.fields.values_mut() {
                        *value = None;
                    }
                }
                if let FieldsCommandRef::Replace(values) | FieldsCommandRef::Edit(values) = fields {
                    for pair in values.iter() {
                        let (name, value) =
                            pair.map_err(|_| unknown("invalid source work field"))?;
                        if let Some(slot) = entry.fields.get_mut(name) {
                            *slot = value
                                .map(|value| {
                                    Ok(ValueDescriptor {
                                        field_type: value.field_type(),
                                        fingerprint: borrowed_value_fingerprint(value).map_err(
                                            |_| unknown("invalid source work value encoding"),
                                        )?,
                                    })
                                })
                                .transpose()?;
                        }
                    }
                }
                if entry.body_needed
                    && let BodyCommandRef::Replace(value) = body
                {
                    entry.body = Some(BodyDescriptor {
                        bytes: value.len() as u64,
                        sha256: sha2::Sha256::digest(value).into(),
                    });
                }
                entry.metadata = Some(WorkMetadata {
                    version: command.output_highwater,
                    priority: metadata.priority,
                    available_at: metadata.available_at,
                    insertion_sequence: metadata.insertion_sequence,
                    lifecycle: metadata.lifecycle,
                    eligible_since: metadata.eligible_since,
                    attempts: metadata.attempts,
                    lease_expires_at: metadata.lease_expires_at,
                    lease_hash: metadata.lease_hash.copied(),
                });
            }
        }
        entry.highwater = command.output_highwater;
        Ok(())
    }
    pub(super) fn descriptor(&self, group: usize, key: &str) -> QueueResult<&WorkDescriptor> {
        if !self
            .membership
            .get(group)
            .is_some_and(|keys| keys.contains(key))
        {
            return Err(unknown("work key outside declared group"));
        }
        self.entries
            .get(key)
            .ok_or_else(|| unknown("declared work descriptor missing"))
    }
}
fn declared_keys(group: &CommitGroup) -> impl Iterator<Item = &str> {
    group
        .claimed_updates
        .iter()
        .map(|u| u.key.as_str())
        .chain(group.work_updates.iter().map(|u| u.key.as_str()))
        .chain(group.work_inserts.iter().map(|u| u.key.as_str()))
}
fn add_names(entry: &mut WorkDescriptor, names: &BTreeSet<String>) {
    for name in names {
        entry.fields.entry(name.clone()).or_insert(None);
    }
}

#[cfg(test)]
#[path = "work_tests.rs"]
mod tests;
