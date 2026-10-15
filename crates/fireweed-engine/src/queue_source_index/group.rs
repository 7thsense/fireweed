//! Addressed ordinary-group preimages from one authenticated source cut.
//! No projection transaction, source replay or alternate business planner lives here.
mod admission;
mod publication;
use super::*;
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_planner::{self, CommitDelta, RecordIdentity, RecordSnapshot, StoredWork},
    queue_publication::IdentityRef,
    queue_schema_codec::AdmittedQueueDefinition,
};
use admission::request_scratch;
use fireweed_core::queue_contract::*;
pub use publication::{QueueSourceGroupEncodingLimits, QueueSourceGroupPublication};
use serde::de::DeserializeOwned;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
fn unknown(reason: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("source group preimage: {reason}"))
}

/// Knowledge is sealed to these exact borrowed declarations. In particular a
/// state Check's metadata-only image must never be retargeted to Put/Delete.
pub struct QueueSourceGroupPreimage<'a> {
    cut: QueueSourceCut<'a>,
    queue: &'a QueueAddress,
    group: &'a CommitGroup,
    definition: &'a AdmittedQueueDefinition,
    snapshot: RecordSnapshot,
    job: &'a Arc<QueueReservation>,
    // All retained maps/strings and their guards are destroyed before this owner.
    field_charges: Vec<OwnedQueueBufferCharge>,
    charge: OwnedQueueBufferCharge,
}
/// A speculative result, with the authenticated before-image retained privately
/// for later bound publication encoding. No durable acknowledgement is implied.
pub struct QueueSourceGroupPlan<'a> {
    // Receipt IDs and cloned values drop before both funding owners.
    delta: CommitDelta,
    preimage: QueueSourceGroupPreimage<'a>,
    _charge: OwnedQueueBufferCharge,
}
impl QueueSourceGroupPlan<'_> {
    pub fn receipt(&self) -> &CommitReceipt {
        &self.delta.receipt
    }
    pub fn cut(&self) -> QueueSourceCut<'_> {
        self.preimage.cut
    }
}
impl<'a> QueueSourceGroupPreimage<'a> {
    /// All source reads and prospective unique probes have finished before the
    /// first clock sample. The existing PostgreSQL planner owns the semantics.
    pub fn plan(self, clock: &dyn crate::Clock) -> QueueResult<QueueSourceGroupPlan<'a>> {
        let bytes = crate::queue_memory::group_expansion_budget(
            self.group,
            &self.snapshot,
            self.definition.schema(),
        )?;
        let charge = self
            .job
            .owned_buffer(bytes.checked_add(4096).ok_or(QueueError::TooLarge)?)?;
        let mut image = self.snapshot.clone();
        let time = clock.now();
        if time.nanoseconds >= 1_000_000_000 {
            return Err(unknown("invalid clock nanoseconds"));
        }
        let now = time
            .seconds
            .checked_mul(1_000_000_000)
            .and_then(|n| n.checked_add(i64::from(time.nanoseconds)))
            .ok_or_else(|| unknown("clock outside queue time domain"))?;
        let delta = queue_planner::plan_commit_group(
            self.group,
            &mut image,
            now,
            self.definition.schema(),
        )?;
        Ok(QueueSourceGroupPlan {
            preimage: self,
            delta,
            _charge: charge,
        })
    }
}
async fn fixed<T: DeserializeOwned + Copy, R: QueueSourceReader>(
    reader: &R,
    cut: QueueSourceCut<'_>,
    key: QueueSourceKey<'_>,
    job: &Arc<QueueReservation>,
) -> QueueResult<Option<T>> {
    let _charge = job.owned_buffer(key.encoded_len()? as u64)?;
    let key = key.encode()?;
    let owner = reader.lookup(&key).await?;
    reader
        .point(&owner)?
        .value_for(&cut, &key)?
        .map(crate::queue_storage_layout::decode)
        .transpose()
}
/// Resolve original commit identity before calling this fresh-planning component;
/// previews deliberately skip original resolution. This component is inactive
/// until the facade also supplies the bound original and publication paths.
pub async fn load_group_preimage<'a, R: QueueSourceReader>(
    reader: &'a R,
    queue: &'a QueueAddress,
    group: &'a CommitGroup,
    definition: &'a AdmittedQueueDefinition,
    limits: &QueueLimits,
    job: &'a Arc<QueueReservation>,
) -> QueueResult<QueueSourceGroupPreimage<'a>> {
    let charge = job.owned_buffer(request_scratch(group, definition.schema())?)?;
    group.preflight_for(queue, limits)?;
    let cut = reader.cut()?;
    if !cut.belongs_to(job)
        || !definition.belongs_to(job)
        || cut.address().tenant != queue.tenant
        || cut.address().queue != queue.queue
        || cut.through() == 0
    {
        return Err(unknown(
            "queue, definition, operation or nonempty cut differs",
        ));
    }
    // Administrative selection and old-body materialization need their own
    // exact-cut source resolver. Never claim an empty placeholder is loaded.
    if group.administration.is_some() || group.work_updates.iter().any(|u|
        matches!(&u.change, UnclaimedChange::Administrative { patch, .. } if patch.body.is_some())) {
        return Err(unknown("administrative body source resolver is not available"));
    }
    group.validate_for(queue, definition.schema(), limits)?;
    let genesis: QueueSourceGenesis = fixed(reader, cut, QueueSourceKey::Genesis, job)
        .await?
        .ok_or_else(|| unknown("missing genesis"))?;
    if genesis.definition_pin != definition.pin()
        || genesis.source.log_offset() != 0
        || genesis.source.byte_length() == 0
    {
        return Err(unknown("genesis definition differs"));
    }
    let next_sequence = fixed(reader, cut, QueueSourceKey::Sequence, job)
        .await?
        .ok_or_else(|| unknown("missing sequence"))?;
    let mut output = QueueSourceGroupPreimage {
        cut,
        queue,
        group,
        definition,
        snapshot: RecordSnapshot {
            next_sequence,
            ..Default::default()
        },
        job,
        field_charges: Vec::new(),
        charge,
    };
    let names = queue_planner::required_work_fields(group, definition.schema())?;
    let mut keys = BTreeMap::<&str, bool>::new();
    for update in &group.claimed_updates {
        *keys.entry(&update.key).or_default() |= update.disposition != Disposition::Check;
    }
    for key in group
        .work_updates
        .iter()
        .map(|u| u.key.as_str())
        .chain(group.work_inserts.iter().map(|u| u.key.as_str()))
    {
        keys.insert(key, true);
    }
    let no_fields = BTreeSet::new();
    for (key, needs_fields) in keys {
        let fact: Option<QueueSourceRecord> = fixed(
            reader,
            cut,
            QueueSourceKey::Record {
                collection: None,
                key,
            },
            job,
        )
        .await?;
        let Some(fact) = fact else { continue };
        fact.validate(None)?;
        output.snapshot.versions.insert(
            RecordIdentity {
                collection: None,
                key: key.into(),
            },
            fact.highwater,
        );
        if let Some(QueueSourceLive::Work { metadata, body }) = fact.live {
            if metadata.insertion_sequence > next_sequence
                || body.span().log_offset() >= cut.through()
            {
                return Err(unknown(
                    "work sequence or body lies outside captured source",
                ));
            }
            let (fields, coverage, guard) = super::fields::load_group_fields(
                reader,
                cut,
                IdentityRef {
                    collection: None,
                    key,
                },
                fact.field_count,
                &definition.schema().work,
                Some(if needs_fields { &names } else { &no_fields }),
                job,
            )
            .await?
            .into_parts();
            output.field_charges.push(guard);
            output.snapshot.work.insert(
                key.into(),
                StoredWork {
                    record: WorkRecord {
                        work: NewWork {
                            key: key.into(),
                            priority: metadata.priority,
                            available_at: metadata.available_at,
                            fields,
                            body: Default::default(),
                        },
                        version: metadata.version,
                        insertion_sequence: metadata.insertion_sequence,
                        lifecycle: metadata.lifecycle,
                        eligible_since: metadata.eligible_since,
                        attempts: metadata.attempts,
                        lease_expires_at: metadata.lease_expires_at,
                    },
                    lease_hash: metadata.lease_hash,
                    field_coverage: coverage,
                    body_loaded: false,
                },
            );
        }
    }
    for update in &group.state_updates {
        let collection = update.collection.as_str();
        let key = update.key.as_str();
        let schema = definition
            .schema()
            .state
            .get(collection)
            .ok_or_else(|| QueueError::Invalid("undeclared state collection".into()))?;
        let fact: Option<QueueSourceRecord> = fixed(
            reader,
            cut,
            QueueSourceKey::Record {
                collection: Some(collection),
                key,
            },
            job,
        )
        .await?;
        let Some(fact) = fact else { continue };
        fact.validate(Some(collection))?;
        output.snapshot.versions.insert(
            RecordIdentity {
                collection: Some(collection.into()),
                key: key.into(),
            },
            fact.highwater,
        );
        if let Some(QueueSourceLive::State { value }) = fact.live {
            if value.span().log_offset() >= cut.through() {
                return Err(unknown("state value lies outside captured source"));
            }
            let fields = if update.change == StateChange::Check {
                Fields::new()
            } else {
                let (fields, _, guard) = super::fields::load_group_fields(
                    reader,
                    cut,
                    IdentityRef {
                        collection: Some(collection),
                        key,
                    },
                    fact.field_count,
                    schema,
                    None,
                    job,
                )
                .await?
                .into_parts();
                output.field_charges.push(guard);
                fields
            };
            output.snapshot.state.insert(
                (collection.into(), key.into()),
                StateRecord {
                    collection: collection.into(),
                    key: key.into(),
                    version: fact.highwater,
                    fields,
                    value: Default::default(),
                },
            );
        }
    }
    // Admit tuple generation before the existing dependency algorithm allocates.
    let bytes =
        crate::queue_memory::group_expansion_budget(group, &output.snapshot, definition.schema())?;
    output.charge.grow(bytes)?;
    let probes =
        queue_planner::prospective_unique_probes(group, &output.snapshot, definition.schema())?;
    for tuple in probes {
        let key = QueueSourceKey::UniqueOwner {
            collection: tuple.collection.as_deref(),
            index: &tuple.index,
            sort: &tuple.full_sort_key,
        };
        let _key_charge = job.owned_buffer(key.encoded_len()? as u64)?;
        let key = key.encode()?;
        let owner = reader.lookup(&key).await?;
        let point = reader.point(&owner)?;
        if let Some(value) = point.value_for(&cut, &key)? {
            let owner = super::cell::unique_owner(value, tuple.collection.as_deref())?;
            output.charge.grow(
                (owner.key.len() as u64)
                    .checked_add(owner.collection.map_or(0, |v| v.len() as u64))
                    .and_then(|n| n.checked_mul(4))
                    .and_then(|n| n.checked_add(1024))
                    .ok_or(QueueError::TooLarge)?,
            )?;
            output.snapshot.unique_owners.insert(
                tuple.clone(),
                RecordIdentity {
                    collection: owner.collection.map(str::to_owned),
                    key: owner.key.into(),
                },
            );
        }
        output.snapshot.unique_probes_loaded.insert(tuple);
    }
    if !reader.cut()?.same_cut(&cut) {
        return Err(unknown("reader changed captured cut"));
    }
    Ok(output)
}

#[cfg(test)]
mod tests;
