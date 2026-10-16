//! Addressed ordinary-group preimages from one authenticated source cut.
//! No projection transaction, source replay or alternate business planner lives here.
mod admission;
mod batch;
mod body;
pub use batch::{
    QueueSourceBatchPlan, QueueSourceBatchPreimage, QueueSourceBatchPublication,
    load_batch_preimage,
};
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
    _retained_charges: Vec<OwnedQueueBufferCharge>,
    _charge: OwnedQueueBufferCharge,
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
    /// Consume the speculative plan without publishing a receipt. The returned
    /// DTO has an independent owner; dropping the preimage cannot release it.
    pub fn into_preview(
        self,
        max_response_bytes: u64,
    ) -> QueueResult<
        crate::queue_contract::OwnedQueueValue<PreviewResult, Vec<OwnedQueueBufferCharge>>,
    > {
        let encoded = encoded_len(&self.delta.receipt)?;
        let headers = crate::queue_memory::receipt_headers(&self.delta.receipt)?;
        let capacity = encoded
            .checked_mul(16)
            .and_then(|n| n.checked_add(headers.checked_mul(2)?))
            .and_then(|n| n.checked_add(4096))
            .ok_or(QueueError::TooLarge)?;
        let charge = self.preimage.job.owned_buffer(capacity)?;
        let preview = PreviewResult::try_from(self.delta.receipt.outcome)?;
        if encoded_len(&preview)? > max_response_bytes {
            return Err(QueueError::TooLarge);
        }
        Ok(crate::queue_contract::OwnedQueueValue::new(
            preview,
            vec![charge],
        ))
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
        let now = timestamp_nanos(time).map_err(|_| unknown("clock outside queue time domain"))?;
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
pub async fn load_group_preimage<'a, R: QueueSourceRecordReader>(
    reader: &'a R,
    queue: &'a QueueAddress,
    group: &'a CommitGroup,
    definition: &'a AdmittedQueueDefinition,
    limits: &QueueLimits,
    job: &'a Arc<QueueReservation>,
) -> QueueResult<QueueSourceGroupPreimage<'a>> {
    let charge = job.owned_buffer(request_scratch(group, definition.schema())?)?;
    validate_ordinary_group(queue, group, definition, limits)?;
    let mut image = load_image(reader, queue, group, definition, job, charge).await?;
    image.load_unique(reader, group, definition, job).await?;
    Ok(QueueSourceGroupPreimage {
        cut: image.cut,
        queue,
        group,
        definition,
        job,
        snapshot: image.snapshot,
        _retained_charges: image.retained_charges,
        _charge: image.charge,
    })
}
fn validate_ordinary_group(
    queue: &QueueAddress,
    group: &CommitGroup,
    definition: &AdmittedQueueDefinition,
    limits: &QueueLimits,
) -> QueueResult<()> {
    group.preflight_for(queue, limits)?;
    if group.administration.is_some() {
        return Err(unknown(
            "fresh administrative source resolver is not available",
        ));
    }
    group.validate_for(queue, definition.schema(), limits)
}
struct SourceImage<'a> {
    cut: QueueSourceCut<'a>,
    snapshot: RecordSnapshot,
    retained_charges: Vec<OwnedQueueBufferCharge>,
    charge: OwnedQueueBufferCharge,
}
async fn load_image<'a, R: QueueSourceRecordReader>(
    reader: &'a R,
    queue: &QueueAddress,
    group: &CommitGroup,
    definition: &AdmittedQueueDefinition,
    job: &Arc<QueueReservation>,
    charge: OwnedQueueBufferCharge,
) -> QueueResult<SourceImage<'a>> {
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
    let genesis = super::genesis::load_source_genesis_at(reader, cut, job).await?;
    if genesis.definition_pin != definition.pin()
        || genesis.source.log_offset() != 0
        || genesis.source.byte_length() == 0
    {
        return Err(unknown("genesis definition differs"));
    }
    let next_sequence = fixed(reader, cut, QueueSourceKey::Sequence, job)
        .await?
        .ok_or_else(|| unknown("missing sequence"))?;
    let mut output = SourceImage {
        cut,
        snapshot: RecordSnapshot {
            next_sequence,
            ..Default::default()
        },
        retained_charges: Vec::new(),
        charge,
    };
    let names_by_key = queue_planner::required_work_fields_by_key(group, definition.schema())?;
    let no_fields = BTreeSet::new();
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
    let body_keys = group.work_updates.iter().filter_map(|update|
        matches!(&update.change, UnclaimedChange::Administrative { patch, .. } if patch.body.is_some())
            .then_some(update.key.as_str())).collect::<BTreeSet<_>>();
    let mut body_needs = Vec::new();
    for (key, needs_fields) in keys {
        let (fact, body) = body::work_fact(reader, cut, key, body_keys.contains(key), job).await?;
        if let Some(body) = body {
            body_needs.push(body);
        }
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
            let names = if needs_fields {
                names_by_key
                    .get(key)
                    .ok_or_else(|| unknown("missing addressed work field declaration"))?
            } else {
                &no_fields
            };
            let (fields, coverage, guard) = super::fields::load_group_fields(
                reader,
                cut,
                IdentityRef {
                    collection: None,
                    key,
                },
                fact.field_count,
                &definition.schema().work,
                Some(names),
                job,
            )
            .await?
            .into_parts();
            output.retained_charges.push(guard);
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
    let mut states = BTreeMap::new();
    for update in &group.state_updates {
        *states
            .entry((update.collection.as_str(), update.key.as_str()))
            .or_insert(false) |= update.change != StateChange::Check;
    }
    for ((collection, key), needs_fields) in states {
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
            let fields = if !needs_fields {
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
                output.retained_charges.push(guard);
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
    output
        .load_bodies(reader, &body_needs, definition, job)
        .await?;
    if !reader.cut()?.same_cut(&cut) {
        return Err(unknown("reader changed captured cut"));
    }
    Ok(output)
}

impl SourceImage<'_> {
    async fn load_unique<R: QueueSourceReader>(
        &mut self,
        reader: &R,
        group: &CommitGroup,
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<()> {
        let cut = self.cut;
        // Admit tuple generation before the existing dependency algorithm allocates.
        let bytes = crate::queue_memory::group_expansion_budget(
            group,
            &self.snapshot,
            definition.schema(),
        )?;
        self.charge.grow(bytes)?;
        let probes =
            queue_planner::prospective_unique_probes(group, &self.snapshot, definition.schema())?;
        for tuple in probes {
            if self.snapshot.unique_probes_loaded.contains(&tuple) {
                continue;
            }
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
                self.charge.grow(
                    (owner.key.len() as u64)
                        .checked_add(owner.collection.map_or(0, |v| v.len() as u64))
                        .and_then(|n| n.checked_mul(4))
                        .and_then(|n| n.checked_add(1024))
                        .ok_or(QueueError::TooLarge)?,
                )?;
                self.snapshot.unique_owners.insert(
                    tuple.clone(),
                    RecordIdentity {
                        collection: owner.collection.map(str::to_owned),
                        key: owner.key.into(),
                    },
                );
            }
            self.snapshot.unique_probes_loaded.insert(tuple);
        }
        if !reader.cut()?.same_cut(&cut) {
            return Err(unknown("unique probe reader changed cut"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
