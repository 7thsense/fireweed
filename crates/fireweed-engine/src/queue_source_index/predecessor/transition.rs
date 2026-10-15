//! Complete record facts derived from one sealed predecessor and checked command.
mod fields;
mod named;
pub use named::QueueSourceNamedEffect;
mod postings;

use super::*;
use crate::{
    queue_fields::CheckedFields,
    queue_group_state_authority::WorkMetadata,
    queue_publication::{BodyCommandRef, EffectRef, FieldsCommandRef, FieldsRef, ValueRef},
    queue_source_index::QueueSourcePayload,
};
use fireweed_core::queue_contract::CollectionDefinition;

/// Retains the loaded old fields for subsequent scalar/named-index reduction.
/// This is not a publishable edit set and carries no storage transaction handle.
pub struct QueueSourceRecordTransition<'c, 'a> {
    predecessor: &'c QueueSourcePredecessor<'c, 'a>,
    fields: QueueSourceFields,
    definition: &'a CollectionDefinition,
    fact: QueueSourceRecord,
    _charge: OwnedQueueBufferCharge,
}
impl QueueSourceRecordTransition<'_, '_> {
    pub fn fact(&self) -> QueueSourceRecord {
        self.fact
    }
    pub fn before_fields(&self) -> QueueResult<CheckedFields<'_>> {
        self.fields.checked()
    }
    pub fn resolved(&self) -> CommandRef<'_> {
        self.predecessor.resolved()
    }
}
impl<'c, 'a> QueueSourceRecordTransition<'c, 'a> {
    pub(super) fn context_owner(&self) -> &'c OwnedQueueBufferCharge {
        self.predecessor.command.owner
    }
    pub(super) fn publication_identity(&self) -> crate::queue_publication::IdentityRef<'a> {
        self.predecessor.command.resolved.identity
    }
}
fn validate_field(
    definition: &CollectionDefinition,
    name: &str,
    value: Option<ValueRef<'_>>,
    job: &Arc<QueueReservation>,
) -> QueueResult<()> {
    if !definition.admits_field(name) {
        return Err(unknown("resolved field is undeclared"));
    }
    if let Some(value) = value {
        let bytes = match value {
            ValueRef::Text(value) => value.len(),
            ValueRef::Bytes(value) => value.len(),
            _ => 0,
        };
        let _charge = job.owned_buffer(
            bytes
                .checked_add(std::mem::size_of::<fireweed_core::queue_contract::FieldValue>())
                .ok_or(QueueError::TooLarge)? as u64,
        )?;
        let owned = value.into_owned();
        definition
            .validate_value(name, &owned)
            .map_err(|_| unknown("resolved field violates authenticated schema"))?;
    }
    Ok(())
}
fn field_count(
    before: &CheckedFields<'_>,
    prior_count: u64,
    fields: FieldsRef<'_>,
    replace: bool,
    definition: &CollectionDefinition,
    job: &Arc<QueueReservation>,
) -> QueueResult<u64> {
    let mut delta = 0i128;
    for entry in fields.iter() {
        let (name, value) = entry.map_err(|_| unknown("invalid resolved field entry"))?;
        validate_field(definition, name, value, job)?;
        if !replace && before.get(name)?.is_some() {
            delta -= 1;
        }
        if value.is_some() {
            delta += 1;
        } else if replace {
            return Err(unknown("replacement field has no value"));
        }
    }
    let count = i128::from(if replace { 0 } else { prior_count }) + delta;
    if count < 0 {
        return Err(unknown("source field count underflow"));
    }
    u64::try_from(count).map_err(|_| QueueError::TooLarge)
}
impl<'a, R: QueueSourceReader> QueueSourcePublicationContext<'a, R> {
    pub async fn record_transition<'c>(
        &'c self,
        predecessor: &'c QueueSourcePredecessor<'c, 'a>,
    ) -> QueueResult<QueueSourceRecordTransition<'c, 'a>> {
        // fields() first checks both context identity and exact cut, including
        // Preserve. Its loaded membership is retained through index compilation.
        let charge = self
            .job
            .owned_buffer(std::mem::size_of::<QueueSourceRecordTransition<'_, '_>>() as u64)?;
        let fields = self.fields(predecessor).await?;
        let old = predecessor.fact;
        let command = predecessor.command.resolved;
        let definition = match command.identity.collection {
            None => &self.definition.schema().work,
            Some(name) => self
                .definition
                .schema()
                .state
                .get(name)
                .ok_or_else(|| unknown("undeclared state collection"))?,
        };
        let checked_fields = fields.checked()?;
        let old_count = old.map_or(0, |fact| fact.field_count);
        if fields.present_count() as u64 > old_count {
            return Err(unknown("selected fields exceed source membership count"));
        }
        let (count, live) = match command.effect {
            EffectRef::Delete => (0, None),
            EffectRef::StatePut {
                fields: replacement,
                value,
            } => (
                field_count(
                    &checked_fields,
                    old_count,
                    replacement,
                    true,
                    definition,
                    self.job,
                )?,
                Some(QueueSourceLive::State {
                    value: QueueSourcePayload::from_checked(
                        self.checked,
                        self.cut.through(),
                        value,
                    )?,
                }),
            ),
            EffectRef::WorkPut {
                metadata,
                fields: change,
                body,
            } => {
                let old_sequence = old.and_then(|fact| match fact.live {
                    Some(QueueSourceLive::Work { metadata, .. }) => {
                        Some(metadata.insertion_sequence)
                    }
                    _ => None,
                });
                if old_sequence != Some(metadata.insertion_sequence)
                    && metadata.insertion_sequence <= self.publication().summary().initial_sequence
                {
                    return Err(unknown(
                        "new work sequence does not advance source insertion order",
                    ));
                }

                let count = match change {
                    FieldsCommandRef::Preserve => old_count,
                    FieldsCommandRef::Replace(replacement) => field_count(
                        &checked_fields,
                        old_count,
                        replacement,
                        true,
                        definition,
                        self.job,
                    )?,
                    FieldsCommandRef::Edit(edits) => field_count(
                        &checked_fields,
                        old_count,
                        edits,
                        false,
                        definition,
                        self.job,
                    )?,
                };
                let body = match body {
                    BodyCommandRef::Replace(bytes) => {
                        QueueSourcePayload::from_checked(self.checked, self.cut.through(), bytes)?
                    }
                    BodyCommandRef::Preserve => match old.and_then(|fact| fact.live) {
                        Some(QueueSourceLive::Work { body, .. }) => body,
                        _ => return Err(unknown("preserved body has no live source predecessor")),
                    },
                };
                (
                    count,
                    Some(QueueSourceLive::Work {
                        metadata: WorkMetadata {
                            version: command.output_highwater,
                            priority: metadata.priority,
                            available_at: metadata.available_at,
                            insertion_sequence: metadata.insertion_sequence,
                            lifecycle: metadata.lifecycle,
                            eligible_since: metadata.eligible_since,
                            attempts: metadata.attempts,
                            lease_expires_at: metadata.lease_expires_at,
                            lease_hash: metadata.lease_hash.copied(),
                        },
                        body,
                    }),
                )
            }
        };
        let fact = QueueSourceRecord {
            highwater: command.output_highwater,
            field_count: count,
            live,
        };
        fact.validate(command.identity.collection)?;
        Ok(QueueSourceRecordTransition {
            predecessor,
            fields,
            definition,
            fact,
            _charge: charge,
        })
    }
}
