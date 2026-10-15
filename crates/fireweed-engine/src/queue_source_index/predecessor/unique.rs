//! Publication-wide initial-owner authority and order-independent final effects.
use super::*;
use crate::{
    queue_log_record::OwnedQueueLogRecord,
    queue_planner::{RecordIdentity, UniqueIdentity},
    queue_publication::IdentityRef,
    queue_unique_stream::{
        UniqueClaimOrigin, UniqueEvidence, UniqueEvidenceEffect, UniqueTupleReducer,
    },
};
use fireweed_core::queue_contract::encoded_len;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy)]
struct Effect<'a> {
    owner: IdentityRef<'a>,
    claim: bool,
}
/// Every canonical command, including no-effect commands, must advance coverage.
/// This accumulator carries no usable compiled seal or publication handle.
pub struct QueueSourceUniqueOwnerAccumulator<'c, 'a, R: QueueSourceReader> {
    context: &'c QueueSourcePublicationContext<'a, R>,
    processed: u64,
    last: Option<IdentityRef<'a>>,
    poisoned: bool,
    groups: BTreeMap<UniqueIdentity, Vec<Effect<'a>>>,
    // Map keys/evidence drop before their transferred buffer guards.
    key_charges: Vec<OwnedQueueBufferCharge>,
    _charge: OwnedQueueBufferCharge,
}
/// Provisional edits bound to one proposed record, cut, and operation. Only the
/// complete all-variant compiler may incorporate these into its publication seal.
pub struct QueueSourceUniqueOwnerEdits<'a> {
    output: super::super::edits::SourceEdits<'a>,
}
impl QueueSourceUniqueOwnerEdits<'_> {
    pub fn edits(&self) -> impl ExactSizeIterator<Item = (&[u8], Option<&[u8]>)> {
        self.output.edits()
    }
    pub fn verify(
        &self,
        record: &OwnedQueueLogRecord,
        cut: &QueueSourceCut<'_>,
        job: &QueueReservation,
    ) -> QueueResult<()> {
        self.output.verify(record, cut, job)
    }
    fn push(&mut self, key: &[u8], owner: Option<&RecordIdentity>) -> QueueResult<()> {
        self.output.insert_encoded(key, owner)
    }
}
#[derive(Serialize, Deserialize)]
struct OwnerRef<'a> {
    #[serde(borrow)]
    collection: Option<&'a str>,
    key: &'a str,
}
fn owner_ref<'a>(bytes: &'a [u8], collection: Option<&str>) -> QueueResult<OwnerRef<'a>> {
    let (owner, rest): (OwnerRef<'a>, _) =
        postcard::take_from_bytes(bytes).map_err(|_| unknown("invalid source unique owner"))?;
    if !rest.is_empty()
        || owner.collection != collection
        || owner.key.trim().is_empty()
        || encoded_len(&owner)? != bytes.len() as u64
    {
        return Err(unknown("invalid source unique owner identity"));
    }
    Ok(owner)
}
fn identity_bytes(owner: IdentityRef<'_>) -> QueueResult<u64> {
    (owner.key.len() as u64)
        .checked_add(owner.collection.map_or(0, |name| name.len() as u64))
        .and_then(|n| n.checked_mul(4))
        .and_then(|n| n.checked_add(1024))
        .ok_or(QueueError::TooLarge)
}
fn owned_identity(owner: IdentityRef<'_>) -> RecordIdentity {
    RecordIdentity {
        collection: owner.collection.map(str::to_string),
        key: owner.key.to_string(),
    }
}
impl<'a, R: QueueSourceReader> QueueSourcePublicationContext<'a, R> {
    pub fn unique_owners<'c>(
        &'c self,
    ) -> QueueResult<QueueSourceUniqueOwnerAccumulator<'c, 'a, R>> {
        Ok(QueueSourceUniqueOwnerAccumulator {
            context: self,
            processed: 0,
            last: None,
            poisoned: false,
            groups: BTreeMap::new(),
            key_charges: Vec::new(),
            _charge: self.job.owned_buffer(
                1024 + std::mem::size_of::<QueueSourceUniqueOwnerAccumulator<'_, '_, R>>() as u64,
            )?,
        })
    }
}
impl<'c, 'a, R: QueueSourceReader> QueueSourceUniqueOwnerAccumulator<'c, 'a, R> {
    pub fn observe(&mut self, transition: &QueueSourceRecordTransition<'_, 'a>) -> QueueResult<()> {
        if self.poisoned {
            return Err(unknown("unique owner accumulator poisoned"));
        }
        self.poisoned = true;
        if !std::ptr::eq(transition.context_owner(), &self.context._charge) {
            return Err(unknown("unique owner transition context differs"));
        }
        let owner = transition.publication_identity();
        if self.processed >= self.context.publication().summary().commands
            || self
                .last
                .is_some_and(|last| (last.collection, last.key) >= (owner.collection, owner.key))
        {
            return Err(unknown("unique owner command coverage differs"));
        }
        transition.visit_named_postings(self.context.job, &mut |effect| {
            for (sort, claim) in [
                (effect.before.filter(|_| effect.before_unique), false),
                (effect.after.filter(|_| effect.after_unique), true),
            ] {
                let Some(sort) = sort else {
                    continue;
                };
                self.retain(owner, effect.index, sort, claim)?;
            }
            Ok(())
        })?;
        self.processed = self.processed.checked_add(1).ok_or(QueueError::TooLarge)?;
        self.last = Some(owner);
        self.poisoned = false;
        Ok(())
    }
    fn retain(
        &mut self,
        owner: IdentityRef<'a>,
        index: &str,
        sort: &[u8],
        claim: bool,
    ) -> QueueResult<()> {
        // The separately admitted duplicate-key probe is transient. Only a new
        // group transfers its guard; repeated evidence retains no tuple copies.
        let bytes = (index.len() as u64)
            .checked_add(sort.len() as u64)
            .and_then(|n| n.checked_add(owner.collection.map_or(0, |name| name.len() as u64)))
            .and_then(|n| n.checked_mul(4))
            .and_then(|n| n.checked_add(1024))
            .ok_or(QueueError::TooLarge)?;
        let key_charge = self.context.job.owned_buffer(bytes)?;
        let key = UniqueIdentity {
            collection: owner.collection.map(str::to_string),
            index: index.to_string(),
            full_sort_key: sort.to_vec(),
        };
        if !self.groups.contains_key(&key) {
            self._charge
                .grow(1024 + 2 * std::mem::size_of::<OwnedQueueBufferCharge>() as u64)?;
            self.key_charges.push(key_charge);
        }
        let group = self.groups.entry(key).or_default();
        if group.len() == group.capacity() {
            let capacity = group
                .capacity()
                .checked_mul(2)
                .ok_or(QueueError::TooLarge)?
                .max(4);
            let additional = capacity
                .checked_sub(group.capacity())
                .and_then(|n| n.checked_mul(std::mem::size_of::<Effect<'a>>()))
                .ok_or(QueueError::TooLarge)?;
            self._charge
                .grow(u64::try_from(additional).map_err(|_| QueueError::TooLarge)?)?;
            group.reserve_exact(capacity - group.len());
        }
        group.push(Effect { owner, claim });
        Ok(())
    }
    pub async fn finish(self) -> QueueResult<QueueSourceUniqueOwnerEdits<'a>> {
        if self.poisoned || self.processed != self.context.publication().summary().commands {
            return Err(unknown("incomplete unique owner command coverage"));
        }
        let context = self.context;
        let mut output = QueueSourceUniqueOwnerEdits {
            output: super::super::edits::SourceEdits::new(
                context.checked.owner(),
                context.cut,
                context.job,
                super::super::QueueSourceCompileLimits {
                    edits: u64::MAX,
                    input_bytes: u64::MAX,
                },
                "unique owner",
            )?,
        };
        for (tuple, effects) in &self.groups {
            let source_key = QueueSourceKey::UniqueOwner {
                collection: tuple.collection.as_deref(),
                index: &tuple.index,
                sort: &tuple.full_sort_key,
            };
            let _key_charge = context.job.owned_buffer(source_key.encoded_len()? as u64)?;
            let source_key = source_key.encode()?;
            let lookup = context.reader.lookup(&source_key).await?;
            let point = context.reader.point(&lookup)?;
            let borrowed = point
                .value_for(&context.cut, &source_key)?
                .map(|bytes| owner_ref(bytes, tuple.collection.as_deref()))
                .transpose()?;
            let _initial_charge =
                context
                    .job
                    .owned_buffer(borrowed.as_ref().map_or(Ok(0), |owner| {
                        identity_bytes(IdentityRef {
                            collection: owner.collection,
                            key: owner.key,
                        })
                    })?)?;
            let initial = borrowed.map(|owner| {
                owned_identity(IdentityRef {
                    collection: owner.collection,
                    key: owner.key,
                })
            });
            let mut reducer = UniqueTupleReducer::new(tuple, true, initial.as_ref(), context.job)?;
            for effect in effects {
                let _owner_charge = context.job.owned_buffer(identity_bytes(effect.owner)?)?;
                let owner = owned_identity(effect.owner);
                reducer.observe(UniqueEvidence {
                    tuple,
                    initial_loaded: true,
                    initial_owner: initial.as_ref(),
                    effect: if effect.claim {
                        UniqueEvidenceEffect::Claim {
                            owner: &owner,
                            origin: UniqueClaimOrigin::Ordinary,
                        }
                    } else {
                        UniqueEvidenceEffect::Release(&owner)
                    },
                })?;
            }
            let decision = reducer.finish_with(|owner| {
                if owner != initial.as_ref() {
                    output.push(&source_key, owner)?;
                }
                Ok(())
            })?;
            if decision.administrative_collision || decision.final_collision {
                return Err(unknown("resolved unique owner effects collide"));
            }
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests;
