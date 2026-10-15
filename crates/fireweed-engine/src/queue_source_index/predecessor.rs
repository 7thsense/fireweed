//! Bind resolved commands, schema and preimages to one authenticated source cut.
mod intrinsics;
#[cfg(test)]
mod test_fixture;
pub use intrinsics::{QueueSourceIntrinsicAccumulator, QueueSourceIntrinsicEdits};
mod aggregates;
mod originals;
pub use originals::QueueSourceOriginalEdits;
mod coverage;
pub use aggregates::{QueueSourceAggregateAccumulator, QueueSourceAggregateEdits};
mod record_edits;
pub use record_edits::{QueueSourceRecordEditAccumulator, QueueSourceRecordEdits};

use super::{
    QueueSourceCut, QueueSourceFields, QueueSourceGenesis, QueueSourceKey, QueueSourceLive,
    QueueSourceReader, QueueSourceRecord, load_command_fields,
};
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_claim_publication::AdmittedClaimBinding,
    queue_log_record::{CheckedQueueLogRecord, QueueLogPayload},
    queue_publication::{CommandRef, PublicationView},
    queue_schema_codec::AdmittedQueueDefinition,
    queue_storage_layout,
};
use fireweed_core::queue_contract::{QueueError, QueueResult};
use serde::de::DeserializeOwned;
use std::sync::Arc;
fn unknown(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("source predecessor: {message}"))
}

/// Checked context only; this cannot publish or manufacture a compiled edit set.
/// All reads remain at the original reader cut, including rejected/receipt-only
/// publications whose command iterator is empty.
pub struct QueueSourcePublicationContext<'a, R: QueueSourceReader> {
    reader: &'a R,
    checked: &'a CheckedQueueLogRecord<'a>,
    definition: &'a AdmittedQueueDefinition,
    job: &'a Arc<QueueReservation>,
    cut: QueueSourceCut<'a>,
    claim_binding: Option<AdmittedClaimBinding<'a>>,
    _charge: OwnedQueueBufferCharge,
}
/// Only the validated publication iterator creates this token. Copies of a
/// public CommandRef cannot introduce an extra command into this context.
pub struct QueueSourceCommand<'c, 'a> {
    resolved: CommandRef<'a>,
    owner: &'c OwnedQueueBufferCharge,
}
pub struct QueueSourcePredecessor<'c, 'a> {
    command: &'c QueueSourceCommand<'c, 'a>,
    cut: QueueSourceCut<'a>,
    fact: Option<QueueSourceRecord>,
    _charge: OwnedQueueBufferCharge,
}
impl QueueSourcePredecessor<'_, '_> {
    pub fn fact(&self) -> Option<QueueSourceRecord> {
        self.fact
    }
    pub fn resolved(&self) -> CommandRef<'_> {
        self.command.resolved
    }
}
impl<'a, R: QueueSourceReader> QueueSourcePublicationContext<'a, R> {
    pub async fn begin(
        reader: &'a R,
        checked: &'a CheckedQueueLogRecord<'a>,
        definition: &'a AdmittedQueueDefinition,
        job: &'a Arc<QueueReservation>,
    ) -> QueueResult<Self> {
        let cut = reader.cut()?;
        cut.validate_publication(checked, job)?;
        if !definition.belongs_to(job)
            || checked.view().definition_pin_bytes() != *definition.pin().as_bytes()
        {
            return Err(unknown("publication definition or operation differs"));
        }
        let charge = job.owned_buffer(std::mem::size_of::<Self>() as u64)?;
        let claim_binding = match checked.view().payload() {
            QueueLogPayload::Genesis(_) => {
                return Err(unknown("genesis has no predecessor publication"));
            }
            QueueLogPayload::Publication(_) => None,
            QueueLogPayload::Claim(claim) => Some(claim.bind(job)?),
        };
        let context = Self {
            reader,
            checked,
            definition,
            job,
            cut,
            claim_binding,
            _charge: charge,
        };
        let genesis: QueueSourceGenesis = context
            .fixed(QueueSourceKey::Genesis)
            .await?
            .ok_or_else(|| unknown("source genesis fact missing"))?;
        if genesis.definition_pin != definition.pin()
            || genesis.source.log_offset() != 0
            || genesis.source.byte_length() == 0
        {
            return Err(unknown("source genesis identity differs"));
        }
        let sequence: u64 = context
            .fixed(QueueSourceKey::Sequence)
            .await?
            .ok_or_else(|| unknown("source sequence missing"))?;
        if sequence != context.publication().summary().initial_sequence {
            return Err(unknown("publication initial sequence differs from source"));
        }
        Ok(context)
    }
    fn publication(&self) -> &PublicationView<'a> {
        match self.checked.view().payload() {
            QueueLogPayload::Publication(publication) => publication,
            QueueLogPayload::Claim(claim) => claim.commands(),
            QueueLogPayload::Genesis(_) => unreachable!("begin rejects genesis"),
        }
    }
    async fn fixed<T: DeserializeOwned + Copy>(
        &self,
        key: QueueSourceKey<'_>,
    ) -> QueueResult<Option<T>> {
        let _charge = self.job.owned_buffer(key.encoded_len()? as u64)?;
        let key = key.encode()?;
        let owner = self.reader.lookup(&key).await?;
        let point = self.reader.point(&owner)?;
        point
            .value_for(&self.cut, &key)?
            .map(queue_storage_layout::decode)
            .transpose()
    }
    pub fn commands(&self) -> impl Iterator<Item = QueueResult<QueueSourceCommand<'_, 'a>>> {
        self.publication().commands().map(|command| {
            Ok(QueueSourceCommand {
                resolved: command.map_err(|_| unknown("invalid resolved command"))?,
                owner: &self._charge,
            })
        })
    }
    pub fn claim_binding(&self) -> Option<&AdmittedClaimBinding<'a>> {
        self.claim_binding.as_ref()
    }
    pub async fn predecessor<'c>(
        &'c self,
        command: &'c QueueSourceCommand<'c, 'a>,
    ) -> QueueResult<QueueSourcePredecessor<'c, 'a>> {
        if !std::ptr::eq(command.owner, &self._charge) {
            return Err(unknown("command belongs to another publication context"));
        }
        let resolved = command.resolved;
        if resolved
            .identity
            .collection
            .is_some_and(|name| !self.definition.schema().state.contains_key(name))
        {
            return Err(unknown("undeclared state collection"));
        }
        let charge = self
            .job
            .owned_buffer(std::mem::size_of::<QueueSourcePredecessor<'_, '_>>() as u64)?;
        let fact: Option<QueueSourceRecord> = self
            .fixed(QueueSourceKey::Record {
                collection: resolved.identity.collection,
                key: resolved.identity.key,
            })
            .await?;
        if let Some(fact) = fact {
            fact.validate(resolved.identity.collection)?;
            match fact.live {
                Some(QueueSourceLive::Work { metadata, body }) => {
                    if metadata.insertion_sequence > self.publication().summary().initial_sequence
                        || body.span().log_offset() >= self.cut.through()
                    {
                        return Err(unknown("work fact references future source state"));
                    }
                }
                Some(QueueSourceLive::State { value })
                    if value.span().log_offset() >= self.cut.through() =>
                {
                    return Err(unknown("state fact references future source state"));
                }
                _ => {}
            }
        }
        let highwater = fact.map_or(0, |fact| fact.highwater);
        let version = fact.and_then(|fact| fact.live.map(|_| fact.highwater));
        if highwater != resolved.prior_highwater || version != resolved.expected_version {
            return Err(unknown("resolved predecessor differs from source"));
        }
        if let QueueLogPayload::Claim(claim) = self.checked.view().payload() {
            let Some(QueueSourceLive::Work { metadata, .. }) = fact.and_then(|fact| fact.live)
            else {
                return Err(unknown("claim predecessor is not live work"));
            };
            let lease = self
                .claim_binding
                .as_ref()
                .and_then(|binding| binding.lease(resolved.identity.key))
                .ok_or_else(|| unknown("claim lease binding missing"))?;
            let expected = crate::queue_claim::plan_claim_metadata(
                metadata,
                lease.token,
                crate::queue_claim::ClaimContext {
                    now: claim.claimed_at(),
                    eligible_at: claim.eligible_at(),
                    expires_at: claim.expires_at(),
                    insertion_head: self.publication().summary().initial_sequence,
                },
            )?;
            let crate::queue_publication::EffectRef::WorkPut {
                metadata: actual, ..
            } = resolved.effect
            else {
                return Err(unknown("claim command is not work"));
            };
            let actual = crate::queue_group_state_authority::WorkMetadata {
                version: resolved.output_highwater,
                priority: actual.priority,
                available_at: actual.available_at,
                insertion_sequence: actual.insertion_sequence,
                lifecycle: actual.lifecycle,
                eligible_since: actual.eligible_since,
                attempts: actual.attempts,
                lease_expires_at: actual.lease_expires_at,
                lease_hash: actual.lease_hash.copied(),
            };
            if actual != expected {
                return Err(unknown("claim transition differs from source predecessor"));
            }
        }
        Ok(QueueSourcePredecessor {
            command,
            cut: self.cut,
            fact,
            _charge: charge,
        })
    }
    /// The declaration and membership count come exclusively from this context's
    /// authenticated genesis/record facts, not caller-provided cache metadata.
    pub async fn fields(
        &self,
        predecessor: &QueueSourcePredecessor<'_, 'a>,
    ) -> QueueResult<QueueSourceFields> {
        if !std::ptr::eq(predecessor.command.owner, &self._charge)
            || !predecessor.cut.same_cut(&self.cut)
        {
            return Err(unknown("field predecessor context differs"));
        }
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
        load_command_fields(
            self.reader,
            command,
            predecessor.fact.map_or(0, |fact| fact.field_count),
            definition,
            self.job,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        queue_admission::QueueAdmission,
        queue_group_state_authority::WorkMetadata,
        queue_log_record::{
            OwnedQueueLogRecord, encode_checked_genesis, encode_checked_publication_record,
        },
        queue_namespace::AddressRef,
        queue_planner::{RecordIdentity, RecordSnapshot, plan_commit_group},
        queue_publication::{BodyCommandRef, EffectRef, PublicationReceipt, encode_publication},
        queue_schema_codec::encode_definition,
        queue_source_index::{QueueSourcePayload, QueueSourcePoint, QueueSourceSpan},
    };
    use fireweed_core::queue_contract::*;
    use std::collections::BTreeMap;
    pub(super) struct Reader {
        pub(super) through: u64,
        pub(super) identity: Arc<OwnedQueueBufferCharge>,
        pub(super) facts: BTreeMap<Vec<u8>, Vec<u8>>,
    }
    impl QueueSourceReader for Reader {
        type Point = Vec<u8>;
        type Range = (Vec<u8>, Vec<u8>);
        fn cut(&self) -> QueueResult<QueueSourceCut<'_>> {
            QueueSourceCut::from_captured(
                &self.identity,
                AddressRef {
                    tenant: "t",
                    queue: "q",
                },
                1,
                self.through,
                self.identity.reservation(),
            )
        }
        async fn lookup(&self, key: &[u8]) -> QueueResult<Self::Point> {
            Ok(key.to_vec())
        }
        fn point<'a>(&'a self, owner: &'a Self::Point) -> QueueResult<QueueSourcePoint<'a>> {
            Ok(self
                .cut()?
                .point_from_source(owner, self.facts.get(owner).map(Vec::as_slice)))
        }
        async fn complete_range(&self, lower: &[u8], upper: &[u8]) -> QueueResult<Self::Range> {
            Ok((lower.to_vec(), upper.to_vec()))
        }
        fn visit_range(
            &self,
            owner: &Self::Range,
            lower: &[u8],
            upper: &[u8],
            visitor: &mut dyn for<'a> FnMut(QueueSourcePoint<'a>) -> QueueResult<()>,
        ) -> QueueResult<()> {
            assert_eq!((&owner.0[..], &owner.1[..]), (lower, upper));
            for (key, value) in &self.facts {
                if key.as_slice() >= lower && key.as_slice() < upper {
                    visitor(self.cut()?.point_from_source(key, Some(value)))?;
                }
            }
            Ok(())
        }
    }
    fn check_work_record_edits(
        context: &QueueSourcePublicationContext<'_, Reader>,
        transition: &QueueSourceRecordTransition<'_, '_>,
        before: &RecordSnapshot,
        after: &RecordSnapshot,
        schema: &QueueSchema,
    ) {
        fn inventory(
            definition: &CollectionDefinition,
            row: Option<&crate::queue_planner::StoredWork>,
        ) -> BTreeMap<Vec<u8>, Vec<u8>> {
            let mut map = BTreeMap::new();
            let Some(row) = row else {
                return map;
            };
            let fields = &row.record.work.fields;
            for (field, value) in fields {
                map.insert(
                    QueueSourceKey::Field {
                        collection: None,
                        key: "work\0α",
                        field,
                    }
                    .encode()
                    .unwrap(),
                    queue_storage_layout::encode(value).unwrap(),
                );
                let sort = crate::queue_index::ordered_key(&[Some(value)]).unwrap();
                map.insert(
                    QueueSourceKey::ScalarPosting {
                        collection: None,
                        field,
                        sort: &sort,
                        key: "work\0α",
                    }
                    .encode()
                    .unwrap(),
                    Vec::new(),
                );
            }
            for (index, columns) in &definition.indexes {
                let values: Vec<_> = columns.iter().map(|name| fields.get(name)).collect();
                let sort = crate::queue_index::ordered_key(&values).unwrap();
                map.insert(
                    QueueSourceKey::NamedPosting {
                        collection: None,
                        index,
                        sort: &sort,
                        key: "work\0α",
                    }
                    .encode()
                    .unwrap(),
                    Vec::new(),
                );
            }
            map
        }
        let mut actual = inventory(&schema.work, before.work.get("work\0α"));
        let mut builder = context
            .record_edits(super::super::QueueSourceCompileLimits {
                edits: 1000,
                input_bytes: 1 << 20,
            })
            .unwrap();
        builder.observe(transition).unwrap();
        let output = builder.finish().unwrap();
        output
            .verify(context.checked.owner(), &context.cut, context.job)
            .unwrap();
        let mut fact_count = 0;
        for (key, value) in output.edits() {
            if key.starts_with(&[1, 2]) {
                fact_count += 1;
                assert_eq!(
                    key,
                    QueueSourceKey::Record {
                        collection: None,
                        key: "work\0α"
                    }
                    .encode()
                    .unwrap()
                );
                let fact: QueueSourceRecord = postcard::from_bytes(value.unwrap()).unwrap();
                assert_eq!(
                    fact.highwater,
                    after.versions[&RecordIdentity {
                        collection: None,
                        key: "work\0α".into()
                    }]
                );
                if let Some(row) = after.work.get("work\0α") {
                    assert_eq!(fact.field_count, row.record.work.fields.len() as u64);
                    let Some(QueueSourceLive::Work { metadata, body }) = fact.live else {
                        panic!()
                    };
                    assert_eq!(
                        metadata,
                        WorkMetadata {
                            version: row.record.version,
                            priority: row.record.work.priority,
                            available_at: row.record.work.available_at,
                            insertion_sequence: row.record.insertion_sequence,
                            lifecycle: row.record.lifecycle,
                            eligible_since: row.record.eligible_since,
                            attempts: row.record.attempts,
                            lease_expires_at: row.record.lease_expires_at,
                            lease_hash: row.lease_hash,
                        }
                    );
                    body.verify_cached(&row.record.work.body).unwrap();
                } else {
                    assert!(fact.live.is_none());
                    assert_eq!(fact.field_count, 0);
                }
            } else if let Some(value) = value {
                actual.insert(key.to_vec(), value.to_vec());
            } else {
                assert!(actual.remove(key).is_some());
            }
        }
        assert_eq!(fact_count, 1);
        assert_eq!(actual, inventory(&schema.work, after.work.get("work\0α")));
    }

    fn check_scalar_postings(
        transition: &QueueSourceRecordTransition<'_, '_>,
        collection: Option<&str>,
        key: &str,
        before: &Fields,
        after: &Fields,
        job: &Arc<QueueReservation>,
    ) -> usize {
        fn inventory(
            collection: Option<&str>,
            key: &str,
            fields: &Fields,
        ) -> std::collections::BTreeSet<Vec<u8>> {
            fields
                .iter()
                .map(|(field, value)| {
                    let sort = crate::queue_index::ordered_key(&[Some(value)]).unwrap();
                    QueueSourceKey::ScalarPosting {
                        collection,
                        field,
                        sort: &sort,
                        key,
                    }
                    .encode()
                    .unwrap()
                })
                .collect()
        }
        let mut actual = inventory(collection, key, before);
        let mut calls = 0;
        transition
            .visit_scalar_postings(job, &mut |posting, present| {
                let key = posting.encode()?;
                if present {
                    assert!(actual.insert(key));
                } else {
                    assert!(actual.remove(&key));
                }
                calls += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, inventory(collection, key, after));
        if calls > 0 {
            let mut stopped = 0;
            let result = transition.visit_scalar_postings(job, &mut |_, _| {
                stopped += 1;
                Err(QueueError::OutcomeUnknown("posting stopped".into()))
            });
            assert!(
                matches!(result, Err(QueueError::OutcomeUnknown(ref message)) if message == "posting stopped")
            );
            assert_eq!(stopped, 1);
        }
        calls
    }
    fn check_named_postings(
        transition: &QueueSourceRecordTransition<'_, '_>,
        definition: &CollectionDefinition,
        before: Option<&Fields>,
        after: Option<&Fields>,
        job: &Arc<QueueReservation>,
    ) -> usize {
        fn inventory(
            definition: &CollectionDefinition,
            fields: Option<&Fields>,
        ) -> BTreeMap<String, (Vec<u8>, bool)> {
            let Some(fields) = fields else {
                return BTreeMap::new();
            };
            definition
                .indexes
                .iter()
                .map(|(index, columns)| {
                    let values: Vec<_> = columns.iter().map(|name| fields.get(name)).collect();
                    let unique = definition.unique_indexes.contains(index)
                        && values.iter().all(Option::is_some);
                    (
                        index.clone(),
                        (crate::queue_index::ordered_key(&values).unwrap(), unique),
                    )
                })
                .collect()
        }
        let mut actual = inventory(definition, before);
        let mut calls = 0;
        transition
            .visit_named_postings(job, &mut |effect| {
                let old = actual.remove(effect.index);
                assert_eq!(old.as_ref().map(|(sort, _)| sort.as_slice()), effect.before);
                assert_eq!(
                    old.as_ref().is_some_and(|(_, unique)| *unique),
                    effect.before_unique
                );
                if let Some(sort) = effect.after {
                    actual.insert(
                        effect.index.to_string(),
                        (sort.to_vec(), effect.after_unique),
                    );
                } else {
                    assert!(!effect.after_unique);
                }
                calls += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, inventory(definition, after));
        if calls > 0 {
            let mut stopped = 0;
            let result = transition.visit_named_postings(job, &mut |_| {
                stopped += 1;
                Err(QueueError::OutcomeUnknown("named stopped".into()))
            });
            assert!(
                matches!(result, Err(QueueError::OutcomeUnknown(ref message)) if message == "named stopped")
            );
            assert_eq!(stopped, 1);
        }
        calls
    }
    pub(super) fn group(id: &str) -> CommitGroup {
        CommitGroup {
            request_id: id.into(),
            administration: None,
            claimed_updates: vec![],
            state_updates: vec![],
            work_updates: vec![],
            work_inserts: vec![],
        }
    }
    pub(super) fn record(
        queue: &QueueAddress,
        schema: &QueueSchema,
        before: &RecordSnapshot,
        group: &CommitGroup,
        job: &Arc<QueueReservation>,
    ) -> (OwnedQueueLogRecord, RecordSnapshot) {
        let mut after = before.clone();
        let probes =
            crate::queue_planner::required_unique_probes(group, &after, 0, schema).unwrap();
        after.unique_probes_loaded.extend(probes);
        let delta = plan_commit_group(group, &mut after, 0, schema).unwrap();
        let receipt = PublicationReceipt::commit(queue, group, &delta.receipt).unwrap();
        let canonical = crate::queue_planner::combine_deltas(
            std::slice::from_ref(&delta),
            before.next_sequence,
        )
        .unwrap();
        let frame = encode_publication(
            queue,
            &encode_definition(schema).unwrap(),
            before,
            &canonical,
            &[receipt],
            1 << 20,
            job,
        )
        .unwrap();
        (
            encode_checked_publication_record(1, frame.as_bytes(), 1 << 20, job).unwrap(),
            after,
        )
    }
    #[test]
    fn context_binds_schema_sequence_commands_records_and_field_membership() {
        futures::executor::block_on(async {
            let admission = QueueAdmission::new(2, 8 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let foreign = Arc::new(admission.reserve(0).unwrap());
            let schema = QueueSchema {
                work: CollectionDefinition {
                    fields: BTreeMap::from([
                        ("0".into(), FieldType::Integer),
                        ("a".into(), FieldType::Integer),
                        ("b".into(), FieldType::Text),
                        ("z".into(), FieldType::Integer),
                    ]),
                    indexes: BTreeMap::from([
                        ("ab".into(), vec!["a".into(), "b".into()]),
                        ("ba".into(), vec!["b".into(), "a".into()]),
                        ("bz".into(), vec!["b".into(), "z".into()]),
                    ]),
                    unique_indexes: Default::default(),
                    additional_field_prefixes: Default::default(),
                },
                state: Default::default(),
                progress_bound_nanos: 1,
                max_attempts: 3,
            };
            let raw = encode_definition(&schema).unwrap();
            let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
            let queue = QueueAddress {
                tenant: "t".into(),
                queue: "q".into(),
            };
            let genesis =
                encode_checked_genesis(1, &queue, &definition, &[7; 32], 1 << 20, &job).unwrap();
            let checked_genesis = genesis.checked().unwrap();
            let mut seed = group("seed");
            seed.work_inserts.push(NewWork {
                key: "work\0α".into(),
                priority: 3,
                available_at: 0,
                fields: Fields::from([
                    ("a".into(), FieldValue::Integer(7)),
                    ("b".into(), FieldValue::Text("full\0β".into())),
                ]),
                body: vec![1, 2, 3].into(),
            });
            seed.work_inserts.push(NewWork {
                key: "other".into(),
                priority: 0,
                available_at: 0,
                fields: Fields::new(),
                body: Default::default(),
            });
            let (seed_record, snapshot) =
                record(&queue, &schema, &RecordSnapshot::default(), &seed, &job);
            let checked_seed = seed_record.checked().unwrap();
            let QueueLogPayload::Publication(seed_view) = checked_seed.view().payload() else {
                panic!()
            };
            let seed_command = seed_view
                .commands()
                .map(Result::unwrap)
                .find(|command| command.identity.key == "work\0α")
                .unwrap();
            let EffectRef::WorkPut {
                metadata,
                body: BodyCommandRef::Replace(body),
                ..
            } = seed_command.effect
            else {
                panic!()
            };
            let fact = QueueSourceRecord {
                highwater: 1,
                field_count: 2,
                live: Some(QueueSourceLive::Work {
                    metadata: WorkMetadata {
                        version: 1,
                        priority: metadata.priority,
                        available_at: metadata.available_at,
                        insertion_sequence: metadata.insertion_sequence,
                        lifecycle: metadata.lifecycle,
                        eligible_since: metadata.eligible_since,
                        attempts: metadata.attempts,
                        lease_expires_at: metadata.lease_expires_at,
                        lease_hash: metadata.lease_hash.copied(),
                    },
                    body: QueueSourcePayload::from_checked(&checked_seed, 1, body).unwrap(),
                }),
            };
            let genesis_fact = QueueSourceGenesis {
                source: QueueSourceSpan::from_checked(
                    &checked_genesis,
                    0,
                    &genesis.bytes()[checked_genesis.payload_interval().unwrap()],
                )
                .unwrap(),
                definition_pin: definition.pin(),
            };
            let record_key = QueueSourceKey::Record {
                collection: None,
                key: "work\0α",
            }
            .encode()
            .unwrap();
            let mut facts = BTreeMap::from([
                (
                    QueueSourceKey::Genesis.encode().unwrap(),
                    queue_storage_layout::encode(&genesis_fact).unwrap(),
                ),
                (
                    QueueSourceKey::Sequence.encode().unwrap(),
                    queue_storage_layout::encode(&snapshot.next_sequence).unwrap(),
                ),
                (
                    record_key.clone(),
                    queue_storage_layout::encode(&fact).unwrap(),
                ),
            ]);
            // Independent preimages come from the existing planner's full image,
            // not from the source compiler being exercised.
            for (name, value) in &snapshot.work["work\0α"].record.work.fields {
                facts.insert(
                    QueueSourceKey::Field {
                        collection: None,
                        key: "work\0α",
                        field: name,
                    }
                    .encode()
                    .unwrap(),
                    queue_storage_layout::encode(value).unwrap(),
                );
            }
            for lifecycle in [
                Lifecycle::Pending,
                Lifecycle::Leased,
                Lifecycle::Completed,
                Lifecycle::Failed,
            ] {
                let count = snapshot
                    .work
                    .values()
                    .filter(|row| row.record.lifecycle == lifecycle)
                    .count() as u64;
                facts.insert(
                    QueueSourceKey::LifecycleCount(lifecycle).encode().unwrap(),
                    queue_storage_layout::encode(&count).unwrap(),
                );
            }
            let reader = Reader {
                through: 2,
                identity: Arc::new(job.owned_buffer(128).unwrap()),
                facts,
            };
            let mut patch = group("patch");
            patch.work_updates.push(UnclaimedUpdate {
                key: "work\0α".into(),
                expect: StateExpect::Version(1),
                change: UnclaimedChange::Patch(WorkPatch {
                    fields: BTreeMap::from([("a".into(), Some(FieldValue::Integer(8)))]),
                    ..Default::default()
                }),
            });
            let (target, _) = record(&queue, &schema, &snapshot, &patch, &job);
            let checked = target.checked().unwrap();
            let baseline = admission.usage().unwrap();
            {
                let context =
                    QueueSourcePublicationContext::begin(&reader, &checked, &definition, &job)
                        .await
                        .unwrap();
                assert!(context.claim_binding().is_none());
                let command = context.commands().next().unwrap().unwrap();
                let before = context.predecessor(&command).await.unwrap();
                assert_eq!(before.fact().unwrap().field_count, 2);
                let fields = context.fields(&before).await.unwrap();
                assert_eq!(
                    fields.checked().unwrap().get("a").unwrap(),
                    Some(&FieldValue::Integer(7))
                );
                assert_eq!(
                    fields.checked().unwrap().get("b").unwrap(),
                    Some(&FieldValue::Text("full\0β".into()))
                );
                let other =
                    QueueSourcePublicationContext::begin(&reader, &checked, &definition, &job)
                        .await
                        .unwrap();
                assert!(other.predecessor(&command).await.is_err());
                assert!(other.fields(&before).await.is_err());
                assert!(other.record_transition(&before).await.is_err());
                let transition = context.record_transition(&before).await.unwrap();
                assert_eq!(transition.fact().field_count, 2);
                let Some(QueueSourceLive::Work { metadata, body }) = transition.fact().live else {
                    panic!()
                };
                assert_eq!((metadata.version, metadata.priority), (2, 3));
                assert_eq!(body.span().log_offset(), 1);
                assert_eq!(
                    body.descriptor(),
                    fact.live
                        .map(|live| match live {
                            QueueSourceLive::Work { body, .. } => body.descriptor(),
                            _ => panic!(),
                        })
                        .unwrap()
                );
                assert_eq!(
                    transition.before_fields().unwrap().get("b").unwrap(),
                    snapshot.work["work\0α"].record.work.fields.get("b")
                );
            }
            assert_eq!(admission.usage().unwrap(), baseline);
            // Derive final field membership from checked writer publications,
            // then compare the borrowed effects to an independent snapshot diff.
            for change in [
                UnclaimedChange::Patch(WorkPatch {
                    priority: Some(9),
                    ..Default::default()
                }),
                UnclaimedChange::Patch(WorkPatch {
                    fields: BTreeMap::from([("a".into(), Some(FieldValue::Integer(7)))]),
                    ..Default::default()
                }),
                UnclaimedChange::Patch(WorkPatch {
                    fields: BTreeMap::from([
                        ("a".into(), None),
                        ("z".into(), Some(FieldValue::Integer(2))),
                    ]),
                    ..Default::default()
                }),
                UnclaimedChange::Replace(NewWork {
                    key: "work\0α".into(),
                    priority: 3,
                    available_at: 0,
                    fields: Fields::from([
                        ("0".into(), FieldValue::Integer(0)),
                        ("b".into(), FieldValue::Text("new\0β".into())),
                        ("z".into(), FieldValue::Integer(9)),
                    ]),
                    body: Default::default(),
                }),
                UnclaimedChange::Replace(NewWork {
                    key: "work\0α".into(),
                    priority: 3,
                    available_at: 0,
                    fields: Fields::new(),
                    body: Default::default(),
                }),
                UnclaimedChange::Delete,
            ] {
                let mut request = group("field-effects");
                request.work_updates.push(UnclaimedUpdate {
                    key: "work\0α".into(),
                    expect: StateExpect::Version(1),
                    change,
                });
                let (publication, after) = record(&queue, &schema, &snapshot, &request, &job);
                let checked = publication.checked().unwrap();
                let context =
                    QueueSourcePublicationContext::begin(&reader, &checked, &definition, &job)
                        .await
                        .unwrap();
                let command = context.commands().next().unwrap().unwrap();
                let predecessor = context.predecessor(&command).await.unwrap();
                let transition = context.record_transition(&predecessor).await.unwrap();
                let edit_baseline = admission.usage().unwrap();
                check_work_record_edits(&context, &transition, &snapshot, &after, &schema);
                super::intrinsics::tests::assert_inventory_delta(&context, &snapshot, &after).await;
                assert_eq!(admission.usage().unwrap(), edit_baseline);
                let mut effects = BTreeMap::new();
                let mut order = Vec::new();
                transition
                    .visit_field_changes(&mut |name, old, new| {
                        order.push(name.to_string());
                        assert!(
                            effects
                                .insert(
                                    name.to_string(),
                                    (
                                        old.map(|value| value.into_owned()),
                                        new.map(|value| value.into_owned())
                                    )
                                )
                                .is_none()
                        );
                        Ok(())
                    })
                    .unwrap();
                if let EffectRef::WorkPut {
                    fields: crate::queue_publication::FieldsCommandRef::Preserve,
                    ..
                } = transition.resolved().effect
                {
                    assert!(effects.is_empty());
                    assert!(
                        transition
                            .visit_after_fields(&["a"], &mut |_, _| Ok(()))
                            .is_err()
                    );
                }
                // Applying the stream exactly reconstructs the writer's final
                // bag; only explicit edits may appear for partial commands.
                let mut rebuilt = snapshot.work["work\0α"].record.work.fields.clone();
                for (name, (old, new)) in &effects {
                    assert_eq!(rebuilt.get(name), old.as_ref());
                    match new {
                        Some(value) => {
                            rebuilt.insert(name.clone(), value.clone());
                        }
                        None => {
                            rebuilt.remove(name);
                        }
                    }
                }
                assert_eq!(order, effects.keys().cloned().collect::<Vec<_>>());
                let names: Vec<_> = effects.keys().map(String::as_str).collect();
                let mut final_values = BTreeMap::new();
                transition
                    .visit_after_fields(&names, &mut |name, value| {
                        final_values
                            .insert(name.to_string(), value.map(|value| value.into_owned()));
                        Ok(())
                    })
                    .unwrap();
                for (name, (_, expected_value)) in &effects {
                    assert_eq!(final_values[name], *expected_value);
                }
                let mut invalid_calls = 0;
                for names in [["z", "a"], ["a", "a"]] {
                    assert!(
                        transition
                            .visit_after_fields(&names, &mut |_, _| {
                                invalid_calls += 1;
                                Ok(())
                            })
                            .is_err()
                    );
                }
                assert_eq!(invalid_calls, 0);
                if let EffectRef::WorkPut {
                    fields: crate::queue_publication::FieldsCommandRef::Edit(_),
                    ..
                } = transition.resolved().effect
                {
                    let mut dependency = None;
                    transition
                        .visit_after_fields(&["b"], &mut |_, value| {
                            dependency = value.map(|value| value.into_owned());
                            Ok(())
                        })
                        .unwrap();
                    assert_eq!(
                        dependency,
                        snapshot.work["work\0α"]
                            .record
                            .work
                            .fields
                            .get("b")
                            .cloned()
                    );
                    assert!(
                        transition
                            .visit_after_fields(&["not-loaded"], &mut |_, _| Ok(()))
                            .is_err()
                    );
                }
                let expected = after
                    .work
                    .get("work\0α")
                    .map(|row| row.record.work.fields.clone())
                    .unwrap_or_default();
                assert_eq!(rebuilt, expected);
                let named_baseline = admission.usage().unwrap();
                let named_count = check_named_postings(
                    &transition,
                    &schema.work,
                    Some(&snapshot.work["work\0α"].record.work.fields),
                    after.work.get("work\0α").map(|row| &row.record.work.fields),
                    &job,
                );
                assert_eq!(admission.usage().unwrap(), named_baseline);
                let mut foreign_named = 0;
                assert!(
                    transition
                        .visit_named_postings(&foreign, &mut |_| {
                            foreign_named += 1;
                            Ok(())
                        })
                        .is_err()
                );
                assert_eq!(foreign_named, 0);
                if named_count > 0 {
                    let mut peak = 0;
                    transition
                        .visit_named_postings(&job, &mut |_| {
                            peak = peak.max(admission.usage()?.1 - named_baseline.1);
                            Ok(())
                        })
                        .unwrap();
                    assert!(peak > 0);
                    let mut filler = job
                        .owned_buffer((8 << 20) - named_baseline.1 - peak)
                        .unwrap();
                    transition
                        .visit_named_postings(&job, &mut |_| Ok(()))
                        .unwrap();
                    filler.grow(1).unwrap();
                    let failure = transition.visit_named_postings(&job, &mut |_| Ok(()));
                    assert!(
                        matches!(failure, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("admitted buffer capacity exhausted"))
                    );
                    drop(filler);
                    assert_eq!(admission.usage().unwrap(), named_baseline);
                }

                let posting_baseline = admission.usage().unwrap();
                let count = check_scalar_postings(
                    &transition,
                    None,
                    "work\0α",
                    &snapshot.work["work\0α"].record.work.fields,
                    &expected,
                    &job,
                );
                assert_eq!(admission.usage().unwrap(), posting_baseline);
                if snapshot.work["work\0α"].record.work.fields == expected {
                    assert_eq!(count, 0);
                }
                let mut foreign_calls = 0;
                assert!(
                    transition
                        .visit_scalar_postings(&foreign, &mut |_, _| {
                            foreign_calls += 1;
                            Ok(())
                        })
                        .is_err()
                );
                assert_eq!(foreign_calls, 0);
                if count == 0 {
                    let filler = job.owned_buffer((8 << 20) - posting_baseline.1).unwrap();
                    transition
                        .visit_scalar_postings(&job, &mut |_, _| panic!("no-op posting"))
                        .unwrap();
                    drop(filler);
                    assert_eq!(admission.usage().unwrap(), posting_baseline);
                }
                if count > 0 {
                    let mut peak = 0;
                    transition
                        .visit_scalar_postings(&job, &mut |_, _| {
                            peak = peak.max(admission.usage()?.1 - posting_baseline.1);
                            Ok(())
                        })
                        .unwrap();
                    assert!(peak > 0);
                    let mut filler = job
                        .owned_buffer((8 << 20) - posting_baseline.1 - peak)
                        .unwrap();
                    transition
                        .visit_scalar_postings(&job, &mut |_, _| Ok(()))
                        .unwrap();
                    filler.grow(1).unwrap();
                    let failure = transition.visit_scalar_postings(&job, &mut |_, _| Ok(()));
                    assert!(
                        matches!(failure, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("admitted buffer capacity exhausted"))
                    );
                    drop(filler);
                    assert_eq!(admission.usage().unwrap(), posting_baseline);
                }
                if let EffectRef::WorkPut {
                    fields: crate::queue_publication::FieldsCommandRef::Edit(edits),
                    ..
                } = transition.resolved().effect
                {
                    assert_eq!(effects.len() as u64, edits.len());
                    assert!(!effects.contains_key("b")); // tuple dependency only
                }
                if !effects.is_empty() {
                    let mut calls = 0;
                    let failure = transition.visit_field_changes(&mut |_, _, _| {
                        calls += 1;
                        Err(QueueError::OutcomeUnknown("visitor stopped".into()))
                    });
                    assert!(
                        matches!(failure, Err(QueueError::OutcomeUnknown(ref message)) if message == "visitor stopped")
                    );
                    assert_eq!(calls, 1);
                }
            }
            assert_eq!(admission.usage().unwrap(), baseline);
            let foreign_definition = AdmittedQueueDefinition::from_raw(&raw, &foreign).unwrap();
            assert!(
                QueueSourcePublicationContext::begin(&reader, &checked, &foreign_definition, &job)
                    .await
                    .is_err()
            );
            let mut wrong_schema = schema.clone();
            wrong_schema.max_attempts += 1;
            let wrong_definition =
                AdmittedQueueDefinition::from_raw(&encode_definition(&wrong_schema).unwrap(), &job)
                    .unwrap();
            assert!(
                QueueSourcePublicationContext::begin(&reader, &checked, &wrong_definition, &job)
                    .await
                    .is_err()
            );
            let mut wrong_reader = Reader {
                through: 2,
                identity: reader.identity.clone(),
                facts: reader.facts.clone(),
            };
            wrong_reader.facts.insert(
                QueueSourceKey::Genesis.encode().unwrap(),
                queue_storage_layout::encode(&QueueSourceGenesis {
                    definition_pin: wrong_definition.pin(),
                    ..genesis_fact
                })
                .unwrap(),
            );
            let mismatch = QueueSourcePublicationContext::begin(
                &wrong_reader,
                &checked,
                &wrong_definition,
                &job,
            )
            .await;
            assert!(
                matches!(mismatch, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("publication definition or operation differs"))
            );

            for (key, value) in [
                (
                    QueueSourceKey::Sequence.encode().unwrap(),
                    Some(queue_storage_layout::encode(&3u64).unwrap()),
                ),
                (QueueSourceKey::Genesis.encode().unwrap(), None),
                (
                    QueueSourceKey::Genesis.encode().unwrap(),
                    Some(
                        queue_storage_layout::encode(&QueueSourceGenesis {
                            definition_pin: wrong_definition.pin(),
                            ..genesis_fact
                        })
                        .unwrap(),
                    ),
                ),
            ] {
                let mut bad = Reader {
                    through: 2,
                    identity: reader.identity.clone(),
                    facts: reader.facts.clone(),
                };
                if let Some(value) = value {
                    bad.facts.insert(key, value);
                } else {
                    bad.facts.remove(&key);
                }
                assert!(
                    QueueSourcePublicationContext::begin(&bad, &checked, &definition, &job)
                        .await
                        .is_err()
                );
            }
            let Some(QueueSourceLive::Work {
                metadata: old_metadata,
                body: old_body,
            }) = fact.live
            else {
                panic!()
            };
            for bad_fact in [
                None,
                Some(QueueSourceRecord {
                    highwater: 2,
                    live: Some(QueueSourceLive::Work {
                        metadata: WorkMetadata {
                            version: 2,
                            ..old_metadata
                        },
                        body: old_body,
                    }),
                    ..fact
                }),
                Some(QueueSourceRecord {
                    highwater: 1,
                    field_count: 0,
                    live: None,
                }),
            ] {
                let mut bad = Reader {
                    through: 2,
                    identity: reader.identity.clone(),
                    facts: reader.facts.clone(),
                };
                if let Some(fact) = bad_fact {
                    bad.facts.insert(
                        record_key.clone(),
                        queue_storage_layout::encode(&fact).unwrap(),
                    );
                } else {
                    bad.facts.remove(&record_key);
                }
                let context =
                    QueueSourcePublicationContext::begin(&bad, &checked, &definition, &job)
                        .await
                        .unwrap();
                let command = context.commands().next().unwrap().unwrap();
                let failure = context.predecessor(&command).await;
                assert!(
                    matches!(failure, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("resolved predecessor differs from source"))
                );
            }
            for future_fact in [
                QueueSourceRecord {
                    live: Some(QueueSourceLive::Work {
                        metadata: WorkMetadata {
                            insertion_sequence: 3,
                            ..old_metadata
                        },
                        body: old_body,
                    }),
                    ..fact
                },
                QueueSourceRecord {
                    live: Some(QueueSourceLive::Work {
                        metadata: old_metadata,
                        body: QueueSourcePayload::from_checked(&checked_seed, 2, body).unwrap(),
                    }),
                    ..fact
                },
            ] {
                let mut bad = Reader {
                    through: 2,
                    identity: reader.identity.clone(),
                    facts: reader.facts.clone(),
                };
                bad.facts.insert(
                    record_key.clone(),
                    queue_storage_layout::encode(&future_fact).unwrap(),
                );
                let context =
                    QueueSourcePublicationContext::begin(&bad, &checked, &definition, &job)
                        .await
                        .unwrap();
                let command = context.commands().next().unwrap().unwrap();
                let failure = context.predecessor(&command).await;
                assert!(
                    matches!(failure, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("future source state"))
                );
            }
            let mut replace = group("replace");
            replace.work_updates.push(UnclaimedUpdate {
                key: "work\0α".into(),
                expect: StateExpect::Version(1),
                change: UnclaimedChange::Replace(NewWork {
                    key: "work\0α".into(),
                    priority: -5,
                    available_at: 7,
                    fields: Fields::from([("a".into(), FieldValue::Integer(11))]),
                    body: bytes::Bytes::from_static(b"new body"),
                }),
            });
            let mut remove = group("remove-field");
            remove.work_updates.push(UnclaimedUpdate {
                key: "work\0α".into(),
                expect: StateExpect::Version(1),
                change: UnclaimedChange::Patch(WorkPatch {
                    fields: BTreeMap::from([("a".into(), None)]),
                    body: Some(bytes::Bytes::from_static(b"new body")),
                    ..Default::default()
                }),
            });
            for request in [&replace, &remove] {
                let (target, _) = record(&queue, &schema, &snapshot, request, &job);
                let checked = target.checked().unwrap();
                let context =
                    QueueSourcePublicationContext::begin(&reader, &checked, &definition, &job)
                        .await
                        .unwrap();
                let command = context.commands().next().unwrap().unwrap();
                let predecessor = context.predecessor(&command).await.unwrap();
                let transition = context.record_transition(&predecessor).await.unwrap();
                assert_eq!(
                    (transition.fact().highwater, transition.fact().field_count),
                    (2, 1)
                );
                let Some(QueueSourceLive::Work { body, .. }) = transition.fact().live else {
                    panic!()
                };
                assert_eq!(body.span().log_offset(), 2);
                assert_eq!(body.span().resolve(&checked, 2).unwrap(), b"new body");
                body.verify_cached(b"new body").unwrap();
                assert!(body.verify_cached(b"bad body").is_err());
            }
            // A structurally valid command may reuse an already allocated
            // insertion slot. Recompute FWQL2 so this reaches correspondence.
            let QueueLogPayload::Publication(target_view) = checked.view().payload() else {
                panic!()
            };
            let fragment = target_view.command_fragments().next().unwrap().unwrap();
            let sequence_in_fragment =
                1 + 8 + fragment.command.identity.key.len() + 1 + 8 + 8 + 8 + 1 + 8 + 8;
            assert_eq!(
                &fragment.as_bytes()[sequence_in_fragment..sequence_in_fragment + 8],
                &1u64.to_le_bytes()
            );
            let sequence_offset = fragment.as_bytes().as_ptr().addr()
                - target.bytes().as_ptr().addr()
                + sequence_in_fragment;
            let charge = job
                .owned_buffer(target.bytes().len() as u64 * 4 + 4096)
                .unwrap();
            let mut changed = target.bytes().to_vec();
            changed[sequence_offset..sequence_offset + 8].copy_from_slice(&2u64.to_le_bytes());
            let end = changed.len() - 32;
            let digest = sha2::Sha256::digest(&changed[..end]);
            changed[end..].copy_from_slice(&digest);
            let changed =
                OwnedQueueLogRecord::from_admitted_bytes(changed.into(), charge, &job).unwrap();
            let changed_checked = changed.checked().unwrap();
            let changed_context =
                QueueSourcePublicationContext::begin(&reader, &changed_checked, &definition, &job)
                    .await
                    .unwrap();
            let changed_command = changed_context.commands().next().unwrap().unwrap();
            let predecessor = changed_context.predecessor(&changed_command).await.unwrap();
            let failure = changed_context.record_transition(&predecessor).await;
            assert!(
                matches!(failure, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("new work sequence does not advance source insertion order"))
            );
            let mut delete_work = group("delete-work");
            delete_work.work_updates.push(UnclaimedUpdate {
                key: "work\0α".into(),
                expect: StateExpect::Version(1),
                change: UnclaimedChange::Delete,
            });
            let mut recreate_work = group("recreate-work");
            recreate_work.work_inserts.push(NewWork {
                key: "work\0α".into(),
                priority: 4,
                available_at: 0,
                fields: Fields::new(),
                body: bytes::Bytes::from_static(b"recreated"),
            });
            let mut packed_snapshot = snapshot.clone();
            let deleted =
                plan_commit_group(&delete_work, &mut packed_snapshot, 0, &schema).unwrap();
            let recreated =
                plan_commit_group(&recreate_work, &mut packed_snapshot, 0, &schema).unwrap();
            let deltas = [deleted, recreated];
            let packed =
                crate::queue_planner::combine_deltas(&deltas, snapshot.next_sequence).unwrap();
            let receipts = [
                PublicationReceipt::commit(&queue, &delete_work, &deltas[0].receipt).unwrap(),
                PublicationReceipt::commit(&queue, &recreate_work, &deltas[1].receipt).unwrap(),
            ];
            let frame =
                encode_publication(&queue, &raw, &snapshot, &packed, &receipts, 1 << 20, &job)
                    .unwrap();
            let packed_record =
                encode_checked_publication_record(1, frame.as_bytes(), 1 << 20, &job).unwrap();
            let packed_checked = packed_record.checked().unwrap();
            let packed_context =
                QueueSourcePublicationContext::begin(&reader, &packed_checked, &definition, &job)
                    .await
                    .unwrap();
            let packed_command = packed_context.commands().next().unwrap().unwrap();
            let packed_before = packed_context.predecessor(&packed_command).await.unwrap();
            let transition = packed_context
                .record_transition(&packed_before)
                .await
                .unwrap();
            assert_eq!(
                (transition.fact().highwater, transition.fact().field_count),
                (3, 0)
            );
            let Some(QueueSourceLive::Work { metadata, .. }) = transition.fact().live else {
                panic!()
            };
            assert_eq!(metadata.insertion_sequence, 3);
            for (name, value, reason) in [
                (
                    "a",
                    FieldValue::Boolean(true),
                    "resolved field violates authenticated schema",
                ),
                (
                    "rogue",
                    FieldValue::Integer(1),
                    "resolved field is undeclared",
                ),
            ] {
                let mut after = snapshot.clone();
                let mut delta = plan_commit_group(&patch, &mut after, 0, &schema).unwrap();
                delta.work_fields.insert(
                    "work\0α".into(),
                    crate::queue_fields::ResolvedFields::Edit(BTreeMap::from([(
                        name.into(),
                        Some(value.clone()),
                    )])),
                );
                delta.work[0].record.work.fields =
                    snapshot.work["work\0α"].record.work.fields.clone();
                delta.work[0].record.work.fields.insert(name.into(), value);
                let receipt = PublicationReceipt::commit(&queue, &patch, &delta.receipt).unwrap();
                let frame =
                    encode_publication(&queue, &raw, &snapshot, &delta, &[receipt], 1 << 20, &job)
                        .unwrap();
                let invalid =
                    encode_checked_publication_record(1, frame.as_bytes(), 1 << 20, &job).unwrap();
                let invalid_checked = invalid.checked().unwrap();
                let context = QueueSourcePublicationContext::begin(
                    &reader,
                    &invalid_checked,
                    &definition,
                    &job,
                )
                .await
                .unwrap();
                let command = context.commands().next().unwrap().unwrap();
                let predecessor = context.predecessor(&command).await.unwrap();
                let failure = context.record_transition(&predecessor).await;
                assert!(
                    matches!(failure, Err(QueueError::OutcomeUnknown(ref message)) if message.contains(reason)),
                    "{name}"
                );
            }
            let mut bad_count = Reader {
                through: 2,
                identity: reader.identity.clone(),
                facts: reader.facts.clone(),
            };
            bad_count.facts.insert(
                record_key.clone(),
                queue_storage_layout::encode(&QueueSourceRecord {
                    field_count: 1,
                    ..fact
                })
                .unwrap(),
            );
            let context =
                QueueSourcePublicationContext::begin(&bad_count, &checked, &definition, &job)
                    .await
                    .unwrap();
            let command = context.commands().next().unwrap().unwrap();
            let predecessor = context.predecessor(&command).await.unwrap();
            let failure = context.record_transition(&predecessor).await;
            assert!(
                matches!(failure, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("selected fields exceed source membership count"))
            );
            // Canonical claim publication binds one returned token to the same
            // resolved transition, then checks that transition against source.
            let token = vec![7; 32];
            let claimed = crate::queue_claim::plan_claim(
                snapshot.work["work\0α"].clone(),
                token.clone(),
                crate::queue_claim::ClaimContext {
                    now: 10,
                    eligible_at: 0,
                    expires_at: 1_000_010,
                    insertion_head: 2,
                },
            )
            .unwrap();
            let versions = vec![RecordVersion {
                collection: None,
                key: "work\0α".into(),
                version: 2,
            }];
            let claim_delta = crate::queue_planner::CommitDelta {
                work: vec![claimed.work],
                work_fields: BTreeMap::from([(
                    "work\0α".into(),
                    crate::queue_fields::ResolvedFields::Preserve,
                )]),
                work_bodies: BTreeMap::from([(
                    "work\0α".into(),
                    crate::queue_body::ResolvedBody::Preserve,
                )]),
                deleted_work: vec![],
                state: vec![],
                deleted_state: vec![],
                versions: versions.clone(),
                next_sequence: 2,
                receipt: CommitReceipt {
                    request_id: String::new(),
                    outcome: GroupOutcome::Committed { versions },
                },
            };
            let request = ClaimRequest {
                queue: queue.clone(),
                request_id: "claim".into(),
                filter: Filter::default(),
                order: ClaimOrder::PriorityAscending,
                eligible_at: 0,
                lease_nanos: 1_000_000,
                max_records: 1,
                max_bytes: 1 << 20,
                fields: Some(Default::default()),
                include_body: false,
            };
            let limits = QueueLimits {
                max_records: 10,
                max_groups: 10,
                max_request_bytes: 1 << 20,
                max_group_bytes: 1 << 20,
                max_response_bytes: 1 << 20,
                max_body_bytes: 1 << 20,
                max_state_bytes: 1 << 20,
            };
            let receipt = crate::queue_claim_receipt::encode_claim_receipt(
                &[crate::queue_claim_receipt::ClaimLease {
                    key: "work\0α".into(),
                    token: token.clone(),
                    version: 2,
                }],
                1 << 20,
                &job,
            )
            .unwrap();
            let encode_claim = |delta: &crate::queue_planner::CommitDelta| {
                let commands = crate::queue_publication::encode_publication_pinned(
                    &queue,
                    definition.pin(),
                    &snapshot,
                    delta,
                    &[],
                    1 << 20,
                    &job,
                )
                .unwrap();
                let publication = crate::queue_claim_publication::encode_claim_publication(
                    crate::queue_claim_publication::ClaimPublicationInput {
                        request: &request,
                        definition: &definition,
                        commands: commands.as_bytes(),
                        receipt: &receipt,
                        claimed_at: 10,
                        limits: &limits,
                    },
                    1 << 20,
                    &job,
                )
                .unwrap();
                crate::queue_log_record::encode_checked_claim_record(1, &publication, 1 << 20, &job)
                    .unwrap()
            };
            let claim_record = encode_claim(&claim_delta);
            let checked_claim = claim_record.checked().unwrap();
            let context =
                QueueSourcePublicationContext::begin(&reader, &checked_claim, &definition, &job)
                    .await
                    .unwrap();
            assert!(context.claim_binding().is_some());
            let claim_command = context.commands().next().unwrap().unwrap();
            let claim_predecessor = context.predecessor(&claim_command).await.unwrap();
            let claim_transition = context.record_transition(&claim_predecessor).await.unwrap();
            assert_eq!(claim_transition.fact().field_count, 2);
            let Some(QueueSourceLive::Work { metadata, body }) = claim_transition.fact().live
            else {
                panic!()
            };
            assert_eq!(
                (metadata.version, metadata.lifecycle, metadata.attempts),
                (2, Lifecycle::Leased, 1)
            );
            assert_eq!(body.span().log_offset(), 1);
            let mut aggregates = context
                .aggregates(super::super::QueueSourceCompileLimits {
                    edits: 5,
                    input_bytes: 4096,
                })
                .unwrap();
            aggregates.observe(&claim_transition).unwrap();
            let counts = aggregates.finish().await.unwrap();
            let actual: BTreeMap<_, _> = counts
                .edits()
                .map(|(key, value)| {
                    (
                        key.to_vec(),
                        postcard::from_bytes::<u64>(value.unwrap()).unwrap(),
                    )
                })
                .collect();
            let mut after_claim = snapshot.work.clone();
            for row in &claim_delta.work {
                after_claim.insert(row.record.work.key.clone(), row.clone());
            }
            let mut claimed_snapshot = snapshot.clone();
            claimed_snapshot.work = after_claim.clone();
            claimed_snapshot
                .versions
                .extend(claim_delta.versions.iter().map(|v| {
                    (
                        RecordIdentity {
                            collection: v.collection.clone(),
                            key: v.key.clone(),
                        },
                        v.version,
                    )
                }));
            super::intrinsics::tests::assert_inventory_delta(
                &context,
                &snapshot,
                &claimed_snapshot,
            )
            .await;
            super::intrinsics::tests::assert_one_claim_and_renewal(
                &context,
                &snapshot,
                &claimed_snapshot,
                &token,
            )
            .await;
            let expected: BTreeMap<_, _> = [Lifecycle::Pending, Lifecycle::Leased]
                .into_iter()
                .map(|lifecycle| {
                    (
                        QueueSourceKey::LifecycleCount(lifecycle).encode().unwrap(),
                        after_claim
                            .values()
                            .filter(|row| row.record.lifecycle == lifecycle)
                            .count() as u64,
                    )
                })
                .collect();
            assert_eq!(actual, expected);
            drop(counts);
            let mut changed_delta = claim_delta.clone();
            changed_delta.work[0].record.work.priority += 1;
            let changed_claim = encode_claim(&changed_delta);
            let checked_changed = changed_claim.checked().unwrap();
            let changed_context =
                QueueSourcePublicationContext::begin(&reader, &checked_changed, &definition, &job)
                    .await
                    .unwrap();
            let changed_command = changed_context.commands().next().unwrap().unwrap();
            let failure = changed_context.predecessor(&changed_command).await;
            assert!(
                matches!(failure, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("claim transition differs from source predecessor"))
            );
            for (label, metadata, reason) in [
                (
                    "attempt mismatch",
                    WorkMetadata {
                        attempts: 7,
                        ..old_metadata
                    },
                    "claim transition differs from source predecessor",
                ),
                (
                    "attempt overflow",
                    WorkMetadata {
                        attempts: u32::MAX,
                        ..old_metadata
                    },
                    "attempts exhausted",
                ),
                (
                    "terminal",
                    WorkMetadata {
                        lifecycle: Lifecycle::Completed,
                        ..old_metadata
                    },
                    "native selection returned ineligible work",
                ),
                (
                    "not due",
                    WorkMetadata {
                        available_at: 1,
                        ..old_metadata
                    },
                    "native selection returned ineligible work",
                ),
                (
                    "unexpired lease",
                    WorkMetadata {
                        lifecycle: Lifecycle::Leased,
                        lease_expires_at: Some(30),
                        lease_hash: Some(crate::queue_planner::lease_hash(&[9])),
                        ..old_metadata
                    },
                    "native selection returned ineligible work",
                ),
                (
                    "reused lease",
                    WorkMetadata {
                        lifecycle: Lifecycle::Leased,
                        lease_expires_at: Some(10),
                        lease_hash: Some(crate::queue_planner::lease_hash(&token)),
                        ..old_metadata
                    },
                    "reused predecessor lease token",
                ),
            ] {
                let mut bad = Reader {
                    through: 2,
                    identity: reader.identity.clone(),
                    facts: reader.facts.clone(),
                };
                bad.facts.insert(
                    record_key.clone(),
                    queue_storage_layout::encode(&QueueSourceRecord {
                        live: Some(QueueSourceLive::Work {
                            metadata,
                            body: old_body,
                        }),
                        ..fact
                    })
                    .unwrap(),
                );
                let context =
                    QueueSourcePublicationContext::begin(&bad, &checked_claim, &definition, &job)
                        .await
                        .unwrap();
                let command = context.commands().next().unwrap().unwrap();
                let failure = context.predecessor(&command).await;
                assert!(
                    matches!(failure, Err(QueueError::OutcomeUnknown(ref message)) if message.contains(reason)),
                    "{label}"
                );
            }
            let mut expired = Reader {
                through: 2,
                identity: reader.identity.clone(),
                facts: reader.facts.clone(),
            };
            let expired_metadata = WorkMetadata {
                lifecycle: Lifecycle::Leased,
                lease_expires_at: Some(10),
                lease_hash: Some(crate::queue_planner::lease_hash(&[9])),
                ..old_metadata
            };
            expired.facts.insert(
                record_key.clone(),
                queue_storage_layout::encode(&QueueSourceRecord {
                    live: Some(QueueSourceLive::Work {
                        metadata: expired_metadata,
                        body: old_body,
                    }),
                    ..fact
                })
                .unwrap(),
            );
            let expired_context =
                QueueSourcePublicationContext::begin(&expired, &checked_claim, &definition, &job)
                    .await
                    .unwrap();
            let expired_command = expired_context.commands().next().unwrap().unwrap();
            expired_context.predecessor(&expired_command).await.unwrap();
            // Mutate the persisted receipt token, retain valid structure and
            // recompute FWQL2 checksum. Begin must fail on binding, not framing.
            let QueueLogPayload::Claim(claim) = checked_claim.view().payload() else {
                panic!()
            };
            let entry = claim.receipt().entries().next().unwrap().unwrap();
            let token_offset = entry.token.as_ptr().addr() - claim_record.bytes().as_ptr().addr();
            let charge = job
                .owned_buffer(claim_record.bytes().len() as u64 * 4 + 4096)
                .unwrap();
            let mut corrupted = claim_record.bytes().to_vec();
            corrupted[token_offset] ^= 1;
            let end = corrupted.len() - 32;
            use sha2::Digest;
            let digest = sha2::Sha256::digest(&corrupted[..end]);
            corrupted[end..].copy_from_slice(&digest);
            let corrupted =
                OwnedQueueLogRecord::from_admitted_bytes(corrupted.into(), charge, &job).unwrap();
            let checked_corrupted = corrupted.checked().unwrap();
            let bad_binding = QueueSourcePublicationContext::begin(
                &reader,
                &checked_corrupted,
                &definition,
                &job,
            )
            .await;
            assert!(
                matches!(bad_binding, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("lease entry and resolved metadata differ"))
            );
            let mut create = group("fresh");
            create.work_inserts.push(NewWork {
                key: "new".into(),
                priority: 0,
                available_at: 0,
                fields: Fields::new(),
                body: Default::default(),
            });
            let (new_record, _) = record(&queue, &schema, &snapshot, &create, &job);
            let checked_new = new_record.checked().unwrap();
            let context =
                QueueSourcePublicationContext::begin(&reader, &checked_new, &definition, &job)
                    .await
                    .unwrap();
            let command = context.commands().next().unwrap().unwrap();
            assert!(
                context
                    .predecessor(&command)
                    .await
                    .unwrap()
                    .fact()
                    .is_none()
            );
            let mut tombstone_reader = Reader {
                through: 2,
                identity: reader.identity.clone(),
                facts: reader.facts.clone(),
            };
            tombstone_reader.facts.insert(
                QueueSourceKey::Record {
                    collection: None,
                    key: "new",
                }
                .encode()
                .unwrap(),
                queue_storage_layout::encode(&QueueSourceRecord {
                    highwater: 1,
                    field_count: 0,
                    live: None,
                })
                .unwrap(),
            );
            let fresh_context = QueueSourcePublicationContext::begin(
                &tombstone_reader,
                &checked_new,
                &definition,
                &job,
            )
            .await
            .unwrap();
            let fresh_command = fresh_context.commands().next().unwrap().unwrap();
            let mismatch = fresh_context.predecessor(&fresh_command).await;
            assert!(
                matches!(mismatch, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("resolved predecessor differs from source"))
            );
            let mut deleted_snapshot = snapshot.clone();
            deleted_snapshot.versions.insert(
                crate::queue_planner::RecordIdentity {
                    collection: None,
                    key: "new".into(),
                },
                1,
            );
            let (recreated, recreated_snapshot) =
                record(&queue, &schema, &deleted_snapshot, &create, &job);
            let checked_recreated = recreated.checked().unwrap();
            let recreated_context = QueueSourcePublicationContext::begin(
                &tombstone_reader,
                &checked_recreated,
                &definition,
                &job,
            )
            .await
            .unwrap();
            let recreated_command = recreated_context.commands().next().unwrap().unwrap();
            let recreated_predecessor = recreated_context
                .predecessor(&recreated_command)
                .await
                .unwrap();
            assert_eq!(recreated_predecessor.fact().unwrap().highwater, 1);
            assert_eq!(recreated_predecessor.resolved().output_highwater, 2);
            let recreated_transition = recreated_context
                .record_transition(&recreated_predecessor)
                .await
                .unwrap();
            super::intrinsics::tests::assert_inventory_delta(
                &recreated_context,
                &deleted_snapshot,
                &recreated_snapshot,
            )
            .await;
            assert_eq!(
                check_named_postings(
                    &recreated_transition,
                    &schema.work,
                    None,
                    Some(&Fields::new()),
                    &job
                ),
                schema.work.indexes.len()
            );

            assert_eq!(
                recreated_context
                    .fields(&recreated_predecessor)
                    .await
                    .unwrap()
                    .present_count(),
                0
            );
        });
    }
    #[test]
    fn state_source_references_and_declared_collection_are_checked_before_fields() {
        futures::executor::block_on(async {
            let admission = QueueAdmission::new(1, 8 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let empty = CollectionDefinition {
                fields: BTreeMap::from([
                    ("a".into(), FieldType::Integer),
                    ("b".into(), FieldType::Text),
                    ("z".into(), FieldType::Integer),
                ]),
                indexes: BTreeMap::from([("ba".into(), vec!["b".into(), "a".into()])]),
                unique_indexes: std::collections::BTreeSet::from(["ba".into()]),
                additional_field_prefixes: Default::default(),
            };
            let schema = QueueSchema {
                work: empty.clone(),
                state: BTreeMap::from([("state".into(), empty)]),
                progress_bound_nanos: 1,
                max_attempts: 3,
            };
            let definition =
                AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job)
                    .unwrap();
            let queue = QueueAddress {
                tenant: "t".into(),
                queue: "q".into(),
            };
            let genesis =
                encode_checked_genesis(1, &queue, &definition, &[7; 32], 1 << 20, &job).unwrap();
            let checked_genesis = genesis.checked().unwrap();
            let genesis_fact = QueueSourceGenesis {
                source: QueueSourceSpan::from_checked(
                    &checked_genesis,
                    0,
                    &genesis.bytes()[checked_genesis.payload_interval().unwrap()],
                )
                .unwrap(),
                definition_pin: definition.pin(),
            };
            let mut seed = group("seed-state");
            seed.state_updates.push(StateUpdate {
                collection: "state".into(),
                key: "row".into(),
                expect: StateExpect::Absent,
                change: StateChange::Put {
                    fields: Fields::from([
                        ("a".into(), FieldValue::Integer(7)),
                        ("b".into(), FieldValue::Text("old\0β".into())),
                    ]),
                    value: vec![7, 8].into(),
                },
            });
            let (seed_record, snapshot) =
                record(&queue, &schema, &RecordSnapshot::default(), &seed, &job);
            let checked_seed = seed_record.checked().unwrap();
            let QueueLogPayload::Publication(publication) = checked_seed.view().payload() else {
                panic!()
            };
            let EffectRef::StatePut { value, .. } =
                publication.commands().next().unwrap().unwrap().effect
            else {
                panic!()
            };
            let key = QueueSourceKey::Record {
                collection: Some("state"),
                key: "row",
            }
            .encode()
            .unwrap();
            let mut reader = Reader {
                through: 2,
                identity: Arc::new(job.owned_buffer(128).unwrap()),
                facts: BTreeMap::from([
                    (
                        QueueSourceKey::Genesis.encode().unwrap(),
                        queue_storage_layout::encode(&genesis_fact).unwrap(),
                    ),
                    (
                        QueueSourceKey::Sequence.encode().unwrap(),
                        queue_storage_layout::encode(&0u64).unwrap(),
                    ),
                ]),
            };
            for (name, value) in &snapshot.state[&("state".into(), "row".into())].fields {
                reader.facts.insert(
                    QueueSourceKey::Field {
                        collection: Some("state"),
                        key: "row",
                        field: name,
                    }
                    .encode()
                    .unwrap(),
                    queue_storage_layout::encode(value).unwrap(),
                );
            }
            let mut patch = group("patch-state");
            patch.state_updates.push(StateUpdate {
                collection: "state".into(),
                key: "row".into(),
                expect: StateExpect::Version(1),
                change: StateChange::Put {
                    fields: Fields::from([("z".into(), FieldValue::Integer(9))]),
                    value: vec![9].into(),
                },
            });
            let (target, patched) = record(&queue, &schema, &snapshot, &patch, &job);
            let checked = target.checked().unwrap();
            for offset in [1, 2] {
                let fact = QueueSourceRecord {
                    highwater: 1,
                    field_count: 2,
                    live: Some(QueueSourceLive::State {
                        value: QueueSourcePayload::from_checked(&checked_seed, offset, value)
                            .unwrap(),
                    }),
                };
                reader
                    .facts
                    .insert(key.clone(), queue_storage_layout::encode(&fact).unwrap());
                let context =
                    QueueSourcePublicationContext::begin(&reader, &checked, &definition, &job)
                        .await
                        .unwrap();
                let command = context.commands().next().unwrap().unwrap();
                let result = context.predecessor(&command).await;
                if offset == 1 {
                    let predecessor = result.unwrap();
                    assert_eq!(
                        context.fields(&predecessor).await.unwrap().present_count(),
                        2
                    );
                    let transition = context.record_transition(&predecessor).await.unwrap();
                    assert_eq!(
                        (transition.fact().highwater, transition.fact().field_count),
                        (2, 1)
                    );
                    let mut rebuilt = snapshot.state[&("state".into(), "row".into())]
                        .fields
                        .clone();
                    let mut order = Vec::new();
                    transition
                        .visit_field_changes(&mut |name, old, new| {
                            order.push(name.to_string());
                            assert_eq!(rebuilt.get(name), old.map(|v| v.into_owned()).as_ref());
                            match new {
                                Some(value) => {
                                    rebuilt.insert(name.to_string(), value.into_owned());
                                }
                                None => {
                                    rebuilt.remove(name);
                                }
                            }
                            Ok(())
                        })
                        .unwrap();
                    assert_eq!(order, ["a", "b", "z"]);
                    assert_eq!(
                        rebuilt,
                        patched.state[&("state".into(), "row".into())].fields
                    );
                    check_named_postings(
                        &transition,
                        &schema.state["state"],
                        Some(&snapshot.state[&("state".into(), "row".into())].fields),
                        Some(&rebuilt),
                        &job,
                    );
                    check_scalar_postings(
                        &transition,
                        Some("state"),
                        "row",
                        &snapshot.state[&("state".into(), "row".into())].fields,
                        &rebuilt,
                        &job,
                    );

                    super::intrinsics::tests::assert_inventory_delta(&context, &snapshot, &patched)
                        .await;
                    let Some(QueueSourceLive::State { value }) = transition.fact().live else {
                        panic!()
                    };
                    assert_eq!(value.span().log_offset(), 2);
                    assert_eq!(value.span().resolve(&checked, 2).unwrap(), &[9]);
                } else {
                    assert!(
                        matches!(result, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("state fact references future source state"))
                    );
                }
            }
            reader.facts.insert(
                key.clone(),
                queue_storage_layout::encode(&QueueSourceRecord {
                    highwater: 1,
                    field_count: 2,
                    live: Some(QueueSourceLive::State {
                        value: QueueSourcePayload::from_checked(&checked_seed, 1, value).unwrap(),
                    }),
                })
                .unwrap(),
            );
            let mut delete = group("delete-state");
            delete.state_updates.push(StateUpdate {
                collection: "state".into(),
                key: "row".into(),
                expect: StateExpect::Version(1),
                change: StateChange::Delete,
            });
            let (deleted, deleted_snapshot) = record(&queue, &schema, &snapshot, &delete, &job);
            let checked_delete = deleted.checked().unwrap();
            {
                let context = QueueSourcePublicationContext::begin(
                    &reader,
                    &checked_delete,
                    &definition,
                    &job,
                )
                .await
                .unwrap();
                let command = context.commands().next().unwrap().unwrap();
                let predecessor = context.predecessor(&command).await.unwrap();
                let transition = context.record_transition(&predecessor).await.unwrap();
                assert_eq!(
                    (transition.fact().highwater, transition.fact().field_count),
                    (2, 0)
                );
                assert!(transition.fact().live.is_none());
                super::intrinsics::tests::assert_inventory_delta(
                    &context,
                    &snapshot,
                    &deleted_snapshot,
                )
                .await;
                let mut deleted_fields = Vec::new();
                transition
                    .visit_field_changes(&mut |name, old, new| {
                        assert!(old.is_some() && new.is_none());
                        deleted_fields.push(name.to_string());
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(deleted_fields, ["a", "b"]);
                check_named_postings(
                    &transition,
                    &schema.state["state"],
                    Some(&snapshot.state[&("state".into(), "row".into())].fields),
                    None,
                    &job,
                );

                check_scalar_postings(
                    &transition,
                    Some("state"),
                    "row",
                    &snapshot.state[&("state".into(), "row".into())].fields,
                    &Fields::new(),
                    &job,
                );
            }
            let mut empty_state = group("empty-state");
            empty_state.state_updates.push(StateUpdate {
                collection: "state".into(),
                key: "empty".into(),
                expect: StateExpect::Absent,
                change: StateChange::Put {
                    fields: Fields::new(),
                    value: Default::default(),
                },
            });
            let (created, created_snapshot) =
                record(&queue, &schema, &snapshot, &empty_state, &job);
            let checked_created = created.checked().unwrap();
            {
                let context = QueueSourcePublicationContext::begin(
                    &reader,
                    &checked_created,
                    &definition,
                    &job,
                )
                .await
                .unwrap();
                let command = context.commands().next().unwrap().unwrap();
                let predecessor = context.predecessor(&command).await.unwrap();
                assert!(predecessor.fact().is_none());
                let transition = context.record_transition(&predecessor).await.unwrap();
                assert_eq!(transition.fact().field_count, 0);
                super::intrinsics::tests::assert_inventory_delta(
                    &context,
                    &snapshot,
                    &created_snapshot,
                )
                .await;
                assert_eq!(
                    check_named_postings(
                        &transition,
                        &schema.state["state"],
                        None,
                        Some(&created_snapshot.state[&("state".into(), "empty".into())].fields),
                        &job
                    ),
                    1
                );
            }
            let mut complete_state = group("complete-state");
            complete_state.state_updates.push(StateUpdate {
                collection: "state".into(),
                key: "row".into(),
                expect: StateExpect::Version(1),
                change: StateChange::Put {
                    fields: Fields::from([
                        ("a".into(), FieldValue::Integer(8)),
                        ("b".into(), FieldValue::Text("complete\0β".into())),
                    ]),
                    value: vec![1].into(),
                },
            });
            let (complete, complete_snapshot) =
                record(&queue, &schema, &snapshot, &complete_state, &job);
            let checked_complete = complete.checked().unwrap();
            {
                let context = QueueSourcePublicationContext::begin(
                    &reader,
                    &checked_complete,
                    &definition,
                    &job,
                )
                .await
                .unwrap();
                let command = context.commands().next().unwrap().unwrap();
                let predecessor = context.predecessor(&command).await.unwrap();
                let transition = context.record_transition(&predecessor).await.unwrap();
                assert_eq!(
                    check_named_postings(
                        &transition,
                        &schema.state["state"],
                        Some(&snapshot.state[&("state".into(), "row".into())].fields),
                        Some(&complete_snapshot.state[&("state".into(), "row".into())].fields),
                        &job
                    ),
                    1
                );
                super::intrinsics::tests::assert_inventory_delta(
                    &context,
                    &snapshot,
                    &complete_snapshot,
                )
                .await;
                let mut complete_owner = false;
                transition
                    .visit_named_postings(&job, &mut |effect| {
                        complete_owner |= effect.after_unique;
                        Ok(())
                    })
                    .unwrap();
                assert!(complete_owner);
            }
            // Structurally checked commands can name a collection absent from
            // the authenticated schema. Reject it before any field path runs.
            let mut no_state = schema.clone();
            no_state.state.clear();
            let no_state_definition =
                AdmittedQueueDefinition::from_raw(&encode_definition(&no_state).unwrap(), &job)
                    .unwrap();
            let no_state_genesis =
                encode_checked_genesis(1, &queue, &no_state_definition, &[7; 32], 1 << 20, &job)
                    .unwrap();
            let checked_no_state = no_state_genesis.checked().unwrap();
            let no_state_fact = QueueSourceGenesis {
                source: QueueSourceSpan::from_checked(
                    &checked_no_state,
                    0,
                    &no_state_genesis.bytes()[checked_no_state.payload_interval().unwrap()],
                )
                .unwrap(),
                definition_pin: no_state_definition.pin(),
            };
            reader.facts.insert(
                QueueSourceKey::Genesis.encode().unwrap(),
                queue_storage_layout::encode(&no_state_fact).unwrap(),
            );
            let mut after = snapshot.clone();
            let delta = plan_commit_group(&patch, &mut after, 0, &schema).unwrap();
            let receipt = PublicationReceipt::commit(&queue, &patch, &delta.receipt).unwrap();
            let frame = crate::queue_publication::encode_publication_pinned(
                &queue,
                no_state_definition.pin(),
                &snapshot,
                &delta,
                &[receipt],
                1 << 20,
                &job,
            )
            .unwrap();
            let target =
                encode_checked_publication_record(1, frame.as_bytes(), 1 << 20, &job).unwrap();
            let checked = target.checked().unwrap();
            let context =
                QueueSourcePublicationContext::begin(&reader, &checked, &no_state_definition, &job)
                    .await
                    .unwrap();
            let command = context.commands().next().unwrap().unwrap();
            let failure = context.predecessor(&command).await;
            assert!(
                matches!(failure, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("undeclared state collection"))
            );
        });
    }
}

mod transition;
pub use transition::{QueueSourceNamedEffect, QueueSourceRecordTransition};

mod unique;
pub use unique::{QueueSourceUniqueOwnerAccumulator, QueueSourceUniqueOwnerEdits};
