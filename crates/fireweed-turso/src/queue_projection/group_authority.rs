//! Addressed ordinary group read set. Work uses explicit field/body knowledge;
//! state changes replace/delete complete values and need only old fields/version.
use super::*;
use fireweed_engine::queue_planner::{RecordIdentity, RecordSnapshot, required_work_fields};
use fireweed_engine::queue_state_authority::StateDescriptor;
use std::collections::{BTreeMap, BTreeSet};
#[derive(Default)]
struct WorkRead {
    fields: bool,
    body: bool,
    names: BTreeSet<String>,
}
#[derive(Default)]
struct StateRead {
    fields: bool,
    source: Option<StateDescriptor>,
}
#[derive(Default)]
struct ReadSet<'a> {
    work: BTreeMap<&'a str, WorkRead>,
    state: BTreeMap<(&'a str, &'a str), StateRead>,
}
pub(super) struct GroupRead<'a> {
    pub group: &'a CommitGroup,
    pub states:
        Option<&'a fireweed_engine::queue_group_state_authority::AdmittedGroupStateAuthority>,
    pub sequence: u64,
}
pub(super) async fn load_group(
    connection: &Connection,
    queue_id: i64,
    schema: &QueueSchema,
    input: GroupRead<'_>,
    job: &Arc<QueueReservation>,
    charges: &mut Vec<OwnedQueueBufferCharge>,
) -> QueueResult<RecordSnapshot> {
    load_group_with_fields(connection, queue_id, schema, input, job, charges, false).await
}
pub(super) async fn load_group_with_fields(
    connection: &Connection,
    queue_id: i64,
    schema: &QueueSchema,
    input: GroupRead<'_>,
    job: &Arc<QueueReservation>,
    charges: &mut Vec<OwnedQueueBufferCharge>,
    complete_work_fields: bool,
) -> QueueResult<RecordSnapshot> {
    load_groups(
        connection,
        queue_id,
        schema,
        std::slice::from_ref(&input),
        job,
        charges,
        complete_work_fields,
    )
    .await
}
pub(super) async fn load_groups(
    connection: &Connection,
    queue_id: i64,
    schema: &QueueSchema,
    inputs: &[GroupRead<'_>],
    job: &Arc<QueueReservation>,
    charges: &mut Vec<OwnedQueueBufferCharge>,
    complete_work_fields: bool,
) -> QueueResult<RecordSnapshot> {
    let sequence = inputs
        .first()
        .ok_or_else(|| unknown("empty group materialization set"))?
        .sequence;
    // Keys and declarations are borrowed; reserve the actual union containers,
    // not another copy of all incoming values. Name copies are admitted below.
    let capacity = inputs.iter().try_fold(4096u64, |total, input| {
        let group = input.group;
        let work = group
            .claimed_updates
            .len()
            .checked_add(group.work_updates.len())
            .and_then(|n| n.checked_add(group.work_inserts.len()))
            .ok_or(QueueError::TooLarge)?;
        let work_bytes = (work as u64)
            .checked_mul(
                (std::mem::size_of::<(&str, WorkRead)>() as u64)
                    .checked_mul(4)
                    .ok_or(QueueError::TooLarge)?,
            )
            .ok_or(QueueError::TooLarge)?;
        let state_bytes = (group.state_updates.len() as u64)
            .checked_mul(
                (std::mem::size_of::<((&str, &str), StateRead)>() as u64)
                    .checked_mul(4)
                    .ok_or(QueueError::TooLarge)?,
            )
            .ok_or(QueueError::TooLarge)?;
        total
            .checked_add(work_bytes)
            .and_then(|n| n.checked_add(state_bytes))
            .ok_or(QueueError::TooLarge)
    })?;
    charges.push(job.owned_buffer(capacity)?);
    let mut readset = ReadSet::default();
    for input in inputs {
        if input.sequence != sequence {
            return Err(unknown("group materialization sequence mismatch"));
        }
        let group = input.group;
        let names = required_work_fields(group, schema)?;
        let count =
            group.claimed_updates.len() + group.work_updates.len() + group.work_inserts.len();
        let capacity = names
            .iter()
            .try_fold(1024u64, |total, name| {
                total
                    .checked_add(name.len() as u64)
                    .and_then(|n| n.checked_add(256))
                    .ok_or(QueueError::TooLarge)
            })?
            .checked_mul(count as u64)
            .and_then(|n| n.checked_mul(16))
            .ok_or(QueueError::TooLarge)?;
        charges.push(job.owned_buffer(capacity)?);
        for update in &group.claimed_updates {
            let need = readset.work.entry(update.key.as_str()).or_default();
            if update.disposition != Disposition::Check {
                need.fields = true;
                need.names.extend(names.iter().cloned());
            }
        }
        for update in &group.work_updates {
            let need = readset.work.entry(update.key.as_str()).or_default();
            need.fields = true;
            need.names.extend(names.iter().cloned());
            need.body |= matches!(&update.change, UnclaimedChange::Administrative {patch,..} if patch.body.is_some());
        }
        for insert in &group.work_inserts {
            let need = readset.work.entry(insert.key.as_str()).or_default();
            need.fields = true;
            need.names.extend(names.iter().cloned());
        }
        for (position, update) in group.state_updates.iter().enumerate() {
            let need = readset
                .state
                .entry((update.collection.as_str(), update.key.as_str()))
                .or_default();
            let source = input
                .states
                .map(|states| states.descriptor(position, &update.key))
                .transpose()?;
            if let (Some(old), Some(new)) = (need.source, source)
                && old != new
            {
                return Err(unknown("group materialization source identity disagrees"));
            }
            need.source = need.source.or(source);
            need.fields |= update.change != StateChange::Check;
        }
    }
    let mut snapshot = RecordSnapshot {
        next_sequence: sequence,
        ..Default::default()
    };
    for (key, need) in readset.work {
        let identity = RecordIdentity {
            collection: None,
            key: key.into(),
        };
        match super::work::load_work_authority(connection, queue_id, key, sequence).await? {
            super::work::WorkAuthority::Absent => {}
            super::work::WorkAuthority::Deleted { version } => {
                snapshot.versions.insert(identity, version);
            }
            super::work::WorkAuthority::Present { id, mut work } => {
                let mut total = 0;
                let mut exceeded = false;
                if need.fields {
                    payload::load_payload(
                        connection,
                        id,
                        &schema.work,
                        payload::PayloadSelection {
                            fields: if complete_work_fields {
                                None
                            } else {
                                Some(&need.names)
                            },
                            include_body: need.body,
                            max_bytes: u64::MAX,
                        },
                        &mut payload::NativePayloadBudget {
                            job,
                            charges,
                            total: &mut total,
                            response_limit: &mut exceeded,
                        },
                        &mut work.record.work.fields,
                        &mut work.record.work.body,
                    )
                    .await?;
                }
                work.field_coverage = if !need.fields {
                    FieldCoverage::Selected {
                        names: BTreeSet::new(),
                        prefixes: Default::default(),
                    }
                } else if complete_work_fields {
                    FieldCoverage::Complete
                } else {
                    FieldCoverage::Selected {
                        names: need.names,
                        prefixes: Default::default(),
                    }
                };
                work.body_loaded = need.body;
                snapshot.versions.insert(identity, work.record.version);
                snapshot.work.insert(key.into(), *work);
            }
        }
    }
    for ((collection_name, key), need) in readset.state {
        let collection = collection_number(schema, Some(collection_name))?;
        let definition = collection_definition(schema, collection)?;
        let authority =
            super::state_authority::load_state_authority(connection, queue_id, collection, key)
                .await?;
        let source = need.source;
        if let Some(source) = source {
            super::state_source::validate_presence(source, &authority)?;
        }
        let (version, id) = match authority {
            super::state_authority::StateAuthority::Absent => continue,
            super::state_authority::StateAuthority::Deleted { version } => (version, None),
            super::state_authority::StateAuthority::Present { id, version } => (version, Some(id)),
        };
        snapshot.versions.insert(
            RecordIdentity {
                collection: Some(collection_name.into()),
                key: key.into(),
            },
            version,
        );
        let Some(id) = id else { continue };
        let mut fields = Fields::new();
        let mut body = bytes::Bytes::new();
        let mut total = 0;
        let mut exceeded = false;
        if need.fields {
            if let Some(source) = source {
                super::state_source::validate_fields_metadata(connection, id, source).await?;
            }
            payload::load_payload(
                connection,
                id,
                definition,
                payload::PayloadSelection {
                    fields: None,
                    include_body: false,
                    max_bytes: match source {
                        Some(
                            fireweed_engine::queue_state_authority::StateDescriptor::Present {
                                fields,
                                ..
                            },
                        ) => fields.encoded_bytes,
                        _ => u64::MAX,
                    },
                },
                &mut payload::NativePayloadBudget {
                    job,
                    charges,
                    total: &mut total,
                    response_limit: &mut exceeded,
                },
                &mut fields,
                &mut body,
            )
            .await
            .map_err(|error| {
                if source.is_some() && matches!(error, QueueError::TooLarge) {
                    unknown("cached state fields exceeded source bounds")
                } else {
                    error
                }
            })?;
            if let Some(source) = source {
                source.verify_fields(version, &fields)?;
            }
        }
        snapshot.state.insert(
            (collection_name.into(), key.into()),
            StateRecord {
                collection: collection_name.into(),
                key: key.into(),
                version,
                fields,
                value: bytes::Bytes::new(),
            },
        );
    }
    Ok(snapshot)
}
