//! Addressed ordinary group read set. Work uses explicit field/body knowledge;
//! state changes replace/delete complete values and need only old fields/version.
use super::*;
use fireweed_engine::queue_planner::{RecordIdentity, RecordSnapshot, required_work_fields};
use std::collections::BTreeSet;
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
    let GroupRead {
        group,
        states,
        sequence,
    } = input;
    let names = required_work_fields(group, schema)?;
    let work_keys: BTreeSet<_> = group
        .claimed_updates
        .iter()
        .map(|u| u.key.as_str())
        .chain(group.work_updates.iter().map(|u| u.key.as_str()))
        .chain(group.work_inserts.iter().map(|u| u.key.as_str()))
        .collect();
    let check_keys: BTreeSet<_> = group
        .claimed_updates
        .iter()
        .filter(|u| u.disposition == Disposition::Check)
        .map(|u| u.key.as_str())
        .collect();
    let empty_names = BTreeSet::new();
    let body_keys:BTreeSet<_>=group.work_updates.iter().filter(|u|matches!(&u.change,UnclaimedChange::Administrative {patch,..} if patch.body.is_some())).map(|u|u.key.as_str()).collect();
    let certificate = names.iter().try_fold(1024u64, |n, name| {
        n.checked_add(name.len() as u64)
            .and_then(|n| n.checked_add(256))
            .ok_or(QueueError::TooLarge)
    })?;
    charges.push(
        job.owned_buffer(
            certificate
                .checked_mul(work_keys.len() as u64)
                .and_then(|n| n.checked_mul(16))
                .ok_or(QueueError::TooLarge)?,
        )?,
    );
    let mut snapshot = RecordSnapshot {
        next_sequence: sequence,
        ..Default::default()
    };
    for key in work_keys {
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
                if !check_keys.contains(key) {
                    payload::load_payload(
                        connection,
                        id,
                        &schema.work,
                        payload::PayloadSelection {
                            fields: if complete_work_fields {
                                None
                            } else {
                                Some(&names)
                            },
                            include_body: body_keys.contains(key),
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
                work.field_coverage = if check_keys.contains(key) {
                    FieldCoverage::Selected {
                        names: empty_names.clone(),
                        prefixes: Default::default(),
                    }
                } else if complete_work_fields {
                    FieldCoverage::Complete
                } else {
                    FieldCoverage::Selected {
                        names: names.clone(),
                        prefixes: Default::default(),
                    }
                };
                work.body_loaded = body_keys.contains(key);
                snapshot.versions.insert(identity, work.record.version);
                snapshot.work.insert(key.into(), *work);
            }
        }
    }
    for (position, update) in group.state_updates.iter().enumerate() {
        let collection = collection_number(schema, Some(&update.collection))?;
        let definition = collection_definition(schema, collection)?;
        let authority = super::state_authority::load_state_authority(
            connection,
            queue_id,
            collection,
            &update.key,
        )
        .await?;
        let source = states
            .map(|states| states.descriptor(position, &update.key))
            .transpose()?;
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
                collection: Some(update.collection.clone()),
                key: update.key.clone(),
            },
            version,
        );
        let Some(id) = id else { continue };
        let mut fields = Fields::new();
        let mut body = bytes::Bytes::new();
        let mut total = 0;
        let mut exceeded = false;
        if update.change != StateChange::Check {
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
            (update.collection.clone(), update.key.clone()),
            StateRecord {
                collection: update.collection.clone(),
                key: update.key.clone(),
                version,
                fields,
                value: bytes::Bytes::new(),
            },
        );
    }
    Ok(snapshot)
}
