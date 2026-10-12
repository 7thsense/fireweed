//! Addressed ordinary group read set. Work uses explicit field/body knowledge;
//! state changes replace/delete complete values and need only old fields/version.
use super::*;
use fireweed_engine::queue_planner::{RecordIdentity, RecordSnapshot, required_work_fields};
use std::collections::BTreeSet;
pub(super) async fn load_group(
    connection: &Connection,
    queue_id: i64,
    schema: &QueueSchema,
    group: &CommitGroup,
    sequence: u64,
    job: &Arc<QueueReservation>,
    charges: &mut Vec<OwnedQueueBufferCharge>,
) -> QueueResult<RecordSnapshot> {
    let names = required_work_fields(group, schema)?;
    let work_keys: BTreeSet<_> = group
        .claimed_updates
        .iter()
        .map(|u| u.key.as_str())
        .chain(group.work_updates.iter().map(|u| u.key.as_str()))
        .chain(group.work_inserts.iter().map(|u| u.key.as_str()))
        .collect();
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
                payload::load_payload(
                    connection,
                    id,
                    &schema.work,
                    payload::PayloadSelection {
                        fields: Some(&names),
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
                work.field_coverage = FieldCoverage::Selected {
                    names: names.clone(),
                    prefixes: Default::default(),
                };
                work.body_loaded = body_keys.contains(key);
                snapshot.versions.insert(identity, work.record.version);
                snapshot.work.insert(key.into(), *work);
            }
        }
    }
    for update in &group.state_updates {
        let collection = collection_number(schema, Some(&update.collection))?;
        let definition = collection_definition(schema, collection)?;
        let digest = discovery_digest(update.key.as_bytes());
        let row=one(connection,"SELECT CASE WHEN typeof(id)='integer' THEN id ELSE NULL END,CASE WHEN typeof(version)='blob' AND length(version)=8 THEN version ELSE NULL END,CASE WHEN typeof(active)='integer' THEN active ELSE NULL END FROM q7_records WHERE queue_id=?1 AND collection=?2 AND key_hash=?3 AND key=?4",vec![Value::Integer(queue_id),Value::Integer(i64::from(collection)),blob(&digest),blob(update.key.as_bytes())],3).await?;
        let Some(row) = row else { continue };
        let id = integer(&row[0])?;
        let version = unsigned(&row[1])?;
        if id <= 0 || version == 0 {
            return Err(unknown("invalid state high-water metadata"));
        }
        snapshot.versions.insert(
            RecordIdentity {
                collection: Some(update.collection.clone()),
                key: update.key.clone(),
            },
            version,
        );
        match integer(&row[2])? {
            0 => continue,
            1 => {}
            _ => return Err(unknown("invalid state active metadata")),
        };
        let mut fields = Fields::new();
        let mut body = bytes::Bytes::new();
        let mut total = 0;
        let mut exceeded = false;
        payload::load_payload(
            connection,
            id,
            definition,
            payload::PayloadSelection {
                fields: None,
                include_body: false,
                max_bytes: u64::MAX,
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
        .await?;
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
