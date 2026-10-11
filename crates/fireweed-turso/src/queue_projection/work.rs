//! Shared fixed-size native work metadata. Payloads are loaded only after raw
//! ownership/eligibility checks under the same transaction snapshot.
use super::*;
pub(super) enum WorkAuthority {
    Absent,
    Deleted { version: u64 },
    Present { id: i64, work: Box<StoredWork> },
}
pub(super) async fn load_current_work(
    connection: &Connection,
    queue_id: i64,
    key: &str,
    insertion_head: u64,
) -> QueueResult<Option<(i64, StoredWork)>> {
    match load_work_authority(connection, queue_id, key, insertion_head).await? {
        WorkAuthority::Absent => Ok(None),
        WorkAuthority::Deleted { .. } => Err(QueueError::ClaimExpired),
        WorkAuthority::Present { id, work } => Ok(Some((id, *work))),
    }
}
/// Addressed metadata only. The caller admits full key/header ownership before
/// this function constructs a StoredWork; deleted keys preserve the version HWM.
pub(super) async fn load_work_authority(
    connection: &Connection,
    queue_id: i64,
    key: &str,
    insertion_head: u64,
) -> QueueResult<WorkAuthority> {
    let key_hash = discovery_digest(key.as_bytes());
    let row = one(connection,"SELECT CASE WHEN typeof(id)='integer' THEN id ELSE NULL END,CASE WHEN typeof(version)='blob' AND length(version)=8 THEN version ELSE NULL END,CASE WHEN typeof(active)='integer' THEN active ELSE NULL END,CASE WHEN typeof(lifecycle)='integer' THEN lifecycle ELSE NULL END,CASE WHEN typeof(priority)='integer' THEN priority ELSE NULL END,CASE WHEN typeof(available_at)='integer' THEN available_at ELSE NULL END,CASE WHEN typeof(eligible_since)='integer' THEN eligible_since ELSE NULL END,CASE WHEN typeof(attempts)='integer' THEN attempts ELSE NULL END,CASE WHEN lease_until IS NULL OR typeof(lease_until)='integer' THEN lease_until ELSE '' END,CASE WHEN lease_hash IS NULL OR (typeof(lease_hash)='blob' AND length(lease_hash)=32) THEN lease_hash ELSE x'' END,CASE WHEN typeof(insertion_sequence)='blob' AND length(insertion_sequence)=8 THEN insertion_sequence ELSE NULL END FROM q7_records WHERE queue_id=?1 AND collection=0 AND key_hash=?2 AND key=?3",vec![Value::Integer(queue_id),blob(&key_hash),blob(key.as_bytes())],11).await?;
    let Some(row) = row else {
        return Ok(WorkAuthority::Absent);
    };
    let version = unsigned(&row[1])?;
    let id = integer(&row[0])?;
    if id <= 0 || version == 0 {
        return Err(unknown("invalid work high-water metadata"));
    }
    match integer(&row[2])? {
        0 => return Ok(WorkAuthority::Deleted { version }),
        1 => {}
        _ => return Err(unknown("invalid active state")),
    }
    let lifecycle = match integer(&row[3])? {
        0 => Lifecycle::Pending,
        1 => Lifecycle::Leased,
        2 => Lifecycle::Completed,
        3 => Lifecycle::Failed,
        _ => return Err(unknown("invalid lifecycle")),
    };
    let expiry = match row[8] {
        Value::Null => None,
        _ => Some(integer(&row[8])?),
    };
    let hash = match &row[9] {
        Value::Null => None,
        _ => Some(bytes(&row[9])?.try_into().map_err(unknown)?),
    };

    let insertion_sequence = unsigned(&row[10])?;
    if insertion_sequence == 0 || insertion_sequence > insertion_head {
        return Err(unknown("invalid current work metadata"));
    }
    Ok(WorkAuthority::Present {
        id,
        work: Box::new(StoredWork {
            record: WorkRecord {
                work: NewWork {
                    key: key.into(),
                    priority: integer(&row[4])?,
                    available_at: integer(&row[5])?,
                    fields: Fields::new(),
                    body: bytes::Bytes::new(),
                },
                version,
                insertion_sequence,
                lifecycle,
                eligible_since: integer(&row[6])?,
                attempts: u32::try_from(integer(&row[7])?).map_err(unknown)?,
                lease_expires_at: expiry,
            },
            lease_hash: hash,
            field_coverage: fireweed_engine::queue_fields::FieldCoverage::Selected {
                names: Default::default(),
                prefixes: Default::default(),
            },
            body_loaded: false,
        }),
    })
}
