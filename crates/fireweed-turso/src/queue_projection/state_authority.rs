//! Exact addressed state metadata shared by planners and consumer reads.
use super::*;
pub(super) enum StateAuthority {
    Absent,
    Deleted { version: u64 },
    Present { id: i64, version: u64 },
}
pub(super) async fn load_state_authority(
    connection: &Connection,
    queue_id: i64,
    collection: i32,
    key: &str,
) -> QueueResult<StateAuthority> {
    let digest = discovery_digest(key.as_bytes());
    let row=one(connection,"SELECT CASE WHEN typeof(id)='integer' THEN id ELSE NULL END,CASE WHEN typeof(version)='blob' AND length(version)=8 THEN version ELSE NULL END,CASE WHEN typeof(active)='integer' THEN active ELSE NULL END FROM q7_records WHERE queue_id=?1 AND collection=?2 AND key_hash=?3 AND key=?4",vec![Value::Integer(queue_id),Value::Integer(i64::from(collection)),blob(&digest),blob(key.as_bytes())],3).await?;
    let Some(row) = row else {
        return Ok(StateAuthority::Absent);
    };
    let id = integer(&row[0])?;
    let version = unsigned(&row[1])?;
    if id <= 0 || version == 0 {
        return Err(unknown("invalid state high-water metadata"));
    }
    match integer(&row[2])? {
        0 => Ok(StateAuthority::Deleted { version }),
        1 => Ok(StateAuthority::Present { id, version }),
        _ => Err(unknown("invalid state active metadata")),
    }
}
