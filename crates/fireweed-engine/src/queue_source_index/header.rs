//! Header facts loaded together under one original authenticated source cut.
use super::{
    QueueSourceGenesis, QueueSourceKey, QueueSourceReader, genesis::load_source_genesis_at,
};
use crate::queue_admission::QueueReservation;
use fireweed_core::queue_contract::{QueueError, QueueResult};
use std::sync::Arc;
fn unknown(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("source header: {message}"))
}
/// Constructor-local facts; the adapter retains the source capability alongside
/// the verified persisted record and definition in its recovery owner.
pub struct QueueSourceHeader {
    genesis: QueueSourceGenesis,
    sequence: u64,
}
impl QueueSourceHeader {
    pub fn genesis(&self) -> &QueueSourceGenesis {
        &self.genesis
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}
pub async fn load_source_header<R: QueueSourceReader>(
    reader: &R,
    job: &Arc<QueueReservation>,
) -> QueueResult<QueueSourceHeader> {
    // Never recapture after the first await, including between header points.
    let cut = reader.cut()?;
    let genesis = load_source_genesis_at(reader, cut, job).await?;
    let key = QueueSourceKey::Sequence;
    let _charge = job.owned_buffer(key.encoded_len()? as u64)?;
    let key = key.encode()?;
    let owner = reader.lookup(&key).await?;
    let point = reader.point(&owner)?;
    let bytes = point
        .value_for(&cut, &key)?
        .ok_or_else(|| unknown("covered source has no sequence fact"))?;
    if bytes.len() > 10 {
        return Err(unknown("invalid canonical sequence"));
    }
    let sequence: u64 = crate::queue_storage_layout::decode(bytes)?;
    let mut scratch = [0u8; 10];
    let canonical = postcard::to_slice(&sequence, &mut scratch)
        .map_err(|_| unknown("invalid canonical sequence"))?;
    if canonical != bytes || (cut.through() == 1 && sequence != 0) {
        return Err(unknown(
            "invalid canonical sequence or genesis-only sequence",
        ));
    }
    Ok(QueueSourceHeader { genesis, sequence })
}
