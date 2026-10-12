//! One scoped authenticated query-cursor format for all API-007 backends.
use crate::queue_storage_layout::{decode, encode};
use fireweed_core::queue_contract::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[derive(Serialize, Deserialize)]
pub struct QueryCursor {
    pub scope: [u8; 32],
    pub sort: Vec<u8>,
    pub key: Vec<u8>,
}
pub fn cursor_mac(secret: &[u8], message: &[u8]) -> [u8; 32] {
    let mut key = [0u8; 64];
    if secret.len() > 64 {
        key[..32].copy_from_slice(&Sha256::digest(secret));
    } else {
        key[..secret.len()].copy_from_slice(secret);
    }
    let mut inner = Sha256::new();
    inner.update(key.map(|b| b ^ 0x36));
    inner.update(message);
    let digest = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(key.map(|b| b ^ 0x5c));
    outer.update(digest);
    outer.finalize().into()
}
pub fn make_cursor(cursor: QueryCursor, secret: &[u8]) -> QueueResult<Vec<u8>> {
    let bytes = encode(&(1u8, cursor))?;
    encode(&(cursor_mac(secret, &bytes), bytes))
}
pub fn read_cursor(raw: &[u8], secret: &[u8], scope: [u8; 32]) -> QueueResult<QueryCursor> {
    let invalid = || QueueError::Invalid("invalid or mismatched query cursor".into());
    let (tag, bytes): ([u8; 32], Vec<u8>) = decode(raw).map_err(|_| invalid())?;
    let expected = cursor_mac(secret, &bytes);
    if tag.iter().zip(expected).fold(0u8, |d, (a, b)| d | (a ^ b)) != 0 {
        return Err(invalid());
    }
    let (version, cursor): (u8, QueryCursor) = decode(&bytes).map_err(|_| invalid())?;
    if version != 1 || cursor.scope != scope {
        return Err(invalid());
    }
    Ok(cursor)
}
